// Package admin 提供锁的管理与读写接口。
//
// 只读接口只报告谁持有、租约还剩多久、谁在等，不提供强制解锁（删除墓碑会回退 fencing token）。
// 读写接口把底层 Storage 的 CAS 租约锁暴露成无状态 HTTP：每次获取/续租/释放都携带 owner，
// 服务端不启动看门狗，客户端必须在租约内续租；获取返回的 fencing token 由业务资源侧原子校验。
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

// Service 把一个 Storage 暴露成锁的读写管理接口。
// Namespace 非空时所有 lock ID 自动加前缀，用于租户/环境隔离。
type Service struct {
	storage   storage.Storage
	options   storage_lock.StorageLockOptions
	namespace string
}

// NewService 创建锁服务。options 为空时使用默认租约配置。
// namespace 非空时所有 lock ID 自动加上该前缀，用于租户/环境隔离。
func NewService(s storage.Storage, namespace string, options storage_lock.StorageLockOptions) (*Service, error) {
	if s == nil {
		return nil, storage_lock.ErrStorageNil
	}
	if options.LeaseExpireAfter == 0 {
		options = *storage_lock.NewStorageLockOptions()
	}
	// HTTP 锁服务是无状态请求-响应：服务端不启动看门狗，租约按 TTL 自然过期，
	// 客户端必须在租约内通过 /renew 续租（参考 Redisson 的 lease 模式）。
	options.DisableWatchDog = true
	return &Service{storage: s, options: options, namespace: namespace}, nil
}

// Handler 返回完整路由（只读查询 + 读写操作）。
//
//	GET    /locks               列出锁
//	GET    /locks/{id}          查看一把锁
//	GET    /locks/{id}/waiters  查看等待队列
//	POST   /locks/{id}/acquire  获取锁：body {"owner_id","mode","ttl","wait"}
//	POST   /locks/{id}/renew    续租：body {"owner_id","ttl"}
//	POST   /locks/{id}/release  释放锁：body {"owner_id"}
//	DELETE /locks/{id}          释放锁（等价于 release）
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/locks", s.handleList)
	mux.HandleFunc("/locks/", s.handleLock)
	return mux
}

// ListenAndServe 在 addr 上提供锁服务接口。
func (s *Service) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, s.Handler())
}

// fullLockID 应用命名空间前缀。
func (s *Service) fullLockID(id string) string {
	if s.namespace == "" {
		return id
	}
	return s.namespace + "/" + id
}

// AcquireRequest 是获取锁的请求体。
type AcquireRequest struct {
	OwnerID string `json:"owner_id,omitempty"`
	// Mode 是 "write"（默认）或 "read"。
	Mode string `json:"mode,omitempty"`
	// TTL 是期望的租约时长（如 "10s"、"5m"）。空时用服务默认。
	TTL string `json:"ttl,omitempty"`
	// Wait 为 true 时阻塞等待直到拿到锁或 ctx 超时（默认 false，立即返回）。
	Wait bool `json:"wait,omitempty"`
}

// AcquireResult 是获取锁成功的响应体。
type AcquireResult struct {
	LockID       string          `json:"lock_id"`
	OwnerID      string          `json:"owner_id"`
	FencingToken storage.Version `json:"fencing_token"`
	LeaseExpire  string          `json:"lease_expire,omitempty"`
}

// ReleaseRequest 是释放/续租的请求体。
type ReleaseRequest struct {
	OwnerID string `json:"owner_id"`
	// TTL 仅在 renew 时使用。
	TTL string `json:"ttl,omitempty"`
}

func (s *Service) handleLock(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/locks/")
	id, action := splitAction(path)
	if id == "" {
		http.NotFound(w, r)
		return
	}
	switch action {
	case "":
		switch r.Method {
		case http.MethodGet:
			s.inspectLock(w, r, id)
		case http.MethodDelete:
			s.release(w, r, id)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "waiters":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.waiters(w, r, id)
	case "events":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.events(w, r, id)
	case "acquire":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.acquire(w, r, id)
	case "renew":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.renew(w, r, id)
	case "release":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.release(w, r, id)
	default:
		http.NotFound(w, r)
	}
}

// splitAction 把 "/locks/{id}/{action}" 拆成 id 与 action。
func splitAction(path string) (string, string) {
	if idx := strings.IndexByte(path, '/'); idx >= 0 {
		return path[:idx], path[idx+1:]
	}
	return path, ""
}

func (s *Service) acquire(w http.ResponseWriter, r *http.Request, id string) {
	var req AcquireRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body: "+err.Error(), http.StatusBadRequest)
		return
	}
	options := s.options
	if req.TTL != "" {
		ttl, err := time.ParseDuration(req.TTL)
		if err != nil {
			http.Error(w, "invalid ttl: "+err.Error(), http.StatusBadRequest)
			return
		}
		options.LeaseExpireAfter = ttl
	}
	options.LockId = s.fullLockID(id)
	lock, err := storage_lock.NewStorageLockWithOptions(s.storage, &options)
	if err != nil {
		writeError(w, err)
		return
	}
	owner := req.OwnerID
	if owner == "" {
		owner = storage_lock.NewOwnerIdGenerator().GenOwnerId()
	}
	var acquireErr error
	if req.Wait {
		acquireErr = lock.Lock(r.Context(), owner)
	} else {
		acquireErr = lock.TryLock(r.Context(), owner)
	}
	if acquireErr != nil {
		if errors.Is(acquireErr, storage_lock.ErrLockBusy) {
			writeError(w, acquireErr)
			return
		}
		writeError(w, acquireErr)
		return
	}
	token, err := lock.GetFencingToken(r.Context(), owner)
	if err != nil {
		_ = lock.UnLock(context.WithoutCancel(r.Context()), owner)
		writeError(w, err)
		return
	}
	expire, _ := lock.TTL(r.Context(), owner)
	writeJSON(w, AcquireResult{LockID: id, OwnerID: owner, FencingToken: token, LeaseExpire: expire.String()})
}

func (s *Service) renew(w http.ResponseWriter, r *http.Request, id string) {
	var req ReleaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.OwnerID == "" {
		http.Error(w, "owner_id required", http.StatusBadRequest)
		return
	}
	options := s.options
	options.LockId = s.fullLockID(id)
	lock, err := storage_lock.NewStorageLockWithOptions(s.storage, &options)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := lock.Renew(r.Context(), req.OwnerID); err != nil {
		writeError(w, err)
		return
	}
	expire, _ := lock.TTL(r.Context(), req.OwnerID)
	writeJSON(w, AcquireResult{LockID: id, OwnerID: req.OwnerID, LeaseExpire: expire.String()})
}

func (s *Service) release(w http.ResponseWriter, r *http.Request, id string) {
	var req ReleaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.OwnerID == "" {
		http.Error(w, "owner_id required", http.StatusBadRequest)
		return
	}
	options := s.options
	options.LockId = s.fullLockID(id)
	lock, err := storage_lock.NewStorageLockWithOptions(s.storage, &options)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := lock.UnLock(r.Context(), req.OwnerID); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, map[string]string{"released": id})
}

func (s *Service) inspectLock(w http.ResponseWriter, r *http.Request, id string) {
	server, err := New(s.storage)
	if err != nil {
		writeError(w, err)
		return
	}
	view, err := server.inspect(r.Context(), s.fullLockID(id))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, view)
}

func (s *Service) waiters(w http.ResponseWriter, r *http.Request, id string) {
	server, err := New(s.storage)
	if err != nil {
		writeError(w, err)
		return
	}
	view, err := server.inspect(r.Context(), s.fullLockID(id))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, view.Waiters)
}

func (s *Service) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	server, err := New(s.storage)
	if err != nil {
		writeError(w, err)
		return
	}
	server.list(w, r)
}
