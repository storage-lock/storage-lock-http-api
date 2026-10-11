// Package admin 提供锁的只读管理接口。
// 它只报告谁持有、租约还剩多久、谁在等，不提供强制解锁：
// 强制删除墓碑会回退 fencing token。
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

// Server 把一个 Storage 暴露成管理查询接口。
type Server struct {
	storage storage.Storage
	now     func() time.Time
}

// New 创建管理服务。storage 必须已经初始化。
func New(s storage.Storage) (*Server, error) {
	if s == nil {
		return nil, storage_lock.ErrStorageNil
	}
	return &Server{storage: s, now: time.Now}, nil
}

// Handler 返回管理路由。
//
//	GET /locks              列出锁
//	GET /locks/{id}         查看一把锁
//	GET /locks/{id}/waiters 查看等待队列
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/locks", s.list)
	mux.HandleFunc("/locks/", s.lock)
	return mux
}

// ListenAndServe 在 addr 上提供管理接口。
func (s *Server) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, s.Handler())
}

type lockView struct {
	LockID           string                `json:"lock_id"`
	OwnerID          string                `json:"owner_id,omitempty"`
	Held             bool                  `json:"held"`
	ReadLock         bool                  `json:"read_lock"`
	LockCount        int                   `json:"lock_count"`
	Version          storage.Version       `json:"version"`
	RemainingLease   string                `json:"remaining_lease,omitempty"`
	WriteIntentOwner string                `json:"write_intent_owner,omitempty"`
	Waiters          []storage.LockWaiter  `json:"waiters,omitempty"`
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	items, err := storage_lock.ListLocks(r.Context(), s.storage, "")
	if err != nil {
		writeError(w, err)
		return
	}
	views := []lockView{}
	for items.Next() {
		if info := items.Value(); info != nil {
			views = append(views, s.view(info))
		}
	}
	writeJSON(w, views)
}

func (s *Server) lock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/locks/")
	id, waitersOnly := strings.CutSuffix(path, "/waiters")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	view, err := s.inspect(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	if waitersOnly {
		writeJSON(w, view.Waiters)
		return
	}
	writeJSON(w, view)
}

func (s *Server) inspect(ctx context.Context, id string) (lockView, error) {
	lock, err := storage_lock.NewStorageLock(s.storage, id)
	if err != nil {
		return lockView{}, err
	}
	status, err := lock.Inspect(ctx)
	if err != nil {
		return lockView{}, err
	}
	return s.view(&status.Information), nil
}

func (s *Server) view(info *storage.LockInformation) lockView {
	now := s.now()
	held := info.LockCount > 0 && now.Before(info.LeaseExpireTime)
	view := lockView{
		LockID: info.LockId, OwnerID: info.OwnerId, Held: held, ReadLock: info.ReadLock,
		LockCount: info.LockCount, Version: info.Version, WriteIntentOwner: info.WriteIntentOwner,
		Waiters: append([]storage.LockWaiter(nil), info.Waiters...),
	}
	if held {
		view.RemainingLease = info.LeaseExpireTime.Sub(now).String()
	}
	return view
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, storage_lock.ErrLockNotFound) {
		status = http.StatusNotFound
	}
	http.Error(w, err.Error(), status)
}
