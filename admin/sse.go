// SSE 推送：把锁状态变化实时推送给浏览器或非 Go 客户端，
// 复用 StorageLock.Subscribe（含跨实例/跨进程的存储原生 Watch 转发）。
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

const (
	// sseEventName 锁状态变更事件的 SSE event 名。
	sseEventName = "lock"
	// 保持连接的心跳间隔：低于多数代理的超时阈值，避免代理中断长连接。
	sseHeartbeatInterval = 25 * time.Second
)

// sseError 通过 SSE 通道把错误以 named event 推给客户端（HTTP 头已设置，无法再改状态码）。
type sseError struct {
	Error string `json:"error"`
}

// handleEvents 订阅指定锁的状态变化，以 Server-Sent Events 流推送。
// 首次连接先推送当前快照，之后每次锁状态变化推送一条事件。
// 客户端断开或请求 context 取消时清理订阅。
func (s *Service) events(w http.ResponseWriter, r *http.Request, id string) {
	fullID := s.fullLockID(id)
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	options := s.options
	options.LockId = fullID
	// SSE 是可观测端点，不需要看门狗协程（无锁可续租）。
	options.DisableWatchDog = true
	lock, err := storage_lock.NewStorageLockWithOptions(s.storage, &options)
	if err != nil {
		writeError(w, err)
		return
	}

	// 用短轮询兜底的锁创建方式：若存储不支持原生 Watch，Subscribe 仍会工作
	// （转发仅在有 LockWatchable 时启用，本地通知仍然可靠）。
	ch, err := lock.Subscribe(r.Context(), fullID)
	if err != nil {
		writeError(w, err)
		return
	}
	defer lock.Unsubscribe(fullID, ch)

	// 设置 SSE 响应头
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ctx := r.Context()

	// 心跳：定时发送注释行保持连接活性
	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()

	// 手动首推：Subscribe 已在内部 goroutine 推送当前快照，这里统一下游格式
	rawPush := func(event string, data any) error {
		raw, err := json.Marshal(data)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, raw); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	// 立即推送一次当前快照（若锁记录存在）
	if initial, err := s.peekLock(fullID); err == nil && initial != nil {
		_ = rawPush("snapshot", initial)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			if _, err := w.Write([]byte(": ping\n\n")); err != nil {
				return
			}
			flusher.Flush()
		case info, ok := <-ch:
			if !ok {
				// 订阅通道被关闭（例如 Unsubscribe），结束流
				return
			}
			server, err := New(s.storage)
			if err != nil {
				return
			}
			_ = rawPush(sseEventName, server.view(info))
		}
	}
}

// peekLock 读取一把锁的当前快照，锁记录不存在时返回 nil（不是错误）。
func (s *Service) peekLock(id string) (*storage.LockInformation, error) {
	lock, err := storage_lock.NewStorageLock(s.storage, id)
	if err != nil {
		return nil, err
	}
	status, err := lock.Inspect(context.Background())
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &status.Information, nil
}

func isNotFound(err error) bool {
	return errors.Is(err, storage_lock.ErrLockNotFound)
}
