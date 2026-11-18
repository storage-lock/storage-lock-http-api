package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"context"
	"strings"
	"testing"

	"github.com/storage-lock/go-memory-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/require"
)

// TestListDoesNotLeakAcrossNamespaces 锁住一个越权 bug：
// handleList 之前调 admin.New(s.storage).list，而 Server.list 用空前缀
// ListLocks(storage, "")，于是挂着 namespace 的 Service 会把所有租户的锁都列出来。
func TestListDoesNotLeakAcrossNamespaces(t *testing.T) {
	store := memory_storage.NewMemoryStorage()
	ctx := context.Background()

	svcA, err := NewService(store, "tenant-a", storage_lock.StorageLockOptions{})
	require.NoError(t, err)
	svcB, err := NewService(store, "tenant-b", storage_lock.StorageLockOptions{})
	require.NoError(t, err)

	// 两个租户各持一把锁。
	a, err := storage_lock.NewStorageLock(store, "tenant-a/shared")
	require.NoError(t, err)
	require.NoError(t, a.Lock(ctx, "owner-a"))
	b, err := storage_lock.NewStorageLock(store, "tenant-b/shared")
	require.NoError(t, err)
	require.NoError(t, b.Lock(ctx, "owner-b"))

	// 租户 A 只应该看到自己那把。
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/locks", nil)
	svcA.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var gotA []map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gotA))
	require.Len(t, gotA, 1, "租户 A 只应看到自己的锁")
	require.Equal(t, "tenant-a/shared", gotA[0]["lock_id"])

	// 租户 B 同理。
	rec = httptest.NewRecorder()
	svcB.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/locks", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var gotB []map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gotB))
	require.Len(t, gotB, 1)
	require.Equal(t, "tenant-b/shared", gotB[0]["lock_id"])
}

// TestSameLockIDIsolatedAcrossNamespaces 验证同名的 lock id 在不同租户下互不干扰。
func TestSameLockIDIsolatedAcrossNamespaces(t *testing.T) {
	store := memory_storage.NewMemoryStorage()
	svcA, err := NewService(store, "tenant-a", storage_lock.StorageLockOptions{})
	require.NoError(t, err)
	svcB, err := NewService(store, "tenant-b", storage_lock.StorageLockOptions{})
	require.NoError(t, err)

	body := `{"owner_id":"same-owner"}`
	acquire := func(svc *Service) int {
		rec := httptest.NewRecorder()
		svc.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/locks/same/acquire", strings.NewReader(body)))
		return rec.Code
	}

	require.Equal(t, http.StatusOK, acquire(svcA))
	// 不同租户下同名锁必须仍然可以拿到，两者互不相干。
	require.Equal(t, http.StatusOK, acquire(svcB))
}
