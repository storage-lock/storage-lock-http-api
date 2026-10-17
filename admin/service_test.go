package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	memory_storage "github.com/storage-lock/go-memory-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	options := *storage_lock.NewStorageLockOptions()
	options.LeaseExpireAfter = 3 * time.Second
	options.LeaseRefreshInterval = time.Second
	svc, err := NewService(memory_storage.NewMemoryStorage(), "", options)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func doJSON(t *testing.T, srv *httptest.Server, method, path string, body any) (*http.Response, string) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, srv.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp, string(data)
}

func TestServiceAcquireRelease(t *testing.T) {
	svc := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	resp, body := doJSON(t, srv, http.MethodPost, "/locks/order-1/acquire", map[string]string{"owner_id": "svc-a"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("acquire status = %d, body = %s", resp.StatusCode, body)
	}
	var result AcquireResult
	if err := json.Unmarshal([]byte(body), &result); err != nil {
		t.Fatal(err)
	}
	if result.OwnerID != "svc-a" || result.FencingToken == 0 {
		t.Fatalf("acquire result = %#v", result)
	}

	// 他人获取同一锁 → 409。
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/order-1/acquire", map[string]string{"owner_id": "svc-b"})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second acquire status = %d, body = %s", resp.StatusCode, body)
	}

	// owner 释放成功。
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/order-1/release", map[string]string{"owner_id": "svc-a"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("release status = %d, body = %s", resp.StatusCode, body)
	}

	// 释放后他人可获取。
	resp, _ = doJSON(t, srv, http.MethodPost, "/locks/order-1/acquire", map[string]string{"owner_id": "svc-b"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("acquire after release status = %d", resp.StatusCode)
	}
}

func TestServiceNonOwnerReleaseFails(t *testing.T) {
	svc := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	doJSON(t, srv, http.MethodPost, "/locks/job/acquire", map[string]string{"owner_id": "svc-a"})
	resp, body := doJSON(t, srv, http.MethodPost, "/locks/job/release", map[string]string{"owner_id": "svc-b"})
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("non-owner release should fail, body = %s", body)
	}
}

func TestServiceRenew(t *testing.T) {
	svc := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	doJSON(t, srv, http.MethodPost, "/locks/lease/acquire", map[string]string{"owner_id": "svc-a", "ttl": "3s"})
	resp, body := doJSON(t, srv, http.MethodPost, "/locks/lease/renew", map[string]string{"owner_id": "svc-a"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("renew status = %d, body = %s", resp.StatusCode, body)
	}
	// renew 后租约仍有效（TTL > 0）。
	time.Sleep(500 * time.Millisecond)
	resp, _ = doJSON(t, srv, http.MethodPost, "/locks/lease/acquire", map[string]string{"owner_id": "svc-b"})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("lock should still be held after renew, status = %d", resp.StatusCode)
	}
}

func TestServiceLeaseExpires(t *testing.T) {
	svc := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	doJSON(t, srv, http.MethodPost, "/locks/short/acquire", map[string]string{"owner_id": "svc-a", "ttl": "3s"})
	time.Sleep(3100 * time.Millisecond)
	// 租约过期后他人可获取。
	resp, _ := doJSON(t, srv, http.MethodPost, "/locks/short/acquire", map[string]string{"owner_id": "svc-b"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("acquire after lease expiry = %d", resp.StatusCode)
	}
}

func TestServiceWaitAcquire(t *testing.T) {
	svc := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	doJSON(t, srv, http.MethodPost, "/locks/w/acquire", map[string]string{"owner_id": "svc-a"})
	done := make(chan *http.Response, 1)
	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		raw, _ := json.Marshal(map[string]any{"owner_id": "svc-b", "wait": true})
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/locks/w/acquire", bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		done <- resp
	}()
	time.Sleep(300 * time.Millisecond)
	// 释放后等待方拿到锁。
	doJSON(t, srv, http.MethodPost, "/locks/w/release", map[string]string{"owner_id": "svc-a"})
	select {
	case resp := <-done:
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("wait acquire = %d", resp.StatusCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait acquire timed out")
	}
}

func TestServiceNamespaceIsolation(t *testing.T) {
	options := *storage_lock.NewStorageLockOptions()
	options.LeaseExpireAfter = 3 * time.Second
	options.LeaseRefreshInterval = time.Second
	svc, err := NewService(memory_storage.NewMemoryStorage(), "tenant-a", options)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	doJSON(t, srv, http.MethodPost, "/locks/order/acquire", map[string]string{"owner_id": "a"})
	// 同 namespace 下冲突。
	resp, _ := doJSON(t, srv, http.MethodPost, "/locks/order/acquire", map[string]string{"owner_id": "b"})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("same-namespace second acquire = %d", resp.StatusCode)
	}

	// 另一个 namespace 的服务看不到/不冲突（物理隔离）。
	svc2, _ := NewService(memory_storage.NewMemoryStorage(), "tenant-b", options)
	srv2 := httptest.NewServer(svc2.Handler())
	defer srv2.Close()
	resp2, _ := doJSON(t, srv2, http.MethodPost, "/locks/order/acquire", map[string]string{"owner_id": "c"})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("cross-namespace acquire should succeed, got %d", resp2.StatusCode)
	}
}

func TestServiceInspectReadOnlyEndpointsStillWork(t *testing.T) {
	svc := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	doJSON(t, srv, http.MethodPost, "/locks/see/acquire", map[string]string{"owner_id": "svc-a"})
	resp, body := doJSON(t, srv, http.MethodGet, "/locks/see", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("inspect status = %d", resp.StatusCode)
	}
	if body == "" {
		t.Fatal("empty inspect body")
	}
	resp, body = doJSON(t, srv, http.MethodGet, "/locks/see/waiters", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("waiters status = %d", resp.StatusCode)
	}
	resp, _ = doJSON(t, srv, http.MethodGet, "/locks", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d", resp.StatusCode)
	}
}

var _ = context.Background
