package admin

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	memory_storage "github.com/storage-lock/go-memory-storage"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

type httpClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *httpClock) GetTime(context.Context) (time.Time, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now, nil
}

func (c *httpClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *httpClock) Set(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}

func newClockService(t *testing.T, clock *httpClock, fair bool, depth int) (*Service, *memory_storage.MemoryStorage) {
	t.Helper()
	options := *storage_lock.NewStorageLockOptions()
	options.LeaseExpireAfter = 3 * time.Second
	options.LeaseRefreshInterval = time.Second
	options.TimeProvider = clock
	options.Fair = fair
	options.MaxReentrantDepth = depth
	store := memory_storage.NewMemoryStorage()
	svc, err := NewService(store, "", options)
	if err != nil {
		t.Fatal(err)
	}
	return svc, store
}

func readLock(t *testing.T, store *memory_storage.MemoryStorage, id string) *storage.LockInformation {
	t.Helper()
	raw, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	info, err := storage.LockInformationFromJsonString(raw)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func TestHTTPBranches(t *testing.T) {
	clock := &httpClock{now: time.Now()}
	svc, store := newClockService(t, clock, false, 0)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	for _, id := range []string{"fresh-a", "fresh-b"} {
		resp, body := doJSON(t, srv, http.MethodPost, "/locks/"+id+"/acquire", map[string]string{"owner_id": "owner"})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("acquire %s = %d %s", id, resp.StatusCode, body)
		}
		if !strings.Contains(body, `"fencing_token":1`) {
			t.Fatalf("acquire %s body = %s", id, body)
		}
		fmt.Printf("acquire %s %s", id, body)
		resp, body = doJSON(t, srv, http.MethodPost, "/locks/"+id+"/release", map[string]string{"owner_id": "owner"})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("release %s = %d %s", id, resp.StatusCode, body)
		}
	}

	resp, body := doJSON(t, srv, http.MethodPost, "/locks/shared/acquire", map[string]string{
		"owner_id": "reader-a", "mode": "read",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reader-a = %d %s", resp.StatusCode, body)
	}
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/shared/acquire", map[string]string{
		"owner_id": "reader-b", "mode": "read",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reader-b = %d %s", resp.StatusCode, body)
	}
	shared := readLock(t, store, "shared")
	if !shared.ReadLock || shared.ReadDepth("reader-a") != 1 || shared.ReadDepth("reader-b") != 1 {
		t.Fatalf("shared record = %+v", shared)
	}
	if shared.OwnerId != "reader-b" {
		t.Fatalf("last owner = %s", shared.OwnerId)
	}
	beforeWrite := *shared
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/shared/acquire", map[string]string{"owner_id": "writer"})
	if resp.StatusCode != http.StatusConflict || !strings.Contains(body, storage_lock.ErrLockBusy.Error()) {
		t.Fatalf("write on read = %d %s", resp.StatusCode, body)
	}
	afterWrite := readLock(t, store, "shared")
	if afterWrite.Version != beforeWrite.Version || !afterWrite.LeaseExpireTime.Equal(beforeWrite.LeaseExpireTime) || afterWrite.OwnerId != beforeWrite.OwnerId {
		t.Fatalf("write changed record from %+v to %+v", beforeWrite, *afterWrite)
	}
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/shared/acquire", map[string]string{
		"owner_id": "omitted", "mode": "write",
	})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("explicit write = %d %s", resp.StatusCode, body)
	}

	clock.Advance(time.Second)
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/shared/renew", map[string]string{"owner_id": "reader-a"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reader renew = %d %s", resp.StatusCode, body)
	}
	renewed := readLock(t, store, "shared")
	if !renewed.LeaseExpireTime.After(shared.LeaseExpireTime) {
		t.Fatalf("lease did not move: %s -> %s", shared.LeaseExpireTime, renewed.LeaseExpireTime)
	}
	if !renewed.ReadLock || renewed.ReadDepth("reader-a") != 1 || renewed.ReadDepth("reader-b") != 1 || renewed.OwnerId != "reader-b" {
		t.Fatalf("reader renew rewrote holders: %+v", renewed)
	}
	fmt.Printf("reader renew %s", body)

	stable := *renewed
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/shared/renew", map[string]string{"owner_id": "stranger"})
	if resp.StatusCode != http.StatusLocked || !strings.Contains(body, storage_lock.ErrLockNotBelongYou.Error()) {
		t.Fatalf("stranger renew = %d %s", resp.StatusCode, body)
	}
	if got := readLock(t, store, "shared"); got.Version != stable.Version || !got.LeaseExpireTime.Equal(stable.LeaseExpireTime) {
		t.Fatalf("stranger renew changed %+v", got)
	}

	clock.Set(stable.LeaseExpireTime)
	expiredResp, expiredBody := doJSON(t, srv, http.MethodPost, "/locks/shared/renew", map[string]string{"owner_id": "reader-a"})
	if expiredResp.StatusCode != http.StatusGone || !strings.Contains(expiredBody, storage_lock.ErrLeaseExpired.Error()) {
		t.Fatalf("exact renew = %d %s", expiredResp.StatusCode, expiredBody)
	}
	if got := readLock(t, store, "shared"); got.Version != stable.Version || !got.LeaseExpireTime.Equal(stable.LeaseExpireTime) {
		t.Fatalf("exact renew changed %+v", got)
	}

	resp, view := doJSON(t, srv, http.MethodGet, "/locks/shared", nil)
	if resp.StatusCode != http.StatusOK || !strings.Contains(view, `"read_lock":true`) {
		t.Fatalf("inspect = %d %s", resp.StatusCode, view)
	}
	fmt.Printf("inspect %s", view)

	statuses := map[int]string{}
	check := func(name string, status int, body, text string) {
		t.Helper()
		if !strings.Contains(body, text) {
			t.Fatalf("%s body %q does not contain %q", name, body, text)
		}
		if status == http.StatusInternalServerError {
			t.Fatalf("%s mapped to 500", name)
		}
		if prev, ok := statuses[status]; ok {
			t.Fatalf("%s and %s both use %d", prev, name, status)
		}
		statuses[status] = name
	}

	check("lease expired", expiredResp.StatusCode, expiredBody, storage_lock.ErrLeaseExpired.Error())

	resp, body = doJSON(t, srv, http.MethodPost, "/locks/missing/renew", map[string]string{"owner_id": "nobody"})
	check("not found", resp.StatusCode, body, storage_lock.ErrLockNotFound.Error())
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("not found = %d", resp.StatusCode)
	}

	resp, body = doJSON(t, srv, http.MethodPost, "/locks/busy/acquire", map[string]string{"owner_id": "holder"})
	if resp.StatusCode != http.StatusOK {
		t.Fatal(body)
	}
	busyBefore := readLock(t, store, "busy")
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/busy/acquire", map[string]string{"owner_id": "other"})
	check("busy", resp.StatusCode, body, storage_lock.ErrLockBusy.Error())
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("busy = %d", resp.StatusCode)
	}
	if got := readLock(t, store, "busy"); got.Version != busyBefore.Version || got.OwnerId != busyBefore.OwnerId || !got.LeaseExpireTime.Equal(busyBefore.LeaseExpireTime) {
		t.Fatalf("busy acquire changed %+v", got)
	}

	resp, body = doJSON(t, srv, http.MethodPost, "/locks/empty-owner/renew", map[string]string{"owner_id": ""})
	check("empty owner", resp.StatusCode, body, storage_lock.ErrOwnerIdEmpty.Error())
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty owner = %d %s", resp.StatusCode, body)
	}

	resp, body = doJSON(t, srv, http.MethodPost, "/locks/busy/release", map[string]string{"owner_id": "other"})
	check("not belong", resp.StatusCode, body, storage_lock.ErrLockNotBelongYou.Error())
	if got := readLock(t, store, "busy"); got.Version != busyBefore.Version {
		t.Fatalf("release changed %+v", got)
	}

	fairClock := &httpClock{now: time.Now()}
	fairSvc, fairStore := newClockService(t, fairClock, true, 0)
	now, err := fairClock.GetTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	seed := &storage.LockInformation{
		LockId: "fair-turn", OwnerId: "previous", Version: 4, LockCount: 0,
		LeaseExpireTime: now.Add(time.Hour),
		Waiters:         []storage.LockWaiter{{OwnerId: "head", Mode: "write", Since: now}},
	}
	if err := fairStore.CreateWithVersion(context.Background(), "fair-turn", 4, seed); err != nil {
		t.Fatal(err)
	}
	fairSrv := httptest.NewServer(fairSvc.Handler())
	defer fairSrv.Close()
	resp, body = doJSON(t, fairSrv, http.MethodPost, "/locks/fair-turn/acquire", map[string]string{"owner_id": "cutter"})
	check("not your turn", resp.StatusCode, body, storage_lock.ErrNotYourTurn.Error())
	if got := readLock(t, fairStore, "fair-turn"); got.Version != 4 || got.OwnerId != "previous" {
		t.Fatalf("fair acquire changed %+v", got)
	}

	intentClock := &httpClock{now: time.Now()}
	intentSvc, intentStore := newClockService(t, intentClock, false, 0)
	intentNow, err := intentClock.GetTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	intent := &storage.LockInformation{
		LockId: "intent", OwnerId: "reader-a", Version: 2, LockCount: 1, ReadLock: true,
		ReadOwners: map[string]int{"reader-a": 1}, LeaseExpireTime: intentNow.Add(time.Hour),
		WriteIntentOwner: "writer", WriteIntentSince: intentNow,
	}
	if err := intentStore.CreateWithVersion(context.Background(), "intent", 2, intent); err != nil {
		t.Fatal(err)
	}
	intentSrv := httptest.NewServer(intentSvc.Handler())
	defer intentSrv.Close()
	resp, body = doJSON(t, intentSrv, http.MethodPost, "/locks/intent/acquire", map[string]string{
		"owner_id": "newbie", "mode": "read",
	})
	check("upgrade conflict", resp.StatusCode, body, storage_lock.ErrUpgradeConflict.Error())
	if got := readLock(t, intentStore, "intent"); got.Version != 2 || got.OwnerId != "reader-a" || !got.LeaseExpireTime.Equal(intent.LeaseExpireTime) {
		t.Fatalf("upgrade conflict changed %+v", got)
	}

	depthClock := &httpClock{now: time.Now()}
	depthSvc, depthStore := newClockService(t, depthClock, false, 1)
	depthSrv := httptest.NewServer(depthSvc.Handler())
	defer depthSrv.Close()
	resp, body = doJSON(t, depthSrv, http.MethodPost, "/locks/depth/acquire", map[string]string{"owner_id": "owner"})
	if resp.StatusCode != http.StatusOK {
		t.Fatal(body)
	}
	depthBefore := readLock(t, depthStore, "depth")
	resp, body = doJSON(t, depthSrv, http.MethodPost, "/locks/depth/acquire", map[string]string{"owner_id": "owner"})
	check("max depth", resp.StatusCode, body, storage_lock.ErrMaxReentrantDepth.Error())
	if got := readLock(t, depthStore, "depth"); got.Version != depthBefore.Version || got.LockCount != depthBefore.LockCount {
		t.Fatalf("max depth changed %+v", got)
	}

	if len(statuses) != 8 {
		t.Fatalf("statuses = %v", statuses)
	}
}
