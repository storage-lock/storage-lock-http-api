package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

func TestHTTPRenew(t *testing.T) {
	clock := &httpClock{now: time.Now()}
	svc, store := newClockService(t, clock, false, 0)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	for _, id := range []string{"fence-a", "fence-b"} {
		resp, body := doJSON(t, srv, http.MethodPost, "/locks/"+id+"/acquire", map[string]string{"owner_id": "owner"})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("acquire %s = %d %s", id, resp.StatusCode, body)
		}
		fmt.Printf("acquire %s %s", id, body)
		if !strings.Contains(body, `"fencing_token":1`) {
			t.Fatalf("acquire %s body = %s", id, body)
		}
	}
	fmt.Println("PASS fencing version 1")

	resp, body := doJSON(t, srv, http.MethodPost, "/locks/legal/acquire", map[string]string{"owner_id": "owner"})
	if resp.StatusCode != http.StatusOK {
		t.Fatal(body)
	}
	legalBefore := readLock(t, store, "legal")
	now, err := clock.GetTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/legal/renew", map[string]string{"owner_id": "owner", "ttl": "10s"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("legal renew = %d %s", resp.StatusCode, body)
	}
	legalAfter := readLock(t, store, "legal")
	if legalAfter.LeaseExpireTime.Before(now.Add(10*time.Second)) || legalAfter.LeaseExpireTime.Before(legalBefore.LeaseExpireTime) {
		t.Fatalf("legal expire %s now %s before %s", legalAfter.LeaseExpireTime, now, legalBefore.LeaseExpireTime)
	}
	if legalAfter.Version != legalBefore.Version+1 || legalAfter.OwnerId != legalBefore.OwnerId || legalAfter.LockCount != legalBefore.LockCount || legalAfter.ReadLock != legalBefore.ReadLock {
		t.Fatalf("legal renew rewrote holders: %+v", legalAfter)
	}
	fmt.Println("PASS legal duration")

	resp, body = doJSON(t, srv, http.MethodPost, "/locks/floor/acquire", map[string]string{"owner_id": "owner", "ttl": "10s"})
	if resp.StatusCode != http.StatusOK {
		t.Fatal(body)
	}
	floorBefore := readLock(t, store, "floor")
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/floor/renew", map[string]string{"owner_id": "owner", "ttl": "3s"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("floor renew = %d %s", resp.StatusCode, body)
	}
	floorAfter := readLock(t, store, "floor")
	if floorAfter.LeaseExpireTime.Before(floorBefore.LeaseExpireTime) {
		t.Fatalf("shorter ttl wrote earlier: %s -> %s", floorBefore.LeaseExpireTime, floorAfter.LeaseExpireTime)
	}

	illegalBefore := readLock(t, store, "floor")
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/floor/renew", map[string]string{"owner_id": "owner", "ttl": "1s"})
	if resp.StatusCode == http.StatusInternalServerError || resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, storage_lock.ErrLeaseExpireAfter.Error()) {
		t.Fatalf("short ttl = %d %s", resp.StatusCode, body)
	}
	if got := readLock(t, store, "floor"); got.Version != illegalBefore.Version || !got.LeaseExpireTime.Equal(illegalBefore.LeaseExpireTime) || got.OwnerId != illegalBefore.OwnerId || got.LockCount != illegalBefore.LockCount {
		t.Fatalf("short ttl changed %+v", got)
	}
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/floor/renew", map[string]string{"owner_id": "owner", "ttl": "abc"})
	if resp.StatusCode == http.StatusInternalServerError || !strings.Contains(body, "invalid ttl:") {
		t.Fatalf("bad ttl = %d %s", resp.StatusCode, body)
	}
	if got := readLock(t, store, "floor"); got.Version != illegalBefore.Version || !got.LeaseExpireTime.Equal(illegalBefore.LeaseExpireTime) {
		t.Fatalf("bad ttl changed %+v", got)
	}
	fmt.Println("PASS illegal duration")

	resp, body = doJSON(t, srv, http.MethodPost, "/locks/shared/acquire", map[string]string{"owner_id": "reader-a", "mode": "read"})
	if resp.StatusCode != http.StatusOK {
		t.Fatal(body)
	}
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/shared/acquire", map[string]string{"owner_id": "reader-b", "mode": "read"})
	if resp.StatusCode != http.StatusOK {
		t.Fatal(body)
	}
	sharedBefore := readLock(t, store, "shared")
	if sharedBefore.OwnerId != "reader-b" || sharedBefore.ReadDepth("reader-a") != 1 {
		t.Fatalf("shared = %+v", sharedBefore)
	}
	sharedNow, err := clock.GetTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/shared/renew", map[string]string{"owner_id": "reader-a", "ttl": "10s"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reader renew = %d %s", resp.StatusCode, body)
	}
	sharedAfter := readLock(t, store, "shared")
	if sharedAfter.LeaseExpireTime.Before(sharedNow.Add(10*time.Second)) || sharedAfter.LeaseExpireTime.Before(sharedBefore.LeaseExpireTime) {
		t.Fatalf("reader expire %s", sharedAfter.LeaseExpireTime)
	}
	if !sharedAfter.ReadLock || sharedAfter.OwnerId != "reader-b" || sharedAfter.ReadDepth("reader-a") != 1 || sharedAfter.ReadDepth("reader-b") != 1 || sharedAfter.LockCount != sharedBefore.LockCount {
		t.Fatalf("reader renew rewrote %+v", sharedAfter)
	}
	fmt.Println("PASS reader renew")

	stable := *sharedAfter
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/shared/renew", map[string]string{"owner_id": "stranger", "ttl": "10s"})
	if resp.StatusCode == http.StatusInternalServerError || !strings.Contains(body, storage_lock.ErrLockNotBelongYou.Error()) {
		t.Fatalf("stranger = %d %s", resp.StatusCode, body)
	}
	if got := readLock(t, store, "shared"); got.Version != stable.Version || !got.LeaseExpireTime.Equal(stable.LeaseExpireTime) || got.OwnerId != stable.OwnerId {
		t.Fatalf("stranger changed %+v", got)
	}
	fmt.Println("PASS non-holder")

	resp, viewBody := doJSON(t, srv, http.MethodGet, "/locks/shared", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatal(viewBody)
	}
	requireHTTPInspect(t, viewBody, sameClockStatus(t, store, clock, "shared"))
	fmt.Printf("inspect %s", viewBody)
	fmt.Println("PASS inspect snapshot")

	clock.Set(stable.LeaseExpireTime)
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/shared/renew", map[string]string{"owner_id": "reader-a", "ttl": "10s"})
	if resp.StatusCode == http.StatusInternalServerError || !strings.Contains(body, storage_lock.ErrLeaseExpired.Error()) {
		t.Fatalf("exact renew = %d %s", resp.StatusCode, body)
	}
	if got := readLock(t, store, "shared"); got.Version != stable.Version || !got.LeaseExpireTime.Equal(stable.LeaseExpireTime) || got.OwnerId != stable.OwnerId || got.LockCount != stable.LockCount {
		t.Fatalf("exact renew changed %+v", got)
	}
	fmt.Println("PASS exact expiry")

	resp, viewBody = doJSON(t, srv, http.MethodGet, "/locks/shared", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatal(viewBody)
	}
	exact := sameClockStatus(t, store, clock, "shared")
	requireHTTPInspect(t, viewBody, exact)
	if exact.Held || exact.RemainingLease != 0 {
		t.Fatalf("exact inspect = held %v remaining %s", exact.Held, exact.RemainingLease)
	}
	fmt.Printf("inspect exact %s", viewBody)
}

func sameClockStatus(t *testing.T, store storage.Storage, clock *httpClock, id string) *storage_lock.LockStatus {
	t.Helper()
	options := *storage_lock.NewStorageLockOptions()
	options.LeaseExpireAfter = 3 * time.Second
	options.LeaseRefreshInterval = time.Second
	options.TimeProvider = clock
	options.DisableWatchDog = true
	options.LockId = id
	lock, err := storage_lock.NewStorageLockWithOptions(store, &options)
	if err != nil {
		t.Fatal(err)
	}
	status, err := lock.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return status
}

func requireHTTPInspect(t *testing.T, body string, status *storage_lock.LockStatus) {
	t.Helper()
	var view struct {
		Held           bool   `json:"held"`
		RemainingLease string `json:"remaining_lease"`
	}
	if err := json.Unmarshal([]byte(body), &view); err != nil {
		t.Fatal(err)
	}
	if view.Held != status.Held {
		t.Fatalf("held http %v inspect %v body %s", view.Held, status.Held, body)
	}
	if status.Held {
		want := status.Information.LeaseExpireTime.Sub(status.ObservedAt)
		if status.RemainingLease != want || view.RemainingLease != status.RemainingLease.String() {
			t.Fatalf("remaining http %s inspect %s expire-now %s", view.RemainingLease, status.RemainingLease, want)
		}
		return
	}
	if status.RemainingLease != 0 || (view.RemainingLease != "" && view.RemainingLease != "0s") {
		t.Fatalf("expired remaining http %q inspect %s", view.RemainingLease, status.RemainingLease)
	}
}
