package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServiceReadModeSharedAndWriteExclusive(t *testing.T) {
	svc := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	resp, body := doJSON(t, srv, http.MethodPost, "/locks/shared/acquire", map[string]string{
		"owner_id": "reader-a",
		"mode":     "read",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first read acquire = %d, body = %s", resp.StatusCode, body)
	}

	resp, body = doJSON(t, srv, http.MethodPost, "/locks/shared/acquire", map[string]string{
		"owner_id": "writer",
		"mode":     "write",
	})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("write acquire while read lease is valid = %d, body = %s", resp.StatusCode, body)
	}

	resp, body = doJSON(t, srv, http.MethodPost, "/locks/shared/acquire", map[string]string{
		"owner_id": "reader-b",
		"mode":     "read",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("second read acquire = %d, body = %s", resp.StatusCode, body)
	}

	resp, body = doJSON(t, srv, http.MethodPost, "/locks/shared/acquire", map[string]string{
		"owner_id": "omitted",
	})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("omitted mode should stay exclusive, status = %d, body = %s", resp.StatusCode, body)
	}

	resp, body = doJSON(t, srv, http.MethodPost, "/locks/exclusive/acquire", map[string]string{
		"owner_id": "writer-1",
		"mode":     "write",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write mode on a free lock = %d, body = %s", resp.StatusCode, body)
	}
	resp, body = doJSON(t, srv, http.MethodPost, "/locks/exclusive/acquire", map[string]string{
		"owner_id": "writer-2",
		"mode":     "write",
	})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second write acquire = %d, body = %s", resp.StatusCode, body)
	}
}
