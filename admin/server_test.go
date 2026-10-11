package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	memory_storage "github.com/storage-lock/go-memory-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/require"
)

func TestAdminListsHolderAndWaiter(t *testing.T) {
	store := memory_storage.NewMemoryStorage()
	lock, err := storage_lock.NewStorageLock(store, "orders")
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, lock.RLock(ctx, "reader"))

	server, err := New(store)
	require.NoError(t, err)
	server.now = time.Now

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/locks/orders", nil))
	require.Equal(t, http.StatusOK, response.Code)
	var view lockView
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &view))
	require.True(t, view.Held)
	require.Equal(t, "reader", view.OwnerID)
	require.True(t, view.ReadLock)
	require.NotEmpty(t, view.RemainingLease)

	missing := httptest.NewRecorder()
	server.Handler().ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/locks/missing", nil))
	require.Equal(t, http.StatusNotFound, missing.Code)

	list := httptest.NewRecorder()
	server.Handler().ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/locks", nil))
	require.Equal(t, http.StatusOK, list.Code)
	require.Contains(t, list.Body.String(), "orders")
}
