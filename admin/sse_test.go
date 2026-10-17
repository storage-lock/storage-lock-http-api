package admin

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// readSSELine 从 SSE 响应流中读取一行（直到换行）。
type sseStream struct {
	scanner *bufio.Scanner
	body    interface{ Close() error }
}

func newSSEStream(t *testing.T, srv *httptest.Server, path string) *sseStream {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sse status = %d", resp.StatusCode)
	}
	return &sseStream{
		scanner: bufio.NewScanner(resp.Body),
		body:    resp.Body,
	}
}

func (s *sseStream) Close() { _ = s.body.Close() }

// nextEvent 读取下一个完整 SSE 事件（含 event: 与 data: 行，直到空行）。
// 返回事件名与 data 内容；读到 EOF 返回 ok=false。
func (s *sseStream) nextEvent(t *testing.T) (event, data string, ok bool) {
	t.Helper()
	event = ""
	var dataLines []string
	for s.scanner.Scan() {
		line := s.scanner.Text()
		if line == "" {
			// 事件结束
			return event, strings.Join(dataLines, "\n"), true
		}
		if strings.HasPrefix(line, "event:") {
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		} else if strings.HasPrefix(line, ":") {
			// 注释（心跳），跳过
		}
	}
	// EOF：若已有部分数据，当作最后事件返回
	if len(dataLines) > 0 {
		return event, strings.Join(dataLines, "\n"), true
	}
	return event, "", false
}

// waitSSE 等待直到读到包含关键字段的 lock 事件（带超时）。
func waitSSE(t *testing.T, s *sseStream, wantKey, wantVal string, timeout time.Duration) (string, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, data, ok := s.nextEvent(t)
		if !ok {
			return "", false
		}
		if strings.Contains(data, wantKey) && strings.Contains(data, wantVal) {
			return data, true
		}
	}
	return "", false
}

func TestServiceSSESnapshotAndEvents(t *testing.T) {
	svc := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	// 先建立一把锁，确保 snapshot 有内容
	_, body := doJSON(t, srv, http.MethodPost, "/locks/sse-lock/acquire", map[string]string{"owner_id": "svc-holder"})
	if !strings.Contains(body, "fencing_token") {
		t.Fatalf("initial acquire failed: %s", body)
	}

	stream := newSSEStream(t, srv, "/locks/sse-lock/events")
	defer stream.Close()

	// 1. 应收到 snapshot（当前持有者信息）
	done := make(chan bool, 1)
	go func() {
		event, data, ok := stream.nextEvent(t)
		if ok && event == "snapshot" && strings.Contains(data, "svc-holder") {
			done <- true
			return
		}
		done <- false
	}()
	select {
	case got := <-done:
		if !got {
			t.Fatal("未收到初始 snapshot 事件")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("等待 snapshot 超时")
	}

	// 2. 另一客户端释放/重新获取，SSE 应推送 lock 事件
	resp, body := doJSON(t, srv, http.MethodPost, "/locks/sse-lock/release", map[string]string{"owner_id": "svc-holder"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("release failed: %s", body)
	}

	if data, ok := waitSSE(t, stream, `"held":false`, `"lock_id":"sse-lock"`, 3*time.Second); !ok {
		t.Fatalf("未收到释放后的 lock 事件，last data=%q", data)
	}
}
