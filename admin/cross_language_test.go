package admin

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	memory_storage "github.com/storage-lock/go-memory-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

// TestCrossLanguageClients 用同一个 HTTP 锁服务 + 同一份存储启动两个租户前缀，
// 依次用 Python 客户端和 Java 客户端验证 acquire/conflict/renew/release/inspect
// 与租户隔离语义。需要本机有 python3 与 javac/java，缺失则跳过。
func TestCrossLanguageClients(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not found, skipping cross-language test")
	}
	for _, tool := range []string{"javac", "java"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found, skipping cross-language test", tool)
		}
	}

	storage := memory_storage.NewMemoryStorage()
	options := *storage_lock.NewStorageLockOptions()
	options.LeaseExpireAfter = 5 * time.Second
	options.LeaseRefreshInterval = time.Second

	svcA, err := NewService(storage, "tenant-a", options)
	if err != nil {
		t.Fatal(err)
	}
	svcB, err := NewService(storage, "tenant-b", options)
	if err != nil {
		t.Fatal(err)
	}

	// 两个租户挂在同一个端口的不同前缀下（生产可用反向代理等价实现）。
	root := http.NewServeMux()
	root.Handle("/tenant-a/", http.StripPrefix("/tenant-a", svcA.Handler()))
	root.Handle("/tenant-b/", http.StripPrefix("/tenant-b", svcB.Handler()))
	srv := httptest.NewServer(root)
	defer srv.Close()

	repoRoot := filepath.Join("..", "..")
	pyDir := filepath.Join(repoRoot, "python-storage-lock")
	javaSrc := filepath.Join(repoRoot, "java-storage-lock", "src", "main", "java")

	// 跨语言客户端库不在仓库内，需要同仓平级目录存在（CI 会额外 checkout，
	// 本地开发缺目录时跳过而不是硬失败）
	if _, err := os.Stat(pyDir); err != nil {
		t.Skipf("python-storage-lock client not found at %s, skipping", pyDir)
	}
	if _, err := os.Stat(javaSrc); err != nil {
		t.Skipf("java-storage-lock client not found at %s, skipping", javaSrc)
	}

	// 1) Python 客户端。
	pyScript := `
import sys
sys.path.insert(0, %PYDIR%)
from lock_client import LockClient, LockBusyError

base = sys.argv[1]
a = LockClient(base + "/tenant-a")
r = a.acquire("order-1", "py-a", ttl="5s")
assert r.get("owner_id") == "py-a", r
assert r.get("fencing_token", 0) > 0, r
try:
    a.acquire("order-1", "py-b", ttl="5s")
    raise SystemExit("PY-FAIL: second acquire should conflict")
except LockBusyError:
    pass
r2 = a.renew("order-1", "py-a")
assert r2.get("owner_id") == "py-a", r2
info = a.inspect("order-1")
assert info.get("owner_id") == "py-a" and info.get("held"), info
a.release("order-1", "py-a")
r3 = a.acquire("order-1", "py-b", ttl="5s")
assert r3.get("owner_id") == "py-b", r3
a.release("order-1", "py-b")

b = LockClient(base + "/tenant-b")
a.acquire("shared", "py-a", ttl="5s")
b.acquire("shared", "py-b", ttl="5s")  # 不同租户不冲突
try:
    a.acquire("shared", "py-a2", ttl="5s")
    raise SystemExit("PY-FAIL: same-tenant second acquire should conflict")
except LockBusyError:
    pass
a.release("shared", "py-a")
b.release("shared", "py-b")
print("PY-OK")
`[1:]
	pyScript = strings.Replace(pyScript, "%PYDIR%", strconv.Quote(pyDir), 1)
	cmd := exec.Command("python3", "-c", pyScript, srv.URL)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("python client failed: %v\n%s", err, out)
	} else {
		t.Logf("python client: %s", strings.TrimSpace(string(out)))
	}

	// 2) Java 客户端：编译库 + 测试入口，再执行。
	mainSrc := "testdata/CrossLangMain.java"
	tmpDir := t.TempDir()
	javac := exec.Command("javac", "-d", tmpDir,
		filepath.Join(javaSrc, "lock", "storage", "LockClient.java"),
		mainSrc)
	if out, err := javac.CombinedOutput(); err != nil {
		t.Fatalf("javac failed: %v\n%s", err, out)
	}
	java := exec.Command("java", "-cp", tmpDir, "CrossLangMain", srv.URL)
	if out, err := java.CombinedOutput(); err != nil {
		t.Fatalf("java client failed: %v\n%s", err, out)
	} else {
		t.Logf("java client: %s", strings.TrimSpace(string(out)))
	}
}
