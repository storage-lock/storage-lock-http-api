import lock.storage.LockClient;
import lock.storage.LockClient.AcquireResult;
import lock.storage.LockClient.LockBusyException;

/**
 * 跨语言集成测试的 Java 入口：由 Go 测试（cross_language_test.go）编译并执行，
 * 对着同一份 Go HTTP 锁服务验证与 Python 客户端一致的语义。
 */
public class CrossLangMain {
    public static void main(String[] args) throws Exception {
        String base = args[0];

        LockClient a = new LockClient(base + "/tenant-a");
        LockClient b = new LockClient(base + "/tenant-b");

        AcquireResult r = a.acquire("order-1", "java-a", "5s");
        if (!"java-a".equals(r.ownerId) || r.fencingToken <= 0) {
            throw new AssertionError("JAVA-FAIL: acquire -> " + r);
        }
        try {
            a.acquire("order-1", "java-b", "5s");
            throw new AssertionError("JAVA-FAIL: second acquire should conflict");
        } catch (LockBusyException expected) { /* 409 正确 */ }
        a.renew("order-1", "java-a");
        String info = a.inspect("order-1");
        if (!info.contains("java-a")) {
            throw new AssertionError("JAVA-FAIL: inspect -> " + info);
        }
        a.release("order-1", "java-a");
        AcquireResult r2 = a.acquire("order-1", "java-b", "5s");
        if (!"java-b".equals(r2.ownerId)) {
            throw new AssertionError("JAVA-FAIL: acquire after release -> " + r2);
        }
        a.release("order-1", "java-b");

        // 同一份存储上的租户隔离：不同前缀互不冲突，同前缀互斥。
        a.acquire("shared", "java-a", "5s");
        b.acquire("shared", "java-b", "5s");
        try {
            a.acquire("shared", "java-a2", "5s");
            throw new AssertionError("JAVA-FAIL: same-tenant second acquire should conflict");
        } catch (LockBusyException expected) { /* 409 正确 */ }
        b.release("shared", "java-b");
        a.release("shared", "java-a");

        System.out.println("JAVA-OK");
    }
}
