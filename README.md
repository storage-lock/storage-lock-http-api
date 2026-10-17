# storage-lock HTTP 锁服务

把 `Storage` 暴露成无状态 HTTP 锁服务：租约、owner、fencing token 语义全部在
服务端实现，客户端只做 HTTP 调用，不需要本地 CAS 逻辑。

## 读写接口（`admin.Service`）

- `GET    /locks`                   列出锁。
- `GET    /locks/{id}`              查看一把锁（持有者、读写模式、剩余租约、等待队列、fencing 版本）。
- `GET    /locks/{id}/waiters`      只返回等待队列。
- `GET    /locks/{id}/events`       **SSE 推送**：订阅锁状态变更（首次推送 snapshot，
  之后持有/释放/过期/等待队列变化实时推送），可替代轮询。底层复用 `StorageLock.Subscribe`
  与存储原生 Watch（跨进程/跨实例可见）。
- `POST   /locks/{id}/acquire`      获取锁，body `{"owner_id","mode","ttl","wait"}`。
- `POST   /locks/{id}/renew`        续租，body `{"owner_id","ttl"}`。
- `POST   /locks/{id}/release`      释放锁，body `{"owner_id"}`（等价 `DELETE /locks/{id}`）。

SSE 消费示例：

```bash
curl -N http://host:8080/locks/order-1/events
# event: snapshot
# data: {"lock_id":"order-1","owner_id":"...","held":true,...}
# event: lock
# data: {"lock_id":"order-1","owner_id":"...","held":false,...}
```

语义要点：

- **租约模式**：服务端不启动看门狗（`DisableWatchDog=true`），租约按 `ttl`（如 `"5s"`）
  自然过期；客户端拿到锁后必须周期 `renew`，否则会被他人抢占（类似 Redisson lease 模式）。
- **fencing token**：`acquire` 返回 `fencing_token`，业务资源侧用它做原子护栏。
- **错误映射**：404 = 锁不存在，409 = 被他人持有（`ErrLockBusy`），400 = 请求体问题。
- **命名空间隔离**：`NewService(storage, namespace, opts)` 会为所有 lock ID 加
  `namespace/` 前缀。不同租户可共享同一份存储，同一端口挂在不同前缀下：

```go
mux := http.NewServeMux()
mux.Handle("/tenant-a/", http.StripPrefix("/tenant-a", svcA.Handler()))
mux.Handle("/tenant-b/", http.StripPrefix("/tenant-b", svcB.Handler()))
```

## 客户端

- Python：`python-storage-lock/lock_client.py`，`LockClient(base_url)`，
  409 → `LockBusyError`。
- Java：`java-storage-lock/src/main/java/lock/storage/LockClient.java`（无依赖，
  JDK 11+ 原生 `java.net.http`），409 → `LockBusyException`。

跨语言语义一致性由 `admin/cross_language_test.go` 验证：同一份存储 + 同一服务，
Python 与 Java 客户端依次跑 acquire/conflict/renew/release/inspect/租户隔离，
全部通过（测试内用 `python3 -c` 与 `javac`/`java` 编译执行）。

## 只读管理接口（`admin.Server`）

`admin.Server` 只读接口依然可用（上面的 GET 路由）。没有强制解锁/删除接口：
删除墓碑会让 fencing token 回退，管理面不允许做这件事。
