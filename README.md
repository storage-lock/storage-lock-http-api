# storage-lock HTTP 管理接口

`admin.Server` 把一个已初始化的 Storage 暴露成只读查询：

- `GET /locks`：列出锁。
- `GET /locks/{id}`：持有者、读写模式、剩余租约、写意图、等待队列和 fencing 版本。
- `GET /locks/{id}/waiters`：只返回等待队列。

没有解锁或删除接口。删除墓碑会让 fencing token 回退，管理面不允许做这件事。
