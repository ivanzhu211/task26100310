# archive-destroy

离线档案库的可恢复批量销毁组件（仅 Python 标准库 + SQLite）。

## 用法

```python
from archive_destroy import Archive

archive = Archive("archive.db", "blobs/", clock=time.time)
archive.ingest("rec-1", stream, retain_until=1735689600.0)
receipt = archive.destroy(["rec-1"], idempotency_key="req-42")
data = archive.read("rec-1")   # 存活 -> bytes；已销毁 -> Receipt
archive.recover()              # 打开时自动执行，也可手动调用
```

- blob 按 SHA-256 内容寻址，共享 blob 在最后一条存活引用销毁后才物理删除。
- Destroy 受理前全量校验（存活、到期、流式重算校验和），任一失败整组拒绝且不占用幂等键。
- 受理决定单事务落库，不可撤销；Recover 完成中断的物理清理，绝不回滚。
- 幂等键永久绑定无序 record_id 集合，重试收敛到同一份凭证。

## 测试

```
python3 -m unittest discover -s tests -v
```

含子进程 `os._exit` 硬崩溃注入，覆盖 Destroy/Recover 每次提交与文件状态变更前后的全部中断窗口。
