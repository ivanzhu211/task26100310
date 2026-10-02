# task26100310

离线档案库的可恢复批量销毁组件（Python 标准库 + SQLite，无第三方依赖）。

## 布局

- `archive/store.py` — `ArchiveStore`：`ingest` / `destroy` / `read` / `recover`
- `archive/errors.py` — 异常类型
- `archive/faults.py` — 崩溃注入点（`ARCHIVE_CRASH_POINTS` 环境变量，`os._exit` 硬退出）
- `tests/test_archive.py` — 功能测试
- `tests/test_crash.py` + `tests/crash_worker.py` — 子进程强制退出的崩溃恢复测试

## 语义

- blob 按 SHA-256 内容寻址存放于 `<root>/blobs/`，元数据在 `<root>/archive.sqlite3`，文件内容不入库。
- `destroy` 只有在全部目标存活、已到期且重算校验和匹配时才受理；受理（SQLite 提交）后不可撤销，
  恢复必须完成整个集合。凭证持久化在批次行中，区分实际回收与因共享保留的 blob。
- 每次打开 `ArchiveStore` 都会先执行 `recover()` 完成中断的批次，再接受其他操作。
- 时钟可注入：`ArchiveStore(root, clock=lambda: some_utc_datetime)`。

## 运行测试

```
python3 -m unittest discover -s tests -v
```
