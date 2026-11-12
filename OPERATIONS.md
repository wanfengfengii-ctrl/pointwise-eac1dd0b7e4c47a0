# 采样入账发布与运行操作说明

账务核心 `ledger.py` 保持逐字不变；`service.py`、`checks.py`、`samples.py`
仍可直接按原方式使用。发布治理全部由 `ops.py` 完成。

## 目录与产物

`STATE` 是调用者给出的绝对路径，运行数据、不可变包和状态都在其中：

```
STATE/
  state.json           已提交代次（原子重命名）
  pending.json         仅发布进行中存在的事务日志
  packages/<sha>.pyz   按内容摘要寻址的不可变 zipapp
  ledger.sqlite        业务数据库（与 state.json 的 format 一致）
  bin/ops.py           首次成功发布时安装的自包含管理入口
  bin/...              与入口同目录的可重建源码快照（ledger.py、runtime 等）
  run.lock             协调器单例锁（进程死亡时由内核释放）
  write.lock           写入接纳 / 迁移排空锁
  logs/                协调器、工作进程与 launchd 日志
```

构建产物（`build --out`）与 `STATE` 分离。安装后删除源码目录和构建目录不影响
运行；`STATE/packages` 内的包已校验且不可变。

## 固定入口

```sh
python3 -B ops.py build --release r1 --out /abs/artifact/r1.pyz
python3 -B ops.py promote --state /abs/state --artifact /abs/artifact/r1.pyz \
    --expect 0 --request-id install-1
python3 -B ops.py rollback --state /abs/state --expect 1 --request-id back-1
python3 -B ops.py run --state /abs/state --host 127.0.0.1 --port 8765
```

- 空 `STATE` 代次为 0；首次成功发布为 1；每个成功的 promote/rollback 加 1。
- `--expect` 必须等于执行线性化点的当前代次；并发同代次最多一个成功。
- 相同 `--request-id` 且同意图（操作、原 expect、promote 产物摘要）的重试返回
  最初结果，优先于代次检查；不同意图拒绝。旧请求重放不会把 head 回退。
- 成功时 stdout 一行 JSON，含 `generation`、`release`、`format`、
  `request_id`；失败非零退出并给出可读原因。
- 首次成功发布的 JSON 额外给出 `management_entry`，即
  `STATE/bin/ops.py` 的绝对路径。源码移走后继续使用该入口，参数一致。

`build` 使用标准库 zipapp，内容清单与 SHA-256：

- 产物内嵌 `MANIFEST.json`（逐文件 SHA-256、绑定 release/format）；
- 旁边写 `<artifact>.manifest.json` 和 `<artifact>.sha256`；
- 相同源码与 release 重建字节完全相同；不含运行数据或绝对构建路径。

一次性引导（`setup` 使用，幂等）：

```sh
python3 -B ops.py bootstrap --state /abs/state --release r1
```

已安装时只做恢复与校验，不重复发布。

## 升级与回退语义

`promote` 接受任意已校验的 r1/r2 包；`rollback` 选择当前 head 之前最近一次
成功提交的包。两者都调用现有 `ledger.migrate()` 做双向转换，因此：

- 新版本期间新增的数据、批次回执全部保留；
- 回退不是还原备份，而是对当前完整数据库做反向转换；
- 转换是 `ledger.migrate()` 自带的单事务，失败不会留下半转换格式。

发布持久化顺序：

1. 校验产物与内容清单（损坏包在此直接拒绝，线上状态不变）；
2. 原子写 `pending.json`；工作进程随后对新写入返回 503 + `Retry-After`；
3. 取得 `write.lock` 排他锁，等待已跨过接纳检查的写入完成；
4. `ledger.migrate()` 事务提交；
5. 原子重命名 `state.json`（代次生效）；
6. 删除 `pending.json`。

协调器在任一步被 SIGKILL 后，下一次 `run`、`recover` 或下一次变更命令都会
在 `lock` 下自动完成或安全中止日志：

| 终止位置 | 持久状态 | 自动恢复 |
| --- | --- | --- |
| 写 journal 前 | 无变化 | 无 |
| journal 后、迁移前 | 数据库旧格式，代次旧 | 完成迁移并提交新代次 |
| 迁移事务中 | SQLite 回滚到旧格式 | 重做迁移 |
| 迁移已提交、state 未提交 | 数据库新格式，代次旧 | 以 journal 提交新代次，格式一致 |
| state 已提交、journal 未删 | 新代次、新格式 | 删除日志，保持新代次 |

正常运行中的协调器也会在发现 `pending.json` 时执行同一恢复，因此变更命令
被杀后无需值守人员介入。恢复不要求手工改指针、表结构或回执。

## 进程代次隔离

`run` 是稳定入口：它取得 `run.lock` 单例锁，读取已提交状态，启动一个
工作进程代次，并持有监听 socket 的移交权。工作进程：

- 每次写入前读取 `state.json` 与 `pending.json`，代次或格式不符、或发布
  进行中，一律在连接账务前返回 503；旧进程不能接纳新代次写入；
- 导出在同一个 SQLite 读事务里逐行输出，开始后即固定快照；
- 协调器切换代次时关闭旧工作进程的监听 fd（借助 `SO_REUSEPORT` 新代次
  立即绑定同一地址），已接受的旧导出线程继续读完旧快照；协调器不等待慢导出；
- 协调器死亡时控制管道 EOF 使工作进程主动关闭监听、排空已有导出后退出。
  隔离判断不依赖 PID 文件，也不依赖端口恰好空闲。

并发启动同一 `STATE` 的第二个 `run` 会因 `run.lock` 直接退出，不会形成第二个
接纳写入的服务。

## 用户级 LaunchAgent

```sh
# 在 macOS 用户会话中安装并 bootstrap（自动使用绝对解释器与已安装入口）
python3 -B /abs/state/bin/ops.py agent-install --state /abs/state --port 8765

# 卸载：bootout、删除 plist、停止受管进程；数据与历史包保留
python3 -B /abs/state/bin/ops.py agent-uninstall --state /abs/state
```

- plist 位于 `~/Library/LaunchAgents/<label>.plist`（可用环境变量
  `POINTWISE_LAUNCH_AGENTS_DIR` 指定其它目录）；
- `ProgramArguments` 全部为绝对路径，支持含空格和中文的目录；
- 含 `RunAtLoad`、`KeepAlive`、`WorkingDirectory`、`StandardOutPath`、
  `StandardErrorPath` 和 `EnvironmentVariables`；
- 安装后从任意当前目录启动，源码和构建目录可删除；
- 卸载命令只移除本服务托管的进程和 plist，保留 `STATE/ledger.sqlite`、
  `STATE/packages` 与全部历史。

非 macOS 环境仍会生成同样内容的 plist 并说明未注册 launchd；发布算法与故障
回放不依赖 launchd。

## 验收与故障回放

运行清单 `.pointwise-runtime.json` 已改为实际启动托管服务：

- `setup`：`bootstrap`（可重入安装/恢复）
- `start`：`run`（稳定 HTTP 入口）
- `verify`：`python3 -B ops.py verify --state var/state`
  - 对已安装包真实调用 `checks.py` 的业务回归；
  - 在临时状态中重跑发布、回退、幂等、并发、损坏包、journal 杀进程恢复断言；
  - 成功输出 `BUSINESS_REGRESSION_OK`、`RELEASE_RECOVERY_OK`、
    `RELEASE_VERIFY_OK`。
- `demo`：真实构建 r1/r2、发布、导入、回退并打印各阶段快照与回执。

完整独立进程证据：

```sh
python3 -B acceptance.py --workdir /abs/evidence
```

它每次重建工作目录，覆盖：上线业务与冲突回滚、慢导出跨 r1→r2 切换、
带新增数据的回退、并发期望代次、损坏包拒绝、journal/迁移边界 SIGKILL 后
自动恢复、协调器 SIGKILL 后导出存活与旧进程隔离、含空格中文路径的
LaunchAgent 安装/卸载。成功输出 `EVIDENCE_OK`。

仅在验证时启用的暂停点（与真实持久化/接纳边界对应）：

- `POINTWISE_TEST_PAUSE_AT=after-pending`：journal 已原子落盘，迁移尚未开始；
- `after-migrate`：迁移事务已提交，`state.json` 尚未提交；
- `after-commit`：新代次已提交，journal 尚未删除；
- `POINTWISE_TEST_PAUSE_SECONDS` 控制停留时间，同时生成
  `STATE/.pause-<point>` 标记供外部进程发 SIGKILL。

## HTTP 与账务语义

沿用基线：`POST /batches`、`GET /snapshot`、`GET /export`（NDJSON）、
`GET /health`。非法输入与批次冲突仍为 409，整批不生效；相同批次重放返回同一
结果；已提交但客户端未收到响应的请求可安全重试。切换停接纳期间返回
503 和 `Retry-After`，503 不产生账务效果。响应继续携带 `X-Release`，
健康信息扩展了 `generation`，未改变原字段含义。

## 故障范围

覆盖本机进程 SIGKILL、写入中断和启动失败，依赖 SQLite 事务与原子重命名。
不覆盖断电、磁盘硬件损坏、恶意目录篡改，或绕过托管入口直连数据库。
