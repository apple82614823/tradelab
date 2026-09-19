# 历史工程详细参考（通用示例版）

> 本文保留私有开发阶段的技术说明，便于理解兼容模块和已有测试，不代表服务器当前状态。
> `/home/trading-example` 是虚构的部署账户路径；所有 `<...>` 都是待替换的示例占位符。
> 示例币种、记录标识和授权摘要不是任何真实账户的操作依据。
> 文中“本次发布”“正式生产”“另外三套系统”等为历史场景的通用描述，不是授权执行指令。
> 初次阅读请从根目录 README 开始。不要直接复制本页执行数据库升级、作废或实盘操作。

本项目按需求实现 Binance USD-M Futures 自动监控与开多逻辑。默认是 `BINANCE_DRY_RUN=true`，不会新开真实订单。

## 启动

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

编辑 `.env` 后启动：

```bash
caffeinate -i python main.py
```

## 重要安全默认值

- `BINANCE_DRY_RUN=true`：默认只模拟，不真实下单。
- `MULTI_STRATEGY_ENABLED=false`：默认保留原单策略路径；部署时必须明确设置为 `true` 才启用当前活跃集 N01-N05。
- 真实交易必须同时设置：
  - `BINANCE_DRY_RUN=false`
  - `BINANCE_CONFIRM_LIVE_TRADING=I_UNDERSTAND_REAL_MONEY_RISK`
- API Key 只放 `.env`，不要提交到 Git。
- 模拟开仓后也会写入 `state/position.json`，用于防止重复触发。只有停服、完成备份，并确认它是纯模拟状态且没有 `execution_pending` 后，才能按明确的重置流程处理；不得直接删除可能关联实盘仓位的 state。
- 复盘数据库默认写入 `data/trading_review.sqlite3`，用于后续 AI 拉数据分析，不会提交到 Git。

本项目的测试、代码审查和发布门只代表工程验收，不构成投资建议，也不保证策略在历史或未来实盘中盈利。

## 示例 Linux 安全发布门

以下门槛适用于示例 Linux 主机。发布必须使用固定绝对 `WorkingDirectory`，由 service manager 管理唯一服务实例，并同时保留程序内 `INSTANCE_LOCK_FILE` 的 `flock` 单实例保护。禁止在旧进程仍运行时直接启动新进程；发布顺序必须是 stop、确认停止、备份/校验、切换版本、start。

### 发布前硬门槛

1. 固定部署目录和权限。代码目录、`.env`、`state/`、`data/`、`logs/` 不使用临时 cwd；服务账户独占读写，建议 `umask 0077`，目录 `0700`，`.env` 与运行状态文件 `0600`。不要把 API Key、账户信息、主机 IP 或 `.env` 内容输出到终端、CI 日志或工单。
2. 记录旧 commit，并备份 `.env`、整个 `state/`、Review DB 和独立 N16 claim ledger。SQLite 处于 WAL 模式或服务在线时，不复制单个 `.sqlite3` 文件；对两个数据库分别使用 SQLite `.backup` 生成同代一致备份，再分别执行 `PRAGMA integrity_check` 和 `PRAGMA foreign_key_check`，并完成 Review 永久全图与 ledger 的全量逐 claim 交叉认证。全部通过才继续。
3. 只输出非敏感预检结果，例如 commit、Python 版本、测试通过数量、文件是否存在、权限是否合格、数据库检查 PASS/FAIL。不得输出环境变量值、账户余额、仓位、订单、交易所响应、域名解析结果或公网 IP。
4. `BINANCE_DRY_RUN=true` 才是“停止新真实下单”的开关；`MULTI_STRATEGY_ENABLED=false` 只是退回旧单策略路径，不是停实单开关。切换 dry-run 也不会自动平仓或解决待确认订单。
5. 若 state 表示 `dry_run=false`、存在 `execution_pending`，或交易所仍有 live 仓位/保护单，禁止删除 `state/`、覆盖数据库、直接降级旧 commit 或把现场当作纯模拟环境。先保持原版本和审计数据，由人工完成仓位与订单核对、恢复或明确处置。
6. 发布产物通过本仓库全量测试和 `compileall` 后，先停止旧服务并确认 inactive，再切换版本。启动失败时保留新旧代码、备份和原 state 做诊断，不用第二个进程“顶上去”。

一个不含账户、IP 或秘密的 systemd 占位示例：

```ini
[Unit]
Description=Binance trading bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=<service-user>
Group=<service-group>
WorkingDirectory=/home/trading-example/binance-trading-bot/current
EnvironmentFile=/home/trading-example/binance-trading-bot/shared/.env
ExecStart=/home/trading-example/binance-trading-bot/current/.venv/bin/python /home/trading-example/binance-trading-bot/current/main.py
Restart=on-failure
RestartSec=30
UMask=0077

[Install]
WantedBy=multi-user.target
```

service manager 的单元名也必须唯一。本次发布单元固定为 `binance-trading-bot.service`，不允许通配或替换为其他 unit。`Config` 继续兼容五个文件变量的绝对路径；这项兼容性不等于生产发布可以指向任意目录。N16 独立账本 `config.n16_claim_ledger_file` 不是新的 `.env` 变量，而是由 effective `STATE_FILE` 的同一父目录固定派生。发布会话必须从待发布代码读取 effective 配置，只做脱敏路径证明：`STATE_FILE`、`DRY_RUN_ACCOUNT_FILE`、`INSTANCE_LOCK_FILE` 以及派生的 `/home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3` 必须落在 `/home/trading-example/binance-trading-bot/shared/state`，`REVIEW_DB_FILE` 必须落在 `/home/trading-example/binance-trading-bot/shared/data`，`LOG_FILE` 必须落在 `/home/trading-example/binance-trading-bot/shared/logs`。这六个 resolved path 必须两两不同；已存在文件的 `(st_dev, st_ino)` 也必须两两不同，任何路径重名、symlink alias 或 hardlink alias（例如 `STATE_FILE` 与 ledger 重名）都是 `NO-GO`。逐级父链必须是真实目录且无 symlink，目标不得是 symlink；已存在的目标必须是 regular file 且 `st_nlink == 1`，尚不存在的目标也只能在已证明的专属父目录中安全创建。任一路径越界或身份不明都必须在启动服务前 `NO-GO`，检查过程只能输出 `PASS/NO-GO`，不能输出 `.env` 内容、秘密或配置值。

在读取配置前，`/home/trading-example/binance-trading-bot/shared/.env` 本身也必须是该精确路径下的 regular、非 symlink、`st_nlink == 1`、权限 `0600` 文件，父链必须真实。还必须用 `systemctl show` 脱敏证明 `EnvironmentFiles` 只有这一份精确 `.env`，且 unit 的 `Environment` 中没有五个路径变量的覆盖；检查值只能保存在 shell 变量中做匹配，不能打印。否则手工加载 shared `.env` 不能声称等于服务的 effective 配置，必须 `NO-GO`。下面的示例先锁住这两个前提，再从 Binance 专属 `EnvironmentFile` 装载实际值，但只打印一个结论。它不改变既定的绝对路径兼容语义：

```bash
cd /home/trading-example/binance-trading-bot/current || exit 1
env_file=/home/trading-example/binance-trading-bot/shared/.env
test -f "$env_file" || exit 1
test ! -L "$env_file" || exit 1
test "$(stat -c %h -- "$env_file")" = "1" || exit 1
test "$(stat -c %a -- "$env_file")" = "600" || exit 1
test "$(realpath -e -- "$env_file")" = "$env_file" || exit 1
unit_environment_files="$(systemctl show binance-trading-bot.service -p EnvironmentFiles --value)" || exit 1
test "$unit_environment_files" = "/home/trading-example/binance-trading-bot/shared/.env (ignore_errors=no)" || exit 1
unit_environment="$(systemctl show binance-trading-bot.service -p Environment --value)" || exit 1
for variable in STATE_FILE DRY_RUN_ACCOUNT_FILE LOG_FILE REVIEW_DB_FILE INSTANCE_LOCK_FILE; do
  case "$unit_environment" in *"$variable="*) exit 1 ;; esac
done
unset unit_environment || exit 1
unset STATE_FILE DRY_RUN_ACCOUNT_FILE LOG_FILE REVIEW_DB_FILE INSTANCE_LOCK_FILE || exit 1
python3 - <<'PY' || exit 1
import os
import stat
from pathlib import Path

from trading_bot.config import _load_env_file, load_config


def no_go():
    print("effective_path_preflight=NO-GO")
    raise SystemExit(1)


def require_real_directory_chain(path):
    if not path.is_absolute():
        no_go()
    current = Path(path.anchor)
    for component in path.parts[1:]:
        current = current / component
        try:
            details = os.lstat(str(current))
        except OSError:
            no_go()
        if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
            no_go()


try:
    shared = Path("/home/trading-example/binance-trading-bot/shared")
    _load_env_file(shared / ".env")
    config = load_config()
    required = (
        ("STATE_FILE", "state_file", shared / "state"),
        ("DRY_RUN_ACCOUNT_FILE", "dry_run_account_file", shared / "state"),
        ("LOG_FILE", "log_file", shared / "logs"),
        ("REVIEW_DB_FILE", "review_db_file", shared / "data"),
        ("INSTANCE_LOCK_FILE", "instance_lock_file", shared / "state"),
        (
            "DERIVED_N16_CLAIM_LEDGER",
            "n16_claim_ledger_file",
            shared / "state",
        ),
    )
    effective_targets = {}
    for _name, attribute, allowed_parent in required:
        target = Path(getattr(config, attribute))
        require_real_directory_chain(allowed_parent)
        require_real_directory_chain(target.parent)
        if not target.is_absolute():
            no_go()
        if os.path.lexists(str(target)):
            details = os.lstat(str(target))
            if (
                stat.S_ISLNK(details.st_mode)
                or not stat.S_ISREG(details.st_mode)
                or details.st_nlink != 1
            ):
                no_go()
            effective = target.resolve(strict=True)
        else:
            effective = target.parent.resolve(strict=True) / target.name
        if effective.parent != allowed_parent:
            no_go()
        if (
            _name == "DERIVED_N16_CLAIM_LEDGER"
            and effective
            != shared / "state" / "n16_claim_ledger.sqlite3"
        ):
            no_go()
        effective_targets[_name] = effective
    names = tuple(effective_targets)
    for index, left_name in enumerate(names):
        left = effective_targets[left_name]
        for right_name in names[index + 1:]:
            right = effective_targets[right_name]
            if left == right:
                no_go()
            if left.exists() and right.exists():
                try:
                    if os.path.samefile(left, right):
                        no_go()
                except OSError:
                    no_go()
    print("effective_path_preflight=PASS")
except SystemExit:
    raise
except Exception:
    no_go()
PY
```

发布操作骨架如下。它不依赖未启用的 `set -e`，每一步都显式检查返回码；所有文件操作都必须留在 `/home/trading-example/binance-trading-*` 专属路径：

```bash
systemctl stop binance-trading-bot.service || exit 1
active_state="$(systemctl show binance-trading-bot.service -p ActiveState --value)" || exit 1
sub_state="$(systemctl show binance-trading-bot.service -p SubState --value)" || exit 1
main_pid="$(systemctl show binance-trading-bot.service -p MainPID --value)" || exit 1
control_group="$(systemctl show binance-trading-bot.service -p ControlGroup --value)" || exit 1
test "$active_state" = "inactive" || exit 1
test "$sub_state" = "dead" || exit 1
test "$main_pid" = "0" || exit 1
test -n "$control_group" || exit 1
cgroup_procs="/sys/fs/cgroup${control_group}/cgroup.procs"
test -f "$cgroup_procs" || exit 1
test ! -s "$cgroup_procs" || exit 1

command -v lsof >/dev/null 2>&1 || exit 1
umask 077 || exit 1
python3 -m trading_bot.release_backup \
  --binance-root /home/trading-example/binance-trading-bot \
  --backup-root /home/trading-example/binance-trading-backups \
  --current-dir /home/trading-example/binance-trading-bot/current \
  --env-file /home/trading-example/binance-trading-bot/shared/.env \
  --state-dir /home/trading-example/binance-trading-bot/shared/state \
  --state-file /home/trading-example/binance-trading-bot/shared/state/position.json \
  --dry-run-account-file /home/trading-example/binance-trading-bot/shared/state/dry_run_account.json \
  --log-file /home/trading-example/binance-trading-bot/shared/logs/trading.log \
  --review-db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 || exit 1

nrestarts_before="$(systemctl show binance-trading-bot.service -p NRestarts --value)" || exit 1
systemctl start binance-trading-bot.service || exit 1
active_state="$(systemctl show binance-trading-bot.service -p ActiveState --value)" || exit 1
sub_state="$(systemctl show binance-trading-bot.service -p SubState --value)" || exit 1
main_pid="$(systemctl show binance-trading-bot.service -p MainPID --value)" || exit 1
test "$active_state" = "active" || exit 1
test "$sub_state" = "running" || exit 1
case "$main_pid" in ''|*[!0-9]*) exit 1 ;; esac
test "$main_pid" -gt 0 || exit 1
# 主发布会话必须等待既定观察窗，再执行下面两行；不得省略观察窗。
nrestarts_after="$(systemctl show binance-trading-bot.service -p NRestarts --value)" || exit 1
test "$nrestarts_after" = "$nrestarts_before" || exit 1
```

`trading_bot.release_backup` 不信任锁前布尔值或 inode：它先取六个 effective path 的一代证据，用 `O_NOFOLLOW` 打开已存在的实例锁，取得 `flock -n` 后在同一临界区内重新认证六个 resolved path、每个 parent `(dev,ino)`、已存 main `(dev,ino,nlink,type)` 和 SQLite sidecar。备份根与 application root、`current`、`shared/state/data/logs`、六个 effective main 及其 parent 必须按 resolved path component 严格互不相交：禁止同路径、任一方为另一方祖先/后代或 symlink 别名；正式 sibling `/home/trading-example/binance-trading-backups` 才可通过。该目录树证明在锁前和锁后都重做，且位于任何 SQLite 打开、`release-*` 创建或文件复制之前。锁前至锁后任一 existence/identity 变化都在备份目录创建前 `NO-GO`。Review 与已存 ledger 只能用 SQLite `mode=rw` 打开，且实际新 FD 的 `(dev,ino)` 在任何 PRAGMA/query 前必须与持锁后证据一致；checkpoint、`journal_mode=DELETE`、backup 和完整性检查后再次核对同一 identity。

对真正 PRE-N16 缺失 ledger，工具在锁后重验 main 与 `-wal/-shm/-journal` 均不存在，全程绝不向 SQLite 或 `lsof` 传入该路径；写 `n16_claim_ledger.absent=ABSENT_PRE_N16` 前后还要再次证明持续缺失，不得创建 0 字节 ledger。已存 ledger 则只会产生同代 `n16_claim_ledger.sqlite3` 权威备份，绝不同时写 absence marker。每次备份都是 Binance backup root 内新建的随机唯一 `0700` 目录，目标全部 `O_EXCL`、不覆盖旧代；`state/` 副本排除锁与 ledger 主文件，回滚必须使用同目录内的 Review/ledger/absence marker/state 一整代。已存 ledger 的文件级完整性通过后，切换前还必须对工作副本与备份副本各执行一次下文停服 `--dry-run` 的 Review 永久全图、ledger catalog/index/integrity 及全量逐 claim 交叉认证；任一 pair 不一致都必须 `NO-GO`。

本轮不修改 unit 文件，因此禁止重载 systemd manager 配置；也禁止按进程名批量杀进程、修改全局 journald/logrotate/Python/SQLite 配置或操作其他 unit。主发布会话可在发布前后只读记录另外三套系统的 PID、`NRestarts`、active state 和端口；任一发生变化必须立即 `NO-GO`，不得尝试替它们修复或重启。回滚也必须只 stop 上述 Binance unit，并先通过 live/state 安全门；有 live 或 `execution_pending` 时不得直接切旧 commit。

### `strategy_signals` 有界保留与一次性维护

`strategy_signals` 只是当前扫描判断集，不再承担永久交易史。运行时最多保留两代：上一个已完整发布的 `CURRENT` 和正在计算的 `STAGING`。调度器只在本轮所有原始判断都成功持久化后才于一个 SQLite 事务中切换当前指针并删除旧轮普通信号。默认旧单策略也必须以独立的 `LEGACY_SINGLE` 身份先写完本轮全部 `PASSED`、`REJECTED` 与 cooldown `VOID` 判断并发布，不能把它误标成正式 N01。任一判断写入失败、进程中断或切换事务失败，都不发布半轮数据：旧 `CURRENT` 保持完整，本轮全部通过信号禁止进入纸单、实单或交易所下单执行。发布发生在新信号对应的纸单、实单和交易所下单副作用之前，不依赖 `scans.completed_at` 或交易执行结果。

`PASSED` 的原始完整分析另外写入 `strategy_passed_signal_audits`。N06/N07/N08/N11/N12/N16-N25 已发布的可执行代表 structure 写入小容量、无 `strategy_signals` 外键的 `strategy_passed_structure_ledger`。N16-N19 沿用各自的永久结构/coverage 表；N17/N18/N19 的覆盖真源由停服显式安装的 `history_coverage_epoch_chain`、`history_coverage_epoch_heads` 与不可变的 `history_coverage_publication_receipts` 共同认证。每一次 NEW、CONTIGUOUS 或 GAP 覆盖推进都必须由同一 CURRENT 发布事务写入绑定 scan、批次 expected count、manifest 与结果 head 的永久回执；回执按 strategy/symbol 连续编号、绑定前序回执哈希，并由 head 固定回执数量和最新哈希。N19 首个 C 已由 family 唯一锁定，所以历史终态按 strategy/symbol/family 永久唯一封存，structure 与 evidence hash 是该唯一事件必须精确绑定的内容而不是第二事件身份；后续 scan 重放相同 family 的同内容或内容变体都会在首写前整批拒绝，不会重复推进 coverage publication count。专属 SQL 写门拒绝普通连接追加、替换或协调改写 chain、head、mirror、installation 与 receipt。发布还会双向核对本 scan 的全部 N17–N19 `(strategy_id,symbol)` 与 proposal/receipt 集合，缺少、多余或重复均整批回滚。若停扫或掉榜使新的固定122根窗口与旧覆盖水位不再重叠，只允许在没有跨缺口活动结构时追加不可改删的新 epoch；N17 的 required-symbol、proposal 预判与发布事务内复核共用“活动 stage 且没有 ACTIVE structure claim”的同一权威谓词。新 epoch 绑定旧链头、旧水位、旧 source hash、新窗口与发布 scan，并与 coverage mirror、PASSED audit/ledger 和 CURRENT 切换同一事务提交。不回填、不跨缺口伪称连续；删除、改写、断链、乱序、孤儿、head/mirror/receipt 不一致会在启动、发布和 retention/VACUUM 首写前拒绝。旧 N17–N25 数据库必须先停服运行 `--install-history-coverage-epochs`，普通启动不会自动迁移；若旧 V3 已存在无法从永久批次证据重建的 N19 历史终态，显式升级同样会在任何 DDL 前拒绝。N20 保存压缩市场 episode；N21-N25 将每个原始 PASSED 的完整3–4点观测及派生值写入 `micro_passed_analyses`，可执行代表再与 `micro_strategy_lifecycle` 同事务申领。普通 signal 只保存不超过16KiB的摘要引用。同轮多币命中仍保留所有原始分析，只有固定排序第一名申领可执行 PASSED audit/ledger，代表失败不递补。普通行和 claim 先写 `STAGED`，完整批次发布时才一起转为永久 `ACTIVE`；未发布批次只清理本轮 STAGED，不删除任何既有永久证据。

N15 已收口的冻结快照不再物理丢弃。每个 `(N15,e_time)` 只允许一条不可变 `n15_snapshot_terminal_receipts`：回执保存原始 canonical payload 的精确 SHA 和无损压缩字节、winner 身份、收口原因与时间；winner 为空时明确证明没有 entry state，winner 存在时则与同事务内的永久 entry terminal 逐字段和 detail hash 配对。回执的单条 `INSERT` 触发活跃快照移除，见证写入、entry 配对、图确认或移除任一步失败都会回滚，原 active snapshot 继续留在恢复集合并关闭该生命周期。回执不进入候选或实时 Kline 门，retention/VACUUM 永久保留且全图复核；普通重复轮不会再复制回执。

N19 历史终态以单条 canonical bundle 作为 SQLite 原子写入单元：terminal receipt、family seal、coverage binding 与 `CONFIRMED→MISSED` 状态迁移都由同一条 `INSERT` 派生，任一派生步骤失败都会回滚整条语句；调用方即使捕获该语句异常后继续 `COMMIT`，也不会留下终态半链。受保护图另有数据库内单调 generation，受保护表变化由 trigger 自动推进；运行期只提升在同一 SQLite snapshot 内完成完整或受影响 owner 认证后的 generation。连接关闭、文件时间、WAL/SHM 变化都不能提升可信高水位。普通 signal/event 不推进该 generation，因此普通热路径不会重复扫描永久历史。

`PASSED` 的原始 `decision` / `reason` / `detail_json` 以永久 `ACTIVE` audit 为权威发布证据，并受批次 manifest 认证。发布后，当前普通行的这三个字段只是计划/纸单/实单执行状态 overlay，可在有界且严格 JSON 校验下合法更新，不会改写原始发布 manifest。同批重试、当前 reader、下一批替换和离线维护都从 audit 重建 `PASSED` 原始证据，同时对 ordinary 行的 scan/strategy/symbol/passed/structure 等不可变身份作精确绑定；`REJECTED` 行没有执行 overlay，仍全字段精确参与 manifest。非法 JSON、非内建类型或超容量 overlay 必须 fail-closed，不能被当作正常 current 结果展示。

N16 的永久 claim 还使用 Binance 专属 state 目录中的独立 `n16_claim_ledger.sqlite3`。正式发布协议是 `PREPARED → Review 发布事务 → COMMITTED/READY`；任一写入失败、提交回执不明或两库状态混合，都会关闭当轮及后续 paper/live/exchange 总闸。普通运行只用固定元数据与链头做 `O(1)` 认证，并对 CURRENT、开放 paper/live 以及本地 pending 所涉及的 structure 用唯一索引做 `O(log n)` 逐 claim 交叉认证，不在每次启动扫描全部历史。停服维护、install、resolve、retention 和 VACUUM 才执行 Review 永久全图、全量逐 claim 交叉认证、ledger catalog/index/integrity 的 `O(n)` 全审。威胁边界是“Review DB 单故障域可任意损坏，独立 state ledger 保持可信”；不宣称能抵御 Review 与 state ledger 被同时协调伪造，后者需要本次范围外的远程追加账本。

正式运行合同是合作式授权主体加部署隔离：所有仓库授权的 Review、ledger、SQLite sidecar 与路径 namespace 写操作，只能由运行中的 Binance 服务执行，或在服务已停止后由持有同一正式 `/home/trading-example/binance-trading-bot/shared/state/trading_bot.lock` 的 maintenance、release backup、VOID 入口执行。正式锁必须已经存在、basename 逐字为 `trading_bot.lock`、与 N16 ledger 位于同一真实父目录、为非 symlink 的单链接 regular file；工具不会创建另一个锁。运行期间禁止同 UID 进程绕过工具直接 rename、link、unlink 或打开数据库路径。提交前的 FD/inode、路径与 sidecar 身份检查用于发现授权入口的漂移和误操作，不构成两个 SQLite 文件之间的 OS 原子事务，也不声称冻结目录 namespace；不合作进程直接执行原始文件系统操作超出代码保证。

存量历史库不会在普通启动时自动回填、无界删除或 `VACUUM`。首次启用必须在只停止 Binance 应用服务的维护窗口中，先完成上文备份和数据库硬门，再使用显式维护入口。CLI 本身保留通用的 Binance 命名兼容，但正式生产编排只能逐字传入 `/home/trading-example/binance-trading-bot` 与 `/home/trading-example/binance-trading-backups` 两个专属根；禁止通配、禁止把变量展开成更宽的 root，也禁止加入第三个目录。固定路径骨架如下：

```bash
# 仅真正 pre-N16 且独立账本缺失的停服首装场景使用。
python3 maintain_strategy_signals.py --install-n16 \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock || exit 1

# N16 两库边界 READY 后，仅在同一停服维护窗口显式安装 N17。
python3 maintain_strategy_signals.py --install-n17 \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock || exit 1

# N16 READY 且 N17 CURRENT 后，仅在同一停服维护窗口显式安装 N19。
python3 maintain_strategy_signals.py --install-n19 \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock || exit 1

# N16 READY、N17 CURRENT、N19 CURRENT 后，仅在同一停服维护窗口显式安装 N18。
python3 maintain_strategy_signals.py --install-n18 \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock || exit 1

# N16 READY 且 N17/N19/N18 CURRENT 后，仅在同一停服维护窗口显式安装 N20。
python3 maintain_strategy_signals.py --install-n20 \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock || exit 1

# N16 READY 且 N17/N19/N18/N20 CURRENT 后，一次性显式安装 N21-N25。
python3 maintain_strategy_signals.py --install-n21-n25 \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock || exit 1

# N17-N25 全部 CURRENT 后，显式把既有 N17/N18/N19 coverage 升级为不可变 epoch chain。
# 同一停服边界也把精确 v3 显式升级为 v4：N19 coverage NO_CHANGE
# 终态发布会由 coverage receipt binding 与永久 terminal receipt 双向认证；
# 同一命令还显式安装 N15 冻结快照终态回执代次：既有 active
# snapshot 只补精确 payload hash，不删除、不改写；普通启动绝不自动安装。
# retention 删除普通 signal/batch 后仍可独立复核，普通启动绝不自动升级。
python3 maintain_strategy_signals.py --install-history-coverage-epochs \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock || exit 1

# 仅为已获用户逐字授权的 EXAMPLEUSDT 单条 v3 历史终态使用。
# inspect 全程只读；把输出的 12 个 expected 字段逐项原样回传给 install。
python3 maintain_strategy_signals.py --inspect-authorized-legacy-v3-witness \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock || exit 1

python3 maintain_strategy_signals.py --install-authorized-legacy-v3-witness \
  --expected-symbol EXAMPLEUSDT \
  --expected-family-id '<INSPECT_FAMILY_ID>' \
  --expected-structure-id '<INSPECT_STRUCTURE_ID>' \
  --expected-terminal-evidence-sha256 '<INSPECT_OUTPUT>' \
  --expected-state-row-sha256 '<INSPECT_OUTPUT>' \
  --expected-receipt-count '<INSPECT_OUTPUT>' \
  --expected-receipt-set-sha256 '<INSPECT_OUTPUT>' \
  --expected-coverage-graph-sha256 '<INSPECT_OUTPUT>' \
  --expected-coverage-catalog-sha256 '<INSPECT_OUTPUT>' \
  --expected-review-canonical-sha256 '<INSPECT_OUTPUT>' \
  --authorization-sha256 '<YOUR_AUTHORIZATION_SHA256>' \
  --expected-review-plan-sha256 '<INSPECT_OUTPUT>' \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock || exit 1

# 仅在上一步发生确认异常后，重新 inspect/核对同一组 expected 值，再显式选
# SAFE_ABORT 或 SAFE_COMMIT；工具不会自动猜测或自动补写。
python3 maintain_strategy_signals.py --resolve-authorized-legacy-v3-witness \
  --legacy-witness-resolution '<SAFE_ABORT_OR_SAFE_COMMIT>' \
  --expected-symbol EXAMPLEUSDT --expected-family-id '<INSPECT_FAMILY_ID>' \
  --expected-structure-id '<INSPECT_STRUCTURE_ID>' \
  --expected-terminal-evidence-sha256 '<INSPECT_OUTPUT>' \
  --expected-state-row-sha256 '<INSPECT_OUTPUT>' \
  --expected-receipt-count '<INSPECT_OUTPUT>' \
  --expected-receipt-set-sha256 '<INSPECT_OUTPUT>' \
  --expected-coverage-graph-sha256 '<INSPECT_OUTPUT>' \
  --expected-coverage-catalog-sha256 '<INSPECT_OUTPUT>' \
  --expected-review-canonical-sha256 '<INSPECT_OUTPUT>' \
  --authorization-sha256 '<YOUR_AUTHORIZATION_SHA256>' \
  --expected-review-plan-sha256 '<INSPECT_OUTPUT>' \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock || exit 1

该入口不生成或回填 v4 terminal receipt，也不选择历史 scan/batch owner。它只把
`AUTHORIZED_LEGACY_V3_UNBOUND` 见证写入独立永久表，并在 N16 ledger 中镜像同一
plan/witness 摘要。原 v3 receipt 顺序、哈希链、head 和 N19 state 逐字保持；同一
family 的 normal receipt 与 legacy witness 由 family seal 互斥。任一 PREPARED、
Review-only、ledger-only 或混合代次都会阻断普通启动、retention 和 VACUUM。

该显式安装不会把完整 Review DB 复制到内存。只读源快照以逐行、定长摘要块和
有界归并计算 canonical/full 摘要；模拟安装使用权限为 `0600` 的临时 SQLite
文件，实际打开的 FD 会在第一条 SQLite 操作前认证，并在成功或失败后删除。
CLI 会在 stderr 依次输出 source attestation、file simulation、ledger prepared、
Review apply/commit 和 ledger commit 阶段及累计耗时，便于停服维护诊断。发布前可
在本地生成式数据上复跑约 290 MiB Review 与 10251 条 receipt 的资源门（不读取
服务器或 `.env`，临时数据退出时删除）：

```bash
PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 \
  tests/authorized_legacy_witness_scale_gate.py \
  --database-mib 290 --receipt-count 10251 \
  --max-install-seconds 120 --max-rss-mib 512
```

# 仅用于旧版 N17 已持久化的“未闭合尾K→已闭合K”唯一可证缺陷。
# 必须先停服并完成 Review DB、N16 ledger 和 state 的同代备份；普通启动绝不自动修复。
python3 maintain_strategy_signals.py --repair-n17-frozen-evidence \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock || exit 1

`--repair-n17-frozen-evidence` 先在只读快照中认证 N16 ledger READY、N17/N19/N18/N20 CURRENT 和 N17 每条生命周期；它明确支持生产真实的 N20 CURRENT + N21–N25 PRE_MICRO 代次，也支持 micro CURRENT 的幂等复核。发布顺序必须是先 repair，严格复核 N17，再执行 `--install-n21-n25`；禁止为了修复 N17 先制造 micro 半升级。它只接受一种唯一可证转换：冻结 source 的原最后未闭合K与 structure 中同 open time 的已闭合K满足 OHLC 与累计量单调演进，且仅替换这一行后原严格解码器能完整复现 box/touch/A/C 及全部派生值。多行冲突、box 窗口冲突、证据不唯一或任一前代图不一致均在写前拒绝。修复只更新 N17 该行的 `evidence_json/evidence_sha256`，不改结构身份、阶段、原因、排名或任何交易规则；事务失败则完整回滚。无法唯一认证时必须保持 fail-closed，回滚只能使用旧代代码 + 维护前 Review DB + 同代 N16 ledger + 对应 state 的整套备份，禁止删除历史行或单边恢复。

# 仅精确 SAFE_ABORT 或 SAFE_COMMIT 分类才允许解析 PREPARED。
python3 maintain_strategy_signals.py --resolve-n16-publication \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock || exit 1

python3 maintain_strategy_signals.py --dry-run \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock \
  --keep-scan-id <independently-confirmed-complete-scan-id> \
  --keep-signal-count <independently-confirmed-exact-count> || exit 1

python3 maintain_strategy_signals.py --apply \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock \
  --keep-scan-id <same-scan-id> \
  --keep-signal-count <same-exact-count> \
  --keep-manifest-sha256 <dry-run-retained-manifest-sha256> \
  --batch-size 500 || exit 1

停服后若保留一个未发布 `STAGING`，`--dry-run` 必须同时报告其
`scan_id/recorded_count/manifest` 以及 STAGED audit、structure ledger 和 micro
execution lifecycle 计数。`--apply` 只在 CURRENT 与这个唯一 STAGING
全图严格一致时，按 micro lifecycle → structure ledger → audit → ordinary
signal → batch 的依赖顺序单事务清理。`micro_passed_analyses` 原始分析永久
保留；任何 ACTIVE claim、多个 STAGING、manifest/范围/图冲突或注入失败都在
写前拒绝或完整回滚。禁止用手工 SQL 替代这个入口。

# VACUUM 源必须已经是停服、checkpoint 完成且三类 sidecar 均不存在的
# 工作副本。禁止用数条彼此独立的 sqlite3 shell 命令声称它们处于一个
# 连续持锁区间；本仓库当前没有授权这种 raw shell 编排。任一 sidecar
# 存在即 NO-GO，不得删除或绕过。
test ! -e /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3-wal || exit 1
test ! -e /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3-shm || exit 1
test ! -e /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3-journal || exit 1
test ! -e /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3-wal || exit 1
test ! -e /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3-shm || exit 1
test ! -e /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3-journal || exit 1

python3 maintain_strategy_signals.py \
  --binance-root /home/trading-example/binance-trading-bot \
  --binance-root /home/trading-example/binance-trading-backups \
  --vacuum-into "$backup_dir/trading_review.compacted.sqlite3" \
  --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 \
  --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 \
  --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock || exit 1
```

`--dry-run` 以 SQLite `mode=ro` 读取数据库，先核对由运维独立确认的完整行数，同时执行 Review 永久全图、独立 ledger catalog/index/integrity 与两库全量逐 claim 交叉认证，再报告 `retained_manifest_sha256`、回填数量、可删除数量、外键/完整性和保护哈希，不安装 schema。每条维护命令都必须逐字传入同一个绝对 `--n16-claim-ledger`，不得由 Review 或 compact 输出目录推导 sibling。不带 manifest 的首次 dry-run 会明确报告 `keep_attestation_verified=false`；只有带回同一 scan/count/manifest 的 `--apply` 才允许写入。`scans.completed_at` 仅作信息展示，绝不作为完整发布证明。`--apply` 先将所有 `passed=1` 的永久审计和十策略 ledger 严格回填为 `ACTIVE`，再分批删除非保留 scan 信号；已提交批次可幂等续跑，不重置 `sqlite_sequence`。

N21-N25 发布后，上述 inspect/apply/每批删除/最终激活/VACUUM 都会依次认证 N16 独立 ledger，以及 N17、N19、N18、N20 和 N21-N25 的专用 schema/root/state/coverage/压缩证据；任一单边缺失或冲突都在首个写入前 `NO-GO`。

普通启动不会创建、迁移或修复 N16 Review schema/独立 ledger。pre-N16、缺 ledger、`INSTALLING`、`PREPARED` 和任何半升级状态均只读零写拒绝，必须在停服且持有 Binance 实例锁的显式维护入口处理。`--install-n16` 只允许从可证明的 pre-N16 空 N16 生命周期起点进入 `INSTALLING → Review schema commit → READY`；CURRENT+ledger 缺失绝不 bootstrap。`--resolve-n16-publication` 在任何 ledger phase 写入前先完成 Review 永久全图和 PREPARED base 全量核对，仅精确 `SAFE_ABORT` 或 `SAFE_COMMIT` 允许状态变化；混合、冲突、身份不明或查询失败都保留 PREPARED 并 `NO-GO`。

N17 同样不由普通启动新建或修复。`--install-n17` 只从已认证的 N16 CURRENT/READY、无 N17 业务痕迹的精确 pre-N17 起点安装；pre-N17、部分表、错索引、外来 trigger/incoming FK 或 root 冲突都是只读零写拒绝，不会自动猜测升级。

N19 也不由普通启动新建或修复。`--install-n19` 只从 N16 CURRENT/READY、N17 CURRENT 且无 N19 业务痕迹的精确 pre-N19 起点安装；pre-N19、部分表、错索引、外来 trigger/incoming FK 或 root 冲突均只读零写拒绝。N16 半安装/发布恢复诊断优先，其后是 N17 代次诊断；二者都完整后才会报告 pre-N19。

N18 同样只允许通过停服显式 `--install-n18` 从 N16 CURRENT/READY、N17 CURRENT、N19 CURRENT 且无 N18 业务痕迹的精确 pre-N18 起点安装。普通启动面对 pre-N18、半升级、缺表、错索引、外来 trigger/incoming FK 或 root 冲突都只读零写拒绝；诊断顺序固定为 N16、N17、N19、N18，不用新代错误遮蔽旧代恢复。

N20 只允许通过停服显式 `--install-n20` 从 N16 CURRENT/READY、N17/N19/N18 CURRENT 且无 N20 业务痕迹的精确 pre-N20 起点安装。普通启动面对 pre-N20、半升级、缺表、错索引、外来 trigger/incoming FK、root 或压缩证据冲突都只读零写拒绝；诊断顺序固定为 N16、N17、N19、N18、N20。

N21-N25 只允许通过停服显式 `--install-n21-n25` 从 N16 两库 READY 且 N17/N19/N18/N20 全部 CURRENT 的精确边界一次性安装。普通启动面对 pre-N21-N25、部分表、错索引、外来 trigger/incoming FK 或安装 root 冲突都只读零写拒绝；内存微观测缓存不写入 SQLite，也不能替代永久 PASSED audit/lifecycle 证据。

`VACUUM INTO` 是分离的显式步骤，只能写入已存在的 Binance 专属目录中的新文件；程序不会在 apply 时暗中执行，也不会暗中 checkpoint、切换 journal mode 或删除 sidecar。正常 apply 关闭后仍可能留下合法 `-wal/-shm`；若没有由一个受控入口在同一次正式锁持有期间完成 checkpoint、journal 切换和关闭验证，就不能继续压缩。任一 sidecar 仍存在都必须 `NO-GO`；绝不能直接删除尚未成功 checkpoint 的 sidecar，也不得把这些操作用于生产其他数据库或任何全局 SQLite 配置。VACUUM 源与目标父目录必须是不同 inode；目标先在已打开并认证的父目录 fd 下的私有 `0700` staging 目录生成，再以不覆盖方式发布，父路径在最后间隙被 rename、symlink 或替换也不能把写入导向外部目录。源预检使用 sidecar-free 的 immutable 只读快照，并在写入目标前再次验证同一主文件/父目录/sidecar identity；目标生成后还会比较完整 schema catalog、数据库标识、全表及保护证据，并在 CLI 最终 scope 复核成功前继续持有父目录 fd；任一步失败都只按已认证 inode 清理未发布输出。物理替换、启停和回滚由发布会话在停服窗口单独演练。正式编排只允许按示例各声明一次精确的应用根与备份根；数据库、锁和目标路径会在拿锁前做 resolve/same-file 隔离校验。

运行时启动与离线 VACUUM 的零写承诺不同：数据库父目录必须预先创建；主库、父目录和 sidecar 都先做 inode/link 身份认证。没有 WAL 或 WAL 为空时，启动 preflight 用 `mode=ro&immutable=1` 读取 checkpointed main；只有经过 regular/non-symlink/`st_nlink == 1` 验证的非空 WAL 才用普通 `mode=ro` 读取最新页，此时 SQLite 可能合法更新 Binance 自身 SHM，但不得触碰任何外部或其他系统 sidecar。VACUUM 则要求三类 source sidecar 全部不存在，并在同一个 immutable 只读源连接中验证后执行 `VACUUM INTO`，源 main、journal mode 和目录不得被切换或改写。

有界清理后的 compact DB 依赖新版本的永久 audit 与 durable ledger；旧 commit 不认识这些防重复证据，绝不能把旧代码单独指向新 DB。正式发布必须将“新代码 + Review DB + N16 claim ledger + 对应 state”作为同一代次原子切换。回滚前必须确认没有 live 仓位或 `execution_pending`，并且只能同代恢复“旧 commit + 停服前 Review DB + 同代 N16 ledger + 对应 state”。只恢复 Review、只恢复 ledger、只恢复 state、只切代码或混合不同检查点都是“单边恢复 NO-GO”。停服前 Review DB 和 N16 ledger 的同代一致备份必须保留到新版本稳定且用户另行批准清理，以上文件仍只能位于 Binance 专属路径，不能借发布或回滚触碰另外三套系统。

上述工具和日志保留只允许作用于 Binance 应用自身的显式文件路径。不得改动全局 journald/logrotate/Python/SQLite 配置，不得控制或扫描其他三套服务、端口、目录、数据库或环境。应用 `LOG_FILE` 在自身目录内每日轮转，只删除超过15天的同名应用日志；不触碰 journald 和其他系统日志。

## 多策略候选池

系统只做 U 本位永续合约开多。N01-N05 使用负资金费率候选：

```text
lastFundingRate <= -FUNDING_RATE_ABS_THRESHOLD
```

默认阈值是 `0.015`，也就是资金费率小于等于 `-1.5%`。候选列表按资金费率从最负到较负排序。

当前生产活跃策略集精确为 N01-N05，只使用原负资金费率候选。N06-N25 的分析实现、schema 和历史证据仅为兼容查询与已有 OPEN 纸单的 close-only 自然结算保留；运行时不再为 N06-N25 构建候选、分析、新纸单或新实盘机会。完整 CURRENT 精确等于 N01-N05 的当轮资金费率候选结果；无候选时以受认证的零行 CURRENT 原子发布，不产生虚假信号或开仓。

## 策略框架

- N01-N05：沿用 A/B/C 形态、96 根已收盘 K 线趋势与当前 K 线转阳条件。
- N06：独立的二次破高回调企稳分析器，不使用 A/B/C 或 96 根趋势条件。
- N07：二次收盘破高后的 P1 近距离回踩，不等待 N06 的 5 根企稳，也不要求当前 K 线为阳线。
- N08：至少 20 根长时间震荡后的最早五连阳，只在第 6 根开始后前 120 秒入场。
- N09：唯一 S1 到 L 的带噪声单段慢跌，在快速反弹首次触及 P50 时按 P43 下限入场。
- N10：48根支撑窗口后的放量假跌破 W、主动买入确认 C，并在当前 E 的前120秒入场。
- N11：EMA20 高于 EMA50 环境中，连续至少 8 根波动收缩后放量突破，只在首次有效回踩后的下一根 K 线前 120 秒入场。
- N12：成交额前100中最近96根已收盘涨幅为正的前10强，只做最近有效上涨段后的首次缩量回调与首次重启确认。
- N13：Top100 广度牛市中的第11-60名轮动币，只做脱离96根VWAP后首次回踩冻结价值区并完成主动买入确认的机会。
- N14：Top100 中相对市场出现局部恐慌卖压、但不是系统性崩跌的币，只做卖压冲击后紧邻吸收、主动买入确认和短窗收复。
- N15：市场由弱转暖时，从 B 弱市中最抗跌、并在紧邻 C 回暖时率先突破的币中冻结唯一先行者。
- N16：成熟上升趋势中，锁定高低点抬高后第一次 EMA20 动态支撑触及、第一次主动买入确认和紧邻入场窗。
- N17：20–64根横向箱体完成后，只锁定第一次下沿正常承接 T、紧邻卖压衰减 A 与最多两根内的首个主动买入确认 C。
- N18：EMA20 不低于 EMA50 的上升三角中，只锁定三次水平压力、三次抬高低点、第三次吸收触点 A 与最多四根内的首个放量突破 B。
- N19：10–19根中速三段阶梯下跌后，锁定第一个创新低 X、成交额/主动买入衰竭与最多三根内的首个确认 C，并独立使用同轮 Top100 排除系统性崩跌。
- N20：在 D1 实时冻结牛市 Top100，持续记录2–6根市场回调，以96根收益和 ATR 抗跌排名冻结全部合格领涨候选，并只让六级稳定排序的唯一 winner 进入紧邻恢复 C 的入场窗。
- N21-N25：复用同一轮15m累计成交与 premiumIndex 的45–150秒跨轮增量，分别识别连续主动买流、卖压吸收背离、成交速率点火、BTC/ETH领先后的滞后回补及负溢价压缩恢复；五策略共享一份最多400条的内存观测，不新增行情请求。
- N01-N05 不再新开纸单，也不读取纸单连胜或 `live_eligible`。CURRENT 全量原子发布成功后，合格信号直接进入既有账户、风险、仓位和执行门。
- 真实交易全局最多 1 个仓位；检测到手动或外部仓位时跳过真实下单，不回退为纸单。
- 真实下单明确失败也不回填为纸交易或更新纸单资格字段；N06-N25 历史 OPEN 纸单仅允许按已完成 K 线结算，不强制伪造 VOID。
- 同一策略的兼容活动模式仍为 `IDLE`、`PAPER_OPEN` 或 `LIVE_OPEN`：历史 `PAPER_OPEN` 在 close-only 收口前阻断该策略新的真实开仓，`LIVE_OPEN` 阻断重复真实开仓；系统不会因任一模式新建纸单。
- 多个 N01-N05 真实候选使用 `strategy_id + symbol` 做稳定排序，不读取纸单胜率、资金费率强弱、成交额或最近交易时间。
- 全局同币冷却与策略亏损冷却是两层独立规则。全局4小时冷却命中时阻断该币种新的真实开仓，并记录 `GLOBAL_SYMBOL_COOLDOWN_UNTIL`；手工、外部或系统真实仓位也只阻断新的真实开仓。系统不再新增任何纸单；仅 N06-N25 历史 OPEN 纸单保留 close-only 自然结算。

N06 和 N07 共用已收盘 15m K 线的 pivot/fractal 高低点与二次破高骨架，左右确认各 2 根。人工走势段参数集中为：端点索引差至少 5 根、ATR 周期 14、close 端点净位移至少 `1.2×ATR`、close 路径效率 ER 至少 0.35，且上涨/回调段的 close 线性回归斜率必须分别严格大于/小于 0。段首尾 close 方向也必须与段方向一致：上涨段末 close 严格高于首 close，回调段则严格低于；不允许回归斜率掩盖反向端点。一根涨跌交替、低位移或低 ER 只视为段内噪声。前置下跌必须由 `3个递降高点 + 3个递降低点` 交替组成，其中每一条摆动腿都通过同样的有效段检查。

共同骨架顺序为：有效前置下跌、已收盘 close 第一次严格突破 H0、有效第一上涨段形成候选 H1、H1 后有效回调段形成候选 P1、P1 后至少 5 根有效上涨并由已收盘 close 严格突破 H1。H1 必须是该上涨段至 P1 前的最高已确认 pivot，P1 必须是 H1 后至二破前的最低点；期间新高/新低会更新候选，不会过早锁定旧 pivot。如果最低价被后续同价重测，P1 固定为首次到达该低价的时间，更高局部低点或同价后续低点不会创建第二个骨架。只有到此时 H1/P1 才追认为正式结构，上影线越过不成立。同一结构 ID 锁定最终 symbol、H0、第一次突破、H1、P1 与第二次突破，不包含滚动窗口索引。所有段参数、斜率、ATR、位移与 ER 都进入策略配置和信号 JSON。

N06 在第二次收盘破高后不再把首根 low 当 P2：第二上涨段必须继续并以已确认的最高 pivot 锁定真实 H2，再等待 H2→P2 的独立有效回调段，要求 `P2 > P1`。P2 之后才另行计算连续 5 根不再创新低，走势段的5根不能与稳定确认重复计数。分析按历史前缀枚举已确认 H2/P2，并锁定最早完成稳定确认的结构；之后新出现的更高 H2 或新回调不会改写已完成的 H2/P2/稳定时间。第 5 根确认 K 线必须是最新已收盘 K 线，当前未收盘 K 线必须紧随其后，且只允许在 `0 <= elapsed < 120s` 内且当前 K 线为阳线时触发。历史完成事件只回填 `MISSED`，不补开仓。

N07 首先要求第一波回调振幅 `pullback_pct = (H1 - P1) / H1 >= 6%`，6% 边界允许。第二次收盘破高后，先以已确认的第二上涨高点 H2 为起点，要求 H2 到最新已收盘 K 线已构成有效回调段；当前未收盘 K 线才可作为首次触及 `[P1*1.01, P1*1.015]` 的入场 K 线。单根尖刺首次入区记录 `N07_INVALID_FIRST_TOUCH_SEGMENT` 并永久消费；回调尚未成段且仍未触区时只继续等待。更早已收盘 K 线的 `low` 必须严格大于区间上界；已穿越下界、触区后反弹到上界之上，或任何 `low <= P1`，都会永久消费该骨架。

N08 使用独立的震荡分析器，不读取 N06/N07 二次破高结构。分析器对五连阳第 1 根之前、紧邻的 20-96 根已收盘 K 线从长到短验证，选择最长有效后缀窗口。窗口至少包含 2 个 pivot high、2 个 pivot low 和 4 个压缩后交替转折；连续同类 pivot 只保留更极端者。上下沿分别是 pivot high/low 中位数，同类 pivot 离散和窗口影线均不得超过箱体高度的 25%。

在有效箱体后，N08 只锁定第一组完整五连阳，每根只要求 `close > open`，不要求收盘递增或突破上沿。分析器只解释当前未收盘第 6 根前紧邻的 5 根已收盘 K 线；如果前一根仍是阳线，则视为 6 根或更长序列的滚动窗口并拒绝。五连阳期间 low 跌破箱体下容差边界则结构失效。第 6 根未收盘 K 线的 open time 必须严格紧接第 5 根，并且只接受 `0 <= elapsed_seconds < 120`；当前价使用该 K 线的实时 close。

箱体首次通过、错过 120 秒窗口或出现其他终结性结果后，会写入 `n08_structure_states`。同价格箱体后续出现另一组五连阳仍按已消费结构拒绝，状态在进程重启后继续生效。此后 K 线严格越过旧箱体上/下容差边界时先持久记录重置时间；只有新箱体的开始时间晚于该重置时间，旧状态才转为 `RETIRED` 并允许新结构触发。边界内的窗口平移，以及当前第 6 根才发生的越界，都不能绕过旧状态。

N08 每次评估当前候选前，还会按时间顺序回放本次 K 线响应中“第 6 根已经收盘”的历史箱体和最早五连阳。停机期间完整错过的事件会以 `HISTORICAL_N08_STRUCTURE_MISSED` 幂等补入 `n08_structure_states`，再结合中间的容差越界时间推进 `CONSUMED`/`RETIRED`，因此同箱体第二组五连阳不会因数据库为空而重新开仓，明确越界后形成的新箱体也不会被旧事件全局阻塞。

`n08_history_coverage` 按策略和币种持久保存连续处理的15分钟 K 线水位。首次启动只看到 `96 + 5 + 当前线` 共102根、且没有箱体开始前的覆盖证据时，记录 `HISTORICAL_N08_CONTEXT_INCOMPLETE` 并安全拒绝；如果机器人此前已连续观察到箱体开始之前，本轮响应与旧水位重叠或相邻，则完整96根箱体可以正常通过。响应内部不连续，或新响应与旧水位之间存在无法由当前窗口回放的长缺口时，连续水位从缺口后重置并记录 `n08_history_coverage_gap`，不能假装拥有前序上下文。该机制不增加币安请求次数。

N06、N08、N10-N25 都有绝对120秒截止时间；N21-N25 从最后必要数据的实际到达时间起算，旧策略仍按各自冻结 candle 边界起算。deadline 随 `TradePlan` 进入信号详情和订单审计，纸交易及真实单的 plan、journal、set leverage、BUY 前均复核；达到或超过截止时间不创建仓位。N01-N05、N07 与 N09 的 deadline 为空。

N09 使用独立慢跌分析器。S1 是 2 左/2 右确认的局部高点，L 是快速反弹前的最低 low；系统不会在原 S1 因明显反弹、横盘或趋势不合格后改选所谓 S2。S1 到 L 至少 20 根已收盘 15m K 线，close 回归必须 `slope < 0` 且 `R² >= 0.65`。任一阴线实体、任一阶段低点后的最大逆势反弹都不得超过深度 `D = S1 - L` 的 20%。所有连续 8 根窗口都必须满足第 1 根到第 8 根 close 的净跌幅严格大于 D 的 5%，等于 5% 即按横盘拒绝；红绿 K 交替和小幅噪声不影响上述结构判定。

N09 定义 `P50 = L + 0.50D`、`P43 = L + 0.43D`。首次 high 触及 P50 必须不超过 40 根，且不超过慢跌时长的一半。当前共享 K 线 close 位于闭开区间 `[P43, S1)` 即可触发，即使已经回落到 P43-P50 之间也允许；低于 P43、达到 S1、历史 K 线已经首触 P50，或历史首触后曾到 S1，均永久消费该结构。分析器按时间锁定活动 S1：活动结构未首次触达 P50 前，下降途中的较低 pivot 不会被改算成 S2；历史首次触达、超时或到达 S1 形成终结边界后，才允许边界后的新 pivot 建立下一套结构。同一响应中的历史终结事件会按时间幂等补入 `n09_structure_states`，随后当前新结构可以继续判断；重启后旧结构状态也不会阻塞新 identity。

122 根不是无限历史。对于一套恰好在响应开头保留 2 根左侧确认上下文的独立结构，可覆盖 `慢跌根数 + 反弹根数 <= 120`：包括 80+40 的最大反弹组合边界，以及最短 1 根反弹时最多 119 根慢跌。更长结构、或同一响应需要容纳更多旧结构时间线时，早期上下文会离开窗口；系统只对当前响应中仍完整可见且可确定推进的 S1/终结事件判断，不宣称支持窗口外的无限历史。

N09 稳定原因码包括：`SLOW_DECLINE_DURATION_TOO_SHORT`、`SLOW_DECLINE_TREND_NOT_QUALIFIED`、`SINGLE_CANDLE_DROP_TOO_LARGE`、`COUNTERTREND_REBOUND_TOO_LARGE`、`SIDEWAYS_WINDOW_DETECTED`、`REBOUND_TOO_SLOW`、`P50_NOT_TOUCHED`、`P43_MISSED`、`S1_REACHED`、`HISTORICAL_P50_TOUCH_MISSED`、`STRUCTURE_CONSUMED`、`HISTORY_CONTEXT_INSUFFICIENT` 和计划阶段的 `N09_STOP_PCT_OUT_OF_RANGE`。

N10 面向当前未收盘 E 识别固定序列：E 前一根是确认 C，再前一根是假跌破 W，W 前紧邻48根已收盘K线为支撑窗 B；51根 open time 必须严格按15分钟连续。`S=min(B.low)`，除产生 S 的K线外必须另有一根 low 位于 `[S, S×1.005]`，且两次触碰至少间隔4根。W 跌破深度只接受 `[0.3%,1.5%]`，必须 `close>S`、下影占整根至少50%，quote asset volume 至少为此前20根精确中位数的2.5倍。C 必须 `close>W.high`、`low>=W.low`，主动买入成交额比例至少55%。

E 使用本次15m响应中的当前 close 作为共享入场价和建单参考价，不读取候选快照中的 mark price；只接受 `[W.high, W.high×1.015]`，且 `E.low>=W.low`、`0<=elapsed<120000ms`。价格过低、追价过远、低点刷新或窗口过期都会立即消费 W；同一 E 后续恢复不能重触发。分析器按时间回放响应内已完成的历史 W+C+E，并将错过事件幂等写入 `n10_structure_states`；旧 W 不会阻塞后来独立的新 W。结构 ID 固定包含 symbol、B起止、S、两次支撑触碰、W时间/low/high及C时间。

N10 稳定原因码包括：`N10_NOT_ENOUGH_SUPPORT_HISTORY`、`N10_KLINE_DATA_INVALID`、`N10_KLINE_SEQUENCE_INVALID`、`N10_SUPPORT_NOT_RETESTED`、`N10_SWEEP_DEPTH_OUT_OF_RANGE`、`N10_SWEEP_NOT_RECLAIMED`、`N10_VOLUME_SPIKE_NOT_CONFIRMED`、`N10_LOWER_WICK_TOO_SMALL`、`N10_CONFIRMATION_NOT_BROKEN_HIGH`、`N10_STRUCTURE_LOW_BROKEN`、`N10_TAKER_BUY_RATIO_TOO_LOW`、`N10_ENTRY_WINDOW_EXPIRED`、`N10_ENTRY_PRICE_BELOW_RECLAIM`、`N10_ENTRY_PRICE_TOO_EXTENDED`、`N10_STRUCTURE_CONSUMED`、`N10_HISTORY_CONTEXT_INSUFFICIENT`、`N10_STATE_READ_FAILED`、`N10_STATE_PERSIST_FAILED`、`N10_STOP_PCT_OUT_OF_RANGE` 和 `N10_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE`。

N11 使用 `EMA20 > EMA50` 的上涨环境。突破前至少连续 8 根已收盘 K 线必须满足 `BB20=MA20±2σ` 完整位于 `KC20=EMA20±1.5×ATR14` 内，边界相等允许。首次突破必须已收盘 close 严格超过前20根最高价 R，阳线实体至少 `0.6×前置ATR14`，收盘位于整根顶部25%，quote volume 至少为前20根中位数的1.8倍，主动买入 quote 比例至少55%。

突破后第1-6根已收盘 K 线中，第一根 `low <= R+0.3×ATR` 的 K 线即锁定为首次回踩。它必须未跌破 `R-0.3×ATR`、`close>=R`、收在整根上半部，且 quote volume 不超过突破 K 线的80%。首次触碰失败会立即永久消费，不等第二次回踩。有效回踩后只允许在下一根当前 15m K 线的 `0<=elapsed<120000ms` 内入场，当前 close 必须位于闭区间 `[R, retest.close+0.5×ATR]`。历史错过、失败首触、超时、价格越界与已发信号均写入 `strategy_structure_terminal_states`，结构 ID 使用 symbol、固定 squeeze 时间、突破时间和 R，支持历史补账、窗口平移和重启幂等。

N12 先在每轮共享批次中用每币同一份 K 线计算 `(96根最后已收盘close / 第一根open)-1`，只保留涨幅为正的前10名；同涨幅按 quoteVolume rank、symbol 稳定排序。首次完整批次会把该 current 15m open time 对应的完整候选集合、96根收益、quoteVolume rank 和最终强势 rank 写入 `n12_rank_snapshots`；同一当前 K 线内后续轮询及进程重启都复用该快照，换入币和 quoteVolume rank 变化不能改榜，下一根当前 K 线才创建新快照并清理旧快照。排名不进入 structure ID。参与 top100 的任一候选只要 K 线断档、长度不足、当前/最后已收盘时间轴与其他候选不一致，或行情相对本轮检查时间已经陈旧，本轮 N12 横截面排名整体 fail-closed，不会过滤坏候选后继续排名，也不会写入快照。历史补账仍使用同轮共享 K 线按历史入场时点重建各币恰好连续的前置96根排名；任一币缺少完整上下文时只记 `N12_HISTORICAL_RANK_CONTEXT_INSUFFICIENT`，不用当前冻结快照替代。

N12 选择最近的2左/2右 pivot 上涨段 L→H：至少5根、涨幅至少3%、位移至少 `3×atr_at_h`、close 斜率为正、ER至少0.50，且任一阳线实体严格小于段位移的50%。`atr_at_h` 是截至 H 收盘的 Wilder ATR14。首次回调2-5根，深度闭区间 `[20%,45%]`，close 不低于半程位，成交额中位数不超过上涨段的70%，high 不高于 H。首次 C 必须阳线、严格收过前一根high、严格低于 H、收在顶部25%，主动买比例至少55%，成交额至少为回调中位数的1.2倍。

N12 只在 C 后紧接的当前 K 线 `0<=elapsed<120000ms` 入场，价格闭区间为 `[C.high, C.close+0.5×atr_at_c]`，当前 low 允许等于 P 但不得低于 P。`atr_at_c` 是截至 C 收盘的 Wilder ATR14，与 `atr_at_h` 分别写入审计 JSON，两者都不进入 structure ID。只有出现首个基本 C（阳线且 close 严格高于前一根 high）后才生成 symbol+L/H/P/C 时间构成的完整 ID；5根回调全部收盘后仍等待下一根候选 C 收盘，只有该候选收盘且不是基本 C 才写 `N12_CONFIRMATION_NOT_FOUND`。C 出现前的首次回调失败只写入 `n12_stage_states` 的 L/H 阶段终态，不伪造 C 或完整结构。C 已出现的当前与历史终态通过同一 SQLite 事务原子写入 stage/full 两张表，任一写失败会整体回滚。遗留半状态仅在既有 payload 能证明同一 structure ID 时按原 status/reason/detail 补齐；合法 `stage_event` 保持 stage-only，无法证明关联或两侧 payload 冲突时以 `N12_STATE_INCONSISTENT` fail-closed。重启和后续更漂亮回调都不会复活。

N13 在每根当前未收盘15m K线 E 的第一次完整、时间对齐 Top100 批次中冻结横截面快照。快照必须恰好包含原生 rank 1-100 的100个唯一成员，收益排名和两个市场广度都从行数据重算验证。同一 E 后续轮询和重启复用原100个成员，不用新入榜币替换旧成员；缺任一冻结成员的共享行情或快照语义不一致时整批 fail-closed，不新增行情请求，下一根 E 才允许换榜。市场条件为96根收益为正的币种占比至少60%、最新已收盘价高于各自96根滚动VWAP的币种占比至少50%。个币必须96根收益严格为正、收益排名位于闭区间11-60，且当前 C 的96根VWAP严格高于配置签名指定的斜率参考VWAP；当前默认参考8根前，并以通用字段 `vwap_slope_reference` 审计实际参考值。N12 专注收益前10名龙头的首次缩量回调，N13 专注第11-60名在广度牛市中的价值区轮动，两者不共享排名状态。N13 的设计目标是在其专属行情中保留小时级机会，而不是增加周级低频门槛；当前实现没有用历史回测宣称策略有效。

结构先于市场过滤锁定。A 必须满足 `low_A > VWAP_A+0.25×ATR14_A`，并冻结价值区 `Z_A=[VWAP_A-0.50×ATR_A, VWAP_A+0.25×ATR_A]`。T 是 A 后第1-8根中第一根与 Z 相交的K线；先从区间下方跳空穿过或第9根才触碰都使本轮失效。T 到 T+3 中第一根同时满足 `close>=当前VWAP`、阳线、`close>前收`、收盘位置至少60%、主动买入成交额占比至少50%的完整K线才是 C；单项失败继续寻找，只有 `close<冻结Z下界` 才立即破坏价值区。交易所返回的合法平 K（`O=H=L=C`）可以参与历史 VWAP/ATR 和快照计算，但不能成为 C，会记录 `N13_CONFIRMATION_FLAT_CANDLE` 后继续查找。结构 ID 只含 symbol、T时间和C时间。

N13 只在紧接 C 的当前 E 中接受 `0<=elapsed<120000ms`，实时价闭区间为 `[C.close, C.close+0.5×ATR_C]`。价格低于 C.close 且 E.low 未跌破 P 时只等待，不消费；价格超过上界、超时或 `E.low<P` 才终结，`E.low==P` 允许。完整 C 锁定后若冻结市场快照明确不满足条件，该 T/C 被消费，不能改用更晚的“漂亮 C”；快照上下文不完整则不消费，可在同一 E 内恢复。历史 C 只用于防止重启补开，不借用当前 rank/breadth 描述历史。stage、历史和当前终态以单个 SQLite 事务写入 `n13_rotation_states`，冲突以 `N13_STATE_INCONSISTENT` fail-closed。当前终态使用 schema 固定的 canonical envelope：只包含 A/T/C、P、冻结区、C 时 VWAP/ATR 等不可变证据并保存 SHA256，不包含实时 E、当前价或 elapsed；读取时同时重算结构证据和摘要，额外字段、改值或坏摘要都会拒绝。

N14 面向“市场整体尚可，但单币出现局部恐慌抛售后迅速衰减”的行情，与 N09 的长慢跌反弹、N10 的支撑假跌破、N12 的强势回调和 N13 的广度牛市 VWAP 轮动都不同。每根已收盘 S 都从当时原生 Top100 完整快照计算：市场1小时中位收益不得低于 -0.75%，目标币1小时跌幅排名必须位于1-20，并至少比市场中位数多跌 `1×ATR%`；若全市场阴线广度达到75%且中位单根跌幅达到系统性下杀阈值，则直接以 `N14_SYSTEMIC_CRASH_VETO` 排除，不把普跌误当局部错杀。

S 本身必须为实体至少 `0.8×ATR14` 的阴线，quote volume 至少为前20根中位数的1.5倍，taker-buy quote 比例不高于40%，且收盘位于整根底部30%；此前3根中已经出现同类完整冲击时不重复建结构。紧接 S 的 A 必须保有至少0.8倍成交额、继续保持偏卖方成交，但振幅明显收缩且不能有效刷新冲击低点，表示卖压仍在成交、价格破坏却在衰减。A 后最多2根已收盘 K 线寻找首个 C：C 不得跌破 `P=min(S.low,A.low)`，必须为阳线、收过前收和 A.high、收复 S 实体中点、收在顶部30%，taker-buy quote 比例至少55%，成交额至少0.8倍中位数。这里的 taker-buy 比例只是交易方向代理，不宣称等同订单簿吸收证据。

C 完成时冻结 Top100 的上涨广度及通过门：广度至少40%，或相对 S 时改善至少15个百分点。随后只在紧接 C 的当前 E 前120秒接受 `[C.close, C.close+0.5×ATR_C]` 闭区间；价格尚低于下界只等待，跌破 P、追价超过上界或超时才消费。结构 ID 固定为 symbol+S/A/C 时间，stage、终态、S 原始K线、冲击派生量、冻结市场场景、C广度及门类型均使用严格 schema 和摘要持久化；同一 E 复扫和重启复用冻结值，不因瞬时广度变化改写已经确认的 C。跨过 E 仍未入场则只记 `HISTORICAL_N14_ENTRY_MISSED`，绝不补开。

N14 不依赖历史回测结论或一周、半月级过滤器，只判断当前共享 Top100、最近15分钟结构和紧邻入场窗。它仍保留必要的首发冲击、吸收和确认条件，不能承诺固定开单次数；设计目标是专属行情出现时及时触发，而不是靠极端苛刻条件制造纸面高胜率。同轮多个 N14 通过时，先按 `flow_flip`、C收盘位置、成交额排名和 symbol 固定唯一内在第一名；第一名的状态/信号/计划失败不得递补第二名，但第二名或不合格币的状态故障也不会拖累健康第一名。

N15 固定使用最新相邻的 B/C 和当前 E，不回看选择更弱 B 或更漂亮 C。首次完整、连续、同轴的原生 Top100 批次冻结 B/C 证据、两套 move 排名、弱市/回暖广度、中位数、唯一 winner 和配置摘要；同一 E 内换榜、重启或 quoteVolume 排名变化都不能改写 winner。B 的 ATR 截至 B-1，C 的 ATR 截至 C-1，C 的20根成交额中位数严格排除 C。弱市要求 `down_B>=55%` 且 move 中位数不高于 `-0.10`，系统性崩跌 veto 要求 `down_B>=75%` 且中位数不高于 `-0.50`；回暖要求 `up_C>=55%` 且相对 `up_B` 改善至少20个百分点，平 K 不计入 up/down。

N15 winner 只由冻结 B/C 证据按 C move、B rank、C 主动买入比例、原生成交额排名和 symbol 决定。只在紧接 C 的 E 前120秒接受 `[C.close,C.close+0.5×ATR_C_pre]`；价格低于下界只等待 winner，跌破 P、追价或超时则消费整个机会，绝不递补第二名。N15 不读取订单簿，也没有新增 REST、WebSocket 或扫描进程。当前实现没有使用历史回测或参数拟合，不构成历史有效性、实盘盈利或固定开单频率背书；路径只跨相邻 B/C/E，定位不是周级低频过滤器。

N15 同一 E 的冻结 Top100 成员会并入主循环已有的去重 K 线集合，即使成员随后掉出即时 Top100，也仍只在该轮统一请求一次 K 线，用于恢复冻结 winner 和审计排名。快照 schema v2 只在短生命周期的 `n15_market_snapshots` 中保存创建时从窗口首根到 B 的有界连续 OHLC/成交额证据，并用正式 Wilder ATR、V20、排名、广度、reason 和 winner 算法独立重算；这些原始证据不会复制到普通 signal、paper 或 live 明细。跨 E 时先把旧 winner 保守登记为 `HISTORICAL_N15_ENTRY_MISSED` 再清理旧快照，滚动窗口即使丢掉最早 ATR seed 也不会重算改写冻结结果；停机时间超过响应覆盖范围时仍只使用通过 identity、schema、配置签名、原始证据重算和 hash 校验的 v2 快照收口，绝不补开。旧 schema v1 在同一 E、原创建窗口仍完整时原位升级当前唯一一行，不做启动期无界回填；部署恰逢下一边界、固定122根窗口只丢一根 seed 时，兼容读取还必须逐币种精确对账 B/C/V20，并按正式 Decimal/Wilder 舍入规则从物理可行的最小 seed 正向递推，证明存量 ATR 不低于可达下界；随后只写历史 MISSED，绝不补开。超出这一可证明窄窗的 v1 会保留原证据并 fail-closed 等待显式维护，不把“无法证明”冒充损坏去删除。边界首扫若100个当前候选的 E 轴尚未一致，或冻结成员行情暂缺，会给出明确原因、保留旧快照并安全等待；第一轮轴完整的扫描即完成旧 winner 终态和新 context。真正违反 schema、配置、identity、原始证据或派生语义的快照仍会隔离并阻断当前轮，下一轮可重新冻结。

N16 固定读取最新 122 根连续 15m K 线，结构逻辑使用 96 根已收盘线，左右 pivot 各 2 根。时间轴必须为 `L1<H1<L2<H2<A<C<E`：L2 严格高于 L1，H2 相对 H1 推进至少 `0.25×ATR_H2`，L1 到 H2 至少 16 根；A 时 `EMA20>EMA50`，EMA50 相对 8 根前严格上升。L2→H2 上升腿位移至少 `2×ATR_H2`、close 路径效率至少 0.40。这些是 N16 的历史规则说明。N16/N17 属于停用历史策略；既有历史证据、纸单及真实链路只读可查询，历史 OPEN 纸单仅 close-only 自然结算；运行时不再扫描、不新增纸单、不产生真实候选。

H2 后第 2–8 根中第一根同时满足 `low<=EMA20+0.25×ATR` 且 `close>=EMA20-0.20×ATR` 的触及固定为 A，不改选后续更漂亮的 A。从 H2 后第1根到 A 的回调深度必须为 L2→H2 上升腿的 15%–45%，回调 quote volume 中位数不超过上升腿中位数的90%。A 后 1–3 根中第一根同时满足阳线、`close>前一根high`、`close>EMA20`、收盘位置至少65%、taker-buy quote 比例至少52%、quote volume 至少为前20根中位数的90%的 K 线固定为 C。`P` 是 H2 后至 C 的全部 low 最小值，后续不会为了更优外观改选 A/C。

E 必须紧接 C，只在 `0<=elapsed<120000ms` 且实时 close 位于闭区间 `[C.close,C.close+0.50×ATR_C]` 时通过。低于下界只等待；`E.low<P`、高于上界或截止时间到达才终结消费，历史已收盘 E 只记 MISSED，绝不补开。identity 仅使用 N16、规则版本、symbol 和 L1/H1/L2/H2/A/C 绝对 open time；窗口索引、价格和 ATR 不进 ID。`TOUCH_LOCKED`、`CONFIRMING`、`CONFIRMED`、`CONSUMED`、`MISSED`、`INVALID`、`EXPIRED` 的原始连续 K 线、派生值、首次合格 E 观察、配置和 canonical hash 写入专用 `n16_trend_support_states`，证据上限 128KiB；普通 signal 只保留引用与有界摘要。同 identity 证据冲突整轮 fail-closed；未发布批次不消费结构，掉出即时 Top100 的活动 episode 仍并入共享行情抓取直至终态。

同轮多个 N16 币种通过时，全部原始命中都保留专用永久证据；仅按 `quote_volume_rank→symbol→structure_id` 排序的第一名进入可执行 PASSED audit/ledger，其计划失败不递补。结构止损为 `floor(P)-1 tick`；不足全局最小止损距离时继续向下扩展，超过全局最大距离时拒绝而不向上钳回。真实成交后必须仍在冻结入场区间，然后重算止损、风险、保证金和数量，TP 向有利方向按 tick 取整并证明实际 `R>=5`。N16 已不在当前活跃集合；实现、永久证据和历史 paper/live 记录仅作兼容读取与收口，不构成当前资格门或开仓路径。这些只是历史工程与规则说明，不保证信号频率、胜率或实盘收益。

N17 固定读取最新 122 根连续 15m K 线。在 T 前的已收盘线中取 20–64 根的最长紧邻有效后缀箱体 B，pivot 左右各 2，至少各2个 pivot high/low 且压缩后至少4个交替转折。上下沿是同类pivot中位数，离散与影线越界都不超过箱体高度的25%，首尾净位移不超过高度的50%。T 是箱体完成后首次下沿承接；`L*(1-0.003)<T.low<=L+0.20*ATR14`、`T.close>=L`，成交额严格低于前20根精确中位数的2.5倍。达到0.3%跌破或同时满足 N14 局部恐慌定义都硬排除，不与 N10/N14 双重 PASSED。

T 后紧邻已收盘 A 必须 `low>=T.low`、`close>=L`、range 不超过 T 的85%，且 `sell_quote=quote_volume-taker_buy_quote_volume` 不超过 T 的85%。A 后最多两根中只锁定首个有效 C：`low>=T.low`，close 严格突破 T/A/中间K线的最高 high，收盘位置至少65%，taker-buy quote 比例至少52%，quote volume 至少为前20根中位数的80%。期间任何 `low<T.low` 立即 INVALID，两根内无确认则 CONSUMED；E 前紧邻5根若全为阳线，按 N08 语义硬排除。E 必须紧接 C，仅在 `0<=elapsed<120000ms`、close 位于闭区间 `[C.close,C.close+0.50×ATR_C]` 且 `low>=T.low` 时通过；历史错过、超时、破位或越界均永久终结，绝不追单。

N17 identity 绑定 symbol、B起止、上下沿、T open time/T.low，同一箱体只认首次T。只有价格明确越过旧箱体容差边界，且新箱体 start 晚于已持久 reset 时间，才允许新 family。活动成员掉出即时 Top100 后仍并入共享K线集合直到终态，不为掉榜币创建新 family。同轮多币的可执行代表按“离L更近、sell_quote衰减更强、quote-volume rank、symbol、structure_id”排序；第一名失败不递补。止损为 T.low 按 tick 向下标准化后再下移1 tick，不足1%向下扩到精确1%，超过5%拒绝；真实成交仍须在冻结入场闭区间，并以实际成交均价和最终止损向上对齐TP，证明实际 `R>=5`。N17 已不在当前活跃集合；实现、永久证据和历史 paper/live 记录仅作兼容读取与收口，不构成当前资格门或开仓路径。这是历史工程规则说明，不保证收益。

N19 固定读取最新 122 根连续 15m K 线（121根已收盘+当前E）。它只接受 `S→L1→R1→L2→R2→X→C→E` 的10–19根三段阶梯下跌：`L1>L2>X`、`S>R1>R2`，总跌幅为闭区间 `2–6×ATR_X`，两次反弹均为紧前下降腿的20%–55%。X 是 R2 后第一根创新低；它的 range、quote volume 与 taker-buy 改善必须同时证明衰竭。X后1–3根内只锁定第一根 bullish 且 `close>X.high` 的 C；该 C 的收盘位置、taker-buy 或成交额任一失败即永久 CONSUMED，不改选后续“更漂亮”的 C。

C 时独立用该轮原生完整 Top100 与同份共享K线计算1小时收益。下跌成员比例至少75%且精确中位收益不高于-1.0%时使用 `N19_SYSTEMIC_CRASH_VETO`拒绝；成员缺失、错轴或非法数据都以 `N19_MARKET_CONTEXT_INSUFFICIENT` fail-closed。普通 N15 弱市不是 N19 veto，N19 也不调用 N09/N14/N15 分析器。E 必须紧接C，仅在 `0<=elapsed<120000ms`、close 位于闭区间 `[C.close,C.close+0.50×ATR_C]` 且 `E.low>=X.low` 时通过；历史错过、超时、破位或超上界均永久终结。

N19 family identity 只绑定 strategy/symbol 和 S/L1/R1/L2/R2/X 绝对 open time，structure identity 再绑定首个 C 绝对 open time。失败 C、终态 cutoff 与严格晚于 cutoff 的 `close>R2` reset 都先永久落账；只有新 S 严格晚于已持久 reset 才允许新 family。活动 family 掉出 Top100 仍加入同一去重K线并集直至终态，不为掉榜币新建 family。同轮多币的可执行代表严格按 `quote_volume_rank→symbol→structure_id` 选第一名，计划/状态/信号失败不递补。这些是规则和安全验收，不保证频率、胜率或收益。

N18 固定读取最新122根连续15m K线（121根已收盘+当前E），只接受 `L1→H1→L2→H2→L3→A→B→E`。三次压力触点的中位数为R，离散同时不超过 `0.35×ATR_A` 与 `0.5%×R`；三个低点严格抬高，初始高度至少 `2×ATR_A`，最终高度比不超过65%，且 A 时 `EMA20>=EMA50`。A 必须是同一 family 的第三次压力吸收触点，任何吸收门失败都永久消费该 family，不向后改签。

A 后1–4根只锁定第一根 bullish 且 `close>R+0.10×ATR_B` 的突破候选B；body、收盘位置、主动买入或成交额任一失败即 CONSUMED。E 必须紧接B，仅在 `0<=elapsed<120000ms`、close 位于闭区间 `[B.close,B.close+0.50×ATR_B]` 且 `low>=R` 时通过；若同时跌破L3，优先按更严重的结构失效处理。episode identity只绑定六个形态点绝对时间，structure identity再绑定B绝对时间；终态后新 family 的L1必须严格晚于旧 terminal cutoff。掉出Top100的活动episode仍进入同一去重K线并集。同轮代表严格按 `quote_volume_rank→symbol→structure_id` 选第一名，失败不递补；这是工程规则验收，不保证收益。

## 仓位和止损止盈

N01-N05 使用振幅止损：

```text
24h振幅 = (24h最高价 - 24h最低价) / 24h最低价
止损百分比 = 24h振幅 × 0.12，最低 1%，最高 5%
最大杠杆 = 交易所返回的该币种最大杠杆
风险金额 = 可用余额 × 20%
开仓数量 = 风险金额 / (开仓价 × 止损百分比)
止损价 = 开仓价 × (1 - 止损百分比)
止盈价 = 开仓价 × (1 + 止损百分比 × 5)
```

如果按这个仓位计算出的保证金占用超过 `MAX_MARGIN_BALANCE_FRACTION`，系统会跳过该币种，不会缩小仓位硬开。

N06 使用结构止损，不读取 24h 振幅：

```text
止损价 = P1
R = 精度处理后的开仓价 - 精度处理后的 P1
止盈价 = 开仓价 + 5 × R
开仓数量 = 风险金额 / R
```

N06 的信号价计划用于纸交易和真实下单前的数量/风险检查。真实市价单成交后必须确认实际 `avgPrice`，再以 P1 为固定止损、按实际成交价重新计算不低于 5R 的向上 tick 对齐止盈；无法确认成交价会立即紧急平仓。state 与 `trade_reviews` 记录实际成交价和实际保护价。P2 只用于确认企稳，不作为止损价。

N07 同样固定以 P1 为止损，但使用独立的保证金封顶仓位公式：

```text
target_risk_amount = balance × 20%
target_risk_qty = target_risk_amount / (entry_price - P1)
margin_cap_qty = balance × MAX_MARGIN_BALANCE_FRACTION × leverage / entry_price
final_qty = min(target_risk_qty, margin_cap_qty, exchange_max_qty)
actual_risk_amount = final_qty × (entry_price - P1)
```

`final_qty` 再按 `stepSize` 向下对齐。低杠杆时 N07 会缩小仓位而不是丢弃信号，并记录 `target_risk_amount`、`actual_risk_amount` 和 `risk_capped_by_margin`；只有缩小后仍不满足 `minQty`/`minNotional`、P1 不低于入场价或价格/数量无效时才拒绝。该缩仓规则不影响 N01-N06。

N08 使用与 N01-N05 相同的 24h 振幅止损百分比，但低杠杆时不放弃信号：

```text
stop_pct = clamp(amplitude_24h × 0.12, 1%, 5%)
target_risk_qty = balance × 20% / (entry_price × stop_pct)
margin_cap_qty = balance × MAX_MARGIN_BALANCE_FRACTION × leverage / entry_price
final_qty = min(target_risk_qty, margin_cap_qty, exchange_max_qty)
```

N08 计划止损按信号价和 `stop_pct` 计算，数量按 `stepSize` 向下对齐；只有缩仓后低于 `minQty`/`minNotional` 或价格/数量无效时才拒绝。

N09 的实际成交价为 F，固定止盈为 S1，并由固定 1:5 反推止损：

```text
risk_distance = (S1 - F) / 5
SL = F - risk_distance
TP = S1
stop_pct = (F - SL) / F
```

精度处理后必须保持 `TP > F > SL` 且实际盈亏比不低于 5；`stop_pct` 只接受闭区间 `[1%, 5%]`，不做钳制。仓位先按余额 20% 风险计算，再与最大杠杆可承载保证金数量和交易所最大数量取最小值，低杠杆时缩量继续。真实成交后 N09 也使用下述成交后硬校验、超额减仓和紧急清理机制，止盈仍固定 S1，止损按实际 F 重新反推。

N10 先将 W.low 按 tickSize 标准化，实际止损为其下方1个 tick。建单时会把 `W.high` 和 `W.high×1.015` 作为结构化闭区间写入 TradePlan；真实成交后先校验实际成交均价仍在该区间，再计算止损比例、R、风险和保证金。越界成交使用 `N10_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE` 归因并紧急全平，绝不挂保护单或保存成功持仓。信号价和真实成交价阶段都要求 `stop_pct=(entry-SL)/entry` 位于闭区间 `[1%,5%]`，止盈按 `entry+5×(entry-SL)` 向上对齐 tick，确保实际R倍数不低于5。仓位使用余额20%目标风险和保证金上限取小；低杠杆缩量继续。真实成交后重新核验实际数量、风险、保证金和止损比例，超限先减仓，无法确认或保护失败则紧急清理，不保存成功持仓。

N11 将回踩 low 向下对齐 tick 后的下方1个 tick 作为结构止损。若该距离不足1%，向下扩大到1%；若精度后超过5%，使用 `N11_STOP_PCT_OUT_OF_RANGE` 拒绝，不钳制。止盈按实际入场价 `entry+5R` 向上对齐 tick。TradePlan 同时保存 `[R, retest.close+0.5×ATR]` 的闭区间，真实成交价越界使用 `N11_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE` 紧急全平。仓位使用余额20%目标风险、最大杠杆保证金上限和交易所最大数量取小；低杠杆缩量继续。成交后重算安全数量、风险、保证金、止损和至少5R止盈，超限减仓；减仓、持仓确认或保护失败都进入可审计的紧急清理，不保存成功 state。

N12 将 P 向下对齐 tick 后再下移1个 tick 作为原始结构止损。若参考价或实际成交价对应的止损距离不足1%，止损继续向下扩大到1%；精度后超过5%则使用 N12 专用原因拒绝，不钳制。TradePlan 保存 `[C.high, C.close+0.5×atr_at_c]` 闭区间及绝对 deadline，真实成交后重验区间、止损比例、风险、保证金、最终数量和至少5R止盈。低杠杆按保证金上限缩量继续；成交越界、减仓失败、仓位无法确认或保护失败均进入 N12 可审计的紧急清理，不保存成功 state。

N13 将 P 向下对齐 tick 后再下移1个 tick 作为原始结构止损，不足1%向下扩至1%，精度后超过5%则以 `N13_STOP_PCT_OUT_OF_RANGE` 拒绝。TradePlan 保存 `[C.close, C.close+0.5×ATR_C]` 闭区间和绝对 deadline；真实成交越界以 `N13_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE` 归因并紧急全平。仓位继续使用余额20%目标风险、保证金上限、低杠杆缩量、成交后重验和至少5R保护。

N14 同样把 `P=min(S.low,A.low)` 向下对齐后再下移1个 tick 作为原始结构止损；不足1%向下扩至1%，超过5%以 `N14_STOP_PCT_OUT_OF_RANGE` 拒绝。TradePlan 固定保存 `[C.close,C.close+0.5×ATR_C]` 和绝对 deadline；真实成交越界以 `N14_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE` 紧急全平，不挂保护单。仓位、低杠杆缩量、成交后风险/保证金重验与至少5R保护沿用同一硬上限。

N16 将 P 向下对齐后再下移1个 tick 作为原始结构止损；不足1%时向下扩展到1%，精度后超过5%使用 `N16_STOP_PCT_OUT_OF_RANGE` 拒绝，不钳回结构止损。TradePlan 保存 `[C.close,C.close+0.5×ATR_C]` 闭区间和绝对 deadline；真实成交越界以 `N16_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE` 紧急全平。仓位、低杠杆缩量、成交后风险/保证金重验与实际不低于5R的保护沿用同一硬上限。

N17 将 T.low 向下对齐后再下移1个 tick 作为原始结构止损；不足1%时向下扩展到精确1%，精度后超过5%使用 `N17_STOP_PCT_OUT_OF_RANGE` 拒绝。TradePlan 保存 `[C.close,C.close+0.5×ATR_C]` 闭区间和绝对 deadline；真实成交越界以 `N17_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE` 紧急全平，不挂保护单也不保存成功仓位。仓位仍按余额20%目标风险、保证金上限、低杠杆缩量和成交后实际 `R>=5` 重验执行。

N19 将 X.low 向下对齐后再下移1个 tick 作为原始结构止损；不足1%时向下扩展到精确1%，精度后超过5%使用 `N19_STOP_PCT_OUT_OF_RANGE` 拒绝。TradePlan 保存 `[C.close,C.close+0.5×ATR_C]` 闭区间和绝对 deadline；真实成交越界以 `N19_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE` 紧急全平，不挂保护单也不保存成功仓位。仓位仍按余额20%目标风险、保证金上限、低杠杆缩量和成交后实际 `R>=5` 重验执行。

N20 将唯一 winner 在 D1…C 的最早最低 P 向下对齐后再下移1个 tick 作为原始结构止损；不足1%时向下扩展到精确1%，精度后超过5%使用 `N20_STOP_PCT_OUT_OF_RANGE` 拒绝。TradePlan 保存 `[C.close,C.close+0.5×ATR_C]` 闭区间和绝对 deadline；真实成交越界以 `N20_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE` 紧急全平，不挂保护单也不保存成功仓位。仓位仍按余额20%目标风险、保证金上限、低杠杆缩量和成交后实际 `R>=5` 重验执行。

N21-N25 将确认观测时当前15m累计 low 向下对齐后再下移1个 tick 作为原始结构止损，不足1%向下扩至1%，精度后超过5%拒绝。入场上界分别为确认价加 `0.25/0.25/0.30/0.25/0.20×ATR`，deadline 为最后必要行情实际到达时间加120秒；paper、plan、pre-submit、set leverage 与 BUY 前均以同一绝对时间复核。真实成交越界立即全平且不挂保护，最终止盈按实际成交与最终止损向上取 tick 并证明 `R>=5`。

N07-N25 真实市价成交后都会先查询实际 LONG 仓位数量，并用实际成交均价重新计算风险上限和保证金上限。如果原成交数量超限，先用 reduce-only/LONG-side 市价单减掉超额数量，验证减仓回执后再查询最终剩余仓位。只有 `post_fill_actual_risk_amount <= target_risk_amount` 且 `post_fill_required_margin <= balance × MAX_MARGIN_BALANCE_FRACTION` 时才挂保护单，并保持各策略定义的止损以及精度后至少5R。

减仓未成交、最终仓位无法确认或硬上限仍超限时，系统不挂保护单、不保存持仓 state，而是进入紧急清理。清理会先按未确认前的完整成交数量尝试平仓；如果超量 reduce-only 被拒绝，再查询精确剩余数量重试。任何无法确认已空仓的情况都以错误和完整审计信息结束，不会静默记为成功。

N07-N25 的真实市价 BUY 在提交前先持久化 client order ID、策略、计划和预生成的 STOP/TP client ID；响应丢失、超时或身份不一致时不重复下单，也不继续挂保护，而是按原 ID 查询并进入可恢复的 `execution_pending`。保护单创建与撤销也同时校验 symbol、client ID、类型、方向和 closePosition；撤销响应丢失后仍以 GET 终态确认。按 symbol 全撤只允许在已证明该币种全部开放 Algo 单都属于本次保护时使用，存在人工或其他系统订单时禁止批量撤销。

N16-N25 带120秒入场窗口的真实单会先完成身份与首次 deadline 检查，再持久化 `PRE_SUBMIT_RESERVED`，复核 deadline，并以原子 compare-and-save 推进到可恢复的 `MARKET_ORDER_SUBMITTING`；只有这些必要 state 写入全部确认后才允许 `set_leverage` 和 BUY。窗口到期或任一 journal 写入失败时，杠杆、市价单和保护单调用均为0；杠杆调用失败则保留已认证 journal，由下一轮按固定 client ID 安全恢复，不会重复 BUY。当前 reduce-only 减仓和应急 SELL 仍沿用既有接口，尚未为每次 SELL 增加独立 client order ID，因此不能宣称整个异常清理链具备绝对请求幂等性。

真实仓位从交易所消失后，系统使用 state 中保存的止损/止盈 `algoId` 查询 Algo 状态和触发后的实际订单，不再用平仓后的当前 mark price 猜测胜负。止损成交记 `LOSS`，止盈成交记 `WIN`；判定前必须先撤销并确认另一张仍为 NEW 的保护单。人工/外部平仓记 `MANUAL_OR_EXTERNAL_CLOSE`。查询失败、状态非终态、兄弟保护单未确认撤销或结果冲突时进入 `LIVE_RESULT_PENDING`，本地审计 state 保留，该策略禁止后续实盘与纸交易，并写入报警事件。

纸交易平仓不使用每60秒瞬时 mark price。开仓后的首个不完整1分钟从精确 `opened_at` 开始查询 aggTrades，按成交时间和聚合成交ID判定先触发止损还是止盈；无法确定同时成交顺序时保守按止损并记录冲突。首段无触发后，再按币种复用已完成 1m K线的 high/low 继续检查。

## 目录

```text
trading_bot/
├── main.py                 # 单进程多策略主循环
├── monitor.py              # 资金费率与成交额候选池
├── analyzer.py             # A/B/C 分析器
├── double_break.py         # N06/N07 共用二次破高骨架
├── n06_analyzer.py         # N06 P2 企稳入场分析器
├── n07_analyzer.py         # N07 P1 近距离回踩分析器
├── n08_analyzer.py         # N08 震荡五连阳分析器
├── n09_analyzer.py         # N09 单段慢跌半程反弹分析器
├── n10_analyzer.py         # N10 放量假跌破收复分析器
├── n11_analyzer.py         # N11 波动收缩突破首次回踩分析器
├── n12_analyzer.py         # N12 相对强势首次缩量回踩分析器
├── n13_analyzer.py         # N13 广度牛市VWAP轮动分析器
├── n14_analyzer.py         # N14 局部卖压衰减反转分析器
├── n14_snapshot.py         # N14 Top100/S冲击冻结快照
├── n15_analyzer.py         # N15 弱市回暖先行入场分析器
├── n15_snapshot.py         # N15 Top100 B/C证据与唯一winner快照
├── n16_analyzer.py         # N16 成熟趋势动态支撑续涨分析器
├── n16_claim_ledger.py     # N16 独立永久claim账本与两阶段发布认证
├── n17_analyzer.py         # N17 箱体下沿正常承接反弹分析器
├── n17_schema.py           # N17 专用生命周期 schema 与显式安装认证
├── n18_analyzer.py         # N18 上升三角压力吸收突破分析器
├── n18_schema.py           # N18 专用生命周期 schema 与显式安装认证
├── n19_analyzer.py         # N19 中速阶梯下跌衰竭反转分析器
├── n19_schema.py           # N19 专用生命周期 schema 与显式安装认证
├── n20_analyzer.py         # N20 牛市回调相对强势领涨恢复分析器
├── n20_schema.py           # N20 压缩永久证据 schema 与显式安装认证
├── micro_observation.py    # N21-N25 有界内存观测、generation proposal/CAS
├── micro_analyzer.py       # N21-N25 跨轮微结构分析器
├── micro_schema.py         # N21-N25 永久PASSED/lifecycle显式安装认证
├── release_backup.py       # 持锁后身份复核与成对发布备份
├── strategies.py           # N01-N25 定义（N16-N25为独立frozen定义）
├── strategy_scheduler.py   # 策略分发与实盘候选排序
├── signal_retention.py     # strategy_signals 离线迁移、校验与续跑
├── paper_trader.py         # 独立纸交易仓位
├── trader.py               # 下单逻辑
├── state.py                # 持仓状态管理
├── logger.py               # 日志
├── config.py               # 参数配置
├── recorder.py             # 交易复盘数据库
├── binance_client.py
└── precision.py
```

## 复盘数据库

系统会自动创建 SQLite 数据库，默认路径：

```text
data/trading_review.sqlite3
```

主要表：

- `scans`：每次轮询扫描概要
- `signal_reviews`：每个候选币种的资金费率、趋势、形态、是否通过
- `trade_reviews`：开仓成功/失败、24h振幅、止损百分比、止盈百分比、风险金额、N07-N25 目标/实际风险、成交前/成交/最终保护数量、成交后风险/保证金、是否减仓、保证金占用、保护价与订单回执
- `events`：跳过扫描、已有持仓等运行事件
- `strategy_definitions`：策略配置快照
- `strategy_signals`：只保留最新完整 `CURRENT` 轮的逐策略/逐币种判断，计算中可同时存在一个不可见的 `STAGING`
- `strategy_signal_batches` / `strategy_signal_current`：两代批次状态、完整行数/摘要和原子当前指针
- `strategy_passed_signal_audits`：永久保存每条原始 `PASSED` 的完整分析证据，不依赖普通信号行
- `strategy_passed_structure_ledger`：永久保存 N06/N07/N08/N11/N12/N16-N25 的已发布可执行 structure identity，不外键依赖 `strategy_signals`
- `strategy_paper_trades`：每个策略独立的纸交易开平仓结果及 `last_checked_at` 1m 检查点
- `strategy_states`：连胜、胜率、真实下单资格与 `live_result_pending` 安全阻断状态
- `strategy_live_links`：真实交易与触发策略的关联
- `strategy_structure_terminal_states`：N06/N07/N11/N12 的 `MISSED`/`INVALID`/`TRADED`/`CONSUMED` 终态，按 strategy + structure ID 唯一，支持历史回填和重启幂等
- `n12_stage_states`：在 P/C 尚未形成完整 structure ID 前，以 strategy + symbol + L/H 时间锁死 N12 首次回调或首次确认失败，避免重启后改选后续结构
- `n12_rank_snapshots`：按当前15分钟 open time 持久冻结 N12 完整横截面候选、收益和排名；仅保留当前/最近一份快照
- `n13_market_snapshots`：按当前15分钟 open time 冻结并校验 N13 完整 Top100、收益排名、VWAP/ATR 与两个市场广度；仅保留当前一份快照
- `n13_rotation_states`：以 strategy + symbol + episode 时间保存 A/T/C 阶段、历史错过和当前终态；一次评估中的多条状态原子提交，重复 payload 必须完全一致
- `n14_market_snapshots`：按 S open time 冻结并校验 N14 原生 Top100、原始 S K线、冲击派生量、市场场景与固定配置签名
- `n14_active_episodes` / `n14_sell_impact_states`：保存 S/A/C 活动阶段、冻结 C 广度/门类型、历史错过和终态；原子写入且 payload 必须严格一致
- `n15_market_snapshots`：按 E open time 冻结并重算校验 N15 原生 Top100、B/C证据、横截面指标和唯一winner
- `n15_entry_states`：保存 N15 winner 当前 E 的 PASSED/MISSED 消费终态，精确幂等且冲突 fail-closed
- `n16_trend_support_states`：按 N16 episode 保存 L1/H1/L2/H2/A/C、固定122根 seed、连续派生证据、首次合格 E 观察、stage/reason 和 canonical hash，同 structure 不允许证据分叉
- `n17_range_support_states` / `n17_history_coverage`：保存箱体 B、首次 T/A/C、入场窗、reset/new-family 和每币连续历史水位；同 family/structure 证据必须完全一致
- `n18_triangle_states` / `n18_history_coverage`：保存 L1/H1/L2/H2/L3/A/B、水平压力、收敛、入场窗、终态 cutoff 和每币连续历史水位；同 episode/structure 证据冲突 fail-closed
- `n19_staircase_states` / `n19_history_coverage`：保存 S/L1/R1/L2/R2/X/C、Top100 上下文、终态 cutoff、reset/new-family 和每币连续历史水位；同 family/structure 证据冲突 fail-closed
- `n20_market_episodes`：永久保存 D1 时冻结的原生 Top100、M0/D1…Dk/C、市场 breadth/median、全部合格候选和唯一 winner；canonical 原文上限8MiB、zlib BLOB上限2MiB，原文 SHA256、大小、尾随数据和严格 JSON 读取时全部复核
- `state/n16_claim_ledger.sqlite3`：与 Review DB 独立故障域的 N16 永久 claim 账本，保存唯一 structure、顺序链头与两阶段发布元数据；必须与 Review/state 同代备份和回滚

N19 只允许在 N16 独立 ledger READY 且 N17 CURRENT 的精确代次上通过 `--install-n19` 停服安装。普通启动不会回填 N19 历史 signal、修表、删记录或执行 VACUUM；pre-N19、半安装、错 schema/root/index/trigger/incoming FK 均只读零写拒绝。

N18 只允许在 N16 独立 ledger READY、N17 CURRENT、N19 CURRENT 的精确代次上通过 `--install-n18` 停服安装。普通启动不会回填 N18 历史 signal、修表、删记录或执行 VACUUM；pre-N18、半安装、错 schema/root/index/trigger/incoming FK 均只读零写拒绝。

启动时 `ReviewRecorder` 会检查已安装库的表结构。N01-N15 现有兼容迁移保持原语义；N16 不会由普通构造器新建、迁移、bootstrap 或修复，只能由停服显式 maintenance 在两库路径/身份、实例锁与完整 pre-N16 起点都已证明后安装。N17 只允许在精确 N16 CURRENT/READY 边界后由 `--install-n17` 显式原子安装；普通启动面对 pre-N17、半升级或错 schema 只读零写拒绝。N06/N07/N11/N12 的完整结构终态增量写入 `strategy_structure_terminal_states`。N08 箱体、五连阳、历史补账结果、入场 K 线时间和绝对 deadline 保存在现有 JSON 审计字段；`n08_structure_states` 保存结构消费/释放状态，`n08_history_coverage` 保存每币种连续覆盖水位和缺口。N09 的 S1/L 和首触状态写入 `n09_structure_states`；N10 的 B/S/W/C/E identity、消费原因和历史补账写入独立 `n10_structure_states`；N12 的未完成阶段锁写入 `n12_stage_states`；N13 的冻结横截面写入 `n13_market_snapshots`，轮动阶段、历史和当前消费状态写入 `n13_rotation_states`；N14 使用独立快照、活动与终态表；N15 使用 `n15_market_snapshots` 和 `n15_entry_states`；N16/N17 普通启动不回填历史 signal、不删除记录也不执行 VACUUM。

策略效果展示使用已关闭交易样本数作为门槛。`strategy_effectiveness_status` 在样本少于30笔时只返回 `INSUFFICIENT_SAMPLE`（样本不足），达到30笔仅返回 `READY_FOR_EVALUATION`（可评估），不会仅凭样本数量把策略标记为“有效”。

旧规则纸单只能逐笔显式作废：

```bash
python3 -m trading_bot.void_paper_trade --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock --trade-id '<YOUR_TRADE_ID>' --strategy-id N06 --symbol '<YOUR_SYMBOL>' --confirm VOID_RULE_VERSION_INVALIDATED
python3 -m trading_bot.void_paper_trade --db /home/trading-example/binance-trading-bot/shared/data/trading_review.sqlite3 --n16-claim-ledger /home/trading-example/binance-trading-bot/shared/state/n16_claim_ledger.sqlite3 --lock-file /home/trading-example/binance-trading-bot/shared/state/trading_bot.lock --trade-id '<YOUR_TRADE_ID>' --strategy-id N07 --symbol '<YOUR_SYMBOL>' --confirm VOID_RULE_VERSION_INVALIDATED
```

必须先停止 Binance 服务，再执行上述命令。命令只接受与 N16 ledger 同一真实父目录中、已存在且单链接的正式 `trading_bot.lock`，并在构造 Recorder 之前取得该锁，直到完整 VOID 事务结束才释放。命令会严格校验 trade ID、策略、币种、`OPEN` 状态和确认字符，并在同一 SQLite 事务中写入 `result=VOID`、`exit_reason=RULE_VERSION_INVALIDATED` 与审计事件。重复执行返回 `ALREADY_VOID`；`VOID` 不改变连胜、胜负计数、胜率或实盘资格。不存在自动批量作废，未列出的 N08 纸单不受影响。

简单查看：

```bash
sqlite3 data/trading_review.sqlite3 ".tables"
sqlite3 data/trading_review.sqlite3 "select * from scans order by id desc limit 5;"
```

## API 说明

币安 USD-M Futures 在 2025-12-09 后将条件单迁移到 Algo Service。因此本系统：

- 市价开多：`POST /fapi/v1/order`
- 止损/止盈：`POST /fapi/v1/algoOrder`
- 保护单结果确认：`GET /fapi/v1/algoOrder`
- 纸交易首个部分分钟：`GET /fapi/v1/aggTrades`

这和旧版“止损止盈也走 `/fapi/v1/order`”不同。
