from pathlib import Path
import re
import unittest

from trading_bot.signal_retention import build_parser


ROOT = Path(__file__).resolve().parents[1]


class ReleaseIsolationDocumentationTests(unittest.TestCase):
    def test_readme_allows_only_the_binance_service_control_commands(self):
        readme = (ROOT / "docs" / "LEGACY_ENGINEERING.md").read_text(encoding="utf-8")

        self.assertNotIn("systemctl daemon-reload", readme)
        self.assertNotIn("<service-unit>", readme)
        self.assertNotIn("killall", readme)
        self.assertNotIn("pkill", readme)

        systemctl_lines = [
            line.strip()
            for line in readme.splitlines()
            if line.strip().startswith("systemctl ")
        ]
        self.assertEqual(
            systemctl_lines,
            [
                "systemctl stop binance-trading-bot.service || exit 1",
                "systemctl start binance-trading-bot.service || exit 1",
            ],
        )

    def test_readme_keeps_other_systems_read_only_and_no_go(self):
        readme = (ROOT / "docs" / "LEGACY_ENGINEERING.md").read_text(encoding="utf-8")

        self.assertIn("/home/trading-example/binance-trading-*", readme)
        self.assertIn("另外三套系统", readme)
        self.assertIn("NRestarts", readme)
        self.assertIn("NO-GO", readme)
        self.assertIn("不得尝试替它们修复或重启", readme)

    def test_vacuum_preparation_is_explicit_binance_only_and_never_raw_delete(self):
        readme = (ROOT / "docs" / "LEGACY_ENGINEERING.md").read_text(encoding="utf-8")

        self.assertNotIn("checkpoint=\"$(sqlite3", readme)
        self.assertNotIn("journal_mode=\"$(sqlite3", readme)
        self.assertIn("本仓库当前没有授权这种 raw shell 编排", readme)
        self.assertIn("-wal/-shm/-journal", readme)
        self.assertIn("绝不能直接删除尚未成功 checkpoint 的 sidecar", readme)
        self.assertIn("binance-trading-bot.service", readme)
        self.assertNotIn("rm -f *-wal", readme)
        self.assertNotIn("rm -f *-shm", readme)
        self.assertNotIn("rm -f *-journal", readme)

    def test_code_database_and_state_publish_or_rollback_only_as_a_pair(self):
        readme = (ROOT / "docs" / "LEGACY_ENGINEERING.md").read_text(encoding="utf-8")

        self.assertIn(
            "新代码 + Review DB + N16 claim ledger + 对应 state",
            readme,
        )
        self.assertIn(
            "旧 commit + 停服前 Review DB + 同代 N16 ledger + 对应 state",
            readme,
        )
        self.assertIn("单边恢复 NO-GO", readme)
        self.assertIn("只恢复 Review", readme)
        self.assertIn("只恢复 ledger", readme)
        self.assertIn("只恢复 state", readme)
        self.assertIn("只切代码", readme)
        self.assertIn("没有 live 仓位或 `execution_pending`", readme)
        self.assertIn("用户另行批准清理", readme)
        self.assertIn("durable ledger", readme)

    def test_lifecycle_ledger_is_explicit_in_every_maintenance_boundary(self):
        readme = (ROOT / "docs" / "LEGACY_ENGINEERING.md").read_text(encoding="utf-8")
        commands = re.findall(
            r"python3 maintain_strategy_signals\.py.*?\|\| exit 1",
            readme,
            flags=re.DOTALL,
        )
        self.assertEqual(len(commands), 15)
        expected_modes = (
            "--install-n16",
            "--install-n17",
            "--install-n19",
            "--install-n18",
            "--install-n20",
            "--install-n21-n25",
            "--install-history-coverage-epochs",
            "--inspect-authorized-legacy-v3-witness",
            "--install-authorized-legacy-v3-witness",
            "--resolve-authorized-legacy-v3-witness",
            "--repair-n17-frozen-evidence",
            "--resolve-n16-publication",
            "--dry-run",
            "--apply",
            "--vacuum-into",
        )
        self.assertEqual(
            [
                next(mode for mode in expected_modes if mode in command)
                for command in commands
            ],
            list(expected_modes),
        )
        explicit_ledger = (
            "--n16-claim-ledger "
            "/home/trading-example/binance-trading-bot/shared/state/"
            "n16_claim_ledger.sqlite3"
        )
        for command in commands:
            self.assertIn(explicit_ledger, command)

        for marker in (
            "--install-n16",
            "--install-n17",
            "--install-n19",
            "--install-n18",
            "--install-n20",
            "--install-n21-n25",
            "--install-history-coverage-epochs",
            "--inspect-authorized-legacy-v3-witness",
            "--install-authorized-legacy-v3-witness",
            "--resolve-authorized-legacy-v3-witness",
            "--repair-n17-frozen-evidence",
            "--resolve-n16-publication",
            "python3 -m trading_bot.release_backup",
            "SQLite `mode=rw`",
            "-wal/-shm/-journal",
            "n16_claim_ledger.absent=ABSENT_PRE_N16",
            "catalog/index/integrity",
            "Review 永久全图",
            "全量逐 claim 交叉认证",
            "`O(1)`",
            "`O(log n)`",
            "`O(n)`",
        ):
            self.assertIn(marker, readme)

        help_text = build_parser().format_help()
        normalized_help = " ".join(help_text.split())
        self.assertIn("--n16-claim-ledger", help_text)
        self.assertIn("--install-n16", help_text)
        self.assertIn("--resolve-n16-publication", help_text)
        self.assertIn("corresponding state", normalized_help)
        self.assertIn("one-sided restore is NO-GO", normalized_help)

    def test_pre_n16_missing_ledger_is_recorded_without_open_or_backup(self):
        readme = (ROOT / "docs" / "LEGACY_ENGINEERING.md").read_text(encoding="utf-8")
        self.assertIn("python3 -m trading_bot.release_backup", readme)
        self.assertIn("n16_claim_ledger.absent", readme)
        self.assertIn("ABSENT_PRE_N16", readme)
        self.assertIn("不得创建 0 字节 ledger", readme)
        self.assertIn("全程绝不向 SQLite 或 `lsof` 传入该路径", readme)
        self.assertNotIn('sqlite3 "$claim_ledger"', readme)
        self.assertNotIn('lsof -t -- "$claim_ledger"', readme)

    def test_effective_runtime_paths_and_inodes_are_pairwise_distinct(self):
        readme = (ROOT / "docs" / "LEGACY_ENGINEERING.md").read_text(encoding="utf-8")
        self.assertIn("这六个 resolved path 必须两两不同", readme)
        self.assertIn("os.path.samefile(left, right)", readme)
        self.assertIn("STATE_FILE` 与 ledger 重名", readme)

    def test_void_examples_pass_the_same_explicit_n16_ledger(self):
        readme = (ROOT / "docs" / "LEGACY_ENGINEERING.md").read_text(encoding="utf-8")
        void_commands = [
            line
            for line in readme.splitlines()
            if line.startswith("python3 -m trading_bot.void_paper_trade")
        ]
        self.assertEqual(len(void_commands), 2)
        for command in void_commands:
            self.assertIn(
                "--n16-claim-ledger /home/trading-example/binance-trading-bot/"
                "shared/state/n16_claim_ledger.sqlite3",
                command,
            )
            self.assertIn(
                "--lock-file /home/trading-example/binance-trading-bot/"
                "shared/state/trading_bot.lock",
                command,
            )

    def test_readme_states_the_cooperative_official_lock_threat_contract(self):
        readme = (ROOT / "docs" / "LEGACY_ENGINEERING.md").read_text(encoding="utf-8")
        self.assertIn("合作式授权主体加部署隔离", readme)
        self.assertIn("工具不会创建另一个锁", readme)
        self.assertIn("不构成两个 SQLite 文件之间的 OS 原子事务", readme)
        self.assertIn("不合作进程直接执行原始文件系统操作超出代码保证", readme)

    def test_each_release_uses_a_private_unique_non_overwriting_backup_directory(self):
        readme = (ROOT / "docs" / "LEGACY_ENGINEERING.md").read_text(encoding="utf-8")

        self.assertIn("--backup-root /home/trading-example/binance-trading-backups", readme)
        self.assertIn("严格互不相交", readme)
        self.assertIn("祖先/后代", readme)
        self.assertIn("任何 SQLite 打开、`release-*` 创建或文件复制之前", readme)
        self.assertIn("随机唯一 `0700` 目录", readme)
        self.assertIn("目标全部 `O_EXCL`", readme)
        self.assertIn("不覆盖旧代", readme)
        self.assertIn("trading_review.sqlite3", readme)
        self.assertNotIn(
            "> /home/trading-example/binance-trading-backups/old-commit.txt",
            readme,
        )
        self.assertNotIn(
            ".backup '/home/trading-example/binance-trading-backups/trading_review.sqlite3'",
            readme,
        )

    def test_effective_file_paths_are_attested_without_printing_secrets(self):
        readme = (ROOT / "docs" / "LEGACY_ENGINEERING.md").read_text(encoding="utf-8")

        for variable in (
            "STATE_FILE",
            "DRY_RUN_ACCOUNT_FILE",
            "LOG_FILE",
            "REVIEW_DB_FILE",
            "INSTANCE_LOCK_FILE",
        ):
            self.assertIn(variable, readme)
        for directory in (
            "/home/trading-example/binance-trading-bot/shared/state",
            "/home/trading-example/binance-trading-bot/shared/data",
            "/home/trading-example/binance-trading-bot/shared/logs",
        ):
            self.assertIn(directory, readme)
        self.assertIn(
            "/home/trading-example/binance-trading-bot/shared/state/"
            "n16_claim_ledger.sqlite3",
            readme,
        )
        self.assertIn("effective_path_preflight=PASS", readme)
        self.assertIn("effective_path_preflight=NO-GO", readme)
        self.assertIn("逐级父链", readme)
        self.assertIn("st_nlink == 1", readme)
        self.assertIn("不能输出 `.env` 内容、秘密或配置值", readme)
        self.assertIn('test -f "$env_file" || exit 1', readme)
        self.assertIn('test ! -L "$env_file" || exit 1', readme)
        self.assertIn('stat -c %h -- "$env_file"', readme)
        self.assertIn('stat -c %a -- "$env_file"', readme)
        self.assertIn("EnvironmentFiles", readme)
        self.assertIn(
            "/home/trading-example/binance-trading-bot/shared/.env "
            "(ignore_errors=no)",
            readme,
        )
        self.assertIn("unit 的 `Environment` 中没有五个路径变量的覆盖", readme)
        self.assertIn("否则手工加载 shared `.env` 不能声称等于服务的 effective 配置", readme)
        self.assertIn(
            'case "$unit_environment" in *"$variable="*) exit 1 ;; esac',
            readme,
        )
        self.assertNotIn(
            'case " $unit_environment " in *" $variable="*)', readme
        )
        self.assertLess(
            readme.index("effective_path_preflight=PASS"),
            readme.index("systemctl start binance-trading-bot.service"),
        )

    def test_stop_and_start_gates_prove_unit_database_and_lock_state(self):
        readme = (ROOT / "docs" / "LEGACY_ENGINEERING.md").read_text(encoding="utf-8")

        for field in (
            "ActiveState",
            "SubState",
            "MainPID",
            "ControlGroup",
            "cgroup.procs",
            "NRestarts",
        ):
            self.assertIn(field, readme)
        self.assertIn('test "$active_state" = "inactive" || exit 1', readme)
        self.assertIn('test "$sub_state" = "dead" || exit 1', readme)
        self.assertIn('test "$main_pid" = "0" || exit 1', readme)
        self.assertIn('test -n "$control_group" || exit 1', readme)
        self.assertIn(
            'cgroup_procs="/sys/fs/cgroup${control_group}/cgroup.procs"',
            readme,
        )
        self.assertIn('test -f "$cgroup_procs" || exit 1', readme)
        self.assertIn('test ! -s "$cgroup_procs" || exit 1', readme)
        self.assertNotIn('if test -n "$control_group"', readme)
        self.assertIn("command -v lsof", readme)
        self.assertIn("取得 `flock -n` 后", readme)
        self.assertIn("python3 -m trading_bot.release_backup", readme)
        self.assertIn('test "$active_state" = "active" || exit 1', readme)
        self.assertIn('test "$sub_state" = "running" || exit 1', readme)
        self.assertIn('test "$main_pid" -gt 0 || exit 1', readme)
        self.assertIn('test "$nrestarts_after" = "$nrestarts_before" || exit 1', readme)
        self.assertIn("不依赖未启用的 `set -e`", readme)
        self.assertIn("umask 077 || exit 1", readme)

    def test_formal_maintenance_uses_only_two_exact_binance_roots(self):
        readme = (ROOT / "docs" / "LEGACY_ENGINEERING.md").read_text(encoding="utf-8")

        self.assertIn(
            "正式生产编排只能逐字传入 "
            "`/home/trading-example/binance-trading-bot` 与 "
            "`/home/trading-example/binance-trading-backups`",
            readme,
        )
        self.assertIn(
            "--binance-root /home/trading-example/binance-trading-bot", readme
        )
        self.assertIn(
            "--binance-root /home/trading-example/binance-trading-backups", readme
        )
        self.assertNotIn("--binance-root /absolute/", readme)
        self.assertIn("禁止通配", readme)
        self.assertIn("禁止把变量展开成更宽的 root", readme)


if __name__ == "__main__":
    unittest.main()
