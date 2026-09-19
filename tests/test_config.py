import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from trading_bot.config import Config, _bool_env, load_config


class ConfigTests(unittest.TestCase):
    def test_bool_env_accepts_only_all_explicit_true_and_false_tokens(self):
        for name in ("BINANCE_DRY_RUN", "MULTI_STRATEGY_ENABLED"):
            for value in ("1", "true", "yes", "on", " TRUE ", "YeS"):
                with self.subTest(name=name, value=value):
                    with patch.dict(os.environ, {name: value}, clear=True):
                        self.assertTrue(_bool_env(name, False))
            for value in ("0", "false", "no", "off", " FALSE ", "OfF"):
                with self.subTest(name=name, value=value):
                    with patch.dict(os.environ, {name: value}, clear=True):
                        self.assertFalse(_bool_env(name, True))

    def test_bool_env_uses_default_only_when_variable_is_absent(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(_bool_env("BINANCE_DRY_RUN", True))
            self.assertFalse(_bool_env("MULTI_STRATEGY_ENABLED", False))

    def test_bool_env_rejects_empty_unknown_and_misspelled_values_with_name(self):
        for name in ("BINANCE_DRY_RUN", "MULTI_STRATEGY_ENABLED"):
            for value in ("", "   ", "ture", "truthy", "2"):
                with self.subTest(name=name, value=value):
                    with patch.dict(os.environ, {name: value}, clear=True):
                        with self.assertRaisesRegex(ValueError, name):
                            _bool_env(name, True)

    def test_misspelled_dry_run_value_cannot_silently_enable_live_mode(self):
        with patch("trading_bot.config._load_env_file"), patch.dict(
            os.environ,
            {"BINANCE_DRY_RUN": "ture"},
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "BINANCE_DRY_RUN"):
                load_config()

    def test_multi_strategy_is_disabled_by_default(self):
        self.assertFalse(Config.__dataclass_fields__["multi_strategy_enabled"].default)

        with patch("trading_bot.config._load_env_file"), patch.dict(os.environ, {}, clear=True):
            config = load_config()

        self.assertFalse(config.multi_strategy_enabled)

    def test_multi_strategy_can_be_explicitly_enabled(self):
        with patch("trading_bot.config._load_env_file"), patch.dict(
            os.environ,
            {"MULTI_STRATEGY_ENABLED": "true"},
            clear=True,
        ):
            config = load_config()

        self.assertTrue(config.multi_strategy_enabled)

    def test_multi_strategy_rejects_121_for_n09_boundary(self):
        with patch("trading_bot.config._load_env_file"), patch.dict(
            os.environ,
            {"MULTI_STRATEGY_ENABLED": "true", "KLINE_LIMIT": "121"},
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "at least 122"):
                load_config()

    def test_multi_strategy_accepts_122_while_single_strategy_keeps_old_limit(self):
        with patch("trading_bot.config._load_env_file"), patch.dict(
            os.environ,
            {"MULTI_STRATEGY_ENABLED": "true", "KLINE_LIMIT": "122"},
            clear=True,
        ):
            multi_config = load_config()
        with patch("trading_bot.config._load_env_file"), patch.dict(
            os.environ,
            {"MULTI_STRATEGY_ENABLED": "false", "KLINE_LIMIT": "100"},
            clear=True,
        ):
            single_config = load_config()

        self.assertEqual(multi_config.kline_limit, 122)
        self.assertEqual(single_config.kline_limit, 100)

    def test_poll_interval_must_be_positive(self):
        for value in ("0", "-1"):
            with self.subTest(value=value), patch(
                "trading_bot.config._load_env_file"
            ), patch.dict(
                os.environ,
                {"POLL_INTERVAL_SECONDS": value},
                clear=True,
            ):
                with self.assertRaisesRegex(ValueError, "POLL_INTERVAL_SECONDS"):
                    load_config()

    def test_kline_interval_must_be_exactly_15m(self):
        for value in ("5m", "15M", " 15m", "15m "):
            with self.subTest(value=value), patch(
                "trading_bot.config._load_env_file"
            ), patch.dict(os.environ, {"KLINE_INTERVAL": value}, clear=True):
                with self.assertRaisesRegex(ValueError, "KLINE_INTERVAL"):
                    load_config()

    def test_runtime_paths_are_absolute_and_relative_values_anchor_to_project_root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir) / "project"
            project_root.mkdir()
            absolute_dir = Path(tmpdir) / "absolute"
            values = {
                "STATE_FILE": "relative/state.json",
                "DRY_RUN_ACCOUNT_FILE": str(absolute_dir / "account.json"),
                "LOG_FILE": "relative/trading.log",
                "REVIEW_DB_FILE": str(absolute_dir / "review.sqlite3"),
                "INSTANCE_LOCK_FILE": "relative/trading_bot.lock",
            }
            with patch("trading_bot.config._load_env_file"), patch(
                "trading_bot.config.PROJECT_ROOT", project_root
            ), patch.dict(os.environ, values, clear=True):
                config = load_config()

            self.assertEqual(config.state_file, str((project_root / "relative/state.json").resolve()))
            self.assertEqual(config.dry_run_account_file, str((absolute_dir / "account.json").resolve()))
            self.assertEqual(config.log_file, str((project_root / "relative/trading.log").resolve()))
            self.assertEqual(config.review_db_file, str((absolute_dir / "review.sqlite3").resolve()))
            self.assertEqual(
                config.instance_lock_file,
                str((project_root / "relative/trading_bot.lock").resolve()),
            )
            self.assertEqual(
                config.n16_claim_ledger_file,
                str(
                    (project_root / "relative/n16_claim_ledger.sqlite3").resolve()
                ),
            )
            for path in (
                config.state_file,
                config.dry_run_account_file,
                config.log_file,
                config.review_db_file,
                config.instance_lock_file,
            ):
                self.assertTrue(Path(path).is_absolute())

    def test_runtime_persistence_paths_must_be_pairwise_distinct(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = root / "n16_claim_ledger.sqlite3"
            values = {
                "STATE_FILE": str(state),
                "DRY_RUN_ACCOUNT_FILE": str(root / "account.json"),
                "LOG_FILE": str(root / "trading.log"),
                "REVIEW_DB_FILE": str(root / "review.sqlite3"),
                "INSTANCE_LOCK_FILE": str(root / "bot.lock"),
            }
            with patch("trading_bot.config._load_env_file"), patch.dict(
                os.environ, values, clear=True
            ), self.assertRaisesRegex(
                ValueError,
                "STATE_FILE conflicts with DERIVED_N16_CLAIM_LEDGER",
            ):
                load_config()

    def test_existing_runtime_persistence_hardlinks_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            review = root / "review.sqlite3"
            lock = root / "bot.lock"
            review.write_bytes(b"application-owned-sentinel")
            os.link(review, lock)
            values = {
                "STATE_FILE": str(root / "position.json"),
                "DRY_RUN_ACCOUNT_FILE": str(root / "account.json"),
                "LOG_FILE": str(root / "trading.log"),
                "REVIEW_DB_FILE": str(review),
                "INSTANCE_LOCK_FILE": str(lock),
            }
            with patch("trading_bot.config._load_env_file"), patch.dict(
                os.environ, values, clear=True
            ), self.assertRaisesRegex(
                ValueError,
                "REVIEW_DB_FILE conflicts with INSTANCE_LOCK_FILE",
            ):
                load_config()
            self.assertEqual(review.read_bytes(), b"application-owned-sentinel")

    def test_default_env_and_runtime_paths_are_project_anchored_from_temporary_cwd(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            project_root = root / "project"
            temporary_cwd = root / "elsewhere"
            project_root.mkdir()
            temporary_cwd.mkdir()
            (project_root / ".env").write_text(
                "POLL_INTERVAL_SECONDS=17\n",
                encoding="utf-8",
            )
            (temporary_cwd / ".env").write_text(
                "POLL_INTERVAL_SECONDS=0\n",
                encoding="utf-8",
            )
            original_cwd = Path.cwd()
            try:
                os.chdir(temporary_cwd)
                with patch("trading_bot.config.PROJECT_ROOT", project_root), patch.dict(
                    os.environ, {}, clear=True
                ):
                    config = load_config()
            finally:
                os.chdir(original_cwd)

            self.assertEqual(config.poll_interval_seconds, 17)
            self.assertEqual(config.state_file, str((project_root / "state/position.json").resolve()))
            self.assertEqual(
                config.dry_run_account_file,
                str((project_root / "state/dry_run_account.json").resolve()),
            )
            self.assertEqual(config.log_file, str((project_root / "logs/trading.log").resolve()))
            self.assertEqual(
                config.review_db_file,
                str((project_root / "data/trading_review.sqlite3").resolve()),
            )
            self.assertEqual(
                config.instance_lock_file,
                str((project_root / "state/trading_bot.lock").resolve()),
            )
            self.assertEqual(
                config.n16_claim_ledger_file,
                str(
                    (project_root / "state/n16_claim_ledger.sqlite3").resolve()
                ),
            )


if __name__ == "__main__":
    unittest.main()
