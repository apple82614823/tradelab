from pathlib import Path
import os
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from trading_bot.instance_lock import (
    InstanceLock,
    InstanceLockError,
    validate_official_instance_lock,
)
from trading_bot.main import TradingBot, main


class InstanceLockTests(unittest.TestCase):
    def test_official_lock_requires_exact_existing_single_link_sibling(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state = Path(tmpdir) / "shared" / "state"
            state.mkdir(parents=True)
            ledger = state / "n16_claim_ledger.sqlite3"
            ledger.write_bytes(b"ledger")
            lock_file = state / "trading_bot.lock"

            with self.assertRaisesRegex(InstanceLockError, "must already exist"):
                validate_official_instance_lock(lock_file, ledger)

            lock_file.write_bytes(b"lock")
            proof = validate_official_instance_lock(lock_file, ledger)
            self.assertEqual(proof.path, lock_file)
            self.assertEqual(
                proof.parent_identity,
                (state.stat().st_dev, state.stat().st_ino),
            )

            with self.assertRaisesRegex(InstanceLockError, "basename"):
                validate_official_instance_lock(state / "alternate.lock", ledger)

            other_state = Path(tmpdir) / "other" / "state"
            other_state.mkdir(parents=True)
            other_lock = other_state / "trading_bot.lock"
            other_lock.write_bytes(b"other")
            with self.assertRaisesRegex(InstanceLockError, "same real parent"):
                validate_official_instance_lock(other_lock, ledger)

            lock_file.unlink()
            external = Path(tmpdir) / "external.lock"
            external.write_bytes(b"external")
            os.link(external, lock_file)
            with self.assertRaisesRegex(InstanceLockError, "exactly one hard link"):
                validate_official_instance_lock(lock_file, ledger)

    def _bot_config(self, tmpdir, lock_file):
        root = Path(tmpdir)
        return SimpleNamespace(
            instance_lock_file=lock_file,
            log_file=str(root / "trading.log"),
            state_file=str(root / "state.json"),
            review_db_file=str(root / "review.sqlite3"),
            n16_claim_ledger_file=str(
                root / "n16_claim_ledger.sqlite3"
            ),
            funding_rate_abs_threshold=1,
            trend_window=96,
        )

    def _bot_dependency_patches(self, recorder_side_effect=None):
        recorder = Mock()
        return (
            patch("trading_bot.main.setup_logging", return_value=Mock()),
            patch("trading_bot.main.BinanceFuturesClient", return_value=Mock()),
            patch("trading_bot.main.StateStore", return_value=Mock()),
            patch(
                "trading_bot.main.ReviewRecorder",
                return_value=recorder,
                side_effect=recorder_side_effect,
            ),
            patch("trading_bot.main.FundingMonitor", return_value=Mock()),
            patch("trading_bot.main.Trader", return_value=Mock()),
            patch("trading_bot.main.load_active_strategies", return_value=()),
            patch("trading_bot.main.PaperTrader", return_value=Mock()),
            patch("trading_bot.main.StrategyScheduler", return_value=Mock()),
        )

    def test_independent_file_descriptors_cannot_hold_same_lock(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_file = str(Path(tmpdir) / "trading_bot.lock")
            with InstanceLock(lock_file) as first:
                self.assertTrue(first.acquired)
                second = InstanceLock(lock_file)
                with self.assertRaisesRegex(InstanceLockError, "INSTANCE_LOCK_FILE"):
                    second.acquire()
                self.assertFalse(second.acquired)

    def test_stale_unlocked_file_can_be_acquired(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_file = Path(tmpdir) / "trading_bot.lock"
            lock_file.write_text("999999\n", encoding="utf-8")

            with InstanceLock(str(lock_file)) as lock:
                self.assertTrue(lock.acquired)
                self.assertEqual(lock_file.read_text(encoding="utf-8"), f"{os.getpid()}\n")

    def test_fsync_failure_releases_flock_closes_handle_and_preserves_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_file = str(Path(tmpdir) / "trading_bot.lock")
            failure = OSError("forced fsync failure")
            first = InstanceLock(lock_file)

            with patch("trading_bot.instance_lock.os.fsync", side_effect=failure):
                with self.assertRaises(OSError) as raised:
                    first.acquire()

            self.assertIs(raised.exception, failure)
            self.assertFalse(first.acquired)
            with InstanceLock(lock_file) as second:
                self.assertTrue(second.acquired)

    def test_bot_owns_unacquired_external_lock_and_releases_it_on_close(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_file = str(Path(tmpdir) / "trading_bot.lock")
            lock = InstanceLock(lock_file)
            config = self._bot_config(tmpdir, lock_file)
            patches = self._bot_dependency_patches()
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
                bot = TradingBot(config=config, instance_lock=lock)

            self.assertTrue(lock.acquired)
            bot.close()
            self.assertFalse(lock.acquired)

    def test_bot_releases_self_acquired_external_lock_when_construction_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_file = str(Path(tmpdir) / "trading_bot.lock")
            lock = InstanceLock(lock_file)
            config = self._bot_config(tmpdir, lock_file)
            failure = RuntimeError("forced recorder construction failure")
            patches = self._bot_dependency_patches(recorder_side_effect=failure)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
                with self.assertRaises(RuntimeError) as raised:
                    TradingBot(config=config, instance_lock=lock)

            self.assertIs(raised.exception, failure)
            self.assertFalse(lock.acquired)
            with InstanceLock(lock_file) as second:
                self.assertTrue(second.acquired)

    def test_bot_does_not_release_lock_already_owned_by_external_context(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_file = str(Path(tmpdir) / "trading_bot.lock")
            config = self._bot_config(tmpdir, lock_file)
            patches = self._bot_dependency_patches()
            with InstanceLock(lock_file) as lock:
                with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
                    bot = TradingBot(config=config, instance_lock=lock)
                bot.close()
                self.assertTrue(lock.acquired)

    def test_second_process_fails_nonzero_while_first_process_holds_lock(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_file = str(Path(tmpdir) / "trading_bot.lock")
            holder_code = (
                "import sys\n"
                "from trading_bot.instance_lock import InstanceLock\n"
                "with InstanceLock(sys.argv[1]):\n"
                " print('LOCKED', flush=True)\n"
                " sys.stdin.readline()\n"
            )
            contender_code = (
                "import sys\n"
                "from trading_bot.instance_lock import InstanceLock\n"
                "with InstanceLock(sys.argv[1]):\n"
                " pass\n"
            )
            holder = subprocess.Popen(
                [sys.executable, "-c", holder_code, lock_file],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                self.assertEqual(holder.stdout.readline().strip(), "LOCKED")
                contender = subprocess.run(
                    [sys.executable, "-c", contender_code, lock_file],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertNotEqual(contender.returncode, 0)
                self.assertIn("INSTANCE_LOCK_FILE", contender.stderr)
            finally:
                if holder.stdin:
                    holder.stdin.write("\n")
                    holder.stdin.flush()
                holder.communicate(timeout=10)

    def test_main_does_not_construct_bot_when_lock_is_held(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_file = str(Path(tmpdir) / "trading_bot.lock")
            config = SimpleNamespace(instance_lock_file=lock_file)
            with InstanceLock(lock_file), patch(
                "trading_bot.main.load_config", return_value=config
            ), patch("trading_bot.main.TradingBot") as bot_class:
                with self.assertRaises(InstanceLockError):
                    main()

            bot_class.assert_not_called()


if __name__ == "__main__":
    unittest.main()
