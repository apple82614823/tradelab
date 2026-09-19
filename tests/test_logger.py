import io
import logging
import os
import tempfile
import unittest
from contextlib import redirect_stderr
from datetime import date, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from unittest.mock import patch

from trading_bot.logger import APPLICATION_LOG_RETENTION_DAYS, setup_logging


class _TrackingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1
        super().close()


class ApplicationLoggerTests(unittest.TestCase):
    @staticmethod
    def _close_handlers(logger: logging.Logger) -> None:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()

    def test_setup_uses_daily_application_file_rotation_and_console(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_path = Path(tmp_dir) / "binance-only" / "trading.log"
            logger = logging.Logger("test_trading_bot_daily_rotation")
            self.addCleanup(self._close_handlers, logger)
            console_output = io.StringIO()

            with redirect_stderr(console_output), patch(
                "trading_bot.logger.logging.getLogger", return_value=logger
            ):
                configured = setup_logging(str(log_path))
                configured.info("application-only-log")

            self.assertIs(configured, logger)
            self.assertFalse(logger.propagate)
            self.assertEqual(logging.INFO, logger.level)

            file_handlers = [
                handler
                for handler in logger.handlers
                if isinstance(handler, TimedRotatingFileHandler)
            ]
            console_handlers = [
                handler
                for handler in logger.handlers
                if type(handler) is logging.StreamHandler
            ]
            self.assertEqual(1, len(file_handlers))
            self.assertEqual(1, len(console_handlers))

            file_handler = file_handlers[0]
            file_handler.flush()
            self.assertEqual("MIDNIGHT", file_handler.when)
            self.assertEqual(24 * 60 * 60, file_handler.interval)
            self.assertEqual(APPLICATION_LOG_RETENTION_DAYS, file_handler.backupCount)
            self.assertFalse(file_handler.utc)
            self.assertEqual(str(log_path), file_handler.baseFilename)
            self.assertIn("application-only-log", log_path.read_text(encoding="utf-8"))
            self.assertIn("application-only-log", console_output.getvalue())

    def test_reconfiguration_explicitly_closes_every_old_handler(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            old_log_path = Path(tmp_dir) / "old.log"
            new_log_path = Path(tmp_dir) / "new.log"
            logger = logging.Logger("test_trading_bot_handler_close")
            old_file_handler = logging.FileHandler(old_log_path, encoding="utf-8")
            old_tracking_handler = _TrackingHandler()
            logger.addHandler(old_file_handler)
            logger.addHandler(old_tracking_handler)
            self.addCleanup(self._close_handlers, logger)

            with redirect_stderr(io.StringIO()), patch(
                "trading_bot.logger.logging.getLogger", return_value=logger
            ):
                setup_logging(str(new_log_path))

            self.assertNotIn(old_file_handler, logger.handlers)
            self.assertNotIn(old_tracking_handler, logger.handlers)
            self.assertIsNone(old_file_handler.stream)
            self.assertEqual(1, old_tracking_handler.close_count)
            self.assertEqual(2, len(logger.handlers))

    def test_rotation_selects_only_expired_backups_for_this_application_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_path = Path(tmp_dir) / "trading.log"
            logger = logging.Logger("test_trading_bot_backup_cleanup")
            self.addCleanup(self._close_handlers, logger)

            with redirect_stderr(io.StringIO()), patch(
                "trading_bot.logger.logging.getLogger", return_value=logger
            ):
                setup_logging(str(log_path))

            file_handler = next(
                handler
                for handler in logger.handlers
                if isinstance(handler, TimedRotatingFileHandler)
            )
            today = date.today()
            expired = Path(
                f"{log_path}.{today - timedelta(days=APPLICATION_LOG_RETENTION_DAYS + 1):%Y-%m-%d}"
            )
            boundary = Path(
                f"{log_path}.{today - timedelta(days=APPLICATION_LOG_RETENTION_DAYS):%Y-%m-%d}"
            )
            recent = Path(f"{log_path}.{today - timedelta(days=1):%Y-%m-%d}")
            for backup in (expired, boundary, recent):
                backup.write_text("application-backup", encoding="utf-8")
            unrelated = Path(tmp_dir) / "other-service.log.2025-01-01"
            unrelated.write_text("must-not-be-managed", encoding="utf-8")

            selected = [Path(path) for path in file_handler.getFilesToDelete()]

            self.assertEqual([expired], selected)
            self.assertNotIn(boundary, selected)
            self.assertNotIn(recent, selected)
            self.assertNotIn(unrelated, selected)

    def test_rotation_count_never_deletes_exact_fifteen_day_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_path = Path(tmp_dir) / "trading.log"
            handler = TimedRotatingFileHandler
            logger = logging.Logger("test_trading_bot_strict_age_cleanup")
            self.addCleanup(self._close_handlers, logger)

            with redirect_stderr(io.StringIO()), patch(
                "trading_bot.logger.logging.getLogger", return_value=logger
            ):
                setup_logging(str(log_path))

            file_handler = next(
                item
                for item in logger.handlers
                if isinstance(item, handler)
            )
            today = date.today()
            backups = []
            for age in range(APPLICATION_LOG_RETENTION_DAYS + 2):
                backup = Path(
                    f"{log_path}.{today - timedelta(days=age):%Y-%m-%d}"
                )
                backup.write_text("application-backup", encoding="utf-8")
                backups.append(backup)

            selected = {Path(path) for path in file_handler.getFilesToDelete()}

            self.assertEqual(selected, {backups[APPLICATION_LOG_RETENTION_DAYS + 1]})
            self.assertNotIn(backups[APPLICATION_LOG_RETENTION_DAYS], selected)

    def test_rotation_rejects_links_directories_and_multi_link_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_path = Path(tmp_dir) / "trading.log"
            logger = logging.Logger("test_trading_bot_rotation_file_types")
            self.addCleanup(self._close_handlers, logger)

            with redirect_stderr(io.StringIO()), patch(
                "trading_bot.logger.logging.getLogger", return_value=logger
            ):
                setup_logging(str(log_path))

            file_handler = next(
                item
                for item in logger.handlers
                if isinstance(item, TimedRotatingFileHandler)
            )
            expired_date = date.today() - timedelta(
                days=APPLICATION_LOG_RETENTION_DAYS + 1
            )
            valid = Path(f"{log_path}.{expired_date:%Y-%m-%d}")
            valid.write_text("application-backup", encoding="utf-8")

            symlink = Path(f"{log_path}.{expired_date:%Y-%m-%d}.symlink")
            symlink.symlink_to(valid)
            directory = Path(f"{log_path}.{expired_date:%Y-%m-%d}.directory")
            directory.mkdir()
            hardlink_source = Path(tmp_dir) / "external-history.log"
            hardlink_source.write_text("external-history", encoding="utf-8")
            hardlink = Path(f"{log_path}.{expired_date:%Y-%m-%d}.hardlink")
            os.link(str(hardlink_source), str(hardlink))

            # Give every hostile type the exact otherwise-valid application
            # suffix in its own isolated directory so name parsing cannot be
            # the reason it is rejected.
            hostile_results = []
            for hostile in (symlink, directory, hardlink):
                isolated = Path(tmp_dir) / hostile.suffix[1:]
                isolated.mkdir()
                isolated_log = isolated / "trading.log"
                isolated_handler = type(file_handler)(
                    str(isolated_log),
                    when="midnight",
                    backupCount=APPLICATION_LOG_RETENTION_DAYS,
                )
                try:
                    exact_name = Path(
                        f"{isolated_log}.{expired_date:%Y-%m-%d}"
                    )
                    if hostile.is_symlink():
                        exact_name.symlink_to(valid)
                    elif hostile.is_dir():
                        exact_name.mkdir()
                    else:
                        os.link(str(hardlink_source), str(exact_name))
                    hostile_results.append(
                        (exact_name, isolated_handler.getFilesToDelete())
                    )
                finally:
                    isolated_handler.close()

            selected = {Path(path) for path in file_handler.getFilesToDelete()}
            self.assertEqual({valid}, selected)
            for hostile, result in hostile_results:
                self.assertNotIn(str(hostile), result)


if __name__ == "__main__":
    unittest.main()
