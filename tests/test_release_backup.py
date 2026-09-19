from contextlib import closing, contextmanager
import hashlib
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import trading_bot.release_backup as release_backup_module
from trading_bot.release_backup import ReleaseBackupError, create_release_backup


def file_fingerprint(path: Path):
    details = path.lstat()
    return (
        hashlib.sha256(path.read_bytes()).hexdigest(),
        int(details.st_dev),
        int(details.st_ino),
        int(details.st_nlink),
        int(details.st_size),
    )


def tree_fingerprint(root: Path):
    rows = []
    for path in sorted(root.rglob("*"), key=lambda item: str(item)):
        details = path.lstat()
        relative = str(path.relative_to(root))
        if path.is_symlink():
            payload = ("symlink", os.readlink(path))
        elif path.is_file():
            payload = ("file", hashlib.sha256(path.read_bytes()).hexdigest())
        else:
            payload = ("directory", "")
        rows.append(
            (
                relative,
                payload,
                int(details.st_dev),
                int(details.st_ino),
                int(details.st_nlink),
                int(details.st_mode),
                int(details.st_size),
            )
        )
    return tuple(rows)


def advance_schema_version(
    connection: sqlite3.Connection,
    target: int,
) -> None:
    current = connection.execute(
        "PRAGMA schema_version"
    ).fetchone()[0]
    if type(current) is not int or current <= 0 or target < current:
        raise AssertionError("invalid schema version fixture")
    for value in range(current, target):
        connection.execute(
            "CREATE TABLE release_schema_cookie_%d(value INTEGER)"
            % value
        )
    connection.commit()
    if connection.execute(
        "PRAGMA schema_version"
    ).fetchone() != (target,):
        raise AssertionError("schema version fixture did not advance")


class ReleaseBackupTests(unittest.TestCase):
    def make_fixture(
        self,
        root: str,
        *,
        with_ledger: bool,
        app_root: Path = None,
        backup_root: Path = None,
    ):
        base = Path(root)
        app_root = app_root or (base / "binance-app")
        backup_root = backup_root or (base / "binance-backups")
        shared = app_root / "shared"
        state = shared / "state"
        data = shared / "data"
        logs = shared / "logs"
        current = app_root / "current"
        for directory in (backup_root, state, data, logs, current):
            directory.mkdir(parents=True, mode=0o700)
        git_no_auto_maintenance = [
            "-c",
            "maintenance.auto=false",
            "-c",
            "maintenance.autoDetach=false",
            "-c",
            "gc.auto=0",
            "-c",
            "gc.autoDetach=false",
        ]
        subprocess.run(
            ["git", *git_no_auto_maintenance, "init", "-q", str(current)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for key, value in (
            ("maintenance.auto", "false"),
            ("maintenance.autoDetach", "false"),
            ("gc.auto", "0"),
            ("gc.autoDetach", "false"),
        ):
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(current),
                    "config",
                    "--local",
                    key,
                    value,
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        subprocess.run(
            [
                "git",
                "-C",
                str(current),
                *git_no_auto_maintenance,
                "-c",
                "user.name=Release Test",
                "-c",
                "user.email=release@example.invalid",
                "commit",
                "--allow-empty",
                "-q",
                "-m",
                "fixture",
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        env_file = shared / ".env"
        env_file.write_text("SECRET=not-printed\n", encoding="utf-8")
        os.chmod(env_file, 0o600)
        state_file = state / "position.json"
        state_file.write_text("{}\n", encoding="utf-8")
        account_file = state / "dry_run_account.json"
        account_file.write_text('{"balance":"1000"}\n', encoding="utf-8")
        lock_file = state / "trading_bot.lock"
        lock_file.write_text("\n", encoding="utf-8")
        log_file = logs / "trading.log"
        log_file.write_text("safe\n", encoding="utf-8")
        review_db = data / "trading_review.sqlite3"
        with closing(sqlite3.connect(review_db)) as connection:
            connection.execute(
                "CREATE TABLE release_fixture(id INTEGER PRIMARY KEY, value TEXT)"
            )
            connection.execute("INSERT INTO release_fixture VALUES(1,'review')")
            connection.commit()
        claim_ledger = state / "n16_claim_ledger.sqlite3"
        if with_ledger:
            with closing(sqlite3.connect(claim_ledger)) as connection:
                connection.execute(
                    "CREATE TABLE ledger_fixture(id INTEGER PRIMARY KEY, value TEXT)"
                )
                connection.execute("INSERT INTO ledger_fixture VALUES(1,'ledger')")
                connection.commit()
        return {
            "app_root": app_root,
            "backup_root": backup_root,
            "current_dir": current,
            "env_file": env_file,
            "state_dir": state,
            "state_file": state_file,
            "dry_run_account_file": account_file,
            "log_file": log_file,
            "review_db": review_db,
            "lock_file": lock_file,
            "claim_ledger": claim_ledger,
        }

    def run_backup(self, fixture, **kwargs):
        with patch(
            "trading_bot.release_backup._assert_no_openers",
            return_value=None,
        ):
            return create_release_backup(**fixture, **kwargs)

    def test_official_lock_contract_rejects_before_release_scope_or_sqlite(self):
        for mode in ("wrong_name", "missing", "wrong_parent", "alias"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                fixture = self.make_fixture(root, with_ledger=True)
                original_lock = fixture["lock_file"]
                external = Path(root) / "external-lock"
                external.write_bytes(b"external-lock-sentinel")
                if mode == "wrong_name":
                    candidate = original_lock.with_name("alternate.lock")
                    original_lock.rename(candidate)
                    fixture["lock_file"] = candidate
                elif mode == "missing":
                    original_lock.unlink()
                elif mode == "wrong_parent":
                    other = Path(root) / "other" / "state"
                    other.mkdir(parents=True)
                    candidate = other / "trading_bot.lock"
                    candidate.write_bytes(b"other")
                    fixture["lock_file"] = candidate
                else:
                    original_lock.unlink()
                    os.link(external, original_lock)
                external_before = external.read_bytes()
                with patch(
                    "trading_bot.release_backup._capture_release_proof",
                    side_effect=AssertionError("release scope must not run"),
                ) as capture, patch(
                    "trading_bot.release_backup.sqlite3.connect",
                    side_effect=AssertionError("SQLite must not open"),
                ) as connect:
                    with self.assertRaises(ReleaseBackupError):
                        create_release_backup(**fixture)
                capture.assert_not_called()
                connect.assert_not_called()
                self.assertEqual(external.read_bytes(), external_before)
                self.assertEqual(list(fixture["backup_root"].iterdir()), [])

    def test_missing_ledger_stays_absent_and_records_one_generation_marker(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self.make_fixture(root, with_ledger=False)
            opener_checks = []
            with patch(
                "trading_bot.release_backup._assert_no_openers",
                side_effect=lambda paths: opener_checks.append(tuple(paths)),
            ):
                backup = create_release_backup(**fixture)

            self.assertFalse(fixture["claim_ledger"].exists())
            self.assertTrue(opener_checks)
            self.assertTrue(
                all(
                    fixture["claim_ledger"] not in paths
                    for paths in opener_checks
                )
            )
            self.assertEqual(
                (backup / "n16_claim_ledger.absent").read_text(
                    encoding="ascii"
                ),
                "ABSENT_PRE_N16\n",
            )
            self.assertFalse((backup / "n16_claim_ledger.sqlite3").exists())
            self.assertTrue((backup / "trading_review.sqlite3").is_file())
            self.assertEqual(stat_mode(backup), 0o700)
            manifest = (backup / "release_backup_manifest.json").read_text(
                encoding="utf-8"
            )
            self.assertIn('"ledger":"ABSENT_PRE_N16"', manifest)

    def test_existing_ledger_is_backed_up_without_an_absence_marker(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self.make_fixture(root, with_ledger=True)
            review_identity = file_fingerprint(fixture["review_db"])[1:4]
            ledger_identity = file_fingerprint(fixture["claim_ledger"])[1:4]
            backup = self.run_backup(fixture)

            self.assertEqual(
                file_fingerprint(fixture["review_db"])[1:4], review_identity
            )
            self.assertEqual(
                file_fingerprint(fixture["claim_ledger"])[1:4], ledger_identity
            )
            self.assertTrue((backup / "n16_claim_ledger.sqlite3").is_file())
            self.assertFalse((backup / "n16_claim_ledger.absent").exists())
            with closing(
                sqlite3.connect(backup / "n16_claim_ledger.sqlite3")
            ) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT value FROM ledger_fixture WHERE id=1"
                    ).fetchone(),
                    ("ledger",),
                )

    def test_sqlite_backups_preserve_schema_cookie_and_database_metadata(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self.make_fixture(root, with_ledger=True)
            expectations = (
                (
                    fixture["review_db"],
                    "trading_review.sqlite3",
                    74,
                    41,
                    1_234_567,
                    ("review",),
                    "SELECT value FROM release_fixture WHERE id=1",
                ),
                (
                    fixture["claim_ledger"],
                    "n16_claim_ledger.sqlite3",
                    83,
                    43,
                    7_654_321,
                    ("ledger",),
                    "SELECT value FROM ledger_fixture WHERE id=1",
                ),
            )
            for (
                source,
                _target_name,
                schema_version,
                user_version,
                application_id,
                _data,
                _query,
            ) in expectations:
                with closing(sqlite3.connect(source)) as connection:
                    connection.execute(
                        "PRAGMA user_version=%d" % user_version
                    )
                    connection.execute(
                        "PRAGMA application_id=%d" % application_id
                    )
                    connection.commit()
                    advance_schema_version(
                        connection, schema_version
                    )
            source_fingerprints = {
                source: file_fingerprint(source)
                for source, *_unused in expectations
            }

            backup = self.run_backup(fixture)

            for (
                source,
                target_name,
                schema_version,
                user_version,
                application_id,
                data,
                query,
            ) in expectations:
                self.assertEqual(
                    file_fingerprint(source),
                    source_fingerprints[source],
                )
                for suffix in ("-wal", "-shm", "-journal"):
                    self.assertFalse(
                        Path(str(source) + suffix).exists()
                    )
                    self.assertFalse(
                        Path(str(backup / target_name) + suffix).exists()
                    )
                with closing(
                    sqlite3.connect(backup / target_name)
                ) as connection:
                    self.assertEqual(
                        connection.execute(
                            "PRAGMA schema_version"
                        ).fetchone(),
                        (schema_version,),
                    )
                    self.assertEqual(
                        connection.execute(
                            "PRAGMA user_version"
                        ).fetchone(),
                        (user_version,),
                    )
                    self.assertEqual(
                        connection.execute(
                            "PRAGMA application_id"
                        ).fetchone(),
                        (application_id,),
                    )
                    self.assertEqual(
                        connection.execute(query).fetchone(),
                        data,
                    )
                    self.assertEqual(
                        connection.execute(
                            "PRAGMA integrity_check"
                        ).fetchone(),
                        ("ok",),
                    )
                    self.assertEqual(
                        connection.execute(
                            "PRAGMA foreign_key_check"
                        ).fetchall(),
                        [],
                    )

    def test_schema_cookie_failures_remove_private_backup_without_source_change(self):
        failures = (
            (
                "_write_backup_schema_version",
                ReleaseBackupError("forced schema restore failure"),
            ),
            (
                "_commit_backup_schema_version",
                sqlite3.OperationalError(
                    "forced schema commit failure"
                ),
            ),
            (
                "_verify_backup_schema_version",
                ReleaseBackupError("forced schema verify failure"),
            ),
        )
        for helper, failure in failures:
            with self.subTest(helper=helper):
                with tempfile.TemporaryDirectory() as root:
                    fixture = self.make_fixture(root, with_ledger=True)
                    with closing(
                        sqlite3.connect(fixture["review_db"])
                    ) as connection:
                        advance_schema_version(connection, 74)
                    source_fingerprint = file_fingerprint(
                        fixture["review_db"]
                    )
                    with patch(
                        "trading_bot.release_backup.%s" % helper,
                        side_effect=failure,
                    ):
                        with self.assertRaises(ReleaseBackupError):
                            self.run_backup(fixture)

                    self.assertEqual(
                        file_fingerprint(fixture["review_db"]),
                        source_fingerprint,
                    )
                    with closing(
                        sqlite3.connect(fixture["review_db"])
                    ) as connection:
                        self.assertEqual(
                            connection.execute(
                                "PRAGMA schema_version"
                            ).fetchone(),
                            (74,),
                        )
                    self.assertEqual(
                        list(fixture["backup_root"].iterdir()), []
                    )
                    for suffix in ("-wal", "-shm", "-journal"):
                        self.assertFalse(
                            Path(
                                str(fixture["review_db"]) + suffix
                            ).exists()
                        )

    def test_target_connection_close_exception_removes_private_backup(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self.make_fixture(root, with_ledger=True)
            with closing(
                sqlite3.connect(fixture["review_db"])
            ) as connection:
                advance_schema_version(connection, 74)
            source_fingerprint = file_fingerprint(
                fixture["review_db"]
            )
            original = (
                release_backup_module
                ._attested_maintenance_connection
            )
            open_count = 0

            @contextmanager
            def close_failure(opener, expected_identity):
                nonlocal open_count
                open_count += 1
                current = open_count
                with original(
                    opener, expected_identity
                ) as connection:
                    yield connection
                if current == 2:
                    raise OSError(
                        "forced target close acknowledgement failure"
                    )

            with patch(
                "trading_bot.release_backup."
                "_attested_maintenance_connection",
                new=close_failure,
            ):
                with self.assertRaises(ReleaseBackupError):
                    self.run_backup(fixture)

            self.assertEqual(
                file_fingerprint(fixture["review_db"]),
                source_fingerprint,
            )
            self.assertEqual(
                list(fixture["backup_root"].iterdir()), []
            )
            for suffix in ("-wal", "-shm", "-journal"):
                self.assertFalse(
                    Path(
                        str(fixture["review_db"]) + suffix
                    ).exists()
                )

    def test_absent_ledger_appearing_before_lock_is_zero_write_no_go(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self.make_fixture(root, with_ledger=False)
            appeared = {}

            def appear():
                with closing(sqlite3.connect(fixture["claim_ledger"])) as connection:
                    connection.execute(
                        "CREATE TABLE external_sentinel(value TEXT NOT NULL)"
                    )
                    connection.execute(
                        "INSERT INTO external_sentinel VALUES('preserve')"
                    )
                    connection.commit()
                appeared["fingerprint"] = file_fingerprint(
                    fixture["claim_ledger"]
                )

            with self.assertRaisesRegex(
                ReleaseBackupError, "scope changed"
            ):
                self.run_backup(fixture, before_lock_hook=appear)
            self.assertEqual(
                file_fingerprint(fixture["claim_ledger"]),
                appeared["fingerprint"],
            )
            self.assertEqual(list(fixture["backup_root"].iterdir()), [])

    def test_existing_ledger_missing_or_replaced_before_lock_is_zero_write(self):
        for mode in ("missing", "replacement"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                fixture = self.make_fixture(root, with_ledger=True)
                held = Path(root) / "held-ledger.sqlite3"
                evidence = {}

                def change():
                    fixture["claim_ledger"].rename(held)
                    evidence["held"] = file_fingerprint(held)
                    if mode == "replacement":
                        with closing(
                            sqlite3.connect(fixture["claim_ledger"])
                        ) as connection:
                            connection.execute(
                                "CREATE TABLE replacement(value TEXT NOT NULL)"
                            )
                            connection.execute(
                                "INSERT INTO replacement VALUES('external')"
                            )
                            connection.commit()
                        evidence["replacement"] = file_fingerprint(
                            fixture["claim_ledger"]
                        )

                with self.assertRaisesRegex(
                    ReleaseBackupError, "scope changed"
                ):
                    self.run_backup(fixture, before_lock_hook=change)
                self.assertEqual(file_fingerprint(held), evidence["held"])
                if mode == "missing":
                    self.assertFalse(fixture["claim_ledger"].exists())
                else:
                    self.assertEqual(
                        file_fingerprint(fixture["claim_ledger"]),
                        evidence["replacement"],
                    )
                self.assertEqual(list(fixture["backup_root"].iterdir()), [])

    def assert_overlap_is_zero_write(self, fixture, observed_root: Path):
        before = tree_fingerprint(observed_root)
        with patch(
            "trading_bot.release_backup._backup_sqlite",
            side_effect=AssertionError("SQLite backup must not be reached"),
        ) as sqlite_backup:
            with self.assertRaisesRegex(ReleaseBackupError, "overlaps"):
                self.run_backup(fixture)
        sqlite_backup.assert_not_called()
        self.assertEqual(tree_fingerprint(observed_root), before)
        self.assertFalse(
            any(
                path.name.startswith("release-")
                for path in observed_root.rglob("*")
            )
        )

    def test_backup_root_rejects_every_runtime_tree_before_any_side_effect(self):
        cases = ("state_dir", "current_dir", "data", "logs")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as root:
                fixture = self.make_fixture(root, with_ledger=True)
                if case == "data":
                    overlap = fixture["review_db"].parent
                elif case == "logs":
                    overlap = fixture["log_file"].parent
                else:
                    overlap = fixture[case]
                fixture["backup_root"] = overlap
                self.assert_overlap_is_zero_write(fixture, Path(root))

    def test_backup_root_rejects_any_application_descendant(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self.make_fixture(root, with_ledger=True)
            overlap = fixture["app_root"] / "nested" / "backups"
            overlap.mkdir(parents=True, mode=0o700)
            fixture["backup_root"] = overlap
            self.assert_overlap_is_zero_write(fixture, Path(root))

    def test_application_root_rejects_being_below_backup_root(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            backup_root = base / "binance-backups"
            fixture = self.make_fixture(
                root,
                with_ledger=True,
                app_root=backup_root / "nested-app",
                backup_root=backup_root,
            )
            self.assert_overlap_is_zero_write(fixture, base)

    def test_backup_root_symlink_alias_is_rejected_without_side_effects(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self.make_fixture(root, with_ledger=True)
            alias = Path(root) / "backup-alias"
            alias.symlink_to(fixture["backup_root"], target_is_directory=True)
            fixture["backup_root"] = alias
            before = tree_fingerprint(Path(root))
            with self.assertRaises(ReleaseBackupError):
                self.run_backup(fixture)
            self.assertEqual(tree_fingerprint(Path(root)), before)
            self.assertFalse(
                any(
                    path.name.startswith("release-")
                    for path in Path(root).rglob("*")
                )
            )


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


if __name__ == "__main__":
    unittest.main()
