import hashlib
import io
import json
import logging
import os
import sqlite3
import stat
import tempfile
import unittest
from copy import deepcopy
from contextlib import contextmanager, redirect_stderr
from pathlib import Path
from unittest.mock import patch
from urllib.parse import unquote, urlsplit

import trading_bot.signal_retention as signal_retention_module
from tests.recorder_test_utils import seal_test_database_catalog
from tests.test_micro_strategies import MicroObservationTests
from tests.test_n17 import legacy_live_tail_record
from trading_bot.instance_lock import InstanceLock, InstanceLockError
from trading_bot.micro_analyzer import analyze_n21
from trading_bot.micro_schema import MICRO_TRIGGER_SQL, micro_schema_status
from trading_bot.coverage_epoch_schema import (
    TRIGGER_SQL as COVERAGE_EPOCH_TRIGGER_SQL,
    coverage_epoch_chain_sha256,
    coverage_publication_receipt_sha256,
    validate_coverage_epoch_graph,
)
from trading_bot.n17_analyzer import decode_n17_state_evidence
from trading_bot.n15_snapshot import build_n15_snapshot
from trading_bot.n15_terminal_schema import (
    N15_TERMINAL_INDEX_SQL,
    N15_TERMINAL_TABLES,
    N15_TERMINAL_TRIGGER_SQL,
    n15_terminal_schema_status,
    validate_n15_terminal_graph,
)
from trading_bot.recorder import (
    HistoryCoverageProposal,
    ReviewRecorder,
    _N16_INDEX_SQL,
    _N16_PREINSTALL_EVENTS_TABLE_SQL,
)
from trading_bot.signal_retention import (
    SignalRetentionMaintenanceError,
    _backfilled_final_summary,
    _advance_protected_generation_after_explicit_maintenance,
    _database_checks,
    _delete_one_batch,
    _install_n16_claim_boundary,
    _install_n17_lifecycle_boundary,
    _install_n18_lifecycle_boundary,
    _install_n19_lifecycle_boundary,
    _install_n20_lifecycle_boundary,
    _install_coverage_epoch_boundary,
    _install_micro_lifecycle_boundary,
    _repair_n17_frozen_evidence_boundary,
    _open_database,
    _sqlite_schema_identity,
    _signal_evidence_sha256,
    _strict_absent_retention_history_is_empty,
    _upsert_audit,
    _validate_retention_dependencies,
    _validate_cli_scope,
    _validate_ledger_row,
    _validate_signal_row,
    apply_signal_retention_maintenance as _apply_signal_retention_maintenance,
    build_parser,
    inspect_signal_retention as _inspect_signal_retention,
    main as _signal_retention_main,
    vacuum_signal_database_into as _vacuum_signal_database_into,
)
from trading_bot.n16_claim_ledger import N16PermanentClaimLedger
from trading_bot.strategies import N15_STRATEGY
from trading_bot.strategy_scheduler import StrategyScheduler
from tests.test_n15 import n15_seed_drift_windows


_NOW = "2026-07-14T00:00:00+00:00"


@contextmanager
def _sqlite_connection(*args, **kwargs):
    connection = sqlite3.connect(*args, **kwargs)
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def _file_sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _file_fingerprint(path):
    details = os.lstat(str(path))
    return (
        _file_sha256(path),
        int(details.st_size),
        int(details.st_mode),
        int(details.st_mtime_ns),
        int(details.st_nlink),
        int(details.st_dev),
        int(details.st_ino),
    )


def _open_until_descriptor_is_reused(path, descriptor):
    opened = []
    for _attempt in range(256):
        replacement = os.open(str(path), os.O_RDONLY)
        opened.append(replacement)
        if replacement == descriptor:
            return opened
        if replacement > descriptor:
            raise AssertionError(
                "descriptor %d was skipped while forcing fd reuse" % descriptor
            )
    raise AssertionError("descriptor %d was not reused" % descriptor)


def _close_test_descriptors(descriptors, close_function):
    for descriptor in reversed(descriptors):
        try:
            close_function(descriptor)
        except OSError:
            pass


def _vacuum_source_evidence(path):
    path = Path(path)
    sidecars = {}
    for suffix in ("-wal", "-shm", "-journal"):
        candidate = Path(str(path) + suffix)
        sidecars[suffix] = (
            _file_fingerprint(candidate) if candidate.exists() else None
        )
    connection = sqlite3.connect(
        path.resolve().as_uri() + "?mode=ro&immutable=1", uri=True
    )
    try:
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()
    finally:
        connection.close()
    return _file_fingerprint(path), sidecars, journal_mode


def _directory_fingerprint(path):
    path = Path(path)
    details = os.lstat(str(path))
    return (
        int(details.st_mode),
        int(details.st_mtime_ns),
        int(details.st_ctime_ns),
        int(details.st_nlink),
        int(details.st_dev),
        int(details.st_ino),
        tuple(sorted(item.name for item in path.iterdir())),
    )


def _rows(connection, table):
    return connection.execute('SELECT * FROM "%s" ORDER BY rowid' % table).fetchall()


def _drop_empty_n16_schema_for_legacy_fixture(connection):
    """Turn a fresh modern test DB into a provable pre-N16 legacy fixture."""
    database_path = Path(
        connection.execute("PRAGMA database_list").fetchone()[2]
    )
    traces = connection.execute(
        "SELECT (SELECT COUNT(*) FROM strategy_definitions WHERE strategy_id='N16'), "
        "(SELECT COUNT(*) FROM strategy_signals WHERE strategy_id='N16'), "
        "(SELECT COUNT(*) FROM strategy_passed_signal_audits WHERE strategy_id='N16'), "
        "(SELECT COUNT(*) FROM strategy_passed_structure_ledger WHERE strategy_id='N16'), "
        "(SELECT COUNT(*) FROM n16_trend_support_states), "
        "(SELECT COUNT(*) FROM n16_consumption_seals), "
        "(SELECT COUNT(*) FROM n16_first_claim_witness), "
        "(SELECT COUNT(*) FROM strategy_lifecycle_installations)"
    ).fetchone()
    if traces != (0, 0, 0, 0, 0, 0, 0, 1):
        raise AssertionError("legacy fixture contains N16 history")
    triggers = connection.execute(
        "SELECT name FROM sqlite_schema WHERE type='trigger' "
        "AND name GLOB 'trg_n16_*' ORDER BY name"
    ).fetchall()
    for (name,) in triggers:
        connection.execute('DROP TRIGGER "%s"' % name)
    for index_name in _N16_INDEX_SQL:
        connection.execute('DROP INDEX IF EXISTS "%s"' % index_name)
    for table in (
        "n16_consumption_seals",
        "n16_first_claim_witness",
        "n16_lifecycle_guard",
        "n16_trend_support_states",
        "strategy_lifecycle_installations",
    ):
        connection.execute('DROP TABLE "%s"' % table)
    connection.execute("ALTER TABLE events RENAME TO events_n16_old")
    connection.execute(_N16_PREINSTALL_EVENTS_TABLE_SQL)
    connection.execute(
        "INSERT INTO events(id,occurred_at,event_type,symbol,payload_json) "
        "SELECT id,occurred_at,event_type,symbol,payload_json "
        "FROM events_n16_old ORDER BY id"
    )
    connection.execute("DROP TABLE events_n16_old")
    connection.execute(
        "CREATE INDEX idx_events_type_time "
        "ON events(event_type, occurred_at)"
    )
    connection.commit()
    ledger_path = database_path.with_name("n16_claim_ledger.sqlite3")
    for suffix in ("-wal", "-shm", "-journal", ""):
        candidate = Path(str(ledger_path) + suffix)
        if candidate.exists():
            candidate.unlink()


class SignalRetentionMaintenanceTests(unittest.TestCase):
    def test_opened_database_descriptor_requires_one_expected_inode_increment(self):
        expected = (101, 202)
        other = (303, 404)
        second_other = (505, 606)
        cases = {
            "delayed_or_missing": ({}, {}),
            "expected_not_increased": ({7: expected}, {7: expected}),
            "preexisting_expected_removed": (
                {7: expected},
                {8: expected, 9: expected},
            ),
            "ambiguous_multiple_expected": ({}, {7: expected, 8: expected}),
            "wrong_new_inode": ({}, {7: other}),
        }
        for name, (before, after) in cases.items():
            with self.subTest(name=name), patch.object(
                signal_retention_module,
                "_maintenance_regular_fd_snapshot",
                return_value=after,
            ):
                with self.assertRaisesRegex(
                    SignalRetentionMaintenanceError,
                    "descriptor is not attested",
                ):
                    signal_retention_module._validate_maintenance_opened_descriptor(
                        before,
                        expected,
                    )

        accepted = (
            ({7: other}, {7: other, 8: expected}),
            # An unrelated descriptor number may be closed and reused by the
            # expected SQLite inode during the open window.
            ({7: other}, {7: expected}),
            # Unrelated files may independently close, open, or reuse numbers;
            # an already-open expected descriptor must remain represented.
            (
                {3: expected, 7: other},
                {3: expected, 7: second_other, 8: expected, 9: other},
            ),
        )
        for before, after in accepted:
            with patch.object(
                signal_retention_module,
                "_maintenance_regular_fd_snapshot",
                return_value=after,
            ):
                signal_retention_module._validate_maintenance_opened_descriptor(
                    before,
                    expected,
                )

    @staticmethod
    def _claim_ledger(path):
        return Path(path).with_name("n16_claim_ledger.sqlite3")

    @staticmethod
    def _create_official_lock(ledger):
        lock_file = Path(ledger).with_name("trading_bot.lock")
        lock_file.write_text("\n", encoding="utf-8")
        return lock_file

    def _advance_test_protected_generation(self, path):
        ledger = N16PermanentClaimLedger(self._claim_ledger(path))
        with _sqlite_connection(path) as connection:
            _advance_protected_generation_after_explicit_maintenance(
                connection, ledger
            )

    def _inspect_raw(self, database, *args, **kwargs):
        if "n16_claim_ledger" not in kwargs:
            raise AssertionError("test must pass N16 claim ledger explicitly")
        return _inspect_signal_retention(database, *args, **kwargs)

    def _apply_raw(self, database, *args, **kwargs):
        if "n16_claim_ledger" not in kwargs:
            raise AssertionError("test must pass N16 claim ledger explicitly")
        return _apply_signal_retention_maintenance(database, *args, **kwargs)

    def _vacuum_raw(self, database, destination, *args, **kwargs):
        if "n16_claim_ledger" not in kwargs:
            raise AssertionError("test must pass N16 claim ledger explicitly")
        return _vacuum_signal_database_into(
            database, destination, *args, **kwargs
        )

    def _cli(self, argv):
        values = list(argv)
        if "--n16-claim-ledger" not in values:
            raise AssertionError("test CLI must pass N16 claim ledger explicitly")
        return _signal_retention_main(values)

    def _database(self, directory):
        path = Path(directory) / "review.sqlite3"
        with _sqlite_connection(path) as connection:
            connection.commit()
        _install_n16_claim_boundary(path, self._claim_ledger(path))
        _install_n17_lifecycle_boundary(path, self._claim_ledger(path))
        _install_n19_lifecycle_boundary(path, self._claim_ledger(path))
        _install_n18_lifecycle_boundary(path, self._claim_ledger(path))
        _install_n20_lifecycle_boundary(path, self._claim_ledger(path))
        _install_micro_lifecycle_boundary(path, self._claim_ledger(path))
        _install_coverage_epoch_boundary(path, self._claim_ledger(path))
        with _sqlite_connection(path) as connection:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute(
                "UPDATE strategy_signal_current SET current_scan_id=NULL, "
                "retention_active=0, migration_state='PENDING', "
                "migration_cutoff_signal_id=NULL, source_signal_count=NULL, "
                "source_passed_count=NULL, source_manifest_sha256=NULL, "
                "retention_origin=NULL "
                "WHERE singleton_id=1"
            )
            for scan_id in (1, 2, 3):
                connection.execute(
                    "INSERT INTO scans (id, started_at, completed_at, mode, "
                    "scanned_count, candidate_count, candidates_json, opened, note) "
                    "VALUES (?, ?, ?, 'DRY_RUN', 100, 100, '[]', 0, ?)",
                    (
                        scan_id,
                        "2026-07-14T00:0%d:00+00:00" % scan_id,
                        "2026-07-14T00:0%d:30+00:00" % scan_id,
                        "scan-%d" % scan_id,
                    ),
                )
            connection.execute(
                "INSERT INTO events (occurred_at, event_type, symbol, payload_json) "
                "VALUES (?, 'PROTECTED_EVENT', 'BTCUSDT', '{\"safe\":true}')",
                (_NOW,),
            )
            connection.execute(
                "INSERT INTO n13_rotation_states (strategy_id, symbol, t_time, "
                "structure_id, status, reason, detail_json, created_at, updated_at) "
                "VALUES ('N13', 'BTCUSDT', '1783919700000', NULL, 'INVALID', "
                "'N13_CONFIRMATION_NOT_FOUND', '{\"frozen\":true}', ?, ?)",
                (_NOW, _NOW),
            )
        return path

    def _vacuum_destination(self, directory, filename="compacted.sqlite3"):
        parent = Path(directory) / "vacuum-output"
        parent.mkdir(exist_ok=True)
        return parent / filename

    def _signal(
        self,
        path,
        scan_id,
        strategy_id,
        symbol,
        passed,
        structure_id=None,
        decision=None,
        matched_patterns=None,
    ):
        with _sqlite_connection(path) as connection:
            cursor = connection.execute(
                """
                INSERT INTO strategy_signals (
                    scan_id, strategy_id, symbol, funding_rate, matched_patterns,
                    trend_slope, current_bullish, passed, decision, reason,
                    structure_id, detail_json, created_at
                ) VALUES (?, ?, ?, '-0.01', ?, '1', 1, ?, ?, ?, ?, ?, ?)
                """,
                (
                    scan_id,
                    strategy_id,
                    symbol,
                    json.dumps([] if matched_patterns is None else matched_patterns),
                    passed,
                    decision or ("PASSED" if passed == 1 else "REJECTED"),
                    "PASSED" if passed == 1 else "NO_MATCH",
                    structure_id,
                    json.dumps({"evidence": symbol}, sort_keys=True),
                    _NOW,
                ),
            )
            return cursor.lastrowid

    def _seed_standard_history(self, path):
        ids = []
        ids.append(self._signal(path, 1, "N01", "BTCUSDT", 1, decision="LIVE_OPENED"))
        for index, strategy_id in enumerate(("N06", "N07", "N08", "N11", "N12")):
            ids.append(
                self._signal(
                    path,
                    1 if index < 3 else 2,
                    strategy_id,
                    "%sUSDT" % strategy_id,
                    1,
                    "%s-structure" % strategy_id.lower(),
                    decision="PLAN_REJECTED" if index == 0 else "PASSED",
                )
            )
        ids.append(self._signal(path, 2, "N03", "OLDUSDT", 0))
        ids.append(self._signal(path, 3, "N01", "CURRENTUSDT", 0))
        ids.append(self._signal(path, 3, "N04", "LATESTUSDT", 1))
        return ids

    def _attestation(self, path, scan_id):
        manifest = "0" * 64
        count = 0
        with _sqlite_connection(path) as connection:
            rows = connection.execute(
                "SELECT id, scan_id, strategy_id, symbol, funding_rate, "
                "matched_patterns, trend_slope, current_bullish, passed, "
                "decision, reason, structure_id, detail_json, created_at "
                "FROM strategy_signals WHERE scan_id = ? ORDER BY id",
                (scan_id,),
            ).fetchall()
        for row in rows:
            evidence_sha = _signal_evidence_sha256(row)
            manifest = hashlib.sha256(
                (manifest + ":" + evidence_sha).encode("ascii")
            ).hexdigest()
            count += 1
        return count, manifest

    def _inspect(self, path, scan_id):
        count, manifest = self._attestation(path, scan_id)
        return self._inspect_raw(
            str(path.resolve()),
            scan_id,
            count,
            manifest,
            n16_claim_ledger=str(self._claim_ledger(path).resolve()),
        )

    def _apply(self, path, scan_id, batch_size=500):
        count, manifest = self._attestation(path, scan_id)
        return self._apply_raw(
            str(path.resolve()),
            scan_id,
            count,
            manifest,
            batch_size,
            n16_claim_ledger=str(self._claim_ledger(path).resolve()),
        )

    def _checkpoint_for_vacuum(self, path):
        connection = sqlite3.connect(path)
        try:
            self.assertEqual(
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone(),
                (0, 0, 0),
            )
            self.assertEqual(
                connection.execute("PRAGMA journal_mode=DELETE").fetchone(),
                ("delete",),
            )
        finally:
            connection.close()
        wal = Path(str(path) + "-wal")
        shm = Path(str(path) + "-shm")
        journal = Path(str(path) + "-journal")
        self.assertFalse(wal.exists())
        self.assertFalse(journal.exists())
        # macOS SQLite can retain a stale SHM after the exact successful
        # checkpoint, DELETE-mode switch, and final close.  Only that SHM is
        # removed, after proving it is a private regular test file; WAL and
        # journal files are never unlinked by this helper.
        if shm.exists():
            details = os.lstat(str(shm))
            self.assertTrue(stat.S_ISREG(details.st_mode))
            self.assertEqual(details.st_nlink, 1)
            shm.unlink()
        self.assertFalse(Path(str(path) + "-wal").exists())
        self.assertFalse(Path(str(path) + "-shm").exists())
        self.assertFalse(Path(str(path) + "-journal").exists())

    def _insert_audit(self, connection, source, symbol=None):
        source = list(source)
        if symbol is not None:
            source[3] = symbol
        source = tuple(source)
        evidence_sha = _signal_evidence_sha256(source)
        connection.execute(
            """
            INSERT INTO strategy_passed_signal_audits (
                source_signal_id, source_scan_id, strategy_id, symbol,
                funding_rate, matched_patterns, trend_slope, current_bullish,
                passed, decision, reason, structure_id, detail_json,
                signal_created_at, evidence_sha256, created_at, claim_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')
            """,
            source + (evidence_sha, _NOW),
        )
        return evidence_sha

    def _publish_micro_current(self, path):
        recorder = ReviewRecorder(
            str(path),
            logging.getLogger("post-cutover-micro-current"),
            n16_claim_ledger_file=self._claim_ledger(path),
        )
        scan_id = recorder.begin_scan(1, [], True)
        self.assertEqual(scan_id, 4)
        window = MicroObservationTests._n21_window("MICROPOSTUSDT", rank=3)
        analysis = analyze_n21(window, window.observations[-1].observed_at_ms)
        self.assertTrue(analysis.passed, analysis.reason)
        self.assertTrue(recorder.record_micro_passed_analysis(scan_id, analysis))
        self.assertIsNotNone(
            recorder.record_strategy_signal(
                scan_id=scan_id,
                strategy_id="N21",
                symbol=analysis.symbol,
                funding_rate="",
                matched_patterns=(),
                trend_slope="",
                current_bullish=True,
                passed=True,
                decision="PASSED",
                reason="PASSED",
                structure_id=analysis.structure_id,
                detail=analysis.detail_json(),
            )
        )
        for strategy_id in ("N22", "N23", "N24", "N25"):
            self.assertIsNotNone(
                recorder.record_strategy_signal(
                    scan_id=scan_id,
                    strategy_id=strategy_id,
                    symbol=strategy_id + "POSTUSDT",
                    funding_rate="",
                    matched_patterns=(),
                    trend_slope="",
                    current_bullish=False,
                    passed=False,
                    decision="REJECTED",
                    reason=strategy_id + "_MICRO_OBSERVATION_COLD_START",
                    structure_id=None,
                    detail={"post_cutover": True},
                )
            )
        self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 5))
        with recorder._connect() as connection:
            manifest = connection.execute(
                "SELECT manifest_sha256 FROM strategy_signal_batches "
                "WHERE scan_id=? AND state='CURRENT'",
                (scan_id,),
            ).fetchone()[0]
        return recorder, scan_id, manifest

    def test_install_n16_bad_ledger_schema_is_controlled_zero_write_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            app_root = Path(directory) / "binance-app"
            data_root = app_root / "data"
            state_root = app_root / "state"
            data_root.mkdir(parents=True, mode=0o700)
            state_root.mkdir(mode=0o700)
            review = data_root / "review.sqlite3"
            ledger = data_root / "n16_claim_ledger.sqlite3"
            with _sqlite_connection(review) as connection:
                connection.execute(
                    "CREATE TABLE legacy_marker(id INTEGER PRIMARY KEY)"
                )
            with _sqlite_connection(ledger) as connection:
                connection.execute(
                    "CREATE TABLE wrong_ledger_schema(id INTEGER PRIMARY KEY)"
                )
            self._create_official_lock(ledger)
            before = (
                _file_fingerprint(review),
                _file_fingerprint(ledger),
                _directory_fingerprint(data_root),
            )
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                result = self._cli(
                    [
                        "--install-n16",
                        "--db",
                        str(review.resolve()),
                        "--n16-claim-ledger",
                        str(ledger.resolve()),
                        "--lock-file",
                        str((data_root / "trading_bot.lock").resolve()),
                        "--binance-root",
                        str(app_root.resolve()),
                    ]
                )
            self.assertEqual(result, 1)
            self.assertIn("strategy signal maintenance failed", stderr.getvalue())
            self.assertNotIn("Traceback", stderr.getvalue())
            self.assertEqual(
                (
                    _file_fingerprint(review),
                    _file_fingerprint(ledger),
                    _directory_fingerprint(data_root),
                ),
                before,
            )

    def test_dry_run_uses_read_only_database_and_does_not_install_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            with _sqlite_connection(path) as connection:
                for table in (
                    "strategy_signal_batches",
                    "strategy_passed_structure_ledger",
                    "strategy_passed_signal_audits",
                    "strategy_signal_current",
                ):
                    connection.execute("DROP TABLE %s" % table)
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            before = _file_sha256(path)
            directory_before = sorted(item.name for item in Path(directory).iterdir())
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "N16 lifecycle schema"
            ):
                self._inspect(path, 3)
            after = _file_sha256(path)
            self.assertEqual(before, after)
            self.assertEqual(
                sorted(item.name for item in Path(directory).iterdir()),
                directory_before,
            )
            with _sqlite_connection(path) as connection:
                names = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
            self.assertFalse(
                {
                    "strategy_signal_current",
                    "strategy_passed_signal_audits",
                }
                & names
            )

    def test_wrong_apply_attestation_is_pure_read_only_before_schema_install(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            with _sqlite_connection(path) as connection:
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                tables_before = connection.execute(
                    "SELECT name, sql FROM sqlite_master WHERE type='table' "
                    "ORDER BY name"
                ).fetchall()
            count, manifest = self._attestation(path, 3)
            file_before = _file_sha256(path)
            directory_before = sorted(item.name for item in Path(directory).iterdir())
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "count.*attestation"
            ):
                self._apply_raw(
                    str(path.resolve()), 3, count + 1, manifest,
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
            self.assertEqual(_file_sha256(path), file_before)
            self.assertEqual(
                sorted(item.name for item in Path(directory).iterdir()),
                directory_before,
            )
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT name, sql FROM sqlite_master WHERE type='table' "
                        "ORDER BY name"
                    ).fetchall(),
                    tables_before,
                )

    def test_apply_revalidates_n16_after_read_only_preflight_before_wal(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            count, manifest = self._attestation(path, 3)
            calls = {"count": 0}
            injected = {}
            original_open = _open_database

            def inject_wrong_n16_index(*args, **kwargs):
                calls["count"] += 1
                if calls["count"] == 2:
                    with _sqlite_connection(path) as connection:
                        connection.execute(
                            "DROP INDEX idx_n16_trend_support_structure"
                        )
                        connection.execute(
                            "CREATE INDEX idx_n16_trend_support_structure "
                            "ON n16_trend_support_states(symbol, structure_id DESC)"
                        )
                        connection.commit()
                        self.assertEqual(
                            connection.execute(
                                "PRAGMA wal_checkpoint(TRUNCATE)"
                            ).fetchone(),
                            (0, 0, 0),
                        )
                        self.assertEqual(
                            connection.execute(
                                "PRAGMA journal_mode=DELETE"
                            ).fetchone(),
                            ("delete",),
                        )
                    for suffix in ("-wal", "-shm", "-journal"):
                        sidecar = Path(str(path) + suffix)
                        if sidecar.exists():
                            sidecar.unlink()
                    injected["file"] = _file_fingerprint(path)
                    injected["directory"] = _directory_fingerprint(
                        Path(directory)
                    )
                return original_open(*args, **kwargs)

            with patch(
                "trading_bot.signal_retention._open_database",
                side_effect=inject_wrong_n16_index,
            ), self.assertRaisesRegex(
                SignalRetentionMaintenanceError,
                "N16",
            ):
                self._apply_raw(
                    str(path.resolve()), 3, count, manifest,
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
            # The coverage-family seal generation adds one final read-only
            # Review/ledger pairing attestation after the existing prewrite
            # rejection path.
            self.assertEqual(calls["count"], 5)
            self.assertEqual(_file_fingerprint(path), injected["file"])
            self.assertEqual(
                _directory_fingerprint(Path(directory)),
                injected["directory"],
            )

    def test_legacy_source_with_impossible_n16_pass_is_read_only_no_go(self):
        """A pre-retention source can never authenticate an N16 claim."""
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._signal(
                path,
                1,
                "N16",
                "FORGEDN16USDT",
                1,
                "forged-n16-structure",
            )
            self._signal(path, 3, "N01", "CURRENTUSDT", 0)
            with _sqlite_connection(path) as connection:
                for (trigger_name,) in connection.execute(
                    "SELECT name FROM sqlite_schema WHERE type='trigger' "
                    "AND name GLOB 'trg_n16_*' ORDER BY name"
                ).fetchall():
                    connection.execute('DROP TRIGGER "%s"' % trigger_name)
                for table in (
                    "n16_consumption_seals",
                    "n16_first_claim_witness",
                    "n16_lifecycle_guard",
                    "n16_trend_support_states",
                    "strategy_lifecycle_installations",
                ):
                    connection.execute('DROP TABLE "%s"' % table)
                for table in (
                    "strategy_signal_batches",
                    "strategy_passed_structure_ledger",
                    "strategy_passed_signal_audits",
                    "strategy_signal_current",
                ):
                    connection.execute("DROP TABLE %s" % table)
            with _sqlite_connection(path) as connection:
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            count, manifest = self._attestation(path, 3)
            before = _file_fingerprint(path)
            directory_before = _directory_fingerprint(Path(directory))
            for operation in (
                lambda: self._inspect_raw(
                    str(path.resolve()), 3, count, manifest,
                            n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                ),
                lambda: self._apply_raw(
                    str(path.resolve()), 3, count, manifest,
                            n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                ),
            ):
                with self.subTest(operation=operation), self.assertRaisesRegex(
                    SignalRetentionMaintenanceError,
                    "N16 lifecycle schema|impossible post-legacy strategy evidence",
                ):
                    operation()
                self.assertEqual(_file_fingerprint(path), before)
                self.assertEqual(
                    _directory_fingerprint(Path(directory)), directory_before
                )

    def test_invalid_keep_runtime_envelope_is_preflight_no_go(self):
        invalid_details = (
            json.dumps({"oversized": "x" * 70_000}),
            '{"nested":' * 40 + "0" + "}" * 40,
            '{"non_finite":NaN}',
        )
        for detail_json in invalid_details:
            with self.subTest(kind=detail_json[:24]), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                self._seed_standard_history(path)
                with _sqlite_connection(path) as connection:
                    current_id = connection.execute(
                        "SELECT MIN(id) FROM strategy_signals WHERE scan_id=3"
                    ).fetchone()[0]
                    connection.execute(
                        "UPDATE strategy_signals SET detail_json=? WHERE id=?",
                        (detail_json, current_id),
                    )
                    for table in (
                        "strategy_signal_batches",
                        "strategy_passed_structure_ledger",
                        "strategy_passed_signal_audits",
                        "strategy_signal_current",
                    ):
                        connection.execute("DROP TABLE %s" % table)
                with _sqlite_connection(path) as connection:
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                count, manifest = self._attestation(path, 3)
                file_before = _file_sha256(path)
                directory_before = sorted(
                    item.name for item in Path(directory).iterdir()
                )
                with self.assertRaises(SignalRetentionMaintenanceError):
                    self._inspect_raw(
                        str(path.resolve()), 3, count, manifest,
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )
                with self.assertRaises(SignalRetentionMaintenanceError):
                    self._apply_raw(
                        str(path.resolve()), 3, count, manifest,
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )
                self.assertEqual(_file_sha256(path), file_before)
                self.assertEqual(
                    sorted(item.name for item in Path(directory).iterdir()),
                    directory_before,
                )
                with _sqlite_connection(path) as connection:
                    names = {
                        row[0]
                        for row in connection.execute(
                            "SELECT name FROM sqlite_master WHERE type='table'"
                        )
                    }
                self.assertNotIn("strategy_signal_current", names)

    def test_preexisting_retention_trigger_is_rejected_before_schema_write(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            with _sqlite_connection(path) as connection:
                connection.execute(
                    "CREATE TRIGGER hostile_current_insert BEFORE INSERT ON "
                    "strategy_signal_current BEGIN INSERT INTO events "
                    "(occurred_at, event_type, symbol, payload_json) VALUES "
                    "('2026-07-14T00:00:00+00:00', 'HOSTILE', 'BTCUSDT', '{}'); END"
                )
                events_before = _rows(connection, "events")
                signals_before = _rows(connection, "strategy_signals")
                state_before = _rows(connection, "strategy_signal_current")
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError,
                "history coverage epoch graph is inconsistent",
            ):
                self._inspect(path, 3)
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError,
                "history coverage epoch graph is inconsistent",
            ):
                self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                self.assertEqual(_rows(connection, "events"), events_before)
                self.assertEqual(_rows(connection, "strategy_signals"), signals_before)
                self.assertEqual(
                    _rows(connection, "strategy_signal_current"), state_before
                )

    def test_correct_attestation_migrates_without_touching_protected_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            with _sqlite_connection(path) as connection:
                sentinel_before = _rows(
                    connection, "events"
                )
            count, manifest = self._attestation(path, 3)
            report = self._apply_raw(
                str(path.resolve()), 3, count, manifest, 2,
                         n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertEqual(report.migration_state, "COMPLETE")
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    _rows(connection, "events"),
                    sentinel_before,
                )
                names = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                self.assertTrue(
                    {
                        "strategy_signal_batches",
                        "strategy_signal_current",
                        "strategy_passed_signal_audits",
                        "strategy_passed_structure_ledger",
                    }.issubset(names)
                )

    def test_first_dry_run_reports_candidate_manifest_but_apply_requires_it(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            count, manifest = self._attestation(path, 3)
            report = self._inspect_raw(
                str(path.resolve()), 3, count,
                         n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertFalse(report.keep_attestation_verified)
            self.assertEqual(report.retained_manifest_sha256, manifest)
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "count.*attestation"
            ):
                self._inspect_raw(
                    str(path.resolve()), 3, count + 1,
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
            with self.assertRaises(SignalRetentionMaintenanceError):
                self._apply_raw(
                    str(path.resolve()), 3, count, None,
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT migration_state FROM strategy_signal_current"
                    ).fetchone(),
                    ("PENDING",),
                )

    def test_apply_backfills_every_passed_signal_and_all_five_ledgers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            ids = self._seed_standard_history(path)
            with _sqlite_connection(path) as connection:
                connection.execute(
                    "INSERT INTO strategy_paper_trades "
                    "(strategy_id, symbol, opened_at, last_checked_at, closed_at, "
                    "entry_price, stop_loss_price, take_profit_price, result, "
                    "exit_reason, r_multiple, funding_rate, orders_json, detail_json) "
                    "VALUES ('N06', 'PAPERUSDT', ?, ?, ?, '100', '99', '105', "
                    "'VOID', 'RULE_VERSION_INVALIDATED', NULL, '', "
                    "'{\"paper\":true}', '{\"analysis\":true}')",
                    (_NOW, _NOW, _NOW),
                )
                trade_id = connection.execute(
                    "INSERT INTO trade_reviews "
                    "(opened_at, closed_at, exit_reason, symbol, side, quantity, "
                    "entry_price, stop_loss_price, take_profit_price, leverage, "
                    "dry_run, status, orders_json) VALUES (?, ?, 'STOP_LOSS', "
                    "'LIVEUSDT', 'BUY', '1', '100', '99', '105', 10, 0, "
                    "'CLOSED_STOP_LOSS', '{\"live\":true}')",
                    (_NOW, _NOW),
                ).lastrowid
                connection.execute(
                    "INSERT INTO strategy_live_links "
                    "(strategy_id, trade_review_id, symbol, opened_at, closed_at, "
                    "result, created_at) VALUES ('N01', ?, 'LIVEUSDT', ?, ?, "
                    "'LOSS', ?)",
                    (trade_id, _NOW, _NOW, _NOW),
                )
                connection.execute(
                    "INSERT INTO strategy_states "
                    "(strategy_id, consecutive_wins, paper_trade_count, win_count, "
                    "loss_count, win_rate, live_eligible, live_result_pending, "
                    "last_trade_result, last_trade_closed_at, updated_at) "
                    "VALUES ('N01', 1, 3, 2, 1, '0.66666667', 0, 1, "
                    "'LOSS', ?, ?)",
                    (_NOW, _NOW),
                )
                connection.execute(
                    "INSERT INTO n08_history_coverage "
                    "(strategy_id, symbol, continuous_from_open_time, "
                    "continuous_until_open_time, last_response_first_open_time, "
                    "last_response_last_open_time, last_gap_from_open_time, "
                    "last_gap_to_open_time, updated_at) VALUES "
                    "('N08', 'N08USDT', '1', '2', '1', '2', NULL, NULL, ?)",
                    (_NOW,),
                )
                connection.execute(
                    "INSERT INTO strategy_structure_terminal_states "
                    "(strategy_id, symbol, structure_id, status, reason, detail_json, "
                    "created_at, updated_at) VALUES ('N11', 'N11USDT', 'terminal-1', "
                    "'CONSUMED', 'PASSED', '{\"terminal\":true}', ?, ?)",
                    (_NOW, _NOW),
                )
                connection.execute(
                    "INSERT INTO n14_market_snapshots "
                    "(strategy_id, s_time, payload_json, created_at, updated_at) "
                    "VALUES ('N14', '1783919700000', '{\"snapshot\":true}', ?, ?)",
                    (_NOW, _NOW),
                )
                connection.execute(
                    "INSERT INTO n14_active_episodes "
                    "(strategy_id, symbol, s_time, stage, detail_json, created_at, "
                    "updated_at) VALUES ('N14', 'N14USDT', '1783919700000', "
                    "'S_LOCKED', '{\"active\":true}', ?, ?)",
                    (_NOW, _NOW),
                )
                protected_before = {
                    table: _rows(connection, table)
                    for table in (
                        "scans",
                        "events",
                        "strategy_paper_trades",
                        "trade_reviews",
                        "strategy_live_links",
                        "strategy_states",
                        "n08_history_coverage",
                        "strategy_structure_terminal_states",
                        "n13_rotation_states",
                        "n14_market_snapshots",
                        "n14_active_episodes",
                        "n17_range_support_states",
                        "n17_history_coverage",
                        "n19_staircase_states",
                        "n19_history_coverage",
                        "n19_lifecycle_installation",
                    )
                }
                sequence_before = connection.execute(
                    "SELECT seq FROM sqlite_sequence WHERE name='strategy_signals'"
                ).fetchone()
            dry_run = self._inspect(path, 3)
            report = self._apply(path, 3, 2)
            self.assertEqual(report.migration_state, "COMPLETE")
            self.assertEqual(report.source_signal_count, 9)
            self.assertEqual(report.source_passed_count, 7)
            self.assertEqual(report.audit_count, 7)
            self.assertEqual(report.ledger_count, 5)
            self.assertEqual(report.deleted_signal_count, 7)
            self.assertEqual(
                report.protected_manifest_sha256,
                dry_run.protected_manifest_sha256,
            )
            self.assertEqual(report.n13_rotation_sha256, dry_run.n13_rotation_sha256)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT DISTINCT scan_id FROM strategy_signals"
                    ).fetchall(),
                    [(3,)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT strategy_id, decision FROM strategy_passed_signal_audits "
                        "WHERE source_signal_id IN (?, ?) ORDER BY source_signal_id",
                        (ids[0], ids[1]),
                    ).fetchall(),
                    [("N01", "LIVE_OPENED"), ("N06", "PLAN_REJECTED")],
                )
                self.assertEqual(
                    {
                        row[0]
                        for row in connection.execute(
                            "SELECT strategy_id FROM strategy_passed_structure_ledger"
                        )
                    },
                    {"N06", "N07", "N08", "N11", "N12"},
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT DISTINCT claim_state "
                        "FROM strategy_passed_signal_audits"
                    ).fetchall(),
                    [("ACTIVE",)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT DISTINCT claim_state "
                        "FROM strategy_passed_structure_ledger"
                    ).fetchall(),
                    [("ACTIVE",)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id, retention_active, migration_state "
                        "FROM strategy_signal_current"
                    ).fetchone(),
                    (3, 1, "COMPLETE"),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT scan_id, state, recorded_count, expected_count "
                        "FROM strategy_signal_batches"
                    ).fetchall(),
                    [(3, "CURRENT", 2, 2)],
                )
                self.assertEqual(
                    connection.execute("PRAGMA foreign_key_check").fetchall(), []
                )
                self.assertEqual(
                    connection.execute("PRAGMA integrity_check").fetchone(), ("ok",)
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT seq FROM sqlite_sequence WHERE name='strategy_signals'"
                    ).fetchone(),
                    sequence_before,
                )
                for table, before in protected_before.items():
                    self.assertEqual(_rows(connection, table), before, table)

    def test_empty_funding_and_trend_strings_are_preserved_for_structured_signals(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            source_id = self._signal(
                path, 1, "N06", "BTCUSDT", 1, "structured-signal"
            )
            self._signal(path, 3, "N01", "CURRENTUSDT", 0)
            with _sqlite_connection(path) as connection:
                connection.execute(
                    "UPDATE strategy_signals SET funding_rate='', trend_slope='' "
                    "WHERE id=?",
                    (source_id,),
                )
            self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT funding_rate, trend_slope "
                        "FROM strategy_passed_signal_audits "
                        "WHERE source_signal_id=?",
                        (source_id,),
                    ).fetchone(),
                    ("", ""),
                )

    def test_backfill_is_byte_stable_and_reentrant(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            count, manifest = self._attestation(path, 3)
            first = self._apply(path, 3, batch_size=1)
            with _sqlite_connection(path) as connection:
                audits = _rows(connection, "strategy_passed_signal_audits")
                ledgers = _rows(connection, "strategy_passed_structure_ledger")
                current = _rows(connection, "strategy_signals")
                sequence = connection.execute(
                    "SELECT seq FROM sqlite_sequence WHERE name='strategy_signals'"
                ).fetchone()
            second = self._apply(path, 3, batch_size=1)
            self.assertEqual(second.deleted_signal_count, 0)
            self.assertEqual(first.source_manifest_sha256, second.source_manifest_sha256)
            with _sqlite_connection(path) as connection:
                self.assertEqual(_rows(connection, "strategy_passed_signal_audits"), audits)
                self.assertEqual(_rows(connection, "strategy_passed_structure_ledger"), ledgers)
                self.assertEqual(_rows(connection, "strategy_signals"), current)
                self.assertEqual(
                    connection.execute(
                        "SELECT seq FROM sqlite_sequence WHERE name='strategy_signals'"
                    ).fetchone(),
                    sequence,
                )

    def test_n16_ledger_change_after_backfill_blocks_first_delete(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            with _sqlite_connection(path) as connection:
                before_count = connection.execute(
                    "SELECT COUNT(*) FROM strategy_signals"
                ).fetchone()[0]
            real_attest = signal_retention_module._attest_n16_claim_boundary
            calls = {"count": 0}

            def fail_before_delete(connection, ledger_path):
                calls["count"] += 1
                if calls["count"] == 4:
                    raise SignalRetentionMaintenanceError(
                        "N16 claim ledger changed before deletion"
                    )
                return real_attest(connection, ledger_path)

            with patch(
                "trading_bot.signal_retention._attest_n16_claim_boundary",
                side_effect=fail_before_delete,
            ), self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "before deletion"
            ):
                self._apply(path, 3, batch_size=10_000)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals"
                    ).fetchone()[0],
                    before_count,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT retention_active,migration_state "
                        "FROM strategy_signal_current"
                    ).fetchone(),
                    (0, "BACKFILLED"),
                )

    def test_n16_ledger_change_before_activation_leaves_backfilled_state(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            real_attest = signal_retention_module._attest_n16_claim_boundary
            calls = {"count": 0}

            def fail_before_activation(connection, ledger_path):
                calls["count"] += 1
                if calls["count"] == 6:
                    raise SignalRetentionMaintenanceError(
                        "N16 claim ledger changed before activation"
                    )
                return real_attest(connection, ledger_path)

            with patch(
                "trading_bot.signal_retention._attest_n16_claim_boundary",
                side_effect=fail_before_activation,
            ), self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "before activation"
            ):
                self._apply(path, 3, batch_size=10_000)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT retention_active,migration_state "
                        "FROM strategy_signal_current"
                    ).fetchone(),
                    (0, "BACKFILLED"),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id != 3"
                    ).fetchone(),
                    (0,),
                )
    def test_backfill_claims_are_active_and_abandoned_staging_cleanup_cannot_delete_them(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            self._apply(path, 3)
            connection = sqlite3.connect(path)
            try:
                active_audits = connection.execute(
                    "SELECT * FROM strategy_passed_signal_audits "
                    "WHERE claim_state='ACTIVE' ORDER BY id"
                ).fetchall()
                active_ledgers = connection.execute(
                    "SELECT * FROM strategy_passed_structure_ledger "
                    "WHERE claim_state='ACTIVE' ORDER BY id"
                ).fetchall()
                self.assertEqual(len(active_audits), 7)
                self.assertEqual(len(active_ledgers), 5)
            finally:
                connection.close()

            recorder = ReviewRecorder(
                str(path),
                logging.getLogger("maintenance-active-claim-cleanup"),
                n16_claim_ledger_file=self._claim_ledger(path),
            )
            staging_scan = recorder.begin_scan(1, [], True)
            self.assertIsNotNone(staging_scan)
            self.assertIsNotNone(
                recorder.record_strategy_signal(
                    staging_scan,
                    "N06",
                    "NEWUSDT",
                    "-0.01",
                    (),
                    "1",
                    True,
                    True,
                    "PASSED",
                    "PASSED",
                    structure_id="new-structure",
                    detail={"staged": True},
                )
            )
            replacement_scan = recorder.begin_scan(1, [], True)
            self.assertIsNotNone(replacement_scan)
            self.assertNotEqual(replacement_scan, staging_scan)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_signal_audits "
                        "WHERE claim_state='ACTIVE' ORDER BY id"
                    ).fetchall(),
                    active_audits,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_structure_ledger "
                        "WHERE claim_state='ACTIVE' ORDER BY id"
                    ).fetchall(),
                    active_ledgers,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits "
                        "WHERE claim_state='STAGED'"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                        "WHERE claim_state='STAGED'"
                    ).fetchone(),
                    (0,),
                )

    def test_deletion_crash_keeps_committed_backfill_and_resumes_short_batches(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            with _sqlite_connection(path) as connection:
                signals_before = _rows(connection, "strategy_signals")
            calls = [0]

            def crash_after_first(connection, keep_scan_id, batch_size):
                calls[0] += 1
                self.assertFalse(connection.in_transaction)
                if calls[0] == 2:
                    raise RuntimeError("simulated process crash")
                removed = _delete_one_batch(connection, keep_scan_id, batch_size)
                self.assertFalse(connection.in_transaction)
                return removed

            with patch(
                "trading_bot.signal_retention._delete_one_batch",
                side_effect=crash_after_first,
            ):
                with self.assertRaisesRegex(RuntimeError, "simulated process crash"):
                    self._apply(path, 3, 2)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT retention_active, migration_state "
                        "FROM strategy_signal_current"
                    ).fetchone(),
                    (0, "BACKFILLED"),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits"
                    ).fetchone()[0],
                    7,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_structure_ledger"
                    ).fetchone(),
                    (5,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signal_batches"
                    ).fetchone(),
                    (1,),
                )
                remaining = _rows(connection, "strategy_signals")
                self.assertEqual(len(remaining), len(signals_before) - 2)
                self.assertEqual([row[0] for row in remaining], list(range(3, 10)))
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id=3"
                    ).fetchone(),
                    (2,),
                )
            report = self._apply(path, 3, 2)
            self.assertEqual(report.migration_state, "COMPLETE")
            self.assertEqual(report.deleted_signal_count, 5)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT DISTINCT scan_id FROM strategy_signals"
                    ).fetchall(),
                    [(3,)],
                )

    def test_incoming_foreign_key_blocks_before_backfill_or_delete(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            ids = self._seed_standard_history(path)
            with _sqlite_connection(path) as connection:
                connection.execute(
                    "CREATE TABLE hostile_signal_child ("
                    "id INTEGER PRIMARY KEY, signal_id INTEGER NOT NULL, "
                    "FOREIGN KEY(signal_id) REFERENCES strategy_signals(id) "
                    "ON DELETE CASCADE)"
                )
                connection.execute(
                    "INSERT INTO hostile_signal_child VALUES (1, ?)", (ids[0],)
                )
                signals_before = _rows(connection, "strategy_signals")
                child_before = _rows(connection, "hostile_signal_child")
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError,
                "history coverage epoch graph is inconsistent",
            ):
                self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                self.assertEqual(_rows(connection, "strategy_signals"), signals_before)
                self.assertEqual(
                    _rows(connection, "hostile_signal_child"), child_before
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT retention_active, migration_state "
                        "FROM strategy_signal_current"
                    ).fetchone(),
                    (0, "PENDING"),
                )

    def test_backfilled_delete_trigger_blocks_resume_without_side_effect(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            with patch(
                "trading_bot.signal_retention._delete_one_batch",
                side_effect=RuntimeError("stop before first delete"),
            ):
                with self.assertRaisesRegex(RuntimeError, "stop before first delete"):
                    self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT retention_active, migration_state "
                        "FROM strategy_signal_current"
                    ).fetchone(),
                    (0, "BACKFILLED"),
                )
                connection.execute(
                    "CREATE TRIGGER hostile_signal_delete AFTER DELETE ON "
                    "strategy_signals BEGIN INSERT INTO events "
                    "(occurred_at, event_type, symbol, payload_json) VALUES "
                    "('2026-07-14T00:00:00+00:00', 'HOSTILE', 'BTCUSDT', '{}'); END"
                )
                signals_before = _rows(connection, "strategy_signals")
                events_before = _rows(connection, "events")
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError,
                "history coverage epoch graph is inconsistent",
            ):
                self._apply(path, 3, 2)
            with _sqlite_connection(path) as connection:
                self.assertEqual(_rows(connection, "strategy_signals"), signals_before)
                self.assertEqual(_rows(connection, "events"), events_before)
                self.assertEqual(
                    connection.execute(
                        "SELECT retention_active, migration_state "
                        "FROM strategy_signal_current"
                    ).fetchone(),
                    (0, "BACKFILLED"),
                )

    def test_final_validation_failure_leaves_safe_backfilled_and_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            with patch(
                "trading_bot.signal_retention._backfilled_final_summary",
                side_effect=RuntimeError("simulated final validation failure"),
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "simulated final validation failure"
                ):
                    self._apply(path, 3, 1)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id, retention_active, migration_state "
                        "FROM strategy_signal_current"
                    ).fetchone(),
                    (3, 0, "BACKFILLED"),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT DISTINCT scan_id FROM strategy_signals"
                    ).fetchall(),
                    [(3,)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signal_batches"
                    ).fetchone(),
                    (1,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits"
                    ).fetchone(),
                    (7,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_structure_ledger"
                    ).fetchone(),
                    (5,),
                )
            retry = self._apply(path, 3, 1)
            self.assertEqual(retry.deleted_signal_count, 0)
            self.assertEqual(retry.migration_state, "COMPLETE")

    def test_activation_post_update_tamper_rolls_back_to_backfilled(self):
        for tamper in ("pointer", "protected"):
            with self.subTest(tamper=tamper), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                self._seed_standard_history(path)
                ready = [False]
                injected = [False]

                def mark_finalized(connection, keep_scan_id):
                    summary = _backfilled_final_summary(connection, keep_scan_id)
                    ready[0] = True
                    return summary

                def inject_after_dependency_check(connection):
                    _validate_retention_dependencies(connection)
                    if ready[0] and connection.in_transaction and not injected[0]:
                        injected[0] = True
                        if tamper == "pointer":
                            connection.execute(
                                "CREATE TEMP TRIGGER hostile_activation "
                                "AFTER UPDATE OF retention_active ON "
                                "strategy_signal_current WHEN NEW.retention_active=1 "
                                "BEGIN UPDATE strategy_signal_current "
                                "SET current_scan_id=1 WHERE singleton_id=1; END"
                            )
                        else:
                            connection.execute(
                                "CREATE TEMP TRIGGER hostile_activation "
                                "AFTER UPDATE OF retention_active ON "
                                "strategy_signal_current WHEN NEW.retention_active=1 "
                                "BEGIN INSERT INTO events "
                                "(occurred_at, event_type, symbol, payload_json) "
                                "VALUES ('2026-07-14T00:00:00+00:00', "
                                "'HOSTILE', 'BTCUSDT', '{}'); END"
                            )

                with _sqlite_connection(path) as connection:
                    events_before = _rows(connection, "events")
                with patch(
                    "trading_bot.signal_retention._backfilled_final_summary",
                    side_effect=mark_finalized,
                ), patch(
                    "trading_bot.signal_retention._validate_retention_dependencies",
                    side_effect=inject_after_dependency_check,
                ):
                    with self.assertRaises(SignalRetentionMaintenanceError):
                        self._apply(path, 3, 1)
                self.assertTrue(injected[0])
                with _sqlite_connection(path) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT current_scan_id, retention_active, migration_state "
                            "FROM strategy_signal_current"
                        ).fetchone(),
                        (3, 0, "BACKFILLED"),
                    )
                    self.assertEqual(_rows(connection, "events"), events_before)
                    self.assertEqual(
                        connection.execute(
                            "SELECT DISTINCT scan_id FROM strategy_signals"
                        ).fetchall(),
                        [(3,)],
                    )
                retry = self._apply(path, 3, 1)
                self.assertEqual(retry.migration_state, "COMPLETE")
                self.assertEqual(retry.deleted_signal_count, 0)

    def test_activation_runs_database_checks_after_update_before_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            saw_complete_inside_transaction = [False]

            def fail_post_update_checks(connection):
                checks = _database_checks(connection)
                state = connection.execute(
                    "SELECT retention_active, migration_state "
                    "FROM strategy_signal_current WHERE singleton_id=1"
                ).fetchone()
                if connection.in_transaction and state == (1, "COMPLETE"):
                    saw_complete_inside_transaction[0] = True
                    raise SignalRetentionMaintenanceError(
                        "simulated post-update database check failure"
                    )
                return checks

            with patch(
                "trading_bot.signal_retention._database_checks",
                side_effect=fail_post_update_checks,
            ):
                with self.assertRaisesRegex(
                    SignalRetentionMaintenanceError,
                    "post-update database check failure",
                ):
                    self._apply(path, 3, 1)
            self.assertTrue(saw_complete_inside_transaction[0])
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id, retention_active, migration_state "
                        "FROM strategy_signal_current"
                    ).fetchone(),
                    (3, 0, "BACKFILLED"),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT DISTINCT scan_id FROM strategy_signals"
                    ).fetchall(),
                    [(3,)],
                )
            retry = self._apply(path, 3, 1)
            self.assertEqual(retry.migration_state, "COMPLETE")

    def test_published_passed_update_remains_maintenance_and_vacuum_valid(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            ids = self._seed_standard_history(path)
            count, manifest = self._attestation(path, 3)
            self._apply_raw(
                str(path.resolve()), 3, count, manifest,
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            recorder = ReviewRecorder(
                str(path),
                logging.getLogger("published-maintenance-update"),
                n16_claim_ledger_file=self._claim_ledger(path),
            )
            current_passed_id = ids[-1]
            with _sqlite_connection(path) as connection:
                batch_before = _rows(connection, "strategy_signal_batches")
                audit_before = _rows(connection, "strategy_passed_signal_audits")
            connection.close()
            recorder.update_strategy_signal(
                current_passed_id,
                decision="PLAN_REJECTED",
                reason="POST_PUBLISH_PLAN_REJECTED",
                detail={"plan": {"status": "REJECTED"}},
            )
            report = self._inspect_raw(
                str(path.resolve()), 3, count, manifest,
                         n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertTrue(report.keep_attestation_verified)
            self.assertEqual(report.retained_manifest_sha256, manifest)
            reapply = self._apply_raw(
                str(path.resolve()), 3, count, manifest,
                          n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertEqual(reapply.deleted_signal_count, 0)
            destination = self._vacuum_destination(
                directory, "published.compacted.sqlite3"
            )
            self._checkpoint_for_vacuum(path)
            self._vacuum_raw(
                str(path.resolve()), str(destination.resolve()),
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            with _sqlite_connection(path) as source, _sqlite_connection(
                destination
            ) as target:
                self.assertEqual(
                    _rows(source, "strategy_signal_batches"), batch_before
                )
                self.assertEqual(
                    _rows(source, "strategy_passed_signal_audits"), audit_before
                )
                self.assertEqual(
                    _rows(target, "strategy_signals"),
                    _rows(source, "strategy_signals"),
                )

    def test_fresh_genesis_first_current_inspect_and_vacuum_are_valid(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fresh.sqlite3"
            ledger = self._claim_ledger(path)
            with _sqlite_connection(path) as connection:
                connection.commit()
            _install_n16_claim_boundary(path, ledger)
            _install_n17_lifecycle_boundary(path, ledger)
            _install_n19_lifecycle_boundary(path, ledger)
            _install_n18_lifecycle_boundary(path, ledger)
            _install_n20_lifecycle_boundary(path, ledger)
            _install_micro_lifecycle_boundary(path, ledger)
            _install_coverage_epoch_boundary(path, ledger)
            recorder = ReviewRecorder(
                str(path),
                logging.getLogger("fresh-genesis-maintenance"),
                n16_claim_ledger_file=ledger,
            )
            scan_id = recorder.begin_scan(1, [], True)
            self.assertIsNotNone(scan_id)
            signal_id = recorder.record_strategy_signal(
                scan_id,
                "N01",
                "BTCUSDT",
                "-0.01",
                (),
                "1",
                True,
                False,
                "REJECTED",
                "NO_MATCH",
                detail={"fresh": True},
            )
            self.assertIsNotNone(signal_id)
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            with _sqlite_connection(path) as connection:
                manifest = connection.execute(
                    "SELECT manifest_sha256 FROM strategy_signal_batches "
                    "WHERE scan_id = ?",
                    (scan_id,),
                ).fetchone()[0]
                marker = connection.execute(
                    """
                    SELECT migration_cutoff_signal_id, source_signal_count,
                           source_passed_count, source_manifest_sha256,
                           retention_origin
                    FROM strategy_signal_current WHERE singleton_id=1
                    """
                ).fetchone()
            connection.close()
            self.assertEqual(marker, (0, 0, 0, "0" * 64, "GENESIS"))
            report = self._inspect_raw(
                str(path.resolve()), scan_id, 1, manifest,
                         n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertEqual(report.migration_state, "COMPLETE")
            self._checkpoint_for_vacuum(path)
            destination = self._vacuum_destination(
                directory, "fresh.compacted.sqlite3"
            )
            compacted = self._vacuum_raw(
                str(path.resolve()), str(destination.resolve()),
                            n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertTrue(compacted.vacuum_performed)

    def test_maintenance_and_vacuum_reject_orphan_coverage_epoch_before_write(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            recorder = ReviewRecorder(
                path,
                logging.getLogger("coverage-maintenance-boundary"),
                n16_claim_ledger_file=self._claim_ledger(path),
            )
            scan_id = recorder.begin_scan(1, [], True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    scan_id, "N01", "BTCUSDT", "", (), "", False, False,
                    "REJECTED", "NO_MATCH",
                ),
                int,
            )
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            orphan_sha = coverage_epoch_chain_sha256(
                "N17", "ORPHANUSDT", 1, 1, 1, "a" * 64,
                None, None, None, scan_id,
            )
            with _sqlite_connection(path) as connection:
                connection.create_function(
                    "_coverage_epoch_mutation_authorized",
                    6,
                    lambda *_args: 1,
                )
                connection.execute(
                    "INSERT INTO history_coverage_epoch_chain VALUES "
                    "(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        "N17", "ORPHANUSDT", 1, 1, 1, "a" * 64,
                        None, None, None, orphan_sha, scan_id, _NOW,
                    ),
                )
            before = _file_fingerprint(path)
            ledger_before = _file_fingerprint(self._claim_ledger(path))
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "coverage epoch"
            ):
                self._inspect(path, scan_id)
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "coverage epoch"
            ):
                self._apply(path, scan_id)
            self.assertEqual(_file_fingerprint(path), before)
            self.assertEqual(
                _file_fingerprint(self._claim_ledger(path)), ledger_before
            )
            self._checkpoint_for_vacuum(path)
            destination = self._vacuum_destination(
                directory, "orphan.compacted.sqlite3"
            )
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "coverage epoch"
            ):
                self._vacuum_raw(
                    str(path.resolve()),
                    str(destination.resolve()),
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
            self.assertFalse(destination.exists())

    def test_maintenance_and_vacuum_reject_missing_contiguous_receipt_before_write(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            recorder = ReviewRecorder(
                path,
                logging.getLogger("coverage-receipt-maintenance-boundary"),
                n16_claim_ledger_file=self._claim_ledger(path),
            )
            base = 900_000
            proposals = (
                HistoryCoverageProposal(
                    "N17", "RECEIPTUSDT", base,
                    base + 120 * 900_000,
                    base + 121 * 900_000, "a" * 64,
                ),
                HistoryCoverageProposal(
                    "N17", "RECEIPTUSDT", base + 900_000,
                    base + 121 * 900_000,
                    base + 122 * 900_000, "b" * 64,
                ),
            )
            scan_ids = []
            for proposal in proposals:
                scan_id = recorder.begin_scan(1, [], True)
                scan_ids.append(scan_id)
                self.assertIsInstance(
                    recorder.record_strategy_signal(
                        scan_id, "N17", "RECEIPTUSDT", "", (), "", False,
                        False, "REJECTED", "N17_NO_SETUP",
                    ),
                    int,
                )
                self.assertTrue(
                    recorder.publish_strategy_signal_batch(
                        scan_id, 1, (proposal,)
                    )
                )
            with _sqlite_connection(path) as connection:
                connection.execute(
                    'DROP TRIGGER "trg_history_coverage_receipt_no_delete"'
                )
                connection.execute(
                    "DELETE FROM history_coverage_publication_receipts "
                    "WHERE source_scan_id=? AND strategy_id='N17' "
                    "AND symbol='RECEIPTUSDT'",
                    (scan_ids[0],),
                )
                connection.execute(
                    COVERAGE_EPOCH_TRIGGER_SQL[
                        "trg_history_coverage_receipt_no_delete"
                    ]
                )
            with _sqlite_connection(path) as connection:
                current = connection.execute(
                    "SELECT b.scan_id,b.recorded_count,b.manifest_sha256 "
                    "FROM strategy_signal_batches b "
                    "JOIN strategy_signal_current c "
                    "ON c.current_scan_id=b.scan_id WHERE c.singleton_id=1"
                ).fetchone()
            before = _file_fingerprint(path)
            ledger_before = _file_fingerprint(self._claim_ledger(path))
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "coverage"
            ):
                self._inspect_raw(
                    str(path.resolve()), current[0], current[1], current[2],
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "coverage"
            ):
                self._apply_raw(
                    str(path.resolve()), current[0], current[1], current[2],
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
            self.assertEqual(_file_fingerprint(path), before)
            self.assertEqual(
                _file_fingerprint(self._claim_ledger(path)), ledger_before
            )
            self._checkpoint_for_vacuum(path)
            destination = self._vacuum_destination(
                directory, "missing-receipt.compacted.sqlite3"
            )
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "coverage"
            ):
                self._vacuum_raw(
                    str(path.resolve()),
                    str(destination.resolve()),
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
            self.assertFalse(destination.exists())

    def test_maintenance_entries_reject_latest_receipt_and_head_conflicts_zero_write(self):
        attacks = (
            "missing_latest",
            "publication_count",
            "latest_sha",
            "covered",
            "source_sha",
            "result_epoch",
        )
        for attack in attacks:
            with self.subTest(attack=attack), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                recorder = ReviewRecorder(
                    path,
                    logging.getLogger("coverage-latest-" + attack),
                    n16_claim_ledger_file=self._claim_ledger(path),
                )
                base = 900_000
                proposals = (
                    HistoryCoverageProposal(
                        "N17", "LATESTUSDT", base,
                        base + 120 * 900_000,
                        base + 121 * 900_000, "a" * 64,
                    ),
                    HistoryCoverageProposal(
                        "N17", "LATESTUSDT", base + 900_000,
                        base + 121 * 900_000,
                        base + 122 * 900_000, "b" * 64,
                    ),
                )
                scan_ids = []
                for proposal in proposals:
                    scan_id = recorder.begin_scan(1, [], True)
                    scan_ids.append(scan_id)
                    self.assertIsInstance(
                        recorder.record_strategy_signal(
                            scan_id, "N17", "LATESTUSDT", "", (), "",
                            False, False, "REJECTED", "N17_NO_SETUP",
                        ),
                        int,
                    )
                    self.assertTrue(
                        recorder.publish_strategy_signal_batch(
                            scan_id, 1, (proposal,)
                        )
                    )
                with _sqlite_connection(path) as connection:
                    if attack == "missing_latest":
                        connection.execute(
                            'DROP TRIGGER "trg_history_coverage_receipt_no_delete"'
                        )
                        connection.execute(
                            "DELETE FROM history_coverage_publication_receipts "
                            "WHERE source_scan_id=? AND strategy_id='N17' "
                            "AND symbol='LATESTUSDT'",
                            (scan_ids[-1],),
                        )
                        connection.execute(
                            COVERAGE_EPOCH_TRIGGER_SQL[
                                "trg_history_coverage_receipt_no_delete"
                            ]
                        )
                    elif attack in {
                        "publication_count", "latest_sha", "covered",
                        "source_sha",
                    }:
                        connection.execute(
                            'DROP TRIGGER "trg_history_coverage_head_transition"'
                        )
                        connection.execute(
                            'DROP TRIGGER '
                            '"trg_history_coverage_head_update_authorized"'
                        )
                        assignment = {
                            "publication_count": "publication_count=3",
                            "latest_sha": "latest_receipt_sha256='" + "f" * 64 + "'",
                            "covered": (
                                "covered_through_time_ms="
                                "covered_through_time_ms+900000"
                            ),
                            "source_sha": "source_sha256='" + "c" * 64 + "'",
                        }[attack]
                        connection.execute(
                            "UPDATE history_coverage_epoch_heads SET "
                            + assignment
                            + " WHERE strategy_id='N17' "
                            "AND symbol='LATESTUSDT'"
                        )
                        connection.execute(
                            COVERAGE_EPOCH_TRIGGER_SQL[
                                "trg_history_coverage_head_transition"
                            ]
                        )
                        connection.execute(
                            COVERAGE_EPOCH_TRIGGER_SQL[
                                "trg_history_coverage_head_update_authorized"
                            ]
                        )
                    else:
                        receipt = tuple(connection.execute(
                            "SELECT source_scan_id,strategy_id,symbol,"
                            "source_start_time_ms,covered_through_time_ms,"
                            "source_sha256,result_epoch_ordinal,"
                            "result_epoch_start_time_ms,"
                            "result_chain_head_sha256,publication_ordinal,"
                            "previous_receipt_sha256,batch_expected_count,"
                            "batch_manifest_sha256 FROM "
                            "history_coverage_publication_receipts "
                            "WHERE source_scan_id=? AND strategy_id='N17' "
                            "AND symbol='LATESTUSDT'",
                            (scan_ids[-1],),
                        ).fetchone())
                        changed = receipt[:6] + (receipt[6] + 1,) + receipt[7:]
                        receipt_sha = coverage_publication_receipt_sha256(
                            *changed
                        )
                        connection.execute(
                            'DROP TRIGGER "trg_history_coverage_receipt_no_update"'
                        )
                        connection.execute(
                            "UPDATE history_coverage_publication_receipts SET "
                            "result_epoch_ordinal=?,receipt_sha256=? "
                            "WHERE source_scan_id=? AND strategy_id='N17' "
                            "AND symbol='LATESTUSDT'",
                            (changed[6], receipt_sha, scan_ids[-1]),
                        )
                        connection.execute(
                            COVERAGE_EPOCH_TRIGGER_SQL[
                                "trg_history_coverage_receipt_no_update"
                            ]
                        )
                        connection.execute(
                            'DROP TRIGGER "trg_history_coverage_head_transition"'
                        )
                        connection.execute(
                            'DROP TRIGGER '
                            '"trg_history_coverage_head_update_authorized"'
                        )
                        connection.execute(
                            "UPDATE history_coverage_epoch_heads SET "
                            "latest_receipt_sha256=? "
                            "WHERE strategy_id='N17' "
                            "AND symbol='LATESTUSDT'",
                            (receipt_sha,),
                        )
                        connection.execute(
                            COVERAGE_EPOCH_TRIGGER_SQL[
                                "trg_history_coverage_head_transition"
                            ]
                        )
                        connection.execute(
                            COVERAGE_EPOCH_TRIGGER_SQL[
                                "trg_history_coverage_head_update_authorized"
                            ]
                        )
                with _sqlite_connection(path) as connection:
                    current = connection.execute(
                        "SELECT b.scan_id,b.recorded_count,b.manifest_sha256 "
                        "FROM strategy_signal_batches b "
                        "JOIN strategy_signal_current c "
                        "ON c.current_scan_id=b.scan_id "
                        "WHERE c.singleton_id=1"
                    ).fetchone()
                before = _file_fingerprint(path)
                ledger_before = _file_fingerprint(self._claim_ledger(path))
                names_before = sorted(os.listdir(directory))
                for operation in ("inspect", "apply"):
                    with self.assertRaisesRegex(
                        SignalRetentionMaintenanceError, "coverage"
                    ):
                        getattr(self, "_" + operation + "_raw")(
                            str(path.resolve()),
                            current[0],
                            current[1],
                            current[2],
                            n16_claim_ledger=str(
                                self._claim_ledger(path).resolve()
                            ),
                        )
                    self.assertEqual(_file_fingerprint(path), before)
                    self.assertEqual(
                        _file_fingerprint(self._claim_ledger(path)),
                        ledger_before,
                    )
                    self.assertEqual(
                        sorted(os.listdir(directory)), names_before
                    )
                self._checkpoint_for_vacuum(path)
                destination = self._vacuum_destination(
                    directory, attack + ".compacted.sqlite3"
                )
                with self.assertRaisesRegex(
                    SignalRetentionMaintenanceError, "coverage"
                ):
                    self._vacuum_raw(
                        str(path.resolve()),
                        str(destination.resolve()),
                        n16_claim_ledger=str(
                            self._claim_ledger(path).resolve()
                        ),
                    )
                self.assertFalse(destination.exists())
                for suffix in ("-wal", "-shm", "-journal"):
                    self.assertFalse(
                        Path(str(destination) + suffix).exists()
                    )

    def test_legacy_marker_survives_larger_and_smaller_future_currents(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            self._apply(path, 3)
            recorder = ReviewRecorder(
                str(path),
                logging.getLogger("legacy-rotation-maintenance"),
                n16_claim_ledger_file=self._claim_ledger(path),
            )
            with recorder._connect() as connection:
                marker_before = connection.execute(
                    """
                    SELECT migration_cutoff_signal_id, source_signal_count,
                           source_passed_count, source_manifest_sha256,
                           retention_origin
                    FROM strategy_signal_current WHERE singleton_id=1
                    """
                ).fetchone()
            self.assertEqual(marker_before[4], "LEGACY")
            for expected_count in (10, 1):
                scan_id = recorder.begin_scan(1, [], True)
                self.assertIsNotNone(scan_id)
                for index in range(expected_count):
                    self.assertIsNotNone(
                        recorder.record_strategy_signal(
                            scan_id,
                            "N01",
                            "FUTURE%02dUSDT" % index,
                            "-0.01",
                            (),
                            "1",
                            True,
                            False,
                            "REJECTED",
                            "NO_MATCH",
                            detail={"future": index},
                        )
                    )
                self.assertTrue(
                    recorder.publish_strategy_signal_batch(
                        scan_id, expected_count
                    )
                )
                with recorder._connect() as connection:
                    marker_after = connection.execute(
                        """
                        SELECT migration_cutoff_signal_id, source_signal_count,
                               source_passed_count, source_manifest_sha256,
                               retention_origin
                        FROM strategy_signal_current WHERE singleton_id=1
                        """
                    ).fetchone()
                    manifest = connection.execute(
                        "SELECT manifest_sha256 FROM strategy_signal_batches "
                        "WHERE scan_id = ?",
                        (scan_id,),
                    ).fetchone()[0]
                self.assertEqual(marker_after, marker_before)
                report = self._inspect_raw(
                    str(path.resolve()), scan_id, expected_count, manifest,
                             n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
                self.assertEqual(report.retained_signal_count, expected_count)

    def test_completed_legacy_retention_accepts_post_cutover_n16_to_n20(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            self._apply(path, 3)
            recorder = ReviewRecorder(
                str(path),
                logging.getLogger("legacy-post-cutover-maintenance"),
                n16_claim_ledger_file=self._claim_ledger(path),
            )
            with recorder._connect() as connection:
                sealed_source = connection.execute(
                    "SELECT migration_cutoff_signal_id, source_signal_count, "
                    "source_passed_count, source_manifest_sha256, retention_origin "
                    "FROM strategy_signal_current WHERE singleton_id=1"
                ).fetchone()
            self.assertEqual(sealed_source[4], "LEGACY")

            scan_id = recorder.begin_scan(100, [], True)
            self.assertIsNotNone(scan_id)
            coverage_proposals = []
            for strategy_id in ("N16", "N17", "N18", "N19", "N20"):
                symbol = strategy_id + "POSTCUTOVERUSDT"
                self.assertIsNotNone(
                    recorder.record_strategy_signal(
                        scan_id,
                        strategy_id,
                        symbol,
                        "-0.01",
                        (),
                        "0",
                        False,
                        False,
                        "REJECTED",
                        strategy_id + "_NO_ACTIVE_STRUCTURE",
                        detail={"post_cutover": True, "strategy_id": strategy_id},
                    )
                )
                if strategy_id in ("N17", "N18", "N19"):
                    source_start = 900_000
                    covered_through = source_start + (120 * 900_000)
                    coverage_proposals.append(
                        HistoryCoverageProposal(
                            strategy_id,
                            symbol,
                            source_start,
                            covered_through,
                            covered_through + 900_000,
                            strategy_id[-1] * 64,
                        )
                    )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    scan_id, 5, tuple(coverage_proposals)
                )
            )

            with recorder._connect() as connection:
                marker_after_publish = connection.execute(
                    "SELECT migration_cutoff_signal_id, source_signal_count, "
                    "source_passed_count, source_manifest_sha256, retention_origin "
                    "FROM strategy_signal_current WHERE singleton_id=1"
                ).fetchone()
                self.assertEqual(marker_after_publish, sealed_source)
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE id <= ?",
                        (sealed_source[0],),
                    ).fetchone(),
                    (0,),
                )
                manifest = connection.execute(
                    "SELECT manifest_sha256 FROM strategy_signal_batches "
                    "WHERE scan_id=? AND state='CURRENT'",
                    (scan_id,),
                ).fetchone()[0]
                before_maintenance = signal_retention_module._all_table_hashes(
                    connection
                )

            dry_run = self._inspect_raw(
                str(path.resolve()),
                scan_id,
                5,
                manifest,
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertEqual(dry_run.source_signal_count, sealed_source[1])
            self.assertEqual(dry_run.source_passed_count, sealed_source[2])
            self.assertEqual(dry_run.source_manifest_sha256, sealed_source[3])
            self.assertEqual(dry_run.retained_signal_count, 5)
            applied = self._apply_raw(
                str(path.resolve()),
                scan_id,
                5,
                manifest,
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertEqual(applied.deleted_signal_count, 0)
            with recorder._connect() as connection:
                self.assertEqual(
                    signal_retention_module._all_table_hashes(connection),
                    before_maintenance,
                )

            self._checkpoint_for_vacuum(path)
            destination = self._vacuum_destination(
                directory, "post-cutover.compacted.sqlite3"
            )
            vacuumed = self._vacuum_raw(
                str(path.resolve()),
                str(destination.resolve()),
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertTrue(vacuumed.vacuum_performed)
            with _sqlite_connection(path) as source, _sqlite_connection(
                destination
            ) as target:
                self.assertEqual(
                    signal_retention_module._all_table_hashes(source),
                    before_maintenance,
                )
                self.assertEqual(
                    signal_retention_module._all_table_hashes(target),
                    before_maintenance,
                )

    def test_completed_legacy_retention_preserves_post_cutover_micro_graph(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            initial = self._apply(path, 3)
            self.assertGreater(initial.deleted_signal_count, 0)
            recorder, scan_id, manifest = self._publish_micro_current(path)

            permanent_tables = (
                "micro_passed_analyses",
                "micro_strategy_lifecycle",
                "strategy_passed_signal_audits",
                "strategy_passed_structure_ledger",
            )
            with recorder._connect() as connection:
                cutoff, source_count, source_passed, source_manifest = (
                    connection.execute(
                        "SELECT migration_cutoff_signal_id,source_signal_count,"
                        "source_passed_count,source_manifest_sha256 "
                        "FROM strategy_signal_current WHERE singleton_id=1"
                    ).fetchone()
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE id<=?",
                        (cutoff,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT strategy_id,passed,decision FROM strategy_signals "
                        "WHERE scan_id=? ORDER BY strategy_id",
                        (scan_id,),
                    ).fetchall(),
                    [("N21", 1, "PASSED")]
                    + [(item, 0, "REJECTED") for item in ("N22", "N23", "N24", "N25")],
                )
                permanent_counts = {
                    table: connection.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE strategy_id='N21'"
                    ).fetchone()[0]
                    for table in permanent_tables
                }
                self.assertEqual(permanent_counts, {table: 1 for table in permanent_tables})
                permanent_hashes = {
                    table: signal_retention_module._table_full_hash(connection, table)
                    for table in permanent_tables
                }
                all_hashes = signal_retention_module._all_table_hashes(connection)

            report = self._inspect_raw(
                str(path.resolve()), scan_id, 5, manifest,
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertEqual(report.source_signal_count, source_count)
            self.assertEqual(report.source_passed_count, source_passed)
            self.assertEqual(report.source_manifest_sha256, source_manifest)
            self.assertEqual(report.retained_signal_count, 5)
            applied = self._apply_raw(
                str(path.resolve()), scan_id, 5, manifest,
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertEqual(applied.deleted_signal_count, 0)
            with recorder._connect() as connection:
                self.assertEqual(
                    {
                        table: connection.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE strategy_id='N21'"
                        ).fetchone()[0]
                        for table in permanent_tables
                    },
                    permanent_counts,
                )
                self.assertEqual(
                    {
                        table: signal_retention_module._table_full_hash(connection, table)
                        for table in permanent_tables
                    },
                    permanent_hashes,
                )
                self.assertEqual(
                    signal_retention_module._all_table_hashes(connection), all_hashes
                )

            self._checkpoint_for_vacuum(path)
            destination = self._vacuum_destination(
                directory, "post-cutover-micro.compacted.sqlite3"
            )
            vacuumed = self._vacuum_raw(
                str(path.resolve()), str(destination.resolve()),
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertTrue(vacuumed.vacuum_performed)
            with _sqlite_connection(path) as source, _sqlite_connection(
                destination
            ) as target:
                self.assertEqual(
                    signal_retention_module._all_table_hashes(source), all_hashes
                )
                self.assertEqual(
                    signal_retention_module._all_table_hashes(target), all_hashes
                )
                for table in permanent_tables:
                    self.assertEqual(
                        signal_retention_module._table_full_hash(source, table),
                        permanent_hashes[table],
                    )
                    self.assertEqual(
                        signal_retention_module._table_full_hash(target, table),
                        permanent_hashes[table],
                    )

    def test_coverage_epoch_gap_chain_survives_maintenance_and_vacuum(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            self._apply(path, 3)
            first_start = 900_000
            first_covered = first_start + (120 * 900_000)
            second_start = first_covered + 1_800_000
            second_covered = second_start + (120 * 900_000)
            first_source = "a" * 64
            second_source = "b" * 64
            recorder = ReviewRecorder(
                path,
                logging.getLogger("coverage-maintenance-roundtrip"),
                n16_claim_ledger_file=self._claim_ledger(path),
            )
            for start, covered, source in (
                (first_start, first_covered, first_source),
                (second_start, second_covered, second_source),
            ):
                proposal = HistoryCoverageProposal(
                    "N17",
                    "GAPUSDT",
                    start,
                    covered,
                    covered + 900_000,
                    source,
                )
                scan_id = recorder.begin_scan(1, [], True)
                self.assertIsInstance(
                    recorder.record_strategy_signal(
                        scan_id, "N17", "GAPUSDT", "", (), "", False, False,
                        "REJECTED", "N17_NO_SETUP",
                    ),
                    int,
                )
                self.assertTrue(
                    recorder.publish_strategy_signal_batch(
                        scan_id, 1, (proposal,)
                    )
                )
            with _sqlite_connection(path) as connection:
                validate_coverage_epoch_graph(connection)
                current = connection.execute(
                    "SELECT b.scan_id,b.recorded_count,b.manifest_sha256 "
                    "FROM strategy_signal_batches b "
                    "JOIN strategy_signal_current c "
                    "ON c.current_scan_id=b.scan_id WHERE c.singleton_id=1"
                ).fetchone()
                before = signal_retention_module._all_table_hashes(connection)
                epoch_hashes = {
                    table: signal_retention_module._table_full_hash(
                        connection, table
                    )
                    for table in (
                        "history_coverage_epoch_chain",
                        "history_coverage_epoch_heads",
                        "history_coverage_epoch_installation",
                        "history_coverage_publication_receipts",
                    )
                }

            report = self._inspect_raw(
                str(path.resolve()), current[0], current[1], current[2],
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertEqual(report.retained_signal_count, current[1])
            applied = self._apply_raw(
                str(path.resolve()), current[0], current[1], current[2],
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertEqual(applied.deleted_signal_count, 0)
            with _sqlite_connection(path) as connection:
                validate_coverage_epoch_graph(connection)
                self.assertEqual(
                    signal_retention_module._all_table_hashes(connection), before
                )
                self.assertEqual(
                    {
                        table: signal_retention_module._table_full_hash(
                            connection, table
                        )
                        for table in epoch_hashes
                    },
                    epoch_hashes,
                )

            self._checkpoint_for_vacuum(path)
            destination = self._vacuum_destination(
                directory, "coverage-epoch.compacted.sqlite3"
            )
            vacuumed = self._vacuum_raw(
                str(path.resolve()), str(destination.resolve()),
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertTrue(vacuumed.vacuum_performed)
            with _sqlite_connection(path) as source, _sqlite_connection(
                destination
            ) as target:
                validate_coverage_epoch_graph(source)
                validate_coverage_epoch_graph(target)
                self.assertEqual(
                    signal_retention_module._all_table_hashes(source), before
                )
                self.assertEqual(
                    signal_retention_module._all_table_hashes(target), before
                )

    def test_n15_terminal_receipt_survives_retention_and_vacuum(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            recorder = ReviewRecorder(
                path,
                logging.getLogger("n15-terminal-maintenance-roundtrip"),
                n16_claim_ledger_file=self._claim_ledger(path),
            )
            items, first_raws, second_raws, checked = (
                n15_seed_drift_windows()
            )
            scheduler = StrategyScheduler(
                (N15_STRATEGY,), 96, recorder,
                logging.getLogger("n15-terminal-maintenance-scheduler"),
            )
            first = scheduler._build_n15_batch_context(
                {"quote_volume_top": items}, first_raws, checked
            )
            self.assertTrue(first.complete)
            second = scheduler._build_n15_batch_context(
                {"quote_volume_top": items},
                second_raws,
                checked + 900_000,
            )
            self.assertTrue(second.complete)
            scan_id = recorder.begin_scan(1, [], True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    scan_id, "N01", "BTCUSDT", "", (), "", False, False,
                    "REJECTED", "NO_MATCH",
                ),
                int,
            )
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            with _sqlite_connection(path) as connection:
                validate_n15_terminal_graph(connection)
                current = connection.execute(
                    "SELECT b.scan_id,b.recorded_count,b.manifest_sha256 "
                    "FROM strategy_signal_batches b JOIN "
                    "strategy_signal_current c ON c.current_scan_id=b.scan_id "
                    "WHERE c.singleton_id=1"
                ).fetchone()
                permanent_hashes = {
                    table: signal_retention_module._table_full_hash(
                        connection, table
                    )
                    for table in (
                        "n15_snapshot_terminal_receipts",
                        "n15_snapshot_terminal_installation",
                        "n15_entry_states",
                    )
                }
            inspected = self._inspect_raw(
                str(Path(path).resolve()), *current,
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertEqual(inspected.retained_signal_count, 1)
            applied = self._apply_raw(
                str(Path(path).resolve()), *current,
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertEqual(applied.deleted_signal_count, 0)
            self._checkpoint_for_vacuum(path)
            destination = self._vacuum_destination(
                directory, "n15-terminal.compacted.sqlite3"
            )
            vacuumed = self._vacuum_raw(
                str(Path(path).resolve()), str(destination.resolve()),
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertTrue(vacuumed.vacuum_performed)
            with _sqlite_connection(path) as source, _sqlite_connection(
                destination
            ) as target:
                validate_n15_terminal_graph(source)
                validate_n15_terminal_graph(target)
                for table, expected in permanent_hashes.items():
                    self.assertEqual(
                        signal_retention_module._table_full_hash(source, table),
                        expected,
                    )
                    self.assertEqual(
                        signal_retention_module._table_full_hash(target, table),
                        expected,
                    )

    def test_pre_n15_terminal_database_requires_explicit_lossless_upgrade(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(self._database(directory))
            ledger = self._claim_ledger(path)
            recorder = ReviewRecorder(
                path,
                logging.getLogger("pre-n15-terminal-fixture"),
                n16_claim_ledger_file=ledger,
            )
            items, first_raws, _second_raws, _checked = (
                n15_seed_drift_windows()
            )
            no_winner_raws = deepcopy(first_raws)
            for raw in no_winner_raws.values():
                raw[120][1:5] = ["100", "100.2", "99.8", "100"]
            payload, snapshot = build_n15_snapshot(
                N15_STRATEGY, items, no_winner_raws
            )
            self.assertIsNone(snapshot.winner_symbol)
            self.assertEqual(
                recorder.record_n15_market_snapshot(
                    "N15", str(snapshot.e_open_time_ms), payload
                ),
                "OK",
            )
            with _sqlite_connection(path) as connection:
                frozen_payload = connection.execute(
                    "SELECT payload_json FROM n15_market_snapshots"
                ).fetchone()[0]
                for trigger in N15_TERMINAL_TRIGGER_SQL:
                    connection.execute('DROP TRIGGER "%s"' % trigger)
                for index in N15_TERMINAL_INDEX_SQL:
                    connection.execute('DROP INDEX "%s"' % index)
                for table in reversed(N15_TERMINAL_TABLES):
                    connection.execute('DROP TABLE "%s"' % table)
                connection.execute(
                    "ALTER TABLE n15_market_snapshots "
                    "DROP COLUMN payload_sha256"
                )
            seal_test_database_catalog(path, ledger)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    n15_terminal_schema_status(connection),
                    "PRE_N15_TERMINAL",
                )
                before = signal_retention_module._all_table_hashes(connection)
            with self.assertRaisesRegex(
                RuntimeError, "N15 terminal receipt schema requires explicit"
            ):
                ReviewRecorder(
                    path,
                    logging.getLogger("pre-n15-terminal-runtime-rejected"),
                    n16_claim_ledger_file=ledger,
                )
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    signal_retention_module._all_table_hashes(connection), before
                )
            _install_coverage_epoch_boundary(path, ledger)
            with _sqlite_connection(path) as connection:
                self.assertEqual(n15_terminal_schema_status(connection), "CURRENT")
                self.assertEqual(
                    connection.execute(
                        "SELECT payload_json FROM n15_market_snapshots"
                    ).fetchone()[0],
                    frozen_payload,
                )
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_snapshot_terminal_receipts"
                ).fetchone()[0], 0)
            upgraded = ReviewRecorder(
                path,
                logging.getLogger("post-n15-terminal-upgrade"),
                n16_claim_ledger_file=ledger,
            )
            self.assertTrue(upgraded.delete_n15_market_snapshots_before(
                "N15", str(snapshot.e_open_time_ms + 900_000)
            ))
            with _sqlite_connection(path) as connection:
                validate_n15_terminal_graph(connection)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_snapshot_terminal_receipts"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT winner_symbol,snapshot_payload_size,close_reason "
                    "FROM n15_snapshot_terminal_receipts"
                ).fetchone(), (
                    None,
                    len(frozen_payload.encode("utf-8")),
                    "N15_NO_WINNER_SNAPSHOT_CLOSED",
                ))
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_entry_states"
                ).fetchone()[0], 0)

    def test_real_n20_pre_micro_order_repairs_seven_n17_rows_before_install(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "review.sqlite3"
            ledger = self._claim_ledger(path)
            with _sqlite_connection(path):
                pass
            _install_n16_claim_boundary(path, ledger)
            _install_n17_lifecycle_boundary(path, ledger)
            _install_n19_lifecycle_boundary(path, ledger)
            _install_n18_lifecycle_boundary(path, ledger)
            _install_n20_lifecycle_boundary(path, ledger)
            with _sqlite_connection(path) as connection:
                self.assertEqual(micro_schema_status(connection), "PRE_MICRO")
                for index in range(7):
                    broken, _fixed = legacy_live_tail_record(
                        "LEGACY%02dUSDT" % index
                    )
                    connection.execute(
                        "INSERT INTO n17_range_support_states("
                        "strategy_id,symbol,family_id,structure_id,stage,reason,"
                        "quote_volume_rank,box_start_time_ms,box_end_time_ms,"
                        "reset_after_time_ms,evidence_json,evidence_sha256,"
                        "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            broken.strategy_id, broken.symbol, broken.family_id,
                            broken.structure_id, broken.stage, broken.reason,
                            broken.quote_volume_rank, broken.box_start_time_ms,
                            broken.box_end_time_ms, broken.reset_after_time_ms,
                            broken.evidence_json, broken.evidence_sha256,
                            _NOW, _NOW,
                        ),
                    )
                before_reverse = signal_retention_module._all_table_hashes(connection)
                before_catalog = _sqlite_schema_identity(connection)

            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError,
                "N17 lifecycle installation is required",
            ):
                _install_micro_lifecycle_boundary(path, ledger)
            with _sqlite_connection(path) as connection:
                self.assertEqual(micro_schema_status(connection), "PRE_MICRO")
                self.assertEqual(
                    signal_retention_module._all_table_hashes(connection),
                    before_reverse,
                )
                self.assertEqual(_sqlite_schema_identity(connection), before_catalog)

            original_plan = signal_retention_module._n17_evidence_repair_plan
            repair_plan_calls = 0

            def fail_pre_micro_transaction(connection, *, permit_legacy_repair):
                nonlocal repair_plan_calls
                if permit_legacy_repair:
                    repair_plan_calls += 1
                elif repair_plan_calls >= 2:
                    raise RuntimeError("injected PRE_MICRO repair failure")
                return original_plan(
                    connection, permit_legacy_repair=permit_legacy_repair
                )

            with patch.object(
                signal_retention_module,
                "_n17_evidence_repair_plan",
                side_effect=fail_pre_micro_transaction,
            ), self.assertRaisesRegex(RuntimeError, "PRE_MICRO repair failure"):
                _repair_n17_frozen_evidence_boundary(path, ledger)
            with _sqlite_connection(path) as connection:
                self.assertEqual(micro_schema_status(connection), "PRE_MICRO")
                self.assertEqual(
                    signal_retention_module._all_table_hashes(connection),
                    before_reverse,
                )
                self.assertEqual(_sqlite_schema_identity(connection), before_catalog)

            repaired = _repair_n17_frozen_evidence_boundary(path, ledger)
            self.assertEqual(repaired.repaired_row_count, 7)
            self.assertEqual(repaired.resolution, "REPAIRED")
            with _sqlite_connection(path) as connection:
                self.assertEqual(micro_schema_status(connection), "PRE_MICRO")
                rows = connection.execute(
                    "SELECT symbol,evidence_json FROM n17_range_support_states "
                    "ORDER BY symbol"
                ).fetchall()
                self.assertEqual(len(rows), 7)
                for symbol, evidence_json in rows:
                    self.assertEqual(
                        decode_n17_state_evidence(
                            evidence_json, expected_symbol=symbol
                        ).symbol,
                        symbol,
                    )
                n17_hash = signal_retention_module._table_full_hash(
                    connection, "n17_range_support_states"
                )

            pre_micro_replay = _repair_n17_frozen_evidence_boundary(path, ledger)
            self.assertEqual(pre_micro_replay.repaired_row_count, 0)
            self.assertEqual(pre_micro_replay.resolution, "ALREADY_CURRENT")
            _install_micro_lifecycle_boundary(path, ledger)
            with _sqlite_connection(path) as connection:
                self.assertEqual(micro_schema_status(connection), "CURRENT")
            current_replay = _repair_n17_frozen_evidence_boundary(path, ledger)
            self.assertEqual(current_replay.repaired_row_count, 0)
            self.assertEqual(current_replay.resolution, "ALREADY_CURRENT")
            _install_coverage_epoch_boundary(path, ledger)

            # Retention fixtures must preserve the real foreign-key graph;
            # this database was intentionally built generation-by-generation
            # instead of through _database(), so seed the three source scans
            # only after the deployment-order assertions above are complete.
            with _sqlite_connection(path) as connection:
                connection.execute(
                    "UPDATE strategy_signal_current SET current_scan_id=NULL, "
                    "retention_active=0, migration_state='PENDING', "
                    "migration_cutoff_signal_id=NULL, source_signal_count=NULL, "
                    "source_passed_count=NULL, source_manifest_sha256=NULL, "
                    "retention_origin=NULL WHERE singleton_id=1"
                )
                for scan_id in (1, 2, 3):
                    connection.execute(
                        "INSERT INTO scans (id, started_at, completed_at, mode, "
                        "scanned_count, candidate_count, candidates_json, opened, note) "
                        "VALUES (?, ?, ?, 'DRY_RUN', 100, 100, '[]', 0, ?)",
                        (
                            scan_id,
                            "2026-07-14T00:0%d:00+00:00" % scan_id,
                            "2026-07-14T00:0%d:30+00:00" % scan_id,
                            "scan-%d" % scan_id,
                        ),
                    )
            self._seed_standard_history(path)
            self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    signal_retention_module._table_full_hash(
                        connection, "n17_range_support_states"
                    ),
                    n17_hash,
                )

            recorder = ReviewRecorder(
                str(path),
                logging.getLogger("pre-micro-n17-staging-retention"),
                n16_claim_ledger_file=ledger,
            )
            staging_scan = recorder.begin_scan(1500, [], True)
            self.assertEqual(staging_scan, 4)
            window = MicroObservationTests._n21_window("STAGINGMICROUSDT", rank=3)
            analysis = analyze_n21(
                window, window.observations[-1].observed_at_ms
            )
            self.assertTrue(analysis.passed, analysis.reason)
            self.assertTrue(
                recorder.record_micro_passed_analysis(staging_scan, analysis)
            )
            self.assertIsNotNone(
                recorder.record_strategy_signal(
                    scan_id=staging_scan,
                    strategy_id="N21",
                    symbol=analysis.symbol,
                    funding_rate="",
                    matched_patterns=(),
                    trend_slope="",
                    current_bullish=True,
                    passed=True,
                    decision="PASSED",
                    reason="PASSED",
                    structure_id=analysis.structure_id,
                    detail=analysis.detail_json(),
                )
            )
            for index in range(1499):
                self.assertIsNotNone(
                    recorder.record_strategy_signal(
                        scan_id=staging_scan,
                        strategy_id="N06",
                        symbol="R%04dUSDT" % index,
                        funding_rate="",
                        matched_patterns=(),
                        trend_slope="",
                        current_bullish=False,
                        passed=False,
                        decision="REJECTED",
                        reason="NO_MATCH",
                        structure_id=None,
                        detail={"staging_fixture": True},
                    )
                )
            with _sqlite_connection(path) as connection:
                before_failed_cleanup = signal_retention_module._all_table_hashes(
                    connection
                )

            original_cleanup = signal_retention_module._delete_staged_strategy_signal_batch

            def fail_after_staging_delete(*args, **kwargs):
                original_cleanup(*args, **kwargs)
                raise RuntimeError("injected maintenance STAGING cleanup failure")

            with patch.object(
                signal_retention_module,
                "_delete_staged_strategy_signal_batch",
                side_effect=fail_after_staging_delete,
            ), self.assertRaisesRegex(RuntimeError, "STAGING cleanup failure"):
                self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    signal_retention_module._all_table_hashes(connection),
                    before_failed_cleanup,
                )

            dry_run = self._inspect(path, 3)
            self.assertTrue(dry_run.staging_cleanup_required)
            self.assertEqual(dry_run.staging_scan_id, staging_scan)
            self.assertEqual(dry_run.staging_signal_count, 1500)
            self.assertEqual(dry_run.staging_passed_claim_count, 1)
            self.assertEqual(dry_run.staging_ledger_claim_count, 1)
            self.assertEqual(dry_run.staging_micro_claim_count, 1)
            applied_staging = self._apply(path, 3)
            self.assertTrue(applied_staging.staging_cleanup_performed)
            self.assertEqual(applied_staging.deleted_signal_count, 1500)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_passed_analyses "
                        "WHERE source_scan_id=?",
                        (staging_scan,),
                    ).fetchone(),
                    (1,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM micro_strategy_lifecycle "
                        "WHERE source_scan_id=?",
                        (staging_scan,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id=?",
                        (staging_scan,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signal_batches "
                        "WHERE scan_id=?",
                        (staging_scan,),
                    ).fetchone(),
                    (0,),
                )
            self._checkpoint_for_vacuum(path)
            destination = self._vacuum_destination(
                directory, "pre-micro-n17-repaired.compacted.sqlite3"
            )
            report = self._vacuum_raw(
                str(path.resolve()),
                str(destination.resolve()),
                n16_claim_ledger=str(ledger.resolve()),
            )
            self.assertTrue(report.vacuum_performed)
            with _sqlite_connection(destination) as connection:
                self.assertEqual(
                    signal_retention_module._table_full_hash(
                        connection, "n17_range_support_states"
                    ),
                    n17_hash,
                )

    def test_n17_legacy_live_tail_requires_explicit_repair_before_retention(self):
        def insert_legacy_row(path, record):
            with _sqlite_connection(path) as connection:
                connection.execute(
                    "INSERT INTO n17_range_support_states("
                    "strategy_id,symbol,family_id,structure_id,stage,reason,"
                    "quote_volume_rank,box_start_time_ms,box_end_time_ms,"
                    "reset_after_time_ms,evidence_json,evidence_sha256,"
                    "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        record.strategy_id,
                        record.symbol,
                        record.family_id,
                        record.structure_id,
                        record.stage,
                        record.reason,
                        record.quote_volume_rank,
                        record.box_start_time_ms,
                        record.box_end_time_ms,
                        record.reset_after_time_ms,
                        record.evidence_json,
                        record.evidence_sha256,
                        _NOW,
                        _NOW,
                    ),
                )

        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            self._apply(path, 3)
            broken, fixed = legacy_live_tail_record()
            insert_legacy_row(path, broken)
            count, manifest = self._attestation(path, 3)
            with _sqlite_connection(path) as connection:
                before_failed_maintenance = signal_retention_module._all_table_hashes(
                    connection
                )
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError,
                "N17 lifecycle installation is required",
            ):
                self._inspect_raw(
                    str(path.resolve()),
                    3,
                    count,
                    manifest,
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    signal_retention_module._all_table_hashes(connection),
                    before_failed_maintenance,
                )

            original_plan = signal_retention_module._n17_evidence_repair_plan
            repair_plan_calls = 0

            def fail_after_update(connection, *, permit_legacy_repair):
                nonlocal repair_plan_calls
                if permit_legacy_repair:
                    repair_plan_calls += 1
                elif repair_plan_calls >= 2:
                    raise RuntimeError("injected N17 post-update validation failure")
                return original_plan(
                    connection, permit_legacy_repair=permit_legacy_repair
                )

            with patch.object(
                signal_retention_module,
                "_n17_evidence_repair_plan",
                side_effect=fail_after_update,
            ), self.assertRaisesRegex(RuntimeError, "post-update"):
                _repair_n17_frozen_evidence_boundary(
                    path, self._claim_ledger(path)
                )
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    signal_retention_module._all_table_hashes(connection),
                    before_failed_maintenance,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM sqlite_schema WHERE type='trigger' "
                        "AND name='trg_n17_state_identity_immutable'"
                    ).fetchone(),
                    (1,),
                )

            report = _repair_n17_frozen_evidence_boundary(
                path, self._claim_ledger(path)
            )
            self.assertEqual(report.repaired_row_count, 1)
            with _sqlite_connection(path) as connection:
                repaired = connection.execute(
                    "SELECT family_id,structure_id,stage,reason,evidence_json,"
                    "evidence_sha256 FROM n17_range_support_states"
                ).fetchone()
                self.assertEqual(repaired[:4], (
                    fixed.family_id,
                    fixed.structure_id,
                    fixed.stage,
                    fixed.reason,
                ))
                self.assertEqual(repaired[4:], (
                    fixed.evidence_json,
                    fixed.evidence_sha256,
                ))
                permanent_after_repair = {
                    table: signal_retention_module._table_full_hash(connection, table)
                    for table in (
                        "n17_range_support_states",
                        "strategy_passed_signal_audits",
                        "strategy_passed_structure_ledger",
                        "n16_consumption_seals",
                        "n16_trend_support_states",
                        "micro_passed_analyses",
                        "micro_strategy_lifecycle",
                    )
                }

            dry_run = self._inspect_raw(
                str(path.resolve()),
                3,
                count,
                manifest,
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertTrue(dry_run.keep_attestation_verified)
            applied = self._apply_raw(
                str(path.resolve()),
                3,
                count,
                manifest,
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertTrue(applied.keep_attestation_verified)
            self._checkpoint_for_vacuum(path)
            destination = self._vacuum_destination(
                directory, "n17-repaired.compacted.sqlite3"
            )
            vacuumed = self._vacuum_raw(
                str(path.resolve()),
                str(destination.resolve()),
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertTrue(vacuumed.vacuum_performed)
            with _sqlite_connection(path) as source, _sqlite_connection(
                destination
            ) as target:
                for table, expected_hash in permanent_after_repair.items():
                    self.assertEqual(
                        signal_retention_module._table_full_hash(source, table),
                        expected_hash,
                    )
                    self.assertEqual(
                        signal_retention_module._table_full_hash(target, table),
                        expected_hash,
                    )

        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            broken, _fixed = legacy_live_tail_record("HOSTILEUSDT")
            hostile = json.loads(broken.evidence_json)
            hostile["source"][0]["high"] = str(
                float(hostile["source"][0]["high"]) + 0.01
            )
            unsigned = dict(hostile)
            unsigned.pop("canonical_sha256")
            hostile["canonical_sha256"] = hashlib.sha256(
                json.dumps(
                    unsigned,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            ).hexdigest()
            hostile_record = type(broken)(
                strategy_id=broken.strategy_id,
                symbol=broken.symbol,
                family_id=broken.family_id,
                structure_id=broken.structure_id,
                stage=broken.stage,
                reason=broken.reason,
                quote_volume_rank=broken.quote_volume_rank,
                box_start_time_ms=broken.box_start_time_ms,
                box_end_time_ms=broken.box_end_time_ms,
                reset_after_time_ms=broken.reset_after_time_ms,
                evidence=hostile,
            )
            insert_legacy_row(path, hostile_record)
            with _sqlite_connection(path) as connection:
                before = signal_retention_module._all_table_hashes(connection)
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "not uniquely repairable"
            ):
                _repair_n17_frozen_evidence_boundary(
                    path, self._claim_ledger(path)
                )
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    signal_retention_module._all_table_hashes(connection), before
                )

    def test_pre_micro_unrepairable_n17_is_zero_write_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "review.sqlite3"
            ledger = self._claim_ledger(path)
            with _sqlite_connection(path):
                pass
            _install_n16_claim_boundary(path, ledger)
            _install_n17_lifecycle_boundary(path, ledger)
            _install_n19_lifecycle_boundary(path, ledger)
            _install_n18_lifecycle_boundary(path, ledger)
            _install_n20_lifecycle_boundary(path, ledger)

            broken, _fixed = legacy_live_tail_record("PREHOSTILEUSDT")
            hostile = json.loads(broken.evidence_json)
            hostile["source"][0]["high"] = str(
                float(hostile["source"][0]["high"]) + 0.01
            )
            unsigned = dict(hostile)
            unsigned.pop("canonical_sha256")
            hostile["canonical_sha256"] = hashlib.sha256(
                json.dumps(
                    unsigned,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            ).hexdigest()
            hostile_record = type(broken)(
                strategy_id=broken.strategy_id,
                symbol=broken.symbol,
                family_id=broken.family_id,
                structure_id=broken.structure_id,
                stage=broken.stage,
                reason=broken.reason,
                quote_volume_rank=broken.quote_volume_rank,
                box_start_time_ms=broken.box_start_time_ms,
                box_end_time_ms=broken.box_end_time_ms,
                reset_after_time_ms=broken.reset_after_time_ms,
                evidence=hostile,
            )
            with _sqlite_connection(path) as connection:
                connection.execute(
                    "INSERT INTO n17_range_support_states("
                    "strategy_id,symbol,family_id,structure_id,stage,reason,"
                    "quote_volume_rank,box_start_time_ms,box_end_time_ms,"
                    "reset_after_time_ms,evidence_json,evidence_sha256,"
                    "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        hostile_record.strategy_id,
                        hostile_record.symbol,
                        hostile_record.family_id,
                        hostile_record.structure_id,
                        hostile_record.stage,
                        hostile_record.reason,
                        hostile_record.quote_volume_rank,
                        hostile_record.box_start_time_ms,
                        hostile_record.box_end_time_ms,
                        hostile_record.reset_after_time_ms,
                        hostile_record.evidence_json,
                        hostile_record.evidence_sha256,
                        _NOW,
                        _NOW,
                    ),
                )
                self.assertEqual(micro_schema_status(connection), "PRE_MICRO")
                before = signal_retention_module._all_table_hashes(connection)
                catalog = _sqlite_schema_identity(connection)

            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "not uniquely repairable"
            ):
                _repair_n17_frozen_evidence_boundary(path, ledger)
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError,
                "N17 lifecycle installation is required",
            ):
                _install_micro_lifecycle_boundary(path, ledger)
            with _sqlite_connection(path) as connection:
                self.assertEqual(micro_schema_status(connection), "PRE_MICRO")
                self.assertEqual(
                    signal_retention_module._all_table_hashes(connection), before
                )
                self.assertEqual(_sqlite_schema_identity(connection), catalog)

    def test_post_cutover_micro_graph_tampering_is_zero_write_rejected(self):
        cases = (
            (
                "micro_passed_analyses",
                "trg_micro_analysis_immutable",
            ),
            (
                "micro_strategy_lifecycle",
                "trg_micro_lifecycle_immutable",
            ),
        )
        for table, trigger in cases:
            with self.subTest(table=table), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                self._seed_standard_history(path)
                self._apply(path, 3)
                _recorder, scan_id, manifest = self._publish_micro_current(path)
                with _sqlite_connection(path) as connection:
                    connection.execute(f'DROP TRIGGER "{trigger}"')
                    connection.execute(
                        f'UPDATE "{table}" SET evidence_json=? '
                        "WHERE strategy_id='N21'",
                        ('{"tampered":true}',),
                    )
                    connection.execute(MICRO_TRIGGER_SQL[trigger])
                self._checkpoint_for_vacuum(path)
                before = _file_fingerprint(path)
                directory_before = _directory_fingerprint(Path(directory))
                for operation in ("inspect", "apply"):
                    with self.subTest(operation=operation), self.assertRaises(
                        (RuntimeError, SignalRetentionMaintenanceError)
                    ):
                        if operation == "inspect":
                            self._inspect_raw(
                                str(path.resolve()), scan_id, 5, manifest,
                                n16_claim_ledger=str(
                                    self._claim_ledger(path).resolve()
                                ),
                            )
                        else:
                            self._apply_raw(
                                str(path.resolve()), scan_id, 5, manifest,
                                n16_claim_ledger=str(
                                    self._claim_ledger(path).resolve()
                                ),
                            )
                    self.assertEqual(_file_fingerprint(path), before)
                    self.assertEqual(
                        _directory_fingerprint(Path(directory)), directory_before
                    )

    def test_completed_legacy_cutoff_rejects_every_post_legacy_strategy(self):
        for forged_strategy in (
            "N16", "N21", "N22", "N23", "N24", "N25",
        ):
            with self.subTest(
                strategy=forged_strategy
            ), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                self._seed_standard_history(path)
                self._apply(path, 3)
                recorder = ReviewRecorder(
                    str(path),
                    logging.getLogger("legacy-cutoff-impossible-strategy"),
                    n16_claim_ledger_file=self._claim_ledger(path),
                )
                scan_id = recorder.begin_scan(1, [], True)
                self.assertIsNotNone(scan_id)
                self.assertIsNotNone(
                    recorder.record_strategy_signal(
                        scan_id,
                        forged_strategy,
                        "POSTCUTOVERUSDT",
                        "-0.01",
                        (),
                        "0",
                        False,
                        False,
                        "REJECTED",
                        forged_strategy + "_NO_ACTIVE_STRUCTURE",
                        detail={"post_cutover": True},
                    )
                )
                self.assertTrue(
                    recorder.publish_strategy_signal_batch(scan_id, 1)
                )
                with _sqlite_connection(path) as connection:
                    cutoff = connection.execute(
                        "SELECT migration_cutoff_signal_id "
                        "FROM strategy_signal_current WHERE singleton_id=1"
                    ).fetchone()[0]
                    connection.execute(
                        "INSERT INTO strategy_signals ("
                        "id,scan_id,strategy_id,symbol,funding_rate,matched_patterns,"
                        "trend_slope,current_bullish,passed,decision,reason,structure_id,"
                        "detail_json,created_at) VALUES "
                        "(?,?,?,'FORGEDCUTOFFUSDT','-0.01','[]','0',0,0,"
                        "'REJECTED',?,NULL,'{}',?)",
                        (
                            cutoff,
                            scan_id,
                            forged_strategy,
                            forged_strategy + "_FORGED_LEGACY",
                            _NOW,
                        ),
                    )
                    manifest = connection.execute(
                        "SELECT manifest_sha256 FROM strategy_signal_batches "
                        "WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone()[0]
                self._checkpoint_for_vacuum(path)
                before = _file_fingerprint(path)
                directory_before = _directory_fingerprint(Path(directory))
                with self.assertRaisesRegex(
                    SignalRetentionMaintenanceError,
                    "impossible post-legacy strategy evidence",
                ):
                    self._inspect_raw(
                        str(path.resolve()),
                        scan_id,
                        2,
                        manifest,
                        n16_claim_ledger=str(
                            self._claim_ledger(path).resolve()
                        ),
                    )
                self.assertEqual(_file_fingerprint(path), before)
                self.assertEqual(
                    _directory_fingerprint(Path(directory)), directory_before
                )

    def test_legacy_current_cannot_cross_migration_cutoff(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                first_id, last_id = connection.execute(
                    "SELECT first_signal_id, last_signal_id "
                    "FROM strategy_signal_batches WHERE state='CURRENT'"
                ).fetchone()
                self.assertLess(first_id, last_id)
                connection.execute(
                    "UPDATE strategy_signal_current "
                    "SET migration_cutoff_signal_id = ? WHERE singleton_id=1",
                    (first_id,),
                )
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError,
                "crosses the migration cutoff",
            ):
                self._inspect(path, 3)

    def test_originless_completed_legacy_is_upgraded_only_by_maintenance(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                marker_before = connection.execute(
                    """
                    SELECT migration_cutoff_signal_id, source_signal_count,
                           source_passed_count, source_manifest_sha256
                    FROM strategy_signal_current WHERE singleton_id=1
                    """
                ).fetchone()
                connection.execute(
                    "ALTER TABLE strategy_signal_current "
                    "DROP COLUMN retention_origin"
                )
                schema_before = connection.execute(
                    "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                    "ORDER BY type COLLATE BINARY, name COLLATE BINARY"
                ).fetchall()
            self._advance_test_protected_generation(path)
            database_sha256_before = _file_sha256(path)
            with self.assertRaisesRegex(
                RuntimeError,
                "explicit maintenance upgrade",
            ):
                ReviewRecorder(
                    str(path),
                    logging.getLogger("originless-runtime-must-not-upgrade"),
                    n16_claim_ledger_file=self._claim_ledger(path),
                )
            self.assertEqual(_file_sha256(path), database_sha256_before)
            with _sqlite_connection(path) as connection:
                schema_after_runtime = connection.execute(
                    "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                    "ORDER BY type COLLATE BINARY, name COLLATE BINARY"
                ).fetchall()
                self.assertNotIn(
                    "retention_origin",
                    [
                        row[1]
                        for row in connection.execute(
                            "PRAGMA table_info(strategy_signal_current)"
                        )
                    ],
                )
            self.assertEqual(schema_after_runtime, schema_before)
            dry_run = self._inspect(path, 3)
            self.assertFalse(dry_run.schema_ready)
            self.assertEqual(dry_run.migration_state, "COMPLETE")
            applied = self._apply(path, 3)
            self.assertEqual(applied.deleted_signal_count, 0)
            with _sqlite_connection(path) as connection:
                marker_after = connection.execute(
                    """
                    SELECT migration_cutoff_signal_id, source_signal_count,
                           source_passed_count, source_manifest_sha256,
                           retention_origin
                    FROM strategy_signal_current WHERE singleton_id=1
                    """
                ).fetchone()
            self.assertEqual(marker_after, marker_before + ("LEGACY",))
            retry = self._apply(path, 3)
            self.assertEqual(retry.deleted_signal_count, 0)

    def test_claim_state_leading_indexes_require_explicit_maintenance_upgrade(self):
        required = (
            (
                "idx_passed_audit_claim_state_scan",
                "strategy_passed_signal_audits",
            ),
            (
                "idx_passed_ledger_claim_state_scan",
                "strategy_passed_structure_ledger",
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                pragma_before = (
                    connection.execute("PRAGMA application_id").fetchone(),
                    connection.execute("PRAGMA user_version").fetchone(),
                )
                for name, _table in required:
                    connection.execute(f'DROP INDEX "{name}"')
                schema_without_indexes = _sqlite_schema_identity(connection)
            self._advance_test_protected_generation(path)
            database_before_runtime = _file_sha256(path)
            with self.assertRaisesRegex(
                RuntimeError, "explicit maintenance verification"
            ):
                ReviewRecorder(
                    path,
                    logging.getLogger("missing-claim-state-index-runtime"),
                    n16_claim_ledger_file=self._claim_ledger(path),
                )
            self.assertEqual(_file_sha256(path), database_before_runtime)
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "index"
            ):
                self._inspect(path, 3)
            report = self._apply(path, 3)
            self.assertEqual(report.deleted_signal_count, 0)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    (
                        connection.execute("PRAGMA application_id").fetchone(),
                        connection.execute("PRAGMA user_version").fetchone(),
                    ),
                    pragma_before,
                )
                self.assertNotEqual(
                    _sqlite_schema_identity(connection), schema_without_indexes
                )
                for name, table in required:
                    plan = connection.execute(
                        "EXPLAIN QUERY PLAN SELECT source_scan_id FROM "
                        f"{table} WHERE claim_state='STAGED'"
                    ).fetchall()
                    self.assertTrue(
                        any(name in row[3] for row in plan),
                        plan,
                    )
            ReviewRecorder(
                path,
                logging.getLogger("upgraded-claim-state-index-runtime"),
                n16_claim_ledger_file=self._claim_ledger(path),
            )

    def test_wrong_claim_state_leading_index_is_prewrite_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                connection.execute(
                    "DROP INDEX idx_passed_audit_claim_state_scan"
                )
                connection.execute(
                    "CREATE INDEX idx_passed_audit_claim_state_scan "
                    "ON strategy_passed_signal_audits(source_scan_id,claim_state)"
                )
            self._advance_test_protected_generation(path)
            before = _file_fingerprint(path)
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "index"
            ):
                self._apply(path, 3)
            self.assertEqual(_file_fingerprint(path), before)

    def test_originless_completed_legacy_corruption_rejects_before_schema_write(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                connection.execute(
                    "ALTER TABLE strategy_signal_current "
                    "DROP COLUMN retention_origin"
                )
                connection.execute(
                    "UPDATE strategy_passed_signal_audits "
                    "SET evidence_sha256 = ? WHERE id = "
                    "(SELECT MIN(id) FROM strategy_passed_signal_audits)",
                    ("f" * 64,),
                )
                schema_before = connection.execute(
                    "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                    "ORDER BY type, name"
                ).fetchall()
                data_before = {
                    table: _rows(connection, table)
                    for table in (
                        "strategy_signals",
                        "strategy_signal_current",
                        "strategy_signal_batches",
                        "strategy_passed_signal_audits",
                        "strategy_passed_structure_ledger",
                        "sqlite_sequence",
                    )
                }
            with self.assertRaises(SignalRetentionMaintenanceError):
                self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                schema_after = connection.execute(
                    "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                    "ORDER BY type, name"
                ).fetchall()
                self.assertNotIn(
                    "retention_origin",
                    [row[1] for row in connection.execute(
                        "PRAGMA table_info(strategy_signal_current)"
                    )],
                )
                self.assertEqual(
                    {
                        table: _rows(connection, table)
                        for table in data_before
                    },
                    data_before,
                )
            self.assertEqual(schema_after, schema_before)

    def test_absent_legacy_history_requires_cli_backfill_before_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            ids = self._seed_standard_history(path)
            self.assertEqual(len(ids), 9)
            count, manifest = self._attestation(path, 3)
            connection = sqlite3.connect(path)
            try:
                _drop_empty_n16_schema_for_legacy_fixture(connection)
                connection.execute("DROP INDEX idx_strategy_signals_scan_id")
                for table in (
                    "strategy_signal_current",
                    "strategy_signal_batches",
                    "strategy_passed_signal_audits",
                    "strategy_passed_structure_ledger",
                ):
                    connection.execute("DROP TABLE %s" % table)
                self.assertEqual(
                    signal_retention_module._retention_schema_version(connection),
                    "ABSENT",
                )
                self.assertFalse(
                    _strict_absent_retention_history_is_empty(connection)
                )
                connection.commit()
            finally:
                connection.close()
            schema_before = sqlite3.connect(path)
            try:
                catalog_before = schema_before.execute(
                    "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                    "ORDER BY type COLLATE BINARY, name COLLATE BINARY"
                ).fetchall()
            finally:
                schema_before.close()
            database_before = _file_sha256(path)
            with self.assertRaisesRegex(RuntimeError, "maintenance"):
                ReviewRecorder(
                    str(path),
                    logging.getLogger("absent-legacy-runtime"),
                    n16_claim_ledger_file=self._claim_ledger(path),
                )
            self.assertEqual(_file_sha256(path), database_before)
            connection = sqlite3.connect(path)
            try:
                self.assertEqual(
                    connection.execute(
                        "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                        "ORDER BY type COLLATE BINARY, name COLLATE BINARY"
                    ).fetchall(),
                    catalog_before,
                )
            finally:
                connection.close()
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "N16 lifecycle"
            ):
                self._inspect_raw(str(path.resolve()), 3, count, manifest,
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),)
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "N16 lifecycle"
            ):
                self._apply_raw(
                    str(path.resolve()), 3, count, manifest, batch_size=2,
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
            self.assertEqual(_file_sha256(path), database_before)

    def test_preclaim_legacy_requires_n16_installation_before_retention_writes(self):
        for mode in ("valid", "wrong_index"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                self._seed_standard_history(path)
                count, manifest = self._attestation(path, 3)
                with _sqlite_connection(path) as connection:
                    _drop_empty_n16_schema_for_legacy_fixture(connection)
                    connection.execute("DROP INDEX idx_passed_audit_claim_batch")
                    connection.execute("DROP INDEX idx_passed_ledger_claim_batch")
                    connection.execute(
                        "DROP INDEX idx_passed_audit_claim_state_scan"
                    )
                    connection.execute(
                        "DROP INDEX idx_passed_ledger_claim_state_scan"
                    )
                    connection.execute(
                        "ALTER TABLE strategy_passed_signal_audits "
                        "DROP COLUMN claim_state"
                    )
                    connection.execute(
                        "ALTER TABLE strategy_passed_structure_ledger "
                        "DROP COLUMN claim_state"
                    )
                    connection.execute(
                        "ALTER TABLE strategy_signal_current "
                        "DROP COLUMN retention_origin"
                    )
                    if mode == "wrong_index":
                        connection.execute("DROP INDEX idx_strategy_signals_scan_id")
                        connection.execute(
                            "CREATE INDEX idx_strategy_signals_scan_id "
                            "ON strategy_signals(symbol)"
                        )
                    catalog_before = connection.execute(
                        "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                        "ORDER BY type COLLATE BINARY, name COLLATE BINARY"
                    ).fetchall()
                    data_before = {
                        table: _rows(connection, table)
                        for table in (
                            "strategy_signals",
                            "strategy_signal_current",
                            "strategy_signal_batches",
                            "strategy_passed_signal_audits",
                            "strategy_passed_structure_ledger",
                            "sqlite_sequence",
                        )
                    }
                self._checkpoint_for_vacuum(path)
                before = (
                    _file_fingerprint(path),
                    _directory_fingerprint(path.parent),
                )
                for operation in (
                    lambda: self._inspect_raw(
                        str(path.resolve()), 3, count, manifest,
                                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    ),
                    lambda: self._apply_raw(
                        str(path.resolve()), 3, count, manifest, batch_size=2,
                                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    ),
                ):
                    with self.assertRaisesRegex(
                        SignalRetentionMaintenanceError, "N16 lifecycle"
                    ):
                        operation()
                    self.assertEqual(
                        (
                            _file_fingerprint(path),
                            _directory_fingerprint(path.parent),
                        ),
                        before,
                    )
                with _sqlite_connection(path) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                            "ORDER BY type COLLATE BINARY, name COLLATE BINARY"
                        ).fetchall(),
                        catalog_before,
                    )
                    self.assertEqual(
                        {table: _rows(connection, table) for table in data_before},
                        data_before,
                    )

    def test_complete_maintenance_rejects_immutable_current_identity_tamper(self):
        cases = (
            ("symbol", "FORGEDUSDT"),
            ("scan_id", 2),
            ("strategy_id", "N05"),
            ("passed", 0),
            ("structure_id", "forged-structure"),
        )
        for column, value in cases:
            with self.subTest(column=column), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                ids = self._seed_standard_history(path)
                count, manifest = self._attestation(path, 3)
                self._apply_raw(
                    str(path.resolve()), 3, count, manifest,
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
                with _sqlite_connection(path) as connection:
                    connection.execute(
                        "UPDATE strategy_signals SET %s = ? WHERE id = ?" % column,
                        (value, ids[-1]),
                    )
                before = _file_sha256(path)
                with self.assertRaises(SignalRetentionMaintenanceError):
                    self._inspect_raw(
                        str(path.resolve()), 3, count, manifest,
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )
                self.assertEqual(_file_sha256(path), before)

    def test_backfilled_current_accepts_older_same_symbol_canonical_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            old_id = self._signal(
                path, 1, "N11", "BTCUSDT", 1, "same-current-structure"
            )
            self._signal(
                path, 3, "N11", "BTCUSDT", 1, "same-current-structure"
            )
            count, manifest = self._attestation(path, 3)
            self._apply_raw(
                str(path.resolve()), 3, count, manifest,
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT source_signal_id, source_scan_id, claim_state "
                        "FROM strategy_passed_structure_ledger"
                    ).fetchone(),
                    (old_id, 1, "ACTIVE"),
                )
            self.assertTrue(
                self._inspect_raw(
                    str(path.resolve()), 3, count, manifest,
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                ).keep_attestation_verified
            )

    def test_same_structure_with_different_symbols_blocks_before_backfill(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._signal(path, 1, "N06", "BTCUSDT", 1, "same-structure")
            self._signal(path, 2, "N06", "ETHUSDT", 1, "same-structure")
            self._signal(path, 3, "N01", "CURRENTUSDT", 0)
            before = path.read_bytes()
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "multiple symbols"
            ):
                self._apply(path, 3)
            self.assertNotEqual(path.read_bytes(), b"")
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT retention_active, migration_state "
                        "FROM strategy_signal_current"
                    ).fetchone(),
                    (0, "PENDING"),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits"
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM strategy_signals").fetchone()[0],
                    3,
                )
            self.assertTrue(before)

    def test_same_structure_and_symbol_backfills_once_with_deterministic_source(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            first_id = self._signal(
                path, 1, "N11", "BTCUSDT", 1, "same-structure"
            )
            self._signal(path, 2, "N11", "BTCUSDT", 1, "same-structure")
            self._signal(path, 3, "N01", "CURRENTUSDT", 0)
            report = self._apply(path, 3)
            self.assertEqual(report.audit_count, 2)
            self.assertEqual(report.ledger_count, 1)
            with _sqlite_connection(path) as connection:
                ledger_before = _rows(
                    connection, "strategy_passed_structure_ledger"
                )
                self.assertEqual(ledger_before[0][4], first_id)
            self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    _rows(connection, "strategy_passed_structure_ledger"),
                    ledger_before,
                )

    def test_existing_audit_or_ledger_identity_conflict_blocks_all_deletion(self):
        for conflict in ("audit", "ledger"):
            with self.subTest(conflict=conflict), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                source_id = self._signal(
                    path, 1, "N06", "BTCUSDT", 1, "strict-structure"
                )
                self._signal(path, 3, "N01", "CURRENTUSDT", 0)
                with _sqlite_connection(path) as connection:
                    source = connection.execute(
                        "SELECT id, scan_id, strategy_id, symbol, funding_rate, "
                        "matched_patterns, trend_slope, current_bullish, passed, "
                        "decision, reason, structure_id, detail_json, created_at "
                        "FROM strategy_signals WHERE id = ?",
                        (source_id,),
                    ).fetchone()
                    if conflict == "audit":
                        self._insert_audit(connection, source, symbol="ETHUSDT")
                    else:
                        evidence_sha = self._insert_audit(connection, source)
                        connection.execute(
                            "INSERT INTO strategy_passed_structure_ledger "
                            "(strategy_id, symbol, structure_id, source_signal_id, "
                            "source_scan_id, source_signal_created_at, evidence_sha256, "
                            "created_at) VALUES ('N06', 'ETHUSDT', 'strict-structure', "
                            "?, 1, ?, ?, ?)",
                            (source_id, _NOW, evidence_sha, _NOW),
                        )
                with self.assertRaises(SignalRetentionMaintenanceError):
                    self._apply(path, 3)
                with _sqlite_connection(path) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT retention_active, migration_state "
                            "FROM strategy_signal_current"
                        ).fetchone(),
                        (0, "PENDING"),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_signals"
                        ).fetchone(),
                        (2,),
                    )

    def test_invalid_sqlite_types_and_json_block_without_deletion(self):
        cases = (
            ("passed", "not-an-integer"),
            ("matched_patterns", "{}"),
            ("detail_json", "{\"x\":NaN}"),
            ("structure_id", ""),
            ("strategy_id", "N21"),
            ("strategy_id", ""),
            ("symbol", "btcusdt"),
            ("symbol", "BTC/USD"),
        )
        for column, value in cases:
            with self.subTest(column=column), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                source_id = self._signal(
                    path, 1, "N06", "BTCUSDT", 1, "strict-structure"
                )
                self._signal(path, 3, "N01", "CURRENTUSDT", 0)
                with _sqlite_connection(path) as connection:
                    connection.execute(
                        "UPDATE strategy_signals SET %s = ? WHERE id = ?" % column,
                        (value, source_id),
                    )
                with self.assertRaises(SignalRetentionMaintenanceError):
                    self._apply(path, 3)
                with _sqlite_connection(path) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_signals"
                        ).fetchone()[0],
                        2,
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT migration_state FROM strategy_signal_current"
                        ).fetchone(),
                        ("PENDING",),
                    )

    def test_str_subclasses_are_rejected_by_signal_identity_validator(self):
        class HostileString(str):
            pass

        valid = (
            1,
            1,
            "N06",
            "BTCUSDT",
            "-0.01",
            "[]",
            "1",
            1,
            1,
            "PASSED",
            "PASSED",
            "structure-1",
            "{}",
            _NOW,
        )
        for index in (2, 3, 11):
            with self.subTest(index=index):
                hostile = list(valid)
                hostile[index] = HostileString(hostile[index])
                with self.assertRaises(SignalRetentionMaintenanceError):
                    _validate_signal_row(tuple(hostile))

    def test_exact_unicode_exchange_symbol_survives_retention_validation(self):
        valid = (
            1, 1, "N16", "龙虾USDT", "-0.01", "[]", "1", 1, 0,
            "REJECTED", "N16_STRUCTURE_NOT_FOUND", None, "{}", _NOW,
        )
        self.assertEqual(_validate_signal_row(valid)[3], "龙虾USDT")
        for hostile in ("龙虾/USDT", "龙虾\nUSDT", "ＡUSDT"):
            row = list(valid)
            row[3] = hostile
            with self.subTest(hostile=repr(hostile)), self.assertRaises(
                SignalRetentionMaintenanceError
            ):
                _validate_signal_row(tuple(row))

        ledger = (
            1, "N16", "龙虾USDT", "structure-1", 1, 1, _NOW,
            "a" * 64, _NOW, "ACTIVE",
        )
        self.assertIsNone(_validate_ledger_row(ledger))

    def test_all_passed_ledger_strategies_require_structure_id_before_delete(self):
        for strategy_id in ("N06", "N07", "N08", "N11", "N12"):
            with self.subTest(strategy_id=strategy_id), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                self._signal(path, 1, strategy_id, strategy_id + "USDT", 1, None)
                self._signal(path, 3, "N01", "CURRENTUSDT", 0)
                with self.assertRaisesRegex(
                    SignalRetentionMaintenanceError, "missing structure_id"
                ):
                    self._apply(path, 3)
                with _sqlite_connection(path) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_signals"
                        ).fetchone(),
                        (2,),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT migration_state FROM strategy_signal_current"
                        ).fetchone(),
                        ("PENDING",),
                    )

    def test_legacy_n16_signal_is_impossible_and_never_mints_claims(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._signal(
                path,
                1,
                "N16",
                "N16USDT",
                1,
                "a70ebc165a381c3887d6126b",
            )
            self._signal(path, 3, "N01", "CURRENTUSDT", 0)
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError,
                "impossible post-legacy strategy evidence",
            ):
                self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals"
                    ).fetchone(),
                    (2,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_structure_ledger"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT migration_state FROM strategy_signal_current"
                    ).fetchone(),
                    ("PENDING",),
                )

    def test_audit_write_failure_rolls_back_backfill_and_never_deletes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._signal(path, 1, "N06", "BTCUSDT", 1, "strict-structure")
            self._signal(path, 3, "N01", "CURRENTUSDT", 0)
            def fail_after_audit_insert(connection, source, created_at):
                _upsert_audit(connection, source, created_at)
                raise sqlite3.IntegrityError("audit write failed")

            with patch(
                "trading_bot.signal_retention._upsert_audit",
                side_effect=fail_after_audit_insert,
            ):
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError, "audit write failed"
                ):
                    self._apply(path, 3)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT retention_active, migration_state "
                        "FROM strategy_signal_current"
                    ).fetchone(),
                    (0, "PENDING"),
                )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM strategy_signals").fetchone(),
                    (2,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits"
                    ).fetchone(),
                    (0,),
                )

    def test_explicit_keep_attestation_not_completed_at_proves_current_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            count, manifest = self._attestation(path, 3)
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "count.*attestation"
            ):
                self._inspect_raw(
                    str(path.resolve()), 3, count + 1, manifest,
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "manifest.*attestation"
            ):
                self._apply_raw(
                    str(path.resolve()), 3, count, "f" * 64,
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
            self._signal(path, 3, "N01", "EXTRAUSDT", 0)
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "count.*attestation"
            ):
                self._inspect_raw(
                    str(path.resolve()), 3, count, manifest,
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )

    def test_misleading_later_completed_scan_does_not_replace_attested_current(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            count, manifest = self._attestation(path, 3)
            with _sqlite_connection(path) as connection:
                connection.execute(
                    "INSERT INTO scans (id, started_at, completed_at, mode, "
                    "scanned_count, candidate_count, candidates_json, opened, note) "
                    "VALUES (4, ?, ?, 'DRY_RUN', 100, 100, '[]', 0, 'partial')",
                    (_NOW, _NOW),
                )
            self._signal(path, 4, "N01", "PARTIALUSDT", 0)
            report = self._apply_raw(
                str(path.resolve()), 3, count, manifest,
                         n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertEqual(report.keep_scan_id, 3)
            self.assertEqual(report.latest_completed_scan_id, 4)
            with _sqlite_connection(path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT DISTINCT scan_id FROM strategy_signals"
                    ).fetchall(),
                    [(3,)],
                )

    def test_signal_sequence_is_not_reset_or_reused_after_maintenance(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            ids = self._seed_standard_history(path)
            self._apply(path, 3)
            recorder = ReviewRecorder(
                str(path),
                logging.getLogger("sequence-after-maintenance"),
                n16_claim_ledger_file=self._claim_ledger(path),
            )
            with recorder._connect() as connection:
                connection.execute(
                    "INSERT INTO scans (started_at, completed_at, mode, scanned_count, "
                    "candidate_count, candidates_json) VALUES (?, NULL, 'DRY_RUN', 1, 1, '[]')",
                    (_NOW,),
                )
                scan_id = connection.execute("SELECT MAX(id) FROM scans").fetchone()[0]
                connection.execute(
                    "INSERT INTO strategy_signal_batches (scan_id, state, recorded_count, "
                    "expected_count, first_signal_id, last_signal_id, manifest_sha256, "
                    "completed_at, created_at, updated_at) "
                    "VALUES (?, 'STAGING', 0, NULL, NULL, NULL, ?, NULL, ?, ?)",
                    (scan_id, "0" * 64, _NOW, _NOW),
                )
            new_id = recorder.record_strategy_signal(
                scan_id,
                "N01",
                "NEWUSDT",
                "-0.01",
                (),
                "1",
                True,
                False,
                "REJECTED",
                "NO_MATCH",
                detail={"new": True},
            )
            self.assertIsNotNone(new_id)
            self.assertGreater(new_id, max(ids))

    def test_vacuum_is_never_implicit_and_requires_explicit_new_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            destination_parent = Path(directory) / "vacuum-output"
            destination_parent.mkdir()
            destination = destination_parent / "compacted.sqlite3"
            self._apply(path, 3)
            self.assertFalse(destination.exists())
            self._checkpoint_for_vacuum(path)
            source_before = _vacuum_source_evidence(path)
            source_parent_before = _directory_fingerprint(path.parent)
            report = self._vacuum_raw(
                str(path.resolve()), str(destination.resolve()),
                         n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            self.assertEqual(_vacuum_source_evidence(path), source_before)
            self.assertEqual(
                _directory_fingerprint(path.parent), source_parent_before
            )
            self.assertTrue(report.vacuum_performed)
            self.assertTrue(destination.is_file())
            with _sqlite_connection(path) as source, _sqlite_connection(
                destination
            ) as target:
                self.assertEqual(target.execute("PRAGMA integrity_check").fetchone(), ("ok",))
                self.assertEqual(target.execute("PRAGMA foreign_key_check").fetchall(), [])
                for table in (
                    "strategy_signal_current",
                    "strategy_signal_batches",
                    "strategy_signals",
                    "strategy_passed_signal_audits",
                    "strategy_passed_structure_ledger",
                    "events",
                    "n13_rotation_states",
                    "n17_range_support_states",
                    "n17_history_coverage",
                    "n19_staircase_states",
                    "n19_history_coverage",
                    "n19_lifecycle_installation",
                ):
                    self.assertEqual(_rows(target, table), _rows(source, table), table)
                target_report = self._inspect_raw(
                    destination,
                    report.keep_scan_id,
                    report.retained_signal_count,
                    report.retained_manifest_sha256,
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )
                self.assertEqual(
                    report.n13_rotation_sha256,
                    target_report.n13_rotation_sha256,
                )
                self.assertEqual(
                    report.protected_manifest_sha256,
                    target_report.protected_manifest_sha256,
                )
            with self.assertRaises(SignalRetentionMaintenanceError):
                self._vacuum_raw(
                    str(path.resolve()), str(destination.resolve()),
                    n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                )

    def test_vacuum_preserves_full_schema_catalog_and_database_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            self._apply(path, 3)
            connection = sqlite3.connect(path)
            try:
                connection.executescript(
                    """
                    CREATE TABLE vacuum_schema_probe (
                        id INTEGER PRIMARY KEY,
                        value TEXT NOT NULL,
                        touched INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE UNIQUE INDEX idx_vacuum_schema_probe_value
                    ON vacuum_schema_probe(value) WHERE touched = 0;
                    CREATE VIEW vacuum_schema_probe_view AS
                    SELECT id, value FROM vacuum_schema_probe WHERE touched = 0;
                    CREATE TRIGGER vacuum_schema_probe_trigger
                    AFTER UPDATE OF value ON vacuum_schema_probe
                    BEGIN
                        UPDATE vacuum_schema_probe SET touched = touched + 1
                        WHERE id = NEW.id;
                    END;
                    PRAGMA user_version=123;
                    PRAGMA application_id=456;
                    """
                )
            finally:
                connection.close()
            self._advance_test_protected_generation(path)
            self._checkpoint_for_vacuum(path)
            source = sqlite3.connect(path)
            try:
                source_identity = _sqlite_schema_identity(source)
                source_catalog = source.execute(
                    "SELECT type, name, tbl_name, sql FROM main.sqlite_schema "
                    "ORDER BY type COLLATE BINARY, name COLLATE BINARY, "
                    "tbl_name COLLATE BINARY, sql COLLATE BINARY"
                ).fetchall()
            finally:
                source.close()
            destination = self._vacuum_destination(
                directory, "schema.compacted.sqlite3"
            )
            self._vacuum_raw(
                str(path.resolve()), str(destination.resolve()),
                n16_claim_ledger=str(self._claim_ledger(path).resolve()),
            )
            target = sqlite3.connect(destination)
            try:
                self.assertEqual(_sqlite_schema_identity(target), source_identity)
                self.assertEqual(
                    target.execute(
                        "SELECT type, name, tbl_name, sql FROM main.sqlite_schema "
                        "ORDER BY type COLLATE BINARY, name COLLATE BINARY, "
                        "tbl_name COLLATE BINARY, sql COLLATE BINARY"
                    ).fetchall(),
                    source_catalog,
                )
                self.assertEqual(target.execute("PRAGMA user_version").fetchone(), (123,))
                self.assertEqual(target.execute("PRAGMA application_id").fetchone(), (456,))
            finally:
                target.close()

    def test_vacuum_target_schema_or_pragma_tamper_is_rejected(self):
        for mode in ("index", "user_version"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                self._seed_standard_history(path)
                self._apply(path, 3)
                self._checkpoint_for_vacuum(path)
                destination_parent = Path(directory) / "vacuum-output"
                destination_parent.mkdir()
                source_before = _vacuum_source_evidence(path)
                source_parent_before = _directory_fingerprint(path.parent)
                destination = destination_parent / ("tamper-%s.sqlite3" % mode)
                original_open = signal_retention_module._open_immutable_database
                tampered = [False]

                @contextmanager
                def tampering_open(
                    opened_path,
                    expected_identity=None,
                    file_scope=None,
                ):
                    if not tampered[0]:
                        injected = sqlite3.connect(destination)
                        try:
                            if mode == "index":
                                injected.execute(
                                    "DROP INDEX idx_n16_trend_support_active"
                                )
                            else:
                                current_version = injected.execute(
                                    "PRAGMA user_version"
                                ).fetchone()[0]
                                injected.execute(
                                    "PRAGMA user_version=%d"
                                    % (current_version + 1)
                                )
                            injected.commit()
                        finally:
                            injected.close()
                        tampered[0] = True
                    with original_open(
                        opened_path,
                        expected_identity=expected_identity,
                        file_scope=file_scope,
                    ) as opened:
                        yield opened

                with patch.object(
                    signal_retention_module,
                    "_open_immutable_database",
                    tampering_open,
                ):
                    with self.assertRaisesRegex(
                        SignalRetentionMaintenanceError,
                        "(history coverage epoch graph is inconsistent"
                        "|schema identity|N16 lifecycle schema)",
                    ):
                        self._vacuum_raw(
                            str(path.resolve()), str(destination.resolve()),
                            n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                        )
                self.assertTrue(tampered[0])
                self.assertEqual(_vacuum_source_evidence(path), source_before)
                self.assertEqual(
                    _directory_fingerprint(path.parent), source_parent_before
                )
                self.assertFalse(destination.exists())

    def test_vacuum_invalid_source_uses_immutable_gate_before_any_rw_open(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    """
                    UPDATE strategy_signal_current
                    SET current_scan_id=NULL, retention_active=0,
                        migration_state='PENDING',
                        migration_cutoff_signal_id=NULL,
                        source_signal_count=NULL, source_passed_count=NULL,
                        source_manifest_sha256=NULL, retention_origin=NULL
                    WHERE singleton_id=1
                    """
                )
                connection.commit()
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                connection.execute("PRAGMA journal_mode=WAL")
            finally:
                connection.close()
            for suffix in ("-wal", "-shm", "-journal"):
                sidecar = Path(str(path) + suffix)
                if sidecar.exists():
                    sidecar.unlink()
            destination = self._vacuum_destination(
                directory, "must-not-exist.sqlite3"
            )
            before = (
                _file_fingerprint(path),
                os.stat(path).st_mtime_ns,
                tuple(sorted(item.name for item in Path(directory).iterdir())),
            )
            with patch.object(
                signal_retention_module,
                "_open_database",
                side_effect=AssertionError("RW open must not run"),
            ):
                with self.assertRaisesRegex(
                    SignalRetentionMaintenanceError,
                    "completed signal retention",
                ):
                    self._vacuum_raw(
                        str(path.resolve()), str(destination.resolve()),
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )
            self.assertFalse(destination.exists())
            self.assertEqual(
                (
                    _file_fingerprint(path),
                    os.stat(path).st_mtime_ns,
                    tuple(sorted(item.name for item in Path(directory).iterdir())),
                ),
                before,
            )

    def test_maintenance_openers_attest_actual_inode_before_any_query(self):
        openers = {
            "read_only": lambda path: signal_retention_module._open_database(
                path, read_only=True
            ),
            "read_write": lambda path: signal_retention_module._open_database(
                path, read_only=False
            ),
            "immutable": lambda path: signal_retention_module._open_immutable_database(
                path
            ),
            "vacuum_source": lambda path: signal_retention_module._open_immutable_vacuum_source(
                path
            ),
        }
        for opener_name, opener in openers.items():
            with self.subTest(opener=opener_name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source_parent = root / "binance-data"
                source_parent.mkdir()
                path = self._database(source_parent)
                self._checkpoint_for_vacuum(path)
                saved_parent = root / "binance-saved"
                external_parent = root / "external-system"
                external_parent.mkdir()
                external = external_parent / path.name
                with _sqlite_connection(external) as connection:
                    connection.execute(
                        "CREATE TABLE sentinel "
                        "(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                    )
                    connection.execute(
                        "INSERT INTO sentinel VALUES (1, 'UNCHANGED')"
                    )
                external_before = _file_fingerprint(external)
                external_directory_before = _directory_fingerprint(external_parent)
                original_connect = sqlite3.connect
                swaps = []

                def swapping_connect(database, *args, **kwargs):
                    if swaps:
                        return original_connect(database, *args, **kwargs)
                    os.replace(source_parent, saved_parent)
                    os.symlink(str(external_parent), str(source_parent))
                    try:
                        opened = original_connect(database, *args, **kwargs)
                    except BaseException:
                        os.unlink(source_parent)
                        os.replace(saved_parent, source_parent)
                        swaps.append(True)
                        raise
                    os.unlink(source_parent)
                    os.replace(saved_parent, source_parent)
                    swaps.append(True)
                    return opened

                with patch.object(
                    signal_retention_module.sqlite3,
                    "connect",
                    side_effect=swapping_connect,
                ), patch.object(
                    signal_retention_module,
                    "_verify_sqlite_main_identity",
                ) as path_verify:
                    with self.assertRaisesRegex(
                        SignalRetentionMaintenanceError,
                        "descriptor is not attested",
                    ):
                        with opener(path):
                            self.fail("external SQLite connection was accepted")
                path_verify.assert_not_called()
                self.assertEqual(swaps, [True])
                self.assertEqual(_file_fingerprint(external), external_before)
                self.assertEqual(
                    _directory_fingerprint(external_parent),
                    external_directory_before,
                )
                self.assertFalse(Path(str(external) + "-wal").exists())
                self.assertFalse(Path(str(external) + "-shm").exists())

    def test_maintenance_rw_parent_swap_cannot_create_external_database(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_parent = root / "binance-data"
            source_parent.mkdir()
            path = self._database(source_parent)
            self._checkpoint_for_vacuum(path)
            saved_parent = root / "binance-saved"
            external_parent = root / "external-system"
            external_parent.mkdir()
            external = external_parent / path.name
            external_before = _directory_fingerprint(external_parent)
            original_connect = sqlite3.connect
            swaps = []

            def swapping_connect(database, *args, **kwargs):
                os.replace(source_parent, saved_parent)
                os.symlink(str(external_parent), str(source_parent))
                try:
                    return original_connect(database, *args, **kwargs)
                finally:
                    os.unlink(source_parent)
                    os.replace(saved_parent, source_parent)
                    swaps.append(True)

            with patch.object(
                signal_retention_module.sqlite3,
                "connect",
                side_effect=swapping_connect,
            ):
                with self.assertRaises(sqlite3.OperationalError):
                    with signal_retention_module._open_database(
                        path, read_only=False
                    ):
                        self.fail("missing external database was created")
            self.assertEqual(swaps, [True])
            self.assertFalse(external.exists())
            self.assertEqual(
                _directory_fingerprint(external_parent), external_before
            )

    def test_vacuum_rejects_source_and_target_in_same_parent_before_open(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = path.parent / "same-parent.compacted.sqlite3"
            source_before = _vacuum_source_evidence(path)
            parent_before = _directory_fingerprint(path.parent)
            with patch.object(
                signal_retention_module,
                "_open_immutable_vacuum_source",
            ) as source_open:
                with self.assertRaisesRegex(
                    SignalRetentionMaintenanceError,
                    "different parent directory",
                ):
                    self._vacuum_raw(
                        str(path.resolve()), str(destination.resolve()),
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )
            source_open.assert_not_called()
            self.assertFalse(destination.exists())
            self.assertEqual(_vacuum_source_evidence(path), source_before)
            self.assertEqual(
                _directory_fingerprint(path.parent), parent_before
            )

    def test_cli_rejects_same_parent_vacuum_before_lock_or_sqlite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "binance-only"
            root.mkdir()
            lock_parent = root / "state"
            lock_parent.mkdir()
            path = self._database(root)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = root / "same-parent.compacted.sqlite3"
            self._create_official_lock(self._claim_ledger(path))
            source_before = _vacuum_source_evidence(path)
            parent_before = _directory_fingerprint(root)
            stderr = io.StringIO()
            with patch(
                "trading_bot.signal_retention.InstanceLock.acquire"
            ) as acquire, patch(
                "trading_bot.signal_retention.vacuum_signal_database_into"
            ) as vacuum, redirect_stderr(stderr):
                exit_code = self._cli(
                    [
                        "--vacuum-into",
                        str(destination),
                        "--db",
                        str(path.resolve()),
                        "--binance-root",
                        str(root.resolve()),
                        "--lock-file",
                        str((root / "trading_bot.lock").resolve()),
                        "--n16-claim-ledger",
                        str(self._claim_ledger(path).resolve()),
                    ]
                )
            self.assertEqual(exit_code, 1)
            acquire.assert_not_called()
            vacuum.assert_not_called()
            self.assertIn("different parent directory", stderr.getvalue())
            self.assertFalse(destination.exists())
            self.assertEqual(_vacuum_source_evidence(path), source_before)
            self.assertEqual(_directory_fingerprint(root), parent_before)

    def test_cli_final_scope_failure_discards_uncommitted_vacuum_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "binance-trading-data"
            data_root.mkdir()
            backup_root = root / "binance-trading-backups"
            backup_root.mkdir()
            output_parent = backup_root / "release"
            output_parent.mkdir()
            lock_parent = data_root / "state"
            lock_parent.mkdir()
            path = self._database(data_root)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = output_parent / "review.compacted.sqlite3"
            self._create_official_lock(self._claim_ledger(path))
            source_before = _vacuum_source_evidence(path)
            original_revalidate = (
                signal_retention_module._revalidate_cli_scope_after_lock
            )
            calls = []

            def fail_after_complete_scope_check(*args, **kwargs):
                identity = original_revalidate(*args, **kwargs)
                calls.append(
                    kwargs["vacuum_destination_exists"]
                    if "vacuum_destination_exists" in kwargs
                    else args[1]
                )
                if calls[-1]:
                    raise SignalRetentionMaintenanceError(
                        "forced final CLI scope failure"
                    )
                return identity

            stderr = io.StringIO()
            with patch.object(
                signal_retention_module,
                "_revalidate_cli_scope_after_lock",
                side_effect=fail_after_complete_scope_check,
            ), redirect_stderr(stderr):
                exit_code = self._cli(
                    [
                        "--vacuum-into",
                        str(destination),
                        "--db",
                        str(path.resolve()),
                        "--binance-root",
                        str(data_root.resolve()),
                        "--binance-root",
                        str(backup_root.resolve()),
                        "--lock-file",
                        str((data_root / "trading_bot.lock").resolve()),
                        "--n16-claim-ledger",
                        str(self._claim_ledger(path).resolve()),
                    ]
                )
            self.assertEqual(exit_code, 1)
            self.assertEqual(calls, [False, True])
            self.assertIn("forced final CLI scope failure", stderr.getvalue())
            self.assertFalse(destination.exists())
            self.assertFalse(
                any(
                    item.name.startswith(".vacuum-stage-")
                    for item in output_parent.iterdir()
                )
            )
            self.assertEqual(_vacuum_source_evidence(path), source_before)

    def test_cli_precommit_rejects_new_source_sidecar_and_discards_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "binance-trading-data"
            data_root.mkdir()
            backup_root = root / "binance-trading-backups"
            backup_root.mkdir()
            output_parent = backup_root / "release"
            output_parent.mkdir()
            lock_parent = data_root / "state"
            lock_parent.mkdir()
            path = self._database(data_root)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = output_parent / "review.compacted.sqlite3"
            self._create_official_lock(self._claim_ledger(path))
            main_before = _file_fingerprint(path)
            injected_sidecar = Path(str(path) + "-wal")
            original_revalidate = (
                signal_retention_module._revalidate_cli_scope_after_lock
            )
            injected_fingerprint = []

            def inject_before_precommit(*args, **kwargs):
                destination_exists = (
                    kwargs["vacuum_destination_exists"]
                    if "vacuum_destination_exists" in kwargs
                    else args[1]
                )
                if destination_exists and not injected_fingerprint:
                    injected_sidecar.write_bytes(b"PRIVATE-BINANCE-SIDECAR")
                    injected_fingerprint.append(
                        _file_fingerprint(injected_sidecar)
                    )
                return original_revalidate(*args, **kwargs)

            stderr = io.StringIO()
            with patch.object(
                signal_retention_module,
                "_revalidate_cli_scope_after_lock",
                side_effect=inject_before_precommit,
            ), redirect_stderr(stderr):
                exit_code = self._cli(
                    [
                        "--vacuum-into",
                        str(destination),
                        "--db",
                        str(path.resolve()),
                        "--binance-root",
                        str(data_root.resolve()),
                        "--binance-root",
                        str(backup_root.resolve()),
                        "--lock-file",
                        str((data_root / "trading_bot.lock").resolve()),
                        "--n16-claim-ledger",
                        str(self._claim_ledger(path).resolve()),
                    ]
                )
            self.assertEqual(exit_code, 1)
            self.assertEqual(len(injected_fingerprint), 1)
            self.assertIn("sidecar identity changed", stderr.getvalue())
            self.assertFalse(destination.exists())
            self.assertEqual(_file_fingerprint(path), main_before)
            self.assertEqual(
                _file_fingerprint(injected_sidecar), injected_fingerprint[0]
            )
            self.assertFalse(
                any(
                    item.name.startswith(".vacuum-stage-")
                    for item in output_parent.iterdir()
                )
            )

    def test_anchored_vacuum_target_parent_replacement_never_writes_replacement(self):
        for replacement_kind in ("symlink", "directory"):
            with self.subTest(replacement=replacement_kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source_parent = root / "binance-data"
                source_parent.mkdir()
                target_root = root / "binance-backups"
                target_root.mkdir()
                target_parent = target_root / "release"
                target_parent.mkdir()
                saved_parent = target_root / "release-attested"
                external_parent = root / "external-system"
                external_parent.mkdir()
                path = self._database(source_parent)
                self._seed_standard_history(path)
                self._apply(path, 3)
                self._checkpoint_for_vacuum(path)
                destination = target_parent / "review.compacted.sqlite3"
                source_before = _vacuum_source_evidence(path)
                source_parent_before = _directory_fingerprint(source_parent)
                external_before = _directory_fingerprint(external_parent)
                original_absent = (
                    signal_retention_module._require_anchored_vacuum_names_absent
                )
                checks = []

                def swap_after_final_absence_check(parent_descriptor, names):
                    original_absent(parent_descriptor, names)
                    checks.append(True)
                    if len(checks) != 2:
                        return
                    os.replace(target_parent, saved_parent)
                    if replacement_kind == "symlink":
                        os.symlink(str(external_parent), str(target_parent))
                    else:
                        target_parent.mkdir()

                with patch.object(
                    signal_retention_module,
                    "_require_anchored_vacuum_names_absent",
                    side_effect=swap_after_final_absence_check,
                ):
                    with self.assertRaises(SignalRetentionMaintenanceError):
                        self._vacuum_raw(
                            str(path.resolve()), str(destination.resolve()),
                            n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                        )
                # Parent is checked once before staging, the private staging
                # directory immediately before VACUUM, and the parent again
                # before no-overwrite publication.
                self.assertEqual(len(checks), 3)
                self.assertEqual(_vacuum_source_evidence(path), source_before)
                self.assertEqual(
                    _directory_fingerprint(source_parent),
                    source_parent_before,
                )
                self.assertEqual(
                    _directory_fingerprint(external_parent), external_before
                )
                self.assertFalse((external_parent / destination.name).exists())
                self.assertFalse((saved_parent / destination.name).exists())
                if replacement_kind == "directory":
                    self.assertFalse((target_parent / destination.name).exists())

    def test_vacuum_parent_replacement_after_creation_discards_anchored_output(self):
        for replacement_kind in ("symlink", "directory"):
            with self.subTest(replacement=replacement_kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source_parent = root / "binance-data"
                source_parent.mkdir()
                target_root = root / "binance-backups"
                target_root.mkdir()
                target_parent = target_root / "release"
                target_parent.mkdir()
                saved_parent = target_root / "release-attested"
                external_parent = root / "external-system"
                external_parent.mkdir()
                path = self._database(source_parent)
                self._seed_standard_history(path)
                self._apply(path, 3)
                self._checkpoint_for_vacuum(path)
                destination = target_parent / "review.compacted.sqlite3"
                source_before = _vacuum_source_evidence(path)
                external_before = _directory_fingerprint(external_parent)
                original_execute = (
                    signal_retention_module._execute_anchored_vacuum_into
                )

                def create_then_replace_parent(*args, **kwargs):
                    handle = original_execute(*args, **kwargs)
                    os.replace(target_parent, saved_parent)
                    if replacement_kind == "symlink":
                        os.symlink(str(external_parent), str(target_parent))
                    else:
                        target_parent.mkdir()
                    return handle

                with patch.object(
                    signal_retention_module,
                    "_execute_anchored_vacuum_into",
                    side_effect=create_then_replace_parent,
                ):
                    with self.assertRaises(SignalRetentionMaintenanceError):
                        self._vacuum_raw(
                            str(path.resolve()), str(destination.resolve()),
                            n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                        )
                self.assertEqual(_vacuum_source_evidence(path), source_before)
                self.assertEqual(
                    _directory_fingerprint(external_parent), external_before
                )
                self.assertFalse((external_parent / destination.name).exists())
                self.assertFalse((saved_parent / destination.name).exists())
                if replacement_kind == "directory":
                    self.assertFalse((target_parent / destination.name).exists())

    def test_vacuum_ack_loss_cleans_private_staging_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_parent = root / "binance-data"
            source_parent.mkdir()
            target_parent = root / "binance-backups"
            target_parent.mkdir()
            path = self._database(source_parent)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = target_parent / "review.compacted.sqlite3"
            source_before = _vacuum_source_evidence(path)
            target_entries_before = tuple(target_parent.iterdir())
            original_open = signal_retention_module._open_immutable_vacuum_source

            class AckLossConnection:
                def __init__(self, connection):
                    self.connection = connection

                def __getattr__(self, name):
                    return getattr(self.connection, name)

                def execute(self, sql, parameters=()):
                    result = self.connection.execute(sql, parameters)
                    if type(sql) is str and sql.startswith("VACUUM INTO"):
                        raise RuntimeError("simulated VACUUM acknowledgement loss")
                    return result

            @contextmanager
            def ack_loss_open(*args, **kwargs):
                with original_open(*args, **kwargs) as connection:
                    yield AckLossConnection(connection)

            with patch.object(
                signal_retention_module,
                "_open_immutable_vacuum_source",
                ack_loss_open,
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "acknowledgement loss"
                ):
                    self._vacuum_raw(
                        str(path.resolve()), str(destination.resolve()),
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )
            self.assertEqual(_vacuum_source_evidence(path), source_before)
            self.assertEqual(tuple(target_parent.iterdir()), target_entries_before)
            self.assertFalse(destination.exists())

    def test_vacuum_commit_close_ack_loss_removes_published_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_parent = root / "binance-data"
            source_parent.mkdir()
            target_parent = root / "binance-backups"
            target_parent.mkdir()
            path = self._database(source_parent)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = target_parent / "review.compacted.sqlite3"
            source_before = _vacuum_source_evidence(path)
            original_close = os.close
            close_calls = []

            def close_then_raise_on_commit(descriptor):
                close_calls.append(descriptor)
                result = original_close(descriptor)
                if len(close_calls) == 3:
                    raise OSError("simulated commit close acknowledgement loss")
                return result

            with patch.object(
                signal_retention_module.os,
                "close",
                side_effect=close_then_raise_on_commit,
            ):
                with self.assertRaisesRegex(
                    SignalRetentionMaintenanceError,
                    "commit descriptor close failed",
                ):
                    self._vacuum_raw(
                        str(path.resolve()), str(destination.resolve()),
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )

            self.assertGreaterEqual(len(close_calls), 4)
            self.assertEqual(_vacuum_source_evidence(path), source_before)
            self.assertFalse(destination.exists())
            self.assertEqual(tuple(target_parent.iterdir()), ())

    def test_cleanup_close_ack_loss_never_closes_reused_external_fd(self):
        with tempfile.TemporaryDirectory() as directory:
            sentinel = Path(directory) / "external-sentinel.bin"
            sentinel_payload = b"external-fd-must-remain-owned-by-caller"
            sentinel.write_bytes(sentinel_payload)
            sentinel_before = _file_fingerprint(sentinel)
            original_close = os.close
            victim = os.open(str(sentinel), os.O_RDONLY)
            opened = []
            injected = []

            def close_then_reuse_and_raise(descriptor):
                result = original_close(descriptor)
                if descriptor == victim and not injected:
                    injected.append(descriptor)
                    opened.extend(
                        _open_until_descriptor_is_reused(sentinel, descriptor)
                    )
                    raise OSError(
                        "simulated cleanup close acknowledgement loss"
                    )
                return result

            try:
                with patch.object(
                    signal_retention_module.os,
                    "close",
                    side_effect=close_then_reuse_and_raise,
                ):
                    with self.assertRaisesRegex(
                        OSError, "cleanup close acknowledgement loss"
                    ):
                        signal_retention_module._close_descriptor_after_cleanup(
                            victim
                        )

                replacement = opened[-1]
                self.assertEqual(replacement, victim)
                os.fstat(replacement)
                os.lseek(replacement, 0, os.SEEK_SET)
                self.assertEqual(os.read(replacement, 4096), sentinel_payload)
                self.assertEqual(_file_fingerprint(sentinel), sentinel_before)
            finally:
                _close_test_descriptors(opened, original_close)

    def test_vacuum_commit_close_ack_loss_preserves_reused_external_fd(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_parent = root / "binance-data"
            source_parent.mkdir()
            target_parent = root / "binance-backups"
            target_parent.mkdir()
            sentinel = root / "external-sentinel.bin"
            sentinel_payload = b"external-commit-parent-fd"
            sentinel.write_bytes(sentinel_payload)
            sentinel_before = _file_fingerprint(sentinel)
            path = self._database(source_parent)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = target_parent / "review.compacted.sqlite3"
            source_before = _vacuum_source_evidence(path)
            original_close = os.close
            close_calls = []
            opened = []

            def close_then_reuse_commit_parent(descriptor):
                close_calls.append(descriptor)
                result = original_close(descriptor)
                if len(close_calls) == 3:
                    opened.extend(
                        _open_until_descriptor_is_reused(sentinel, descriptor)
                    )
                    raise OSError(
                        "simulated commit parent fd acknowledgement loss"
                    )
                return result

            try:
                with patch.object(
                    signal_retention_module.os,
                    "close",
                    side_effect=close_then_reuse_commit_parent,
                ):
                    with self.assertRaisesRegex(
                        SignalRetentionMaintenanceError,
                        "commit descriptor close failed",
                    ):
                        self._vacuum_raw(
                            str(path.resolve()), str(destination.resolve()),
                            n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                        )

                replacement = opened[-1]
                self.assertEqual(replacement, close_calls[2])
                os.fstat(replacement)
                os.lseek(replacement, 0, os.SEEK_SET)
                self.assertEqual(os.read(replacement, 4096), sentinel_payload)
                self.assertEqual(_file_fingerprint(sentinel), sentinel_before)
                self.assertEqual(_vacuum_source_evidence(path), source_before)
                self.assertFalse(destination.exists())
                self.assertEqual(tuple(target_parent.iterdir()), ())
            finally:
                _close_test_descriptors(opened, original_close)

    def test_vacuum_commit_guard_ack_loss_preserves_reused_external_fd(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_parent = root / "binance-data"
            source_parent.mkdir()
            target_parent = root / "binance-backups"
            target_parent.mkdir()
            sentinel = root / "external-sentinel.bin"
            sentinel_payload = b"external-commit-guard-fd"
            sentinel.write_bytes(sentinel_payload)
            sentinel_before = _file_fingerprint(sentinel)
            path = self._database(source_parent)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = target_parent / "review.compacted.sqlite3"
            source_before = _vacuum_source_evidence(path)
            original_close = os.close
            close_calls = []
            opened = []

            def close_then_reuse_commit_guard(descriptor):
                close_calls.append(descriptor)
                result = original_close(descriptor)
                if len(close_calls) == 4:
                    opened.extend(
                        _open_until_descriptor_is_reused(sentinel, descriptor)
                    )
                    raise OSError(
                        "simulated commit guard fd acknowledgement loss"
                    )
                return result

            try:
                with patch.object(
                    signal_retention_module.os,
                    "close",
                    side_effect=close_then_reuse_commit_guard,
                ):
                    with self.assertRaisesRegex(
                        SignalRetentionMaintenanceError,
                        "commit guard close failed",
                    ):
                        self._vacuum_raw(
                            str(path.resolve()), str(destination.resolve()),
                            n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                        )

                replacement = opened[-1]
                self.assertEqual(replacement, close_calls[3])
                os.fstat(replacement)
                os.lseek(replacement, 0, os.SEEK_SET)
                self.assertEqual(os.read(replacement, 4096), sentinel_payload)
                self.assertEqual(_file_fingerprint(sentinel), sentinel_before)
                self.assertEqual(_vacuum_source_evidence(path), source_before)
                self.assertFalse(destination.exists())
                self.assertEqual(tuple(target_parent.iterdir()), ())
            finally:
                _close_test_descriptors(opened, original_close)

    def test_vacuum_stage_close_ack_loss_preserves_reused_external_fd(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_parent = root / "binance-data"
            source_parent.mkdir()
            target_parent = root / "binance-backups"
            target_parent.mkdir()
            sentinel = root / "external-sentinel.bin"
            sentinel_payload = b"external-stage-fd"
            sentinel.write_bytes(sentinel_payload)
            sentinel_before = _file_fingerprint(sentinel)
            path = self._database(source_parent)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = target_parent / "review.compacted.sqlite3"
            source_before = _vacuum_source_evidence(path)
            cwd_before = os.stat(".")
            original_close = os.close
            close_calls = []
            opened = []

            def close_then_reuse_stage(descriptor):
                close_calls.append(descriptor)
                result = original_close(descriptor)
                if len(close_calls) == 1:
                    opened.extend(
                        _open_until_descriptor_is_reused(sentinel, descriptor)
                    )
                    raise OSError("simulated stage fd acknowledgement loss")
                return result

            try:
                with patch.object(
                    signal_retention_module.os,
                    "close",
                    side_effect=close_then_reuse_stage,
                ):
                    with self.assertRaisesRegex(
                        OSError, "stage fd acknowledgement loss"
                    ):
                        self._vacuum_raw(
                            str(path.resolve()), str(destination.resolve()),
                            n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                        )

                replacement = opened[-1]
                self.assertEqual(replacement, close_calls[0])
                os.fstat(replacement)
                os.lseek(replacement, 0, os.SEEK_SET)
                self.assertEqual(os.read(replacement, 4096), sentinel_payload)
                cwd_after = os.stat(".")
                self.assertEqual(
                    (cwd_after.st_dev, cwd_after.st_ino),
                    (cwd_before.st_dev, cwd_before.st_ino),
                )
                self.assertEqual(_file_fingerprint(sentinel), sentinel_before)
                self.assertEqual(_vacuum_source_evidence(path), source_before)
                self.assertFalse(destination.exists())
                self.assertEqual(tuple(target_parent.iterdir()), ())
            finally:
                _close_test_descriptors(opened, original_close)

    def test_vacuum_link_ack_loss_removes_published_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_parent = root / "binance-data"
            source_parent.mkdir()
            target_parent = root / "binance-backups"
            target_parent.mkdir()
            path = self._database(source_parent)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = target_parent / "review.compacted.sqlite3"
            source_before = _vacuum_source_evidence(path)
            original_link = os.link
            link_calls = []

            def link_then_raise(*args, **kwargs):
                link_calls.append(True)
                original_link(*args, **kwargs)
                raise OSError("simulated link acknowledgement loss")

            with patch.object(
                signal_retention_module.os,
                "link",
                side_effect=link_then_raise,
            ):
                with self.assertRaises(SignalRetentionMaintenanceError):
                    self._vacuum_raw(
                        str(path.resolve()), str(destination.resolve()),
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )

            self.assertEqual(link_calls, [True])
            self.assertEqual(_vacuum_source_evidence(path), source_before)
            self.assertFalse(destination.exists())
            self.assertEqual(tuple(target_parent.iterdir()), ())

    def test_vacuum_fchdir_ack_loss_restores_original_cwd(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_parent = root / "binance-data"
            source_parent.mkdir()
            target_parent = root / "binance-backups"
            target_parent.mkdir()
            path = self._database(source_parent)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = target_parent / "review.compacted.sqlite3"
            source_before = _vacuum_source_evidence(path)
            cwd_before = os.getcwd()
            original_fchdir = os.fchdir
            fchdir_calls = []

            def fchdir_then_raise_first(descriptor):
                fchdir_calls.append(descriptor)
                result = original_fchdir(descriptor)
                if len(fchdir_calls) == 1:
                    raise OSError("simulated fchdir acknowledgement loss")
                return result

            with patch.object(
                signal_retention_module.os,
                "fchdir",
                side_effect=fchdir_then_raise_first,
            ):
                with self.assertRaisesRegex(
                    OSError, "fchdir acknowledgement loss"
                ):
                    self._vacuum_raw(
                        str(path.resolve()), str(destination.resolve()),
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )

            self.assertGreaterEqual(len(fchdir_calls), 2)
            self.assertEqual(os.getcwd(), cwd_before)
            self.assertEqual(_vacuum_source_evidence(path), source_before)
            self.assertFalse(destination.exists())
            self.assertEqual(tuple(target_parent.iterdir()), ())

    def test_vacuum_mkdir_ack_loss_removes_private_stage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_parent = root / "binance-data"
            source_parent.mkdir()
            target_parent = root / "binance-backups"
            target_parent.mkdir()
            path = self._database(source_parent)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = target_parent / "review.compacted.sqlite3"
            source_before = _vacuum_source_evidence(path)
            original_mkdir = os.mkdir
            mkdir_calls = []

            def mkdir_then_raise(path_value, *args, **kwargs):
                result = original_mkdir(path_value, *args, **kwargs)
                if str(path_value).startswith(".vacuum-stage-"):
                    mkdir_calls.append(path_value)
                    raise OSError("simulated mkdir acknowledgement loss")
                return result

            with patch.object(
                signal_retention_module.os,
                "mkdir",
                side_effect=mkdir_then_raise,
            ):
                with self.assertRaisesRegex(
                    OSError, "mkdir acknowledgement loss"
                ):
                    self._vacuum_raw(
                        str(path.resolve()), str(destination.resolve()),
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )

            self.assertEqual(len(mkdir_calls), 1)
            self.assertEqual(_vacuum_source_evidence(path), source_before)
            self.assertFalse(destination.exists())
            self.assertEqual(tuple(target_parent.iterdir()), ())

    def test_vacuum_transient_stage_unlink_failure_cleans_all_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_parent = root / "binance-data"
            source_parent.mkdir()
            target_parent = root / "binance-backups"
            target_parent.mkdir()
            path = self._database(source_parent)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = target_parent / "review.compacted.sqlite3"
            source_before = _vacuum_source_evidence(path)
            original_unlink = os.unlink
            injected = []

            def fail_first_stage_unlink(path_value, *args, **kwargs):
                if path_value == destination.name and not injected:
                    injected.append(True)
                    raise OSError("simulated transient stage unlink failure")
                return original_unlink(path_value, *args, **kwargs)

            with patch.object(
                signal_retention_module.os,
                "unlink",
                side_effect=fail_first_stage_unlink,
            ):
                with self.assertRaisesRegex(
                    OSError, "transient stage unlink failure"
                ):
                    self._vacuum_raw(
                        str(path.resolve()), str(destination.resolve()),
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )
            self.assertEqual(injected, [True])
            self.assertFalse(destination.exists())
            self.assertEqual(tuple(target_parent.iterdir()), ())
            self.assertEqual(_vacuum_source_evidence(path), source_before)

    def test_vacuum_transient_cwd_restore_failure_retries_and_cleans(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_parent = root / "binance-data"
            source_parent.mkdir()
            target_parent = root / "binance-backups"
            target_parent.mkdir()
            path = self._database(source_parent)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = target_parent / "review.compacted.sqlite3"
            cwd_before = os.stat(".")
            original_fchdir = os.fchdir
            calls = []

            def fail_first_restore(descriptor):
                calls.append(descriptor)
                if len(calls) == 2:
                    raise OSError("simulated transient cwd restore failure")
                return original_fchdir(descriptor)

            with patch.object(
                signal_retention_module.os,
                "fchdir",
                side_effect=fail_first_restore,
            ):
                with self.assertRaisesRegex(
                    OSError, "transient cwd restore failure"
                ):
                    self._vacuum_raw(
                        str(path.resolve()), str(destination.resolve()),
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )
            cwd_after = os.stat(".")
            self.assertGreaterEqual(len(calls), 3)
            self.assertEqual(
                (cwd_after.st_dev, cwd_after.st_ino),
                (cwd_before.st_dev, cwd_before.st_ino),
            )
            self.assertFalse(destination.exists())
            self.assertEqual(tuple(target_parent.iterdir()), ())

    def test_vacuum_descriptor_close_failure_still_discards_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_parent = root / "binance-data"
            source_parent.mkdir()
            target_parent = root / "binance-backups"
            target_parent.mkdir()
            path = self._database(source_parent)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = target_parent / "review.compacted.sqlite3"
            original_close = os.close
            close_calls = []

            def close_then_fail_once(descriptor):
                close_calls.append(descriptor)
                result = original_close(descriptor)
                if len(close_calls) == 2:
                    raise OSError("simulated descriptor close acknowledgement loss")
                return result

            with patch.object(
                signal_retention_module.os,
                "close",
                side_effect=close_then_fail_once,
            ):
                with self.assertRaisesRegex(
                    SignalRetentionMaintenanceError,
                    "descriptor cleanup was incomplete",
                ):
                    self._vacuum_raw(
                        str(path.resolve()), str(destination.resolve()),
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )
            self.assertGreaterEqual(len(close_calls), 3)
            self.assertFalse(destination.exists())
            self.assertEqual(tuple(target_parent.iterdir()), ())

    def test_vacuum_target_validation_failure_retries_safe_discard(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_parent = root / "binance-data"
            source_parent.mkdir()
            target_parent = root / "binance-backups"
            target_parent.mkdir()
            path = self._database(source_parent)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = target_parent / "review.compacted.sqlite3"
            source_before = _vacuum_source_evidence(path)
            original_remove = signal_retention_module._remove_anchored_vacuum_output
            remove_calls = []

            def transient_remove_failure(*args, **kwargs):
                remove_calls.append(True)
                if len(remove_calls) == 1:
                    raise OSError("simulated transient unlink failure")
                return original_remove(*args, **kwargs)

            with patch.object(
                signal_retention_module,
                "_open_immutable_database",
                side_effect=SignalRetentionMaintenanceError(
                    "forced target validation failure"
                ),
            ), patch.object(
                signal_retention_module,
                "_remove_anchored_vacuum_output",
                side_effect=transient_remove_failure,
            ):
                with self.assertRaisesRegex(
                    SignalRetentionMaintenanceError,
                    "forced target validation failure",
                ):
                    self._vacuum_raw(
                        str(path.resolve()), str(destination.resolve()),
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )
            self.assertEqual(len(remove_calls), 2)
            self.assertFalse(destination.exists())
            self.assertFalse(
                any(
                    item.name.startswith(".vacuum-stage-")
                    for item in target_parent.iterdir()
                )
            )
            self.assertEqual(_vacuum_source_evidence(path), source_before)

    def test_vacuum_target_validation_attests_opened_external_inode_and_cleans_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_parent = root / "binance-data"
            source_parent.mkdir()
            target_parent = root / "binance-backups"
            target_parent.mkdir()
            saved_parent = root / "binance-backups-saved"
            external_parent = root / "external-system"
            external_parent.mkdir()
            path = self._database(source_parent)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = target_parent / "review.compacted.sqlite3"
            external = external_parent / destination.name
            with _sqlite_connection(external) as connection:
                connection.execute(
                    "CREATE TABLE sentinel "
                    "(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO sentinel VALUES (1, 'UNCHANGED')"
                )
            external_before = _file_fingerprint(external)
            external_parent_before = _directory_fingerprint(external_parent)
            source_before = _vacuum_source_evidence(path)
            original_connect = sqlite3.connect
            swapped = []

            def swap_target_parent_during_validation(database, *args, **kwargs):
                database_text = os.fspath(database)
                if database_text.startswith("file:"):
                    database_path = Path(
                        unquote(urlsplit(database_text).path)
                    )
                else:
                    database_path = Path(database_text)
                if swapped or database_path.name != destination.name:
                    return original_connect(database, *args, **kwargs)
                os.replace(target_parent, saved_parent)
                os.symlink(str(external_parent), str(target_parent))
                opened = original_connect(database, *args, **kwargs)
                os.unlink(target_parent)
                os.replace(saved_parent, target_parent)
                swapped.append(True)
                return opened

            with patch.object(
                signal_retention_module.sqlite3,
                "connect",
                side_effect=swap_target_parent_during_validation,
            ):
                with self.assertRaisesRegex(
                    SignalRetentionMaintenanceError,
                    "descriptor is not attested",
                ):
                    self._vacuum_raw(
                        str(path.resolve()), str(destination.resolve()),
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )
            self.assertEqual(swapped, [True])
            self.assertEqual(_vacuum_source_evidence(path), source_before)
            self.assertEqual(_file_fingerprint(external), external_before)
            self.assertEqual(
                _directory_fingerprint(external_parent), external_parent_before
            )
            self.assertFalse(destination.exists())
            self.assertFalse(Path(str(external) + "-wal").exists())
            self.assertFalse(Path(str(external) + "-shm").exists())

    def test_vacuum_rejects_existing_source_sidecars_before_connect(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._checkpoint_for_vacuum(path)
            sidecars = [Path(str(path) + suffix) for suffix in ("-wal", "-shm")]
            for index, sidecar in enumerate(sidecars):
                sidecar.write_bytes(("sentinel-%d" % index).encode("ascii"))
            before = {
                item: (_file_fingerprint(item), os.stat(item).st_mtime_ns)
                for item in [path] + sidecars
            }
            destination = self._vacuum_destination(
                directory, "must-not-open.sqlite3"
            )
            with patch.object(
                signal_retention_module,
                "_open_immutable_vacuum_source",
                side_effect=AssertionError("immutable connect must not run"),
            ):
                with self.assertRaisesRegex(
                    SignalRetentionMaintenanceError,
                    "sidecars must not already exist",
                ):
                    self._vacuum_raw(
                        str(path.resolve()), str(destination.resolve()),
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )
            self.assertFalse(destination.exists())
            self.assertEqual(
                {
                    item: (_file_fingerprint(item), os.stat(item).st_mtime_ns)
                    for item in [path] + sidecars
                },
                before,
            )

    def test_vacuum_rejects_sidecar_appearing_between_validation_and_vacuum(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            self._seed_standard_history(path)
            self._apply(path, 3)
            self._checkpoint_for_vacuum(path)
            destination = self._vacuum_destination(
                directory, "toctou-must-not-exist.sqlite3"
            )
            injected_sidecar = Path(str(path) + "-wal")
            original_validate = signal_retention_module._validated_vacuum_source
            injected = [False]

            def injecting_validation(connection):
                result = original_validate(connection)
                if not injected[0]:
                    injected_sidecar.write_bytes(b"late-safe-looking-sidecar")
                    injected[0] = True
                return result

            with patch.object(
                signal_retention_module,
                "_validated_vacuum_source",
                injecting_validation,
            ):
                with self.assertRaisesRegex(
                    SignalRetentionMaintenanceError,
                    "sidecars must not already exist",
                ):
                    self._vacuum_raw(
                        str(path.resolve()), str(destination.resolve()),
                        n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                    )
            self.assertTrue(injected[0])
            self.assertFalse(destination.exists())
            self.assertEqual(
                injected_sidecar.read_bytes(),
                b"late-safe-looking-sidecar",
            )

    def test_help_documents_binance_only_isolation_boundary(self):
        help_text = build_parser().format_help()
        self.assertIn("Binance-only", help_text)
        self.assertIn("other services", help_text)
        self.assertIn("journald", help_text)
        self.assertIn("VACUUM is never implicit", help_text)
        self.assertIn("--binance-root", help_text)
        self.assertIn("--keep-signal-count", help_text)
        self.assertIn("--keep-manifest-sha256", help_text)

    def test_separate_declared_binance_backup_root_is_allowed(self):
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "binance-trading-data"
            backup_root = Path(directory) / "binance-trading-backups"
            data_root.mkdir()
            backup_root.mkdir()
            path = self._database(data_root)
            (data_root / "state").mkdir()
            lock_file = data_root / "trading_bot.lock"
            destination = backup_root / "review.compacted.sqlite3"
            database, ledger, lock, roots, target = _validate_cli_scope(
                path,
                self._claim_ledger(path),
                lock_file,
                [data_root, backup_root],
                destination,
                False,
            )
            self.assertEqual(database, path.resolve())
            self.assertEqual(ledger, self._claim_ledger(path).resolve())
            self.assertEqual(lock, lock_file.resolve())
            self.assertEqual(roots, (data_root.resolve(), backup_root.resolve()))
            self.assertEqual(target, destination.resolve())

    def test_cli_reports_instance_lock_conflict_without_traceback(self):
        with tempfile.TemporaryDirectory() as directory:
            binance_root = Path(directory) / "binance-only"
            binance_root.mkdir()
            path = self._database(binance_root)
            self._create_official_lock(self._claim_ledger(path))
            self._signal(path, 3, "N01", "CURRENTUSDT", 0)
            count, manifest = self._attestation(path, 3)
            stderr = io.StringIO()
            with patch(
                "trading_bot.signal_retention.InstanceLock.acquire",
                side_effect=InstanceLockError("already held"),
            ), redirect_stderr(stderr):
                exit_code = self._cli(
                    [
                        "--dry-run",
                        "--db",
                        str(path.resolve()),
                        "--binance-root",
                        str(binance_root.resolve()),
                        "--lock-file",
                        str((binance_root / "trading_bot.lock").resolve()),
                        "--keep-scan-id",
                        "3",
                        "--keep-signal-count",
                        str(count),
                        "--keep-manifest-sha256",
                        manifest,
                        "--n16-claim-ledger",
                        str(self._claim_ledger(path).resolve()),
                    ]
                )
            self.assertEqual(exit_code, 1)
            self.assertIn("already held", stderr.getvalue())
            self.assertNotIn("Traceback", stderr.getvalue())

    def test_cli_official_lock_contract_rejects_before_scope_or_sqlite(self):
        for mode in ("wrong_name", "missing", "wrong_parent", "alias"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                binance_root = Path(directory) / "binance-only"
                binance_root.mkdir()
                path = self._database(binance_root)
                ledger = self._claim_ledger(path)
                lock_file = ledger.with_name("trading_bot.lock")
                lock_file.write_bytes(b"official-lock")
                external = Path(directory) / "external-lock"
                external.write_bytes(b"external-lock-sentinel")
                if mode == "wrong_name":
                    candidate = lock_file.with_name("alternate.lock")
                    lock_file.rename(candidate)
                    lock_file = candidate
                elif mode == "missing":
                    lock_file.unlink()
                elif mode == "wrong_parent":
                    other = Path(directory) / "other" / "state"
                    other.mkdir(parents=True)
                    lock_file = other / "trading_bot.lock"
                    lock_file.write_bytes(b"other-lock")
                else:
                    lock_file.unlink()
                    os.link(external, lock_file)
                external_before = _file_fingerprint(external)
                stderr = io.StringIO()
                with patch.object(
                    signal_retention_module,
                    "_validate_cli_scope",
                    side_effect=AssertionError("CLI scope must not run"),
                ) as validate_scope, patch.object(
                    signal_retention_module.sqlite3,
                    "connect",
                    side_effect=AssertionError("SQLite must not open"),
                ) as connect, redirect_stderr(stderr):
                    result = self._cli(
                        [
                            "--dry-run",
                            "--db", str(path.resolve()),
                            "--n16-claim-ledger", str(ledger.resolve()),
                            "--lock-file", str(lock_file.absolute()),
                            "--binance-root", str(binance_root.resolve()),
                            "--keep-scan-id", "3",
                            "--keep-signal-count", "1",
                        ]
                    )
                self.assertEqual(result, 1)
                validate_scope.assert_not_called()
                connect.assert_not_called()
                self.assertEqual(_file_fingerprint(external), external_before)
                self.assertIn("strategy signal maintenance failed", stderr.getvalue())

    def test_cli_rejects_database_as_lock_before_acquire_without_modifying_it(self):
        with tempfile.TemporaryDirectory() as directory:
            binance_root = Path(directory) / "binance-only"
            binance_root.mkdir()
            path = self._database(binance_root)
            self._create_official_lock(self._claim_ledger(path))
            self._signal(path, 3, "N01", "CURRENTUSDT", 0)
            count, manifest = self._attestation(path, 3)
            before = _file_sha256(path)
            stderr = io.StringIO()
            with patch(
                "trading_bot.signal_retention.InstanceLock.acquire"
            ) as acquire, redirect_stderr(stderr):
                exit_code = self._cli(
                    [
                        "--dry-run",
                        "--db",
                        str(path.resolve()),
                        "--binance-root",
                        str(binance_root.resolve()),
                        "--lock-file",
                        str(path.resolve()),
                        "--keep-scan-id",
                        "3",
                        "--keep-signal-count",
                        str(count),
                        "--keep-manifest-sha256",
                        manifest,
                        "--n16-claim-ledger",
                        str(self._claim_ledger(path).resolve()),
                    ]
                )
            self.assertEqual(exit_code, 1)
            acquire.assert_not_called()
            self.assertEqual(_file_sha256(path), before)
            self.assertIn("basename", stderr.getvalue())

    def test_cli_rejects_database_inode_swap_after_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            binance_root = Path(directory) / "binance-only"
            binance_root.mkdir()
            path = self._database(binance_root)
            self._create_official_lock(self._claim_ledger(path))
            self._signal(path, 3, "N01", "CURRENTUSDT", 0)
            count, manifest = self._attestation(path, 3)
            original_acquire = InstanceLock.acquire
            original_bytes = path.read_bytes()
            displaced = path.with_name("review.displaced.sqlite3")

            def acquire_then_swap(lock):
                acquired = original_acquire(lock)
                os.replace(str(path), str(displaced))
                path.write_bytes(original_bytes)
                return acquired

            stderr = io.StringIO()
            with patch(
                "trading_bot.signal_retention.InstanceLock.acquire",
                side_effect=acquire_then_swap,
                autospec=True,
            ), redirect_stderr(stderr):
                exit_code = self._cli(
                    [
                        "--dry-run",
                        "--db",
                        str(path.resolve()),
                        "--binance-root",
                        str(binance_root.resolve()),
                        "--lock-file",
                        str((binance_root / "trading_bot.lock").resolve()),
                        "--keep-scan-id",
                        "3",
                        "--keep-signal-count",
                        str(count),
                        "--keep-manifest-sha256",
                        manifest,
                        "--n16-claim-ledger",
                        str(self._claim_ledger(path).resolve()),
                    ]
                )
            self.assertEqual(exit_code, 1)
            self.assertIn("database identity changed after lock", stderr.getvalue())
            self.assertNotIn("Traceback", stderr.getvalue())

    def test_cli_rejects_vacuum_parent_symlink_swap_after_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "binance-trading-data"
            backup_root = Path(directory) / "binance-trading-backups"
            data_root.mkdir()
            backup_root.mkdir()
            destination_parent = backup_root / "daily"
            destination_parent.mkdir()
            real_parent = backup_root / "daily-real"
            path = self._database(data_root)
            self._create_official_lock(self._claim_ledger(path))
            destination = destination_parent / "review.compacted.sqlite3"
            original_acquire = InstanceLock.acquire

            def acquire_then_swap_parent(lock):
                acquired = original_acquire(lock)
                os.replace(str(destination_parent), str(real_parent))
                os.symlink(str(real_parent), str(destination_parent))
                return acquired

            stderr = io.StringIO()
            with patch(
                "trading_bot.signal_retention.InstanceLock.acquire",
                side_effect=acquire_then_swap_parent,
                autospec=True,
            ), redirect_stderr(stderr):
                exit_code = self._cli(
                    [
                        "--vacuum-into",
                        str(destination),
                        "--db",
                        str(path.resolve()),
                        "--binance-root",
                        str(data_root.resolve()),
                        "--binance-root",
                        str(backup_root.resolve()),
                        "--lock-file",
                        str((data_root / "trading_bot.lock").resolve()),
                        "--n16-claim-ledger",
                        str(self._claim_ledger(path).resolve()),
                    ]
                )
            self.assertEqual(exit_code, 1)
            self.assertIn("non-symlink directory", stderr.getvalue())
            self.assertFalse(destination.exists())

    def test_cli_rejects_source_database_hardlink_before_lock_or_sqlite(self):
        with tempfile.TemporaryDirectory() as directory:
            binance_root = Path(directory) / "binance-only"
            binance_root.mkdir()
            path = self._database(binance_root)
            self._create_official_lock(self._claim_ledger(path))
            self._signal(path, 3, "N01", "CURRENTUSDT", 0)
            count, manifest = self._attestation(path, 3)
            external = Path(directory) / "other-system.sqlite3"
            os.link(str(path), str(external))
            before = _file_fingerprint(path)
            stderr = io.StringIO()
            with patch(
                "trading_bot.signal_retention.InstanceLock.acquire"
            ) as acquire, patch(
                "trading_bot.signal_retention.sqlite3.connect"
            ) as connect, redirect_stderr(stderr):
                exit_code = self._cli(
                    [
                        "--apply",
                        "--db",
                        str(path.resolve()),
                        "--binance-root",
                        str(binance_root.resolve()),
                        "--lock-file",
                        str((binance_root / "trading_bot.lock").resolve()),
                        "--keep-scan-id",
                        "3",
                        "--keep-signal-count",
                        str(count),
                        "--keep-manifest-sha256",
                        manifest,
                        "--n16-claim-ledger",
                        str(self._claim_ledger(path).resolve()),
                    ]
                )
            self.assertEqual(exit_code, 1)
            acquire.assert_not_called()
            connect.assert_not_called()
            self.assertEqual(_file_fingerprint(path), before)
            self.assertEqual(_file_fingerprint(external), before)
            self.assertIn("exactly one hard link", stderr.getvalue())

    def test_cli_rejects_existing_lock_hardlink_before_pid_or_sqlite(self):
        with tempfile.TemporaryDirectory() as directory:
            binance_root = Path(directory) / "binance-only"
            binance_root.mkdir()
            path = self._database(binance_root)
            self._signal(path, 3, "N01", "CURRENTUSDT", 0)
            count, manifest = self._attestation(path, 3)
            external = Path(directory) / "other-system.lock"
            external.write_bytes(b"OTHER-SYSTEM-LOCK\n")
            lock_file = binance_root / "trading_bot.lock"
            os.link(str(external), str(lock_file))
            before = _file_fingerprint(external)
            stderr = io.StringIO()
            with patch(
                "trading_bot.signal_retention.InstanceLock.acquire"
            ) as acquire, patch(
                "trading_bot.signal_retention.sqlite3.connect"
            ) as connect, redirect_stderr(stderr):
                exit_code = self._cli(
                    [
                        "--dry-run",
                        "--db",
                        str(path.resolve()),
                        "--binance-root",
                        str(binance_root.resolve()),
                        "--lock-file",
                        str(lock_file.resolve()),
                        "--keep-scan-id",
                        "3",
                        "--keep-signal-count",
                        str(count),
                        "--keep-manifest-sha256",
                        manifest,
                        "--n16-claim-ledger",
                        str(self._claim_ledger(path).resolve()),
                    ]
                )
            self.assertEqual(exit_code, 1)
            acquire.assert_not_called()
            connect.assert_not_called()
            self.assertEqual(_file_fingerprint(external), before)
            self.assertEqual(_file_fingerprint(lock_file), before)
            self.assertIn("exactly one hard link", stderr.getvalue())

    def test_cli_lock_capture_to_acquire_alias_swap_has_no_external_write(self):
        for alias_kind in ("symlink", "hardlink"):
            with self.subTest(alias_kind=alias_kind), tempfile.TemporaryDirectory() as directory:
                binance_root = Path(directory) / "binance-only"
                binance_root.mkdir()
                path = self._database(binance_root)
                self._create_official_lock(self._claim_ledger(path))
                self._signal(path, 3, "N01", "CURRENTUSDT", 0)
                count, manifest = self._attestation(path, 3)
                lock_file = binance_root / "trading_bot.lock"
                lock_file.write_bytes(b"STALE-BINANCE-LOCK\n")
                displaced = binance_root / "trading_bot.lock.displaced"
                external = Path(directory) / "other-system.lock"
                external.write_bytes(b"OTHER-SYSTEM-SENTINEL\n")
                original_acquire = InstanceLock.acquire
                injected = {}

                def swap_then_acquire(lock):
                    os.replace(str(lock_file), str(displaced))
                    if alias_kind == "symlink":
                        os.symlink(str(external), str(lock_file))
                    else:
                        os.link(str(external), str(lock_file))
                    injected["fingerprint"] = _file_fingerprint(external)
                    return original_acquire(lock)

                stderr = io.StringIO()
                with patch(
                    "trading_bot.signal_retention.InstanceLock.acquire",
                    side_effect=swap_then_acquire,
                    autospec=True,
                ), patch(
                    "trading_bot.signal_retention.sqlite3.connect"
                ) as connect, redirect_stderr(stderr):
                    exit_code = self._cli(
                        [
                            "--dry-run",
                            "--db",
                            str(path.resolve()),
                            "--binance-root",
                            str(binance_root.resolve()),
                            "--lock-file",
                            str(lock_file.resolve()),
                            "--keep-scan-id",
                            "3",
                            "--keep-signal-count",
                            str(count),
                            "--keep-manifest-sha256",
                            manifest,
                            "--n16-claim-ledger",
                            str(self._claim_ledger(path).resolve()),
                        ]
                    )
                self.assertEqual(exit_code, 1)
                connect.assert_not_called()
                self.assertEqual(
                    _file_fingerprint(external), injected["fingerprint"]
                )
                self.assertEqual(external.read_bytes(), b"OTHER-SYSTEM-SENTINEL\n")
                self.assertNotIn("Traceback", stderr.getvalue())

    def test_cli_missing_official_lock_is_not_created_during_acquire(self):
        with tempfile.TemporaryDirectory() as directory:
            binance_root = Path(directory) / "binance-only"
            binance_root.mkdir()
            path = self._database(binance_root)
            self._signal(path, 3, "N01", "CURRENTUSDT", 0)
            count, manifest = self._attestation(path, 3)
            lock_file = binance_root / "trading_bot.lock"
            stderr = io.StringIO()
            with patch(
                "trading_bot.signal_retention.InstanceLock.acquire"
            ) as acquire, patch(
                "trading_bot.signal_retention.sqlite3.connect"
            ) as connect, redirect_stderr(stderr):
                exit_code = self._cli(
                    [
                        "--dry-run",
                        "--db", str(path.resolve()),
                        "--binance-root", str(binance_root.resolve()),
                        "--lock-file", str(lock_file.resolve()),
                        "--keep-scan-id", "3",
                        "--keep-signal-count", str(count),
                        "--keep-manifest-sha256", manifest,
                        "--n16-claim-ledger",
                        str(self._claim_ledger(path).resolve()),
                    ]
                )
            self.assertEqual(exit_code, 1)
            acquire.assert_not_called()
            connect.assert_not_called()
            self.assertFalse(lock_file.exists())
            self.assertIn("must already exist", stderr.getvalue())

    def test_cli_rejects_hardlinked_source_sqlite_sidecars_before_open(self):
        for suffix in ("-wal", "-shm", "-journal"):
            with self.subTest(suffix=suffix), tempfile.TemporaryDirectory() as directory:
                binance_root = Path(directory) / "binance-only"
                binance_root.mkdir()
                path = self._database(binance_root)
                self._create_official_lock(self._claim_ledger(path))
                self._signal(path, 3, "N01", "CURRENTUSDT", 0)
                count, manifest = self._attestation(path, 3)
                with _sqlite_connection(path) as connection:
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    connection.execute("PRAGMA journal_mode=DELETE")
                sidecar = Path(str(path) + suffix)
                if os.path.lexists(str(sidecar)):
                    sidecar.unlink()
                external = Path(directory) / ("other-system" + suffix)
                payload = (b"\0" if suffix == "-journal" else b"X") + b"SAFE" * 700
                external.write_bytes(payload)
                os.link(str(external), str(sidecar))
                sentinel_before = _file_fingerprint(external)
                database_before = _file_fingerprint(path)
                stderr = io.StringIO()
                with patch(
                    "trading_bot.signal_retention.InstanceLock.acquire"
                ) as acquire, patch(
                    "trading_bot.signal_retention.sqlite3.connect"
                ) as connect, redirect_stderr(stderr):
                    exit_code = self._cli(
                        [
                            "--apply",
                            "--db",
                            str(path.resolve()),
                            "--binance-root",
                            str(binance_root.resolve()),
                            "--lock-file",
                            str((binance_root / "trading_bot.lock").resolve()),
                            "--keep-scan-id",
                            "3",
                            "--keep-signal-count",
                            str(count),
                            "--keep-manifest-sha256",
                            manifest,
                            "--n16-claim-ledger",
                            str(self._claim_ledger(path).resolve()),
                        ]
                    )
                self.assertEqual(exit_code, 1)
                acquire.assert_not_called()
                connect.assert_not_called()
                self.assertEqual(_file_fingerprint(external), sentinel_before)
                self.assertEqual(_file_fingerprint(sidecar), sentinel_before)
                self.assertEqual(_file_fingerprint(path), database_before)
                self.assertIn("exactly one hard link", stderr.getvalue())

    def test_cli_rejects_sidecar_alias_between_read_only_and_write_open(self):
        with tempfile.TemporaryDirectory() as directory:
            binance_root = Path(directory) / "binance-only"
            binance_root.mkdir()
            path = self._database(binance_root)
            self._create_official_lock(self._claim_ledger(path))
            self._signal(path, 3, "N01", "CURRENTUSDT", 0)
            count, manifest = self._attestation(path, 3)
            with _sqlite_connection(path) as connection:
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                connection.execute("PRAGMA journal_mode=DELETE")
            sidecar = Path(str(path) + "-wal")
            if os.path.lexists(str(sidecar)):
                sidecar.unlink()
            external = Path(directory) / "other-system-wal"
            external.write_bytes(b"READ-TO-WRITE-SENTINEL" * 100)
            calls = {"count": 0}
            injected = {}

            def open_with_interstage_alias(*args, **kwargs):
                calls["count"] += 1
                if calls["count"] == 2:
                    os.link(str(external), str(sidecar))
                    injected["fingerprint"] = _file_fingerprint(external)
                return _open_database(*args, **kwargs)

            stderr = io.StringIO()
            with patch(
                "trading_bot.signal_retention._open_database",
                side_effect=open_with_interstage_alias,
            ), redirect_stderr(stderr):
                exit_code = self._cli(
                    [
                        "--apply",
                        "--db",
                        str(path.resolve()),
                        "--binance-root",
                        str(binance_root.resolve()),
                        "--lock-file",
                        str((binance_root / "trading_bot.lock").resolve()),
                        "--keep-scan-id",
                        "3",
                        "--keep-signal-count",
                        str(count),
                        "--keep-manifest-sha256",
                        manifest,
                        "--n16-claim-ledger",
                        str(self._claim_ledger(path).resolve()),
                    ]
                )
            self.assertEqual(exit_code, 1)
            # The explicit coverage-family pairing gate performs one
            # additional anchored read-only open before returning the
            # controlled sidecar-alias rejection.
            self.assertEqual(calls["count"], 4)
            self.assertEqual(_file_fingerprint(external), injected["fingerprint"])
            self.assertEqual(_file_fingerprint(sidecar), injected["fingerprint"])
            self.assertIn("exactly one hard link", stderr.getvalue())

    def test_cli_rejects_target_sidecar_alias_appearing_before_lock(self):
        for suffix in ("-wal", "-shm", "-journal"):
            with self.subTest(suffix=suffix), tempfile.TemporaryDirectory() as directory:
                binance_root = Path(directory) / "binance-only"
                binance_root.mkdir()
                path = self._database(binance_root)
                self._create_official_lock(self._claim_ledger(path))
                destination = self._vacuum_destination(
                    binance_root, "review.compacted.sqlite3"
                )
                sidecar = Path(str(destination) + suffix)
                external = Path(directory) / ("other-vacuum" + suffix)
                external.write_bytes(b"EXTERNAL-VACUUM-SENTINEL" * 100)
                original_acquire = InstanceLock.acquire
                injected = {}

                def inject_then_acquire(lock):
                    os.link(str(external), str(sidecar))
                    injected["fingerprint"] = _file_fingerprint(external)
                    return original_acquire(lock)

                stderr = io.StringIO()
                with patch(
                    "trading_bot.signal_retention.InstanceLock.acquire",
                    side_effect=inject_then_acquire,
                    autospec=True,
                ), patch(
                    "trading_bot.signal_retention.vacuum_signal_database_into"
                ) as vacuum, redirect_stderr(stderr):
                    exit_code = self._cli(
                        [
                            "--vacuum-into",
                            str(destination),
                            "--db",
                            str(path.resolve()),
                            "--binance-root",
                            str(binance_root.resolve()),
                            "--lock-file",
                            str((binance_root / "trading_bot.lock").resolve()),
                            "--n16-claim-ledger",
                            str(self._claim_ledger(path).resolve()),
                        ]
                    )
                self.assertEqual(exit_code, 1)
                vacuum.assert_not_called()
                self.assertEqual(
                    _file_fingerprint(external), injected["fingerprint"]
                )
                self.assertEqual(_file_fingerprint(sidecar), injected["fingerprint"])
                self.assertIn("exactly one hard link", stderr.getvalue())

    def test_vacuum_rejects_preexisting_target_sidecars_before_source_open(self):
        for suffix in ("-wal", "-shm", "-journal"):
            with self.subTest(suffix=suffix), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                destination = self._vacuum_destination(directory)
                sidecar = Path(str(destination) + suffix)
                external = Path(directory) / ("external" + suffix)
                external.write_bytes(b"PREEXISTING-TARGET-SIDECAR")
                os.link(str(external), str(sidecar))
                before = _file_fingerprint(external)
                with patch("trading_bot.signal_retention._open_database") as opened:
                    with self.assertRaises(SignalRetentionMaintenanceError):
                        self._vacuum_raw(
                            str(path.resolve()), str(destination.resolve()),
                            n16_claim_ledger=str(self._claim_ledger(path).resolve()),
                        )
                opened.assert_not_called()
                self.assertEqual(_file_fingerprint(external), before)
                self.assertEqual(_file_fingerprint(sidecar), before)

    def test_cli_rejects_vacuum_output_hardlink_alias_before_reporting(self):
        with tempfile.TemporaryDirectory() as directory:
            binance_root = Path(directory) / "binance-only"
            binance_root.mkdir()
            path = self._database(binance_root)
            self._create_official_lock(self._claim_ledger(path))
            destination = self._vacuum_destination(
                binance_root, "review.compacted.sqlite3"
            )
            source_hash = _file_sha256(path)

            class ForgedOutput:
                identity = (0, 0)

            def forge_alias(*args, **kwargs):
                os.link(str(path), str(destination))
                kwargs["_precommit_validator"](ForgedOutput())
                raise AssertionError("precommit validator accepted forged alias")

            stderr = io.StringIO()
            with patch(
                "trading_bot.signal_retention.vacuum_signal_database_into",
                side_effect=forge_alias,
            ), redirect_stderr(stderr):
                exit_code = self._cli(
                    [
                        "--vacuum-into",
                        str(destination),
                        "--db",
                        str(path.resolve()),
                        "--binance-root",
                        str(binance_root.resolve()),
                        "--lock-file",
                        str((binance_root / "trading_bot.lock").resolve()),
                        "--n16-claim-ledger",
                        str(self._claim_ledger(path).resolve()),
                    ]
                )
            self.assertEqual(exit_code, 1)
            self.assertIn("exactly one hard link", stderr.getvalue())
            self.assertTrue(destination.exists())
            self.assertTrue(path.samefile(destination))
            self.assertEqual(_file_sha256(path), source_hash)


if __name__ == "__main__":
    unittest.main()
