import hashlib
import logging
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from trading_bot.monitor import FundingCandidate
from trading_bot.recorder import (
    ReviewRecorder,
    _N16_INDEX_SQL,
    _N16_PREINSTALL_EVENTS_TABLE_SQL,
)
from trading_bot.signal_retention import (
    SignalRetentionMaintenanceError,
    _install_n16_claim_boundary,
    _install_retention_schema,
    _retention_schema_version,
    _strict_absent_retention_history_is_empty,
    _validate_retention_dependencies,
    _verify_retention_schema,
)
from tests.recorder_test_utils import make_test_recorder, test_claim_ledger_path


@contextmanager
def _sqlite_connection(*args, **kwargs):
    connection = sqlite3.connect(*args, **kwargs)
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def _candidate(symbol="BTCUSDT"):
    return FundingCandidate(
        symbol=symbol,
        funding_rate=Decimal("-0.01"),
        mark_price=Decimal("100"),
    )


def _database_file_evidence(path):
    evidence = {}
    for suffix in ("", "-wal", "-shm", "-journal"):
        candidate = Path(str(path) + suffix)
        if not candidate.exists():
            evidence[suffix] = None
            continue
        stat = os.lstat(str(candidate))
        evidence[suffix] = (
            hashlib.sha256(candidate.read_bytes()).hexdigest(),
            stat.st_size,
            stat.st_mtime_ns,
            stat.st_ctime_ns,
            stat.st_nlink,
            stat.st_dev,
            stat.st_ino,
        )
    return evidence


def _checkpoint_sidecar_free(path):
    connection = sqlite3.connect(path)
    try:
        result = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if result is None or result[0] != 0:
            raise AssertionError("test database checkpoint failed")
    finally:
        connection.close()
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(path) + suffix)
        if sidecar.exists():
            sidecar.unlink()
    journal = Path(str(path) + "-journal")
    if journal.exists():
        raise AssertionError("unexpected rollback journal in test fixture")


def _drop_empty_n16_schema_for_legacy_fixture(connection):
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
    for (name,) in connection.execute(
        "SELECT name FROM sqlite_schema WHERE type='trigger' "
        "AND name GLOB 'trg_n16_*' ORDER BY name"
    ).fetchall():
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
    ledger_path = test_claim_ledger_path(database_path)
    for suffix in ("-wal", "-shm", "-journal", ""):
        candidate = Path(str(ledger_path) + suffix)
        if candidate.exists():
            candidate.unlink()


class SignalRetentionOriginRuntimeTests(unittest.TestCase):
    def _path(self, directory):
        return Path(directory) / "review.sqlite3"

    def _recorder(self, directory):
        return make_test_recorder(
            str(self._path(directory)),
            logging.getLogger("signal-retention-origin-test"),
        )

    def _begin(self, recorder, symbol="BTCUSDT"):
        scan_id = recorder.begin_scan(1, [_candidate(symbol)], True)
        self.assertIsNotNone(scan_id)
        return scan_id

    def _record(self, recorder, scan_id, passed=False, structure_id=None):
        return recorder.record_strategy_signal(
            scan_id=scan_id,
            strategy_id="N06" if passed else "N01",
            symbol="BTCUSDT",
            funding_rate="-0.01",
            matched_patterns=(),
            trend_slope="1",
            current_bullish=True,
            passed=passed,
            decision="PASSED" if passed else "REJECTED",
            reason="PASSED" if passed else "NO_MATCH",
            structure_id=structure_id,
            detail={"evidence": "bounded", "structure_id": structure_id},
        )

    def test_genesis_marker_is_canonical_and_stable_across_one_two_one(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = self._recorder(directory)
            expected_marker = (
                1,
                "COMPLETE",
                0,
                0,
                0,
                "0" * 64,
                "GENESIS",
            )
            for count in (1, 2, 1):
                scan_id = self._begin(recorder)
                for index in range(count):
                    self.assertIsNotNone(
                        self._record(recorder, scan_id, passed=False)
                    )
                self.assertTrue(
                    recorder.publish_strategy_signal_batch(scan_id, count)
                )
                with recorder._connect() as connection:
                    row = connection.execute(
                        """
                        SELECT retention_active, migration_state,
                               migration_cutoff_signal_id, source_signal_count,
                               source_passed_count, source_manifest_sha256,
                               retention_origin
                        FROM strategy_signal_current WHERE singleton_id = 1
                        """
                    ).fetchone()
                self.assertEqual(row, expected_marker)
                self.assertEqual(recorder.current_strategy_signal_scan_id(), scan_id)
                self.assertEqual(len(recorder.list_current_strategy_signals()), count)

    def test_fresh_abandoned_staging_survives_reopen_then_cleans_strictly(self):
        for passed in (False, True):
            with self.subTest(passed=passed), tempfile.TemporaryDirectory() as directory:
                recorder = self._recorder(directory)
                abandoned_scan = self._begin(recorder)
                structure_id = "n06-abandoned" if passed else None
                self.assertIsNotNone(
                    self._record(
                        recorder,
                        abandoned_scan,
                        passed=passed,
                        structure_id=structure_id,
                    )
                )
                reopened = self._recorder(directory)
                replacement_scan = self._begin(reopened, "ETHUSDT")
                with reopened._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_signals "
                            "WHERE scan_id = ?",
                            (abandoned_scan,),
                        ).fetchone(),
                        (0,),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_passed_signal_audits "
                            "WHERE source_scan_id = ?",
                            (abandoned_scan,),
                        ).fetchone(),
                        (0,),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                            "WHERE source_scan_id = ?",
                            (abandoned_scan,),
                        ).fetchone(),
                        (0,),
                    )
                self.assertIsNotNone(self._record(reopened, replacement_scan))
                self.assertTrue(
                    reopened.publish_strategy_signal_batch(replacement_scan, 1)
                )

    def test_unpublished_genesis_rejects_active_or_orphaned_evidence(self):
        for mode in ("active_claim", "orphan_signal"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                recorder = self._recorder(directory)
                scan_id = self._begin(recorder)
                signal_id = self._record(
                    recorder,
                    scan_id,
                    passed=True,
                    structure_id="n06-hostile-staging",
                )
                self.assertIsNotNone(signal_id)
                with _sqlite_connection(self._path(directory)) as connection:
                    connection.create_function(
                        "_n16_claim_chain_advance",
                        17,
                        recorder._advance_n16_claim_chain,
                    )
                    connection.create_function(
                        "_n16_guard_mutation_authorized",
                        3,
                        recorder._authorize_n16_guard_mutation,
                    )
                    if mode == "active_claim":
                        connection.execute(
                            "UPDATE strategy_passed_signal_audits "
                            "SET claim_state='ACTIVE' WHERE source_signal_id = ?",
                            (signal_id,),
                        )
                        connection.execute(
                            "UPDATE strategy_passed_structure_ledger "
                            "SET claim_state='ACTIVE' WHERE source_signal_id = ?",
                            (signal_id,),
                        )
                    else:
                        connection.execute(
                            "UPDATE strategy_signals SET scan_id=NULL WHERE id = ?",
                            (signal_id,),
                        )
                    connection.execute(
                        "ALTER TABLE strategy_paper_trades "
                        "DROP COLUMN last_checked_at"
                    )
                _checkpoint_sidecar_free(self._path(directory))
                before = _database_file_evidence(self._path(directory))
                with self.assertRaises(RuntimeError):
                    self._recorder(directory)
                self.assertEqual(
                    _database_file_evidence(self._path(directory)), before
                )
                with _sqlite_connection(self._path(directory)) as connection:
                    self.assertNotIn(
                        "last_checked_at",
                        [
                            row[1]
                            for row in connection.execute(
                                "PRAGMA table_info(strategy_paper_trades)"
                            )
                        ],
                    )

    def test_origin_marker_mismatches_block_all_current_readers(self):
        mutations = (
            ("retention_origin=NULL",),
            ("retention_origin='LEGACY'",),
            (
                "retention_origin='GENESIS', migration_cutoff_signal_id=1, "
                "source_signal_count=1, source_passed_count=0, "
                "source_manifest_sha256='" + "1" * 64 + "'",
            ),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation[0]), tempfile.TemporaryDirectory() as directory:
                recorder = self._recorder(directory)
                scan_id = self._begin(recorder)
                self.assertIsNotNone(self._record(recorder, scan_id))
                self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
                with recorder._connect() as connection:
                    connection.execute(
                        "UPDATE strategy_signal_current SET %s WHERE singleton_id=1"
                        % mutation[0]
                    )
                with self.assertRaises(RuntimeError):
                    recorder.current_strategy_signal_scan_id()
                with self.assertRaises(RuntimeError):
                    recorder.list_current_strategy_signals()
                self.assertIsNone(
                    recorder.begin_scan(1, [_candidate("ETHUSDT")], True)
                )

    def test_publish_trigger_marker_tamper_rolls_back_everything(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = self._recorder(directory)
            scan_id = self._begin(recorder)
            self.assertIsNotNone(self._record(recorder, scan_id))
            with recorder._connect() as connection:
                connection.execute(
                    """
                    CREATE TRIGGER hostile_retention_origin
                    AFTER UPDATE OF current_scan_id ON strategy_signal_current
                    BEGIN
                        UPDATE strategy_signal_current
                        SET retention_origin='LEGACY'
                        WHERE singleton_id=1;
                    END
                    """
                )
            self.assertFalse(recorder.publish_strategy_signal_batch(scan_id, 1))
            with _sqlite_connection(self._path(directory)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id, retention_origin "
                        "FROM strategy_signal_current WHERE singleton_id=1"
                    ).fetchone(),
                    (None, "GENESIS"),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT state FROM strategy_signal_batches WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    ("STAGING",),
                )


class SignalRetentionSchemaCertificationTests(unittest.TestCase):
    def _database(self, directory):
        path = Path(directory) / "review.sqlite3"
        make_test_recorder(
            str(path), logging.getLogger("signal-retention-schema-test")
        )
        return path

    def test_recorder_current_schema_is_strictly_certified(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            with _sqlite_connection(path) as connection:
                self.assertEqual(_retention_schema_version(connection), "CURRENT")
                _verify_retention_schema(connection)

    def test_runtime_preflight_rejects_noncurrent_schema_without_any_file_write(self):
        for mode in ("partial_claim", "wrong_index", "wrong_constraint"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                connection = sqlite3.connect(path)
                try:
                    _drop_empty_n16_schema_for_legacy_fixture(connection)
                    if mode == "partial_claim":
                        connection.execute("DROP INDEX idx_passed_audit_claim_batch")
                        connection.execute(
                            "DROP INDEX idx_passed_audit_claim_state_scan"
                        )
                        connection.execute(
                            "ALTER TABLE strategy_passed_signal_audits "
                            "DROP COLUMN claim_state"
                        )
                    elif mode == "wrong_index":
                        connection.execute("DROP INDEX idx_strategy_signals_scan_id")
                        connection.execute(
                            "CREATE INDEX idx_strategy_signals_scan_id "
                            "ON strategy_signals(symbol)"
                        )
                    else:
                        original = connection.execute(
                            "SELECT sql FROM sqlite_schema "
                            "WHERE type='table' "
                            "AND name='strategy_signal_current'"
                        ).fetchone()[0]
                        changed = original.replace(
                            "migration_state TEXT NOT NULL DEFAULT 'PENDING'",
                            "migration_state TEXT NOT NULL DEFAULT 'COMPLETE'",
                        )
                        self.assertNotEqual(changed, original)
                        connection.execute(
                            "ALTER TABLE strategy_signal_current "
                            "RENAME TO strategy_signal_current_old"
                        )
                        connection.execute(changed)
                        connection.execute(
                            "INSERT INTO strategy_signal_current "
                            "SELECT * FROM strategy_signal_current_old"
                        )
                        connection.execute(
                            "DROP TABLE strategy_signal_current_old"
                        )
                    with self.assertRaises(SignalRetentionMaintenanceError):
                        _verify_retention_schema(connection)
                    connection.commit()
                finally:
                    connection.close()
                _checkpoint_sidecar_free(path)
                before = _database_file_evidence(path)
                with self.assertRaisesRegex(RuntimeError, "maintenance"):
                    ReviewRecorder(
                        str(path),
                        logging.getLogger("runtime-schema-preflight-%s" % mode),
                        n16_claim_ledger_file=test_claim_ledger_path(path),
                    )
                self.assertEqual(_database_file_evidence(path), before)

    def test_runtime_preflight_rejects_dependencies_before_insert_or_ddl(self):
        for mode in ("trigger", "incoming_fk", "absent_incoming_fk"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                with _sqlite_connection(path) as connection:
                    connection.execute(
                        "CREATE TABLE runtime_preflight_sentinel "
                        "(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                    )
                    connection.execute(
                        "INSERT INTO runtime_preflight_sentinel "
                        "VALUES (1, 'UNCHANGED')"
                    )
                    if mode == "trigger":
                        connection.execute(
                            """
                            CREATE TRIGGER hostile_runtime_current_insert
                            BEFORE INSERT ON strategy_signal_current
                            BEGIN
                                UPDATE runtime_preflight_sentinel
                                SET value='MUTATED' WHERE id=1;
                            END
                            """
                        )
                    else:
                        connection.execute(
                            "CREATE TABLE hostile_runtime_child ("
                            "id INTEGER PRIMARY KEY, current_id INTEGER, "
                            "FOREIGN KEY(current_id) "
                            "REFERENCES strategy_signal_current(singleton_id) "
                            "ON DELETE CASCADE)"
                        )
                        if mode == "absent_incoming_fk":
                            _drop_empty_n16_schema_for_legacy_fixture(connection)
                            connection.execute(
                                "DROP INDEX idx_strategy_signals_scan_id"
                            )
                            for table in (
                                "strategy_signal_current",
                                "strategy_signal_batches",
                                "strategy_passed_signal_audits",
                                "strategy_passed_structure_ledger",
                            ):
                                connection.execute("DROP TABLE %s" % table)
                    with self.assertRaises(SignalRetentionMaintenanceError):
                        _validate_retention_dependencies(connection)
                _checkpoint_sidecar_free(path)
                before = _database_file_evidence(path)
                with self.assertRaisesRegex(RuntimeError, "maintenance"):
                    ReviewRecorder(
                        str(path),
                        logging.getLogger("runtime-dependency-%s" % mode),
                        n16_claim_ledger_file=test_claim_ledger_path(path),
                    )
                self.assertEqual(_database_file_evidence(path), before)
                with _sqlite_connection(path) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT value FROM runtime_preflight_sentinel"
                        ).fetchone(),
                        ("UNCHANGED",),
                    )

    def test_runtime_preflight_rejects_invalid_or_maintenance_only_state(self):
        for mode in (
            "missing_singleton",
            "partial_marker",
            "backfilled",
            "pending_history",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                recorder = ReviewRecorder(
                    str(path),
                    logging.getLogger("runtime-state-fixture"),
                    n16_claim_ledger_file=test_claim_ledger_path(path),
                )
                if mode == "pending_history":
                    scan_id = recorder.begin_scan(
                        1, [_candidate("BTCUSDT")], True
                    )
                    self.assertIsNotNone(scan_id)
                    self.assertIsNotNone(
                        recorder.record_strategy_signal(
                            scan_id=scan_id,
                            strategy_id="N01",
                            symbol="BTCUSDT",
                            funding_rate="-0.01",
                            matched_patterns=(),
                            trend_slope="1",
                            current_bullish=True,
                            passed=False,
                            decision="REJECTED",
                            reason="NO_MATCH",
                            detail={"pending": True},
                        )
                    )
                with recorder._connect() as connection:
                    if mode == "missing_singleton":
                        connection.execute(
                            "DELETE FROM strategy_signal_current "
                            "WHERE singleton_id=1"
                        )
                    elif mode == "partial_marker":
                        connection.execute(
                            "UPDATE strategy_signal_current "
                            "SET migration_cutoff_signal_id=1 "
                            "WHERE singleton_id=1"
                        )
                    elif mode == "backfilled":
                        connection.execute(
                            "INSERT INTO scans (id, started_at, mode, "
                            "scanned_count, candidate_count, candidates_json) "
                            "VALUES (1, '2026-07-14T00:00:00+00:00', "
                            "'DRY_RUN', 1, 1, '[]')"
                        )
                        connection.execute(
                            """
                            UPDATE strategy_signal_current
                            SET current_scan_id=1, retention_active=0,
                                migration_state='BACKFILLED',
                                migration_cutoff_signal_id=1,
                                source_signal_count=1,
                                source_passed_count=0,
                                source_manifest_sha256=?,
                                retention_origin='LEGACY'
                            WHERE singleton_id=1
                            """,
                            ("1" * 64,),
                        )
                    else:
                        connection.execute(
                            """
                            UPDATE strategy_signal_current
                            SET current_scan_id=NULL, retention_active=0,
                                migration_state='PENDING',
                                migration_cutoff_signal_id=NULL,
                                source_signal_count=NULL,
                                source_passed_count=NULL,
                                source_manifest_sha256=NULL,
                                retention_origin=NULL
                            WHERE singleton_id=1
                            """
                        )
                _checkpoint_sidecar_free(path)
                before = _database_file_evidence(path)
                with self.assertRaises(RuntimeError):
                    ReviewRecorder(
                        str(path),
                        logging.getLogger("runtime-invalid-state-%s" % mode),
                        n16_claim_ledger_file=test_claim_ledger_path(path),
                    )
                self.assertEqual(_database_file_evidence(path), before)

    def test_runtime_snapshot_handles_sidecar_free_wal_header_and_latest_wal_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            _checkpoint_sidecar_free(path)
            header = path.read_bytes()[:20]
            self.assertEqual(header[18:20], b"\x02\x02")
            reopened = ReviewRecorder(
                str(path),
                logging.getLogger("sidecar-free-wal-header"),
                n16_claim_ledger_file=test_claim_ledger_path(path),
            )
            self.assertIsNone(reopened.current_strategy_signal_scan_id())

        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            _checkpoint_sidecar_free(path)
            writer_script = """
import os
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1])
connection.execute("PRAGMA journal_mode=WAL")
connection.execute("PRAGMA wal_autocheckpoint=0")
connection.execute("DROP INDEX idx_strategy_signals_scan_id")
connection.execute(
    "CREATE INDEX idx_strategy_signals_scan_id ON strategy_signals(symbol)"
)
connection.commit()
checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
if checkpoint is None or checkpoint[0] != 0:
    os._exit(2)
connection.execute("DROP INDEX idx_strategy_signals_scan_id")
connection.execute(
    "CREATE INDEX idx_strategy_signals_scan_id ON strategy_signals(scan_id)"
)
connection.commit()
os._exit(0)
"""
            completed = subprocess.run(
                [sys.executable, "-c", writer_script, str(path)],
                check=False,
            )
            self.assertEqual(completed.returncode, 0)
            wal = Path(str(path) + "-wal")
            self.assertTrue(wal.is_file())
            self.assertGreater(wal.stat().st_size, 0)
            with self.assertRaisesRegex(
                RuntimeError,
                "protected Review catalog changed outside maintenance",
            ):
                ReviewRecorder(
                    str(path),
                    logging.getLogger("latest-schema-in-wal"),
                    n16_claim_ledger_file=test_claim_ledger_path(path),
                )

    def test_runtime_nonempty_wal_rejects_latest_invalid_schema_before_runtime_dml(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            with _sqlite_connection(path) as connection:
                connection.execute(
                    "CREATE TABLE runtime_wal_sentinel "
                    "(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO runtime_wal_sentinel "
                    "VALUES (1, 'UNCHANGED')"
                )
            _checkpoint_sidecar_free(path)
            main_before = hashlib.sha256(path.read_bytes()).hexdigest()
            writer = sqlite3.connect(path)
            try:
                self.assertEqual(
                    writer.execute("PRAGMA journal_mode=WAL").fetchone(),
                    ("wal",),
                )
                writer.execute("DROP INDEX idx_strategy_signals_scan_id")
                writer.execute(
                    "CREATE INDEX idx_strategy_signals_scan_id "
                    "ON strategy_signals(symbol)"
                )
                writer.commit()
                wal = Path(str(path) + "-wal")
                self.assertGreater(wal.stat().st_size, 0)
                with self.assertRaisesRegex(RuntimeError, "maintenance"):
                    ReviewRecorder(
                        str(path),
                        logging.getLogger("invalid-schema-in-wal"),
                        n16_claim_ledger_file=test_claim_ledger_path(path),
                    )
                self.assertEqual(
                    hashlib.sha256(path.read_bytes()).hexdigest(), main_before
                )
                self.assertEqual(
                    writer.execute(
                        "SELECT value FROM runtime_wal_sentinel"
                    ).fetchone(),
                    ("UNCHANGED",),
                )
                self.assertEqual(
                    writer.execute(
                        "SELECT sql FROM sqlite_schema "
                        "WHERE type='index' "
                        "AND name='idx_strategy_signals_scan_id'"
                    ).fetchone(),
                    (
                        "CREATE INDEX idx_strategy_signals_scan_id "
                        "ON strategy_signals(symbol)",
                    ),
                )
            finally:
                writer.close()

    def test_runtime_sidecar_aliases_and_journal_fail_before_external_write(self):
        for mode in ("hardlink_wal", "symlink_wal", "journal"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                _checkpoint_sidecar_free(path)
                external = Path(directory) / "external-sentinel"
                external.write_bytes(b"OTHER-SYSTEM-SENTINEL")
                if mode == "hardlink_wal":
                    os.link(str(external), str(path) + "-wal")
                elif mode == "symlink_wal":
                    os.symlink(str(external), str(path) + "-wal")
                else:
                    Path(str(path) + "-journal").write_bytes(b"HOT-JOURNAL")
                sentinel_before = _database_file_evidence(external)[""]
                with self.assertRaises(RuntimeError):
                    ReviewRecorder(
                        str(path),
                        logging.getLogger("runtime-sidecar-%s" % mode),
                        n16_claim_ledger_file=test_claim_ledger_path(path),
                    )
                self.assertEqual(
                    _database_file_evidence(external)[""], sentinel_before
                )

    def test_missing_or_empty_main_rejects_all_external_sidecar_hardlinks(self):
        for main_mode in ("missing", "empty"):
            for suffix in ("-wal", "-shm", "-journal"):
                with self.subTest(main=main_mode, suffix=suffix), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    path = root / "review.sqlite3"
                    if main_mode == "empty":
                        path.write_bytes(b"")
                    external = root / ("external%s" % suffix.replace("-", "_"))
                    external.write_bytes(b"OTHER-SYSTEM-SIDECAR")
                    os.link(str(external), str(path) + suffix)
                    external_before = _database_file_evidence(external)[""]
                    directory_before = tuple(sorted(item.name for item in root.iterdir()))
                    with self.assertRaises(RuntimeError):
                        ReviewRecorder(
                            str(path),
                            logging.getLogger(
                                "new-main-sidecar-%s-%s" % (main_mode, suffix)
                            ),
                            n16_claim_ledger_file=test_claim_ledger_path(path),
                        )
                    self.assertEqual(
                        _database_file_evidence(external)[""], external_before
                    )
                    self.assertEqual(
                        tuple(sorted(item.name for item in root.iterdir())),
                        directory_before,
                    )
                    self.assertEqual(path.exists(), main_mode == "empty")

    def test_runtime_rejects_main_and_parent_aliases_without_external_changes(self):
        for mode in (
            "main_hardlink",
            "main_symlink",
            "parent_symlink",
            "ancestor_symlink_missing_tail",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                external_parent = root / "external-system"
                external_parent.mkdir()
                external = external_parent / "external.sqlite3"
                with _sqlite_connection(external) as connection:
                    connection.execute(
                        "CREATE TABLE sentinel "
                        "(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                    )
                    connection.execute(
                        "INSERT INTO sentinel VALUES (1, 'UNCHANGED')"
                    )
                binance_parent = root / "binance-only"
                binance_parent.mkdir()
                if mode == "main_hardlink":
                    path = binance_parent / "review.sqlite3"
                    os.link(str(external), str(path))
                elif mode == "main_symlink":
                    path = binance_parent / "review.sqlite3"
                    os.symlink(str(external), str(path))
                elif mode == "parent_symlink":
                    linked_parent = root / "binance-parent-link"
                    os.symlink(str(external_parent), str(linked_parent))
                    path = linked_parent / "external.sqlite3"
                else:
                    linked_ancestor = root / "binance-ancestor-link"
                    os.symlink(str(external_parent), str(linked_ancestor))
                    path = linked_ancestor / "missing-tail" / "review.sqlite3"
                external_before = _database_file_evidence(external)
                directory_before = tuple(
                    sorted(item.name for item in external_parent.iterdir())
                )
                with self.assertRaises(RuntimeError):
                    ReviewRecorder(
                        str(path),
                        logging.getLogger("runtime-main-alias-%s" % mode),
                        n16_claim_ledger_file=test_claim_ledger_path(path),
                    )
                self.assertEqual(_database_file_evidence(external), external_before)
                self.assertEqual(
                    tuple(sorted(item.name for item in external_parent.iterdir())),
                    directory_before,
                )
                self.assertFalse((external_parent / "missing-tail").exists())

    def test_runtime_rejects_existing_tail_beneath_untrusted_ancestor_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            external_parent = root / "external-system"
            external_tail = external_parent / "existing-tail"
            external_tail.mkdir(parents=True)
            linked_ancestor = root / "binance-ancestor-link"
            os.symlink(str(external_parent), str(linked_ancestor))
            path = linked_ancestor / "existing-tail" / "review.sqlite3"
            before = tuple(sorted(item.name for item in external_tail.iterdir()))
            with self.assertRaisesRegex(RuntimeError, "parent chain"):
                ReviewRecorder(
                    str(path),
                    logging.getLogger("runtime-existing-ancestor-link"),
                    n16_claim_ledger_file=test_claim_ledger_path(path),
                )
            self.assertEqual(
                tuple(sorted(item.name for item in external_tail.iterdir())),
                before,
            )
            self.assertFalse(path.exists())

    def test_explicit_n16_install_uses_attested_parent_fd(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binance_parent = root / "binance-only"
            binance_parent.mkdir()
            saved_parent = root / "binance-original"
            external_parent = root / "external-system"
            external_parent.mkdir()
            path = binance_parent / "review.sqlite3"
            with _sqlite_connection(path) as connection:
                connection.commit()
            state_parent = root / "state-only"
            state_parent.mkdir()
            claim_ledger = state_parent / "n16_claim_ledger.sqlite3"
            original_validate = ReviewRecorder._validated_runtime_sidecars
            swapped = []

            def swap_parent_before_create(recorder):
                if not swapped:
                    os.replace(binance_parent, saved_parent)
                    os.symlink(str(external_parent), str(binance_parent))
                    swapped.append(True)
                return original_validate(recorder)

            with patch.object(
                ReviewRecorder,
                "_validated_runtime_sidecars",
                autospec=True,
                side_effect=swap_parent_before_create,
            ):
                with self.assertRaises(RuntimeError):
                    _install_n16_claim_boundary(
                        path.resolve(),
                        claim_ledger.resolve(),
                    )
            self.assertEqual(swapped, [True])
            self.assertEqual(tuple(external_parent.iterdir()), ())
            self.assertFalse((external_parent / "review.sqlite3").exists())

    def test_runtime_connect_swap_is_rejected_before_wal_pragma(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "review.sqlite3"
            recorder = make_test_recorder(
                str(path),
                logging.getLogger("runtime-connect-swap-fixture"),
            )
            _checkpoint_sidecar_free(path)
            external = root / "external.sqlite3"
            with _sqlite_connection(external) as connection:
                connection.execute(
                    "CREATE TABLE sentinel "
                    "(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO sentinel VALUES (1, 'UNCHANGED')"
                )
            original_connect = sqlite3.connect
            swapped = []
            external_before = []

            def swapping_connect(database, *args, **kwargs):
                if not swapped:
                    original = root / "review.original.sqlite3"
                    os.replace(path, original)
                    os.link(str(external), str(path))
                    external_before.append(_database_file_evidence(external))
                    swapped.append(True)
                return original_connect(database, *args, **kwargs)

            with patch(
                "trading_bot.recorder.sqlite3.connect",
                side_effect=swapping_connect,
            ):
                with self.assertRaises(RuntimeError) as raised:
                    recorder.current_strategy_signal_scan_id()
            self.assertNotIsInstance(raised.exception, UnboundLocalError)
            self.assertEqual(swapped, [True])
            self.assertEqual(
                _database_file_evidence(external), external_before[0]
            )
            self.assertFalse(Path(str(external) + "-wal").exists())
            self.assertFalse(Path(str(external) + "-shm").exists())

    def test_runtime_connect_symlink_swap_is_rejected_before_wal_pragma(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "review.sqlite3"
            recorder = make_test_recorder(
                str(path),
                logging.getLogger("runtime-connect-symlink-swap"),
            )
            _checkpoint_sidecar_free(path)
            external = root / "external.sqlite3"
            with _sqlite_connection(external) as connection:
                connection.execute(
                    "CREATE TABLE sentinel "
                    "(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO sentinel VALUES (1, 'UNCHANGED')"
                )
            original_connect = sqlite3.connect
            swapped = []
            external_before = []

            def swapping_connect(database, *args, **kwargs):
                if not swapped:
                    original = root / "review.original.sqlite3"
                    os.replace(path, original)
                    os.symlink(str(external), str(path))
                    external_before.append(_database_file_evidence(external))
                    swapped.append(True)
                return original_connect(database, *args, **kwargs)

            with patch(
                "trading_bot.recorder.sqlite3.connect",
                side_effect=swapping_connect,
            ):
                with self.assertRaises(RuntimeError) as raised:
                    recorder.current_strategy_signal_scan_id()
            self.assertNotIsInstance(raised.exception, UnboundLocalError)
            self.assertEqual(swapped, [True])
            self.assertEqual(
                _database_file_evidence(external), external_before[0]
            )
            self.assertFalse(Path(str(external) + "-wal").exists())
            self.assertFalse(Path(str(external) + "-shm").exists())

    def test_runtime_connect_swap_then_restore_cannot_hide_opened_external_inode(self):
        for mode in ("hardlink", "symlink"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                path = root / "review.sqlite3"
                recorder = make_test_recorder(
                    str(path),
                    logging.getLogger("runtime-restored-swap-%s" % mode),
                )
                _checkpoint_sidecar_free(path)
                external = root / "external.sqlite3"
                with _sqlite_connection(external) as connection:
                    connection.execute(
                        "CREATE TABLE sentinel "
                        "(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                    )
                    connection.execute(
                        "INSERT INTO sentinel VALUES (1, 'UNCHANGED')"
                    )
                external_hash = hashlib.sha256(external.read_bytes()).hexdigest()
                original_connect = sqlite3.connect
                swapped = []

                def swapping_connect(database, *args, **kwargs):
                    if swapped:
                        return original_connect(database, *args, **kwargs)
                    original = root / "review.original.sqlite3"
                    os.replace(path, original)
                    if mode == "hardlink":
                        os.link(str(external), str(path))
                    else:
                        os.symlink(str(external), str(path))
                    opened = original_connect(database, *args, **kwargs)
                    os.unlink(path)
                    os.replace(original, path)
                    swapped.append(True)
                    return opened

                with patch(
                    "trading_bot.recorder.sqlite3.connect",
                    side_effect=swapping_connect,
                ):
                    with self.assertRaises(RuntimeError) as raised:
                        recorder.current_strategy_signal_scan_id()
                self.assertNotIsInstance(raised.exception, UnboundLocalError)
                self.assertEqual(swapped, [True])
                self.assertEqual(
                    hashlib.sha256(external.read_bytes()).hexdigest(),
                    external_hash,
                )
                with _sqlite_connection(external) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT name FROM sqlite_schema WHERE type='table' "
                            "ORDER BY name"
                        ).fetchall(),
                        [("sentinel",)],
                    )
                    self.assertEqual(
                        connection.execute("SELECT * FROM sentinel").fetchall(),
                        [(1, "UNCHANGED")],
                    )
                self.assertFalse(Path(str(external) + "-wal").exists())
                self.assertFalse(Path(str(external) + "-shm").exists())

    def test_runtime_parent_swap_then_restore_never_creates_or_uses_external_database(self):
        for external_exists in (False, True):
            with self.subTest(external_exists=external_exists), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                binance_parent = root / "binance-only"
                binance_parent.mkdir()
                path = binance_parent / "review.sqlite3"
                recorder = make_test_recorder(
                    str(path),
                    logging.getLogger(
                        "runtime-parent-restored-swap-%s" % external_exists
                    ),
                )
                _checkpoint_sidecar_free(path)
                saved_parent = root / "binance-saved"
                external_parent = root / "external-system"
                external_parent.mkdir()
                external = external_parent / path.name
                if external_exists:
                    with _sqlite_connection(external) as connection:
                        connection.execute(
                            "CREATE TABLE sentinel "
                            "(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                        )
                        connection.execute(
                            "INSERT INTO sentinel VALUES (1, 'UNCHANGED')"
                        )
                external_before = _database_file_evidence(external)
                external_directory_before = tuple(
                    sorted(item.name for item in external_parent.iterdir())
                )
                original_connect = sqlite3.connect
                swaps = []

                def swapping_connect(database, *args, **kwargs):
                    if swaps:
                        return original_connect(database, *args, **kwargs)
                    os.replace(binance_parent, saved_parent)
                    os.symlink(str(external_parent), str(binance_parent))
                    try:
                        opened = original_connect(database, *args, **kwargs)
                    except BaseException:
                        os.unlink(binance_parent)
                        os.replace(saved_parent, binance_parent)
                        swaps.append(True)
                        raise
                    os.unlink(binance_parent)
                    os.replace(saved_parent, binance_parent)
                    swaps.append(True)
                    return opened

                with patch(
                    "trading_bot.recorder.sqlite3.connect",
                    side_effect=swapping_connect,
                ):
                    with self.assertRaises((RuntimeError, sqlite3.OperationalError)) as raised:
                        recorder.current_strategy_signal_scan_id()
                self.assertNotIsInstance(raised.exception, UnboundLocalError)
                self.assertEqual(swaps, [True])
                self.assertEqual(
                    _database_file_evidence(external), external_before
                )
                self.assertEqual(
                    tuple(sorted(item.name for item in external_parent.iterdir())),
                    external_directory_before,
                )
                self.assertFalse(Path(str(external) + "-wal").exists())
                self.assertFalse(Path(str(external) + "-shm").exists())

    def test_runtime_read_only_preflight_holds_descriptor_lock_through_close(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            import trading_bot.signal_retention as signal_retention_module

            original_version = signal_retention_module._retention_schema_version
            first_entered = threading.Event()
            second_entered = threading.Event()
            release_first = threading.Event()
            errors = []

            def gated_version(connection):
                if threading.current_thread().name == "first-preflight":
                    first_entered.set()
                    if not release_first.wait(5):
                        raise RuntimeError("timed out waiting to release preflight")
                else:
                    second_entered.set()
                return original_version(connection)

            def construct():
                try:
                    ReviewRecorder(
                        str(path),
                        logging.getLogger("serialized-preflight"),
                        n16_claim_ledger_file=test_claim_ledger_path(path),
                    )
                except Exception as exc:  # pragma: no cover - asserted below
                    errors.append(exc)

            with patch.object(
                signal_retention_module,
                "_retention_schema_version",
                side_effect=gated_version,
            ):
                first = threading.Thread(
                    target=construct, name="first-preflight"
                )
                second = threading.Thread(
                    target=construct, name="second-preflight"
                )
                first.start()
                self.assertTrue(first_entered.wait(5))
                second.start()
                second_ran_while_first_connection_was_open = (
                    second_entered.wait(0.25)
                )
                release_first.set()
                first.join(10)
                second.join(10)

            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
            self.assertFalse(second_ran_while_first_connection_was_open)
            self.assertTrue(second_entered.is_set())
            self.assertEqual(errors, [])

    def test_parallel_recorders_do_not_misclassify_reused_descriptors(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            worker_count = 4
            start = threading.Barrier(worker_count)
            errors = []
            errors_lock = threading.Lock()

            def worker():
                try:
                    start.wait(5)
                    for _ in range(4):
                        recorder = ReviewRecorder(
                            str(path),
                            logging.getLogger("parallel-recorder"),
                            n16_claim_ledger_file=test_claim_ledger_path(path),
                        )
                        self.assertIsNone(
                            recorder.current_strategy_signal_scan_id()
                        )
                except Exception as exc:  # pragma: no cover - asserted below
                    with errors_lock:
                        errors.append(exc)

            threads = [threading.Thread(target=worker) for _ in range(worker_count)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(20)

            self.assertTrue(all(not thread.is_alive() for thread in threads))
            self.assertEqual(errors, [])

    def test_runtime_connection_rejects_disappeared_expected_fd_before_first_sql(self):
        recorder = object.__new__(ReviewRecorder)
        recorder._runtime_database_identity = (101, 202)

        class ForbiddenConnection:
            def execute(self, *_args, **_kwargs):
                raise AssertionError("SQLite SQL must not run before fd rejection")

        with patch.object(
            recorder,
            "_runtime_regular_fd_snapshot",
            return_value={8: (101, 202), 9: (101, 202)},
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "descriptor is not attested",
            ):
                recorder._validate_opened_runtime_connection(
                    ForbiddenConnection(),
                    {7: (101, 202)},
                )

    def test_runtime_connection_rejects_wrong_main_with_expected_inode_decoy(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            recorder = ReviewRecorder(
                str(path),
                logging.getLogger("runtime-wrong-main-decoy"),
                n16_claim_ledger_file=test_claim_ledger_path(path),
            )
            external = Path(directory) / "external.sqlite3"
            with _sqlite_connection(external) as wrong_connection, patch.object(
                recorder,
                "_runtime_regular_fd_snapshot",
                return_value={7: recorder._runtime_database_identity},
            ):
                # A concurrent descriptor for the expected inode can satisfy
                # the FD-count proof, but it cannot bind a wrong SQLite main.
                with self.assertRaisesRegex(
                    RuntimeError,
                    "opened ReviewRecorder database is not attested",
                ):
                    recorder._validate_opened_runtime_connection(
                        wrong_connection,
                        {},
                    )

    def test_runtime_absent_catalog_rejects_case_and_object_type_collisions(self):
        for mode in (
            "uppercase_index",
            "same_name_view",
            "case_table",
            "case_source",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                with _sqlite_connection(path) as connection:
                    if mode == "case_source":
                        connection.execute(
                            "INSERT INTO strategy_signals ("
                            "scan_id, strategy_id, symbol, funding_rate, "
                            "matched_patterns, trend_slope, current_bullish, "
                            "passed, decision, reason, detail_json, created_at) "
                            "VALUES (NULL, 'N01', 'BTCUSDT', '-0.01', '[]', "
                            "'1', 1, 0, 'REJECTED', 'NO_MATCH', '{}', "
                            "'2026-07-14T00:00:00+00:00')"
                        )
                    _drop_empty_n16_schema_for_legacy_fixture(connection)
                    # This fixture deliberately removes the retention owner
                    # batch table below.  Remove the dependent micro trigger
                    # first so newer SQLite versions can still construct the
                    # intended catalog-name collision; runtime certification
                    # must reject the resulting missing trigger as well.
                    connection.execute(
                        "DROP TRIGGER IF EXISTS trg_micro_lifecycle_no_delete"
                    )
                    connection.execute("DROP INDEX idx_strategy_signals_scan_id")
                    for table in (
                        "strategy_signal_current",
                        "strategy_signal_batches",
                        "strategy_passed_signal_audits",
                        "strategy_passed_structure_ledger",
                    ):
                        connection.execute("DROP TABLE %s" % table)
                    if mode == "uppercase_index":
                        connection.execute(
                            "CREATE INDEX IDX_STRATEGY_SIGNALS_SCAN_ID "
                            "ON strategy_signals(symbol)"
                        )
                    elif mode == "same_name_view":
                        connection.execute(
                            "CREATE VIEW strategy_signal_current AS SELECT 1 AS forged"
                        )
                    elif mode == "case_table":
                        connection.execute(
                            "CREATE TABLE Strategy_Signal_Current "
                            "(singleton_id INTEGER PRIMARY KEY, forged TEXT)"
                        )
                    else:
                        connection.execute(
                            "ALTER TABLE strategy_signals RENAME TO signals_tmp"
                        )
                        connection.execute(
                            "ALTER TABLE signals_tmp RENAME TO Strategy_Signals"
                        )
                    with self.assertRaises(SignalRetentionMaintenanceError):
                        _retention_schema_version(connection)
                with _sqlite_connection(path) as connection:
                    catalog_before = connection.execute(
                        "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                        "ORDER BY type COLLATE BINARY, name COLLATE BINARY"
                    ).fetchall()
                _checkpoint_sidecar_free(path)
                before = _database_file_evidence(path)
                with self.assertRaisesRegex(RuntimeError, "maintenance"):
                    ReviewRecorder(
                        str(path),
                        logging.getLogger("runtime-catalog-collision-%s" % mode),
                        n16_claim_ledger_file=test_claim_ledger_path(path),
                    )
                self.assertEqual(_database_file_evidence(path), before)
                with _sqlite_connection(path) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                            "ORDER BY type COLLATE BINARY, name COLLATE BINARY"
                        ).fetchall(),
                        catalog_before,
                    )

    def test_runtime_absent_schema_requires_strictly_empty_signal_history(self):
        with tempfile.TemporaryDirectory() as directory:
            fresh = Path(directory) / "fresh.sqlite3"
            recorder = make_test_recorder(
                str(fresh), logging.getLogger("strict-empty-genesis")
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT migration_cutoff_signal_id, source_signal_count, "
                        "source_passed_count, source_manifest_sha256, "
                        "retention_origin FROM strategy_signal_current"
                    ).fetchone(),
                    (0, 0, 0, "0" * 64, "GENESIS"),
                )

        for mode in ("legacy_rows", "sequence_only"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                recorder = ReviewRecorder(
                    str(path),
                    logging.getLogger("absent-history-fixture"),
                    n16_claim_ledger_file=test_claim_ledger_path(path),
                )
                scan_id = recorder.begin_scan(
                    9, [_candidate("S%02dUSDT" % index) for index in range(9)], True
                )
                self.assertIsNotNone(scan_id)
                for index in range(9):
                    self.assertIsNotNone(
                        recorder.record_strategy_signal(
                            scan_id=scan_id,
                            strategy_id="N01",
                            symbol="S%02dUSDT" % index,
                            funding_rate="-0.01",
                            matched_patterns=(),
                            trend_slope="1",
                            current_bullish=True,
                            passed=False,
                            decision="REJECTED",
                            reason="NO_MATCH",
                            detail={"index": index},
                        )
                    )
                with recorder._connect() as connection:
                    if mode == "sequence_only":
                        connection.execute("DELETE FROM strategy_signals")
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
                        _retention_schema_version(connection), "ABSENT"
                    )
                    self.assertFalse(
                        _strict_absent_retention_history_is_empty(connection)
                    )
                _checkpoint_sidecar_free(path)
                before = _database_file_evidence(path)
                with self.assertRaisesRegex(RuntimeError, "maintenance"):
                    ReviewRecorder(
                        str(path),
                        logging.getLogger("absent-history-runtime-%s" % mode),
                        n16_claim_ledger_file=test_claim_ledger_path(path),
                    )
                self.assertEqual(_database_file_evidence(path), before)

    def test_constraintless_same_columns_and_extra_index_are_rejected(self):
        for mode in ("constraintless", "extra_index"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                with _sqlite_connection(path) as connection:
                    _drop_empty_n16_schema_for_legacy_fixture(connection)
                    if mode == "constraintless":
                        connection.execute("DROP TABLE strategy_signal_batches")
                        connection.execute(
                            """
                            CREATE TABLE strategy_signal_batches (
                                scan_id INTEGER, state TEXT,
                                recorded_count INTEGER DEFAULT 0,
                                expected_count INTEGER, first_signal_id INTEGER,
                                last_signal_id INTEGER, manifest_sha256 TEXT,
                                completed_at TEXT, created_at TEXT, updated_at TEXT
                            )
                            """
                        )
                    else:
                        connection.execute(
                            "CREATE INDEX hostile_extra_retention_index "
                            "ON strategy_signal_batches(recorded_count)"
                        )
                    with self.assertRaises(SignalRetentionMaintenanceError):
                        _verify_retention_schema(connection)

    def test_table_checks_fk_unique_autoincrement_and_defaults_are_exact(self):
        mutations = {
            "state_check": (
                "strategy_signal_batches",
                "state IN ('STAGING', 'CURRENT')",
                "state IN ('STAGING', 'CURRENT', 'EVIL')",
            ),
            "foreign_key": (
                "strategy_signal_batches",
                "FOREIGN KEY(scan_id) REFERENCES scans(id)",
                "FOREIGN KEY(scan_id) REFERENCES scans(id) ON DELETE CASCADE",
            ),
            "autoincrement": (
                "strategy_passed_signal_audits",
                "PRIMARY KEY AUTOINCREMENT",
                "PRIMARY KEY",
            ),
            "source_unique": (
                "strategy_passed_signal_audits",
                "source_signal_id INTEGER NOT NULL UNIQUE",
                "source_signal_id INTEGER NOT NULL",
            ),
            "compound_unique": (
                "strategy_passed_structure_ledger",
                "UNIQUE(strategy_id, structure_id)",
                "UNIQUE(strategy_id, symbol, structure_id)",
            ),
            "default": (
                "strategy_signal_current",
                "migration_state TEXT NOT NULL DEFAULT 'PENDING'",
                "migration_state TEXT NOT NULL DEFAULT 'COMPLETE'",
            ),
        }
        for mode, (table, old, new) in mutations.items():
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                with _sqlite_connection(path) as connection:
                    original = connection.execute(
                        "SELECT sql FROM sqlite_schema "
                        "WHERE type='table' AND name=?",
                        (table,),
                    ).fetchone()[0]
                    changed = original.replace(old, new)
                    self.assertNotEqual(changed, original)
                    explicit_indexes = connection.execute(
                        "SELECT name FROM sqlite_schema WHERE type='index' "
                        "AND tbl_name=? AND sql IS NOT NULL",
                        (table,),
                    ).fetchall()
                    for (index_name,) in explicit_indexes:
                        connection.execute("DROP INDEX \"%s\"" % index_name)
                    old_table = table + "_old"
                    connection.execute(
                        "ALTER TABLE \"%s\" RENAME TO \"%s\""
                        % (table, old_table)
                    )
                    connection.execute(changed)
                    connection.execute("DROP TABLE \"%s\"" % old_table)
                    connection.commit()
                    with self.assertRaises(SignalRetentionMaintenanceError):
                        _verify_retention_schema(connection)

    def test_owned_index_flags_columns_collation_order_and_predicate_are_exact(self):
        replacements = {
            "wrong_column": (
                "idx_strategy_signals_scan_id",
                "CREATE INDEX idx_strategy_signals_scan_id "
                "ON strategy_signals(symbol)",
            ),
            "wrong_unique": (
                "idx_strategy_signals_scan_id",
                "CREATE UNIQUE INDEX idx_strategy_signals_scan_id "
                "ON strategy_signals(scan_id)",
            ),
            "nocase_desc": (
                "idx_strategy_signals_scan_id",
                "CREATE INDEX idx_strategy_signals_scan_id "
                "ON strategy_signals(scan_id COLLATE NOCASE DESC)",
            ),
            "expression": (
                "idx_strategy_signals_scan_id",
                "CREATE INDEX idx_strategy_signals_scan_id "
                "ON strategy_signals(lower(strategy_id))",
            ),
            "wrong_partial_predicate": (
                "idx_strategy_signal_one_staging",
                "CREATE UNIQUE INDEX idx_strategy_signal_one_staging "
                "ON strategy_signal_batches(state) WHERE state='CURRENT'",
            ),
            "wrong_partial_flag": (
                "idx_strategy_signal_one_staging",
                "CREATE UNIQUE INDEX idx_strategy_signal_one_staging "
                "ON strategy_signal_batches(state)",
            ),
            "reverse_columns": (
                "idx_passed_structure_symbol",
                "CREATE INDEX idx_passed_structure_symbol "
                "ON strategy_passed_structure_ledger"
                "(structure_id, symbol, strategy_id)",
            ),
        }
        for mode, (name, replacement) in replacements.items():
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                with _sqlite_connection(path) as connection:
                    connection.execute("DROP INDEX %s" % name)
                    connection.execute(replacement)
                    with self.assertRaises(SignalRetentionMaintenanceError):
                        _verify_retention_schema(connection)

    def test_exact_originless_and_preclaim_legacy_upgrade_atomically(self):
        for preclaim in (False, True):
            with self.subTest(preclaim=preclaim), tempfile.TemporaryDirectory() as directory:
                path = self._database(directory)
                with _sqlite_connection(path) as connection:
                    _drop_empty_n16_schema_for_legacy_fixture(connection)
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
                    if preclaim:
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
                    connection.commit()
                    self.assertEqual(
                        _retention_schema_version(connection),
                        "LEGACY_PRECLAIM" if preclaim else "LEGACY_ORIGINLESS",
                    )
                    _install_retention_schema(connection)
                    _verify_retention_schema(connection)
                    self.assertEqual(
                        _retention_schema_version(connection), "CURRENT"
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT retention_origin "
                            "FROM strategy_signal_current"
                        ).fetchone(),
                        (None,),
                    )

    def test_mixed_legacy_schema_is_rejected_before_upgrade(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._database(directory)
            with _sqlite_connection(path) as connection:
                _drop_empty_n16_schema_for_legacy_fixture(connection)
                connection.execute("DROP INDEX idx_passed_audit_claim_batch")
                connection.execute(
                    "DROP INDEX idx_passed_audit_claim_state_scan"
                )
                connection.execute(
                    "ALTER TABLE strategy_passed_signal_audits "
                    "DROP COLUMN claim_state"
                )
                with self.assertRaises(SignalRetentionMaintenanceError):
                    _retention_schema_version(connection)
                with self.assertRaises(SignalRetentionMaintenanceError):
                    _install_retention_schema(connection)


if __name__ == "__main__":
    unittest.main()
