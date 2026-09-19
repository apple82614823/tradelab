from decimal import Decimal
import hashlib
import logging
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tests.recorder_test_utils import (
    commit_test_schema_change,
    make_test_recorder,
)
from trading_bot.monitor import FundingCandidate
from trading_bot.recorder import ReviewRecorder


def _candidate(symbol="BTCUSDT"):
    return FundingCandidate(
        symbol=symbol,
        funding_rate=Decimal("-0.01"),
        mark_price=Decimal("100"),
    )


def _record_signal(
    recorder,
    scan_id,
    *,
    strategy_id="N01",
    symbol="BTCUSDT",
    passed=False,
    structure_id=None,
    reason=None,
    detail=None,
):
    return recorder.record_strategy_signal(
        scan_id=scan_id,
        strategy_id=strategy_id,
        symbol=symbol,
        funding_rate="-0.01",
        matched_patterns=(),
        trend_slope="1",
        current_bullish=True,
        passed=passed,
        decision="PASSED" if passed else "REJECTED",
        reason=reason or ("PASSED" if passed else "NO_MATCH"),
        structure_id=structure_id,
        detail=(
            {"structure_id": structure_id, "evidence": symbol}
            if detail is None
            else detail
        ),
    )


class StrategySignalRetentionTests(unittest.TestCase):
    def _recorder(self, tmpdir):
        return make_test_recorder(
            str(Path(tmpdir) / "review.sqlite3"),
            logging.getLogger("test_signal_retention"),
        )

    def _begin(self, recorder, symbol="BTCUSDT"):
        scan_id = recorder.begin_scan(1, [_candidate(symbol)], dry_run=True)
        self.assertIsNotNone(scan_id)
        return scan_id

    def test_last_completed_current_remains_visible_while_staging_then_switches(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            first_scan = self._begin(recorder)
            self.assertIsNotNone(_record_signal(recorder, first_scan))
            self.assertIsNotNone(
                _record_signal(
                    recorder,
                    first_scan,
                    strategy_id="N02",
                    symbol="ETHUSDT",
                )
            )
            self.assertIsNone(recorder.current_strategy_signal_scan_id())
            self.assertEqual(recorder.list_current_strategy_signals(), [])
            self.assertTrue(recorder.publish_strategy_signal_batch(first_scan, 2))

            first_rows = recorder.list_current_strategy_signals()
            self.assertEqual(recorder.current_strategy_signal_scan_id(), first_scan)
            self.assertEqual([row[2] for row in first_rows], ["BTCUSDT", "ETHUSDT"])

            second_scan = self._begin(recorder, "SOLUSDT")
            self.assertIsNotNone(
                _record_signal(recorder, second_scan, symbol="SOLUSDT")
            )
            self.assertEqual(recorder.current_strategy_signal_scan_id(), first_scan)
            self.assertEqual(recorder.list_current_strategy_signals(), first_rows)

            self.assertTrue(recorder.publish_strategy_signal_batch(second_scan, 1))
            self.assertEqual(recorder.current_strategy_signal_scan_id(), second_scan)
            self.assertEqual(
                [row[2] for row in recorder.list_current_strategy_signals()],
                ["SOLUSDT"],
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM strategy_signals").fetchone()[0],
                    1,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT state, scan_id FROM strategy_signal_batches"
                    ).fetchall(),
                    [("CURRENT", second_scan)],
                )

    def test_incomplete_batch_never_switches_and_next_begin_discards_only_staging(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            current_scan = self._begin(recorder)
            self.assertIsNotNone(_record_signal(recorder, current_scan))
            self.assertTrue(recorder.publish_strategy_signal_batch(current_scan, 1))
            current_rows = recorder.list_current_strategy_signals()

            failed_scan = self._begin(recorder, "ETHUSDT")
            self.assertIsNotNone(
                _record_signal(recorder, failed_scan, symbol="ETHUSDT")
            )
            self.assertFalse(recorder.publish_strategy_signal_batch(failed_scan, 2))
            self.assertEqual(recorder.current_strategy_signal_scan_id(), current_scan)
            self.assertEqual(recorder.list_current_strategy_signals(), current_rows)

            replacement_scan = self._begin(recorder, "SOLUSDT")
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT scan_id, state FROM strategy_signal_batches ORDER BY scan_id"
                    ).fetchall(),
                    [(current_scan, "CURRENT"), (replacement_scan, "STAGING")],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id = ?",
                        (failed_scan,),
                    ).fetchone()[0],
                    0,
                )

    def test_switch_cleanup_failure_rolls_back_and_retry_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            first_scan = self._begin(recorder)
            self.assertIsNotNone(_record_signal(recorder, first_scan))
            self.assertTrue(recorder.publish_strategy_signal_batch(first_scan, 1))
            second_scan = self._begin(recorder, "ETHUSDT")
            self.assertIsNotNone(
                _record_signal(recorder, second_scan, symbol="ETHUSDT")
            )
            with recorder._connect() as connection:
                connection.execute(
                    "CREATE TRIGGER fail_current_cleanup BEFORE DELETE ON strategy_signals "
                    "WHEN OLD.scan_id = %d BEGIN SELECT RAISE(ABORT, 'cleanup failed'); END"
                    % first_scan
                )
                commit_test_schema_change(recorder, connection)
            self.assertFalse(recorder.publish_strategy_signal_batch(second_scan, 1))
            self.assertEqual(recorder.current_strategy_signal_scan_id(), first_scan)
            self.assertEqual(
                [row[2] for row in recorder.list_current_strategy_signals()],
                ["BTCUSDT"],
            )
            with recorder._connect() as connection:
                connection.execute("DROP TRIGGER fail_current_cleanup")
                commit_test_schema_change(recorder, connection)
            self.assertTrue(recorder.publish_strategy_signal_batch(second_scan, 1))
            self.assertTrue(recorder.publish_strategy_signal_batch(second_scan, 1))
            self.assertEqual(recorder.current_strategy_signal_scan_id(), second_scan)

    def test_switch_rejects_incomplete_old_current_without_partial_changes(self):
        for column, value in (("expected_count", None), ("completed_at", None)):
            with self.subTest(column=column), tempfile.TemporaryDirectory() as tmpdir:
                recorder = self._recorder(tmpdir)
                old_scan = self._begin(recorder)
                old_signal = _record_signal(
                    recorder,
                    old_scan,
                    strategy_id="N06",
                    structure_id=f"n06-old-{column}",
                    passed=True,
                )
                self.assertTrue(recorder.publish_strategy_signal_batch(old_scan, 1))
                new_scan = self._begin(recorder, "ETHUSDT")
                new_signal = _record_signal(
                    recorder,
                    new_scan,
                    strategy_id="N07",
                    symbol="ETHUSDT",
                    structure_id=f"n07-new-{column}",
                    passed=True,
                )
                self.assertIsNotNone(new_signal)
                with recorder._connect() as connection:
                    connection.execute(
                        f"UPDATE strategy_signal_batches SET {column} = ? "
                        "WHERE scan_id = ?",
                        (value, old_scan),
                    )
                    before = {
                        "pointer": connection.execute(
                            "SELECT * FROM strategy_signal_current"
                        ).fetchall(),
                        "batches": connection.execute(
                            "SELECT * FROM strategy_signal_batches ORDER BY scan_id"
                        ).fetchall(),
                        "signals": connection.execute(
                            "SELECT * FROM strategy_signals ORDER BY id"
                        ).fetchall(),
                        "audits": connection.execute(
                            "SELECT * FROM strategy_passed_signal_audits ORDER BY id"
                        ).fetchall(),
                        "ledgers": connection.execute(
                            "SELECT * FROM strategy_passed_structure_ledger ORDER BY id"
                        ).fetchall(),
                    }
                self.assertFalse(
                    recorder.publish_strategy_signal_batch(new_scan, 1)
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT * FROM strategy_signal_current"
                        ).fetchall(),
                        before["pointer"],
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT * FROM strategy_signal_batches ORDER BY scan_id"
                        ).fetchall(),
                        before["batches"],
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT * FROM strategy_signals ORDER BY id"
                        ).fetchall(),
                        before["signals"],
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT * FROM strategy_passed_signal_audits ORDER BY id"
                        ).fetchall(),
                        before["audits"],
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT * FROM strategy_passed_structure_ledger ORDER BY id"
                        ).fetchall(),
                        before["ledgers"],
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT id FROM strategy_signals WHERE id = ?",
                            (old_signal,),
                        ).fetchone(),
                        (old_signal,),
                    )

    def test_current_publish_retry_rejects_manifest_tamper(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            scan_id = self._begin(recorder)
            self.assertIsNotNone(_record_signal(recorder, scan_id))
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            with recorder._connect() as connection:
                cursor = connection.execute(
                    "UPDATE strategy_signal_batches "
                    "SET manifest_sha256 = ? WHERE scan_id = ?",
                    ("f" * 64, scan_id),
                )
                self.assertEqual(cursor.rowcount, 1)

            self.assertFalse(
                recorder.publish_strategy_signal_batch(scan_id, 1)
            )

    def test_current_publish_retry_uses_immutable_audit_after_execution_update(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            scan_id = self._begin(recorder)
            signal_id = _record_signal(
                recorder,
                scan_id,
                strategy_id="N06",
                structure_id="n06-published-before-plan-update",
                passed=True,
            )
            self.assertIsNotNone(signal_id)
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            with recorder._connect() as connection:
                batch_before = connection.execute(
                    "SELECT * FROM strategy_signal_batches WHERE scan_id = ?",
                    (scan_id,),
                ).fetchone()
                audit_before = connection.execute(
                    "SELECT * FROM strategy_passed_signal_audits "
                    "WHERE source_signal_id = ?",
                    (signal_id,),
                ).fetchone()
                ledger_before = connection.execute(
                    "SELECT * FROM strategy_passed_structure_ledger "
                    "WHERE source_signal_id = ?",
                    (signal_id,),
                ).fetchone()

            recorder.update_strategy_signal(
                signal_id,
                decision="PLAN_REJECTED",
                reason="TRADE_PLAN_INVALID:TEST",
                detail={"plan_error": "bounded-test-detail"},
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT decision, reason FROM strategy_signals WHERE id = ?",
                        (signal_id,),
                    ).fetchone(),
                    ("PLAN_REJECTED", "TRADE_PLAN_INVALID:TEST"),
                )

            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            self.assertEqual(len(recorder.list_current_strategy_signals()), 1)
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_signal_batches WHERE scan_id = ?",
                        (scan_id,),
                    ).fetchone(),
                    batch_before,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_signal_audits "
                        "WHERE source_signal_id = ?",
                        (signal_id,),
                    ).fetchone(),
                    audit_before,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_structure_ledger "
                        "WHERE source_signal_id = ?",
                        (signal_id,),
                    ).fetchone(),
                    ledger_before,
                )

            next_scan = self._begin(recorder, "ETHUSDT")
            self.assertIsNotNone(
                _record_signal(recorder, next_scan, symbol="ETHUSDT")
            )
            self.assertTrue(recorder.publish_strategy_signal_batch(next_scan, 1))
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_signal_audits "
                        "WHERE source_signal_id = ?",
                        (signal_id,),
                    ).fetchone(),
                    audit_before,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_structure_ledger "
                        "WHERE source_signal_id = ?",
                        (signal_id,),
                    ).fetchone(),
                    ledger_before,
                )

    def test_current_publish_retry_rejects_each_immutable_identity_change(self):
        mutations = {
            "symbol": ("symbol", "ETHUSDT"),
            "strategy": ("strategy_id", "N07"),
            "passed": ("passed", 0),
            "structure": ("structure_id", "n06-forged-structure"),
        }
        for name, (column, value) in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                recorder = self._recorder(tmpdir)
                scan_id = self._begin(recorder)
                signal_id = _record_signal(
                    recorder,
                    scan_id,
                    strategy_id="N06",
                    structure_id="n06-immutable-publication-identity",
                    passed=True,
                )
                self.assertIsNotNone(signal_id)
                self.assertTrue(
                    recorder.publish_strategy_signal_batch(scan_id, 1)
                )
                recorder.update_strategy_signal(
                    signal_id,
                    decision="LIVE_OPENED",
                    reason="LIVE_OPENED",
                    detail={"trade_review_id": 123},
                )
                with recorder._connect() as connection:
                    cursor = connection.execute(
                        f"UPDATE strategy_signals SET {column} = ? WHERE id = ?",
                        (value, signal_id),
                    )
                    self.assertEqual(cursor.rowcount, 1)
                    tampered = connection.execute(
                        "SELECT * FROM strategy_signals WHERE id = ?",
                        (signal_id,),
                    ).fetchone()
                self.assertFalse(
                    recorder.publish_strategy_signal_batch(scan_id, 1)
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT * FROM strategy_signals WHERE id = ?",
                            (signal_id,),
                        ).fetchone(),
                        tampered,
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT current_scan_id FROM strategy_signal_current "
                            "WHERE singleton_id = 1"
                        ).fetchone(),
                        (scan_id,),
                    )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            scan_id = self._begin(recorder)
            signal_id = _record_signal(
                recorder,
                scan_id,
                strategy_id="N06",
                structure_id="n06-immutable-scan-identity",
                passed=True,
            )
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            next_scan = self._begin(recorder, "ETHUSDT")
            with recorder._connect() as connection:
                connection.execute(
                    "UPDATE strategy_signals SET scan_id = ? WHERE id = ?",
                    (next_scan, signal_id),
                )
            self.assertFalse(recorder.publish_strategy_signal_batch(scan_id, 1))

    def test_current_rejected_row_remains_a_full_immutable_commitment(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            scan_id = self._begin(recorder)
            signal_id = _record_signal(recorder, scan_id, passed=False)
            self.assertIsNotNone(signal_id)
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            with recorder._connect() as connection:
                connection.execute(
                    "UPDATE strategy_signals SET decision = ?, reason = ?, "
                    "detail_json = ? WHERE id = ?",
                    ("FORGED", "FORGED", "{}", signal_id),
                )
            self.assertFalse(recorder.publish_strategy_signal_batch(scan_id, 1))
            with self.assertRaises(RuntimeError):
                recorder.list_current_strategy_signals()

    def test_current_execution_overlay_is_bounded_strict_json(self):
        class TextSubclass(str):
            pass

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            scan_id = self._begin(recorder)
            signal_id = _record_signal(
                recorder,
                scan_id,
                strategy_id="N06",
                structure_id="n06-overlay-bounds",
                passed=True,
            )
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            with recorder._connect() as connection:
                original = connection.execute(
                    "SELECT decision, reason, detail_json FROM strategy_signals "
                    "WHERE id = ?",
                    (signal_id,),
                ).fetchone()

            recorder.update_strategy_signal(
                signal_id,
                decision=TextSubclass("LIVE_OPENED"),
            )
            recorder.update_strategy_signal(
                signal_id,
                detail={"oversized": "x" * 262_145},
            )
            recorder.update_strategy_signal(
                signal_id,
                detail={"invalid": object()},
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT decision, reason, detail_json FROM strategy_signals "
                        "WHERE id = ?",
                        (signal_id,),
                    ).fetchone(),
                    original,
                )

            recorder.update_strategy_signal(
                signal_id,
                decision="LIVE_OPENED",
                reason="LIVE_OPENED",
                detail={"trade_review_id": 123},
            )
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            self.assertEqual(len(recorder.list_current_strategy_signals()), 1)

            with recorder._connect() as connection:
                connection.execute(
                    "UPDATE strategy_signals SET detail_json = ? WHERE id = ?",
                    ('{"duplicate": 1, "duplicate": 2}', signal_id),
                )
            self.assertFalse(recorder.publish_strategy_signal_batch(scan_id, 1))
            with self.assertRaises(RuntimeError):
                recorder.list_current_strategy_signals()

    def test_pointer_after_update_corruption_rolls_back_current_and_claim_activation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            old_scan = self._begin(recorder)
            old_signal_id = _record_signal(
                recorder,
                old_scan,
                strategy_id="N06",
                structure_id="n06-old-active-before-pointer-trigger",
                passed=True,
            )
            self.assertIsNotNone(old_signal_id)
            self.assertTrue(recorder.publish_strategy_signal_batch(old_scan, 1))
            old_current = tuple(recorder.list_current_strategy_signals())
            with recorder._connect() as connection:
                old_audit = connection.execute(
                    "SELECT * FROM strategy_passed_signal_audits "
                    "WHERE source_signal_id = ?",
                    (old_signal_id,),
                ).fetchone()
                old_ledger = connection.execute(
                    "SELECT * FROM strategy_passed_structure_ledger "
                    "WHERE source_signal_id = ?",
                    (old_signal_id,),
                ).fetchone()

            staged_scan = self._begin(recorder, "ETHUSDT")
            staged_signal_id = _record_signal(
                recorder,
                staged_scan,
                strategy_id="N07",
                symbol="ETHUSDT",
                structure_id="n07-staged-pointer-trigger",
                passed=True,
            )
            self.assertIsNotNone(staged_signal_id)
            with recorder._connect() as connection:
                connection.execute(
                    "CREATE TRIGGER corrupt_new_current_after_pointer "
                    "AFTER UPDATE OF current_scan_id ON strategy_signal_current "
                    f"WHEN NEW.current_scan_id = {staged_scan} BEGIN "
                    "DELETE FROM strategy_signals "
                    "WHERE scan_id = NEW.current_scan_id; END"
                )
                commit_test_schema_change(recorder, connection)

            self.assertFalse(
                recorder.publish_strategy_signal_batch(staged_scan, 1)
            )
            self.assertEqual(recorder.current_strategy_signal_scan_id(), old_scan)
            self.assertEqual(
                tuple(recorder.list_current_strategy_signals()), old_current
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_signal_audits "
                        "WHERE source_signal_id = ?",
                        (old_signal_id,),
                    ).fetchone(),
                    old_audit,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_structure_ledger "
                        "WHERE source_signal_id = ?",
                        (old_signal_id,),
                    ).fetchone(),
                    old_ledger,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT state FROM strategy_signal_batches "
                        "WHERE scan_id = ?",
                        (staged_scan,),
                    ).fetchone(),
                    ("STAGING",),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id = ?",
                        (staged_scan,),
                    ).fetchone(),
                    (1,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT claim_state FROM strategy_passed_signal_audits "
                        "WHERE source_signal_id = ?",
                        (staged_signal_id,),
                    ).fetchone(),
                    ("STAGED",),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT claim_state FROM strategy_passed_structure_ledger "
                        "WHERE source_signal_id = ?",
                        (staged_signal_id,),
                    ).fetchone(),
                    ("STAGED",),
                )
                connection.execute("DROP TRIGGER corrupt_new_current_after_pointer")
                commit_test_schema_change(recorder, connection)

            self.assertTrue(
                recorder.publish_strategy_signal_batch(staged_scan, 1)
            )
            self.assertEqual(
                recorder.current_strategy_signal_scan_id(), staged_scan
            )

    def test_passed_signal_audit_and_structure_ledger_commit_together(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            scan_id = self._begin(recorder)
            signal_id = _record_signal(
                recorder,
                scan_id,
                strategy_id="N06",
                structure_id="n06-structure-001",
                passed=True,
            )
            self.assertIsNotNone(signal_id)
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT source_signal_id, strategy_id, symbol, structure_id, "
                        "claim_state "
                        "FROM strategy_passed_structure_ledger"
                    ).fetchall(),
                    [(
                        signal_id,
                        "N06",
                        "BTCUSDT",
                        "n06-structure-001",
                        "STAGED",
                    )],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT source_signal_id, strategy_id, symbol, claim_state "
                        "FROM strategy_passed_signal_audits"
                    ).fetchone(),
                    (signal_id, "N06", "BTCUSDT", "STAGED"),
                )
            self.assertEqual(
                recorder.inspect_passed_structure(
                    "N06", "BTCUSDT", "n06-structure-001"
                ),
                "CONSUMED",
            )
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT claim_state FROM strategy_passed_signal_audits"
                    ).fetchone(),
                    ("ACTIVE",),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT claim_state FROM strategy_passed_structure_ledger"
                    ).fetchone(),
                    ("ACTIVE",),
                )

    def test_ledger_failure_rolls_back_signal_and_permanent_audit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            scan_id = self._begin(recorder)
            with recorder._connect() as connection:
                connection.execute(
                    "CREATE TRIGGER fail_ledger BEFORE INSERT ON "
                    "strategy_passed_structure_ledger BEGIN "
                    "SELECT RAISE(ABORT, 'ledger failed'); END"
                )
                commit_test_schema_change(recorder, connection)
            self.assertIsNone(
                _record_signal(
                    recorder,
                    scan_id,
                    strategy_id="N07",
                    structure_id="n07-structure-001",
                    passed=True,
                )
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM strategy_signals").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits"
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT recorded_count FROM strategy_signal_batches WHERE scan_id = ?",
                        (scan_id,),
                    ).fetchone()[0],
                    0,
                )

    def test_existing_consumed_structure_never_creates_second_passed_signal(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            first_scan = self._begin(recorder)
            first_signal = _record_signal(
                recorder,
                first_scan,
                strategy_id="N08",
                structure_id="n08-structure-001",
                passed=True,
            )
            self.assertIsNotNone(first_signal)
            self.assertTrue(recorder.publish_strategy_signal_batch(first_scan, 1))
            before = None
            with recorder._connect() as connection:
                before = connection.execute(
                    "SELECT * FROM strategy_passed_structure_ledger"
                ).fetchone()

            same_scan = self._begin(recorder)
            self.assertIsNone(
                _record_signal(
                    recorder,
                    same_scan,
                    strategy_id="N08",
                    structure_id="n08-structure-001",
                    passed=True,
                )
            )
            other_scan = self._begin(recorder, "ETHUSDT")
            self.assertIsNone(
                _record_signal(
                    recorder,
                    other_scan,
                    strategy_id="N08",
                    symbol="ETHUSDT",
                    structure_id="n08-structure-001",
                    passed=True,
                )
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE id != ?",
                        (first_signal,),
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_structure_ledger"
                    ).fetchone(),
                    before,
                )

    def test_same_scan_exact_passed_structure_retry_reuses_staged_claim(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            scan_id = self._begin(recorder)
            first_id = _record_signal(
                recorder,
                scan_id,
                strategy_id="N06",
                structure_id="n06-exact-staged-retry",
                passed=True,
            )
            retry_id = _record_signal(
                recorder,
                scan_id,
                strategy_id="N06",
                structure_id="n06-exact-staged-retry",
                passed=True,
            )
            self.assertEqual(retry_id, first_id)
            self.assertIsNone(
                _record_signal(
                    recorder,
                    scan_id,
                    strategy_id="N06",
                    structure_id="n06-exact-staged-retry",
                    passed=True,
                    reason="DIFFERENT_PAYLOAD",
                )
            )
            self.assertIsNone(
                _record_signal(
                    recorder,
                    scan_id,
                    strategy_id="N06",
                    symbol="ETHUSDT",
                    structure_id="n06-exact-staged-retry",
                    passed=True,
                )
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT recorded_count FROM strategy_signal_batches "
                        "WHERE scan_id = ?",
                        (scan_id,),
                    ).fetchone(),
                    (1,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id = ?",
                        (scan_id,),
                    ).fetchone(),
                    (1,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits "
                        "WHERE source_scan_id = ?",
                        (scan_id,),
                    ).fetchone(),
                    (1,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                        "WHERE source_scan_id = ?",
                        (scan_id,),
                    ).fetchone(),
                    (1,),
                )
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            next_scan = self._begin(recorder, "ETHUSDT")
            self.assertIsNone(
                _record_signal(
                    recorder,
                    next_scan,
                    strategy_id="N06",
                    symbol="BTCUSDT",
                    structure_id="n06-exact-staged-retry",
                    passed=True,
                )
            )

    def test_signal_ids_are_not_reused_after_current_rotation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            first_scan = self._begin(recorder)
            first_id = _record_signal(recorder, first_scan)
            self.assertTrue(recorder.publish_strategy_signal_batch(first_scan, 1))
            second_scan = self._begin(recorder, "ETHUSDT")
            second_id = _record_signal(recorder, second_scan, symbol="ETHUSDT")
            self.assertTrue(recorder.publish_strategy_signal_batch(second_scan, 1))
            self.assertGreater(second_id, first_id)
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT seq FROM sqlite_sequence WHERE name='strategy_signals'"
                    ).fetchone()[0],
                    second_id,
                )

    def test_scanless_signal_is_rejected_without_writing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            self.assertIsNone(_record_signal(recorder, None, passed=True))
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM strategy_signals").fetchone()[0],
                    0,
                )

    def test_invalid_detail_never_poison_staging_or_its_cleanup(self):
        nested = {"leaf": "ok"}
        for _ in range(34):
            nested = {"nested": nested}

        class TextSubclass(str):
            pass

        invalid_details = {
            "oversize": {"payload": "x" * 65_537},
            "too_deep": nested,
            "nonfinite": {"value": float("nan")},
            "invalid_nested_type": {"value": object()},
            "non_builtin_key": {TextSubclass("key"): "value"},
        }
        for name, detail in invalid_details.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                recorder = self._recorder(tmpdir)
                scan_id = self._begin(recorder)
                self.assertIsNone(
                    _record_signal(
                        recorder,
                        scan_id,
                        passed=False,
                        detail=detail,
                    )
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT recorded_count FROM strategy_signal_batches "
                            "WHERE scan_id = ?",
                            (scan_id,),
                        ).fetchone(),
                        (0,),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_signals"
                        ).fetchone(),
                        (0,),
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
                replacement_scan = self._begin(recorder, "ETHUSDT")
                self.assertNotEqual(replacement_scan, scan_id)
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT scan_id, state, recorded_count "
                            "FROM strategy_signal_batches"
                        ).fetchall(),
                        [(replacement_scan, "STAGING", 0)],
                    )

    def test_abandoned_batch_releases_staged_passed_claim_without_touching_active(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            active_scan = self._begin(recorder)
            active_signal_id = _record_signal(
                recorder,
                active_scan,
                strategy_id="N06",
                structure_id="n06-active-before-abandon",
                passed=True,
            )
            self.assertIsNotNone(active_signal_id)
            self.assertTrue(
                recorder.publish_strategy_signal_batch(active_scan, 1)
            )
            with recorder._connect() as connection:
                active_ledger = connection.execute(
                    "SELECT * FROM strategy_passed_structure_ledger "
                    "WHERE source_signal_id = ?",
                    (active_signal_id,),
                ).fetchone()
                active_audit = connection.execute(
                    "SELECT * FROM strategy_passed_signal_audits "
                    "WHERE source_signal_id = ?",
                    (active_signal_id,),
                ).fetchone()

            scan_id = self._begin(recorder)
            signal_id = _record_signal(
                recorder,
                scan_id,
                strategy_id="N06",
                structure_id="n06-staged-before-publish",
                passed=True,
            )
            self.assertIsNotNone(signal_id)
            self.assertFalse(recorder.publish_strategy_signal_batch(scan_id, 2))
            self.assertEqual(
                recorder.inspect_passed_structure(
                    "N06", "BTCUSDT", "n06-staged-before-publish"
                ),
                "CONSUMED",
            )

            replacement = self._begin(recorder, "ETHUSDT")
            self.assertIsNotNone(replacement)
            self.assertEqual(
                recorder.inspect_passed_structure(
                    "N06", "BTCUSDT", "n06-staged-before-publish"
                ),
                "MISSING",
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE id = ?",
                        (signal_id,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits "
                        "WHERE source_signal_id = ?",
                        (signal_id,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                        "WHERE source_signal_id = ?",
                        (signal_id,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_structure_ledger "
                        "WHERE source_signal_id = ?",
                        (active_signal_id,),
                    ).fetchone(),
                    active_ledger,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_signal_audits "
                        "WHERE source_signal_id = ?",
                        (active_signal_id,),
                    ).fetchone(),
                    active_audit,
                )

    def test_staged_owner_queries_are_indexed_and_history_bounded(self):
        measurements = []
        for permanent_count in (1_000, 2_000, 4_000):
            with self.subTest(permanent_count=permanent_count), \
                    tempfile.TemporaryDirectory() as tmpdir:
                recorder = self._recorder(tmpdir)
                current_scan = self._begin(recorder)
                self.assertIsNotNone(_record_signal(recorder, current_scan))
                self.assertTrue(
                    recorder.publish_strategy_signal_batch(current_scan, 1)
                )
                now = "2026-08-12T00:00:00+00:00"
                audits = []
                ledgers = []
                for index in range(permanent_count):
                    source_signal_id = 10_000_000 + index
                    source_scan_id = 20_000_000 + index
                    structure_id = f"{index:024x}"
                    symbol = f"H{index:05d}USDT"
                    evidence_sha256 = f"{index + 1:064x}"
                    audits.append((
                        source_signal_id, source_scan_id, "N06", symbol,
                        "", "[]", "", 1, 1, "PASSED", "PASSED",
                        structure_id, "{}", now, evidence_sha256, now,
                        "ACTIVE",
                    ))
                    ledgers.append((
                        "N06", symbol, structure_id, source_signal_id,
                        source_scan_id, now, evidence_sha256, now, "ACTIVE",
                    ))
                with recorder._connect() as connection:
                    connection.executemany(
                        "INSERT INTO strategy_passed_signal_audits("
                        "source_signal_id,source_scan_id,strategy_id,symbol,"
                        "funding_rate,matched_patterns,trend_slope,"
                        "current_bullish,passed,decision,reason,structure_id,"
                        "detail_json,signal_created_at,evidence_sha256,"
                        "created_at,claim_state) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        audits,
                    )
                    connection.executemany(
                        "INSERT INTO strategy_passed_structure_ledger("
                        "strategy_id,symbol,structure_id,source_signal_id,"
                        "source_scan_id,source_signal_created_at,"
                        "evidence_sha256,created_at,claim_state) "
                        "VALUES(?,?,?,?,?,?,?,?,?)",
                        ledgers,
                    )
                with recorder._read_only_runtime_snapshot() as connection:
                    for table, index_name in (
                        (
                            "strategy_passed_signal_audits",
                            "idx_passed_audit_claim_state_scan",
                        ),
                        (
                            "strategy_passed_structure_ledger",
                            "idx_passed_ledger_claim_state_scan",
                        ),
                    ):
                        plan = connection.execute(
                            "EXPLAIN QUERY PLAN SELECT source_scan_id FROM "
                            f"{table} WHERE claim_state='STAGED'"
                        ).fetchall()
                        self.assertTrue(
                            any(index_name in row[3] for row in plan), plan
                        )

                def measured(call):
                    steps = 0

                    def progress():
                        nonlocal steps
                        steps += 1
                        return 0

                    connection.set_progress_handler(progress, 1)
                    try:
                        result = call()
                    finally:
                        connection.set_progress_handler(None, 0)
                    return result, steps

                with recorder.strategy_round_runtime_scope() as connection:
                    next_scan, begin_steps = measured(
                        lambda: recorder.begin_scan(1, [_candidate()], True)
                    )
                    self.assertIsInstance(next_scan, int)
                    self.assertIsNotNone(_record_signal(recorder, next_scan))
                    published, publish_steps = measured(
                        lambda: recorder.publish_strategy_signal_batch(
                            next_scan, 1
                        )
                    )
                    self.assertTrue(published)
                    abandoned_scan = recorder.begin_scan(
                        1, [_candidate("ABANDONEDUSDT")], True
                    )
                    self.assertIsInstance(abandoned_scan, int)
                    self.assertIsNotNone(
                        _record_signal(
                            recorder,
                            abandoned_scan,
                            symbol="ABANDONEDUSDT",
                        )
                    )
                with recorder.strategy_round_runtime_scope() as connection:
                    replacement_scan, cleanup_steps = measured(
                        lambda: recorder.begin_scan(
                            1, [_candidate("REPLACEMENTUSDT")], True
                        )
                    )
                self.assertIsInstance(replacement_scan, int)
                self.assertEqual(
                    recorder.current_strategy_signal_scan_id(), next_scan
                )
                measurements.append(
                    (begin_steps, publish_steps, cleanup_steps)
                )

        for column in range(3):
            values = [row[column] for row in measurements]
            self.assertLessEqual(max(values) - min(values), 500, values)

    def test_publish_claim_activation_failure_rolls_back_then_retry_is_permanent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            scan_id = self._begin(recorder)
            signal_id = _record_signal(
                recorder,
                scan_id,
                strategy_id="N07",
                structure_id="n07-activation-rollback",
                passed=True,
            )
            self.assertIsNotNone(signal_id)
            with recorder._connect() as connection:
                connection.execute(
                    "CREATE TRIGGER fail_ledger_activation BEFORE UPDATE OF claim_state "
                    "ON strategy_passed_structure_ledger "
                    "WHEN NEW.claim_state = 'ACTIVE' BEGIN "
                    "SELECT RAISE(ABORT, 'activation failed'); END"
                )
                commit_test_schema_change(recorder, connection)
            self.assertFalse(recorder.publish_strategy_signal_batch(scan_id, 1))
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT claim_state FROM strategy_passed_signal_audits "
                        "WHERE source_signal_id = ?",
                        (signal_id,),
                    ).fetchone(),
                    ("STAGED",),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT claim_state FROM strategy_passed_structure_ledger "
                        "WHERE source_signal_id = ?",
                        (signal_id,),
                    ).fetchone(),
                    ("STAGED",),
                )
                connection.execute("DROP TRIGGER fail_ledger_activation")
                commit_test_schema_change(recorder, connection)

            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            with recorder._connect() as connection:
                active_ledger = connection.execute(
                    "SELECT * FROM strategy_passed_structure_ledger "
                    "WHERE source_signal_id = ?",
                    (signal_id,),
                ).fetchone()
                self.assertEqual(active_ledger[-1], "ACTIVE")
                self.assertEqual(
                    connection.execute(
                        "SELECT claim_state FROM strategy_passed_signal_audits "
                        "WHERE source_signal_id = ?",
                        (signal_id,),
                    ).fetchone(),
                    ("ACTIVE",),
                )

            self._begin(recorder, "ETHUSDT")
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_structure_ledger "
                        "WHERE source_signal_id = ?",
                        (signal_id,),
                    ).fetchone(),
                    active_ledger,
                )

    def test_later_rejected_write_failure_releases_earlier_staged_n06_claim(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            scan_id = self._begin(recorder)
            passed_signal_id = _record_signal(
                recorder,
                scan_id,
                strategy_id="N06",
                structure_id="n06-before-rejected-write-failure",
                passed=True,
            )
            self.assertIsNotNone(passed_signal_id)
            with recorder._connect() as connection:
                connection.execute(
                    "CREATE TRIGGER fail_later_rejected BEFORE INSERT ON "
                    "strategy_signals WHEN NEW.passed = 0 BEGIN "
                    "SELECT RAISE(ABORT, 'rejected write failed'); END"
                )
                commit_test_schema_change(recorder, connection)
            self.assertIsNone(
                _record_signal(
                    recorder,
                    scan_id,
                    strategy_id="N01",
                    passed=False,
                )
            )
            self.assertFalse(recorder.publish_strategy_signal_batch(scan_id, 2))
            with recorder._connect() as connection:
                connection.execute("DROP TRIGGER fail_later_rejected")
                commit_test_schema_change(recorder, connection)
                self.assertEqual(
                    connection.execute(
                        "SELECT claim_state FROM strategy_passed_signal_audits "
                        "WHERE source_signal_id = ?",
                        (passed_signal_id,),
                    ).fetchone(),
                    ("STAGED",),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT claim_state FROM strategy_passed_structure_ledger "
                        "WHERE source_signal_id = ?",
                        (passed_signal_id,),
                    ).fetchone(),
                    ("STAGED",),
                )

            replacement_scan = self._begin(recorder, "ETHUSDT")
            self.assertIsNotNone(replacement_scan)
            self.assertEqual(
                recorder.inspect_passed_structure(
                    "N06",
                    "BTCUSDT",
                    "n06-before-rejected-write-failure",
                ),
                "MISSING",
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id = ?",
                        (scan_id,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits "
                        "WHERE source_scan_id = ?",
                        (scan_id,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                        "WHERE source_scan_id = ?",
                        (scan_id,),
                    ).fetchone(),
                    (0,),
                )

    def test_tampered_staged_audit_or_ledger_blocks_publish_and_cleanup(self):
        for table in (
            "strategy_passed_signal_audits",
            "strategy_passed_structure_ledger",
        ):
            with self.subTest(table=table), tempfile.TemporaryDirectory() as tmpdir:
                recorder = self._recorder(tmpdir)
                scan_id = self._begin(recorder)
                signal_id = _record_signal(
                    recorder,
                    scan_id,
                    strategy_id="N08",
                    structure_id="n08-tampered-staged-claim",
                    passed=True,
                )
                self.assertIsNotNone(signal_id)
                with recorder._connect() as connection:
                    connection.execute(
                        f"UPDATE {table} SET evidence_sha256 = ? "
                        "WHERE source_signal_id = ?",
                        ("f" * 64, signal_id),
                    )
                    before = connection.execute(
                        f"SELECT * FROM {table} WHERE source_signal_id = ?",
                        (signal_id,),
                    ).fetchone()
                self.assertEqual(
                    recorder.inspect_passed_structure(
                        "N08", "BTCUSDT", "n08-tampered-staged-claim"
                    ),
                    "INCONSISTENT",
                )
                self.assertFalse(
                    recorder.publish_strategy_signal_batch(scan_id, 1)
                )
                self.assertIsNone(
                    recorder.begin_scan(1, [_candidate("ETHUSDT")], dry_run=True)
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            f"SELECT * FROM {table} WHERE source_signal_id = ?",
                            (signal_id,),
                        ).fetchone(),
                        before,
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_signals "
                            "WHERE scan_id = ?",
                            (scan_id,),
                        ).fetchone(),
                        (1,),
                    )

    def test_current_publish_retry_strictly_revalidates_active_claim_identity(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            scan_id = self._begin(recorder)
            signal_id = _record_signal(
                recorder,
                scan_id,
                strategy_id="N11",
                structure_id="n11-active-retry-identity",
                passed=True,
            )
            self.assertIsNotNone(signal_id)
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            with recorder._connect() as connection:
                connection.execute(
                    "UPDATE strategy_passed_signal_audits "
                    "SET evidence_sha256 = ? WHERE source_signal_id = ?",
                    ("e" * 64, signal_id),
                )
                before = connection.execute(
                    "SELECT * FROM strategy_passed_signal_audits "
                    "WHERE source_signal_id = ?",
                    (signal_id,),
                ).fetchone()
            self.assertEqual(
                recorder.inspect_passed_structure(
                    "N11", "BTCUSDT", "n11-active-retry-identity"
                ),
                "INCONSISTENT",
            )
            self.assertFalse(recorder.publish_strategy_signal_batch(scan_id, 1))
            with self.assertRaises(RuntimeError):
                recorder.current_strategy_signal_scan_id()
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_signal_audits "
                        "WHERE source_signal_id = ?",
                        (signal_id,),
                    ).fetchone(),
                    before,
                )

    def test_orphaned_staged_claim_blocks_new_scan_without_deleting_evidence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            scan_id = self._begin(recorder)
            signal_id = _record_signal(
                recorder,
                scan_id,
                strategy_id="N12",
                structure_id="n12-orphaned-staged-claim",
                passed=True,
            )
            self.assertIsNotNone(signal_id)
            with recorder._connect() as connection:
                connection.execute(
                    "DELETE FROM strategy_signal_batches WHERE scan_id = ?",
                    (scan_id,),
                )
                audit = connection.execute(
                    "SELECT * FROM strategy_passed_signal_audits "
                    "WHERE source_signal_id = ?",
                    (signal_id,),
                ).fetchone()
                ledger = connection.execute(
                    "SELECT * FROM strategy_passed_structure_ledger "
                    "WHERE source_signal_id = ?",
                    (signal_id,),
                ).fetchone()
            self.assertIsNone(
                recorder.begin_scan(1, [_candidate("ETHUSDT")], dry_run=True)
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_signal_audits "
                        "WHERE source_signal_id = ?",
                        (signal_id,),
                    ).fetchone(),
                    audit,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_passed_structure_ledger "
                        "WHERE source_signal_id = ?",
                        (signal_id,),
                    ).fetchone(),
                    ledger,
                )

    def test_rejected_signal_without_batch_owner_blocks_runtime_and_reopen(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            current_scan = self._begin(recorder)
            self.assertIsNotNone(_record_signal(recorder, current_scan))
            self.assertTrue(
                recorder.publish_strategy_signal_batch(current_scan, 1)
            )
            orphan_scan = self._begin(recorder, "ETHUSDT")
            orphan_signal = _record_signal(
                recorder, orphan_scan, symbol="ETHUSDT", passed=False
            )
            self.assertIsNotNone(orphan_signal)
            with recorder._connect() as connection:
                removed = connection.execute(
                    "DELETE FROM strategy_signal_batches WHERE scan_id = ?",
                    (orphan_scan,),
                )
                self.assertEqual(removed.rowcount, 1)
                before_signals = connection.execute(
                    "SELECT * FROM strategy_signals ORDER BY id"
                ).fetchall()
                before_batches = connection.execute(
                    "SELECT * FROM strategy_signal_batches ORDER BY scan_id"
                ).fetchall()
                before_pointer = connection.execute(
                    "SELECT * FROM strategy_signal_current"
                ).fetchall()
                self.assertEqual(
                    connection.execute("PRAGMA foreign_key_check").fetchall(),
                    [],
                )
                self.assertEqual(
                    connection.execute("PRAGMA integrity_check").fetchone(),
                    ("ok",),
                )

            with self.assertRaisesRegex(RuntimeError, "batch owner"):
                recorder.current_strategy_signal_scan_id()
            with self.assertRaisesRegex(RuntimeError, "batch owner"):
                recorder.list_current_strategy_signals()
            self.assertIsNone(
                recorder.begin_scan(1, [_candidate("SOLUSDT")], dry_run=True)
            )
            self.assertFalse(
                recorder.publish_strategy_signal_batch(orphan_scan, 1)
            )
            with self.assertRaisesRegex(RuntimeError, "batch owner"):
                self._recorder(tmpdir)
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_signals ORDER BY id"
                    ).fetchall(),
                    before_signals,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_signal_batches ORDER BY scan_id"
                    ).fetchall(),
                    before_batches,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM strategy_signal_current"
                    ).fetchall(),
                    before_pointer,
                )

    def test_publish_rejects_null_or_unowned_signal_scan_without_mutation(self):
        for mode in ("null", "unowned"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder = self._recorder(tmpdir)
                current_scan = self._begin(recorder)
                self.assertIsNotNone(_record_signal(recorder, current_scan))
                self.assertTrue(
                    recorder.publish_strategy_signal_batch(current_scan, 1)
                )
                staging_scan = self._begin(recorder, "ETHUSDT")
                signal_id = _record_signal(
                    recorder, staging_scan, symbol="ETHUSDT", passed=False
                )
                self.assertIsNotNone(signal_id)
                with recorder._connect() as connection:
                    replacement_scan = None
                    if mode == "unowned":
                        cursor = connection.execute(
                            "INSERT INTO scans (started_at, mode, scanned_count, "
                            "candidate_count, candidates_json) "
                            "VALUES ('2026-07-14T00:00:00+00:00', "
                            "'DRY_RUN', 0, 0, '[]')"
                        )
                        replacement_scan = cursor.lastrowid
                    connection.execute(
                        "UPDATE strategy_signals SET scan_id = ? WHERE id = ?",
                        (replacement_scan, signal_id),
                    )
                    before = {
                        table: connection.execute(
                            'SELECT * FROM "%s" ORDER BY rowid' % table
                        ).fetchall()
                        for table in (
                            "strategy_signals",
                            "strategy_signal_batches",
                            "strategy_signal_current",
                            "strategy_passed_signal_audits",
                            "strategy_passed_structure_ledger",
                        )
                    }
                self.assertFalse(
                    recorder.publish_strategy_signal_batch(staging_scan, 1)
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        {
                            table: connection.execute(
                                'SELECT * FROM "%s" ORDER BY rowid' % table
                            ).fetchall()
                            for table in before
                        },
                        before,
                    )


if __name__ == "__main__":
    unittest.main()
