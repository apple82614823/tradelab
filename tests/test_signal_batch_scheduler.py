from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests.recorder_test_utils import (
    commit_test_schema_change,
    make_test_recorder,
)
from trading_bot.main import TradingBot
from trading_bot.exchange_symbol import authenticated_symbol_set_sha256
from trading_bot.monitor import FundingCandidate, FundingMonitor, StrategyMarketScan
from trading_bot.n16_claim_ledger import N16ClaimLedgerError
from trading_bot.recorder import (
    ReviewRecorder,
    StrategySignalBatchWriteResult,
)
from trading_bot.signal_retention import (
    _complete_state_summary,
    _validate_keep_attestation,
)
from trading_bot.state import PositionState, StateStore
from trading_bot.micro_analyzer import MicroAnalysisResult
from trading_bot.strategies import (
    N06_STRATEGY,
    load_all_strategies,
    load_first_stage_strategies,
)
from trading_bot.strategy_scheduler import StrategyScheduler
from trading_bot.trader import SyncResult, TradePlan
from tests.swing_fixtures import build_valid_swing_klines
from tests.test_multistrategy import c_pattern_klines


LOGGER = logging.getLogger("test_signal_batch_scheduler")


def _candidate(funding_rate: str = "-0.021") -> FundingCandidate:
    return FundingCandidate(
        "BOUNCEUSDT",
        Decimal(funding_rate),
        Decimal("193"),
    )


def _n06_candidate() -> FundingCandidate:
    return FundingCandidate(
        "N06USDT",
        None,
        Decimal("123"),
        quote_volume=Decimal("1000000"),
        quote_volume_rank=1,
        candidate_universe="quote_volume_top",
    )


def _trade_plan(symbol: str) -> TradePlan:
    return TradePlan(
        symbol=symbol,
        leverage=10,
        quantity=Decimal("10"),
        entry_price=Decimal("100"),
        stop_loss_price=Decimal("99"),
        take_profit_price=Decimal("105"),
        stop_loss_pct=Decimal("0.01"),
        take_profit_pct=Decimal("0.05"),
        amplitude_24h_pct=Decimal("0.1"),
        high_24h_price=Decimal("105"),
        low_24h_price=Decimal("95"),
        risk_amount=Decimal("10"),
        notional_value=Decimal("1000"),
        required_margin=Decimal("100"),
        balance=Decimal("1000"),
    )


class SignalBatchSchedulerTests(unittest.TestCase):
    def _first_stage(self, tmpdir: str):
        recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), LOGGER)
        strategies = load_first_stage_strategies()
        recorder.upsert_strategy_definitions(strategies)
        scheduler = StrategyScheduler(strategies, 96, recorder, LOGGER)
        return recorder, scheduler

    def _evaluate_first_stage(
        self,
        recorder: ReviewRecorder,
        scheduler: StrategyScheduler,
        funding_rate: str = "-0.021",
    ):
        candidate = _candidate(funding_rate)
        scan_id = recorder.begin_scan(1, [candidate], dry_run=True)
        self.assertIsNotNone(scan_id)
        result = scheduler.evaluate(
            scan_id,
            [candidate],
            {candidate.symbol: c_pattern_klines()},
        )
        return scan_id, result

    def _seed_current(self, recorder: ReviewRecorder, scheduler: StrategyScheduler):
        scan_id, result = self._evaluate_first_stage(recorder, scheduler)
        self.assertTrue(result.signal_batch_published)
        self.assertEqual(len(result.signals), 5)
        self.assertEqual(len(result.passed_signals), 5)
        self.assertEqual(recorder.current_strategy_signal_scan_id(), scan_id)
        return scan_id, tuple(recorder.list_current_strategy_signals())

    def test_successful_evaluate_publishes_only_after_every_raw_decision_is_durable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._first_stage(tmpdir)
            call_order = []
            original_record = recorder.record_strategy_signals
            original_publish = recorder.publish_strategy_signal_batch

            def recording_signals(scan_id, records):
                result = original_record(scan_id, records)
                self.assertTrue(result.complete)
                call_order.append(
                    ("record_batch", scan_id, len(result.signal_ids))
                )
                return result

            def publishing_batch(
                scan_id, expected_count, history_coverage_proposals=()
            ):
                call_order.append(("publish", scan_id, expected_count))
                return original_publish(
                    scan_id, expected_count, history_coverage_proposals
                )

            with patch.object(
                recorder,
                "record_strategy_signals",
                side_effect=recording_signals,
            ), patch.object(
                recorder,
                "publish_strategy_signal_batch",
                side_effect=publishing_batch,
            ):
                scan_id, result = self._evaluate_first_stage(recorder, scheduler)

            self.assertTrue(result.signal_batch_published)
            self.assertEqual(
                [item[0] for item in call_order],
                ["record_batch", "publish"],
            )
            self.assertEqual(call_order[0], ("record_batch", scan_id, 5))
            self.assertEqual(call_order[-1], ("publish", scan_id, 5))
            self.assertEqual(recorder.current_strategy_signal_scan_id(), scan_id)
            current = recorder.list_current_strategy_signals()
            self.assertEqual(len(current), 5)
            self.assertEqual(
                {row[1] for row in current},
                {"N01", "N02", "N03", "N04", "N05"},
            )

    def test_empty_active_candidate_set_publishes_exact_zero_row_current(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._first_stage(tmpdir)
            first_scan = recorder.begin_scan(861, [], dry_run=True)
            self.assertIsNotNone(first_scan)

            first = scheduler.evaluate(first_scan, [], {})

            self.assertTrue(first.signal_batch_published)
            self.assertEqual(first.signals, [])
            self.assertEqual(first.passed_signals, [])
            self.assertEqual(first.live_candidates, [])
            self.assertEqual(recorder.current_strategy_signal_scan_id(), first_scan)
            self.assertEqual(recorder.list_current_strategy_signals(), [])
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state,recorded_count,expected_count,"
                        "first_signal_id,last_signal_id,manifest_sha256 "
                        "FROM strategy_signal_batches WHERE scan_id=?",
                        (first_scan,),
                    ).fetchone(),
                    ("CURRENT", 0, None, None, None, "0" * 64),
                )
                retention_summary = _complete_state_summary(
                    connection, first_scan
                )
                self.assertEqual(retention_summary.retained_count, 0)
                self.assertEqual(
                    retention_summary.retained_manifest_sha256, "0" * 64
                )
                self.assertTrue(
                    _validate_keep_attestation(
                        retention_summary, 0, "0" * 64
                    )
                )

            second_scan = recorder.begin_scan(862, [], dry_run=True)
            self.assertIsNotNone(second_scan)
            second = scheduler.evaluate(second_scan, [], {})

            self.assertTrue(second.signal_batch_published)
            self.assertEqual(recorder.current_strategy_signal_scan_id(), second_scan)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT scan_id,state,recorded_count,expected_count "
                        "FROM strategy_signal_batches"
                    ).fetchall(),
                    [(second_scan, "CURRENT", 0, None)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals"
                    ).fetchone(),
                    (0,),
                )

    def test_main_empty_funding_round_publishes_zero_without_execution(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._first_stage(tmpdir)
            calls = []

            class Client:
                def get_klines(self, _symbol):
                    raise AssertionError("empty active round must not fetch Klines")

            class Monitor(FundingMonitor):
                def __init__(self):
                    pass

                def scan_for_strategies(self, _volume_top_n):
                    registry = frozenset({"BTCUSDT"})
                    return StrategyMarketScan(
                        861,
                        [],
                        [],
                        authenticated_symbols=registry,
                        authenticated_symbols_sha256=(
                            authenticated_symbol_set_sha256(registry)
                        ),
                    )

            class Trader:
                def close_dry_run_position_if_triggered(self):
                    return None

                def sync_state_with_exchange(self):
                    calls.append("sync")
                    return SyncResult(has_position=False)

                def build_trade_plan(self, _symbol, _mark_price):
                    raise AssertionError("empty active round has no plan")

                def open_long_plan_with_protection(self, _plan):
                    raise AssertionError("empty active round has no live open")

            class CloseOnlyPaper:
                def close_triggered_open_trades(self, *_args):
                    calls.append("close_sweep")
                    return []

                def open_trade(self, *_args, **_kwargs):
                    raise AssertionError("new paper opens are disabled")

            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=True)
            bot.logger = LOGGER
            bot.client = Client()
            bot.recorder = recorder
            bot.paper_trader = CloseOnlyPaper()
            bot.monitor = Monitor()
            bot.trader = Trader()
            bot.strategy_scheduler = scheduler
            bot.strategies = load_first_stage_strategies()
            bot.state = StateStore(str(Path(tmpdir) / "state.json"))

            bot._run_once_multi_strategy()

            self.assertEqual(calls, ["close_sweep", "sync"])
            self.assertEqual(recorder.list_current_strategy_signals(), [])
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state,recorded_count,expected_count "
                        "FROM strategy_signal_batches"
                    ).fetchall(),
                    [("CURRENT", 0, None)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_paper_trades"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_live_links"
                    ).fetchone(),
                    (0,),
                )

    def test_scheduler_reads_global_cooldowns_once_per_round(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._first_stage(tmpdir)
            with patch.object(
                recorder,
                "active_symbol_cooldowns",
                wraps=recorder.active_symbol_cooldowns,
            ) as bulk_read, patch.object(
                recorder,
                "active_symbol_cooldown",
                wraps=recorder.active_symbol_cooldown,
            ) as single_read, patch.object(
                recorder,
                "active_strategy_symbol_cooldowns",
                wraps=recorder.active_strategy_symbol_cooldowns,
            ) as strategy_bulk_read, patch.object(
                recorder,
                "active_strategy_symbol_cooldown",
                wraps=recorder.active_strategy_symbol_cooldown,
            ) as strategy_single_read:
                _scan_id, result = self._evaluate_first_stage(
                    recorder, scheduler
                )

            self.assertTrue(result.signal_batch_published)
            self.assertEqual(len(result.signals), 5)
            self.assertEqual(bulk_read.call_count, 1)
            self.assertEqual(single_read.call_count, 0)
            self.assertEqual(strategy_bulk_read.call_count, 1)
            self.assertEqual(strategy_single_read.call_count, 0)

    def test_global_cooldown_chunks_share_one_sqlite_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _scheduler = self._first_stage(tmpdir)
            symbols = tuple(f"C{index:04d}USDT" for index in range(501))
            now = datetime.now(timezone.utc)
            until = (now + timedelta(hours=4)).isoformat()
            created_at = now.isoformat()
            with recorder._connect() as connection:
                connection.executemany(
                    """
                    INSERT INTO symbol_cooldowns(
                        symbol,cooldown_until,reason,source_trade_id,
                        created_at,updated_at
                    ) VALUES(?,?,?,NULL,?,?)
                    """,
                    (
                        (
                            symbol,
                            until,
                            "SNAPSHOT_TEST",
                            created_at,
                            created_at,
                        )
                        for symbol in symbols[:500]
                    ),
                )

            statements = []
            snapshot_transactions = []
            select_count = 0
            connection_count = 0
            original_snapshot = recorder._read_only_runtime_snapshot

            @contextmanager
            def traced_snapshot():
                nonlocal connection_count, select_count
                connection_count += 1
                with original_snapshot() as connection:
                    snapshot_transactions.append(connection.in_transaction)
                    def trace(statement):
                        nonlocal select_count
                        statements.append(statement)
                        if "FROM symbol_cooldowns" not in statement:
                            return
                        select_count += 1
                        if select_count != 2:
                            return
                        with closing(
                            sqlite3.connect(recorder.db_file)
                        ) as external:
                            with external:
                                external.execute(
                                    """
                                    INSERT INTO symbol_cooldowns(
                                        symbol,cooldown_until,reason,
                                        source_trade_id,created_at,updated_at
                                    ) VALUES(?,?,?,NULL,?,?)
                                    """,
                                    (
                                        symbols[-1],
                                        until,
                                        "LATE_INSERT",
                                        created_at,
                                        created_at,
                                    ),
                                )

                    connection.set_trace_callback(trace)
                    try:
                        yield connection
                    finally:
                        connection.set_trace_callback(None)

            with patch.object(
                recorder, "_read_only_runtime_snapshot", traced_snapshot
            ):
                snapshot = recorder.active_symbol_cooldowns(symbols, now)

            self.assertEqual(connection_count, 1)
            self.assertEqual(snapshot_transactions, [True])
            self.assertEqual(select_count, 2)
            self.assertEqual(len(snapshot), 500)
            self.assertNotIn(symbols[-1], snapshot)
            self.assertIsNotNone(
                recorder.active_symbol_cooldown(symbols[-1], now)
            )

    def test_global_cooldown_snapshot_read_failure_stops_before_signal_writes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._first_stage(tmpdir)
            with patch.object(
                recorder,
                "active_symbol_cooldowns",
                side_effect=RuntimeError("cooldown snapshot unavailable"),
            ), patch.object(
                recorder,
                "record_strategy_signals",
                wraps=recorder.record_strategy_signals,
            ) as batch_write:
                with self.assertRaisesRegex(
                    RuntimeError,
                    "cooldown snapshot unavailable",
                ):
                    self._evaluate_first_stage(recorder, scheduler)
            self.assertEqual(batch_write.call_count, 0)
            self.assertIsNone(recorder.current_strategy_signal_scan_id())
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_paper_trades"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_live_links"
                    ).fetchone(),
                    (0,),
                )

    def test_round_scope_keeps_query_only_snapshot_separate_from_writes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _scheduler = self._first_stage(tmpdir)
            read_connection = None
            with recorder.strategy_round_runtime_scope() as write_connection:
                ledger_descriptor = getattr(
                    recorder.n16_claim_ledger._runtime_attestation_scope,
                    "descriptor",
                    None,
                )
                self.assertIs(type(ledger_descriptor), int)
                expected_ledger_identity = (
                    recorder.n16_claim_ledger._file_scope.main_identity
                )
                ledger_details = os.fstat(ledger_descriptor)
                self.assertEqual(
                    (ledger_details.st_dev, ledger_details.st_ino),
                    expected_ledger_identity,
                )
                self.assertIsNone(getattr(
                    recorder.n16_claim_ledger._runtime_attestation_scope,
                    "connection",
                    None,
                ))
                with recorder.n16_claim_ledger.runtime_attestation_transaction():
                    ledger_connection = getattr(
                        recorder.n16_claim_ledger._runtime_attestation_scope,
                        "connection",
                        None,
                    )
                    self.assertIsNotNone(ledger_connection)
                    self.assertEqual(
                        ledger_connection.execute(
                            "PRAGMA query_only"
                        ).fetchone(),
                        (1,),
                    )
                    self.assertTrue(ledger_connection.in_transaction)
                    with self.assertRaises(sqlite3.OperationalError):
                        ledger_connection.execute(
                            "UPDATE n16_claim_ledger_installation "
                            "SET installed_at=installed_at"
                        )
                injected = []
                with self.assertRaisesRegex(
                    N16ClaimLedgerError,
                    "exact SQLite connection",
                ):
                    recorder.n16_claim_ledger.commit_attested_review(
                        lambda: injected.append(True), recorder
                    )
                self.assertEqual(injected, [])
                write_connection.execute("BEGIN")
                write_connection.execute(
                    "INSERT INTO events(occurred_at,event_type,payload_json) "
                    "VALUES('2026-01-01T00:00:00+00:00','GUARDED','{}')"
                )
                recorder.n16_claim_ledger.commit_attested_review(
                    write_connection,
                    recorder,
                )
                self.assertFalse(write_connection.in_transaction)
                self.assertEqual(
                    write_connection.execute(
                        "SELECT COUNT(*) FROM events "
                        "WHERE event_type='GUARDED'"
                    ).fetchone(),
                    (1,),
                )
                with recorder._read_only_runtime_snapshot() as snapshot:
                    read_connection = snapshot
                    self.assertIsNot(snapshot, write_connection)
                    self.assertEqual(
                        snapshot.execute("PRAGMA query_only").fetchone(),
                        (1,),
                    )
                    self.assertTrue(snapshot.in_transaction)
                    original_count = snapshot.execute(
                        "SELECT COUNT(*) FROM events"
                    ).fetchone()[0]
                    with self.assertRaises(sqlite3.OperationalError):
                        snapshot.execute(
                            "INSERT INTO events(occurred_at,event_type,payload_json) "
                            "VALUES('2026-01-01T00:00:00+00:00','FORBIDDEN','{}')"
                        )

                with closing(sqlite3.connect(recorder.db_file)) as external:
                    with external:
                        external.execute(
                            "INSERT INTO events(occurred_at,event_type,payload_json) "
                            "VALUES('2026-01-01T00:00:01+00:00','LATE','{}')"
                        )

                with recorder._read_only_runtime_snapshot() as same_snapshot:
                    self.assertIs(same_snapshot, read_connection)
                    self.assertEqual(
                        same_snapshot.execute(
                            "SELECT COUNT(*) FROM events"
                        ).fetchone(),
                        (original_count,),
                    )

                with recorder._connect():
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "snapshot boundary is invalid",
                    ):
                        with recorder._read_only_runtime_snapshot():
                            pass
                self.assertIsNone(getattr(
                    recorder.n16_claim_ledger._runtime_attestation_scope,
                    "connection",
                    None,
                ))
                self.assertEqual(
                    (os.fstat(ledger_descriptor).st_dev,
                     os.fstat(ledger_descriptor).st_ino),
                    expected_ledger_identity,
                )

            self.assertIsNone(
                getattr(recorder._runtime_round_scope, "connection", None)
            )
            self.assertIsNone(
                getattr(recorder._runtime_round_scope, "read_connection", None)
            )
            with self.assertRaises(sqlite3.ProgrammingError):
                read_connection.execute("SELECT 1")
            with self.assertRaises(OSError):
                os.fstat(ledger_descriptor)
            with recorder._read_only_runtime_snapshot() as snapshot:
                self.assertEqual(
                    snapshot.execute("SELECT COUNT(*) FROM events").fetchone(),
                    (original_count + 2,),
                )

    def test_round_scope_exception_rolls_back_and_discards_both_connections(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _scheduler = self._first_stage(tmpdir)
            with self.assertRaisesRegex(RuntimeError, "injected round failure"):
                with recorder.strategy_round_runtime_scope():
                    with recorder._connect() as connection:
                        connection.execute(
                            "INSERT INTO events(occurred_at,event_type,payload_json) "
                            "VALUES('2026-01-01T00:00:00+00:00','ROLLBACK','{}')"
                        )
                        raise RuntimeError("injected round failure")
            self.assertIsNone(
                getattr(recorder._runtime_round_scope, "connection", None)
            )
            self.assertIsNone(
                getattr(recorder._runtime_round_scope, "read_connection", None)
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM events WHERE event_type='ROLLBACK'"
                    ).fetchone(),
                    (0,),
                )

    def test_round_scope_is_signal_and_manifest_equivalent_to_per_operation_connections(self):
        with tempfile.TemporaryDirectory() as root:
            (Path(root) / "scoped").mkdir()
            (Path(root) / "legacy").mkdir()
            scoped, scoped_scheduler = self._first_stage(
                str(Path(root) / "scoped")
            )
            legacy, legacy_scheduler = self._first_stage(
                str(Path(root) / "legacy")
            )

            @contextmanager
            def per_operation_scope():
                yield None

            frozen_now = "2026-08-12T00:00:00+00:00"
            with patch(
                "trading_bot.recorder.utc_now",
                return_value=frozen_now,
            ):
                scoped_scan, scoped_result = self._evaluate_first_stage(
                    scoped, scoped_scheduler
                )
                with patch.object(
                    legacy,
                    "strategy_round_runtime_scope",
                    per_operation_scope,
                ):
                    legacy_scan, legacy_result = self._evaluate_first_stage(
                        legacy, legacy_scheduler
                    )
            self.assertEqual(scoped_scan, legacy_scan)
            self.assertEqual(scoped_result.signals, legacy_result.signals)
            self.assertEqual(
                scoped_result.passed_signals,
                legacy_result.passed_signals,
            )
            self.assertEqual(
                scoped.list_current_strategy_signals(),
                legacy.list_current_strategy_signals(),
            )
            manifests = []
            for recorder in (scoped, legacy):
                with recorder._read_only_runtime_snapshot() as connection:
                    manifests.append(connection.execute(
                        "SELECT expected_count,recorded_count,first_signal_id,"
                        "last_signal_id,manifest_sha256 FROM "
                        "strategy_signal_batches WHERE state='CURRENT'"
                    ).fetchone())
            self.assertEqual(manifests[0], manifests[1])

    def test_round_scope_rechecks_parent_identity_before_each_write_transaction(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            data_parent = root_path / "data"
            data_parent.mkdir()
            recorder, _scheduler = self._first_stage(str(data_parent))
            held_parent = root_path / "held-data"
            external_parent = root_path / "external-data"
            marker = b"external-directory-must-not-change"

            with recorder.strategy_round_runtime_scope():
                data_parent.rename(held_parent)
                data_parent.mkdir()
                (data_parent / "marker.bin").write_bytes(marker)
                try:
                    self.assertFalse(
                        recorder.record_event(
                            "must_not_persist",
                            {"scope": "replaced"},
                        )
                    )
                    self.assertEqual(
                        (data_parent / "marker.bin").read_bytes(), marker
                    )
                    self.assertEqual(
                        tuple(data_parent.iterdir()),
                        (data_parent / "marker.bin",),
                    )
                finally:
                    data_parent.rename(external_parent)
                    held_parent.rename(data_parent)

            self.assertEqual(
                (external_parent / "marker.bin").read_bytes(), marker
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM events "
                        "WHERE event_type='must_not_persist'"
                    ).fetchone(),
                    (0,),
                )

    def test_round_scope_rechecks_independent_ledger_before_each_write(self):
        for mode in (
            "parent_replace_restore",
            "inode_replace_restore",
            "schema_drift",
            "highwater_drift",
            "phase_drift",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                root_path = Path(root)
                review_parent = root_path / "review"
                ledger_parent = root_path / "ledger"
                review_parent.mkdir()
                ledger_parent.mkdir()
                database = review_parent / "review.sqlite3"
                ledger = ledger_parent / "claims.sqlite3"
                recorder = make_test_recorder(
                    database,
                    LOGGER,
                    n16_claim_ledger_file=ledger,
                )
                recorder.upsert_strategy_definitions(
                    load_first_stage_strategies()
                )
                external_marker = None
                with recorder.strategy_round_runtime_scope():
                    self.assertTrue(
                        recorder.record_event("ledger_boundary_before", {})
                    )
                    if mode == "parent_replace_restore":
                        held = root_path / "held-ledger"
                        external = root_path / "external-ledger"
                        ledger_parent.rename(held)
                        ledger_parent.mkdir()
                        external_marker = ledger_parent / "marker.bin"
                        external_marker.write_bytes(b"external-ledger-parent")
                        self.assertFalse(
                            recorder.record_event("ledger_boundary_after", {})
                        )
                        ledger_parent.rename(external)
                        held.rename(ledger_parent)
                        external_marker = external / "marker.bin"
                    elif mode == "inode_replace_restore":
                        held = ledger.with_name("held-claims.sqlite3")
                        external = ledger.with_name("external-claims.sqlite3")
                        ledger.rename(held)
                        ledger.write_bytes(b"external-ledger-inode")
                        self.assertFalse(
                            recorder.record_event("ledger_boundary_after", {})
                        )
                        ledger.rename(external)
                        held.rename(ledger)
                        external_marker = external
                    elif mode == "schema_drift":
                        with closing(sqlite3.connect(ledger)) as external:
                            external.execute(
                                "CREATE TABLE foreign_ledger_table(id INTEGER)"
                            )
                            external.commit()
                        self.assertFalse(
                            recorder.record_event("ledger_boundary_after", {})
                        )
                    elif mode == "highwater_drift":
                        generation, schema, catalog = (
                            recorder.n16_claim_ledger
                            .protected_generation_highwater()
                        )
                        with closing(sqlite3.connect(ledger)) as external:
                            external.execute(
                                "UPDATE n16_protected_generation_highwater "
                                "SET generation=?,review_schema_version=?,"
                                "family_catalog_sha256=?,updated_at=? "
                                "WHERE singleton_id=1 AND generation=?",
                                (
                                    generation + 1,
                                    schema,
                                    catalog,
                                    "2026-08-12T00:00:00+00:00",
                                    generation,
                                ),
                            )
                            external.commit()
                        self.assertFalse(
                            recorder.record_event("ledger_boundary_after", {})
                        )
                    else:
                        digest = "1" * 64
                        with closing(sqlite3.connect(ledger)) as external:
                            external.execute(
                                "UPDATE n16_claim_ledger_meta SET "
                                "phase='PREPARED',pending_txn_id=?,"
                                "pending_scan_id=1,pending_batch_sha256=?,"
                                "pending_claim_count=1,"
                                "pending_target_chain_sha256=?,"
                                "pending_base_current_scan_id=NULL,"
                                "pending_base_current_manifest_sha256=?,"
                                "pending_retention_sha256=?,"
                                "pending_staging_sha256=?,"
                                "pending_passed_claims_sha256=?,updated_at=? "
                                "WHERE singleton_id=1 AND phase='READY'",
                                (
                                    digest, digest, digest, digest, digest,
                                    digest, digest,
                                    "2026-08-12T00:00:00+00:00",
                                ),
                            )
                            external.commit()
                        self.assertFalse(
                            recorder.record_event("ledger_boundary_after", {})
                        )
                if external_marker is not None:
                    self.assertIn(
                        external_marker.read_bytes(),
                        {b"external-ledger-parent", b"external-ledger-inode"},
                    )
                with closing(sqlite3.connect(database)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT event_type FROM events WHERE event_type "
                            "LIKE 'ledger_boundary_%' ORDER BY id"
                        ).fetchall(),
                        [("ledger_boundary_before",)],
                    )
                self.assertIsNone(
                    getattr(recorder._runtime_round_scope, "connection", None)
                )
                self.assertIsNone(
                    getattr(
                        recorder.n16_claim_ledger._runtime_attestation_scope,
                        "connection",
                        None,
                    )
                )

    def test_round_scope_ledger_drift_before_review_commit_rolls_back(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            database = root_path / "review.sqlite3"
            ledger = root_path / "claims.sqlite3"
            recorder = make_test_recorder(
                database,
                LOGGER,
                n16_claim_ledger_file=ledger,
            )
            recorder.upsert_strategy_definitions(
                load_first_stage_strategies()
            )
            original_transaction = (
                recorder.n16_claim_ledger.runtime_attestation_transaction
            )
            transaction_count = 0

            @contextmanager
            def drift_after_first_snapshot():
                nonlocal transaction_count
                with original_transaction():
                    yield
                transaction_count += 1
                if transaction_count != 1:
                    return
                digest = "1" * 64
                with closing(sqlite3.connect(ledger)) as external:
                    external.execute(
                        "UPDATE n16_claim_ledger_meta SET "
                        "phase='PREPARED',pending_txn_id=?,"
                        "pending_scan_id=1,pending_batch_sha256=?,"
                        "pending_claim_count=1,"
                        "pending_target_chain_sha256=?,"
                        "pending_base_current_scan_id=NULL,"
                        "pending_base_current_manifest_sha256=?,"
                        "pending_retention_sha256=?,"
                        "pending_staging_sha256=?,"
                        "pending_passed_claims_sha256=?,updated_at=? "
                        "WHERE singleton_id=1 AND phase='READY'",
                        (
                            digest,
                            digest,
                            digest,
                            digest,
                            digest,
                            digest,
                            digest,
                            "2026-08-12T00:00:00+00:00",
                        ),
                    )
                    external.commit()

            with recorder.strategy_round_runtime_scope(), patch.object(
                recorder.n16_claim_ledger,
                "runtime_attestation_transaction",
                drift_after_first_snapshot,
            ):
                self.assertFalse(
                    recorder.record_event(
                        "ledger_commit_boundary",
                        {"must": "roll back"},
                    )
                )

            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM events "
                        "WHERE event_type='ledger_commit_boundary'"
                    ).fetchone(),
                    (0,),
                )
            self.assertIsNone(
                getattr(recorder._runtime_round_scope, "connection", None)
            )
            self.assertIsNone(
                getattr(
                    recorder.n16_claim_ledger._runtime_attestation_scope,
                    "connection",
                    None,
                )
            )

    def test_per_operation_ledger_drift_before_review_commit_rolls_back(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            database = root_path / "review.sqlite3"
            ledger = root_path / "claims.sqlite3"
            recorder = make_test_recorder(
                database,
                LOGGER,
                n16_claim_ledger_file=ledger,
            )
            recorder.upsert_strategy_definitions(
                load_first_stage_strategies()
            )
            original_attest = (
                recorder._attest_n16_claim_ledger_after_catalog_attestation
            )
            attest_count = 0

            def attest_then_drift(connection):
                nonlocal attest_count
                result = original_attest(connection)
                attest_count += 1
                if attest_count != 1:
                    return result
                generation, schema, catalog = (
                    recorder.n16_claim_ledger
                    .protected_generation_highwater()
                )
                with closing(sqlite3.connect(ledger)) as external:
                    external.execute(
                        "UPDATE n16_protected_generation_highwater "
                        "SET generation=?,review_schema_version=?,"
                        "family_catalog_sha256=?,updated_at=? "
                        "WHERE singleton_id=1 AND generation=?",
                        (
                            generation + 1,
                            schema,
                            catalog,
                            "2026-08-12T00:00:00+00:00",
                            generation,
                        ),
                    )
                    external.commit()
                return result

            with patch.object(
                recorder,
                "_attest_n16_claim_ledger_after_catalog_attestation",
                attest_then_drift,
            ):
                self.assertFalse(
                    recorder.record_event(
                        "per_operation_commit_boundary",
                        {"must": "roll back"},
                    )
                )

            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM events "
                        "WHERE event_type='per_operation_commit_boundary'"
                    ).fetchone(),
                    (0,),
                )

    def test_round_scope_commit_preflight_rejects_ledger_content_drift_matrix(self):
        modes = (
            "installing_phase",
            "highwater",
            "claim",
            "witness",
            "catalog",
        )
        for mode in modes:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                root_path = Path(root)
                database = root_path / "review.sqlite3"
                ledger = root_path / "claims.sqlite3"
                recorder = make_test_recorder(
                    database,
                    LOGGER,
                    n16_claim_ledger_file=ledger,
                )
                recorder.upsert_strategy_definitions(
                    load_first_stage_strategies()
                )
                original_transaction = (
                    recorder.n16_claim_ledger
                    .runtime_attestation_transaction
                )
                transaction_count = 0

                def mutate_ledger():
                    digest = "1" * 64
                    now = "2026-08-12T00:00:00+00:00"
                    with closing(sqlite3.connect(ledger)) as external:
                        if mode == "installing_phase":
                            external.execute(
                                "PRAGMA ignore_check_constraints=ON"
                            )
                            external.execute(
                                "UPDATE n16_claim_ledger_meta "
                                "SET phase='INSTALLING',updated_at=? "
                                "WHERE singleton_id=1",
                                (now,),
                            )
                        elif mode == "highwater":
                            generation, schema, catalog = (
                                recorder.n16_claim_ledger
                                .protected_generation_highwater()
                            )
                            external.execute(
                                "UPDATE n16_protected_generation_highwater "
                                "SET generation=?,review_schema_version=?,"
                                "family_catalog_sha256=?,updated_at=? "
                                "WHERE singleton_id=1 AND generation=?",
                                (
                                    generation + 1,
                                    schema,
                                    catalog,
                                    now,
                                    generation,
                                ),
                            )
                        elif mode == "claim":
                            external.execute(
                                "INSERT INTO n16_permanent_claims VALUES "
                                "(1,?,'COMMITTED',1,1,'N16_V1','N16',"
                                "1,1,1,1,'DRIFTUSDT',?,?,?,?,?,?,?,?)",
                                (
                                    digest,
                                    "2" * 24,
                                    "3" * 24,
                                    "4" * 64,
                                    "5" * 64,
                                    now,
                                    now,
                                    "6" * 64,
                                    "7" * 64,
                                ),
                            )
                        elif mode == "witness":
                            ledger_uuid = external.execute(
                                "SELECT ledger_uuid "
                                "FROM n16_claim_ledger_meta "
                                "WHERE singleton_id=1"
                            ).fetchone()[0]
                            external.execute(
                                "UPDATE n16_legacy_witness_mirror SET "
                                "phase='PREPARED',review_plan_sha256=?,"
                                "witness_sha256=?,"
                                "pre_review_canonical_sha256=?,"
                                "authorization_sha256=?,ledger_uuid=?,"
                                "base_review_snapshot_sha256=?,"
                                "target_review_snapshot_sha256=?,"
                                "base_catalog_sha256=?,target_catalog_sha256=?,"
                                "prepared_at=?,committed_at=NULL "
                                "WHERE singleton_id=1 AND phase='EMPTY'",
                                (
                                    digest,
                                    "2" * 64,
                                    "3" * 64,
                                    "2dd9bf0e9a19d4464ff53ddb9bba35a8"
                                    "50306499d12d88a34257541dcd0b04a1",
                                    ledger_uuid,
                                    "4" * 64,
                                    "5" * 64,
                                    "6" * 64,
                                    "7" * 64,
                                    now,
                                ),
                            )
                        else:
                            external.execute(
                                "CREATE TABLE foreign_commit_boundary(id INTEGER)"
                            )
                        external.commit()

                @contextmanager
                def drift_after_first_snapshot():
                    nonlocal transaction_count
                    with original_transaction():
                        yield
                    transaction_count += 1
                    if transaction_count == 1:
                        mutate_ledger()

                with recorder.strategy_round_runtime_scope(), patch.object(
                    recorder.n16_claim_ledger,
                    "runtime_attestation_transaction",
                    drift_after_first_snapshot,
                ):
                    self.assertFalse(
                        recorder.record_event(
                            "ledger_commit_content_drift",
                            {"mode": mode},
                        )
                    )
                with closing(sqlite3.connect(database)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM events "
                            "WHERE event_type='ledger_commit_content_drift'"
                        ).fetchone(),
                        (0,),
                    )

    def test_ledger_path_aba_after_fresh_attestation_rolls_back_review(self):
        modes = (
            "main_absent",
            "main_external",
            "parent_external",
            "wal",
            "shm",
            "journal",
        )
        for scoped in (False, True):
            for mode in modes:
                with self.subTest(scoped=scoped, mode=mode), tempfile.TemporaryDirectory() as root:
                    root_path = Path(root)
                    review_parent = root_path / "review"
                    ledger_parent = root_path / "ledger"
                    review_parent.mkdir()
                    ledger_parent.mkdir()
                    database = review_parent / "review.sqlite3"
                    ledger = ledger_parent / "claims.sqlite3"
                    recorder = make_test_recorder(
                        database,
                        LOGGER,
                        n16_claim_ledger_file=ledger,
                    )
                    recorder.upsert_strategy_definitions(
                        load_first_stage_strategies()
                    )
                    original_attest = (
                        recorder._attest_n16_ledger_before_review_commit
                    )
                    external_path = root_path / ("external-" + mode)
                    external_payload = b"external-ledger-aba-marker"

                    def attest_then_aba(connection):
                        original_attest(connection)
                        if mode.startswith("main"):
                            held = root_path / "held-ledger-main"
                            ledger.rename(held)
                            if mode == "main_external":
                                ledger.write_bytes(external_payload)
                                ledger.rename(external_path)
                            held.rename(ledger)
                        elif mode == "parent_external":
                            held = root_path / "held-ledger-parent"
                            ledger_parent.rename(held)
                            ledger_parent.mkdir()
                            marker = ledger_parent / "marker.bin"
                            marker.write_bytes(external_payload)
                            ledger_parent.rename(external_path)
                            held.rename(ledger_parent)
                        else:
                            sidecar = Path(str(ledger) + "-" + mode)
                            if sidecar.exists():
                                held = root_path / ("held-ledger-" + mode)
                                sidecar.rename(held)
                                sidecar.write_bytes(external_payload)
                                sidecar.rename(external_path)
                                held.rename(sidecar)
                            else:
                                sidecar.write_bytes(external_payload)
                                sidecar.rename(external_path)

                    with patch.object(
                        recorder,
                        "_attest_n16_ledger_before_review_commit",
                        attest_then_aba,
                    ):
                        if scoped:
                            with recorder.strategy_round_runtime_scope():
                                result = recorder.record_event(
                                    "ledger_path_aba",
                                    {"mode": mode},
                                )
                        else:
                            result = recorder.record_event(
                                "ledger_path_aba",
                                {"mode": mode},
                            )
                    self.assertFalse(result)
                    with closing(sqlite3.connect(database)) as connection:
                        self.assertEqual(
                            connection.execute(
                                "SELECT COUNT(*) FROM events "
                                "WHERE event_type='ledger_path_aba'"
                            ).fetchone(),
                            (0,),
                        )
                    if external_path.exists():
                        if external_path.is_dir():
                            self.assertEqual(
                                (external_path / "marker.bin").read_bytes(),
                                external_payload,
                            )
                        else:
                            self.assertEqual(
                                external_path.read_bytes(),
                                external_payload,
                            )
                    self.assertIsNone(
                        getattr(recorder._runtime_round_scope, "connection", None)
                    )
                    self.assertIsNone(
                        getattr(
                            recorder.n16_claim_ledger._runtime_attestation_scope,
                            "connection",
                            None,
                        )
                    )
                    self.assertIsNone(
                        getattr(
                            recorder.n16_claim_ledger._runtime_attestation_scope,
                            "descriptor",
                            None,
                        )
                    )

    def test_round_scope_rejects_parent_or_sidecar_aba_before_commit(self):
        for mode in ("parent_absent", "parent_with_database", "wal_sidecar"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                root_path = Path(root)
                data_parent = root_path / "data"
                data_parent.mkdir()
                recorder, _scheduler = self._first_stage(str(data_parent))
                external_parent = root_path / "external-data"
                external_payloads = {}

                with recorder.strategy_round_runtime_scope():
                    self.assertTrue(recorder.record_event("aba_before", {}))

                    def transient_aba():
                        if mode.startswith("parent"):
                            held_parent = root_path / "held-data"
                            replacement = data_parent
                            data_parent.rename(held_parent)
                            replacement.mkdir()
                            marker = replacement / "marker.bin"
                            marker.write_bytes(b"external-parent-marker")
                            if mode == "parent_with_database":
                                external_db = replacement / "review.sqlite3"
                                with closing(sqlite3.connect(external_db)) as connection:
                                    connection.execute(
                                        "CREATE TABLE external_only(value TEXT)"
                                    )
                                    connection.commit()
                            replacement.rename(external_parent)
                            held_parent.rename(data_parent)
                            for path in external_parent.iterdir():
                                external_payloads[path.name] = path.read_bytes()
                        else:
                            wal = Path(str(recorder.db_file) + "-wal")
                            held_wal = root_path / "held-review-wal"
                            external_wal = root_path / "external-review-wal"
                            wal.rename(held_wal)
                            wal.write_bytes(b"external-wal")
                            wal.rename(external_wal)
                            held_wal.rename(wal)
                            external_payloads[external_wal.name] = (
                                external_wal.read_bytes()
                            )
                        return "2026-08-12T00:00:00+00:00"

                    with patch(
                        "trading_bot.recorder.utc_now",
                        side_effect=transient_aba,
                    ):
                        self.assertFalse(recorder.record_event("aba_after", {}))

                if mode.startswith("parent"):
                    self.assertTrue(external_parent.is_dir())
                    self.assertEqual(
                        {
                            path.name: path.read_bytes()
                            for path in external_parent.iterdir()
                        },
                        external_payloads,
                    )
                else:
                    external_wal = root_path / "external-review-wal"
                    self.assertEqual(
                        external_wal.read_bytes(),
                        external_payloads[external_wal.name],
                    )
                with closing(sqlite3.connect(recorder.db_file)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT event_type FROM events WHERE event_type "
                            "LIKE 'aba_%' ORDER BY id"
                        ).fetchall(),
                        [("aba_before",)],
                    )

    def test_round_signal_batch_uses_one_authenticated_connection_for_2000_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _scheduler = self._first_stage(tmpdir)
            scan_id = recorder.begin_scan(100, [], dry_run=True)
            records = tuple(
                {
                    "strategy_id": "N01",
                    "symbol": f"Q{index:04d}USDT",
                    "funding_rate": "",
                    "matched_patterns": (),
                    "trend_slope": "",
                    "current_bullish": False,
                    "passed": False,
                    "decision": "REJECTED",
                    "reason": "PERFORMANCE_GATE",
                    "structure_id": None,
                    "detail": {"index": index},
                }
                for index in range(2000)
            )
            with patch.object(
                recorder,
                "_open_identity_attested_runtime_connection",
                wraps=recorder._open_identity_attested_runtime_connection,
            ) as opened, patch.object(
                recorder,
                "_attest_legacy_witness_pair",
                wraps=recorder._attest_legacy_witness_pair,
            ) as witness_pair, patch.object(
                recorder,
                "_attest_n16_claim_ledger_after_catalog_attestation",
                wraps=(
                    recorder
                    ._attest_n16_claim_ledger_after_catalog_attestation
                ),
            ) as paired_ledger, patch.object(
                recorder.n16_claim_ledger,
                "attest",
                wraps=recorder.n16_claim_ledger.attest,
            ) as ledger_attest:
                result = recorder.record_strategy_signals(
                    scan_id, records
                )
            self.assertTrue(result.complete)
            self.assertEqual(len(result.signal_ids), 2000)
            self.assertEqual(opened.call_count, 1)
            self.assertEqual(witness_pair.call_count, 2)
            self.assertEqual(paired_ledger.call_count, 1)
            self.assertEqual(ledger_attest.call_count, 2)

    def test_round_signal_batch_first_middle_last_failure_rolls_back_all_rows(self):
        for failure_index in (0, 1, 2):
            with self.subTest(failure_index=failure_index), tempfile.TemporaryDirectory() as tmpdir:
                recorder, _scheduler = self._first_stage(tmpdir)
                scan_id = recorder.begin_scan(100, [], dry_run=True)
                records = (
                    {
                        "strategy_id": "N06",
                        "symbol": "FIRSTUSDT",
                        "funding_rate": "",
                        "matched_patterns": (),
                        "trend_slope": "",
                        "current_bullish": True,
                        "passed": True,
                        "decision": "PASSED",
                        "reason": "PASSED",
                        "structure_id": "1" * 24,
                        "detail": {"position": "first"},
                    },
                    {
                        "strategy_id": "N01",
                        "symbol": "MIDDLEUSDT",
                        "funding_rate": "",
                        "matched_patterns": (),
                        "trend_slope": "",
                        "current_bullish": False,
                        "passed": False,
                        "decision": "REJECTED",
                        "reason": "NO_MATCH",
                        "structure_id": None,
                        "detail": {"position": "middle"},
                    },
                    {
                        "strategy_id": "N07",
                        "symbol": "LASTUSDT",
                        "funding_rate": "",
                        "matched_patterns": (),
                        "trend_slope": "",
                        "current_bullish": True,
                        "passed": True,
                        "decision": "PASSED",
                        "reason": "PASSED",
                        "structure_id": "2" * 24,
                        "detail": {"position": "last"},
                    },
                )
                with recorder._connect() as connection:
                    connection.execute(
                        "CREATE TRIGGER fail_selected_signal "
                        "BEFORE INSERT ON strategy_signals "
                        f"WHEN NEW.symbol='{records[failure_index]['symbol']}' "
                        "BEGIN SELECT RAISE(ABORT, 'selected signal failed'); END"
                    )
                    commit_test_schema_change(recorder, connection)
                result = recorder.record_strategy_signals(
                    scan_id, records
                )
                self.assertFalse(result.complete)
                self.assertEqual(result.failed_index, failure_index)
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT recorded_count,first_signal_id,"
                            "last_signal_id FROM strategy_signal_batches "
                            "WHERE scan_id=?",
                            (scan_id,),
                        ).fetchone(),
                        (0, None, None),
                    )
                    for table in (
                        "strategy_signals",
                        "strategy_passed_signal_audits",
                        "strategy_passed_structure_ledger",
                        "micro_strategy_lifecycle",
                    ):
                        self.assertEqual(
                            connection.execute(
                                f"SELECT COUNT(*) FROM {table} "
                                "WHERE source_scan_id=?"
                                if table != "strategy_signals"
                                else
                                "SELECT COUNT(*) FROM strategy_signals "
                                "WHERE scan_id=?",
                                (scan_id,),
                            ).fetchone(),
                            (0,),
                        )
                    connection.execute(
                        "DROP TRIGGER fail_selected_signal"
                    )
                    commit_test_schema_change(recorder, connection)

    def test_round_signal_batch_catalog_change_is_rejected_before_any_row(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _scheduler = self._first_stage(tmpdir)
            scan_id = recorder.begin_scan(100, [], dry_run=True)
            with closing(
                sqlite3.connect(recorder.db_file)
            ) as external:
                with external:
                    external.execute(
                        "CREATE TABLE hostile_batch_catalog(value INTEGER)"
                    )
            result = recorder.record_strategy_signals(
                scan_id,
                ({
                    "strategy_id": "N01",
                    "symbol": "HOSTILEUSDT",
                    "funding_rate": "",
                    "matched_patterns": (),
                    "trend_slope": "",
                    "current_bullish": False,
                    "passed": False,
                    "decision": "REJECTED",
                    "reason": "NO_MATCH",
                    "structure_id": None,
                    "detail": {},
                },),
            )
            self.assertFalse(result.complete)
            with closing(sqlite3.connect(recorder.db_file)) as check:
                self.assertEqual(
                    check.execute(
                        "SELECT recorded_count FROM "
                        "strategy_signal_batches WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    (0,),
                )

    def test_catalog_change_after_batch_before_publish_preserves_old_current(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._first_stage(tmpdir)
            old_scan, _old_current = self._seed_current(
                recorder, scheduler
            )
            original_batch = recorder.record_strategy_signals

            def batch_then_change_catalog(scan_id, records):
                result = original_batch(scan_id, records)
                self.assertTrue(result.complete)
                with closing(
                    sqlite3.connect(recorder.db_file)
                ) as external:
                    with external:
                        external.execute(
                            "CREATE TABLE "
                            "hostile_between_batch_and_publish("
                            "value INTEGER)"
                        )
                return result

            with patch.object(
                recorder,
                "record_strategy_signals",
                side_effect=batch_then_change_catalog,
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "protected Review catalog changed",
                ):
                    new_scan, _result = self._evaluate_first_stage(
                        recorder,
                        scheduler,
                        funding_rate="-0.016",
                    )
            with closing(sqlite3.connect(recorder.db_file)) as check:
                new_scan = check.execute(
                    "SELECT max(id) FROM scans"
                ).fetchone()[0]
                self.assertEqual(
                    check.execute(
                        "SELECT current_scan_id FROM "
                        "strategy_signal_current WHERE singleton_id=1"
                    ).fetchone(),
                    (old_scan,),
                )
                self.assertEqual(
                    check.execute(
                        "SELECT state,recorded_count,expected_count "
                        "FROM strategy_signal_batches WHERE scan_id=?",
                        (new_scan,),
                    ).fetchone(),
                    ("STAGING", 5, None),
                )

    def test_rejected_signal_write_failure_preserves_old_current_and_blocks_all_passed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._first_stage(tmpdir)
            old_scan, old_current = self._seed_current(recorder, scheduler)
            with patch.object(
                recorder,
                "record_strategy_signals",
                return_value=StrategySignalBatchWriteResult(
                    failed_index=2
                ),
            ):
                staging_scan, result = self._evaluate_first_stage(
                    recorder,
                    scheduler,
                    funding_rate="-0.016",
                )

            self.assertFalse(result.signal_batch_published)
            self.assertEqual(
                [failure.log_payload() for failure in result.signal_audit_failures],
                [
                    {
                        "code": "SIGNAL_RECORD_PERSIST_FAILED",
                        "strategy_id": "N03",
                        "symbol": "BOUNCEUSDT",
                    }
                ],
            )
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])
            self.assertEqual(recorder.current_strategy_signal_scan_id(), old_scan)
            self.assertEqual(tuple(recorder.list_current_strategy_signals()), old_current)
            self.assertTrue(
                all(
                    not signal.passed
                    for signal in result.signals
                )
            )
            originally_passed = [
                signal
                for signal in result.signals
                if signal.strategy.strategy_id in {"N01", "N02", "N05"}
            ]
            self.assertEqual(
                {signal.reason for signal in originally_passed},
                {"SIGNAL_AUDIT_PERSIST_FAILED"},
            )
            with recorder._connect() as connection:
                staging = connection.execute(
                    "SELECT state, recorded_count FROM strategy_signal_batches "
                    "WHERE scan_id = ?",
                    (staging_scan,),
                ).fetchone()
                self.assertEqual(staging, ("STAGING", 0))

    def test_passed_signal_write_failure_preserves_old_current_and_blocks_entire_round(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._first_stage(tmpdir)
            old_scan, old_current = self._seed_current(recorder, scheduler)
            with patch.object(
                recorder,
                "record_strategy_signals",
                return_value=StrategySignalBatchWriteResult(
                    failed_index=0
                ),
            ):
                _, result = self._evaluate_first_stage(
                    recorder,
                    scheduler,
                    funding_rate="-0.016",
                )

            self.assertFalse(result.signal_batch_published)
            self.assertEqual(
                result.signal_audit_failures[0].code,
                "SIGNAL_RECORD_PERSIST_FAILED",
            )
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])
            self.assertEqual(recorder.current_strategy_signal_scan_id(), old_scan)
            self.assertEqual(tuple(recorder.list_current_strategy_signals()), old_current)
            reasons = {
                signal.strategy.strategy_id: signal.reason
                for signal in result.signals
            }
            self.assertEqual(reasons["N01"], "SIGNAL_AUDIT_PERSIST_FAILED")
            self.assertEqual(reasons["N02"], "SIGNAL_AUDIT_PERSIST_FAILED")
            self.assertEqual(reasons["N05"], "SIGNAL_AUDIT_PERSIST_FAILED")

    def test_sqlite_publish_failure_rolls_back_switch_and_blocks_execution_candidates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._first_stage(tmpdir)
            old_scan, old_current = self._seed_current(recorder, scheduler)
            def fail_inside_publish(
                connection, scan_id, expected_count, manifest, proposals
            ):
                connection.execute(
                    "INSERT INTO deliberately_missing_publish_table VALUES (1)"
                )

            with patch.object(
                recorder,
                "_apply_history_coverage_proposals",
                side_effect=fail_inside_publish,
            ):
                staging_scan, result = self._evaluate_first_stage(
                    recorder,
                    scheduler,
                    funding_rate="-0.016",
                )

            self.assertFalse(result.signal_batch_published)
            self.assertEqual(
                [failure.code for failure in result.signal_audit_failures],
                ["SIGNAL_BATCH_PUBLISH_REJECTED"],
            )
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])
            self.assertEqual(recorder.current_strategy_signal_scan_id(), old_scan)
            self.assertEqual(tuple(recorder.list_current_strategy_signals()), old_current)
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state, recorded_count, expected_count "
                        "FROM strategy_signal_batches WHERE scan_id = ?",
                        (staging_scan,),
                    ).fetchone(),
                    ("STAGING", 5, None),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id = ?",
                        (staging_scan,),
                    ).fetchone()[0],
                    5,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits "
                        "WHERE source_scan_id = ? AND claim_state = 'STAGED'",
                        (staging_scan,),
                    ).fetchone(),
                    (3,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits "
                        "WHERE source_scan_id = ? AND claim_state = 'ACTIVE'",
                        (staging_scan,),
                    ).fetchone(),
                    (0,),
                )

    def test_passed_permanent_audit_or_ledger_failure_never_reaches_execution(self):
        for table, trigger_name in (
            ("strategy_passed_signal_audits", "fail_passed_audit"),
            ("strategy_passed_structure_ledger", "fail_structure_ledger"),
        ):
            with self.subTest(table=table), tempfile.TemporaryDirectory() as tmpdir:
                recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), LOGGER)
                recorder.upsert_strategy_definitions((N06_STRATEGY,))
                with recorder._connect() as connection:
                    connection.execute(
                        "UPDATE strategy_states SET consecutive_wins=2, "
                        "paper_trade_count=2, win_count=2, win_rate='1', "
                        "live_eligible=1 WHERE strategy_id='N06'"
                    )
                    connection.execute(
                        f"CREATE TRIGGER {trigger_name} BEFORE INSERT ON {table} "
                        "BEGIN SELECT RAISE(ABORT, 'durable audit failed'); END"
                    )
                    commit_test_schema_change(recorder, connection)
                scheduler = StrategyScheduler((N06_STRATEGY,), 96, recorder, LOGGER)
                candidate = _n06_candidate()
                scan_id = recorder.begin_scan(1, [candidate], dry_run=True)

                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": [candidate]},
                    {candidate.symbol: build_valid_swing_klines()},
                )

                self.assertFalse(result.signal_batch_published)
                self.assertEqual(len(result.signals), 1)
                self.assertFalse(result.signals[0].passed)
                self.assertEqual(result.signals[0].reason, "SIGNAL_AUDIT_PERSIST_FAILED")
                self.assertEqual(result.passed_signals, [])
                self.assertEqual(result.live_candidates, [])
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
                            "SELECT COUNT(*) FROM strategy_passed_structure_ledger"
                        ).fetchone()[0],
                        0,
                    )

    def test_micro_audit_failure_reasons_are_exact_bounded_and_interleaved(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"), LOGGER
            )
            strategy = next(
                item for item in load_all_strategies()
                if item.strategy_id == "N21"
            )
            scheduler = StrategyScheduler((strategy,), 96, recorder, LOGGER)
            candidate = FundingCandidate(
                "FLOWUSDT",
                None,
                Decimal("100"),
                quote_volume=Decimal("1000000"),
                quote_volume_rank=1,
                candidate_universe="quote_volume_top",
            )
            cold = MicroAnalysisResult(
                "N21",
                candidate.symbol,
                False,
                "N21_MICRO_OBSERVATION_COLD_START",
                None,
                None,
                {},
            )

            first_scan = recorder.begin_scan(1, [candidate], dry_run=True)
            with patch(
                "trading_bot.strategy_scheduler.analyze_micro_strategy",
                return_value=cold,
            ):
                first = scheduler.evaluate(
                    first_scan,
                    {"quote_volume_top": [candidate]},
                    {candidate.symbol: []},
                    micro_context_complete=True,
                )
            self.assertTrue(first.signal_batch_published)
            self.assertEqual(first.signal_audit_failures, ())

            failed_scan = recorder.begin_scan(1, [candidate], dry_run=True)
            with patch(
                "trading_bot.strategy_scheduler.analyze_micro_strategy",
                return_value=cold,
            ), self.assertLogs(LOGGER.name, level="ERROR") as captured:
                failed = scheduler.evaluate(
                    failed_scan,
                    {"quote_volume_top": [candidate]},
                    {candidate.symbol: []},
                    micro_context_complete=False,
                )
            self.assertFalse(failed.signal_batch_published)
            self.assertEqual(
                [failure.log_payload() for failure in failed.signal_audit_failures],
                [{"code": "MICRO_CONTEXT_INCOMPLETE"}],
            )
            self.assertEqual(failed.signal_audit_failure_overflow, 0)
            self.assertIn(f"scan_id={failed_scan}", captured.output[0])
            self.assertIn('"code":"MICRO_CONTEXT_INCOMPLETE"', captured.output[0])
            self.assertLess(len(captured.output[0].encode("utf-8")), 16_384)
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state,recorded_count,expected_count "
                        "FROM strategy_signal_batches WHERE scan_id=?",
                        (failed_scan,),
                    ).fetchone(),
                    ("STAGING", 1, None),
                )

            recovered_scan = recorder.begin_scan(1, [candidate], dry_run=True)
            with patch(
                "trading_bot.strategy_scheduler.analyze_micro_strategy",
                return_value=cold,
            ):
                recovered = scheduler.evaluate(
                    recovered_scan,
                    {"quote_volume_top": [candidate]},
                    {candidate.symbol: []},
                    micro_context_complete=True,
                )
            self.assertTrue(recovered.signal_batch_published)
            self.assertEqual(recovered.signal_audit_failures, ())
            with recorder._connect() as connection:
                self.assertIsNone(
                    connection.execute(
                        "SELECT 1 FROM strategy_signal_batches WHERE scan_id=?",
                        (failed_scan,),
                    ).fetchone()
                )

    def test_micro_analyzer_exception_is_not_silent_and_blocks_publication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"), LOGGER
            )
            strategy = next(
                item for item in load_all_strategies()
                if item.strategy_id == "N21"
            )
            scheduler = StrategyScheduler((strategy,), 96, recorder, LOGGER)
            candidate = FundingCandidate(
                "FLOWUSDT",
                None,
                Decimal("100"),
                quote_volume=Decimal("1000000"),
                quote_volume_rank=1,
                candidate_universe="quote_volume_top",
            )
            scan_id = recorder.begin_scan(1, [candidate], dry_run=True)
            with patch(
                "trading_bot.strategy_scheduler.analyze_micro_strategy",
                side_effect=RuntimeError("sensitive internal detail"),
            ), self.assertLogs(LOGGER.name, level="ERROR") as captured:
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": [candidate]},
                    {candidate.symbol: []},
                    micro_context_complete=True,
                )
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(
                [failure.log_payload() for failure in result.signal_audit_failures],
                [
                    {
                        "code": "MICRO_ANALYZER_EXCEPTION",
                        "strategy_id": "N21",
                        "symbol": "FLOWUSDT",
                    },
                    {
                        "code": "N21_ANALYSIS_INVALID",
                        "strategy_id": "N21",
                        "symbol": "FLOWUSDT",
                    },
                ],
            )
            self.assertNotIn("sensitive internal detail", "\n".join(captured.output))

    def test_signal_audit_failure_collection_and_log_are_strictly_bounded(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"), LOGGER
            )
            strategy = next(
                item for item in load_all_strategies()
                if item.strategy_id == "N21"
            )
            scheduler = StrategyScheduler((strategy,), 96, recorder, LOGGER)
            candidates = [
                FundingCandidate(
                    f"M{index:03d}USDT",
                    None,
                    Decimal("100"),
                    quote_volume=Decimal(1000 - index),
                    quote_volume_rank=index + 1,
                    candidate_universe="quote_volume_top",
                )
                for index in range(100)
            ]
            scan_id = recorder.begin_scan(100, candidates, dry_run=True)

            def fail_analysis(_strategy_id, _symbol, *_args):
                raise RuntimeError("bounded analyzer failure")

            with patch(
                "trading_bot.strategy_scheduler.analyze_micro_strategy",
                side_effect=fail_analysis,
            ), self.assertLogs(LOGGER.name, level="ERROR") as captured:
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": candidates},
                    {candidate.symbol: [] for candidate in candidates},
                    micro_context_complete=True,
                )
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(len(result.signal_audit_failures), 64)
            self.assertEqual(result.signal_audit_failure_overflow, 136)
            audit_logs = [
                line for line in captured.output
                if "Strategy signal audit incomplete" in line
            ]
            self.assertEqual(len(audit_logs), 1)
            self.assertLess(len(audit_logs[0].encode("utf-8")), 16_384)
            self.assertIn("overflow=136", audit_logs[0])

    def test_main_failed_batch_releases_earlier_n06_staged_claim_next_scan(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), LOGGER)
            recorder.upsert_strategy_definitions((N06_STRATEGY,))
            scheduler = StrategyScheduler((N06_STRATEGY,), 96, recorder, LOGGER)

            old_candidate = FundingCandidate(
                "OLDUSDT", None, Decimal("100"), candidate_universe="quote_volume_top"
            )
            old_scan = recorder.begin_scan(1, [old_candidate], dry_run=True)
            self.assertIsNotNone(old_scan)
            self.assertIsNotNone(
                recorder.record_strategy_signal(
                    old_scan,
                    "N01",
                    old_candidate.symbol,
                    "",
                    (),
                    "",
                    False,
                    False,
                    "REJECTED",
                    "OLD_CURRENT",
                    detail={"old": True},
                )
            )
            self.assertTrue(recorder.publish_strategy_signal_batch(old_scan, 1))
            old_current = tuple(recorder.list_current_strategy_signals())

            passed_candidate = _n06_candidate()
            rejected_candidate = FundingCandidate(
                "BADN06USDT",
                None,
                Decimal("100"),
                quote_volume=Decimal("900000"),
                quote_volume_rank=2,
                candidate_universe="quote_volume_top",
            )
            valid_klines = build_valid_swing_klines()
            checked_at_seconds = (int(valid_klines[-1][0]) + 30_000) / 1000
            execution_calls = []

            class Client:
                def get_klines(self, symbol):
                    return valid_klines if symbol == passed_candidate.symbol else []

                def get_klines_for_interval(self, *args, **kwargs):
                    return []

                def get_aggregate_trades(self, *args, **kwargs):
                    return []

            class Monitor:
                def scan_for_strategies(self, volume_top_n):
                    return StrategyMarketScan(
                        2,
                        [],
                        [passed_candidate, rejected_candidate],
                    )

            class Trader:
                def close_dry_run_position_if_triggered(self):
                    return None

                def sync_state_with_exchange(self):
                    return SyncResult(has_position=False)

                def build_trade_plan(self, *args, **kwargs):
                    execution_calls.append("plan")
                    raise AssertionError("an unpublished N06 batch must not build a plan")

                def open_long_plan_with_protection(self, *args, **kwargs):
                    execution_calls.append("live")
                    raise AssertionError("an unpublished N06 batch must not trade")

            class PaperTrader:
                def close_triggered_open_trades(self, *args):
                    return []

                def open_trade(self, *args, **kwargs):
                    execution_calls.append("paper")
                    raise AssertionError("an unpublished N06 batch must not open paper")

            with recorder._connect() as connection:
                connection.execute(
                    "CREATE TRIGGER fail_later_rejected_signal "
                    "BEFORE INSERT ON strategy_signals WHEN NEW.passed = 0 "
                    "BEGIN SELECT RAISE(ABORT, 'later rejected failed'); END"
                )
                commit_test_schema_change(recorder, connection)

            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=True)
            bot.logger = LOGGER
            bot.client = Client()
            bot.recorder = recorder
            bot.paper_trader = PaperTrader()
            bot.monitor = Monitor()
            bot.trader = Trader()
            bot.strategy_scheduler = scheduler
            bot.strategies = (N06_STRATEGY,)
            bot.state = StateStore(str(Path(tmpdir) / "state.json"))

            with patch("trading_bot.main.time.time", return_value=checked_at_seconds):
                bot._run_once_multi_strategy()

            self.assertEqual(execution_calls, [])
            self.assertEqual(recorder.current_strategy_signal_scan_id(), old_scan)
            self.assertEqual(tuple(recorder.list_current_strategy_signals()), old_current)
            with recorder._connect() as connection:
                failed_scan = connection.execute(
                    "SELECT max(id) FROM scans"
                ).fetchone()[0]
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id=?",
                        (failed_scan,),
                    ).fetchone(),
                    (0,),
                )
                self.assertIsNone(
                    connection.execute(
                        "SELECT state FROM strategy_signal_batches "
                        "WHERE scan_id=?",
                        (failed_scan,),
                    ).fetchone()
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                        "WHERE source_scan_id=? AND claim_state='STAGED'",
                        (failed_scan,),
                    ).fetchone(),
                    (0,),
                )
                connection.execute("DROP TRIGGER fail_later_rejected_signal")
                commit_test_schema_change(recorder, connection)

            replacement_scan = recorder.begin_scan(
                1, [rejected_candidate], dry_run=True
            )
            self.assertIsNotNone(replacement_scan)
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id=?",
                        (failed_scan,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits "
                        "WHERE source_scan_id=?",
                        (failed_scan,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                        "WHERE source_scan_id=?",
                        (failed_scan,),
                    ).fetchone(),
                    (0,),
                )

    def test_main_opens_direct_live_only_after_current_batch_publish(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = self._first_stage(tmpdir)
            events = []
            original_publish = recorder.publish_strategy_signal_batch

            def publish(scan_id, expected_count, history_coverage_proposals=()):
                published = original_publish(
                    scan_id, expected_count, history_coverage_proposals
                )
                if published:
                    events.append(("publish", scan_id))
                return published

            class Client:
                def get_klines(self, symbol):
                    return c_pattern_klines()

                def get_klines_for_interval(self, *args, **kwargs):
                    return []

                def get_aggregate_trades(self, *args, **kwargs):
                    return []

            class Monitor:
                def scan_for_strategies(self, volume_top_n):
                    candidate = _candidate()
                    return StrategyMarketScan(1, [candidate], [])

            class Trader:
                def close_dry_run_position_if_triggered(self):
                    return None

                def sync_state_with_exchange(self):
                    return SyncResult(has_position=False)

                def build_trade_plan(self, symbol, mark_price):
                    return _trade_plan(symbol)

                def open_long_plan_with_protection(self, plan):
                    events.append(("live", recorder.current_strategy_signal_scan_id()))
                    return PositionState(
                        symbol=plan.symbol,
                        quantity=str(plan.quantity),
                        entry_price=str(plan.entry_price),
                        stop_loss_price=str(plan.stop_loss_price),
                        take_profit_price=str(plan.take_profit_price),
                        leverage=plan.leverage,
                        opened_at="2026-07-14T00:00:00+00:00",
                        dry_run=True,
                        orders={"plan": {}},
                    )

            class PaperTrader:
                def close_triggered_open_trades(self, *args):
                    return []

                def open_trade(self, strategy_id, symbol, funding_rate, plan, detail):
                    events.append(("paper", recorder.current_strategy_signal_scan_id()))
                    return len(events)

            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=True)
            bot.logger = LOGGER
            bot.client = Client()
            bot.recorder = recorder
            bot.paper_trader = PaperTrader()
            bot.monitor = Monitor()
            bot.trader = Trader()
            bot.strategy_scheduler = scheduler
            bot.strategies = load_first_stage_strategies()
            bot.state = StateStore(str(Path(tmpdir) / "state.json"))

            with patch.object(
                recorder,
                "publish_strategy_signal_batch",
                side_effect=publish,
            ), patch.object(
                recorder,
                "get_strategy_state",
                side_effect=AssertionError(
                    "direct live routing must not read paper qualification"
                ),
            ):
                bot._run_once_multi_strategy()

            self.assertTrue(events)
            self.assertEqual(events[0][0], "publish")
            published_scan = events[0][1]
            self.assertEqual(recorder.current_strategy_signal_scan_id(), published_scan)
            self.assertEqual([event[0] for event in events].count("live"), 1)
            self.assertEqual([event[0] for event in events].count("paper"), 0)
            self.assertTrue(
                all(scan_id == published_scan for _, scan_id in events[1:])
            )


if __name__ == "__main__":
    unittest.main()
