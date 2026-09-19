from __future__ import annotations

from contextlib import contextmanager
from decimal import Decimal
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests.recorder_test_utils import (
    commit_test_schema_change,
    make_test_recorder,
)
from trading_bot.analyzer import AnalysisResult
from trading_bot.main import TradingBot
from trading_bot.monitor import FundingCandidate, StrategyMarketScan
from trading_bot.recorder import ReviewRecorder
from trading_bot.signal_retention import (
    _install_n16_claim_boundary,
    inspect_signal_retention,
)
from trading_bot.state import PositionState, StateStore
from trading_bot.strategies import load_first_stage_strategies
from trading_bot.strategy_scheduler import SchedulerResult, StrategySignalDecision
from trading_bot.trader import SyncResult


LOGGER = logging.getLogger("test_signal_retention_guards")


def _candidate(symbol: str = "GUARDUSDT") -> FundingCandidate:
    return FundingCandidate(
        symbol=symbol,
        funding_rate=Decimal("-0.02"),
        mark_price=Decimal("100"),
    )


def _record_strategy_signal(
    recorder: ReviewRecorder,
    scan_id: int,
    *,
    strategy_id: str = "N01",
    symbol: str = "GUARDUSDT",
    passed: bool = False,
    structure_id: str | None = None,
) -> int | None:
    return recorder.record_strategy_signal(
        scan_id=scan_id,
        strategy_id=strategy_id,
        symbol=symbol,
        funding_rate="-0.02",
        matched_patterns=("GUARD",),
        trend_slope="1",
        current_bullish=True,
        passed=passed,
        decision="PASSED" if passed else "REJECTED",
        reason="PASSED" if passed else "GUARD_REJECTED",
        structure_id=structure_id,
        detail={"guard": strategy_id},
    )


class _SingleStrategyTrader:
    def __init__(self, call_order=None, *, succeed: bool = False) -> None:
        self.open_calls = 0
        self.call_order = call_order
        self.succeed = succeed

    def close_dry_run_position_if_triggered(self):
        return None

    def sync_state_with_exchange(self):
        return SyncResult(has_position=False)

    def open_long_with_protection(self, symbol, mark_price):
        self.open_calls += 1
        if self.call_order is not None:
            self.call_order.append(("open", symbol))
        if self.succeed:
            return PositionState(
                symbol=symbol,
                quantity="1",
                entry_price=str(mark_price),
                stop_loss_price="99",
                take_profit_price="105",
                leverage=10,
                opened_at="2026-07-14T00:00:00+00:00",
                dry_run=True,
                orders={"plan": {}},
            )
        raise AssertionError("an unaudited single-strategy signal must not place an order")


class _SingleStrategyClient:
    def __init__(self) -> None:
        self.kline_calls = 0

    def get_klines(self, symbol):
        self.kline_calls += 1
        return []


class _SingleStrategyMonitor:
    def __init__(self, candidates) -> None:
        self.candidates = (
            [candidates]
            if isinstance(candidates, FundingCandidate)
            else list(candidates)
        )

    def scan(self):
        return len(self.candidates), list(self.candidates)


class _SingleStrategyRecorder:
    def __init__(
        self,
        *,
        scan_id: int | None,
        signal_id: int | None,
        signal_ids=None,
        publish_result: bool = False,
        publish_error: Exception | None = None,
        current_scan_id="AUTO",
        cooldowns=None,
    ) -> None:
        self.scan_id = scan_id
        self.signal_id = signal_id
        self.signal_ids = list(signal_ids) if signal_ids is not None else None
        self.publish_result = publish_result
        self.publish_error = publish_error
        self.forced_current_scan_id = current_scan_id
        self.published_scan_id = None
        self.cooldowns = dict(cooldowns or {})
        self.record_calls = 0
        self.record_decisions = []
        self.publish_calls = []
        self.call_order = []
        self.completed = []
        self.events = []
        self.trade_opens = []

    def begin_scan(self, scanned_count, candidates, dry_run):
        return self.scan_id

    def active_symbol_cooldown(self, symbol):
        return self.cooldowns.get(symbol)

    def record_event(self, event_type, payload=None, symbol=None):
        self.events.append((event_type, payload, symbol))
        return True

    def record_signal(self, scan_id, candidate, analysis, decision):
        self.record_calls += 1
        self.record_decisions.append((candidate.symbol, decision))
        self.call_order.append(("record", candidate.symbol, decision))
        if self.signal_ids is not None:
            return self.signal_ids.pop(0)
        return self.signal_id

    def publish_strategy_signal_batch(self, scan_id, expected_count):
        self.publish_calls.append((scan_id, expected_count))
        self.call_order.append(("publish", scan_id, expected_count))
        if self.publish_error is not None:
            raise self.publish_error
        if self.publish_result:
            self.published_scan_id = scan_id
        return self.publish_result

    def current_strategy_signal_scan_id(self):
        self.call_order.append(("current", self.scan_id))
        if self.forced_current_scan_id != "AUTO":
            return self.forced_current_scan_id
        return self.published_scan_id

    def record_trade_open(self, scan_id, state):
        self.trade_opens.append((scan_id, state.symbol))
        return len(self.trade_opens)

    def complete_scan(self, scan_id, opened):
        self.completed.append((scan_id, opened))


class _HostileMultiRecorder:
    def __init__(self) -> None:
        self.events = []
        self.completed = []

    def expire_stale_n14_active_episodes(self, strategy_id, current_open_time_ms):
        return "OK"

    def get_required_n14_snapshot_symbols(self, strategy_id, current_open_time_ms):
        return []

    def get_pending_n15_snapshot_symbols(self, strategy_id):
        return []

    def begin_scan(self, scanned_count, candidates, dry_run):
        return 91

    def record_event(self, event_type, payload=None, symbol=None):
        self.events.append((event_type, payload, symbol))
        return True

    def complete_scan(self, scan_id, opened):
        self.completed.append((scan_id, opened))

    def current_strategy_signal_scan_id(self):
        return None


class _HostileMultiTrader:
    def __init__(self) -> None:
        self.close_calls = 0
        self.sync_calls = 0
        self.build_calls = 0
        self.open_calls = 0

    def close_dry_run_position_if_triggered(self):
        self.close_calls += 1
        return None

    def sync_state_with_exchange(self):
        self.sync_calls += 1
        return SyncResult(has_position=False)

    def build_trade_plan(self, symbol, mark_price):
        self.build_calls += 1
        raise AssertionError("an unpublished batch must not build a trade plan")

    def open_long_plan_with_protection(self, plan):
        self.open_calls += 1
        raise AssertionError("an unpublished batch must not place a live order")


class _HostileMultiPaperTrader:
    def __init__(self) -> None:
        self.close_calls = 0
        self.open_calls = 0

    def close_triggered_open_trades(self, *args):
        self.close_calls += 1
        return []

    def open_trade(self, *args, **kwargs):
        self.open_calls += 1
        raise AssertionError("an unpublished batch must not open a paper trade")


class SignalRetentionGuardTests(unittest.TestCase):
    def _recorder(self, tmpdir: str) -> ReviewRecorder:
        database = Path(tmpdir) / "review.sqlite3"
        ledger = Path(tmpdir) / "n16_claim_ledger.sqlite3"
        return make_test_recorder(
            str(database), LOGGER, n16_claim_ledger_file=ledger
        )

    def _begin(self, recorder: ReviewRecorder, symbol: str = "GUARDUSDT") -> int:
        scan_id = recorder.begin_scan(1, [_candidate(symbol)], dry_run=True)
        self.assertIsNotNone(scan_id)
        return scan_id

    def _seed_current(
        self,
        recorder: ReviewRecorder,
        symbol: str = "OLDUSDT",
    ) -> tuple[int, tuple[tuple[object, ...], ...]]:
        scan_id = self._begin(recorder, symbol)
        self.assertIsNotNone(
            _record_strategy_signal(recorder, scan_id, symbol=symbol)
        )
        self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
        return scan_id, tuple(recorder.list_current_strategy_signals())

    def test_target_passed_signal_without_structure_never_writes_any_audit(self):
        for strategy_id in ("N06", "N07", "N08", "N11", "N12", "N16"):
            with self.subTest(strategy_id=strategy_id), tempfile.TemporaryDirectory() as tmpdir:
                recorder = self._recorder(tmpdir)
                scan_id = self._begin(recorder)

                self.assertIsNone(
                    _record_strategy_signal(
                        recorder,
                        scan_id,
                        strategy_id=strategy_id,
                        passed=True,
                        structure_id=None,
                    )
                )

                with recorder._connect() as connection:
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
                    self.assertEqual(
                        connection.execute(
                            "SELECT recorded_count FROM strategy_signal_batches "
                            "WHERE scan_id = ?",
                            (scan_id,),
                        ).fetchone(),
                        (0,),
                    )

    def test_after_insert_row_deletion_cannot_publish_a_phantom_batch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            current_scan, current_rows = self._seed_current(recorder)
            staging_scan = self._begin(recorder, "PHANTOMUSDT")
            with recorder._connect() as connection:
                connection.execute(
                    "CREATE TRIGGER delete_new_strategy_signal "
                    "AFTER INSERT ON strategy_signals "
                    "WHEN NEW.scan_id = %d BEGIN "
                    "DELETE FROM strategy_signals WHERE id = NEW.id; END"
                    % staging_scan
                )
                commit_test_schema_change(recorder, connection)

            phantom_id = _record_strategy_signal(
                recorder,
                staging_scan,
                symbol="PHANTOMUSDT",
            )

            self.assertIsNotNone(phantom_id)
            self.assertFalse(
                recorder.publish_strategy_signal_batch(staging_scan, 1)
            )
            with self.assertRaises(RuntimeError):
                recorder.current_strategy_signal_scan_id()
            with self.assertRaises(RuntimeError):
                recorder.list_current_strategy_signals()
            with recorder._connect() as connection:
                self.assertEqual(
                    tuple(
                        connection.execute(
                            "SELECT id, strategy_id, symbol, passed, decision, "
                            "reason, structure_id, detail_json, created_at "
                            "FROM strategy_signals WHERE scan_id = ? ORDER BY id",
                            (current_scan,),
                        ).fetchall()
                    ),
                    current_rows,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT recorded_count FROM strategy_signal_batches "
                        "WHERE scan_id = ?",
                        (staging_scan,),
                    ).fetchone(),
                    (1,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id = ?",
                        (staging_scan,),
                    ).fetchone(),
                    (0,),
                )

    def test_staging_manifest_tamper_never_switches_current(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            current_scan, current_rows = self._seed_current(recorder)
            staging_scan = self._begin(recorder, "TAMPERUSDT")
            signal_id = _record_strategy_signal(
                recorder,
                staging_scan,
                symbol="TAMPERUSDT",
            )
            self.assertIsNotNone(signal_id)
            with recorder._connect() as connection:
                cursor = connection.execute(
                    "UPDATE strategy_signals SET reason = 'MANIFEST_TAMPERED' "
                    "WHERE id = ?",
                    (signal_id,),
                )
                self.assertEqual(cursor.rowcount, 1)

            self.assertFalse(
                recorder.publish_strategy_signal_batch(staging_scan, 1)
            )
            with self.assertRaises(RuntimeError):
                recorder.current_strategy_signal_scan_id()
            with self.assertRaises(RuntimeError):
                recorder.list_current_strategy_signals()
            with recorder._connect() as connection:
                self.assertEqual(
                    tuple(
                        connection.execute(
                            "SELECT id, strategy_id, symbol, passed, decision, "
                            "reason, structure_id, detail_json, created_at "
                            "FROM strategy_signals WHERE scan_id = ? ORDER BY id",
                            (current_scan,),
                        ).fetchall()
                    ),
                    current_rows,
                )

    def test_current_reader_keeps_one_sqlite_snapshot_during_publish_race(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._recorder(tmpdir)
            old_scan, old_rows = self._seed_current(writer, "OLDUSDT")
            new_scan = self._begin(writer, "NEWUSDT")
            self.assertIsNotNone(
                _record_strategy_signal(writer, new_scan, symbol="NEWUSDT")
            )
            reader = self._recorder(tmpdir)
            original_connect = reader._connect
            switched = []

            class PointerCursor:
                def __init__(self, cursor):
                    self.cursor = cursor

                def fetchone(self):
                    row = self.cursor.fetchone()
                    self.assert_old_pointer(row)
                    switched.append(
                        writer.publish_strategy_signal_batch(new_scan, 1)
                    )
                    return row

                @staticmethod
                def assert_old_pointer(row):
                    if row != (
                        old_scan,
                        1,
                        "COMPLETE",
                        0,
                        0,
                        0,
                        "0" * 64,
                        "GENESIS",
                    ):
                        raise AssertionError("reader did not capture the old pointer")

            class ConnectionProxy:
                def __init__(self, connection):
                    self.connection = connection
                    self.intercepted = False

                def execute(self, sql, parameters=()):
                    cursor = self.connection.execute(sql, parameters)
                    normalized = " ".join(sql.split())
                    if (
                        not self.intercepted
                        and normalized.startswith(
                            "SELECT current_scan_id, retention_active, migration_state"
                        )
                    ):
                        self.intercepted = True
                        return PointerCursor(cursor)
                    return cursor

            @contextmanager
            def intercepted_connect():
                with original_connect() as connection:
                    yield ConnectionProxy(connection)

            with patch.object(reader, "_connect", new=intercepted_connect):
                raced_rows = tuple(reader.list_current_strategy_signals())

            self.assertEqual(switched, [True])
            self.assertEqual(raced_rows, old_rows)
            self.assertTrue(raced_rows)
            self.assertEqual(raced_rows[0][2], "OLDUSDT")
            self.assertEqual(writer.current_strategy_signal_scan_id(), new_scan)
            self.assertEqual(
                [row[2] for row in reader.list_current_strategy_signals()],
                ["NEWUSDT"],
            )

    def _single_strategy_bot(
        self,
        tmpdir: str,
        recorder: _SingleStrategyRecorder,
        candidates=None,
        *,
        successful_order: bool = False,
    ) -> tuple[TradingBot, _SingleStrategyClient, _SingleStrategyTrader]:
        candidate_list = candidates or [_candidate("SINGLEUSDT")]
        client = _SingleStrategyClient()
        trader = _SingleStrategyTrader(
            recorder.call_order,
            succeed=successful_order,
        )
        bot = TradingBot.__new__(TradingBot)
        bot.config = SimpleNamespace(
            dry_run=True,
            trend_window=96,
            multi_strategy_enabled=False,
        )
        bot.logger = LOGGER
        bot.client = client
        bot.monitor = _SingleStrategyMonitor(candidate_list)
        bot.recorder = recorder
        bot.trader = trader
        bot.state = StateStore(str(Path(tmpdir) / "state.json"))
        return bot, client, trader

    def test_single_strategy_begin_scan_failure_stops_before_klines_and_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = _SingleStrategyRecorder(scan_id=None, signal_id=None)
            bot, client, trader = self._single_strategy_bot(tmpdir, recorder)

            bot._run_once_single_strategy()

            self.assertEqual(client.kline_calls, 0)
            self.assertEqual(recorder.record_calls, 0)
            self.assertEqual(trader.open_calls, 0)
            self.assertEqual(recorder.completed, [])

    def test_single_strategy_passed_audit_failure_stops_before_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = _SingleStrategyRecorder(scan_id=41, signal_id=None)
            bot, client, trader = self._single_strategy_bot(tmpdir, recorder)
            passed = AnalysisResult(
                symbol="SINGLEUSDT",
                passed=True,
                trend_slope=Decimal("1"),
                pattern="C_UP_PULLBACK_BOUNCE",
                current_bullish=True,
                detail="passed",
                matched_patterns=("C",),
            )

            with patch("trading_bot.main.analyze_symbol", return_value=passed):
                bot._run_once_single_strategy()

            self.assertEqual(client.kline_calls, 1)
            self.assertEqual(recorder.record_calls, 1)
            self.assertEqual(trader.open_calls, 0)
            self.assertEqual(recorder.publish_calls, [])
            self.assertEqual(recorder.completed, [(41, False)])

    def test_single_strategy_rejected_failure_blocks_later_passed_default_route(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            candidates = [_candidate("REJECTEDUSDT"), _candidate("PASSEDUSDT")]
            recorder = _SingleStrategyRecorder(
                scan_id=42,
                signal_id=None,
                signal_ids=[None, 7],
            )
            bot, client, trader = self._single_strategy_bot(
                tmpdir, recorder, candidates
            )
            rejected = AnalysisResult(
                "REJECTEDUSDT", False, Decimal("1"), None, True, "rejected"
            )
            passed = AnalysisResult(
                "PASSEDUSDT",
                True,
                Decimal("1"),
                "C_UP_PULLBACK_BOUNCE",
                True,
                "passed",
                ("C",),
            )

            with patch(
                "trading_bot.main.analyze_symbol",
                side_effect=[rejected, passed],
            ):
                bot.run_once()

            self.assertEqual(client.kline_calls, 1)
            self.assertEqual(recorder.record_decisions, [("REJECTEDUSDT", "REJECTED")])
            self.assertEqual(recorder.publish_calls, [])
            self.assertEqual(trader.open_calls, 0)
            self.assertEqual(recorder.completed, [(42, False)])

    def test_single_strategy_later_rejected_failure_blocks_earlier_passed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            candidates = [_candidate("EARLYPASSUSDT"), _candidate("LATEFAILUSDT")]
            recorder = _SingleStrategyRecorder(
                scan_id=46,
                signal_id=None,
                signal_ids=[7, None],
            )
            bot, client, trader = self._single_strategy_bot(
                tmpdir, recorder, candidates
            )
            passed = AnalysisResult(
                "EARLYPASSUSDT",
                True,
                Decimal("1"),
                "C_UP_PULLBACK_BOUNCE",
                True,
                "passed",
                ("C",),
            )
            rejected = AnalysisResult(
                "LATEFAILUSDT", False, Decimal("1"), None, True, "rejected"
            )

            with patch(
                "trading_bot.main.analyze_symbol",
                side_effect=[passed, rejected],
            ):
                bot.run_once()

            self.assertEqual(client.kline_calls, 2)
            self.assertEqual(
                recorder.record_decisions,
                [("EARLYPASSUSDT", "PASSED"), ("LATEFAILUSDT", "REJECTED")],
            )
            self.assertEqual(recorder.publish_calls, [])
            self.assertEqual(trader.open_calls, 0)
            self.assertEqual(recorder.completed, [(46, False)])

    def test_single_strategy_passed_never_executes_when_publish_fails(self):
        for publish_result, publish_error in (
            (False, None),
            (True, RuntimeError("forced publish failure")),
        ):
            with self.subTest(
                publish_result=publish_result,
                publish_error=publish_error,
            ), tempfile.TemporaryDirectory() as tmpdir:
                recorder = _SingleStrategyRecorder(
                    scan_id=43,
                    signal_id=7,
                    publish_result=publish_result,
                    publish_error=publish_error,
                )
                bot, _, trader = self._single_strategy_bot(tmpdir, recorder)
                passed = AnalysisResult(
                    "SINGLEUSDT",
                    True,
                    Decimal("1"),
                    "C_UP_PULLBACK_BOUNCE",
                    True,
                    "passed",
                    ("C",),
                )

                with patch("trading_bot.main.analyze_symbol", return_value=passed):
                    bot.run_once()

                self.assertEqual(recorder.publish_calls, [(43, 1)])
                self.assertEqual(trader.open_calls, 0)
                self.assertEqual(recorder.completed, [(43, False)])

    def test_single_strategy_void_audit_failure_blocks_later_passed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            void_candidate = _candidate("VOIDUSDT")
            passed_candidate = _candidate("PASSEDUSDT")
            cooldown = SimpleNamespace(
                cooldown_until="2026-07-15T00:00:00+00:00",
                reason="STOP_LOSS",
                source_trade_id=5,
            )
            recorder = _SingleStrategyRecorder(
                scan_id=44,
                signal_id=None,
                signal_ids=[None, 9],
                cooldowns={void_candidate.symbol: cooldown},
            )
            bot, client, trader = self._single_strategy_bot(
                tmpdir,
                recorder,
                [void_candidate, passed_candidate],
            )

            bot.run_once()

            self.assertEqual(client.kline_calls, 0)
            self.assertEqual(recorder.record_decisions, [("VOIDUSDT", "VOID")])
            self.assertEqual(recorder.publish_calls, [])
            self.assertEqual(trader.open_calls, 0)
            self.assertEqual(recorder.completed, [(44, False)])

    def test_single_strategy_records_every_decision_before_publish_and_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            first = _candidate("FIRSTUSDT")
            second = _candidate("SECONDUSDT")
            recorder = _SingleStrategyRecorder(
                scan_id=45,
                signal_id=7,
                signal_ids=[7, 8],
                publish_result=True,
            )
            bot, client, trader = self._single_strategy_bot(
                tmpdir,
                recorder,
                [first, second],
                successful_order=True,
            )
            passed = AnalysisResult(
                "FIRSTUSDT",
                True,
                Decimal("1"),
                "C_UP_PULLBACK_BOUNCE",
                True,
                "passed",
                ("C",),
            )
            rejected = AnalysisResult(
                "SECONDUSDT", False, Decimal("1"), None, True, "rejected"
            )

            with patch(
                "trading_bot.main.analyze_symbol",
                side_effect=[passed, rejected],
            ):
                bot.run_once()

            self.assertEqual(client.kline_calls, 2)
            self.assertEqual(
                recorder.call_order[:5],
                [
                    ("record", "FIRSTUSDT", "PASSED"),
                    ("record", "SECONDUSDT", "REJECTED"),
                    ("publish", 45, 2),
                    ("current", 45),
                    ("open", "FIRSTUSDT"),
                ],
            )
            self.assertEqual(trader.open_calls, 1)
            self.assertEqual(recorder.trade_opens, [(45, "FIRSTUSDT")])
            self.assertEqual(recorder.completed, [(45, True)])

    def test_default_single_strategy_real_sqlite_publishes_before_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            call_order = []
            trader = _SingleStrategyTrader(call_order, succeed=True)
            candidate = _candidate("REALSQLUSDT")
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(
                dry_run=True,
                trend_window=96,
                multi_strategy_enabled=False,
            )
            bot.logger = LOGGER
            bot.client = _SingleStrategyClient()
            bot.monitor = _SingleStrategyMonitor(candidate)
            bot.recorder = recorder
            bot.trader = trader
            bot.state = StateStore(str(Path(tmpdir) / "state.json"))
            passed = AnalysisResult(
                candidate.symbol,
                True,
                Decimal("1"),
                "C_UP_PULLBACK_BOUNCE",
                True,
                "passed",
                ("C",),
            )

            with patch("trading_bot.main.analyze_symbol", return_value=passed):
                bot.run_once()

            current_scan_id = recorder.current_strategy_signal_scan_id()
            self.assertIsNotNone(current_scan_id)
            current = recorder.list_current_strategy_signals()
            self.assertEqual(len(current), 1)
            self.assertEqual(current[0][1:5], (
                "LEGACY_SINGLE",
                candidate.symbol,
                1,
                "PASSED",
            ))
            self.assertEqual(trader.open_calls, 1)
            with recorder._connect() as connection:
                manifest = connection.execute(
                    "SELECT manifest_sha256 FROM strategy_signal_batches "
                    "WHERE scan_id = ?",
                    (current_scan_id,),
                ).fetchone()[0]
                self.assertEqual(
                    connection.execute(
                        "SELECT state, recorded_count, expected_count "
                        "FROM strategy_signal_batches WHERE scan_id = ?",
                        (current_scan_id,),
                    ).fetchone(),
                    ("CURRENT", 1, 1),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits "
                        "WHERE source_scan_id = ? AND claim_state = 'ACTIVE'",
                        (current_scan_id,),
                    ).fetchone(),
                    (1,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM signal_reviews WHERE scan_id = ?",
                        (current_scan_id,),
                    ).fetchone(),
                    (1,),
                )
            report = inspect_signal_retention(
                str((Path(tmpdir) / "review.sqlite3").resolve()),
                current_scan_id,
                1,
                manifest,
                n16_claim_ledger=str(
                    (Path(tmpdir) / "n16_claim_ledger.sqlite3").resolve()
                ),
            )
            self.assertEqual(report.keep_scan_id, current_scan_id)
            self.assertTrue(report.keep_attestation_verified)

    def test_real_sqlite_later_rejected_write_failure_blocks_earlier_passed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._recorder(tmpdir)
            first = _candidate("SQLPASSUSDT")
            second = _candidate("SQLFAILUSDT")
            trader = _SingleStrategyTrader([], succeed=True)
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(
                dry_run=True,
                trend_window=96,
                multi_strategy_enabled=False,
            )
            bot.logger = LOGGER
            bot.client = _SingleStrategyClient()
            bot.monitor = _SingleStrategyMonitor([first, second])
            bot.recorder = recorder
            bot.trader = trader
            bot.state = StateStore(str(Path(tmpdir) / "state.json"))
            passed = AnalysisResult(
                first.symbol,
                True,
                Decimal("1"),
                "C_UP_PULLBACK_BOUNCE",
                True,
                "passed",
                ("C",),
            )
            rejected = AnalysisResult(
                second.symbol, False, Decimal("1"), None, True, "rejected"
            )
            with recorder._connect() as connection:
                connection.execute(
                    "CREATE TRIGGER fail_legacy_rejected_ordinary "
                    "BEFORE INSERT ON strategy_signals "
                    "WHEN NEW.decision = 'REJECTED' BEGIN "
                    "SELECT RAISE(ABORT, 'forced rejected write failure'); END"
                )
                commit_test_schema_change(recorder, connection)

            with patch(
                "trading_bot.main.analyze_symbol",
                side_effect=[passed, rejected],
            ):
                bot.run_once()

            self.assertEqual(trader.open_calls, 0)
            self.assertIsNone(recorder.current_strategy_signal_scan_id())
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state, recorded_count FROM strategy_signal_batches"
                    ).fetchone(),
                    ("STAGING", 1),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT decision FROM strategy_signals"
                    ).fetchall(),
                    [("PASSED",)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT claim_state FROM strategy_passed_signal_audits"
                    ).fetchall(),
                    [("STAGED",)],
                )

    def test_multi_strategy_distrusts_unpublished_hostile_scheduler_result(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            strategy = load_first_stage_strategies()[0]
            candidate = _candidate("HOSTILEUSDT")
            signal = StrategySignalDecision(
                strategy=strategy,
                candidate=candidate,
                analysis=None,
                passed=True,
                decision="PASSED",
                reason="PASSED",
                signal_id=999,
            )

            class Monitor:
                def scan_for_strategies(self, volume_top_n):
                    return StrategyMarketScan(1, [candidate], [])

            class Client:
                def get_klines(self, symbol):
                    return []

                def get_klines_for_interval(self, *args, **kwargs):
                    return []

                def get_aggregate_trades(self, *args, **kwargs):
                    return []

            class Scheduler:
                def evaluate(self, scan_id, candidates, raw_klines, checked_at_ms=None):
                    return SchedulerResult(
                        signals=[signal],
                        passed_signals=[signal],
                        live_candidates=[],
                        signal_batch_published=False,
                    )

            recorder = _HostileMultiRecorder()
            trader = _HostileMultiTrader()
            paper_trader = _HostileMultiPaperTrader()
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=True)
            bot.logger = LOGGER
            bot.client = Client()
            bot.monitor = Monitor()
            bot.recorder = recorder
            bot.trader = trader
            bot.paper_trader = paper_trader
            bot.strategy_scheduler = Scheduler()
            bot.strategies = (strategy,)
            bot.state = StateStore(str(Path(tmpdir) / "state.json"))

            with patch.object(
                bot,
                "_build_strategy_plan",
                side_effect=AssertionError(
                    "an unpublished batch must not build a trade plan"
                ),
            ):
                bot._run_once_multi_strategy()

            self.assertEqual(trader.build_calls, 0)
            self.assertEqual(trader.open_calls, 0)
            self.assertEqual(trader.close_calls, 0)
            self.assertEqual(trader.sync_calls, 0)
            self.assertEqual(paper_trader.close_calls, 0)
            self.assertEqual(paper_trader.open_calls, 0)
            self.assertEqual(recorder.completed, [(91, False)])
            self.assertEqual(
                [event[0] for event in recorder.events],
                ["strategy_signal_batch_execution_blocked"],
            )

    def test_multi_strategy_distrusts_true_flag_without_current_batch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            strategy = load_first_stage_strategies()[0]
            candidate = _candidate("HOSTILETRUEUSDT")
            signal = StrategySignalDecision(
                strategy=strategy,
                candidate=candidate,
                analysis=None,
                passed=True,
                decision="PASSED",
                reason="PASSED",
                signal_id=999,
            )

            class Monitor:
                def scan_for_strategies(self, volume_top_n):
                    return StrategyMarketScan(1, [candidate], [])

            class Client:
                def get_klines(self, symbol):
                    return []

                def get_klines_for_interval(self, *args, **kwargs):
                    return []

                def get_aggregate_trades(self, *args, **kwargs):
                    return []

            class Scheduler:
                def evaluate(self, scan_id, candidates, raw_klines, checked_at_ms=None):
                    return SchedulerResult(
                        signals=[signal],
                        passed_signals=[signal],
                        live_candidates=[],
                        signal_batch_published=True,
                    )

            recorder = _HostileMultiRecorder()
            trader = _HostileMultiTrader()
            paper_trader = _HostileMultiPaperTrader()
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=True)
            bot.logger = LOGGER
            bot.client = Client()
            bot.monitor = Monitor()
            bot.recorder = recorder
            bot.trader = trader
            bot.paper_trader = paper_trader
            bot.strategy_scheduler = Scheduler()
            bot.strategies = (strategy,)
            bot.state = StateStore(str(Path(tmpdir) / "state.json"))

            bot._run_once_multi_strategy()

            self.assertEqual(trader.build_calls, 0)
            self.assertEqual(trader.open_calls, 0)
            self.assertEqual(trader.close_calls, 0)
            self.assertEqual(trader.sync_calls, 0)
            self.assertEqual(paper_trader.close_calls, 0)
            self.assertEqual(paper_trader.open_calls, 0)
            self.assertEqual(recorder.completed, [(91, False)])
            self.assertEqual(
                [event[0] for event in recorder.events],
                ["strategy_signal_batch_execution_blocked"],
            )


if __name__ == "__main__":
    unittest.main()
