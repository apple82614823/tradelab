from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests.recorder_test_utils import make_test_recorder
from trading_bot.analyzer import AnalysisResult
from trading_bot.binance_client import BinanceAPIError
from trading_bot.main import (
    TradingBot,
    _bounded_audit_error,
    _next_fixed_poll_tick,
    _stale_kline_snapshot_symbols,
    _unready_live_kline_value_symbols,
)
from trading_bot.micro_analyzer import (
    MicroAnalysisResult,
    analyze_micro_strategy,
    analyze_n22,
    analyze_n25,
)
from trading_bot.micro_observation import (
    MicroObservationCache,
    MicroObservationError,
    MicroObservationSampler,
)
from trading_bot.monitor import FundingCandidate, StrategyMarketScan
from trading_bot.n15_snapshot import build_n15_snapshot
from trading_bot.n15_terminal_schema import decode_n15_terminal_payload
from trading_bot.paper_trader import PaperTrader
from trading_bot.recorder import (
    ReviewRecorder,
    StrategyState,
)
from trading_bot.state import PositionState, StateStore
from trading_bot.strategies import (
    N15_STRATEGY,
    N17_STRATEGY,
    N20_STRATEGY,
    N21_STRATEGY,
    load_all_strategies,
    load_first_stage_strategies,
)
from trading_bot.strategy_scheduler import (
    LiveTradeCandidate,
    N20BatchContext,
    SchedulerResult,
    StrategyScheduler,
    StrategySignalDecision,
)
from trading_bot.trader import (
    EntryWindowExpiredError,
    LiveCloseResolution,
    SyncResult,
    TradePlan,
    Trader,
    assert_entry_window_open,
)
from tests.test_trader import FakeCloseResolutionClient, live_test_config
from tests.test_micro_strategies import (
    OPEN_TIME,
    authenticated_top100_observations,
    observation,
)


def plan(symbol):
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


def signal(strategy, symbol, signal_id=None):
    return StrategySignalDecision(
        strategy=strategy,
        candidate=FundingCandidate(symbol, Decimal("-0.02"), Decimal("100")),
        analysis=AnalysisResult(
            symbol=symbol,
            passed=True,
            trend_slope=Decimal("1"),
            pattern="C_UP_PULLBACK_BOUNCE",
            current_bullish=True,
            detail="passed",
            matched_patterns=("C",),
        ),
        passed=True,
        decision="PASSED",
        reason="PASSED",
        signal_id=signal_id,
    )


class FakeClient:
    def __init__(self):
        self.kline_calls = []

    def get_klines(self, symbol):
        self.kline_calls.append(symbol)
        return [[0, "1", "1", "1", "1", "0", 0]]

    def get_klines_for_interval(self, symbol, interval, limit=1500, start_time_ms=None):
        return []

    def get_aggregate_trades(self, symbol, start_time_ms, end_time_ms):
        return []


class FakeMonitor:
    def __init__(self, candidates):
        self.candidates = candidates

    def scan_for_strategies(self, volume_top_n):
        return StrategyMarketScan(len(self.candidates), self.candidates, [])


def boundary_kline_rows(last_open_time_ms):
    start = last_open_time_ms - 121 * 900_000
    return [
        [
            start + index * 900_000,
            "100",
            "101",
            "99",
            "100.5",
            "1",
            start + (index + 1) * 900_000 - 1,
            "100",
            10,
            "10",
            "55",
        ]
        for index in range(122)
    ]


class FakeTrader:
    def __init__(self, plans, has_position=False, fail_live=False):
        self.plans = plans
        self.has_position = has_position
        self.fail_live = fail_live
        self.live_calls = 0
        self.sync_calls = 0

    def close_dry_run_position_if_triggered(self):
        return None

    def sync_state_with_exchange(self):
        self.sync_calls += 1
        return SyncResult(has_position=self.has_position)

    def build_trade_plan(self, symbol, mark_price):
        return self.plans[symbol]

    def open_long_plan_with_protection(self, trade_plan):
        self.live_calls += 1
        if self.fail_live:
            raise BinanceAPIError(
                self.fail_live
                if type(self.fail_live) is str
                else "simulated live failure"
            )
        return PositionState(
            symbol=trade_plan.symbol,
            quantity=str(trade_plan.quantity),
            entry_price=str(trade_plan.entry_price),
            stop_loss_price=str(trade_plan.stop_loss_price),
            take_profit_price=str(trade_plan.take_profit_price),
            leverage=trade_plan.leverage,
            opened_at="2026-07-10T00:00:00+00:00",
            dry_run=True,
            orders={"plan": {}},
        )


class FakeScheduler:
    def __init__(self, passed_signals, live_candidates):
        self.passed_signals = passed_signals
        self.live_candidates = live_candidates

    def evaluate(self, scan_id, candidates, raw_klines_by_symbol, checked_at_ms=None):
        return SchedulerResult(
            self.passed_signals,
            self.passed_signals,
            self.live_candidates,
            True,
        )

    def choose_live_candidate(self, live_candidates, live_blocked):
        return None if live_blocked or not live_candidates else live_candidates[0]


def eligible_state(strategy_id):
    return StrategyState(
        strategy_id=strategy_id,
        consecutive_wins=2,
        paper_trade_count=2,
        win_count=2,
        loss_count=0,
        win_rate="1",
        live_eligible=True,
        last_trade_result="WIN",
        last_trade_closed_at="2026-07-09T00:00:00+00:00",
        updated_at="2026-07-09T00:00:00+00:00",
    )


class MainMultiStrategyRoutingTests(unittest.TestCase):
    def test_fixed_poll_tick_skips_missed_slots_without_drift_or_burst(self):
        self.assertEqual(_next_fixed_poll_tick(0.0, 20.0, 60), 60.0)
        self.assertEqual(_next_fixed_poll_tick(60.0, 195.0, 60), 240.0)
        self.assertEqual(_next_fixed_poll_tick(240.0, 250.0, 60), 300.0)
        self.assertEqual(_next_fixed_poll_tick(0.0, 60.0, 60), 120.0)

    def test_run_forever_uses_fixed_grid_and_stop_wakes_wait(self):
        class Clock:
            def __init__(self):
                self.now = 0.0

            def monotonic(self):
                return self.now

        class StopEvent:
            def __init__(self, clock):
                self.clock = clock
                self.stopped = False
                self.waits = []

            def is_set(self):
                return self.stopped

            def set(self):
                self.stopped = True

            def wait(self, timeout):
                self.waits.append(timeout)
                self.clock.now += timeout
                if len(self.waits) == 3:
                    self.stopped = True
                return self.stopped

        clock = Clock()
        stop_event = StopEvent(clock)
        starts = []
        durations = iter((20.0, 135.0, 10.0))
        bot = TradingBot.__new__(TradingBot)
        bot.config = SimpleNamespace(
            dry_run=True,
            base_url="https://example.invalid",
            poll_interval_seconds=60,
        )
        bot.logger = SimpleNamespace(
            info=lambda *args, **kwargs: None,
            exception=lambda *args, **kwargs: None,
        )
        bot._poll_stop_event = stop_event
        bot._start_micro_observation_sampler = lambda: True
        bot.close = lambda: stop_event.set()

        def run_once():
            starts.append(clock.now)
            clock.now += next(durations)
            if len(starts) == 3:
                raise RuntimeError("deterministic round failure")

        bot.run_once = run_once
        with patch("trading_bot.main.time.monotonic", clock.monotonic):
            bot.run_forever()

        self.assertEqual(starts, [0.0, 60.0, 240.0])
        self.assertEqual(stop_event.waits, [40.0, 45.0, 50.0])
        self.assertTrue(stop_event.is_set())

    def test_production_passed_samples_keep_exact_120_second_order_boundary(self):
        deadline_ms = 1_800_000_120_000
        for strategy_id, symbol in (
            ("N16", "ENAUSDT"),
            ("N16", "SKHYUSDT"),
            ("N16", "SYNUSDT"),
            ("N17", "BNBUSDT"),
            ("N20", "DOTUSDT"),
        ):
            with self.subTest(strategy_id=strategy_id, symbol=symbol):
                candidate_plan = replace(
                    plan(symbol),
                    entry_candle_open_time_ms=deadline_ms - 120_000,
                    entry_deadline_ms=deadline_ms,
                    structure_context={"strategy_id": strategy_id},
                )
                self.assertIsNone(
                    assert_entry_window_open(candidate_plan, deadline_ms - 1)
                )
                with self.assertRaises(EntryWindowExpiredError):
                    assert_entry_window_open(candidate_plan, deadline_ms)

    def test_shared_strategy_kline_generation_is_bounded_parallel_and_exact(self):
        class TimedClient:
            def __init__(self):
                self.lock = threading.Lock()
                self.active = 0
                self.max_active = 0
                self.calls = []

            def get_klines(self, symbol):
                with self.lock:
                    self.active += 1
                    self.max_active = max(self.max_active, self.active)
                    self.calls.append(symbol)
                try:
                    time.sleep(0.02)
                    return [[symbol]]
                finally:
                    with self.lock:
                        self.active -= 1

        client = TimedClient()
        bot = TradingBot.__new__(TradingBot)
        bot.client = client
        bot._kline_request_slots = threading.BoundedSemaphore(10)
        symbols = tuple("S%03dUSDT" % index for index in range(100))

        started = time.perf_counter()
        rows, observed_at, failures = bot._fetch_strategy_kline_generation(
            symbols
        )
        elapsed = time.perf_counter() - started

        self.assertEqual(set(rows), set(symbols))
        self.assertEqual(set(observed_at), set(symbols))
        self.assertEqual(failures, ())
        self.assertEqual(len(client.calls), 100)
        self.assertEqual(set(client.calls), set(symbols))
        self.assertGreaterEqual(client.max_active, 2)
        self.assertLessEqual(client.max_active, 10)
        self.assertLess(elapsed, 0.8)

    def test_shared_strategy_kline_generation_reports_exact_failure_and_closes(self):
        class FailingClient:
            def __init__(self):
                self.calls = []

            def get_klines(self, symbol):
                self.calls.append(symbol)
                if symbol == "BADUSDT":
                    raise BinanceAPIError("bounded failure")
                return [[symbol]]

        bot = TradingBot.__new__(TradingBot)
        bot.client = FailingClient()
        bot._kline_request_slots = threading.BoundedSemaphore(10)

        rows, observed_at, failures = bot._fetch_strategy_kline_generation(
            ("GOODUSDT", "BADUSDT")
        )

        self.assertEqual(rows, {"GOODUSDT": [["GOODUSDT"]]})
        self.assertEqual(set(observed_at), {"GOODUSDT", "BADUSDT"})
        self.assertEqual(tuple(item[0] for item in failures), ("BADUSDT",))
        self.assertEqual(set(bot.client.calls), {"GOODUSDT", "BADUSDT"})
        self.assertFalse(any(
            thread.name.startswith("strategy-kline")
            for thread in threading.enumerate()
        ))

    def test_untrusted_symbol_fails_before_scan_and_every_execution_side_effect(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            strategy = load_first_stage_strategies()[0]
            selected = signal(strategy, "BAD/USDT", signal_id=501)
            live_candidate = LiveTradeCandidate(
                selected,
                eligible_state(strategy.strategy_id),
            )
            bot, recorder = self._bot(
                tmpdir,
                [selected],
                [live_candidate],
            )

            bot._run_once_multi_strategy()

            self.assertEqual(bot.client.kline_calls, [])
            self.assertEqual(bot.trader.sync_calls, 0)
            self.assertEqual(bot.trader.live_calls, 0)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signal_batches"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_paper_trades"
                    ).fetchone(),
                    (0,),
                )
    def test_live_failure_diagnostic_is_bounded_printable_and_redacted(self):
        error = BinanceAPIError(
            "api_key=super-secret\nsecret:other-secret " + "x" * 5000
        )

        detail = _bounded_audit_error(error)

        self.assertLessEqual(len(detail), 2048)
        self.assertNotIn("super-secret", detail)
        self.assertNotIn("other-secret", detail)
        self.assertNotIn("\n", detail)
        self.assertIn("api_key=[REDACTED]", detail)
        self.assertIn("secret:[REDACTED]", detail)
        self.assertTrue(detail.endswith("...[TRUNCATED]"))

    def test_audit_error_redacts_structured_credentials_without_losing_identity(self):
        cases = (
            (
                "Authorization: Bearer TOPSECRET symbol=BTCUSDT reason=failed",
                ("TOPSECRET",),
                ("symbol=BTCUSDT", "reason=failed"),
            ),
            (
                "Authorization=Basic BASE64; symbol=ETHUSDT reason=denied",
                ("BASE64",),
                ("symbol=ETHUSDT", "reason=denied"),
            ),
            (
                'token="abc def" symbol=SOLUSDT reason=timeout',
                ("abc def",),
                ("symbol=SOLUSDT", "reason=timeout"),
            ),
            (
                "ToKeN = abc def symbol=XRPUSDT reason=timeout",
                ("abc def",),
                ("symbol=XRPUSDT", "reason=timeout"),
            ),
            (
                '{"api_secret": "json secret", "symbol":"ADAUSDT"}',
                ("json secret",),
                ('"symbol":"ADAUSDT"',),
            ),
            (
                "https://api.test/order?signature=sig123&timestamp=7&apiKey=key456",
                ("sig123", "key456"),
                ("timestamp=7",),
            ),
            (
                "AuThOrIzAtIoN:\x00Bearer\x1fCONTROLSECRET "
                "symbol=CTRLUSDT reason=bad",
                ("CONTROLSECRET",),
                ("symbol=CTRLUSDT", "reason=bad"),
            ),
        )
        for message, secrets, retained in cases:
            with self.subTest(message=message):
                detail = _bounded_audit_error(RuntimeError(message), limit=256)
                self.assertLessEqual(len(detail), 256)
                self.assertTrue(all(character.isprintable() for character in detail))
                self.assertIn("[REDACTED]", detail)
                for secret in secrets:
                    self.assertNotIn(secret, detail)
                for expected in retained:
                    self.assertIn(expected, detail)

        ordinary = _bounded_audit_error(
            RuntimeError(
                "symbol=TOKENUSDT reason=signature verification failed"
            )
        )
        self.assertIn("symbol=TOKENUSDT", ordinary)
        self.assertIn("reason=signature verification failed", ordinary)

        long_detail = _bounded_audit_error(
            RuntimeError(
                "Authorization: Bearer LONGSECRET symbol=LONGUSDT reason=bad "
                + "x" * 5000
            )
        )
        self.assertEqual(len(long_detail), 2048)
        self.assertNotIn("LONGSECRET", long_detail)
        self.assertIn("symbol=LONGUSDT", long_detail)
        self.assertTrue(long_detail.endswith("...[TRUNCATED]"))

    def _bot(self, tmpdir, passed_signals, live_candidates, has_position=False, fail_live=False):
        recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_main_multi"))
        recorder.upsert_strategy_definitions(tuple(item.strategy for item in passed_signals))

        bot = TradingBot.__new__(TradingBot)
        bot.config = SimpleNamespace(dry_run=True)
        bot.logger = logging.getLogger("test_main_multi")
        bot.client = FakeClient()
        bot.recorder = recorder
        bot.paper_trader = PaperTrader(recorder, bot.logger)
        bot.monitor = FakeMonitor([item.candidate for item in passed_signals])
        bot.trader = FakeTrader(
            {item.candidate.symbol: plan(item.candidate.symbol) for item in passed_signals},
            has_position=has_position,
            fail_live=fail_live,
        )
        bot.strategy_scheduler = FakeScheduler(passed_signals, live_candidates)
        # These routing tests isolate post-publication plan/live/paper
        # behavior.  The real scheduler publication transaction and main's
        # hostile-true CURRENT attestation are covered in the signal-batch
        # guard suites.
        bot._signal_batch_is_current = lambda scan_id: True
        def attest_empty_n16_claims(scan_id, expected_claims):
            if type(scan_id) is not int or scan_id <= 0 or expected_claims != ():
                raise AssertionError(
                    "routing fixture received an unexpected N16 claim"
                )

        recorder.assert_n16_current_scan_claims = attest_empty_n16_claims
        # FakeScheduler deliberately skips ordinary signal INSERTs so these
        # five tests can isolate post-publication routing.  Overlay durability
        # and its fail-closed return value have dedicated real-SQLite guards.
        recorder.update_strategy_signal = lambda *_args, **_kwargs: True
        bot.strategies = tuple(item.strategy for item in passed_signals)
        bot.state = StateStore(str(Path(tmpdir) / "state.json"))
        return bot, recorder

    def test_main_uses_delayed_sampler_lease_and_aborts_it_on_publish_failure(self):
        class ActiveSampler:
            def __init__(self, inner):
                self.inner = inner
                self.confirm_calls = 0
                self.abort_calls = 0
                self.freeze_completed_at = None

            @property
            def is_running(self):
                return True

            @property
            def generation(self):
                return self.inner.generation

            @property
            def boot_id(self):
                return self.inner.boot_id

            def freeze_for_scan(self, **kwargs):
                lease = self.inner.freeze_for_scan(**kwargs)
                self.freeze_completed_at = time.perf_counter()
                return lease

            def confirm(self, lease, *, current_scan_id):
                self.confirm_calls += 1
                return self.inner.confirm(
                    lease, current_scan_id=current_scan_id
                )

            def abort(self, lease=None):
                self.abort_calls += 1
                self.inner.abort(lease)

            def snapshot(self):
                return self.inner.snapshot()

        symbols = tuple(
            "BTCUSDT" if rank == 1
            else "FLOWUSDT" if rank == 7
            else f"S{rank:03d}USDT"
            for rank in range(1, 101)
        )
        candidates = [
            FundingCandidate(
                symbol,
                None,
                Decimal("100.30") if symbol == "FLOWUSDT" else Decimal("100"),
                quote_volume=Decimal(1_000_000 - rank),
                quote_volume_rank=rank,
                candidate_universe="quote_volume_top",
            )
            for rank, symbol in enumerate(symbols, start=1)
        ]
        premium = {
            symbol: {
                "symbol": symbol,
                "markPrice": "100",
                "indexPrice": "100",
                "lastFundingRate": "0",
            }
            for symbol in symbols
        }

        def warmed_sampler():
            sampler = MicroObservationSampler(boot_id="boot")
            flow_values = (
                ("100", "1000", 100, "500"),
                ("100.05", "1100", 110, "550"),
                ("100.10", "1200", 120, "602"),
                ("100.30", "1300", 130, "658"),
            )
            for ordinal, offset in enumerate(
                (1, 60_001, 120_001, 180_001), 1
            ):
                sample = {}
                for rank, symbol in enumerate(symbols, start=1):
                    sample[symbol] = observation(
                        symbol,
                        ordinal,
                        ordinal,
                        OPEN_TIME + offset,
                        quote=str(1000 + ordinal * 100),
                        trades=100 + ordinal * 10,
                        taker=str(500 + ordinal * 55),
                        rank=rank,
                    )
                close, quote, trades, taker = flow_values[ordinal - 1]
                sample["FLOWUSDT"] = observation(
                    "FLOWUSDT",
                    ordinal,
                    ordinal,
                    OPEN_TIME + offset,
                    close=close,
                    low="99.5",
                    quote=quote,
                    trades=trades,
                    taker=taker,
                    rank=7,
                )
                sample = authenticated_top100_observations(sample)
                self.assertTrue(sampler.commit_sample(sample))
            return ActiveSampler(sampler)

        class KlineClient(FakeClient):
            def get_klines(self, symbol):
                self.kline_calls.append(symbol)
                return boundary_kline_rows(OPEN_TIME)

        for publish_ok in (True, False):
            with self.subTest(publish_ok=publish_ok), tempfile.TemporaryDirectory() as tmpdir:
                bot, recorder = self._bot(tmpdir, [], [])
                recorder.upsert_strategy_definitions((N21_STRATEGY,))
                bot.strategies = (N21_STRATEGY,)
                bot.monitor = SimpleNamespace(
                    scan_for_strategies=lambda _top_n: StrategyMarketScan(
                        100,
                        [],
                        candidates,
                        premium,
                        OPEN_TIME + 180_001,
                    )
                )
                bot.client = KlineClient()
                bot.strategy_scheduler = StrategyScheduler(
                    bot.strategies, 96, recorder, bot.logger
                )
                bot.trader.has_position = True
                bot.trader.build_micro_observation_margin_capped_trade_plan = (
                    lambda _strategy_id, symbol, *_args, **kwargs: replace(
                        plan(symbol),
                        stop_mode="micro_observation_margin_capped",
                        structure_id=kwargs["structure_id"],
                    )
                )
                bot.micro_observation_cache = MicroObservationCache(
                    boot_id="legacy-unused"
                )
                active_sampler = warmed_sampler()
                bot.micro_observation_sampler = active_sampler
                phase_times = {}
                real_publish = recorder.publish_strategy_signal_batch

                def publish_batch(*args, **kwargs):
                    if not publish_ok:
                        return False
                    published = real_publish(*args, **kwargs)
                    if published:
                        phase_times["current"] = time.perf_counter()
                    return published

                observed_window_lengths = []

                def analyze_warmed(_strategy_id, symbol, windows, _checked_at):
                    observed_window_lengths.append(
                        len(windows[symbol].observations)
                    )
                    return analyze_micro_strategy(
                        _strategy_id,
                        symbol,
                        windows,
                        _checked_at,
                    )

                with patch(
                    "trading_bot.main.time.time",
                    return_value=(OPEN_TIME + 180_100) / 1000,
                ), patch(
                    "trading_bot.strategy_scheduler.analyze_micro_strategy",
                    side_effect=analyze_warmed,
                ), patch.object(
                    recorder,
                    "publish_strategy_signal_batch",
                    side_effect=publish_batch,
                ), patch.object(
                    bot.paper_trader,
                    "open_trade",
                    side_effect=AssertionError(
                        "inactive micro strategies must not open paper trades"
                    ),
                ):
                    bot._run_once_multi_strategy()

                self.assertEqual(observed_window_lengths, [4] * 100)

                if publish_ok:
                    self.assertEqual(active_sampler.confirm_calls, 1)
                    self.assertEqual(active_sampler.abort_calls, 0)
                    current_scan = recorder.current_strategy_signal_scan_id()
                    self.assertIsInstance(current_scan, int)
                    self.assertEqual(
                        len(recorder.list_current_strategy_signals()),
                        100,
                    )
                    self.assertEqual(
                        {len(window.observations) for window in active_sampler.snapshot().values()},
                        {4},
                    )
                    self.assertEqual(
                        set(phase_times),
                        {"current"},
                    )
                    self.assertIsNotNone(active_sampler.freeze_completed_at)
                    self.assertLessEqual(
                        active_sampler.freeze_completed_at,
                        phase_times["current"],
                    )
                else:
                    self.assertEqual(active_sampler.confirm_calls, 0)
                    self.assertEqual(active_sampler.abort_calls, 1)
                    self.assertEqual(active_sampler.snapshot(), {})
                    self.assertIsNone(
                        recorder.current_strategy_signal_scan_id()
                    )
                    self.assertEqual(phase_times, {})
                self.assertEqual(bot.trader.live_calls, 0)
                with recorder._read_only_runtime_snapshot() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_paper_trades"
                        ).fetchone(),
                        (0,),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM events WHERE event_type = "
                            "'strategy_entry_window_expired_before_order'"
                        ).fetchone(),
                        (0,),
                    )

    def test_micro_sampler_uses_one_credential_free_bounded_public_client(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bot = TradingBot.__new__(TradingBot)
            bot.config = replace(
                live_test_config(str(Path(tmpdir) / "account.json")),
                api_key="SHOULD_NOT_REACH_SAMPLER",
                api_secret="SHOULD_NOT_REACH_SAMPLER",
                request_timeout_seconds=10,
                request_retries=3,
            )
            bot.logger = logging.getLogger("micro-public-client")
            bot.strategies = (N21_STRATEGY,)
            bot.micro_observation_sampler = MicroObservationSampler(
                boot_id="public-client"
            )
            with patch.object(
                bot.micro_observation_sampler,
                "start",
                return_value=True,
            ) as start:
                self.assertTrue(bot._start_micro_observation_sampler())
            self.assertEqual(start.call_count, 1)
            self.assertEqual(bot._micro_sampler_client.config.api_key, "")
            self.assertEqual(bot._micro_sampler_client.config.api_secret, "")
            self.assertTrue(bot._micro_sampler_client.config.dry_run)
            self.assertEqual(
                bot._micro_sampler_client.config.request_retries, 1
            )
            self.assertEqual(
                bot._micro_sampler_client.config.request_timeout_seconds, 3
            )
            self.assertTrue(bot._stop_micro_observation_sampler())

    def test_main_freezes_rotated_sampler_generation_before_slow_scheduler(self):
        base_symbols = tuple(
            "BTCUSDT" if rank == 1 else f"R{rank:03d}USDT"
            for rank in range(1, 101)
        )
        main_symbols = (*base_symbols[:-1], "MAINNEWUSDT")
        worker_symbols = (*base_symbols[:-1], "WORKERNEWUSDT")

        def sampler_rows(identity, symbols, observed_at_ms, quote):
            return authenticated_top100_observations({
                symbol: observation(
                    symbol,
                    identity.sample_ordinal,
                    identity.generation,
                    observed_at_ms,
                    quote=quote,
                    trades=100 + identity.generation * 10,
                    taker=str(Decimal(quote) / 2),
                    rank=rank,
                )
                for rank, symbol in enumerate(symbols, start=1)
            })

        class ActiveSampler:
            def __init__(self, inner):
                self.inner = inner
                self.confirm_calls = 0

            @property
            def is_running(self):
                return True

            @property
            def generation(self):
                return self.inner.generation

            @property
            def boot_id(self):
                return self.inner.boot_id

            def freeze_for_scan(self, **kwargs):
                return self.inner.freeze_for_scan(**kwargs)

            def confirm(self, lease, *, current_scan_id):
                self.confirm_calls += 1
                return self.inner.confirm(
                    lease, current_scan_id=current_scan_id
                )

            def abort(self, lease=None):
                self.inner.abort(lease)

        candidates = [
            FundingCandidate(
                symbol,
                None,
                Decimal("100"),
                quote_volume=Decimal(1_000_000 - rank),
                quote_volume_rank=rank,
                candidate_universe="quote_volume_top",
            )
            for rank, symbol in enumerate(main_symbols, start=1)
        ]
        premium = {
            symbol: {
                "symbol": symbol,
                "markPrice": "100",
                "indexPrice": "100",
                "lastFundingRate": "0",
            }
            for symbol in set(main_symbols) | set(worker_symbols)
        }

        class KlineClient(FakeClient):
            def get_klines(self, symbol):
                self.kline_calls.append(symbol)
                return boundary_kline_rows(OPEN_TIME)

        with tempfile.TemporaryDirectory() as tmpdir:
            bot, recorder = self._bot(tmpdir, [], [])
            recorder.upsert_strategy_definitions((N21_STRATEGY,))
            bot.strategies = (N21_STRATEGY,)
            bot.monitor = SimpleNamespace(
                scan_for_strategies=lambda _top_n: StrategyMarketScan(
                    861,
                    [],
                    candidates,
                    premium,
                    OPEN_TIME + 60_001,
                )
            )
            bot.client = KlineClient()
            scheduler = StrategyScheduler(
                bot.strategies, 96, recorder, bot.logger
            )
            bot.strategy_scheduler = scheduler
            bot.micro_observation_cache = MicroObservationCache(
                boot_id="unused-cache"
            )
            sampler = MicroObservationSampler(boot_id="boot")
            first_identity = sampler.next_identity()
            self.assertTrue(sampler.commit_sample(sampler_rows(
                first_identity,
                base_symbols,
                OPEN_TIME + 1,
                "1000",
            )))
            active_sampler = ActiveSampler(sampler)
            bot.micro_observation_sampler = active_sampler

            original_evaluate = scheduler.evaluate
            worker_commits = []

            def slow_context_then_evaluate(*args, **kwargs):
                # This is the production race window: N20/N16-N19 context
                # work lets the worker publish another rotated cohort after
                # main has prepared its Klines.  The main lease must already
                # be immutable before entering this function.
                identity = sampler.next_identity()
                worker_commits.append(sampler.commit_sample(sampler_rows(
                    identity,
                    worker_symbols,
                    OPEN_TIME + 120_001,
                    "1200",
                )))
                return original_evaluate(*args, **kwargs)

            def reject_micro(_strategy_id, symbol, windows, _checked_at):
                self.assertEqual(set(windows), set(main_symbols))
                return MicroAnalysisResult(
                    "N21",
                    symbol,
                    False,
                    "N21_FLOW_PERSISTENCE_NOT_MET",
                    None,
                    None,
                    {},
                )

            original_freeze = active_sampler.freeze_for_scan

            def freeze_after_non_micro_context(**kwargs):
                self.assertEqual(worker_commits, [True])
                return original_freeze(**kwargs)

            with patch(
                "trading_bot.main.time.time",
                return_value=(OPEN_TIME + 60_100) / 1000,
            ), patch.object(
                scheduler,
                "evaluate",
                side_effect=slow_context_then_evaluate,
            ), patch.object(
                active_sampler,
                "freeze_for_scan",
                side_effect=freeze_after_non_micro_context,
            ), patch(
                "trading_bot.strategy_scheduler.analyze_micro_strategy",
                side_effect=reject_micro,
            ):
                bot._run_once_multi_strategy()

            self.assertEqual(worker_commits, [True])
            self.assertEqual(active_sampler.confirm_calls, 1)
            current_scan = recorder.current_strategy_signal_scan_id()
            self.assertIsInstance(current_scan, int)
            self.assertEqual(len(recorder.list_current_strategy_signals()), 100)
            self.assertEqual(bot.trader.live_calls, 0)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id=? AND reason="
                        "'MICRO_SAMPLER_SNAPSHOT_FAILED'",
                        (current_scan,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_paper_trades"
                    ).fetchone(),
                    (0,),
                )

    def test_n20_audit_failure_aborts_exact_lease_then_next_round_recovers(self):
        symbols = tuple(
            "BTCUSDT" if rank == 1 else f"N{rank:03d}USDT"
            for rank in range(1, 101)
        )
        candidates = [
            FundingCandidate(
                symbol,
                None,
                Decimal("100"),
                quote_volume=Decimal(1_000_000 - rank),
                quote_volume_rank=rank,
                candidate_universe="quote_volume_top",
            )
            for rank, symbol in enumerate(symbols, start=1)
        ]
        premium = {
            symbol: {
                "symbol": symbol,
                "markPrice": "100",
                "indexPrice": "100",
                "lastFundingRate": "0",
            }
            for symbol in symbols
        }

        class ActiveSampler:
            def __init__(self, inner):
                self.inner = inner
                self.abort_calls = 0
                self.confirm_calls = 0

            @property
            def is_running(self):
                return True

            def freeze_for_scan(self, **kwargs):
                return self.inner.freeze_for_scan(**kwargs)

            def confirm(self, lease, *, current_scan_id):
                self.confirm_calls += 1
                return self.inner.confirm(
                    lease, current_scan_id=current_scan_id
                )

            def abort(self, lease=None):
                self.abort_calls += 1
                self.inner.abort(lease)

        class KlineClient(FakeClient):
            def __init__(self):
                super().__init__()
                self.paper_kline_calls = []

            def get_klines(self, symbol):
                self.kline_calls.append(symbol)
                return boundary_kline_rows(OPEN_TIME)

            def get_klines_for_interval(
                self,
                symbol,
                interval,
                limit=1500,
                start_time_ms=None,
            ):
                self.paper_kline_calls.append(
                    (symbol, interval, limit, start_time_ms)
                )
                return [[
                    start_time_ms,
                    "100",
                    "101",
                    "99",
                    "100",
                    "1",
                    start_time_ms + 59_999,
                ]]

        with tempfile.TemporaryDirectory() as tmpdir:
            bot, recorder = self._bot(tmpdir, [], [])
            strategies = (N20_STRATEGY, N21_STRATEGY)
            recorder.upsert_strategy_definitions(strategies)
            observed_at = [OPEN_TIME + 60_001]
            bot.monitor = SimpleNamespace(
                scan_for_strategies=lambda _top_n: StrategyMarketScan(
                    861,
                    [],
                    candidates,
                    premium,
                    observed_at[0],
                )
            )
            bot.client = KlineClient()
            bot.strategies = strategies
            scheduler = StrategyScheduler(
                strategies, 96, recorder, bot.logger
            )
            bot.strategy_scheduler = scheduler
            bot.micro_observation_cache = MicroObservationCache(
                boot_id="unused-cache"
            )
            sampler = MicroObservationSampler(boot_id="n20-recovery")
            initial_identity = sampler.next_identity()
            initial = authenticated_top100_observations({
                symbol: observation(
                    symbol,
                    initial_identity.sample_ordinal,
                    initial_identity.generation,
                    OPEN_TIME + 1,
                    quote="1000",
                    trades=100,
                    taker="500",
                    rank=rank,
                )
                for rank, symbol in enumerate(symbols, start=1)
            })
            initial = {
                symbol: replace(
                    item,
                    boot_id=initial_identity.boot_id,
                )
                for symbol, item in initial.items()
            }
            # Re-hash after binding the sampler boot identity.
            initial = authenticated_top100_observations(initial)
            self.assertTrue(sampler.commit_sample(initial))
            active_sampler = ActiveSampler(sampler)
            bot.micro_observation_sampler = active_sampler

            def reject_micro(_strategy_id, symbol, _windows, _checked_at):
                return MicroAnalysisResult(
                    "N21",
                    symbol,
                    False,
                    "N21_FLOW_PERSISTENCE_NOT_MET",
                    None,
                    None,
                    {},
                )

            # Establish the production shape first: an existing complete
            # CURRENT must survive the next failed round byte-for-byte.
            with patch(
                "trading_bot.main.time.time",
                return_value=observed_at[0] / 1000,
            ), patch(
                "trading_bot.strategy_scheduler.analyze_micro_strategy",
                side_effect=reject_micro,
            ):
                bot._run_once_multi_strategy()
            old_current_scan = recorder.current_strategy_signal_scan_id()
            self.assertIsInstance(old_current_scan, int)
            self.assertEqual(len(recorder.list_current_strategy_signals()), 200)
            self.assertEqual(active_sampler.confirm_calls, 1)
            with recorder._read_only_runtime_snapshot() as connection:
                old_current_rows = tuple(connection.execute(
                    "SELECT * FROM strategy_signals WHERE scan_id=? "
                    "ORDER BY id",
                    (old_current_scan,),
                ).fetchall())
                self.assertEqual(len(old_current_rows), 200)
                old_first_id = old_current_rows[0][0]
                old_last_id = old_current_rows[-1][0]
                self.assertEqual(
                    connection.execute(
                        "SELECT state, recorded_count, expected_count "
                        "FROM strategy_signal_batches WHERE scan_id=?",
                        (old_current_scan,),
                    ).fetchone(),
                    ("CURRENT", 200, 200),
                )

            paper_trade_id = recorder.open_strategy_paper_trade(
                strategy_id="N21",
                symbol="PAPERUSDT",
                entry_price="100",
                stop_loss_price="95",
                take_profit_price="105",
                funding_rate="",
                orders={},
                detail={"fixture": "publication-gated-paper-check"},
                opened_at="2026-01-01T00:00:00+00:00",
            )
            self.assertIsInstance(paper_trade_id, int)
            original_last_checked = "2026-01-01T00:01:00+00:00"
            self.assertTrue(recorder.update_strategy_paper_last_checked(
                paper_trade_id,
                original_last_checked,
            ))
            sync_calls_before_failure = bot.trader.sync_calls

            observed_at[0] = OPEN_TIME + 120_001
            with patch(
                "trading_bot.main.time.time",
                return_value=observed_at[0] / 1000,
            ), patch.object(
                scheduler,
                "_build_n20_batch_context",
                return_value=N20BatchContext(
                    None,
                    False,
                    "N20_MARKET_CONTEXT_INSUFFICIENT",
                ),
            ), patch(
                "trading_bot.strategy_scheduler.analyze_micro_strategy",
                side_effect=reject_micro,
            ):
                bot._run_once_multi_strategy()
            self.assertEqual(
                recorder.current_strategy_signal_scan_id(),
                old_current_scan,
            )
            self.assertEqual(active_sampler.abort_calls, 1)
            self.assertEqual(active_sampler.confirm_calls, 1)
            self.assertEqual(sampler.frame_count, 0)
            self.assertEqual(bot.client.paper_kline_calls, [])
            self.assertEqual(
                bot.trader.sync_calls,
                sync_calls_before_failure,
            )
            failed_paper = recorder.get_open_strategy_paper_trade("N21")
            self.assertIsNotNone(failed_paper)
            self.assertEqual(failed_paper.result, "OPEN")
            self.assertEqual(
                failed_paper.last_checked_at,
                original_last_checked,
            )
            with recorder._read_only_runtime_snapshot() as connection:
                retained_rows = tuple(connection.execute(
                    "SELECT * FROM strategy_signals WHERE scan_id=? "
                    "ORDER BY id",
                    (old_current_scan,),
                ).fetchall())
                self.assertEqual(retained_rows, old_current_rows)
                self.assertEqual(
                    (retained_rows[0][0], retained_rows[-1][0]),
                    (old_first_id, old_last_id),
                )
                failed_scan = connection.execute(
                    "SELECT scan_id FROM strategy_signal_batches "
                    "WHERE state='STAGING'"
                ).fetchone()
                self.assertIsNotNone(failed_scan)
                failed_scan_id = failed_scan[0]
                self.assertEqual(
                    connection.execute(
                        "SELECT state, recorded_count, expected_count "
                        "FROM strategy_signal_batches ORDER BY scan_id"
                    ).fetchall(),
                    [("CURRENT", 200, 200), ("STAGING", 200, None)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_paper_trades "
                        "WHERE result != 'OPEN'"
                    ).fetchone(),
                    (0,),
                )

            observed_at[0] = OPEN_TIME + 180_001
            with patch(
                "trading_bot.main.time.time",
                return_value=observed_at[0] / 1000,
            ), patch(
                "trading_bot.strategy_scheduler.analyze_micro_strategy",
                side_effect=reject_micro,
            ):
                bot._run_once_multi_strategy()
            current_scan = recorder.current_strategy_signal_scan_id()
            self.assertIsInstance(current_scan, int)
            self.assertNotEqual(current_scan, old_current_scan)
            self.assertEqual(active_sampler.abort_calls, 1)
            self.assertEqual(active_sampler.confirm_calls, 2)
            self.assertEqual(len(recorder.list_current_strategy_signals()), 200)
            self.assertEqual(bot.trader.live_calls, 0)
            self.assertEqual(
                bot.trader.sync_calls,
                sync_calls_before_failure + 1,
            )
            self.assertEqual(len(bot.client.paper_kline_calls), 1)
            recovered_paper = recorder.get_open_strategy_paper_trade("N21")
            self.assertIsNotNone(recovered_paper)
            self.assertEqual(recovered_paper.result, "OPEN")
            self.assertGreater(
                datetime.fromisoformat(recovered_paper.last_checked_at),
                datetime.fromisoformat(original_last_checked),
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signal_batches "
                        "WHERE state='STAGING'"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signal_batches "
                        "WHERE scan_id IN (?, ?)",
                        (old_current_scan, failed_scan_id),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id IN (?, ?)",
                        (old_current_scan, failed_scan_id),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT state, recorded_count, expected_count "
                        "FROM strategy_signal_batches WHERE scan_id=?",
                        (current_scan,),
                    ).fetchone(),
                    ("CURRENT", 200, 200),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_paper_trades "
                        "WHERE result='OPEN'"
                    ).fetchone(),
                    (1,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id=? AND reason IN "
                        "('N20_MARKET_CONTEXT_INSUFFICIENT',"
                        "'MICRO_SAMPLER_SNAPSHOT_FAILED')",
                        (current_scan,),
                    ).fetchone(),
                    (0,),
                )

    def test_micro_sampler_collects_one_exact_read_only_top100_generation(self):
        symbols = tuple(f"Q{rank:03d}USDT" for rank in range(1, 101))
        candidates = [
            FundingCandidate(
                symbol,
                None,
                Decimal("100"),
                quote_volume=Decimal(1_000_000 - rank),
                quote_volume_rank=rank,
                candidate_universe="quote_volume_top",
            )
            for rank, symbol in enumerate(symbols, start=1)
        ]
        premium = {
            symbol: {
                "symbol": symbol,
                "markPrice": "100",
                "indexPrice": "100",
                "lastFundingRate": "0",
            }
            for symbol in symbols
        }

        class PublicKlineClient:
            def __init__(self):
                self.calls = []

            def get_klines(self, symbol):
                self.calls.append(symbol)
                return boundary_kline_rows(OPEN_TIME)

        bot = TradingBot.__new__(TradingBot)
        scan_calls = []
        bot._micro_sampler_monitor = SimpleNamespace(
            scan_for_strategies=lambda top_n: (
                scan_calls.append(top_n)
                or StrategyMarketScan(
                    854,
                    [],
                    candidates,
                    premium,
                    OPEN_TIME + 180_000,
                )
            )
        )
        bot._micro_sampler_client = PublicKlineClient()
        sampler = MicroObservationSampler(boot_id="public-generation")
        with patch(
            "trading_bot.main.time.time",
            return_value=(OPEN_TIME + 180_100) / 1000,
        ):
            observations = bot._collect_micro_observation_sample(
                sampler.next_identity(),
                threading.Event(),
            )
        self.assertEqual(scan_calls, [100])
        self.assertEqual(len(observations), 100)
        self.assertEqual(len(bot._micro_sampler_client.calls), 100)
        self.assertEqual(set(bot._micro_sampler_client.calls), set(symbols))
        self.assertEqual(
            {item.quote_volume_rank for item in observations.values()},
            set(range(1, 101)),
        )
        self.assertEqual(
            len({item.universe_sha256 for item in observations.values()}),
            1,
        )

    def test_micro_sampler_kline_collection_is_bounded_and_concurrent(self):
        symbols = tuple(f"Q{rank:03d}USDT" for rank in range(1, 101))
        candidates = [
            FundingCandidate(
                symbol,
                None,
                Decimal("100"),
                quote_volume=Decimal(1_000_000 - rank),
                quote_volume_rank=rank,
                candidate_universe="quote_volume_top",
            )
            for rank, symbol in enumerate(symbols, start=1)
        ]
        premium = {
            symbol: {
                "symbol": symbol,
                "markPrice": "100",
                "indexPrice": "100",
                "lastFundingRate": "0",
            }
            for symbol in symbols
        }
        active = 0
        maximum_active = 0
        calls = []
        lock = threading.Lock()
        concurrent = threading.Event()
        release = threading.Event()

        class BlockingPublicKlineClient:
            def get_klines(self, symbol):
                nonlocal active, maximum_active
                with lock:
                    calls.append(symbol)
                    active += 1
                    maximum_active = max(maximum_active, active)
                    if active >= 4:
                        concurrent.set()
                try:
                    if not release.wait(5):
                        raise RuntimeError("blocked Kline fixture timed out")
                    return boundary_kline_rows(OPEN_TIME)
                finally:
                    with lock:
                        active -= 1

        bot = TradingBot.__new__(TradingBot)
        bot._micro_sampler_monitor = SimpleNamespace(
            scan_for_strategies=lambda _top_n: StrategyMarketScan(
                854,
                [],
                candidates,
                premium,
                OPEN_TIME + 180_000,
            )
        )
        bot._micro_sampler_client = BlockingPublicKlineClient()
        sampler = MicroObservationSampler(boot_id="bounded-concurrent")
        result = []
        errors = []

        def collect():
            try:
                result.append(bot._collect_micro_observation_sample(
                    sampler.next_identity(),
                    threading.Event(),
                ))
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        with patch(
            "trading_bot.main.time.time",
            return_value=(OPEN_TIME + 180_100) / 1000,
        ):
            worker = threading.Thread(target=collect)
            worker.start()
            try:
                self.assertTrue(
                    concurrent.wait(2),
                    "Top100 Klines were still collected serially",
                )
            finally:
                release.set()
                worker.join(10)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 100)
        self.assertEqual(set(calls), set(symbols))
        self.assertEqual(len(calls), 100)
        self.assertGreaterEqual(maximum_active, 4)
        self.assertLessEqual(maximum_active, 10)

    def test_micro_sampler_rotation_collects_one_bounded_prior_cohort(self):
        original = tuple(f"Q{rank:03d}USDT" for rank in range(1, 101))
        rotated = (*original[:-1], "NEWUSDT")
        current_symbols = [original]
        observed_at_ms = [OPEN_TIME + 60_000]
        ordinal = [1]

        def candidates():
            return [
                FundingCandidate(
                    symbol,
                    None,
                    Decimal("100"),
                    quote_volume=Decimal(1_000_000 - rank),
                    quote_volume_rank=rank,
                    candidate_universe="quote_volume_top",
                )
                for rank, symbol in enumerate(
                    current_symbols[0], start=1
                )
            ]

        premium = {
            symbol: {
                "symbol": symbol,
                "markPrice": "100",
                "indexPrice": "100",
                "lastFundingRate": "0",
            }
            for symbol in set(original) | set(rotated)
        }
        calls = []

        class PublicKlineClient:
            def get_klines(self, symbol):
                calls.append((ordinal[0], symbol))
                rows = boundary_kline_rows(OPEN_TIME)
                rows[-1][4] = str(Decimal("100") + Decimal(ordinal[0]) / 100)
                rows[-1][5] = str(1 + ordinal[0])
                rows[-1][7] = str(100 + ordinal[0] * 10)
                rows[-1][8] = 10 + ordinal[0]
                rows[-1][10] = str(55 + ordinal[0] * 5)
                return rows

        bot = TradingBot.__new__(TradingBot)
        bot._micro_sampler_monitor = SimpleNamespace(
            scan_for_strategies=lambda _top_n: StrategyMarketScan(
                854,
                [],
                candidates(),
                premium,
                observed_at_ms[0],
            )
        )
        bot._micro_sampler_client = PublicKlineClient()
        sampler = MicroObservationSampler(boot_id="rotation-cohort")
        with patch(
            "trading_bot.main.time.time",
            side_effect=lambda: observed_at_ms[0] / 1000,
        ):
            first = bot._collect_micro_observation_sample(
                sampler.next_identity(), threading.Event()
            )
            self.assertTrue(sampler.commit_sample(first))
            current_symbols[0] = rotated
            observed_at_ms[0] = OPEN_TIME + 120_000
            ordinal[0] = 2
            second = bot._collect_micro_observation_sample(
                sampler.next_identity(), threading.Event()
            )
            self.assertEqual(len(second), 100)
            self.assertEqual(len(second.continuation_observations), 100)
            self.assertTrue(sampler.commit_sample(second))

        second_calls = {
            symbol for sample_ordinal, symbol in calls
            if sample_ordinal == 2
        }
        self.assertEqual(len(second_calls), 101)
        self.assertIn(original[-1], second_calls)
        self.assertIn("NEWUSDT", second_calls)
        self.assertEqual(sampler.n22_market_context_entry_count, 100)

    def test_micro_sampler_optional_departed_member_failures_advance_current_frame(self):
        original = tuple(f"Q{rank:03d}USDT" for rank in range(1, 101))
        departed = original[-1]
        rotated = (*original[:-1], "NEWUSDT")

        for failure_mode in ("premium", "kline", "budget"):
            with self.subTest(failure_mode=failure_mode):
                current_symbols = [original]
                ordinal = [1]
                observed_at_ms = [OPEN_TIME + 60_000]
                calls = []

                def candidates():
                    return [
                        FundingCandidate(
                            symbol,
                            None,
                            Decimal("100"),
                            quote_volume=Decimal(1_000_000 - rank),
                            quote_volume_rank=rank,
                            candidate_universe="quote_volume_top",
                        )
                        for rank, symbol in enumerate(
                            current_symbols[0], start=1
                        )
                    ]

                premium = {
                    symbol: {
                        "symbol": symbol,
                        "markPrice": "100",
                        "indexPrice": "100",
                        "lastFundingRate": "0",
                    }
                    for symbol in set(original) | set(rotated)
                }

                class OptionalFailureClient:
                    def get_klines(self, symbol):
                        calls.append((ordinal[0], symbol))
                        if (
                            failure_mode == "kline"
                            and ordinal[0] == 5
                            and symbol == departed
                        ):
                            raise BinanceAPIError(
                                "injected departed Kline failure"
                            )
                        rows = boundary_kline_rows(OPEN_TIME)
                        rows[-1][4] = str(
                            Decimal("100")
                            + Decimal(ordinal[0]) / Decimal("100")
                        )
                        rows[-1][5] = str(1 + ordinal[0])
                        rows[-1][7] = str(100 + ordinal[0] * 10)
                        rows[-1][8] = 10 + ordinal[0]
                        rows[-1][10] = str(55 + ordinal[0] * 5)
                        return rows

                bot = TradingBot.__new__(TradingBot)
                bot.logger = logging.getLogger(
                    f"micro-optional-{failure_mode}"
                )
                bot._micro_sampler_monitor = SimpleNamespace(
                    scan_for_strategies=lambda _top_n: StrategyMarketScan(
                        854,
                        [],
                        candidates(),
                        premium,
                        observed_at_ms[0],
                    )
                )
                bot._micro_sampler_client = OptionalFailureClient()
                sampler = MicroObservationSampler(
                    boot_id=f"optional-{failure_mode}"
                )

                with patch(
                    "trading_bot.main.time.time",
                    side_effect=lambda: observed_at_ms[0] / 1000,
                ):
                    for value in range(1, 5):
                        ordinal[0] = value
                        observed_at_ms[0] = OPEN_TIME + value * 60_000
                        sample = bot._collect_micro_observation_sample(
                            sampler.next_identity(), threading.Event()
                        )
                        self.assertTrue(sampler.commit_sample(sample))

                    current_symbols[0] = rotated
                    ordinal[0] = 5
                    observed_at_ms[0] = OPEN_TIME + 300_000
                    if failure_mode == "premium":
                        premium.pop(departed)

                    original_fetch = bot._fetch_micro_kline_generation

                    def fetch_with_optional_budget(
                        symbols, stop_event, deadline_monotonic
                    ):
                        if (
                            failure_mode == "budget"
                            and tuple(symbols) == (departed,)
                        ):
                            raise MicroObservationError(
                                "micro Kline generation exceeded its budget"
                            )
                        return original_fetch(
                            symbols, stop_event, deadline_monotonic
                        )

                    with patch.object(
                        bot,
                        "_fetch_micro_kline_generation",
                        side_effect=fetch_with_optional_budget,
                    ), self.assertLogs(
                        bot.logger, level="WARNING"
                    ) as warning_logs:
                        degraded = bot._collect_micro_observation_sample(
                            sampler.next_identity(), threading.Event()
                        )
                    self.assertEqual(len(degraded), 100)
                    self.assertEqual(
                        len(degraded.continuation_observations), 0
                    )
                    self.assertTrue(any(
                        "N22 continuation unavailable" in row
                        for row in warning_logs.output
                    ))
                    self.assertTrue(sampler.commit_sample(degraded))

                    fifth_calls = tuple(
                        symbol for sample_ordinal, symbol in calls
                        if sample_ordinal == 5
                    )
                    self.assertEqual(len(set(fifth_calls)), 100 if failure_mode in {
                        "premium", "budget"
                    } else 101)
                    self.assertEqual(
                        fifth_calls.count(departed),
                        0 if failure_mode in {"premium", "budget"} else 1,
                    )

                    self.assertEqual(sampler.generation, 5)
                    self.assertEqual(sampler.frame_count, 4)
                    self.assertLessEqual(
                        sum(
                            len(window.observations)
                            for window in sampler.snapshot().values()
                        ),
                        400,
                    )
                    self.assertLessEqual(
                        sampler.n22_market_context_entry_count, 400
                    )
                    self.assertEqual(
                        sampler.next_identity().continuation_ranked_symbols,
                        tuple((symbol, rank) for rank, symbol in enumerate(
                            rotated, start=1
                        )),
                    )
                    self.assertNotIn(
                        departed,
                        dict(
                            sampler.next_identity()
                            .continuation_ranked_symbols
                        ),
                    )

                    degraded_lease = sampler.freeze_for_scan(
                        scan_id=905,
                        current_symbols=rotated,
                        fallback_observations={},
                        captured_at_ms=observed_at_ms[0] + 100,
                    )
                    generation_four = next(
                        item
                        for item in degraded_lease.windows[rotated[0]].observations
                        if item.generation == 4
                    )
                    self.assertIsNone(
                        degraded_lease.windows.n22_market_context(
                            4,
                            5,
                            generation_four.universe_sha256,
                        )
                    )
                    self.assertEqual(
                        analyze_n22(
                            degraded_lease.windows[rotated[0]],
                            degraded_lease.windows,
                            degraded_lease.captured_at_ms,
                        ).reason,
                        "N22_MARKET_CONTEXT_WARMING",
                    )
                    self.assertNotEqual(
                        analyze_n25(
                            degraded_lease.windows[rotated[0]],
                            degraded_lease.windows,
                            degraded_lease.captured_at_ms,
                        ).reason,
                        "N25_MARKET_CONTEXT_INSUFFICIENT",
                    )
                    self.assertTrue(sampler.confirm(
                        degraded_lease, current_scan_id=905
                    ))

                    premium[departed] = {
                        "symbol": departed,
                        "markPrice": "100",
                        "indexPrice": "100",
                        "lastFundingRate": "0",
                    }
                    ordinal[0] = 6
                    observed_at_ms[0] = OPEN_TIME + 360_000
                    recovered = bot._collect_micro_observation_sample(
                        sampler.next_identity(), threading.Event()
                    )
                    self.assertTrue(sampler.commit_sample(recovered))

                recovered_lease = sampler.freeze_for_scan(
                    scan_id=906,
                    current_symbols=rotated,
                    fallback_observations={},
                    captured_at_ms=observed_at_ms[0] + 100,
                )
                self.assertNotEqual(
                    analyze_n22(
                        recovered_lease.windows[rotated[0]],
                        recovered_lease.windows,
                        recovered_lease.captured_at_ms,
                    ).reason,
                    "N22_MARKET_CONTEXT_WARMING",
                )
                self.assertTrue(sampler.confirm(
                    recovered_lease, current_scan_id=906
                ))
                self.assertFalse(any(
                    thread.name.startswith("micro-kline")
                    for thread in threading.enumerate()
                ))

    def test_micro_sampler_rotated_current_member_failure_remains_fail_closed(self):
        original = tuple(f"Q{rank:03d}USDT" for rank in range(1, 101))
        rotated = (*original[:-1], "NEWUSDT")
        current_symbols = [original]

        def candidates():
            return [
                FundingCandidate(
                    symbol,
                    None,
                    Decimal("100"),
                    quote_volume=Decimal(1_000_000 - rank),
                    quote_volume_rank=rank,
                    candidate_universe="quote_volume_top",
                )
                for rank, symbol in enumerate(current_symbols[0], start=1)
            ]

        premium = {
            symbol: {
                "symbol": symbol,
                "markPrice": "100",
                "indexPrice": "100",
                "lastFundingRate": "0",
            }
            for symbol in set(original) | set(rotated)
        }

        class CurrentFailureClient:
            fail_current = False

            def get_klines(self, symbol):
                if self.fail_current and symbol == "NEWUSDT":
                    raise BinanceAPIError("injected current member failure")
                return boundary_kline_rows(OPEN_TIME)

        bot = TradingBot.__new__(TradingBot)
        bot._micro_sampler_monitor = SimpleNamespace(
            scan_for_strategies=lambda _top_n: StrategyMarketScan(
                854, [], candidates(), premium, OPEN_TIME + 60_000
            )
        )
        bot._micro_sampler_client = CurrentFailureClient()
        sampler = MicroObservationSampler(boot_id="current-failure")
        with patch(
            "trading_bot.main.time.time",
            return_value=(OPEN_TIME + 60_100) / 1000,
        ):
            first = bot._collect_micro_observation_sample(
                sampler.next_identity(), threading.Event()
            )
            self.assertTrue(sampler.commit_sample(first))
            current_symbols[0] = rotated
            bot._micro_sampler_client.fail_current = True
            with self.assertRaisesRegex(
                MicroObservationError,
                "generation request failed",
            ):
                bot._collect_micro_observation_sample(
                    sampler.next_identity(), threading.Event()
                )
        self.assertEqual(sampler.generation, 1)
        self.assertEqual(sampler.frame_count, 1)
        self.assertFalse(any(
            thread.name.startswith("micro-kline")
            for thread in threading.enumerate()
        ))

    def test_micro_sampler_completes_while_main_scan_uses_shared_network_capacity(self):
        symbols = tuple(f"Q{rank:03d}USDT" for rank in range(1, 101))
        candidates = [
            FundingCandidate(
                symbol,
                None,
                Decimal("100"),
                quote_volume=Decimal(1_000_000 - rank),
                quote_volume_rank=rank,
                candidate_universe="quote_volume_top",
            )
            for rank, symbol in enumerate(symbols, start=1)
        ]
        premium = {
            symbol: {
                "symbol": symbol,
                "markPrice": "100",
                "indexPrice": "100",
                "lastFundingRate": "0",
            }
            for symbol in symbols
        }
        lock = threading.Lock()
        active = 0
        maximum_active = 0
        sampler_calls = []
        main_calls = []
        main_results = []
        main_started = threading.Event()

        def bounded_request(symbol, calls):
            nonlocal active, maximum_active
            with lock:
                calls.append(symbol)
                active += 1
                maximum_active = max(maximum_active, active)
            try:
                time.sleep(0.003)
                return boundary_kline_rows(OPEN_TIME)
            finally:
                with lock:
                    active -= 1

        class SharedCapacityClient:
            def get_klines(self, symbol):
                return bounded_request(symbol, sampler_calls)

        class MainCapacityClient:
            def get_klines(self, symbol):
                return bounded_request(symbol, main_calls)

        def run_main_scan():
            main_started.set()
            rows, observed_at, failures = (
                bot._fetch_strategy_kline_generation(symbols)
            )
            main_results.append((rows, observed_at, failures))

        bot = TradingBot.__new__(TradingBot)
        bot._micro_sampler_monitor = SimpleNamespace(
            scan_for_strategies=lambda _top_n: StrategyMarketScan(
                854,
                [],
                candidates,
                premium,
                OPEN_TIME + 180_000,
            )
        )
        bot._micro_sampler_client = SharedCapacityClient()
        bot.client = MainCapacityClient()
        bot._kline_request_slots = threading.BoundedSemaphore(4)
        sampler = MicroObservationSampler(boot_id="shared-capacity")
        main_worker = threading.Thread(target=run_main_scan)
        main_worker.start()
        self.assertTrue(main_started.wait(1))
        with patch(
            "trading_bot.main.time.time",
            return_value=(OPEN_TIME + 180_100) / 1000,
        ):
            observations = bot._collect_micro_observation_sample(
                sampler.next_identity(),
                threading.Event(),
            )
        main_worker.join(5)

        self.assertFalse(main_worker.is_alive())
        self.assertEqual(len(main_results), 1)
        self.assertEqual(set(main_results[0][0]), set(symbols))
        self.assertEqual(set(main_results[0][1]), set(symbols))
        self.assertEqual(main_results[0][2], ())
        self.assertEqual(len(observations), 100)
        self.assertEqual(set(sampler_calls), set(symbols))
        self.assertEqual(set(main_calls), set(symbols))
        self.assertGreaterEqual(maximum_active, 2)
        self.assertLessEqual(maximum_active, 4)
        self.assertFalse(any(
            thread.name.startswith("micro-kline")
            for thread in threading.enumerate()
        ))

    def test_micro_sampler_real_generations_warm_four_point_windows(self):
        symbols = tuple(f"Q{rank:03d}USDT" for rank in range(1, 101))
        candidates = [
            FundingCandidate(
                symbol,
                None,
                Decimal("100"),
                quote_volume=Decimal(1_000_000 - rank),
                quote_volume_rank=rank,
                candidate_universe="quote_volume_top",
            )
            for rank, symbol in enumerate(symbols, start=1)
        ]
        current_time_ms = [OPEN_TIME + 60_000]
        ordinal = [1]

        def market_scan(_top_n):
            premium = {
                symbol: {
                    "symbol": symbol,
                    "markPrice": "100",
                    "indexPrice": "100",
                    "lastFundingRate": "0",
                }
                for symbol in symbols
            }
            return StrategyMarketScan(
                854,
                [],
                candidates,
                premium,
                current_time_ms[0],
            )

        class AdvancingPublicKlineClient:
            def get_klines(self, _symbol):
                rows = boundary_kline_rows(OPEN_TIME)
                rows[-1][4] = str(Decimal("100.5") + Decimal(ordinal[0]) / 100)
                rows[-1][5] = str(1 + ordinal[0])
                rows[-1][7] = str(100 + ordinal[0] * 10)
                rows[-1][8] = 10 + ordinal[0]
                rows[-1][10] = str(55 + ordinal[0] * 5)
                return rows

        bot = TradingBot.__new__(TradingBot)
        bot._micro_sampler_monitor = SimpleNamespace(
            scan_for_strategies=market_scan
        )
        bot._micro_sampler_client = AdvancingPublicKlineClient()
        sampler = MicroObservationSampler(boot_id="real-warmup")
        with patch(
            "trading_bot.main.time.time",
            side_effect=lambda: current_time_ms[0] / 1000,
        ):
            for value in range(1, 5):
                ordinal[0] = value
                current_time_ms[0] = OPEN_TIME + value * 60_000
                observations = bot._collect_micro_observation_sample(
                    sampler.next_identity(),
                    threading.Event(),
                )
                self.assertTrue(sampler.commit_sample(observations))

        self.assertEqual(len(sampler.snapshot()), 100)
        self.assertEqual(
            {len(window.observations) for window in sampler.snapshot().values()},
            {4},
        )
        self.assertTrue(all(
            45_000 <= increment.interval_ms <= 150_000
            for window in sampler.snapshot().values()
            for increment in window.increments
        ))

    def test_micro_sampler_request_failure_preserves_last_good_and_recovers(self):
        symbols = tuple(f"Q{rank:03d}USDT" for rank in range(1, 101))
        candidates = [
            FundingCandidate(
                symbol,
                None,
                Decimal("100"),
                quote_volume=Decimal(1_000_000 - rank),
                quote_volume_rank=rank,
                candidate_universe="quote_volume_top",
            )
            for rank, symbol in enumerate(symbols, start=1)
        ]
        premium = {
            symbol: {
                "symbol": symbol,
                "markPrice": "100",
                "indexPrice": "100",
                "lastFundingRate": "0",
            }
            for symbol in symbols
        }

        class RecoveringClient:
            fail_symbol = None

            def get_klines(self, symbol):
                if symbol == self.fail_symbol:
                    raise BinanceAPIError("injected public Kline failure")
                return boundary_kline_rows(OPEN_TIME)

        bot = TradingBot.__new__(TradingBot)
        bot._micro_sampler_monitor = SimpleNamespace(
            scan_for_strategies=lambda _top_n: StrategyMarketScan(
                854,
                [],
                candidates,
                premium,
                OPEN_TIME + 60_000,
            )
        )
        bot._micro_sampler_client = RecoveringClient()
        sampler = MicroObservationSampler(boot_id="network-recovery")
        with patch(
            "trading_bot.main.time.time",
            return_value=(OPEN_TIME + 60_100) / 1000,
        ):
            first = bot._collect_micro_observation_sample(
                sampler.next_identity(),
                threading.Event(),
            )
            self.assertTrue(sampler.commit_sample(first))
            bot._micro_sampler_client.fail_symbol = symbols[49]
            with self.assertRaisesRegex(
                MicroObservationError,
                "generation request failed",
            ):
                bot._collect_micro_observation_sample(
                    sampler.next_identity(),
                    threading.Event(),
                )
            sampler.mark_failed("public Kline failure")
            last_good = sampler.snapshot()
            self.assertEqual(len(last_good), 100)
            self.assertEqual(
                {
                    window.observations[-1].generation
                    for window in last_good.values()
                },
                {1},
            )
            bot._micro_sampler_client.fail_symbol = None
            recovered = bot._collect_micro_observation_sample(
                sampler.next_identity(),
                threading.Event(),
            )
            self.assertTrue(sampler.commit_sample(recovered))

        self.assertEqual(len(sampler.snapshot()), 100)
        self.assertFalse(any(
            thread.name.startswith("micro-kline")
            for thread in threading.enumerate()
        ))

    def test_micro_sampler_hard_deadline_cancels_pending_requests(self):
        symbols = tuple(f"Q{rank:03d}USDT" for rank in range(1, 101))
        candidates = [
            FundingCandidate(
                symbol,
                None,
                Decimal("100"),
                quote_volume=Decimal(1_000_000 - rank),
                quote_volume_rank=rank,
                candidate_universe="quote_volume_top",
            )
            for rank, symbol in enumerate(symbols, start=1)
        ]
        premium = {
            symbol: {
                "symbol": symbol,
                "markPrice": "100",
                "indexPrice": "100",
                "lastFundingRate": "0",
            }
            for symbol in symbols
        }
        calls = []

        class SlowClient:
            def get_klines(self, symbol):
                calls.append(symbol)
                time.sleep(0.1)
                return boundary_kline_rows(OPEN_TIME)

        bot = TradingBot.__new__(TradingBot)
        bot._micro_sampler_monitor = SimpleNamespace(
            scan_for_strategies=lambda _top_n: StrategyMarketScan(
                854,
                [],
                candidates,
                premium,
                OPEN_TIME + 60_000,
            )
        )
        bot._micro_sampler_client = SlowClient()
        sampler = MicroObservationSampler(boot_id="hard-deadline")
        started = time.monotonic()
        with patch(
            "trading_bot.main._MICRO_SAMPLE_COLLECTION_BUDGET_SECONDS",
            0.02,
        ), patch(
            "trading_bot.main.time.time",
            return_value=(OPEN_TIME + 60_100) / 1000,
        ):
            with self.assertRaisesRegex(
                MicroObservationError,
                "exceeded its budget",
            ):
                bot._collect_micro_observation_sample(
                    sampler.next_identity(),
                    threading.Event(),
                )
        self.assertLess(time.monotonic() - started, 1)
        self.assertLessEqual(len(calls), 10)
        self.assertFalse(any(
            thread.name.startswith("micro-kline")
            for thread in threading.enumerate()
        ))

    def _closed_live_bot(self, tmpdir, resolution):
        n01 = load_first_stage_strategies()[0]
        recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("test_live_close"))
        recorder.upsert_strategy_definitions((n01,))
        with recorder._connect() as connection:
            connection.execute(
                """
                UPDATE strategy_states
                SET consecutive_wins = 2, paper_trade_count = 2, win_count = 2,
                    win_rate = '1', live_eligible = 1
                WHERE strategy_id = 'N01'
                """
            )
        state = PositionState(
            symbol="LIVEUSDT",
            quantity="10",
            entry_price="100",
            stop_loss_price="95",
            take_profit_price="125",
            leverage=10,
            opened_at="2026-07-10T10:00:00+00:00",
            dry_run=False,
            orders={
                "plan": {"risk_amount": "10"},
                "open": {
                    "orderId": 10,
                    "clientOrderId": "mkt-live",
                    "symbol": "LIVEUSDT",
                    "side": "BUY",
                    "type": "MARKET",
                    "status": "FILLED",
                    "executedQty": "10",
                },
                "stop": {
                    "algoId": 11,
                    "clientAlgoId": "sl-live",
                    "symbol": "LIVEUSDT",
                    "algoType": "CONDITIONAL",
                    "side": "SELL",
                    "orderType": "STOP_MARKET",
                    "closePosition": True,
                    "algoStatus": "NEW",
                },
                "take_profit": {
                    "algoId": 22,
                    "clientAlgoId": "tp-live",
                    "symbol": "LIVEUSDT",
                    "algoType": "CONDITIONAL",
                    "side": "SELL",
                    "orderType": "TAKE_PROFIT_MARKET",
                    "closePosition": True,
                    "algoStatus": "NEW",
                },
                "strategy": {"strategy_id": "N01"},
            },
        )
        scan_id = recorder.begin_scan(0, [], dry_run=False)
        trade_id = recorder.record_trade_open(scan_id, state)
        recorder.record_strategy_live_open("N01", trade_id, state.symbol, state.opened_at)
        state_store = StateStore(str(Path(tmpdir) / "state.json"))
        state_store.save(state)

        bot = TradingBot.__new__(TradingBot)
        bot.config = SimpleNamespace(symbol_cooldown_hours=4)
        bot.logger = logging.getLogger("test_live_close")
        bot.recorder = recorder
        bot.state = state_store
        bot.trader = SimpleNamespace(resolve_closed_live_position=lambda closed_state: resolution)
        return bot, recorder, state

    def test_flat_cleanup_without_live_result_preserves_n25_qualification(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_n25_flat_cleanup"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            with recorder._connect() as connection:
                connection.execute(
                    "UPDATE strategy_states SET consecutive_wins=2, "
                    "paper_trade_count=2,win_count=2,loss_count=0,win_rate='1',"
                    "live_eligible=1,live_result_pending=0 "
                    "WHERE strategy_id='N25'"
                )
            before = recorder.get_strategy_state("N25")
            state_store = StateStore(Path(tmpdir) / "position.json")
            local_state = PositionState(
                symbol="TUSDT", quantity="79486", entry_price="0.003992",
                stop_loss_price="0.0038", take_profit_price="0.004952",
                leverage=10, opened_at="2026-07-19T21:06:08+00:00",
                dry_run=False,
                orders={
                    "strategy": {
                        "strategy_id": "N25", "signal_id": 7761956,
                        "structure_id": "5bfef983dccc6efdc403ef86",
                    },
                    "emergency_cleanup_pending": {
                        "placed_protection_orders": [],
                    },
                },
            )
            state_store.save(local_state)
            bot = TradingBot.__new__(TradingBot)
            bot.logger = logging.getLogger("test_n25_flat_cleanup")
            bot.recorder = recorder
            bot.state = state_store
            bot.paper_trader = SimpleNamespace(
                close_triggered_open_trades=lambda *_args: (_ for _ in ()).throw(
                    AssertionError("paper close must not run during recovery")
                )
            )
            bot.client = SimpleNamespace()
            bot._attested_local_execution_state = lambda: (True, local_state)
            bot._clear_finalized_n16_state_before_reconciliation = lambda _state: False
            bot._repair_n16_live_audit_before_reconciliation = (
                lambda state: (True, state)
            )
            bot._close_dry_run_with_attested_state = lambda _state: None
            bot._sync_with_attested_state = lambda _state: SyncResult(
                has_position=False,
                pending_resolved=True,
                pending_detail={
                    "reason": "EMERGENCY_CLEANUP_CONFIRMED_FLAT_WITHOUT_LIVE_RESULT",
                    "realized_pnl": "-0.079486",
                    "fees": "-0.31726833",
                },
            )
            with patch.object(
                recorder,
                "mark_strategy_live_result_pending",
                side_effect=AssertionError("qualification must not be changed"),
            ):
                self.assertIsNone(
                    bot._reconcile_multi_strategy_after_publication()
                )

            self.assertIsNone(state_store.load())
            self.assertEqual(recorder.get_strategy_state("N25"), before)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_live_links "
                        "WHERE strategy_id='N25'"
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM trade_reviews"
                    ).fetchone()[0],
                    0,
                )

    def test_pre_live_cleanup_rejects_any_active_live_link(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resolution = LiveCloseResolution(
                "LOSS", "STOP_LOSS", Decimal("94.9"), {}
            )
            _bot, recorder, state = self._closed_live_bot(tmpdir, resolution)

            with self.assertRaisesRegex(RuntimeError, "active live link"):
                recorder.assert_no_active_live_link_for_cleanup(
                    "N01", state.symbol
                )

    def test_direct_live_does_not_require_eligibility_or_open_any_paper(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            n01, n02 = load_first_stage_strategies()[:2]
            live_signal = signal(n01, "LIVEUSDT", signal_id=101)
            paper_signal = signal(n02, "PAPERUSDT", signal_id=102)
            live_candidate = LiveTradeCandidate(live_signal)
            bot, recorder = self._bot(tmpdir, [live_signal, paper_signal], [live_candidate])

            bot._run_once_multi_strategy()

            with recorder._connect() as connection:
                paper_rows = connection.execute(
                    "SELECT strategy_id, symbol FROM strategy_paper_trades ORDER BY id"
                ).fetchall()
                live_rows = connection.execute(
                    "SELECT strategy_id, symbol FROM strategy_live_links ORDER BY id"
                ).fetchall()
                live_orders = connection.execute(
                    "SELECT orders_json FROM trade_reviews WHERE status = 'OPENED'"
                ).fetchone()[0]
            state = recorder.get_strategy_state("N01")
            self.assertEqual(paper_rows, [])
            self.assertEqual(live_rows, [("N01", "LIVEUSDT")])
            self.assertEqual(json.loads(live_orders)["strategy"]["signal_id"], 101)
            self.assertEqual(state.paper_trade_count, 0)
            self.assertEqual(state.consecutive_wins, 0)
            self.assertFalse(state.live_eligible)

    def test_micro_cache_cas_false_or_exception_blocks_every_side_effect(self):
        class FailingCache:
            def __init__(self, failure):
                self.boot_id = "test-boot"
                self.generation = 0
                self.failure = failure
                self.clear_calls = 0
                self.commit_calls = 0

            def propose(self, scan_id, observations):
                self.observations = observations
                return SimpleNamespace(windows={})

            def commit(self, proposal, *, current_scan_id):
                self.commit_calls += 1
                if isinstance(self.failure, Exception):
                    raise self.failure
                return self.failure

            def clear(self):
                self.clear_calls += 1

        for failure in (False, RuntimeError("cache commit acknowledgement lost")):
            with self.subTest(failure=type(failure).__name__), tempfile.TemporaryDirectory() as tmpdir:
                selected = signal(N21_STRATEGY, "M000USDT", signal_id=301)
                live_candidate = LiveTradeCandidate(
                    selected, eligible_state("N21")
                )
                bot, recorder = self._bot(
                    tmpdir, [selected], [live_candidate]
                )
                volume_candidates = [
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
                premium_rows = {
                    item.symbol: {
                        "symbol": item.symbol,
                        "markPrice": "100",
                        "indexPrice": "100",
                        "lastFundingRate": "0",
                    }
                    for item in volume_candidates
                }
                bot.monitor = SimpleNamespace(
                    scan_for_strategies=lambda _top_n: StrategyMarketScan(
                        100,
                        [],
                        volume_candidates,
                        premium_rows,
                        1,
                    )
                )
                cache = FailingCache(failure)
                bot.micro_observation_cache = cache
                bot.strategies = (N21_STRATEGY,)
                built = SimpleNamespace(symbol="sentinel")
                with patch(
                    "trading_bot.main.build_micro_observation",
                    return_value=built,
                ):
                    bot._run_once_multi_strategy()

                self.assertEqual(cache.commit_calls, 1)
                self.assertGreaterEqual(cache.clear_calls, 1)
                self.assertEqual(len(cache.observations), 100)
                self.assertEqual(
                    sorted(bot.client.kline_calls),
                    sorted(item.symbol for item in volume_candidates),
                )
                self.assertEqual(len(set(bot.client.kline_calls)), 100)
                self.assertEqual(bot.trader.sync_calls, 0)
                self.assertEqual(bot.trader.live_calls, 0)
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_paper_trades"
                        ).fetchone()[0],
                        0,
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_live_links"
                        ).fetchone()[0],
                        0,
                    )

    def test_micro_source_failure_logs_exact_symbol_blocks_then_recovers(self):
        class Cache:
            boot_id = "test-boot"
            generation = 0

            def __init__(self):
                self.clear_calls = 0

            def propose(self, scan_id, observations):
                return SimpleNamespace(windows={})

            def commit(self, proposal, *, current_scan_id):
                self.generation += 1
                return True

            def clear(self):
                self.clear_calls += 1
                self.generation += 1

        with tempfile.TemporaryDirectory() as tmpdir:
            bot, recorder = self._bot(tmpdir, [], [])
            volume_candidates = [
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
            premium_rows = {
                item.symbol: {
                    "symbol": item.symbol,
                    "markPrice": "100",
                    "indexPrice": "100",
                    "lastFundingRate": "0",
                }
                for item in volume_candidates
            }
            bot.monitor = SimpleNamespace(
                scan_for_strategies=lambda _top_n: StrategyMarketScan(
                    100, [], volume_candidates, premium_rows, 1
                )
            )
            bot.strategies = (N21_STRATEGY,)
            bot.strategy_scheduler = StrategyScheduler(
                bot.strategies, 96, recorder, bot.logger
            )
            bot.micro_observation_cache = Cache()
            cold = MicroAnalysisResult(
                "N21", "", False, "N21_MICRO_OBSERVATION_COLD_START",
                None, None, {},
            )

            def fail_one_source(*, symbol, **_kwargs):
                if symbol == "M050USDT":
                    raise MicroObservationError("invalid source detail")
                return SimpleNamespace(symbol=symbol)

            with patch(
                "trading_bot.main.build_micro_observation",
                side_effect=fail_one_source,
            ), patch(
                "trading_bot.strategy_scheduler.analyze_micro_strategy",
                side_effect=lambda _strategy, symbol, *_args: replace(
                    cold, symbol=symbol
                ),
            ), self.assertLogs(bot.logger.name, level="ERROR") as captured:
                bot._run_once_multi_strategy()

            joined = "\n".join(captured.output)
            self.assertIn('"code":"MICRO_OBSERVATION_BUILD_FAILED"', joined)
            self.assertIn('"symbol":"M050USDT"', joined)
            self.assertNotIn("invalid source detail", joined)
            self.assertEqual(bot.trader.sync_calls, 0)
            self.assertEqual(bot.trader.live_calls, 0)
            with recorder._connect() as connection:
                failed_scan = connection.execute(
                    "SELECT scan_id FROM strategy_signal_batches "
                    "WHERE state='STAGING'"
                ).fetchone()[0]
                self.assertEqual(
                    connection.execute(
                        "SELECT recorded_count,expected_count "
                        "FROM strategy_signal_batches WHERE scan_id=?",
                        (failed_scan,),
                    ).fetchone(),
                    (100, None),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_paper_trades"
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_live_links"
                    ).fetchone()[0],
                    0,
                )

            with patch(
                "trading_bot.main.build_micro_observation",
                side_effect=lambda *, symbol, **_kwargs: SimpleNamespace(
                    symbol=symbol
                ),
            ), patch(
                "trading_bot.strategy_scheduler.analyze_micro_strategy",
                side_effect=lambda _strategy, symbol, *_args: replace(
                    cold, symbol=symbol
                ),
            ):
                bot._run_once_multi_strategy()
            current_scan = recorder.current_strategy_signal_scan_id()
            self.assertIsInstance(current_scan, int)
            self.assertNotEqual(current_scan, failed_scan)
            with recorder._connect() as connection:
                self.assertIsNone(
                    connection.execute(
                        "SELECT 1 FROM strategy_signal_batches WHERE scan_id=?",
                        (failed_scan,),
                    ).fetchone()
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id=?",
                        (current_scan,),
                    ).fetchone()[0],
                    100,
                )

    def test_kline_boundary_refresh_is_bounded_atomic_and_same_generation(self):
        class BoundaryClient:
            def __init__(self, target_open_time_ms, ready_after):
                self.target_open_time_ms = target_open_time_ms
                self.ready_after = ready_after
                self.calls = {}

            def get_klines(self, symbol):
                count = self.calls.get(symbol, 0)
                self.calls[symbol] = count + 1
                ready_after = (
                    self.ready_after.get(symbol)
                    if isinstance(self.ready_after, dict)
                    else self.ready_after
                )
                ready = (
                    ready_after is not None
                    and count >= ready_after
                )
                return boundary_kline_rows(
                    self.target_open_time_ms
                    if ready
                    else self.target_open_time_ms - 900_000
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            bot, recorder = self._bot(tmpdir, [], [])
            recorder.upsert_strategy_definitions((N21_STRATEGY,))
            candidates = [
                FundingCandidate(
                    f"B{index:03d}USDT",
                    None,
                    Decimal("100"),
                    quote_volume=Decimal(1000 - index),
                    quote_volume_rank=index + 1,
                    candidate_universe="quote_volume_top",
                )
                for index in range(100)
            ]
            target_open = 1_900_000_800_000

            def market_scan(open_time_ms):
                premium = {
                    item.symbol: {
                        "symbol": item.symbol,
                        "markPrice": "100",
                        "indexPrice": "100",
                        "lastFundingRate": "0",
                    }
                    for item in candidates
                }
                return StrategyMarketScan(
                    100,
                    [],
                    candidates,
                    premium,
                    open_time_ms + 1_000,
                )

            current_market_scan = [market_scan(target_open)]
            bot.monitor = SimpleNamespace(
                scan_for_strategies=lambda _top_n: current_market_scan[0]
            )
            bot.strategies = (N21_STRATEGY,)
            bot.strategy_scheduler = StrategyScheduler(
                bot.strategies, 96, recorder, bot.logger
            )
            bot.micro_observation_cache = MicroObservationCache(
                boot_id="boundary-boot"
            )
            client = BoundaryClient(
                target_open,
                ready_after={
                    item.symbol: 0 if index < 50 else 1
                    for index, item in enumerate(candidates)
                },
            )
            bot.client = client
            now_seconds = (target_open + 5_000) / 1000
            with patch(
                "trading_bot.main.time.time", return_value=now_seconds
            ), patch("trading_bot.main.time.sleep") as sleep_mock:
                bot._run_once_multi_strategy()

            first_current = recorder.current_strategy_signal_scan_id()
            self.assertIsInstance(first_current, int)
            self.assertEqual(sum(client.calls.values()), 150)
            self.assertEqual(set(client.calls.values()), {1, 2})
            self.assertEqual(sleep_mock.call_count, 1)
            self.assertEqual(
                {
                    window.observations[-1].kline_open_time_ms
                    for window in bot.micro_observation_cache.snapshot().values()
                },
                {target_open},
            )
            sync_calls_before_unready = bot.trader.sync_calls
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state,recorded_count,expected_count "
                        "FROM strategy_signal_batches WHERE scan_id=?",
                        (first_current,),
                    ).fetchone(),
                    ("CURRENT", 100, 100),
                )

            next_open = target_open + 900_000
            current_market_scan[0] = market_scan(next_open)
            client.target_open_time_ms = next_open
            client.ready_after = None
            client.calls = {}
            evaluate_calls = 0
            original_evaluate = bot.strategy_scheduler.evaluate

            def count_evaluate(*args, **kwargs):
                nonlocal evaluate_calls
                evaluate_calls += 1
                return original_evaluate(*args, **kwargs)

            with patch.object(
                bot.strategy_scheduler, "evaluate", side_effect=count_evaluate
            ), patch(
                "trading_bot.main.time.time",
                return_value=(next_open + 5_000) / 1000,
            ), patch("trading_bot.main.time.sleep") as sleep_mock:
                bot._run_once_multi_strategy()

            self.assertEqual(evaluate_calls, 0)
            self.assertEqual(sum(client.calls.values()), 300)
            self.assertEqual(set(client.calls.values()), {3})
            self.assertEqual(sleep_mock.call_count, 2)
            self.assertEqual(
                recorder.current_strategy_signal_scan_id(), first_current
            )
            self.assertEqual(bot.micro_observation_cache.snapshot(), {})
            self.assertEqual(
                bot.trader.sync_calls, sync_calls_before_unready
            )
            self.assertEqual(bot.trader.live_calls, 0)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signal_batches "
                        "WHERE state='STAGING'"
                    ).fetchone(),
                    (0,),
                )
                for table in (
                    "strategy_passed_signal_audits",
                    "strategy_passed_structure_ledger",
                    "strategy_paper_trades",
                    "strategy_live_links",
                    "trade_reviews",
                ):
                    self.assertEqual(
                        connection.execute(
                            f"SELECT COUNT(*) FROM {table}"
                        ).fetchone(),
                        (0,),
                    )

            recovered_open = next_open + 900_000
            current_market_scan[0] = market_scan(recovered_open)
            client.target_open_time_ms = recovered_open
            client.ready_after = 0
            client.calls = {}
            with patch(
                "trading_bot.main.time.time",
                return_value=(recovered_open + 5_000) / 1000,
            ), patch("trading_bot.main.time.sleep") as sleep_mock:
                bot._run_once_multi_strategy()
            self.assertEqual(sum(client.calls.values()), 100)
            self.assertEqual(set(client.calls.values()), {1})
            sleep_mock.assert_not_called()
            self.assertNotEqual(
                recorder.current_strategy_signal_scan_id(), first_current
            )

    def test_kline_boundary_all_new_generation_uses_completed_snapshot_time(self):
        class NewGenerationClient:
            def __init__(self, target_open_time_ms):
                self.target_open_time_ms = target_open_time_ms
                self.calls = []

            def get_klines(self, symbol):
                self.calls.append(symbol)
                return boundary_kline_rows(self.target_open_time_ms)

        with tempfile.TemporaryDirectory() as tmpdir:
            bot, recorder = self._bot(tmpdir, [], [])
            strategies = (N17_STRATEGY, N20_STRATEGY, N21_STRATEGY)
            recorder.upsert_strategy_definitions(strategies)
            candidates = [
                FundingCandidate(
                    f"G{index:03d}USDT",
                    None,
                    Decimal("100"),
                    quote_volume=Decimal(1000 - index),
                    quote_volume_rank=index + 1,
                    candidate_universe="quote_volume_top",
                )
                for index in range(100)
            ]
            target_open = 1_900_000_800_000
            premium = {
                item.symbol: {
                    "symbol": item.symbol,
                    "markPrice": "100",
                    "indexPrice": "100",
                    "lastFundingRate": "0",
                }
                for item in candidates
            }
            bot.monitor = SimpleNamespace(
                scan_for_strategies=lambda _top_n: StrategyMarketScan(
                    100,
                    [],
                    candidates,
                    premium,
                    target_open - 100,
                )
            )
            bot.strategies = strategies
            bot.strategy_scheduler = StrategyScheduler(
                strategies, 96, recorder, bot.logger
            )
            bot.micro_observation_cache = MicroObservationCache(
                boot_id="all-new-generation"
            )
            client = NewGenerationClient(target_open)
            bot.client = client
            observed_times = iter(
                [target_open - 100]
                + [target_open + 1_000] * 100
                + [target_open + 5_000]
            )
            last_time = [target_open + 5_000]

            def fake_time():
                try:
                    last_time[0] = next(observed_times)
                except StopIteration:
                    pass
                return last_time[0] / 1000

            captured_checked_at = []
            original_evaluate = bot.strategy_scheduler.evaluate

            def capture_evaluate(*args, **kwargs):
                captured_checked_at.append(kwargs["checked_at_ms"])
                return original_evaluate(*args, **kwargs)

            with patch(
                "trading_bot.main.time.time", side_effect=fake_time
            ), patch("trading_bot.main.time.sleep") as sleep_mock, patch.object(
                bot.strategy_scheduler,
                "evaluate",
                side_effect=capture_evaluate,
            ):
                bot._run_once_multi_strategy()

            self.assertEqual(len(client.calls), 100)
            self.assertEqual(set(client.calls), {item.symbol for item in candidates})
            sleep_mock.assert_not_called()
            self.assertEqual(captured_checked_at, [target_open + 5_000])
            current_scan = recorder.current_strategy_signal_scan_id()
            self.assertIsInstance(current_scan, int)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state,recorded_count,expected_count "
                        "FROM strategy_signal_batches WHERE scan_id=?",
                        (current_scan,),
                    ).fetchone(),
                    ("CURRENT", 300, 300),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id=? AND strategy_id='N17' "
                        "AND reason IN ('N17_KLINE_DATA_INVALID',"
                        "'N17_FROZEN_EVIDENCE_INVALID')",
                        (current_scan,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id=? AND strategy_id='N20' "
                        "AND reason='N20_MARKET_CONTEXT_INSUFFICIENT'",
                        (current_scan,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signal_batches "
                        "WHERE state='STAGING'"
                    ).fetchone(),
                    (0,),
                )

    def test_kline_boundary_zero_volume_uses_one_bounded_retry_budget(self):
        class ReadinessClient:
            def __init__(self):
                self.target_open_time_ms = None
                self.mode_by_symbol = {}
                self.short_symbols = set()
                self.calls = {}

            def configure(
                self,
                target_open_time_ms,
                mode_by_symbol,
                short_symbols=(),
            ):
                self.target_open_time_ms = target_open_time_ms
                self.mode_by_symbol = mode_by_symbol
                self.short_symbols = set(short_symbols)
                self.calls = {}

            def get_klines(self, symbol):
                count = self.calls.get(symbol, 0)
                self.calls[symbol] = count + 1
                old_attempts, zero_attempts = self.mode_by_symbol.get(
                    symbol, (0, 0)
                )
                is_old = count < old_attempts
                rows = boundary_kline_rows(
                    self.target_open_time_ms - 900_000
                    if is_old
                    else self.target_open_time_ms
                )
                if (
                    not is_old
                    and count < old_attempts + zero_attempts
                ):
                    rows[-1][5] = "0"
                    rows[-1][7] = "0"
                    rows[-1][10] = "0"
                return rows[-40:] if symbol in self.short_symbols else rows

        with tempfile.TemporaryDirectory() as tmpdir:
            bot, recorder = self._bot(tmpdir, [], [])
            strategies = (N17_STRATEGY, N20_STRATEGY, N21_STRATEGY)
            recorder.upsert_strategy_definitions(strategies)
            candidates = [
                FundingCandidate(
                    f"V{index:03d}USDT",
                    None,
                    Decimal("100"),
                    quote_volume=Decimal(1000 - index),
                    quote_volume_rank=index + 1,
                    candidate_universe="quote_volume_top",
                )
                for index in range(100)
            ]
            special_symbol = candidates[0].symbol
            current_open = [1_900_000_800_000]
            premium = {
                item.symbol: {
                    "symbol": item.symbol,
                    "markPrice": "100",
                    "indexPrice": "100",
                    "lastFundingRate": "0",
                }
                for item in candidates
            }
            bot.monitor = SimpleNamespace(
                scan_for_strategies=lambda _top_n: StrategyMarketScan(
                    100,
                    [],
                    candidates,
                    premium,
                    current_open[0] + 1_000,
                )
            )
            bot.strategies = strategies
            bot.strategy_scheduler = StrategyScheduler(
                strategies, 96, recorder, bot.logger
            )
            bot.micro_observation_cache = MicroObservationCache(
                boot_id="value-readiness"
            )
            client = ReadinessClient()
            bot.client = client

            # All 100 rows are already on the new live axis.  One row has
            # zero cumulative volume only on its first response.
            client.configure(current_open[0], {special_symbol: (0, 1)})
            with patch(
                "trading_bot.main.time.time",
                return_value=(current_open[0] + 5_000) / 1000,
            ), patch("trading_bot.main.time.sleep") as sleep_mock:
                bot._run_once_multi_strategy()
            first_current = recorder.current_strategy_signal_scan_id()
            self.assertIsInstance(first_current, int)
            self.assertEqual(sum(client.calls.values()), 101)
            self.assertEqual(client.calls[special_symbol], 2)
            self.assertEqual(sleep_mock.call_count, 1)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state,recorded_count,expected_count "
                        "FROM strategy_signal_batches WHERE scan_id=?",
                        (first_current,),
                    ).fetchone(),
                    ("CURRENT", 300, 300),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id=? AND strategy_id='N20' "
                        "AND reason='N20_MARKET_CONTEXT_INSUFFICIENT'",
                        (first_current,),
                    ).fetchone(),
                    (0,),
                )

            # A persistently zero current row exhausts the same two-retry
            # budget before begin_scan and leaves no STAGING batch.
            current_open[0] += 900_000
            client.configure(current_open[0], {special_symbol: (0, 10)})
            sync_calls_before = bot.trader.sync_calls
            with self.assertLogs(bot.logger, level="WARNING") as captured, patch(
                "trading_bot.main.time.time",
                return_value=(current_open[0] + 5_000) / 1000,
            ), patch("trading_bot.main.time.sleep") as sleep_mock:
                bot._run_once_multi_strategy()
            self.assertEqual(client.calls[special_symbol], 3)
            self.assertEqual(sum(client.calls.values()), 102)
            self.assertEqual(sleep_mock.call_count, 2)
            self.assertTrue(any(
                "axis_not_ready=0 value_not_ready=1 attempts=3" in message
                for message in captured.output
            ))
            self.assertEqual(
                recorder.current_strategy_signal_scan_id(), first_current
            )
            self.assertEqual(bot.trader.sync_calls, sync_calls_before)
            self.assertEqual(bot.trader.live_calls, 0)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signal_batches "
                        "WHERE state='STAGING'"
                    ).fetchone(),
                    (0,),
                )
                for table in (
                    "strategy_passed_signal_audits",
                    "strategy_passed_structure_ledger",
                    "strategy_paper_trades",
                    "strategy_live_links",
                    "trade_reviews",
                ):
                    self.assertEqual(
                        connection.execute(
                            f"SELECT COUNT(*) FROM {table}"
                        ).fetchone(),
                        (0,),
                    )

            # The same symbol can advance from old-axis to new-axis/zero and
            # then ready using the one shared retry budget.
            current_open[0] += 900_000
            client.configure(current_open[0], {special_symbol: (1, 1)})
            with patch(
                "trading_bot.main.time.time",
                return_value=(current_open[0] + 5_000) / 1000,
            ), patch("trading_bot.main.time.sleep") as sleep_mock:
                bot._run_once_multi_strategy()
            self.assertEqual(client.calls[special_symbol], 3)
            self.assertEqual(sum(client.calls.values()), 102)
            self.assertEqual(sleep_mock.call_count, 2)
            self.assertNotEqual(
                recorder.current_strategy_signal_scan_id(), first_current
            )

            # A newly listed short-history member and an old-axis/zero-volume
            # member share the same bounded readiness pass.  Only the latter
            # is retried; the short member is carried into the scheduler as a
            # deterministic history-readiness rejection, not fabricated into
            # 122 bars and not charged another retry budget.
            current_open[0] += 900_000
            newcomer = FundingCandidate(
                "GRVTUSDT",
                None,
                Decimal("100"),
                quote_volume=Decimal("1"),
                quote_volume_rank=100,
                candidate_universe="quote_volume_top",
            )
            candidates[-1] = newcomer
            premium[newcomer.symbol] = {
                "symbol": newcomer.symbol,
                "markPrice": "100",
                "indexPrice": "100",
                "lastFundingRate": "0",
            }
            client.configure(
                current_open[0],
                {special_symbol: (1, 1)},
                short_symbols={newcomer.symbol},
            )
            with patch(
                "trading_bot.main.time.time",
                return_value=(current_open[0] + 5_000) / 1000,
            ), patch("trading_bot.main.time.sleep") as sleep_mock:
                bot._run_once_multi_strategy()
            latest_scan = recorder.current_strategy_signal_scan_id()
            self.assertIsInstance(latest_scan, int)
            self.assertEqual(client.calls[newcomer.symbol], 1)
            self.assertEqual(client.calls[special_symbol], 3)
            self.assertEqual(sum(client.calls.values()), 102)
            self.assertEqual(sleep_mock.call_count, 2)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state,recorded_count,expected_count "
                        "FROM strategy_signal_batches WHERE scan_id=?",
                        (latest_scan,),
                    ).fetchone(),
                    ("CURRENT", 300, 300),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT reason FROM strategy_signals "
                        "WHERE scan_id=? AND strategy_id='N17' "
                        "AND symbol='GRVTUSDT'",
                        (latest_scan,),
                    ).fetchone(),
                    ("N17_HISTORY_SOURCE_INSUFFICIENT",),
                )

    def test_departed_n15_no_winner_zero_volume_is_lifecycle_scoped(self):
        old_open_time_ms = 1_900_000_800_000
        current_open_time_ms = old_open_time_ms + 900_000
        hft_symbol = "HFTUSDT"
        old_candidates = [
            FundingCandidate(
                hft_symbol if rank == 1 else f"V{rank:03d}USDT",
                None,
                Decimal("100"),
                quote_volume=Decimal(1_000_000 - rank),
                quote_volume_rank=rank,
                candidate_universe="quote_volume_top",
            )
            for rank in range(1, 101)
        ]
        current_candidates = [
            FundingCandidate(
                "NEWUSDT" if rank == 1 else f"V{rank:03d}USDT",
                None,
                Decimal("100"),
                quote_volume=Decimal(1_000_000 - rank),
                quote_volume_rank=rank,
                candidate_universe="quote_volume_top",
            )
            for rank in range(1, 101)
        ]
        old_raw = {
            item.symbol: boundary_kline_rows(old_open_time_ms)
            for item in old_candidates
        }
        for index in (5, 7, 10):
            old_raw[hft_symbol][-1][index] = "0"
        payload, frozen = build_n15_snapshot(
            N15_STRATEGY, old_candidates, old_raw
        )
        self.assertIsNone(frozen.winner_symbol)

        current_raw = {
            item.symbol: boundary_kline_rows(current_open_time_ms)
            for item in current_candidates
        }
        hft_rows = boundary_kline_rows(current_open_time_ms)
        for row in hft_rows[-2:]:
            for index in (5, 7, 10):
                row[index] = "0"
        current_raw[hft_symbol] = hft_rows

        class LifecycleClient:
            def __init__(self):
                self.calls = {}

            def get_klines(self, symbol):
                self.calls[symbol] = self.calls.get(symbol, 0) + 1
                return deepcopy(current_raw[symbol])

        with tempfile.TemporaryDirectory() as tmpdir:
            bot, recorder = self._bot(tmpdir, [], [])
            strategies = (N15_STRATEGY, N20_STRATEGY, N21_STRATEGY)
            recorder.upsert_strategy_definitions(strategies)
            self.assertEqual(
                recorder.record_n15_market_snapshot(
                    "N15", str(old_open_time_ms), payload
                ),
                "OK",
            )
            premium = {
                item.symbol: {
                    "symbol": item.symbol,
                    "markPrice": "100",
                    "indexPrice": "100",
                    "lastFundingRate": "0",
                }
                for item in current_candidates
            }
            bot.monitor = SimpleNamespace(
                scan_for_strategies=lambda _top_n: StrategyMarketScan(
                    854,
                    [],
                    current_candidates,
                    premium,
                    current_open_time_ms + 1_000,
                )
            )
            bot.strategies = strategies
            bot.strategy_scheduler = StrategyScheduler(
                strategies, 96, recorder, bot.logger
            )
            bot.micro_observation_cache = MicroObservationCache(
                boot_id="n15-settling"
            )
            client = LifecycleClient()
            bot.client = client

            original_delete = recorder.delete_n15_market_snapshots_before
            verified_before_cleanup = []

            def audited_delete(strategy_id, current_e_time):
                verified_before_cleanup.append(
                    recorder.get_n15_market_snapshot(
                        strategy_id, str(old_open_time_ms)
                    )
                )
                return original_delete(strategy_id, current_e_time)

            recorder.delete_n15_market_snapshots_before = audited_delete
            with self.assertLogs(bot.logger, level="WARNING") as captured, patch(
                "trading_bot.main.time.time",
                return_value=(current_open_time_ms + 5_000) / 1000,
            ), patch("trading_bot.main.time.sleep") as sleep_mock:
                bot._run_once_multi_strategy()

            current_scan = recorder.current_strategy_signal_scan_id()
            self.assertIsInstance(current_scan, int)
            self.assertEqual(sleep_mock.call_count, 0)
            self.assertEqual(client.calls[hft_symbol], 1)
            self.assertEqual(sum(client.calls.values()), 101)
            self.assertEqual(verified_before_cleanup, [payload])
            self.assertTrue(any(
                "readiness is scoped to its owning strategy | "
                "count=1 symbols=HFTUSDT" in message
                for message in captured.output
            ))
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state,recorded_count,expected_count FROM "
                        "strategy_signal_batches WHERE scan_id=?",
                        (current_scan,),
                    ).fetchone(),
                    ("CURRENT", 300, 300),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT strategy_id,COUNT(*) FROM strategy_signals "
                        "WHERE scan_id=? GROUP BY strategy_id ORDER BY strategy_id",
                        (current_scan,),
                    ).fetchall(),
                    [("N15", 100), ("N20", 100), ("N21", 100)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id=? AND symbol=?",
                        (current_scan, hft_symbol),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n15_entry_states"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT e_time FROM n15_market_snapshots"
                    ).fetchall(),
                    [(str(current_open_time_ms),)],
                )
                receipt = connection.execute(
                    "SELECT strategy_id,e_time,winner_symbol,"
                    "snapshot_payload_blob,snapshot_payload_size,"
                    "snapshot_payload_sha256,snapshot_blob_sha256,close_reason,"
                    "entry_symbol,entry_structure_id,entry_status,entry_reason,"
                    "entry_detail_json,entry_detail_sha256,receipt_sha256,"
                    "closed_at FROM n15_snapshot_terminal_receipts "
                    "WHERE strategy_id='N15' AND e_time=?",
                    (str(old_open_time_ms),),
                ).fetchone()
                self.assertIsNotNone(receipt)
                restored_json, restored = decode_n15_terminal_payload(receipt)
                self.assertEqual(restored, payload)
                self.assertEqual(
                    restored_json,
                    json.dumps(
                        payload,
                        ensure_ascii=False,
                        default=str,
                        sort_keys=True,
                    ),
                )
                self.assertIsNone(receipt[2])
                self.assertEqual(
                    receipt[7], "N15_NO_WINNER_SNAPSHOT_CLOSED"
                )
            self.assertEqual(bot.trader.live_calls, 0)

            # The resolved historical cohort is no longer reintroduced into
            # the global live-value gate on the following scan.
            client.calls = {}
            with patch(
                "trading_bot.main.time.time",
                return_value=(current_open_time_ms + 6_000) / 1000,
            ), patch("trading_bot.main.time.sleep") as sleep_mock:
                bot._run_once_multi_strategy()
            self.assertIsInstance(
                recorder.current_strategy_signal_scan_id(), int
            )
            self.assertEqual(client.calls.get(hft_symbol, 0), 0)
            self.assertEqual(sum(client.calls.values()), 100)
            self.assertEqual(sleep_mock.call_count, 0)
            self.assertEqual(bot.trader.live_calls, 0)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n15_snapshot_terminal_receipts "
                        "WHERE strategy_id='N15' AND e_time=?",
                        (str(old_open_time_ms),),
                    ).fetchone(),
                    (1,),
                )

    def test_kline_snapshot_classifier_rechecks_each_completed_generation(self):
        symbols = ("BTCUSDT", "ETHUSDT")
        first_open = 1_900_000_800_000

        def snapshot(open_time_ms, observed_at_ms):
            rows = {
                symbol: boundary_kline_rows(open_time_ms)
                for symbol in symbols
            }
            observed = {symbol: observed_at_ms for symbol in symbols}
            return rows, observed

        first_rows, first_observed = snapshot(
            first_open, first_open + 1_000
        )
        self.assertEqual(
            _stale_kline_snapshot_symbols(
                first_rows,
                first_observed,
                first_open + 900_000 + 5_000,
            ),
            symbols,
        )
        second_rows, second_observed = snapshot(
            first_open + 900_000, first_open + 900_000 + 1_000
        )
        self.assertEqual(
            _stale_kline_snapshot_symbols(
                second_rows,
                second_observed,
                first_open + 1_800_000 + 5_000,
            ),
            symbols,
        )
        third_rows, third_observed = snapshot(
            first_open + 1_800_000, first_open + 1_800_000 + 1_000
        )
        self.assertEqual(
            _stale_kline_snapshot_symbols(
                third_rows,
                third_observed,
                first_open + 1_800_000 + 5_000,
            ),
            (),
        )
        third_rows["BTCUSDT"][-1][5] = "0"
        third_rows["BTCUSDT"][-1][7] = "0"
        third_rows["BTCUSDT"][-1][10] = "0"
        self.assertEqual(
            _unready_live_kline_value_symbols(
                third_rows,
                first_open + 1_800_000 + 5_000,
            ),
            ("BTCUSDT",),
        )

    def test_paper_close_or_checkpoint_failure_blocks_exchange_and_paper_open(self):
        for mode in ("close", "checkpoint"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                n01, n02 = load_first_stage_strategies()[:2]
                paper_signal = signal(n01, "PAPERUSDT", signal_id=91)
                live_signal = signal(n02, "LIVEUSDT", signal_id=92)
                live_candidate = LiveTradeCandidate(
                    live_signal, eligible_state("N02")
                )
                bot, recorder = self._bot(
                    tmpdir,
                    [paper_signal, live_signal],
                    [live_candidate],
                )
                bot.paper_trader.open_trade(
                    "N01",
                    "PAPERUSDT",
                    Decimal("-0.02"),
                    plan("PAPERUSDT"),
                    {},
                    opened_at=(
                        datetime.now(timezone.utc) - timedelta(minutes=10)
                    ).replace(second=0, microsecond=0).isoformat(),
                )
                with recorder._connect() as connection:
                    state_before = tuple(
                        connection.execute(
                            "SELECT * FROM strategy_states WHERE strategy_id='N01'"
                        ).fetchone()
                    )
                if mode == "close":
                    bot.client.get_klines_for_interval = (
                        lambda _symbol, _interval, limit=1500, start_time_ms=None: [
                            [
                                start_time_ms,
                                "100",
                                "106",
                                "99",
                                "101",
                                "0",
                                start_time_ms + 59_999,
                            ]
                        ]
                    )
                    failure_patch = patch.object(
                        recorder,
                        "_apply_strategy_trade_result",
                        side_effect=RuntimeError("forced qualification failure"),
                    )
                else:
                    bot.client.get_klines_for_interval = (
                        lambda _symbol, _interval, limit=1500, start_time_ms=None: [
                            [
                                start_time_ms,
                                "100",
                                "104",
                                "99.5",
                                "100",
                                "0",
                                start_time_ms + 59_999,
                            ]
                        ]
                    )
                    failure_patch = patch.object(
                        recorder,
                        "update_strategy_paper_last_checked",
                        return_value=False,
                    )

                with failure_patch:
                    bot._run_once_multi_strategy()

                self.assertEqual(bot.trader.sync_calls, 0)
                self.assertEqual(bot.trader.live_calls, 0)
                with recorder._connect() as connection:
                    paper_rows = connection.execute(
                        "SELECT strategy_id, result, closed_at, last_checked_at "
                        "FROM strategy_paper_trades ORDER BY id"
                    ).fetchall()
                    state_after = tuple(
                        connection.execute(
                            "SELECT * FROM strategy_states WHERE strategy_id='N01'"
                        ).fetchone()
                    )
                self.assertEqual(
                    paper_rows,
                    [("N01", "OPEN", None, None)],
                )
                self.assertEqual(state_after, state_before)

    def test_current_passed_signals_never_attempt_new_paper_open(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            n01, n02 = load_first_stage_strategies()[:2]
            first = signal(n01, "FIRSTUSDT", signal_id=101)
            second = signal(n02, "SECONDUSDT", signal_id=102)
            bot, recorder = self._bot(tmpdir, [first, second], [])

            with patch.object(
                recorder,
                "open_strategy_paper_trade",
                side_effect=AssertionError("new paper opens are disabled"),
            ) as open_paper:
                bot._run_once_multi_strategy()

            self.assertEqual(open_paper.call_count, 0)
            self.assertEqual(bot.trader.live_calls, 0)
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_paper_trades"
                    ).fetchone()[0],
                    0,
                )

    def test_resolved_pre_submit_state_clears_only_after_audit_and_exact_cas(self):
        for mode in ("success", "event_failure", "state_replaced"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                original = PositionState(
                    symbol="N16USDT",
                    quantity="1",
                    entry_price="100",
                    stop_loss_price="99",
                    take_profit_price="105",
                    leverage=10,
                    opened_at="2026-07-16T00:00:00+00:00",
                    dry_run=False,
                    orders={
                        "strategy": {
                            "strategy_id": "N16",
                            "signal_id": 41,
                            "structure_id": "a" * 24,
                        },
                        "execution_pending": {
                            "phase": "MARKET_ORDER_SUBMITTING"
                        },
                    },
                )
                replacement = replace(original, symbol="REPLACEDUSDT")
                state_store = StateStore(str(Path(tmpdir) / "state.json"))
                state_store.save(original)
                event_calls = []

                bot = TradingBot.__new__(TradingBot)
                bot.logger = logging.getLogger("test-n16-pending-cas")
                bot.state = state_store
                bot.recorder = SimpleNamespace(
                    record_event=lambda *_args, **_kwargs: (
                        event_calls.append(1)
                        or mode != "event_failure"
                    ),
                    mark_strategy_live_result_pending=lambda *_args, **_kwargs: self.fail(
                        "confirmed-not-executed must not burn live qualification"
                    ),
                )
                bot.paper_trader = SimpleNamespace(
                    close_triggered_open_trades=lambda *_args, **_kwargs: []
                )
                bot.client = SimpleNamespace()
                bot._attested_local_execution_state = lambda: (True, original)
                bot._clear_finalized_n16_state_before_reconciliation = (
                    lambda _state: False
                )
                bot._repair_n16_live_audit_before_reconciliation = (
                    lambda state: (True, state)
                )
                bot._close_dry_run_with_attested_state = lambda _state: None

                def resolved_sync(state):
                    self.assertEqual(state, original)
                    if mode == "state_replaced":
                        state_store.save(replacement)
                    return SyncResult(
                        has_position=False,
                        pending_resolved=True,
                        pending_detail={
                            "reason": "MARKET_ORDER_CONFIRMED_NOT_EXECUTED",
                            "resolution": "MARKET_ORDER_ABSENCE_CONFIRMED_TWICE",
                        },
                    )

                bot._sync_with_attested_state = resolved_sync

                self.assertIsNone(
                    bot._reconcile_multi_strategy_after_publication()
                )
                self.assertEqual(event_calls, [1])
                if mode == "success":
                    self.assertIsNone(state_store.load())
                elif mode == "event_failure":
                    self.assertEqual(state_store.load(), original)
                else:
                    self.assertEqual(state_store.load(), replacement)

    def test_live_order_failure_has_no_paper_fallback_or_score_update(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            n01 = load_first_stage_strategies()[0]
            live_signal = signal(n01, "FAILUSDT", signal_id=201)
            live_candidate = LiveTradeCandidate(live_signal, eligible_state("N01"))
            bot, recorder = self._bot(
                tmpdir,
                [live_signal],
                [live_candidate],
                fail_live=True,
            )

            bot._run_once_multi_strategy()

            with recorder._connect() as connection:
                paper_count = connection.execute("SELECT COUNT(*) FROM strategy_paper_trades").fetchone()[0]
                live_count = connection.execute("SELECT COUNT(*) FROM strategy_live_links").fetchone()[0]
                failed_count = connection.execute(
                    "SELECT COUNT(*) FROM trade_reviews WHERE status = 'FAILED'"
                ).fetchone()[0]
            state = recorder.get_strategy_state("N01")
            self.assertEqual(paper_count, 0)
            self.assertEqual(live_count, 0)
            self.assertEqual(failed_count, 1)
            self.assertEqual(state.paper_trade_count, 0)
            self.assertEqual(state.consecutive_wins, 0)

    def test_live_failure_detail_event_and_logger_share_redacted_diagnostic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            n01 = load_first_stage_strategies()[0]
            live_signal = signal(n01, "SECRETUSDT", signal_id=209)
            live_candidate = LiveTradeCandidate(
                live_signal, eligible_state("N01")
            )
            bot, recorder = self._bot(
                tmpdir,
                [live_signal],
                [live_candidate],
                fail_live=(
                    "Authorization: Bearer TOPSECRET "
                    "token=abc def symbol=SECRETUSDT reason=timeout"
                ),
            )
            overlays = []

            def capture_overlay(*_args, **kwargs):
                overlays.append(kwargs)
                return True

            recorder.update_strategy_signal = capture_overlay
            with self.assertLogs("test_main_multi", level="ERROR") as logs:
                bot._run_once_multi_strategy()

            with recorder._read_only_runtime_snapshot() as connection:
                failed_error = connection.execute(
                    "SELECT error FROM trade_reviews WHERE status = 'FAILED'"
                ).fetchone()[0]
                event_payload = json.loads(connection.execute(
                    "SELECT payload_json FROM events "
                    "WHERE event_type = 'strategy_live_order_failed'"
                ).fetchone()[0])
            live_overlay = next(
                item for item in overlays
                if item.get("reason") == "LIVE_ORDER_FAILED"
            )
            diagnostics = (
                failed_error,
                live_overlay["detail"]["live_order_error"],
                event_payload["error"],
            )
            self.assertEqual(len(set(diagnostics)), 1)
            combined = "\n".join(diagnostics + tuple(logs.output))
            self.assertNotIn("TOPSECRET", combined)
            self.assertNotIn("abc def", combined)
            self.assertIn("Authorization: [REDACTED]", combined)
            self.assertIn("token=[REDACTED]", combined)
            self.assertIn("symbol=SECRETUSDT", combined)
            self.assertIn("reason=timeout", combined)

    def test_live_order_failure_overlay_failure_blocks_remaining_execution(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            n01 = load_first_stage_strategies()[0]
            live_signal = signal(n01, "FAILUSDT", signal_id=211)
            live_candidate = LiveTradeCandidate(
                live_signal, eligible_state("N01")
            )
            bot, recorder = self._bot(
                tmpdir, [live_signal], [live_candidate], fail_live=True
            )
            calls = []

            def fail_second_overlay(*_args, **kwargs):
                calls.append(kwargs.get("reason"))
                return len(calls) == 1

            recorder.update_strategy_signal = fail_second_overlay
            bot._run_once_multi_strategy()

            self.assertEqual(calls, [None, "LIVE_ORDER_FAILED"])
            self.assertEqual(bot.trader.live_calls, 1)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_paper_trades"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_live_links"
                ).fetchone()[0], 0)

    def test_manual_position_blocks_live_without_paper_fallback(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            n01 = load_first_stage_strategies()[0]
            live_signal = signal(n01, "BLOCKEDUSDT", signal_id=202)
            live_candidate = LiveTradeCandidate(live_signal, eligible_state("N01"))
            bot, recorder = self._bot(
                tmpdir,
                [live_signal],
                [live_candidate],
                has_position=True,
            )

            bot._run_once_multi_strategy()

            with recorder._connect() as connection:
                paper_count = connection.execute("SELECT COUNT(*) FROM strategy_paper_trades").fetchone()[0]
                live_count = connection.execute("SELECT COUNT(*) FROM strategy_live_links").fetchone()[0]
            self.assertEqual(paper_count, 0)
            self.assertEqual(live_count, 0)

    def test_existing_live_trade_never_causes_new_paper_open(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            n01, n02 = load_first_stage_strategies()[:2]
            live_strategy_signal = signal(n01, "NEXTUSDT", signal_id=203)
            other_strategy_signal = signal(n02, "OTHERUSDT", signal_id=204)
            bot, recorder = self._bot(
                tmpdir,
                [live_strategy_signal, other_strategy_signal],
                [],
            )
            recorder.record_strategy_live_open(
                "N01",
                None,
                "EXISTINGUSDT",
                "2026-07-10T00:00:00+00:00",
            )

            bot._run_once_multi_strategy()

            with recorder._connect() as connection:
                rows = connection.execute(
                    "SELECT strategy_id, symbol FROM strategy_paper_trades ORDER BY id"
                ).fetchall()
            self.assertEqual(rows, [])

    def test_existing_paper_trade_blocks_new_live_for_same_strategy(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            n01 = load_first_stage_strategies()[0]
            next_signal = signal(n01, "NEXTUSDT", signal_id=205)
            live_candidate = LiveTradeCandidate(next_signal, eligible_state("N01"))
            bot, recorder = self._bot(tmpdir, [next_signal], [live_candidate])
            bot.paper_trader.open_trade(
                "N01",
                "PAPERUSDT",
                Decimal("-0.02"),
                plan("PAPERUSDT"),
                {},
            )

            bot._run_once_multi_strategy()

            self.assertEqual(bot.trader.live_calls, 0)
            with recorder._connect() as connection:
                paper_count = connection.execute(
                    "SELECT COUNT(*) FROM strategy_paper_trades WHERE strategy_id = 'N01' AND result = 'OPEN'"
                ).fetchone()[0]
                live_count = connection.execute(
                    "SELECT COUNT(*) FROM strategy_live_links WHERE strategy_id = 'N01'"
                ).fetchone()[0]
            self.assertEqual(paper_count, 1)
            self.assertEqual(live_count, 0)

    def test_confirmed_live_loss_resets_eligibility_after_price_rebound(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resolution = LiveCloseResolution(
                "LOSS",
                "STOP_LOSS",
                Decimal("94.9"),
                {"source": "STOP_ALGO_ORDER"},
            )
            bot, recorder, state = self._closed_live_bot(tmpdir, resolution)

            blocked = bot._handle_closed_strategy_live_state(state)
            self.assertTrue(bot._handle_closed_strategy_live_state(state))

            strategy_state = recorder.get_strategy_state("N01")
            self.assertFalse(blocked)
            self.assertFalse(strategy_state.live_eligible)
            self.assertFalse(strategy_state.live_result_pending)
            self.assertEqual(strategy_state.consecutive_wins, 0)
            self.assertIsNone(bot.state.load())

    def test_confirmed_live_take_profit_keeps_eligibility_after_retrace(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resolution = LiveCloseResolution(
                "WIN",
                "TAKE_PROFIT",
                Decimal("125.1"),
                {"source": "TAKE_PROFIT_ALGO_ORDER"},
            )
            bot, recorder, state = self._closed_live_bot(tmpdir, resolution)

            blocked = bot._handle_closed_strategy_live_state(state)

            strategy_state = recorder.get_strategy_state("N01")
            self.assertFalse(blocked)
            self.assertTrue(strategy_state.live_eligible)
            self.assertFalse(strategy_state.live_result_pending)
            self.assertEqual(strategy_state.consecutive_wins, 2)

    def test_unconfirmed_live_result_blocks_future_live_candidates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resolution = LiveCloseResolution(
                "LIVE_RESULT_PENDING",
                "LIVE_RESULT_PENDING",
                None,
                {"reason": "PROTECTION_ORDER_QUERY_FAILED"},
            )
            bot, recorder, state = self._closed_live_bot(tmpdir, resolution)

            blocked = bot._handle_closed_strategy_live_state(state)

            strategy_state = recorder.get_strategy_state("N01")
            n01 = load_first_stage_strategies()[0]
            scheduler = StrategyScheduler((n01,), 96, recorder, logging.getLogger("test_live_pending"))
            live_candidate = LiveTradeCandidate(signal(n01, "NEXTUSDT"), strategy_state)
            self.assertTrue(blocked)
            self.assertTrue(strategy_state.live_result_pending)
            self.assertFalse(strategy_state.live_eligible)
            self.assertEqual(recorder.strategy_activity_mode("N01"), "LIVE_OPEN")
            self.assertIsNone(
                scheduler.choose_live_candidate([live_candidate], live_blocked=False)
            )
            n02 = load_first_stage_strategies()[1]
            other_candidate = LiveTradeCandidate(
                signal(n02, "OTHERUSDT"),
                eligible_state("N02"),
            )
            self.assertEqual(
                scheduler.choose_live_candidate(
                    [live_candidate, other_candidate],
                    live_blocked=False,
                ),
                other_candidate,
            )
            self.assertIsNotNone(bot.state.load())
            with recorder._connect() as connection:
                terminal_close_events = connection.execute(
                    "SELECT COUNT(*) FROM events WHERE event_type = 'live_position_closed'"
                ).fetchone()[0]
                cooldowns = connection.execute(
                    "SELECT COUNT(*) FROM symbol_cooldowns WHERE symbol = ?",
                    (state.symbol,),
                ).fetchone()[0]
            self.assertEqual(terminal_close_events, 0)
            self.assertEqual(cooldowns, 0)

    def _pending_live_recovery_fixture(self, tmpdir):
        n01 = load_first_stage_strategies()[0]
        recorder = make_test_recorder(
            str(Path(tmpdir) / "review.sqlite3"),
            logging.getLogger("test_pending_live_recovery"),
        )
        recorder.upsert_strategy_definitions((n01,))
        with recorder._connect() as connection:
            connection.execute(
                """
                UPDATE strategy_states
                SET consecutive_wins = 2, paper_trade_count = 2, win_count = 2,
                    win_rate = '1', live_eligible = 1
                WHERE strategy_id = 'N01'
                """
            )
        state = PositionState(
            symbol="DODOXUSDT",
            quantity="10",
            entry_price="100",
            stop_loss_price="95",
            take_profit_price="125",
            leverage=10,
            opened_at="2026-07-13T00:00:00+00:00",
            dry_run=False,
            orders={
                "plan": {"risk_amount": "10"},
                "open": {
                    "orderId": 100,
                    "clientOrderId": "mkt-dodo",
                    "symbol": "DODOXUSDT",
                    "side": "BUY",
                    "type": "MARKET",
                    "status": "FILLED",
                    "executedQty": "10",
                },
                "stop": {
                    "algoId": 11,
                    "clientAlgoId": "sl-dodo",
                    "symbol": "DODOXUSDT",
                    "algoType": "CONDITIONAL",
                    "side": "SELL",
                    "orderType": "STOP_MARKET",
                    "closePosition": True,
                    "algoStatus": "NEW",
                },
                "take_profit": {
                    "algoId": 22,
                    "clientAlgoId": "tp-dodo",
                    "symbol": "DODOXUSDT",
                    "algoType": "CONDITIONAL",
                    "side": "SELL",
                    "orderType": "TAKE_PROFIT_MARKET",
                    "closePosition": True,
                    "algoStatus": "NEW",
                },
                "strategy": {"strategy_id": "N01"},
            },
        )
        scan_id = recorder.begin_scan(0, [], dry_run=False)
        trade_id = recorder.record_trade_open(scan_id, state)
        recorder.record_strategy_live_open("N01", trade_id, state.symbol, state.opened_at)
        recorder.record_trade_close(
            state,
            "LIVE_RESULT_PENDING",
            "",
            "",
            "",
            "",
            "",
            {"reason": "PROTECTION_ORDER_QUERY_FAILED"},
        )
        recorder.mark_strategy_live_result_pending("N01", "PROTECTION_ORDER_QUERY_FAILED")
        state_store = StateStore(str(Path(tmpdir) / "state.json"))
        state_store.save(state)
        client = FakeCloseResolutionClient(
            {
                11: {
                    "algoId": 11,
                    "clientAlgoId": "sl-dodo",
                    "symbol": "DODOXUSDT",
                    "algoStatus": "FINISHED",
                    "actualOrderId": "111",
                },
                22: {
                    "algoId": 22,
                    "clientAlgoId": "tp-dodo",
                    "symbol": "DODOXUSDT",
                    "algoStatus": "EXPIRED",
                    "actualOrderId": "",
                },
            },
            {
                111: {
                    "orderId": 111,
                    "status": "FILLED",
                    "executedQty": "10",
                    "avgPrice": "94.9",
                }
            },
        )
        bot = TradingBot.__new__(TradingBot)
        bot.config = SimpleNamespace(symbol_cooldown_hours=4)
        bot.logger = logging.getLogger("test_pending_live_recovery")
        bot.recorder = recorder
        bot.state = state_store
        bot.trader = Trader(
            client,
            live_test_config(str(Path(tmpdir) / "account.json")),
            state_store,
            bot.logger,
        )
        return bot, recorder, state_store, state, trade_id

    def test_pending_live_loss_recovery_reuses_final_audit_after_crash(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bot, recorder, state_store, state, trade_id = self._pending_live_recovery_fixture(tmpdir)
            with patch.object(
                state_store,
                "compare_and_clear",
                side_effect=OSError("simulated crash before state clear"),
            ), patch.object(
                recorder,
                "claim_strategy_live_open_audit",
                side_effect=AssertionError(
                    "exact CLOSED_LIVE_RESULT_PENDING must bypass pre-audit claim"
                ),
            ):
                self.assertTrue(bot._handle_closed_strategy_live_state(state))
            with recorder._connect() as connection:
                crash_rows = connection.execute("SELECT id, status FROM trade_reviews").fetchall()
                crash_close_events = connection.execute(
                    "SELECT COUNT(*) FROM events WHERE event_type = 'live_position_closed'"
                ).fetchone()[0]
                crash_strategy_state = connection.execute(
                    "SELECT consecutive_wins, live_eligible, live_result_pending, "
                    "last_trade_result FROM strategy_states WHERE strategy_id = 'N01'"
                ).fetchone()
                crash_cooldown = connection.execute(
                    "SELECT cooldown_until, reason, source_trade_id, updated_at "
                    "FROM symbol_cooldowns WHERE symbol = ?",
                    (state.symbol,),
                ).fetchone()
            self.assertEqual(crash_rows, [(trade_id, "CLOSED_STOP_LOSS")])
            self.assertEqual(crash_close_events, 2)
            self.assertIsNotNone(state_store.load())

            class NoExchangeResolutionAllowed:
                def resolve_closed_live_position(self, _state):
                    raise AssertionError(
                        "durably finalized state must not re-query the exchange"
                    )

            bot.trader = NoExchangeResolutionAllowed()
            self.assertFalse(bot._handle_closed_strategy_live_state(state_store.load()))
            self.assertIsNone(state_store.load())
            strategy_state = recorder.get_strategy_state("N01")
            self.assertEqual(strategy_state.consecutive_wins, 0)
            self.assertFalse(strategy_state.live_eligible)
            self.assertFalse(strategy_state.live_result_pending)
            with recorder._connect() as connection:
                final_rows = connection.execute("SELECT id, status FROM trade_reviews").fetchall()
                live_links = connection.execute(
                    "SELECT result, closed_at IS NOT NULL FROM strategy_live_links"
                ).fetchall()
                close_events = connection.execute(
                    "SELECT COUNT(*) FROM events WHERE event_type = 'live_position_closed'"
                ).fetchone()[0]
                final_strategy_state = connection.execute(
                    "SELECT consecutive_wins, live_eligible, live_result_pending, "
                    "last_trade_result FROM strategy_states WHERE strategy_id = 'N01'"
                ).fetchone()
                final_cooldown = connection.execute(
                    "SELECT cooldown_until, reason, source_trade_id, updated_at "
                    "FROM symbol_cooldowns WHERE symbol = ?",
                    (state.symbol,),
                ).fetchone()
            self.assertEqual(final_rows, [(trade_id, "CLOSED_STOP_LOSS")])
            self.assertEqual(live_links, [("LOSS", 1)])
            self.assertEqual(close_events, crash_close_events)
            self.assertEqual(final_strategy_state, crash_strategy_state)
            self.assertEqual(final_cooldown, crash_cooldown)

    def test_final_trade_audit_crash_recovers_same_link_without_exchange_or_duplicate_events(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bot, recorder, state_store, state, trade_id = self._pending_live_recovery_fixture(tmpdir)
            self.assertEqual(
                recorder.record_trade_close(
                    state, "STOP_LOSS", "94.9", "", "", "", "", {"source": "old_path"}
                ),
                trade_id,
            )
            self.assertTrue(
                recorder.set_symbol_cooldown(
                    state.symbol,
                    datetime.now(timezone.utc) + timedelta(hours=4),
                    "STOP_LOSS",
                    trade_id,
                )
            )
            with recorder._connect() as connection:
                before_events = connection.execute(
                    "SELECT event_type, COUNT(*) FROM events "
                    "WHERE event_type IN ('live_position_closed', 'symbol_cooldown_set') "
                    "GROUP BY event_type ORDER BY event_type"
                ).fetchall()

            class NoExchangeResolutionAllowed:
                def resolve_closed_live_position(self, _state):
                    raise AssertionError(
                        "a recoverable final trade audit must not re-query the exchange"
                    )

            bot.trader = NoExchangeResolutionAllowed()
            self.assertFalse(bot._handle_closed_strategy_live_state(state_store.load()))
            self.assertIsNone(state_store.load())
            with recorder._connect() as connection:
                review = connection.execute(
                    "SELECT id, status FROM trade_reviews"
                ).fetchall()
                link = connection.execute(
                    "SELECT result, closed_at IS NOT NULL FROM strategy_live_links"
                ).fetchall()
                after_events = connection.execute(
                    "SELECT event_type, COUNT(*) FROM events "
                    "WHERE event_type IN ('live_position_closed', 'symbol_cooldown_set') "
                    "GROUP BY event_type ORDER BY event_type"
                ).fetchall()
            self.assertEqual(review, [(trade_id, "CLOSED_STOP_LOSS")])
            self.assertEqual(link, [("LOSS", 1)])
            self.assertEqual(after_events, before_events)

    def test_review_only_pre_audit_crash_claims_exact_link_then_finalizes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resolution = LiveCloseResolution(
                "LOSS",
                "STOP_LOSS",
                Decimal("94.9"),
                {"source": "STOP_ALGO_ORDER"},
            )
            bot, recorder, state = self._closed_live_bot(tmpdir, resolution)
            with recorder._connect() as connection:
                trade_id = connection.execute(
                    "SELECT id FROM trade_reviews"
                ).fetchone()[0]
                connection.execute("DELETE FROM strategy_live_links")

            class CountingResolver:
                calls = 0

                def resolve_closed_live_position(self, _state):
                    self.calls += 1
                    return resolution

            resolver = CountingResolver()
            bot.trader = resolver
            self.assertFalse(bot._handle_closed_strategy_live_state(state))
            self.assertEqual(resolver.calls, 1)
            self.assertIsNone(bot.state.load())
            with recorder._connect() as connection:
                reviews = connection.execute(
                    "SELECT id, status FROM trade_reviews"
                ).fetchall()
                links = connection.execute(
                    "SELECT trade_review_id, result, closed_at IS NOT NULL "
                    "FROM strategy_live_links"
                ).fetchall()
            self.assertEqual(reviews, [(trade_id, "CLOSED_STOP_LOSS")])
            self.assertEqual(links, [(trade_id, "LOSS", 1)])

    def test_pre_audit_claim_identity_conflict_retains_state_without_resolver(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resolution = LiveCloseResolution(
                "LOSS",
                "STOP_LOSS",
                Decimal("94.9"),
                {"source": "STOP_ALGO_ORDER"},
            )
            bot, recorder, state = self._closed_live_bot(tmpdir, resolution)
            with recorder._connect() as connection:
                connection.execute(
                    "UPDATE trade_reviews SET quantity = '11'"
                )
                before_reviews = connection.execute(
                    "SELECT id, quantity, status, orders_json FROM trade_reviews"
                ).fetchall()
                before_links = connection.execute(
                    "SELECT id, trade_review_id, closed_at, result "
                    "FROM strategy_live_links"
                ).fetchall()

            class NoExchangeResolutionAllowed:
                def resolve_closed_live_position(self, _state):
                    raise AssertionError(
                        "conflicting pre-audit identity must not query the exchange"
                    )

            bot.trader = NoExchangeResolutionAllowed()
            self.assertTrue(bot._handle_closed_strategy_live_state(state))
            self.assertEqual(bot.state.load(), state)
            with recorder._connect() as connection:
                after_reviews = connection.execute(
                    "SELECT id, quantity, status, orders_json FROM trade_reviews"
                ).fetchall()
                after_links = connection.execute(
                    "SELECT id, trade_review_id, closed_at, result "
                    "FROM strategy_live_links"
                ).fetchall()
            self.assertEqual(after_reviews, before_reviews)
            self.assertEqual(after_links, before_links)

    def test_take_profit_final_review_recovers_even_when_strategy_state_already_matches_win(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resolution = LiveCloseResolution(
                "LIVE_RESULT_PENDING",
                "LIVE_RESULT_PENDING",
                None,
                {},
            )
            bot, recorder, state = self._closed_live_bot(tmpdir, resolution)
            with recorder._connect() as connection:
                trade_id = connection.execute(
                    "SELECT id FROM trade_reviews"
                ).fetchone()[0]
                connection.execute(
                    """
                    UPDATE strategy_states
                    SET live_eligible = 1, live_result_pending = 0,
                        last_trade_result = 'WIN'
                    WHERE strategy_id = 'N01'
                    """
                )
            self.assertEqual(
                recorder.record_trade_close(
                    state,
                    "TAKE_PROFIT",
                    "125.1",
                    "",
                    "",
                    "",
                    "",
                    {"source": "old_path"},
                ),
                trade_id,
            )
            self.assertTrue(
                recorder.set_symbol_cooldown(
                    state.symbol,
                    datetime.now(timezone.utc) + timedelta(hours=4),
                    "TAKE_PROFIT",
                    trade_id,
                )
            )
            with recorder._connect() as connection:
                before_events = connection.execute(
                    "SELECT event_type, COUNT(*) FROM events "
                    "WHERE event_type IN ('live_position_closed', 'symbol_cooldown_set') "
                    "GROUP BY event_type ORDER BY event_type"
                ).fetchall()
                before_cooldown = connection.execute(
                    "SELECT cooldown_until, reason, source_trade_id, updated_at "
                    "FROM symbol_cooldowns WHERE symbol = ?",
                    (state.symbol,),
                ).fetchone()

            class NoExchangeResolutionAllowed:
                def resolve_closed_live_position(self, _state):
                    raise AssertionError(
                        "terminal TAKE_PROFIT review must be recovered without exchange query"
                    )

            bot.trader = NoExchangeResolutionAllowed()
            self.assertFalse(bot._handle_closed_strategy_live_state(state))
            self.assertIsNone(bot.state.load())
            strategy_state = recorder.get_strategy_state("N01")
            self.assertTrue(strategy_state.live_eligible)
            self.assertFalse(strategy_state.live_result_pending)
            self.assertEqual(strategy_state.last_trade_result, "WIN")
            with recorder._connect() as connection:
                reviews = connection.execute(
                    "SELECT id, status FROM trade_reviews"
                ).fetchall()
                links = connection.execute(
                    "SELECT trade_review_id, result, closed_at IS NOT NULL "
                    "FROM strategy_live_links"
                ).fetchall()
                after_events = connection.execute(
                    "SELECT event_type, COUNT(*) FROM events "
                    "WHERE event_type IN ('live_position_closed', 'symbol_cooldown_set') "
                    "GROUP BY event_type ORDER BY event_type"
                ).fetchall()
                after_cooldown = connection.execute(
                    "SELECT cooldown_until, reason, source_trade_id, updated_at "
                    "FROM symbol_cooldowns WHERE symbol = ?",
                    (state.symbol,),
                ).fetchone()
            self.assertEqual(reviews, [(trade_id, "CLOSED_TAKE_PROFIT")])
            self.assertEqual(links, [(trade_id, "WIN", 1)])
            self.assertEqual(after_events, before_events)
            self.assertEqual(after_cooldown, before_cooldown)

    def test_finalized_live_state_cas_does_not_delete_replacement(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bot, recorder, state_store, state, _ = self._pending_live_recovery_fixture(tmpdir)
            recorder.finalize_strategy_live_result(
                state,
                "N01",
                "LOSS",
                "STOP_LOSS",
                "94.9",
                {"source": "STOP_ALGO_ORDER"},
                datetime.now(timezone.utc) + timedelta(hours=4),
            )
            replacement = PositionState(
                symbol="REPLACEMENTUSDT",
                quantity="1",
                entry_price="10",
                stop_loss_price="9",
                take_profit_price="15",
                leverage=1,
                opened_at="2026-07-13T00:15:00+00:00",
                dry_run=False,
                orders={"strategy": {"strategy_id": "N01"}},
            )
            state_store.save(replacement)

            self.assertTrue(bot._handle_closed_strategy_live_state(state))
            self.assertEqual(state_store.load(), replacement)
            self.assertEqual(recorder.get_strategy_state("N01").last_trade_result, "LOSS")

    def test_manual_live_close_is_recorded_without_scoring(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resolution = LiveCloseResolution(
                "MANUAL_OR_EXTERNAL_CLOSE",
                "MANUAL_OR_EXTERNAL_CLOSE",
                None,
                {"reason": "NO_PROTECTION_ORDER_FILLED"},
            )
            bot, recorder, state = self._closed_live_bot(tmpdir, resolution)

            blocked = bot._handle_closed_strategy_live_state(state)

            strategy_state = recorder.get_strategy_state("N01")
            with recorder._connect() as connection:
                exit_reason = connection.execute(
                    "SELECT exit_reason FROM trade_reviews"
                ).fetchone()[0]
            self.assertTrue(blocked)
            self.assertEqual(exit_reason, "MANUAL_OR_EXTERNAL_CLOSE")
            self.assertTrue(strategy_state.live_result_pending)
            self.assertEqual(strategy_state.paper_trade_count, 2)
            self.assertEqual(strategy_state.consecutive_wins, 2)
            self.assertIsNotNone(bot.state.load())


if __name__ == "__main__":
    unittest.main()
