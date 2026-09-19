from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import hashlib
import json
import logging
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
from trading_bot.binance_client import BinanceAPIError
from trading_bot.main import TradingBot
from trading_bot.monitor import FundingCandidate, StrategyMarketScan
from trading_bot.n14_analyzer import (
    N14AnalysisResult,
    N14Event,
    N14MarketScenario,
    analyze_n14_sell_pressure_decay_reversal,
    evaluate_n14_shock,
    n14_wilder_atr,
    parse_n14_klines,
)
from trading_bot.n14_snapshot import (
    build_n14_snapshot,
    decode_n14_snapshot,
    n14_bullish_breadth_for_current_c,
)
from trading_bot.paper_trader import PaperTrader
from trading_bot.recorder import (
    ReviewRecorder,
    StrategySignalBatchWriteResult,
)
from trading_bot.state import PositionState, StateStore
from trading_bot.strategies import N14_STRATEGY, load_all_strategies
from trading_bot.strategy_scheduler import (
    N14BatchContext,
    StrategyScheduler,
    StrategySignalDecision,
    _n14_active_envelope,
    _n14_terminal_envelope,
)
from trading_bot.trader import EntryWindowExpiredError, SyncResult, TradePlan, Trader
from tests.test_trader import (
    FakeLiveExecutionClient,
    RuleConstrainedClient,
    live_test_config,
    test_config,
)


BASE_TIME_MS = 1_800_000_000_000
INTERVAL_MS = 900_000


def kline(
    index,
    open_price="100",
    high="100.1",
    low="99.9",
    close="100",
    quote_volume="100",
    taker_buy_quote="50",
):
    open_time = BASE_TIME_MS + index * INTERVAL_MS
    return [
        open_time,
        str(open_price),
        str(high),
        str(low),
        str(close),
        "1",
        open_time + INTERVAL_MS - 1,
        str(quote_volume),
        "1",
        "1",
        str(taker_buy_quote),
        "0",
    ]


def record_exact_live_result(recorder, strategy_id, symbol, result, opened_at):
    state = PositionState(
        symbol=symbol,
        quantity="10",
        entry_price="100",
        stop_loss_price="95",
        take_profit_price="125",
        leverage=10,
        opened_at=opened_at,
        dry_run=False,
        orders={"plan": {}, "strategy": {"strategy_id": strategy_id}},
    )
    trade_id = recorder.record_trade_open(None, state)
    if trade_id is None:
        raise AssertionError("live trade review open was not recorded")
    if recorder.record_strategy_live_open(strategy_id, trade_id, symbol, opened_at) is None:
        raise AssertionError("exact live link was not recorded")
    exit_reason = "TAKE_PROFIT" if result == "WIN" else "STOP_LOSS"
    exit_price = "125" if result == "WIN" else "95"
    if recorder.record_trade_close(
        state, exit_reason, exit_price, "", "", "", "", {}
    ) != trade_id:
        raise AssertionError("live trade review close was not recorded")
    return recorder.record_strategy_live_result(
        strategy_id,
        result,
        symbol,
        trade_id,
        opened_at,
    )


def n14_klines(*, elapsed_ms=30_000, second_confirmation=False):
    count = 123 if second_confirmation else 122
    rows = [kline(index) for index in range(count)]
    rows[115] = kline(115, "100.5", "100.6", "99.9", "100")
    rows[118] = kline(
        118, "100", "100.05", "99.75", "99.8", "150", "60"
    )
    rows[119] = kline(
        119, "99.82", "99.9", "99.72", "99.8", "80", "36"
    )
    rows[120] = kline(
        120, "99.8", "100.02", "99.72", "99.96", "80", "44"
    )
    if second_confirmation:
        rows[120] = kline(
            120, "99.8", "99.9", "99.72", "99.8", "80", "40"
        )
        rows[121] = kline(
            121, "99.8", "100.02", "99.72", "99.96", "80", "44"
        )
        rows[122] = kline(
            122, "99.96", "100.1", "99.72", "99.96", "100", "50"
        )
    else:
        rows[121] = kline(
            121, "99.96", "100.1", "99.72", "99.96", "100", "50"
        )
    checked = rows[-1][0] + elapsed_ms
    return rows, checked


def scenario(raw, **overrides):
    candles = parse_n14_klines(raw)
    atr = n14_wilder_atr(candles[:-1])[117]
    values = {
        "s_open_time_ms": raw[118][0],
        "quote_volume_rank": 1,
        "r1h": Decimal(str(raw[118][4])) / Decimal(str(raw[115][1]))
        - Decimal("1"),
        "rank_down": 1,
        "atr_pct": atr / Decimal(str(raw[117][4])),
        "residual": Decimal(str(raw[118][4])) / Decimal(str(raw[115][1]))
        - Decimal("1"),
        "market_1h_median": Decimal("0"),
        "red_breadth_s": Decimal("0.01"),
        "median_bar_return_s": Decimal("0"),
        "median_atr_pct_s": Decimal("0.002"),
        "bullish_breadth_s": Decimal("0"),
        "bullish_breadth_c": Decimal("0.40"),
        "prior_full_shock_present": False,
    }
    values.update(overrides)
    return N14MarketScenario(**values)


def analyze(raw, checked, **kwargs):
    market = kwargs.pop("market_scenario", scenario(raw))
    return analyze_n14_sell_pressure_decay_reversal(
        "N14USDT",
        raw,
        market_scenarios={market.s_open_time_ms: market},
        market_context_complete=kwargs.pop("market_context_complete", True),
        checked_at_ms=checked,
        target_s_open_time_ms=raw[118][0],
        **kwargs,
    )


def candidate(symbol, rank):
    return FundingCandidate(
        symbol=symbol,
        funding_rate=None,
        mark_price=Decimal("999"),
        quote_volume=Decimal("1000000") - Decimal(rank),
        quote_volume_rank=rank,
        candidate_universe="quote_volume_top",
    )


def top100():
    return [candidate(f"S{rank:03d}USDT", rank) for rank in range(1, 101)]


def full_batch(raw=None):
    base_raw, checked = n14_klines() if raw is None else (raw, raw[-1][0] + 30_000)
    items = top100()
    by_symbol = {
        item.symbol: (
            deepcopy(base_raw)
            if item == items[0]
            else [kline(index) for index in range(len(base_raw))]
        )
        for item in items
    }
    for rank, item in enumerate(items[1:40], start=2):
        by_symbol[item.symbol][120] = kline(
            120, "100", "100.1", "99.9", "100.05", "100", "50"
        )
    return items, by_symbol, checked


def recorder_scheduler(tmpdir, strategy=N14_STRATEGY):
    recorder = make_test_recorder(
        str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("n14")
    )
    recorder.upsert_strategy_definitions((strategy,))
    scheduler = StrategyScheduler((strategy,), 96, recorder, logging.getLogger("n14"))
    return recorder, scheduler


def resign_snapshot(payload):
    unsigned = {
        key: value for key, value in payload.items() if key != "canonical_sha256"
    }
    canonical = json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    payload["canonical_sha256"] = hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()


class N14AnalyzerTests(unittest.TestCase):
    def test_valid_structure_uses_s_minus_one_atr_v20_and_s_rank(self):
        raw, checked = n14_klines()
        result = analyze(raw, checked)
        self.assertTrue(result.passed, result.reason)
        structure = result.structure
        candles = parse_n14_klines(raw)
        expected_atr = n14_wilder_atr(candles[:-1])[117]
        self.assertEqual(structure.atr_s_reference, expected_atr)
        self.assertEqual(structure.volume_median_s, Decimal("100"))
        self.assertEqual(structure.quote_volume_rank, 1)
        self.assertEqual(structure.p, Decimal("99.72"))
        self.assertEqual(structure.flow_flip, Decimal("0.10"))
        self.assertEqual(structure.entry_min_price, structure.c.close)
        self.assertEqual(
            structure.entry_max_price,
            structure.c.close + Decimal("0.5") * structure.atr_c,
        )

    def test_parser_strict_time_ohlc_and_flat_bar(self):
        raw, _ = n14_klines()
        flat = deepcopy(raw)
        flat[20] = kline(20, "100", "100", "100", "100", "0", "0")
        self.assertEqual(parse_n14_klines(flat)[20].range, Decimal("0"))
        for name, mutation in {
            "bool_time": lambda rows: rows[0].__setitem__(0, True),
            "float_time": lambda rows: rows[0].__setitem__(0, float(rows[0][0])),
            "nonpositive_time": lambda rows: rows[0].__setitem__(0, 0),
            "high_below_low": lambda rows: rows[20].__setitem__(2, "99"),
            "flat_mismatched_open": lambda rows: rows[20].__setitem__(1, "99.9"),
        }.items():
            with self.subTest(name=name):
                broken = deepcopy(flat)
                mutation(broken)
                with self.assertRaises(ValueError):
                    parse_n14_klines(broken)

    def test_zero_prior_volume_is_allowed_when_exact_median_is_positive(self):
        raw, checked = n14_klines()
        raw[101][7] = "0"
        raw[101][10] = "0"
        self.assertTrue(analyze(raw, checked).passed)
        for index in range(98, 118):
            raw[index][7] = "0"
            raw[index][10] = "0"
        result = analyze(raw, checked)
        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "N14_NOT_SHOCK")

    def test_shock_numeric_boundaries_are_inclusive(self):
        raw, _ = n14_klines()
        candles = parse_n14_klines(raw)
        atrs = n14_wilder_atr(candles[:-1])
        # 0.175 makes the exact 30% close-location boundary finite:
        # body=0.14, lower wick=0.06, full range=0.20.
        atr = Decimal("0.175")
        atrs[117] = atr
        close = Decimal("99.8")
        body = Decimal("0.8") * atr
        high = close + body
        low = close - Decimal("0.06")
        raw[118] = kline(
            118,
            close + body,
            high,
            low,
            close,
            "150",
            "60",
        )
        assessment = evaluate_n14_shock(
            parse_n14_klines(raw)[:-1], atrs, 118
        )
        self.assertTrue(assessment.passed, assessment.reason)

        cases = {
            "body": (1, str(close + body - Decimal("0.000001")), "N14_NOT_SHOCK"),
            "volume": (7, "149.999", "N14_SHOCK_VOLUME_TOO_LOW"),
            "taker": (10, "60.001", "N14_SHOCK_TAKER_BUY_TOO_HIGH"),
        }
        for name, (column, value, reason) in cases.items():
            with self.subTest(name=name):
                changed = deepcopy(raw)
                changed[118][column] = value
                assessed = evaluate_n14_shock(
                    parse_n14_klines(changed)[:-1], atrs, 118
                )
                self.assertEqual(assessed.reason, reason)

    def test_market_boundaries_and_systemic_and_semantics(self):
        raw, checked = n14_klines()
        base = scenario(
            raw,
            market_1h_median=Decimal("-0.0075"),
            rank_down=20,
        )
        exact_residual = replace(base, residual=-base.atr_pct)
        self.assertTrue(analyze(raw, checked, market_scenario=exact_residual).passed)
        for market, reason in (
            (replace(base, market_1h_median=Decimal("-0.0075001")), "N14_MARKET_1H_MEDIAN_TOO_LOW"),
            (replace(base, rank_down=21), "N14_RELATIVE_DOWNSIDE_RANK_OUT_OF_RANGE"),
            (replace(base, residual=-base.atr_pct + Decimal("0.000001")), "N14_RESIDUAL_DROP_TOO_SMALL"),
            (replace(base, r1h=Decimal("0")), "N14_NOT_SHOCK"),
        ):
            with self.subTest(reason=reason):
                self.assertEqual(analyze(raw, checked, market_scenario=market).reason, reason)

        crash = replace(
            exact_residual,
            red_breadth_s=Decimal("0.75"),
            median_atr_pct_s=Decimal("0.002"),
            median_bar_return_s=Decimal("-0.001"),
        )
        self.assertEqual(
            analyze(raw, checked, market_scenario=crash).reason,
            "N14_SYSTEMIC_CRASH_VETO",
        )
        self.assertTrue(
            analyze(
                raw,
                checked,
                market_scenario=replace(crash, red_breadth_s=Decimal("0.749999")),
            ).passed
        )
        self.assertTrue(
            analyze(
                raw,
                checked,
                market_scenario=replace(
                    crash, median_bar_return_s=Decimal("-0.000999")
                ),
            ).passed
        )

    def test_absorption_boundaries_and_fixed_s_plus_one(self):
        raw, checked = n14_klines()
        atr = n14_wilder_atr(parse_n14_klines(raw)[:-1])[117]
        s_range = Decimal("0.30")
        exact = deepcopy(raw)
        exact[119] = kline(
            119,
            "99.82",
            Decimal("99.75") - Decimal("0.20") * atr
            + Decimal("0.8") * s_range,
            Decimal("99.75") - Decimal("0.20") * atr,
            Decimal("99.8") - Decimal("0.20") * atr,
            "80",
            "36",
        )
        # Exact low/close/range boundaries remain valid; C is retuned above A.high.
        exact[120] = kline(
            120,
            exact[119][4],
            "100.05",
            exact[119][3],
            "100.01",
            "80",
            "44",
        )
        exact[121] = kline(
            121,
            "100.01",
            "100.10",
            "99.72",
            "100.01",
            "100",
            "50",
        )
        self.assertTrue(analyze(exact, checked).passed)

        mutations = {
            "N14_ABSORPTION_VOLUME_TOO_LOW": (7, "79.999"),
            "N14_ABSORPTION_TAKER_BUY_TOO_HIGH": (10, "36.001"),
            "N14_ABSORPTION_RANGE_TOO_LARGE": (2, "100.01"),
            "N14_ABSORPTION_LOW_TOO_LOW": (3, str(Decimal("99.75") - Decimal("0.20") * atr - Decimal("0.000001"))),
            "N14_ABSORPTION_CLOSE_TOO_LOW": (4, str(Decimal("99.8") - Decimal("0.20") * atr - Decimal("0.000001"))),
        }
        for reason, (column, value) in mutations.items():
            with self.subTest(reason=reason):
                changed = deepcopy(raw)
                changed[119][column] = value
                if column in {2, 3, 4}:
                    # Preserve OHLC validity while crossing the intended threshold.
                    o = Decimal(changed[119][1])
                    h = Decimal(changed[119][2])
                    l = Decimal(changed[119][3])
                    c = Decimal(changed[119][4])
                    changed[119][2] = str(max(h, o, c))
                    changed[119][3] = str(min(l, o, c))
                self.assertEqual(analyze(changed, checked).reason, reason)

        failed_a = deepcopy(raw)
        failed_a[119][7] = "79"
        failed_a[120] = deepcopy(raw[119])
        failed_a[120][0] = raw[120][0]
        self.assertEqual(
            analyze(failed_a, checked).reason,
            "N14_ABSORPTION_VOLUME_TOO_LOW",
        )

    def test_first_complete_c_can_be_a_plus_two_and_low_break_is_terminal(self):
        raw, checked = n14_klines(second_confirmation=True)
        result = analyze(raw, checked)
        self.assertTrue(result.passed, result.reason)
        self.assertEqual(result.structure.c.index, 121)
        self.assertEqual(
            result.structure.failed_confirmation_reasons,
            ("N14_CONFIRMATION_NOT_BULLISH",),
        )

        broken = deepcopy(raw)
        broken[120][3] = "99.719"
        result = analyze(broken, checked)
        self.assertEqual(result.reason, "N14_CONFIRMATION_LOW_BROKE_P")
        self.assertEqual(result.stage_events[0].status, "INVALID")

    def test_confirmation_boundaries_and_failure_reasons(self):
        raw, checked = n14_klines()
        midpoint = (Decimal(raw[118][1]) + Decimal(raw[118][4])) / Decimal("2")
        variants = {
            "N14_CONFIRMATION_NOT_BULLISH": lambda rows: rows[120].__setitem__(1, "99.96"),
            "N14_CONFIRMATION_CLOSE_NOT_ADVANCING": lambda rows: (
                rows[120].__setitem__(1, "99.79"),
                rows[120].__setitem__(4, "99.8"),
            ),
            "N14_CONFIRMATION_DID_NOT_BREAK_A_HIGH": lambda rows: rows[120].__setitem__(4, "99.9"),
            "N14_CONFIRMATION_SHOCK_MIDPOINT_NOT_RECLAIMED": lambda rows: (
                rows[119].__setitem__(2, "99.89"),
                rows[120].__setitem__(4, str(midpoint - Decimal("0.0001"))),
            ),
            "N14_CONFIRMATION_CLOSE_LOCATION_TOO_LOW": lambda rows: rows[120].__setitem__(4, "99.929"),
            "N14_CONFIRMATION_TAKER_BUY_RATIO_TOO_LOW": lambda rows: rows[120].__setitem__(10, "43.999"),
            "N14_CONFIRMATION_VOLUME_TOO_LOW": lambda rows: rows[120].__setitem__(7, "79.999"),
        }
        for reason, mutate in variants.items():
            with self.subTest(reason=reason):
                changed = deepcopy(raw)
                mutate(changed)
                o = Decimal(changed[120][1])
                c = Decimal(changed[120][4])
                changed[120][2] = str(max(Decimal(changed[120][2]), o, c))
                changed[120][3] = str(min(Decimal(changed[120][3]), o, c))
                result = analyze(changed, checked)
                if result.structure is not None:
                    failed_reasons = result.structure.failed_confirmation_reasons
                elif result.stage_events:
                    failed_reasons = result.stage_events[0].detail.get(
                        "failed_confirmation_reasons", []
                    )
                else:
                    failed_reasons = result.active_event.detail.get(
                        "failed_confirmation_reasons", []
                    )
                self.assertIn(reason, failed_reasons)

    def test_cascade_or_gate_and_context_recovery(self):
        raw, checked = n14_klines()
        base = scenario(raw, bullish_breadth_s=Decimal("0.25"))
        self.assertTrue(
            analyze(
                raw,
                checked,
                market_scenario=replace(base, bullish_breadth_c=Decimal("0.40")),
            ).passed
        )
        self.assertTrue(
            analyze(
                raw,
                checked,
                market_scenario=replace(base, bullish_breadth_c=Decimal("0.40")),
                cascade_breadth_min=Decimal("0.41"),
            ).passed
        )
        failed = analyze(
            raw,
            checked,
            market_scenario=replace(base, bullish_breadth_c=Decimal("0.399")),
        )
        self.assertEqual(failed.reason, "N14_MARKET_CASCADE_NOT_STABILIZED")
        self.assertTrue(failed.consume_current)
        incomplete = analyze(raw, checked, market_context_complete=False)
        self.assertEqual(incomplete.reason, "N14_MARKET_CONTEXT_INSUFFICIENT")
        self.assertFalse(incomplete.consume_current)

    def test_member_axis_gap_is_transient_rejection_not_a_batch_audit_failure(self):
        items, raws, checked = full_batch()
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            mismatched = deepcopy(raws)
            mismatched[items[-1].symbol][-1][0] += INTERVAL_MS
            scan_id = recorder.begin_scan(len(items), items, True)
            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": items},
                mismatched,
                checked_at_ms=checked,
            )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(len(result.signals), 100)
            self.assertEqual(
                {signal.reason for signal in result.signals},
                {"N14_MARKET_CONTEXT_INSUFFICIENT"},
            )
            aligned_scan = recorder.begin_scan(len(items), items, True)
            recovered = scheduler.evaluate(
                aligned_scan,
                {"quote_volume_top": items},
                raws,
                checked_at_ms=checked,
            )
            self.assertTrue(recovered.signal_batch_published)
            self.assertNotEqual(
                {signal.reason for signal in recovered.signals},
                {"N14_MARKET_CONTEXT_INSUFFICIENT"},
            )

    def test_entry_time_price_and_p_boundaries(self):
        raw, checked = n14_klines()
        base = analyze(raw, checked)
        lower = base.structure.entry_min_price
        upper = base.structure.entry_max_price
        for elapsed, price in ((0, lower), (119_999, upper)):
            with self.subTest(elapsed=elapsed, price=price):
                changed = deepcopy(raw)
                changed[-1][4] = str(price)
                changed[-1][2] = str(max(Decimal(changed[-1][2]), price))
                self.assertTrue(analyze(changed, changed[-1][0] + elapsed).passed)
        for elapsed in (-1, 120_000):
            self.assertEqual(
                analyze(raw, raw[-1][0] + elapsed).reason,
                "N14_ENTRY_WINDOW_EXPIRED",
            )
        waiting = deepcopy(raw)
        waiting[-1][4] = str(lower - Decimal("0.001"))
        self.assertEqual(analyze(waiting, checked).reason, "N14_WAITING_ENTRY_PRICE")
        extended = deepcopy(raw)
        extended[-1][4] = str(upper + Decimal("0.001"))
        extended[-1][2] = extended[-1][4]
        self.assertEqual(
            analyze(extended, checked).reason, "N14_ENTRY_PRICE_TOO_EXTENDED"
        )
        equal_p = deepcopy(raw)
        equal_p[-1][3] = "99.72"
        self.assertTrue(analyze(equal_p, checked).passed)
        below_p = deepcopy(raw)
        below_p[-1][3] = "99.719"
        self.assertEqual(analyze(below_p, checked).reason, "N14_ENTRY_LOW_BROKE_P")


class N14SnapshotAndSchedulerTests(unittest.TestCase):
    def test_snapshot_is_exact_top100_reranked_and_breadth_recomputed(self):
        items, raws, checked = full_batch()
        prefix = {symbol: rows[:120] for symbol, rows in raws.items()}
        payload, snapshot, current = build_n14_snapshot(
            N14_STRATEGY, items, prefix
        )
        self.assertEqual(current, prefix[items[0].symbol][-1][0])
        self.assertEqual(len(snapshot.rows), 100)
        self.assertEqual(set(row.rank_down for row in snapshot.rows.values()), set(range(1, 101)))
        self.assertEqual(snapshot.rows[items[0].symbol].quote_volume_rank, 1)
        self.assertEqual(decode_n14_snapshot(payload, "N14"), snapshot)
        self.assertEqual(
            n14_bullish_breadth_for_current_c(snapshot, raws, raws[items[0].symbol][-1][0]),
            Decimal("0.40"),
        )

    def test_snapshot_bad_payload_types_ranks_breadth_and_hash_fail_closed(self):
        items, raws, _ = full_batch()
        payload, _, _ = build_n14_snapshot(
            N14_STRATEGY,
            items,
            {symbol: rows[:120] for symbol, rows in raws.items()},
        )
        mutations = {
            "rows_99": lambda value: value["rows"].pop(),
            "extra": lambda value: value.__setitem__("extra", 1),
            "bool_rank": lambda value: value["rows"][0].__setitem__("rank_down", True),
            "duplicate_qv": lambda value: value["rows"][1].__setitem__("quote_volume_rank", 1),
            "wrong_rank": lambda value: value["rows"][0].__setitem__("rank_down", 100),
            "fake_breadth": lambda value: value["market"].__setitem__("red_breadth_s", "1"),
            "zero_atr": lambda value: value["rows"][0].__setitem__("atr_pct", "0"),
            "symbol_not_str": lambda value: value["rows"][0].__setitem__("symbol", 123),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                broken = deepcopy(payload)
                mutate(broken)
                resign_snapshot(broken)
                with self.assertRaises(ValueError):
                    decode_n14_snapshot(broken, "N14")
        bad_hash = deepcopy(payload)
        bad_hash["canonical_sha256"] = "0" * 64
        with self.assertRaises(ValueError):
            decode_n14_snapshot(bad_hash, "N14")

    def test_snapshot_recorder_is_exact_idempotent_and_strategy_scoped(self):
        items, raws, _ = full_batch()
        payload, snapshot, current = build_n14_snapshot(
            N14_STRATEGY,
            items,
            {symbol: rows[:120] for symbol, rows in raws.items()},
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            s_time = str(snapshot.s_open_time_ms)
            self.assertEqual(recorder.record_n14_market_snapshot("N14", s_time, payload), "OK")
            self.assertEqual(recorder.record_n14_market_snapshot("N14", s_time, payload), "OK")
            changed = deepcopy(payload)
            changed["rows"][0]["r1h"] = "9"
            resign_snapshot(changed)
            self.assertEqual(
                recorder.record_n14_market_snapshot("N14", s_time, changed),
                "N14_STATE_INCONSISTENT",
            )
            self.assertEqual(
                recorder.record_n14_market_snapshot("OTHER", s_time, changed),
                "N14_STATE_INCONSISTENT",
            )
            symbols = recorder.get_active_n14_snapshot_symbols("N14", current)
            self.assertEqual(symbols, set())
            self.assertEqual(
                recorder.get_required_n14_snapshot_symbols("N14", current),
                set(snapshot.rows),
            )

    def test_required_snapshot_symbols_reject_orphan_active_episode(self):
        items, raws, _ = full_batch()
        payload, snapshot, current = build_n14_snapshot(
            N14_STRATEGY,
            items,
            raws,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            self.assertEqual(
                recorder.record_n14_market_snapshot(
                    "N14", str(snapshot.s_open_time_ms), payload
                ),
                "OK",
            )
            orphan_s_time = current - 2 * INTERVAL_MS
            with recorder._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO n14_active_episodes(
                        strategy_id,symbol,s_time,stage,detail_json,
                        created_at,updated_at
                    ) VALUES('N14','ORPHANUSDT',?,'S_LOCKED','{}',?,?)
                    """,
                    (
                        str(orphan_s_time),
                        "2026-07-14T00:00:00+00:00",
                        "2026-07-14T00:00:00+00:00",
                    ),
                )
            with self.assertRaisesRegex(ValueError, "active snapshot missing"):
                recorder.get_required_n14_snapshot_symbols("N14", current)

    def test_required_snapshot_symbols_are_exact_latest_and_active_union(self):
        items, raws, _ = full_batch()
        latest_payload, latest_snapshot, current = build_n14_snapshot(
            N14_STRATEGY,
            items,
            raws,
        )
        older_member = candidate("OLDERUSDT", 100)
        older_items = [*items[:-1], older_member]
        older_raws = {
            item.symbol: deepcopy(raws[item.symbol][:-2])
            for item in items[:-1]
        }
        older_raws[older_member.symbol] = [
            kline(index) for index in range(len(next(iter(raws.values()))) - 2)
        ]
        older_payload, older_snapshot, older_current = build_n14_snapshot(
            N14_STRATEGY,
            older_items,
            older_raws,
        )
        self.assertEqual(older_current, current - 2 * INTERVAL_MS)
        self.assertEqual(
            older_snapshot.s_open_time_ms,
            current - 3 * INTERVAL_MS,
        )

        stale_member = candidate("STALEUSDT", 100)
        stale_items = [*items[:-1], stale_member]
        stale_raws = {
            item.symbol: deepcopy(raws[item.symbol][:-3])
            for item in items[:-1]
        }
        stale_raws[stale_member.symbol] = [
            kline(index) for index in range(len(next(iter(raws.values()))) - 3)
        ]
        stale_payload, stale_snapshot, _ = build_n14_snapshot(
            N14_STRATEGY,
            stale_items,
            stale_raws,
        )

        source_raw = raws[items[0].symbol][:-2]
        active_analysis = analyze(
            source_raw,
            source_raw[-1][0] + 30_000,
        )
        self.assertIsNotNone(active_analysis.active_event)
        active_event = active_analysis.active_event
        assert active_event is not None
        self.assertEqual(
            active_event.s_time, str(older_snapshot.s_open_time_ms)
        )
        active_record = {
            "strategy_id": "N14",
            "symbol": older_member.symbol,
            "s_time": active_event.s_time,
            "stage": active_event.status,
            "detail": _n14_active_envelope(
                "N14",
                older_member.symbol,
                active_event,
                older_snapshot.config_signature,
            ),
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            for payload, snapshot in (
                (latest_payload, latest_snapshot),
                (older_payload, older_snapshot),
                (stale_payload, stale_snapshot),
            ):
                self.assertEqual(
                    recorder.record_n14_market_snapshot(
                        "N14", str(snapshot.s_open_time_ms), payload
                    ),
                    "OK",
                )
            self.assertEqual(
                recorder.record_n14_state_batch_atomically([], [active_record]),
                "OK",
            )

            required = recorder.get_required_n14_snapshot_symbols(
                "N14", current
            )
            self.assertEqual(
                required,
                set(latest_snapshot.rows) | set(older_snapshot.rows),
            )
            self.assertIn(older_member.symbol, required)
            self.assertNotIn(stale_member.symbol, required)

    def test_main_fetches_dropped_latest_snapshot_member_without_active_episode(self):
        items, raws, _ = full_batch()
        payload, snapshot, current = build_n14_snapshot(
            N14_STRATEGY,
            items,
            raws,
        )
        dropped = items[-1]
        entrant = candidate("NEWUSDT", 100)
        rotated = [*items[:-1], entrant]
        entrant_raw = [kline(index) for index in range(len(next(iter(raws.values()))))]
        available_raws = {**raws, entrant.symbol: entrant_raw}

        class RotationMonitor:
            def scan_for_strategies(self, volume_top_n):
                if volume_top_n != 100:
                    raise AssertionError("N14 must request the exact top-100 batch")
                return StrategyMarketScan(100, [], rotated)

        class TrackingClient:
            def __init__(self):
                self.requested = []

            def get_klines(self, symbol):
                self.requested.append(symbol)
                return deepcopy(available_raws[symbol])

            def get_klines_for_interval(self, *args, **kwargs):
                return []

            def get_aggregate_trades(self, *args, **kwargs):
                return []

        class IdleTrader:
            def close_dry_run_position_if_triggered(self):
                return None

            def sync_state_with_exchange(self):
                return SyncResult(has_position=False)

            def open_long_plan_with_protection(self, _plan):
                raise AssertionError("this N14 restore regression must not trade")

        class IdlePaperTrader:
            def __init__(self):
                self.open_calls = 0

            def close_triggered_open_trades(self, *args, **kwargs):
                return []

            def open_trade(self, *args, **kwargs):
                self.open_calls += 1
                raise AssertionError("this N14 restore regression must not trade")

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            self.assertEqual(
                recorder.record_n14_market_snapshot(
                    "N14", str(snapshot.s_open_time_ms), payload
                ),
                "OK",
            )
            self.assertEqual(
                recorder.list_n14_active_snapshot_times("N14"), set()
            )
            self.assertNotIn(dropped.symbol, {item.symbol for item in rotated})
            self.assertEqual(
                len({item.symbol for item in rotated} | set(snapshot.rows)),
                101,
            )
            incomplete_without_dropped = scheduler._build_n14_batch_context(
                {"quote_volume_top": rotated, "negative_funding": []},
                {
                    item.symbol: deepcopy(available_raws[item.symbol])
                    for item in rotated
                },
                current + 30_000,
            )
            self.assertFalse(incomplete_without_dropped.complete)

            client = TrackingClient()
            paper_trader = IdlePaperTrader()
            state = StateStore(str(Path(tmpdir) / "state.json"))
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=True)
            bot.logger = logging.getLogger("n14_member_rotation_main")
            bot.client = client
            bot.recorder = recorder
            bot.paper_trader = paper_trader
            bot.monitor = RotationMonitor()
            bot.trader = IdleTrader()
            bot.strategy_scheduler = scheduler
            bot.strategies = (N14_STRATEGY,)
            bot.state = state

            contexts = []
            original_build_context = scheduler._build_n14_batch_context

            def capture_context(*args, **kwargs):
                context = original_build_context(*args, **kwargs)
                contexts.append(context)
                return context

            with patch.object(
                scheduler,
                "_build_n14_batch_context",
                side_effect=capture_context,
            ), patch(
                "trading_bot.main.time.time",
                return_value=(current + 30_000) / 1000,
            ):
                bot._run_once_multi_strategy()

            expected_fetch = {item.symbol for item in rotated} | set(snapshot.rows)
            self.assertEqual(set(client.requested), expected_fetch)
            self.assertEqual(len(client.requested), 101)
            self.assertEqual(client.requested.count(dropped.symbol), 1)
            self.assertEqual(len(contexts), 1)
            self.assertTrue(contexts[0].complete)
            self.assertIn(snapshot.s_open_time_ms, contexts[0].snapshots)
            self.assertEqual(
                len(contexts[0].snapshots[snapshot.s_open_time_ms].snapshot.rows),
                100,
            )
            current_scan_id = recorder.current_strategy_signal_scan_id()
            self.assertIsNotNone(current_scan_id)
            with recorder._connect() as connection:
                signal_rows = connection.execute(
                    "SELECT symbol,reason FROM strategy_signals "
                    "WHERE scan_id=? AND strategy_id='N14' ORDER BY symbol",
                    (current_scan_id,),
                ).fetchall()
                fetch_failures = connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type='strategy_kline_fetch_failed'"
                ).fetchone()[0]
            # The dropped frozen member remains in the single shared Kline
            # fetch and frozen-context reconstruction above, but it is not a
            # current Top100 candidate and therefore must not create a 101st
            # ordinary signal row or enter execution selection.
            self.assertEqual(len(signal_rows), 100)
            self.assertEqual(fetch_failures, 0)
            self.assertNotEqual(
                {reason for _symbol, reason in signal_rows},
                {"N14_MARKET_CONTEXT_INSUFFICIENT"},
            )
            self.assertNotIn(
                dropped.symbol, {symbol for symbol, _reason in signal_rows}
            )
            self.assertEqual(paper_trader.open_calls, 0)

    def test_batch_state_is_atomic_idempotent_conflict_detecting_and_retryable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            raw, _ = n14_klines()
            active_analysis = analyze(
                raw[:120], raw[119][0] + 30_000
            )
            self.assertIsNotNone(active_analysis.active_event)
            active_event = active_analysis.active_event
            failed_raw = deepcopy(raw[:121])
            failed_raw[119][7] = "79"
            terminal_analysis = analyze(
                failed_raw, failed_raw[-1][0] + 30_000
            )
            terminal_event = terminal_analysis.stage_events[0]
            terminal_envelope = _n14_terminal_envelope(
                "N14",
                "AUSDT",
                terminal_event.s_time,
                None,
                terminal_event.status,
                terminal_event.reason,
                "cfg",
                None,
                terminal_event.detail,
            )
            terminal = {
                "strategy_id": "N14",
                "symbol": "AUSDT",
                "s_time": terminal_event.s_time,
                "structure_id": None,
                "status": terminal_event.status,
                "reason": terminal_event.reason,
                "detail": terminal_envelope,
            }
            active_envelope = _n14_active_envelope(
                "N14", "BUSDT", active_event, "cfg"
            )
            active = {
                "strategy_id": "N14",
                "symbol": "BUSDT",
                "s_time": active_event.s_time,
                "stage": active_event.status,
                "detail": active_envelope,
            }
            with recorder._connect() as connection:
                connection.execute(
                    """
                    CREATE TRIGGER fail_n14_active BEFORE INSERT ON n14_active_episodes
                    WHEN NEW.symbol='BUSDT'
                    BEGIN SELECT RAISE(ABORT, 'forced active failure'); END
                    """
                )
                commit_test_schema_change(recorder, connection)
            self.assertEqual(
                recorder.record_n14_state_batch_atomically([terminal], [active]),
                "N14_STATE_PERSIST_FAILED",
            )
            with recorder._connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM n14_sell_impact_states").fetchone()[0], 0)
                connection.execute("DROP TRIGGER fail_n14_active")
                commit_test_schema_change(recorder, connection)
            self.assertEqual(recorder.record_n14_state_batch_atomically([terminal], [active]), "OK")
            self.assertEqual(recorder.record_n14_state_batch_atomically([terminal], [active]), "OK")
            conflict = {**terminal, "reason": "different"}
            self.assertEqual(
                recorder.record_n14_state_batch_atomically([conflict], []),
                "N14_STATE_INCONSISTENT",
            )

    def _run_three_scans(self, scheduler, items, raws):
        target_results = []
        for current_index in range(113, 122):
            batch = {
                symbol: rows[: current_index + 1]
                for symbol, rows in raws.items()
            }
            scan_id = scheduler.recorder.begin_scan(len(items), items, True)
            self.assertIsNotNone(scan_id)
            result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": items, "negative_funding": []},
                    batch,
                    checked_at_ms=batch[items[0].symbol][-1][0] + 30_000,
                )
            if current_index >= 119:
                target_results.append(result)
        return target_results

    def test_scheduler_freezes_s_stage_a_stage_passes_and_restart_consumes(self):
        items, raws, _ = full_batch()
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            staged = []
            for current_index in range(113, 122):
                batch = {
                    symbol: rows[: current_index + 1]
                    for symbol, rows in raws.items()
                }
                scan_id = recorder.begin_scan(len(items), items, True)
                self.assertIsNotNone(scan_id)
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": items, "negative_funding": []},
                    batch,
                    checked_at_ms=batch[items[0].symbol][-1][0] + 30_000,
                )
                if current_index >= 119:
                    staged.append(result)
                if current_index == 119:
                    active = recorder.get_validated_n14_active_episode(
                        "N14",
                        items[0].symbol,
                        str(raws[items[0].symbol][118][0]),
                    )
                    self.assertIsNotNone(active)
                    self.assertEqual(active["stage"], "S_LOCKED")
            first, second, third = staged
            self.assertEqual(first.signals[0].reason, "N14_ABSORPTION_PENDING")
            self.assertIn(second.signals[0].reason, {"N14_CONFIRMATION_PENDING", "N14_NOT_SHOCK"})
            self.assertEqual(len(third.passed_signals), 1)
            self.assertEqual(third.passed_signals[0].candidate.symbol, items[0].symbol)
            restarted = StrategyScheduler((N14_STRATEGY,), 96, recorder, logging.getLogger("n14_restart"))
            replay_scan_id = recorder.begin_scan(len(items), items, True)
            self.assertIsNotNone(replay_scan_id)
            replay = restarted.evaluate(
                replay_scan_id,
                {"quote_volume_top": items, "negative_funding": []},
                raws,
                checked_at_ms=raws[items[0].symbol][-1][0] + 30_000,
            )
            target = next(signal for signal in replay.signals if signal.candidate.symbol == items[0].symbol)
            self.assertEqual(target.reason, "N14_EPISODE_CONSUMED")
            self.assertEqual(replay.passed_signals, [])

    def test_signal_audit_failure_never_enters_execution_lists(self):
        items, raws, _ = full_batch()
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            self._run_three_scans(scheduler, items, raws)
            # Use a fresh database so the final episode is not already consumed.
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            self._run_three_scans(scheduler, items, raws)
            # Recreate one new current episode under another symbol and force audit loss.
            scan_id = recorder.begin_scan(len(items), items, True)
            self.assertIsNotNone(scan_id)
            with patch.object(
                recorder,
                "record_strategy_signals",
                return_value=StrategySignalBatchWriteResult(
                    failed_index=0
                ),
            ):
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": items, "negative_funding": []},
                    raws,
                    checked_at_ms=raws[items[0].symbol][-1][0] + 30_000,
                )
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])

    def test_only_top_sorted_n14_signal_reaches_plan_or_live(self):
        raw, checked = n14_klines()
        analyses = {}
        for symbol, rank, tb_c in (
            ("ZZZUSDT", 1, "44"),
            ("AAAUSDT", 2, "48"),
            ("MMMUSDT", 3, "46"),
        ):
            local = deepcopy(raw)
            local[120][10] = tb_c
            analyses[symbol] = analyze(
                local,
                checked,
                market_scenario=replace(scenario(local), quote_volume_rank=rank),
            )
        candidates = [candidate(symbol, rank) for rank, symbol in enumerate(analyses, 1)]
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            self.assertTrue(
                record_exact_live_result(
                    recorder, "N14", "XUSDT", "WIN", "2026-07-13T00:00:00+00:00"
                )
            )
            self.assertTrue(
                record_exact_live_result(
                    recorder, "N14", "YUSDT", "WIN", "2026-07-13T00:01:00+00:00"
                )
            )

            def decision(strategy, item, *args, **kwargs):
                analysis = analyses[item.symbol]
                return StrategySignalDecision(strategy, item, analysis, True, "PASSED", "PASSED")

            scan_id = recorder.begin_scan(len(candidates), candidates, True)
            self.assertIsNotNone(scan_id)
            with patch.object(scheduler, "_build_n14_batch_context", return_value=N14BatchContext({}, checked // INTERVAL_MS * INTERVAL_MS, True)), patch.object(scheduler, "_evaluate_candidate", side_effect=decision):
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": candidates, "negative_funding": []},
                    {},
                    checked_at_ms=checked,
                )
            self.assertEqual(len(result.signals), 3)
            self.assertEqual([item.candidate.symbol for item in result.passed_signals], ["AAAUSDT"])
            self.assertEqual([item.signal.candidate.symbol for item in result.live_candidates], ["AAAUSDT"])

    def test_legacy_cross_symbol_results_update_stats_without_qualification(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            for symbol in ("AUSDT", "BUSDT"):
                trade_id = recorder.open_strategy_paper_trade(
                    "N14", symbol, "100", "99", "105", "", {}, {}
                )
                recorder.close_strategy_paper_trade(
                    trade_id, "WIN", "TAKE_PROFIT", "105", "5"
                )
            state = recorder.get_strategy_state("N14")
            self.assertEqual(state.consecutive_wins, 0)
            self.assertEqual(state.paper_trade_count, 2)
            self.assertEqual(state.win_count, 2)
            self.assertEqual(state.loss_count, 0)
            self.assertFalse(state.live_eligible)
            self.assertTrue(
                record_exact_live_result(
                    recorder, "N14", "CUSDT", "LOSS", "2026-07-13T00:02:00+00:00"
                )
            )
            reset = recorder.get_strategy_state("N14")
            self.assertEqual(reset.consecutive_wins, 0)
            self.assertEqual(reset.paper_trade_count, 2)
            self.assertEqual(reset.win_count, 2)
            self.assertEqual(reset.loss_count, 0)
            self.assertFalse(reset.live_eligible)
            self.assertEqual(reset.last_trade_result, "LOSS")


class N14MainAndTraderTests(unittest.TestCase):
    def test_configuration_and_main_plan_use_s_snapshot_structure(self):
        by_id = {strategy.strategy_id: strategy for strategy in load_all_strategies()}
        strategy_ids = [
            strategy.strategy_id for strategy in load_all_strategies()
        ]
        self.assertEqual(
            strategy_ids[:20],
            [f"N{index:02d}" for index in range(1, 21)],
        )
        self.assertEqual(strategy_ids, [f"N{index:02d}" for index in range(1, 26)])
        payload = N14_STRATEGY.to_jsonable()
        self.assertEqual(payload["n14_market_1h_median_min"], "-0.0075")
        self.assertEqual(payload["n14_confirmation_max_bars"], 2)
        self.assertEqual(payload["n14_cascade_improvement_min"], "0.15")

        raw, checked = n14_klines()
        analysis = analyze(raw, checked)
        signal = StrategySignalDecision(
            N14_STRATEGY,
            candidate("N14USDT", 1),
            analysis,
            True,
            "PASSED",
            "PASSED",
        )

        class FakeTrader:
            def build_sell_pressure_decay_margin_capped_trade_plan(self, *args, **kwargs):
                self.args = (args, kwargs)
                return TradePlan(
                    symbol=args[0], leverage=50, quantity=Decimal("1"),
                    entry_price=args[1], stop_loss_price=Decimal("98"),
                    take_profit_price=Decimal("110"), stop_loss_pct=Decimal("0.02"),
                    take_profit_pct=Decimal("0.1"), amplitude_24h_pct=Decimal("0"),
                    high_24h_price=Decimal("0"), low_24h_price=Decimal("0"),
                    risk_amount=Decimal("2"), notional_value=Decimal("100"),
                    required_margin=Decimal("2"), balance=Decimal("1000"),
                    stop_mode="sell_pressure_decay_margin_capped",
                    structure_id=kwargs["structure_id"],
                    entry_min_price=kwargs["entry_min_price"],
                    entry_max_price=kwargs["entry_max_price"],
                )

        bot = TradingBot.__new__(TradingBot)
        bot.trader = FakeTrader()
        plan = bot._build_strategy_plan(signal)
        self.assertEqual(bot.trader.args[0][1], analysis.structure.entry.close)
        self.assertEqual(bot.trader.args[0][2], analysis.structure.p)
        self.assertEqual(plan.entry_deadline_ms, raw[-1][0] + 120_000)
        self.assertEqual(plan.structure_context["quote_volume_rank_at_s"], 1)

    def test_stop_bounds_low_leverage_and_five_r(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                RuleConstrainedClient(leverage=50),
                test_config(f"{tmpdir}/account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("n14_plan"),
            )
            plan = trader.build_sell_pressure_decay_margin_capped_trade_plan(
                "N14USDT", Decimal("100"), Decimal("99.5"), Decimal("5"),
                entry_min_price=Decimal("100"), entry_max_price=Decimal("101"),
            )
            self.assertEqual(plan.stop_loss_pct, Decimal("0.01"))
            self.assertGreaterEqual(
                (plan.take_profit_price - plan.entry_price)
                / (plan.entry_price - plan.stop_loss_price),
                Decimal("5"),
            )
            with self.assertRaisesRegex(BinanceAPIError, "N14_STOP_PCT_OUT_OF_RANGE"):
                trader.build_sell_pressure_decay_margin_capped_trade_plan(
                    "N14USDT", Decimal("100"), Decimal("95"), Decimal("5"),
                    entry_min_price=Decimal("100"), entry_max_price=Decimal("101"),
                )
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                RuleConstrainedClient(leverage=1),
                test_config(f"{tmpdir}/account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("n14_low_leverage"),
            )
            plan = trader.build_sell_pressure_decay_margin_capped_trade_plan(
                "N14USDT", Decimal("100"), Decimal("99.5"), Decimal("5"),
                entry_min_price=Decimal("100"), entry_max_price=Decimal("101"),
            )
            self.assertEqual(plan.quantity, Decimal("9.5"))
            self.assertTrue(plan.risk_capped_by_margin)

    def test_actual_fill_outside_range_emergency_closes_without_protection(self):
        entry_min = Decimal("100")
        entry_max = Decimal("101")
        allowed_min = entry_min * Decimal("0.995")
        allowed_max = entry_max * Decimal("1.005")
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                FakeLiveExecutionClient({}, leverage=50),
                live_test_config(f"{tmpdir}/account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("n14_boundary"),
            )
            plan = trader.build_sell_pressure_decay_margin_capped_trade_plan(
                "N14USDT", Decimal("100"), Decimal("99"), Decimal("5"),
                entry_min_price=entry_min, entry_max_price=entry_max,
            )
            trader._assert_structured_actual_entry_range(plan, allowed_min)
            trader._assert_structured_actual_entry_range(plan, allowed_max)

        for actual in (
            allowed_min - Decimal("0.001"),
            allowed_max + Decimal("0.001"),
        ):
            with self.subTest(actual=actual), tempfile.TemporaryDirectory() as tmpdir:
                client = FakeLiveExecutionClient({}, leverage=50)
                state_store = StateStore(f"{tmpdir}/state.json")
                trader = Trader(
                    client,
                    live_test_config(f"{tmpdir}/account.json"),
                    state_store,
                    logging.getLogger("n14_live"),
                )
                plan = trader.build_sell_pressure_decay_margin_capped_trade_plan(
                    "N14USDT", Decimal("100"), Decimal("99"), Decimal("5"),
                    entry_min_price=entry_min, entry_max_price=entry_max,
                )
                client.open_response = {
                    "orderId": 1401,
                    "avgPrice": str(actual),
                    "executedQty": str(plan.quantity),
                }
                client.position_quantities = [str(plan.quantity), "0"]
                client.close_side_effects = [{"status": "FILLED", "executedQty": str(plan.quantity)}]
                with self.assertRaisesRegex(
                    BinanceAPIError, "N14_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE"
                ):
                    trader.open_long_plan_with_protection(plan)
                self.assertEqual(client.protection_calls, [])
                stored = state_store.load()
                self.assertIsNotNone(stored)
                assert stored is not None
                self.assertEqual(stored.quantity, "0")
                self.assertEqual(stored.orders["strategy"]["strategy_id"], "N14")
                self.assertTrue(
                    stored.orders["execution_cleanup_resolved"]["emergency_cleanup"][
                        "confirmed_closed"
                    ]
                )
                self.assertTrue(
                    stored.orders["execution_cleanup_resolved"]["protection_cleanup"][
                        "confirmed_all_canceled"
                    ]
                )
                self.assertTrue(trader.sync_state_with_exchange().pending_resolved)
                self.assertIsNotNone(state_store.load())

    def test_paper_and_live_deadline_are_hard_gates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            plan = TradePlan(
                symbol="N14USDT", leverage=1, quantity=Decimal("1"),
                entry_price=Decimal("100"), stop_loss_price=Decimal("99"),
                take_profit_price=Decimal("105"), stop_loss_pct=Decimal("0.01"),
                take_profit_pct=Decimal("0.05"), amplitude_24h_pct=Decimal("0"),
                high_24h_price=Decimal("0"), low_24h_price=Decimal("0"),
                risk_amount=Decimal("1"), notional_value=Decimal("100"),
                required_margin=Decimal("100"), balance=Decimal("1000"),
                stop_mode="sell_pressure_decay_margin_capped", entry_deadline_ms=120_000,
            )
            paper = PaperTrader(recorder, logging.getLogger("n14_paper"), clock_ms=lambda: 120_000)
            with self.assertRaises(EntryWindowExpiredError):
                paper.open_trade("N14", "N14USDT", None, plan, {})


if __name__ == "__main__":
    unittest.main()
