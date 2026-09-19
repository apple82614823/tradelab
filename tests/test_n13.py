from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import logging
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from tests.recorder_test_utils import (
    commit_test_schema_change,
    make_test_recorder,
)
from trading_bot.binance_client import BinanceAPIError
from trading_bot.main import TradingBot
from trading_bot.monitor import FundingCandidate
from trading_bot.n13_analyzer import (
    N13AnalysisResult,
    N13Event,
    analyze_n13_vwap_rotation,
    n13_metrics,
    parse_n13_klines,
)
from trading_bot.paper_trader import PaperTrader
from trading_bot.recorder import ReviewRecorder
from trading_bot.state import PositionState, StateStore
from trading_bot.strategies import N13_STRATEGY, load_all_strategies
from trading_bot.strategy_scheduler import (
    N12BatchContext,
    N13BatchContext,
    N13MarketRow,
    StrategyScheduler,
    StrategySignalDecision,
    _n13_existing_event_state_match,
)
from trading_bot.trader import TradePlan, Trader
from tests.test_trader import (
    FakeLiveExecutionClient,
    RuleConstrainedClient,
    live_test_config,
    test_config,
)


BASE_TIME_MS = 1_730_000_700_000
INTERVAL_MS = 900_000
N13_STATE_COLUMNS = (
    "id",
    "strategy_id",
    "symbol",
    "t_time",
    "structure_id",
    "status",
    "reason",
    "detail_json",
    "created_at",
    "updated_at",
)


def kline(
    index,
    open_price,
    high,
    low,
    close,
    *,
    base_volume="1",
    quote_volume=None,
    taker_ratio="0.55",
):
    base = Decimal(str(base_volume))
    quote = (
        Decimal(str(quote_volume))
        if quote_volume is not None
        else Decimal(str(close)) * base
    )
    taker = quote * Decimal(str(taker_ratio))
    open_time = BASE_TIME_MS + index * INTERVAL_MS
    return [
        open_time,
        str(open_price),
        str(high),
        str(low),
        str(close),
        str(base),
        open_time + INTERVAL_MS - 1,
        str(quote),
        "1",
        "1",
        str(taker),
        "0",
    ]


def n13_klines():
    rows = [
        kline(index, "100", "100.5", "99.5", "100")
        for index in range(96)
    ]
    rows.append(kline(96, "101.2", "102.2", "101", "102"))
    rows.extend(
        kline(index, "100.9", "101.3", "100.7", "101")
        for index in range(97, 103)
    )
    rows.append(kline(103, "100.4", "101.6", "100.2", "101.4"))
    rows.append(kline(104, "101.4", "101.8", "101.2", "101.5"))
    return rows, BASE_TIME_MS + 104 * INTERVAL_MS + 30_000


def flat_n13_klines():
    rows = [
        kline(index, "100", "100.5", "99.5", "100")
        for index in range(105)
    ]
    return rows, BASE_TIME_MS + 104 * INTERVAL_MS + 30_000


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


def analyze(raw, checked, **kwargs):
    return analyze_n13_vwap_rotation(
        "S011USDT",
        raw,
        return_rank=kwargs.pop("return_rank", 11),
        return_24h=kwargs.pop("return_24h", Decimal("0.014")),
        positive_return_breadth=kwargs.pop(
            "positive_return_breadth", Decimal("1")
        ),
        above_vwap_breadth=kwargs.pop(
            "above_vwap_breadth", Decimal("1")
        ),
        snapshot_context_complete=kwargs.pop("snapshot_context_complete", True),
        checked_at_ms=checked,
        **kwargs,
    )


def recorder_scheduler(tmpdir, strategy=N13_STRATEGY):
    recorder = make_test_recorder(
        str(Path(tmpdir) / "review.sqlite3"),
        logging.getLogger("n13"),
    )
    recorder.upsert_strategy_definitions((strategy,))
    return recorder, StrategyScheduler(
        (strategy,), 96, recorder, logging.getLogger("n13")
    )


def full_batch():
    raw, checked = n13_klines()
    items = top100()
    return items, {item.symbol: deepcopy(raw) for item in items}, checked


def n13_current_a_switch_windows():
    """Natural 122-bar current->historical replay that selects a new A."""
    raw, _ = n13_klines()
    first = [
        kline(index, "100", "100.5", "99.5", "100")
        for index in range(-17, 0)
    ] + raw
    first[0] = kline(-17, "100", "200", "1", "100")
    first[17 + 95] = kline(95, "101", "101.2", "100.264", "101")
    second = deepcopy(first[1:])
    second.append(kline(105, "101.5", "101.9", "101.3", "101.6"))
    third = deepcopy(second[1:])
    third.append(kline(106, "101.6", "102", "101.4", "101.7"))
    return (
        (first, BASE_TIME_MS + 104 * INTERVAL_MS + 30_000),
        (second, BASE_TIME_MS + 105 * INTERVAL_MS + 30_000),
        (third, BASE_TIME_MS + 106 * INTERVAL_MS + 30_000),
    )


def n13_historical_a_switch_windows():
    """Natural 122-bar historical replay that selects a new A."""
    raw, _ = n13_klines()
    first = [
        kline(index, "100", "100.5", "99.5", "100")
        for index in range(-16, 0)
    ] + raw
    first.append(kline(105, "101.5", "101.9", "101.3", "101.6"))
    first[0] = kline(-16, "100", "200", "1", "100")
    first[16 + 95] = kline(95, "101", "101.2", "100.264", "101")
    second = deepcopy(first[1:])
    second.append(kline(106, "101.6", "102", "101.4", "101.7"))
    third = deepcopy(second[1:])
    third.append(kline(107, "101.7", "102.1", "101.5", "101.8"))
    return (
        (first, BASE_TIME_MS + 105 * INTERVAL_MS + 30_000),
        (second, BASE_TIME_MS + 106 * INTERVAL_MS + 30_000),
        (third, BASE_TIME_MS + 107 * INTERVAL_MS + 30_000),
    )


def n13_t_stage_reason_switch_windows():
    """Natural 122-bar NOT_FOUND->BAND replay for one immutable T."""
    raw, _ = n13_klines()
    first = [
        kline(index, "100", "100.5", "99.5", "100")
        for index in range(-17, 0)
    ] + raw
    first[1] = kline(-16, "100", "101", "99", "100")
    first[17 + 95] = kline(95, "101", "101.2", "100.264", "101")
    for index, close in (
        (100, "99.510"),
        (101, "99.55"),
        (102, "99.60"),
        (103, "99.65"),
    ):
        first[17 + index] = kline(
            index,
            "99.8",
            "100.1",
            "99.4",
            close,
            taker_ratio="0.49",
        )
    second = deepcopy(first[1:])
    second.append(kline(105, "100", "100.5", "99.5", "100"))
    third = deepcopy(second[1:])
    third.append(kline(106, "100", "100.5", "99.5", "100"))
    return (
        (first, BASE_TIME_MS + 104 * INTERVAL_MS + 30_000),
        (second, BASE_TIME_MS + 105 * INTERVAL_MS + 30_000),
        (third, BASE_TIME_MS + 106 * INTERVAL_MS + 30_000),
    )


def n13_t_stage_reverse_reason_switch_windows():
    """Natural 122-bar BAND->NOT_FOUND replay for one immutable T."""
    windows = []
    for rows, checked in n13_current_a_switch_windows()[:2]:
        rows = deepcopy(rows)
        for absolute_index in range(100, 104):
            open_time_ms = BASE_TIME_MS + absolute_index * INTERVAL_MS
            local_index = next(
                index
                for index, row in enumerate(rows)
                if row[0] == open_time_ms
            )
            rows[local_index] = kline(
                absolute_index,
                "99.7",
                "100.4",
                "99.4",
                "99.508",
            )
        windows.append((rows, checked))
    return tuple(windows)


def n13_current_terminal_to_value_band_windows():
    """Natural current structure -> stage replay after the ATR seed slides."""
    windows = []
    for rows, checked in n13_t_stage_reason_switch_windows()[:2]:
        rows = deepcopy(rows)
        c_index = next(
            index
            for index, row in enumerate(rows)
            if row[0] == BASE_TIME_MS + 103 * INTERVAL_MS
        )
        rows[c_index] = kline(
            103,
            "99.6",
            "100.25",
            "99.4",
            "100.2",
            taker_ratio="0.60",
        )
        windows.append((rows, checked))
    return tuple(windows)


def legacy_n13_state_detail(value):
    """Emulate the pre-fix persisted event format: only indexes were removed."""
    if isinstance(value, dict):
        return {
            key: legacy_n13_state_detail(item)
            for key, item in value.items()
            if key != "index"
        }
    if isinstance(value, (list, tuple)):
        return [legacy_n13_state_detail(item) for item in value]
    return value


def resign_n13_t_stage_detail(detail):
    unsigned = legacy_n13_state_detail(
        {
            key: value
            for key, value in detail.items()
            if key != "canonical_sha256"
        }
    )
    detail["canonical_sha256"] = hashlib.sha256(
        json.dumps(
            unsigned,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return detail


def pre_fix_n13_current_terminal_envelope(
    strategy_id,
    symbol,
    t_time,
    structure,
    status,
    reason,
):
    """Independent serializer matching the pre-fix schema-v1 writer."""
    evidence = {
        "structure_id": structure.structure_id,
        "a": legacy_n13_state_detail(structure.a.json()),
        "t": legacy_n13_state_detail(structure.t.json()),
        "c": legacy_n13_state_detail(structure.c.json()),
        "p": str(structure.p),
        "vwap_c": str(structure.vwap_c),
        "atr_c": str(structure.atr_c),
        "entry_min_price": str(structure.entry_min_price),
        "entry_max_price": str(structure.entry_max_price),
        "zone_lower": str(structure.zone_lower),
        "zone_upper": str(structure.zone_upper),
        "armed_expiry_time_ms": structure.armed_expiry_time_ms,
        "failed_confirmation_reasons": list(
            structure.failed_confirmation_reasons
        ),
    }
    unsigned = {
        "schema_version": 1,
        "strategy_id": strategy_id,
        "symbol": symbol,
        "t_time": t_time,
        "structure_id": structure.structure_id,
        "status": status,
        "reason": reason,
        "evidence": evidence,
    }
    canonical = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        **unsigned,
        "canonical_sha256": hashlib.sha256(
            canonical.encode("utf-8")
        ).hexdigest(),
    }


def official_n13_structure_id(symbol, t_time, c_time):
    return hashlib.sha256(
        f"{symbol}|{t_time}|{c_time}".encode("utf-8")
    ).hexdigest()[:24]


def n13_full_row_snapshot(connection, strategy_id, symbol, t_time):
    columns = tuple(
        row[1]
        for row in connection.execute(
            "PRAGMA table_info(n13_rotation_states)"
        ).fetchall()
    )
    row = connection.execute(
        """
        SELECT * FROM n13_rotation_states
        WHERE strategy_id=? AND symbol=? AND t_time=?
        """,
        (strategy_id, symbol, t_time),
    ).fetchone()
    count = connection.execute(
        "SELECT COUNT(*) FROM n13_rotation_states"
    ).fetchone()[0]
    return columns, row, count


def paper_plan(symbol):
    return TradePlan(
        symbol=symbol,
        leverage=50,
        quantity=Decimal("10"),
        entry_price=Decimal("100"),
        stop_loss_price=Decimal("99"),
        take_profit_price=Decimal("105"),
        stop_loss_pct=Decimal("0.01"),
        take_profit_pct=Decimal("0.05"),
        amplitude_24h_pct=Decimal("0"),
        high_24h_price=Decimal("0"),
        low_24h_price=Decimal("0"),
        risk_amount=Decimal("100"),
        notional_value=Decimal("1000"),
        required_margin=Decimal("20"),
        balance=Decimal("1000"),
        stop_mode="vwap_rotation_margin_capped",
    )


def close_paper_win(paper):
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000) + 180_000

    def get_klines(symbol, start_time_ms):
        return [[
            start_time_ms,
            "100",
            "105",
            "100",
            "101",
            "0",
            start_time_ms + 59_999,
        ]]

    return paper.close_triggered_open_trades(
        get_klines,
        lambda symbol, start_time_ms, end_time_ms: [],
        now_ms=now_ms,
    )


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


class N13AnalyzerTests(unittest.TestCase):
    def test_complete_structure_uses_frozen_a_zone_first_touch_and_c(self):
        raw, checked = n13_klines()
        result = analyze(raw, checked)

        self.assertTrue(result.passed, result.reason)
        structure = result.structure
        self.assertEqual(
            (structure.a.index, structure.t.index, structure.c.index),
            (96, 103, 103),
        )
        metrics = n13_metrics(parse_n13_klines(raw[:-1]))
        self.assertEqual(structure.zone_lower, metrics[96].lower)
        self.assertEqual(structure.zone_upper, metrics[96].upper)
        self.assertNotEqual(structure.zone_upper, metrics[103].upper)
        self.assertEqual(structure.p, Decimal("100.2"))
        self.assertEqual(structure.entry_min_price, structure.c.close)
        self.assertEqual(
            structure.entry_max_price,
            structure.c.close + Decimal("0.5") * structure.atr_c,
        )
        self.assertEqual(
            structure.armed_expiry_time_ms,
            structure.a.open_time_ms + 8 * INTERVAL_MS,
        )

    def test_exact_vwap_wilder_atr_and_rising_reference(self):
        raw, checked = n13_klines()
        candles = parse_n13_klines(raw[:-1])
        metrics = n13_metrics(candles)
        self.assertEqual(metrics[95].vwap, Decimal("100"))
        self.assertEqual(metrics[95].atr, Decimal("1"))
        expected_vwap = (
            sum((bar.quote_volume for bar in candles[8:104]), Decimal("0"))
            / sum((bar.base_volume for bar in candles[8:104]), Decimal("0"))
        )
        self.assertEqual(metrics[103].vwap, expected_vwap)
        self.assertGreater(metrics[103].vwap, metrics[95].vwap)
        result = analyze(raw, checked)
        self.assertEqual(
            result.vwap_slope_reference,
            metrics[95].vwap,
        )

    def test_permanent_gate_summary_rejects_unbounded_scalar_payloads(self):
        raw, checked = n13_klines()
        result = analyze(raw, checked)
        self.assertTrue(result.passed, result.reason)
        self.assertIsNotNone(result.structure)
        self.assertIsNotNone(result.structure.entry)
        hostile_text = "N13_AUDIT_ATTACK" * 1_000
        hostile = replace(
            result,
            structure=replace(
                result.structure,
                entry=replace(result.structure.entry, low=hostile_text),
                positive_return_breadth=hostile_text,
                above_vwap_breadth=hostile_text,
            ),
            vwap_slope_reference=hostile_text,
        )
        detail = hostile.detail_json()
        structure = detail["structure"]
        self.assertEqual(structure["entry_low"], "INVALID")
        self.assertEqual(
            structure["positive_return_breadth"],
            "INVALID",
        )
        self.assertEqual(
            structure["above_vwap_breadth"],
            "INVALID",
        )
        self.assertEqual(
            structure["vwap_slope_reference"],
            "INVALID",
        )
        encoded = json.dumps(
            detail,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        self.assertNotIn("N13_AUDIT_ATTACK", encoded)
        self.assertNotIn("metric_context_bars", encoded)
        self.assertLessEqual(len(encoded.encode("utf-8")), 2048)

    def test_t_is_allowed_at_a_plus_eight_and_next_bar_expires(self):
        raw, checked = n13_klines()
        raw[95] = kline(95, "101.2", "102.2", "101", "102")
        raw[96] = kline(96, "100.9", "101.3", "100.7", "101")

        at_eight = analyze(raw, checked)
        self.assertTrue(at_eight.passed, at_eight.reason)
        self.assertEqual((at_eight.structure.a.index, at_eight.structure.t.index), (95, 103))

        expired = analyze(raw, checked, armed_max_bars=7)
        self.assertFalse(expired.passed)
        self.assertEqual(expired.reason, "N13_ARMED_WINDOW_EXPIRED")
        self.assertTrue(expired.stage_events)

    def test_first_touch_continues_until_first_complete_confirmation(self):
        raw, checked = n13_klines()
        raw[100] = kline(100, "100.2", "100.4", "99.7", "99.8")
        raw[101] = kline(101, "100.7", "101", "100", "100.6")
        raw[102] = kline(
            102, "100.5", "101.1", "100.4", "101", taker_ratio="0.49"
        )

        result = analyze(raw, checked)
        self.assertTrue(result.passed, result.reason)
        self.assertEqual((result.structure.t.index, result.structure.c.index), (100, 103))
        self.assertEqual(result.structure.p, Decimal("99.7"))

    def test_flat_touch_bar_is_not_c_and_later_complete_c_can_pass(self):
        raw, checked = n13_klines()
        raw[100] = kline(100, "100", "100", "100", "100")

        result = analyze(raw, checked)

        self.assertTrue(result.passed, result.reason)
        self.assertEqual((result.structure.t.index, result.structure.c.index), (100, 103))
        self.assertIn(
            "N13_CONFIRMATION_FLAT_CANDLE",
            result.structure.failed_confirmation_reasons,
        )

    def test_confirmation_location_and_taker_boundaries_are_inclusive(self):
        raw, checked = n13_klines()
        raw[103] = kline(
            103,
            "100.8",
            "102.2",
            "100.2",
            "101.4",
            taker_ratio="0.50",
        )
        result = analyze(raw, checked)
        self.assertTrue(result.passed, result.reason)
        self.assertEqual(
            (result.structure.c.close - result.structure.c.low)
            / (result.structure.c.high - result.structure.c.low),
            Decimal("0.6"),
        )

        self.assertFalse(
            analyze(
                raw,
                checked,
                confirmation_close_location_min=Decimal("0.6001"),
            ).passed
        )
        self.assertFalse(
            analyze(
                raw,
                checked,
                confirmation_taker_buy_ratio_min=Decimal("0.5001"),
            ).passed
        )

    def test_only_close_below_frozen_lower_breaks_confirmation_episode(self):
        raw, checked = n13_klines()
        raw[100] = kline(100, "100.2", "100.4", "99.7", "99.8")
        raw[101] = kline(101, "100.7", "101", "100", "100.6")
        raw[102] = kline(
            102, "100.5", "101.1", "100.4", "101", taker_ratio="0.49"
        )
        continued = analyze(raw, checked)
        self.assertTrue(continued.passed, continued.reason)

        broken = deepcopy(raw)
        broken[100] = kline(100, "100", "100.4", "99.2", "99.4")
        for index in range(101, 104):
            broken[index] = kline(index, "100", "100.4", "99.5", "100")
        result = analyze(broken, checked)
        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "N13_VALUE_BAND_BROKEN")
        self.assertTrue(result.stage_events)

    def test_frozen_lower_and_current_vwap_closed_boundaries_pass(self):
        raw, checked = n13_klines()
        metrics = n13_metrics(parse_n13_klines(raw[:-1]))
        frozen_lower = metrics[96].lower
        frozen_upper = metrics[96].upper
        raw[100] = kline(
            100,
            frozen_lower + Decimal("0.1"),
            frozen_upper,
            frozen_lower - Decimal("0.1"),
            frozen_lower,
        )
        raw[101] = kline(101, "100.7", "101", "100", "100.6")
        raw[102] = kline(
            102, "100.5", "101.1", "100.4", "101", taker_ratio="0.49"
        )
        equal_lower = analyze(raw, checked)
        self.assertTrue(equal_lower.passed, equal_lower.reason)
        self.assertEqual(equal_lower.structure.t.index, 100)
        self.assertEqual(equal_lower.structure.c.index, 103)

        vwap_equal, checked = n13_klines()
        prior_quote = sum(
            (Decimal(str(vwap_equal[index][7])) for index in range(8, 103)),
            Decimal("0"),
        )
        c_quote = Decimal("101.4") * Decimal("96") - prior_quote
        vwap_equal[103][7] = str(c_quote)
        vwap_equal[103][10] = str(c_quote * Decimal("0.50"))
        equal_vwap = analyze(vwap_equal, checked)
        self.assertTrue(equal_vwap.passed, equal_vwap.reason)
        self.assertEqual(equal_vwap.structure.c.close, equal_vwap.structure.vwap_c)

    def test_value_zone_skip_pending_and_confirmation_window_are_explicit(self):
        rows, checked = flat_n13_klines()
        rows[96] = kline(96, "101.2", "102.2", "101", "102")
        metrics = n13_metrics(parse_n13_klines(rows[:-1]))
        lower = metrics[96].lower
        rows[97] = kline(
            97,
            lower - Decimal("0.10"),
            lower - Decimal("0.01"),
            lower - Decimal("0.20"),
            lower - Decimal("0.10"),
        )
        skipped = analyze(rows, checked)
        self.assertEqual(skipped.reason, "N13_VALUE_ZONE_SKIPPED")
        self.assertIn(
            "N13_VALUE_ZONE_SKIPPED",
            {event.reason for event in skipped.stage_events},
        )

        pending, checked = n13_klines()
        pending[103] = kline(103, "101.3", "101.6", "100.7", "101.2")
        waiting = analyze(pending, checked)
        self.assertEqual(waiting.reason, "N13_VALUE_TOUCH_PENDING")
        self.assertFalse(waiting.consume_current)

        four, checked = n13_klines()
        four[100] = kline(100, "100.2", "100.4", "99.7", "99.8")
        four[101] = kline(101, "100.7", "101", "100", "100.6")
        four[102] = kline(
            102, "100.5", "101.1", "100.4", "101", taker_ratio="0.49"
        )
        self.assertTrue(analyze(four, checked, confirmation_max_bars=4).passed)
        three = analyze(four, checked, confirmation_max_bars=3)
        self.assertEqual(three.reason, "N13_CONFIRMATION_NOT_FOUND")
        self.assertTrue(three.stage_events)

    def test_terminal_episode_rearms_only_from_later_a_and_new_structure_passes(self):
        rows = [
            kline(index, "100", "100.5", "99.5", "100")
            for index in range(96)
        ]
        rows.append(kline(96, "101.2", "102.2", "101", "102"))
        rows.extend(
            kline(index, "100.2", "100.5", "99.8", "100")
            for index in range(97, 101)
        )
        rows.append(kline(101, "101.2", "102.2", "101", "102"))
        rows.extend(
            kline(index, "100.9", "101.3", "100.7", "101")
            for index in range(102, 109)
        )
        rows.append(kline(109, "100.4", "101.6", "100.2", "101.4"))
        rows.append(kline(110, "101.4", "101.8", "101.2", "101.5"))

        result = analyze(
            rows,
            BASE_TIME_MS + 110 * INTERVAL_MS + 30_000,
        )

        self.assertTrue(result.passed, result.reason)
        self.assertIn(
            "N13_CONFIRMATION_NOT_FOUND",
            {event.reason for event in result.stage_events},
        )
        self.assertEqual(
            (
                result.structure.a.index,
                result.structure.t.index,
                result.structure.c.index,
            ),
            (101, 109, 109),
        )

    def test_entry_price_time_and_p_boundaries(self):
        raw, checked = n13_klines()
        base = analyze(raw, checked)
        maximum = base.structure.entry_max_price

        lower = deepcopy(raw)
        lower[-1] = kline(104, "101.4", "101.6", "100.2", "101.4")
        self.assertTrue(analyze(lower, checked).passed)

        upper = deepcopy(raw)
        upper[-1] = kline(
            104,
            "101.4",
            maximum + Decimal("0.1"),
            "100.2",
            maximum,
        )
        self.assertTrue(analyze(upper, checked).passed)

        waiting = deepcopy(raw)
        waiting[-1] = kline(104, "101.3", "101.4", "100.2", "101.3")
        waiting_result = analyze(waiting, checked)
        self.assertEqual(waiting_result.reason, "N13_WAITING_ENTRY_PRICE")
        self.assertFalse(waiting_result.consume_current)

        too_high = deepcopy(upper)
        too_high[-1] = kline(
            104,
            "101.4",
            maximum + Decimal("0.2"),
            "100.2",
            maximum + Decimal("0.01"),
        )
        too_high_result = analyze(too_high, checked)
        self.assertEqual(too_high_result.reason, "N13_ENTRY_PRICE_TOO_EXTENDED")
        self.assertTrue(too_high_result.consume_current)

        below_p = deepcopy(raw)
        below_p[-1] = kline(104, "101.4", "101.8", "100.19", "101.5")
        self.assertEqual(analyze(below_p, checked).reason, "N13_ENTRY_LOW_BROKE_P")

        entry_time = int(raw[-1][0])
        self.assertTrue(analyze(raw, entry_time).passed)
        self.assertTrue(analyze(raw, entry_time + 119_999).passed)
        self.assertEqual(
            analyze(raw, entry_time - 1).reason,
            "N13_ENTRY_WINDOW_EXPIRED",
        )
        self.assertEqual(
            analyze(raw, entry_time + 120_000).reason,
            "N13_ENTRY_WINDOW_EXPIRED",
        )
        tighter = analyze(
            raw,
            checked,
            entry_extension_atr_max=Decimal("0.01"),
        )
        self.assertEqual(tighter.reason, "N13_ENTRY_PRICE_TOO_EXTENDED")

    def test_market_filters_apply_only_after_complete_c_and_consume(self):
        raw, checked = n13_klines()
        for kwargs, reason in (
            ({"positive_return_breadth": Decimal("0.599")}, "N13_MARKET_BREADTH_NOT_MET"),
            ({"above_vwap_breadth": Decimal("0.499")}, "N13_MARKET_BREADTH_NOT_MET"),
            ({"return_24h": Decimal("0")}, "N13_RETURN_NOT_POSITIVE"),
            ({"return_rank": 10}, "N13_RETURN_RANK_OUT_OF_RANGE"),
            ({"return_rank": 61}, "N13_RETURN_RANK_OUT_OF_RANGE"),
        ):
            with self.subTest(reason=reason, kwargs=kwargs):
                result = analyze(raw, checked, **kwargs)
                self.assertEqual(result.reason, reason)
                self.assertTrue(result.consume_current)
                self.assertIsNotNone(result.structure)

        self.assertTrue(
            analyze(
                raw,
                checked,
                positive_return_breadth=Decimal("0.60"),
                above_vwap_breadth=Decimal("0.50"),
                return_rank=11,
            ).passed
        )
        self.assertTrue(analyze(raw, checked, return_rank=60).passed)

        flat, flat_checked = flat_n13_klines()
        no_structure = analyze(
            flat,
            flat_checked,
            positive_return_breadth=Decimal("0"),
            above_vwap_breadth=Decimal("0"),
        )
        self.assertEqual(no_structure.reason, "N13_NOT_ARMED")
        self.assertFalse(no_structure.consume_current)

    def test_vwap_must_rise_and_incomplete_snapshot_never_consumes(self):
        raw, checked = n13_klines()
        fixed_vwap = deepcopy(raw)
        for row in fixed_vwap:
            row[7] = "100"
            row[10] = "55"
        not_rising = analyze(fixed_vwap, checked)
        self.assertEqual(not_rising.reason, "N13_VWAP_NOT_RISING")
        self.assertTrue(not_rising.consume_current)

        incomplete = analyze(raw, checked, snapshot_context_complete=False)
        self.assertEqual(
            incomplete.reason,
            "N13_RELATIVE_RANK_CONTEXT_INSUFFICIENT",
        )
        self.assertFalse(incomplete.consume_current)
        self.assertIsNone(incomplete.structure)

    def test_historical_structure_is_missed_without_borrowing_current_context(self):
        raw, _ = n13_klines()
        raw.append(kline(105, "101.5", "101.9", "101.3", "101.6"))
        checked = BASE_TIME_MS + 105 * INTERVAL_MS + 30_000
        result = analyze(raw, checked, return_rank=37)

        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "HISTORICAL_N13_ENTRY_MISSED")
        self.assertTrue(result.historical_events)
        structure = result.historical_events[-1].structure
        self.assertIsNone(structure.return_rank)
        self.assertIsNone(structure.return_24h)
        self.assertIsNone(structure.positive_return_breadth)
        self.assertIsNone(structure.above_vwap_breadth)
        self.assertFalse(structure.snapshot_context_available)

    def test_identity_depends_only_on_symbol_t_and_c(self):
        raw, checked = n13_klines()
        first = analyze(raw, checked, return_rank=11)
        changed = deepcopy(raw)
        changed[96] = kline(96, "101.3", "102.4", "101", "102.1")
        second = analyze(changed, checked, return_rank=60)
        self.assertTrue(first.passed)
        self.assertTrue(second.passed)
        self.assertEqual(first.structure.structure_id, second.structure.structure_id)
        self.assertNotEqual(first.structure.return_rank, second.structure.return_rank)


class N13SnapshotAndStateTests(unittest.TestCase):
    def test_snapshot_requires_exact_complete_aligned_top100(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _, scheduler = recorder_scheduler(tmpdir)
            items, raws, checked = full_batch()
            complete = scheduler._build_n13_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertTrue(complete.complete)
            self.assertEqual(len(complete.rows), 100)
            self.assertEqual(complete.positive_breadth, Decimal("1"))
            self.assertEqual(complete.above_vwap_breadth, Decimal("1"))
            self.assertEqual(complete.rows["S011USDT"].return_rank, 11)
            self.assertEqual(complete.rows["S100USDT"].return_rank, 100)

        for mutation in ("only_99", "duplicate_rank", "missing_raw", "misaligned"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmpdir:
                _, scheduler = recorder_scheduler(tmpdir)
                items, raws, checked = full_batch()
                if mutation == "only_99":
                    items = items[:-1]
                elif mutation == "duplicate_rank":
                    items[-1] = replace(items[-1], quote_volume_rank=99)
                elif mutation == "missing_raw":
                    raws.pop(items[-1].symbol)
                else:
                    raws[items[-1].symbol][-1][0] += 1
                    raws[items[-1].symbol][-1][6] += 1
                context = scheduler._build_n13_batch_context(
                    {"quote_volume_top": items}, raws, checked
                )
                self.assertFalse(context.complete)
                self.assertEqual(context.rows, {})

    def test_legal_flat_history_builds_snapshot_but_high_below_low_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _, scheduler = recorder_scheduler(tmpdir)
            items, raws, checked = full_batch()
            flat_symbol = items[49].symbol
            raws[flat_symbol][20] = kline(20, "100", "100", "100", "100")
            context = scheduler._build_n13_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertTrue(context.complete)
            self.assertEqual(len(context.rows), 100)

        with tempfile.TemporaryDirectory() as tmpdir:
            _, scheduler = recorder_scheduler(tmpdir)
            items, raws, checked = full_batch()
            broken_symbol = items[49].symbol
            raws[broken_symbol][20] = kline(20, "100", "99", "100", "100")
            context = scheduler._build_n13_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertFalse(context.complete)
            self.assertEqual(context.rows, {})

    def test_same_bar_snapshot_freezes_old_membership_and_new_entrant_waits(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            items, raws, checked = full_batch()
            first = scheduler._build_n13_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertTrue(first.complete)

            entrant = candidate("NEWUSDT", 100)
            replaced = [*items[:-1], entrant]
            replaced_raws = {
                **{item.symbol: raws[item.symbol] for item in items[:-1]},
                entrant.symbol: deepcopy(next(iter(raws.values()))),
            }
            frozen_raws = {
                **replaced_raws,
                items[-1].symbol: raws[items[-1].symbol],
            }
            frozen = scheduler._build_n13_batch_context(
                {"quote_volume_top": replaced}, frozen_raws, checked
            )
            self.assertTrue(frozen.complete)
            self.assertIn(items[-1].symbol, frozen.rows)
            self.assertNotIn(entrant.symbol, frozen.rows)

            result = scheduler.evaluate(
                None,
                {"quote_volume_top": replaced},
                frozen_raws,
                checked_at_ms=checked,
            )
            entrant_signal = next(
                signal
                for signal in result.signals
                if signal.candidate.symbol == entrant.symbol
            )
            self.assertEqual(
                entrant_signal.reason,
                "N13_RELATIVE_RANK_CONTEXT_INSUFFICIENT",
            )
            old_signal = next(
                signal
                for signal in result.signals
                if signal.candidate.symbol == items[-1].symbol
            )
            self.assertEqual(
                old_signal.candidate.candidate_universe,
                "quote_volume_top_frozen_n13",
            )
            self.assertEqual(old_signal.analysis.return_rank, 100)
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n13_rotation_states WHERE symbol='NEWUSDT'"
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n13_rotation_states WHERE symbol=?",
                        (items[-1].symbol,),
                    ).fetchone()[0],
                    1,
                )

    def test_dropped_frozen_member_obeys_one_round_global_cooldown_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            items, raws, checked = full_batch()
            self.assertTrue(
                scheduler._build_n13_batch_context(
                    {"quote_volume_top": items}, raws, checked
                ).complete
            )
            dropped = items[-1]
            entrant = candidate("NEWUSDT", 100)
            current = [*items[:-1], entrant]
            shared_raw = {
                **{item.symbol: raws[item.symbol] for item in items[:-1]},
                entrant.symbol: deepcopy(next(iter(raws.values()))),
                dropped.symbol: raws[dropped.symbol],
            }
            self.assertTrue(
                recorder.set_symbol_cooldown(
                    dropped.symbol,
                    datetime.now(timezone.utc) + timedelta(hours=4),
                    "TEST_GLOBAL_COOLDOWN",
                    None,
                )
            )
            with patch.object(
                recorder,
                "active_symbol_cooldowns",
                wraps=recorder.active_symbol_cooldowns,
            ) as bulk_read, patch.object(
                recorder,
                "active_symbol_cooldown",
                wraps=recorder.active_symbol_cooldown,
            ) as single_read:
                scan_id = recorder.begin_scan(len(current), current, True)
                self.assertIsNotNone(scan_id)
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": current},
                    shared_raw,
                    checked_at_ms=checked,
                )
            dropped_signal = next(
                signal
                for signal in result.signals
                if signal.candidate.symbol == dropped.symbol
            )
            self.assertFalse(dropped_signal.passed)
            self.assertTrue(
                dropped_signal.reason.startswith(
                    "GLOBAL_SYMBOL_COOLDOWN_UNTIL:"
                )
            )
            self.assertNotIn(
                dropped.symbol,
                {item.candidate.symbol for item in result.passed_signals},
            )
            self.assertNotIn(
                dropped.symbol,
                {item.signal.candidate.symbol for item in result.live_candidates},
            )
            self.assertEqual(bulk_read.call_count, 1)
            self.assertEqual(single_read.call_count, 0)
            self.assertTrue(result.signal_batch_published)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id FROM strategy_signal_current "
                        "WHERE singleton_id=1"
                    ).fetchone(),
                    (scan_id,),
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

    def test_frozen_old_member_without_shared_raw_is_skipped_without_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            items, raws, checked = full_batch()
            self.assertTrue(scheduler._build_n13_batch_context(
                {"quote_volume_top": items}, raws, checked
            ).complete)
            entrant = candidate("NEWUSDT", 100)
            replaced = [*items[:-1], entrant]
            replaced_raws = {
                **{item.symbol: raws[item.symbol] for item in items[:-1]},
                entrant.symbol: deepcopy(next(iter(raws.values()))),
            }
            result = scheduler.evaluate(
                None,
                {"quote_volume_top": replaced},
                replaced_raws,
                checked_at_ms=checked,
            )
            self.assertTrue(all(
                signal.reason == "N13_RELATIVE_RANK_CONTEXT_INSUFFICIENT"
                for signal in result.signals
            ))
            self.assertEqual(result.passed_signals, [])
            self.assertNotIn(
                items[-1].symbol,
                {signal.candidate.symbol for signal in result.signals},
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n13_rotation_states"
                    ).fetchone()[0],
                    0,
                )

    def test_next_bar_rotates_snapshot_to_the_new_complete_top100(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            items, raws, checked = full_batch()
            self.assertTrue(scheduler._build_n13_batch_context(
                {"quote_volume_top": items}, raws, checked
            ).complete)

            entrant = candidate("NEWUSDT", 100)
            replaced = [*items[:-1], entrant]
            next_raws = {
                **{item.symbol: deepcopy(raws[item.symbol]) for item in items[:-1]},
                entrant.symbol: deepcopy(raws[items[0].symbol]),
            }
            for rows in next_raws.values():
                rows.append(kline(105, "101.5", "101.9", "101.3", "101.6"))
            next_checked = BASE_TIME_MS + 105 * INTERVAL_MS + 30_000
            rotated = scheduler._build_n13_batch_context(
                {"quote_volume_top": replaced}, next_raws, next_checked
            )
            self.assertTrue(rotated.complete)
            self.assertIn(entrant.symbol, rotated.rows)
            self.assertNotIn(items[-1].symbol, rotated.rows)
            with recorder._connect() as connection:
                snapshots = connection.execute(
                    "SELECT current_open_time FROM n13_market_snapshots"
                ).fetchall()
            self.assertEqual(snapshots, [(str(next_raws[entrant.symbol][-1][0]),)])

    def test_corrupt_snapshot_payloads_fail_closed(self):
        def rows_1(payload):
            payload["rows"] = payload["rows"][:1]

        def rows_99(payload):
            payload["rows"].pop()

        def rows_101(payload):
            extra = deepcopy(payload["rows"][-1])
            extra["symbol"] = "EXTRAUSDT"
            payload["rows"].append(extra)

        def duplicate_symbol(payload):
            payload["rows"][1]["symbol"] = payload["rows"][0]["symbol"]

        def empty_symbol(payload):
            payload["rows"][0]["symbol"] = ""

        def duplicate_quote_rank(payload):
            payload["rows"][1]["quote_volume_rank"] = payload["rows"][0]["quote_volume_rank"]

        def current_time_bool(payload):
            payload["current_open_time_ms"] = True

        def quote_rank_bool(payload):
            payload["rows"][0]["quote_volume_rank"] = True

        def return_rank_duplicate(payload):
            payload["rows"][1]["return_rank"] = payload["rows"][0]["return_rank"]

        def return_rank_bool(payload):
            payload["rows"][0]["return_rank"] = True

        def return_rank_missing(payload):
            payload["rows"][0].pop("return_rank")

        def above_not_bool(payload):
            payload["rows"][0]["close_above_vwap"] = 1

        def nonpositive_vwap(payload):
            payload["rows"][0]["vwap_c"] = "0"

        def nonpositive_slope_vwap(payload):
            payload["rows"][0]["vwap_slope_reference"] = "-1"

        def nonpositive_atr(payload):
            payload["rows"][0]["atr_c"] = "0"

        def nonfinite_return(payload):
            payload["rows"][0]["return_24h"] = float("nan")

        def inconsistent_rank(payload):
            first = payload["rows"][0]["return_rank"]
            payload["rows"][0]["return_rank"] = payload["rows"][1]["return_rank"]
            payload["rows"][1]["return_rank"] = first

        def fake_positive_breadth(payload):
            for index, row in enumerate(payload["rows"]):
                row["return_24h"] = "1" if index < 11 else "-1"
                row["return_rank"] = index + 1
            payload["positive_return_breadth"] = "1"

        def fake_above_breadth(payload):
            for row in payload["rows"]:
                row["close_above_vwap"] = False
            payload["above_vwap_breadth"] = "1"

        mutations = {
            "rows_1": rows_1,
            "rows_99": rows_99,
            "rows_101": rows_101,
            "duplicate_symbol": duplicate_symbol,
            "empty_symbol": empty_symbol,
            "duplicate_quote_rank": duplicate_quote_rank,
            "current_time_bool": current_time_bool,
            "quote_rank_bool": quote_rank_bool,
            "return_rank_duplicate": return_rank_duplicate,
            "return_rank_bool": return_rank_bool,
            "return_rank_missing": return_rank_missing,
            "above_not_bool": above_not_bool,
            "nonpositive_vwap": nonpositive_vwap,
            "nonpositive_slope_vwap": nonpositive_slope_vwap,
            "nonpositive_atr": nonpositive_atr,
            "nonfinite_return": nonfinite_return,
            "inconsistent_rank": inconsistent_rank,
            "fake_positive_breadth": fake_positive_breadth,
            "fake_above_breadth": fake_above_breadth,
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                items, raws, checked = full_batch()
                valid = scheduler._build_n13_batch_context(
                    {"quote_volume_top": items}, raws, checked
                )
                self.assertTrue(valid.complete)
                with recorder._connect() as connection:
                    payload = json.loads(connection.execute(
                        "SELECT payload_json FROM n13_market_snapshots"
                    ).fetchone()[0])
                    mutate(payload)
                    connection.execute(
                        "UPDATE n13_market_snapshots SET payload_json=?",
                        (json.dumps(payload, allow_nan=True),),
                    )
                restarted = StrategyScheduler(
                    (N13_STRATEGY,), 96, recorder, logging.getLogger("n13_corrupt")
                )
                context = restarted._build_n13_batch_context(
                    {"quote_volume_top": items}, raws, checked
                )
                self.assertFalse(context.complete)
                self.assertEqual(context.rows, {})

    def test_snapshot_config_signature_fails_closed_same_bar_and_rebuilds_next_bar(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            items, raws, checked = full_batch()
            original = scheduler._build_n13_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertTrue(original.complete)

            changed_strategy = replace(
                N13_STRATEGY,
                n13_vwap_lookback_bars=95,
            )
            restarted = StrategyScheduler(
                (changed_strategy,),
                96,
                recorder,
                logging.getLogger("n13_config_restart"),
            )
            same_bar = restarted._build_n13_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertFalse(same_bar.complete)
            self.assertEqual(same_bar.rows, {})
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n13_rotation_states"
                    ).fetchone()[0],
                    0,
                )

            next_raws = deepcopy(raws)
            for rows in next_raws.values():
                rows.append(
                    kline(105, "101.5", "101.9", "101.3", "101.6")
                )
            next_checked = BASE_TIME_MS + 105 * INTERVAL_MS + 30_000
            next_bar = restarted._build_n13_batch_context(
                {"quote_volume_top": items}, next_raws, next_checked
            )
            self.assertTrue(next_bar.complete)
            with recorder._connect() as connection:
                payload = json.loads(connection.execute(
                    "SELECT payload_json FROM n13_market_snapshots"
                ).fetchone()[0])
            self.assertEqual(
                payload["config_signature"]["vwap_lookback_bars"],
                95,
            )
            self.assertEqual(payload["current_open_time_ms"], next_raws[items[0].symbol][-1][0])

    def test_atomic_state_is_exact_idempotent_and_detects_each_field_conflict(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            base = {
                "strategy_id": "N13",
                "symbol": "AAAUSDT",
                "t_time": "123",
                "structure_id": "sid",
                "status": "MISSED",
                "reason": "reason",
                "detail": {"a": 1},
            }
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically([base]), "OK"
            )
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically([deepcopy(base)]),
                "OK",
            )
            for field, value in (
                ("structure_id", "different"),
                ("status", "INVALID"),
                ("reason", "different"),
                ("detail", {"a": 2}),
            ):
                changed = deepcopy(base)
                changed[field] = value
                self.assertEqual(
                    recorder.record_n13_rotation_states_atomically([changed]),
                    "N13_STATE_INCONSISTENT",
                    field,
                )

    def test_atomic_state_deduplicates_same_batch_key_before_require_new(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            base = {
                "strategy_id": "N13",
                "symbol": "AAAUSDT",
                "t_time": "123",
                "structure_id": "sid",
                "status": "CONSUMED",
                "reason": "PASSED",
                "detail": {"stable": {"a": 1, "b": 2}},
                "require_new": False,
            }
            duplicate = deepcopy(base)
            duplicate["detail"] = {"stable": {"b": 2, "a": 1}}
            duplicate["require_new"] = True
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically(
                    [base, duplicate]
                ),
                "OK",
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n13_rotation_states"
                    ).fetchone()[0],
                    1,
                )
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically(
                    [base, duplicate]
                ),
                "N13_EPISODE_CONSUMED",
            )

    def test_atomic_state_rejects_conflicting_same_batch_key_without_writes(self):
        base = {
            "strategy_id": "N13",
            "symbol": "AAAUSDT",
            "t_time": "123",
            "structure_id": "sid",
            "status": "MISSED",
            "reason": "HISTORICAL_N13_ENTRY_MISSED",
            "detail": {"stable": {"p": "100"}},
        }
        for field, value in (
            ("structure_id", "different"),
            ("status", "INVALID"),
            ("reason", "different"),
            ("detail", {"stable": {"p": "99"}}),
        ):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmpdir:
                recorder, _ = recorder_scheduler(tmpdir)
                conflict = deepcopy(base)
                conflict[field] = value
                self.assertEqual(
                    recorder.record_n13_rotation_states_atomically(
                        [base, conflict]
                    ),
                    "N13_STATE_INCONSISTENT",
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM n13_rotation_states"
                        ).fetchone()[0],
                        0,
                    )

    def test_atomic_state_rejects_nonstandard_json_before_first_write(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            invalid = {
                "strategy_id": "N13",
                "symbol": "AAAUSDT",
                "t_time": "123",
                "structure_id": None,
                "status": "INVALID",
                "reason": "N13_ARMED_WINDOW_EXPIRED",
                "detail": {"not_finite": float("nan")},
            }
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically([invalid]),
                "N13_STATE_INCONSISTENT",
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n13_rotation_states"
                    ).fetchone()[0],
                    0,
                )

    def test_atomic_state_rolls_back_first_insert_when_later_record_conflicts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            existing = {
                "strategy_id": "N13",
                "symbol": "AAAUSDT",
                "t_time": "2",
                "structure_id": "sid2",
                "status": "MISSED",
                "reason": "old",
                "detail": {"old": True},
            }
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically([existing]), "OK"
            )
            new = {
                "strategy_id": "N13",
                "symbol": "AAAUSDT",
                "t_time": "1",
                "structure_id": None,
                "status": "INVALID",
                "reason": "new",
                "detail": {"new": True},
            }
            conflict = deepcopy(existing)
            conflict["reason"] = "changed"
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically([new, conflict]),
                "N13_STATE_INCONSISTENT",
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n13_rotation_states WHERE t_time='1'"
                    ).fetchone()[0],
                    0,
                )

    def test_atomic_state_database_failure_rolls_back_to_zero_and_retries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            records = [
                {
                    "strategy_id": "N13",
                    "symbol": "AAAUSDT",
                    "t_time": str(index),
                    "structure_id": None,
                    "status": "INVALID",
                    "reason": f"reason-{index}",
                    "detail": {"index": index},
                }
                for index in (1, 2)
            ]
            with recorder._connect() as connection:
                connection.execute(
                    """
                    CREATE TRIGGER force_n13_second_insert_failure
                    BEFORE INSERT ON n13_rotation_states
                    WHEN NEW.t_time = '2'
                    BEGIN
                        SELECT RAISE(ABORT, 'forced N13 second insert failure');
                    END
                    """
                )
                commit_test_schema_change(recorder, connection)
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically(records),
                "N13_STATE_PERSIST_FAILED",
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n13_rotation_states"
                    ).fetchone()[0],
                    0,
                )
                connection.execute(
                    "DROP TRIGGER force_n13_second_insert_failure"
                )
                commit_test_schema_change(recorder, connection)
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically(records), "OK"
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n13_rotation_states"
                    ).fetchone()[0],
                    2,
                )

    def test_scheduler_state_read_and_atomic_write_fail_closed(self):
        raw, checked = n13_klines()
        analysis = analyze(raw, checked)
        item = candidate("S011USDT", 11)
        for mode, reason in (
            ("read", "N13_STATE_READ_FAILED"),
            ("write", "N13_STATE_PERSIST_FAILED"),
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                if mode == "read":
                    recorder.get_n13_rotation_state = lambda *args: (_ for _ in ()).throw(
                        RuntimeError("read failed")
                    )
                else:
                    recorder.record_n13_rotation_states_atomically = (
                        lambda records: "N13_STATE_PERSIST_FAILED"
                    )
                decision = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    item,
                    analysis,
                )
                self.assertIsNotNone(decision)
                self.assertEqual(decision.reason, reason)

    def test_consumed_current_episode_is_not_emitted_twice(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            items, raws, checked = full_batch()
            first_scan_id = recorder.begin_scan(len(items), items, True)
            self.assertIsNotNone(first_scan_id)
            first = scheduler.evaluate(
                first_scan_id,
                {"quote_volume_top": items},
                raws,
                checked_at_ms=checked,
            )
            self.assertTrue(
                next(
                    signal
                    for signal in first.signals
                    if signal.candidate.symbol == "S011USDT"
                ).passed
            )
            second_scan_id = recorder.begin_scan(len(items), items, True)
            self.assertIsNotNone(second_scan_id)
            second = scheduler.evaluate(
                second_scan_id,
                {"quote_volume_top": items},
                raws,
                checked_at_ms=checked,
            )
            repeated = next(
                signal
                for signal in second.signals
                if signal.candidate.symbol == "S011USDT"
            )
            self.assertFalse(repeated.passed)
            self.assertEqual(repeated.reason, "N13_EPISODE_CONSUMED")
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n13_rotation_states"
                    ).fetchone()[0],
                    100,
                )

    def test_current_terminal_envelope_ignores_live_e_but_rejects_tampering(self):
        raw, checked = n13_klines()
        original = analyze(raw, checked)
        changed = deepcopy(raw)
        changed[-1] = kline(104, "101.4", "101.8", "101.2", "101.6")
        changed_analysis = analyze(changed, checked + 1)
        self.assertTrue(changed_analysis.passed, changed_analysis.reason)
        item = candidate("S011USDT", 11)

        for mode in (
            "extra_field",
            "changed_value",
            "rehashed_stable_identity",
            "bad_hash",
            "column_structure_id",
            "column_status",
            "column_reason",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                self.assertIsNone(
                    scheduler._apply_n13_state(N13_STRATEGY, item, original)
                )
                unchanged = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    item,
                    changed_analysis,
                )
                self.assertEqual(unchanged.reason, "N13_EPISODE_CONSUMED")

                with recorder._connect() as connection:
                    row = connection.execute(
                        "SELECT id, detail_json FROM n13_rotation_states"
                    ).fetchone()
                    envelope = json.loads(row[1])
                    self.assertEqual(envelope["schema_version"], 1)
                    self.assertNotIn("entry", envelope["evidence"])
                    self.assertNotIn("elapsed_ms", envelope)
                    if mode == "extra_field":
                        envelope["tampered"] = True
                    elif mode == "changed_value":
                        envelope["evidence"]["p"] = "0"
                    elif mode == "rehashed_stable_identity":
                        envelope["evidence"]["a"]["close"] = "0"
                        unsigned = {
                            key: value
                            for key, value in envelope.items()
                            if key != "canonical_sha256"
                        }
                        canonical = json.dumps(
                            unsigned,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        envelope["canonical_sha256"] = hashlib.sha256(
                            canonical.encode("utf-8")
                        ).hexdigest()
                    elif mode == "bad_hash":
                        envelope["canonical_sha256"] = "0" * 64
                    if mode == "column_structure_id":
                        connection.execute(
                            """
                            UPDATE n13_rotation_states
                            SET structure_id='different' WHERE id=?
                            """,
                            (row[0],),
                        )
                    elif mode == "column_status":
                        connection.execute(
                            """
                            UPDATE n13_rotation_states SET status='INVALID'
                            WHERE id=?
                            """,
                            (row[0],),
                        )
                    elif mode == "column_reason":
                        connection.execute(
                            """
                            UPDATE n13_rotation_states SET reason='different'
                            WHERE id=?
                            """,
                            (row[0],),
                        )
                    else:
                        connection.execute(
                            """
                            UPDATE n13_rotation_states SET detail_json=?
                            WHERE id=?
                            """,
                            (json.dumps(envelope, sort_keys=True), row[0]),
                        )

                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    item,
                    changed_analysis,
                )
                self.assertEqual(rejected.reason, "N13_STATE_INCONSISTENT")

    def test_n13_state_json_rejects_duplicate_keys_in_v1_and_legacy_rows(self):
        item = candidate("S011USDT", 11)
        for mode in ("v1_current", "legacy_historical"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                raw, checked = n13_klines()
                if mode == "v1_current":
                    analysis = analyze(raw, checked)
                    self.assertIsNone(
                        scheduler._apply_n13_state(
                            N13_STRATEGY,
                            item,
                            analysis,
                        )
                    )
                    needle = '"status": "CONSUMED"'
                    replacement = (
                        '"status": "CONFLICTING", '
                        '"status": "CONSUMED"'
                    )
                else:
                    raw.append(
                        kline(105, "101.5", "101.9", "101.3", "101.6")
                    )
                    analysis = analyze(
                        raw,
                        BASE_TIME_MS + 105 * INTERVAL_MS + 30_000,
                    )
                    event = analysis.historical_events[-1]
                    detail = legacy_n13_state_detail(
                        {
                            "event": event.reason,
                            "structure": event.structure.json(),
                            "detail": event.detail,
                        }
                    )
                    self.assertEqual(
                        recorder.record_n13_rotation_states_atomically(
                            [
                                {
                                    "strategy_id": "N13",
                                    "symbol": item.symbol,
                                    "t_time": event.t_time,
                                    "structure_id": event.structure.structure_id,
                                    "status": event.status,
                                    "reason": event.reason,
                                    "detail": detail,
                                }
                            ]
                        ),
                        "OK",
                    )
                    needle = '"event": "HISTORICAL_N13_ENTRY_MISSED"'
                    replacement = (
                        '"event": "CONFLICTING", '
                        '"event": "HISTORICAL_N13_ENTRY_MISSED"'
                    )
                with recorder._connect() as connection:
                    row = connection.execute(
                        "SELECT id, detail_json FROM n13_rotation_states"
                    ).fetchone()
                    duplicated = row[1].replace(needle, replacement, 1)
                    self.assertNotEqual(duplicated, row[1])
                    connection.execute(
                        "UPDATE n13_rotation_states SET detail_json=? WHERE id=?",
                        (duplicated, row[0]),
                    )
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    item,
                    analysis,
                )
                self.assertEqual(rejected.reason, "N13_STATE_INCONSISTENT")

    def test_restart_rejects_null_structure_half_state_as_inconsistent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            raw, _ = n13_klines()
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically(
                    [
                        {
                            "strategy_id": "N13",
                            "symbol": "S011USDT",
                            "t_time": str(raw[103][0]),
                            "structure_id": None,
                            "status": "INVALID",
                            "reason": "HALF_STAGE",
                            "detail": {"legacy_half_state": True},
                        }
                    ]
                ),
                "OK",
            )
            restarted_recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("n13_half_restart"),
            )
            restarted = StrategyScheduler(
                (N13_STRATEGY,),
                96,
                restarted_recorder,
                logging.getLogger("n13_half_restart"),
            )
            items, raws, checked = full_batch()
            result = restarted.evaluate(
                None,
                {"quote_volume_top": items},
                raws,
                checked_at_ms=checked,
            )
            target = next(
                signal
                for signal in result.signals
                if signal.candidate.symbol == "S011USDT"
            )
            self.assertFalse(target.passed)
            self.assertEqual(target.reason, "N13_STATE_INCONSISTENT")
            with restarted_recorder._connect() as connection:
                persisted = connection.execute(
                    """
                    SELECT structure_id, status, reason
                    FROM n13_rotation_states
                    WHERE strategy_id='N13' AND symbol='S011USDT'
                    """
                ).fetchone()
            self.assertEqual(persisted, (None, "INVALID", "HALF_STAGE"))

    def test_restart_rejects_illegal_same_sid_terminal_rows(self):
        raw, checked = n13_klines()
        analysis = analyze(raw, checked)
        structure = analysis.structure
        item = candidate("S011USDT", 11)
        cases = (
            ("bad_status", "INVALID", "PASSED", {"structure_id": structure.structure_id}),
            ("bad_reason", "CONSUMED", "BAD_REASON", {"structure_id": structure.structure_id}),
            ("bad_detail", "CONSUMED", "PASSED", {"structure_id": "other"}),
            ("invalid_json", "CONSUMED", "PASSED", None),
        )
        for name, status, reason, detail in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                recorder, _ = recorder_scheduler(tmpdir)
                if detail is not None:
                    self.assertEqual(
                        recorder.record_n13_rotation_states_atomically(
                            [
                                {
                                    "strategy_id": "N13",
                                    "symbol": item.symbol,
                                    "t_time": str(structure.t.open_time_ms),
                                    "structure_id": structure.structure_id,
                                    "status": status,
                                    "reason": reason,
                                    "detail": detail,
                                }
                            ]
                        ),
                        "OK",
                    )
                else:
                    with recorder._connect() as connection:
                        connection.execute(
                            """
                            INSERT INTO n13_rotation_states (
                                strategy_id, symbol, t_time, structure_id,
                                status, reason, detail_json, created_at, updated_at
                            ) VALUES ('N13', ?, ?, ?, ?, ?, '{bad json', 'x', 'x')
                            """,
                            (
                                item.symbol,
                                str(structure.t.open_time_ms),
                                structure.structure_id,
                                status,
                                reason,
                            ),
                        )
                restarted_recorder = make_test_recorder(
                    str(Path(tmpdir) / "review.sqlite3"),
                    logging.getLogger(f"n13_{name}_restart"),
                )
                restarted = StrategyScheduler(
                    (N13_STRATEGY,),
                    96,
                    restarted_recorder,
                    logging.getLogger(f"n13_{name}_restart"),
                )
                decision = restarted._apply_n13_state(
                    N13_STRATEGY,
                    item,
                    analysis,
                )
                self.assertIsNotNone(decision)
                self.assertEqual(decision.reason, "N13_STATE_INCONSISTENT")

    def test_scheduler_does_not_mask_negative_entry_elapsed_as_context_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _, scheduler = recorder_scheduler(tmpdir)
            items, raws, _ = full_batch()
            entry_open = int(next(iter(raws.values()))[-1][0])
            result = scheduler.evaluate(
                None,
                {"quote_volume_top": items},
                raws,
                checked_at_ms=entry_open - 1,
            )
            target = next(
                signal
                for signal in result.signals
                if signal.candidate.symbol == "S011USDT"
            )
            self.assertEqual(target.reason, "N13_ENTRY_WINDOW_EXPIRED")
            self.assertTrue(target.analysis.consume_current)

    def test_consumed_current_episode_is_not_rewritten_as_next_bar_history(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            items, raws, checked = full_batch()
            first_scan_id = recorder.begin_scan(len(items), items, True)
            self.assertIsNotNone(first_scan_id)
            first = scheduler.evaluate(
                first_scan_id,
                {"quote_volume_top": items},
                raws,
                checked_at_ms=checked,
            )
            self.assertTrue(
                next(
                    signal
                    for signal in first.signals
                    if signal.candidate.symbol == "S011USDT"
                ).passed
            )

            next_raws = deepcopy(raws)
            for rows in next_raws.values():
                rows.append(
                    kline(105, "101.5", "101.9", "101.3", "101.6")
                )
            next_checked = BASE_TIME_MS + 105 * INTERVAL_MS + 30_000
            second_scan_id = recorder.begin_scan(len(items), items, True)
            self.assertIsNotNone(second_scan_id)
            second = scheduler.evaluate(
                second_scan_id,
                {"quote_volume_top": items},
                next_raws,
                checked_at_ms=next_checked,
            )
            target = next(
                signal
                for signal in second.signals
                if signal.candidate.symbol == "S011USDT"
            )
            self.assertEqual(target.reason, "HISTORICAL_N13_ENTRY_MISSED")
            self.assertNotEqual(target.reason, "N13_STATE_INCONSISTENT")
            with recorder._connect() as connection:
                row = connection.execute(
                    """
                    SELECT status, reason FROM n13_rotation_states
                    WHERE strategy_id='N13' AND symbol='S011USDT'
                    """
                ).fetchone()
            self.assertEqual(row, ("CONSUMED", "PASSED"))

    def test_consumed_current_episode_survives_fixed_window_slide(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            raw, checked = n13_klines()
            raw = [
                kline(index, "100", "100.5", "99.5", "100")
                for index in range(-17, 0)
            ] + raw
            # Production requests a fixed 122-bar window.  A small difference
            # in its first candle makes the Wilder seed drift after left-shift.
            raw[0] = kline(-17, "100", "100.6", "99.4", "100")
            self.assertEqual(len(raw), 122)
            item = candidate("S011USDT", 11)
            current = analyze(raw, checked)
            self.assertTrue(current.passed, current.reason)
            t_time = str(current.structure.t.open_time_ms)
            legacy_envelope = pre_fix_n13_current_terminal_envelope(
                "N13",
                item.symbol,
                t_time,
                current.structure,
                "CONSUMED",
                "PASSED",
            )
            with recorder._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO n13_rotation_states (
                        strategy_id, symbol, t_time, structure_id,
                        status, reason, detail_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "N13",
                        item.symbol,
                        t_time,
                        current.structure.structure_id,
                        "CONSUMED",
                        "PASSED",
                        json.dumps(
                            legacy_envelope,
                            ensure_ascii=False,
                            default=str,
                            sort_keys=True,
                        ),
                        "pre-fix-created",
                        "pre-fix-updated",
                    ),
                )
                columns, persisted_before, count_before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    item.symbol,
                    t_time,
                )
            self.assertEqual(columns, N13_STATE_COLUMNS)
            self.assertEqual(count_before, 1)
            self.assertEqual(
                json.loads(persisted_before[7])["schema_version"],
                1,
            )
            repeated_current = scheduler._apply_n13_state(
                N13_STRATEGY,
                item,
                current,
            )
            self.assertEqual(repeated_current.reason, "N13_EPISODE_CONSUMED")
            with recorder._connect() as connection:
                self.assertEqual(
                    n13_full_row_snapshot(
                        connection,
                        "N13",
                        item.symbol,
                        t_time,
                    ),
                    (columns, persisted_before, count_before),
                )

            slid = deepcopy(raw[1:])
            slid.append(kline(105, "101.5", "101.9", "101.3", "101.6"))
            self.assertEqual(len(slid), 122)
            historical = analyze(
                slid,
                BASE_TIME_MS + 105 * INTERVAL_MS + 30_000,
            )
            self.assertEqual(historical.reason, "HISTORICAL_N13_ENTRY_MISSED")
            self.assertEqual(
                historical.structure.structure_id,
                current.structure.structure_id,
            )
            self.assertNotEqual(
                historical.structure.atr_c,
                current.structure.atr_c,
            )

            restarted_recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("n13_fixed_window_restart"),
            )
            restarted = StrategyScheduler(
                (N13_STRATEGY,),
                96,
                restarted_recorder,
                logging.getLogger("n13_fixed_window_restart"),
            )
            decision = restarted._apply_n13_state(
                N13_STRATEGY,
                item,
                historical,
            )
            self.assertIsNone(decision)
            self.assertIsNone(
                restarted._apply_n13_state(
                    N13_STRATEGY,
                    item,
                    historical,
                )
            )
            with restarted_recorder._connect() as connection:
                after_snapshot = n13_full_row_snapshot(
                    connection,
                    "N13",
                    item.symbol,
                    t_time,
                )
            _, persisted_after, count_after = after_snapshot
            self.assertEqual(persisted_after, persisted_before)
            self.assertEqual(count_after, count_before)

            corrupted = json.loads(persisted_after[7])
            corrupted["evidence"]["a"]["close"] = "0"
            unsigned = {
                key: value
                for key, value in corrupted.items()
                if key != "canonical_sha256"
            }
            canonical = json.dumps(
                unsigned,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            corrupted["canonical_sha256"] = hashlib.sha256(
                canonical.encode("utf-8")
            ).hexdigest()
            with restarted_recorder._connect() as connection:
                connection.execute(
                    "UPDATE n13_rotation_states SET detail_json=? WHERE id=?",
                    (json.dumps(corrupted, sort_keys=True), persisted_after[0]),
                )
                corrupted_before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    item.symbol,
                    t_time,
                )
            rejected = restarted._apply_n13_state(
                N13_STRATEGY,
                item,
                historical,
            )
            self.assertEqual(rejected.reason, "N13_STATE_INCONSISTENT")
            with restarted_recorder._connect() as connection:
                corrupted_after = n13_full_row_snapshot(
                    connection,
                    "N13",
                    item.symbol,
                    t_time,
                )
            self.assertEqual(corrupted_after, corrupted_before)

    def test_legacy_historical_event_survives_repeated_fixed_window_slide(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            raw, _ = n13_klines()
            raw = [
                kline(index, "100", "100.5", "99.5", "100")
                for index in range(-17, 0)
            ] + raw
            raw[1] = kline(-16, "100", "100.6", "99.4", "100")
            raw[2] = kline(-15, "100", "100.7", "99.3", "100")
            first_window = deepcopy(raw[1:])
            first_window.append(
                kline(105, "101.5", "101.9", "101.3", "101.6")
            )
            first = analyze(
                first_window,
                BASE_TIME_MS + 105 * INTERVAL_MS + 30_000,
            )
            event = first.historical_events[-1]
            self.assertEqual(event.reason, "HISTORICAL_N13_ENTRY_MISSED")
            legacy_detail = legacy_n13_state_detail(
                {
                    "event": event.reason,
                    "structure": event.structure.json(),
                    "detail": event.detail,
                }
            )
            self.assertIn("atr_c", legacy_detail["structure"])
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically(
                    [
                        {
                            "strategy_id": "N13",
                            "symbol": "S011USDT",
                            "t_time": event.t_time,
                            "structure_id": event.structure.structure_id,
                            "status": event.status,
                            "reason": event.reason,
                            "detail": legacy_detail,
                        }
                    ]
                ),
                "OK",
            )
            with recorder._connect() as connection:
                columns, persisted_before, count_before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    event.t_time,
                )
            self.assertEqual(columns, N13_STATE_COLUMNS)
            self.assertEqual(count_before, 1)

            second_window = deepcopy(first_window[1:])
            second_window.append(
                kline(106, "101.6", "102", "101.4", "101.7")
            )
            second = analyze(
                second_window,
                BASE_TIME_MS + 106 * INTERVAL_MS + 30_000,
            )
            self.assertEqual(second.reason, "HISTORICAL_N13_ENTRY_MISSED")
            self.assertEqual(
                second.structure.structure_id,
                event.structure.structure_id,
            )
            self.assertNotEqual(second.structure.atr_c, event.structure.atr_c)
            self.assertEqual(second.structure.vwap_c, event.structure.vwap_c)
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    second,
                )
            )
            third_window = deepcopy(second_window[1:])
            third_window.append(
                kline(107, "101.7", "102.1", "101.5", "101.8")
            )
            self.assertEqual(len(third_window), 122)
            third = analyze(
                third_window,
                BASE_TIME_MS + 107 * INTERVAL_MS + 30_000,
            )
            self.assertEqual(third.reason, "HISTORICAL_N13_ENTRY_MISSED")
            self.assertEqual(
                third.structure.structure_id,
                event.structure.structure_id,
            )
            self.assertNotEqual(third.structure.atr_c, second.structure.atr_c)
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    third,
                )
            )
            with recorder._connect() as connection:
                after_snapshot = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    event.t_time,
                )
            _, persisted_after, count_after = after_snapshot
            self.assertEqual(persisted_after, persisted_before)
            self.assertEqual(count_after, count_before)

            corrupted = json.loads(persisted_after[7])
            corrupted["structure"]["p"] = "0"
            with recorder._connect() as connection:
                connection.execute(
                    "UPDATE n13_rotation_states SET detail_json=? WHERE id=?",
                    (json.dumps(corrupted, sort_keys=True), persisted_after[0]),
                )
                corrupted_before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    event.t_time,
                )
            rejected = scheduler._apply_n13_state(
                N13_STRATEGY,
                candidate("S011USDT", 11),
                third,
            )
            self.assertEqual(rejected.reason, "N13_STATE_INCONSISTENT")
            with recorder._connect() as connection:
                corrupted_after = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    event.t_time,
                )
            self.assertEqual(corrupted_after, corrupted_before)

    def test_prefixed_v1_current_terminal_survives_natural_a_switch(self):
        windows = n13_current_a_switch_windows()
        self.assertTrue(all(len(window) == 122 for window, _ in windows))
        first, second, third = (
            analyze(
                window,
                checked,
                positive_return_breadth=Decimal("0"),
            )
            for window, checked in windows
        )
        self.assertFalse(first.passed)
        self.assertTrue(first.consume_current)
        self.assertEqual(first.reason, "N13_MARKET_BREADTH_NOT_MET")
        self.assertEqual(second.reason, "HISTORICAL_N13_ENTRY_MISSED")
        self.assertEqual(third.reason, "HISTORICAL_N13_ENTRY_MISSED")
        structures = (first.structure, second.structure, third.structure)
        self.assertEqual(
            {structure.structure_id for structure in structures},
            {official_n13_structure_id(
                "S011USDT",
                first.structure.t.open_time_ms,
                first.structure.c.open_time_ms,
            )},
        )
        self.assertEqual(
            legacy_n13_state_detail(first.structure.t.json()),
            legacy_n13_state_detail(second.structure.t.json()),
        )
        self.assertEqual(
            legacy_n13_state_detail(first.structure.c.json()),
            legacy_n13_state_detail(second.structure.c.json()),
        )
        self.assertNotEqual(
            first.structure.a.open_time_ms,
            second.structure.a.open_time_ms,
        )
        self.assertEqual(
            second.structure.a.open_time_ms,
            third.structure.a.open_time_ms,
        )
        self.assertNotEqual(
            legacy_n13_state_detail(first.structure.a.json()),
            legacy_n13_state_detail(second.structure.a.json()),
        )
        self.assertNotEqual(
            first.structure.armed_expiry_time_ms,
            second.structure.armed_expiry_time_ms,
        )
        for structure in structures:
            self.assertEqual(
                structure.armed_expiry_time_ms,
                structure.a.open_time_ms + 8 * INTERVAL_MS,
            )
        for field in (
            "p",
            "vwap_c",
            "entry_min_price",
            "failed_confirmation_reasons",
        ):
            self.assertEqual(
                getattr(first.structure, field),
                getattr(second.structure, field),
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            item = candidate("S011USDT", 11)
            t_time = str(first.structure.t.open_time_ms)
            envelope = pre_fix_n13_current_terminal_envelope(
                "N13",
                item.symbol,
                t_time,
                first.structure,
                "MISSED",
                first.reason,
            )
            with recorder._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO n13_rotation_states (
                        strategy_id, symbol, t_time, structure_id,
                        status, reason, detail_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "N13",
                        item.symbol,
                        t_time,
                        first.structure.structure_id,
                        "MISSED",
                        first.reason,
                        json.dumps(envelope, sort_keys=True),
                        "pre-fix-created",
                        "pre-fix-updated",
                    ),
                )
                before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    item.symbol,
                    t_time,
                )
            self.assertEqual(before[0], N13_STATE_COLUMNS)
            self.assertEqual(before[2], 1)
            for replay in (second, third, second):
                self.assertIsNone(
                    scheduler._apply_n13_state(
                        N13_STRATEGY,
                        item,
                        replay,
                    )
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        n13_full_row_snapshot(
                            connection,
                            "N13",
                            item.symbol,
                            t_time,
                        ),
                        before,
                    )

    def test_current_breadth_terminal_to_value_band_remains_candidate_fail_closed(self):
        windows = n13_current_terminal_to_value_band_windows()
        first = analyze(
            *windows[0],
            positive_return_breadth=Decimal("0"),
        )
        replay = analyze(*windows[1])
        self.assertEqual(first.reason, "N13_MARKET_BREADTH_NOT_MET")
        self.assertTrue(first.consume_current)
        self.assertIsNotNone(first.structure)
        self.assertEqual(replay.reason, "N13_VALUE_BAND_BROKEN")
        self.assertIsNone(replay.structure)
        replay_event = next(
            event
            for event in replay.stage_events
            if event.reason == "N13_VALUE_BAND_BROKEN"
        )
        self.assertEqual(
            replay_event.t_time,
            str(first.structure.t.open_time_ms),
        )
        self.assertEqual(
            legacy_n13_state_detail(replay_event.detail["t"]),
            legacy_n13_state_detail(first.structure.t.json()),
        )
        self.assertNotEqual(
            replay_event.detail["zone_lower"],
            str(first.structure.zone_lower),
        )
        # Breadth chooses the current terminal reason only after a structure
        # exists. It cannot cause this historical stage transition.
        replay_with_low_breadth = analyze(
            *windows[1],
            positive_return_breadth=Decimal("0"),
        )
        self.assertEqual(
            replay_with_low_breadth.reason,
            "N13_VALUE_BAND_BROKEN",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            item = candidate("S011USDT", 11)
            t_time = str(first.structure.t.open_time_ms)
            self.assertIsNone(
                scheduler._apply_n13_state(N13_STRATEGY, item, first)
            )
            with recorder._connect() as connection:
                before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    item.symbol,
                    t_time,
                )
            with self.assertLogs("n13", level="WARNING") as captured:
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    item,
                    replay,
                )
            self.assertEqual(rejected.reason, "N13_STATE_INCONSISTENT")
            self.assertTrue(
                any(
                    "candidate rejected" in message
                    for message in captured.output
                )
            )
            self.assertTrue(
                all(
                    "rejecting atomic batch" not in message
                    for message in captured.output
                )
            )
            with recorder._connect() as connection:
                after = n13_full_row_snapshot(
                    connection,
                    "N13",
                    item.symbol,
                    t_time,
                )
                self.assertEqual(after, before)
                for table in (
                    "strategy_passed_signal_audits",
                    "strategy_passed_structure_ledger",
                    "strategy_paper_trades",
                    "strategy_live_links",
                ):
                    self.assertEqual(
                        connection.execute(
                            f"SELECT COUNT(*) FROM {table}"
                        ).fetchone(),
                        (0,),
                    )

    def test_legacy_historical_survives_natural_a_switch(self):
        windows = n13_historical_a_switch_windows()
        self.assertTrue(all(len(window) == 122 for window, _ in windows))
        first, second, third = (
            analyze(window, checked)
            for window, checked in windows
        )
        for analysis in (first, second, third):
            self.assertEqual(
                analysis.reason,
                "HISTORICAL_N13_ENTRY_MISSED",
            )
        structures = (first.structure, second.structure, third.structure)
        self.assertEqual(
            {structure.structure_id for structure in structures},
            {official_n13_structure_id(
                "S011USDT",
                first.structure.t.open_time_ms,
                first.structure.c.open_time_ms,
            )},
        )
        self.assertEqual(
            legacy_n13_state_detail(first.structure.t.json()),
            legacy_n13_state_detail(second.structure.t.json()),
        )
        self.assertEqual(
            legacy_n13_state_detail(first.structure.c.json()),
            legacy_n13_state_detail(second.structure.c.json()),
        )
        self.assertNotEqual(
            first.structure.a.open_time_ms,
            second.structure.a.open_time_ms,
        )
        self.assertEqual(
            second.structure.a.open_time_ms,
            third.structure.a.open_time_ms,
        )
        self.assertNotEqual(
            legacy_n13_state_detail(first.structure.a.json()),
            legacy_n13_state_detail(second.structure.a.json()),
        )
        self.assertNotEqual(
            first.structure.armed_expiry_time_ms,
            second.structure.armed_expiry_time_ms,
        )
        for structure in structures:
            self.assertEqual(
                structure.armed_expiry_time_ms,
                structure.a.open_time_ms + 8 * INTERVAL_MS,
            )
        for field in (
            "p",
            "vwap_c",
            "entry_min_price",
            "failed_confirmation_reasons",
        ):
            self.assertEqual(
                getattr(first.structure, field),
                getattr(second.structure, field),
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            item = candidate("S011USDT", 11)
            event = first.historical_events[-1]
            t_time = event.t_time
            detail = legacy_n13_state_detail(
                {
                    "event": event.reason,
                    "structure": event.structure.json(),
                    "detail": event.detail,
                }
            )
            with recorder._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO n13_rotation_states (
                        strategy_id, symbol, t_time, structure_id,
                        status, reason, detail_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "N13",
                        item.symbol,
                        t_time,
                        event.structure.structure_id,
                        event.status,
                        event.reason,
                        json.dumps(detail, sort_keys=True),
                        "legacy-created",
                        "legacy-updated",
                    ),
                )
                before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    item.symbol,
                    t_time,
                )
            self.assertEqual(before[0], N13_STATE_COLUMNS)
            self.assertEqual(before[2], 1)
            for replay in (second, third, second):
                self.assertIsNone(
                    scheduler._apply_n13_state(
                        N13_STRATEGY,
                        item,
                        replay,
                    )
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        n13_full_row_snapshot(
                            connection,
                            "N13",
                            item.symbol,
                            t_time,
                        ),
                        before,
                    )

    def test_historical_a_switch_accepts_reverse_and_same_batch_replay(self):
        windows = n13_historical_a_switch_windows()
        first, second, _ = (
            analyze(window, checked)
            for window, checked in windows
        )
        first_event = first.historical_events[-1]
        second_event = second.historical_events[-1]
        for event in (first_event, second_event):
            self.assertEqual(
                event.structure.armed_expiry_time_ms,
                event.structure.a.open_time_ms + 8 * INTERVAL_MS,
            )
        self.assertGreater(
            first_event.structure.a.open_time_ms,
            second_event.structure.a.open_time_ms,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            item = candidate("S011USDT", 11)
            detail = legacy_n13_state_detail(
                {
                    "event": second_event.reason,
                    "structure": second_event.structure.json(),
                    "detail": second_event.detail,
                }
            )
            with recorder._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO n13_rotation_states (
                        strategy_id, symbol, t_time, structure_id,
                        status, reason, detail_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "N13",
                        item.symbol,
                        second_event.t_time,
                        second_event.structure.structure_id,
                        second_event.status,
                        second_event.reason,
                        json.dumps(detail, sort_keys=True),
                        "reverse-created",
                        "reverse-updated",
                    ),
                )
                before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    item.symbol,
                    second_event.t_time,
                )
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    item,
                    first,
                )
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    n13_full_row_snapshot(
                        connection,
                        "N13",
                        item.symbol,
                        second_event.t_time,
                    ),
                    before,
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            item = candidate("S011USDT", 11)
            same_batch = replace(
                second,
                historical_events=(first_event, second_event),
            )
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    item,
                    same_batch,
                )
            )
            with recorder._connect() as connection:
                rows = connection.execute(
                    "SELECT * FROM n13_rotation_states"
                ).fetchall()
            self.assertEqual(len(rows), 1)
            stored = json.loads(rows[0][7])
            self.assertEqual(
                stored["structure"]["a"]["open_time_ms"],
                first_event.structure.a.open_time_ms,
            )

    def test_same_a_historical_replay_keeps_a_ohlcv_exact(self):
        _, second_window, third_window = n13_historical_a_switch_windows()
        second = analyze(*second_window)
        third = analyze(*third_window)
        event = second.historical_events[-1]
        replay = third.historical_events[-1]
        self.assertEqual(
            event.structure.a.open_time_ms,
            replay.structure.a.open_time_ms,
        )
        self.assertEqual(
            event.structure.armed_expiry_time_ms,
            event.structure.a.open_time_ms + 8 * INTERVAL_MS,
        )

        detail = legacy_n13_state_detail(
            {
                "event": event.reason,
                "structure": event.structure.json(),
                "detail": event.detail,
            }
        )
        stored_a = detail["structure"]["a"]
        original_close = stored_a["close"]
        stored_a["close"] = str(
            (
                Decimal(stored_a["open"])
                + Decimal(stored_a["high"])
            )
            / 2
        )
        self.assertNotEqual(stored_a["close"], original_close)
        self.assertGreater(Decimal(stored_a["close"]), 0)
        self.assertGreaterEqual(
            Decimal(stored_a["high"]),
            max(
                Decimal(stored_a["open"]),
                Decimal(stored_a["close"]),
            ),
        )
        self.assertLessEqual(
            Decimal(stored_a["low"]),
            min(
                Decimal(stored_a["open"]),
                Decimal(stored_a["close"]),
            ),
        )
        self.assertGreater(
            Decimal(stored_a["low"]),
            Decimal(detail["structure"]["zone_upper"]),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            item = candidate("S011USDT", 11)
            with recorder._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO n13_rotation_states (
                        strategy_id, symbol, t_time, structure_id,
                        status, reason, detail_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "N13",
                        item.symbol,
                        event.t_time,
                        event.structure.structure_id,
                        event.status,
                        event.reason,
                        json.dumps(detail, sort_keys=True),
                        "same-a-created",
                        "same-a-updated",
                    ),
                )
                before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    item.symbol,
                    event.t_time,
                )
            rejected = scheduler._apply_n13_state(
                N13_STRATEGY,
                item,
                third,
            )
            self.assertEqual(
                rejected.reason,
                "N13_STATE_INCONSISTENT",
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    n13_full_row_snapshot(
                        connection,
                        "N13",
                        item.symbol,
                        event.t_time,
                    ),
                    before,
                )

    def test_historical_a_switch_rejects_invalid_or_stable_conflicts(self):
        windows = n13_current_a_switch_windows()
        first, replay, _ = (
            analyze(
                window,
                checked,
                positive_return_breadth=Decimal("0"),
            )
            for window, checked in windows
        )
        item = candidate("S011USDT", 11)
        t_time = str(first.structure.t.open_time_ms)
        base = pre_fix_n13_current_terminal_envelope(
            "N13",
            item.symbol,
            t_time,
            first.structure,
            "MISSED",
            first.reason,
        )

        def rehash(envelope):
            unsigned = {
                key: value
                for key, value in envelope.items()
                if key != "canonical_sha256"
            }
            canonical = json.dumps(
                unsigned,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            envelope["canonical_sha256"] = hashlib.sha256(
                canonical.encode("utf-8")
            ).hexdigest()

        replay_a = legacy_n13_state_detail(replay.structure.a.json())
        modes = (
            "same_a_valid_ohlcv",
            "a_without_matching_expiry",
            "expiry_without_matching_a",
            "t_after_expiry",
            "zero_price",
            "high_below_open_close",
            "negative_volume",
            "taker_above_quote",
            "invalid_zone_order",
            "a_not_above_zone",
            "t_does_not_touch_zone",
            "zero_atr",
            "negative_atr",
            "entry_max_formula",
            "entry_min_not_c_close",
            "zero_vwap",
            "vwap_above_c",
            "zero_p",
            "p_above_t_c_low",
            "stable_p",
            "stable_vwap",
            "stable_entry_min",
            "stable_t_c",
            "stable_failed_reasons",
        )
        for mode in modes:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                envelope = deepcopy(base)
                evidence = envelope["evidence"]
                if mode == "same_a_valid_ohlcv":
                    evidence["a"]["close"] = "101.9"
                elif mode == "a_without_matching_expiry":
                    evidence["a"] = deepcopy(replay_a)
                elif mode == "expiry_without_matching_a":
                    evidence["armed_expiry_time_ms"] -= INTERVAL_MS
                elif mode == "t_after_expiry":
                    evidence["a"] = {
                        **deepcopy(replay_a),
                        "open_time_ms": (
                            evidence["t"]["open_time_ms"]
                            - 9 * INTERVAL_MS
                        ),
                        "low": "100.5",
                    }
                    evidence["armed_expiry_time_ms"] = (
                        evidence["a"]["open_time_ms"]
                        + 8 * INTERVAL_MS
                    )
                elif mode == "zero_price":
                    evidence["a"]["close"] = "0"
                elif mode == "high_below_open_close":
                    evidence["a"]["high"] = "101.1"
                elif mode == "negative_volume":
                    evidence["a"]["base_volume"] = "-1"
                elif mode == "taker_above_quote":
                    evidence["a"]["taker_buy_quote_volume"] = "103"
                elif mode == "invalid_zone_order":
                    evidence["zone_lower"] = "101"
                    evidence["zone_upper"] = "100"
                elif mode == "a_not_above_zone":
                    evidence["zone_upper"] = evidence["a"]["low"]
                elif mode == "t_does_not_touch_zone":
                    evidence["zone_lower"] = "1"
                    evidence["zone_upper"] = "2"
                elif mode == "zero_atr":
                    evidence["atr_c"] = "0"
                elif mode == "negative_atr":
                    evidence["atr_c"] = "-1"
                elif mode == "entry_max_formula":
                    evidence["entry_max_price"] = evidence[
                        "entry_min_price"
                    ]
                elif mode == "entry_min_not_c_close":
                    evidence["entry_min_price"] = str(
                        Decimal(evidence["c"]["close"])
                        + Decimal("0.01")
                    )
                elif mode == "zero_vwap":
                    evidence["vwap_c"] = "0"
                elif mode == "vwap_above_c":
                    evidence["vwap_c"] = str(
                        Decimal(evidence["c"]["close"])
                        + Decimal("0.01")
                    )
                elif mode == "zero_p":
                    evidence["p"] = "0"
                elif mode == "p_above_t_c_low":
                    evidence["p"] = str(
                        min(
                            Decimal(evidence["t"]["low"]),
                            Decimal(evidence["c"]["low"]),
                        )
                        + Decimal("0.01")
                    )
                elif mode == "stable_p":
                    evidence["p"] = "100.1"
                elif mode == "stable_vwap":
                    evidence["vwap_c"] = "100.2"
                elif mode == "stable_entry_min":
                    evidence["entry_min_price"] = "101.3"
                elif mode == "stable_t_c":
                    evidence["t"]["high"] = "101.7"
                    evidence["c"]["high"] = "101.7"
                elif mode == "stable_failed_reasons":
                    evidence["failed_confirmation_reasons"] = [
                        "N13_CONFIRMATION_CLOSE_BELOW_VWAP"
                    ]
                else:
                    raise AssertionError(mode)
                rehash(envelope)
                with recorder._connect() as connection:
                    connection.execute(
                        """
                        INSERT INTO n13_rotation_states (
                            strategy_id, symbol, t_time, structure_id,
                            status, reason, detail_json,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "N13",
                            item.symbol,
                            t_time,
                            first.structure.structure_id,
                            "MISSED",
                            first.reason,
                            json.dumps(envelope, sort_keys=True),
                            "hostile-created",
                            "hostile-updated",
                        ),
                    )
                    before = n13_full_row_snapshot(
                        connection,
                        "N13",
                        item.symbol,
                        t_time,
                    )
                replay_analysis = (
                    first if mode == "same_a_valid_ohlcv" else replay
                )
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    item,
                    replay_analysis,
                )
                self.assertEqual(
                    rejected.reason,
                    "N13_STATE_INCONSISTENT",
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        n13_full_row_snapshot(
                            connection,
                            "N13",
                            item.symbol,
                            t_time,
                        ),
                        before,
                    )

    def test_legacy_historical_pricing_contract_is_strict(self):
        first, replay, _ = (
            analyze(window, checked)
            for window, checked in n13_historical_a_switch_windows()
        )
        event = first.historical_events[-1]
        base_detail = legacy_n13_state_detail(
            {
                "event": event.reason,
                "structure": event.structure.json(),
                "detail": event.detail,
            }
        )
        for mode in (
            "zero_atr",
            "negative_atr",
            "entry_max_formula",
            "entry_min_not_c_close",
            "zero_vwap",
            "zero_p",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                detail = deepcopy(base_detail)
                structure = detail["structure"]
                if mode == "zero_atr":
                    structure["atr_c"] = "0"
                elif mode == "negative_atr":
                    structure["atr_c"] = "-1"
                elif mode == "entry_max_formula":
                    structure["entry_max_price"] = structure[
                        "entry_min_price"
                    ]
                elif mode == "entry_min_not_c_close":
                    structure["entry_min_price"] = str(
                        Decimal(structure["c"]["close"])
                        + Decimal("0.01")
                    )
                elif mode == "zero_vwap":
                    structure["vwap_c"] = "0"
                elif mode == "zero_p":
                    structure["p"] = "0"
                else:
                    raise AssertionError(mode)
                with recorder._connect() as connection:
                    connection.execute(
                        """
                        INSERT INTO n13_rotation_states (
                            strategy_id, symbol, t_time, structure_id,
                            status, reason, detail_json,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "N13",
                            "S011USDT",
                            event.t_time,
                            event.structure.structure_id,
                            event.status,
                            event.reason,
                            json.dumps(detail, sort_keys=True),
                            "pricing-created",
                            "pricing-updated",
                        ),
                    )
                    before = n13_full_row_snapshot(
                        connection,
                        "N13",
                        "S011USDT",
                        event.t_time,
                    )
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    replay,
                )
                self.assertEqual(
                    rejected.reason,
                    "N13_STATE_INCONSISTENT",
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        n13_full_row_snapshot(
                            connection,
                            "N13",
                            "S011USDT",
                            event.t_time,
                        ),
                        before,
                    )

    def test_runtime_pricing_contract_uses_strategy_extension(self):
        raw, checked = n13_klines()
        extension = Decimal("0.45")
        configured = replace(
            N13_STRATEGY,
            n13_entry_extension_atr_max=extension,
        )
        valid = analyze(
            raw,
            checked,
            entry_extension_atr_max=extension,
        )
        self.assertTrue(valid.passed, valid.reason)
        self.assertEqual(
            valid.structure.entry_max_price,
            valid.structure.entry_min_price
            + extension * valid.structure.atr_c,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir, configured)
            self.assertIsNone(
                scheduler._apply_n13_state(
                    configured,
                    candidate("S011USDT", 11),
                    valid,
                )
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n13_rotation_states"
                    ).fetchone()[0],
                    1,
                )

        default = analyze(raw, checked)
        self.assertTrue(default.passed, default.reason)
        for mode in (
            "zero_atr",
            "entry_max_formula",
            "entry_min_not_c_close",
            "vwap_above_c",
            "p_above_t_c_low",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                structure = default.structure
                if mode == "zero_atr":
                    structure = replace(structure, atr_c=Decimal("0"))
                elif mode == "entry_max_formula":
                    structure = replace(
                        structure,
                        entry_max_price=structure.entry_min_price,
                    )
                elif mode == "entry_min_not_c_close":
                    structure = replace(
                        structure,
                        entry_min_price=(
                            structure.c.close + Decimal("0.01")
                        ),
                    )
                elif mode == "vwap_above_c":
                    structure = replace(
                        structure,
                        vwap_c=structure.c.close + Decimal("0.01"),
                    )
                elif mode == "p_above_t_c_low":
                    structure = replace(
                        structure,
                        p=min(structure.t.low, structure.c.low)
                        + Decimal("0.01"),
                    )
                else:
                    raise AssertionError(mode)
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    replace(default, structure=structure),
                )
                self.assertEqual(
                    rejected.reason,
                    "N13_STATE_INCONSISTENT",
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT * FROM n13_rotation_states"
                        ).fetchall(),
                        [],
                    )

    def test_different_a_legacy_replay_requires_full_derived_evidence(self):
        windows = n13_historical_a_switch_windows()
        first, replay, _ = (
            analyze(window, checked)
            for window, checked in windows
        )
        event = first.historical_events[-1]
        detail = legacy_n13_state_detail(
            {
                "event": event.reason,
                "structure": event.structure.json(),
                "detail": event.detail,
            }
        )
        for key in (
            "atr_c",
            "entry_max_price",
            "zone_lower",
            "zone_upper",
        ):
            del detail["structure"][key]

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            item = candidate("S011USDT", 11)
            with recorder._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO n13_rotation_states (
                        strategy_id, symbol, t_time, structure_id,
                        status, reason, detail_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "N13",
                        item.symbol,
                        event.t_time,
                        event.structure.structure_id,
                        event.status,
                        event.reason,
                        json.dumps(detail, sort_keys=True),
                        "projected-created",
                        "projected-updated",
                    ),
                )
                before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    item.symbol,
                    event.t_time,
                )
            rejected = scheduler._apply_n13_state(
                N13_STRATEGY,
                item,
                replay,
            )
            self.assertEqual(rejected.reason, "N13_STATE_INCONSISTENT")
            with recorder._connect() as connection:
                self.assertEqual(
                    n13_full_row_snapshot(
                        connection,
                        "N13",
                        item.symbol,
                        event.t_time,
                    ),
                    before,
                )

    def test_historical_stable_conflict_rolls_back_earlier_valid_event(self):
        first, replay, _ = (
            analyze(window, checked)
            for window, checked in n13_historical_a_switch_windows()
        )
        original = first.historical_events[-1]
        switched = replay.historical_events[-1]
        self.assertNotEqual(
            original.structure.a.open_time_ms,
            switched.structure.a.open_time_ms,
        )

        stage_rows, stage_checked = flat_n13_klines()
        stage_rows[96] = kline(96, "101.2", "102.2", "101", "102")
        stage_rows = [
            kline(index, "100", "100.5", "99.5", "100")
            for index in range(-17, 0)
        ] + stage_rows
        stage_rows[0] = kline(-17, "100", "100.6", "99.4", "100")
        a_index = 17 + 96
        metrics = n13_metrics(parse_n13_klines(stage_rows[:-1]))
        lower = metrics[a_index].lower
        stage_rows[a_index + 1] = kline(
            97,
            lower - Decimal("0.10"),
            lower - Decimal("0.01"),
            lower - Decimal("0.20"),
            lower - Decimal("0.10"),
        )
        stage_event = analyze(stage_rows, stage_checked).stage_events[-1]
        self.assertEqual(stage_event.reason, "N13_VALUE_ZONE_SKIPPED")
        self.assertNotEqual(stage_event.t_time, original.t_time)

        stage_only = replace(
            replay,
            stage_events=(stage_event,),
            historical_events=(),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    stage_only,
                )
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT t_time FROM n13_rotation_states"
                    ).fetchall(),
                    [(stage_event.t_time,)],
                )

        def forge_stable_field(mode):
            structure = switched.structure
            if mode == "t_ohlc":
                return replace(
                    structure,
                    t=replace(
                        structure.t,
                        high=structure.t.high + Decimal("0.01"),
                    ),
                )
            if mode == "c_ohlc":
                return replace(
                    structure,
                    c=replace(
                        structure.c,
                        high=structure.c.high + Decimal("0.01"),
                    ),
                )
            if mode == "p":
                return replace(
                    structure,
                    p=structure.p + Decimal("0.01"),
                )
            if mode == "vwap":
                return replace(
                    structure,
                    vwap_c=structure.vwap_c + Decimal("0.01"),
                )
            if mode == "entry_min":
                return replace(
                    structure,
                    entry_min_price=(
                        structure.entry_min_price + Decimal("0.01")
                    ),
                )
            if mode == "failed_reasons":
                return replace(
                    structure,
                    failed_confirmation_reasons=(
                        "N13_CONFIRMATION_CLOSE_BELOW_VWAP",
                    ),
                )
            raise AssertionError(mode)

        for mode in (
            "t_ohlc",
            "c_ohlc",
            "p",
            "vwap",
            "entry_min",
            "failed_reasons",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                conflict = replace(
                    original,
                    structure=forge_stable_field(mode),
                )
                hostile = replace(
                    replay,
                    stage_events=(stage_event,),
                    historical_events=(original, conflict),
                )
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    hostile,
                )
                self.assertEqual(
                    rejected.reason,
                    "N13_STATE_INCONSISTENT",
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT * FROM n13_rotation_states"
                        ).fetchall(),
                        [],
                    )

    def test_runtime_stage_zone_evidence_must_self_prove(self):
        raw, checked = n13_klines()
        current = analyze(raw, checked)
        self.assertTrue(current.passed, current.reason)

        stage_rows, _ = flat_n13_klines()
        stage_rows[95] = kline(95, "101.2", "102.2", "101", "102")
        for index in range(96, 105):
            stage_rows[index] = kline(
                index,
                "101.2",
                "101.6",
                "101",
                "101.3",
            )
        stage_rows.append(kline(105, "101.2", "101.6", "101", "101.3"))
        stage = analyze(
            stage_rows,
            BASE_TIME_MS + 105 * INTERVAL_MS + 30_000,
        ).stage_events[-1]
        self.assertEqual(stage.reason, "N13_ARMED_WINDOW_EXPIRED")

        for mode in (
            "inverted_zone",
            "negative_zone_upper",
            "a_not_armed",
            "wrong_expiry",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                detail = deepcopy(stage.detail)
                if mode == "inverted_zone":
                    detail["zone_lower"] = "101"
                    detail["zone_upper"] = "100"
                elif mode == "negative_zone_upper":
                    detail["zone_lower"] = "-2"
                    detail["zone_upper"] = "-1"
                elif mode == "a_not_armed":
                    detail["zone_upper"] = detail["a"]["low"]
                elif mode == "wrong_expiry":
                    detail["armed_expiry_time_ms"] += INTERVAL_MS
                else:
                    raise AssertionError(mode)
                hostile = replace(
                    current,
                    stage_events=(replace(stage, detail=detail),),
                )
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    hostile,
                )
                self.assertEqual(
                    rejected.reason,
                    "N13_STATE_INCONSISTENT",
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT * FROM n13_rotation_states"
                        ).fetchall(),
                        [],
                    )

    def _assert_legacy_minimal_stage_replays(self, analyses, reason):
        first_event = next(
            event
            for event in analyses[0].stage_events
            if event.reason == reason
        )
        replay_events = [
            next(
                event
                for event in analysis.stage_events
                if event.t_time == first_event.t_time
                and event.reason == reason
            )
            for analysis in analyses[1:]
        ]
        self.assertEqual(len(replay_events), 2)
        self.assertIn("zone_lower", first_event.detail)
        self.assertTrue(
            all("zone_lower" in event.detail for event in replay_events)
        )
        minimal_detail = deepcopy(first_event.detail)
        del minimal_detail["zone_lower"]
        del minimal_detail["zone_upper"]
        persisted_detail = legacy_n13_state_detail(
            {
                "event": reason,
                "structure": None,
                "detail": minimal_detail,
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            item = candidate("S011USDT", 11)
            with recorder._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO n13_rotation_states (
                        strategy_id, symbol, t_time, structure_id,
                        status, reason, detail_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "N13",
                        item.symbol,
                        first_event.t_time,
                        None,
                        first_event.status,
                        reason,
                        json.dumps(persisted_detail, sort_keys=True),
                        "minimal-created",
                        "minimal-updated",
                    ),
                )
                before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    item.symbol,
                    first_event.t_time,
                )
            self.assertEqual(before[0], N13_STATE_COLUMNS)
            self.assertEqual(before[2], 1)
            for analysis in analyses[1:]:
                self.assertIsNone(
                    scheduler._apply_n13_state(
                        N13_STRATEGY,
                        item,
                        analysis,
                    )
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        n13_full_row_snapshot(
                            connection,
                            "N13",
                            item.symbol,
                            first_event.t_time,
                        ),
                        before,
                    )

    def test_legacy_armed_stage_without_zone_replays_full_events(self):
        rows, _ = flat_n13_klines()
        rows[95] = kline(95, "101.2", "102.2", "101", "102")
        for index in range(96, 105):
            rows[index] = kline(
                index,
                "101.2",
                "101.6",
                "101",
                "101.3",
            )
        rows = [
            kline(index, "100", "100.5", "99.5", "100")
            for index in range(-16, 0)
        ] + rows
        rows[0] = kline(-16, "100", "100.6", "99.4", "100")
        rows.append(kline(105, "101.3", "101.7", "101.1", "101.4"))
        analyses = []
        for offset in range(3):
            analyses.append(
                analyze(
                    rows,
                    BASE_TIME_MS
                    + (105 + offset) * INTERVAL_MS
                    + 30_000,
                )
            )
            rows = deepcopy(rows[1:])
            rows.append(
                kline(
                    106 + offset,
                    "101.4",
                    "101.8",
                    "101.2",
                    "101.5",
                )
            )
        self._assert_legacy_minimal_stage_replays(
            analyses,
            "N13_ARMED_WINDOW_EXPIRED",
        )

    def test_legacy_skipped_stage_without_zone_replays_full_events(self):
        rows, checked = flat_n13_klines()
        rows[96] = kline(96, "101.2", "102.2", "101", "102")
        rows = [
            kline(index, "100", "100.5", "99.5", "100")
            for index in range(-17, 0)
        ] + rows
        rows[0] = kline(-17, "100", "100.6", "99.4", "100")
        a_index = 17 + 96
        metrics = n13_metrics(parse_n13_klines(rows[:-1]))
        lower = metrics[a_index].lower
        rows[a_index + 1] = kline(
            97,
            lower - Decimal("0.10"),
            lower - Decimal("0.01"),
            lower - Decimal("0.20"),
            lower - Decimal("0.10"),
        )
        analyses = []
        for offset in range(3):
            analyses.append(
                analyze(rows, checked + offset * INTERVAL_MS)
            )
            rows = deepcopy(rows[1:])
            rows.append(kline(105 + offset, "100", "100.5", "99.5", "100"))
        self._assert_legacy_minimal_stage_replays(
            analyses,
            "N13_VALUE_ZONE_SKIPPED",
        )

    def test_legacy_t_stage_survives_natural_reason_switch(self):
        windows = n13_t_stage_reason_switch_windows()
        self.assertEqual([len(rows) for rows, _ in windows], [122, 122, 122])
        first = analyze(*windows[0])
        second = analyze(*windows[1])
        third = analyze(*windows[2])
        first_event = next(
            event
            for event in first.stage_events
            if event.reason == "N13_CONFIRMATION_NOT_FOUND"
        )
        second_event = next(
            event
            for event in second.stage_events
            if event.reason == "N13_VALUE_BAND_BROKEN"
        )
        self.assertEqual(first_event.t_time, second_event.t_time)
        self.assertEqual(
            legacy_n13_state_detail(first_event.detail["t"]),
            legacy_n13_state_detail(second_event.detail["t"]),
        )
        self.assertEqual(
            first_event.detail["failed_confirmation_reasons"],
            ["N13_CONFIRMATION_CLOSE_BELOW_VWAP"] * 4,
        )
        self.assertEqual(
            second_event.detail["failed_confirmation_reasons"],
            [],
        )
        third_event = next(
            event
            for event in third.stage_events
            if event.t_time == first_event.t_time
        )
        self.assertEqual(
            third_event.reason,
            "N13_CONFIRMATION_NOT_FOUND",
        )
        first_metrics = n13_metrics(
            parse_n13_klines(windows[0][0][:-1])
        )
        second_metrics = n13_metrics(
            parse_n13_klines(windows[1][0][:-1])
        )
        first_a = windows[0][0][112]
        second_a = windows[1][0][112]
        self.assertNotEqual(first_a[0], second_a[0])
        t_close = Decimal(first_event.detail["t"]["close"])
        self.assertLess(first_metrics[112].lower, t_close)
        self.assertGreater(second_metrics[112].lower, t_close)

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            legacy_detail = {
                "event": first_event.reason,
                "structure": None,
                "detail": {
                    "t": legacy_n13_state_detail(first_event.detail["t"]),
                    "failed_confirmation_reasons": [
                        "N13_CONFIRMATION_CLOSE_BELOW_VWAP",
                        "N13_CONFIRMATION_CLOSE_BELOW_VWAP",
                        "N13_CONFIRMATION_TAKER_BUY_RATIO_TOO_LOW",
                        "N13_CONFIRMATION_TAKER_BUY_RATIO_TOO_LOW",
                    ],
                },
            }
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically(
                    [
                        {
                            "strategy_id": "N13",
                            "symbol": "S011USDT",
                            "t_time": first_event.t_time,
                            "structure_id": None,
                            "status": first_event.status,
                            "reason": first_event.reason,
                            "detail": legacy_detail,
                        }
                    ]
                ),
                "OK",
            )
            with recorder._connect() as connection:
                before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    first_event.t_time,
                )
            self.assertEqual(before[0], N13_STATE_COLUMNS)
            self.assertIsNotNone(before[1])
            self.assertEqual(before[2], 1)
            before_hash = hashlib.sha256(
                repr(before).encode("utf-8")
            ).hexdigest()
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    second,
                )
            )
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    third,
                )
            )
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    second,
                )
            )
            with recorder._connect() as connection:
                after = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    first_event.t_time,
                )
            self.assertEqual(after, before)
            self.assertEqual(
                hashlib.sha256(repr(after).encode("utf-8")).hexdigest(),
                before_hash,
            )

    def test_legacy_t_stage_survives_reverse_natural_reason_switch(self):
        windows = n13_t_stage_reverse_reason_switch_windows()
        self.assertEqual([len(rows) for rows, _ in windows], [122, 122])
        first = analyze(*windows[0])
        second = analyze(*windows[1])
        first_event = next(
            event
            for event in first.stage_events
            if event.reason == "N13_VALUE_BAND_BROKEN"
        )
        second_event = next(
            event
            for event in second.stage_events
            if event.reason == "N13_CONFIRMATION_NOT_FOUND"
        )
        self.assertEqual(first_event.t_time, second_event.t_time)
        self.assertEqual(
            legacy_n13_state_detail(first_event.detail["t"]),
            legacy_n13_state_detail(second_event.detail["t"]),
        )
        self.assertEqual(first_event.detail["failed_confirmation_reasons"], [])
        self.assertEqual(
            second_event.detail["failed_confirmation_reasons"],
            ["N13_CONFIRMATION_CLOSE_BELOW_VWAP"] * 4,
        )
        self.assertNotEqual(
            first_event.detail["a"]["open_time_ms"],
            second_event.detail["a"]["open_time_ms"],
        )
        self.assertNotEqual(
            first_event.detail["zone_lower"],
            second_event.detail["zone_lower"],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            legacy_detail = {
                "event": first_event.reason,
                "structure": None,
                "detail": {
                    "t": legacy_n13_state_detail(first_event.detail["t"]),
                    "failed_confirmation_reasons": [],
                },
            }
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically(
                    [
                        {
                            "strategy_id": "N13",
                            "symbol": "S011USDT",
                            "t_time": first_event.t_time,
                            "structure_id": None,
                            "status": "INVALID",
                            "reason": first_event.reason,
                            "detail": legacy_detail,
                        }
                    ]
                ),
                "OK",
            )
            with recorder._connect() as connection:
                before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    first_event.t_time,
                )
            self.assertEqual(before[0], N13_STATE_COLUMNS)
            self.assertIsNotNone(before[1])
            self.assertEqual(before[2], 1)
            before_hash = hashlib.sha256(
                repr(before).encode("utf-8")
            ).hexdigest()
            for replay in (second, second):
                self.assertIsNone(
                    scheduler._apply_n13_state(
                        N13_STRATEGY,
                        candidate("S011USDT", 11),
                        replay,
                    )
                )
            with recorder._connect() as connection:
                after = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    first_event.t_time,
                )
            self.assertEqual(after, before)
            self.assertEqual(
                hashlib.sha256(repr(after).encode("utf-8")).hexdigest(),
                before_hash,
            )

    def test_new_t_stage_format_is_strict_and_replays_natural_switches(self):
        analyses = [
            analyze(rows, checked)
            for rows, checked in n13_t_stage_reason_switch_windows()
        ]
        events = [analysis.stage_events[0] for analysis in analyses]
        self.assertEqual(
            [event.reason for event in events],
            [
                "N13_CONFIRMATION_NOT_FOUND",
                "N13_VALUE_BAND_BROKEN",
                "N13_CONFIRMATION_NOT_FOUND",
            ],
        )
        self.assertEqual(events[0].t_time, events[1].t_time)
        self.assertEqual(events[0].t_time, events[2].t_time)
        self.assertNotEqual(
            events[0].detail["a"]["open_time_ms"],
            events[1].detail["a"]["open_time_ms"],
        )
        self.assertEqual(
            events[0].detail["a"]["open_time_ms"],
            events[2].detail["a"]["open_time_ms"],
        )
        self.assertNotEqual(
            events[0].detail["zone_lower"],
            events[2].detail["zone_lower"],
        )
        self.assertNotEqual(
            events[0].detail["zone_upper"],
            events[2].detail["zone_upper"],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    analyses[0],
                )
            )
            with recorder._connect() as connection:
                before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    events[0].t_time,
                )
            stored = json.loads(before[1][7])
            self.assertEqual(stored["detail"]["schema_version"], 2)
            self.assertIn("a", stored["detail"])
            self.assertGreater(Decimal(stored["detail"]["vwap_a"]), 0)
            self.assertGreater(Decimal(stored["detail"]["atr_a"]), 0)
            self.assertIn("zone_lower", stored["detail"])
            self.assertIn("confirmation_checks", stored["detail"])
            self.assertEqual(
                stored["detail"]["vwap_lookback_bars"],
                N13_STRATEGY.n13_vwap_lookback_bars,
            )
            self.assertEqual(
                stored["detail"]["atr_period"],
                N13_STRATEGY.n13_atr_period,
            )
            self.assertEqual(
                stored["detail"]["upper_atr_fraction"],
                str(N13_STRATEGY.n13_upper_atr_fraction),
            )
            self.assertEqual(
                stored["detail"]["lower_atr_fraction"],
                str(N13_STRATEGY.n13_lower_atr_fraction),
            )
            resigned = resign_n13_t_stage_detail(
                deepcopy(stored["detail"])
            )
            self.assertEqual(
                resigned["canonical_sha256"],
                stored["detail"]["canonical_sha256"],
            )
            for replay in (
                analyses[0],
                analyses[1],
                analyses[2],
                analyses[1],
            ):
                self.assertIsNone(
                    scheduler._apply_n13_state(
                        N13_STRATEGY,
                        candidate("S011USDT", 11),
                        replay,
                    )
                )
            with recorder._connect() as connection:
                after = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    events[0].t_time,
                )
            self.assertEqual(after, before)

    def test_t_stage_metric_source_is_rotation_only_and_size_bounded(self):
        analysis = analyze(*n13_t_stage_reason_switch_windows()[0])
        event = analysis.stage_events[0]
        full_detail = legacy_n13_state_detail(event.detail)
        compact_detail = analysis.detail_json()["stage_events"][0]["detail"]
        legacy_detail = {
            "t": legacy_n13_state_detail(event.detail["t"]),
            "failed_confirmation_reasons": list(
                event.detail["failed_confirmation_reasons"]
            ),
        }
        previous_candidate = deepcopy(full_detail)
        previous_candidate.pop("metric_context_bars")
        previous_candidate["schema_version"] = 1
        resign_n13_t_stage_detail(previous_candidate)

        def encoded_size(value):
            return len(
                json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            )

        full_size = encoded_size(full_detail)
        previous_size = encoded_size(previous_candidate)
        legacy_size = encoded_size(legacy_detail)
        compact_size = encoded_size(compact_detail)
        self.assertEqual(len(full_detail["metric_context_bars"]), 121)
        self.assertGreater(full_size, previous_size)
        self.assertGreater(previous_size, legacy_size)
        self.assertLessEqual(full_size, 64 * 1024)
        self.assertLessEqual(compact_size, 2 * 1024)
        self.assertLess(compact_size, previous_size)
        self.assertNotIn("metric_context_bars", compact_detail)
        self.assertEqual(
            compact_detail["metric_context_bar_count"],
            len(full_detail["metric_context_bars"]),
        )
        self.assertEqual(
            compact_detail["canonical_sha256"],
            full_detail["canonical_sha256"],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    analysis,
                )
            )
            with recorder._connect() as connection:
                state_before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    event.t_time,
                )
            decision = scheduler._rejected(
                N13_STRATEGY,
                candidate("S011USDT", 11),
                analysis.reason,
                analysis,
            )
            scan_id = recorder.begin_scan(
                1,
                [candidate("S011USDT", 11)],
                True,
            )
            self.assertIsNotNone(scan_id)
            for _ in range(2):
                self.assertIsNotNone(
                    scheduler._record_signal(scan_id, decision)
                )
                self.assertIsNone(
                    scheduler._apply_n13_state(
                        N13_STRATEGY,
                        candidate("S011USDT", 11),
                        analysis,
                    )
                )
            with recorder._connect() as connection:
                state_after = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    event.t_time,
                )
                signal_rows = connection.execute(
                    "SELECT detail_json FROM strategy_signals "
                    "WHERE strategy_id='N13' ORDER BY id"
                ).fetchall()
            self.assertEqual(state_after, state_before)
            self.assertEqual(state_after[2], 1)
            self.assertEqual(len(signal_rows), 2)
            for (signal_json,) in signal_rows:
                self.assertNotIn("metric_context_bars", signal_json)
                self.assertLessEqual(len(signal_json.encode("utf-8")), 4096)

    def test_invalid_n13_audit_details_are_bounded_across_reuse_paths(self):
        stage_analysis = analyze(*n13_t_stage_reason_switch_windows()[0])
        stage_event = stage_analysis.stage_events[0]
        historical_analysis = analyze(*n13_historical_a_switch_windows()[0])
        historical_event = historical_analysis.historical_events[0]
        huge = "X" * 1_000_000

        def stage_with_detail(detail):
            return replace(
                stage_analysis,
                stage_events=(replace(stage_event, detail=detail),),
            )

        schema = deepcopy(stage_event.detail)
        schema["schema_version"] = 1
        resign_n13_t_stage_detail(schema)
        missing_checks = deepcopy(stage_event.detail)
        missing_checks.pop("confirmation_checks")
        resign_n13_t_stage_detail(missing_checks)
        too_many_checks = deepcopy(stage_event.detail)
        too_many_checks["confirmation_checks"] = (
            too_many_checks["confirmation_checks"] * 20
        )
        resign_n13_t_stage_detail(too_many_checks)
        smaller_confirmation_max = deepcopy(stage_event.detail)
        smaller_confirmation_max["confirmation_max_bars"] = 1
        resign_n13_t_stage_detail(smaller_confirmation_max)
        invalid_outcome = deepcopy(stage_event.detail)
        invalid_outcome["confirmation_checks"][0]["outcome"] = "N13_FAKE"
        resign_n13_t_stage_detail(invalid_outcome)
        nonhex_hash = deepcopy(stage_event.detail)
        nonhex_hash["canonical_sha256"] = "G" * 64
        huge_hash = deepcopy(stage_event.detail)
        huge_hash["canonical_sha256"] = huge
        huge_t = deepcopy(stage_event.detail)
        huge_t["t"]["close"] = huge
        resign_n13_t_stage_detail(huge_t)
        huge_zone = deepcopy(stage_event.detail)
        huge_zone["zone_upper"] = huge
        resign_n13_t_stage_detail(huge_zone)
        huge_failed = deepcopy(stage_event.detail)
        huge_failed["failed_confirmation_reasons"] = [huge]
        resign_n13_t_stage_detail(huge_failed)
        non_list_context = deepcopy(stage_event.detail)
        non_list_context["metric_context_bars"] = huge
        resign_n13_t_stage_detail(non_list_context)
        historical_huge = replace(
            historical_analysis,
            historical_events=(
                replace(
                    historical_event,
                    detail={"metric_context_bars": [huge]},
                ),
            ),
        )
        cases = {
            "schema": stage_with_detail(schema),
            "missing_checks": stage_with_detail(missing_checks),
            "too_many_checks": stage_with_detail(too_many_checks),
            "smaller_confirmation_max": stage_with_detail(
                smaller_confirmation_max
            ),
            "invalid_outcome": stage_with_detail(invalid_outcome),
            "nonhex_hash": stage_with_detail(nonhex_hash),
            "huge_hash": stage_with_detail(huge_hash),
            "huge_t": stage_with_detail(huge_t),
            "huge_zone": stage_with_detail(huge_zone),
            "huge_failed": stage_with_detail(huge_failed),
            "non_list_context": stage_with_detail(non_list_context),
            "nested_huge": stage_with_detail(
                {"outer": {"metric_context_bars": [huge]}}
            ),
            "unknown_huge": stage_with_detail({"x": huge}),
            "non_dict_huge": stage_with_detail(huge),
            "historical_huge": historical_huge,
        }

        self.assertEqual(
            cases["schema"].detail_json()["stage_events"][0]["detail"][
                "schema_version"
            ],
            "INVALID",
        )
        self.assertEqual(
            cases["missing_checks"].detail_json()["stage_events"][0][
                "detail"
            ]["audit_detail_status"],
            "INVALID",
        )
        for mode in (
            "too_many_checks",
            "smaller_confirmation_max",
            "invalid_outcome",
        ):
            summary = cases[mode].detail_json()["stage_events"][0][
                "detail"
            ]
            self.assertEqual(summary["confirmation_outcomes"], "INVALID")
            if mode != "invalid_outcome":
                self.assertEqual(
                    summary["confirmation_check_count"],
                    "INVALID",
                )
        self.assertEqual(
            cases["nonhex_hash"].detail_json()["stage_events"][0]["detail"][
                "canonical_sha256"
            ],
            "INVALID",
        )
        self.assertEqual(
            cases["huge_t"].detail_json()["stage_events"][0]["detail"]["t"],
            "INVALID",
        )
        self.assertEqual(
            cases["huge_zone"].detail_json()["stage_events"][0]["detail"][
                "zone_upper"
            ],
            "INVALID",
        )
        self.assertEqual(
            cases["huge_failed"].detail_json()["stage_events"][0][
                "detail"
            ]["failed_confirmation_reason_count"],
            "INVALID",
        )

        for mode, hostile in cases.items():
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                decision = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    hostile,
                )
                self.assertIsNotNone(decision)
                self.assertEqual(decision.reason, "N13_STATE_INCONSISTENT")
                bounded = hostile.detail_json()
                bounded_json = json.dumps(
                    bounded,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                self.assertNotIn("metric_context_bars", bounded_json)
                self.assertNotIn(huge[:128], bounded_json)
                self.assertLessEqual(len(bounded_json.encode("utf-8")), 4096)
                scan_id = recorder.begin_scan(
                    1,
                    [candidate("S011USDT", 11)],
                    True,
                )
                self.assertIsNotNone(scan_id)
                signal_id = scheduler._record_signal(scan_id, decision)
                self.assertIsNotNone(signal_id)
                recorder.update_strategy_signal(
                    signal_id,
                    decision="LIVE_OPENED",
                    reason="LIVE_OPENED",
                    detail={"trade_review_id": 1, "paper_fallback": False},
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM n13_rotation_states"
                        ).fetchone()[0],
                        0,
                    )
                    signal_json = connection.execute(
                        "SELECT detail_json FROM strategy_signals WHERE id=?",
                        (signal_id,),
                    ).fetchone()[0]
                self.assertNotIn("metric_context_bars", signal_json)
                self.assertNotIn(huge[:128], signal_json)
                self.assertLessEqual(len(signal_json.encode("utf-8")), 4096)

                bot = object.__new__(TradingBot)
                paper_detail = bot._paper_trade_detail(
                    1,
                    decision,
                    paper_plan("S011USDT"),
                )
                paper_json = json.dumps(
                    paper_detail,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                self.assertNotIn("metric_context_bars", paper_json)
                self.assertNotIn(huge[:128], paper_json)
                self.assertLessEqual(len(paper_json.encode("utf-8")), 4096)
                paper = PaperTrader(
                    recorder,
                    logging.getLogger("n13_bounded_paper"),
                )
                trade_id = paper.open_trade(
                    "N13",
                    "S011USDT",
                    None,
                    paper_plan("S011USDT"),
                    paper_detail,
                )
                self.assertIsNotNone(trade_id)
                with recorder._connect() as connection:
                    persisted_paper = connection.execute(
                        "SELECT detail_json FROM strategy_paper_trades "
                        "WHERE id=?",
                        (trade_id,),
                    ).fetchone()[0]
                self.assertNotIn("metric_context_bars", persisted_paper)
                self.assertNotIn(huge[:128], persisted_paper)
                self.assertLessEqual(
                    len(persisted_paper.encode("utf-8")),
                    4096,
                )

        # Keep the hostile integer convertible under CPython's default
        # 4,300-digit safety limit while remaining far beyond signed 64-bit
        # time/rank bounds and large enough to expose accidental JSON copying.
        huge_int = 10 ** 4_000
        self.assertGreater(huge_int, 9_223_372_036_854_775_807)
        huge_int_prefix = str(huge_int)[:128]
        huge_time_detail = deepcopy(stage_event.detail)
        huge_time_detail["t"]["open_time_ms"] = huge_int
        huge_time = stage_with_detail(huge_time_detail)
        huge_outer_time = replace(
            stage_analysis,
            stage_events=(
                replace(stage_event, t_time=str(huge_int)),
            ),
        )
        many_events = replace(
            stage_analysis,
            stage_events=(stage_event,) * 10_001,
        )
        current = analyze(*n13_klines())
        hostile_structure = replace(
            current.structure,
            symbol=huge,
            a=replace(current.structure.a, open_time_ms=huge_int),
            failed_confirmation_reasons=(huge,),
        )
        hostile_current = replace(
            current,
            structure=hostile_structure,
            elapsed_ms=huge_int,
            return_rank=huge_int,
        )
        for mode, hostile in {
            "huge_inner_time": huge_time,
            "huge_outer_time": huge_outer_time,
            "huge_event_count": many_events,
            "hostile_current_structure": hostile_current,
        }.items():
            with self.subTest(mode=mode):
                bounded_json = json.dumps(
                    hostile.detail_json(),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                self.assertNotIn("metric_context_bars", bounded_json)
                self.assertNotIn(huge[:128], bounded_json)
                self.assertNotIn(huge_int_prefix, bounded_json)
                self.assertLessEqual(len(bounded_json.encode("utf-8")), 4096)

    def test_t_stage_prefix_is_slope_and_active_current_independent(self):
        rows, checked = n13_t_stage_reason_switch_windows()[0]
        expected = analyze(
            rows,
            checked,
            vwap_slope_lookback_bars=1,
        ).stage_events[0]
        self.assertGreaterEqual(
            len(expected.detail["metric_context_bars"]),
            N13_STRATEGY.n13_vwap_lookback_bars + 8,
        )
        for slope in (1, 7, 8):
            with self.subTest(slope=slope):
                replay = analyze(
                    rows,
                    checked,
                    vwap_slope_lookback_bars=slope,
                ).stage_events[0]
                self.assertEqual(replay.t_time, expected.t_time)
                self.assertEqual(replay.status, expected.status)
                self.assertEqual(replay.reason, expected.reason)
                self.assertEqual(
                    legacy_n13_state_detail(replay.detail),
                    legacy_n13_state_detail(expected.detail),
                )

        for current in (
            kline(
                104,
                "100",
                "200",
                "1",
                "150",
                base_volume="100",
                taker_ratio="0.9",
            ),
            kline(
                104,
                "50",
                "60",
                "40",
                "55",
                base_volume="0.1",
                taker_ratio="0.1",
            ),
        ):
            changed = deepcopy(rows)
            changed[-1] = current
            replay = analyze(
                changed,
                checked,
                vwap_slope_lookback_bars=1,
            ).stage_events[0]
            self.assertEqual(replay.t_time, expected.t_time)
            self.assertEqual(replay.reason, expected.reason)
            self.assertEqual(
                legacy_n13_state_detail(replay.detail),
                legacy_n13_state_detail(expected.detail),
            )

    def test_n13_reused_payloads_have_a_final_persistence_bound(self):
        huge = "X" * 1_000_000
        stage_analysis = analyze(*n13_t_stage_reason_switch_windows()[0])
        stage_event = stage_analysis.stage_events[0]
        passed = analyze(*n13_klines())
        passed_with_three_valid_stages = replace(
            passed,
            stage_events=(stage_event, stage_event, stage_event),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    passed_with_three_valid_stages,
                )
            )
            decision = StrategySignalDecision(
                strategy=N13_STRATEGY,
                candidate=candidate("S011USDT", 11),
                analysis=passed_with_three_valid_stages,
                passed=True,
                decision="PASSED",
                reason="PASSED",
            )
            scan_id = recorder.begin_scan(
                1,
                [candidate("S011USDT", 11)],
                True,
            )
            self.assertIsNotNone(scan_id)
            signal_id = scheduler._record_signal(scan_id, decision)
            self.assertIsNotNone(signal_id)
            bot = object.__new__(TradingBot)
            paper_detail = bot._paper_trade_detail(
                1,
                decision,
                paper_plan("S011USDT"),
            )
            paper_text = json.dumps(
                paper_detail,
                ensure_ascii=False,
                sort_keys=True,
            )
            self.assertNotIn("metric_context_bars", paper_text)
            self.assertLessEqual(len(paper_text.encode("utf-8")), 4096)
            trade_id = recorder.open_strategy_paper_trade(
                "N13",
                "S011USDT",
                "100",
                "99",
                "105",
                "",
                {},
                paper_detail,
            )
            self.assertIsNotNone(trade_id)
            with recorder._connect() as connection:
                signal_text = connection.execute(
                    "SELECT detail_json FROM strategy_signals WHERE id=?",
                    (signal_id,),
                ).fetchone()[0]
                paper_text = connection.execute(
                    "SELECT detail_json FROM strategy_paper_trades WHERE id=?",
                    (trade_id,),
                ).fetchone()[0]
            for payload in (signal_text, paper_text):
                self.assertNotIn("metric_context_bars", payload)
                self.assertLessEqual(len(payload.encode("utf-8")), 4096)

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            hostile_candidate = replace(
                candidate("S011USDT", 11),
                candidate_universe={"metric_context_bars": [huge]},
            )
            decision = scheduler._rejected(
                N13_STRATEGY,
                hostile_candidate,
                stage_analysis.reason,
                stage_analysis,
            )
            scan_id = recorder.begin_scan(
                1,
                [candidate("S011USDT", 11)],
                True,
            )
            self.assertIsNotNone(scan_id)
            signal_id = scheduler._record_signal(scan_id, decision)
            self.assertIsNotNone(signal_id)
            recorder.update_strategy_signal(
                signal_id,
                decision="LIVE_OPENED",
                reason="LIVE_OPENED",
                detail={
                    "execution_plan": {
                        "metric_context_bars": [huge],
                    }
                },
            )
            bot = object.__new__(TradingBot)
            hostile_plan = replace(
                paper_plan("S011USDT"),
                structure_context={"metric_context_bars": [huge]},
            )
            hostile_paper_detail = bot._paper_trade_detail(
                1,
                decision,
                hostile_plan,
            )
            trade_id = recorder.open_strategy_paper_trade(
                "N13",
                "S011USDT",
                "100",
                "99",
                "105",
                "",
                {},
                hostile_paper_detail,
            )
            self.assertIsNotNone(trade_id)
            recorder.close_strategy_paper_trade(
                trade_id,
                "LOSS",
                "STOP_LOSS",
                "99",
                "-1",
                {"metric_context_bars": [huge]},
            )
            with recorder._connect() as connection:
                payloads = (
                    connection.execute(
                        "SELECT detail_json FROM strategy_signals WHERE id=?",
                        (signal_id,),
                    ).fetchone()[0],
                    connection.execute(
                        "SELECT detail_json FROM strategy_paper_trades WHERE id=?",
                        (trade_id,),
                    ).fetchone()[0],
                )
            for payload in payloads:
                self.assertNotIn("metric_context_bars", payload)
                self.assertNotIn(huge[:128], payload)
                self.assertLessEqual(len(payload.encode("utf-8")), 4096)

    def test_new_t_stage_nondefault_config_signature_replays_exactly(self):
        strategy = replace(
            N13_STRATEGY,
            n13_vwap_lookback_bars=95,
            n13_atr_period=13,
            n13_upper_atr_fraction=Decimal("0.26"),
            n13_lower_atr_fraction=Decimal("0.49"),
            n13_confirmation_close_location_min=Decimal("0.59"),
            n13_confirmation_taker_buy_ratio_min=Decimal("0.49"),
        )
        rows, checked = n13_t_stage_reason_switch_windows()[0]
        analysis = analyze(
            rows,
            checked,
            vwap_lookback_bars=strategy.n13_vwap_lookback_bars,
            atr_period=strategy.n13_atr_period,
            upper_atr_fraction=strategy.n13_upper_atr_fraction,
            lower_atr_fraction=strategy.n13_lower_atr_fraction,
            armed_max_bars=strategy.n13_armed_max_bars,
            confirmation_max_bars=strategy.n13_confirmation_max_bars,
            confirmation_close_location_min=(
                strategy.n13_confirmation_close_location_min
            ),
            confirmation_taker_buy_ratio_min=(
                strategy.n13_confirmation_taker_buy_ratio_min
            ),
            entry_extension_atr_max=(
                strategy.n13_entry_extension_atr_max
            ),
        )
        event = analysis.stage_events[0]
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir, strategy)
            for _ in range(2):
                self.assertIsNone(
                    scheduler._apply_n13_state(
                        strategy,
                        candidate("S011USDT", 11),
                        analysis,
                    )
                )
            with recorder._connect() as connection:
                columns, row, count = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    event.t_time,
                )
            self.assertEqual(columns, N13_STATE_COLUMNS)
            self.assertEqual(count, 1)
            detail = json.loads(row[7])["detail"]
            self.assertEqual(detail["vwap_lookback_bars"], 95)
            self.assertEqual(detail["atr_period"], 13)
            self.assertEqual(detail["upper_atr_fraction"], "0.26")
            self.assertEqual(detail["lower_atr_fraction"], "0.49")
            self.assertEqual(
                detail["confirmation_close_location_min"],
                "0.59",
            )
            self.assertEqual(
                detail["confirmation_taker_buy_ratio_min"],
                "0.49",
            )

    def test_legacy_t_stage_collision_does_not_block_valid_current_modes(self):
        stage_analyses = [
            analyze(rows, checked)
            for rows, checked in n13_t_stage_reason_switch_windows()[:2]
        ]
        legacy_event = stage_analyses[0].stage_events[0]
        replay_event = stage_analyses[1].stage_events[0]
        raw, checked = n13_klines()
        waiting_raw = deepcopy(raw)
        waiting_raw[-1] = kline(
            104,
            "101.4",
            "101.6",
            "101.2",
            "101.3",
        )
        modes = (
            ("waiting", analyze(waiting_raw, checked), None, None),
            ("passed", analyze(raw, checked), "CONSUMED", "PASSED"),
            (
                "terminal",
                analyze(
                    raw,
                    BASE_TIME_MS + 104 * INTERVAL_MS + 120_000,
                ),
                "MISSED",
                "N13_ENTRY_WINDOW_EXPIRED",
            ),
        )
        self.assertEqual(modes[0][1].reason, "N13_WAITING_ENTRY_PRICE")
        self.assertIsNotNone(modes[0][1].structure.entry)
        self.assertEqual(modes[1][1].reason, "PASSED")
        self.assertEqual(modes[2][1].reason, "N13_ENTRY_WINDOW_EXPIRED")
        for name, current, expected_status, expected_reason in modes:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                legacy_detail = {
                    "event": legacy_event.reason,
                    "structure": None,
                    "detail": {
                        "t": legacy_n13_state_detail(
                            legacy_event.detail["t"]
                        ),
                        "failed_confirmation_reasons": list(
                            legacy_event.detail[
                                "failed_confirmation_reasons"
                            ]
                        ),
                    },
                }
                self.assertEqual(
                    recorder.record_n13_rotation_states_atomically(
                        [
                            {
                                "strategy_id": "N13",
                                "symbol": "S011USDT",
                                "t_time": legacy_event.t_time,
                                "structure_id": None,
                                "status": "INVALID",
                                "reason": legacy_event.reason,
                                "detail": legacy_detail,
                            }
                        ]
                    ),
                    "OK",
                )
                with recorder._connect() as connection:
                    legacy_before = n13_full_row_snapshot(
                        connection,
                        "N13",
                        "S011USDT",
                        legacy_event.t_time,
                    )
                combined = replace(
                    current,
                    stage_events=(replay_event,),
                )
                self.assertIsNone(
                    scheduler._apply_n13_state(
                        N13_STRATEGY,
                        candidate("S011USDT", 11),
                        combined,
                    )
                )
                with recorder._connect() as connection:
                    legacy_after = n13_full_row_snapshot(
                        connection,
                        "N13",
                        "S011USDT",
                        legacy_event.t_time,
                    )
                    rows = connection.execute(
                        "SELECT t_time,status,reason FROM "
                        "n13_rotation_states ORDER BY t_time"
                    ).fetchall()
                self.assertEqual(legacy_after[:2], legacy_before[:2])
                if expected_status is None:
                    self.assertEqual(legacy_after[2], legacy_before[2])
                    self.assertEqual(len(rows), 1)
                    self.assertIsNone(
                        scheduler._apply_n13_state(
                            N13_STRATEGY,
                            candidate("S011USDT", 11),
                            combined,
                        )
                    )
                else:
                    self.assertEqual(
                        legacy_after[2], legacy_before[2] + 1
                    )
                    self.assertEqual(len(rows), 2)
                    current_row = next(
                        row
                        for row in rows
                        if row[0]
                        == str(current.structure.t.open_time_ms)
                    )
                    self.assertEqual(
                        current_row[1:],
                        (expected_status, expected_reason),
                    )
                    replay = scheduler._apply_n13_state(
                        N13_STRATEGY,
                        candidate("S011USDT", 11),
                        combined,
                    )
                    self.assertEqual(
                        replay.reason,
                        "N13_EPISODE_CONSUMED",
                    )

    def test_isolated_t_stage_warning_is_bounded_but_matcher_always_runs(self):
        analyses = [
            analyze(rows, checked)
            for rows, checked in n13_t_stage_reason_switch_windows()
        ]
        legacy_event = analyses[0].stage_events[0]
        legacy_detail = {
            "event": legacy_event.reason,
            "structure": None,
            "detail": {
                "t": legacy_n13_state_detail(legacy_event.detail["t"]),
                "failed_confirmation_reasons": list(
                    legacy_event.detail["failed_confirmation_reasons"]
                ),
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            logger = Mock()
            scheduler = StrategyScheduler(
                (N13_STRATEGY,),
                96,
                recorder,
                logger,
            )
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically(
                    [
                        {
                            "strategy_id": "N13",
                            "symbol": "S011USDT",
                            "t_time": legacy_event.t_time,
                            "structure_id": None,
                            "status": legacy_event.status,
                            "reason": legacy_event.reason,
                            "detail": legacy_detail,
                        }
                    ]
                ),
                "OK",
            )
            with patch(
                "trading_bot.strategy_scheduler."
                "_n13_existing_event_state_match",
                wraps=_n13_existing_event_state_match,
            ) as matcher:
                for replay in (analyses[1], analyses[1], analyses[1]):
                    self.assertIsNone(
                        scheduler._apply_n13_state(
                            N13_STRATEGY,
                            candidate("S011USDT", 11),
                            replay,
                        )
                    )
                self.assertEqual(matcher.call_count, 3)
                isolated = [
                    call
                    for call in logger.warning.call_args_list
                    if "collision isolated" in call.args[0]
                ]
                self.assertEqual(len(isolated), 1)

                # A completed scheduler round emits one bounded aggregate for
                # the two repeated fingerprints instead of two more details.
                scan_id = recorder.begin_scan(0, [], dry_run=True)
                completed = scheduler.evaluate(
                    scan_id, [], {}, BASE_TIME_MS
                )
                self.assertEqual(completed.signal_audit_failures, ())
                self.assertTrue(completed.signal_batch_published)
                self.assertEqual(
                    recorder.current_strategy_signal_scan_id(), scan_id
                )
                summaries = [
                    call
                    for call in logger.warning.call_args_list
                    if "replay summary" in call.args[0]
                ]
                self.assertEqual(len(summaries), 1)
                self.assertEqual(
                    summaries[0].args[1:], (scan_id, 2, "1")
                )

                # A new strict-valid replay variant is not hidden by the
                # limiter and receives its own complete fingerprinted warning.
                self.assertIsNone(
                    scheduler._apply_n13_state(
                        N13_STRATEGY,
                        candidate("S011USDT", 11),
                        analyses[2],
                    )
                )
                self.assertEqual(matcher.call_count, 4)
                isolated = [
                    call
                    for call in logger.warning.call_args_list
                    if "collision isolated" in call.args[0]
                ]
                self.assertEqual(len(isolated), 2)
                self.assertNotEqual(
                    isolated[0].args[-1],
                    isolated[1].args[-1],
                )

    def test_isolated_warning_cache_is_bounded_and_conflicts_are_never_limited(self):
        analyses = [
            analyze(rows, checked)
            for rows, checked in n13_t_stage_reason_switch_windows()[:2]
        ]
        legacy_event = analyses[0].stage_events[0]
        legacy_detail = {
            "event": legacy_event.reason,
            "structure": None,
            "detail": {
                "t": legacy_n13_state_detail(legacy_event.detail["t"]),
                "failed_confirmation_reasons": list(
                    legacy_event.detail["failed_confirmation_reasons"]
                ),
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            logger = Mock()
            scheduler = StrategyScheduler(
                (N13_STRATEGY,),
                96,
                recorder,
                logger,
            )
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically(
                    [
                        {
                            "strategy_id": "N13",
                            "symbol": "S011USDT",
                            "t_time": legacy_event.t_time,
                            "structure_id": None,
                            "status": legacy_event.status,
                            "reason": legacy_event.reason,
                            "detail": legacy_detail,
                        }
                    ]
                ),
                "OK",
            )
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    analyses[1],
                )
            )
            with recorder._connect() as connection:
                connection.execute(
                    "UPDATE n13_rotation_states SET status='CONSUMED' "
                    "WHERE strategy_id='N13' AND symbol='S011USDT' "
                    "AND t_time=?",
                    (legacy_event.t_time,),
                )
                before = connection.execute(
                    "SELECT * FROM n13_rotation_states ORDER BY id"
                ).fetchall()
            for _ in range(2):
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    analyses[1],
                )
                self.assertEqual(
                    rejected.reason,
                    "N13_STATE_INCONSISTENT",
                )
            conflicts = [
                call
                for call in logger.warning.call_args_list
                if "rotation state conflict detected" in call.args[0]
            ]
            self.assertEqual(len(conflicts), 2)
            self.assertTrue(all(len(call.args[-1]) == 64 for call in conflicts))
            with recorder._connect() as connection:
                after = connection.execute(
                    "SELECT * FROM n13_rotation_states ORDER BY id"
                ).fetchall()
            self.assertEqual(after, before)

            # Exercise the LRU bound with a small patched capacity; eviction
            # only permits a future detailed warning and cannot skip matching.
            existing = tuple(before[0])
            replay_record = {
                "strategy_id": "N13",
                "symbol": "S011USDT",
                "t_time": legacy_event.t_time,
                "structure_id": None,
                "status": "INVALID",
                "reason": legacy_event.reason,
                "detail": legacy_detail,
            }
            with patch(
                "trading_bot.strategy_scheduler."
                "_N13_ISOLATED_WARNING_FINGERPRINT_CACHE_MAX",
                3,
            ):
                for index in range(4):
                    variant = deepcopy(replay_record)
                    variant["detail"]["variant"] = index
                    scheduler._warn_n13_isolated_collision(
                        existing,
                        variant,
                    )
                self.assertLessEqual(
                    len(scheduler._n13_isolated_warning_fingerprints),
                    3,
                )

    def test_same_batch_historical_and_current_key_conflict_writes_nothing(self):
        raw, checked = n13_klines()
        current = analyze(raw, checked)
        self.assertTrue(current.passed, current.reason)
        historical_raw = deepcopy(raw)
        historical_raw.append(
            kline(105, "101.5", "101.9", "101.3", "101.6")
        )
        historical = analyze(
            historical_raw,
            BASE_TIME_MS + 105 * INTERVAL_MS + 30_000,
        )
        historical_event = historical.historical_events[-1]
        self.assertEqual(
            historical_event.t_time,
            str(current.structure.t.open_time_ms),
        )
        combined = replace(
            current,
            historical_events=(historical_event,),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            rejected = scheduler._apply_n13_state(
                N13_STRATEGY,
                candidate("S011USDT", 11),
                combined,
            )
            self.assertEqual(rejected.reason, "N13_STATE_INCONSISTENT")
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM n13_rotation_states"
                    ).fetchall(),
                    [],
                )

    def test_legacy_t_stage_at_current_key_never_authorizes_entry(self):
        raw, checked = n13_klines()
        waiting_raw = deepcopy(raw)
        waiting_raw[-1] = kline(
            104,
            "101.4",
            "101.6",
            "101.2",
            "101.3",
        )
        analyses = (
            ("waiting", analyze(waiting_raw, checked)),
            ("passed", analyze(raw, checked)),
        )
        self.assertEqual(analyses[0][1].reason, "N13_WAITING_ENTRY_PRICE")
        self.assertTrue(analyses[1][1].passed, analyses[1][1].reason)
        for name, analysis in analyses:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                structure = analysis.structure
                self.assertIsNotNone(structure)
                self.assertIsNotNone(structure.entry)
                t_time = str(structure.t.open_time_ms)
                legacy_detail = {
                    "event": "N13_VALUE_BAND_BROKEN",
                    "structure": None,
                    "detail": {
                        "t": legacy_n13_state_detail(structure.t.json()),
                        "failed_confirmation_reasons": [],
                    },
                }
                self.assertEqual(
                    recorder.record_n13_rotation_states_atomically(
                        [
                            {
                                "strategy_id": "N13",
                                "symbol": "S011USDT",
                                "t_time": t_time,
                                "structure_id": None,
                                "status": "INVALID",
                                "reason": "N13_VALUE_BAND_BROKEN",
                                "detail": legacy_detail,
                            }
                        ]
                    ),
                    "OK",
                )
                with recorder._connect() as connection:
                    before = n13_full_row_snapshot(
                        connection,
                        "N13",
                        "S011USDT",
                        t_time,
                    )
                self.assertEqual(before[0], N13_STATE_COLUMNS)
                self.assertIsNotNone(before[1])
                self.assertEqual(before[2], 1)
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    analysis,
                )
                self.assertEqual(
                    rejected.reason,
                    "N13_STATE_INCONSISTENT",
                )
                with recorder._connect() as connection:
                    after = n13_full_row_snapshot(
                        connection,
                        "N13",
                        "S011USDT",
                        t_time,
                    )
                self.assertEqual(after, before)

    def test_runtime_t_stage_evidence_is_strict_before_database_access(self):
        stage_analysis = analyze(*n13_t_stage_reason_switch_windows()[1])
        stage_event = stage_analysis.stage_events[0]
        self.assertEqual(stage_event.reason, "N13_VALUE_BAND_BROKEN")
        raw, checked = n13_klines()
        current = analyze(raw, checked)
        self.assertTrue(current.passed, current.reason)

        class AlwaysEqualString(str):
            def __eq__(self, other):
                return True

            def __ne__(self, other):
                return False

        def forged_event(mode):
            detail = deepcopy(stage_event.detail)
            event = stage_event
            if mode == "t_ohlcv":
                detail["t"]["close"] = "99.511"
                resign_n13_t_stage_detail(detail)
            elif mode == "anchor":
                event = replace(
                    event,
                    t_time=str(int(event.t_time) + INTERVAL_MS),
                )
            elif mode == "status":
                event = replace(event, status="MISSED")
            elif mode == "reason":
                event = replace(event, reason="N13_FAKE_EVENT")
            elif mode == "failures_type":
                detail["failed_confirmation_reasons"] = "invalid"
                resign_n13_t_stage_detail(detail)
            elif mode == "failures_content":
                detail["failed_confirmation_reasons"] = ["N13_FAKE"]
                resign_n13_t_stage_detail(detail)
            elif mode == "a_invalid":
                detail["a"]["low"] = detail["zone_upper"]
                resign_n13_t_stage_detail(detail)
            elif mode == "vwap_a":
                detail["vwap_a"] = "0"
                resign_n13_t_stage_detail(detail)
            elif mode == "atr_a":
                detail["atr_a"] = "0"
                resign_n13_t_stage_detail(detail)
            elif mode == "a_hash_integrity":
                detail["a"]["close"] = "101.9"
            elif mode == "zone_hash_integrity":
                detail["zone_lower"] = str(
                    Decimal(detail["zone_lower"]) + Decimal("0.001")
                )
            elif mode == "expiry":
                detail["armed_expiry_time_ms"] += INTERVAL_MS
                resign_n13_t_stage_detail(detail)
            elif mode == "inverted_zone":
                detail["zone_lower"] = "101"
                detail["zone_upper"] = "100"
                resign_n13_t_stage_detail(detail)
            elif mode == "outcome":
                detail["confirmation_checks"][-1]["outcome"] = (
                    "N13_CONFIRMATION_CLOSE_BELOW_VWAP"
                )
                resign_n13_t_stage_detail(detail)
            elif mode == "vwap":
                detail["confirmation_checks"][-1]["vwap"] = "-1"
                resign_n13_t_stage_detail(detail)
            elif mode == "pre_touch_gap":
                detail["pre_touch_bars"][0]["open_time_ms"] += INTERVAL_MS
                resign_n13_t_stage_detail(detail)
            elif mode == "metric_context_seed":
                detail["metric_context_bars"][0]["high"] = "100.6"
                resign_n13_t_stage_detail(detail)
            elif mode == "metric_context_gap":
                detail["metric_context_bars"][1][
                    "open_time_ms"
                ] += INTERVAL_MS
                resign_n13_t_stage_detail(detail)
            elif mode == "metric_context_a_fork":
                a_time = detail["a"]["open_time_ms"]
                context_a = next(
                    item
                    for item in detail["metric_context_bars"]
                    if item["open_time_ms"] == a_time
                )
                context_a["high"] = "101.3"
                resign_n13_t_stage_detail(detail)
            elif mode == "metric_context_check_fork":
                detail["metric_context_bars"][-1]["close"] = "99.509"
                resign_n13_t_stage_detail(detail)
            elif mode == "metric_context_truncated_seed":
                detail["metric_context_bars"] = detail[
                    "metric_context_bars"
                ][-N13_STRATEGY.n13_vwap_lookback_bars:]
                resign_n13_t_stage_detail(detail)
            elif mode == "check_vwap_resigned":
                detail["confirmation_checks"][-1]["vwap"] = "1000"
                resign_n13_t_stage_detail(detail)
            elif mode == "weak_schema_v1":
                detail.pop("metric_context_bars")
                detail["schema_version"] = 1
                resign_n13_t_stage_detail(detail)
            elif mode == "armed_config":
                detail["armed_max_bars"] += 1
                resign_n13_t_stage_detail(detail)
            elif mode == "vwap_config":
                detail["vwap_lookback_bars"] += 1
                resign_n13_t_stage_detail(detail)
            elif mode == "atr_config":
                detail["atr_period"] += 1
                resign_n13_t_stage_detail(detail)
            elif mode == "upper_config":
                detail["upper_atr_fraction"] = "0.26"
                resign_n13_t_stage_detail(detail)
            elif mode == "lower_config":
                detail["lower_atr_fraction"] = "0.51"
                resign_n13_t_stage_detail(detail)
            elif mode == "confirmation_config":
                detail["confirmation_max_bars"] = 3
                resign_n13_t_stage_detail(detail)
            elif mode == "location_config":
                detail["confirmation_close_location_min"] = "0.61"
                resign_n13_t_stage_detail(detail)
            elif mode == "taker_config":
                detail["confirmation_taker_buy_ratio_min"] = "0.51"
                resign_n13_t_stage_detail(detail)
            elif mode.startswith("str_subclass_"):
                field = {
                    "str_subclass_upper": "upper_atr_fraction",
                    "str_subclass_lower": "lower_atr_fraction",
                    "str_subclass_location": (
                        "confirmation_close_location_min"
                    ),
                    "str_subclass_taker": (
                        "confirmation_taker_buy_ratio_min"
                    ),
                }[mode]
                detail[field] = AlwaysEqualString("evil")
                resign_n13_t_stage_detail(detail)
            elif mode == "hash":
                detail["canonical_sha256"] = "0" * 64
            else:
                raise AssertionError(mode)
            return replace(event, detail=detail)

        for mode in (
            "t_ohlcv",
            "anchor",
            "status",
            "reason",
            "failures_type",
            "failures_content",
            "a_invalid",
            "vwap_a",
            "atr_a",
            "a_hash_integrity",
            "zone_hash_integrity",
            "expiry",
            "inverted_zone",
            "outcome",
            "vwap",
            "pre_touch_gap",
            "metric_context_seed",
            "metric_context_gap",
            "metric_context_a_fork",
            "metric_context_check_fork",
            "metric_context_truncated_seed",
            "check_vwap_resigned",
            "weak_schema_v1",
            "armed_config",
            "vwap_config",
            "atr_config",
            "upper_config",
            "lower_config",
            "confirmation_config",
            "location_config",
            "taker_config",
            "str_subclass_upper",
            "str_subclass_lower",
            "str_subclass_location",
            "str_subclass_taker",
            "hash",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                hostile = replace(
                    current,
                    stage_events=(forged_event(mode),),
                )
                with patch.object(
                    recorder,
                    "get_n13_rotation_state",
                    side_effect=AssertionError("database read attempted"),
                ):
                    rejected = scheduler._apply_n13_state(
                        N13_STRATEGY,
                        candidate("S011USDT", 11),
                        hostile,
                    )
                self.assertEqual(
                    rejected.reason,
                    "N13_STATE_INCONSISTENT",
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT * FROM n13_rotation_states"
                        ).fetchall(),
                        [],
                    )

    def test_same_batch_t_stage_collision_cannot_disappear_into_legacy_bridge(self):
        stage_analyses = [
            analyze(rows, checked)
            for rows, checked in n13_t_stage_reason_switch_windows()[:2]
        ]
        raw, checked = n13_klines()
        current = analyze(raw, checked)
        hostile = replace(
            current,
            stage_events=(
                stage_analyses[0].stage_events[0],
                stage_analyses[1].stage_events[0],
            ),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            with patch.object(
                recorder,
                "get_n13_rotation_state",
                side_effect=AssertionError("database read attempted"),
            ):
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    hostile,
                )
            self.assertEqual(rejected.reason, "N13_STATE_INCONSISTENT")
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM n13_rotation_states"
                    ).fetchall(),
                    [],
                )

    def test_legacy_t_stage_bridge_rejects_corrupt_existing_identity(self):
        analyses = [
            analyze(rows, checked)
            for rows, checked in n13_t_stage_reason_switch_windows()[:2]
        ]
        legacy_event = analyses[0].stage_events[0]
        replay = analyses[1]
        base_detail = {
            "event": legacy_event.reason,
            "structure": None,
            "detail": {
                "t": legacy_n13_state_detail(legacy_event.detail["t"]),
                "failed_confirmation_reasons": list(
                    legacy_event.detail["failed_confirmation_reasons"]
                ),
            },
        }
        for mode in (
            "status",
            "structure_id",
            "reason",
            "event",
            "failed_type",
            "failed_content",
            "failed_count",
            "t_ohlcv",
            "anchor",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                detail = deepcopy(base_detail)
                status = "INVALID"
                structure_id = None
                reason = legacy_event.reason
                if mode == "status":
                    status = "MISSED"
                elif mode == "structure_id":
                    structure_id = "forged"
                elif mode == "reason":
                    reason = "N13_FAKE_EVENT"
                elif mode == "event":
                    detail["event"] = "N13_VALUE_BAND_BROKEN"
                elif mode == "failed_type":
                    detail["detail"]["failed_confirmation_reasons"] = (
                        "invalid"
                    )
                elif mode == "failed_content":
                    detail["detail"]["failed_confirmation_reasons"] = [
                        "N13_FAKE"
                    ] * 4
                elif mode == "failed_count":
                    detail["detail"]["failed_confirmation_reasons"] = []
                elif mode == "t_ohlcv":
                    detail["detail"]["t"]["high"] = "0"
                elif mode == "anchor":
                    detail["detail"]["t"]["open_time_ms"] += INTERVAL_MS
                else:
                    raise AssertionError(mode)
                self.assertEqual(
                    recorder.record_n13_rotation_states_atomically(
                        [
                            {
                                "strategy_id": "N13",
                                "symbol": "S011USDT",
                                "t_time": legacy_event.t_time,
                                "structure_id": structure_id,
                                "status": status,
                                "reason": reason,
                                "detail": detail,
                            }
                        ]
                    ),
                    "OK",
                )
                with recorder._connect() as connection:
                    before = n13_full_row_snapshot(
                        connection,
                        "N13",
                        "S011USDT",
                        legacy_event.t_time,
                    )
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    replay,
                )
                self.assertEqual(
                    rejected.reason,
                    "N13_STATE_INCONSISTENT",
                )
                with recorder._connect() as connection:
                    after = n13_full_row_snapshot(
                        connection,
                        "N13",
                        "S011USDT",
                        legacy_event.t_time,
                    )
                self.assertEqual(after, before)

    def test_runtime_legacy_t_stage_shape_is_never_bridge_eligible(self):
        stage_analysis = analyze(*n13_t_stage_reason_switch_windows()[1])
        event = stage_analysis.stage_events[0]
        legacy_runtime = replace(
            event,
            detail={
                "t": deepcopy(event.detail["t"]),
                "failed_confirmation_reasons": list(
                    event.detail["failed_confirmation_reasons"]
                ),
            },
        )
        raw, checked = n13_klines()
        current = analyze(raw, checked)
        hostile = replace(current, stage_events=(legacy_runtime,))
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            with patch.object(
                recorder,
                "get_n13_rotation_state",
                side_effect=AssertionError("database read attempted"),
            ):
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    hostile,
                )
            self.assertEqual(rejected.reason, "N13_STATE_INCONSISTENT")
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM n13_rotation_states"
                    ).fetchall(),
                    [],
                )

    def test_new_t_stage_same_reconstruction_tamper_is_not_a_replay(self):
        analysis = analyze(*n13_t_stage_reason_switch_windows()[0])
        event = analysis.stage_events[0]
        for mode in ("a_ohlcv", "zone_single", "t_ohlcv"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                self.assertIsNone(
                    scheduler._apply_n13_state(
                        N13_STRATEGY,
                        candidate("S011USDT", 11),
                        analysis,
                    )
                )
                with recorder._connect() as connection:
                    columns, persisted, count = n13_full_row_snapshot(
                        connection,
                        "N13",
                        "S011USDT",
                        event.t_time,
                    )
                    detail = json.loads(persisted[7])
                    evidence = detail["detail"]
                    if mode == "a_ohlcv":
                        evidence["a"]["close"] = "101.1"
                    elif mode == "zone_single":
                        evidence["zone_lower"] = str(
                            Decimal(evidence["zone_lower"])
                            + Decimal("0.001")
                        )
                    elif mode == "t_ohlcv":
                        evidence["t"]["close"] = "99.511"
                        evidence["confirmation_checks"][0]["candle"][
                            "close"
                        ] = "99.511"
                    else:
                        raise AssertionError(mode)
                    resign_n13_t_stage_detail(evidence)
                    connection.execute(
                        "UPDATE n13_rotation_states SET detail_json=? "
                        "WHERE id=?",
                        (json.dumps(detail, sort_keys=True), persisted[0]),
                    )
                    corrupted_before = n13_full_row_snapshot(
                        connection,
                        "N13",
                        "S011USDT",
                        event.t_time,
                    )
                self.assertEqual(columns, N13_STATE_COLUMNS)
                self.assertEqual(count, 1)
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    analysis,
                )
                self.assertEqual(
                    rejected.reason,
                    "N13_STATE_INCONSISTENT",
                )
                with recorder._connect() as connection:
                    corrupted_after = n13_full_row_snapshot(
                        connection,
                        "N13",
                        "S011USDT",
                        event.t_time,
                    )
                self.assertEqual(corrupted_after, corrupted_before)

    def test_different_a_bridge_rejects_single_zone_field_tamper(self):
        analyses = [
            analyze(rows, checked)
            for rows, checked in n13_t_stage_reason_switch_windows()[:2]
        ]
        self.assertNotEqual(
            analyses[0].stage_events[0].detail["a"]["open_time_ms"],
            analyses[1].stage_events[0].detail["a"]["open_time_ms"],
        )
        for field, delta in (
            ("zone_lower", Decimal("0.001")),
            ("zone_upper", Decimal("-0.001")),
        ):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                self.assertIsNone(
                    scheduler._apply_n13_state(
                        N13_STRATEGY,
                        candidate("S011USDT", 11),
                        analyses[0],
                    )
                )
                event = analyses[0].stage_events[0]
                with recorder._connect() as connection:
                    _, persisted, _ = n13_full_row_snapshot(
                        connection,
                        "N13",
                        "S011USDT",
                        event.t_time,
                    )
                    detail = json.loads(persisted[7])
                    detail["detail"][field] = str(
                        Decimal(detail["detail"][field]) + delta
                    )
                    resign_n13_t_stage_detail(detail["detail"])
                    connection.execute(
                        "UPDATE n13_rotation_states SET detail_json=? "
                        "WHERE id=?",
                        (json.dumps(detail, sort_keys=True), persisted[0]),
                    )
                    corrupted_before = n13_full_row_snapshot(
                        connection,
                        "N13",
                        "S011USDT",
                        event.t_time,
                    )
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    analyses[1],
                )
                self.assertEqual(
                    rejected.reason,
                    "N13_STATE_INCONSISTENT",
                )
                with recorder._connect() as connection:
                    corrupted_after = n13_full_row_snapshot(
                        connection,
                        "N13",
                        "S011USDT",
                        event.t_time,
                    )
                self.assertEqual(corrupted_after, corrupted_before)

    def test_different_a_bridge_rejects_resigned_derived_metric_forgery(self):
        analyses = [
            analyze(rows, checked)
            for rows, checked in n13_t_stage_reason_switch_windows()[:2]
        ]
        first_event = analyses[0].stage_events[0]
        second_event = analyses[1].stage_events[0]
        self.assertEqual(first_event.reason, "N13_CONFIRMATION_NOT_FOUND")
        self.assertEqual(second_event.reason, "N13_VALUE_BAND_BROKEN")
        self.assertNotEqual(
            first_event.detail["a"]["open_time_ms"],
            second_event.detail["a"]["open_time_ms"],
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    analyses[0],
                )
            )
            with recorder._connect() as connection:
                columns, persisted, count = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    first_event.t_time,
                )
                payload = json.loads(persisted[7])
                evidence = payload["detail"]
                evidence["vwap_a"] = "100.0"
                evidence["atr_a"] = "1.0"
                evidence["zone_lower"] = "99.50"
                evidence["zone_upper"] = "100.250"
                resign_n13_t_stage_detail(evidence)
                connection.execute(
                    "UPDATE n13_rotation_states SET detail_json=? "
                    "WHERE id=?",
                    (json.dumps(payload, sort_keys=True), persisted[0]),
                )
                forged_before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    first_event.t_time,
                )
            self.assertEqual(columns, N13_STATE_COLUMNS)
            self.assertEqual(count, 1)
            rejected = scheduler._apply_n13_state(
                N13_STRATEGY,
                candidate("S011USDT", 11),
                analyses[1],
            )
            self.assertEqual(rejected.reason, "N13_STATE_INCONSISTENT")
            with recorder._connect() as connection:
                forged_after = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    first_event.t_time,
                )
            self.assertEqual(forged_after, forged_before)

    def test_different_a_bridge_rejects_changed_overlapping_metric_candle(self):
        windows = n13_t_stage_reason_switch_windows()
        first_rows = deepcopy(windows[0][0])
        changed_time = BASE_TIME_MS - 10 * INTERVAL_MS
        changed_index = next(
            index
            for index, row in enumerate(first_rows)
            if row[0] == changed_time
        )
        first_rows[changed_index] = kline(
            -10,
            "100",
            "100.5",
            "99.5",
            "100.01",
        )
        first = analyze(first_rows, windows[0][1])
        second = analyze(*windows[1])
        first_event = first.stage_events[0]
        second_event = second.stage_events[0]
        self.assertEqual(first_event.reason, "N13_CONFIRMATION_NOT_FOUND")
        self.assertEqual(second_event.reason, "N13_VALUE_BAND_BROKEN")
        self.assertEqual(first_event.t_time, second_event.t_time)
        first_overlap = next(
            item
            for item in first_event.detail["metric_context_bars"]
            if item["open_time_ms"] == changed_time
        )
        second_overlap = next(
            item
            for item in second_event.detail["metric_context_bars"]
            if item["open_time_ms"] == changed_time
        )
        self.assertNotEqual(
            legacy_n13_state_detail(first_overlap),
            legacy_n13_state_detail(second_overlap),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    first,
                )
            )
            with recorder._connect() as connection:
                before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    first_event.t_time,
                )
            rejected = scheduler._apply_n13_state(
                N13_STRATEGY,
                candidate("S011USDT", 11),
                second,
            )
            self.assertEqual(rejected.reason, "N13_STATE_INCONSISTENT")
            with recorder._connect() as connection:
                after = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    first_event.t_time,
                )
            self.assertEqual(after, before)

    def test_resigned_confirmation_vwap_outcomes_cannot_forge_event(self):
        analysis = analyze(*n13_t_stage_reason_switch_windows()[0])
        event = analysis.stage_events[0]
        self.assertEqual(event.reason, "N13_CONFIRMATION_NOT_FOUND")
        detail = deepcopy(event.detail)
        forged_outcome = "N13_CONFIRMATION_CLOSE_LOCATION_TOO_LOW"
        for check in detail["confirmation_checks"]:
            check["vwap"] = "1"
            check["outcome"] = forged_outcome
        detail["failed_confirmation_reasons"] = [
            forged_outcome
        ] * len(detail["confirmation_checks"])
        resign_n13_t_stage_detail(detail)
        hostile = replace(
            analysis,
            stage_events=(replace(event, detail=detail),),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            with patch.object(
                recorder,
                "get_n13_rotation_state",
                side_effect=AssertionError("database read attempted"),
            ):
                rejected = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    hostile,
                )
            self.assertEqual(rejected.reason, "N13_STATE_INCONSISTENT")
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM n13_rotation_states"
                    ).fetchall(),
                    [],
                )

    def test_n13_existing_guard_blocks_new_record_atomically(self):
        analysis = analyze(*n13_t_stage_reason_switch_windows()[0])
        event = analysis.stage_events[0]
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    analysis,
                )
            )
            guard = tuple(
                recorder.get_n13_rotation_state(
                    "N13",
                    "S011USDT",
                    event.t_time,
                )
            )
            bad_guard = (*guard[:-1], "forged-updated-at")
            with recorder._connect() as connection:
                before = connection.execute(
                    "SELECT * FROM n13_rotation_states ORDER BY id"
                ).fetchall()
            result = recorder.record_n13_rotation_states_atomically(
                [
                    {
                        "strategy_id": "N13",
                        "symbol": "S011USDT",
                        "t_time": str(int(event.t_time) + 10 * INTERVAL_MS),
                        "structure_id": None,
                        "status": "INVALID",
                        "reason": "N13_ARMED_WINDOW_EXPIRED",
                        "detail": {"guard_test": True},
                    }
                ],
                existing_guards=[bad_guard],
            )
            self.assertEqual(result, "N13_STATE_INCONSISTENT")
            with recorder._connect() as connection:
                after = connection.execute(
                    "SELECT * FROM n13_rotation_states ORDER BY id"
                ).fetchall()
            self.assertEqual(after, before)

    def test_n13_existing_guard_rejects_type_coercion_atomically(self):
        analysis = analyze(*n13_t_stage_reason_switch_windows()[0])
        event = analysis.stage_events[0]

        class GuardString(str):
            pass

        for mode in (
            "bool_id",
            "float_id",
            "string_subclass_symbol",
            "string_subclass_detail",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                self.assertIsNone(
                    scheduler._apply_n13_state(
                        N13_STRATEGY,
                        candidate("S011USDT", 11),
                        analysis,
                    )
                )
                guard = tuple(
                    recorder.get_n13_rotation_state(
                        "N13",
                        "S011USDT",
                        event.t_time,
                    )
                )
                forged = list(guard)
                if mode == "bool_id":
                    forged[0] = True
                elif mode == "float_id":
                    forged[0] = float(guard[0])
                elif mode == "string_subclass_symbol":
                    forged[2] = GuardString(guard[2])
                elif mode == "string_subclass_detail":
                    forged[7] = GuardString(guard[7])
                else:
                    raise AssertionError(mode)
                with recorder._connect() as connection:
                    before = connection.execute(
                        "SELECT * FROM n13_rotation_states ORDER BY id"
                    ).fetchall()
                with patch.object(
                    recorder,
                    "_connect",
                    side_effect=AssertionError(
                        "invalid guard reached database connection"
                    ),
                ):
                    result = recorder.record_n13_rotation_states_atomically(
                        [
                            {
                                "strategy_id": "N13",
                                "symbol": "S011USDT",
                                "t_time": str(
                                    int(event.t_time) + 10 * INTERVAL_MS
                                ),
                                "structure_id": None,
                                "status": "INVALID",
                                "reason": "N13_ARMED_WINDOW_EXPIRED",
                                "detail": {"guard_type_test": mode},
                            }
                        ],
                        existing_guards=[tuple(forged)],
                    )
                self.assertEqual(result, "N13_STATE_INCONSISTENT")
                with recorder._connect() as connection:
                    after = connection.execute(
                        "SELECT * FROM n13_rotation_states ORDER BY id"
                    ).fetchall()
                self.assertEqual(after, before)

    def test_legacy_t_stage_guard_rolls_back_failed_current_insert(self):
        stage_analyses = [
            analyze(rows, checked)
            for rows, checked in n13_t_stage_reason_switch_windows()[:2]
        ]
        legacy_event = stage_analyses[0].stage_events[0]
        replay_event = stage_analyses[1].stage_events[0]
        raw, checked = n13_klines()
        current = analyze(raw, checked)
        combined = replace(current, stage_events=(replay_event,))
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            legacy_detail = {
                "event": legacy_event.reason,
                "structure": None,
                "detail": {
                    "t": legacy_n13_state_detail(legacy_event.detail["t"]),
                    "failed_confirmation_reasons": list(
                        legacy_event.detail["failed_confirmation_reasons"]
                    ),
                },
            }
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically(
                    [
                        {
                            "strategy_id": "N13",
                            "symbol": "S011USDT",
                            "t_time": legacy_event.t_time,
                            "structure_id": None,
                            "status": "INVALID",
                            "reason": legacy_event.reason,
                            "detail": legacy_detail,
                        }
                    ]
                ),
                "OK",
            )
            with recorder._connect() as connection:
                before = connection.execute(
                    "SELECT * FROM n13_rotation_states ORDER BY id"
                ).fetchall()
                connection.execute(
                    "CREATE TRIGGER fail_n13_current_insert "
                    "BEFORE INSERT ON n13_rotation_states "
                    "WHEN NEW.t_time != '"
                    + legacy_event.t_time
                    + "' BEGIN SELECT RAISE(FAIL, 'forced current failure'); "
                    "END"
                )
                commit_test_schema_change(recorder, connection)
            rejected = scheduler._apply_n13_state(
                N13_STRATEGY,
                candidate("S011USDT", 11),
                combined,
            )
            self.assertEqual(rejected.reason, "N13_STATE_PERSIST_FAILED")
            with recorder._connect() as connection:
                after = connection.execute(
                    "SELECT * FROM n13_rotation_states ORDER BY id"
                ).fetchall()
            self.assertEqual(after, before)

    def test_legacy_null_structure_stage_survives_fixed_window_slide(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            rows, _ = flat_n13_klines()
            rows[95] = kline(95, "101.2", "102.2", "101", "102")
            for index in range(96, 105):
                rows[index] = kline(
                    index, "101.2", "101.6", "101", "101.3"
                )
            rows = [
                kline(index, "100", "100.5", "99.5", "100")
                for index in range(-16, 0)
            ] + rows
            rows[0] = kline(-16, "100", "100.6", "99.4", "100")
            rows.append(kline(105, "101.3", "101.7", "101.1", "101.4"))
            self.assertEqual(len(rows), 122)
            first = analyze(
                rows,
                BASE_TIME_MS + 105 * INTERVAL_MS + 30_000,
            )
            event = first.stage_events[-1]
            self.assertEqual(event.reason, "N13_ARMED_WINDOW_EXPIRED")
            self.assertIsNone(event.structure)
            legacy_detail = legacy_n13_state_detail(
                {
                    "event": event.reason,
                    "structure": None,
                    "detail": event.detail,
                }
            )
            self.assertIn("zone_lower", legacy_detail["detail"])
            self.assertEqual(
                recorder.record_n13_rotation_states_atomically(
                    [
                        {
                            "strategy_id": "N13",
                            "symbol": "S011USDT",
                            "t_time": event.t_time,
                            "structure_id": None,
                            "status": event.status,
                            "reason": event.reason,
                            "detail": legacy_detail,
                        }
                    ]
                ),
                "OK",
            )
            with recorder._connect() as connection:
                columns, persisted_before, count_before = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    event.t_time,
                )
            self.assertEqual(columns, N13_STATE_COLUMNS)
            self.assertEqual(count_before, 1)

            slid = deepcopy(rows[1:])
            slid.append(kline(106, "101.4", "101.8", "101.2", "101.5"))
            second = analyze(
                slid,
                BASE_TIME_MS + 106 * INTERVAL_MS + 30_000,
            )
            replay = next(
                item
                for item in second.stage_events
                if item.t_time == event.t_time
            )
            self.assertEqual(replay.reason, event.reason)
            self.assertNotEqual(
                replay.detail["zone_lower"],
                event.detail["zone_lower"],
            )
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    second,
                )
            )
            with recorder._connect() as connection:
                _, persisted_after, count_after = n13_full_row_snapshot(
                    connection,
                    "N13",
                    "S011USDT",
                    event.t_time,
                )
            self.assertEqual(persisted_after, persisted_before)
            self.assertEqual(count_after, count_before)

            for mode in (
                "a_only",
                "expiry_only",
                "invalid_a",
                "inverted_zone",
                "negative_zone_upper",
                "a_not_armed",
            ):
                with self.subTest(stage_tamper=mode):
                    corrupted = json.loads(persisted_after[7])
                    if mode == "a_only":
                        a = corrupted["detail"]["a"]
                        a["close"] = (
                            a["high"]
                            if a["close"] != a["high"]
                            else a["open"]
                        )
                    elif mode == "expiry_only":
                        corrupted["detail"][
                            "armed_expiry_time_ms"
                        ] += INTERVAL_MS
                    elif mode == "invalid_a":
                        corrupted["detail"]["a"]["close"] = "0"
                    elif mode == "inverted_zone":
                        corrupted["detail"]["zone_lower"] = "101"
                        corrupted["detail"]["zone_upper"] = "100"
                    elif mode == "negative_zone_upper":
                        corrupted["detail"]["zone_lower"] = "-2"
                        corrupted["detail"]["zone_upper"] = "-1"
                    elif mode == "a_not_armed":
                        corrupted["detail"]["zone_upper"] = corrupted[
                            "detail"
                        ]["a"]["low"]
                    else:
                        raise AssertionError(mode)
                    with recorder._connect() as connection:
                        connection.execute(
                            "UPDATE n13_rotation_states SET detail_json=? "
                            "WHERE id=?",
                            (
                                json.dumps(corrupted, sort_keys=True),
                                persisted_after[0],
                            ),
                        )
                        corrupted_before = n13_full_row_snapshot(
                            connection,
                            "N13",
                            "S011USDT",
                            event.t_time,
                        )
                    rejected = scheduler._apply_n13_state(
                        N13_STRATEGY,
                        candidate("S011USDT", 11),
                        second,
                    )
                    self.assertEqual(
                        rejected.reason,
                        "N13_STATE_INCONSISTENT",
                    )
                    with recorder._connect() as connection:
                        corrupted_after = n13_full_row_snapshot(
                            connection,
                            "N13",
                            "S011USDT",
                            event.t_time,
                        )
                    self.assertEqual(corrupted_after, corrupted_before)

    def test_same_batch_stage_replay_deduplicates_window_relative_detail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            rows, checked = flat_n13_klines()
            rows[96] = kline(96, "101.2", "102.2", "101", "102")
            rows = [
                kline(index, "100", "100.5", "99.5", "100")
                for index in range(-17, 0)
            ] + rows
            rows[0] = kline(-17, "100", "100.6", "99.4", "100")
            a_index = 17 + 96
            metrics = n13_metrics(parse_n13_klines(rows[:-1]))
            lower = metrics[a_index].lower
            rows[a_index + 1] = kline(
                97,
                lower - Decimal("0.10"),
                lower - Decimal("0.01"),
                lower - Decimal("0.20"),
                lower - Decimal("0.10"),
            )
            first = analyze(rows, checked)
            first_event = first.stage_events[-1]
            self.assertEqual(first_event.reason, "N13_VALUE_ZONE_SKIPPED")

            slid = deepcopy(rows[1:])
            slid.append(kline(105, "100", "100.5", "99.5", "100"))
            second = analyze(
                slid,
                BASE_TIME_MS + 105 * INTERVAL_MS + 30_000,
            )
            second_event = next(
                item
                for item in second.stage_events
                if item.t_time == first_event.t_time
            )
            self.assertNotEqual(
                first_event.detail["zone_lower"],
                second_event.detail["zone_lower"],
            )
            same_batch = replace(
                second,
                stage_events=(first_event, second_event),
            )
            self.assertIsNone(
                scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    same_batch,
                )
            )
            with recorder._connect() as connection:
                rows = connection.execute(
                    "SELECT detail_json FROM n13_rotation_states"
                ).fetchall()
            self.assertEqual(len(rows), 1)
            stored = json.loads(rows[0][0])
            self.assertEqual(
                stored["detail"]["zone_lower"],
                first_event.detail["zone_lower"],
            )
            self.assertEqual(
                stored["detail"]["zone_upper"],
                first_event.detail["zone_upper"],
            )

    def test_hostile_fake_events_cannot_enter_stable_batch_deduplication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            raw, checked = n13_klines()
            analysis = analyze(raw, checked)
            self.assertTrue(analysis.passed, analysis.reason)
            fake_time = str(raw[0][0])
            fake_one = N13Event(
                fake_time,
                "INVALID",
                "N13_FAKE_EVENT",
                detail={
                    "zone_lower": "1",
                    "zone_upper": "2",
                },
            )
            fake_two = N13Event(
                fake_time,
                "INVALID",
                "N13_FAKE_EVENT",
                detail={
                    "zone_lower": "3",
                    "zone_upper": "4",
                },
            )
            hostile = replace(
                analysis,
                stage_events=(fake_one, fake_two),
            )
            decision = scheduler._apply_n13_state(
                N13_STRATEGY,
                candidate("S011USDT", 11),
                hostile,
            )
            self.assertEqual(decision.reason, "N13_STATE_INCONSISTENT")
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n13_rotation_states"
                    ).fetchone()[0],
                    0,
                )

    def test_legacy_event_identity_fields_require_exact_builtin_strings(self):
        raw, checked = n13_klines()
        current = analyze(raw, checked)
        self.assertTrue(current.passed, current.reason)

        armed_raw = deepcopy(raw)
        armed_raw[95] = kline(95, "101.2", "102.2", "101", "102")
        armed_raw[96] = kline(96, "100.9", "101.3", "100.7", "101")
        armed = analyze(
            armed_raw,
            checked,
            armed_max_bars=7,
        ).stage_events[-1]

        class EvilStatus(str):
            def __new__(cls):
                return super().__new__(cls, "EVIL")

            def __eq__(self, other):
                return other in {"EVIL", "INVALID"}

            __hash__ = str.__hash__

        class StringSubclass(str):
            pass

        forged_events = (
            replace(armed, status=EvilStatus()),
            replace(armed, reason=StringSubclass(armed.reason)),
        )
        for forged in forged_events:
            with self.subTest(
                field="status" if forged.status == "EVIL" else "reason"
            ), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                hostile = replace(current, stage_events=(forged,))
                decision = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    hostile,
                )
                self.assertEqual(
                    decision.reason,
                    "N13_STATE_INCONSISTENT",
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT * FROM n13_rotation_states"
                        ).fetchall(),
                        [],
                    )

    def test_passed_analysis_contract_is_closed_before_state_access(self):
        raw, checked = n13_klines()
        current = analyze(raw, checked)
        self.assertTrue(current.passed, current.reason)
        self.assertIsNotNone(current.structure)
        self.assertIsNotNone(current.structure.entry)

        class FakeDecimal:
            def __str__(self):
                return "100"

        hostile_analyses = (
            ("consume_false", replace(current, consume_current=False)),
            ("missing_structure", replace(current, structure=None)),
            (
                "missing_entry",
                replace(
                    current,
                    structure=replace(current.structure, entry=None),
                ),
            ),
            (
                "invalid_entry_type",
                replace(
                    current,
                    structure=replace(current.structure, entry=object()),
                ),
            ),
            (
                "invalid_entry_time",
                replace(
                    current,
                    structure=replace(
                        current.structure,
                        entry=replace(
                            current.structure.entry,
                            open_time_ms=(
                                current.structure.entry.open_time_ms
                                + INTERVAL_MS
                            ),
                        ),
                    ),
                ),
            ),
            (
                "invalid_p_type",
                replace(
                    current,
                    structure=replace(current.structure, p=100),
                ),
            ),
            (
                "invalid_entry_bound_type",
                replace(
                    current,
                    structure=replace(
                        current.structure,
                        entry_min_price=FakeDecimal(),
                    ),
                ),
            ),
            (
                "missed_terminal_consume_false",
                replace(
                    current,
                    passed=False,
                    reason="N13_ENTRY_WINDOW_EXPIRED",
                    consume_current=False,
                ),
            ),
            (
                "consume_with_invalid_reason",
                replace(
                    current,
                    passed=False,
                    reason="N13_WAITING_ENTRY_PRICE",
                    consume_current=True,
                ),
            ),
            (
                "consume_with_missing_entry",
                replace(
                    current,
                    passed=False,
                    reason="N13_ENTRY_WINDOW_EXPIRED",
                    consume_current=True,
                    structure=replace(current.structure, entry=None),
                ),
            ),
            (
                "entry_without_consume_or_waiting",
                replace(
                    current,
                    passed=False,
                    reason="N13_NOT_ARMED",
                    consume_current=False,
                ),
            ),
            (
                "passed_reason_mismatch",
                replace(current, reason="N13_NOT_ARMED"),
            ),
            (
                "nonpassed_uses_passed_reason",
                replace(current, passed=False, reason="PASSED"),
            ),
            (
                "event_container_not_tuple",
                replace(current, stage_events=[]),
            ),
        )
        for name, hostile in hostile_analyses:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                decision = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    hostile,
                )
                self.assertEqual(
                    decision.reason,
                    "N13_STATE_INCONSISTENT",
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT * FROM n13_rotation_states"
                        ).fetchall(),
                        [],
                    )

    def test_waiting_entry_price_is_transient_and_repeatable(self):
        raw, checked = n13_klines()
        raw[-1] = kline(104, "101.3", "101.4", "100.2", "101.3")
        waiting = analyze(raw, checked)
        self.assertFalse(waiting.passed)
        self.assertEqual(waiting.reason, "N13_WAITING_ENTRY_PRICE")
        self.assertFalse(waiting.consume_current)
        self.assertIsNotNone(waiting.structure)
        self.assertIsNotNone(waiting.structure.entry)

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            for _ in range(2):
                self.assertIsNone(
                    scheduler._apply_n13_state(
                        N13_STRATEGY,
                        candidate("S011USDT", 11),
                        waiting,
                    )
                )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM n13_rotation_states"
                    ).fetchall(),
                    [],
                )

    def test_all_current_terminal_pairs_require_and_consume_exact_episode(self):
        raw, checked = n13_klines()
        passed = analyze(raw, checked)
        self.assertTrue(passed.passed, passed.reason)
        missed_reasons = (
            "N13_MARKET_BREADTH_NOT_MET",
            "N13_RETURN_NOT_POSITIVE",
            "N13_RETURN_RANK_OUT_OF_RANGE",
            "N13_VWAP_NOT_RISING",
            "N13_ENTRY_WINDOW_EXPIRED",
            "N13_ENTRY_LOW_BROKE_P",
            "N13_ENTRY_PRICE_TOO_EXTENDED",
        )
        analyses = (("CONSUMED", passed),) + tuple(
            (
                "MISSED",
                replace(
                    passed,
                    passed=False,
                    reason=reason,
                    consume_current=True,
                ),
            )
            for reason in missed_reasons
        )
        for expected_status, analysis in analyses:
            with self.subTest(
                status=expected_status,
                reason=analysis.reason,
            ), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                self.assertIsNone(
                    scheduler._apply_n13_state(
                        N13_STRATEGY,
                        candidate("S011USDT", 11),
                        analysis,
                    )
                )
                replay = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    analysis,
                )
                self.assertEqual(replay.reason, "N13_EPISODE_CONSUMED")
                with recorder._connect() as connection:
                    rows = connection.execute(
                        "SELECT status, reason FROM n13_rotation_states"
                    ).fetchall()
                self.assertEqual(
                    rows,
                    [(expected_status, analysis.reason)],
                )

    def test_historical_event_nested_structure_is_validated_before_state_access(self):
        raw, checked = n13_klines()
        current = analyze(raw, checked)
        self.assertTrue(current.passed, current.reason)
        historical_raw = deepcopy(raw)
        historical_raw.append(
            kline(105, "101.5", "101.9", "101.3", "101.6")
        )
        historical = analyze(
            historical_raw,
            BASE_TIME_MS + 105 * INTERVAL_MS + 30_000,
        ).historical_events[-1]
        forged = replace(
            historical,
            structure=replace(historical.structure, a=object()),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            hostile = replace(current, historical_events=(forged,))
            decision = scheduler._apply_n13_state(
                N13_STRATEGY,
                candidate("S011USDT", 11),
                hostile,
            )
            self.assertEqual(
                decision.reason,
                "N13_STATE_INCONSISTENT",
            )
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM n13_rotation_states"
                    ).fetchall(),
                    [],
                )

    def test_event_t_time_must_be_canonical_and_match_reason_anchor(self):
        raw, checked = n13_klines()
        current = analyze(raw, checked)
        self.assertTrue(current.passed, current.reason)

        historical_raw = deepcopy(raw)
        historical_raw.append(
            kline(105, "101.5", "101.9", "101.3", "101.6")
        )
        historical = analyze(
            historical_raw,
            BASE_TIME_MS + 105 * INTERVAL_MS + 30_000,
        ).historical_events[-1]

        armed_raw = deepcopy(raw)
        armed_raw[95] = kline(95, "101.2", "102.2", "101", "102")
        armed_raw[96] = kline(96, "100.9", "101.3", "100.7", "101")
        armed = analyze(
            armed_raw,
            checked,
            armed_max_bars=7,
        ).stage_events[-1]

        broken_raw = deepcopy(raw)
        broken_raw[100] = kline(100, "100", "100.4", "99.2", "99.4")
        for index in range(101, 104):
            broken_raw[index] = kline(
                index, "100", "100.4", "99.5", "100"
            )
        broken = analyze(broken_raw, checked).stage_events[-1]

        anchored_events = (
            ("historical_t", historical, "historical_events"),
            ("armed_a", armed, "stage_events"),
            ("broken_t", broken, "stage_events"),
        )
        for name, event, collection in anchored_events:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                forged = replace(
                    event,
                    t_time=str(int(event.t_time) + INTERVAL_MS),
                )
                hostile = replace(
                    current,
                    historical_events=(forged,)
                    if collection == "historical_events"
                    else (),
                    stage_events=(forged,)
                    if collection == "stage_events"
                    else (),
                )
                decision = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    hostile,
                )
                self.assertEqual(decision.reason, "N13_STATE_INCONSISTENT")
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT * FROM n13_rotation_states"
                        ).fetchall(),
                        [],
                    )

        class TTimeSubclass(str):
            pass

        invalid_times = (
            "0",
            "00",
            f"0{armed.t_time}",
            str(int(armed.t_time) + 1),
            TTimeSubclass(armed.t_time),
        )
        for forged_time in invalid_times:
            with self.subTest(
                t_time=repr(forged_time)
            ), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)
                forged = replace(armed, t_time=forged_time)
                hostile = replace(current, stage_events=(forged,))
                decision = scheduler._apply_n13_state(
                    N13_STRATEGY,
                    candidate("S011USDT", 11),
                    hostile,
                )
                self.assertEqual(decision.reason, "N13_STATE_INCONSISTENT")
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM n13_rotation_states"
                        ).fetchone()[0],
                            0,
                        )

    def test_current_and_historical_structure_identity_is_self_proving(self):
        raw, checked = n13_klines()
        current = analyze(raw, checked)
        self.assertTrue(current.passed, current.reason)
        historical_raw = deepcopy(raw)
        historical_raw.append(
            kline(105, "101.5", "101.9", "101.3", "101.6")
        )
        historical_event = analyze(
            historical_raw,
            BASE_TIME_MS + 105 * INTERVAL_MS + 30_000,
        ).historical_events[-1]

        def forge(structure, mode):
            if mode == "self_consistent_fake_sid":
                return replace(structure, structure_id="f" * 24)
            if mode == "a_equals_t":
                return replace(
                    structure,
                    a=replace(
                        structure.a,
                        open_time_ms=structure.t.open_time_ms,
                    ),
                )
            if mode == "c_before_t":
                forged_c = replace(
                    structure.c,
                    open_time_ms=(
                        structure.t.open_time_ms - INTERVAL_MS
                    ),
                )
                forged_sid = official_n13_structure_id(
                    structure.symbol,
                    structure.t.open_time_ms,
                    forged_c.open_time_ms,
                )
                return replace(
                    structure,
                    c=forged_c,
                    structure_id=forged_sid,
                )
            raise AssertionError(mode)

        for scope in ("current_v1", "legacy_historical"):
            for mode in (
                "self_consistent_fake_sid",
                "a_equals_t",
                "c_before_t",
            ):
                with self.subTest(
                    scope=scope,
                    mode=mode,
                ), tempfile.TemporaryDirectory() as tmpdir:
                    recorder, scheduler = recorder_scheduler(tmpdir)
                    if scope == "current_v1":
                        hostile = replace(
                            current,
                            structure=forge(current.structure, mode),
                        )
                    else:
                        forged_event = replace(
                            historical_event,
                            structure=forge(
                                historical_event.structure,
                                mode,
                            ),
                        )
                        hostile = replace(
                            current,
                            historical_events=(forged_event,),
                        )
                    decision = scheduler._apply_n13_state(
                        N13_STRATEGY,
                        candidate("S011USDT", 11),
                        hostile,
                    )
                    self.assertEqual(
                        decision.reason,
                        "N13_STATE_INCONSISTENT",
                    )
                    with recorder._connect() as connection:
                        self.assertEqual(
                            connection.execute(
                                "SELECT * FROM n13_rotation_states"
                            ).fetchall(),
                            [],
                        )

    def test_signal_audit_records_n13_structure_bullishness_and_detail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            items, raws, checked = full_batch()
            scan_id = recorder.begin_scan(len(items), items, True)
            self.assertIsNotNone(scan_id)
            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": items},
                raws,
                checked_at_ms=checked,
            )
            signal = next(
                item for item in result.signals if item.candidate.symbol == "S011USDT"
            )
            self.assertTrue(signal.passed)
            self.assertTrue(result.signal_batch_published)
            self.assertIsNotNone(signal.analysis.structure)
            self.assertIsNotNone(signal.analysis.structure.entry)
            with recorder._connect() as connection:
                row = connection.execute(
                    """
                    SELECT signal.structure_id, signal.current_bullish,
                           signal.detail_json, audit.detail_json,
                           audit.claim_state
                    FROM strategy_signals AS signal
                    JOIN strategy_passed_signal_audits AS audit
                      ON audit.source_signal_id = signal.id
                    WHERE signal.strategy_id='N13'
                      AND signal.symbol='S011USDT'
                    """
                ).fetchone()
            detail = json.loads(row[2])
            permanent_detail = json.loads(row[3])
            self.assertEqual(row[0], signal.analysis.structure_id)
            self.assertEqual(row[1], 1)
            self.assertEqual(row[4], "ACTIVE")
            self.assertEqual(permanent_detail, detail)
            self.assertEqual(detail["structure_id"], signal.analysis.structure_id)
            self.assertEqual(
                detail["structure"]["entry_min_price"],
                str(signal.analysis.structure.c.close),
            )
            structure = permanent_detail["structure"]
            self.assertEqual(
                structure["entry_low"],
                str(signal.analysis.structure.entry.low),
            )
            self.assertGreaterEqual(
                Decimal(structure["entry_low"]),
                Decimal(structure["p"]),
            )
            self.assertEqual(
                structure["positive_return_breadth"],
                str(signal.analysis.structure.positive_return_breadth),
            )
            self.assertGreaterEqual(
                Decimal(structure["positive_return_breadth"]),
                N13_STRATEGY.n13_positive_breadth_min,
            )
            self.assertEqual(
                structure["above_vwap_breadth"],
                str(signal.analysis.structure.above_vwap_breadth),
            )
            self.assertGreaterEqual(
                Decimal(structure["above_vwap_breadth"]),
                N13_STRATEGY.n13_above_vwap_breadth_min,
            )
            self.assertEqual(
                permanent_detail["current_price"],
                str(signal.analysis.structure.entry.close),
            )
            self.assertEqual(
                permanent_detail["current_price"],
                str(signal.analysis.current_price),
            )
            self.assertEqual(
                structure["vwap_slope_reference"],
                str(signal.analysis.vwap_slope_reference),
            )
            self.assertGreater(
                Decimal(structure["vwap_c"]),
                Decimal(structure["vwap_slope_reference"]),
            )
            encoded = json.dumps(
                permanent_detail,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            self.assertNotIn("metric_context_bars", encoded)
            self.assertLessEqual(len(encoded.encode("utf-8")), 2048)

    def test_scheduler_passes_every_n13_threshold_from_strategy_config(self):
        configured = replace(
            N13_STRATEGY,
            entry_window_seconds=77,
            n13_positive_breadth_min=Decimal("0.61"),
            n13_above_vwap_breadth_min=Decimal("0.51"),
            n13_rank_min=12,
            n13_rank_max=59,
            n13_vwap_lookback_bars=95,
            n13_atr_period=13,
            n13_vwap_slope_lookback_bars=7,
            n13_upper_atr_fraction=Decimal("0.26"),
            n13_lower_atr_fraction=Decimal("0.49"),
            n13_armed_max_bars=6,
            n13_confirmation_max_bars=3,
            n13_confirmation_close_location_min=Decimal("0.62"),
            n13_confirmation_taker_buy_ratio_min=Decimal("0.52"),
            n13_entry_extension_atr_max=Decimal("0.45"),
        )
        raw, checked = n13_klines()
        item = candidate("S012USDT", 12)
        context = N13BatchContext(
            {
                item.symbol: N13MarketRow(
                    Decimal("0.01"),
                    12,
                    12,
                    Decimal("101"),
                    Decimal("100"),
                    Decimal("1"),
                    True,
                )
            },
            Decimal("1"),
            Decimal("1"),
            True,
        )
        empty_result = N13AnalysisResult(
            item.symbol,
            False,
            "N13_NOT_ARMED",
            None,
            False,
            (),
            (),
            Decimal("100"),
            None,
            12,
            Decimal("0.01"),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            _, scheduler = recorder_scheduler(tmpdir, configured)
            with patch(
                "trading_bot.strategy_scheduler.analyze_n13_vwap_rotation",
                return_value=empty_result,
            ) as mocked:
                scheduler._evaluate_candidate(
                    configured,
                    item,
                    {item.symbol: raw},
                    checked,
                    N12BatchContext({}, {}, set(), False),
                    context,
                )
            kwargs = mocked.call_args.kwargs
        expected = {
            "entry_window_seconds": 77,
            "positive_breadth_min": Decimal("0.61"),
            "above_vwap_breadth_min": Decimal("0.51"),
            "return_rank_min": 12,
            "return_rank_max": 59,
            "vwap_lookback_bars": 95,
            "atr_period": 13,
            "vwap_slope_lookback_bars": 7,
            "upper_atr_fraction": Decimal("0.26"),
            "lower_atr_fraction": Decimal("0.49"),
            "armed_max_bars": 6,
            "confirmation_max_bars": 3,
            "confirmation_close_location_min": Decimal("0.62"),
            "confirmation_taker_buy_ratio_min": Decimal("0.52"),
            "entry_extension_atr_max": Decimal("0.45"),
        }
        for key, value in expected.items():
            self.assertEqual(kwargs[key], value, key)

    def test_legacy_paper_wins_update_statistics_without_live_qualification(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            paper = PaperTrader(recorder, logging.getLogger("n13_paper"))
            for symbol in ("FIRSTUSDT", "SECONDUSDT"):
                self.assertIsNotNone(
                    paper.open_trade("N13", symbol, None, paper_plan(symbol), {})
                )
                self.assertEqual(close_paper_win(paper)[0].result, "WIN")
            state = recorder.get_strategy_state("N13")
            self.assertEqual(state.consecutive_wins, 0)
            self.assertEqual(state.paper_trade_count, 2)
            self.assertEqual(state.win_count, 2)
            self.assertEqual(state.loss_count, 0)
            self.assertFalse(state.live_eligible)

            self.assertTrue(
                record_exact_live_result(
                    recorder,
                    "N13",
                    "S011USDT",
                    "LOSS",
                    "2026-07-13T00:00:00+00:00",
                )
            )
            reset = recorder.get_strategy_state("N13")
            self.assertEqual(reset.consecutive_wins, 0)
            self.assertEqual(reset.paper_trade_count, 2)
            self.assertEqual(reset.win_count, 2)
            self.assertEqual(reset.loss_count, 0)
            self.assertFalse(reset.live_eligible)
            self.assertEqual(reset.last_trade_result, "LOSS")


class N13MainAndTraderTests(unittest.TestCase):
    def test_main_uses_c_close_p_entry_range_and_absolute_deadline(self):
        raw, checked = n13_klines()
        analysis = analyze(raw, checked)
        item = candidate("S011USDT", 11)
        signal = StrategySignalDecision(
            N13_STRATEGY,
            item,
            analysis,
            True,
            "PASSED",
            "PASSED",
        )
        base_plan = paper_plan(item.symbol)

        class FakeTrader:
            def build_vwap_rotation_margin_capped_trade_plan(self, *args, **kwargs):
                self.args = (args, kwargs)
                return replace(
                    base_plan,
                    entry_min_price=kwargs["entry_min_price"],
                    entry_max_price=kwargs["entry_max_price"],
                )

        bot = TradingBot.__new__(TradingBot)
        bot.trader = FakeTrader()
        plan = bot._build_strategy_plan(signal)
        structure = analysis.structure
        self.assertEqual(bot.trader.args[0][1], structure.entry.close)
        self.assertEqual(bot.trader.args[0][2], structure.p)
        self.assertEqual(plan.entry_min_price, structure.c.close)
        self.assertEqual(plan.entry_max_price, structure.entry_max_price)
        self.assertEqual(
            plan.entry_deadline_ms,
            structure.entry.open_time_ms + 120_000,
        )

    def test_plan_stop_boundaries_five_r_and_low_leverage_scaling(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                RuleConstrainedClient(leverage=50),
                test_config(f"{tmpdir}/account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("n13_plan"),
            )
            one = trader.build_vwap_rotation_margin_capped_trade_plan(
                "N13USDT",
                Decimal("100"),
                Decimal("99.5"),
                Decimal("5"),
                entry_min_price=Decimal("100"),
                entry_max_price=Decimal("101.5"),
            )
            five = trader.build_vwap_rotation_margin_capped_trade_plan(
                "N13USDT",
                Decimal("100"),
                Decimal("95.01"),
                Decimal("5"),
                entry_min_price=Decimal("100"),
                entry_max_price=Decimal("101.5"),
            )
            self.assertEqual(one.stop_loss_pct, Decimal("0.01"))
            self.assertEqual(five.stop_loss_pct, Decimal("0.05"))
            self.assertGreaterEqual(
                (five.take_profit_price - five.entry_price)
                / (five.entry_price - five.stop_loss_price),
                Decimal("5"),
            )
            with self.assertRaisesRegex(
                BinanceAPIError, "N13_STOP_PCT_OUT_OF_RANGE"
            ):
                trader.build_vwap_rotation_margin_capped_trade_plan(
                    "N13USDT",
                    Decimal("100"),
                    Decimal("95"),
                    Decimal("5"),
                    entry_min_price=Decimal("100"),
                    entry_max_price=Decimal("101.5"),
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                RuleConstrainedClient(leverage=1),
                test_config(f"{tmpdir}/account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("n13_low_leverage"),
            )
            plan = trader.build_vwap_rotation_margin_capped_trade_plan(
                "N13USDT",
                Decimal("100"),
                Decimal("99.5"),
                Decimal("5"),
                entry_min_price=Decimal("100"),
                entry_max_price=Decimal("101.5"),
            )
            self.assertEqual(plan.quantity, Decimal("9.5"))
            self.assertTrue(plan.risk_capped_by_margin)

    def test_actual_fill_outside_closed_range_emergency_cleans(self):
        entry_min = Decimal("100")
        entry_max = Decimal("101.5")
        allowed_min = entry_min * Decimal("0.995")
        allowed_max = entry_max * Decimal("1.005")
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                FakeLiveExecutionClient({}, leverage=50),
                live_test_config(f"{tmpdir}/account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("n13_boundary"),
            )
            plan = trader.build_vwap_rotation_margin_capped_trade_plan(
                "N13USDT",
                Decimal("100"),
                Decimal("99"),
                Decimal("5"),
                entry_min_price=entry_min,
                entry_max_price=entry_max,
            )
            trader._assert_structured_actual_entry_range(plan, allowed_min)
            trader._assert_structured_actual_entry_range(plan, allowed_max)

        for actual_entry in (
            allowed_min - Decimal("0.0001"),
            allowed_max + Decimal("0.0001"),
        ):
            with self.subTest(actual_entry=actual_entry), tempfile.TemporaryDirectory() as tmpdir:
                client = FakeLiveExecutionClient({}, leverage=50)
                state_store = StateStore(f"{tmpdir}/state.json")
                trader = Trader(
                    client,
                    live_test_config(f"{tmpdir}/account.json"),
                    state_store,
                    logging.getLogger("n13_outside"),
                )
                plan = trader.build_vwap_rotation_margin_capped_trade_plan(
                    "N13USDT",
                    Decimal("100"),
                    Decimal("99"),
                    Decimal("5"),
                    entry_min_price=entry_min,
                    entry_max_price=entry_max,
                )
                client.open_response = {
                    "orderId": 1301,
                    "avgPrice": str(actual_entry),
                    "executedQty": str(plan.quantity),
                }
                client.position_quantities = [str(plan.quantity), "0"]
                client.close_side_effects = [{
                    "status": "FILLED",
                    "executedQty": str(plan.quantity),
                }]
                with self.assertRaisesRegex(
                    BinanceAPIError,
                    "N13_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE",
                ):
                    trader.open_long_plan_with_protection(plan)
                self.assertEqual(client.protection_calls, [])
                stored = state_store.load()
                self.assertIsNotNone(stored)
                self.assertEqual(stored.quantity, "0")
                self.assertEqual(
                    stored.orders["strategy"]["strategy_id"], "N13"
                )
                self.assertTrue(
                    stored.orders["execution_cleanup_resolved"][
                        "emergency_cleanup"
                    ]["confirmed_closed"]
                )

    def test_adverse_fill_reduces_then_rebuilds_n13_five_r_protection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient({}, leverage=50)
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/account.json"),
                state_store,
                logging.getLogger("n13_live"),
            )
            plan = trader.build_vwap_rotation_margin_capped_trade_plan(
                "N13USDT",
                Decimal("100"),
                Decimal("99"),
                Decimal("5"),
                entry_min_price=Decimal("100"),
                entry_max_price=Decimal("101.5"),
            )
            client.open_response = {
                "orderId": 1302,
                "avgPrice": "101",
                "executedQty": str(plan.quantity),
            }
            client.position_quantities = [str(plan.quantity), "99.502"]
            excess = plan.quantity - Decimal("99.502")
            client.close_side_effects = [{
                "status": "FILLED",
                "executedQty": str(excess),
            }]
            state = trader.open_long_plan_with_protection(plan)
            self.assertEqual(state.quantity, "99.502")
            self.assertEqual(
                state.orders["post_fill_adjustment"]["strategy"], "N13"
            )
            self.assertGreaterEqual(
                (Decimal(state.take_profit_price) - Decimal(state.entry_price))
                / (Decimal(state.entry_price) - Decimal(state.stop_loss_price)),
                Decimal("5"),
            )

    def test_n13_configuration_is_present_and_serialized(self):
        by_id = {strategy.strategy_id: strategy for strategy in load_all_strategies()}
        strategy_ids = [
            strategy.strategy_id for strategy in load_all_strategies()
        ]
        self.assertEqual(
            strategy_ids[:20],
            [f"N{index:02d}" for index in range(1, 21)],
        )
        self.assertEqual(strategy_ids, [f"N{index:02d}" for index in range(1, 26)])
        strategy = by_id["N13"]
        self.assertEqual(strategy, N13_STRATEGY)
        self.assertEqual(strategy.volume_top_n, 100)
        self.assertEqual(strategy.entry_window_seconds, 120)
        payload = strategy.to_jsonable()
        self.assertEqual(payload["n13_vwap_lookback_bars"], 96)
        self.assertEqual(payload["n13_atr_period"], 14)
        self.assertEqual(payload["n13_vwap_slope_lookback_bars"], 8)
        self.assertEqual(payload["n13_confirmation_close_location_min"], "0.60")
        self.assertEqual(payload["n13_confirmation_taker_buy_ratio_min"], "0.50")


if __name__ == "__main__":
    unittest.main()
