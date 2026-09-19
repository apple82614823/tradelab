from __future__ import annotations

import hashlib
import json
import logging
import os
from copy import deepcopy
from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import trading_bot.n19_analyzer as n19_module
from trading_bot.coverage_epoch_schema import (
    COVERAGE_EPOCH_RULE_VERSION,
    LEGACY_V3_INSTALLATION_TABLE_SQL,
    LEGACY_V3_PUBLICATION_TABLE_SQL,
    TRIGGER_SQL as COVERAGE_TRIGGER_SQL,
    _LEGACY_V3_OBJECTS,
    _coverage_epoch_catalog_sha256_for_objects,
    _upgrade_coverage_epoch_schema_v3,
    coverage_epoch_schema_status,
    n19_terminal_publication_binding_sha256,
    n19_terminal_publication_receipt_sha256,
    validate_coverage_epoch_graph,
)
from trading_bot.coverage_family_seal import (
    OBJECTS as FAMILY_SEAL_OBJECTS,
    PROVED_TERMINAL_DOMAIN,
    family_seal_schema_status,
    family_seal_sha256,
    terminal_bundle_sha256,
)

from trading_bot.n19_analyzer import (
    INTERVAL_MS,
    _historical_missed_record_from_confirmed,
    analyze_n19_staircase_exhaustion_reversal,
    build_n19_market_context,
    decode_n19_state_evidence,
)
from trading_bot.n19_schema import N19_INDEX_SQL, N19_TRIGGER_SQL
from trading_bot.n15_terminal_schema import (
    N15_TERMINAL_INDEX_SQL,
    N15_TERMINAL_TABLES,
    N15_TERMINAL_TRIGGER_SQL,
)
from trading_bot.strategies import (
    N19_STRATEGY,
    N19StrategyDefinition,
    load_all_strategies,
)
from trading_bot.monitor import FundingCandidate
from trading_bot.monitor import StrategyMarketScan
from trading_bot.binance_client import BinanceAPIError
from trading_bot.main import TradingBot, _N19PlanIntegrityError
from trading_bot.recorder import (
    HistoryCoverageProposal,
    PAPER_TRADE_VOID_CONFIRMATION,
    ReviewRecorder,
    StrategySignalBatchWriteResult,
)
from trading_bot.signal_retention import (
    SignalRetentionMaintenanceError,
    _install_n16_claim_boundary,
    _install_n17_lifecycle_boundary,
    _install_n18_lifecycle_boundary,
    _install_n19_lifecycle_boundary,
    _install_n20_lifecycle_boundary,
    _install_coverage_epoch_boundary,
    _install_micro_lifecycle_boundary,
)
from trading_bot.strategy_scheduler import StrategyScheduler
from trading_bot.state import StateStore
from trading_bot.trader import EntryWindowExpiredError, Trader
from tests.recorder_test_utils import make_test_recorder
from tests.test_trader import (
    FakeLiveExecutionClient,
    RuleConstrainedClient,
    live_test_config,
)


BASE_TIME = 1_900_000_000_000 // INTERVAL_MS * INTERVAL_MS


def _row(index, values, quote_volume="100", taker="45"):
    open_price, high, low, close = values
    return [
        BASE_TIME + index * INTERVAL_MS,
        open_price, high, low, close, "0",
        BASE_TIME + (index + 1) * INTERVAL_MS - 1,
        quote_volume, "0", "0", taker,
    ]


def n19_fixture():
    rows = [
        _row(index, ("109", "109.5", "108.5", "109"))
        for index in range(105)
    ]
    values = (
        (("109.5", "110", "109", "109.4"), "100", "45"),
        (("109.4", "109.5", "107.5", "108"), "100", "45"),
        (("108", "108.2", "107", "107.4"), "100", "45"),
        (("107.4", "108", "107.3", "107.8"), "100", "45"),
        (("107.8", "108.5", "107.7", "108.2"), "100", "45"),
        (("108.2", "108.3", "106.2", "106.8"), "100", "45"),
        (("106.8", "107", "105.5", "106"), "100", "45"),
        (("106", "106.2", "105.8", "106.1"), "100", "45"),
        (("106.1", "106.5", "106", "106.4"), "100", "45"),
        (("106.4", "106.8", "106.2", "106.6"), "100", "45"),
        (("106.6", "106.7", "106", "106.2"), "100", "45"),
        (("106.2", "106.3", "105.8", "106"), "100", "45"),
        (("106", "106.1", "105.6", "105.8"), "100", "45"),
        (("105.8", "105.9", "105.5", "105.6"), "100", "45"),
        (("105.6", "105.8", "105.1", "105.5"), "80", "44"),
        (("105.5", "106.4", "105.2", "106.3"), "100", "60"),
        (("106.3", "106.5", "105.8", "106.4"), "100", "55"),
    )
    for index, (prices, volume, taker) in enumerate(values, start=105):
        rows.append(_row(index, prices, volume, taker))
    return rows


def market_fixture(rows=None):
    source = n19_fixture() if rows is None else rows
    symbols = tuple("P%03dUSDT" % value for value in range(100))
    return symbols, {symbol: deepcopy(source) for symbol in symbols}


def candidate(symbol="P000USDT", rank=1):
    return FundingCandidate(
        symbol,
        None,
        Decimal("106.4"),
        quote_volume=Decimal("1000000"),
        quote_volume_rank=rank,
        candidate_universe="quote_volume_top",
    )


def analyze(rows=None, **overrides):
    source = n19_fixture() if rows is None else rows
    symbols, market = market_fixture(source)
    arguments = {
        "quote_volume_rank": 7,
        "market_symbols": symbols,
        "market_klines_by_symbol": market,
        "checked_at_ms": BASE_TIME + 121 * INTERVAL_MS + 60_000,
    }
    arguments.update(overrides)
    return analyze_n19_staircase_exhaustion_reversal(
        "P000USDT", source, **arguments
    )


class N19DefinitionTests(unittest.TestCase):
    def test_n19_is_independent_and_registration_order_is_exact(self):
        strategies = load_all_strategies()
        self.assertEqual(
            [item.strategy_id for item in strategies],
            ["N%02d" % value for value in range(1, 26)],
        )
        frozen = json.dumps(
            [item.to_jsonable() for item in strategies[:17]],
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
        self.assertEqual(
            hashlib.sha256(frozen).hexdigest(),
            "84cf8666ce62f4015b8c7a699494b332b638caa69bbf419c97e36a1686c0e4da",
        )
        self.assertIsInstance(N19_STRATEGY, N19StrategyDefinition)
        self.assertEqual(N19_STRATEGY.risk_reward_ratio, Decimal("5"))
        self.assertEqual(N19_STRATEGY.fixed_input_bars, 122)
        self.assertIsNone(N19_STRATEGY.funding_threshold)


class N19AnalyzerTests(unittest.TestCase):
    def test_valid_three_leg_exhaustion_confirmation_and_entry_pass(self):
        result = analyze()
        self.assertTrue(result.passed, result.reason)
        self.assertEqual(result.reason, "PASSED")
        self.assertEqual(result.state_record.stage, "CONFIRMED")
        self.assertEqual(result.market_context.member_count, 100)
        self.assertFalse(result.market_context.systemic_crash)
        decoded = decode_n19_state_evidence(
            result.state_record.evidence_json, expected_symbol="P000USDT"
        )
        self.assertEqual(decoded.family_id, result.structure.family_id)
        self.assertEqual(decoded.structure_id, result.structure_id)

    def test_same_round_market_context_is_built_once_per_confirmation_axis(self):
        rows = n19_fixture()
        symbols, market = market_fixture(rows)
        cache = {}
        with patch.object(
            n19_module,
            "build_n19_market_context",
            wraps=n19_module.build_n19_market_context,
        ) as build_context:
            results = [
                analyze_n19_staircase_exhaustion_reversal(
                    symbol,
                    market[symbol],
                    quote_volume_rank=rank,
                    market_symbols=symbols,
                    market_klines_by_symbol=market,
                    checked_at_ms=BASE_TIME + 121 * INTERVAL_MS + 60_000,
                    market_context_cache=cache,
                )
                for rank, symbol in enumerate(symbols[:3], start=1)
            ]
        self.assertTrue(all(item.market_context.complete for item in results))
        self.assertEqual(build_context.call_count, 1)
        self.assertEqual(len(cache), 1)

    def test_systemic_crash_equal_threshold_is_veto_but_ordinary_weak_is_not(self):
        rows = n19_fixture()
        symbols, market = market_fixture(rows)
        c_time = int(rows[120][0])
        for index, symbol in enumerate(symbols):
            if index < 75:
                market[symbol][116][1:5] = ["100", "100.5", "99.5", "100"]
                market[symbol][120][1:5] = ["99.2", "99.5", "98.8", "99"]
            else:
                market[symbol][116][1:5] = ["100", "100.5", "99.5", "100"]
                market[symbol][120][1:5] = ["100.5", "101.5", "100", "101"]
        context = build_n19_market_context(c_time, symbols, market)
        self.assertTrue(context.complete)
        self.assertEqual(context.down_breadth, Decimal("0.75"))
        self.assertLessEqual(context.median_return, Decimal("-0.01"))
        result = analyze(
            rows, market_symbols=symbols, market_klines_by_symbol=market
        )
        self.assertEqual(result.reason, "N19_SYSTEMIC_CRASH_VETO")
        self.assertFalse(result.passed)

        for index, symbol in enumerate(symbols):
            market[symbol][116][1:5] = ["100", "100.5", "99.5", "100"]
            market[symbol][120][1:5] = (
                ["99.2", "99.5", "98.8", "99"]
                if index < 74
                else ["100.5", "101.5", "100", "101"]
            )
        ordinary_weak = analyze(
            rows, market_symbols=symbols, market_klines_by_symbol=market
        )
        self.assertNotEqual(ordinary_weak.reason, "N19_SYSTEMIC_CRASH_VETO")

    def test_missing_or_misaligned_top100_is_fail_closed(self):
        rows = n19_fixture()
        symbols, market = market_fixture(rows)
        missing = dict(market)
        missing.pop(symbols[-1])
        self.assertEqual(
            analyze(rows, market_symbols=symbols, market_klines_by_symbol=missing).reason,
            "N19_MARKET_CONTEXT_INSUFFICIENT",
        )
        broken = deepcopy(market)
        broken[symbols[-1]][120][0] += 1
        self.assertEqual(
            analyze(rows, market_symbols=symbols, market_klines_by_symbol=broken).reason,
            "N19_MARKET_CONTEXT_INSUFFICIENT",
        )

    def test_all_frozen_structure_scalar_thresholds_are_exact(self):
        config = dict(n19_module._APPROVED_CONFIG)
        safe = {
            "bars": 15,
            "total_atr": Decimal("4"),
            "l1_l2": Decimal("1"),
            "l2_x": Decimal("0.25"),
            "s_r1": Decimal("0.5"),
            "r1_r2": Decimal("0.5"),
            "rebound_1": Decimal("0.4"),
            "rebound_2": Decimal("0.4"),
            "max_body": Decimal("0.2"),
            "x_range": Decimal("0.5"),
            "atr_x": Decimal("1"),
            "x_volume_ratio": Decimal("0.5"),
            "x_taker_ratio": Decimal("0.6"),
            "x_taker_improvement": Decimal("0.2"),
            "config": config,
        }
        cases = (
            ("bars_min", "bars", 10, 9),
            ("bars_max", "bars", 19, 20),
            ("drop_min", "total_atr", Decimal("2"), Decimal("1.999")),
            ("drop_max", "total_atr", Decimal("6"), Decimal("6.001")),
            ("lower_low", "l1_l2", Decimal("0.35"), Decimal("0.349")),
            ("x_extension", "l2_x", Decimal("0.50"), Decimal("0.501")),
            ("s_r1", "s_r1", Decimal("0.20"), Decimal("0.199")),
            ("r1_r2", "r1_r2", Decimal("0.20"), Decimal("0.199")),
            ("rebound1_min", "rebound_1", Decimal("0.20"), Decimal("0.199")),
            ("rebound1_max", "rebound_1", Decimal("0.55"), Decimal("0.551")),
            ("rebound2_min", "rebound_2", Decimal("0.20"), Decimal("0.199")),
            ("rebound2_max", "rebound_2", Decimal("0.55"), Decimal("0.551")),
            ("body", "max_body", Decimal("0.40"), Decimal("0.401")),
            ("x_range", "x_range", Decimal("0.90"), Decimal("0.901")),
            ("x_volume", "x_volume_ratio", Decimal("0.85"), Decimal("0.851")),
            ("x_taker", "x_taker_ratio", Decimal("0.45"), Decimal("0.449")),
            ("x_improvement", "x_taker_improvement", Decimal("0.08"), Decimal("0.079")),
        )
        for name, field, allowed, rejected in cases:
            with self.subTest(name=name, side="equal"):
                values = dict(safe)
                values[field] = allowed
                self.assertTrue(n19_module._structure_thresholds_pass(**values))
            with self.subTest(name=name, side="outside"):
                values = dict(safe)
                values[field] = rejected
                self.assertFalse(n19_module._structure_thresholds_pass(**values))
        values = dict(safe)
        values["l2_x"] = Decimal("0")
        self.assertFalse(n19_module._structure_thresholds_pass(**values))

    def test_confirmation_thresholds_first_and_third_bar_are_exact(self):
        config = dict(n19_module._APPROVED_CONFIG)
        x = n19_module.N19Candle(
            1, BASE_TIME, Decimal("100"), Decimal("101"), Decimal("99"),
            Decimal("100"), Decimal("100"), Decimal("45"),
        )
        equal = n19_module.N19Candle(
            2, BASE_TIME + INTERVAL_MS, Decimal("105"), Decimal("110"),
            Decimal("100"), Decimal("107"), Decimal("100"), Decimal("55"),
        )
        self.assertTrue(
            n19_module._confirmation_thresholds_pass(
                equal, x, Decimal("0.80"), config
            )
        )
        failures = (
            ("low", {"low": Decimal("98.999"), "open": Decimal("105"), "high": Decimal("110"), "close": Decimal("107")}, Decimal("0.80")),
            ("location", {"low": Decimal("100"), "open": Decimal("105"), "high": Decimal("110"), "close": Decimal("106.999")}, Decimal("0.80")),
            ("taker", {"low": Decimal("100"), "open": Decimal("105"), "high": Decimal("110"), "close": Decimal("107"), "taker_buy_quote_volume": Decimal("54.999")}, Decimal("0.80")),
            ("volume", {}, Decimal("0.799")),
        )
        for name, changes, multiple in failures:
            with self.subTest(name=name):
                candle = n19_module.N19Candle(
                    equal.index,
                    equal.open_time_ms,
                    changes.get("open", equal.open),
                    changes.get("high", equal.high),
                    changes.get("low", equal.low),
                    changes.get("close", equal.close),
                    equal.quote_volume,
                    changes.get(
                        "taker_buy_quote_volume", equal.taker_buy_quote_volume
                    ),
                )
                self.assertFalse(
                    n19_module._confirmation_thresholds_pass(
                        candle, x, multiple, config
                    )
                )

        rows = n19_fixture()
        self.assertEqual(analyze(rows).structure.c.open_time_ms, int(rows[120][0]))
        third = deepcopy(rows)
        third[120] = _row(120, ("105.5", "105.7", "105.2", "105.6"), "100", "50")
        third[121] = _row(121, ("105.6", "105.7", "105.3", "105.6"), "100", "50")
        third.append(_row(122, ("105.5", "106.4", "105.2", "106.3"), "100", "60"))
        third.append(_row(123, ("106.3", "106.5", "105.8", "106.4"), "100", "55"))
        third_result = analyze(
            third, checked_at_ms=BASE_TIME + 123 * INTERVAL_MS + 60_000
        )
        self.assertTrue(third_result.passed, third_result.reason)
        self.assertEqual(third_result.structure.c.open_time_ms, int(third[-2][0]))
        no_confirmation = deepcopy(third)
        no_confirmation[-2] = _row(
            122, ("105.6", "105.7", "105.3", "105.6"), "100", "50"
        )
        no_confirmation_result = analyze(
            no_confirmation,
            checked_at_ms=BASE_TIME + 123 * INTERVAL_MS + 60_000,
        )
        self.assertEqual(
            no_confirmation_result.reason, "N19_CONFIRMATION_NOT_FOUND"
        )
        self.assertEqual(no_confirmation_result.state_record.stage, "CONSUMED")

    def test_entry_window_and_price_interval_boundaries_are_exact(self):
        base = analyze()
        entry_min = base.structure.entry_min
        entry_max = base.structure.entry_max
        current_time = int(n19_fixture()[-1][0])
        for elapsed in (0, 119_999):
            with self.subTest(elapsed=elapsed):
                self.assertTrue(
                    analyze(checked_at_ms=current_time + elapsed).passed
                )
        expired = analyze(checked_at_ms=current_time + 120_000)
        self.assertEqual(expired.reason, "N19_ENTRY_WINDOW_EXPIRED")
        for label, price, expected in (
            ("min", entry_min, "PASSED"),
            ("max", entry_max, "PASSED"),
            ("below", entry_min - Decimal("0.0001"), "N19_ENTRY_BELOW_MIN_WAITING"),
            ("above", entry_max + Decimal("0.0001"), "N19_ENTRY_PRICE_ABOVE_MAX"),
        ):
            with self.subTest(price=label):
                rows = n19_fixture()
                rows[-1][1] = str(price)
                rows[-1][2] = str(price + Decimal("0.1"))
                rows[-1][3] = "105.8"
                rows[-1][4] = str(price)
                result = analyze(rows)
                self.assertEqual(result.reason, expected)
                self.assertEqual(result.passed, expected == "PASSED")

    def test_real_entry_terminal_evidence_decodes_persists_and_replays_exactly(self):
        base = analyze()
        above_rows = deepcopy(n19_fixture())
        above_price = base.structure.entry_max + Decimal("0.0001")
        above_rows[-1][1] = str(above_price)
        above_rows[-1][2] = str(above_price + Decimal("0.1"))
        above_rows[-1][3] = "105.8"
        above_rows[-1][4] = str(above_price)
        broken_rows = deepcopy(n19_fixture())
        broken_rows[-1][3] = str(base.structure.x.low - Decimal("0.0001"))
        cases = (
            (
                "expired",
                n19_fixture(),
                BASE_TIME + 121 * INTERVAL_MS + 120_000,
                "N19_ENTRY_WINDOW_EXPIRED",
            ),
            (
                "above",
                above_rows,
                BASE_TIME + 121 * INTERVAL_MS + 60_000,
                "N19_ENTRY_PRICE_ABOVE_MAX",
            ),
            (
                "broken",
                broken_rows,
                BASE_TIME + 121 * INTERVAL_MS + 60_000,
                "N19_ENTRY_BROKE_X_LOW",
            ),
        )

        def rehash(evidence):
            unsigned = dict(evidence)
            unsigned.pop("canonical_sha256", None)
            evidence["canonical_sha256"] = n19_module._sha256_json(unsigned)
            return evidence

        def changed(field, value):
            if field == "open_time_ms":
                return value + INTERVAL_MS
            increment = Decimal("0.01")
            if field == "low":
                increment = -increment
            return str(Decimal(value) + increment)

        for label, rows, checked_at_ms, reason in cases:
            with self.subTest(case=label):
                result = analyze(rows, checked_at_ms=checked_at_ms)
                self.assertEqual(result.reason, reason)
                record = result.state_record
                entry = record.evidence["structure"]["entry"]
                self.assertEqual(entry, result.structure.entry.to_jsonable())
                self.assertEqual(
                    record.evidence["terminal_cutoff_time_ms"],
                    result.structure.entry.open_time_ms,
                )
                source_entry = next(
                    item
                    for item in record.evidence["source"]
                    if item["open_time_ms"] == entry["open_time_ms"]
                )
                self.assertEqual(source_entry, entry)
                self.assertEqual(
                    decode_n19_state_evidence(
                        record.evidence_json,
                        expected_symbol="P000USDT",
                    ),
                    record,
                )

                closed_rows = deepcopy(rows)
                closed_rows[-1][7] = str(
                    Decimal(str(closed_rows[-1][7])) + Decimal("10")
                )
                closed_rows[-1][10] = str(
                    Decimal(str(closed_rows[-1][10])) + Decimal("5")
                )
                r2_high = Decimal(
                    record.evidence["structure"]["r2"]["high"]
                )
                reset_close = r2_high + Decimal("0.10")
                closed_rows.extend(
                    (
                        _row(
                            122,
                            (
                                str(reset_close),
                                str(reset_close + Decimal("0.10")),
                                str(reset_close - Decimal("0.10")),
                                str(reset_close),
                            ),
                            "100",
                            "55",
                        ),
                        _row(
                            123,
                            (
                                str(reset_close),
                                str(reset_close + Decimal("0.10")),
                                str(reset_close - Decimal("0.10")),
                                str(reset_close),
                            ),
                            "100",
                            "55",
                        ),
                    )
                )
                reset = analyze(
                    closed_rows,
                    checked_at_ms=BASE_TIME + 123 * INTERVAL_MS + 60_000,
                    frozen_evidence=record.evidence_json,
                )
                self.assertEqual(reset.reason, "N19_STRUCTURE_CONSUMED")
                self.assertEqual(len(reset.state_records), 1)
                reset_record = reset.state_records[0]
                self.assertGreater(
                    reset_record.reset_after_time_ms,
                    record.evidence["terminal_cutoff_time_ms"],
                )
                self.assertEqual(
                    reset_record.evidence["structure"]["entry"],
                    next(
                        item
                        for item in reset_record.evidence["source"]
                        if item["open_time_ms"]
                        == record.evidence["structure"]["entry"]["open_time_ms"]
                    ),
                )
                self.assertEqual(
                    decode_n19_state_evidence(
                        reset_record.evidence_json,
                        expected_symbol="P000USDT",
                    ),
                    reset_record,
                )

                post_reset_rows = deepcopy(closed_rows)
                for next_index in range(124, 127):
                    with self.subTest(
                        case=label,
                        reset_replay=next_index,
                    ):
                        post_reset_rows.append(
                            _row(
                                next_index,
                                (
                                    str(reset_close),
                                    str(reset_close + Decimal("0.10")),
                                    str(reset_close - Decimal("0.10")),
                                    str(reset_close),
                                ),
                                "100",
                                "55",
                            )
                        )
                        replay_after_reset = analyze(
                            post_reset_rows,
                            checked_at_ms=(
                                BASE_TIME
                                + next_index * INTERVAL_MS
                                + 60_000
                            ),
                            frozen_evidence=reset_record.evidence_json,
                        )
                        self.assertNotEqual(
                            replay_after_reset.reason,
                            "N19_FROZEN_EVIDENCE_INVALID",
                        )
                        self.assertEqual(
                            decode_n19_state_evidence(
                                reset_record.evidence_json,
                                expected_symbol="P000USDT",
                            ),
                            reset_record,
                        )

                future_rows = deepcopy(n19_fixture())
                for future_row in future_rows:
                    future_row[0] += 200 * INTERVAL_MS
                    future_row[6] += 200 * INTERVAL_MS
                future = analyze(
                    future_rows,
                    checked_at_ms=int(future_rows[-1][0]) + 60_000,
                    frozen_evidence=reset_record.evidence_json,
                )
                self.assertTrue(future.passed, future.reason)
                self.assertNotEqual(
                    future.state_record.family_id,
                    reset_record.family_id,
                )
                self.assertGreater(
                    future.state_record.s_open_time_ms,
                    reset_record.reset_after_time_ms,
                )

                for replay_index in range(3):
                    with self.subTest(case=label, replay=replay_index):
                        replay = analyze(
                            rows,
                            checked_at_ms=checked_at_ms,
                            frozen_evidence=record.evidence_json,
                        )
                        self.assertEqual(replay.reason, "N19_STRUCTURE_CONSUMED")
                        self.assertEqual(replay.state_record, record)

                with tempfile.TemporaryDirectory() as directory:
                    database_path = Path(directory) / "review.sqlite3"
                    recorder = make_test_recorder(
                        database_path,
                        logging.getLogger("n19-real-entry-%s" % label),
                    )
                    self.assertEqual(recorder.record_n19_state(record), "INSERTED")
                    self.assertEqual(
                        recorder.record_n19_state(reset_record),
                        "UPDATED",
                    )
                    for restart_index in range(3):
                        with self.subTest(case=label, restart=restart_index):
                            recorder = make_test_recorder(
                                database_path,
                                logging.getLogger(
                                    "n19-real-entry-%s-%s"
                                    % (label, restart_index)
                                ),
                            )
                            self.assertEqual(
                                recorder.record_n19_state(reset_record),
                                "UNCHANGED",
                            )
                            latest = recorder.get_latest_n19_states(
                                {"P000USDT"}
                            )["P000USDT"]
                            self.assertEqual(
                                (
                                    latest.strategy_id,
                                    latest.symbol,
                                    latest.family_id,
                                    latest.structure_id,
                                    latest.stage,
                                    latest.reason,
                                    latest.quote_volume_rank,
                                    latest.s_open_time_ms,
                                    latest.x_open_time_ms,
                                    latest.reset_after_time_ms,
                                    latest.evidence_json,
                                    latest.evidence_sha256,
                                ),
                                (
                                    reset_record.strategy_id,
                                    reset_record.symbol,
                                    reset_record.family_id,
                                    reset_record.structure_id,
                                    reset_record.stage,
                                    reset_record.reason,
                                    reset_record.quote_volume_rank,
                                    reset_record.s_open_time_ms,
                                    reset_record.x_open_time_ms,
                                    reset_record.reset_after_time_ms,
                                    reset_record.evidence_json,
                                    reset_record.evidence_sha256,
                                ),
                            )
                            self.assertEqual(
                                decode_n19_state_evidence(
                                    latest.evidence_json,
                                    expected_symbol="P000USDT",
                                ),
                                reset_record,
                            )
                            post_restart = analyze(
                                post_reset_rows,
                                checked_at_ms=(
                                    BASE_TIME
                                    + 126 * INTERVAL_MS
                                    + 60_000
                                ),
                                frozen_evidence=latest.evidence_json,
                            )
                            self.assertNotEqual(
                                post_restart.reason,
                                "N19_FROZEN_EVIDENCE_INVALID",
                            )
                            future_after_restart = analyze(
                                future_rows,
                                checked_at_ms=(
                                    int(future_rows[-1][0]) + 60_000
                                ),
                                frozen_evidence=latest.evidence_json,
                            )
                            self.assertTrue(
                                future_after_restart.passed,
                                future_after_restart.reason,
                            )

                for field in (
                    "open_time_ms",
                    "open",
                    "high",
                    "low",
                    "close",
                    "quote_volume",
                    "taker_buy_quote_volume",
                ):
                    for target in ("entry", "source"):
                        with self.subTest(
                            case=label,
                            tamper_target=target,
                            field=field,
                        ):
                            attacked = deepcopy(record.evidence)
                            if target == "entry":
                                item = attacked["structure"]["entry"]
                            else:
                                item = next(
                                    source_item
                                    for source_item in attacked["source"]
                                    if source_item["open_time_ms"]
                                    == entry["open_time_ms"]
                                )
                            item[field] = changed(field, item[field])
                            rehash(attacked)
                            with self.assertRaises(ValueError):
                                decode_n19_state_evidence(
                                    attacked,
                                    expected_symbol="P000USDT",
                                )

    def test_first_x_three_leg_waterfall_axis_and_nonfinite_fail_closed(self):
        first_x_bad = n19_fixture()
        first_x_bad[119][7] = "90"
        first_x_bad[120] = _row(
            120, ("105.4", "105.6", "104.9", "105.3"), "80", "44"
        )
        self.assertEqual(
            analyze(first_x_bad).reason, "N19_NO_STAIRCASE_EXHAUSTION"
        )
        broken_leg = n19_fixture()
        broken_leg[111][1:5] = ["107.3", "107.5", "107.1", "107.2"]
        self.assertEqual(
            analyze(broken_leg).reason, "N19_NO_STAIRCASE_EXHAUSTION"
        )
        waterfall = n19_fixture()
        waterfall[110][4] = "106.2"
        self.assertEqual(
            analyze(waterfall).reason, "N19_NO_STAIRCASE_EXHAUSTION"
        )
        gap = n19_fixture()
        gap[50][0] += INTERVAL_MS
        self.assertEqual(analyze(gap).reason, "N19_KLINE_SEQUENCE_INVALID")
        for value in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(value=value):
                invalid = n19_fixture()
                invalid[119][7] = value
                self.assertEqual(analyze(invalid).reason, "N19_KLINE_DATA_INVALID")

    def test_systemic_crash_requires_both_thresholds(self):
        rows = n19_fixture()
        symbols, base_market = market_fixture(rows)
        c_time = int(rows[120][0])
        for down_count, down_close, expected in (
            (75, "99", True),
            (74, "99", False),
            (75, "99.1", False),
            (74, "99.1", False),
        ):
            with self.subTest(
                down_count=down_count, down_close=down_close
            ):
                market = deepcopy(base_market)
                for index, symbol in enumerate(symbols):
                    market[symbol][116][1:5] = ["100", "100.5", "99.5", "100"]
                    market[symbol][120][1:5] = (
                        [down_close, "100", "98.5", down_close]
                        if index < down_count
                        else ["101", "101.5", "100", "101"]
                    )
                context = build_n19_market_context(c_time, symbols, market)
                self.assertTrue(context.complete)
                self.assertEqual(context.systemic_crash, expected)
                result = analyze(
                    rows,
                    market_symbols=symbols,
                    market_klines_by_symbol=market,
                )
                self.assertEqual(
                    result.reason == "N19_SYSTEMIC_CRASH_VETO", expected
                )

    def test_first_install_only_marks_historical_entry_missed(self):
        rows = n19_fixture()
        rows.append(_row(122, ("106.4", "106.8", "106.2", "106.5"), "100", "55"))
        result = analyze(
            rows,
            checked_at_ms=BASE_TIME + 122 * INTERVAL_MS + 60_000,
        )
        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "N19_HISTORICAL_ENTRY_MISSED")
        self.assertEqual(result.state_record.stage, "MISSED")

    def test_confirmed_historical_transition_attests_every_overlapping_frozen_row(self):
        rows = n19_fixture()
        symbols, incomplete_market = market_fixture(rows)
        incomplete_market.pop(symbols[-1])
        confirmed = analyze(
            rows,
            market_symbols=symbols,
            market_klines_by_symbol=incomplete_market,
        )
        self.assertEqual(
            (confirmed.state_record.stage, confirmed.reason),
            ("CONFIRMED", "N19_MARKET_CONTEXT_INSUFFICIENT"),
        )
        extended = deepcopy(rows) + [
            _row(122, ("106.4", "106.8", "106.2", "106.5"), "100", "55")
        ]
        control = analyze(
            extended,
            checked_at_ms=BASE_TIME + 122 * INTERVAL_MS + 60_000,
            frozen_evidence=confirmed.state_record.evidence_json,
        )
        self.assertEqual(control.reason, "N19_HISTORICAL_ENTRY_MISSED")
        self.assertEqual(
            control.state_record.evidence["source"][-1],
            confirmed.state_record.evidence["source"][-1],
        )

        immutable_times = (
            confirmed.state_record.evidence["source"][1]["open_time_ms"],
            confirmed.state_record.x_open_time_ms,
            confirmed.state_record.evidence["structure"]["c"]["open_time_ms"],
        )
        for open_time_ms in immutable_times:
            with self.subTest(open_time_ms=open_time_ms):
                attacked = deepcopy(extended)
                row = next(item for item in attacked if item[0] == open_time_ms)
                row[4] = str(Decimal(str(row[4])) + Decimal("0.01"))
                row[2] = str(max(Decimal(str(row[2])), Decimal(row[4])))
                result = analyze(
                    attacked,
                    checked_at_ms=BASE_TIME + 122 * INTERVAL_MS + 60_000,
                    frozen_evidence=confirmed.state_record.evidence_json,
                )
                self.assertEqual(result.reason, "N19_FROZEN_EVIDENCE_INVALID")

        # The old source tail was the live E candle.  Once it is closed, a
        # close-only rewrite has no cumulative trade evidence and is rejected.
        attacked_tail = deepcopy(extended)
        attacked_tail[-2][4] = str(
            Decimal(str(attacked_tail[-2][4])) + Decimal("0.01")
        )
        attacked_tail[-2][2] = str(
            max(
                Decimal(str(attacked_tail[-2][2])),
                Decimal(str(attacked_tail[-2][4])),
            )
        )
        self.assertEqual(
            analyze(
                attacked_tail,
                checked_at_ms=BASE_TIME + 122 * INTERVAL_MS + 60_000,
                frozen_evidence=confirmed.state_record.evidence_json,
            ).reason,
            "N19_FROZEN_EVIDENCE_INVALID",
        )

        # A real live-to-closed advance is narrowly provable: only the tail
        # changes, OHLC monotonicity holds, and cumulative quote/taker volume
        # advances consistently.  The closed form becomes permanent evidence.
        naturally_closed = deepcopy(extended)
        naturally_closed[-2][4] = "106.45"
        naturally_closed[-2][7] = "110"
        naturally_closed[-2][10] = "60"
        natural = analyze(
            naturally_closed,
            checked_at_ms=BASE_TIME + 122 * INTERVAL_MS + 60_000,
            frozen_evidence=confirmed.state_record.evidence_json,
        )
        self.assertEqual(natural.reason, "N19_HISTORICAL_ENTRY_MISSED")
        self.assertEqual(
            natural.state_record.evidence["source"][-1],
            n19_module.parse_n19_klines(
                [naturally_closed[-2]]
            )[0].to_jsonable(),
        )
        self.assertIsNone(
            natural.state_record.evidence["structure"]["entry"]
        )
        replay = analyze(
            naturally_closed[1:] + [
                _row(123, ("106.5", "106.8", "106.2", "106.5"), "100", "55")
            ],
            checked_at_ms=BASE_TIME + 123 * INTERVAL_MS + 60_000,
            frozen_evidence=natural.state_record.evidence_json,
        )
        self.assertNotEqual(replay.reason, "N19_FROZEN_EVIDENCE_INVALID")

        forged = deepcopy(confirmed.state_record.evidence)
        c_time = forged["structure"]["c"]["open_time_ms"]
        source_c = next(
            item for item in forged["source"]
            if item["open_time_ms"] == c_time
        )
        source_c["close"] = str(Decimal(source_c["close"]) + Decimal("0.01"))
        source_c["high"] = str(max(Decimal(source_c["high"]), Decimal(source_c["close"])))
        forged["structure"]["c"]["close"] = source_c["close"]
        forged["structure"]["c"]["high"] = source_c["high"]
        unsigned = dict(forged)
        unsigned.pop("canonical_sha256", None)
        forged["canonical_sha256"] = n19_module._sha256_json(unsigned)
        forged_result = analyze(
            extended,
            checked_at_ms=BASE_TIME + 122 * INTERVAL_MS + 60_000,
            frozen_evidence=forged,
        )
        self.assertEqual(
            forged_result.reason,
            "N19_FROZEN_EVIDENCE_INVALID",
        )

    def test_passed_live_entry_closes_into_one_canonical_terminal_record(self):
        passed = analyze()
        rows = deepcopy(n19_fixture())
        rows[-1][2] = "106.6"
        rows[-1][4] = "106.45"
        rows[-1][7] = "110"
        rows[-1][10] = "60"
        rows.append(
            _row(122, ("106.45", "106.8", "106.2", "106.5"), "100", "55")
        )
        terminal = analyze(
            rows,
            checked_at_ms=BASE_TIME + 122 * INTERVAL_MS + 60_000,
            frozen_evidence=passed.state_record.evidence_json,
        )
        self.assertEqual(terminal.reason, "N19_HISTORICAL_ENTRY_MISSED")
        closed_entry = terminal.state_record.evidence["structure"]["entry"]
        self.assertEqual(
            closed_entry,
            terminal.state_record.evidence["source"][-1],
        )
        self.assertNotEqual(
            closed_entry,
            passed.state_record.evidence["structure"]["entry"],
        )
        self.assertEqual(
            decode_n19_state_evidence(
                terminal.state_record.evidence_json,
                expected_symbol="P000USDT",
            ),
            terminal.state_record,
        )
        for offset in range(1, 4):
            replay_rows = rows[offset:] + [
                _row(
                    122 + offset,
                    ("106.5", "106.8", "106.2", "106.5"),
                    "100",
                    "55",
                )
            ]
            replay = analyze(
                replay_rows,
                checked_at_ms=(
                    BASE_TIME
                    + (122 + offset) * INTERVAL_MS
                    + 60_000
                ),
                frozen_evidence=terminal.state_record.evidence_json,
            )
            self.assertNotEqual(
                replay.reason,
                "N19_FROZEN_EVIDENCE_INVALID",
            )

    def test_payload_size_equal_limits_is_allowed_and_one_byte_over_is_rejected(self):
        result = analyze()
        self.assertTrue(result.passed)
        with patch.object(
            n19_module, "_canonical_json", return_value="x" * (16 * 1024)
        ):
            self.assertEqual(result.detail_json()["reason"], "PASSED")
        with patch.object(
            n19_module, "_canonical_json", return_value="x" * (16 * 1024 + 1)
        ):
            with self.assertRaisesRegex(ValueError, "exceeds 16KiB"):
                result.detail_json()

        structure = result.structure
        self.assertIsNotNone(structure)
        with patch.object(
            n19_module, "_canonical_json", return_value="x" * (128 * 1024)
        ):
            record = n19_module._record(
                structure,
                "CONFIRMED",
                "PASSED",
                7,
                n19_module.parse_n19_klines(n19_fixture()),
                result.market_context,
            )
            self.assertEqual(record.stage, "CONFIRMED")
        with patch.object(
            n19_module,
            "_canonical_json",
            return_value="x" * (128 * 1024 + 1),
        ):
            with self.assertRaisesRegex(ValueError, "exceeds 128KiB"):
                n19_module._record(
                    structure,
                    "CONFIRMED",
                    "PASSED",
                    7,
                    n19_module.parse_n19_klines(n19_fixture()),
                    result.market_context,
                )

    def test_failed_first_confirmation_is_frozen_and_cannot_reset_retroactively(self):
        rows = n19_fixture()
        rows[120][1:5] = ["106.3", "107.2", "105.2", "107"]
        rows[120][10] = "40"
        failed = analyze(rows)
        self.assertEqual(failed.reason, "N19_CONFIRMATION_NOT_QUALIFIED")
        self.assertEqual(failed.state_record.stage, "CONSUMED")
        self.assertEqual(
            failed.structure.c.open_time_ms,
            int(rows[120][0]),
        )
        self.assertEqual(
            failed.state_record.evidence["terminal_cutoff_time_ms"],
            int(rows[120][0]),
        )
        self.assertIsNotNone(failed.structure_id)

        same = analyze(rows, frozen_evidence=failed.state_record.evidence_json)
        self.assertEqual(same.reason, "N19_STRUCTURE_CONSUMED")

        equal_reset = deepcopy(rows)
        equal_reset.append(
            _row(122, ("106.5", "107", "106.2", "106.8"), "100", "50")
        )
        equal_reset.append(
            _row(123, ("106.8", "107", "106.5", "106.9"), "100", "50")
        )
        equal_result = analyze(
            equal_reset,
            checked_at_ms=BASE_TIME + 123 * INTERVAL_MS + 60_000,
            frozen_evidence=failed.state_record.evidence_json,
        )
        self.assertEqual(equal_result.reason, "N19_STRUCTURE_CONSUMED")

        strict_reset = deepcopy(equal_reset[:-1])
        strict_reset.append(
            _row(123, ("106.8", "107.2", "106.5", "106.9"), "100", "50")
        )
        strict_reset.append(
            _row(124, ("106.9", "107.1", "106.6", "107"), "100", "50")
        )
        reset_result = analyze(
            strict_reset,
            checked_at_ms=BASE_TIME + 124 * INTERVAL_MS + 60_000,
            frozen_evidence=failed.state_record.evidence_json,
        )
        self.assertEqual(reset_result.reason, "N19_STRUCTURE_CONSUMED")
        self.assertIsNone(reset_result.structure)
        self.assertEqual(len(reset_result.state_records), 1)
        reset_record = reset_result.state_records[0]
        self.assertEqual(
            reset_record.reset_after_time_ms,
            int(strict_reset[-2][0]),
        )
        self.assertGreater(
            reset_record.reset_after_time_ms,
            reset_record.evidence["terminal_cutoff_time_ms"],
        )
        decoded_reset = decode_n19_state_evidence(
            reset_record.evidence_json,
            expected_symbol="P000USDT",
        )
        self.assertEqual(decoded_reset.evidence_sha256, reset_record.evidence_sha256)

        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-reset"),
            )
            self.assertEqual(recorder.record_n19_state(failed.state_record), "INSERTED")
            self.assertEqual(recorder.record_n19_state(reset_record), "UPDATED")
            latest = recorder.get_latest_n19_states({"P000USDT"})["P000USDT"]
            self.assertEqual(latest.reset_after_time_ms, reset_record.reset_after_time_ms)
            self.assertEqual(latest.evidence_sha256, reset_record.evidence_sha256)

        rolled = [
            _row(
                index,
                ("110", "110.2", "109.8", "110"),
                "100",
                "50",
            )
            for index in range(125, 247)
        ]
        restarted = analyze(
            rolled,
            checked_at_ms=BASE_TIME + 246 * INTERVAL_MS + 60_000,
            frozen_evidence=reset_record.evidence_json,
        )
        self.assertEqual(restarted.reason, "N19_NO_STAIRCASE_EXHAUSTION")

        for bad_reset in (
            reset_record.evidence["terminal_cutoff_time_ms"],
            reset_record.evidence["terminal_cutoff_time_ms"] - INTERVAL_MS,
        ):
            bad = dict(reset_record.evidence)
            bad["reset_after_time_ms"] = bad_reset
            unsigned = dict(bad)
            unsigned.pop("canonical_sha256")
            bad["canonical_sha256"] = n19_module._sha256_json(unsigned)
            with self.assertRaisesRegex(ValueError, "terminal cutoff"):
                decode_n19_state_evidence(bad)

    def test_all_terminal_stages_hold_across_overlap_and_gaps_until_reset_is_proven(self):
        confirmed = analyze().state_record
        cutoff = confirmed.evidence["structure"]["c"]["open_time_ms"]
        entry_cutoff = confirmed.evidence["structure"]["entry"]["open_time_ms"]
        terminal_pairs = (
            ("CONSUMED", "N19_CONFIRMATION_NOT_QUALIFIED"),
            ("MISSED", "N19_ENTRY_PRICE_ABOVE_MAX"),
            ("INVALID", "N19_ENTRY_BROKE_X_LOW"),
            ("EXPIRED", "N19_ENTRY_WINDOW_EXPIRED"),
        )

        def terminal_record(stage, reason):
            evidence = deepcopy(confirmed.evidence)
            evidence["stage"] = stage
            evidence["reason"] = reason
            if reason in {
                "N19_ENTRY_BROKE_X_LOW",
                "N19_ENTRY_PRICE_ABOVE_MAX",
                "N19_ENTRY_WINDOW_EXPIRED",
            }:
                evidence["terminal_cutoff_time_ms"] = entry_cutoff
            else:
                evidence["terminal_cutoff_time_ms"] = cutoff
            if reason == "N19_CONFIRMATION_NOT_QUALIFIED":
                evidence["structure"]["entry"] = None
            evidence["reset_after_time_ms"] = None
            unsigned = dict(evidence)
            unsigned.pop("canonical_sha256", None)
            evidence["canonical_sha256"] = n19_module._sha256_json(unsigned)
            record = replace(
                confirmed,
                stage=stage,
                reason=reason,
                reset_after_time_ms=None,
                evidence=evidence,
            )
            self.assertEqual(
                decode_n19_state_evidence(
                    record.evidence_json,
                    expected_symbol="P000USDT",
                ),
                record,
            )
            return record

        windows = {
            "overlap": n19_fixture(),
            "exactly_disconnected": [
                _row(
                    index,
                    ("106.4", "106.8", "106.2", "106.5"),
                    "100",
                    "55",
                )
                for index in range(122, 244)
            ],
            "fully_rolled": [
                _row(
                    index,
                    ("106.4", "106.8", "106.2", "106.5"),
                    "100",
                    "55",
                )
                for index in range(130, 252)
            ],
        }
        reset_rows = deepcopy(n19_fixture())
        reset_rows.extend(
            (
                _row(
                    122,
                    ("106.5", "107", "106.2", "106.8"),
                    "100",
                    "50",
                ),
                _row(
                    123,
                    ("106.8", "107.2", "106.5", "106.9"),
                    "100",
                    "50",
                ),
                _row(
                    124,
                    ("106.9", "107.1", "106.6", "107"),
                    "100",
                    "50",
                ),
            )
        )
        for stage, reason in terminal_pairs:
            prior = terminal_record(stage, reason)
            for label, rows in windows.items():
                with self.subTest(stage=stage, window=label):
                    result = analyze(
                        deepcopy(rows),
                        checked_at_ms=int(rows[-1][0]) + 60_000,
                        frozen_evidence=prior.evidence_json,
                    )
                    self.assertEqual(result.reason, "N19_STRUCTURE_CONSUMED")
                    self.assertEqual(result.state_record, prior)
                    self.assertEqual(
                        result.state_record.evidence_json,
                        prior.evidence_json,
                    )
            with self.subTest(stage=stage, window="provable_reset"):
                reset = analyze(
                    deepcopy(reset_rows),
                    checked_at_ms=BASE_TIME + 124 * INTERVAL_MS + 60_000,
                    frozen_evidence=prior.evidence_json,
                )
                self.assertEqual(reset.reason, "N19_STRUCTURE_CONSUMED")
                self.assertEqual(len(reset.state_records), 1)
                self.assertEqual(reset.state_records[0].stage, stage)
                self.assertEqual(reset.state_records[0].reason, reason)
                self.assertGreater(
                    reset.state_records[0].reset_after_time_ms,
                    cutoff,
                )
            with self.subTest(stage=stage, window="disconnected_new_family"):
                shifted = deepcopy(n19_fixture())
                for row in shifted:
                    row[0] += 200 * INTERVAL_MS
                    row[6] += 200 * INTERVAL_MS
                new_family = analyze(
                    shifted,
                    checked_at_ms=int(shifted[-1][0]) + 60_000,
                    frozen_evidence=prior.evidence_json,
                )
                self.assertTrue(new_family.passed, new_family.reason)
                self.assertEqual(len(new_family.state_records), 2)
                reset_record, new_record = new_family.state_records
                self.assertEqual(
                    (reset_record.stage, reset_record.reason),
                    (stage, reason),
                )
                self.assertEqual(
                    reset_record.evidence["reset_evidence"]["mode"],
                    "DISCONNECTED_WINDOW_V1",
                )
                self.assertGreater(
                    reset_record.reset_after_time_ms,
                    cutoff,
                )
                self.assertNotEqual(new_record.family_id, prior.family_id)
                self.assertGreater(
                    new_record.s_open_time_ms,
                    reset_record.reset_after_time_ms,
                )
            with self.subTest(stage=stage, window="adjacent_closed_reset"):
                adjacent = [
                    _row(
                        index,
                        (
                            "108.6",
                            "109.2",
                            "108.4",
                            "109",
                        )
                        if index == 122
                        else ("106.4", "106.8", "106.2", "106.5"),
                        "100",
                        "55",
                    )
                    for index in range(122, 244)
                ]
                adjacent_result = analyze(
                    adjacent,
                    checked_at_ms=int(adjacent[-1][0]) + 60_000,
                    frozen_evidence=prior.evidence_json,
                )
                self.assertEqual(len(adjacent_result.state_records), 1)
                adjacent_reset = adjacent_result.state_records[0]
                self.assertEqual(
                    adjacent_reset.reset_after_time_ms,
                    int(adjacent[0][0]),
                )
                self.assertEqual(
                    adjacent_reset.evidence["reset_evidence"]["source"][0][
                        "open_time_ms"
                    ],
                    int(adjacent[0][0]),
                )
                self.assertEqual(
                    decode_n19_state_evidence(
                        adjacent_reset.evidence_json,
                        expected_symbol="P000USDT",
                    ),
                    adjacent_reset,
                )
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "review.sqlite3"
                    recorder = make_test_recorder(
                        path,
                        logging.getLogger(
                            "n19-adjacent-reset-" + stage
                        ),
                    )
                    self.assertEqual(
                        recorder.record_n19_state(prior),
                        "INSERTED",
                    )
                    self.assertEqual(
                        recorder.record_n19_state(adjacent_reset),
                        "UPDATED",
                    )
                    reopened = make_test_recorder(
                        path,
                        logging.getLogger(
                            "n19-adjacent-reset-reopen-" + stage
                        ),
                    )
                    durable = reopened.get_latest_n19_states(
                        {"P000USDT"}
                    )["P000USDT"]
                    self.assertEqual(
                        durable.evidence_json,
                        adjacent_reset.evidence_json,
                    )
                for start in (123, 124, 130):
                    following = [
                        _row(
                            index,
                            ("106.4", "106.8", "106.2", "106.5"),
                            "100",
                            "55",
                        )
                        for index in range(start, start + 122)
                    ]
                    replay = analyze(
                        following,
                        checked_at_ms=int(following[-1][0]) + 60_000,
                        frozen_evidence=durable.evidence_json,
                    )
                    self.assertNotEqual(
                        replay.reason,
                        "N19_FROZEN_EVIDENCE_INVALID",
                    )
                    self.assertEqual(
                        durable.reset_after_time_ms,
                        int(adjacent[0][0]),
                    )
            with self.subTest(stage=stage, window="open_reset_not_accepted"):
                open_only = deepcopy(windows["fully_rolled"])
                open_only[-1][1:5] = ["108.6", "109.2", "108.5", "109"]
                held = analyze(
                    open_only,
                    checked_at_ms=int(open_only[-1][0]) + 60_000,
                    frozen_evidence=prior.evidence_json,
                )
                self.assertEqual(held.reason, "N19_STRUCTURE_CONSUMED")
                self.assertEqual(held.state_record, prior)


class N19RecorderSchedulerTests(unittest.TestCase):
    def test_dropped_active_state_obeys_bulk_global_cooldown(self):
        confirmed = analyze()
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-dropped-cooldown"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            self.assertEqual(
                recorder.record_n19_state(confirmed.state_record),
                "INSERTED",
            )
            self.assertTrue(
                recorder.set_symbol_cooldown(
                    "P000USDT",
                    datetime.now(timezone.utc) + timedelta(hours=4),
                    "TEST_GLOBAL_COOLDOWN",
                    None,
                )
            )
            scan_id = recorder.begin_scan(0, [], True)
            scheduler = StrategyScheduler(
                (N19_STRATEGY,),
                96,
                recorder,
                logging.getLogger("n19-dropped-cooldown"),
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
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": [], "negative_funding": []},
                    {"P000USDT": n19_fixture()},
                    BASE_TIME + 121 * INTERVAL_MS + 60_000,
                )
            self.assertEqual(result.signals, [])
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])
            self.assertEqual(bulk_read.call_count, 1)
            self.assertEqual(single_read.call_count, 0)
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

    def test_disconnected_terminal_reset_is_durable_and_restart_reproducible(self):
        confirmed = analyze()
        cutoff = confirmed.state_record.evidence["structure"]["c"][
            "open_time_ms"
        ]
        entry_cutoff = confirmed.state_record.evidence["structure"]["entry"][
            "open_time_ms"
        ]
        shifted = deepcopy(n19_fixture())
        for row in shifted:
            row[0] += 200 * INTERVAL_MS
            row[6] += 200 * INTERVAL_MS

        for stage, reason in (
            ("CONSUMED", "N19_CONFIRMATION_NOT_QUALIFIED"),
            ("MISSED", "N19_ENTRY_PRICE_ABOVE_MAX"),
            ("INVALID", "N19_ENTRY_BROKE_X_LOW"),
            ("EXPIRED", "N19_ENTRY_WINDOW_EXPIRED"),
        ):
            with self.subTest(stage=stage):
                evidence = deepcopy(confirmed.state_record.evidence)
                evidence["stage"] = stage
                evidence["reason"] = reason
                if reason in {
                    "N19_ENTRY_BROKE_X_LOW",
                    "N19_ENTRY_PRICE_ABOVE_MAX",
                    "N19_ENTRY_WINDOW_EXPIRED",
                }:
                    evidence["terminal_cutoff_time_ms"] = entry_cutoff
                else:
                    evidence["terminal_cutoff_time_ms"] = cutoff
                    evidence["structure"]["entry"] = None
                evidence["reset_after_time_ms"] = None
                unsigned = dict(evidence)
                unsigned.pop("canonical_sha256", None)
                evidence["canonical_sha256"] = n19_module._sha256_json(
                    unsigned
                )
                prior = replace(
                    confirmed.state_record,
                    stage=stage,
                    reason=reason,
                    reset_after_time_ms=None,
                    evidence=evidence,
                )
                result = analyze(
                    shifted,
                    checked_at_ms=int(shifted[-1][0]) + 60_000,
                    frozen_evidence=prior.evidence_json,
                )
                self.assertTrue(result.passed, result.reason)
                reset_record = result.state_records[0]
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "review.sqlite3"
                    recorder = make_test_recorder(
                        path,
                        logging.getLogger(
                            "n19-disconnected-reset-" + stage
                        ),
                    )
                    self.assertEqual(
                        recorder.record_n19_state(prior),
                        "INSERTED",
                    )
                    self.assertEqual(
                        recorder.record_n19_state(reset_record),
                        "UPDATED",
                    )
                    reopened = make_test_recorder(
                        path,
                        logging.getLogger(
                            "n19-disconnected-reset-reopen-" + stage
                        ),
                    )
                    durable = reopened.get_latest_n19_states(
                        {"P000USDT"}
                    )["P000USDT"]
                    self.assertEqual(
                        (
                            durable.stage,
                            durable.reason,
                            durable.family_id,
                            durable.structure_id,
                            durable.reset_after_time_ms,
                            durable.evidence_json,
                            durable.evidence_sha256,
                        ),
                        (
                            reset_record.stage,
                            reset_record.reason,
                            reset_record.family_id,
                            reset_record.structure_id,
                            reset_record.reset_after_time_ms,
                            reset_record.evidence_json,
                            reset_record.evidence_sha256,
                        ),
                    )
                    replay = analyze(
                        shifted,
                        checked_at_ms=int(shifted[-1][0]) + 60_001,
                        frozen_evidence=durable.evidence_json,
                    )
                    self.assertTrue(replay.passed, replay.reason)
                    self.assertEqual(
                        replay.state_records[-1].family_id,
                        result.state_records[-1].family_id,
                    )

    def test_historical_terminal_transition_is_time_signal_and_manifest_bound(self):
        rows = n19_fixture()
        symbols, incomplete_market = market_fixture(rows)
        incomplete_market.pop(symbols[-1])
        confirmed = analyze(
            rows,
            market_symbols=symbols,
            market_klines_by_symbol=incomplete_market,
        )
        terminal_record = _historical_missed_record_from_confirmed(
            confirmed.state_record
        )
        rolled = [
            _row(
                index,
                ("106.4", "106.8", "106.2", "106.5"),
                "100",
                "55",
            )
            for index in range(130, 252)
        ]
        terminal = analyze(
            rolled,
            checked_at_ms=int(rolled[-1][0]) + 60_000,
            frozen_evidence=confirmed.state_record.evidence_json,
        )
        self.assertEqual(terminal.state_record, terminal_record)
        base_proposal = StrategyScheduler._history_coverage_proposal(
            "N19", "P000USDT", rolled, 122
        )
        proposal = replace(
            base_proposal,
            n19_terminal_family_id=terminal_record.family_id,
            n19_terminal_structure_id=terminal_record.structure_id,
            n19_terminal_evidence_sha256=terminal_record.evidence_sha256,
            n19_terminal_state_record=terminal_record,
        )
        self.assertNotEqual(
            proposal,
            replace(
                proposal,
                n19_terminal_family_id=None,
                n19_terminal_structure_id=None,
                n19_terminal_evidence_sha256=None,
                n19_terminal_state_record=None,
            ),
        )
        self.assertIn(terminal_record.evidence_sha256, repr(proposal))

        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-terminal-publication-binding"),
            )
            self.assertEqual(
                recorder.record_n19_state(confirmed.state_record),
                "INSERTED",
            )
            not_historical = replace(
                proposal,
                source_start_time_ms=int(rows[0][0]),
                covered_through_time_ms=(
                    confirmed.state_record.evidence["structure"]["c"][
                        "open_time_ms"
                    ]
                ),
                current_open_time_ms=(
                    confirmed.state_record.evidence["structure"]["c"][
                        "open_time_ms"
                    ] + INTERVAL_MS
                ),
            )
            with self.assertRaisesRegex(RuntimeError, "not historical"):
                recorder.prepare_history_coverage_proposal(not_historical)
            self.assertIn(
                recorder.prepare_history_coverage_proposal(proposal),
                {"NEW", "GAP"},
            )

        def publication_case(
            name,
            mutate_signal,
            mutate_proposal=None,
            duplicate=False,
        ):
            with tempfile.TemporaryDirectory() as directory:
                recorder = make_test_recorder(
                    Path(directory) / "review.sqlite3",
                    logging.getLogger("n19-terminal-" + name),
                )
                self.assertEqual(
                    recorder.record_n19_state(confirmed.state_record),
                    "INSERTED",
                )
                scan_id = recorder.begin_scan(100, [], dry_run=True)
                detail = terminal.detail_json()
                signal = {
                    "symbol": "P000USDT",
                    "passed": False,
                    "decision": "REJECTED",
                    "reason": "N19_HISTORICAL_ENTRY_MISSED",
                    "structure_id": terminal_record.structure_id,
                    "detail": detail,
                }
                mutate_signal(signal)
                signal_id = None
                if signal["symbol"] is not None:
                    signal_id = recorder.record_strategy_signal(
                        scan_id,
                        "N19",
                        signal["symbol"],
                        "",
                        (),
                        "",
                        False,
                        signal["passed"],
                        signal["decision"],
                        signal["reason"],
                        signal["structure_id"],
                        signal["detail"],
                    )
                    self.assertIsInstance(signal_id, int)
                    if duplicate:
                        self.assertIsInstance(
                            recorder.record_strategy_signal(
                                scan_id,
                                "N19",
                                signal["symbol"],
                                "",
                                (),
                                "",
                                False,
                                signal["passed"],
                                signal["decision"],
                                signal["reason"],
                                signal["structure_id"],
                                signal["detail"],
                            ),
                            int,
                        )
                attempted = (
                    mutate_proposal(proposal)
                    if mutate_proposal is not None
                    else proposal
                )
                expected_count = (
                    0 if signal_id is None else (2 if duplicate else 1)
                )
                before = recorder.get_latest_n19_states(
                    {"P000USDT"}
                )["P000USDT"]
                self.assertFalse(
                    recorder.publish_strategy_signal_batch(
                        scan_id,
                        expected_count,
                        (attempted,),
                    )
                )
                after = recorder.get_latest_n19_states(
                    {"P000USDT"}
                )["P000USDT"]
                self.assertEqual(
                    (after.stage, after.reason, after.evidence_sha256),
                    (before.stage, before.reason, before.evidence_sha256),
                )
                with recorder._read_only_runtime_snapshot() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT current_scan_id FROM "
                            "strategy_signal_current WHERE singleton_id=1"
                        ).fetchone(),
                        (None,),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM "
                            "history_coverage_epoch_heads"
                        ).fetchone(),
                        (0,),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM "
                            "history_coverage_publication_receipts"
                        ).fetchone(),
                        (0,),
                    )

        cases = {
            "missing_signal": lambda signal: signal.update(symbol=None),
            "wrong_reason": lambda signal: signal.update(
                reason="N19_MARKET_CONTEXT_INSUFFICIENT"
            ),
            "passed": lambda signal: signal.update(
                passed=True, decision="PASSED"
            ),
            "wrong_structure": lambda signal: signal.update(
                structure_id="f" * 24
            ),
            "wrong_hash": lambda signal: signal["detail"].update(
                evidence_sha256="f" * 64
            ),
        }
        for name, mutation in cases.items():
            with self.subTest(case=name):
                publication_case(name, mutation)
        with self.subTest(case="omitted_transition"):
            publication_case(
                "omitted-transition",
                lambda signal: None,
                lambda item: replace(
                    item,
                    n19_terminal_family_id=None,
                    n19_terminal_structure_id=None,
                    n19_terminal_evidence_sha256=None,
                    n19_terminal_state_record=None,
                ),
            )
        with self.subTest(case="duplicate_signal"):
            publication_case(
                "duplicate-signal",
                lambda signal: None,
                duplicate=True,
            )
        with self.subTest(case="wrong_transition_family"):
            publication_case(
                "wrong-transition-family",
                lambda signal: None,
                lambda item: replace(
                    item,
                    n19_terminal_family_id="f" * 24,
                ),
            )
        with self.subTest(case="wrong_transition_digest"):
            publication_case(
                "wrong-transition-digest",
                lambda signal: None,
                lambda item: replace(
                    item,
                    n19_terminal_evidence_sha256="f" * 64,
                ),
            )

        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-terminal-idempotent"),
            )
            self.assertEqual(
                recorder.record_n19_state(confirmed.state_record),
                "INSERTED",
            )
            scan_id = recorder.begin_scan(100, [], dry_run=True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    scan_id,
                    "N19",
                    "P000USDT",
                    "",
                    (),
                    "",
                    False,
                    False,
                    "REJECTED",
                    "N19_HISTORICAL_ENTRY_MISSED",
                    terminal_record.structure_id,
                    terminal.detail_json(),
                ),
                int,
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    scan_id, 1, (proposal,)
                )
            )
            with recorder._read_only_runtime_snapshot() as connection:
                batch_manifest = connection.execute(
                    "SELECT manifest_sha256 FROM strategy_signal_batches "
                    "WHERE scan_id=? AND state='CURRENT'",
                    (scan_id,),
                ).fetchone()
                receipt_manifest = connection.execute(
                    "SELECT batch_manifest_sha256 FROM "
                    "history_coverage_publication_receipts "
                    "WHERE source_scan_id=? AND strategy_id='N19' "
                    "AND symbol='P000USDT'",
                    (scan_id,),
                ).fetchone()
                self.assertEqual(receipt_manifest, batch_manifest)
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    scan_id, 1, (proposal,)
                )
            )
            omitted = replace(
                proposal,
                n19_terminal_family_id=None,
                n19_terminal_structure_id=None,
                n19_terminal_evidence_sha256=None,
                n19_terminal_state_record=None,
            )
            self.assertFalse(
                recorder.publish_strategy_signal_batch(
                    scan_id, 1, (omitted,)
                )
            )

    def test_terminal_no_change_seals_one_permanent_publication_receipt(self):
        rows = n19_fixture()
        symbols, incomplete_market = market_fixture(rows)
        incomplete_market.pop(symbols[-1])
        confirmed = analyze(
            rows,
            market_symbols=symbols,
            market_klines_by_symbol=incomplete_market,
        )
        rolled = [
            _row(
                index,
                ("106.4", "106.8", "106.2", "106.5"),
                "100",
                "55",
            )
            for index in range(130, 252)
        ]
        base_proposal = StrategyScheduler._history_coverage_proposal(
            "N19",
            "P000USDT",
            rolled,
            122,
        )
        terminal = analyze(
            rolled,
            checked_at_ms=int(rolled[-1][0]) + 60_000,
            frozen_evidence=confirmed.state_record.evidence_json,
        )
        terminal_proposal = replace(
            base_proposal,
            n19_terminal_family_id=terminal.state_record.family_id,
            n19_terminal_structure_id=terminal.state_record.structure_id,
            n19_terminal_evidence_sha256=(
                terminal.state_record.evidence_sha256
            ),
            n19_terminal_state_record=terminal.state_record,
        )
        variant_source = deepcopy(terminal.state_record.evidence["source"])
        variant_source[-1]["quote_volume"] = "110"
        variant_source[-1]["taker_buy_quote_volume"] = "60"
        variant_terminal_record = _historical_missed_record_from_confirmed(
            confirmed.state_record,
            variant_source,
        )
        variant_terminal = replace(
            terminal,
            state_record=variant_terminal_record,
        )
        self.assertEqual(
            (
                variant_terminal_record.family_id,
                variant_terminal_record.structure_id,
            ),
            (
                terminal.state_record.family_id,
                terminal.state_record.structure_id,
            ),
        )
        self.assertNotEqual(
            variant_terminal_record.evidence_sha256,
            terminal.state_record.evidence_sha256,
        )
        self.assertEqual(
            decode_n19_state_evidence(
                variant_terminal_record.evidence_json,
                expected_symbol="P000USDT",
            ),
            variant_terminal_record,
        )
        variant_terminal_proposal = replace(
            base_proposal,
            n19_terminal_family_id=variant_terminal_record.family_id,
            n19_terminal_structure_id=variant_terminal_record.structure_id,
            n19_terminal_evidence_sha256=(
                variant_terminal_record.evidence_sha256
            ),
            n19_terminal_state_record=variant_terminal_record,
        )

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            ledger = Path(directory) / "claim.sqlite3"
            recorder = make_test_recorder(
                database,
                logging.getLogger("n19-terminal-no-change-receipt"),
                n16_claim_ledger_file=ledger,
            )
            first_analysis = analyze(
                rolled,
                checked_at_ms=int(rolled[-1][0]) + 60_000,
            )
            first_scan = recorder.begin_scan(100, [], dry_run=True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    first_scan,
                    "N19",
                    "P000USDT",
                    "",
                    (),
                    "",
                    False,
                    False,
                    "REJECTED",
                    first_analysis.reason,
                    first_analysis.structure_id,
                    first_analysis.detail_json(),
                ),
                int,
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    first_scan,
                    1,
                    (base_proposal,),
                )
            )
            self.assertEqual(
                recorder.record_n19_state(confirmed.state_record),
                "INSERTED",
            )

            terminal_scan = recorder.begin_scan(100, [], dry_run=True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    terminal_scan,
                    "N19",
                    "P000USDT",
                    "",
                    (),
                    "",
                    False,
                    False,
                    "REJECTED",
                    terminal.reason,
                    terminal.structure_id,
                    terminal.detail_json(),
                ),
                int,
            )
            self.assertEqual(
                recorder.prepare_history_coverage_proposal(
                    terminal_proposal
                ),
                "NO_CHANGE",
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    terminal_scan,
                    1,
                    (terminal_proposal,),
                )
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    terminal_scan,
                    1,
                    (terminal_proposal,),
                )
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT publication_count FROM "
                        "history_coverage_epoch_heads "
                        "WHERE strategy_id='N19' AND symbol='P000USDT'"
                    ).fetchone(),
                    (2,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM "
                        "history_coverage_publication_receipts "
                        "WHERE source_scan_id=?",
                        (terminal_scan,),
                    ).fetchone(),
                    (1,),
                )
                terminal_receipt = connection.execute(
                    "SELECT source_scan_id,symbol,family_id,structure_id,"
                    "terminal_evidence_json,terminal_evidence_sha256,"
                    "batch_expected_count,batch_manifest_sha256,"
                    "coverage_receipt_sha256,result_epoch_ordinal,"
                    "result_epoch_start_time_ms,result_chain_head_sha256,"
                    "receipt_sha256 "
                    "FROM history_coverage_n19_terminal_receipts "
                    "WHERE source_scan_id=?",
                    (terminal_scan,),
                ).fetchone()
                self.assertIsNotNone(terminal_receipt)
                self.assertEqual(
                    terminal_receipt[12],
                    n19_terminal_publication_receipt_sha256(
                        terminal_receipt[0],
                        terminal_receipt[1],
                        terminal_receipt[2],
                        terminal_receipt[3],
                        terminal_receipt[4],
                        terminal_receipt[5],
                        terminal_receipt[6],
                        terminal_receipt[7],
                        terminal_receipt[8],
                        terminal_receipt[9],
                        terminal_receipt[10],
                        terminal_receipt[11],
                    ),
                )
                coverage_receipt = connection.execute(
                    "SELECT terminal_binding_sha256,receipt_sha256 "
                    "FROM history_coverage_publication_receipts "
                    "WHERE source_scan_id=? AND strategy_id='N19' "
                    "AND symbol='P000USDT'",
                    (terminal_scan,),
                ).fetchone()
                self.assertEqual(coverage_receipt[1], terminal_receipt[8])
                self.assertIsNone(coverage_receipt[0])
                self.assertEqual(
                    connection.execute(
                        "SELECT terminal_binding_sha256 FROM "
                        "history_coverage_authorized_terminal_bindings "
                        "WHERE coverage_receipt_sha256=?",
                        (coverage_receipt[1],),
                    ).fetchone()[0],
                    n19_terminal_publication_binding_sha256(
                        terminal_receipt[0],
                        terminal_receipt[1],
                        terminal_receipt[2],
                        terminal_receipt[3],
                        terminal_receipt[4],
                        terminal_receipt[5],
                        terminal_receipt[6],
                        terminal_receipt[7],
                        terminal_receipt[9],
                        terminal_receipt[10],
                        terminal_receipt[11],
                    ),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT structure_id,terminal_evidence_sha256,"
                        "proof_domain,proof_sha256 FROM "
                        "history_coverage_n19_family_seals "
                        "WHERE strategy_id='N19' AND symbol='P000USDT' "
                        "AND family_id=?",
                        (terminal.state_record.family_id,),
                    ).fetchone(),
                    (
                        terminal.state_record.structure_id,
                        terminal.state_record.evidence_sha256,
                        "NORMAL_TERMINAL_RECEIPT",
                        terminal_receipt[12],
                    ),
                )

            def durable_snapshot():
                with recorder._read_only_runtime_snapshot() as connection:
                    return (
                        connection.execute(
                            "SELECT current_scan_id FROM "
                            "strategy_signal_current WHERE singleton_id=1"
                        ).fetchone(),
                        connection.execute(
                            "SELECT epoch_ordinal,covered_through_time_ms,"
                            "source_sha256,publication_count,"
                            "latest_receipt_sha256 FROM "
                            "history_coverage_epoch_heads "
                            "WHERE strategy_id='N19' "
                            "AND symbol='P000USDT'"
                        ).fetchone(),
                        connection.execute(
                            "SELECT stage,reason,evidence_sha256,updated_at "
                            "FROM n19_staircase_states "
                            "WHERE strategy_id='N19' AND family_id=?",
                            (terminal.state_record.family_id,),
                        ).fetchone(),
                        connection.execute(
                            "SELECT COUNT(*) FROM "
                            "history_coverage_publication_receipts"
                        ).fetchone(),
                        connection.execute(
                            "SELECT COUNT(*) FROM "
                            "history_coverage_n19_terminal_receipts"
                        ).fetchone(),
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_paper_trades"
                        ).fetchone(),
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_live_links"
                        ).fetchone(),
                        connection.execute(
                            "SELECT COUNT(*) FROM trade_reviews"
                        ).fetchone(),
                        connection.execute(
                            "SELECT COUNT(*) FROM events"
                        ).fetchone(),
                    )

            variant_replay_scan = recorder.begin_scan(
                100, [], dry_run=True
            )
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    variant_replay_scan,
                    "N19",
                    "P000USDT",
                    "",
                    (),
                    "",
                    False,
                    False,
                    "REJECTED",
                    variant_terminal_record.reason,
                    variant_terminal_record.structure_id,
                    variant_terminal.detail_json(),
                ),
                int,
            )
            self.assertEqual(
                recorder.prepare_history_coverage_proposal(
                    variant_terminal_proposal
                ),
                "INCONSISTENT",
            )
            before_variant_replay = durable_snapshot()
            self.assertFalse(
                recorder.publish_strategy_signal_batch(
                    variant_replay_scan,
                    1,
                    (variant_terminal_proposal,),
                )
            )
            self.assertEqual(
                durable_snapshot(),
                before_variant_replay,
            )

            replay_terminal_scan = recorder.begin_scan(
                100, [], dry_run=True
            )
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    replay_terminal_scan,
                    "N19",
                    "P000USDT",
                    "",
                    (),
                    "",
                    False,
                    False,
                    "REJECTED",
                    terminal.reason,
                    terminal.structure_id,
                    terminal.detail_json(),
                ),
                int,
            )
            self.assertEqual(
                recorder.prepare_history_coverage_proposal(
                    terminal_proposal
                ),
                "INCONSISTENT",
            )
            before_replay = durable_snapshot()
            self.assertFalse(
                recorder.publish_strategy_signal_batch(
                    replay_terminal_scan,
                    1,
                    (terminal_proposal,),
                )
            )
            self.assertEqual(durable_snapshot(), before_replay)
            del recorder
            recorder = ReviewRecorder(
                database,
                logging.getLogger("n19-terminal-restart-replay"),
                n16_claim_ledger_file=ledger,
            )
            self.assertFalse(
                recorder.publish_strategy_signal_batch(
                    replay_terminal_scan,
                    1,
                    (terminal_proposal,),
                )
            )
            self.assertEqual(durable_snapshot(), before_replay)

            replay = analyze(
                rolled,
                checked_at_ms=int(rolled[-1][0]) + 60_001,
                frozen_evidence=terminal.state_record.evidence_json,
            )
            next_scan = recorder.begin_scan(100, [], dry_run=True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    next_scan,
                    "N19",
                    "P000USDT",
                    "",
                    (),
                    "",
                    False,
                    False,
                    "REJECTED",
                    replay.reason,
                    replay.structure_id,
                    replay.detail_json(),
                ),
                int,
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    next_scan,
                    1,
                    (base_proposal,),
                )
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertIsNone(connection.execute(
                    "SELECT 1 FROM strategy_signal_batches WHERE scan_id=?",
                    (terminal_scan,),
                ).fetchone())
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*),MIN(batch_expected_count),"
                        "MAX(batch_expected_count) FROM "
                        "history_coverage_publication_receipts "
                        "WHERE source_scan_id=?",
                        (terminal_scan,),
                    ).fetchone(),
                    (1, 1, 1),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT publication_count FROM "
                        "history_coverage_epoch_heads "
                        "WHERE strategy_id='N19' AND symbol='P000USDT'"
                    ).fetchone(),
                    (2,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id FROM strategy_signal_current "
                        "WHERE singleton_id=1"
                    ).fetchone(),
                    (next_scan,),
                )
                self.assertIsNone(
                    connection.execute(
                        "SELECT 1 FROM strategy_signals WHERE scan_id=?",
                        (terminal_scan,),
                    ).fetchone()
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT family_id,structure_id,"
                        "terminal_evidence_sha256,batch_expected_count,"
                        "batch_manifest_sha256,coverage_receipt_sha256 "
                        "FROM history_coverage_n19_terminal_receipts "
                        "WHERE source_scan_id=?",
                        (terminal_scan,),
                    ).fetchone(),
                    (
                        terminal.state_record.family_id,
                        terminal.state_record.structure_id,
                        terminal.state_record.evidence_sha256,
                        1,
                        terminal_receipt[7],
                        terminal_receipt[8],
                    ),
                )
                validate_coverage_epoch_graph(connection)
            retained_replay_scan = recorder.begin_scan(
                100, [], dry_run=True
            )
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    retained_replay_scan,
                    "N19",
                    "P000USDT",
                    "",
                    (),
                    "",
                    False,
                    False,
                    "REJECTED",
                    terminal.reason,
                    terminal.structure_id,
                    terminal.detail_json(),
                ),
                int,
            )
            retained_before = durable_snapshot()
            self.assertFalse(
                recorder.publish_strategy_signal_batch(
                    retained_replay_scan,
                    1,
                    (terminal_proposal,),
                )
            )
            self.assertEqual(durable_snapshot(), retained_before)
            with recorder._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "DROP TRIGGER "
                    "trg_history_coverage_n19_terminal_insert_authorized"
                )
                connection.execute(
                    "DROP TRIGGER "
                    "trg_history_coverage_n19_terminal_no_replace"
                )
                connection.execute(
                    "DROP TRIGGER "
                    "trg_history_coverage_n19_terminal_requires_bundle"
                )
                connection.execute(
                    "DROP INDEX "
                    "idx_history_coverage_n19_terminal_identity"
                )
                forged_source_scan = terminal_scan + 100_000
                forged_coverage = "f" * 64
                forged_receipt = n19_terminal_publication_receipt_sha256(
                    forged_source_scan,
                    terminal_receipt[1],
                    terminal_receipt[2],
                    terminal_receipt[3],
                    variant_terminal_record.evidence_json,
                    variant_terminal_record.evidence_sha256,
                    terminal_receipt[6],
                    terminal_receipt[7],
                    forged_coverage,
                    terminal_receipt[9],
                    terminal_receipt[10],
                    terminal_receipt[11],
                )
                connection.execute(
                    "INSERT INTO history_coverage_n19_terminal_receipts "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        forged_source_scan,
                        "N19",
                        terminal_receipt[1],
                        terminal_receipt[2],
                        terminal_receipt[3],
                        variant_terminal_record.evidence_json,
                        variant_terminal_record.evidence_sha256,
                        terminal_receipt[6],
                        terminal_receipt[7],
                        forged_coverage,
                        terminal_receipt[9],
                        terminal_receipt[10],
                        terminal_receipt[11],
                        forged_receipt,
                        "2026-07-26T00:00:00+00:00",
                    ),
                )
                with self.assertRaisesRegex(
                    RuntimeError,
                    "terminal publication identity is duplicated",
                ):
                    validate_coverage_epoch_graph(connection)
                connection.rollback()
            with recorder._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "DROP TRIGGER "
                    "trg_history_coverage_n19_terminal_no_delete"
                )
                connection.execute(
                    "DELETE FROM history_coverage_n19_terminal_receipts "
                    "WHERE source_scan_id=?",
                    (terminal_scan,),
                )
                connection.execute(
                    COVERAGE_TRIGGER_SQL[
                        "trg_history_coverage_n19_terminal_no_delete"
                    ]
                )
                connection.commit()
            with recorder._read_only_runtime_snapshot() as connection:
                with self.assertRaisesRegex(
                    RuntimeError,
                    "terminal publication binding is incomplete|"
                    "historical terminal state has no unique permanent proof",
                ):
                    validate_coverage_epoch_graph(connection)

    def test_historical_terminal_sql_guard_requires_complete_publication_set(
        self,
    ):
        rows = n19_fixture()
        symbols, incomplete_market = market_fixture(rows)
        incomplete_market.pop(symbols[-1])
        confirmed = analyze(
            rows,
            market_symbols=symbols,
            market_klines_by_symbol=incomplete_market,
        )
        rolled = [
            _row(
                index,
                ("106.4", "106.8", "106.2", "106.5"),
                "100",
                "55",
            )
            for index in range(130, 252)
        ]
        terminal = analyze(
            rolled,
            checked_at_ms=int(rolled[-1][0]) + 60_000,
            frozen_evidence=confirmed.state_record.evidence_json,
        )
        self.assertEqual(
            (terminal.state_record.stage, terminal.reason),
            ("MISSED", "N19_HISTORICAL_ENTRY_MISSED"),
        )
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database,
                logging.getLogger("n19-terminal-sql-four-piece"),
            )
            self.assertEqual(
                recorder.record_n19_state(confirmed.state_record),
                "INSERTED",
            )
            source_scan_id = 999
            coverage_receipt_sha = "c" * 64
            batch_manifest_sha = "b" * 64
            chain_sha = "d" * 64
            terminal_receipt_sha = (
                n19_terminal_publication_receipt_sha256(
                    source_scan_id,
                    terminal.symbol,
                    terminal.state_record.family_id,
                    terminal.state_record.structure_id,
                    terminal.state_record.evidence_json,
                    terminal.state_record.evidence_sha256,
                    1,
                    batch_manifest_sha,
                    coverage_receipt_sha,
                    1,
                    int(rolled[0][0]),
                    chain_sha,
                )
            )
            with closing(sqlite3.connect(database)) as external:
                external.create_function(
                    "_coverage_epoch_mutation_authorized",
                    6,
                    lambda *_args: 1,
                )
                external.execute("BEGIN")
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError,
                    "N19 terminal receipt requires one atomic bundle",
                ):
                    external.execute(
                        "INSERT INTO "
                        "history_coverage_n19_terminal_receipts "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            source_scan_id,
                            "N19",
                            terminal.symbol,
                            terminal.state_record.family_id,
                            terminal.state_record.structure_id,
                            terminal.state_record.evidence_json,
                            terminal.state_record.evidence_sha256,
                            1,
                            batch_manifest_sha,
                            coverage_receipt_sha,
                            1,
                            int(rolled[0][0]),
                            chain_sha,
                            terminal_receipt_sha,
                            "2099-01-01T00:00:00+00:00",
                        ),
                    )
                # SQLite ABORT rolls back only the failed statement.  A caller
                # that catches it and commits must still leave no terminal
                # receipt, seal, overlay, bundle, or state transition.
                external.commit()
                terminal_binding_sha = (
                    n19_terminal_publication_binding_sha256(
                        source_scan_id,
                        terminal.symbol,
                        terminal.state_record.family_id,
                        terminal.state_record.structure_id,
                        terminal.state_record.evidence_json,
                        terminal.state_record.evidence_sha256,
                        1,
                        batch_manifest_sha,
                        1,
                        int(rolled[0][0]),
                        chain_sha,
                    )
                )
                external.execute("BEGIN")
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError,
                    "terminal-bound coverage receipt requires one atomic bundle",
                ):
                    external.execute(
                        "INSERT INTO history_coverage_publication_receipts "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            source_scan_id,
                            "N19",
                            terminal.symbol,
                            int(rolled[0][0]),
                            int(rolled[-1][0]),
                            "a" * 64,
                            1,
                            int(rolled[0][0]),
                            chain_sha,
                            1,
                            None,
                            1,
                            batch_manifest_sha,
                            terminal_binding_sha,
                            coverage_receipt_sha,
                            "2099-01-01T00:00:00+00:00",
                        ),
                    )
                external.commit()
                seal_sha = family_seal_sha256(
                    terminal.symbol,
                    terminal.state_record.family_id,
                    terminal.state_record.structure_id,
                    terminal.state_record.evidence_sha256,
                    PROVED_TERMINAL_DOMAIN,
                    terminal_receipt_sha,
                )
                bundle_sha = terminal_bundle_sha256(
                    source_scan_id,
                    terminal.symbol,
                    terminal.state_record.family_id,
                    terminal.state_record.structure_id,
                    "9" * 64,
                    terminal.state_record.evidence_json,
                    terminal.state_record.evidence_sha256,
                    1,
                    batch_manifest_sha,
                    int(rolled[0][0]),
                    int(rolled[-1][0]),
                    "a" * 64,
                    1,
                    None,
                    coverage_receipt_sha,
                    1,
                    int(rolled[0][0]),
                    chain_sha,
                    terminal_receipt_sha,
                    terminal_binding_sha,
                    seal_sha,
                )
                external.execute("BEGIN")
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError,
                    "state transition conflicted",
                ):
                    external.execute(
                        "INSERT INTO history_coverage_n19_terminal_bundles "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            source_scan_id,
                            "N19",
                            terminal.symbol,
                            terminal.state_record.family_id,
                            terminal.state_record.structure_id,
                            "9" * 64,
                            terminal.state_record.evidence_json,
                            terminal.state_record.evidence_sha256,
                            1,
                            batch_manifest_sha,
                            int(rolled[0][0]),
                            int(rolled[-1][0]),
                            "a" * 64,
                            1,
                            None,
                            coverage_receipt_sha,
                            1,
                            int(rolled[0][0]),
                            chain_sha,
                            terminal_receipt_sha,
                            terminal_binding_sha,
                            seal_sha,
                            bundle_sha,
                            "2099-01-01T00:00:00+00:00",
                        ),
                    )
                external.commit()
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT stage,reason,evidence_sha256 FROM "
                        "n19_staircase_states WHERE family_id=?",
                        (confirmed.state_record.family_id,),
                    ).fetchone(),
                    (
                        confirmed.state_record.stage,
                        confirmed.state_record.reason,
                        confirmed.state_record.evidence_sha256,
                    ),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT "
                        "(SELECT COUNT(*) FROM "
                        "history_coverage_n19_terminal_bundles),"
                        "(SELECT COUNT(*) FROM "
                        "history_coverage_n19_terminal_receipts),"
                        "(SELECT COUNT(*) FROM "
                        "history_coverage_n19_family_seals),"
                        "(SELECT COUNT(*) FROM "
                        "history_coverage_authorized_terminal_bindings),"
                        "(SELECT COUNT(*) FROM "
                        "history_coverage_publication_receipts)"
                    ).fetchone(),
                    (0, 0, 0, 0, 0),
                )
                validate_coverage_epoch_graph(connection)

    def test_legacy_confirmed_context_gap_terminalizes_without_rewriting_source(self):
        rows = n19_fixture()
        symbols, incomplete_market = market_fixture(rows)
        incomplete_market.pop(symbols[-1])
        legacy = analyze(
            rows,
            market_symbols=symbols,
            market_klines_by_symbol=incomplete_market,
        )
        self.assertEqual(
            (legacy.state_record.stage, legacy.reason),
            ("CONFIRMED", "N19_MARKET_CONTEXT_INSUFFICIENT"),
        )
        decoded_legacy = decode_n19_state_evidence(
            legacy.state_record.evidence_json,
            expected_symbol="P000USDT",
        )
        self.assertEqual(decoded_legacy.evidence_sha256, legacy.state_record.evidence_sha256)

        rolled = [
            _row(
                index,
                ("106.4", "106.8", "106.2", "106.5"),
                "100",
                "55",
            )
            for index in range(130, 252)
        ]
        current_symbols, current_market = market_fixture(rolled)
        terminal = analyze(
            rolled,
            market_symbols=current_symbols,
            market_klines_by_symbol=current_market,
            checked_at_ms=BASE_TIME + 251 * INTERVAL_MS + 60_000,
            frozen_evidence=legacy.state_record.evidence_json,
        )
        self.assertEqual(
            (terminal.state_record.stage, terminal.reason),
            ("MISSED", "N19_HISTORICAL_ENTRY_MISSED"),
        )
        self.assertEqual(
            terminal.state_record.evidence["source"],
            legacy.state_record.evidence["source"],
        )
        self.assertEqual(
            terminal.state_record.evidence["terminal_cutoff_time_ms"],
            legacy.state_record.evidence["structure"]["c"]["open_time_ms"],
        )
        self.assertEqual(
            decode_n19_state_evidence(
                terminal.state_record.evidence_json,
                expected_symbol="P000USDT",
            ),
            terminal.state_record,
        )

    def test_legacy_confirmed_gap_recovers_through_real_scheduler_publication(self):
        rows = n19_fixture()
        symbols, incomplete_market = market_fixture(rows)
        incomplete_market.pop(symbols[-1])
        legacy = analyze(
            rows,
            market_symbols=symbols,
            market_klines_by_symbol=incomplete_market,
        )
        rolled = [
            _row(
                index,
                ("106.4", "106.8", "106.2", "106.5"),
                "100",
                "55",
            )
            for index in range(130, 252)
        ]
        candidates = [
            FundingCandidate(
                "P%03dUSDT" % index,
                None,
                Decimal("106.5"),
                quote_volume=Decimal("1000000"),
                quote_volume_rank=index + 1,
                candidate_universe="quote_volume_top",
            )
            for index in range(100)
        ]
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-legacy-gap-scheduler"),
            )
            recorder.upsert_strategy_definitions((N19_STRATEGY,))
            self.assertEqual(
                recorder.record_n19_state(legacy.state_record),
                "INSERTED",
            )
            scan_id = recorder.begin_scan(100, candidates, dry_run=True)
            result = StrategyScheduler(
                (N19_STRATEGY,),
                96,
                recorder,
                logging.getLogger("n19-legacy-gap-scheduler"),
            ).evaluate(
                scan_id,
                {"quote_volume_top": candidates, "negative_funding": []},
                {item.symbol: deepcopy(rolled) for item in candidates},
                BASE_TIME + 251 * INTERVAL_MS + 60_000,
            )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(result.passed_signals, [])
            recovered = next(
                item for item in result.signals
                if item.candidate.symbol == "P000USDT"
            )
            self.assertEqual(recovered.reason, "N19_HISTORICAL_ENTRY_MISSED")
            latest = recorder.get_latest_n19_states({"P000USDT"})["P000USDT"]
            self.assertEqual(
                (latest.stage, latest.reason),
                ("MISSED", "N19_HISTORICAL_ENTRY_MISSED"),
            )
            self.assertEqual(recorder.get_active_n19_states(), [])
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state,recorded_count,expected_count "
                        "FROM strategy_signal_batches WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    ("CURRENT", 100, 100),
                )

    def test_existing_coverage_gap_terminal_transition_is_atomic_and_replay_safe(self):
        rows = n19_fixture()
        symbols, incomplete_market = market_fixture(rows)
        incomplete_market.pop(symbols[-1])
        legacy = analyze(
            rows,
            market_symbols=symbols,
            market_klines_by_symbol=incomplete_market,
        )
        self.assertEqual(
            (legacy.state_record.stage, legacy.reason),
            ("CONFIRMED", "N19_MARKET_CONTEXT_INSUFFICIENT"),
        )
        rolled = [
            _row(
                index,
                ("106.4", "106.8", "106.2", "106.5"),
                "100",
                "55",
            )
            for index in range(130, 252)
        ]
        candidates = [
            FundingCandidate(
                "P%03dUSDT" % index,
                None,
                Decimal("106.5"),
                quote_volume=Decimal("1000000"),
                quote_volume_rank=index + 1,
                candidate_universe="quote_volume_top",
            )
            for index in range(100)
        ]
        raw = {item.symbol: deepcopy(rolled) for item in candidates}
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-existing-coverage-gap"),
            )
            recorder.upsert_strategy_definitions((N19_STRATEGY,))
            self.assertEqual(
                recorder.record_n19_state(legacy.state_record),
                "INSERTED",
            )
            old_scan = recorder.begin_scan(100, candidates, dry_run=True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    old_scan,
                    "N19",
                    "P000USDT",
                    "",
                    (),
                    "",
                    False,
                    True,
                    legacy.reason,
                    legacy.reason,
                    legacy.structure_id,
                    legacy.detail_json(),
                ),
                int,
            )
            old_proposal = StrategyScheduler._history_coverage_proposal(
                "N19",
                "P000USDT",
                rows,
                122,
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    old_scan,
                    1,
                    (old_proposal,),
                )
            )
            with recorder._read_only_runtime_snapshot() as connection:
                old_head = connection.execute(
                    "SELECT epoch_ordinal,epoch_start_time_ms,"
                    "covered_through_time_ms,source_sha256,"
                    "publication_count,latest_receipt_sha256 "
                    "FROM history_coverage_epoch_heads "
                    "WHERE strategy_id='N19' AND symbol='P000USDT'"
                ).fetchone()
            scheduler = StrategyScheduler(
                (N19_STRATEGY,),
                96,
                recorder,
                logging.getLogger("n19-existing-coverage-gap"),
            )

            # Inject after the terminal UPDATE has executed.  The surrounding
            # publication transaction must roll it back together with the
            # GAP epoch and CURRENT pointer.
            failed_scan = recorder.begin_scan(100, candidates, dry_run=True)
            real_apply = recorder._apply_n19_coverage_terminal_transition

            def fail_after_terminal_write(connection, proposal, now):
                outcome = real_apply(connection, proposal, now)
                if proposal.symbol == "P000USDT":
                    raise RuntimeError("injected post-terminal failure")
                return outcome

            cloned_terminal_records = []

            def analyze_with_equal_distinct_terminal(*args, **kwargs):
                analysis = (
                    n19_module.analyze_n19_staircase_exhaustion_reversal(
                        *args,
                        **kwargs,
                    )
                )
                if (
                    analysis.state_record is not None
                    and analysis.state_record.reason
                    == "N19_HISTORICAL_ENTRY_MISSED"
                ):
                    clone = replace(
                        analysis.state_record,
                        evidence=deepcopy(analysis.state_record.evidence),
                    )
                    self.assertIsNot(clone, analysis.state_record)
                    self.assertEqual(clone, analysis.state_record)
                    cloned_terminal_records.append(clone)
                    return replace(analysis, state_records=(clone,))
                return analysis

            with patch.object(
                recorder,
                "_apply_n19_coverage_terminal_transition",
                side_effect=fail_after_terminal_write,
            ), patch(
                "trading_bot.strategy_scheduler."
                "analyze_n19_staircase_exhaustion_reversal",
                side_effect=analyze_with_equal_distinct_terminal,
            ):
                failed = scheduler.evaluate(
                    failed_scan,
                    {
                        "quote_volume_top": candidates,
                        "negative_funding": [],
                    },
                    raw,
                    BASE_TIME + 251 * INTERVAL_MS + 60_000,
                )
            self.assertEqual(len(cloned_terminal_records), 1)
            self.assertFalse(failed.signal_batch_published)
            self.assertEqual(failed.passed_signals, [])
            self.assertEqual(failed.live_candidates, [])
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT stage,reason,evidence_sha256 FROM "
                        "n19_staircase_states WHERE family_id=?",
                        (legacy.state_record.family_id,),
                    ).fetchone(),
                    (
                        "CONFIRMED",
                        "N19_MARKET_CONTEXT_INSUFFICIENT",
                        legacy.state_record.evidence_sha256,
                    ),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT epoch_ordinal,epoch_start_time_ms,"
                        "covered_through_time_ms,source_sha256,"
                        "publication_count,latest_receipt_sha256 "
                        "FROM history_coverage_epoch_heads "
                        "WHERE strategy_id='N19' AND symbol='P000USDT'"
                    ).fetchone(),
                    old_head,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id FROM strategy_signal_current "
                        "WHERE singleton_id=1"
                    ).fetchone(),
                    (old_scan,),
                )

            stable = None
            current_scans = []
            for offset in range(3):
                scan_id = recorder.begin_scan(100, candidates, dry_run=True)
                result = scheduler.evaluate(
                    scan_id,
                    {
                        "quote_volume_top": candidates,
                        "negative_funding": [],
                    },
                    raw,
                    BASE_TIME + 251 * INTERVAL_MS + 70_000 + offset,
                )
                self.assertTrue(result.signal_batch_published)
                self.assertEqual(result.passed_signals, [])
                self.assertEqual(result.live_candidates, [])
                decision = next(
                    item
                    for item in result.signals
                    if item.candidate.symbol == "P000USDT"
                )
                self.assertEqual(
                    decision.reason,
                    (
                        "N19_HISTORICAL_ENTRY_MISSED"
                        if offset == 0
                        else "N19_STRUCTURE_CONSUMED"
                    ),
                )
                latest = recorder.get_latest_n19_states(
                    {"P000USDT"}
                )["P000USDT"]
                if stable is None:
                    stable = (
                        latest.stage,
                        latest.reason,
                        latest.evidence_sha256,
                        latest.updated_at,
                    )
                else:
                    self.assertEqual(
                        (
                            latest.stage,
                            latest.reason,
                            latest.evidence_sha256,
                            latest.updated_at,
                        ),
                        stable,
                    )
                current_scans.append(scan_id)
            self.assertEqual(
                stable[:3],
                (
                    "MISSED",
                    "N19_HISTORICAL_ENTRY_MISSED",
                    _historical_missed_record_from_confirmed(
                        legacy.state_record
                    ).evidence_sha256,
                ),
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id FROM strategy_signal_current "
                        "WHERE singleton_id=1"
                    ).fetchone(),
                    (current_scans[-1],),
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
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM trade_reviews"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM events"
                    ).fetchone(),
                    (0,),
                )

    def test_terminal_publication_faults_rollback_real_sqlite_boundaries(self):
        rows = n19_fixture()
        symbols, incomplete_market = market_fixture(rows)
        incomplete_market.pop(symbols[-1])
        legacy = analyze(
            rows,
            market_symbols=symbols,
            market_klines_by_symbol=incomplete_market,
        )
        rolled = [
            _row(
                index,
                ("106.4", "106.8", "106.2", "106.5"),
                "100",
                "55",
            )
            for index in range(130, 252)
        ]
        candidates = [
            FundingCandidate(
                "P%03dUSDT" % index,
                None,
                Decimal("106.5"),
                quote_volume=Decimal("1000000"),
                quote_volume_rank=index + 1,
                candidate_universe="quote_volume_top",
            )
            for index in range(100)
        ]
        raw = {item.symbol: deepcopy(rolled) for item in candidates}
        triggers = {
            "chain": (
                "CREATE TEMP TRIGGER fail_n19_chain "
                "BEFORE INSERT ON history_coverage_epoch_chain "
                "BEGIN SELECT RAISE(ABORT,'injected chain failure'); END"
            ),
            "receipt": (
                "CREATE TEMP TRIGGER fail_n19_receipt "
                "BEFORE INSERT ON history_coverage_publication_receipts "
                "BEGIN SELECT RAISE(ABORT,'injected receipt failure'); END"
            ),
            "current": (
                "CREATE TEMP TRIGGER fail_n19_current "
                "BEFORE UPDATE OF current_scan_id "
                "ON strategy_signal_current "
                "WHEN NEW.current_scan_id IS NOT OLD.current_scan_id "
                "BEGIN SELECT RAISE(ABORT,'injected current failure'); END"
            ),
        }

        for name, trigger_sql in triggers.items():
            with self.subTest(boundary=name), tempfile.TemporaryDirectory() as directory:
                recorder = make_test_recorder(
                    Path(directory) / "review.sqlite3",
                    logging.getLogger("n19-real-sqlite-fault-" + name),
                )
                recorder.upsert_strategy_definitions((N19_STRATEGY,))
                self.assertEqual(
                    recorder.record_n19_state(legacy.state_record),
                    "INSERTED",
                )
                scan_id = recorder.begin_scan(
                    100,
                    candidates,
                    dry_run=True,
                )
                with recorder._read_only_runtime_snapshot() as connection:
                    before_state = connection.execute(
                        "SELECT stage,reason,evidence_json,evidence_sha256,"
                        "updated_at FROM n19_staircase_states"
                    ).fetchall()
                    before_permanent = tuple(
                        connection.execute(
                            f"SELECT * FROM {table} ORDER BY 1,2,3"
                        ).fetchall()
                        for table in (
                            "n19_history_coverage",
                            "history_coverage_epoch_chain",
                            "history_coverage_epoch_heads",
                            "history_coverage_publication_receipts",
                            "strategy_paper_trades",
                            "strategy_live_links",
                            "trade_reviews",
                            "events",
                        )
                    )

                real_connect = recorder._connect
                injected_connection_ids: set[int] = set()

                @contextmanager
                def injected_connect():
                    with real_connect() as connection:
                        # A scheduler round now reuses one identity-attested
                        # RW connection while retaining short transaction
                        # boundaries.  Install the connection-local fault once
                        # so the target write fails rather than a later nested
                        # scope failing on duplicate TEMP trigger creation.
                        if id(connection) not in injected_connection_ids:
                            connection.execute(trigger_sql)
                            injected_connection_ids.add(id(connection))
                        yield connection

                with patch.object(recorder, "_connect", injected_connect):
                    result = StrategyScheduler(
                        (N19_STRATEGY,),
                        96,
                        recorder,
                        logging.getLogger(
                            "n19-real-sqlite-fault-scheduler-" + name
                        ),
                    ).evaluate(
                        scan_id,
                        {
                            "quote_volume_top": candidates,
                            "negative_funding": [],
                        },
                        raw,
                        BASE_TIME + 251 * INTERVAL_MS + 60_000,
                    )
                self.assertFalse(result.signal_batch_published)
                self.assertEqual(result.passed_signals, [])
                self.assertEqual(result.live_candidates, [])
                with recorder._read_only_runtime_snapshot() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT stage,reason,evidence_json,evidence_sha256,"
                            "updated_at FROM n19_staircase_states"
                        ).fetchall(),
                        before_state,
                    )
                    after_permanent = tuple(
                        connection.execute(
                            f"SELECT * FROM {table} ORDER BY 1,2,3"
                        ).fetchall()
                        for table in (
                            "n19_history_coverage",
                            "history_coverage_epoch_chain",
                            "history_coverage_epoch_heads",
                            "history_coverage_publication_receipts",
                            "strategy_paper_trades",
                            "strategy_live_links",
                            "trade_reviews",
                            "events",
                        )
                    )
                    self.assertEqual(after_permanent, before_permanent)
                    self.assertEqual(
                        connection.execute(
                            "SELECT state,recorded_count,expected_count "
                            "FROM strategy_signal_batches WHERE scan_id=?",
                            (scan_id,),
                        ).fetchone(),
                        ("STAGING", 100, None),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT current_scan_id FROM "
                            "strategy_signal_current WHERE singleton_id=1"
                        ).fetchone(),
                        (None,),
                    )

    def test_frozen_evidence_integer_types_are_exact(self):
        result = analyze()
        self.assertTrue(result.passed)

        cases = (
            (("schema_version",), True),
            (("schema_version",), 1.0),
            (("quote_volume_rank",), True),
            (("quote_volume_rank",), 1.0),
            (("quote_volume_rank",), 0),
            (("quote_volume_rank",), 101),
            (("quote_volume_rank",), -1),
            (("source", -1, "open_time_ms"), True),
            (("source", -1, "open_time_ms"), float(BASE_TIME)),
            (("structure", "c", "open_time_ms"), True),
            (("market_context", "c_open_time_ms"), 1.0),
            (("market_context", "member_count"), True),
            (("market_context", "complete"), 1),
            (("reset_after_time_ms",), True),
            (("terminal_cutoff_time_ms",), 1.0),
            (("stage",), True),
            (("reason",), 1),
        )

        def mutate(path, value):
            evidence = deepcopy(result.state_record.evidence)
            target = evidence
            for component in path[:-1]:
                target = target[component]
            target[path[-1]] = value
            unsigned = dict(evidence)
            unsigned.pop("canonical_sha256", None)
            evidence["canonical_sha256"] = n19_module._sha256_json(unsigned)
            return evidence

        for path, value in cases:
            with self.subTest(path=path, value=value):
                evidence = mutate(path, value)
                with self.assertRaises(ValueError):
                    decode_n19_state_evidence(evidence, expected_symbol="P000USDT")
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-exact-evidence-types"),
            )
            record = replace(
                result.state_record,
                evidence=mutate(("quote_volume_rank",), True),
            )
            self.assertEqual(
                recorder.record_n19_state(record), "N19_STATE_INCONSISTENT"
            )

    def test_structure_points_are_exactly_bound_to_frozen_source(self):
        valid = analyze(
            checked_at_ms=BASE_TIME + 121 * INTERVAL_MS + 120_000
        ).state_record
        evidence = deepcopy(valid.evidence)
        self.assertEqual(decode_n19_state_evidence(valid.evidence_json), valid)

        forged = deepcopy(evidence)
        forged["structure"]["r2"]["high"] = "106.6"
        unsigned = dict(forged)
        unsigned.pop("canonical_sha256", None)
        forged["canonical_sha256"] = n19_module._sha256_json(unsigned)
        with self.assertRaisesRegex(ValueError, "point conflicts"):
            decode_n19_state_evidence(forged)

        rolled = [
            _row(
                index,
                (
                    "106.5",
                    "106.9",
                    "106.4",
                    "106.7" if index == 130 else "106.5",
                ),
                "100",
                "55",
            )
            for index in range(130, 252)
        ]
        real = analyze(
            rolled,
            checked_at_ms=int(rolled[-1][0]) + 60_000,
            frozen_evidence=valid.evidence_json,
        )
        self.assertIsNone(real.state_record.reset_after_time_ms)
        forged_record = replace(valid, evidence=forged)
        attacked = analyze(
            rolled,
            checked_at_ms=int(rolled[-1][0]) + 60_000,
            frozen_evidence=forged_record.evidence_json,
        )
        self.assertEqual(attacked.reason, "N19_FROZEN_EVIDENCE_INVALID")

    def test_frozen_evidence_shape_identity_and_hash_attacks_are_zero_write(self):
        result = analyze()
        self.assertTrue(result.passed)

        def rehash(evidence):
            unsigned = dict(evidence)
            unsigned.pop("canonical_sha256", None)
            evidence["canonical_sha256"] = n19_module._sha256_json(unsigned)
            return evidence

        attacks = {}
        attacks["hash"] = deepcopy(result.state_record.evidence)
        attacks["hash"]["canonical_sha256"] = "0" * 64
        attacks["bool_rank"] = rehash(deepcopy(result.state_record.evidence))
        attacks["bool_rank"]["quote_volume_rank"] = True
        attacks["bool_rank"] = rehash(attacks["bool_rank"])
        attacks["float_rank"] = deepcopy(result.state_record.evidence)
        attacks["float_rank"]["quote_volume_rank"] = 7.0
        attacks["float_rank"] = rehash(attacks["float_rank"])
        attacks["missing"] = deepcopy(result.state_record.evidence)
        attacks["missing"].pop("market_context")
        attacks["missing"] = rehash(attacks["missing"])
        attacks["extra"] = deepcopy(result.state_record.evidence)
        attacks["extra"]["unexpected"] = "value"
        attacks["extra"] = rehash(attacks["extra"])
        attacks["structure"] = deepcopy(result.state_record.evidence)
        attacks["structure"]["structure"]["s"]["open_time_ms"] += INTERVAL_MS
        attacks["structure"] = rehash(attacks["structure"])
        attacks["symbol"] = deepcopy(result.state_record.evidence)
        attacks["symbol"]["symbol"] = "P001USDT"
        attacks["symbol"] = rehash(attacks["symbol"])
        attacks["time"] = deepcopy(result.state_record.evidence)
        attacks["time"]["source"][0]["open_time_ms"] += 1
        attacks["time"] = rehash(attacks["time"])

        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-evidence-attack-zero-write"),
            )
            for name, evidence in attacks.items():
                with self.subTest(name=name):
                    with self.assertRaises(ValueError):
                        decode_n19_state_evidence(
                            evidence,
                            expected_symbol="P000USDT",
                        )
                    attacked = replace(result.state_record, evidence=evidence)
                    self.assertEqual(
                        recorder.record_n19_state(attacked),
                        "N19_STATE_INCONSISTENT",
                    )
                    with recorder._read_only_runtime_snapshot() as connection:
                        self.assertEqual(
                            connection.execute(
                                "SELECT COUNT(*) FROM n19_staircase_states"
                            ).fetchone()[0],
                            0,
                        )

    def test_terminal_stage_reason_pairs_are_canonical_at_decode_write_and_claim(self):
        result = analyze()
        self.assertTrue(result.passed)
        c_cutoff_time = result.structure.c.open_time_ms
        entry_cutoff_time = result.structure.entry.open_time_ms
        legal = {
            "MISSED": {
                "N19_HISTORICAL_ENTRY_MISSED",
                "N19_ENTRY_PRICE_ABOVE_MAX",
            },
            "INVALID": {"N19_ENTRY_BROKE_X_LOW"},
            "EXPIRED": {"N19_ENTRY_WINDOW_EXPIRED"},
        }
        terminal_stages = {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}

        def mutated(stage, reason):
            evidence = deepcopy(result.state_record.evidence)
            evidence["stage"] = stage
            evidence["reason"] = reason
            evidence["terminal_cutoff_time_ms"] = (
                entry_cutoff_time
                if reason in {
                    "N19_ENTRY_BROKE_X_LOW",
                    "N19_ENTRY_PRICE_ABOVE_MAX",
                    "N19_ENTRY_WINDOW_EXPIRED",
                }
                else c_cutoff_time
            )
            unsigned = dict(evidence)
            unsigned.pop("canonical_sha256", None)
            evidence["canonical_sha256"] = n19_module._sha256_json(unsigned)
            return evidence

        for correct_stage, reasons in legal.items():
            for reason in sorted(reasons):
                with self.subTest(stage=correct_stage, reason=reason, valid=True):
                    decoded = decode_n19_state_evidence(
                        mutated(correct_stage, reason), expected_symbol="P000USDT"
                    )
                    self.assertEqual((decoded.stage, decoded.reason), (correct_stage, reason))
                for wrong_stage in sorted(terminal_stages - {correct_stage}):
                    with self.subTest(stage=wrong_stage, reason=reason, valid=False):
                        with self.assertRaisesRegex(ValueError, "stage and reason"):
                            decode_n19_state_evidence(
                                mutated(wrong_stage, reason),
                                expected_symbol="P000USDT",
                            )
        for bad_reason in ("N19_UNKNOWN_TERMINAL_REASON", 1, True):
            with self.subTest(reason=bad_reason):
                with self.assertRaises(ValueError):
                    decode_n19_state_evidence(
                        mutated("MISSED", bad_reason), expected_symbol="P000USDT"
                    )

        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-stage-reason"),
            )
            self.assertEqual(recorder.record_n19_state(result.state_record), "INSERTED")
            scan_id = recorder.begin_scan(100, [], True)
            signal_id = recorder.record_strategy_signal(
                scan_id, "N19", "P000USDT", "", (), "", True, True,
                "PASSED", "PASSED", result.structure_id, result.detail_json(),
            )
            proposal = StrategyScheduler._history_coverage_proposal(
                "N19", "P000USDT", n19_fixture(), 122
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(scan_id, 1, (proposal,))
            )
            illegal = mutated("CONSUMED", "N19_HISTORICAL_ENTRY_MISSED")
            illegal_record = replace(
                result.state_record,
                stage="CONSUMED",
                reason="N19_HISTORICAL_ENTRY_MISSED",
                evidence=illegal,
            )
            self.assertEqual(
                recorder.record_n19_state(illegal_record),
                "N19_STATE_INCONSISTENT",
            )
            illegal_json = n19_module._canonical_json(illegal)
            with recorder._connect() as connection:
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError,
                    "historical terminal update is unauthorized",
                ):
                    connection.execute(
                        "UPDATE n19_staircase_states SET "
                        "stage='CONSUMED',reason=?,evidence_json=?,"
                        "evidence_sha256=? WHERE strategy_id='N19' "
                        "AND structure_id=?",
                        (
                            "N19_HISTORICAL_ENTRY_MISSED", illegal_json,
                            hashlib.sha256(
                                illegal_json.encode("utf-8")
                            ).hexdigest(),
                            result.structure_id,
                        ),
                    )
            recorder.assert_n19_execution_claim(
                signal_id, "P000USDT", result.structure_id
            )

    def test_history_coverage_conflict_blocks_publication_and_preserves_current(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-coverage"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            candidates = [candidate("P%03dUSDT" % index, index + 1) for index in range(100)]
            scheduler = StrategyScheduler(
                (N19_STRATEGY,), 96, recorder, logging.getLogger("n19-coverage")
            )
            pending_rows = n19_fixture()
            pending_rows[120][1:5] = ["106", "106.1", "105.5", "105.8"]
            pending_rows[120][10] = "40"
            first_scan = recorder.begin_scan(100, candidates, dry_run=True)
            first = scheduler.evaluate(
                first_scan,
                {"quote_volume_top": candidates, "negative_funding": []},
                {item.symbol: deepcopy(pending_rows) for item in candidates},
                BASE_TIME + 121 * INTERVAL_MS + 60_000,
            )
            self.assertTrue(first.signal_batch_published)
            self.assertEqual(first.passed_signals, [])

            jumped = deepcopy(pending_rows)
            jump = 124 * INTERVAL_MS
            for row in jumped:
                row[0] = int(row[0]) + jump
                row[6] = int(row[6]) + jump
            second_scan = recorder.begin_scan(100, candidates, dry_run=True)
            second = scheduler.evaluate(
                second_scan,
                {"quote_volume_top": candidates, "negative_funding": []},
                {item.symbol: deepcopy(jumped) for item in candidates},
                int(jumped[-1][0]) + 60_000,
            )
            self.assertFalse(second.signal_batch_published)
            self.assertTrue(
                all(
                    item.reason == "N19_HISTORY_COVERAGE_GAP_BLOCKED"
                    for item in second.signals
                )
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id FROM strategy_signal_current "
                        "WHERE singleton_id=1"
                    ).fetchone(),
                    (first_scan,),
                )

    def test_legacy_paper_results_preserve_statistics_without_qualification(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3", logging.getLogger("n19-paper")
            )
            for symbol in ("AAAUSDT", "BBBUSDT"):
                trade_id = recorder.open_strategy_paper_trade(
                    "N19", symbol, "100", "99", "105", "", {}, {}
                )
                self.assertIsInstance(trade_id, int)
                self.assertTrue(
                    recorder.close_strategy_paper_trade(
                        trade_id, "WIN", "TAKE_PROFIT", "105", "5"
                    )
                )
            state = recorder.get_strategy_state("N19")
            self.assertEqual(state.consecutive_wins, 0)
            self.assertEqual(state.paper_trade_count, 2)
            self.assertEqual(state.win_count, 2)
            self.assertEqual(state.loss_count, 0)
            self.assertFalse(state.live_eligible)

            void_id = recorder.open_strategy_paper_trade(
                "N19", "VOIDUSDT", "100", "99", "105", "", {}, {}
            )
            self.assertTrue(
                recorder.void_strategy_paper_trade(
                    void_id,
                    "N19",
                    "VOIDUSDT",
                    PAPER_TRADE_VOID_CONFIRMATION,
                )
            )
            after_void = recorder.get_strategy_state("N19")
            self.assertEqual(after_void.consecutive_wins, 0)
            self.assertEqual(after_void.paper_trade_count, 2)
            self.assertEqual(after_void.win_count, 2)
            self.assertEqual(after_void.loss_count, 0)
            self.assertFalse(after_void.live_eligible)

            loss_id = recorder.open_strategy_paper_trade(
                "N19", "LOSSUSDT", "100", "99", "105", "", {}, {}
            )
            self.assertTrue(
                recorder.close_strategy_paper_trade(
                    loss_id, "LOSS", "STOP_LOSS", "99", "-1"
                )
            )
            reset = recorder.get_strategy_state("N19")
            self.assertEqual(reset.consecutive_wins, 0)
            self.assertEqual(reset.paper_trade_count, 3)
            self.assertEqual(reset.win_count, 2)
            self.assertEqual(reset.loss_count, 1)
            self.assertFalse(reset.live_eligible)
            self.assertEqual(reset.last_trade_result, "LOSS")

    def test_representative_state_failure_never_falls_back(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-no-fallback"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            candidates = [candidate("P%03dUSDT" % index, index + 1) for index in range(100)]
            real_record = recorder.record_n19_state

            def fail_winner(record):
                if record.symbol == "P000USDT":
                    return "N19_STATE_PERSIST_FAILED"
                return real_record(record)

            scan_id = recorder.begin_scan(100, candidates, dry_run=True)
            scheduler = StrategyScheduler(
                (N19_STRATEGY,), 96, recorder, logging.getLogger("n19-no-fallback")
            )
            with patch.object(recorder, "record_n19_state", side_effect=fail_winner):
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": candidates, "negative_funding": []},
                    {item.symbol: n19_fixture() for item in candidates},
                    BASE_TIME + 121 * INTERVAL_MS + 60_000,
                )
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(result.passed_signals, [])
            reasons = {item.candidate.symbol: item.reason for item in result.signals}
            self.assertEqual(reasons["P000USDT"], "N19_STATE_PERSIST_FAILED")
            self.assertEqual(reasons["P001USDT"], "N19_NOT_REPRESENTATIVE")

    def test_signal_context_and_publish_failures_never_expose_execution_candidates(self):
        candidates = [candidate("P%03dUSDT" % index, index + 1) for index in range(100)]
        for mode in ("signal", "context", "publish"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                recorder = make_test_recorder(
                    Path(directory) / "review.sqlite3",
                    logging.getLogger("n19-failure-matrix"),
                )
                recorder.upsert_strategy_definitions(load_all_strategies())
                scan_id = recorder.begin_scan(100, candidates, dry_run=True)
                raw = {item.symbol: n19_fixture() for item in candidates}
                if mode == "context":
                    raw.pop(candidates[-1].symbol)
                scheduler = StrategyScheduler(
                    (N19_STRATEGY,), 96, recorder,
                    logging.getLogger("n19-failure-matrix"),
                )
                patches = []
                if mode == "signal":
                    patches.append(
                        patch.object(
                            recorder,
                            "record_strategy_signals",
                            return_value=StrategySignalBatchWriteResult(
                                failed_index=0
                            ),
                        )
                    )
                if mode == "publish":
                    patches.append(
                        patch.object(
                            recorder, "publish_strategy_signal_batch", return_value=False
                        )
                    )
                for active_patch in patches:
                    active_patch.start()
                try:
                    result = scheduler.evaluate(
                        scan_id,
                        {"quote_volume_top": candidates, "negative_funding": []},
                        raw,
                        BASE_TIME + 121 * INTERVAL_MS + 60_000,
                    )
                finally:
                    for active_patch in reversed(patches):
                        active_patch.stop()
                self.assertFalse(result.signal_batch_published)
                self.assertEqual(result.passed_signals, [])
                self.assertEqual(result.live_candidates, [])
                if mode == "context":
                    self.assertTrue(
                        any(
                            item.reason == "N19_MARKET_CONTEXT_INSUFFICIENT"
                            for item in result.signals
                        )
                    )

    def test_main_fetches_dropped_active_member_once_and_read_failure_blocks_round(self):
        candidates = [
            FundingCandidate(
                "P%03dUSDT" % index,
                None,
                Decimal("106.4"),
                quote_volume=Decimal("1000000"),
                quote_volume_rank=index + 1,
                candidate_universe="quote_volume_top",
            )
            for index in range(100)
        ]

        class Monitor:
            def scan_for_strategies(self, _volume_top_n):
                return StrategyMarketScan(100, [], candidates)

        class Client:
            def __init__(self):
                self.calls = []

            def get_klines(self, symbol):
                self.calls.append(symbol)
                return n19_fixture()

        class Recorder:
            def __init__(self, fail=False):
                self.fail = fail
                self.begin_calls = 0

            def expire_stale_n14_active_episodes(self, *_args):
                return "OK"

            def get_required_n14_snapshot_symbols(self, *_args):
                return ()

            def get_pending_n15_snapshot_symbols(self, *_args):
                return ()

            def get_required_n16_episode_symbols(self, *_args):
                return ()

            def get_required_n17_family_symbols(self):
                return ()

            def get_required_n19_family_symbols(self):
                if self.fail:
                    raise RuntimeError("read failed")
                return ("P000USDT", "DROPPEDUSDT")

            def get_required_n18_family_symbols(self):
                return ()

            def get_required_n20_frozen_symbols(self):
                return ()

            def begin_scan(self, *_args, **_kwargs):
                self.begin_calls += 1
                return 1

            def complete_scan(self, *_args, **_kwargs):
                return None

        class Scheduler:
            def evaluate(self, *_args, **_kwargs):
                return SimpleNamespace(
                    signal_batch_published=True,
                    passed_signals=[],
                    live_candidates=[],
                )

        for fail in (False, True):
            with self.subTest(fail=fail):
                bot = TradingBot.__new__(TradingBot)
                bot.config = SimpleNamespace(dry_run=True)
                bot.logger = logging.getLogger("n19-dropped-member")
                bot.monitor = Monitor()
                bot.client = Client()
                bot.recorder = Recorder(fail=fail)
                bot.strategy_scheduler = Scheduler()
                bot.strategies = load_all_strategies()
                bot._n16_execution_boundary_ready = lambda: True
                bot._prepare_n16_local_recovery_before_scan = lambda: True
                bot._signal_batch_is_current = lambda _scan_id: True
                bot._n16_current_claims_ready = lambda *_args: True
                bot._reconcile_multi_strategy_after_publication = lambda: None
                bot._run_once_multi_strategy()
                if fail:
                    self.assertEqual(bot.recorder.begin_calls, 0)
                    self.assertEqual(bot.client.calls, [])
                else:
                    self.assertEqual(bot.recorder.begin_calls, 1)
                    self.assertEqual(len(bot.client.calls), 101)
                    self.assertEqual(bot.client.calls.count("P000USDT"), 1)
                    self.assertEqual(bot.client.calls.count("DROPPEDUSDT"), 1)

    def test_n06_through_n20_emit_exactly_1500_ordinary_decisions(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-ordinary-count"),
            )
            strategies = tuple(
                strategy
                for strategy in load_all_strategies()
                if 6 <= int(strategy.strategy_id[1:]) <= 20
            )
            self.assertEqual(
                [item.strategy_id for item in strategies],
                ["N%02d" % value for value in range(6, 21)],
            )
            candidates = [candidate("P%03dUSDT" % index, index + 1) for index in range(100)]
            scheduler = StrategyScheduler(
                strategies, 96, recorder, logging.getLogger("n19-ordinary-count")
            )
            recorded = []

            def reject(strategy, item, *_args, **_kwargs):
                return scheduler._rejected(strategy, item, "COUNT_ONLY_REJECTED")

            def record_batch(_scan_id, records):
                recorded.extend(
                    (item["strategy_id"], item["symbol"]) for item in records
                )
                return StrategySignalBatchWriteResult(
                    signal_ids=tuple(range(1, len(records) + 1))
                )

            with patch.object(
                scheduler, "_evaluate_candidate", side_effect=reject
            ), patch.object(
                recorder, "record_strategy_signals", side_effect=record_batch
            ), patch.object(
                recorder, "publish_strategy_signal_batch", return_value=True
            ):
                result = scheduler.evaluate(
                    1,
                    {"quote_volume_top": candidates, "negative_funding": []},
                    {item.symbol: n19_fixture() for item in candidates},
                    BASE_TIME + 121 * INTERVAL_MS + 60_000,
                )
            self.assertEqual(len(result.signals), 1500)
            self.assertEqual(len(recorded), 1500)
            for strategy in strategies:
                self.assertEqual(
                    sum(item[0] == strategy.strategy_id for item in recorded), 100
                )

    def test_publish_and_plan_integrity_failures_block_all_runtime_side_effects(self):
        candidates = [candidate("P%03dUSDT" % index, index + 1) for index in range(100)]
        passed_analysis = analyze()
        signal = SimpleNamespace(
            strategy=N19_STRATEGY,
            candidate=candidates[0],
            analysis=passed_analysis,
            signal_id=1,
        )

        class Monitor:
            def scan_for_strategies(self, _top_n):
                return StrategyMarketScan(100, [], candidates)

        class Client:
            def __init__(self):
                self.market_calls = 0

            def get_klines(self, _symbol):
                return n19_fixture()

        class Recorder:
            def __init__(self):
                self.events = []

            def expire_stale_n14_active_episodes(self, *_args):
                return "OK"

            def get_required_n14_snapshot_symbols(self, *_args):
                return ()

            def get_pending_n15_snapshot_symbols(self, *_args):
                return ()

            def get_required_n16_episode_symbols(self, *_args):
                return ()

            def get_required_n17_family_symbols(self):
                return ()

            def get_required_n19_family_symbols(self):
                return ()

            def begin_scan(self, *_args, **_kwargs):
                return 1

            def complete_scan(self, *_args, **_kwargs):
                return True

            def record_event(self, *args):
                self.events.append(args)
                return True

            def update_strategy_signal(self, *_args, **_kwargs):
                return True

        class Scheduler:
            def __init__(self, mode):
                self.mode = mode

            def evaluate(self, *_args, **_kwargs):
                return SimpleNamespace(
                    signal_batch_published=self.mode != "publish",
                    passed_signals=[] if self.mode == "publish" else [signal],
                    live_candidates=[],
                )

        class ForbiddenTrader:
            def __init__(self):
                self.open_calls = 0

            def open_long_plan_with_protection(self, _plan):
                self.open_calls += 1
                raise AssertionError("live execution must remain blocked")

        class ForbiddenPaper:
            def __init__(self):
                self.open_calls = 0

            def open_trade(self, *_args, **_kwargs):
                self.open_calls += 1
                raise AssertionError("paper execution must remain blocked")

        for mode in ("publish", "plan"):
            with self.subTest(mode=mode):
                bot = TradingBot.__new__(TradingBot)
                bot.config = SimpleNamespace(dry_run=True)
                bot.logger = logging.getLogger("n19-main-gate")
                bot.monitor = Monitor()
                bot.client = Client()
                bot.recorder = Recorder()
                bot.strategy_scheduler = Scheduler(mode)
                bot.strategies = (N19_STRATEGY,)
                bot.trader = ForbiddenTrader()
                bot.paper_trader = ForbiddenPaper()
                bot._n16_execution_boundary_ready = lambda: True
                bot._prepare_n16_local_recovery_before_scan = lambda: True
                bot._signal_batch_is_current = lambda _scan_id: True
                bot._n16_current_claims_ready = lambda *_args: True
                reconcile_calls = []
                bot._reconcile_multi_strategy_after_publication = (
                    lambda: reconcile_calls.append(True)
                )
                if mode == "plan":
                    bot._build_strategy_plan = lambda _signal: (_ for _ in ()).throw(
                        _N19PlanIntegrityError(
                            "N19_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
                        )
                    )
                bot._run_once_multi_strategy()
                self.assertEqual(reconcile_calls, [])
                self.assertEqual(bot.trader.open_calls, 0)
                self.assertEqual(bot.paper_trader.open_calls, 0)
                self.assertEqual(bot.client.market_calls, 0)

    def test_reset_persistence_failure_blocks_publication_and_all_execution_candidates(self):
        terminal_rows = n19_fixture()
        terminal_rows[120][1:5] = ["106.3", "107.2", "105.2", "107"]
        terminal_rows[120][10] = "40"
        terminal = analyze(terminal_rows)
        self.assertEqual(terminal.state_record.stage, "CONSUMED")

        reset_rows = deepcopy(terminal_rows)
        reset_rows.append(
            _row(122, ("106.5", "107", "106.2", "106.8"), "100", "50")
        )
        reset_rows.append(
            _row(123, ("106.8", "107.2", "106.5", "106.9"), "100", "50")
        )
        reset_rows.append(
            _row(124, ("106.9", "107.1", "106.6", "107"), "100", "50")
        )

        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-reset-failure"),
            )
            recorder.upsert_strategy_definitions((N19_STRATEGY,))
            self.assertEqual(recorder.record_n19_state(terminal.state_record), "INSERTED")
            candidates = [
                FundingCandidate(
                    "P%03dUSDT" % index,
                    None,
                    Decimal("107"),
                    quote_volume=Decimal("1000000"),
                    quote_volume_rank=index + 1,
                    candidate_universe="quote_volume_top",
                )
                for index in range(100)
            ]
            scan_id = recorder.begin_scan(100, candidates, dry_run=True)
            raw = {
                candidate.symbol: (
                    reset_rows
                    if candidate.symbol == "P000USDT"
                    else n19_fixture()
                )
                for candidate in candidates
            }
            real_record = recorder.record_n19_state

            def fail_reset(record):
                if record.reset_after_time_ms is not None:
                    return "N19_STATE_PERSIST_FAILED"
                return real_record(record)

            scheduler = StrategyScheduler(
                (N19_STRATEGY,),
                96,
                recorder,
                logging.getLogger("n19-reset-failure"),
            )
            with patch.object(recorder, "record_n19_state", side_effect=fail_reset):
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": candidates, "negative_funding": []},
                    raw,
                    BASE_TIME + 124 * INTERVAL_MS + 60_000,
                )
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertIsNone(
                    connection.execute(
                        "SELECT reset_after_time_ms FROM n19_staircase_states "
                        "WHERE family_id=?",
                        (terminal.state_record.family_id,),
                    ).fetchone()[0]
                )

    def test_explicit_n19_upgrade_is_required_and_wrong_schema_is_zero_write(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            ledger = Path(directory) / "claim.sqlite3"
            with closing(sqlite3.connect(database)) as connection:
                connection.commit()
            _install_n16_claim_boundary(database, ledger)
            _install_n17_lifecycle_boundary(database, ledger)
            before = database.read_bytes()
            before_entries = sorted(os.listdir(directory))
            with self.assertRaisesRegex(RuntimeError, "pre-N19"):
                ReviewRecorder(
                    database,
                    logging.getLogger("n19-pre"),
                    n16_claim_ledger_file=ledger,
                )
            self.assertEqual(database.read_bytes(), before)
            self.assertEqual(sorted(os.listdir(directory)), before_entries)

            _install_n19_lifecycle_boundary(database, ledger)
            with self.assertRaisesRegex(RuntimeError, "pre-N18"):
                ReviewRecorder(
                    database,
                    logging.getLogger("n19-current-pre-n18"),
                    n16_claim_ledger_file=ledger,
                )
            _install_n18_lifecycle_boundary(database, ledger)
            with self.assertRaisesRegex(RuntimeError, "pre-N20"):
                ReviewRecorder(
                    database,
                    logging.getLogger("n19-current-pre-n20"),
                    n16_claim_ledger_file=ledger,
                )
            _install_n20_lifecycle_boundary(database, ledger)
            _install_micro_lifecycle_boundary(database, ledger)
            _install_coverage_epoch_boundary(database, ledger)
            recorder = ReviewRecorder(
                database,
                logging.getLogger("n19-current"),
                n16_claim_ledger_file=ledger,
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT strategy_id,schema_version,rule_version "
                        "FROM n19_lifecycle_installation"
                    ).fetchone(),
                    ("N19", 1, "N19_V1"),
                )
            with closing(sqlite3.connect(database)) as connection:
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                connection.execute("PRAGMA journal_mode=DELETE")
                connection.execute("DROP INDEX idx_n19_staircase_structure")
                connection.execute(
                    "CREATE INDEX idx_n19_staircase_structure "
                    "ON n19_staircase_states(symbol,structure_id DESC)"
                )
                connection.commit()
            tampered = database.read_bytes()
            entries = sorted(os.listdir(directory))
            ledger_before = ledger.read_bytes()
            with self.assertRaisesRegex(RuntimeError, "N19"):
                ReviewRecorder(
                    database,
                    logging.getLogger("n19-tamper"),
                    n16_claim_ledger_file=ledger,
                )
            self.assertEqual(database.read_bytes(), tampered)
            self.assertEqual(ledger.read_bytes(), ledger_before)
            self.assertEqual(sorted(os.listdir(directory)), entries)

    def test_n19_schema_catalog_dependency_and_root_matrix_is_zero_write(self):
        cases = (
            [("index", name) for name in sorted(N19_INDEX_SQL)]
            + [("trigger", name) for name in sorted(N19_TRIGGER_SQL)]
            + [
                ("table", "n19_history_coverage"),
                ("foreign_trigger", "hostile_n19_trigger"),
                ("incoming_fk", "hostile_n19_fk"),
                ("root", "n19_lifecycle_installation"),
            ]
        )
        for mode, name in cases:
            with self.subTest(mode=mode, name=name), tempfile.TemporaryDirectory() as directory:
                database = Path(directory) / "review.sqlite3"
                ledger = Path(directory) / "claim.sqlite3"
                with closing(sqlite3.connect(database)) as connection:
                    connection.commit()
                _install_n16_claim_boundary(database, ledger)
                _install_n17_lifecycle_boundary(database, ledger)
                _install_n19_lifecycle_boundary(database, ledger)
                with closing(sqlite3.connect(database)) as connection:
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    connection.execute("PRAGMA journal_mode=DELETE")
                    if mode == "index":
                        table = (
                            "n19_history_coverage"
                            if name == "idx_n19_coverage_symbol"
                            else "n19_staircase_states"
                        )
                        connection.execute('DROP INDEX "%s"' % name)
                        connection.execute(
                            'CREATE INDEX "%s" ON %s(symbol COLLATE NOCASE DESC)'
                            % (name, table)
                        )
                    elif mode == "trigger":
                        connection.execute('DROP TRIGGER "%s"' % name)
                    elif mode == "table":
                        connection.execute("DROP TABLE n19_history_coverage")
                    elif mode == "foreign_trigger":
                        connection.execute("CREATE TABLE hostile_probe(value INTEGER)")
                        connection.execute(
                            "CREATE TRIGGER hostile_n19_trigger AFTER UPDATE ON "
                            "n19_staircase_states BEGIN INSERT INTO hostile_probe "
                            "VALUES(1); END"
                        )
                    elif mode == "incoming_fk":
                        connection.execute(
                            "CREATE TABLE hostile_n19_fk(parent_id INTEGER "
                            "REFERENCES n19_staircase_states(id) ON DELETE CASCADE)"
                        )
                    else:
                        connection.execute(
                            "DROP TRIGGER trg_n19_installation_immutable"
                        )
                        connection.execute("PRAGMA ignore_check_constraints=ON")
                        connection.execute(
                            "UPDATE n19_lifecycle_installation SET guard_sha256=?",
                            ("f" * 64,),
                        )
                    connection.commit()
                review_before = database.read_bytes()
                ledger_before = ledger.read_bytes()
                entries = sorted(os.listdir(directory))
                with self.assertRaisesRegex(RuntimeError, "N19"):
                    ReviewRecorder(
                        database,
                        logging.getLogger("n19-schema-matrix"),
                        n16_claim_ledger_file=ledger,
                    )
                self.assertEqual(database.read_bytes(), review_before)
                self.assertEqual(ledger.read_bytes(), ledger_before)
                self.assertEqual(sorted(os.listdir(directory)), entries)

    def test_exact_coverage_v3_requires_explicit_terminal_receipt_upgrade(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            ledger = Path(directory) / "claim.sqlite3"
            recorder = make_test_recorder(
                database,
                logging.getLogger("coverage-v3-fixture"),
                n16_claim_ledger_file=ledger,
            )
            legacy_proposal = HistoryCoverageProposal(
                "N17",
                "LEGACYV3USDT",
                BASE_TIME,
                BASE_TIME + 120 * INTERVAL_MS,
                BASE_TIME + 121 * INTERVAL_MS,
                hashlib.sha256(b"legacy-v3-source").hexdigest(),
            )
            legacy_scan = recorder.begin_scan(1, [], True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    legacy_scan,
                    "N17",
                    "LEGACYV3USDT",
                    "",
                    (),
                    "",
                    False,
                    False,
                    "REJECTED",
                    "N17_NO_SETUP",
                ),
                int,
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    legacy_scan,
                    1,
                    (legacy_proposal,),
                )
            )
            with recorder._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                # Model a genuine coverage-v3 database.  The current test
                # recorder also installs the later, independent N15 terminal
                # generation; remove that later generation before renaming
                # coverage tables referenced by its triggers.
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
                for trigger in (
                    "trg_history_coverage_receipt_insert_authorized",
                    "trg_history_coverage_receipt_no_replace",
                    "trg_history_coverage_receipt_no_update",
                    "trg_history_coverage_receipt_no_delete",
                ):
                    connection.execute('DROP TRIGGER "%s"' % trigger)
                connection.execute(
                    "DROP INDEX idx_history_coverage_receipt_owner"
                )
                connection.execute(
                    "ALTER TABLE history_coverage_publication_receipts "
                    "RENAME TO history_coverage_publication_receipts_v4"
                )
                connection.execute(LEGACY_V3_PUBLICATION_TABLE_SQL)
                connection.execute(
                    "INSERT INTO history_coverage_publication_receipts "
                    "SELECT source_scan_id,strategy_id,symbol,"
                    "source_start_time_ms,covered_through_time_ms,"
                    "source_sha256,result_epoch_ordinal,"
                    "result_epoch_start_time_ms,result_chain_head_sha256,"
                    "publication_ordinal,previous_receipt_sha256,"
                    "batch_expected_count,batch_manifest_sha256,"
                    "receipt_sha256,published_at "
                    "FROM history_coverage_publication_receipts_v4"
                )
                connection.execute(
                    "DROP TABLE history_coverage_publication_receipts_v4"
                )
                connection.execute(
                    "CREATE UNIQUE INDEX idx_history_coverage_receipt_owner "
                    "ON history_coverage_publication_receipts("
                    "source_scan_id,strategy_id,symbol)"
                )
                for trigger in (
                    "trg_history_coverage_receipt_insert_authorized",
                    "trg_history_coverage_receipt_no_replace",
                    "trg_history_coverage_receipt_no_update",
                    "trg_history_coverage_receipt_no_delete",
                ):
                    connection.execute(COVERAGE_TRIGGER_SQL[trigger])
                for trigger in (
                    "trg_history_coverage_n19_terminal_insert_authorized",
                    "trg_history_coverage_n19_terminal_no_replace",
                    "trg_history_coverage_n19_terminal_no_update",
                    "trg_history_coverage_n19_terminal_no_delete",
                ):
                    connection.execute('DROP TRIGGER "%s"' % trigger)
                connection.execute(
                    "DROP INDEX idx_history_coverage_n19_terminal_owner"
                )
                connection.execute(
                    "DROP TABLE history_coverage_n19_terminal_receipts"
                )
                family_objects = connection.execute(
                    "SELECT type,name FROM sqlite_schema "
                    "WHERE name IN (%s)"
                    % ",".join("?" for _ in FAMILY_SEAL_OBJECTS),
                    tuple(FAMILY_SEAL_OBJECTS),
                ).fetchall()
                for object_type in ("trigger", "index", "table"):
                    for actual_type, name in family_objects:
                        if actual_type == object_type:
                            connection.execute(
                                "DROP %s \"%s\""
                                % (object_type.upper(), name)
                            )
                for trigger in (
                    "trg_history_coverage_installation_no_update",
                    "trg_history_coverage_installation_no_delete",
                    "trg_history_coverage_installation_no_replace",
                    "trg_history_coverage_installation_insert_authorized",
                ):
                    connection.execute('DROP TRIGGER "%s"' % trigger)
                connection.execute(
                    "DROP TABLE history_coverage_epoch_installation"
                )
                connection.execute(LEGACY_V3_INSTALLATION_TABLE_SQL)
                connection.execute(
                    "INSERT INTO history_coverage_epoch_installation "
                    "VALUES (1,3,?,?,?,?)",
                    (
                        COVERAGE_EPOCH_RULE_VERSION,
                        1,
                        "0" * 64,
                        "2026-07-26T00:00:00+00:00",
                    ),
                )
                for trigger in (
                    "trg_history_coverage_installation_no_update",
                    "trg_history_coverage_installation_no_delete",
                    "trg_history_coverage_installation_no_replace",
                    "trg_history_coverage_installation_insert_authorized",
                ):
                    connection.execute(COVERAGE_TRIGGER_SQL[trigger])
                schema_version = connection.execute(
                    "PRAGMA schema_version"
                ).fetchone()[0]
                catalog_sha256 = _coverage_epoch_catalog_sha256_for_objects(
                    connection,
                    _LEGACY_V3_OBJECTS,
                )
                connection.execute(
                    "DROP TRIGGER "
                    "trg_history_coverage_installation_no_update"
                )
                connection.execute(
                    "UPDATE history_coverage_epoch_installation "
                    "SET catalog_schema_version=?,catalog_sha256=?",
                    (schema_version, catalog_sha256),
                )
                connection.execute(
                    COVERAGE_TRIGGER_SQL[
                        "trg_history_coverage_installation_no_update"
                    ]
                )
                recorder._refresh_n16_catalog_generation_in_transaction(
                    connection
                )
                connection.commit()
            del recorder
            with closing(sqlite3.connect(ledger)) as connection:
                for (name,) in connection.execute(
                    "SELECT name FROM sqlite_schema WHERE type='trigger' "
                    "AND name LIKE 'trg_n16_protected_generation_%'"
                ):
                    connection.execute('DROP TRIGGER "%s"' % name)
                connection.execute(
                    "DROP TABLE n16_protected_generation_highwater"
                )
                connection.execute(
                    "DROP TABLE "
                    "n16_protected_generation_highwater_installation"
                )
                connection.commit()

            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    coverage_epoch_schema_status(connection),
                    "PRE_TERMINAL_RECEIPT",
                )
                symbols, incomplete_market = market_fixture()
                incomplete_market.pop(symbols[-1])
                confirmed = analyze(
                    n19_fixture(),
                    market_symbols=symbols,
                    market_klines_by_symbol=incomplete_market,
                )
                rolled = [
                    _row(
                        index,
                        ("106.4", "106.8", "106.2", "106.5"),
                        "100",
                        "55",
                    )
                    for index in range(130, 252)
                ]
                terminal = analyze(
                    rolled,
                    checked_at_ms=int(rolled[-1][0]) + 60_000,
                    frozen_evidence=confirmed.state_record.evidence_json,
                )
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO n19_staircase_states "
                    "(strategy_id,symbol,family_id,structure_id,stage,"
                    "reason,quote_volume_rank,s_open_time_ms,x_open_time_ms,"
                    "reset_after_time_ms,evidence_json,evidence_sha256,"
                    "created_at,updated_at) "
                    "VALUES ('N19',?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        terminal.state_record.symbol,
                        terminal.state_record.family_id,
                        terminal.state_record.structure_id,
                        terminal.state_record.stage,
                        terminal.state_record.reason,
                        terminal.state_record.quote_volume_rank,
                        terminal.state_record.s_open_time_ms,
                        terminal.state_record.x_open_time_ms,
                        terminal.state_record.reset_after_time_ms,
                        terminal.state_record.evidence_json,
                        terminal.state_record.evidence_sha256,
                        "2026-07-26T00:00:00+00:00",
                        "2026-07-26T00:00:00+00:00",
                    ),
                )
                catalog_before = tuple(
                    connection.execute(
                        "SELECT type,name,tbl_name,sql FROM sqlite_schema "
                        "ORDER BY type,name"
                    )
                )
                with self.assertRaisesRegex(
                    RuntimeError,
                    "terminal publication proof is unavailable",
                ):
                    _upgrade_coverage_epoch_schema_v3(
                        connection,
                        "2026-07-26T00:00:01+00:00",
                    )
                self.assertEqual(
                    tuple(
                        connection.execute(
                            "SELECT type,name,tbl_name,sql "
                            "FROM sqlite_schema ORDER BY type,name"
                        )
                    ),
                    catalog_before,
                )
                connection.rollback()
            review_before = database.read_bytes()
            ledger_before = ledger.read_bytes()
            entries_before = sorted(os.listdir(directory))
            with self.assertRaisesRegex(RuntimeError, "explicit"):
                ReviewRecorder(
                    database,
                    logging.getLogger("coverage-v3-runtime"),
                    n16_claim_ledger_file=ledger,
                )
            self.assertEqual(database.read_bytes(), review_before)
            self.assertEqual(ledger.read_bytes(), ledger_before)
            self.assertEqual(sorted(os.listdir(directory)), entries_before)

            _install_coverage_epoch_boundary(database, ledger)
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    coverage_epoch_schema_status(connection),
                    "CURRENT",
                )
                self.assertEqual(
                    family_seal_schema_status(connection),
                    "CURRENT",
                )
            ReviewRecorder(
                database,
                logging.getLogger("coverage-v3-upgraded-runtime"),
                n16_claim_ledger_file=ledger,
            )

    def test_n16_and_n17_diagnostics_precede_pre_n19_runtime_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            ledger = Path(directory) / "claim.sqlite3"
            with closing(sqlite3.connect(database)) as connection:
                connection.commit()
            before = database.read_bytes()
            with self.assertRaisesRegex(RuntimeError, "pre-N16"):
                ReviewRecorder(
                    database,
                    logging.getLogger("n19-priority-n16"),
                    n16_claim_ledger_file=ledger,
                )
            self.assertEqual(database.read_bytes(), before)
            self.assertFalse(ledger.exists())

            _install_n16_claim_boundary(database, ledger)
            review_before = database.read_bytes()
            ledger_before = ledger.read_bytes()
            with self.assertRaisesRegex(RuntimeError, "pre-N17"):
                ReviewRecorder(
                    database,
                    logging.getLogger("n19-priority-n17"),
                    n16_claim_ledger_file=ledger,
                )
            self.assertEqual(database.read_bytes(), review_before)
            self.assertEqual(ledger.read_bytes(), ledger_before)

    def test_scheduler_persists_all_analysis_and_selects_one_stable_representative(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-scheduler"),
            )
            recorder.upsert_strategy_definitions((N19_STRATEGY,))
            candidates = [
                FundingCandidate(
                    "P%03dUSDT" % index,
                    None,
                    Decimal("106.4"),
                    quote_volume=Decimal("1000000"),
                    quote_volume_rank=index + 1,
                    candidate_universe="quote_volume_top",
                )
                for index in range(100)
            ]
            scan_id = recorder.begin_scan(100, candidates, dry_run=True)
            scheduler = StrategyScheduler(
                (N19_STRATEGY,), 96, recorder, logging.getLogger("n19-scheduler")
            )
            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": candidates, "negative_funding": []},
                {candidate.symbol: n19_fixture() for candidate in candidates},
                BASE_TIME + 121 * INTERVAL_MS + 60_000,
            )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(len(result.signals), 100)
            self.assertEqual(len(result.passed_signals), 1)
            winner = result.passed_signals[0]
            self.assertEqual(winner.candidate.symbol, "P000USDT")
            self.assertEqual(winner.reason, "PASSED")
            self.assertTrue(all(signal.signal_id for signal in result.signals))
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n19_staircase_states"
                    ).fetchone()[0],
                    100,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                        "WHERE strategy_id='N19' AND claim_state='ACTIVE'"
                    ).fetchone()[0],
                    1,
                )
            recorder.assert_n19_execution_claim(
                winner.signal_id,
                winner.candidate.symbol,
                winner.analysis.structure_id,
            )
            self.assertIn(
                winner.candidate.symbol,
                recorder.get_required_n19_family_symbols(),
            )
            replay_scan = recorder.begin_scan(100, candidates, dry_run=True)
            replay = scheduler.evaluate(
                replay_scan,
                {"quote_volume_top": candidates, "negative_funding": []},
                {item.symbol: n19_fixture() for item in candidates},
                BASE_TIME + 121 * INTERVAL_MS + 70_000,
            )
            self.assertTrue(replay.signal_batch_published)
            self.assertEqual(replay.passed_signals, [])
            self.assertEqual(
                next(
                    item.reason
                    for item in replay.signals
                    if item.candidate.symbol == winner.candidate.symbol
                ),
                "N19_STRUCTURE_CONSUMED",
            )
            active_state = recorder.get_latest_n19_states(
                {winner.candidate.symbol}
            )[winner.candidate.symbol]
            terminal_rows = deepcopy(n19_fixture())
            terminal_rows.append(
                _row(122, ("106.4", "106.8", "106.2", "106.5"), "100", "55")
            )
            terminal = analyze(
                terminal_rows,
                checked_at_ms=BASE_TIME + 122 * INTERVAL_MS + 60_000,
                frozen_evidence=active_state.evidence_json,
            )
            self.assertEqual(terminal.reason, "N19_HISTORICAL_ENTRY_MISSED")
            self.assertEqual(terminal.state_record.stage, "MISSED")
            terminal_scan = recorder.begin_scan(1, [], dry_run=True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    terminal_scan,
                    "N19",
                    winner.candidate.symbol,
                    "",
                    (),
                    "",
                    False,
                    False,
                    "REJECTED",
                    "N19_HISTORICAL_ENTRY_MISSED",
                    terminal.state_record.structure_id,
                    terminal.detail_json(),
                ),
                int,
            )
            terminal_proposal = replace(
                StrategyScheduler._history_coverage_proposal(
                    "N19",
                    winner.candidate.symbol,
                    terminal_rows,
                    122,
                ),
                n19_terminal_family_id=terminal.state_record.family_id,
                n19_terminal_structure_id=terminal.state_record.structure_id,
                n19_terminal_evidence_sha256=(
                    terminal.state_record.evidence_sha256
                ),
                n19_terminal_state_record=terminal.state_record,
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    terminal_scan, 1, (terminal_proposal,)
                )
            )
            recorder.assert_n19_execution_claim(
                winner.signal_id,
                winner.candidate.symbol,
                winner.analysis.structure_id,
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertIsNone(connection.execute(
                    "SELECT 1 FROM strategy_signals WHERE id=?",
                    (winner.signal_id,),
                ).fetchone())
            self.assertNotIn(
                winner.candidate.symbol,
                recorder.get_required_n19_family_symbols(),
            )

            reset_rows = deepcopy(terminal_rows)
            reset_rows.append(
                _row(123, ("107", "107", "106.5", "107"), "100", "50")
            )
            reset_rows.append(
                _row(124, ("107", "107", "106.5", "107"), "100", "50")
            )
            reset = analyze(
                reset_rows,
                checked_at_ms=BASE_TIME + 124 * INTERVAL_MS + 60_000,
                frozen_evidence=terminal.state_record.evidence_json,
            )
            self.assertEqual(len(reset.state_records), 1)
            self.assertGreater(
                reset.state_records[0].reset_after_time_ms,
                reset.state_records[0].evidence["terminal_cutoff_time_ms"],
            )
            self.assertEqual(recorder.record_n19_state(reset.state_records[0]), "UPDATED")

    def test_historical_terminal_requires_atomic_publication_proof(self):
        rows = n19_fixture()
        symbols, incomplete_market = market_fixture(rows)
        incomplete_market.pop(symbols[-1])
        confirmed = analyze(
            rows,
            market_symbols=symbols,
            market_klines_by_symbol=incomplete_market,
        )
        rolled = [
            _row(
                index,
                ("106.4", "106.8", "106.2", "106.5"),
                "100",
                "55",
            )
            for index in range(130, 252)
        ]
        terminal = analyze(
            rolled,
            checked_at_ms=int(rolled[-1][0]) + 60_000,
            frozen_evidence=confirmed.state_record.evidence_json,
        )
        self.assertEqual(
            terminal.reason, "N19_HISTORICAL_ENTRY_MISSED"
        )
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database,
                logging.getLogger("n19-terminal-publication-only"),
            )
            self.assertEqual(
                recorder.record_n19_state(confirmed.state_record),
                "INSERTED",
            )
            with recorder._read_only_runtime_snapshot() as connection:
                before = tuple(
                    connection.execute(
                        "SELECT * FROM n19_staircase_states"
                    )
                )
            self.assertEqual(
                recorder.record_n19_state(terminal.state_record),
                "N19_STATE_INCONSISTENT",
            )
            with closing(sqlite3.connect(database)) as connection:
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError,
                    "historical terminal update is unauthorized",
                ):
                    connection.execute(
                        "UPDATE n19_staircase_states SET "
                        "stage=?,reason=?,evidence_json=?,"
                        "evidence_sha256=?,updated_at=? "
                        "WHERE family_id=?",
                        (
                            terminal.state_record.stage,
                            terminal.state_record.reason,
                            terminal.state_record.evidence_json,
                            terminal.state_record.evidence_sha256,
                            "2099-01-01T00:00:00+00:00",
                            terminal.state_record.family_id,
                        ),
                    )
                connection.rollback()
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    tuple(
                        connection.execute(
                            "SELECT * FROM n19_staircase_states"
                        )
                    ),
                    before,
                )
                validate_coverage_epoch_graph(connection)
            scan_id = recorder.begin_scan(1, [], dry_run=True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    scan_id,
                    "N19",
                    "P000USDT",
                    "",
                    (),
                    "",
                    False,
                    False,
                    "REJECTED",
                    "N19_HISTORICAL_ENTRY_MISSED",
                    terminal.state_record.structure_id,
                    terminal.detail_json(),
                ),
                int,
            )
            proposal = replace(
                StrategyScheduler._history_coverage_proposal(
                    "N19", "P000USDT", rolled, 122
                ),
                n19_terminal_family_id=terminal.state_record.family_id,
                n19_terminal_structure_id=(
                    terminal.state_record.structure_id
                ),
                n19_terminal_evidence_sha256=(
                    terminal.state_record.evidence_sha256
                ),
                n19_terminal_state_record=terminal.state_record,
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    scan_id, 1, (proposal,)
                )
            )
            with recorder._read_only_runtime_snapshot() as connection:
                validate_coverage_epoch_graph(connection)
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM "
                        "history_coverage_n19_terminal_receipts "
                        "WHERE source_scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    (1,),
                )

    def test_equal_but_distinct_consumed_state_cannot_publish_passed(self):
        passed = analyze()
        terminal_rows = deepcopy(n19_fixture())
        terminal_rows.append(
            _row(122, ("106.4", "106.8", "106.2", "106.5"), "100", "55")
        )
        durable_terminal_analysis = analyze(
            terminal_rows,
            checked_at_ms=BASE_TIME + 122 * INTERVAL_MS + 60_000,
            frozen_evidence=passed.state_record.evidence_json,
        )
        durable_terminal = durable_terminal_analysis.state_record
        cloned = replace(
            passed.state_record,
            evidence=deepcopy(passed.state_record.evidence),
        )
        self.assertIsNot(cloned, passed.state_record)
        self.assertEqual(cloned, passed.state_record)
        attempted = replace(passed, state_records=(cloned,))

        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-equal-distinct-consumed"),
            )
            recorder.upsert_strategy_definitions((N19_STRATEGY,))
            self.assertEqual(
                recorder.record_n19_state(passed.state_record),
                "INSERTED",
            )
            terminal_scan = recorder.begin_scan(1, [], dry_run=True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    terminal_scan,
                    "N19",
                    "P000USDT",
                    "",
                    (),
                    "",
                    False,
                    False,
                    "REJECTED",
                    "N19_HISTORICAL_ENTRY_MISSED",
                    durable_terminal.structure_id,
                    durable_terminal_analysis.detail_json(),
                ),
                int,
            )
            terminal_proposal = replace(
                StrategyScheduler._history_coverage_proposal(
                    "N19", "P000USDT", terminal_rows, 122
                ),
                n19_terminal_family_id=durable_terminal.family_id,
                n19_terminal_structure_id=durable_terminal.structure_id,
                n19_terminal_evidence_sha256=(
                    durable_terminal.evidence_sha256
                ),
                n19_terminal_state_record=durable_terminal,
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    terminal_scan, 1, (terminal_proposal,)
                )
            )
            candidates = [
                FundingCandidate(
                    "P%03dUSDT" % index,
                    None,
                    Decimal("106.4"),
                    quote_volume=Decimal("1000000"),
                    quote_volume_rank=index + 1,
                    candidate_universe="quote_volume_top",
                )
                for index in range(100)
            ]
            real_analyze = (
                n19_module.analyze_n19_staircase_exhaustion_reversal
            )

            def analyze_once(symbol, *args, **kwargs):
                if symbol == "P000USDT":
                    return attempted
                return real_analyze(symbol, *args, **kwargs)

            scheduler = StrategyScheduler(
                (N19_STRATEGY,),
                96,
                recorder,
                logging.getLogger("n19-equal-distinct-consumed"),
            )
            scan_id = recorder.begin_scan(100, candidates, dry_run=True)
            flat_rows = [
                _row(
                    index,
                    ("100", "100.1", "99.9", "100"),
                    "100",
                    "50",
                )
                for index in range(122)
            ]
            with patch(
                "trading_bot.strategy_scheduler."
                "analyze_n19_staircase_exhaustion_reversal",
                side_effect=analyze_once,
            ):
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": candidates, "negative_funding": []},
                    {
                        item.symbol: (
                            terminal_rows
                            if item.symbol == "P000USDT"
                            else flat_rows
                        )
                        for item in candidates
                    },
                    BASE_TIME + 121 * INTERVAL_MS + 60_000,
                )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(result.passed_signals, [])
            signal = next(
                item
                for item in result.signals
                if item.candidate.symbol == "P000USDT"
            )
            self.assertEqual(signal.reason, "N19_STRUCTURE_CONSUMED")
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT stage,reason,evidence_sha256 FROM "
                        "n19_staircase_states WHERE family_id=?",
                        (durable_terminal.family_id,),
                    ).fetchone(),
                    (
                        durable_terminal.stage,
                        durable_terminal.reason,
                        durable_terminal.evidence_sha256,
                    ),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM "
                        "strategy_passed_structure_ledger "
                        "WHERE strategy_id='N19'"
                    ).fetchone(),
                    (0,),
                )


class N19ExecutionTests(unittest.TestCase):
    def test_published_claim_is_bound_to_exact_n19_plan_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-plan-identity"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            candidates = [candidate("P%03dUSDT" % index, index + 1) for index in range(100)]
            scan_id = recorder.begin_scan(100, candidates, dry_run=True)
            result = StrategyScheduler(
                (N19_STRATEGY,), 96, recorder,
                logging.getLogger("n19-plan-identity"),
            ).evaluate(
                scan_id,
                {"quote_volume_top": candidates, "negative_funding": []},
                {item.symbol: n19_fixture() for item in candidates},
                BASE_TIME + 121 * INTERVAL_MS + 60_000,
            )
            signal = result.passed_signals[0]
            analysis = signal.analysis
            trader = Trader(
                RuleConstrainedClient(leverage=10, tick_size="0.01"),
                live_test_config(str(Path(directory) / "dry-account.json")),
                StateStore(Path(directory) / "position.json"),
                logging.getLogger("n19-plan-identity"),
            )
            plan = trader.build_staircase_exhaustion_margin_capped_trade_plan(
                signal.candidate.symbol,
                analysis.structure.entry.close,
                analysis.structure.x.low,
                Decimal("5"),
                analysis.structure_id,
                entry_min_price=analysis.structure.entry_min,
                entry_max_price=analysis.structure.entry_max,
            )
            plan = replace(
                plan,
                structure_context={
                    "strategy_id": "N19",
                    "rule_version": "N19_V1",
                    "structure_id": analysis.structure_id,
                },
            )
            bound = TradingBot._n19_plan_with_published_identity(signal, plan)
            self.assertEqual(bound.structure_context["signal_id"], signal.signal_id)
            recorder.assert_n19_execution_claim(
                signal.signal_id, signal.candidate.symbol, analysis.structure_id
            )
            with self.assertRaises(_N19PlanIntegrityError):
                TradingBot._n19_plan_with_published_identity(
                    signal, replace(plan, symbol="OTHERUSDT")
                )
            with self.assertRaises(_N19PlanIntegrityError):
                TradingBot._n19_plan_with_published_identity(
                    signal,
                    replace(
                        plan,
                        structure_context={
                            "strategy_id": "N19",
                            "rule_version": "N19_V1",
                            "structure_id": analysis.structure_id,
                            "signal_id": True,
                        },
                    ),
                )

    def test_n19_plan_tick_minimum_maximum_and_actual_five_r_are_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            trader = Trader(
                RuleConstrainedClient(leverage=10, tick_size="0.01"),
                live_test_config(str(Path(directory) / "dry-account.json")),
                StateStore(Path(directory) / "position.json"),
                logging.getLogger("n19-plan"),
            )
            minimum = trader.build_staircase_exhaustion_margin_capped_trade_plan(
                "N19USDT", Decimal("100"), Decimal("99.50"), Decimal("5"),
                "a" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            self.assertEqual(minimum.structure_stop_price, Decimal("99.49"))
            self.assertEqual(minimum.stop_loss_price, Decimal("99.00"))
            self.assertEqual(minimum.stop_loss_pct, Decimal("0.01"))
            self.assertEqual(minimum.take_profit_price, Decimal("105.00"))
            structural = trader.build_staircase_exhaustion_margin_capped_trade_plan(
                "N19USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                "b" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            self.assertEqual(structural.stop_loss_price, Decimal("97.99"))
            actual = trader._execution_plan_from_actual_entry(
                structural, Decimal("100.50")
            )
            self.assertGreaterEqual(
                (actual.take_profit_price - actual.actual_entry_price)
                / (actual.actual_entry_price - actual.stop_loss_price),
                Decimal("5"),
            )
            with self.assertRaisesRegex(BinanceAPIError, "STOP_PCT_OUT_OF_RANGE"):
                trader.build_staircase_exhaustion_margin_capped_trade_plan(
                    "N19USDT", Decimal("100"), Decimal("94"), Decimal("5"),
                    "c" * 24, entry_min_price=Decimal("100"),
                    entry_max_price=Decimal("100.50"),
                )
            for actual_entry in (Decimal("99.99"), Decimal("100.51")):
                with self.subTest(tolerated_fill=actual_entry):
                    tolerated = trader._execution_plan_from_actual_entry(
                        structural, actual_entry
                    )
                    self.assertGreaterEqual(
                        tolerated.risk_reward_ratio, Decimal("5")
                    )
            allowed_min = structural.entry_min_price * Decimal("0.995")
            allowed_max = structural.entry_max_price * Decimal("1.005")
            for actual_entry in (allowed_min, allowed_max):
                with self.subTest(boundary_fill=actual_entry):
                    boundary = trader._execution_plan_from_actual_entry(
                        structural, actual_entry
                    )
                    self.assertGreaterEqual(
                        boundary.risk_reward_ratio, Decimal("5")
                    )
            for actual_entry in (
                allowed_min - Decimal("0.000000000000001"),
                allowed_max + Decimal("0.000000000000001"),
            ):
                with self.subTest(rejected_fill=actual_entry):
                    with self.assertRaisesRegex(
                        BinanceAPIError, "ACTUAL_FILL_OUTSIDE"
                    ):
                        trader._execution_plan_from_actual_entry(
                            structural, actual_entry
                        )
            stepped = Trader(
                RuleConstrainedClient(
                    leverage=10, tick_size="0.01", step_size="0.1"
                ),
                live_test_config(str(Path(directory) / "stepped-account.json")),
                StateStore(Path(directory) / "stepped-position.json"),
                logging.getLogger("n19-plan-step"),
            ).build_staircase_exhaustion_margin_capped_trade_plan(
                "N19USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                "e" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            self.assertEqual(stepped.quantity % Decimal("0.1"), Decimal("0"))
            for client, message in (
                (RuleConstrainedClient(leverage=1, min_qty="10"), "below minQty"),
                (RuleConstrainedClient(leverage=1, min_notional="1000"), "below minNotional"),
            ):
                with self.subTest(exchange_minimum=message), self.assertRaisesRegex(
                    BinanceAPIError, message
                ):
                    Trader(
                        client,
                        live_test_config(
                            str(Path(directory) / (message + "-account.json"))
                        ),
                        StateStore(Path(directory) / (message + "-position.json")),
                        logging.getLogger("n19-plan-minimum"),
                    ).build_staircase_exhaustion_margin_capped_trade_plan(
                        "N19USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                        "f" * 24, entry_min_price=Decimal("100"),
                        entry_max_price=Decimal("100.50"),
                    )

    def test_n19_actual_fill_outside_interval_emergency_closes_without_protection(self):
        with tempfile.TemporaryDirectory() as directory:
            client = FakeLiveExecutionClient({}, leverage=10)
            state = StateStore(Path(directory) / "position.json")
            trader = Trader(
                client,
                live_test_config(str(Path(directory) / "dry-account.json")),
                state,
                logging.getLogger("n19-actual-fill"),
                clock_ms=lambda: 1_720_000_001_000,
            )
            plan = trader.build_staircase_exhaustion_margin_capped_trade_plan(
                "N19USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                "a" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            plan = replace(
                plan,
                entry_candle_open_time_ms=1_720_000_000_000,
                entry_deadline_ms=1_720_000_120_000,
                structure_context={
                    "strategy_id": "N19",
                    "rule_version": "N19_V1",
                    "signal_id": 92,
                    "structure_id": "a" * 24,
                },
            )
            outside_fill = (
                plan.entry_max_price * Decimal("1.005")
                + Decimal("0.000000000000001")
            )
            client.open_response = {
                "orderId": 1,
                "avgPrice": str(outside_fill),
                "executedQty": str(plan.quantity),
            }
            client.position_quantities = [str(plan.quantity), "0"]
            client.close_side_effects = [
                {
                    "status": "FILLED",
                    "executedQty": str(plan.quantity),
                    "orderId": 2,
                }
            ]
            with self.assertRaisesRegex(
                BinanceAPIError, "N19 post-fill adjustment or protection failed"
            ):
                trader.open_long_plan_with_protection(plan)
            self.assertEqual(client.close_calls, [("N19USDT", plan.quantity)])
            self.assertEqual(client.protection_calls, [])
            retained = state.load()
            self.assertIn("execution_cleanup_resolved", retained.orders)
            self.assertTrue(
                retained.orders["execution_cleanup_resolved"]
                ["emergency_cleanup"]["confirmed_closed"]
            )

    def test_n19_journal_and_second_window_check_precede_leverage(self):
        class InitialSaveFailure(StateStore):
            def save(self, _state):
                raise OSError("reservation write failed")

        for mode in ("save", "expired"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                client = FakeLiveExecutionClient({}, leverage=10)
                state = (
                    InitialSaveFailure(Path(directory) / "position.json")
                    if mode == "save"
                    else StateStore(Path(directory) / "position.json")
                )
                times = iter(
                    (1_720_000_119_999,)
                    if mode == "save"
                    else (1_720_000_119_999, 1_720_000_120_000)
                )
                trader = Trader(
                    client,
                    live_test_config(str(Path(directory) / "dry-account.json")),
                    state,
                    logging.getLogger("n19-journal"),
                    clock_ms=lambda: next(times),
                )
                plan = trader.build_staircase_exhaustion_margin_capped_trade_plan(
                    "N19USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                    "d" * 24, entry_min_price=Decimal("99"),
                    entry_max_price=Decimal("101"),
                )
                plan = replace(
                    plan,
                    entry_candle_open_time_ms=1_720_000_000_000,
                    entry_deadline_ms=1_720_000_120_000,
                    structure_context={
                        "strategy_id": "N19",
                        "rule_version": "N19_V1",
                        "signal_id": 91,
                        "structure_id": "d" * 24,
                    },
                )
                expected = BinanceAPIError if mode == "save" else EntryWindowExpiredError
                with self.assertRaises(expected):
                    trader.open_long_plan_with_protection(plan)
                self.assertEqual(client.leverage_calls, [])
                self.assertEqual(client.market_calls, [])
                self.assertIsNone(state.load())

    def test_n19_leverage_failure_retains_recoverable_journal_without_order_replay(self):
        class LeverageFailure(FakeLiveExecutionClient):
            def set_leverage(self, symbol, leverage):
                self.leverage_calls.append((symbol, leverage))
                raise RuntimeError("leverage unavailable")

        with tempfile.TemporaryDirectory() as directory:
            client = LeverageFailure({}, leverage=10)
            state = StateStore(Path(directory) / "position.json")
            trader = Trader(
                client,
                live_test_config(str(Path(directory) / "dry-account.json")),
                state,
                logging.getLogger("n19-leverage-recovery"),
                clock_ms=lambda: 1_720_000_001_000,
            )
            plan = trader.build_staircase_exhaustion_margin_capped_trade_plan(
                "N19USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                "a" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            plan = replace(
                plan,
                entry_candle_open_time_ms=1_720_000_000_000,
                entry_deadline_ms=1_720_000_120_000,
                structure_context={
                    "strategy_id": "N19",
                    "rule_version": "N19_V1",
                    "signal_id": 93,
                    "structure_id": "a" * 24,
                },
            )
            with self.assertRaisesRegex(
                BinanceAPIError, "LEVERAGE_SETUP_FAILED_WITH_PENDING_JOURNAL"
            ):
                trader.open_long_plan_with_protection(plan)
            pending = state.load()
            self.assertEqual(
                pending.orders["execution_pending"]["phase"],
                "MARKET_ORDER_SUBMITTING",
            )
            self.assertEqual(
                pending.orders["execution_pending"]["strategy_id"], "N19"
            )
            self.assertEqual(client.market_calls, [])
            recovery = trader.sync_state_with_exchange()
            self.assertTrue(recovery.execution_pending)
            self.assertFalse(recovery.pending_resolved)
            self.assertEqual(client.market_calls, [])
            self.assertEqual(
                state.load().orders["execution_pending"]["client_order_id"],
                pending.orders["execution_pending"]["client_order_id"],
            )


if __name__ == "__main__":
    unittest.main()
