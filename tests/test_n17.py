from __future__ import annotations

import hashlib
import json
import logging
import os
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from copy import deepcopy
from decimal import Decimal
from dataclasses import replace

import trading_bot.n17_analyzer as n17_module
from trading_bot.coverage_epoch_schema import (
    EPOCH_TABLE_SQL as COVERAGE_EPOCH_TABLE_SQL,
    TRIGGER_SQL as COVERAGE_EPOCH_TRIGGER_SQL,
    coverage_epoch_chain_sha256,
    coverage_epoch_schema_status,
    validate_coverage_epoch_graph,
)

from trading_bot.n17_analyzer import (
    INTERVAL_MS,
    analyze_n17_range_support_rebound,
    decode_n17_state_evidence,
    parse_n17_klines,
    repair_n17_legacy_live_tail_evidence,
    validate_n17_box,
)
from trading_bot.strategies import (
    N16_STRATEGY,
    N17_STRATEGY,
    N17StrategyDefinition,
    N18_STRATEGY,
    N19_STRATEGY,
    N20_STRATEGY,
    load_all_strategies,
)
from trading_bot.monitor import FundingCandidate
from trading_bot.n17_schema import N17_INDEX_SQL
from trading_bot.main import TradingBot, _N17PlanIntegrityError
from trading_bot.recorder import (
    HistoryCoverageProposal,
    PAPER_TRADE_VOID_CONFIRMATION,
    ReviewRecorder,
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
    _repair_n17_frozen_evidence_boundary,
)
from trading_bot.strategy_scheduler import StrategyScheduler
from trading_bot.state import StateStore
from trading_bot.trader import EntryWindowExpiredError, Trader
from trading_bot.binance_client import BinanceAPIError
from tests.recorder_test_utils import make_test_recorder
from tests.test_trader import (
    FakeLiveExecutionClient,
    RuleConstrainedClient,
    live_test_config,
)


BASE_TIME = 1_800_000_000_000 // INTERVAL_MS * INTERVAL_MS


def seed_history_coverage_epoch(
    recorder: ReviewRecorder,
    strategy_id: str,
    symbol: str,
    source_start_time_ms: int,
    covered_through_time_ms: int,
    source_sha256: str,
) -> None:
    proposal = HistoryCoverageProposal(
        strategy_id,
        symbol,
        source_start_time_ms,
        covered_through_time_ms,
        covered_through_time_ms + INTERVAL_MS,
        source_sha256,
    )
    scan_id = recorder.begin_scan(1, [], dry_run=True)
    signal_id = recorder.record_strategy_signal(
        scan_id,
        strategy_id,
        symbol,
        "",
        (),
        "",
        False,
        False,
        "REJECTED",
        f"{strategy_id}_NO_SETUP",
    )
    if type(signal_id) is not int or not recorder.publish_strategy_signal_batch(
        scan_id, 1, (proposal,)
    ):
        raise AssertionError("coverage seed publication failed")


def n17_fixture() -> list[list[str | int]]:
    rows: list[list[str | int]] = []
    wave = (
        Decimal("101.0"),
        Decimal("101.8"),
        Decimal("101.0"),
        Decimal("100.2"),
    )
    for index in range(117):
        close = wave[index % len(wave)]
        open_price = close + (Decimal("0.05") if index % 2 == 0 else Decimal("-0.05"))
        high = max(open_price, close) + Decimal("0.2")
        low = min(open_price, close) - Decimal("0.2")
        rows.append(
            [
                BASE_TIME + index * INTERVAL_MS,
                str(open_price),
                str(high),
                str(low),
                str(close),
                "0",
                BASE_TIME + (index + 1) * INTERVAL_MS - 1,
                "100",
                "0",
                "0",
                "55",
            ]
        )
    for index, values in enumerate(
        (
            ("100.6", "100.9", "99.9", "100.4", "100", "50"),
            ("100.2", "100.7", "100.1", "100.5", "100", "60"),
            ("100.5", "100.9", "100.2", "100.6", "90", "48"),
            ("100.6", "101.2", "100.4", "101.1", "100", "60"),
            ("101.1", "101.35", "100.8", "101.2", "40", "22"),
        ),
        start=117,
    ):
        open_price, high, low, close, quote_volume, taker = values
        rows.append(
            [
                BASE_TIME + index * INTERVAL_MS,
                open_price,
                high,
                low,
                close,
                "0",
                BASE_TIME + (index + 1) * INTERVAL_MS - 1,
                quote_volume,
                "0",
                "0",
                taker,
            ]
        )
    return rows


def analyze(rows: list[list[str | int]] | None = None, **overrides):
    arguments = {
        "quote_volume_rank": 7,
        "checked_at_ms": BASE_TIME + 121 * INTERVAL_MS + 60_000,
    }
    arguments.update(overrides)
    return analyze_n17_range_support_rebound(
        "TESTUSDT", n17_fixture() if rows is None else rows, **arguments
    )


def n17_incremental_lifecycle_series() -> list[list[str | int]]:
    base = n17_fixture()
    series = base[:117]
    while len(series) < 120:
        index = len(series)
        series.append(
            [
                BASE_TIME + index * INTERVAL_MS,
                "100.9",
                "101.2",
                "100.7",
                "101.0",
                "0",
                BASE_TIME + (index + 1) * INTERVAL_MS - 1,
                "100",
                "0",
                "0",
                "55",
            ]
        )
    for offset, source_index in enumerate(
        (117, 118, 119, 120, 121), start=120
    ):
        row = list(base[source_index])
        row[0] = BASE_TIME + offset * INTERVAL_MS
        row[6] = BASE_TIME + (offset + 1) * INTERVAL_MS - 1
        series.append(row)
    return series


def legacy_live_tail_record(symbol: str = "TESTUSDT"):
    fixed = analyze_n17_range_support_rebound(
        symbol,
        n17_fixture(),
        quote_volume_rank=7,
        checked_at_ms=BASE_TIME + 121 * INTERVAL_MS + 60_000,
    ).state_record
    broken = json.loads(fixed.evidence_json)
    confirmation = broken["structure"]["confirmation"]
    source_index = next(
        index for index, row in enumerate(broken["source"])
        if row["open_time_ms"] == confirmation["open_time_ms"]
    )
    broken["source"][source_index] = {
        **confirmation,
        "high": "100.9",
        "low": "100.5",
        "close": "100.8",
        "quote_volume": "70",
        "taker_buy_quote_volume": "40",
    }
    unsigned = dict(broken)
    unsigned.pop("canonical_sha256")
    broken["canonical_sha256"] = hashlib.sha256(
        json.dumps(
            unsigned,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    return replace(fixed, evidence=broken), fixed


class N17DefinitionTests(unittest.TestCase):
    def test_n17_is_independent_and_n01_n16_projection_is_unchanged(self):
        strategies = load_all_strategies()
        self.assertEqual(
            [item.strategy_id for item in strategies],
            ["N%02d" % value for value in range(1, 26)],
        )
        frozen = json.dumps(
            [item.to_jsonable() for item in strategies[:16]],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        # This digest freezes the exact pre-N17 business projection.
        self.assertEqual(
            hashlib.sha256(frozen).hexdigest(),
            "e371c0edf6acc6ee72924bf25b1e8a06ce42492610f3fb21dfa306fb17abe39b",
        )
        self.assertIs(strategies[15], N16_STRATEGY)
        self.assertIs(strategies[16], N17_STRATEGY)
        self.assertIsInstance(N17_STRATEGY, N17StrategyDefinition)
        self.assertEqual(N17_STRATEGY.risk_reward_ratio, Decimal("5"))
        self.assertEqual(N17_STRATEGY.fixed_input_bars, 122)
        self.assertIsNone(N17_STRATEGY.funding_threshold)


class N17AnalyzerTests(unittest.TestCase):
    def test_frozen_evidence_rejects_numeric_type_coercion(self):
        result = analyze()
        self.assertTrue(result.passed)
        for path, replacement in (
            (("schema_version",), True),
            (("schema_version",), 1.0),
            (("quote_volume_rank",), True),
            (("source", -1, "open_time_ms"), 1.0),
            (("structure", "touch", "open_time_ms"), True),
        ):
            with self.subTest(path=path, replacement=replacement):
                evidence = deepcopy(result.state_record.evidence)
                target = evidence
                for component in path[:-1]:
                    target = target[component]
                target[path[-1]] = replacement
                unsigned = dict(evidence)
                unsigned.pop("canonical_sha256", None)
                evidence["canonical_sha256"] = n17_module._sha256_json(unsigned)
                with self.assertRaises(ValueError):
                    decode_n17_state_evidence(evidence)

    def test_valid_range_touch_absorption_confirmation_and_entry_pass(self):
        result = analyze()
        self.assertTrue(result.passed)
        self.assertEqual(result.reason, "PASSED")
        self.assertEqual(result.elapsed_ms, 60_000)
        self.assertEqual(result.structure.box.bars, 64)
        self.assertEqual(result.structure.touch.open_time, str(BASE_TIME + 117 * INTERVAL_MS))
        self.assertEqual(result.structure.absorption.open_time, str(BASE_TIME + 118 * INTERVAL_MS))
        self.assertEqual(result.structure.confirmation.open_time, str(BASE_TIME + 120 * INTERVAL_MS))
        self.assertEqual(result.structure.entry_min, Decimal("101.1"))
        self.assertLessEqual(result.current_price, result.structure.entry_max)
        self.assertEqual(result.state_record.stage, "CONFIRMED")
        self.assertLessEqual(len(result.state_record.evidence_json.encode("utf-8")), 128 * 1024)

    def test_box_uses_longest_valid_adjacent_suffix_and_exact_pivots(self):
        candles = parse_n17_klines(n17_fixture())
        box = validate_n17_box("TESTUSDT", candles[53:117])
        self.assertIsNotNone(box)
        self.assertEqual(box.bars, 64)
        self.assertGreaterEqual(len(box.pivot_highs), 2)
        self.assertGreaterEqual(len(box.pivot_lows), 2)
        self.assertGreaterEqual(len(box.alternating_turns), 4)
        self.assertLessEqual(box.high_spread, box.height * Decimal("0.25"))
        self.assertLessEqual(box.low_spread, box.height * Decimal("0.25"))
        self.assertLessEqual(box.net_move, box.height * Decimal("0.50"))

    def test_definition_mismatch_and_bad_kline_fail_closed(self):
        self.assertEqual(analyze(box_min_bars=21).reason, "N17_DEFINITION_INVALID")
        rows = n17_fixture()
        rows[10][7] = "NaN"
        self.assertEqual(analyze(rows).reason, "N17_KLINE_DATA_INVALID")
        rows = n17_fixture()
        rows[10][0] = int(rows[10][0]) + 1
        self.assertEqual(analyze(rows).reason, "N17_KLINE_DATA_INVALID")

    def test_entry_window_and_closed_price_bounds(self):
        self.assertTrue(
            analyze(checked_at_ms=BASE_TIME + 121 * INTERVAL_MS).passed
        )
        self.assertTrue(
            analyze(checked_at_ms=BASE_TIME + 121 * INTERVAL_MS + 119_999).passed
        )
        self.assertEqual(
            analyze(checked_at_ms=BASE_TIME + 121 * INTERVAL_MS + 120_000).reason,
            "N17_ENTRY_WINDOW_EXPIRED",
        )
        rows = n17_fixture()
        rows[-1][4] = "101.1"
        self.assertTrue(analyze(rows).passed)
        rows = n17_fixture()
        rows[-1][4] = "101.0"
        self.assertEqual(analyze(rows).reason, "N17_ENTRY_BELOW_CONFIRMATION")
        rows = n17_fixture()
        rows[-1][2] = "102"
        rows[-1][4] = "102"
        self.assertEqual(analyze(rows).reason, "N17_ENTRY_PRICE_TOO_EXTENDED")

    def test_n10_n14_and_n08_exclusions(self):
        rows = n17_fixture()
        rows[117][3] = "99.65"
        self.assertEqual(analyze(rows).reason, "N17_N10_BREAKDOWN_EXCLUDED")

        rows = n17_fixture()
        rows[117][1:5] = ["101.2", "101.3", "99.9", "100.1"]
        rows[117][7] = "160"
        rows[117][10] = "60"
        self.assertEqual(analyze(rows).reason, "N17_N14_PANIC_EXCLUDED")

        rows = n17_fixture()
        for index in range(116, 121):
            close = Decimal(str(rows[index][4]))
            rows[index][1] = str(close - Decimal("0.01"))
        self.assertEqual(analyze(rows).reason, "N17_N08_BULLISH_STREAK_EXCLUDED")

    def test_absorption_and_confirmation_are_first_and_strict(self):
        rows = n17_fixture()
        rows[118][3] = "99.89"
        self.assertEqual(analyze(rows).reason, "N17_ABSORPTION_BROKE_TOUCH_LOW")

        rows = n17_fixture()
        rows[119][4] = "101.0"
        rows[119][2] = "101.1"
        rows[119][10] = "60"
        self.assertEqual(analyze(rows).reason, "N17_HISTORICAL_ENTRY_MISSED")

        rows = n17_fixture()
        rows[119][3] = "99.89"
        self.assertEqual(analyze(rows).reason, "N17_CONFIRMATION_BROKE_TOUCH_LOW")

    def test_touch_and_confirmation_threshold_edges(self):
        baseline = analyze()
        lower = baseline.structure.box.lower
        atr_touch = baseline.structure.atr_touch
        rows = n17_fixture()
        rows[117][3] = str(lower * Decimal("0.997"))
        self.assertEqual(analyze(rows).reason, "N17_N10_BREAKDOWN_EXCLUDED")

        rows = n17_fixture()
        rows[117][7] = "250"
        self.assertEqual(analyze(rows).reason, "N17_N10_VOLUME_EXCLUDED")
        rows[117][7] = "249.999"
        self.assertTrue(analyze(rows).passed)

        rows = n17_fixture()
        rows[120][10] = "52"
        self.assertTrue(analyze(rows).passed)
        rows[120][10] = "51.999"
        self.assertEqual(analyze(rows).reason, "N17_CONFIRMATION_NOT_FOUND")
        self.assertGreater(atr_touch, 0)

    def test_absorption_and_confirmation_closed_thresholds(self):
        rows = n17_fixture()
        rows[118][2] = "100.85"
        rows[118][3] = "100.0"
        rows[118][10] = "57.5"
        self.assertTrue(analyze(rows).passed)

        wider = n17_fixture()
        wider[118][2] = "100.851"
        wider[118][3] = "100.0"
        self.assertEqual(analyze(wider).reason, "N17_ABSORPTION_NOT_QUALIFIED")

        sell = n17_fixture()
        sell[118][10] = "57.499"
        self.assertEqual(analyze(sell).reason, "N17_ABSORPTION_NOT_QUALIFIED")

        first = n17_fixture()
        first[119][1:5] = ["100.6", "101.3", "100.4", "101.1"]
        first[119][7] = "80"
        first[119][10] = "41.6"
        result = analyze(first)
        self.assertEqual(result.reason, "N17_HISTORICAL_ENTRY_MISSED")
        self.assertEqual(
            result.structure.confirmation.open_time_ms,
            BASE_TIME + 119 * INTERVAL_MS,
        )

        for field, value in (
            ("taker", "51.999"),
            ("volume", "79.999"),
        ):
            with self.subTest(field=field):
                invalid = n17_fixture()
                if field == "taker":
                    invalid[120][10] = value
                else:
                    invalid[120][7] = value
                self.assertEqual(
                    analyze(invalid).reason, "N17_CONFIRMATION_NOT_FOUND"
                )

    def test_entry_touch_low_and_price_bounds_are_closed(self):
        baseline = analyze()
        rows = n17_fixture()
        rows[-1][3] = str(baseline.structure.touch.low)
        rows[-1][4] = str(baseline.structure.entry_max)
        rows[-1][2] = str(baseline.structure.entry_max + Decimal("0.01"))
        self.assertTrue(analyze(rows).passed)
        rows[-1][3] = str(baseline.structure.touch.low - Decimal("0.001"))
        self.assertEqual(analyze(rows).reason, "N17_ENTRY_BROKE_TOUCH_LOW")

    def test_frozen_family_survives_restart_roll_and_rejects_resigned_conflict(self):
        first = analyze()
        self.assertTrue(first.passed)
        frozen = first.state_record.evidence_json
        same = analyze(frozen_evidence=frozen)
        self.assertTrue(same.passed)
        self.assertEqual(
            same.state_record.evidence_sha256,
            first.state_record.evidence_sha256,
        )
        rolled = n17_fixture()[1:]
        prior = rolled[-1]
        rolled.append(
            [
                int(prior[0]) + INTERVAL_MS,
                "102.1", "102.3", "101.9", "102.2",
                "0", "0", "100", "0", "0", "55",
            ]
        )
        missed = analyze_n17_range_support_rebound(
            "TESTUSDT",
            rolled,
            quote_volume_rank=4,
            checked_at_ms=int(rolled[-1][0]) + 30_000,
            frozen_evidence=frozen,
        )
        self.assertEqual(missed.reason, "N17_HISTORICAL_ENTRY_MISSED")
        tampered = json.loads(frozen)
        tampered["structure"]["confirmation"]["close"] = "102.9"
        unsigned = dict(tampered)
        unsigned.pop("canonical_sha256")
        tampered["canonical_sha256"] = hashlib.sha256(
            json.dumps(
                unsigned,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        invalid = analyze(frozen_evidence=tampered)
        self.assertEqual(invalid.reason, "N17_FROZEN_EVIDENCE_INVALID")
        with self.assertRaisesRegex(ValueError, "duplicate"):
            decode_n17_state_evidence(
                frozen.replace('"schema_version":1', '"schema_version":1,"schema_version":1')
            )

    def test_first_failed_touch_is_consumed_until_proven_reset_and_new_box(self):
        failed_rows = n17_fixture()
        failed_rows[117][4] = "99.9"
        failed = analyze(failed_rows)
        self.assertEqual(failed.reason, "N17_TOUCH_CLOSED_BELOW_LOWER")
        self.assertEqual(failed.state_record.stage, "INVALID")
        replay = analyze(failed_rows, frozen_evidence=failed.state_record.evidence_json)
        self.assertEqual(replay.reason, "N17_STRUCTURE_CONSUMED")

        shifted = n17_fixture()
        shift = 122 * INTERVAL_MS
        for row in shifted:
            row[0] = int(row[0]) + shift
            row[6] = int(row[6]) + shift
        # The first closed bar after the old family visibly crosses its upper
        # tolerance.  The later 64-bar suffix starts after that reset.
        shifted[0][2] = "103"
        shifted[0][4] = "102.5"
        shifted[0][1] = "102.4"
        reset = analyze_n17_range_support_rebound(
            "TESTUSDT",
            shifted,
            quote_volume_rank=7,
            checked_at_ms=BASE_TIME + shift + 121 * INTERVAL_MS + 60_000,
            frozen_evidence=failed.state_record.evidence_json,
        )
        self.assertTrue(reset.passed)
        self.assertNotEqual(reset.structure_id, failed.structure_id)
        self.assertEqual(len(reset.state_records), 2)
        self.assertIsNotNone(reset.state_records[0].reset_after_time_ms)
        self.assertGreater(
            reset.state_records[1].box_start_time_ms,
            reset.state_records[0].reset_after_time_ms,
        )

    def test_incremental_touch_absorption_confirmation_keeps_wilder_seed(self):
        base = n17_fixture()
        wave = base[:117]
        while len(wave) < 120:
            source = [
                BASE_TIME + len(wave) * INTERVAL_MS,
                "100.9", "101.2", "100.7", "101.0", "0",
                BASE_TIME + (len(wave) + 1) * INTERVAL_MS - 1,
                "100", "0", "0", "55",
            ]
            wave.append(source)
        tail = []
        for offset, source_index in enumerate((117, 118, 119, 120, 121), start=120):
            row = list(base[source_index])
            row[0] = BASE_TIME + offset * INTERVAL_MS
            row[6] = BASE_TIME + (offset + 1) * INTERVAL_MS - 1
            tail.append(row)
        series = wave + tail
        evidence = None
        expected = (
            (0, "N17_TOUCH_LOCKED", "TOUCH_LOCKED"),
            (1, "N17_CONFIRMATION_WAITING", "CONFIRMING"),
            (2, "N17_CONFIRMATION_WAITING", "CONFIRMING"),
            (3, "PASSED", "CONFIRMED"),
        )
        for shift, reason, stage in expected:
            window = series[shift : shift + 122]
            result = analyze_n17_range_support_rebound(
                "TESTUSDT",
                window,
                quote_volume_rank=7,
                checked_at_ms=int(window[-1][0]) + 60_000,
                frozen_evidence=evidence,
            )
            self.assertEqual(result.reason, reason)
            self.assertEqual(result.state_record.stage, stage)
            decoded = decode_n17_state_evidence(result.state_record.evidence_json)
            self.assertEqual(decoded.evidence_sha256, result.state_record.evidence_sha256)
            evidence = result.state_record.evidence_json
        self.assertTrue(result.passed)
        self.assertEqual(len(result.state_record.evidence["source"]), 125)

    def test_repeated_confirming_scan_reuses_durable_frozen_record(self):
        series = n17_incremental_lifecycle_series()
        evidence = None
        records = []
        for shift, expected_stage in (
            (0, "TOUCH_LOCKED"),
            (1, "CONFIRMING"),
            (2, "CONFIRMING"),
        ):
            window = series[shift : shift + 122]
            result = analyze_n17_range_support_rebound(
                "TESTUSDT",
                window,
                quote_volume_rank=7 + shift,
                checked_at_ms=int(window[-1][0]) + 60_000,
                frozen_evidence=evidence,
            )
            self.assertEqual(result.state_record.stage, expected_stage)
            records.append(result.state_record)
            evidence = result.state_record.evidence_json

        self.assertEqual(
            records[2].evidence_sha256,
            records[1].evidence_sha256,
        )
        self.assertEqual(records[2].quote_volume_rank, 7)
        self.assertEqual(len(records[2].evidence["source"]), 123)

    def test_live_tail_finalization_keeps_every_lifecycle_stage_reproducible(self):
        base = n17_fixture()
        wave = base[:117]
        while len(wave) < 120:
            index = len(wave)
            wave.append(
                [
                    BASE_TIME + index * INTERVAL_MS,
                    "100.9", "101.2", "100.7", "101.0", "0",
                    BASE_TIME + (index + 1) * INTERVAL_MS - 1,
                    "100", "0", "0", "55",
                ]
            )
        tail = []
        for offset, source_index in enumerate(
            (117, 118, 119, 120, 121), start=120
        ):
            row = list(base[source_index])
            row[0] = BASE_TIME + offset * INTERVAL_MS
            row[6] = BASE_TIME + (offset + 1) * INTERVAL_MS - 1
            tail.append(row)
        series = wave + tail
        provisional = {
            0: ("100.6", "100.2", "100.4", "80", "45"),
            1: ("100.8", "100.3", "100.55", "70", "38"),
            2: ("100.9", "100.5", "100.8", "70", "40"),
        }
        evidence = None
        records = []
        for shift, expected_reason, expected_stage in (
            (0, "N17_TOUCH_LOCKED", "TOUCH_LOCKED"),
            (1, "N17_CONFIRMATION_WAITING", "CONFIRMING"),
            (2, "N17_CONFIRMATION_WAITING", "CONFIRMING"),
            (3, "PASSED", "CONFIRMED"),
        ):
            window = [list(row) for row in series[shift : shift + 122]]
            if shift in provisional:
                high, low, close, volume, taker = provisional[shift]
                window[-1][2:5] = [high, low, close]
                window[-1][7] = volume
                window[-1][10] = taker
            result = analyze_n17_range_support_rebound(
                "TESTUSDT",
                window,
                quote_volume_rank=7,
                checked_at_ms=int(window[-1][0]) + 60_000,
                frozen_evidence=evidence,
            )
            self.assertEqual(result.reason, expected_reason)
            self.assertEqual(result.state_record.stage, expected_stage)
            decoded = decode_n17_state_evidence(
                result.state_record.evidence_json
            )
            self.assertEqual(decoded.evidence_sha256, result.state_record.evidence_sha256)
            records.append(result.state_record)
            evidence = result.state_record.evidence_json

        prior = series[-1]
        series.append(
            [
                int(prior[0]) + INTERVAL_MS,
                "101.2", "101.4", "101.0", "101.3", "0",
                int(prior[0]) + 2 * INTERVAL_MS - 1,
                "100", "0", "0", "55",
            ]
        )
        missed_window = [list(row) for row in series[4:126]]
        missed = analyze_n17_range_support_rebound(
            "TESTUSDT",
            missed_window,
            quote_volume_rank=7,
            checked_at_ms=int(missed_window[-1][0]) + 60_000,
            frozen_evidence=evidence,
        )
        self.assertEqual(missed.reason, "N17_HISTORICAL_ENTRY_MISSED")
        self.assertEqual(
            decode_n17_state_evidence(missed.state_record.evidence_json).stage,
            "MISSED",
        )

        expired = analyze(
            checked_at_ms=BASE_TIME + 121 * INTERVAL_MS + 120_000
        )
        self.assertEqual(expired.state_record.stage, "EXPIRED")
        self.assertEqual(
            decode_n17_state_evidence(expired.state_record.evidence_json).stage,
            "EXPIRED",
        )

        first = records[0]
        for stage, final_values, expected_reason in (
            (
                "CONSUMED",
                ("100.8", "100.0", "100.55", "130", "75"),
                "N17_ABSORPTION_NOT_QUALIFIED",
            ),
            (
                "INVALID",
                ("100.7", "99.8", "100.5", "100", "60"),
                "N17_ABSORPTION_BROKE_TOUCH_LOW",
            ),
        ):
            with self.subTest(stage=stage):
                terminal_series = [list(row) for row in wave + tail]
                high, low, close, volume, taker = final_values
                terminal_series[121][2:5] = [high, low, close]
                terminal_series[121][7] = volume
                terminal_series[121][10] = taker
                window = terminal_series[1:123]
                terminal = analyze_n17_range_support_rebound(
                    "TESTUSDT",
                    window,
                    quote_volume_rank=7,
                    checked_at_ms=int(window[-1][0]) + 60_000,
                    frozen_evidence=first.evidence_json,
                )
                self.assertEqual(terminal.reason, expected_reason)
                self.assertEqual(terminal.state_record.stage, stage)
                self.assertEqual(
                    decode_n17_state_evidence(
                        terminal.state_record.evidence_json
                    ).stage,
                    stage,
                )

        # Recreate the exact legacy writer defect: the finalized absorption is
        # frozen in structure while source retains its earlier live snapshot.
        fixed = terminal
        broken = json.loads(fixed.state_record.evidence_json)
        absorption_time = broken["structure"]["absorption"]["open_time_ms"]
        source_index = next(
            index for index, row in enumerate(broken["source"])
            if row["open_time_ms"] == absorption_time
        )
        broken["source"][source_index] = first.evidence["source"][-1]
        unsigned = dict(broken)
        unsigned.pop("canonical_sha256")
        broken["canonical_sha256"] = hashlib.sha256(
            json.dumps(
                unsigned,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        with self.assertRaisesRegex(ValueError, "cannot be reproduced"):
            decode_n17_state_evidence(broken)
        repaired = repair_n17_legacy_live_tail_evidence(broken)
        self.assertEqual(repaired.evidence_sha256, fixed.state_record.evidence_sha256)

        hostile = json.loads(json.dumps(broken))
        hostile["source"][0]["high"] = str(
            Decimal(hostile["source"][0]["high"]) + Decimal("0.01")
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
        with self.assertRaisesRegex(ValueError, "not uniquely repairable"):
            repair_n17_legacy_live_tail_evidence(hostile)

def candidate(symbol: str, rank: int = 7) -> FundingCandidate:
    return FundingCandidate(
        symbol=symbol,
        funding_rate=None,
        mark_price=Decimal("101.2"),
        quote_volume=Decimal("1000000"),
        quote_volume_rank=rank,
        candidate_universe="quote_volume_top",
    )


class N17RecorderSchedulerTests(unittest.TestCase):
    def test_new_symbol_short_history_is_deferred_without_faking_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("new-symbol-history-readiness"),
            )
            strategies = (
                N17_STRATEGY,
                N18_STRATEGY,
                N19_STRATEGY,
                N20_STRATEGY,
            )
            scheduler = StrategyScheduler(
                strategies,
                96,
                recorder,
                logging.getLogger("new-symbol-history-readiness"),
            )
            symbols = ["S%03dUSDT" % rank for rank in range(1, 100)]
            symbols.append("GRVTUSDT")
            candidates = [
                candidate(symbol, rank)
                for rank, symbol in enumerate(symbols, start=1)
            ]
            start = BASE_TIME - 121 * INTERVAL_MS
            full = [
                [
                    start + index * INTERVAL_MS,
                    "100",
                    "100.2",
                    "99.8",
                    "100",
                    "10",
                    start + (index + 1) * INTERVAL_MS - 1,
                    "1000",
                    100,
                    "100",
                    "500",
                    "0",
                ]
                for index in range(122)
            ]
            raw_by_symbol = {
                symbol: deepcopy(full) for symbol in symbols
            }
            raw_by_symbol["GRVTUSDT"] = deepcopy(full[-40:])
            checked_at = int(full[-1][0]) + 60_000

            scan_one = recorder.begin_scan(100, [], dry_run=True)
            first = scheduler.evaluate(
                scan_one,
                {"quote_volume_top": candidates, "negative_funding": []},
                raw_by_symbol,
                checked_at_ms=checked_at,
            )
            self.assertEqual(first.signal_audit_failures, ())
            self.assertTrue(first.signal_batch_published)
            self.assertEqual(first.passed_signals, [])
            self.assertEqual(first.live_candidates, [])
            self.assertEqual(
                recorder.current_strategy_signal_scan_id(), scan_one
            )
            with recorder._read_only_runtime_snapshot() as connection:
                rows = connection.execute(
                    "SELECT strategy_id,symbol,decision,reason "
                    "FROM strategy_signals WHERE scan_id=? "
                    "ORDER BY strategy_id,symbol",
                    (scan_one,),
                ).fetchall()
                grvt_heads = connection.execute(
                    "SELECT COUNT(*) FROM history_coverage_epoch_heads "
                    "WHERE symbol='GRVTUSDT'"
                ).fetchone()[0]
            self.assertEqual(len(rows), 400)
            self.assertEqual(grvt_heads, 0)
            self.assertEqual(
                {
                    (strategy_id, reason)
                    for strategy_id, symbol, decision, reason in rows
                    if symbol == "GRVTUSDT"
                },
                {
                    ("N17", "N17_HISTORY_SOURCE_INSUFFICIENT"),
                    ("N18", "N18_HISTORY_SOURCE_INSUFFICIENT"),
                    ("N19", "N19_HISTORY_SOURCE_INSUFFICIENT"),
                    ("N20", "N20_HISTORY_SOURCE_INSUFFICIENT"),
                },
            )
            self.assertEqual(
                {
                    reason
                    for strategy_id, symbol, _decision, reason in rows
                    if strategy_id == "N20" and symbol != "GRVTUSDT"
                },
                {"N20_MARKET_CONTEXT_DEFERRED_NEW_MEMBER"},
            )

            raw_by_symbol["GRVTUSDT"] = deepcopy(full)
            scan_two = recorder.begin_scan(100, [], dry_run=True)
            second = scheduler.evaluate(
                scan_two,
                {"quote_volume_top": candidates, "negative_funding": []},
                raw_by_symbol,
                checked_at_ms=checked_at,
            )
            self.assertEqual(second.signal_audit_failures, ())
            self.assertTrue(second.signal_batch_published)
            self.assertEqual(
                recorder.current_strategy_signal_scan_id(), scan_two
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id=?",
                        (scan_two,),
                    ).fetchone(),
                    (400,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM history_coverage_epoch_heads "
                        "WHERE symbol='GRVTUSDT'"
                    ).fetchone(),
                    (3,),
                )

            # Once durable owners exist, a later truncated response is no
            # longer a new-listing warm-up.  It is a continuity inconsistency
            # and keeps the previous CURRENT plus every coverage receipt
            # unchanged.
            with recorder._read_only_runtime_snapshot() as connection:
                protected_before = tuple(connection.execute(
                    "SELECT strategy_id,symbol,epoch_ordinal,"
                    "covered_through_time_ms,source_sha256,"
                    "publication_count,latest_receipt_sha256 "
                    "FROM history_coverage_epoch_heads ORDER BY strategy_id,symbol"
                ).fetchall())
            raw_by_symbol["GRVTUSDT"] = deepcopy(full[-40:])
            scan_three = recorder.begin_scan(100, [], dry_run=True)
            third = scheduler.evaluate(
                scan_three,
                {"quote_volume_top": candidates, "negative_funding": []},
                raw_by_symbol,
                checked_at_ms=checked_at,
            )
            self.assertFalse(third.signal_batch_published)
            self.assertEqual(
                recorder.current_strategy_signal_scan_id(), scan_two
            )
            self.assertTrue({
                "N17_HISTORY_COVERAGE_INCONSISTENT",
                "N18_HISTORY_COVERAGE_INCONSISTENT",
                "N19_HISTORY_COVERAGE_INCONSISTENT",
            }.issubset({failure.code for failure in third.signal_audit_failures}))
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    tuple(connection.execute(
                        "SELECT strategy_id,symbol,epoch_ordinal,"
                        "covered_through_time_ms,source_sha256,"
                        "publication_count,latest_receipt_sha256 "
                        "FROM history_coverage_epoch_heads "
                        "ORDER BY strategy_id,symbol"
                    ).fetchall()),
                    protected_before,
                )

    def test_new_symbol_presence_read_failure_remains_round_wide(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("new-symbol-history-read-failure"),
            )
            scheduler = StrategyScheduler(
                (N17_STRATEGY, N18_STRATEGY, N19_STRATEGY, N20_STRATEGY),
                96,
                recorder,
                logging.getLogger("new-symbol-history-read-failure"),
            )
            symbols = ["S%03dUSDT" % rank for rank in range(1, 100)] + [
                "GRVTUSDT"
            ]
            candidates = [
                candidate(symbol, rank)
                for rank, symbol in enumerate(symbols, start=1)
            ]
            start = BASE_TIME - 121 * INTERVAL_MS
            full = [
                [
                    start + index * INTERVAL_MS,
                    "100", "100.2", "99.8", "100", "10",
                    start + (index + 1) * INTERVAL_MS - 1,
                    "1000", 100, "100", "500", "0",
                ]
                for index in range(122)
            ]
            raw_by_symbol = {
                symbol: deepcopy(full) for symbol in symbols
            }
            raw_by_symbol["GRVTUSDT"] = deepcopy(full[-40:])
            scan_id = recorder.begin_scan(100, [], dry_run=True)
            with patch.object(
                recorder,
                "history_coverage_presence",
                side_effect=sqlite3.OperationalError("injected read failure"),
            ):
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": candidates, "negative_funding": []},
                    raw_by_symbol,
                    checked_at_ms=int(full[-1][0]) + 60_000,
                )
            self.assertNotEqual(result.signal_audit_failures, ())
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])
            self.assertIsNone(recorder.current_strategy_signal_scan_id())
            self.assertIn(
                "N17_HISTORY_COVERAGE_READ_FAILED",
                {failure.code for failure in result.signal_audit_failures},
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state,recorded_count,expected_count "
                        "FROM strategy_signal_batches WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    ("STAGING", 400, None),
                )
                for table in (
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

    def test_explicit_stopped_service_repair_is_unique_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(path, logging.getLogger("n17-repair"))
            broken, fixed = legacy_live_tail_record()
            # The current writer must reject this historical defect.  Inject
            # the row through SQLite only to model an already-deployed legacy
            # generation that predates the write-time decoder gate.
            self.assertEqual(
                recorder.record_n17_state(broken), "N17_STATE_INCONSISTENT"
            )
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    "INSERT INTO n17_range_support_states("
                    "strategy_id,symbol,family_id,structure_id,stage,reason,"
                    "quote_volume_rank,box_start_time_ms,box_end_time_ms,"
                    "reset_after_time_ms,evidence_json,evidence_sha256,"
                    "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        broken.strategy_id,
                        broken.symbol,
                        broken.family_id,
                        broken.structure_id,
                        broken.stage,
                        broken.reason,
                        broken.quote_volume_rank,
                        broken.box_start_time_ms,
                        broken.box_end_time_ms,
                        broken.reset_after_time_ms,
                        broken.evidence_json,
                        broken.evidence_sha256,
                        "2026-07-19T00:00:00+00:00",
                        "2026-07-19T00:00:00+00:00",
                    ),
                )
                connection.commit()
            with self.assertRaisesRegex(ValueError, "cannot be reproduced"):
                decode_n17_state_evidence(broken.evidence_json)

            report = _repair_n17_frozen_evidence_boundary(
                path, Path(recorder.n16_claim_ledger_file)
            )
            self.assertEqual(report.repaired_row_count, 1)
            self.assertEqual(report.resolution, "REPAIRED")
            latest = recorder.get_latest_n17_states({"TESTUSDT"})["TESTUSDT"]
            self.assertEqual(latest.evidence_json, fixed.evidence_json)
            self.assertEqual(
                decode_n17_state_evidence(latest.evidence_json).stage,
                "CONFIRMED",
            )

            replay = _repair_n17_frozen_evidence_boundary(
                path, Path(recorder.n16_claim_ledger_file)
            )
            self.assertEqual(replay.repaired_row_count, 0)
            self.assertEqual(replay.resolution, "ALREADY_CURRENT")

    def test_explicit_n17_upgrade_is_required_and_wrong_schema_is_zero_write(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            ledger = Path(directory) / "claim.sqlite3"
            with closing(sqlite3.connect(database)) as connection:
                connection.commit()
            _install_n16_claim_boundary(database, ledger)
            before = database.read_bytes()
            before_entries = sorted(os.listdir(directory))
            with self.assertRaisesRegex(RuntimeError, "pre-N17"):
                ReviewRecorder(
                    database,
                    logging.getLogger("n17-pre"),
                    n16_claim_ledger_file=ledger,
                )
            self.assertEqual(database.read_bytes(), before)
            self.assertEqual(sorted(os.listdir(directory)), before_entries)

            _install_n17_lifecycle_boundary(database, ledger)
            n17_only = database.read_bytes()
            n17_only_entries = sorted(os.listdir(directory))
            with self.assertRaisesRegex(RuntimeError, "pre-N19"):
                ReviewRecorder(
                    database,
                    logging.getLogger("n17-current-pre-n19"),
                    n16_claim_ledger_file=ledger,
                )
            self.assertEqual(database.read_bytes(), n17_only)
            self.assertEqual(sorted(os.listdir(directory)), n17_only_entries)
            _install_n19_lifecycle_boundary(database, ledger)
            with self.assertRaisesRegex(RuntimeError, "pre-N18"):
                ReviewRecorder(
                    database,
                    logging.getLogger("n17-current-pre-n18"),
                    n16_claim_ledger_file=ledger,
                )
            _install_n18_lifecycle_boundary(database, ledger)
            with self.assertRaisesRegex(RuntimeError, "pre-N20"):
                ReviewRecorder(
                    database,
                    logging.getLogger("n17-current-pre-n20"),
                    n16_claim_ledger_file=ledger,
                )
            _install_n20_lifecycle_boundary(database, ledger)
            _install_micro_lifecycle_boundary(database, ledger)
            _install_coverage_epoch_boundary(database, ledger)
            recorder = ReviewRecorder(
                database,
                logging.getLogger("n17-current"),
                n16_claim_ledger_file=ledger,
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT strategy_id,schema_version,rule_version "
                        "FROM n17_lifecycle_installation"
                    ).fetchone(),
                    ("N17", 1, "N17_V1"),
                )

            with closing(sqlite3.connect(database)) as connection:
                connection.execute("DROP INDEX idx_n17_range_support_structure")
                connection.execute(
                    "CREATE INDEX idx_n17_range_support_structure "
                    "ON n17_range_support_states(symbol,structure_id DESC)"
                )
                connection.commit()
            tampered = database.read_bytes()
            entries = sorted(os.listdir(directory))
            with self.assertRaisesRegex(RuntimeError, "N17"):
                ReviewRecorder(
                    database,
                    logging.getLogger("n17-tamper"),
                    n16_claim_ledger_file=ledger,
                )
            self.assertEqual(database.read_bytes(), tampered)
            self.assertEqual(sorted(os.listdir(directory)), entries)

    def test_every_owned_index_and_dependency_tamper_is_zero_write(self):
        table_for_index = {
            name: (
                "n17_history_coverage"
                if name == "idx_n17_coverage_symbol"
                else "n17_range_support_states"
            )
            for name in N17_INDEX_SQL
        }
        cases = [("index:" + name, name) for name in N17_INDEX_SQL]
        cases += [("foreign_trigger", None), ("incoming_fk", None), ("missing_table", None)]
        for mode, index_name in cases:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                database = Path(directory) / "review.sqlite3"
                ledger = Path(directory) / "claim.sqlite3"
                with closing(sqlite3.connect(database)) as connection:
                    connection.commit()
                _install_n16_claim_boundary(database, ledger)
                _install_n17_lifecycle_boundary(database, ledger)
                with closing(sqlite3.connect(database)) as connection:
                    connection.execute("PRAGMA journal_mode=DELETE")
                    if index_name is not None:
                        table = table_for_index[index_name]
                        connection.execute('DROP INDEX "%s"' % index_name)
                        connection.execute(
                            'CREATE INDEX "%s" ON "%s"(symbol COLLATE NOCASE DESC)'
                            % (index_name, table)
                        )
                    elif mode == "foreign_trigger":
                        connection.execute("CREATE TABLE hostile_probe(value INTEGER)")
                        connection.execute(
                            "CREATE TRIGGER hostile_n17_trigger AFTER UPDATE ON "
                            "n17_range_support_states BEGIN INSERT INTO hostile_probe "
                            "VALUES (1); END"
                        )
                    elif mode == "incoming_fk":
                        connection.execute(
                            "CREATE TABLE hostile_probe(parent_id INTEGER REFERENCES "
                            "n17_range_support_states(id) ON DELETE CASCADE)"
                        )
                    else:
                        connection.execute("DROP TABLE n17_history_coverage")
                    connection.commit()
                before = database.read_bytes()
                entries = sorted(os.listdir(directory))
                with self.assertRaisesRegex(RuntimeError, "N17"):
                    ReviewRecorder(
                        database,
                        logging.getLogger("n17-schema-matrix"),
                        n16_claim_ledger_file=ledger,
                    )
                self.assertEqual(database.read_bytes(), before)
                self.assertEqual(sorted(os.listdir(directory)), entries)

    def test_pre_n17_trace_and_partial_schema_are_rejected_before_write(self):
        for mode in ("event_trace", "partial_schema"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                database = Path(directory) / "review.sqlite3"
                ledger = Path(directory) / "claim.sqlite3"
                with closing(sqlite3.connect(database)) as connection:
                    connection.commit()
                _install_n16_claim_boundary(database, ledger)
                with closing(sqlite3.connect(database)) as connection:
                    connection.execute("PRAGMA journal_mode=DELETE")
                    if mode == "event_trace":
                        connection.execute(
                            "INSERT INTO events(event_type,payload_json,occurred_at) "
                            "VALUES ('ordinary',?,?)",
                            (
                                json.dumps({"strategy_id": "N17"}),
                                "2026-07-17T00:00:00+00:00",
                            ),
                        )
                    else:
                        connection.execute(
                            "CREATE TABLE n17_range_support_states(value INTEGER)"
                        )
                    connection.commit()
                review_before = database.read_bytes()
                ledger_before = ledger.read_bytes()
                entries = sorted(os.listdir(directory))
                with self.assertRaises(SignalRetentionMaintenanceError):
                    _install_n17_lifecycle_boundary(database, ledger)
                self.assertEqual(database.read_bytes(), review_before)
                self.assertEqual(ledger.read_bytes(), ledger_before)
                self.assertEqual(sorted(os.listdir(directory)), entries)

    def test_scheduler_publishes_n17_claim_and_replay_is_consumed(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3", logging.getLogger("n17")
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            scheduler = StrategyScheduler(
                (N17_STRATEGY,), 5, recorder, logging.getLogger("n17-scheduler")
            )
            first_scan = recorder.begin_scan(1, [candidate("TESTUSDT")], True)
            first = scheduler.evaluate(
                first_scan,
                [candidate("TESTUSDT")],
                {"TESTUSDT": n17_fixture()},
                BASE_TIME + 121 * INTERVAL_MS + 60_000,
            )
            self.assertTrue(first.signal_batch_published)
            self.assertEqual(len(first.passed_signals), 1)
            self.assertTrue(first.passed_signals[0].passed)
            published = first.passed_signals[0]
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT stage,reason FROM n17_range_support_states"
                    ).fetchall(),
                    [("CONFIRMED", "PASSED")],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT strategy_id,claim_state FROM "
                        "strategy_passed_structure_ledger"
                    ).fetchall(),
                    [("N17", "ACTIVE")],
                )
            second_scan = recorder.begin_scan(1, [candidate("TESTUSDT")], True)
            second = scheduler.evaluate(
                second_scan,
                [candidate("TESTUSDT")],
                {"TESTUSDT": n17_fixture()},
                BASE_TIME + 121 * INTERVAL_MS + 70_000,
            )
            self.assertTrue(second.signal_batch_published)
            self.assertEqual(second.passed_signals, [])
            self.assertEqual(second.signals[0].reason, "N17_STRUCTURE_CONSUMED")
            recorder.assert_n17_execution_claim(
                published.signal_id,
                published.candidate.symbol,
                published.analysis.structure_id,
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertIsNone(connection.execute(
                    "SELECT 1 FROM strategy_signals WHERE id=?",
                    (published.signal_id,),
                ).fetchone())
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                        "WHERE strategy_id='N17'"
                    ).fetchone(),
                    (1,),
                )
            with recorder._read_only_runtime_snapshot() as connection:
                evidence_json = connection.execute(
                    "SELECT evidence_json FROM n17_range_support_states "
                    "WHERE strategy_id='N17' AND structure_id=?",
                    (published.analysis.structure_id,),
                ).fetchone()[0]
            evidence = json.loads(evidence_json)
            evidence["stage"] = "MISSED"
            # N17 execution claims are stable CONFIRMED/PASSED.  Even a
            # self-consistent component hash must not turn an unrelated pair
            # into a legal execution derivation.
            evidence["reason"] = "PASSED"
            unsigned = dict(evidence)
            unsigned.pop("canonical_sha256", None)
            evidence["canonical_sha256"] = n17_module._sha256_json(unsigned)
            evidence_json = n17_module._canonical_json(evidence)
            # Corrupt the installed graph through an external connection.  A
            # production ReviewRecorder write must now reject this commit at
            # its pre-commit ledger/catalog boundary.
            with closing(sqlite3.connect(recorder.db_file)) as connection, connection:
                connection.execute(
                    "UPDATE n17_range_support_states SET stage='MISSED',"
                    "reason='PASSED',evidence_json=?,evidence_sha256=? "
                    "WHERE strategy_id='N17' AND structure_id=?",
                    (
                        evidence_json,
                        hashlib.sha256(evidence_json.encode("utf-8")).hexdigest(),
                        published.analysis.structure_id,
                    ),
                )
            with self.assertRaisesRegex(RuntimeError, "claim graph conflicts"):
                recorder.assert_n17_execution_claim(
                    published.signal_id,
                    published.candidate.symbol,
                    published.analysis.structure_id,
                )

    def test_legacy_invalid_frozen_evidence_blocks_the_entire_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(path, logging.getLogger("n17-gate"))
            recorder.upsert_strategy_definitions(load_all_strategies())
            broken, _fixed = legacy_live_tail_record()
            with closing(sqlite3.connect(path)) as connection:
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
                        "2026-07-19T00:00:00+00:00",
                        "2026-07-19T00:00:00+00:00",
                    ),
                )
                connection.commit()
            scheduler = StrategyScheduler(
                (N17_STRATEGY,), 5, recorder, logging.getLogger("n17-gate")
            )
            scan_id = recorder.begin_scan(1, [candidate("TESTUSDT")], True)
            result = scheduler.evaluate(
                scan_id,
                [candidate("TESTUSDT")],
                {"TESTUSDT": n17_fixture()},
                BASE_TIME + 121 * INTERVAL_MS + 70_000,
            )
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.signals[0].reason, "N17_FROZEN_EVIDENCE_INVALID")
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id FROM strategy_signal_current "
                        "WHERE singleton_id=1"
                    ).fetchone(),
                    (None,),
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

    def test_repeated_confirming_state_publishes_across_restart_and_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                path, logging.getLogger("n17-repeat-confirming")
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            scheduler = StrategyScheduler(
                (N17_STRATEGY,), 5, recorder,
                logging.getLogger("n17-repeat-confirming"),
            )
            series = n17_incremental_lifecycle_series()
            current_scans = []
            for shift in (0, 1, 2, 2, 2, 2, 2, 2):
                item = candidate("TESTUSDT", 7 + shift)
                scan_id = recorder.begin_scan(1, [item], True)
                result = scheduler.evaluate(
                    scan_id,
                    [item],
                    {"TESTUSDT": series[shift : shift + 122]},
                    int(series[shift + 121][0]) + 60_000,
                )
                self.assertTrue(result.signal_batch_published)
                self.assertEqual(result.passed_signals, [])
                self.assertNotEqual(
                    result.signals[0].reason, "N17_STATE_INCONSISTENT"
                )
                current_scans.append(scan_id)

            frozen = recorder.get_latest_n17_states({"TESTUSDT"})[
                "TESTUSDT"
            ]
            self.assertEqual(frozen.stage, "CONFIRMING")
            self.assertEqual(frozen.quote_volume_rank, 7)
            before_failure = (
                frozen.evidence_sha256,
                frozen.updated_at,
                recorder.current_strategy_signal_scan_id(),
            )
            failed_scan = recorder.begin_scan(
                1, [candidate("TESTUSDT", 10)], True
            )
            with patch.object(
                recorder, "publish_strategy_signal_batch", return_value=False
            ):
                failed = scheduler.evaluate(
                    failed_scan,
                    [candidate("TESTUSDT", 10)],
                    {"TESTUSDT": series[2:124]},
                    int(series[123][0]) + 90_000,
                )
            self.assertFalse(failed.signal_batch_published)
            self.assertEqual(failed.passed_signals, [])
            after_failure = recorder.get_latest_n17_states({"TESTUSDT"})[
                "TESTUSDT"
            ]
            self.assertEqual(
                (after_failure.evidence_sha256, after_failure.updated_at),
                before_failure[:2],
            )
            self.assertEqual(
                recorder.current_strategy_signal_scan_id(), before_failure[2]
            )

            restarted = ReviewRecorder(
                path,
                logging.getLogger("n17-repeat-confirming-restart"),
                n16_claim_ledger_file=recorder.n16_claim_ledger_file,
            )
            restarted.upsert_strategy_definitions(load_all_strategies())
            restarted_scheduler = StrategyScheduler(
                (N17_STRATEGY,), 5, restarted,
                logging.getLogger("n17-repeat-confirming-restart"),
            )
            item = candidate("TESTUSDT", 11)
            restart_scan = restarted.begin_scan(1, [item], True)
            replay = restarted_scheduler.evaluate(
                restart_scan,
                [item],
                {"TESTUSDT": series[2:124]},
                int(series[123][0]) + 100_000,
            )
            self.assertTrue(replay.signal_batch_published)
            self.assertEqual(replay.passed_signals, [])
            with restarted._read_only_runtime_snapshot() as connection:
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

    def test_scheduler_restart_persists_reset_before_new_family(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(path, logging.getLogger("n17-reset"))
            recorder.upsert_strategy_definitions(load_all_strategies())
            scheduler = StrategyScheduler(
                (N17_STRATEGY,), 5, recorder, logging.getLogger("n17-reset")
            )
            failed_rows = n17_fixture()
            failed_rows[117][4] = "99.9"
            first_scan = recorder.begin_scan(1, [candidate("TESTUSDT")], True)
            first = scheduler.evaluate(
                first_scan,
                [candidate("TESTUSDT")],
                {"TESTUSDT": failed_rows},
                BASE_TIME + 121 * INTERVAL_MS + 60_000,
            )
            self.assertTrue(first.signal_batch_published)
            self.assertEqual(first.signals[0].reason, "N17_TOUCH_CLOSED_BELOW_LOWER")

            restarted = ReviewRecorder(
                path,
                logging.getLogger("n17-reset-restart"),
                n16_claim_ledger_file=recorder.n16_claim_ledger_file,
            )
            restarted.upsert_strategy_definitions(load_all_strategies())
            previous = restarted.get_latest_n17_states({"TESTUSDT"})[
                "TESTUSDT"
            ]
            shifted = n17_fixture()
            shift = 122 * INTERVAL_MS
            for row in shifted:
                row[0] = int(row[0]) + shift
                row[6] = int(row[6]) + shift
            shifted[0][1:5] = ["102.4", "103", "102.2", "102.5"]
            second = analyze_n17_range_support_rebound(
                "TESTUSDT",
                shifted,
                quote_volume_rank=7,
                checked_at_ms=int(shifted[-1][0]) + 60_000,
                frozen_evidence=previous.evidence_json,
            )
            self.assertTrue(second.passed)
            self.assertEqual(
                [restarted.record_n17_state(item) for item in second.state_records],
                ["UPDATED", "INSERTED"],
            )
            with restarted._read_only_runtime_snapshot() as connection:
                rows = connection.execute(
                    "SELECT stage,reset_after_time_ms FROM "
                    "n17_range_support_states ORDER BY id"
                ).fetchall()
                self.assertEqual(len(rows), 2)
                self.assertIsNotNone(rows[0][1])
                self.assertEqual(rows[1][0], "CONFIRMED")

    def test_n17_representative_is_deterministic_and_never_falls_back(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3", logging.getLogger("n17-rank")
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            scheduler = StrategyScheduler(
                (N17_STRATEGY,), 5, recorder, logging.getLogger("n17-rank")
            )
            candidates = [candidate("AAAUSDT", 9), candidate("BBBUSDT", 2)]
            scan_id = recorder.begin_scan(2, candidates, True)
            result = scheduler.evaluate(
                scan_id,
                candidates,
                {"AAAUSDT": n17_fixture(), "BBBUSDT": n17_fixture()},
                BASE_TIME + 121 * INTERVAL_MS + 60_000,
            )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual([item.candidate.symbol for item in result.passed_signals], ["BBBUSDT"])
            self.assertEqual(
                {item.candidate.symbol: item.reason for item in result.signals},
                {"AAAUSDT": "N17_NOT_REPRESENTATIVE", "BBBUSDT": "PASSED"},
            )

        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3", logging.getLogger("n17-no-fallback")
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            scheduler = StrategyScheduler(
                (N17_STRATEGY,), 5, recorder, logging.getLogger("n17-no-fallback")
            )
            candidates = [candidate("AAAUSDT", 9), candidate("BBBUSDT", 2)]
            original = recorder.record_n17_state

            def fail_winner(record):
                if record.symbol == "BBBUSDT":
                    return "N17_STATE_PERSIST_FAILED"
                return original(record)

            scan_id = recorder.begin_scan(2, candidates, True)
            with patch.object(recorder, "record_n17_state", side_effect=fail_winner):
                result = scheduler.evaluate(
                    scan_id,
                    candidates,
                    {"AAAUSDT": n17_fixture(), "BBBUSDT": n17_fixture()},
                    BASE_TIME + 121 * INTERVAL_MS + 60_000,
                )
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(
                {item.candidate.symbol: item.reason for item in result.signals},
                {
                    "AAAUSDT": "N17_NOT_REPRESENTATIVE",
                    "BBBUSDT": "N17_STATE_PERSIST_FAILED",
                },
            )

    def test_consumed_confirmed_n17_claim_does_not_block_new_coverage_epoch(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3", logging.getLogger("n17-gap")
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            scheduler = StrategyScheduler(
                (N17_STRATEGY,), 5, recorder, logging.getLogger("n17-gap")
            )
            first_scan = recorder.begin_scan(1, [candidate("TESTUSDT")], True)
            first = scheduler.evaluate(
                first_scan,
                [candidate("TESTUSDT")],
                {"TESTUSDT": n17_fixture()},
                BASE_TIME + 121 * INTERVAL_MS + 60_000,
            )
            self.assertTrue(first.signal_batch_published)

            jumped = n17_fixture()
            jump = 124 * INTERVAL_MS
            for row in jumped:
                row[0] = int(row[0]) + jump
                row[6] = int(row[6]) + jump
            second_scan = recorder.begin_scan(1, [candidate("TESTUSDT")], True)
            second = scheduler.evaluate(
                second_scan,
                [candidate("TESTUSDT")],
                {"TESTUSDT": jumped},
                int(jumped[-1][0]) + 60_000,
            )
            self.assertTrue(second.signal_batch_published)
            self.assertEqual(recorder.get_active_n17_states(), [])
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id FROM strategy_signal_current "
                        "WHERE singleton_id=1"
                    ).fetchone(),
                    (second_scan,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT epoch_ordinal FROM "
                        "history_coverage_epoch_heads "
                        "WHERE strategy_id='N17' AND symbol='TESTUSDT'"
                    ).fetchone(),
                    (2,),
                )

    def test_inactive_history_gaps_are_witnessed_and_published_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("coverage-epoch"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            base = BASE_TIME
            old_covered = base + 120 * INTERVAL_MS
            new_start = old_covered + 2 * INTERVAL_MS
            proposals = tuple(
                HistoryCoverageProposal(
                    strategy_id,
                    f"{strategy_id}USDT",
                    new_start,
                    new_start + 120 * INTERVAL_MS,
                    new_start + 121 * INTERVAL_MS,
                    hashlib.sha256(strategy_id.encode("ascii")).hexdigest(),
                )
                for strategy_id in ("N17", "N18", "N19")
            )
            for proposal in proposals:
                seed_history_coverage_epoch(
                    recorder,
                    proposal.strategy_id,
                    proposal.symbol,
                    base,
                    old_covered,
                    hashlib.sha256(
                        (proposal.strategy_id + "-old").encode("ascii")
                    ).hexdigest(),
                )
                self.assertEqual(
                    recorder.prepare_history_coverage_proposal(proposal),
                    "GAP",
                )

            scan_id = recorder.begin_scan(3, [], dry_run=True)
            for proposal in proposals:
                self.assertIsInstance(
                    recorder.record_strategy_signal(
                        scan_id,
                        proposal.strategy_id,
                        proposal.symbol,
                        "",
                        (),
                        "",
                        False,
                        False,
                        "REJECTED",
                        f"{proposal.strategy_id}_NO_SETUP",
                    ),
                    int,
                )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(scan_id, 3, proposals)
            )
            with recorder._read_only_runtime_snapshot() as connection:
                epochs = connection.execute(
                    "SELECT strategy_id,symbol,epoch_ordinal,"
                    "prior_covered_through_time_ms,source_start_time_ms,"
                    "source_scan_id FROM history_coverage_epoch_chain "
                    "WHERE epoch_ordinal=2 ORDER BY strategy_id"
                ).fetchall()
                self.assertEqual(len(epochs), 3)
                for proposal, epoch in zip(proposals, epochs):
                    self.assertEqual(
                        tuple(epoch),
                        (
                            proposal.strategy_id,
                            proposal.symbol,
                            2,
                            old_covered,
                            new_start,
                            scan_id,
                        ),
                    )
                for proposal in proposals:
                    table = f"{proposal.strategy_id.lower()}_history_coverage"
                    self.assertEqual(
                        connection.execute(
                            f"SELECT source_start_time_ms,"
                            f"covered_through_time_ms,source_sha256 "
                            f"FROM {table} WHERE strategy_id=? AND symbol=?",
                            (proposal.strategy_id, proposal.symbol),
                        ).fetchone(),
                        (
                            base,
                            proposal.covered_through_time_ms,
                            proposal.source_sha256,
                        ),
                    )

    def test_three_strategy_top100_gap_epoch_publishes_without_backfill(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("coverage-top100-gap"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            candidates = [
                candidate(f"G{index:03d}USDT", rank=index + 1)
                for index in range(100)
            ]
            old_start = BASE_TIME - 300 * INTERVAL_MS
            old_covered = old_start + 120 * INTERVAL_MS
            old_hash = hashlib.sha256(b"old-window").hexdigest()
            for strategy_id in ("N17", "N18", "N19"):
                for item in candidates:
                    seed_history_coverage_epoch(
                        recorder,
                        strategy_id,
                        item.symbol,
                        old_start,
                        old_covered,
                        old_hash,
                    )

            new_start = BASE_TIME
            neutral: list[list[str | int]] = []
            for index in range(122):
                neutral.append(
                    [
                        new_start + index * INTERVAL_MS,
                        "100",
                        "100.1",
                        "99.9",
                        "100",
                        "1",
                        new_start + (index + 1) * INTERVAL_MS - 1,
                        "100",
                        "10",
                        "0",
                        "50",
                    ]
                )
            raw = {item.symbol: deepcopy(neutral) for item in candidates}
            scheduler = StrategyScheduler(
                (N17_STRATEGY, N18_STRATEGY, N19_STRATEGY),
                96,
                recorder,
                logging.getLogger("coverage-top100-gap"),
            )
            scan_id = recorder.begin_scan(100, candidates, dry_run=True)
            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": candidates, "negative_funding": []},
                raw,
                new_start + 121 * INTERVAL_MS + 60_000,
            )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(len(result.signals), 300)
            self.assertFalse(result.passed_signals)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM history_coverage_epoch_chain "
                        "WHERE epoch_ordinal=2"
                    ).fetchone(),
                    (300,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id FROM strategy_signal_current "
                        "WHERE singleton_id=1"
                    ).fetchone(),
                    (scan_id,),
                )

    def test_gap_witness_failure_rolls_back_coverage_and_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("coverage-gap-rollback"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            base = BASE_TIME
            old_covered = base + 120 * INTERVAL_MS
            new_start = old_covered + 2 * INTERVAL_MS
            proposals = tuple(
                HistoryCoverageProposal(
                    strategy_id,
                    f"{strategy_id}FAILUSDT",
                    new_start,
                    new_start + 120 * INTERVAL_MS,
                    new_start + 121 * INTERVAL_MS,
                    hashlib.sha256(
                        (strategy_id + "-new").encode("ascii")
                    ).hexdigest(),
                )
                for strategy_id in ("N17", "N18", "N19")
            )
            old_rows: dict[str, tuple[int, int, str]] = {}
            for proposal in proposals:
                old_hash = hashlib.sha256(
                    (proposal.strategy_id + "-old").encode("ascii")
                ).hexdigest()
                old_rows[proposal.strategy_id] = (base, old_covered, old_hash)
                seed_history_coverage_epoch(
                    recorder,
                    proposal.strategy_id,
                    proposal.symbol,
                    base,
                    old_covered,
                    old_hash,
                )
            scan_id = recorder.begin_scan(3, [], dry_run=True)
            for proposal in proposals:
                self.assertIsInstance(
                    recorder.record_strategy_signal(
                        scan_id,
                        proposal.strategy_id,
                        proposal.symbol,
                        "",
                        (),
                        "",
                        False,
                        False,
                        "REJECTED",
                        f"{proposal.strategy_id}_NO_SETUP",
                    ),
                    int,
                )
            with patch.object(
                __import__(
                    "trading_bot.coverage_epoch_schema",
                    fromlist=["coverage_epoch_chain_sha256"],
                ),
                "coverage_epoch_chain_sha256",
                side_effect=RuntimeError("injected epoch chain failure"),
            ):
                self.assertFalse(
                    recorder.publish_strategy_signal_batch(scan_id, 3, proposals)
                )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state FROM strategy_signal_batches WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    ("STAGING",),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM history_coverage_epoch_chain "
                        "WHERE epoch_ordinal > 1"
                    ).fetchone(),
                    (0,),
                )
                for proposal in proposals:
                    table = f"{proposal.strategy_id.lower()}_history_coverage"
                    self.assertEqual(
                        connection.execute(
                            f"SELECT source_start_time_ms,"
                            f"covered_through_time_ms,source_sha256 "
                            f"FROM {table} WHERE strategy_id=? AND symbol=?",
                            (proposal.strategy_id, proposal.symbol),
                        ).fetchone(),
                        old_rows[proposal.strategy_id],
                    )

    def test_coverage_read_error_is_distinct_from_a_proven_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("coverage-read-error"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            scheduler = StrategyScheduler(
                (N17_STRATEGY,),
                96,
                recorder,
                logging.getLogger("coverage-read-error"),
            )
            item = candidate("READFAILUSDT", rank=1)
            scan_id = recorder.begin_scan(1, [item], dry_run=True)
            with patch.object(
                recorder,
                "prepare_history_coverage_proposal",
                side_effect=sqlite3.OperationalError("injected read failure"),
            ):
                result = scheduler.evaluate(
                    scan_id,
                    [item],
                    {item.symbol: n17_fixture()},
                    BASE_TIME + 121 * INTERVAL_MS + 60_000,
                )
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(
                result.signals[0].reason,
                "N17_HISTORY_COVERAGE_READ_FAILED",
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n17_history_coverage"
                    ).fetchone(),
                    (0,),
                )

    def test_coverage_epoch_requires_explicit_upgrade_and_is_immutable(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            ledger = Path(directory) / "claim-ledger.sqlite3"
            with closing(sqlite3.connect(database)) as connection:
                connection.commit()
            _install_n16_claim_boundary(database, ledger)
            _install_n17_lifecycle_boundary(database, ledger)
            _install_n19_lifecycle_boundary(database, ledger)
            _install_n18_lifecycle_boundary(database, ledger)
            _install_n20_lifecycle_boundary(database, ledger)
            _install_micro_lifecycle_boundary(database, ledger)
            old_start = BASE_TIME - 300 * INTERVAL_MS
            old_covered = old_start + 120 * INTERVAL_MS
            old_hash = hashlib.sha256(b"production-old-window").hexdigest()
            with closing(sqlite3.connect(database)) as connection:
                for strategy_id in ("N17", "N18", "N19"):
                    connection.execute(
                        f"INSERT INTO {strategy_id.lower()}_history_coverage "
                        "VALUES (?,?,?,?,?,?)",
                        (
                            strategy_id,
                            f"{strategy_id}USDT",
                            old_start,
                            old_covered,
                            old_hash,
                            "2026-07-23T00:00:00+00:00",
                        ),
                    )
                connection.commit()
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                connection.execute("PRAGMA journal_mode=DELETE")
            before = hashlib.sha256(database.read_bytes()).hexdigest()
            before_files = {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in database.parent.iterdir()
                if path.is_file()
            }
            with self.assertRaisesRegex(
                RuntimeError, "coverage epoch schema requires explicit"
            ):
                ReviewRecorder(
                    database,
                    logging.getLogger("pre-epoch-runtime"),
                    n16_claim_ledger_file=ledger,
                )
            self.assertEqual(
                hashlib.sha256(database.read_bytes()).hexdigest(), before
            )
            self.assertEqual(
                {
                    path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in database.parent.iterdir()
                    if path.is_file()
                },
                before_files,
            )

            _install_coverage_epoch_boundary(database, ledger)
            recorder = ReviewRecorder(
                database,
                logging.getLogger("epoch-runtime"),
                n16_claim_ledger_file=ledger,
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(coverage_epoch_schema_status(connection), "CURRENT")
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM history_coverage_epoch_chain"
                    ).fetchone(),
                    (3,),
                )
                validate_coverage_epoch_graph(connection)
            with recorder._connect() as connection:
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute(
                        "DELETE FROM history_coverage_epoch_chain "
                        "WHERE strategy_id='N17'"
                    )
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute(
                        "UPDATE history_coverage_epoch_chain "
                        "SET initial_source_sha256=? WHERE strategy_id='N17'",
                        (hashlib.sha256(b"tampered").hexdigest(),),
                    )
            with recorder._read_only_runtime_snapshot() as connection:
                validate_coverage_epoch_graph(connection)

    def test_coverage_epoch_chain_detects_deleted_or_reordered_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("epoch-chain-tamper"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            symbol = "CHAINUSDT"
            old_start = BASE_TIME - 300 * INTERVAL_MS
            old_covered = old_start + 120 * INTERVAL_MS
            seed_history_coverage_epoch(
                recorder,
                "N17",
                symbol,
                old_start,
                old_covered,
                hashlib.sha256(b"chain-old").hexdigest(),
            )
            new_start = BASE_TIME
            proposal = HistoryCoverageProposal(
                "N17",
                symbol,
                new_start,
                new_start + 120 * INTERVAL_MS,
                new_start + 121 * INTERVAL_MS,
                hashlib.sha256(b"chain-new").hexdigest(),
            )
            scan_id = recorder.begin_scan(1, [], dry_run=True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    scan_id, "N17", symbol, "", (), "", False, False,
                    "REJECTED", "N17_NO_SETUP",
                ),
                int,
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(scan_id, 1, (proposal,))
            )
            # Deliberately bypass the runtime pre-commit graph gate so the
            # subsequent read/restart paths can prove that corruption is
            # detected independently.
            with closing(sqlite3.connect(recorder.db_file)) as connection, connection:
                connection.execute(
                    'DROP TRIGGER "trg_history_coverage_epoch_no_delete"'
                )
                connection.execute(
                    "DELETE FROM history_coverage_epoch_chain "
                    "WHERE strategy_id='N17' AND symbol=? AND epoch_ordinal=2",
                    (symbol,),
                )
                connection.execute(
                    COVERAGE_EPOCH_TRIGGER_SQL[
                        "trg_history_coverage_epoch_no_delete"
                    ]
                )
            with recorder._read_only_runtime_snapshot() as connection:
                with self.assertRaisesRegex(
                    RuntimeError, "chain length conflicts"
                ):
                    validate_coverage_epoch_graph(connection)
            with self.assertRaises(RuntimeError):
                ReviewRecorder(
                    recorder.db_file,
                    logging.getLogger("epoch-chain-restart"),
                    n16_claim_ledger_file=recorder.n16_claim_ledger_file,
                )

    def test_n17_active_predicate_and_gap_publication_are_identical(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n17-active-predicate"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            symbol = "ACTIVEUSDT"
            old_start = BASE_TIME
            seed_history_coverage_epoch(
                recorder,
                "N17",
                symbol,
                old_start,
                old_start + 120 * INTERVAL_MS,
                hashlib.sha256(b"active-old").hexdigest(),
            )
            active = analyze_n17_range_support_rebound(
                symbol,
                n17_fixture(),
                checked_at_ms=BASE_TIME + 121 * INTERVAL_MS + 60_000,
                quote_volume_rank=1,
            )
            self.assertIsNotNone(active.state_record)
            self.assertEqual(recorder.record_n17_state(active.state_record), "INSERTED")
            proposal = HistoryCoverageProposal(
                "N17",
                symbol,
                old_start + 300 * INTERVAL_MS,
                old_start + 420 * INTERVAL_MS,
                old_start + 421 * INTERVAL_MS,
                hashlib.sha256(b"active-new").hexdigest(),
            )
            self.assertEqual(
                recorder.get_required_n17_family_symbols(), (symbol,)
            )
            self.assertEqual(
                recorder.prepare_history_coverage_proposal(proposal),
                "GAP_BLOCKED",
            )
            scan_id = recorder.begin_scan(1, [], True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    scan_id, "N17", symbol, "", (), "", False, False,
                    "REJECTED", "N17_NO_SETUP",
                ),
                int,
            )
            self.assertFalse(
                recorder.publish_strategy_signal_batch(
                    scan_id, 1, (proposal,)
                )
            )

    def test_epoch_writes_require_publication_and_current_retry_attests_graph(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database, logging.getLogger("epoch-publication-auth")
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            proposal = HistoryCoverageProposal(
                "N17",
                "AUTHUSDT",
                BASE_TIME,
                BASE_TIME + 120 * INTERVAL_MS,
                BASE_TIME + 121 * INTERVAL_MS,
                hashlib.sha256(b"auth-source").hexdigest(),
            )
            scan_id = recorder.begin_scan(1, [], True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    scan_id, "N17", "AUTHUSDT", "", (), "", False, False,
                    "REJECTED", "N17_NO_SETUP",
                ),
                int,
            )
            self.assertFalse(recorder.publish_strategy_signal_batch(scan_id, 1))
            extra = replace(
                proposal,
                symbol="EXTRAUSDT",
                source_sha256=hashlib.sha256(b"extra-source").hexdigest(),
            )
            self.assertFalse(
                recorder.publish_strategy_signal_batch(
                    scan_id, 1, (proposal, extra)
                )
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    scan_id, 1, (proposal,)
                )
            )
            protected_tables = (
                "history_coverage_epoch_chain",
                "history_coverage_epoch_heads",
                "n17_history_coverage",
                "history_coverage_epoch_installation",
                "history_coverage_publication_receipts",
            )
            with recorder._read_only_runtime_snapshot() as connection:
                protected_before = {
                    table: connection.execute(
                        f"SELECT * FROM {table} ORDER BY 1,2"
                    ).fetchall()
                    for table in protected_tables
                }
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    scan_id, 1, (proposal,)
                )
            )

            with closing(sqlite3.connect(database)) as connection:
                attacks = (
                    "INSERT OR REPLACE INTO history_coverage_epoch_chain "
                    "SELECT strategy_id,symbol,epoch_ordinal,"
                    "source_start_time_ms,initial_covered_through_time_ms,"
                    "initial_source_sha256,prior_covered_through_time_ms,"
                    "prior_source_sha256,previous_chain_sha256,chain_sha256,"
                    "source_scan_id,'replacement-time' "
                    "FROM history_coverage_epoch_chain "
                    "WHERE strategy_id='N17' AND symbol='AUTHUSDT'",
                    "UPDATE history_coverage_epoch_heads SET updated_at='x' "
                    "WHERE strategy_id='N17' AND symbol='AUTHUSDT'",
                    "INSERT OR REPLACE INTO history_coverage_epoch_heads "
                    "SELECT strategy_id,symbol,epoch_ordinal,"
                    "epoch_start_time_ms,covered_through_time_ms,"
                    "source_sha256,chain_head_sha256,publication_count,"
                    "latest_receipt_sha256,'replacement-time' "
                    "FROM history_coverage_epoch_heads "
                    "WHERE strategy_id='N17' AND symbol='AUTHUSDT'",
                    "UPDATE n17_history_coverage SET updated_at='x' "
                    "WHERE strategy_id='N17' AND symbol='AUTHUSDT'",
                    "INSERT OR REPLACE INTO n17_history_coverage "
                    "SELECT strategy_id,symbol,source_start_time_ms,"
                    "covered_through_time_ms,source_sha256,"
                    "'replacement-time' FROM n17_history_coverage "
                    "WHERE strategy_id='N17' AND symbol='AUTHUSDT'",
                    "INSERT OR REPLACE INTO "
                    "history_coverage_publication_receipts "
                    "SELECT source_scan_id,strategy_id,symbol,"
                    "source_start_time_ms,covered_through_time_ms,"
                    "source_sha256,result_epoch_ordinal,"
                    "result_epoch_start_time_ms,result_chain_head_sha256,"
                    "publication_ordinal,previous_receipt_sha256,"
                    "batch_expected_count,batch_manifest_sha256,"
                    "receipt_sha256,'replacement-time' FROM "
                    "history_coverage_publication_receipts "
                    "WHERE strategy_id='N17' AND symbol='AUTHUSDT'",
                    "INSERT OR REPLACE INTO history_coverage_epoch_installation "
                    "SELECT singleton_id,schema_version,rule_version,"
                    "catalog_schema_version,catalog_sha256,"
                    "'replacement-time' FROM "
                    "history_coverage_epoch_installation",
                )
                for statement in attacks:
                    with self.assertRaises(sqlite3.DatabaseError):
                        connection.execute(statement)
                connection.rollback()

            with recorder._read_only_runtime_snapshot() as connection:
                manifest = connection.execute(
                    "SELECT manifest_sha256 FROM strategy_signal_batches "
                    "WHERE scan_id=?",
                    (scan_id,),
                ).fetchone()[0]
            recorder._coverage_epoch_mutation_scope = (
                "PUBLISH",
                scan_id,
                1,
                manifest,
                frozenset({("N17", "AUTHUSDT")}),
            )
            try:
                with recorder._connect() as connection:
                    authorized_attacks = (
                        "INSERT OR REPLACE INTO history_coverage_epoch_chain "
                        "SELECT strategy_id,symbol,epoch_ordinal,"
                        "source_start_time_ms,initial_covered_through_time_ms,"
                        "initial_source_sha256,prior_covered_through_time_ms,"
                        "prior_source_sha256,previous_chain_sha256,"
                        "chain_sha256,source_scan_id,'replacement-time' "
                        "FROM history_coverage_epoch_chain "
                        "WHERE strategy_id='N17' AND symbol='AUTHUSDT'",
                        "INSERT OR REPLACE INTO history_coverage_epoch_heads "
                        "SELECT strategy_id,symbol,epoch_ordinal,"
                        "epoch_start_time_ms,covered_through_time_ms,"
                        "source_sha256,chain_head_sha256,publication_count,"
                        "latest_receipt_sha256,'replacement-time' "
                        "FROM history_coverage_epoch_heads "
                        "WHERE strategy_id='N17' AND symbol='AUTHUSDT'",
                        "INSERT OR REPLACE INTO n17_history_coverage "
                        "SELECT strategy_id,symbol,source_start_time_ms,"
                        "covered_through_time_ms,source_sha256,"
                        "'replacement-time' FROM n17_history_coverage "
                        "WHERE strategy_id='N17' AND symbol='AUTHUSDT'",
                        "INSERT OR REPLACE INTO "
                        "history_coverage_publication_receipts "
                        "SELECT source_scan_id,strategy_id,symbol,"
                        "source_start_time_ms,covered_through_time_ms,"
                        "source_sha256,result_epoch_ordinal,"
                        "result_epoch_start_time_ms,"
                        "result_chain_head_sha256,publication_ordinal,"
                        "previous_receipt_sha256,batch_expected_count,"
                        "batch_manifest_sha256,receipt_sha256,"
                        "'replacement-time' FROM "
                        "history_coverage_publication_receipts "
                        "WHERE strategy_id='N17' AND symbol='AUTHUSDT'",
                    )
                    for statement in authorized_attacks:
                        with self.assertRaises(sqlite3.DatabaseError):
                            connection.execute(statement)
                        connection.rollback()
            finally:
                recorder._coverage_epoch_mutation_scope = None
            recorder._coverage_epoch_maintenance_install = True
            recorder._coverage_epoch_mutation_scope = (
                "INSTALL", None, None, None, frozenset()
            )
            try:
                with recorder._connect() as connection:
                    with self.assertRaises(sqlite3.DatabaseError):
                        connection.execute(
                            "INSERT OR REPLACE INTO "
                            "history_coverage_epoch_installation "
                            "SELECT singleton_id,schema_version,rule_version,"
                            "catalog_schema_version,catalog_sha256,"
                            "'replacement-time' FROM "
                            "history_coverage_epoch_installation"
                        )
            finally:
                recorder._coverage_epoch_mutation_scope = None
                recorder._coverage_epoch_maintenance_install = False
            with recorder._read_only_runtime_snapshot() as connection:
                validate_coverage_epoch_graph(connection)
                self.assertEqual(
                    {
                        table: connection.execute(
                            f"SELECT * FROM {table} ORDER BY 1,2"
                        ).fetchall()
                        for table in protected_tables
                    },
                    protected_before,
                )

            orphan_scan = recorder.begin_scan(1, [], True)
            orphan_sha = coverage_epoch_chain_sha256(
                "N17", "ORPHANUSDT", 1, BASE_TIME,
                BASE_TIME + 120 * INTERVAL_MS,
                "c" * 64, None, None, None, orphan_scan,
            )
            recorder._coverage_epoch_mutation_scope = (
                "PUBLISH",
                orphan_scan,
                1,
                "d" * 64,
                frozenset({("N17", "ORPHANUSDT")}),
            )
            try:
                with closing(sqlite3.connect(database)) as connection, connection:
                    connection.create_function(
                        "_coverage_epoch_mutation_authorized",
                        6,
                        recorder._authorize_coverage_epoch_mutation,
                    )
                    connection.execute(
                        "INSERT INTO history_coverage_epoch_chain VALUES "
                        "(?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            "N17", "ORPHANUSDT", 1, BASE_TIME,
                            BASE_TIME + 120 * INTERVAL_MS, "c" * 64,
                            None, None, None, orphan_sha, orphan_scan, "x",
                        ),
                    )
            finally:
                recorder._coverage_epoch_mutation_scope = None
            self.assertFalse(
                recorder.publish_strategy_signal_batch(
                    scan_id, 1, (proposal,)
                )
            )

    def test_new_contiguous_and_gap_each_seal_a_permanent_publication_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database, logging.getLogger("epoch-receipt-chain")
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            symbol = "RECEIPTUSDT"
            proposals = (
                HistoryCoverageProposal(
                    "N17",
                    symbol,
                    BASE_TIME,
                    BASE_TIME + 120 * INTERVAL_MS,
                    BASE_TIME + 121 * INTERVAL_MS,
                    hashlib.sha256(b"receipt-new").hexdigest(),
                ),
                HistoryCoverageProposal(
                    "N17",
                    symbol,
                    BASE_TIME + INTERVAL_MS,
                    BASE_TIME + 121 * INTERVAL_MS,
                    BASE_TIME + 122 * INTERVAL_MS,
                    hashlib.sha256(b"receipt-contiguous").hexdigest(),
                ),
                HistoryCoverageProposal(
                    "N17",
                    symbol,
                    BASE_TIME + 300 * INTERVAL_MS,
                    BASE_TIME + 420 * INTERVAL_MS,
                    BASE_TIME + 421 * INTERVAL_MS,
                    hashlib.sha256(b"receipt-gap").hexdigest(),
                ),
            )
            scan_ids: list[int] = []
            manifests: list[str] = []
            for expected_publication_ordinal, proposal in enumerate(
                proposals, 1
            ):
                scan_id = recorder.begin_scan(1, [], True)
                scan_ids.append(scan_id)
                self.assertIsInstance(
                    recorder.record_strategy_signal(
                        scan_id, "N17", symbol, "", (), "", False, False,
                        "REJECTED", "N17_NO_SETUP",
                    ),
                    int,
                )
                self.assertTrue(
                    recorder.publish_strategy_signal_batch(
                        scan_id, 1, (proposal,)
                    )
                )
                with recorder._read_only_runtime_snapshot() as connection:
                    manifest = connection.execute(
                        "SELECT manifest_sha256 FROM strategy_signal_batches "
                        "WHERE scan_id=? AND state='CURRENT'",
                        (scan_id,),
                    ).fetchone()[0]
                    manifests.append(manifest)
                    receipt = connection.execute(
                        "SELECT publication_ordinal,previous_receipt_sha256,"
                        "batch_expected_count,batch_manifest_sha256,"
                        "receipt_sha256 FROM "
                        "history_coverage_publication_receipts "
                        "WHERE source_scan_id=? AND strategy_id='N17' "
                        "AND symbol=?",
                        (scan_id, symbol),
                    ).fetchone()
                    self.assertIsNotNone(receipt)
                    self.assertEqual(
                        receipt[0], expected_publication_ordinal
                    )
                    self.assertEqual(receipt[2:4], (1, manifest))
                    if expected_publication_ordinal == 1:
                        self.assertIsNone(receipt[1])
                    else:
                        previous = connection.execute(
                            "SELECT receipt_sha256 FROM "
                            "history_coverage_publication_receipts "
                            "WHERE source_scan_id=? AND strategy_id='N17' "
                            "AND symbol=?",
                            (scan_ids[-2], symbol),
                        ).fetchone()[0]
                        self.assertEqual(receipt[1], previous)
                    self.assertEqual(
                        connection.execute(
                            "SELECT publication_count,"
                            "latest_receipt_sha256 FROM "
                            "history_coverage_epoch_heads "
                            "WHERE strategy_id='N17' AND symbol=?",
                            (symbol,),
                        ).fetchone(),
                        (expected_publication_ordinal, receipt[4]),
                    )
                    validate_coverage_epoch_graph(connection)

            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signal_batches "
                        "WHERE scan_id=?",
                        (scan_ids[1],),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id=?",
                        (scan_ids[1],),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT batch_expected_count,batch_manifest_sha256 "
                        "FROM history_coverage_publication_receipts "
                        "WHERE source_scan_id=? AND strategy_id='N17' "
                        "AND symbol=?",
                        (scan_ids[1], symbol),
                    ).fetchone(),
                    (1, manifests[1]),
                )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    scan_ids[-1], 1, (proposals[-1],)
                )
            )

            # Bypass the application pre-commit graph gate to model an
            # independently corrupted permanent receipt chain.
            with closing(sqlite3.connect(database)) as connection, connection:
                connection.execute(
                    'DROP TRIGGER "trg_history_coverage_receipt_no_delete"'
                )
                connection.execute(
                    "DELETE FROM history_coverage_publication_receipts "
                    "WHERE source_scan_id=? AND strategy_id='N17' "
                    "AND symbol=?",
                    (scan_ids[1], symbol),
                )
                connection.execute(
                    COVERAGE_EPOCH_TRIGGER_SQL[
                        "trg_history_coverage_receipt_no_delete"
                    ]
                )
            self.assertFalse(
                recorder.publish_strategy_signal_batch(
                    scan_ids[-1], 1, (proposals[-1],)
                )
            )
            with self.assertRaises(RuntimeError):
                ReviewRecorder(
                    database,
                    logging.getLogger("epoch-receipt-restart"),
                    n16_claim_ledger_file=recorder.n16_claim_ledger_file,
                )

    def test_contiguous_receipt_failure_rolls_back_head_mirror_and_current(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("epoch-contiguous-receipt-rollback"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            symbol = "ROLLBACKUSDT"
            first = HistoryCoverageProposal(
                "N17", symbol, BASE_TIME,
                BASE_TIME + 120 * INTERVAL_MS,
                BASE_TIME + 121 * INTERVAL_MS,
                hashlib.sha256(b"rollback-first").hexdigest(),
            )
            first_scan = recorder.begin_scan(1, [], True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    first_scan, "N17", symbol, "", (), "", False, False,
                    "REJECTED", "N17_NO_SETUP",
                ),
                int,
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    first_scan, 1, (first,)
                )
            )
            second = replace(
                first,
                source_start_time_ms=BASE_TIME + INTERVAL_MS,
                covered_through_time_ms=BASE_TIME + 121 * INTERVAL_MS,
                current_open_time_ms=BASE_TIME + 122 * INTERVAL_MS,
                source_sha256=hashlib.sha256(
                    b"rollback-contiguous"
                ).hexdigest(),
            )
            second_scan = recorder.begin_scan(1, [], True)
            self.assertIsInstance(
                recorder.record_strategy_signal(
                    second_scan, "N17", symbol, "", (), "", False, False,
                    "REJECTED", "N17_NO_SETUP",
                ),
                int,
            )
            with recorder._read_only_runtime_snapshot() as connection:
                head_before = connection.execute(
                    "SELECT * FROM history_coverage_epoch_heads "
                    "WHERE strategy_id='N17' AND symbol=?",
                    (symbol,),
                ).fetchone()
                mirror_before = connection.execute(
                    "SELECT * FROM n17_history_coverage "
                    "WHERE strategy_id='N17' AND symbol=?",
                    (symbol,),
                ).fetchone()
            with patch(
                "trading_bot.coverage_epoch_schema."
                "coverage_publication_receipt_sha256",
                side_effect=RuntimeError("injected receipt failure"),
            ):
                self.assertFalse(
                    recorder.publish_strategy_signal_batch(
                        second_scan, 1, (second,)
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
                self.assertEqual(
                    connection.execute(
                        "SELECT state FROM strategy_signal_batches "
                        "WHERE scan_id=?",
                        (second_scan,),
                    ).fetchone(),
                    ("STAGING",),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM history_coverage_epoch_heads "
                        "WHERE strategy_id='N17' AND symbol=?",
                        (symbol,),
                    ).fetchone(),
                    head_before,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM n17_history_coverage "
                        "WHERE strategy_id='N17' AND symbol=?",
                        (symbol,),
                    ).fetchone(),
                    mirror_before,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM "
                        "history_coverage_publication_receipts "
                        "WHERE strategy_id='N17' AND symbol=?",
                        (symbol,),
                    ).fetchone(),
                    (1,),
                )
                validate_coverage_epoch_graph(connection)

    def test_identical_coverage_is_no_change_and_hot_publish_is_owner_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("coverage-no-change"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            proposals = tuple(
                HistoryCoverageProposal(
                    "N17",
                    f"NC{index:03d}USDT",
                    BASE_TIME,
                    BASE_TIME + 120 * INTERVAL_MS,
                    BASE_TIME + 121 * INTERVAL_MS,
                    hashlib.sha256(
                        f"no-change-{index}".encode("ascii")
                    ).hexdigest(),
                )
                for index in range(100)
            )

            def publish(items):
                scan_id = recorder.begin_scan(len(items), [], True)
                for proposal in items:
                    self.assertIsInstance(
                        recorder.record_strategy_signal(
                            scan_id,
                            proposal.strategy_id,
                            proposal.symbol,
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
                        scan_id, len(items), tuple(items)
                    )
                )
                return scan_id

            publish(proposals)
            self.assertEqual(
                recorder.prepare_history_coverage_proposal(proposals[0]),
                "NO_CHANGE",
            )
            with recorder._read_only_runtime_snapshot() as connection:
                before_heads = connection.execute(
                    "SELECT * FROM history_coverage_epoch_heads "
                    "ORDER BY strategy_id,symbol"
                ).fetchall()
                before_mirrors = connection.execute(
                    "SELECT * FROM n17_history_coverage ORDER BY symbol"
                ).fetchall()
            with patch(
                "trading_bot.coverage_epoch_schema."
                "validate_coverage_epoch_graph",
                side_effect=AssertionError(
                    "hot publication walked the permanent history"
                ),
            ) as full_graph:
                last_no_change_scan = None
                for _ in range(14):
                    last_no_change_scan = publish(proposals)
            self.assertEqual(full_graph.call_count, 0)
            self.assertTrue(
                recorder.publish_strategy_signal_batch(
                    last_no_change_scan, len(proposals), proposals
                )
            )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM "
                        "history_coverage_publication_receipts"
                    ).fetchone(),
                    (100,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT MIN(publication_count),"
                        "MAX(publication_count) FROM "
                        "history_coverage_epoch_heads"
                    ).fetchone(),
                    (1, 1),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM history_coverage_epoch_heads "
                        "ORDER BY strategy_id,symbol"
                    ).fetchall(),
                    before_heads,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM n17_history_coverage ORDER BY symbol"
                    ).fetchall(),
                    before_mirrors,
                )
                validate_coverage_epoch_graph(connection)

            changed = replace(
                proposals[0],
                source_start_time_ms=BASE_TIME + INTERVAL_MS,
                covered_through_time_ms=BASE_TIME + 121 * INTERVAL_MS,
                current_open_time_ms=BASE_TIME + 122 * INTERVAL_MS,
                source_sha256=hashlib.sha256(
                    b"no-change-next-window"
                ).hexdigest(),
            )
            publish((changed,))
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM "
                        "history_coverage_publication_receipts "
                        "WHERE strategy_id='N17' AND symbol=?",
                        (changed.symbol,),
                    ).fetchone(),
                    (2,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT publication_count FROM "
                        "history_coverage_epoch_heads "
                        "WHERE strategy_id='N17' AND symbol=?",
                        (changed.symbol,),
                    ).fetchone(),
                    (2,),
                )
                validate_coverage_epoch_graph(connection)

    def test_coverage_hash_freezes_closed_prefix_not_the_live_tail(self):
        rows = n17_fixture()
        first = StrategyScheduler._history_coverage_proposal(
            "N17", "TAILUSDT", rows, 122
        )
        changed_tail = deepcopy(rows)
        changed_tail[-1][2] = str(Decimal(changed_tail[-1][2]) + Decimal("1"))
        changed_tail[-1][4] = str(
            Decimal(changed_tail[-1][4]) + Decimal("0.1")
        )
        same_coverage = StrategyScheduler._history_coverage_proposal(
            "N17", "TAILUSDT", changed_tail, 122
        )
        self.assertEqual(
            (
                first.source_start_time_ms,
                first.covered_through_time_ms,
                first.source_sha256,
            ),
            (
                same_coverage.source_start_time_ms,
                same_coverage.covered_through_time_ms,
                same_coverage.source_sha256,
            ),
        )
        next_tail = list(changed_tail[-1])
        next_tail[0] = int(next_tail[0]) + INTERVAL_MS
        shifted = changed_tail[1:] + [next_tail]
        next_coverage = StrategyScheduler._history_coverage_proposal(
            "N17", "TAILUSDT", shifted, 122
        )
        self.assertNotEqual(
            next_coverage.source_sha256, first.source_sha256
        )
        self.assertEqual(
            next_coverage.covered_through_time_ms,
            first.covered_through_time_ms + INTERVAL_MS,
        )

    def test_coverage_epoch_partial_or_catalog_drift_is_zero_write_rejected(self):
        attacks = {
            "missing_trigger": (
                'DROP TRIGGER "trg_history_coverage_epoch_no_delete"',
            ),
            "same_name_wrong_index": (
                'DROP INDEX "idx_history_coverage_epoch_head"',
                "CREATE UNIQUE INDEX idx_history_coverage_epoch_head "
                "ON history_coverage_epoch_heads(symbol,strategy_id)",
            ),
        }
        for name, statements in attacks.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as root:
                database = Path(root) / "review.sqlite3"
                ledger = Path(root) / "n16_claim_ledger.sqlite3"
                recorder = make_test_recorder(
                    database,
                    logging.getLogger("coverage-epoch-catalog-" + name),
                    n16_claim_ledger_file=ledger,
                )
                with recorder._connect() as connection:
                    for statement in statements:
                        connection.execute(statement)
                with closing(sqlite3.connect(database)) as connection:
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    connection.execute("PRAGMA journal_mode=DELETE")
                before_database = database.read_bytes()
                before_ledger = ledger.read_bytes()
                before_names = sorted(os.listdir(root))
                with self.assertRaisesRegex(RuntimeError, "coverage epoch"):
                    ReviewRecorder(
                        database,
                        logging.getLogger("coverage-epoch-restart-" + name),
                        n16_claim_ledger_file=ledger,
                    )
                self.assertEqual(database.read_bytes(), before_database)
                self.assertEqual(ledger.read_bytes(), before_ledger)
                self.assertEqual(sorted(os.listdir(root)), before_names)

        with tempfile.TemporaryDirectory() as root:
            database = Path(root) / "review.sqlite3"
            ledger = Path(root) / "n16_claim_ledger.sqlite3"
            with closing(sqlite3.connect(database)) as connection:
                connection.commit()
            _install_n16_claim_boundary(database, ledger)
            _install_n17_lifecycle_boundary(database, ledger)
            _install_n19_lifecycle_boundary(database, ledger)
            _install_n18_lifecycle_boundary(database, ledger)
            _install_n20_lifecycle_boundary(database, ledger)
            _install_micro_lifecycle_boundary(database, ledger)
            with closing(sqlite3.connect(database)) as connection:
                connection.execute(COVERAGE_EPOCH_TABLE_SQL)
                connection.commit()
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                connection.execute("PRAGMA journal_mode=DELETE")
            before_database = database.read_bytes()
            before_ledger = ledger.read_bytes()
            before_names = sorted(os.listdir(root))
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "coverage epoch"
            ):
                _install_coverage_epoch_boundary(database, ledger)
            self.assertEqual(database.read_bytes(), before_database)
            self.assertEqual(ledger.read_bytes(), before_ledger)
            self.assertEqual(sorted(os.listdir(root)), before_names)

    def test_multiple_coverage_gaps_form_one_ordered_chain(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("multiple-coverage-gaps"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            symbol = "MULTIGAPUSDT"
            start1 = BASE_TIME - 600 * INTERVAL_MS
            covered1 = start1 + 120 * INTERVAL_MS
            seed_history_coverage_epoch(
                recorder,
                "N17",
                symbol,
                start1,
                covered1,
                hashlib.sha256(b"epoch-one").hexdigest(),
            )

            def publish_window(start: int, marker: bytes) -> None:
                proposal = HistoryCoverageProposal(
                    "N17",
                    symbol,
                    start,
                    start + 120 * INTERVAL_MS,
                    start + 121 * INTERVAL_MS,
                    hashlib.sha256(marker).hexdigest(),
                )
                scan_id = recorder.begin_scan(1, [], dry_run=True)
                self.assertIsInstance(
                    recorder.record_strategy_signal(
                        scan_id, "N17", symbol, "", (), "", False, False,
                        "REJECTED", "N17_NO_SETUP",
                    ),
                    int,
                )
                self.assertTrue(
                    recorder.publish_strategy_signal_batch(
                        scan_id, 1, (proposal,)
                    )
                )

            start2 = covered1 + 2 * INTERVAL_MS
            publish_window(start2, b"epoch-two")
            start2_contiguous = start2 + INTERVAL_MS
            publish_window(start2_contiguous, b"epoch-two-next")
            covered2 = start2_contiguous + 120 * INTERVAL_MS
            start3 = covered2 + 3 * INTERVAL_MS
            publish_window(start3, b"epoch-three")

            with recorder._read_only_runtime_snapshot() as connection:
                validate_coverage_epoch_graph(connection)
                rows = connection.execute(
                    "SELECT epoch_ordinal,source_start_time_ms,"
                    "prior_covered_through_time_ms,previous_chain_sha256,"
                    "chain_sha256 FROM history_coverage_epoch_chain "
                    "WHERE strategy_id='N17' AND symbol=? "
                    "ORDER BY epoch_ordinal",
                    (symbol,),
                ).fetchall()
                self.assertEqual([row[0] for row in rows], [1, 2, 3])
                self.assertEqual(rows[1][1], start2)
                self.assertEqual(rows[2][1], start3)
                self.assertEqual(rows[1][3], rows[0][4])
                self.assertEqual(rows[2][3], rows[1][4])
                self.assertEqual(rows[2][2], covered2)

    def test_legacy_paper_results_update_statistics_without_qualification(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3", logging.getLogger("n17-paper")
            )
            for symbol in ("AAAUSDT", "BBBUSDT"):
                trade_id = recorder.open_strategy_paper_trade(
                    "N17", symbol, "100", "99", "105", "", {}, {}
                )
                self.assertIsInstance(trade_id, int)
                self.assertTrue(
                    recorder.close_strategy_paper_trade(
                        trade_id, "WIN", "TAKE_PROFIT", "105", "5"
                    )
                )
            state = recorder.get_strategy_state("N17")
            self.assertEqual(state.consecutive_wins, 0)
            self.assertEqual(state.paper_trade_count, 2)
            self.assertEqual(state.win_count, 2)
            self.assertEqual(state.loss_count, 0)
            self.assertFalse(state.live_eligible)

            void_id = recorder.open_strategy_paper_trade(
                "N17", "VOIDUSDT", "100", "99", "105", "", {}, {}
            )
            self.assertTrue(
                recorder.void_strategy_paper_trade(
                    void_id,
                    "N17",
                    "VOIDUSDT",
                    PAPER_TRADE_VOID_CONFIRMATION,
                )
            )
            after_void = recorder.get_strategy_state("N17")
            self.assertEqual(after_void.consecutive_wins, 0)
            self.assertEqual(after_void.paper_trade_count, 2)
            self.assertEqual(after_void.win_count, 2)
            self.assertEqual(after_void.loss_count, 0)
            self.assertFalse(after_void.live_eligible)

            loss_id = recorder.open_strategy_paper_trade(
                "N17", "LOSSUSDT", "100", "99", "105", "", {}, {}
            )
            self.assertTrue(
                recorder.close_strategy_paper_trade(
                    loss_id, "LOSS", "STOP_LOSS", "99", "-1"
                )
            )
            reset = recorder.get_strategy_state("N17")
            self.assertEqual(reset.consecutive_wins, 0)
            self.assertEqual(reset.paper_trade_count, 3)
            self.assertEqual(reset.win_count, 2)
            self.assertEqual(reset.loss_count, 1)
            self.assertFalse(reset.live_eligible)
            self.assertEqual(reset.last_trade_result, "LOSS")


class N17ExecutionTests(unittest.TestCase):
    def test_published_claim_is_bound_to_the_exact_n17_plan_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3", logging.getLogger("n17-plan-id")
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            scheduler = StrategyScheduler(
                (N17_STRATEGY,), 5, recorder, logging.getLogger("n17-plan-id")
            )
            scan_id = recorder.begin_scan(1, [candidate("TESTUSDT")], True)
            result = scheduler.evaluate(
                scan_id,
                [candidate("TESTUSDT")],
                {"TESTUSDT": n17_fixture()},
                BASE_TIME + 121 * INTERVAL_MS + 60_000,
            )
            signal = result.passed_signals[0]
            analysis = signal.analysis
            trader = Trader(
                RuleConstrainedClient(leverage=10, tick_size="0.01"),
                live_test_config(str(Path(directory) / "dry-account.json")),
                StateStore(Path(directory) / "position.json"),
                logging.getLogger("n17-plan-id"),
            )
            plan = trader.build_range_support_absorption_margin_capped_trade_plan(
                "TESTUSDT",
                analysis.structure.entry.close,
                analysis.structure.touch.low,
                Decimal("5"),
                analysis.structure_id,
                entry_min_price=analysis.structure.entry_min,
                entry_max_price=analysis.structure.entry_max,
            )
            plan = replace(
                plan,
                structure_context={
                    "strategy_id": "N17",
                    "rule_version": "N17_V1",
                    "structure_id": analysis.structure_id,
                },
            )
            bound = TradingBot._n17_plan_with_published_identity(signal, plan)
            self.assertEqual(bound.structure_context["signal_id"], signal.signal_id)
            recorder.assert_n17_execution_claim(
                signal.signal_id,
                "TESTUSDT",
                analysis.structure_id,
            )
            with self.assertRaises(_N17PlanIntegrityError):
                TradingBot._n17_plan_with_published_identity(
                    signal, replace(plan, symbol="OTHERUSDT")
                )
            with self.assertRaises(_N17PlanIntegrityError):
                TradingBot._n17_plan_with_published_identity(
                    signal,
                    replace(
                        plan,
                        structure_context={
                            "strategy_id": "N17",
                            "rule_version": "N17_V1",
                            "structure_id": analysis.structure_id,
                            "signal_id": True,
                        },
                    ),
                )

    def test_n17_plan_uses_tick_below_touch_then_exact_one_to_five(self):
        with tempfile.TemporaryDirectory() as root:
            trader = Trader(
                RuleConstrainedClient(leverage=10, tick_size="0.01"),
                live_test_config(str(Path(root) / "dry-account.json")),
                StateStore(Path(root) / "position.json"),
                logging.getLogger("n17-plan"),
            )
            minimum = trader.build_range_support_absorption_margin_capped_trade_plan(
                "N17USDT", Decimal("100"), Decimal("99.50"), Decimal("5"),
                "a" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            self.assertEqual(minimum.stop_loss_price, Decimal("99.00"))
            self.assertEqual(minimum.take_profit_price, Decimal("105.00"))
            self.assertEqual(minimum.stop_loss_pct, Decimal("0.01"))
            structural = trader.build_range_support_absorption_margin_capped_trade_plan(
                "N17USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                "b" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            self.assertEqual(structural.stop_loss_price, Decimal("97.99"))
            self.assertGreaterEqual(
                (structural.take_profit_price - structural.entry_price)
                / (structural.entry_price - structural.stop_loss_price),
                Decimal("5"),
            )
            with self.assertRaisesRegex(BinanceAPIError, "STOP_PCT_OUT_OF_RANGE"):
                trader.build_range_support_absorption_margin_capped_trade_plan(
                    "N17USDT", Decimal("100"), Decimal("94"), Decimal("5"),
                    "c" * 24, entry_min_price=Decimal("100"),
                    entry_max_price=Decimal("100.50"),
                )
            tolerated_fill = trader._execution_plan_from_actual_entry(
                structural, Decimal("100.51")
            )
            self.assertGreaterEqual(
                tolerated_fill.risk_reward_ratio, Decimal("5")
            )
            allowed_min = structural.entry_min_price * Decimal("0.995")
            allowed_max = structural.entry_max_price * Decimal("1.005")
            for actual_entry in (allowed_min, allowed_max):
                with self.subTest(actual_entry=actual_entry):
                    boundary_fill = trader._execution_plan_from_actual_entry(
                        structural, actual_entry
                    )
                    self.assertGreaterEqual(
                        boundary_fill.risk_reward_ratio, Decimal("5")
                    )
            for actual_entry in (
                allowed_min - Decimal("0.000000000000001"),
                allowed_max + Decimal("0.000000000000001"),
            ):
                with self.subTest(actual_entry=actual_entry):
                    with self.assertRaisesRegex(
                        BinanceAPIError, "ACTUAL_FILL_OUTSIDE"
                    ):
                        trader._execution_plan_from_actual_entry(
                            structural, actual_entry
                        )

    def test_n17_journal_and_second_window_check_precede_leverage(self):
        class InitialSaveFailure(StateStore):
            def save(self, _state):
                raise OSError("reservation write failed")

        for mode in ("save", "expired"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                client = FakeLiveExecutionClient({}, leverage=10)
                state = (
                    InitialSaveFailure(Path(root) / "position.json")
                    if mode == "save"
                    else StateStore(Path(root) / "position.json")
                )
                times = iter(
                    (1_720_000_119_999,)
                    if mode == "save"
                    else (1_720_000_119_999, 1_720_000_120_000)
                )
                trader = Trader(
                    client,
                    live_test_config(str(Path(root) / "dry-account.json")),
                    state,
                    logging.getLogger("n17-journal"),
                    clock_ms=lambda: next(times),
                )
                plan = trader.build_range_support_absorption_margin_capped_trade_plan(
                    "N17USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                    "d" * 24, entry_min_price=Decimal("99"),
                    entry_max_price=Decimal("101"),
                )
                plan = replace(
                    plan,
                    entry_candle_open_time_ms=1_720_000_000_000,
                    entry_deadline_ms=1_720_000_120_000,
                    structure_context={
                        "strategy_id": "N17",
                        "rule_version": "N17_V1",
                        "signal_id": 71,
                        "structure_id": "d" * 24,
                    },
                )
                expected = (
                    BinanceAPIError if mode == "save" else EntryWindowExpiredError
                )
                with self.assertRaises(expected):
                    trader.open_long_plan_with_protection(plan)
                self.assertEqual(client.leverage_calls, [])
                self.assertEqual(client.market_calls, [])
                self.assertIsNone(state.load())


if __name__ == "__main__":
    unittest.main()
