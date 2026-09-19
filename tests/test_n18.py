from __future__ import annotations

from contextlib import closing
from copy import deepcopy
from decimal import Decimal
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from dataclasses import replace
from unittest.mock import patch

import trading_bot.n18_analyzer as n18_module

from trading_bot.n18_analyzer import (
    INTERVAL_MS,
    N18Candle,
    _breakout_thresholds_pass,
    _triangle_thresholds_pass,
    analyze_n18_ascending_triangle_breakout,
    decode_n18_state_evidence,
)
from trading_bot.n18_schema import (
    N18_INDEX_SQL,
    N18_TRIGGER_SQL,
    n18_schema_status,
)
from trading_bot.binance_client import BinanceAPIError
from trading_bot.recorder import (
    PAPER_TRADE_VOID_CONFIRMATION,
    ReviewRecorder,
    StrategySignalBatchWriteResult,
)
from trading_bot.main import TradingBot, _N18PlanIntegrityError
from trading_bot.monitor import FundingCandidate, StrategyMarketScan
from trading_bot.state import StateStore
from trading_bot.strategy_scheduler import StrategyScheduler
from trading_bot.trader import EntryWindowExpiredError, Trader
from trading_bot.signal_retention import (
    _install_n16_claim_boundary,
    _install_n17_lifecycle_boundary,
    _install_n18_lifecycle_boundary,
    _install_n19_lifecycle_boundary,
    _install_n20_lifecycle_boundary,
    _install_coverage_epoch_boundary,
    _install_micro_lifecycle_boundary,
)
from trading_bot.strategies import N18_STRATEGY, load_all_strategies
from tests.recorder_test_utils import make_test_recorder
from tests.test_trader import (
    FakeLiveExecutionClient,
    RuleConstrainedClient,
    live_test_config,
)


BASE_TIME = 1_730_000_000_000 // INTERVAL_MS * INTERVAL_MS


def _row(index, open_, high, low, close, volume="100", taker="55"):
    return [
        BASE_TIME + index * INTERVAL_MS,
        str(open_), str(high), str(low), str(close),
        "0", "0", str(volume), "0", "0", str(taker),
    ]


def n18_fixture():
    rows = []
    for index in range(122):
        close = Decimal("97") + Decimal(index) * Decimal("0.02")
        rows.append(
            _row(index, close - Decimal("0.02"), close + Decimal("0.25"),
                 close - Decimal("0.25"), close)
        )
    rows[96] = _row(96, "96.5", "97", "95", "96.6")
    rows[100] = _row(100, "99.2", "100", "98.8", "99.5")
    rows[104] = _row(104, "97", "97.5", "96", "97.2")
    rows[108] = _row(108, "99.2", "100.1", "98.9", "99.6")
    rows[112] = _row(112, "98", "98.4", "97", "98.2")
    rows[116] = _row(116, "99.2", "100.05", "98.8", "99.7")
    for index in (117, 118, 119):
        rows[index] = _row(index, "99.7", "99.95", "99.4", "99.65", "100", "52")
    rows[120] = _row(120, "99.8", "101.5", "99.7", "101.4", "140", "84")
    rows[121] = _row(121, "101.4", "101.8", "100.2", "101.5")
    return rows


def analyze(rows=None, *, elapsed=1_000, **overrides):
    arguments = {
        "quote_volume_rank": 7,
        "checked_at_ms": BASE_TIME + 121 * INTERVAL_MS + elapsed,
    }
    arguments.update(overrides)
    return analyze_n18_ascending_triangle_breakout(
        "TESTUSDT", n18_fixture() if rows is None else rows, **arguments
    )


class N18AnalyzerTests(unittest.TestCase):
    def test_frozen_definition_and_prior_projection_are_exact(self):
        strategies = load_all_strategies()
        self.assertEqual(
            [item.strategy_id for item in strategies],
            ["N%02d" % value for value in range(1, 26)],
        )
        self.assertEqual(strategies[17], N18_STRATEGY)
        self.assertEqual(
            [item.strategy_id for item in strategies if item.strategy_id != "N18"],
            ["N%02d" % value for value in range(1, 18)]
            + ["N19", "N20", "N21", "N22", "N23", "N24", "N25"],
        )
        self.assertEqual(N18_STRATEGY.risk_reward_ratio, Decimal("5"))
        self.assertEqual(N18_STRATEGY.fixed_input_bars, 122)

    def test_real_candles_pass_and_identity_binds_all_absolute_points(self):
        result = analyze()
        self.assertTrue(result.passed)
        self.assertEqual(result.reason, "PASSED")
        self.assertIsNotNone(result.structure)
        structure = result.structure
        self.assertEqual(
            [item.index for item in (
                structure.l1, structure.h1, structure.l2, structure.h2,
                structure.l3, structure.a, structure.b, structure.entry,
            )],
            [96, 100, 104, 108, 112, 116, 120, 121],
        )
        expected_family = hashlib.sha256(
            ("N18|TESTUSDT|" + "|".join(
                str(BASE_TIME + value * INTERVAL_MS)
                for value in (96, 100, 104, 108, 112, 116)
            )).encode()
        ).hexdigest()[:24]
        self.assertEqual(structure.family_id, expected_family)
        self.assertEqual(
            structure.structure_id,
            hashlib.sha256(
                ("N18|%s|%s" % (
                    expected_family, BASE_TIME + 120 * INTERVAL_MS
                )).encode()
            ).hexdigest()[:24],
        )
        decoded = decode_n18_state_evidence(result.state_record.evidence_json)
        self.assertEqual(decoded.evidence_sha256, result.state_record.evidence_sha256)

    def test_entry_window_and_price_boundaries_are_closed_and_deadline_is_open(self):
        base = analyze()
        minimum = base.structure.entry_min
        maximum = base.structure.entry_max
        resistance = base.structure.resistance
        for label, close, elapsed, passed, reason in (
            ("minimum", minimum, 0, True, "PASSED"),
            ("maximum", maximum, 119_999, True, "PASSED"),
            ("below", minimum - Decimal("0.0001"), 1, False, "N18_ENTRY_BELOW_MIN_WAITING"),
            ("above", maximum + Decimal("0.0001"), 1, False, "N18_ENTRY_PRICE_ABOVE_MAX"),
            ("deadline", minimum, 120_000, False, "N18_ENTRY_WINDOW_EXPIRED"),
        ):
            with self.subTest(label=label):
                rows = n18_fixture()
                rows[121] = _row(121, minimum, maximum + Decimal("0.2"), resistance,
                                 close, "100", "55")
                result = analyze(rows, elapsed=elapsed)
                self.assertEqual(result.passed, passed)
                self.assertEqual(result.reason, reason)

    def test_entry_l3_break_has_priority_over_resistance_not_held(self):
        rows = n18_fixture()
        rows[121] = _row(121, "101.4", "101.8", "96.9", "101.5")
        result = analyze(rows)
        self.assertEqual(result.reason, "N18_ENTRY_TRIANGLE_INVALIDATED")
        rows[121] = _row(121, "101.4", "101.8", "100.049", "101.5")
        result = analyze(rows)
        self.assertEqual(result.reason, "N18_ENTRY_BREAKOUT_NOT_HELD")

    def test_first_breakout_candidate_is_consumed_and_not_replaced(self):
        rows = n18_fixture()
        # First qualifying close remains bullish and above the breakout line,
        # but fails taker-buy.  A better later bar must never replace it.
        rows[119] = _row(119, "99.7", "101.2", "99.5", "101", "140", "60")
        rows[120] = _row(120, "99.8", "101.5", "99.7", "101.4", "140", "84")
        result = analyze(rows)
        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "N18_BREAKOUT_NOT_QUALIFIED")
        self.assertEqual(result.structure.b.open_time_ms, BASE_TIME + 119 * INTERVAL_MS)
        self.assertIsNotNone(result.structure_id)

    def test_exact_threshold_helpers_accept_equal_and_reject_one_step_outside(self):
        config = dict(n18_module._APPROVED_CONFIG)

        def absorption_candle(**changes):
            values = {
                "open": Decimal("99.5"), "high": Decimal("100.1"),
                "low": Decimal("99"), "close": Decimal("99.8"),
                "quote_volume": Decimal("100"),
                "taker_buy_quote_volume": Decimal("50"),
            }
            values.update(changes)
            return N18Candle(
                20, BASE_TIME + 20 * INTERVAL_MS,
                values["open"], values["high"], values["low"], values["close"],
                values["quote_volume"], values["taker_buy_quote_volume"],
            )

        safe = {
            "span": 20, "h1_h2": 4, "h2_a": 4,
            "dispersion": Decimal("0.30"), "atr_a": Decimal("1"),
            "resistance": Decimal("100"), "a": absorption_candle(),
            "l1_l2": Decimal("0.30"), "l2_l3": Decimal("0.30"),
            "initial_height": Decimal("3"), "convergence": Decimal("0.50"),
            "ema20": Decimal("2"), "ema50": Decimal("1"),
            "a_volume_multiple": Decimal("1"), "config": config,
        }
        scalar_cases = (
            ("span_min", "span", 12, 11),
            ("span_max", "span", 36, 37),
            ("h1_h2", "h1_h2", 3, 2),
            ("h2_a", "h2_a", 3, 2),
            ("dispersion_atr", "dispersion", Decimal("0.35"), Decimal("0.350001")),
            ("l1_l2", "l1_l2", Decimal("0.20"), Decimal("0.199999")),
            ("l2_l3", "l2_l3", Decimal("0.20"), Decimal("0.199999")),
            ("initial_height", "initial_height", Decimal("2"), Decimal("1.999999")),
            ("convergence", "convergence", Decimal("0.65"), Decimal("0.650001")),
            ("ema_equal", "ema20", Decimal("1"), Decimal("0.999999")),
            ("a_volume", "a_volume_multiple", Decimal("0.80"), Decimal("0.799999")),
        )
        for name, field, equal, outside in scalar_cases:
            with self.subTest(name=name, side="equal"):
                values = dict(safe)
                values[field] = equal
                if field == "ema20":
                    values["ema50"] = Decimal("1")
                self.assertTrue(_triangle_thresholds_pass(**values))
            with self.subTest(name=name, side="outside"):
                values = dict(safe)
                values[field] = outside
                if field == "ema20":
                    values["ema50"] = Decimal("1")
                self.assertFalse(_triangle_thresholds_pass(**values))
        values = dict(safe)
        values["convergence"] = Decimal("0")
        self.assertFalse(_triangle_thresholds_pass(**values))

        for name, candle, expected in (
            ("a_high_lower_equal", absorption_candle(
                high=Decimal("99.75"), close=Decimal("99.6")), True),
            ("a_high_lower_out", absorption_candle(
                high=Decimal("99.749999"), close=Decimal("99.6")), False),
            ("a_high_upper_equal", absorption_candle(high=Decimal("100.1")), True),
            ("a_high_upper_out", absorption_candle(high=Decimal("100.100001")), False),
            ("a_close_equal", absorption_candle(close=Decimal("100")), True),
            ("a_close_out", absorption_candle(close=Decimal("100.000001")), False),
            ("a_location_equal", absorption_candle(
                high=Decimal("100"), close=Decimal("99.6")), True),
            ("a_location_out", absorption_candle(
                high=Decimal("100"), close=Decimal("99.599999")), False),
            ("a_taker_equal", absorption_candle(
                taker_buy_quote_volume=Decimal("50")), True),
            ("a_taker_out", absorption_candle(
                taker_buy_quote_volume=Decimal("49.9999")), False),
        ):
            with self.subTest(name=name):
                values = dict(safe)
                values["a"] = candle
                self.assertEqual(_triangle_thresholds_pass(**values), expected)

        # The independent percentage dispersion cap is inclusive as well.
        fraction = dict(safe)
        fraction.update(
            resistance=Decimal("10"), atr_a=Decimal("100"),
            dispersion=Decimal("0.05"),
            a=N18Candle(
                20, BASE_TIME + 20 * INTERVAL_MS, Decimal("9.5"),
                Decimal("10.1"), Decimal("9"), Decimal("9.8"),
                Decimal("100"), Decimal("50"),
            ),
        )
        self.assertTrue(_triangle_thresholds_pass(**fraction))
        fraction["dispersion"] = Decimal("0.050001")
        self.assertFalse(_triangle_thresholds_pass(**fraction))

        def breakout(**changes):
            values = {
                "open": Decimal("10.30"), "high": Decimal("11"),
                "low": Decimal("10"), "close": Decimal("10.75"),
                "quote_volume": Decimal("100"),
                "taker_buy_quote_volume": Decimal("55"),
            }
            values.update(changes)
            return N18Candle(
                21, BASE_TIME + 21 * INTERVAL_MS,
                values["open"], values["high"], values["low"], values["close"],
                values["quote_volume"], values["taker_buy_quote_volume"],
            )

        breakout_cases = (
            ("body_equal", breakout(open=Decimal("10.35")), Decimal("1.30"), True),
            ("body_out", breakout(open=Decimal("10.350001")), Decimal("1.30"), False),
            ("location_equal", breakout(close=Decimal("10.75")), Decimal("1.30"), True),
            ("location_out", breakout(close=Decimal("10.749999")), Decimal("1.30"), False),
            ("taker_equal", breakout(taker_buy_quote_volume=Decimal("55")), Decimal("1.30"), True),
            ("taker_out", breakout(taker_buy_quote_volume=Decimal("54.9999")), Decimal("1.30"), False),
            ("volume_equal", breakout(), Decimal("1.30"), True),
            ("volume_out", breakout(), Decimal("1.299999"), False),
        )
        for name, candle, volume_multiple, expected in breakout_cases:
            with self.subTest(name=name):
                self.assertEqual(
                    _breakout_thresholds_pass(
                        candle, Decimal("1"), volume_multiple, config
                    ),
                    expected,
                )

    def test_first_and_fourth_breakout_slots_and_historical_entry_are_permanent(self):
        fourth = analyze()
        self.assertTrue(fourth.passed)
        self.assertEqual(fourth.structure.b.index, 120)

        first_rows = n18_fixture()
        # Keep B below A.high so the frozen 2/2 A pivot remains genuine while
        # B.close is still strictly over R + 0.10*ATR_B.
        first_rows[116] = _row(
            116, "99.2", "100.195", "98.8", "99.7", "100", "52"
        )
        first_rows[117] = _row(
            117,
            "99.81639394474204008516087645",
            "100.1947686850859866382797078",
            "99.76639394474204008516087645",
            "100.1946686850859866382797078",
            "140", "84",
        )
        first = analyze(first_rows)
        self.assertEqual(first.structure.b.index, 117)
        self.assertEqual(first.reason, "N18_HISTORICAL_ENTRY_MISSED")
        self.assertEqual(first.state_record.stage, "MISSED")

        absent = n18_fixture()
        absent[120] = _row(120, "99.7", "100", "99.5", "99.7", "100", "52")
        missing = analyze(absent)
        self.assertEqual(missing.reason, "N18_BREAKOUT_NOT_FOUND")
        self.assertEqual(missing.state_record.stage, "CONSUMED")

        rolled = n18_fixture()
        rolled.append(_row(122, "101.5", "101.9", "101.3", "101.6"))
        historical = analyze(
            rolled, checked_at_ms=BASE_TIME + 122 * INTERVAL_MS + 1_000
        )
        self.assertEqual(historical.reason, "N18_HISTORICAL_ENTRY_MISSED")
        self.assertEqual(historical.state_record.stage, "MISSED")

    def test_absorption_failure_and_frozen_source_conflict_do_not_re_sign(self):
        rows = n18_fixture()
        rows[116] = _row(116, "99.2", "100.05", "98.8", "99.7", "100", "49")
        rows[118] = _row(118, "99.2", "100.04", "98.9", "99.8", "110", "60")
        failed = analyze(rows)
        self.assertEqual(failed.reason, "N18_ABSORPTION_NOT_QUALIFIED")
        self.assertEqual(failed.structure.a.open_time_ms, BASE_TIME + 116 * INTERVAL_MS)
        self.assertEqual(failed.state_record.stage, "CONSUMED")

        valid = analyze()
        conflicting = n18_fixture()
        conflicting[116][4] = "99.71"
        self.assertEqual(
            analyze(
                conflicting,
                frozen_evidence=valid.state_record.evidence_json,
            ).reason,
            "N18_FROZEN_EVIDENCE_INVALID",
        )

    def test_payload_size_equal_limits_is_allowed_and_one_byte_over_is_rejected(self):
        result = analyze()
        with patch.object(
            n18_module, "_canonical_json", return_value="x" * (16 * 1024)
        ):
            self.assertEqual(result.detail_json()["reason"], "PASSED")
        with patch.object(
            n18_module, "_canonical_json", return_value="x" * (16 * 1024 + 1)
        ):
            with self.assertRaisesRegex(ValueError, "exceeds 16KiB"):
                result.detail_json()

        candles = n18_module.parse_n18_klines(n18_fixture())
        with patch.object(
            n18_module, "_canonical_json", return_value="x" * (128 * 1024)
        ):
            record = n18_module._record(
                result.structure, "CONFIRMED", "PASSED", 7, candles
            )
            self.assertEqual(record.stage, "CONFIRMED")
        with patch.object(
            n18_module, "_canonical_json", return_value="x" * (128 * 1024 + 1)
        ):
            with self.assertRaisesRegex(ValueError, "exceeds 128KiB"):
                n18_module._record(
                    result.structure, "CONFIRMED", "PASSED", 7, candles
                )

    def test_nonfinite_discontinuous_and_definition_drift_fail_closed(self):
        rows = n18_fixture()
        rows[30][4] = "NaN"
        self.assertEqual(analyze(rows).reason, "N18_KLINE_DATA_INVALID")
        rows = n18_fixture()
        rows[30][0] += INTERVAL_MS
        self.assertEqual(analyze(rows).reason, "N18_KLINE_SEQUENCE_INVALID")
        self.assertEqual(
            analyze(triangle_span_min_bars=11).reason,
            "N18_DEFINITION_INVALID",
        )


class N18LifecycleTests(unittest.TestCase):
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
            (("structure", "entry", "open_time_ms"), True),
            (("structure", "h1_h2_interval_bars"), 1.0),
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
            evidence["canonical_sha256"] = n18_module._sha256_json(unsigned)
            return evidence

        for path, value in cases:
            with self.subTest(path=path, value=value):
                evidence = mutate(path, value)
                with self.assertRaises(ValueError):
                    decode_n18_state_evidence(evidence, expected_symbol="TESTUSDT")
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n18-exact-evidence-types"),
            )
            record = replace(
                result.state_record,
                evidence=mutate(("quote_volume_rank",), True),
            )
            self.assertEqual(
                recorder.record_n18_state(record), "N18_STATE_INCONSISTENT"
            )

    def test_terminal_stage_reason_pairs_are_canonical_at_decode_write_and_claim(self):
        result = analyze()
        self.assertTrue(result.passed)
        entry_time = result.structure.entry.open_time_ms
        legal = {
            "MISSED": {
                "N18_HISTORICAL_ENTRY_MISSED",
                "N18_ENTRY_PRICE_ABOVE_MAX",
            },
            "INVALID": {
                "N18_ENTRY_TRIANGLE_INVALIDATED",
                "N18_ENTRY_BREAKOUT_NOT_HELD",
            },
            "EXPIRED": {"N18_ENTRY_WINDOW_EXPIRED"},
        }
        terminal_stages = {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}

        def mutated(stage, reason):
            evidence = deepcopy(result.state_record.evidence)
            evidence["stage"] = stage
            evidence["reason"] = reason
            evidence["terminal_cutoff_time_ms"] = entry_time
            unsigned = dict(evidence)
            unsigned.pop("canonical_sha256", None)
            evidence["canonical_sha256"] = n18_module._sha256_json(unsigned)
            return evidence

        for correct_stage, reasons in legal.items():
            for reason in sorted(reasons):
                with self.subTest(stage=correct_stage, reason=reason, valid=True):
                    decoded = decode_n18_state_evidence(
                        mutated(correct_stage, reason), expected_symbol="TESTUSDT"
                    )
                    self.assertEqual((decoded.stage, decoded.reason), (correct_stage, reason))
                for wrong_stage in sorted(terminal_stages - {correct_stage}):
                    with self.subTest(stage=wrong_stage, reason=reason, valid=False):
                        with self.assertRaisesRegex(ValueError, "stage and reason"):
                            decode_n18_state_evidence(
                                mutated(wrong_stage, reason),
                                expected_symbol="TESTUSDT",
                            )
        for bad_reason in ("N18_UNKNOWN_TERMINAL_REASON", 1, True):
            with self.subTest(reason=bad_reason):
                with self.assertRaises(ValueError):
                    decode_n18_state_evidence(
                        mutated("MISSED", bad_reason), expected_symbol="TESTUSDT"
                    )

        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n18-stage-reason"),
            )
            self.assertEqual(recorder.record_n18_state(result.state_record), "INSERTED")
            scan_id = recorder.begin_scan(100, [], True)
            signal_id = recorder.record_strategy_signal(
                scan_id, "N18", "TESTUSDT", "", (), "", True, True,
                "PASSED", "PASSED", result.structure_id, result.detail_json(),
            )
            proposal = StrategyScheduler._history_coverage_proposal(
                "N18", "TESTUSDT", n18_fixture(), 122
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(scan_id, 1, (proposal,))
            )
            illegal = mutated("CONSUMED", "N18_HISTORICAL_ENTRY_MISSED")
            illegal_record = replace(
                result.state_record,
                stage="CONSUMED",
                reason="N18_HISTORICAL_ENTRY_MISSED",
                terminal_cutoff_time_ms=entry_time,
                evidence=illegal,
            )
            self.assertEqual(
                recorder.record_n18_state(illegal_record),
                "N18_STATE_INCONSISTENT",
            )
            illegal_json = n18_module._canonical_json(illegal)
            with recorder._connect() as connection:
                connection.execute(
                    "UPDATE n18_triangle_states SET stage='CONSUMED',reason=?,"
                    "terminal_cutoff_time_ms=?,evidence_json=?,evidence_sha256=? "
                    "WHERE strategy_id='N18' AND structure_id=?",
                    (
                        "N18_HISTORICAL_ENTRY_MISSED", entry_time, illegal_json,
                        hashlib.sha256(illegal_json.encode("utf-8")).hexdigest(),
                        result.structure_id,
                    ),
                )
            with self.assertRaisesRegex(RuntimeError, "evidence is invalid"):
                recorder.assert_n18_execution_claim(
                    signal_id, "TESTUSDT", result.structure_id
                )

    @staticmethod
    def _candidates():
        return [
            FundingCandidate(
                "T%03dUSDT" % index, None, Decimal("101.5"),
                quote_volume=Decimal("1000000"), quote_volume_rank=index + 1,
                candidate_universe="quote_volume_top",
            )
            for index in range(100)
        ]

    def test_scheduler_persists_all_claims_and_binds_plan_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(database, logging.getLogger("n18-scheduler"))
            recorder.upsert_strategy_definitions(load_all_strategies())
            candidate = FundingCandidate(
                "TESTUSDT", None, Decimal("101.5"),
                quote_volume=Decimal("1000000"), quote_volume_rank=7,
                candidate_universe="quote_volume_top",
            )
            scan_id = recorder.begin_scan(100, [candidate], dry_run=True)
            result = StrategyScheduler(
                (N18_STRATEGY,), 96, recorder, logging.getLogger("n18-scheduler")
            ).evaluate(
                scan_id,
                {"quote_volume_top": [candidate], "negative_funding": []},
                {"TESTUSDT": n18_fixture()},
                BASE_TIME + 121 * INTERVAL_MS + 1_000,
            )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(len(result.signals), 1)
            self.assertEqual(len(result.passed_signals), 1)
            signal = result.passed_signals[0]
            analysis = signal.analysis
            trader = Trader(
                RuleConstrainedClient(leverage=10, tick_size="0.01"),
                live_test_config(str(Path(directory) / "dry-account.json")),
                StateStore(Path(directory) / "position.json"),
                logging.getLogger("n18-plan"),
            )
            plan = trader.build_ascending_triangle_breakout_margin_capped_trade_plan(
                signal.candidate.symbol, analysis.structure.entry.close,
                analysis.structure.l3.low, Decimal("5"), analysis.structure_id,
                entry_min_price=analysis.structure.entry_min,
                entry_max_price=analysis.structure.entry_max,
            )
            plan = replace(plan, structure_context={
                "strategy_id": "N18", "rule_version": "N18_V1",
                "structure_id": analysis.structure_id,
            })
            bound = TradingBot._n18_plan_with_published_identity(signal, plan)
            self.assertEqual(bound.structure_context["signal_id"], signal.signal_id)
            recorder.assert_n18_execution_claim(
                signal.signal_id, signal.candidate.symbol, analysis.structure_id
            )
            with self.assertRaises(_N18PlanIntegrityError):
                TradingBot._n18_plan_with_published_identity(
                    signal, replace(plan, symbol="OTHERUSDT")
                )

    def test_pre_n18_requires_explicit_zero_write_install_and_wrong_schema_rejects(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            ledger = Path(directory) / "claim.sqlite3"
            with closing(sqlite3.connect(database)) as connection:
                connection.commit()
            _install_n16_claim_boundary(database, ledger)
            _install_n17_lifecycle_boundary(database, ledger)
            _install_n19_lifecycle_boundary(database, ledger)
            before = database.read_bytes()
            entries = sorted(os.listdir(directory))
            with self.assertRaisesRegex(RuntimeError, "pre-N18"):
                ReviewRecorder(database, logging.getLogger("pre-n18"), ledger)
            self.assertEqual(database.read_bytes(), before)
            self.assertEqual(sorted(os.listdir(directory)), entries)
            _install_n18_lifecycle_boundary(database, ledger)
            with self.assertRaisesRegex(RuntimeError, "pre-N20"):
                ReviewRecorder(database, logging.getLogger("n18-pre-n20"), ledger)
            _install_n20_lifecycle_boundary(database, ledger)
            _install_micro_lifecycle_boundary(database, ledger)
            _install_coverage_epoch_boundary(database, ledger)
            recorder = ReviewRecorder(database, logging.getLogger("n18-current"), ledger)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(n18_schema_status(connection), "CURRENT")
            del recorder
            with closing(sqlite3.connect(database)) as connection:
                connection.execute("PRAGMA journal_mode=DELETE")
                connection.execute("DROP INDEX idx_n18_triangle_family")
                connection.execute(
                    "CREATE UNIQUE INDEX idx_n18_triangle_family "
                    "ON n18_triangle_states(strategy_id,symbol)"
                )
                connection.commit()
            damaged = database.read_bytes()
            damaged_entries = sorted(os.listdir(directory))
            with self.assertRaises(RuntimeError):
                ReviewRecorder(database, logging.getLogger("n18-damaged"), ledger)
            self.assertEqual(database.read_bytes(), damaged)
            self.assertEqual(sorted(os.listdir(directory)), damaged_entries)

    def test_confirmed_state_claim_publish_and_restart_are_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(database, logging.getLogger("n18-claim"))
            result = analyze()
            self.assertEqual(recorder.record_n18_state(result.state_record), "INSERTED")
            self.assertEqual(recorder.record_n18_state(result.state_record), "UNCHANGED")
            scan_id = recorder.begin_scan(100, [], True)
            signal_id = recorder.record_strategy_signal(
                scan_id, "N18", "TESTUSDT", "", (), "", True, True,
                "PASSED", "PASSED", result.structure_id, result.detail_json(),
            )
            proposal = StrategyScheduler._history_coverage_proposal(
                "N18", "TESTUSDT", n18_fixture(), 122
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(scan_id, 1, (proposal,))
            )
            recorder.assert_n18_execution_claim(
                signal_id, "TESTUSDT", result.structure_id
            )
            self.assertEqual(
                recorder.record_n18_state(result.state_record),
                "N18_STRUCTURE_CONSUMED",
            )
            restarted = ReviewRecorder(
                database, logging.getLogger("n18-restart"),
                recorder.n16_claim_ledger_file,
            )
            restarted.assert_n18_execution_claim(
                signal_id, "TESTUSDT", result.structure_id
            )
            self.assertEqual(
                restarted.get_required_n18_family_symbols(), ("TESTUSDT",)
            )
            terminal_rows = n18_fixture()
            terminal_rows.append(
                _row(122, "101.5", "101.9", "101.3", "101.6")
            )
            terminal = analyze(
                terminal_rows,
                checked_at_ms=BASE_TIME + 122 * INTERVAL_MS + 1_000,
                frozen_evidence=result.state_record.evidence_json,
            )
            self.assertEqual(terminal.reason, "N18_HISTORICAL_ENTRY_MISSED")
            self.assertEqual(
                restarted.record_n18_state(terminal.state_record), "UPDATED"
            )
            restarted.assert_n18_execution_claim(
                signal_id, "TESTUSDT", result.structure_id
            )
            next_scan = restarted.begin_scan(1, [], True)
            next_signal = restarted.record_strategy_signal(
                next_scan, "N18", "OTHERUSDT", "", (), "", False, False,
                "REJECTED", "N18_TRIANGLE_NOT_FOUND", None, {},
            )
            self.assertIsInstance(next_signal, int)
            next_proposal = StrategyScheduler._history_coverage_proposal(
                "N18", "OTHERUSDT", n18_fixture(), 122
            )
            self.assertTrue(
                restarted.publish_strategy_signal_batch(
                    next_scan, 1, (next_proposal,)
                )
            )
            restarted.assert_n18_execution_claim(
                signal_id, "TESTUSDT", result.structure_id
            )
            with restarted._read_only_runtime_snapshot() as connection:
                self.assertIsNone(connection.execute(
                    "SELECT 1 FROM strategy_signals WHERE id=?", (signal_id,)
                ).fetchone())

    def test_confirmed_live_tail_replays_ten_times_without_state_conflict(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database,
                logging.getLogger("n18-live-tail-idempotence"),
            )
            current = analyze()
            self.assertTrue(current.passed)
            self.assertEqual(
                recorder.record_n18_state(current.state_record),
                "INSERTED",
            )
            for step in range(1, 11):
                rows = n18_fixture()
                rows[121] = _row(
                    121,
                    "101.4",
                    str(Decimal("101.8") + Decimal(step) / Decimal("100")),
                    "100.2",
                    str(Decimal("101.5") + Decimal(step) / Decimal("1000")),
                    str(Decimal("100") + step * 10),
                    str(Decimal("55") + step * 5),
                )
                replay = analyze(
                    rows,
                    elapsed=1_000 + step * 1_000,
                    frozen_evidence=current.state_record.evidence_json,
                )
                self.assertTrue(replay.passed, replay.reason)
                self.assertEqual(replay.state_record.stage, "CONFIRMED")
                self.assertIn(
                    recorder.record_n18_state(replay.state_record),
                    {"UPDATED", "UNCHANGED"},
                )
                current = replay
            restarted = ReviewRecorder(
                database,
                logging.getLogger("n18-live-tail-restart"),
                recorder.n16_claim_ledger_file,
            )
            self.assertEqual(
                restarted.record_n18_state(current.state_record),
                "UNCHANGED",
            )
            durable = restarted.get_latest_n18_states({"TESTUSDT"})[
                "TESTUSDT"
            ]
            self.assertEqual(durable.evidence_sha256, current.state_record.evidence_sha256)

            tampered = deepcopy(current.state_record.evidence)
            tampered["source"][-1]["quote_volume"] = "1"
            tampered["structure"]["entry"]["quote_volume"] = "1"
            unsigned = dict(tampered)
            unsigned.pop("canonical_sha256", None)
            tampered["canonical_sha256"] = n18_module._sha256_json(unsigned)
            self.assertEqual(
                restarted.record_n18_state(replace(
                    current.state_record,
                    evidence=tampered,
                )),
                "N18_STATE_INCONSISTENT",
            )
            self.assertEqual(
                restarted.get_required_n18_family_symbols(), ("TESTUSDT",)
            )

    def test_legacy_paper_results_keep_cross_symbol_statistics_only(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3", logging.getLogger("n18-paper")
            )
            for symbol in ("AAAUSDT", "BBBUSDT"):
                trade_id = recorder.open_strategy_paper_trade(
                    "N18", symbol, "100", "99", "105", "", {}, {}
                )
                self.assertIsInstance(trade_id, int)
                self.assertTrue(recorder.close_strategy_paper_trade(
                    trade_id, "WIN", "TAKE_PROFIT", "105", "5"
                ))
            state = recorder.get_strategy_state("N18")
            self.assertEqual(state.consecutive_wins, 0)
            self.assertEqual(state.paper_trade_count, 2)
            self.assertEqual(state.win_count, 2)
            self.assertEqual(state.loss_count, 0)
            self.assertFalse(state.live_eligible)

            void_id = recorder.open_strategy_paper_trade(
                "N18", "VOIDUSDT", "100", "99", "105", "", {}, {}
            )
            self.assertTrue(recorder.void_strategy_paper_trade(
                void_id, "N18", "VOIDUSDT", PAPER_TRADE_VOID_CONFIRMATION
            ))
            after_void = recorder.get_strategy_state("N18")
            self.assertEqual(after_void.consecutive_wins, 0)
            self.assertEqual(after_void.paper_trade_count, 2)
            self.assertEqual(after_void.win_count, 2)
            self.assertEqual(after_void.loss_count, 0)
            self.assertFalse(after_void.live_eligible)

            loss_id = recorder.open_strategy_paper_trade(
                "N18", "LOSSUSDT", "100", "99", "105", "", {}, {}
            )
            self.assertTrue(recorder.close_strategy_paper_trade(
                loss_id, "LOSS", "STOP_LOSS", "99", "-1"
            ))
            reset = recorder.get_strategy_state("N18")
            self.assertEqual(reset.consecutive_wins, 0)
            self.assertEqual(reset.paper_trade_count, 3)
            self.assertEqual(reset.win_count, 2)
            self.assertEqual(reset.loss_count, 1)
            self.assertFalse(reset.live_eligible)
            self.assertEqual(reset.last_trade_result, "LOSS")

    def test_history_coverage_conflict_blocks_publication_and_keeps_current(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3", logging.getLogger("n18-coverage")
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            candidates = self._candidates()
            scheduler = StrategyScheduler(
                (N18_STRATEGY,), 96, recorder, logging.getLogger("n18-coverage")
            )
            first_scan = recorder.begin_scan(100, candidates, dry_run=True)
            first = scheduler.evaluate(
                first_scan,
                {"quote_volume_top": candidates, "negative_funding": []},
                {item.symbol: n18_fixture() for item in candidates},
                BASE_TIME + 121 * INTERVAL_MS + 1_000,
            )
            self.assertTrue(first.signal_batch_published)

            jumped = n18_fixture()
            for row in jumped:
                row[0] += 124 * INTERVAL_MS
            second_scan = recorder.begin_scan(100, candidates, dry_run=True)
            second = scheduler.evaluate(
                second_scan,
                {"quote_volume_top": candidates, "negative_funding": []},
                {item.symbol: deepcopy(jumped) for item in candidates},
                int(jumped[-1][0]) + 1_000,
            )
            self.assertFalse(second.signal_batch_published)
            self.assertTrue(all(
                item.reason == "N18_HISTORY_COVERAGE_GAP_BLOCKED"
                for item in second.signals
            ))
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(connection.execute(
                    "SELECT current_scan_id FROM strategy_signal_current "
                    "WHERE singleton_id=1"
                ).fetchone(), (first_scan,))

    def test_state_failure_never_falls_back_to_second_ranked_symbol(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3", logging.getLogger("n18-no-fallback")
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            candidates = self._candidates()
            real_record = recorder.record_n18_state

            def fail_winner(record):
                if record.symbol == "T000USDT":
                    return "N18_STATE_PERSIST_FAILED"
                return real_record(record)

            scan_id = recorder.begin_scan(100, candidates, dry_run=True)
            scheduler = StrategyScheduler(
                (N18_STRATEGY,), 96, recorder, logging.getLogger("n18-no-fallback")
            )
            with patch.object(recorder, "record_n18_state", side_effect=fail_winner):
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": candidates, "negative_funding": []},
                    {item.symbol: n18_fixture() for item in candidates},
                    BASE_TIME + 121 * INTERVAL_MS + 1_000,
                )
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(result.passed_signals, [])
            reasons = {item.candidate.symbol: item.reason for item in result.signals}
            self.assertEqual(reasons["T000USDT"], "N18_STATE_PERSIST_FAILED")
            self.assertEqual(reasons["T001USDT"], "N18_NOT_REPRESENTATIVE")

    def test_active_dropped_symbol_is_fetched_once_and_read_failure_blocks_scan(self):
        candidates = self._candidates()

        class Monitor:
            def scan_for_strategies(self, _volume_top_n):
                return StrategyMarketScan(100, [], candidates)

        class Client:
            def __init__(self):
                self.calls = []

            def get_klines(self, symbol):
                self.calls.append(symbol)
                return n18_fixture()

        class Recorder:
            def __init__(self, fail=False):
                self.fail = fail
                self.begin_calls = 0

            def expire_stale_n14_active_episodes(self, *_args): return "OK"
            def get_required_n14_snapshot_symbols(self, *_args): return ()
            def get_pending_n15_snapshot_symbols(self, *_args): return ()
            def get_required_n16_episode_symbols(self, *_args): return ()
            def get_required_n17_family_symbols(self): return ()
            def get_required_n19_family_symbols(self): return ()
            def get_required_n20_frozen_symbols(self): return ()
            def get_required_n18_family_symbols(self):
                if self.fail:
                    raise RuntimeError("read failed")
                return ("T000USDT", "DROPPEDUSDT")
            def begin_scan(self, *_args, **_kwargs):
                self.begin_calls += 1
                return 1
            def complete_scan(self, *_args, **_kwargs): return None

        class Scheduler:
            def evaluate(self, *_args, **_kwargs):
                return SimpleNamespace(
                    signal_batch_published=True, passed_signals=[], live_candidates=[]
                )

        for fail in (False, True):
            with self.subTest(fail=fail):
                bot = TradingBot.__new__(TradingBot)
                bot.config = SimpleNamespace(dry_run=True)
                bot.logger = logging.getLogger("n18-dropped-member")
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
                    self.assertEqual(bot.client.calls.count("T000USDT"), 1)
                    self.assertEqual(bot.client.calls.count("DROPPEDUSDT"), 1)

    def test_n06_through_n20_emit_exactly_1500_ordinary_decisions(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3", logging.getLogger("n18-count")
            )
            strategies = tuple(
                item for item in load_all_strategies()
                if 6 <= int(item.strategy_id[1:]) <= 20
            )
            self.assertEqual(
                [item.strategy_id for item in strategies],
                ["N%02d" % value for value in range(6, 21)],
            )
            candidates = self._candidates()
            scheduler = StrategyScheduler(
                strategies, 96, recorder, logging.getLogger("n18-count")
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

            with patch.object(scheduler, "_evaluate_candidate", side_effect=reject), patch.object(
                recorder, "record_strategy_signals", side_effect=record_batch
            ), patch.object(recorder, "publish_strategy_signal_batch", return_value=True):
                result = scheduler.evaluate(
                    1,
                    {"quote_volume_top": candidates, "negative_funding": []},
                    {item.symbol: n18_fixture() for item in candidates},
                    BASE_TIME + 121 * INTERVAL_MS + 1_000,
                )
            self.assertEqual(len(result.signals), 1500)
            self.assertEqual(len(recorded), 1500)
            for strategy in strategies:
                self.assertEqual(sum(row[0] == strategy.strategy_id for row in recorded), 100)

    def test_n18_plan_tick_risk_limits_exchange_minima_and_actual_five_r_are_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            trader = Trader(
                RuleConstrainedClient(leverage=10, tick_size="0.01"),
                live_test_config(str(Path(directory) / "dry-account.json")),
                StateStore(Path(directory) / "position.json"),
                logging.getLogger("n18-plan-boundaries"),
            )
            minimum = trader.build_ascending_triangle_breakout_margin_capped_trade_plan(
                "N18USDT", Decimal("100"), Decimal("99.50"), Decimal("5"),
                "a" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            self.assertEqual(minimum.structure_stop_price, Decimal("99.49"))
            self.assertEqual(minimum.stop_loss_price, Decimal("99.00"))
            self.assertEqual(minimum.take_profit_price, Decimal("105.00"))
            structural = trader.build_ascending_triangle_breakout_margin_capped_trade_plan(
                "N18USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                "b" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            actual = trader._execution_plan_from_actual_entry(
                structural, Decimal("100.50")
            )
            self.assertGreaterEqual(
                (actual.take_profit_price - actual.actual_entry_price)
                / (actual.actual_entry_price - actual.stop_loss_price),
                Decimal("5"),
            )
            with self.assertRaisesRegex(BinanceAPIError, "STOP_PCT_OUT_OF_RANGE"):
                trader.build_ascending_triangle_breakout_margin_capped_trade_plan(
                    "N18USDT", Decimal("100"), Decimal("94"), Decimal("5"),
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
                RuleConstrainedClient(leverage=10, tick_size="0.01", step_size="0.1"),
                live_test_config(str(Path(directory) / "step-account.json")),
                StateStore(Path(directory) / "step-position.json"),
                logging.getLogger("n18-step"),
            ).build_ascending_triangle_breakout_margin_capped_trade_plan(
                "N18USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                "d" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            self.assertEqual(stepped.quantity % Decimal("0.1"), Decimal("0"))
            for client, message in (
                (RuleConstrainedClient(leverage=1, min_qty="10"), "below minQty"),
                (RuleConstrainedClient(leverage=1, min_notional="1000"), "below minNotional"),
            ):
                with self.subTest(minimum=message), self.assertRaisesRegex(
                    BinanceAPIError, message
                ):
                    Trader(
                        client,
                        live_test_config(str(Path(directory) / (message + ".json"))),
                        StateStore(Path(directory) / (message + "-state.json")),
                        logging.getLogger("n18-minimum"),
                    ).build_ascending_triangle_breakout_margin_capped_trade_plan(
                        "N18USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                        "e" * 24, entry_min_price=Decimal("100"),
                        entry_max_price=Decimal("100.50"),
                    )

    def test_signal_and_publish_failures_never_expose_execution_candidates(self):
        candidates = self._candidates()
        for mode in ("signal", "publish"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                recorder = make_test_recorder(
                    Path(directory) / "review.sqlite3",
                    logging.getLogger("n18-failure-matrix"),
                )
                recorder.upsert_strategy_definitions(load_all_strategies())
                scan_id = recorder.begin_scan(100, candidates, dry_run=True)
                scheduler = StrategyScheduler(
                    (N18_STRATEGY,), 96, recorder,
                    logging.getLogger("n18-failure-matrix"),
                )
                target = (
                    patch.object(
                        recorder,
                        "record_strategy_signals",
                        return_value=StrategySignalBatchWriteResult(
                            failed_index=0
                        ),
                    )
                    if mode == "signal"
                    else patch.object(
                        recorder, "publish_strategy_signal_batch", return_value=False
                    )
                )
                with target:
                    result = scheduler.evaluate(
                        scan_id,
                        {"quote_volume_top": candidates, "negative_funding": []},
                        {item.symbol: n18_fixture() for item in candidates},
                        BASE_TIME + 121 * INTERVAL_MS + 1_000,
                    )
                self.assertFalse(result.signal_batch_published)
                self.assertEqual(result.passed_signals, [])
                self.assertEqual(result.live_candidates, [])

    def test_n18_journal_and_second_window_check_precede_leverage(self):
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
                    logging.getLogger("n18-journal"),
                    clock_ms=lambda: next(times),
                )
                plan = trader.build_ascending_triangle_breakout_margin_capped_trade_plan(
                    "N18USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                    "f" * 24, entry_min_price=Decimal("99"),
                    entry_max_price=Decimal("101"),
                )
                plan = replace(
                    plan,
                    entry_candle_open_time_ms=1_720_000_000_000,
                    entry_deadline_ms=1_720_000_120_000,
                    structure_context={
                        "strategy_id": "N18", "rule_version": "N18_V1",
                        "signal_id": 91, "structure_id": "f" * 24,
                    },
                )
                expected = BinanceAPIError if mode == "save" else EntryWindowExpiredError
                with self.assertRaises(expected):
                    trader.open_long_plan_with_protection(plan)
                self.assertEqual(client.leverage_calls, [])
                self.assertEqual(client.market_calls, [])
                self.assertIsNone(state.load())

    def test_n18_actual_fill_outside_interval_emergency_closes_without_protection(self):
        with tempfile.TemporaryDirectory() as directory:
            client = FakeLiveExecutionClient({}, leverage=10)
            state = StateStore(Path(directory) / "position.json")
            trader = Trader(
                client,
                live_test_config(str(Path(directory) / "dry-account.json")),
                state,
                logging.getLogger("n18-actual-fill"),
                clock_ms=lambda: 1_720_000_001_000,
            )
            plan = trader.build_ascending_triangle_breakout_margin_capped_trade_plan(
                "N18USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                "a" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            plan = replace(
                plan,
                entry_candle_open_time_ms=1_720_000_000_000,
                entry_deadline_ms=1_720_000_120_000,
                structure_context={
                    "strategy_id": "N18", "rule_version": "N18_V1",
                    "signal_id": 92, "structure_id": "a" * 24,
                },
            )
            outside_fill = (
                plan.entry_max_price * Decimal("1.005")
                + Decimal("0.000000000000001")
            )
            client.open_response = {
                "orderId": 1, "avgPrice": str(outside_fill),
                "executedQty": str(plan.quantity),
            }
            client.position_quantities = [str(plan.quantity), "0"]
            client.close_side_effects = [{
                "status": "FILLED", "executedQty": str(plan.quantity), "orderId": 2,
            }]
            with self.assertRaisesRegex(
                BinanceAPIError, "N18 post-fill adjustment or protection failed"
            ):
                trader.open_long_plan_with_protection(plan)
            self.assertEqual(client.close_calls, [("N18USDT", plan.quantity)])
            self.assertEqual(client.protection_calls, [])
            self.assertIn("execution_cleanup_resolved", state.load().orders)

    def test_n18_schema_catalog_dependency_and_root_matrix_is_zero_write(self):
        cases = (
            [("index", name) for name in sorted(N18_INDEX_SQL)]
            + [("trigger", name) for name in sorted(N18_TRIGGER_SQL)]
            + [
                ("table", "n18_history_coverage"),
                ("foreign_trigger", "hostile_n18_trigger"),
                ("incoming_fk", "hostile_n18_fk"),
                ("root", "n18_lifecycle_installation"),
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
                _install_n18_lifecycle_boundary(database, ledger)
                _install_n20_lifecycle_boundary(database, ledger)
                _install_micro_lifecycle_boundary(database, ledger)
                with closing(sqlite3.connect(database)) as connection:
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    connection.execute("PRAGMA journal_mode=DELETE")
                    if mode == "index":
                        table = (
                            "n18_history_coverage"
                            if name == "idx_n18_coverage_symbol"
                            else "n18_triangle_states"
                        )
                        connection.execute('DROP INDEX "%s"' % name)
                        connection.execute(
                            'CREATE INDEX "%s" ON %s(symbol COLLATE NOCASE DESC)'
                            % (name, table)
                        )
                    elif mode == "trigger":
                        connection.execute('DROP TRIGGER "%s"' % name)
                    elif mode == "table":
                        connection.execute("DROP TABLE n18_history_coverage")
                    elif mode == "foreign_trigger":
                        connection.execute("CREATE TABLE hostile_probe(value INTEGER)")
                        connection.execute(
                            "CREATE TRIGGER hostile_n18_trigger AFTER UPDATE ON "
                            "n18_triangle_states BEGIN INSERT INTO hostile_probe VALUES(1); END"
                        )
                    elif mode == "incoming_fk":
                        connection.execute(
                            "CREATE TABLE hostile_n18_fk(parent_id INTEGER "
                            "REFERENCES n18_triangle_states(id) ON DELETE CASCADE)"
                        )
                    else:
                        connection.execute("DROP TRIGGER trg_n18_installation_immutable")
                        connection.execute("PRAGMA ignore_check_constraints=ON")
                        connection.execute(
                            "UPDATE n18_lifecycle_installation SET guard_sha256=?",
                            ("f" * 64,),
                        )
                    connection.commit()
                review_before = database.read_bytes()
                ledger_before = ledger.read_bytes()
                entries = sorted(os.listdir(directory))
                with self.assertRaisesRegex(RuntimeError, "N18"):
                    ReviewRecorder(
                        database, logging.getLogger("n18-schema-matrix"), ledger
                    )
                self.assertEqual(database.read_bytes(), review_before)
                self.assertEqual(ledger.read_bytes(), ledger_before)
                self.assertEqual(sorted(os.listdir(directory)), entries)

    def test_n16_n17_n19_diagnostics_precede_pre_n18_runtime_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            ledger = Path(directory) / "claim.sqlite3"
            with closing(sqlite3.connect(database)) as connection:
                connection.commit()
            with self.assertRaisesRegex(RuntimeError, "pre-N16"):
                ReviewRecorder(database, logging.getLogger("n18-priority-n16"), ledger)
            _install_n16_claim_boundary(database, ledger)
            with self.assertRaisesRegex(RuntimeError, "pre-N17"):
                ReviewRecorder(database, logging.getLogger("n18-priority-n17"), ledger)
            _install_n17_lifecycle_boundary(database, ledger)
            with self.assertRaisesRegex(RuntimeError, "pre-N19"):
                ReviewRecorder(database, logging.getLogger("n18-priority-n19"), ledger)
            _install_n19_lifecycle_boundary(database, ledger)
            with self.assertRaisesRegex(RuntimeError, "pre-N18"):
                ReviewRecorder(database, logging.getLogger("n18-priority-n18"), ledger)

    def test_schema_catalog_is_exact(self):
        self.assertEqual(len(N18_INDEX_SQL), 5)
        self.assertEqual(len(N18_TRIGGER_SQL), 6)
        self.assertEqual(set(N18_INDEX_SQL), {
            "idx_n18_triangle_active", "idx_n18_triangle_family",
            "idx_n18_triangle_structure", "idx_n18_triangle_symbol_latest",
            "idx_n18_coverage_symbol",
        })


if __name__ == "__main__":
    unittest.main()
