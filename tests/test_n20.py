from __future__ import annotations

from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import unittest
from unittest.mock import patch
import zlib
from types import SimpleNamespace

import trading_bot.n20_analyzer as n20_module

from trading_bot.monitor import FundingCandidate
from trading_bot.main import TradingBot, _N20PlanIntegrityError
from trading_bot.monitor import StrategyMarketScan
from trading_bot.n20_analyzer import (
    INTERVAL_MS,
    N20_CANONICAL_MAX_BYTES,
    N20_COMPRESSED_MAX_BYTES,
    N20StateRecord,
    analyze_n20_market_episode,
    canonical_json,
    compress_n20_evidence,
    decode_n20_state_evidence,
    decompress_n20_evidence,
)
from trading_bot.n20_schema import (
    N20_INDEX_SQL,
    N20_TRIGGER_SQL,
    install_n20_schema,
    n20_schema_status,
)
from trading_bot.recorder import (
    PAPER_TRADE_VOID_CONFIRMATION,
    ReviewRecorder,
    StrategySignalBatchWriteResult,
)
from trading_bot.binance_client import BinanceAPIError
from trading_bot.state import StateStore
from trading_bot.trader import EntryWindowExpiredError, Trader
from trading_bot.signal_retention import (
    _install_n16_claim_boundary,
    _install_n17_lifecycle_boundary,
    _install_n18_lifecycle_boundary,
    _install_n19_lifecycle_boundary,
    _install_n20_lifecycle_boundary,
    _install_coverage_epoch_boundary,
    _install_micro_lifecycle_boundary,
    vacuum_signal_database_into,
)
from trading_bot.strategies import N20_STRATEGY, N20StrategyDefinition, load_all_strategies
from trading_bot.strategy_scheduler import StrategyScheduler
from tests.recorder_test_utils import (
    make_test_recorder,
    seal_test_database_catalog,
)
from tests.test_trader import FakeLiveExecutionClient, RuleConstrainedClient, live_test_config


BASE_TIME = 1_950_000_000_000 // INTERVAL_MS * INTERVAL_MS


def _row(index: int, open_: Decimal, high: Decimal, low: Decimal, close: Decimal,
         *, volume_multiple: Decimal = Decimal("1"), taker_ratio: Decimal = Decimal("0.60")):
    quote = close * volume_multiple
    base = volume_multiple
    return [
        BASE_TIME + index * INTERVAL_MS,
        str(open_), str(high), str(low), str(close), str(base),
        BASE_TIME + (index + 1) * INTERVAL_MS - 1,
        str(quote), "0", "0", str(quote * taker_ratio),
    ]


def market_fixture(*, through: int = 121):
    symbols = tuple("L%03dUSDT" % index for index in range(100))
    result = {}
    for rank, symbol in enumerate(symbols, start=1):
        offset = Decimal(rank - 1) / Decimal("100")
        rows = []
        for index in range(through + 1):
            close = Decimal("100") + offset + Decimal(index) * Decimal("0.08")
            rows.append(_row(index, close - Decimal("0.05"), close + Decimal("0.30"),
                             close - Decimal("0.30"), close))
        m0 = Decimal("100") + offset + Decimal(119) * Decimal("0.08")
        rows[119] = _row(119, m0 - Decimal("0.05"), m0 + Decimal("0.20"),
                         m0 - Decimal("0.30"), m0)
        if through >= 120:
            rows[120] = _row(120, m0 - Decimal("0.20"), m0 - Decimal("0.10"),
                             m0 - Decimal("0.75"), m0 - Decimal("0.60"))
        if through >= 121:
            shallow = rank <= 20
            d2_low = m0 - (Decimal("0.85") if shallow else Decimal("1.65"))
            d2_close = m0 - (Decimal("0.75") if shallow else Decimal("1.55"))
            rows[121] = _row(121, d2_close + Decimal("0.10"), d2_close + Decimal("0.15"),
                             d2_low, d2_close)
        if through >= 122:
            shallow = rank <= 20
            c_close = m0 + (Decimal("0.30") if shallow else Decimal("0.10"))
            rows[122] = _row(122, c_close - Decimal("0.30"), c_close + Decimal("0.10"),
                             c_close - Decimal("0.35"), c_close,
                             volume_multiple=Decimal("2"), taker_ratio=Decimal("0.65"))
        if through >= 123:
            c_close = Decimal(rows[122][4])
            rows[123] = _row(123, c_close, c_close + Decimal("0.20"),
                             c_close - Decimal("0.10"), c_close + Decimal("0.01"))
        result[symbol] = rows
    return symbols, result


def analyze(symbols, market, *, checked_index: int, frozen=None):
    kwargs = {}
    if frozen is not None:
        kwargs.update(
            frozen_blob=frozen.encoded[0], frozen_size=frozen.encoded[1],
            frozen_sha256=frozen.encoded[2],
        )
    return analyze_n20_market_episode(
        [(symbol, rank) for rank, symbol in enumerate(symbols, start=1)], market,
        checked_at_ms=BASE_TIME + checked_index * INTERVAL_MS + 1_000,
        **kwargs,
    )


class N20DefinitionAndAnalyzerTests(unittest.TestCase):
    def test_registration_is_exact_and_prior_projection_is_unchanged(self):
        strategies = load_all_strategies()
        self.assertEqual([item.strategy_id for item in strategies], ["N%02d" % i for i in range(1, 26)])
        self.assertIsInstance(strategies[19], N20StrategyDefinition)
        self.assertEqual(strategies[19], N20_STRATEGY)
        self.assertEqual(N20_STRATEGY.risk_reward_ratio, Decimal("5"))
        prior = json.dumps(
            [item.to_jsonable() for item in strategies[:19]],
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()
        self.assertEqual(hashlib.sha256(prior).hexdigest(),
                         "33cac88df5c534b06d129c66c283646c58e0740b33f9827f3116778e818167fb")

    def test_realtime_d1_freezes_then_first_recovery_selects_one_winner(self):
        symbols, initial = market_fixture(through=121)
        first = analyze(symbols, initial, checked_index=121)
        self.assertTrue(first.complete, first.reason)
        self.assertEqual(first.reason, "N20_PULLBACK_ACTIVE")
        self.assertEqual(first.state_record.stage, "PULLBACK_ACTIVE")
        self.assertEqual(len(first.frozen_symbols), 100)
        decoded = decode_n20_state_evidence(*first.state_record.encoded)
        self.assertEqual(decoded.episode_id, first.state_record.episode_id)

        _, recovered = market_fixture(through=123)
        second = analyze(symbols, recovered, checked_index=123, frozen=first.state_record)
        self.assertTrue(second.complete, second.reason)
        passed = [item for item in second.results.values() if item.passed]
        self.assertEqual(len(passed), 1)
        self.assertEqual(passed[0].symbol, symbols[0])
        self.assertEqual(passed[0].reason, "PASSED")
        self.assertEqual(second.state_record.stage, "CONFIRMED")
        self.assertTrue(all(item.state_record.stage == "CONFIRMED" for item in second.results.values()))
        self.assertTrue(any(item.reason == "N20_NOT_WINNER" for item in second.results.values()))

    def test_shared_episode_evidence_is_compressed_once_for_all_signal_details(self):
        symbols, market = market_fixture(through=121)
        with patch.object(
            n20_module.zlib,
            "compress",
            wraps=n20_module.zlib.compress,
        ) as compress:
            result = analyze(symbols, market, checked_index=121)
            self.assertTrue(result.complete, result.reason)
            expected_sha256 = result.state_record.encoded[2]
            for analysis in result.results.values():
                detail = analysis.detail_json()
                self.assertEqual(
                    detail["episode_evidence_sha256"],
                    expected_sha256,
                )
        # One compression constructs the shared episode.  The final explicit
        # state_record.encoded assertion above performs one independent
        # integrity recomputation; the 100 signal details perform none.
        self.assertEqual(compress.call_count, 2)

    def test_frozen_source_overlap_is_exact_and_evidence_only_appends(self):
        symbols, initial = market_fixture(through=121)
        first = analyze(symbols, initial, checked_index=121)
        self.assertTrue(first.complete, first.reason)
        _, recovered = market_fixture(through=123)
        second = analyze(
            symbols, recovered, checked_index=123, frozen=first.state_record,
        )
        self.assertTrue(second.complete, second.reason)
        for symbol in symbols:
            before = first.state_record.evidence["source"][symbol]
            after = second.state_record.evidence["source"][symbol]
            self.assertEqual(after[:len(before)], before)
            self.assertEqual(len(after), len(before) + 2)

        _, intermediate = market_fixture(through=122)
        progressed = analyze(
            symbols, intermediate, checked_index=122, frozen=first.state_record,
        )
        self.assertTrue(progressed.complete, progressed.reason)

        target = symbols[0]
        field_mutations = {
            0: lambda value: value + 1,
            1: lambda value: str(Decimal(value) + Decimal("0.001")),
            2: lambda value: str(Decimal(value) + Decimal("0.001")),
            3: lambda value: str(Decimal(value) - Decimal("0.001")),
            4: lambda value: str(Decimal(value) + Decimal("0.001")),
            5: lambda value: str(Decimal(value) + Decimal("0.001")),
            7: lambda value: str(Decimal(value) + Decimal("0.001")),
            10: lambda value: str(Decimal(value) - Decimal("0.001")),
        }
        for field, mutate in field_mutations.items():
            with self.subTest(kind="d2_field", field=field):
                changed = deepcopy(recovered)
                changed[target][121][field] = mutate(changed[target][121][field])
                result = analyze(
                    symbols, changed, checked_index=123,
                    frozen=progressed.state_record,
                )
                self.assertFalse(result.complete)
                self.assertIn(result.reason, {
                    "N20_FROZEN_SOURCE_CONFLICT", "N20_FROZEN_MEMBER_MISSING",
                })
        for label, index in (("m0", 119), ("d1", 120)):
            with self.subTest(kind=label):
                changed = deepcopy(recovered)
                changed[target][index][4] = str(
                    Decimal(changed[target][index][4]) + Decimal("0.010952")
                )
                result = analyze(
                    symbols, changed, checked_index=123,
                    frozen=first.state_record,
                )
                self.assertFalse(result.complete)
                self.assertEqual(result.reason, "N20_FROZEN_SOURCE_CONFLICT")
        for label, index, frozen in (
            ("m0", 119, first.state_record),
            ("d1", 120, first.state_record),
            ("d2", 121, progressed.state_record),
        ):
            with self.subTest(kind="deleted_" + label):
                changed = deepcopy(recovered)
                del changed[target][index]
                result = analyze(
                    symbols, changed, checked_index=123,
                    frozen=frozen,
                )
                self.assertFalse(result.complete)

    def test_live_tail_can_change_until_close_but_never_rewrites_closed_source(self):
        symbols, initial = market_fixture(through=121)
        first = analyze(symbols, initial, checked_index=121)
        self.assertTrue(first.complete, first.reason)
        target = symbols[0]
        live_changed = deepcopy(initial)
        live_changed[target][121][4] = str(
            Decimal(live_changed[target][121][4]) + Decimal("0.01")
        )
        replay = analyze(
            symbols, live_changed, checked_index=121, frozen=first.state_record,
        )
        self.assertTrue(replay.complete, replay.reason)
        self.assertEqual(
            replay.state_record.evidence["source"],
            first.state_record.evidence["source"],
        )

        _, next_window = market_fixture(through=122)
        next_window[target][121] = deepcopy(live_changed[target][121])
        finalized = analyze(
            symbols, next_window, checked_index=122, frozen=first.state_record,
        )
        self.assertTrue(finalized.complete, finalized.reason)
        old_rows = first.state_record.evidence["source"][target]
        new_rows = finalized.state_record.evidence["source"][target]
        self.assertEqual(new_rows[:len(old_rows)], old_rows)
        self.assertEqual(new_rows[-1]["open_time_ms"], next_window[target][121][0])
        self.assertEqual(new_rows[-1]["close"], next_window[target][121][4])

        changed_closed = deepcopy(live_changed)
        changed_closed[target][120][4] = str(
            Decimal(changed_closed[target][120][4]) + Decimal("0.001")
        )
        self.assertFalse(analyze(
            symbols, changed_closed, checked_index=121,
            frozen=first.state_record,
        ).complete)
        bad_axis = deepcopy(live_changed)
        bad_axis[target][121][0] = bad_axis[target][120][0]
        self.assertFalse(analyze(
            symbols, bad_axis, checked_index=121,
            frozen=first.state_record,
        ).complete)

    def test_membership_axis_and_numeric_input_fail_closed(self):
        symbols, market = market_fixture()
        for label, members in (
            ("missing", symbols[:-1]),
            ("duplicate", symbols[:-1] + (symbols[0],)),
            ("extra", symbols + ("EXTRAUSDT",)),
        ):
            with self.subTest(label=label):
                result = analyze_n20_market_episode(
                    [(symbol, rank) for rank, symbol in enumerate(members, start=1)], market,
                    checked_at_ms=BASE_TIME + 121 * INTERVAL_MS + 1_000,
                )
                self.assertFalse(result.complete)
        broken = deepcopy(market)
        broken[symbols[-1]][-1][0] += 1
        self.assertFalse(analyze(symbols, broken, checked_index=121).complete)
        broken = deepcopy(market)
        broken[symbols[-1]][-1][5] = "NaN"
        self.assertFalse(analyze(symbols, broken, checked_index=121).complete)
        broken = deepcopy(market)
        broken[symbols[-1]][-1][5] = "0"
        self.assertFalse(analyze(symbols, broken, checked_index=121).complete)

    def test_all_context_recovery_and_candidate_threshold_edges_are_exact(self):
        config = dict(n20_module._APPROVED_CONFIG)
        context = {
            "positive_breadth": Decimal("0.65"),
            "above_vwap_breadth": Decimal("0.60"),
            "d1_down_breadth": Decimal("0.55"),
            "d1_median_return": Decimal("-0.0015"),
            "config": config,
        }
        self.assertTrue(n20_module._context_thresholds_pass(**context))
        for field, outside in (
            ("positive_breadth", Decimal("0.649")),
            ("above_vwap_breadth", Decimal("0.599")),
            ("d1_down_breadth", Decimal("0.549")),
            ("d1_median_return", Decimal("-0.001499")),
        ):
            changed = dict(context)
            changed[field] = outside
            self.assertFalse(n20_module._context_thresholds_pass(**changed), field)

        recovery = {
            "up_breadth": Decimal("0.55"),
            "breadth_improvement": Decimal("0.20"),
            "median_step_return": Decimal("0.000001"),
            "pullback_bars": 2,
            "config": config,
        }
        self.assertTrue(n20_module._recovery_thresholds_pass(**recovery))
        for field, value in (
            ("up_breadth", Decimal("0.549")),
            ("breadth_improvement", Decimal("0.199")),
            ("median_step_return", Decimal("0")),
            ("pullback_bars", 1),
            ("pullback_bars", 7),
        ):
            changed = dict(recovery)
            changed[field] = value
            self.assertFalse(n20_module._recovery_thresholds_pass(**changed), (field, value))
        recovery["pullback_bars"] = 6
        self.assertTrue(n20_module._recovery_thresholds_pass(**recovery))
        self.assertEqual(n20_module._pullback_classification(
            Decimal("0.003999"), Decimal("0.79"), config), "WAIT")
        self.assertEqual(n20_module._pullback_classification(
            Decimal("0.004"), Decimal("0.79"), config), "ELIGIBLE")
        self.assertEqual(n20_module._pullback_classification(
            Decimal("0.025"), Decimal("0.80"), config), "ELIGIBLE")
        self.assertEqual(n20_module._pullback_classification(
            Decimal("0.025001"), Decimal("0.79"), config), "TOO_DEEP")
        self.assertEqual(n20_module._pullback_classification(
            Decimal("0.030"), Decimal("0.80"), config), "CRASH")
        self.assertEqual(n20_module._pullback_classification(
            Decimal("0.030"), Decimal("0.79"), config), "TOO_DEEP")

        candidate_values = {
            "baseline_rank": 20, "return96": Decimal("0.000001"),
            "above_vwap96": True, "relative_resilience": Decimal("0.50"),
            "drawdown_atr": Decimal("1.50"), "resilience_rank": 20,
            "bullish": True, "broke_prior_high": True, "above_vwap20": True,
            "recovery_ratio": Decimal("0.50"),
            "close_location": Decimal("0.65"), "taker_ratio": Decimal("0.52"),
            "volume_multiple": Decimal("0.80"), "config": config,
        }
        self.assertTrue(n20_module._candidate_thresholds_pass(**candidate_values))
        for field, value in (
            ("baseline_rank", 21), ("return96", Decimal("0")),
            ("above_vwap96", False), ("relative_resilience", Decimal("0.499")),
            ("drawdown_atr", Decimal("1.501")), ("resilience_rank", 21),
            ("bullish", False), ("broke_prior_high", False),
            ("above_vwap20", False), ("recovery_ratio", Decimal("0.499")),
            ("close_location", Decimal("0.649")),
            ("taker_ratio", Decimal("0.519")),
            ("volume_multiple", Decimal("0.799")),
        ):
            changed = dict(candidate_values)
            changed[field] = value
            self.assertFalse(n20_module._candidate_thresholds_pass(**changed), (field, value))

    def test_entry_window_price_and_p_boundaries(self):
        symbols, initial = market_fixture(through=121)
        frozen = analyze(symbols, initial, checked_index=121).state_record
        _, recovered = market_fixture(through=123)
        base = analyze(symbols, recovered, checked_index=123, frozen=frozen)
        winner = next(item for item in base.results.values() if item.passed).winner
        cases = (
            ("elapsed_zero", winner.entry_min, winner.p, 0, True, "PASSED"),
            ("elapsed_last", winner.entry_max, winner.p, 119_999, True, "PASSED"),
            ("below", winner.entry_min - Decimal("0.001"), winner.p, 1, False, "N20_ENTRY_BELOW_MIN_WAITING"),
            ("above", winner.entry_max + Decimal("0.001"), winner.p, 1, False, "N20_ENTRY_PRICE_ABOVE_MAX"),
            ("deadline", winner.entry_min, winner.p, 120_000, False, "N20_ENTRY_WINDOW_EXPIRED"),
            ("p_break", winner.entry_min, winner.p - Decimal("0.001"), 1, False, "N20_ENTRY_BROKE_P"),
        )
        for label, close, low, elapsed, expected, reason in cases:
            with self.subTest(label=label):
                changed = deepcopy(recovered)
                symbol = winner.symbol
                row = changed[symbol][-1]
                row[1], row[2], row[3], row[4] = str(close), str(max(close, winner.entry_max)), str(low), str(close)
                row[5], row[7], row[10] = "1", str(close), str(close * Decimal("0.6"))
                result = analyze_n20_market_episode(
                    [(item, rank) for rank, item in enumerate(symbols, start=1)], changed,
                    checked_at_ms=BASE_TIME + 123 * INTERVAL_MS + elapsed,
                    frozen_blob=frozen.encoded[0], frozen_size=frozen.encoded[1],
                    frozen_sha256=frozen.encoded[2],
                ).results[symbol]
                self.assertEqual((result.passed, result.reason), (expected, reason))

    def test_first_recovery_and_crash_priority_are_permanent(self):
        symbols, initial = market_fixture(through=121)
        frozen = analyze(symbols, initial, checked_index=121).state_record
        _, recovered = market_fixture(through=123)
        for symbol in symbols:
            # The first market recovery still qualifies, but every individual
            # candidate fails its close-location gate. A later bar cannot replace C.
            recovered[symbol][122][1:5] = [recovered[symbol][122][1], recovered[symbol][122][2],
                                           recovered[symbol][122][3], recovered[symbol][122][1]]
        consumed = analyze(symbols, recovered, checked_index=123, frozen=frozen)
        self.assertEqual(consumed.state_record.stage, "CONSUMED")
        self.assertEqual(consumed.reason, "N20_NO_QUALIFIED_LEADER")

        deep = deepcopy(recovered)
        for index, symbol in enumerate(symbols):
            m0 = Decimal(deep[symbol][119][4])
            value = m0 * (Decimal("0.96") if index < 80 else Decimal("1.01"))
            deep[symbol][121][1:5] = [str(value), str(value + Decimal("0.1")),
                                      str(value - Decimal("0.1")), str(value)]
        crash = analyze(symbols, deep, checked_index=123, frozen=frozen)
        self.assertEqual(crash.reason, "N20_SYSTEMIC_CRASH_VETO")
        self.assertEqual(crash.state_record.stage, "CRASH_VETO")

    def test_realtime_only_and_six_level_winner_order_are_exact(self):
        symbols, historical = market_fixture(through=123)
        result = analyze(symbols, historical, checked_index=123)
        self.assertIsNone(result.state_record)
        self.assertIn(
            result.reason,
            {"N20_PULLBACK_NOT_STARTED", "N20_BULL_CONTEXT_NOT_MET"},
        )

        base = {
            "recovery_ratio": "0.60", "resilience_rank": 2,
            "baseline_rank": 3, "taker_ratio": "0.60",
            "quote_volume_rank": 4, "symbol": "ZZZUSDT",
        }
        fields = (
            ("recovery_ratio", "0.61"), ("resilience_rank", 1),
            ("baseline_rank", 2), ("taker_ratio", "0.61"),
            ("quote_volume_rank", 3), ("symbol", "AAAUSDT"),
        )
        for field, better in fields:
            with self.subTest(field=field):
                preferred = dict(base)
                preferred[field] = better
                self.assertLess(
                    n20_module._winner_sort_key(preferred),
                    n20_module._winner_sort_key(base),
                )


class N20EvidenceAndSchemaTests(unittest.TestCase):
    def test_frozen_evidence_rejects_numeric_type_coercion(self):
        symbols, source = market_fixture(through=121)
        result = analyze(symbols, source, checked_index=121)
        self.assertTrue(result.complete, result.reason)
        for path, replacement in (
            (("schema_version",), True),
            (("schema_version",), 1.0),
            (("members", 0, "rank"), True),
            (("members", 0, "rank"), 1.0),
            (("source", symbols[0], -1, "open_time_ms"), True),
            (("m0_open_time_ms",), 1.0),
        ):
            with self.subTest(path=path, replacement=replacement):
                evidence = deepcopy(result.state_record.evidence)
                target = evidence
                for component in path[:-1]:
                    target = target[component]
                target[path[-1]] = replacement
                unsigned = dict(evidence)
                unsigned.pop("canonical_sha256", None)
                evidence["canonical_sha256"] = n20_module.canonical_sha256(
                    unsigned
                )
                record = replace(result.state_record, evidence=evidence)
                with self.assertRaises(ValueError):
                    decode_n20_state_evidence(*record.encoded)

    def test_stage_reason_and_approved_config_are_write_before_boundaries(self):
        symbols, source = market_fixture(through=121)
        active = analyze(symbols, source, checked_index=121).state_record

        def changed_record(*, stage=None, reason=None, config_change=None):
            evidence = deepcopy(active.evidence)
            if stage is not None:
                evidence["stage"] = stage
            if reason is not None:
                evidence["reason"] = reason
            if config_change is not None:
                config_change(evidence["config"])
            unsigned = dict(evidence)
            unsigned.pop("canonical_sha256", None)
            evidence["canonical_sha256"] = n20_module.canonical_sha256(unsigned)
            return replace(
                active, stage=evidence["stage"], reason=evidence["reason"],
                evidence=evidence,
            )

        all_reasons = [
            reason
            for reasons in n20_module.N20_STAGE_REASONS.values()
            for reason in reasons
        ]
        wrong_pairs = []
        for stage, reasons in n20_module.N20_STAGE_REASONS.items():
            for reason in reasons:
                n20_module.validate_n20_stage_reason(stage, reason)
            wrong_reason = next(reason for reason in all_reasons if reason not in reasons)
            wrong_pairs.append((stage, wrong_reason))
            with self.subTest(stage=stage, wrong_reason=wrong_reason):
                with self.assertRaises(ValueError):
                    n20_module.validate_n20_stage_reason(stage, wrong_reason)
        wrong_pairs.extend((
            ("N20_UNKNOWN_STAGE", "N20_PULLBACK_ACTIVE"),
            ("PULLBACK_ACTIVE", "N20_UNKNOWN_REASON"),
        ))
        config_changes = (
            ("integer_bool", lambda config: config.__setitem__("atr_period", True)),
            ("integer_float", lambda config: config.__setitem__("atr_period", 14.0)),
            ("decimal_changed", lambda config: config.__setitem__("pullback_depth_min", "0.005")),
            ("decimal_number", lambda config: config.__setitem__("pullback_depth_min", 0.004)),
            ("missing", lambda config: config.pop("atr_period")),
            ("extra", lambda config: config.__setitem__("extra", 1)),
        )
        with tempfile.TemporaryDirectory() as temp:
            recorder = make_test_recorder(
                Path(temp) / "review.sqlite3", logging.getLogger("n20-contract"),
            )
            self.assertEqual(recorder.record_n20_episode(active), "INSERTED")
            with recorder._read_only_runtime_snapshot() as connection:
                before = connection.execute(
                    "SELECT stage,reason,evidence_sha256,updated_at FROM "
                    "n20_market_episodes WHERE episode_id=?",
                    (active.episode_id,),
                ).fetchone()
                ledger_before = connection.execute(
                    "SELECT COUNT(*) FROM strategy_passed_structure_ledger"
                ).fetchone()[0]
            for stage, reason in wrong_pairs:
                with self.subTest(stage=stage, reason=reason):
                    record = changed_record(stage=stage, reason=reason)
                    with self.assertRaises(ValueError):
                        decode_n20_state_evidence(*record.encoded)
                    self.assertEqual(
                        recorder.record_n20_episode(record),
                        "N20_STATE_INCONSISTENT",
                    )
            for label, mutation in config_changes:
                with self.subTest(config=label):
                    record = changed_record(config_change=mutation)
                    with self.assertRaises(ValueError):
                        decode_n20_state_evidence(*record.encoded)
                    self.assertEqual(
                        recorder.record_n20_episode(record),
                        "N20_STATE_INCONSISTENT",
                    )
            with recorder._read_only_runtime_snapshot() as connection:
                after = connection.execute(
                    "SELECT stage,reason,evidence_sha256,updated_at FROM "
                    "n20_market_episodes WHERE episode_id=?",
                    (active.episode_id,),
                ).fetchone()
                ledger_after = connection.execute(
                    "SELECT COUNT(*) FROM strategy_passed_structure_ledger"
                ).fetchone()[0]
            self.assertEqual(after, before)
            self.assertEqual(ledger_after, ledger_before)

    def test_compressed_envelope_rejects_corruption_trailing_duplicate_and_bomb(self):
        value = {"a": [1, 2, 3], "b": "ok"}
        blob, size, digest = compress_n20_evidence(value)
        self.assertEqual(decompress_n20_evidence(blob, size, digest), value)
        for label, changed_blob, changed_size, changed_digest in (
            ("trailing", blob + b"x", size, digest),
            ("truncated", blob[:-1], size, digest),
            ("hash", blob, size, "0" * 64),
            ("compressed_limit", b"x" * (N20_COMPRESSED_MAX_BYTES + 1), size, digest),
        ):
            with self.subTest(label=label):
                with self.assertRaises(ValueError):
                    decompress_n20_evidence(changed_blob, changed_size, changed_digest)
        raw = b'{"a":1,"a":2}'
        with self.assertRaises(ValueError):
            decompress_n20_evidence(zlib.compress(raw), len(raw), hashlib.sha256(raw).hexdigest())
        bomb = zlib.compress(b"x" * (N20_CANONICAL_MAX_BYTES + 1))
        with self.assertRaises(ValueError):
            decompress_n20_evidence(bomb, N20_CANONICAL_MAX_BYTES, hashlib.sha256(b"x").hexdigest())

    def test_evidence_size_limits_allow_equal_and_reject_plus_one(self):
        overhead = len(canonical_json({"x": ""}).encode("utf-8"))
        exact = {"x": "a" * (N20_CANONICAL_MAX_BYTES - overhead)}
        with patch.object(n20_module.zlib, "compress", return_value=b"x"):
            _blob, size, _digest = compress_n20_evidence(exact)
        self.assertEqual(size, N20_CANONICAL_MAX_BYTES)
        with self.assertRaisesRegex(ValueError, "8MiB"):
            compress_n20_evidence({"x": exact["x"] + "a"})
        with patch.object(
            n20_module.zlib, "compress",
            return_value=b"x" * N20_COMPRESSED_MAX_BYTES,
        ):
            blob, _size, _digest = compress_n20_evidence({"x": "ok"})
        self.assertEqual(len(blob), N20_COMPRESSED_MAX_BYTES)
        with patch.object(
            n20_module.zlib, "compress",
            return_value=b"x" * (N20_COMPRESSED_MAX_BYTES + 1),
        ), self.assertRaisesRegex(ValueError, "2MiB"):
            compress_n20_evidence({"x": "ok"})

    def test_schema_install_is_explicit_exact_and_immutable(self):
        with closing(sqlite3.connect(":memory:")) as connection:
            self.assertEqual(n20_schema_status(connection), "PRE_N20")
            install_n20_schema(connection, "2026-01-01T00:00:00+00:00")
            self.assertEqual(n20_schema_status(connection), "CURRENT")
            self.assertEqual(
                {row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_schema WHERE "
                    "(name LIKE 'n20_%' OR tbl_name LIKE 'n20_%') "
                    "AND name NOT LIKE 'sqlite_autoindex_%'"
                )},
                {"n20_market_episodes", "n20_lifecycle_installation", *N20_INDEX_SQL, *N20_TRIGGER_SQL},
            )
            connection.execute("DROP INDEX idx_n20_episode_identity")
            with self.assertRaises(RuntimeError):
                n20_schema_status(connection)

    def test_schema_catalog_rejects_every_owned_index_trigger_and_foreign_edge(self):
        cases = (
            [("index", name) for name in sorted(N20_INDEX_SQL)]
            + [("trigger", name) for name in sorted(N20_TRIGGER_SQL)]
            + [("extra_table", "n20_hostile"), ("incoming_fk", "hostile_n20_fk")]
        )
        for mode, name in cases:
            with self.subTest(mode=mode, name=name), closing(
                sqlite3.connect(":memory:")
            ) as connection:
                connection.execute("PRAGMA foreign_keys=ON")
                install_n20_schema(connection, "2026-01-01T00:00:00+00:00")
                if mode == "index":
                    connection.execute('DROP INDEX "%s"' % name)
                    connection.execute(
                        'CREATE INDEX "%s" ON n20_market_episodes(reason)' % name
                    )
                elif mode == "trigger":
                    connection.execute('DROP TRIGGER "%s"' % name)
                    table = (
                        "n20_lifecycle_installation"
                        if "installation" in name else "n20_market_episodes"
                    )
                    connection.execute(
                        'CREATE TRIGGER "%s" BEFORE DELETE ON "%s" '
                        "BEGIN SELECT RAISE(ABORT,'wrong'); END" % (name, table)
                    )
                elif mode == "extra_table":
                    connection.execute("CREATE TABLE n20_hostile(id INTEGER)")
                else:
                    connection.execute(
                        "CREATE TABLE hostile_n20_fk(id INTEGER PRIMARY KEY,"
                        "episode_id INTEGER REFERENCES n20_market_episodes(id))"
                    )
                with self.assertRaises(RuntimeError):
                    n20_schema_status(connection)

    def test_recorder_persists_and_restarts_frozen_episode(self):
        symbols, market = market_fixture(through=121)
        state = analyze(symbols, market, checked_index=121).state_record
        with tempfile.TemporaryDirectory() as temp:
            database = str(Path(temp) / "review.sqlite3")
            recorder = make_test_recorder(database, logging.getLogger("test-n20"))
            self.assertEqual(recorder.record_n20_episode(state), "INSERTED")
            self.assertEqual(recorder.record_n20_episode(state), "UNCHANGED")
            restored = recorder.get_active_n20_episode()
            self.assertEqual(restored.episode_id, state.episode_id)
            self.assertEqual(set(recorder.get_required_n20_frozen_symbols()), set(symbols))
            restarted = make_test_recorder(database, logging.getLogger("test-n20-restart"))
            self.assertEqual(restarted.get_active_n20_episode().evidence_sha256, state.encoded[2])

    def test_recorder_independently_requires_append_only_member_sources(self):
        symbols, initial = market_fixture(through=121)
        first = analyze(symbols, initial, checked_index=121).state_record
        _, recovered = market_fixture(through=123)
        second = analyze(
            symbols, recovered, checked_index=123, frozen=first,
        ).state_record
        target = symbols[0]

        def changed_record(mode):
            evidence = deepcopy(second.evidence)
            if mode == "changed":
                evidence["source"][target][119]["close"] = "109.530952"
            elif mode == "deleted":
                del evidence["source"][target][0]
            else:
                other = symbols[1]
                evidence["source"][target], evidence["source"][other] = (
                    evidence["source"][other], evidence["source"][target],
                )
            unsigned = dict(evidence)
            unsigned.pop("canonical_sha256", None)
            evidence["canonical_sha256"] = n20_module.canonical_sha256(unsigned)
            return replace(second, evidence=evidence)

        for mode in ("changed", "deleted", "member_conflict"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                recorder = make_test_recorder(
                    Path(temp) / "review.sqlite3", logging.getLogger("n20-source-" + mode),
                )
                self.assertEqual(recorder.record_n20_episode(first), "INSERTED")
                before = recorder.get_active_n20_episode().evidence_sha256
                self.assertEqual(
                    recorder.record_n20_episode(changed_record(mode)),
                    "N20_STATE_INCONSISTENT",
                )
                self.assertEqual(
                    recorder.get_active_n20_episode().evidence_sha256, before,
                )

        with tempfile.TemporaryDirectory() as temp:
            recorder = make_test_recorder(
                Path(temp) / "review.sqlite3", logging.getLogger("n20-source-append"),
            )
            self.assertEqual(recorder.record_n20_episode(first), "INSERTED")
            _, closing_market = market_fixture(through=122)
            closing_market[target][121][4] = str(
                Decimal(closing_market[target][121][4]) + Decimal("0.01")
            )
            finalized = analyze(
                symbols, closing_market, checked_index=122, frozen=first,
            ).state_record
            self.assertEqual(recorder.record_n20_episode(finalized), "UPDATED")
            self.assertEqual(recorder.record_n20_episode(finalized), "UNCHANGED")
            self.assertEqual(
                recorder.get_active_n20_episode().evidence_sha256,
                finalized.encoded[2],
            )

    def test_scheduler_source_conflict_keeps_batch_staged_and_execution_empty(self):
        symbols, initial = market_fixture(through=121)
        frozen = analyze(symbols, initial, checked_index=121).state_record
        _, recovered = market_fixture(through=123)
        recovered[symbols[0]][119][4] = "109.530952"
        candidates = [
            FundingCandidate(
                symbol, None, Decimal(recovered[symbol][-1][4]),
                quote_volume=Decimal("1000000"), quote_volume_rank=rank,
                candidate_universe="quote_volume_top",
            )
            for rank, symbol in enumerate(symbols, start=1)
        ]
        with tempfile.TemporaryDirectory() as temp:
            recorder = make_test_recorder(
                Path(temp) / "review.sqlite3", logging.getLogger("n20-source-gate"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            self.assertEqual(recorder.record_n20_episode(frozen), "INSERTED")
            scan_id = recorder.begin_scan(100, candidates, dry_run=True)
            scheduler = StrategyScheduler(
                (N20_STRATEGY,), 122, recorder, logging.getLogger("n20-source-gate"),
            )
            with patch.object(
                recorder, "record_n20_episode", wraps=recorder.record_n20_episode,
            ) as persist:
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": candidates, "negative_funding": []},
                    recovered,
                    BASE_TIME + 123 * INTERVAL_MS + 1_000,
                )
            self.assertEqual(persist.call_count, 0)
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(connection.execute(
                    "SELECT state FROM strategy_signal_batches WHERE scan_id=?",
                    (scan_id,),
                ).fetchone(), ("STAGING",))
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_signals WHERE scan_id=?",
                    (scan_id,),
                ).fetchone(), (100,))
            self.assertEqual(
                recorder.get_active_n20_episode().evidence_sha256,
                frozen.encoded[2],
            )

    def test_corrupt_permanent_evidence_is_rejected_before_startup_write(self):
        symbols, market = market_fixture(through=121)
        state = analyze(symbols, market, checked_index=121).state_record
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "review.sqlite3"
            recorder = make_test_recorder(database, logging.getLogger("n20-corrupt"))
            ledger = Path(recorder.n16_claim_ledger_file)
            self.assertEqual(recorder.record_n20_episode(state), "INSERTED")
            del recorder
            with closing(sqlite3.connect(database)) as connection:
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                connection.execute("PRAGMA journal_mode=DELETE")
                connection.execute(
                    "UPDATE n20_market_episodes SET evidence_blob=x'00'"
                )
                connection.commit()
            before_review = database.read_bytes()
            before_ledger = ledger.read_bytes()
            before_entries = sorted(item.name for item in root.iterdir())
            with self.assertRaisesRegex(RuntimeError, "N20 permanent episode"):
                ReviewRecorder(
                    database, logging.getLogger("n20-corrupt-restart"),
                    n16_claim_ledger_file=ledger,
                )
            self.assertEqual(database.read_bytes(), before_review)
            self.assertEqual(ledger.read_bytes(), before_ledger)
            self.assertEqual(sorted(item.name for item in root.iterdir()), before_entries)

    def test_vacuum_preserves_exact_compressed_episode_and_prior_generations(self):
        symbols, market = market_fixture(through=121)
        state = analyze(symbols, market, checked_index=121).state_record
        with tempfile.TemporaryDirectory(prefix="binance-n20-source-") as source_root, \
             tempfile.TemporaryDirectory(prefix="binance-n20-target-") as target_root:
            database = Path(source_root) / "review.sqlite3"
            target = Path(target_root) / "review.compacted.sqlite3"
            recorder = make_test_recorder(database, logging.getLogger("n20-vacuum"))
            ledger = Path(recorder.n16_claim_ledger_file)
            scan_id = recorder.begin_scan(1, [], dry_run=True)
            self.assertIsNotNone(recorder.record_strategy_signal(
                scan_id, "N01", "BTCUSDT", "-0.01", (), "1", True,
                False, "REJECTED", "NO_MATCH", detail={"n20": "vacuum"},
            ))
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            self.assertEqual(recorder.record_n20_episode(state), "INSERTED")
            with recorder._read_only_runtime_snapshot() as connection:
                before = connection.execute(
                    "SELECT strategy_id,episode_id,stage,reason,evidence_blob,"
                    "evidence_size,evidence_sha256 FROM n20_market_episodes"
                ).fetchall()
            del recorder
            for path in (database, ledger):
                with closing(sqlite3.connect(path)) as connection:
                    self.assertEqual(
                        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone(),
                        (0, 0, 0),
                    )
                    self.assertEqual(
                        connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0],
                        "delete",
                    )
                self.assertFalse(Path(str(path) + "-wal").exists())
                self.assertFalse(Path(str(path) + "-journal").exists())
                shm = Path(str(path) + "-shm")
                if shm.exists():
                    details = os.lstat(str(shm))
                    self.assertTrue(stat.S_ISREG(details.st_mode))
                    self.assertEqual(details.st_nlink, 1)
                    shm.unlink()
            report = vacuum_signal_database_into(
                str(database.resolve()), str(target.resolve()),
                n16_claim_ledger=str(ledger.resolve()),
                _allowed_roots=(Path(source_root).resolve(), Path(target_root).resolve()),
            )
            self.assertTrue(report.vacuum_performed)
            with closing(sqlite3.connect(target)) as connection:
                self.assertEqual(connection.execute(
                    "SELECT strategy_id,episode_id,stage,reason,evidence_blob,"
                    "evidence_size,evidence_sha256 FROM n20_market_episodes"
                ).fetchall(), before)
                self.assertEqual(n20_schema_status(connection), "CURRENT")

    def test_explicit_n20_upgrade_and_schema_tamper_are_zero_write(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "review.sqlite3"
            ledger = root / "claim.sqlite3"
            with closing(sqlite3.connect(database)) as connection:
                connection.commit()
            _install_n16_claim_boundary(database, ledger)
            _install_n17_lifecycle_boundary(database, ledger)
            _install_n19_lifecycle_boundary(database, ledger)
            _install_n18_lifecycle_boundary(database, ledger)
            review_before = database.read_bytes()
            ledger_before = ledger.read_bytes()
            entries_before = sorted(item.name for item in root.iterdir())
            with self.assertRaisesRegex(RuntimeError, "pre-N20"):
                ReviewRecorder(database, logging.getLogger("pre-n20"), n16_claim_ledger_file=ledger)
            self.assertEqual(database.read_bytes(), review_before)
            self.assertEqual(ledger.read_bytes(), ledger_before)
            self.assertEqual(sorted(item.name for item in root.iterdir()), entries_before)

            _install_n20_lifecycle_boundary(database, ledger)
            _install_micro_lifecycle_boundary(database, ledger)
            _install_coverage_epoch_boundary(database, ledger)
            recorder = ReviewRecorder(database, logging.getLogger("n20-current"), n16_claim_ledger_file=ledger)
            self.assertIsNone(recorder.get_active_n20_episode())
            with closing(sqlite3.connect(database)) as connection:
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                connection.execute("PRAGMA journal_mode=DELETE")
                connection.execute("DROP INDEX idx_n20_episode_latest")
                connection.execute(
                    "CREATE INDEX idx_n20_episode_latest ON n20_market_episodes(episode_id)"
                )
                connection.commit()
            review_tampered = database.read_bytes()
            ledger_tampered = ledger.read_bytes()
            entries_tampered = sorted(item.name for item in root.iterdir())
            with self.assertRaisesRegex(RuntimeError, "N20"):
                ReviewRecorder(database, logging.getLogger("n20-tamper"), n16_claim_ledger_file=ledger)
            self.assertEqual(database.read_bytes(), review_tampered)
            self.assertEqual(ledger.read_bytes(), ledger_tampered)
            self.assertEqual(sorted(item.name for item in root.iterdir()), entries_tampered)

    def test_legacy_paper_results_update_statistics_without_two_win_gate(self):
        with tempfile.TemporaryDirectory() as temp:
            recorder = make_test_recorder(Path(temp) / "review.sqlite3", logging.getLogger("n20-paper"))
            for symbol in ("AAAUSDT", "BBBUSDT"):
                trade_id = recorder.open_strategy_paper_trade(
                    "N20", symbol, "100", "99", "105", "", {}, {}
                )
                self.assertTrue(recorder.close_strategy_paper_trade(
                    trade_id, "WIN", "TAKE_PROFIT", "105", "5"
                ))
            state = recorder.get_strategy_state("N20")
            self.assertEqual(state.consecutive_wins, 0)
            self.assertEqual(state.paper_trade_count, 2)
            self.assertEqual(state.win_count, 2)
            self.assertEqual(state.loss_count, 0)
            self.assertFalse(state.live_eligible)
            void_id = recorder.open_strategy_paper_trade(
                "N20", "VOIDUSDT", "100", "99", "105", "", {}, {}
            )
            self.assertTrue(recorder.void_strategy_paper_trade(
                void_id, "N20", "VOIDUSDT", PAPER_TRADE_VOID_CONFIRMATION
            ))
            after_void = recorder.get_strategy_state("N20")
            self.assertEqual(after_void.consecutive_wins, 0)
            self.assertEqual(after_void.paper_trade_count, 2)
            self.assertEqual(after_void.win_count, 2)
            self.assertEqual(after_void.loss_count, 0)
            self.assertFalse(after_void.live_eligible)
            loss_id = recorder.open_strategy_paper_trade(
                "N20", "LOSSUSDT", "100", "99", "105", "", {}, {}
            )
            self.assertTrue(recorder.close_strategy_paper_trade(
                loss_id, "LOSS", "STOP_LOSS", "99", "-1"
            ))
            final = recorder.get_strategy_state("N20")
            self.assertEqual(final.consecutive_wins, 0)
            self.assertEqual(final.paper_trade_count, 3)
            self.assertEqual(final.win_count, 2)
            self.assertEqual(final.loss_count, 1)
            self.assertFalse(final.live_eligible)
            self.assertEqual(final.last_trade_result, "LOSS")

    def test_scheduler_frozen_candidate_set_is_exact_and_deduplicated(self):
        symbols, market = market_fixture(through=121)
        state = analyze(symbols, market, checked_index=121).state_record
        current = [FundingCandidate(symbol, None, Decimal("1"), quote_volume_rank=rank)
                   for rank, symbol in enumerate(symbols[50:] + symbols[:50], start=1)]
        analysis = analyze(symbols, market, checked_index=121)
        candidates = StrategyScheduler._n20_candidates_for_frozen_episode(current, analysis, market)
        self.assertEqual(len(candidates), 100)
        self.assertEqual({item.symbol for item in candidates}, set(symbols))
        self.assertEqual({item.quote_volume_rank for item in candidates}, set(range(1, 101)))

    def test_dropped_frozen_episode_member_obeys_bulk_global_cooldown(self):
        symbols, market = market_fixture(through=121)
        state = analyze(
            symbols, market, checked_index=121
        ).state_record
        with tempfile.TemporaryDirectory() as temp:
            recorder = make_test_recorder(
                Path(temp) / "review.sqlite3",
                logging.getLogger("n20-dropped-cooldown"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            self.assertEqual(
                recorder.record_n20_episode(state),
                "INSERTED",
            )
            self.assertTrue(
                recorder.set_symbol_cooldown(
                    symbols[0],
                    datetime.now(timezone.utc) + timedelta(hours=4),
                    "TEST_GLOBAL_COOLDOWN",
                    None,
                )
            )
            scan_id = recorder.begin_scan(0, [], dry_run=True)
            scheduler = StrategyScheduler(
                (N20_STRATEGY,),
                122,
                recorder,
                logging.getLogger("n20-dropped-cooldown"),
            )
            current_candidates = [
                FundingCandidate(
                    "NEWUSDT" if rank == 1 else symbols[rank - 1],
                    None,
                    Decimal("100"),
                    quote_volume=Decimal("1000000"),
                    quote_volume_rank=rank,
                    candidate_universe="quote_volume_top",
                )
                for rank in range(1, 101)
            ]
            shared_market = {
                **market,
                "NEWUSDT": deepcopy(market[symbols[0]]),
            }
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
                    {
                        "quote_volume_top": current_candidates,
                        "negative_funding": [],
                    },
                    shared_market,
                    BASE_TIME + 121 * INTERVAL_MS + 1_000,
                )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(len(result.signals), 100)
            self.assertNotIn(
                symbols[0],
                {signal.candidate.symbol for signal in result.signals},
            )
            self.assertEqual(
                {signal.candidate.symbol for signal in result.signals},
                {candidate.symbol for candidate in current_candidates},
            )
            self.assertNotIn(
                symbols[0],
                {item.candidate.symbol for item in result.passed_signals},
            )
            self.assertNotIn(
                symbols[0],
                {item.signal.candidate.symbol for item in result.live_candidates},
            )
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

    def test_scheduler_publishes_exact_frozen_hundred_and_one_winner(self):
        symbols, initial = market_fixture(through=121)
        candidates = [
            FundingCandidate(
                symbol, None, Decimal(initial[symbol][-1][4]),
                quote_volume=Decimal("1000000"), quote_volume_rank=rank,
                candidate_universe="quote_volume_top",
            )
            for rank, symbol in enumerate(symbols, start=1)
        ]
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "review.sqlite3"
            recorder = make_test_recorder(database, logging.getLogger("n20-scheduler"))
            recorder.upsert_strategy_definitions(load_all_strategies())
            scheduler = StrategyScheduler((N20_STRATEGY,), 122, recorder, logging.getLogger("n20-scheduler"))
            scan1 = recorder.begin_scan(100, candidates, dry_run=True)
            first = scheduler.evaluate(
                scan1, {"quote_volume_top": candidates, "negative_funding": []},
                initial, BASE_TIME + 121 * INTERVAL_MS + 1_000,
            )
            self.assertTrue(first.signal_batch_published)
            self.assertEqual(len(first.signals), 100)
            self.assertEqual(first.passed_signals, [])

            _, recovered = market_fixture(through=123)
            scan2 = recorder.begin_scan(100, candidates, dry_run=True)
            second = scheduler.evaluate(
                scan2, {"quote_volume_top": candidates, "negative_funding": []},
                recovered, BASE_TIME + 123 * INTERVAL_MS + 1_000,
            )
            self.assertTrue(second.signal_batch_published)
            self.assertEqual(len(second.signals), 100)
            self.assertEqual(len(second.passed_signals), 1)
            self.assertEqual(second.passed_signals[0].candidate.symbol, symbols[0])
            signal = second.passed_signals[0]
            analysis = signal.analysis
            plan_trader = Trader(
                RuleConstrainedClient(leverage=10, tick_size="0.01"),
                live_test_config(str(Path(temp) / "claim-account.json")),
                StateStore(Path(temp) / "claim-position.json"),
                logging.getLogger("n20-claim-plan"),
            )
            plan = plan_trader.build_n20_relative_strength_recovery_margin_capped_trade_plan(
                signal.candidate.symbol, analysis.winner.entry.close,
                analysis.winner.p, Decimal("5"),
                structure_id=analysis.structure_id,
                entry_min_price=analysis.winner.entry_min,
                entry_max_price=analysis.winner.entry_max,
            )
            plan = replace(plan, structure_context={
                "strategy_id": "N20", "rule_version": "N20_V1",
                "structure_id": analysis.structure_id,
            })
            bound = TradingBot._n20_plan_with_published_identity(signal, plan)
            self.assertEqual(bound.structure_context["signal_id"], signal.signal_id)
            recorder.assert_n20_execution_claim(
                signal.signal_id, signal.candidate.symbol, analysis.structure_id
            )
            with self.assertRaises(_N20PlanIntegrityError):
                TradingBot._n20_plan_with_published_identity(
                    signal, replace(plan, symbol="OTHERUSDT")
                )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_passed_signal_audits "
                    "WHERE strategy_id='N20' AND claim_state='ACTIVE'"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                    "WHERE strategy_id='N20' AND claim_state='ACTIVE'"
                ).fetchone()[0], 1)

            # The N20 claim contract admits exactly CONFIRMED/PASSED and the
            # one reconstructible CONSUMED/N20_EPISODE_CONSUMED derivation.
            # Exercise a separate database pair so the real lifecycle below
            # can continue after this self-consistent wrong-pair attack.
            tamper_database = Path(temp) / "tamper-review.sqlite3"
            tamper_ledger = Path(temp) / "tamper-ledger.sqlite3"
            with closing(sqlite3.connect(database)) as source, closing(
                sqlite3.connect(tamper_database)
            ) as target:
                source.backup(target)
            with closing(
                sqlite3.connect(recorder.n16_claim_ledger_file)
            ) as source, closing(sqlite3.connect(tamper_ledger)) as target:
                source.backup(target)
            seal_test_database_catalog(tamper_database, tamper_ledger)
            tampered = ReviewRecorder(
                tamper_database, logging.getLogger("n20-stage-reason"), tamper_ledger
            )
            with tampered._read_only_runtime_snapshot() as connection:
                stored = connection.execute(
                    "SELECT evidence_blob,evidence_size,evidence_sha256 "
                    "FROM n20_market_episodes WHERE strategy_id='N20' "
                    "AND structure_id=?",
                    (analysis.structure_id,),
                ).fetchone()
            decoded = decode_n20_state_evidence(*stored)
            illegal = deepcopy(decoded.evidence)
            illegal["stage"] = "CONSUMED"
            illegal["reason"] = "N20_WRONG_TERMINAL_REASON"
            illegal["terminal_cutoff_time_ms"] = illegal["winner"]["entry"][
                "open_time_ms"
            ]
            unsigned = dict(illegal)
            unsigned.pop("canonical_sha256", None)
            illegal["canonical_sha256"] = n20_module.canonical_sha256(unsigned)
            illegal_record = N20StateRecord(
                "N20", decoded.episode_id, "CONSUMED",
                "N20_WRONG_TERMINAL_REASON", decoded.m0_open_time_ms,
                decoded.d1_open_time_ms, decoded.c_open_time_ms, symbols[0],
                analysis.structure_id, illegal["terminal_cutoff_time_ms"], illegal,
            )
            blob, size, digest = illegal_record.encoded
            with tampered._connect() as connection:
                connection.execute(
                    "UPDATE n20_market_episodes SET stage='CONSUMED',reason=?,"
                    "terminal_cutoff_time_ms=?,evidence_blob=?,evidence_size=?,"
                    "evidence_sha256=? WHERE strategy_id='N20' AND structure_id=?",
                    (
                        "N20_WRONG_TERMINAL_REASON",
                        illegal["terminal_cutoff_time_ms"], sqlite3.Binary(blob),
                        size, digest, analysis.structure_id,
                    ),
                )
            with self.assertRaisesRegex(RuntimeError, "execution evidence is invalid"):
                tampered.assert_n20_execution_claim(
                    signal.signal_id, symbols[0], analysis.structure_id
                )

            scan3 = recorder.begin_scan(100, candidates, dry_run=True)
            third = scheduler.evaluate(
                scan3, {"quote_volume_top": candidates, "negative_funding": []},
                recovered, BASE_TIME + 123 * INTERVAL_MS + 1_000,
            )
            self.assertTrue(third.signal_batch_published)
            self.assertEqual(third.passed_signals, [])
            self.assertIsNone(recorder.get_active_n20_episode())
            self.assertEqual(recorder.get_latest_n20_episode().stage, "CONSUMED")
            recorder.assert_n20_execution_claim(
                signal.signal_id, signal.candidate.symbol, analysis.structure_id
            )
            terminal_before = recorder.get_latest_n20_episode()
            self.assertEqual(terminal_before.stage, "CONSUMED")
            for replay_index in range(10):
                replay_scan = recorder.begin_scan(
                    100, candidates, dry_run=True
                )
                replay = scheduler.evaluate(
                    replay_scan,
                    {
                        "quote_volume_top": candidates,
                        "negative_funding": [],
                    },
                    recovered,
                    BASE_TIME + 123 * INTERVAL_MS + 1_000,
                )
                self.assertTrue(
                    replay.signal_batch_published,
                    (replay_index, replay.signal_audit_failures),
                )
                self.assertEqual(replay.passed_signals, [])
                terminal_after = recorder.get_latest_n20_episode()
                self.assertEqual(terminal_after.stage, "CONSUMED")
                self.assertEqual(
                    terminal_after.evidence_sha256,
                    terminal_before.evidence_sha256,
                )
                recorder = ReviewRecorder(
                    database,
                    logging.getLogger("n20-consumed-restart"),
                    recorder.n16_claim_ledger_file,
                )
                scheduler = StrategyScheduler(
                    (N20_STRATEGY,), 122, recorder,
                    logging.getLogger("n20-consumed-restart"),
                )
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertIsNone(connection.execute(
                    "SELECT 1 FROM strategy_signals WHERE id=?",
                    (signal.signal_id,),
                ).fetchone())

    def test_main_fetch_union_is_exactly_deduplicated_and_read_failure_blocks(self):
        current = tuple("C%03dUSDT" % index for index in range(100))
        frozen = tuple("F%03dUSDT" % index for index in range(100))
        candidates = [
            FundingCandidate(
                symbol, None, Decimal("100"), quote_volume=Decimal("1"),
                quote_volume_rank=rank, candidate_universe="quote_volume_top",
            )
            for rank, symbol in enumerate(current, start=1)
        ]

        class Monitor:
            def scan_for_strategies(self, _volume_top_n):
                return StrategyMarketScan(100, [], candidates)

        class Client:
            def __init__(self):
                self.calls = []

            def get_klines(self, symbol):
                self.calls.append(symbol)
                return market_fixture(through=121)[1]["L000USDT"]

        class Recorder:
            def __init__(self, fail=False):
                self.fail = fail
                self.begin_calls = 0

            def get_required_n20_frozen_symbols(self):
                if self.fail:
                    raise RuntimeError("frozen read failed")
                return frozen

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

            def get_required_n18_family_symbols(self):
                return ()

            def begin_scan(self, *_args, **_kwargs):
                self.begin_calls += 1
                return 1

            def complete_scan(self, *_args, **_kwargs):
                return None

        class Scheduler:
            def evaluate(
                self, _scan, _groups, raw_by_symbol, checked_at_ms=None,
            ):
                self.raw_symbols = set(raw_by_symbol)
                return SimpleNamespace(
                    signal_batch_published=True, passed_signals=[],
                    live_candidates=[],
                )

        for fail in (False, True):
            with self.subTest(fail=fail):
                bot = TradingBot.__new__(TradingBot)
                bot.config = SimpleNamespace(dry_run=True)
                bot.logger = logging.getLogger("n20-union")
                bot.monitor = Monitor()
                bot.client = Client()
                bot.recorder = Recorder(fail)
                bot.strategy_scheduler = Scheduler()
                bot.strategies = (N20_STRATEGY,)
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
                    self.assertEqual(len(bot.client.calls), 200)
                    self.assertEqual(len(set(bot.client.calls)), 200)
                    self.assertEqual(
                        bot.strategy_scheduler.raw_symbols,
                        set(current) | set(frozen),
                    )
    def test_signal_state_and_publish_failures_expose_no_execution_candidate(self):
        symbols, initial = market_fixture(through=121)
        frozen = analyze(symbols, initial, checked_index=121).state_record
        _, recovered = market_fixture(through=123)
        candidates = [
            FundingCandidate(
                symbol, None, Decimal(recovered[symbol][-1][4]),
                quote_volume=Decimal("1000000"), quote_volume_rank=rank,
                candidate_universe="quote_volume_top",
            )
            for rank, symbol in enumerate(symbols, start=1)
        ]
        for mode in ("state", "signal", "publish"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                recorder = make_test_recorder(Path(temp) / "review.sqlite3", logging.getLogger("n20-gate"))
                recorder.upsert_strategy_definitions(load_all_strategies())
                self.assertEqual(recorder.record_n20_episode(frozen), "INSERTED")
                scan = recorder.begin_scan(100, candidates, dry_run=True)
                scheduler = StrategyScheduler((N20_STRATEGY,), 122, recorder, logging.getLogger("n20-gate"))
                target = {
                    "state": patch.object(recorder, "record_n20_episode", return_value="N20_STATE_PERSIST_FAILED"),
                    "signal": patch.object(
                        recorder,
                        "record_strategy_signals",
                        return_value=StrategySignalBatchWriteResult(
                            failed_index=0
                        ),
                    ),
                    "publish": patch.object(recorder, "publish_strategy_signal_batch", return_value=False),
                }[mode]
                with target:
                    result = scheduler.evaluate(
                        scan, {"quote_volume_top": candidates, "negative_funding": []},
                        recovered, BASE_TIME + 123 * INTERVAL_MS + 1_000,
                    )
                self.assertFalse(result.signal_batch_published)
                self.assertEqual(result.passed_signals, [])
                self.assertEqual(result.live_candidates, [])


class N20ExecutionTests(unittest.TestCase):
    def test_plan_tick_minimum_maximum_actual_fill_and_five_r_are_exact(self):
        with tempfile.TemporaryDirectory() as temp:
            trader = Trader(
                RuleConstrainedClient(leverage=10, tick_size="0.01", step_size="0.1"),
                live_test_config(str(Path(temp) / "account.json")),
                StateStore(Path(temp) / "position.json"), logging.getLogger("n20-plan"),
            )
            minimum = trader.build_n20_relative_strength_recovery_margin_capped_trade_plan(
                "N20USDT", Decimal("100"), Decimal("99.50"), Decimal("5"),
                structure_id="a" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            self.assertEqual(minimum.stop_loss_price, Decimal("99.00"))
            self.assertEqual(minimum.stop_loss_pct, Decimal("0.01"))
            self.assertEqual(minimum.take_profit_price, Decimal("105.00"))
            self.assertEqual(minimum.quantity % Decimal("0.1"), 0)
            structural = trader.build_n20_relative_strength_recovery_margin_capped_trade_plan(
                "N20USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                structure_id="b" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            actual = trader._execution_plan_from_actual_entry(structural, Decimal("100.50"))
            self.assertGreaterEqual(
                (actual.take_profit_price - actual.actual_entry_price)
                / (actual.actual_entry_price - actual.stop_loss_price), Decimal("5"),
            )
            with self.assertRaisesRegex(BinanceAPIError, "STOP_PCT_OUT_OF_RANGE"):
                trader.build_n20_relative_strength_recovery_margin_capped_trade_plan(
                    "N20USDT", Decimal("100"), Decimal("94"), Decimal("5"),
                    structure_id="c" * 24, entry_min_price=Decimal("100"),
                    entry_max_price=Decimal("100.50"),
                )
            for price in (Decimal("99.99"), Decimal("100.51")):
                with self.subTest(tolerated_fill=price):
                    tolerated = trader._execution_plan_from_actual_entry(
                        structural, price
                    )
                    self.assertGreaterEqual(
                        tolerated.risk_reward_ratio, Decimal("5")
                    )
            allowed_min = structural.entry_min_price * Decimal("0.995")
            allowed_max = structural.entry_max_price * Decimal("1.005")
            for price in (allowed_min, allowed_max):
                with self.subTest(boundary_fill=price):
                    boundary = trader._execution_plan_from_actual_entry(
                        structural, price
                    )
                    self.assertGreaterEqual(
                        boundary.risk_reward_ratio, Decimal("5")
                    )
            for price in (
                allowed_min - Decimal("0.000000000000001"),
                allowed_max + Decimal("0.000000000000001"),
            ):
                with self.subTest(rejected_fill=price):
                    with self.assertRaisesRegex(
                        BinanceAPIError, "ACTUAL_FILL_OUTSIDE"
                    ):
                        trader._execution_plan_from_actual_entry(
                            structural, price
                        )
            for client, message in (
                (RuleConstrainedClient(leverage=1, min_qty="10"), "below minQty"),
                (RuleConstrainedClient(leverage=1, min_notional="1000"), "below minNotional"),
            ):
                with self.subTest(exchange_minimum=message), self.assertRaisesRegex(
                    BinanceAPIError, message
                ):
                    Trader(
                        client,
                        live_test_config(str(Path(temp) / (message + "-account.json"))),
                        StateStore(Path(temp) / (message + "-position.json")),
                        logging.getLogger("n20-minimum"),
                    ).build_n20_relative_strength_recovery_margin_capped_trade_plan(
                        "N20USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                        structure_id="f" * 24, entry_min_price=Decimal("100"),
                        entry_max_price=Decimal("100.50"),
                    )

    def test_journal_and_second_deadline_check_precede_every_exchange_call(self):
        class SaveFailure(StateStore):
            def save(self, _state):
                raise OSError("reservation failed")

        for mode in ("save", "expired"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                client = FakeLiveExecutionClient({}, leverage=10)
                state = SaveFailure(Path(temp) / "position.json") if mode == "save" else StateStore(Path(temp) / "position.json")
                times = iter((1_720_000_119_999,) if mode == "save" else (1_720_000_119_999, 1_720_000_120_000))
                trader = Trader(
                    client, live_test_config(str(Path(temp) / "account.json")), state,
                    logging.getLogger("n20-journal"), clock_ms=lambda: next(times),
                )
                plan = trader.build_n20_relative_strength_recovery_margin_capped_trade_plan(
                    "N20USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                    structure_id="d" * 24, entry_min_price=Decimal("99"),
                    entry_max_price=Decimal("101"),
                )
                plan = replace(
                    plan, entry_candle_open_time_ms=1_720_000_000_000,
                    entry_deadline_ms=1_720_000_120_000,
                    structure_context={
                        "strategy_id": "N20", "rule_version": "N20_V1",
                        "signal_id": 91, "structure_id": "d" * 24,
                    },
                )
                with self.assertRaises(BinanceAPIError if mode == "save" else EntryWindowExpiredError):
                    trader.open_long_plan_with_protection(plan)
                self.assertEqual(client.leverage_calls, [])
                self.assertEqual(client.market_calls, [])

    def test_actual_fill_outside_interval_emergency_closes_without_protection(self):
        with tempfile.TemporaryDirectory() as temp:
            client = FakeLiveExecutionClient({}, leverage=10)
            state = StateStore(Path(temp) / "position.json")
            trader = Trader(
                client, live_test_config(str(Path(temp) / "account.json")), state,
                logging.getLogger("n20-fill"), clock_ms=lambda: 1_720_000_001_000,
            )
            plan = trader.build_n20_relative_strength_recovery_margin_capped_trade_plan(
                "N20USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                structure_id="e" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            plan = replace(
                plan, entry_candle_open_time_ms=1_720_000_000_000,
                entry_deadline_ms=1_720_000_120_000,
                structure_context={
                    "strategy_id": "N20", "rule_version": "N20_V1",
                    "signal_id": 92, "structure_id": "e" * 24,
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
            with self.assertRaisesRegex(BinanceAPIError, "N20 post-fill"):
                trader.open_long_plan_with_protection(plan)
            self.assertEqual(client.close_calls, [("N20USDT", plan.quantity)])
            self.assertEqual(client.protection_calls, [])

    def test_leverage_failure_retains_exact_recoverable_journal(self):
        class LeverageFailure(FakeLiveExecutionClient):
            def set_leverage(self, symbol, leverage):
                self.leverage_calls.append((symbol, leverage))
                raise RuntimeError("leverage unavailable")

        with tempfile.TemporaryDirectory() as temp:
            client = LeverageFailure({}, leverage=10)
            state = StateStore(Path(temp) / "position.json")
            trader = Trader(
                client, live_test_config(str(Path(temp) / "account.json")), state,
                logging.getLogger("n20-leverage"),
                clock_ms=lambda: 1_720_000_001_000,
            )
            plan = trader.build_n20_relative_strength_recovery_margin_capped_trade_plan(
                "N20USDT", Decimal("100"), Decimal("98"), Decimal("5"),
                structure_id="a" * 24, entry_min_price=Decimal("100"),
                entry_max_price=Decimal("100.50"),
            )
            plan = replace(
                plan, entry_candle_open_time_ms=1_720_000_000_000,
                entry_deadline_ms=1_720_000_120_000,
                structure_context={
                    "strategy_id": "N20", "rule_version": "N20_V1",
                    "signal_id": 93, "structure_id": "a" * 24,
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
                pending.orders["execution_pending"]["strategy_id"], "N20"
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
