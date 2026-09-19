from __future__ import annotations

from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from decimal import Decimal
import hashlib
import json
import logging
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from tests.recorder_test_utils import make_test_recorder
from trading_bot.binance_client import BinanceAPIError
from trading_bot.main import TradingBot
from trading_bot.monitor import FundingCandidate, StrategyMarketScan
from trading_bot import n15_snapshot as n15_snapshot_module
from trading_bot.n15_analyzer import (
    analyze_n15_breadth_recovery_leader,
    historical_n15_missed_detail,
    n15_structure_id,
    validate_n15_state_envelope,
)
from trading_bot.n15_snapshot import (
    build_n15_snapshot,
    decode_n15_snapshot,
    exact_median,
    parse_n15_klines,
    wilder_atr,
)
from trading_bot.n15_terminal_schema import (
    N15_TERMINAL_TRIGGER_SQL,
    decode_n15_terminal_payload,
    n15_terminal_schema_status,
    validate_n15_terminal_graph,
)
from trading_bot.paper_trader import PaperTrader
from trading_bot.recorder import (
    ReviewRecorder,
    StrategySignalBatchWriteResult,
)
from trading_bot.state import PositionState, StateStore
from trading_bot.strategies import N15_STRATEGY, load_all_strategies
from trading_bot.strategy_scheduler import StrategyScheduler
from trading_bot.trader import (
    DryRunCloseResult,
    EntryWindowExpiredError,
    TradePlan,
    Trader,
)
from tests.test_trader import (
    FakeLiveExecutionClient,
    RuleConstrainedClient,
    live_test_config,
    test_config,
)


BASE = 1_730_000_700_000
INTERVAL = 900_000


def kline(index, open_price="100", high="100.5", low="99.5", close="100", quote="100", taker="50"):
    start = BASE + index * INTERVAL
    return [
        start, str(open_price), str(high), str(low), str(close), "1",
        start + INTERVAL - 1, str(quote), "1", "1", str(taker), "0",
    ]


def n15_klines(rank, *, elapsed=30_000):
    rows = [kline(index) for index in range(119)]
    if rank <= 40:
        rows.append(kline(119, "100", "100.3", "99.8", "100.2"))
    else:
        rows.append(kline(119, "100", "100.2", "99.7", "99.8"))
    if rank <= 60:
        rows.append(kline(120, "100", "101", "99.8", "100.8", "80", "41.6"))
    else:
        rows.append(kline(120, "100", "100.2", "99.7", "99.8", "80", "40"))
    rows.append(kline(121, "100.8", "101", "99.8", "100.8"))
    return rows, rows[-1][0] + elapsed


def candidate(symbol, rank):
    return FundingCandidate(
        symbol=symbol,
        funding_rate=None,
        mark_price=Decimal("999"),
        quote_volume=Decimal("1000000") - Decimal(rank),
        quote_volume_rank=rank,
        candidate_universe="quote_volume_top",
    )


def full_batch():
    items = [candidate(f"S{rank:03d}USDT", rank) for rank in range(1, 101)]
    raws = {}
    checked = None
    for item in items:
        raws[item.symbol], checked = n15_klines(item.quote_volume_rank)
    return items, raws, checked


def n15_seed_drift_windows():
    items, first_raws, checked = full_batch()
    for raw in first_raws.values():
        raw[0] = kline(
            0,
            open_price="100",
            high="200",
            low="1",
            close="100",
            quote="100",
            taker="50",
        )
    first_raws["S001USDT"][-1][1] = "100.7"
    first_raws["S001USDT"][-1][4] = "100.7"
    second_raws = {
        symbol: deepcopy(raw[1:]) + [
            kline(
                122,
                open_price=raw[-1][4],
                high="101",
                low="99.8",
                close=raw[-1][4],
            )
        ]
        for symbol, raw in first_raws.items()
    }
    return items, first_raws, second_raws, checked


def n15_minimum_seed_windows():
    items, first_raws, checked = full_batch()
    for raw in first_raws.values():
        raw[0] = kline(
            0, open_price="100", high="100", low="100",
            close="100", quote="100", taker="50",
        )
        raw[1] = kline(
            1, open_price="100", high="100.12345678", low="100",
            close="100", quote="100", taker="50",
        )
    second_raws = {
        symbol: deepcopy(raw[1:]) + [
            kline(
                122,
                open_price=raw[-1][4],
                high="101",
                low="99.8",
                close=raw[-1][4],
            )
        ]
        for symbol, raw in first_raws.items()
    }
    return items, first_raws, second_raws, checked


def scheduler(tmpdir, strategy=N15_STRATEGY):
    recorder = make_test_recorder(str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("n15"))
    recorder.upsert_strategy_definitions((strategy,))
    return recorder, StrategyScheduler((strategy,), 96, recorder, logging.getLogger("n15"))


def resign(payload):
    unsigned = {key: value for key, value in payload.items() if key != "canonical_sha256"}
    encoded = json.dumps(unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload["canonical_sha256"] = hashlib.sha256(encoded.encode()).hexdigest()


def legacy_n15_payload(payload):
    legacy = deepcopy(payload)
    legacy.pop("metric_source")
    legacy["schema_version"] = 1
    resign(legacy)
    return legacy


def independently_serialized_v2_payload(items, raws):
    unsigned = n15_snapshot_module._unsigned_snapshot(
        N15_STRATEGY, items, raws
    )
    unsigned["schema_version"] = 2
    metric_rows = []
    for item in sorted(
        items, key=lambda value: (value.quote_volume_rank, value.symbol)
    ):
        candles = parse_n15_klines(raws[item.symbol])[:-2]
        metric_rows.append({
            "symbol": item.symbol,
            "start_open_time_ms": candles[0].open_time_ms,
            "candles": [[
                str(candle.open),
                str(candle.high),
                str(candle.low),
                str(candle.close),
                str(candle.quote_volume),
            ] for candle in candles],
        })
    unsigned["metric_source"] = {"rows": metric_rows}
    resign(unsigned)
    return unsigned


class N15SnapshotTests(unittest.TestCase):
    def test_fixed_b_c_e_atr_cutoffs_v20_and_exact_market_math(self):
        items, raws, _ = full_batch()
        payload, snapshot = build_n15_snapshot(N15_STRATEGY, items, raws)
        row = snapshot.rows["S001USDT"]
        candles = parse_n15_klines(raws["S001USDT"])
        atrs = wilder_atr(candles)
        self.assertEqual(row.atr_b_pre, atrs[118])
        self.assertEqual(row.atr_c_pre, atrs[119])
        self.assertEqual(row.v20_c, Decimal("100"))
        self.assertEqual((row.b.open_time_ms, row.c.open_time_ms, snapshot.e_open_time_ms),
                         (raws["S001USDT"][119][0], raws["S001USDT"][120][0], raws["S001USDT"][121][0]))
        self.assertEqual((snapshot.up_b, snapshot.down_b), (Decimal("0.4"), Decimal("0.6")))
        self.assertEqual((snapshot.up_c, snapshot.down_c), (Decimal("0.6"), Decimal("0.4")))
        self.assertEqual(snapshot.median_move_b, Decimal("-0.2"))
        self.assertEqual(snapshot.up_c - snapshot.up_b, Decimal("0.2"))
        self.assertEqual(snapshot.winner_symbol, "S001USDT")

    def test_flat_moves_do_not_count_and_even_median_uses_positions_50_51(self):
        self.assertEqual(exact_median([Decimal(index) for index in range(100)]), Decimal("49.5"))
        items, raws, _ = full_batch()
        for rank in (40, 41):
            raws[f"S{rank:03d}USDT"][119] = kline(119)
        _, snapshot = build_n15_snapshot(N15_STRATEGY, items, raws)
        self.assertEqual(snapshot.up_b, Decimal("0.39"))
        self.assertEqual(snapshot.down_b, Decimal("0.59"))

    def test_boundary_configuration_and_systemic_veto(self):
        items, raws, _ = full_batch()
        exact = replace(
            N15_STRATEGY,
            n15_weak_down_breadth_min=Decimal("0.60"),
            n15_weak_median_move_max=Decimal("-0.20"),
            n15_recovery_up_breadth_min=Decimal("0.60"),
            n15_recovery_improvement_min=Decimal("0.20"),
            n15_b_move_min=Decimal("0.20"),
            n15_c_close_location_min=Decimal("0.8333333333333333333333333333"),
            n15_c_taker_buy_ratio_min=Decimal("0.52"),
            n15_c_volume_median_multiple_min=Decimal("0.80"),
        )
        _, snapshot = build_n15_snapshot(exact, items, raws)
        self.assertEqual(snapshot.winner_symbol, "S001USDT")
        veto = replace(
            exact,
            n15_crash_down_breadth_min=Decimal("0.60"),
            n15_crash_median_move_max=Decimal("-0.20"),
        )
        _, blocked = build_n15_snapshot(veto, items, raws)
        self.assertTrue(blocked.systemic_crash_veto)
        self.assertIsNone(blocked.winner_symbol)

    def test_ranks_are_complete_and_ties_use_quote_rank(self):
        items, raws, _ = full_batch()
        _, snapshot = build_n15_snapshot(N15_STRATEGY, items, raws)
        self.assertEqual({row.rank_b for row in snapshot.rows.values()}, set(range(1, 101)))
        self.assertEqual({row.rank_c for row in snapshot.rows.values()}, set(range(1, 101)))
        self.assertEqual(snapshot.rows["S001USDT"].rank_b, 1)
        self.assertEqual(snapshot.rows["S040USDT"].rank_b, 40)

    def test_snapshot_strict_shape_hash_and_recomputed_derived_values(self):
        items, raws, _ = full_batch()
        payload, snapshot = build_n15_snapshot(N15_STRATEGY, items, raws)
        self.assertEqual(
            decode_n15_snapshot(payload, N15_STRATEGY, raws, snapshot.e_open_time_ms),
            snapshot,
        )
        mutations = {
            "one": lambda value: value.__setitem__("rows", value["rows"][:1]),
            "ninety_nine": lambda value: value["rows"].pop(),
            "one_hundred_one": lambda value: value["rows"].append(deepcopy(value["rows"][-1])),
            "duplicate_symbol": lambda value: value["rows"][1].__setitem__("symbol", value["rows"][0]["symbol"]),
            "bool_rank": lambda value: value["rows"][0].__setitem__("quote_volume_rank", True),
            "fake_breadth": lambda value: value.__setitem__("up_b", "1"),
            "fake_rank": lambda value: value["rows"][0].__setitem__("rank_c", 99),
            "fake_winner": lambda value: value.__setitem__("winner_symbol", "S002USDT"),
            "zero_atr": lambda value: value["rows"][0].__setitem__("atr_c_pre", "0"),
            "wrong_config": lambda value: value.__setitem__("config_signature", {}),
            "fake_reason": lambda value: value["rows"][0].__setitem__("reason", "FORGED"),
            "fake_move": lambda value: value["rows"][0].__setitem__("move_c", "999"),
            "metric_source_raw": lambda value: value["metric_source"]["rows"][0]["candles"][0].__setitem__(1, "201"),
            "metric_source_extra": lambda value: value["metric_source"].__setitem__("extra", []),
            "metric_source_time": lambda value: value["metric_source"]["rows"][0].__setitem__("start_open_time_ms", value["metric_source"]["rows"][0]["start_open_time_ms"] + 1),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                broken = deepcopy(payload)
                mutate(broken)
                resign(broken)
                with self.assertRaises(ValueError):
                    decode_n15_snapshot(broken, N15_STRATEGY, raws, snapshot.e_open_time_ms)
        self.assertEqual(payload["schema_version"], 2)
        self.assertEqual(len(payload["metric_source"]["rows"]), 100)
        self.assertTrue(all(
            len(row["candles"]) == 120
            for row in payload["metric_source"]["rows"]
        ))
        self.assertLess(
            len(json.dumps(payload, separators=(",", ":")).encode("utf-8")),
            600_000,
        )
        analysis = analyze_n15_breadth_recovery_leader(
            snapshot.winner_symbol,
            raws[snapshot.winner_symbol],
            snapshot,
            snapshot_context_complete=True,
            checked_at_ms=snapshot.e_open_time_ms + 30_000,
        )
        self.assertNotIn(
            "metric_source",
            json.dumps(analysis.detail_json(), separators=(",", ":")),
        )
        changed_raws = deepcopy(raws)
        changed_raws["S001USDT"][10][2] = "101"
        with self.assertRaises(ValueError):
            decode_n15_snapshot(
                payload,
                N15_STRATEGY,
                changed_raws,
                snapshot.e_open_time_ms,
            )

    def test_v2_rejects_self_consistent_resigned_non_122_windows(self):
        items, raws, _ = full_batch()
        expected_e = raws[items[0].symbol][-1][0]
        for source_count in (21, 119, 121):
            with self.subTest(source_count=source_count):
                if source_count <= 120:
                    forged_raws = {
                        symbol: deepcopy(raw[-(source_count + 2):])
                        for symbol, raw in raws.items()
                    }
                else:
                    forged_raws = {
                        symbol: [kline(-1)] + deepcopy(raw)
                        for symbol, raw in raws.items()
                    }
                forged = independently_serialized_v2_payload(
                    items, forged_raws
                )
                with self.assertRaisesRegex(
                    ValueError, "N15 snapshot envelope inconsistent"
                ):
                    n15_snapshot_module.validate_n15_snapshot_envelope(
                        forged,
                        "N15",
                        expected_e,
                        strategy=N15_STRATEGY,
                    )
                with tempfile.TemporaryDirectory() as tmpdir:
                    recorder, _ = scheduler(tmpdir)
                    self.assertEqual(
                        recorder.record_n15_market_snapshot(
                            "N15", str(expected_e), forged
                        ),
                        "N15_STATE_INCONSISTENT",
                    )
                    with recorder._connect() as connection:
                        self.assertEqual(connection.execute(
                            "SELECT COUNT(*) FROM n15_market_snapshots"
                        ).fetchone()[0], 0)
                with self.assertRaisesRegex(
                    ValueError, "N15 metric source window invalid"
                ):
                    build_n15_snapshot(
                        N15_STRATEGY, items, forged_raws
                    )

    def test_recorder_rejects_resigned_snapshot_semantics_and_noncanonical_time(self):
        items, raws, _ = full_batch()
        payload, snapshot = build_n15_snapshot(N15_STRATEGY, items, raws)
        mutations = {
            "config": lambda value: value.__setitem__("config_signature", {}),
            "reason": lambda value: value["rows"][0].__setitem__("reason", "FORGED"),
            "move": lambda value: value["rows"][0].__setitem__("move_c", "999"),
            "negative_v20": lambda value: value["rows"][0].__setitem__("v20_c", "-1"),
            "integer_v20": lambda value: value["rows"][0].__setitem__("v20_c", 100),
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = scheduler(tmpdir)
            for name, mutate in mutations.items():
                with self.subTest(name=name):
                    broken = deepcopy(payload)
                    mutate(broken)
                    resign(broken)
                    self.assertEqual(
                        recorder.record_n15_market_snapshot(
                            "N15", str(snapshot.e_open_time_ms), broken
                        ),
                        "N15_STATE_INCONSISTENT",
                    )
            self.assertEqual(
                recorder.record_n15_market_snapshot(
                    "N15", f"0{snapshot.e_open_time_ms}", payload
                ),
                "N15_STATE_INCONSISTENT",
            )

    def test_top100_gaps_bad_fields_and_time_misalignment_fail(self):
        items, raws, _ = full_batch()
        cases = []
        cases.append((items[:-1], raws))
        duplicate = deepcopy(items)
        duplicate[-1] = replace(duplicate[-1], quote_volume_rank=99)
        cases.append((duplicate, raws))
        boolean_rank = deepcopy(items)
        boolean_rank[-1] = replace(boolean_rank[-1], quote_volume_rank=True)
        cases.append((boolean_rank, raws))
        bad_time = deepcopy(raws)
        bad_time[items[-1].symbol][50][0] += 1
        cases.append((items, bad_time))
        bad_volume = deepcopy(raws)
        bad_volume[items[-1].symbol][120][7] = "-1"
        cases.append((items, bad_volume))
        for members, data in cases:
            with self.subTest(size=len(members)):
                with self.assertRaises((KeyError, ValueError)):
                    build_n15_snapshot(N15_STRATEGY, members, data)

    def test_non_candidate_flat_zero_volume_is_ineligible_not_batch_fatal(self):
        items, raws, _ = full_batch()
        flat = kline(120, "100", "100", "100", "100", "0", "0")
        raws["S100USDT"][120] = flat
        _, snapshot = build_n15_snapshot(N15_STRATEGY, items, raws)
        self.assertEqual(snapshot.winner_symbol, "S001USDT")
        self.assertEqual(
            snapshot.rows["S100USDT"].reason,
            "N15_C_TARGET_METRICS_INVALID",
        )
        self.assertIsNone(snapshot.rows["S100USDT"].close_location_c)
        self.assertIsNone(snapshot.rows["S100USDT"].taker_ratio_c)

        winner_flat = deepcopy(raws)
        winner_flat["S001USDT"][120] = flat
        winner_flat["S061USDT"][120] = deepcopy(raws["S001USDT"][120])
        _, replaced_winner = build_n15_snapshot(
            N15_STRATEGY, items, winner_flat
        )
        self.assertEqual(replaced_winner.winner_symbol, "S002USDT")

        all_flat = deepcopy(raws)
        for raw in all_flat.values():
            raw[120] = flat
        _, no_winner = build_n15_snapshot(N15_STRATEGY, items, all_flat)
        self.assertIsNone(no_winner.winner_symbol)


class N15AnalyzerSchedulerTests(unittest.TestCase):
    def test_offboard_winner_waits_without_execution_then_expires(self):
        items, raws, checked = full_batch()
        payload, snapshot = build_n15_snapshot(
            N15_STRATEGY, items, raws
        )
        winner = snapshot.winner_symbol
        self.assertEqual(winner, "S001USDT")
        entrant = candidate("NEWUSDT", 1)
        current = [entrant, *items[1:]]
        current_raws = {**raws, entrant.symbol: deepcopy(raws["S100USDT"])}
        for index in (5, 7, 10):
            current_raws[winner][-1][index] = "0"

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            self.assertEqual(
                recorder.record_n15_market_snapshot(
                    "N15", str(snapshot.e_open_time_ms), payload
                ),
                "OK",
            )
            first_scan = recorder.begin_scan(101, current, dry_run=True)
            first = sched.evaluate(
                first_scan,
                {"quote_volume_top": current, "negative_funding": []},
                current_raws,
                checked_at_ms=snapshot.e_open_time_ms + 30_000,
            )
            self.assertTrue(first.signal_batch_published)
            self.assertEqual(len(first.signals), 100)
            self.assertEqual(first.passed_signals, [])
            self.assertEqual(first.live_candidates, [])
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id=? AND symbol=?",
                        (first_scan, winner),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n15_entry_states"
                    ).fetchone(),
                    (0,),
                )

            second_scan = recorder.begin_scan(101, current, dry_run=True)
            second = sched.evaluate(
                second_scan,
                {"quote_volume_top": current, "negative_funding": []},
                current_raws,
                checked_at_ms=snapshot.e_open_time_ms + 120_000,
            )
            self.assertTrue(second.signal_batch_published)
            self.assertEqual(len(second.signals), 100)
            self.assertEqual(second.passed_signals, [])
            self.assertEqual(second.live_candidates, [])
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT symbol,status,reason FROM n15_entry_states"
                    ).fetchall(),
                    [(
                        winner,
                        "MISSED",
                        "N15_ENTRY_WINDOW_EXPIRED",
                    )],
                )

    def test_terminal_envelopes_reject_forged_times_types_and_extra_fields(self):
        items, raws, checked = full_batch()
        payload, snapshot = build_n15_snapshot(N15_STRATEGY, items, raws)
        analysis = analyze_n15_breadth_recovery_leader(
            snapshot.winner_symbol,
            raws[snapshot.winner_symbol],
            snapshot,
            snapshot_context_complete=True,
            checked_at_ms=checked,
        )
        structure = analysis.structure
        current = analysis.detail_json()
        historical = historical_n15_missed_detail(
            "N15", structure.entry.open_time_ms, structure.symbol,
            structure.structure_id, snapshot.rows[structure.symbol],
        )
        current_mutations = {
            "extra_detail": lambda value: value.__setitem__("extra", 1),
            "extra_structure": lambda value: value["structure"].__setitem__("extra", 1),
            "bool_rank": lambda value: value["structure"].__setitem__("quote_volume_rank", True),
            "missing_universe": lambda value: value["structure"].pop("candidate_universe"),
            "forged_universe": lambda value: value["structure"].__setitem__("candidate_universe", "quote_volume_top"),
            "typed_universe": lambda value: value["structure"].__setitem__("candidate_universe", 15),
            "distant_b_c": lambda value: (
                value["structure"]["b"].__setitem__(
                    "open_time_ms", structure.entry.open_time_ms - 20 * INTERVAL
                ),
                value["structure"]["c"].__setitem__(
                    "open_time_ms", structure.entry.open_time_ms - 19 * INTERVAL
                ),
            ),
        }
        historical_mutations = {
            "extra_detail": lambda value: value.__setitem__("extra", 1),
            "extra_structure": lambda value: value["structure"].__setitem__("extra", 1),
            "bool_rank": lambda value: value["structure"].__setitem__("quote_volume_rank", True),
            "missing_universe": lambda value: value["structure"].pop("candidate_universe"),
            "forged_universe": lambda value: value["structure"].__setitem__("candidate_universe", "quote_volume_top"),
            "typed_universe": lambda value: value["structure"].__setitem__("candidate_universe", 15),
            "distant_b_c": lambda value: (
                value["structure"]["b"].__setitem__(
                    "open_time_ms", structure.entry.open_time_ms - 30 * INTERVAL
                ),
                value["structure"]["c"].__setitem__(
                    "open_time_ms", structure.entry.open_time_ms - 29 * INTERVAL
                ),
            ),
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = scheduler(tmpdir)
            for event_type, source, status, reason, mutations in (
                ("current", current, "CONSUMED", "PASSED", current_mutations),
                (
                    "historical", historical, "MISSED",
                    "HISTORICAL_N15_ENTRY_MISSED", historical_mutations,
                ),
            ):
                for name, mutate in mutations.items():
                    with self.subTest(event_type=event_type, mutation=name):
                        forged = deepcopy(source)
                        mutate(forged)
                        if name == "distant_b_c":
                            forged_id = n15_structure_id(
                                "N15", structure.symbol,
                                forged["structure"]["b"]["open_time_ms"],
                                forged["structure"]["c"]["open_time_ms"],
                            )
                            forged["structure_id"] = forged_id
                            forged["structure"]["structure_id"] = forged_id
                        else:
                            forged_id = structure.structure_id
                        self.assertEqual(
                            recorder.record_n15_entry_state(
                                "N15", str(structure.entry.open_time_ms),
                                structure.symbol, forged_id, status, reason,
                                forged,
                            ),
                            "N15_STATE_INCONSISTENT",
                        )
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_entry_states"
                ).fetchone()[0], 0)

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = scheduler(tmpdir)
            valid = analysis.detail_json()
            self.assertEqual(
                recorder.record_n15_market_snapshot(
                    "N15", str(structure.entry.open_time_ms), payload
                ),
                "OK",
            )
            self.assertEqual(
                recorder.record_n15_entry_state(
                    "N15", str(structure.entry.open_time_ms),
                    structure.symbol, structure.structure_id,
                    "CONSUMED", "PASSED", valid,
                ),
                "INSERTED",
            )
            bad = deepcopy(valid)
            bad["structure"].pop("candidate_universe")
            with self.assertRaises(sqlite3.DatabaseError):
                with recorder._connect() as connection:
                    connection.execute(
                        "UPDATE n15_entry_states SET detail_json=? "
                        "WHERE strategy_id='N15' AND e_time=?",
                        (
                            json.dumps(bad),
                            str(structure.entry.open_time_ms),
                        ),
                    )
            self.assertIsNotNone(
                recorder.get_n15_entry_state(
                    "N15", str(structure.entry.open_time_ms)
                )
            )

        forged_priority = deepcopy(current)
        forged_priority["reason"] = "N15_ENTRY_PRICE_TOO_EXTENDED"
        forged_priority["elapsed_ms"] = 30_000
        forged_priority["structure"]["entry"]["close"] = str(
            Decimal(forged_priority["structure"]["entry_max_price"])
            + Decimal("1")
        )
        forged_priority["structure"]["entry"]["high"] = forged_priority["structure"]["entry"]["close"]
        forged_priority["structure"]["entry"]["low"] = str(
            Decimal(forged_priority["structure"]["p"]) - Decimal("0.1")
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = scheduler(tmpdir)
            self.assertEqual(
                recorder.record_n15_entry_state(
                    "N15", str(structure.entry.open_time_ms),
                    structure.symbol, structure.structure_id,
                    "MISSED", "N15_ENTRY_PRICE_TOO_EXTENDED",
                    forged_priority,
                ),
                "N15_STATE_INCONSISTENT",
            )

    def test_entry_boundaries_wait_and_terminal_conditions(self):
        items, raws, checked = full_batch()
        _, snapshot = build_n15_snapshot(N15_STRATEGY, items, raws)
        symbol = snapshot.winner_symbol
        raw = raws[symbol]
        base = analyze_n15_breadth_recovery_leader(
            symbol, raw, snapshot, snapshot_context_complete=True,
            checked_at_ms=raw[-1][0],
        )
        self.assertTrue(base.passed)
        structure = base.structure
        exact_upper = deepcopy(raw)
        exact_upper[-1][2] = exact_upper[-1][4] = str(structure.entry_max_price)
        self.assertTrue(analyze_n15_breadth_recovery_leader(
            symbol, exact_upper, snapshot, snapshot_context_complete=True,
            checked_at_ms=raw[-1][0] + 119_999,
        ).passed)
        waiting = deepcopy(raw)
        waiting[-1][1] = waiting[-1][4] = "100.7"
        self.assertEqual(analyze_n15_breadth_recovery_leader(
            symbol, waiting, snapshot, snapshot_context_complete=True,
            checked_at_ms=checked,
        ).reason, "N15_WAITING_ENTRY_PRICE")
        expired = analyze_n15_breadth_recovery_leader(
            symbol, raw, snapshot, snapshot_context_complete=True,
            checked_at_ms=raw[-1][0] + 120_000,
        )
        self.assertTrue(expired.consume_current)
        self.assertEqual(expired.reason, "N15_ENTRY_WINDOW_EXPIRED")
        broken = deepcopy(raw)
        broken[-1][3] = "99.79"
        broken[-1][1] = broken[-1][4] = "100.8"
        self.assertEqual(analyze_n15_breadth_recovery_leader(
            symbol, broken, snapshot, snapshot_context_complete=True,
            checked_at_ms=checked,
        ).reason, "N15_ENTRY_LOW_BROKE_P")

    def test_same_bar_snapshot_freezes_winner_and_membership_across_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, first_scheduler = scheduler(tmpdir)
            items, raws, checked = full_batch()
            first = first_scheduler._build_n15_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertTrue(first.complete)
            self.assertEqual(first.snapshot.winner_symbol, "S001USDT")
            entrant = candidate("NEWUSDT", 100)
            replaced = [*items[:-1], entrant]
            changed_raws = {**raws, entrant.symbol: deepcopy(raws["S001USDT"])}
            restarted = StrategyScheduler((N15_STRATEGY,), 96, recorder, logging.getLogger("n15_restart"))
            frozen = restarted._build_n15_batch_context(
                {"quote_volume_top": replaced}, changed_raws, checked
            )
            self.assertTrue(frozen.complete)
            self.assertEqual(frozen.snapshot.winner_symbol, "S001USDT")
            self.assertNotIn("NEWUSDT", frozen.snapshot.rows)
            with recorder._connect() as connection:
                snapshot_before_missing = connection.execute(
                    "SELECT * FROM n15_market_snapshots"
                ).fetchall()
            missing_old = deepcopy(changed_raws)
            missing_old.pop(items[-1].symbol)
            with self.assertLogs("n15_restart", level="WARNING") as captured:
                self.assertFalse(restarted._build_n15_batch_context(
                    {"quote_volume_top": replaced}, missing_old, checked
                ).complete)
            self.assertTrue(any(
                "N15 frozen member kline missing" in message
                for message in captured.output
            ))
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT * FROM n15_market_snapshots"
                ).fetchall(), snapshot_before_missing)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type LIKE 'n15_snapshot_quarantine%'"
                ).fetchone()[0], 0)

    def test_incomplete_batch_does_not_freeze_and_complete_retry_does(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            items, raws, checked = full_batch()
            incomplete = sched._build_n15_batch_context(
                {"quote_volume_top": items[:-1]}, raws, checked
            )
            self.assertFalse(incomplete.complete)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_market_snapshots"
                ).fetchone()[0], 0)

            recovered = sched._build_n15_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertTrue(recovered.complete)
            self.assertEqual(recovered.snapshot.winner_symbol, "S001USDT")
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_market_snapshots"
                ).fetchone()[0], 1)

    def test_extra_invalid_candidate_does_not_get_filtered_into_top100(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            items, raws, checked = full_batch()
            invalid = replace(
                candidate("BADUSDT", 100), quote_volume_rank=True
            )
            context = sched._build_n15_batch_context(
                {"quote_volume_top": [*items, invalid]},
                {**raws, "BADUSDT": deepcopy(raws["S100USDT"])},
                checked,
            )
            self.assertFalse(context.complete)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_market_snapshots"
                ).fetchone()[0], 0)

    def test_scheduler_only_winner_passes_and_never_promotes_runner(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            items, raws, checked = full_batch()
            scan_id = recorder.begin_scan(len(items), items, True)
            self.assertIsNotNone(scan_id)
            result = sched.evaluate(scan_id, {"quote_volume_top": items}, raws, checked_at_ms=checked)
            self.assertEqual([item.candidate.symbol for item in result.passed_signals], ["S001USDT"])
            signals = {item.candidate.symbol: item for item in result.signals}
            self.assertEqual(signals["S002USDT"].reason, "N15_NOT_WINNER")
            with recorder._connect() as connection:
                row = connection.execute("SELECT symbol,status,reason FROM n15_entry_states").fetchone()
            self.assertEqual(row, ("S001USDT", "CONSUMED", "PASSED"))
            replay_scan_id = recorder.begin_scan(len(items), items, True)
            self.assertIsNotNone(replay_scan_id)
            replay = sched.evaluate(replay_scan_id, {"quote_volume_top": items}, raws, checked_at_ms=checked)
            winner = next(item for item in replay.signals if item.candidate.symbol == "S001USDT")
            self.assertEqual(winner.reason, "N15_OPPORTUNITY_CONSUMED")
            self.assertEqual(replay.passed_signals, [])

    def test_frozen_rank_and_universe_survive_same_bar_rank_reorder(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            items, raws, checked = full_batch()
            first = sched._build_n15_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertTrue(first.complete)
            reordered = [
                replace(item, quote_volume_rank=(2 if item.quote_volume_rank == 1 else 1))
                if item.quote_volume_rank in {1, 2} else item
                for item in items
            ]
            scan_id = recorder.begin_scan(len(reordered), reordered, True)
            self.assertIsNotNone(scan_id)
            result = sched.evaluate(
                scan_id, {"quote_volume_top": reordered}, raws,
                checked_at_ms=checked,
            )
            signal = result.passed_signals[0]
            self.assertEqual(signal.candidate.quote_volume_rank, 1)
            self.assertEqual(
                signal.candidate.candidate_universe,
                "quote_volume_top_frozen_n15",
            )
            bot = TradingBot.__new__(TradingBot)
            class PlanBuilder:
                def build_breadth_recovery_margin_capped_trade_plan(
                    self, symbol, entry, p, risk_reward, **kwargs
                ):
                    return TradePlan(
                        symbol=symbol, leverage=1, quantity=Decimal("1"),
                        entry_price=entry, stop_loss_price=p,
                        take_profit_price=entry + Decimal("5"),
                        stop_loss_pct=Decimal("0.01"),
                        take_profit_pct=Decimal("0.05"),
                        amplitude_24h_pct=Decimal("0"),
                        high_24h_price=Decimal("0"), low_24h_price=Decimal("0"),
                        risk_amount=Decimal("1"), notional_value=entry,
                        required_margin=entry, balance=Decimal("1000"),
                        stop_mode="breadth_recovery_margin_capped",
                        structure_id=kwargs["structure_id"],
                        entry_min_price=kwargs["entry_min_price"],
                        entry_max_price=kwargs["entry_max_price"],
                    )
            bot.trader = PlanBuilder()
            plan = bot._build_strategy_plan(signal)
            self.assertEqual(plan.structure_context["quote_volume_rank"], 1)
            self.assertEqual(
                plan.structure_context["candidate_universe"],
                "quote_volume_top_frozen_n15",
            )

    def test_concurrent_schedulers_only_one_claim_can_pass(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder_a, sched_a = scheduler(tmpdir)
            recorder_b = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"), logging.getLogger("n15_b")
            )
            sched_b = StrategyScheduler(
                (N15_STRATEGY,), 96, recorder_b, logging.getLogger("n15_b")
            )
            items, raws, checked = full_batch()
            self.assertTrue(sched_a._build_n15_batch_context(
                {"quote_volume_top": items}, raws, checked
            ).complete)
            barrier = threading.Barrier(2)
            for recorder in (recorder_a, recorder_b):
                original = recorder.record_n15_entry_state
                def wrapped(*args, _original=original, **kwargs):
                    barrier.wait(timeout=5)
                    return _original(*args, **kwargs)
                recorder.record_n15_entry_state = wrapped
            def run(item):
                sched, scan_id = item
                return sched.evaluate(
                    scan_id, {"quote_volume_top": items}, raws,
                    checked_at_ms=checked,
                )
            with patch.object(
                recorder_a,
                "record_strategy_signals",
                side_effect=lambda _scan_id, records: (
                    StrategySignalBatchWriteResult(
                        tuple(range(1, len(records) + 1))
                    )
                ),
            ), patch.object(
                recorder_b,
                "record_strategy_signals",
                side_effect=lambda _scan_id, records: (
                    StrategySignalBatchWriteResult(
                        tuple(range(1001, 1001 + len(records)))
                    )
                ),
            ), patch.object(
                recorder_a, "publish_strategy_signal_batch", return_value=True
            ), patch.object(
                recorder_b, "publish_strategy_signal_batch", return_value=True
            ):
                with ThreadPoolExecutor(max_workers=2) as pool:
                    results = list(pool.map(run, ((sched_a, 1), (sched_b, 2))))
            self.assertEqual(
                sum(len(result.passed_signals) for result in results), 1
            )
            reasons = [
                next(item for item in result.signals if item.candidate.symbol == "S001USDT").reason
                for result in results
            ]
            self.assertEqual(sorted(reasons), ["N15_OPPORTUNITY_CONSUMED", "PASSED"])
            with recorder_a._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_entry_states"
                ).fetchone()[0], 1)

    def test_winner_state_read_failure_blocks_the_batch_without_runner_fallback(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            items, raws, checked = full_batch()

            def fail_state_read(*args, **kwargs):
                raise RuntimeError("forced N15 state read failure")

            recorder.get_n15_entry_state = fail_state_read
            result = sched.evaluate(
                None, {"quote_volume_top": items}, raws,
                checked_at_ms=checked,
            )
            winner = next(
                item for item in result.signals
                if item.candidate.symbol == "S001USDT"
            )
            self.assertEqual(winner.reason, "N15_STATE_READ_FAILED")
            self.assertEqual(result.passed_signals, [])
            self.assertTrue(all(
                item.reason == "N15_NOT_WINNER"
                for item in result.signals
                if item.candidate.symbol != "S001USDT"
            ))
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_entry_states"
                ).fetchone()[0], 0)

    def test_winner_null_is_frozen_and_waiting_old_e_backfills_missed(self):
        items, raws, checked = full_batch()
        no_winner_raws = deepcopy(raws)
        for raw in no_winner_raws.values():
            raw[120] = kline(120, "100", "100", "100", "100", "0", "0")
        with tempfile.TemporaryDirectory() as tmpdir:
            _, no_winner_scheduler = scheduler(tmpdir)
            frozen = no_winner_scheduler._build_n15_batch_context(
                {"quote_volume_top": items}, no_winner_raws, checked
            )
            self.assertTrue(frozen.complete)
            self.assertIsNone(frozen.snapshot.winner_symbol)

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            waiting_raws = deepcopy(raws)
            waiting_raws["S001USDT"][-1][1] = "100.7"
            waiting_raws["S001USDT"][-1][4] = "100.7"
            first = sched.evaluate(
                None, {"quote_volume_top": items}, waiting_raws,
                checked_at_ms=checked,
            )
            winner = next(
                item for item in first.signals
                if item.candidate.symbol == "S001USDT"
            )
            self.assertEqual(winner.reason, "N15_WAITING_ENTRY_PRICE")
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_entry_states"
                ).fetchone()[0], 0)
            next_raws = deepcopy(waiting_raws)
            for raw in next_raws.values():
                raw.append(kline(122, raw[-1][4], "101", "99.8", raw[-1][4]))
            next_checked = BASE + 122 * INTERVAL + 30_000
            sched._build_n15_batch_context(
                {"quote_volume_top": items}, next_raws, next_checked
            )
            with recorder._connect() as connection:
                state = connection.execute(
                    "SELECT symbol,status,reason FROM n15_entry_states"
                ).fetchone()
            self.assertEqual(
                state,
                ("S001USDT", "MISSED", "HISTORICAL_N15_ENTRY_MISSED"),
            )

    def test_fixed_122_roll_preserves_frozen_metrics_and_backfills_missed(self):
        items, first_raws, second_raws, checked = n15_seed_drift_windows()
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            first = sched._build_n15_batch_context(
                {"quote_volume_top": items}, first_raws, checked
            )
            self.assertTrue(first.complete)
            self.assertEqual(first.snapshot.winner_symbol, "S001USDT")
            shifted_prefix = {
                symbol: raw[:121] for symbol, raw in second_raws.items()
            }
            shifted_candles = parse_n15_klines(
                shifted_prefix["S001USDT"]
            )
            shifted_atr = wilder_atr(shifted_candles)[-4]
            self.assertEqual(
                (
                    first.snapshot.rows["S001USDT"].b,
                    first.snapshot.rows["S001USDT"].c,
                    first.snapshot.e_open_time_ms,
                ),
                (
                    shifted_candles[-3],
                    shifted_candles[-2],
                    shifted_candles[-1].open_time_ms,
                ),
            )
            self.assertNotEqual(
                first.snapshot.rows["S001USDT"].atr_b_pre,
                shifted_atr,
            )

            second = sched._build_n15_batch_context(
                {"quote_volume_top": items},
                second_raws,
                checked + INTERVAL,
            )
            self.assertTrue(second.complete)
            with recorder._connect() as connection:
                states = connection.execute(
                    "SELECT e_time,symbol,status,reason FROM n15_entry_states"
                ).fetchall()
                snapshots = connection.execute(
                    "SELECT e_time FROM n15_market_snapshots"
                ).fetchall()
                quarantines = connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type='n15_snapshot_quarantined'"
                ).fetchone()[0]
            self.assertEqual(states, [(
                str(first.current_open_time_ms),
                "S001USDT",
                "MISSED",
                "HISTORICAL_N15_ENTRY_MISSED",
            )])
            self.assertEqual(
                snapshots, [(str(second.current_open_time_ms),)]
            )
            self.assertEqual(quarantines, 0)
            with recorder._connect() as connection:
                terminal_before = connection.execute(
                    "SELECT * FROM n15_entry_states"
                ).fetchall()
            replay = sched._build_n15_batch_context(
                {"quote_volume_top": items},
                second_raws,
                checked + INTERVAL,
            )
            self.assertTrue(replay.complete)
            scan_id = recorder.begin_scan(len(items), items, True)
            self.assertIsNotNone(scan_id)
            evaluated = sched.evaluate(
                scan_id,
                {"quote_volume_top": items},
                second_raws,
                checked_at_ms=checked + INTERVAL,
            )
            self.assertTrue(evaluated.signal_batch_published)
            self.assertTrue(all(
                signal.reason != "N15_MARKET_CONTEXT_INSUFFICIENT"
                for signal in evaluated.signals
            ))
            self.assertEqual(evaluated.passed_signals, [])
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM n15_entry_states"
                    ).fetchall(),
                    terminal_before,
                )
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_market_snapshots"
                ).fetchone()[0], 1)
                for table in (
                    "strategy_paper_trades",
                    "strategy_live_links",
                    "trade_reviews",
                ):
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM " + table
                    ).fetchone()[0], 0)

    def test_larger_api_windows_use_exact_latest_122_view_across_boundary(self):
        items, first_raws, second_raws, checked = n15_seed_drift_windows()
        expected_first, first_snapshot = build_n15_snapshot(
            N15_STRATEGY, items, first_raws
        )
        expected_second, second_snapshot = build_n15_snapshot(
            N15_STRATEGY, items, second_raws
        )
        for extra in (1, 28):
            with self.subTest(total_candles=122 + extra), tempfile.TemporaryDirectory() as tmpdir:
                extended_first = {
                    symbol: [
                        kline(index)
                        for index in range(-extra, 0)
                    ] + deepcopy(raw)
                    for symbol, raw in first_raws.items()
                }
                extended_second = {
                    symbol: deepcopy(raw[1:])
                    + [deepcopy(second_raws[symbol][-1])]
                    for symbol, raw in extended_first.items()
                }
                recorder, sched = scheduler(tmpdir)
                first = sched._build_n15_batch_context(
                    {"quote_volume_top": items},
                    extended_first,
                    checked,
                )
                self.assertTrue(first.complete)
                self.assertEqual(
                    first.current_open_time_ms,
                    first_snapshot.e_open_time_ms,
                )
                self.assertEqual(
                    recorder.get_n15_market_snapshot(
                        "N15", str(first_snapshot.e_open_time_ms)
                    ),
                    expected_first,
                )
                second = sched._build_n15_batch_context(
                    {"quote_volume_top": items},
                    extended_second,
                    checked + INTERVAL,
                )
                self.assertTrue(second.complete)
                self.assertEqual(
                    second.current_open_time_ms,
                    second_snapshot.e_open_time_ms,
                )
                self.assertEqual(
                    recorder.get_n15_market_snapshot(
                        "N15", str(second_snapshot.e_open_time_ms)
                    ),
                    expected_second,
                )
                with recorder._connect() as connection:
                    self.assertEqual(connection.execute(
                        "SELECT symbol,status,reason FROM n15_entry_states"
                    ).fetchall(), [(
                        "S001USDT",
                        "MISSED",
                        "HISTORICAL_N15_ENTRY_MISSED",
                    )])
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM events "
                        "WHERE event_type='n15_snapshot_quarantined'"
                    ).fetchone()[0], 0)

    def test_boundary_axis_wait_retains_snapshot_then_first_aligned_scan_recovers(self):
        items, first_raws, second_raws, checked = n15_seed_drift_windows()
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            first = sched._build_n15_batch_context(
                {"quote_volume_top": items}, first_raws, checked
            )
            self.assertTrue(first.complete)
            with recorder._connect() as connection:
                frozen_before = connection.execute(
                    "SELECT * FROM n15_market_snapshots"
                ).fetchall()
            with self.assertLogs("n15", level="WARNING") as captured:
                waiting = sched._build_n15_batch_context(
                    {"quote_volume_top": items},
                    first_raws,
                    checked + INTERVAL,
                )
            self.assertFalse(waiting.complete)
            self.assertTrue(any(
                "N15 current E timing invalid" in message
                for message in captured.output
            ))
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT * FROM n15_market_snapshots"
                ).fetchall(), frozen_before)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_entry_states"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type='n15_snapshot_quarantined'"
                ).fetchone()[0], 0)

    def test_mixed_member_e_axis_publishes_rejections_then_aligned_scan_recovers(self):
        items, raws, checked = full_batch()
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            mixed = deepcopy(raws)
            symbol = items[-1].symbol
            mixed[symbol].append(
                kline(122, "100", "100.1", "99.9", "100", "100", "50")
            )
            scan_id = recorder.begin_scan(len(items), items, True)
            with self.assertLogs("n15", level="WARNING") as captured:
                blocked = sched.evaluate(
                    scan_id,
                    {"quote_volume_top": items},
                    mixed,
                    checked_at_ms=checked,
                )
            self.assertTrue(any(
                "N15 current E axis incomplete" in message
                for message in captured.output
            ))
            self.assertTrue(blocked.signal_batch_published)
            self.assertEqual(blocked.passed_signals, [])
            self.assertEqual(len(blocked.signals), 100)
            self.assertEqual(
                {signal.reason for signal in blocked.signals},
                {"N15_MARKET_CONTEXT_INSUFFICIENT"},
            )

            aligned_scan = recorder.begin_scan(len(items), items, True)
            recovered = sched.evaluate(
                aligned_scan,
                {"quote_volume_top": items},
                raws,
                checked_at_ms=checked,
            )
            self.assertTrue(recovered.signal_batch_published)
            self.assertNotEqual(
                {signal.reason for signal in recovered.signals},
                {"N15_MARKET_CONTEXT_INSUFFICIENT"},
            )

    def test_empty_snapshot_prepare_exception_has_nonempty_diagnostic(self):
        items, raws, checked = full_batch()
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            recorder.get_n15_market_snapshot = (
                lambda *args, **kwargs: (_ for _ in ()).throw(
                    AssertionError()
                )
            )
            with self.assertLogs("n15", level="WARNING") as captured:
                blocked = sched._build_n15_batch_context(
                    {"quote_volume_top": items}, raws, checked
                )
            self.assertFalse(blocked.complete)
            self.assertTrue(any(
                "Unable to prepare N15 snapshot: AssertionError" in message
                for message in captured.output
            ))

    def test_truncated_current_windows_retain_frozen_snapshot_and_terminal_evidence(self):
        items, first_raws, second_raws, checked = n15_seed_drift_windows()
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            first = sched._build_n15_batch_context(
                {"quote_volume_top": items}, first_raws, checked
            )
            self.assertTrue(first.complete)
            with recorder._connect() as connection:
                frozen_before = connection.execute(
                    "SELECT * FROM n15_market_snapshots"
                ).fetchall()
            truncated_same_e = {
                symbol: raw[1:] for symbol, raw in first_raws.items()
            }
            with self.assertLogs("n15", level="WARNING") as captured:
                same_e = sched._build_n15_batch_context(
                    {"quote_volume_top": items},
                    truncated_same_e,
                    checked,
                )
            self.assertFalse(same_e.complete)
            self.assertTrue(any(
                "N15 current kline history incomplete" in message
                for message in captured.output
            ))
            short_new_e = {
                symbol: deepcopy(raw[-3:]) + [deepcopy(second_raws[symbol][-1])]
                for symbol, raw in first_raws.items()
            }
            with self.assertLogs("n15", level="WARNING") as captured:
                next_e = sched._build_n15_batch_context(
                    {"quote_volume_top": items},
                    short_new_e,
                    checked + INTERVAL,
                )
            self.assertFalse(next_e.complete)
            self.assertTrue(any(
                "N15 current kline history incomplete" in message
                for message in captured.output
            ))
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT * FROM n15_market_snapshots"
                ).fetchall(), frozen_before)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_entry_states"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type LIKE 'n15_snapshot_quarantine%'"
                ).fetchone()[0], 0)
            aligned = sched._build_n15_batch_context(
                {"quote_volume_top": items},
                second_raws,
                checked + INTERVAL,
            )
            self.assertTrue(aligned.complete)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT status,reason FROM n15_entry_states"
                ).fetchall(), [(
                    "MISSED", "HISTORICAL_N15_ENTRY_MISSED"
                )])
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_market_snapshots"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type='n15_snapshot_quarantined'"
                ).fetchone()[0], 0)

    def test_current_legacy_snapshot_is_exactly_upgraded_once(self):
        items, raws, checked = full_batch()
        payload, snapshot = build_n15_snapshot(
            N15_STRATEGY, items, raws
        )
        legacy = legacy_n15_payload(payload)
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            self.assertEqual(recorder.record_n15_market_snapshot(
                "N15", str(snapshot.e_open_time_ms), legacy
            ), "OK")
            with recorder._connect() as connection:
                identity_before = connection.execute(
                    "SELECT id,created_at FROM n15_market_snapshots"
                ).fetchone()
            restored = sched._build_n15_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertTrue(restored.complete)
            upgraded = recorder.get_n15_market_snapshot(
                "N15", str(snapshot.e_open_time_ms)
            )
            self.assertEqual(upgraded["schema_version"], 2)
            with recorder._connect() as connection:
                identity_after = connection.execute(
                    "SELECT id,created_at FROM n15_market_snapshots"
                ).fetchone()
                upgraded_json = connection.execute(
                    "SELECT payload_json FROM n15_market_snapshots"
                ).fetchone()[0]
            self.assertEqual(identity_after, identity_before)
            replay = sched._build_n15_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertTrue(replay.complete)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT payload_json FROM n15_market_snapshots"
                ).fetchone()[0], upgraded_json)

    def test_legacy_upgrade_write_failure_retains_snapshot_without_quarantine(self):
        items, raws, checked = full_batch()
        payload, snapshot = build_n15_snapshot(
            N15_STRATEGY, items, raws
        )
        legacy = legacy_n15_payload(payload)
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            self.assertEqual(recorder.record_n15_market_snapshot(
                "N15", str(snapshot.e_open_time_ms), legacy
            ), "OK")
            recorder.upgrade_n15_market_snapshot = (
                lambda *args, **kwargs: "N15_STATE_PERSIST_FAILED"
            )
            blocked = sched._build_n15_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertFalse(blocked.complete)
            with recorder._connect() as connection:
                stored = json.loads(connection.execute(
                    "SELECT payload_json FROM n15_market_snapshots"
                ).fetchone()[0])
                self.assertEqual(stored["schema_version"], 1)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type LIKE 'n15_snapshot_quarantine%'"
                ).fetchone()[0], 0)

    def test_recorder_rejects_semantically_different_legacy_upgrade_pair(self):
        items, raws, _ = full_batch()
        first_payload, snapshot = build_n15_snapshot(
            N15_STRATEGY, items, raws
        )
        legacy = legacy_n15_payload(first_payload)
        alternate_raws = deepcopy(raws)
        for raw in alternate_raws.values():
            raw[0] = kline(
                0, "100", "200", "1", "100", "100", "50"
            )
        alternate_payload, _ = build_n15_snapshot(
            N15_STRATEGY, items, alternate_raws
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = scheduler(tmpdir)
            self.assertEqual(recorder.record_n15_market_snapshot(
                "N15", str(snapshot.e_open_time_ms), legacy
            ), "OK")
            self.assertEqual(recorder.upgrade_n15_market_snapshot(
                "N15",
                str(snapshot.e_open_time_ms),
                legacy,
                alternate_payload,
            ), "N15_STATE_INCONSISTENT")
            with recorder._connect() as connection:
                stored = json.loads(connection.execute(
                    "SELECT payload_json FROM n15_market_snapshots"
                ).fetchone()[0])
            self.assertEqual(stored, legacy)

    def test_legacy_upgrade_missing_frozen_member_retains_original(self):
        items, raws, checked = full_batch()
        payload, snapshot = build_n15_snapshot(
            N15_STRATEGY, items, raws
        )
        legacy = legacy_n15_payload(payload)
        entrant = candidate("NEWUSDT", 100)
        current = [*items[:-1], entrant]
        current_raws = {
            **raws,
            entrant.symbol: deepcopy(raws["S001USDT"]),
        }
        current_raws.pop(items[-1].symbol)
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            self.assertEqual(recorder.record_n15_market_snapshot(
                "N15", str(snapshot.e_open_time_ms), legacy
            ), "OK")
            with self.assertLogs("n15", level="WARNING") as captured:
                blocked = sched._build_n15_batch_context(
                    {"quote_volume_top": current}, current_raws, checked
                )
            self.assertFalse(blocked.complete)
            self.assertTrue(any(
                "N15 frozen member kline missing" in message
                for message in captured.output
            ))
            with recorder._connect() as connection:
                stored = json.loads(connection.execute(
                    "SELECT payload_json FROM n15_market_snapshots"
                ).fetchone()[0])
                self.assertEqual(stored, legacy)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type LIKE 'n15_snapshot_quarantine%'"
                ).fetchone()[0], 0)

    def test_shifted_legacy_snapshot_uses_strict_one_seed_compatibility(self):
        items, first_raws, second_raws, checked = n15_seed_drift_windows()
        payload, snapshot = build_n15_snapshot(
            N15_STRATEGY, items, first_raws
        )
        legacy = legacy_n15_payload(payload)
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            self.assertEqual(recorder.record_n15_market_snapshot(
                "N15", str(snapshot.e_open_time_ms), legacy
            ), "OK")
            recovered = sched._build_n15_batch_context(
                {"quote_volume_top": items},
                second_raws,
                checked + INTERVAL,
            )
            self.assertTrue(recovered.complete)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT symbol,status,reason FROM n15_entry_states"
                ).fetchall(), [(
                    "S001USDT",
                    "MISSED",
                    "HISTORICAL_N15_ENTRY_MISSED",
                )])
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_market_snapshots"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type='n15_snapshot_quarantined'"
                ).fetchone()[0], 0)

    def test_legacy_one_seed_forward_minimum_accepts_rounding_boundary_and_rejects_impossible_raw(self):
        items, first_raws, second_raws, checked = (
            n15_minimum_seed_windows()
        )
        payload, snapshot = build_n15_snapshot(
            N15_STRATEGY, items, first_raws
        )
        legacy = legacy_n15_payload(payload)
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            self.assertEqual(recorder.record_n15_market_snapshot(
                "N15", str(snapshot.e_open_time_ms), legacy
            ), "OK")
            recovered = sched._build_n15_batch_context(
                {"quote_volume_top": items},
                second_raws,
                checked + INTERVAL,
            )
            self.assertTrue(recovered.complete)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type='n15_snapshot_quarantined'"
                ).fetchone()[0], 0)

        impossible = deepcopy(second_raws)
        for raw in impossible.values():
            raw[0][2] = "1000"
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            self.assertEqual(recorder.record_n15_market_snapshot(
                "N15", str(snapshot.e_open_time_ms), legacy
            ), "OK")
            blocked = sched._build_n15_batch_context(
                {"quote_volume_top": items},
                impossible,
                checked + INTERVAL,
            )
            self.assertFalse(blocked.complete)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_entry_states"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_market_snapshots"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type='n15_snapshot_quarantine_failed'"
                ).fetchone()[0], 1)

    def test_unprovable_legacy_multi_seed_shift_is_retained_fail_closed(self):
        items, first_raws, second_raws, checked = n15_seed_drift_windows()
        payload, snapshot = build_n15_snapshot(
            N15_STRATEGY, items, first_raws
        )
        legacy = legacy_n15_payload(payload)
        third_raws = {
            symbol: deepcopy(raw[1:]) + [
                kline(
                    123,
                    open_price=raw[-1][4],
                    high="101",
                    low="99.8",
                    close=raw[-1][4],
                )
            ]
            for symbol, raw in second_raws.items()
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            self.assertEqual(recorder.record_n15_market_snapshot(
                "N15", str(snapshot.e_open_time_ms), legacy
            ), "OK")
            with self.assertLogs("n15", level="WARNING") as captured:
                blocked = sched._build_n15_batch_context(
                    {"quote_volume_top": items},
                    third_raws,
                    checked + 2 * INTERVAL,
                )
            self.assertFalse(blocked.complete)
            self.assertTrue(any(
                "N15 legacy source window incomplete" in message
                for message in captured.output
            ))
            with recorder._connect() as connection:
                stored = json.loads(connection.execute(
                    "SELECT payload_json FROM n15_market_snapshots"
                ).fetchone()[0])
                self.assertEqual(stored, legacy)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_entry_states"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type='n15_snapshot_quarantined'"
                ).fetchone()[0], 0)

    def test_new_snapshot_build_or_write_failure_retains_old_terminal_evidence(self):
        items, first_raws, second_raws, checked = n15_seed_drift_windows()
        failure_modes = ("build", "write")
        for failure_mode in failure_modes:
            with self.subTest(failure_mode=failure_mode), tempfile.TemporaryDirectory() as tmpdir:
                recorder, sched = scheduler(tmpdir)
                old = sched._build_n15_batch_context(
                    {"quote_volume_top": items}, first_raws, checked
                )
                self.assertTrue(old.complete)
                if failure_mode == "build":
                    failure = patch(
                        "trading_bot.strategy_scheduler.build_n15_snapshot",
                        side_effect=ValueError("forced current build failure"),
                    )
                else:
                    original_record = recorder.record_n15_market_snapshot
                    recorder.record_n15_market_snapshot = (
                        lambda *args, **kwargs: "N15_STATE_PERSIST_FAILED"
                    )
                    failure = patch(
                        "trading_bot.strategy_scheduler.build_n15_snapshot",
                        wraps=build_n15_snapshot,
                    )
                with failure:
                    blocked = sched._build_n15_batch_context(
                        {"quote_volume_top": items},
                        second_raws,
                        checked + INTERVAL,
                    )
                self.assertFalse(blocked.complete)
                if failure_mode == "write":
                    recorder.record_n15_market_snapshot = original_record
                with recorder._connect() as connection:
                    self.assertEqual(connection.execute(
                        "SELECT e_time FROM n15_market_snapshots"
                    ).fetchall(), [(str(old.current_open_time_ms),)])
                    self.assertEqual(connection.execute(
                        "SELECT status,reason FROM n15_entry_states"
                    ).fetchall(), [])
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM n15_snapshot_terminal_receipts"
                    ).fetchone()[0], 0)
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM events "
                        "WHERE event_type='n15_snapshot_quarantined'"
                    ).fetchone()[0], 0)
                recovered = sched._build_n15_batch_context(
                    {"quote_volume_top": items},
                    second_raws,
                    checked + INTERVAL,
                )
                self.assertTrue(recovered.complete)
                with recorder._connect() as connection:
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM n15_entry_states"
                    ).fetchone()[0], 1)
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM n15_snapshot_terminal_receipts"
                    ).fetchone()[0], 1)
                    self.assertEqual(connection.execute(
                        "SELECT e_time FROM n15_market_snapshots"
                    ).fetchall(), [(
                        str(recovered.current_open_time_ms),
                    )])

    def test_historical_terminal_is_durable_before_snapshot_cleanup(self):
        items, first_raws, second_raws, checked = n15_seed_drift_windows()
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            old = sched._build_n15_batch_context(
                {"quote_volume_top": items}, first_raws, checked
            )
            self.assertTrue(old.complete)
            original_delete = recorder.delete_n15_market_snapshots_before
            recorder.delete_n15_market_snapshots_before = (
                lambda *args, **kwargs: False
            )
            failed = sched._build_n15_batch_context(
                {"quote_volume_top": items},
                second_raws,
                checked + INTERVAL,
            )
            self.assertFalse(failed.complete)
            with recorder._connect() as connection:
                terminal_before = connection.execute(
                    "SELECT * FROM n15_entry_states"
                ).fetchall()
                self.assertEqual(terminal_before, [])
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_snapshot_terminal_receipts"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT e_time FROM n15_market_snapshots ORDER BY e_time"
                ).fetchall(), [
                    (str(old.current_open_time_ms),),
                    (str(second_raws[items[0].symbol][-1][0]),),
                ])
            recorder.delete_n15_market_snapshots_before = original_delete
            recovered = sched._build_n15_batch_context(
                {"quote_volume_top": items},
                second_raws,
                checked + INTERVAL,
            )
            self.assertTrue(recovered.complete)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_entry_states"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_snapshot_terminal_receipts"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT e_time FROM n15_market_snapshots"
                ).fetchall(), [(
                    str(recovered.current_open_time_ms),
                )])

    def test_winner_terminal_receipt_pairs_entry_and_retries_idempotently(self):
        items, first_raws, second_raws, checked = n15_seed_drift_windows()
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            first = sched._build_n15_batch_context(
                {"quote_volume_top": items}, first_raws, checked
            )
            self.assertTrue(first.complete)
            self.assertIsNotNone(first.snapshot.winner_symbol)
            second = sched._build_n15_batch_context(
                {"quote_volume_top": items}, second_raws, checked + INTERVAL
            )
            self.assertTrue(second.complete)
            with recorder._connect() as connection:
                receipt = connection.execute(
                    "SELECT strategy_id,e_time,winner_symbol,"
                    "snapshot_payload_blob,snapshot_payload_size,"
                    "snapshot_payload_sha256,snapshot_blob_sha256,close_reason,"
                    "entry_symbol,entry_structure_id,entry_status,entry_reason,"
                    "entry_detail_json,entry_detail_sha256,receipt_sha256,"
                    "closed_at FROM n15_snapshot_terminal_receipts"
                ).fetchone()
                state = connection.execute(
                    "SELECT symbol,structure_id,status,reason,detail_json "
                    "FROM n15_entry_states WHERE strategy_id='N15'"
                ).fetchone()
                self.assertEqual(tuple(receipt[8:13]), tuple(state))
                self.assertEqual(receipt[2], first.snapshot.winner_symbol)
                _, restored = decode_n15_terminal_payload(receipt)
                self.assertEqual(
                    restored["winner_symbol"], first.snapshot.winner_symbol
                )
            replay = sched._build_n15_batch_context(
                {"quote_volume_top": items}, second_raws, checked + INTERVAL
            )
            self.assertTrue(replay.complete)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_snapshot_terminal_receipts"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_entry_states"
                ).fetchone()[0], 1)

    def test_terminal_receipt_tamper_and_half_schema_fail_closed(self):
        items, first_raws, second_raws, checked = n15_seed_drift_windows()
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            self.assertTrue(sched._build_n15_batch_context(
                {"quote_volume_top": items}, first_raws, checked
            ).complete)
            self.assertTrue(sched._build_n15_batch_context(
                {"quote_volume_top": items}, second_raws, checked + INTERVAL
            ).complete)
            with self.assertRaises(sqlite3.DatabaseError):
                with recorder._connect() as connection:
                    connection.execute(
                        "UPDATE n15_snapshot_terminal_receipts "
                        "SET snapshot_payload_blob=X'00'"
                    )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                with connection:
                    connection.execute(
                        "DROP TRIGGER trg_n15_snapshot_terminal_no_update"
                    )
                    connection.execute(
                        "UPDATE n15_snapshot_terminal_receipts "
                        "SET snapshot_payload_blob=X'00'"
                    )
                    connection.execute(
                        N15_TERMINAL_TRIGGER_SQL[
                            "trg_n15_snapshot_terminal_no_update"
                        ]
                    )
                with self.assertRaisesRegex(
                    RuntimeError, "snapshot evidence is invalid"
                ):
                    validate_n15_terminal_graph(connection)

        with closing(sqlite3.connect(":memory:")) as connection:
            connection.execute(
                "CREATE TABLE n15_market_snapshots(id INTEGER PRIMARY KEY)"
            )
            self.assertEqual(
                n15_terminal_schema_status(connection), "PRE_N15_TERMINAL"
            )
            connection.execute(
                "ALTER TABLE n15_market_snapshots "
                "ADD COLUMN payload_sha256 TEXT"
            )
            with self.assertRaisesRegex(RuntimeError, "half-installed"):
                n15_terminal_schema_status(connection)

    def test_terminal_archive_insert_delete_and_confirmation_fail_atomically(self):
        items, first_raws, second_raws, checked = n15_seed_drift_windows()
        for failure_point in ("insert", "delete", "confirmation"):
            with self.subTest(failure_point=failure_point), tempfile.TemporaryDirectory() as tmpdir:
                recorder, sched = scheduler(tmpdir)
                first = sched._build_n15_batch_context(
                    {"quote_volume_top": items}, first_raws, checked
                )
                self.assertTrue(first.complete)
                original_authorize = recorder._authorize_coverage_epoch_mutation
                original_confirm = recorder._confirm_n15_snapshot_archive
                if failure_point in {"insert", "delete"}:
                    def authorize(action, *args):
                        if action == "n15_snapshot_terminal_" + failure_point:
                            return 0
                        return original_authorize(action, *args)
                    recorder._authorize_coverage_epoch_mutation = authorize
                else:
                    recorder._confirm_n15_snapshot_archive = (
                        lambda _connection, _identities: (_ for _ in ()).throw(
                            RuntimeError("forced archive confirmation failure")
                        )
                    )
                blocked = sched._build_n15_batch_context(
                    {"quote_volume_top": items},
                    second_raws,
                    checked + INTERVAL,
                )
                self.assertFalse(blocked.complete)
                recorder._authorize_coverage_epoch_mutation = original_authorize
                recorder._confirm_n15_snapshot_archive = original_confirm
                with recorder._connect() as connection:
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM n15_snapshot_terminal_receipts"
                    ).fetchone()[0], 0)
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM n15_entry_states"
                    ).fetchone()[0], 0)
                    self.assertEqual(connection.execute(
                        "SELECT e_time FROM n15_market_snapshots "
                        "ORDER BY CAST(e_time AS INTEGER)"
                    ).fetchall(), [
                        (str(first.current_open_time_ms),),
                        (str(second_raws[items[0].symbol][-1][0]),),
                    ])

    def test_terminal_archive_failure_blocks_current_and_execution_then_retries(self):
        items, first_raws, second_raws, checked = n15_seed_drift_windows()
        for failure_point in ("insert", "delete", "confirmation"):
            with self.subTest(failure_point=failure_point), tempfile.TemporaryDirectory() as tmpdir:
                recorder, sched = scheduler(tmpdir)
                self.assertTrue(sched._build_n15_batch_context(
                    {"quote_volume_top": items}, first_raws, checked
                ).complete)
                original_authorize = recorder._authorize_coverage_epoch_mutation
                original_confirm = recorder._confirm_n15_snapshot_archive

                if failure_point in {"insert", "delete"}:
                    def reject_action(action, *args):
                        if action == "n15_snapshot_terminal_" + failure_point:
                            return 0
                        return original_authorize(action, *args)
                    recorder._authorize_coverage_epoch_mutation = reject_action
                else:
                    recorder._confirm_n15_snapshot_archive = (
                        lambda _connection, _identities: (_ for _ in ()).throw(
                            RuntimeError("forced archive confirmation failure")
                        )
                    )
                failed_scan = recorder.begin_scan(100, items, True)
                failed = sched.evaluate(
                    failed_scan,
                    {"quote_volume_top": items, "negative_funding": []},
                    second_raws,
                    checked_at_ms=checked + INTERVAL,
                )
                self.assertFalse(failed.signal_batch_published)
                self.assertEqual(failed.passed_signals, [])
                self.assertEqual(failed.live_candidates, [])
                recorder._authorize_coverage_epoch_mutation = original_authorize
                recorder._confirm_n15_snapshot_archive = original_confirm
                with recorder._connect() as connection:
                    self.assertEqual(connection.execute(
                        "SELECT current_scan_id FROM strategy_signal_current "
                        "WHERE singleton_id=1"
                    ).fetchone(), (None,))
                    self.assertEqual(connection.execute(
                        "SELECT state,recorded_count,expected_count FROM "
                        "strategy_signal_batches WHERE scan_id=?",
                        (failed_scan,),
                    ).fetchone(), ("STAGING", 100, None))
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM n15_snapshot_terminal_receipts"
                    ).fetchone()[0], 0)
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM n15_entry_states"
                    ).fetchone()[0], 0)
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM n15_market_snapshots"
                    ).fetchone()[0], 2)
                    for table in (
                        "strategy_paper_trades",
                        "strategy_live_links",
                        "trade_reviews",
                    ):
                        self.assertEqual(connection.execute(
                            "SELECT COUNT(*) FROM " + table
                        ).fetchone()[0], 0)
                recovered_scan = recorder.begin_scan(100, items, True)
                recovered = sched.evaluate(
                    recovered_scan,
                    {"quote_volume_top": items, "negative_funding": []},
                    second_raws,
                    checked_at_ms=checked + INTERVAL,
                )
                self.assertTrue(recovered.signal_batch_published)
                with recorder._connect() as connection:
                    self.assertEqual(connection.execute(
                        "SELECT current_scan_id FROM strategy_signal_current "
                        "WHERE singleton_id=1"
                    ).fetchone(), (recovered_scan,))
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM n15_snapshot_terminal_receipts"
                    ).fetchone()[0], 1)

    def test_resigned_snapshot_tampering_is_retained_and_blocks_publication(self):
        items, raws, checked = full_batch()
        payload, snapshot = build_n15_snapshot(
            N15_STRATEGY, items, raws
        )

        def raw_tamper(value):
            value["metric_source"]["rows"][0]["candles"][0][1] = "201"

        def derived_tamper(value):
            value["rows"][0]["move_c"] = "999"

        def config_tamper(value):
            value["config_signature"] = {}

        def identity_tamper(value):
            value["rows"][0]["symbol"] = "FORGEDUSDT"
            value["metric_source"]["rows"][0]["symbol"] = "FORGEDUSDT"

        for name, mutate in {
            "raw": raw_tamper,
            "derived": derived_tamper,
            "config": config_tamper,
            "identity": identity_tamper,
        }.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                recorder, sched = scheduler(tmpdir)
                broken = deepcopy(payload)
                mutate(broken)
                resign(broken)
                with recorder._connect() as connection:
                    connection.execute(
                        "INSERT INTO n15_market_snapshots("
                        "strategy_id,e_time,payload_json,created_at,updated_at"
                        ") VALUES(?,?,?,?,?)",
                        (
                            "N15",
                            str(snapshot.e_open_time_ms),
                            json.dumps(broken),
                            "now",
                            "now",
                        ),
                    )
                scan_id = recorder.begin_scan(len(items), items, True)
                self.assertIsNotNone(scan_id)
                result = sched.evaluate(
                    scan_id,
                    {"quote_volume_top": items},
                    raws,
                    checked_at_ms=checked,
                )
                self.assertFalse(result.signal_batch_published)
                self.assertEqual(result.passed_signals, [])
                self.assertEqual(len(result.signals), 100)
                self.assertTrue(all(
                    signal.reason == "N15_MARKET_CONTEXT_INSUFFICIENT"
                    for signal in result.signals
                ))
                with recorder._connect() as connection:
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM strategy_signal_batches "
                        "WHERE state='CURRENT'"
                    ).fetchone()[0], 0)
                    self.assertEqual(connection.execute(
                        "SELECT state,recorded_count,expected_count FROM "
                        "strategy_signal_batches WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(), ("STAGING", 100, None))
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM n15_market_snapshots"
                    ).fetchone()[0], 1)
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM n15_snapshot_terminal_receipts"
                    ).fetchone()[0], 0)
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM n15_entry_states"
                    ).fetchone()[0], 0)
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM events "
                        "WHERE event_type='n15_snapshot_quarantine_failed'"
                    ).fetchone()[0], 1)
                    for table in (
                        "strategy_paper_trades",
                        "strategy_live_links",
                        "trade_reviews",
                    ):
                        self.assertEqual(connection.execute(
                            "SELECT COUNT(*) FROM " + table
                        ).fetchone()[0], 0)

    def test_historical_overlap_raw_tamper_is_quarantined_fail_closed(self):
        items, first_raws, second_raws, checked = n15_seed_drift_windows()
        tampered = deepcopy(second_raws)
        tampered["S001USDT"][0][2] = "101"
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            first = sched._build_n15_batch_context(
                {"quote_volume_top": items}, first_raws, checked
            )
            self.assertTrue(first.complete)
            blocked = sched._build_n15_batch_context(
                {"quote_volume_top": items},
                tampered,
                checked + INTERVAL,
            )
            self.assertFalse(blocked.complete)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_entry_states"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_market_snapshots"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type='n15_snapshot_quarantine_failed'"
                ).fetchone()[0], 1)

    def test_long_downtime_closes_old_snapshot_and_allows_new_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            items, raws, checked = full_batch()
            old = sched._build_n15_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertTrue(old.complete)
            shift = 130 * INTERVAL
            recent_raws = {
                symbol: [
                    [row[0] + shift, *row[1:]] for row in raw
                ]
                for symbol, raw in raws.items()
            }
            recent_checked = checked + shift
            current = sched._build_n15_batch_context(
                {"quote_volume_top": items}, recent_raws, recent_checked
            )
            self.assertTrue(current.complete)
            self.assertEqual(
                current.current_open_time_ms,
                old.current_open_time_ms + shift,
            )
            with recorder._connect() as connection:
                states = connection.execute(
                    "SELECT e_time,status,reason FROM n15_entry_states"
                ).fetchall()
                snapshots = connection.execute(
                    "SELECT e_time FROM n15_market_snapshots"
                ).fetchall()
            self.assertEqual(states, [(
                str(old.current_open_time_ms), "MISSED",
                "HISTORICAL_N15_ENTRY_MISSED",
            )])
            self.assertEqual(snapshots, [(str(current.current_open_time_ms),)])

    def test_main_unions_frozen_members_and_fetches_each_symbol_once(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            items, raws, checked = full_batch()
            self.assertTrue(sched._build_n15_batch_context(
                {"quote_volume_top": items}, raws, checked
            ).complete)
            entrant = candidate("NEWUSDT", 100)
            current = [*items[:-1], entrant]
            all_raws = {**raws, entrant.symbol: deepcopy(raws["S100USDT"])}

            class FakeClient:
                def __init__(self):
                    self.calls = []
                def get_klines(self, symbol):
                    self.calls.append(symbol)
                    return all_raws[symbol]
                def get_klines_for_interval(self, *args, **kwargs):
                    return []
                def get_aggregate_trades(self, *args, **kwargs):
                    return []
            class FakeTrader:
                def close_dry_run_position_if_triggered(self):
                    return None
                def sync_state_with_exchange(self):
                    return SimpleNamespace(has_position=False, closed_state=None)
            class FakePaper:
                def close_triggered_open_trades(self, *args):
                    return []
            class FakeMonitor:
                def scan_for_strategies(self, volume_top_n):
                    return StrategyMarketScan(100, [], current)
            class InspectScheduler:
                def __init__(self):
                    self.symbols = None
                def evaluate(self, scan_id, groups, raw_by_symbol, checked_at_ms=None):
                    if type(scan_id) is not int or scan_id <= 0:
                        raise AssertionError("main did not begin an audited scan")
                    self.symbols = set(raw_by_symbol)
                    return SimpleNamespace(
                        signals=[],
                        passed_signals=[],
                        live_candidates=[],
                        signal_batch_published=True,
                    )
                def choose_live_candidate(self, candidates, live_blocked):
                    return None

            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(
                dry_run=True, symbol_cooldown_hours=4
            )
            bot.logger = logging.getLogger("n15_main_frozen")
            bot.client = FakeClient()
            bot.recorder = recorder
            bot.monitor = FakeMonitor()
            bot.trader = FakeTrader()
            bot.paper_trader = FakePaper()
            bot.strategy_scheduler = InspectScheduler()
            bot.strategies = (N15_STRATEGY,)
            bot.state = SimpleNamespace(load=lambda: None, clear=lambda: None)
            with patch(
                "trading_bot.main.time.time", return_value=checked / 1000
            ):
                bot._run_once_multi_strategy()
            self.assertEqual(len(bot.client.calls), 101)
            self.assertEqual(len(set(bot.client.calls)), 101)
            self.assertEqual(
                bot.strategy_scheduler.symbols,
                {item.symbol for item in items} | {entrant.symbol},
            )

    def test_snapshot_and_state_persistence_fail_closed_and_conflicts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            items, raws, checked = full_batch()
            recorder.record_n15_market_snapshot = lambda *args: "N15_STATE_PERSIST_FAILED"
            context = sched._build_n15_batch_context({"quote_volume_top": items}, raws, checked)
            self.assertFalse(context.complete)
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = scheduler(tmpdir)
            items, raws, checked = full_batch()
            payload, snapshot = build_n15_snapshot(N15_STRATEGY, items, raws)
            self.assertEqual(
                recorder.record_n15_market_snapshot(
                    "OTHER", str(snapshot.e_open_time_ms), payload
                ),
                "N15_STATE_INCONSISTENT",
            )
            wrong_time = deepcopy(payload)
            self.assertEqual(
                recorder.record_n15_market_snapshot(
                    "N15", str(snapshot.e_open_time_ms + INTERVAL), wrong_time
                ),
                "N15_STATE_INCONSISTENT",
            )
            empty = deepcopy(payload)
            empty["rows"] = []
            resign(empty)
            self.assertEqual(
                recorder.record_n15_market_snapshot(
                    "N15", str(snapshot.e_open_time_ms), empty
                ),
                "N15_STATE_INCONSISTENT",
            )
            analysis = analyze_n15_breadth_recovery_leader(
                snapshot.winner_symbol,
                raws[snapshot.winner_symbol],
                snapshot,
                snapshot_context_complete=True,
                checked_at_ms=checked,
            )
            structure = analysis.structure
            detail = analysis.detail_json()
            e_time = str(structure.entry.open_time_ms)
            args = (
                "N15", e_time, structure.symbol, structure.structure_id,
                "CONSUMED", "PASSED", detail,
            )
            self.assertEqual(
                recorder.record_n15_market_snapshot(
                    "N15", e_time, payload
                ),
                "OK",
            )
            self.assertEqual(
                recorder.record_n15_entry_state(*args), "INSERTED"
            )
            self.assertEqual(
                recorder.record_n15_entry_state(*args), "EXISTS"
            )
            self.assertEqual(
                recorder.record_n15_entry_state(
                    "N15", e_time, "B", structure.structure_id,
                    "CONSUMED", "PASSED", detail,
                ),
                "N15_STATE_INCONSISTENT",
            )
            with self.assertRaises(sqlite3.DatabaseError):
                with recorder._connect() as connection:
                    connection.execute(
                        "UPDATE n15_entry_states SET status='FORGED' "
                        "WHERE strategy_id='N15' AND e_time=?",
                        (e_time,),
                    )
            self.assertIsNotNone(recorder.get_n15_entry_state("N15", e_time))
            strict_scheduler = StrategyScheduler(
                (N15_STRATEGY,), 96, recorder,
                logging.getLogger("n15_strict_state"),
            )
            replay = strict_scheduler.evaluate(
                None, {"quote_volume_top": items}, raws,
                checked_at_ms=checked,
            )
            winner = next(
                item for item in replay.signals
                if item.candidate.symbol == structure.symbol
            )
            self.assertEqual(winner.reason, "N15_OPPORTUNITY_CONSUMED")
            forged = deepcopy(detail)
            forged["reason"] = "NOT_A_REASON"
            self.assertEqual(
                recorder.record_n15_entry_state(
                    "N15", e_time, structure.symbol,
                    structure.structure_id, "FORGED", "NOT_A_REASON", forged,
                ),
                "N15_STATE_INCONSISTENT",
            )

    def test_corrupt_snapshot_is_retained_and_repeatedly_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            items, raws, checked = full_batch()
            payload, snapshot = build_n15_snapshot(N15_STRATEGY, items, raws)
            payload["config_signature"] = {}
            resign(payload)
            with recorder._connect() as connection:
                connection.execute(
                    "INSERT INTO n15_market_snapshots("
                    "strategy_id,e_time,payload_json,created_at,updated_at"
                    ") VALUES(?,?,?,?,?)",
                    (
                        "N15", str(snapshot.e_open_time_ms),
                        json.dumps(payload), "now", "now",
                    ),
                )
            first = sched._build_n15_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertFalse(first.complete)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_market_snapshots"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events WHERE "
                    "event_type='n15_snapshot_quarantine_failed'"
                ).fetchone()[0], 1)
            second = sched._build_n15_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertFalse(second.complete)

    def test_snapshot_delete_failure_reports_retained_not_quarantined(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            items, raws, checked = full_batch()
            payload, snapshot = build_n15_snapshot(N15_STRATEGY, items, raws)
            payload["config_signature"] = {}
            resign(payload)
            with recorder._connect() as connection:
                connection.execute(
                    "INSERT INTO n15_market_snapshots("
                    "strategy_id,e_time,payload_json,created_at,updated_at"
                    ") VALUES(?,?,?,?,?)",
                    (
                        "N15", str(snapshot.e_open_time_ms),
                        json.dumps(payload), "now", "now",
                    ),
                )
            recorder.delete_n15_market_snapshot = lambda *args: False
            result = sched._build_n15_batch_context(
                {"quote_volume_top": items}, raws, checked
            )
            self.assertFalse(result.complete)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_market_snapshots"
                ).fetchone()[0], 1)
                failed = connection.execute(
                    "SELECT payload_json FROM events "
                    "WHERE event_type='n15_snapshot_quarantine_failed'"
                ).fetchone()
                quarantined = connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type='n15_snapshot_quarantined'"
                ).fetchone()[0]
            self.assertTrue(json.loads(failed[0])["retained"])
            self.assertEqual(quarantined, 0)

    def test_resigned_bad_historical_snapshot_is_retained_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            items, raws, checked = full_batch()
            payload, snapshot = build_n15_snapshot(N15_STRATEGY, items, raws)
            payload["rows"][0]["reason"] = "FORGED"
            resign(payload)
            with recorder._connect() as connection:
                connection.execute(
                    "INSERT INTO n15_market_snapshots("
                    "strategy_id,e_time,payload_json,created_at,updated_at"
                    ") VALUES(?,?,?,?,?)",
                    (
                        "N15", str(snapshot.e_open_time_ms),
                        json.dumps(payload), "now", "now",
                    ),
                )
            shifted = {
                symbol: [[row[0] + INTERVAL, *row[1:]] for row in raw]
                for symbol, raw in raws.items()
            }
            first = sched._build_n15_batch_context(
                {"quote_volume_top": items}, shifted, checked + INTERVAL
            )
            self.assertFalse(first.complete)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_market_snapshots"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type='n15_snapshot_quarantine_failed'"
                ).fetchone()[0], 1)
            second = sched._build_n15_batch_context(
                {"quote_volume_top": items}, shifted, checked + INTERVAL
            )
            self.assertFalse(second.complete)

    def test_configuration_and_legacy_paper_results_keep_statistics_only(self):
        strategy_ids = [item.strategy_id for item in load_all_strategies()]
        self.assertEqual(
            strategy_ids[:20],
            [f"N{i:02d}" for i in range(1, 21)],
        )
        self.assertEqual(strategy_ids, [f"N{i:02d}" for i in range(1, 26)])
        self.assertIsNone(N15_STRATEGY.funding_threshold)
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = scheduler(tmpdir)
            for symbol in ("AUSDT", "BUSDT"):
                trade_id = recorder.open_strategy_paper_trade(
                    "N15", symbol, "100", "99", "105", "", {}, {}
                )
                recorder.close_strategy_paper_trade(
                    trade_id, "WIN", "TAKE_PROFIT", "105", "5"
                )
            state = recorder.get_strategy_state("N15")
            self.assertEqual(state.consecutive_wins, 0)
            self.assertEqual(state.paper_trade_count, 2)
            self.assertEqual(state.win_count, 2)
            self.assertEqual(state.loss_count, 0)
            self.assertFalse(state.live_eligible)
            trade_id = recorder.open_strategy_paper_trade(
                "N15", "CUSDT", "100", "99", "105", "", {}, {}
            )
            recorder.close_strategy_paper_trade(
                trade_id, "LOSS", "STOP_LOSS", "99", "-1"
            )
            state = recorder.get_strategy_state("N15")
            self.assertEqual(state.consecutive_wins, 0)
            self.assertEqual(state.paper_trade_count, 3)
            self.assertEqual(state.win_count, 2)
            self.assertEqual(state.loss_count, 1)
            self.assertFalse(state.live_eligible)
            self.assertEqual(state.last_trade_result, "LOSS")


class N15MainTraderTests(unittest.TestCase):
    def test_real_main_waits_for_complete_axis_then_finalizes_without_trade_side_effects(self):
        items, first_raws, second_raws, checked = n15_seed_drift_windows()
        short_new_axis = {
            symbol: deepcopy(raw[-3:]) + [deepcopy(second_raws[symbol][-1])]
            for symbol, raw in first_raws.items()
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, real_scheduler = scheduler(tmpdir)
            frozen = real_scheduler._build_n15_batch_context(
                {"quote_volume_top": items}, first_raws, checked
            )
            self.assertTrue(frozen.complete)
            with recorder._connect() as connection:
                frozen_before = connection.execute(
                    "SELECT * FROM n15_market_snapshots"
                ).fetchall()

            class Client:
                def __init__(self):
                    self.raws = short_new_axis

                def get_klines(self, symbol):
                    return self.raws[symbol]

                def get_klines_for_interval(self, *args, **kwargs):
                    return []

                def get_aggregate_trades(self, *args, **kwargs):
                    return []

            class Monitor:
                def scan_for_strategies(self, volume_top_n):
                    return StrategyMarketScan(100, [], items)

            class NoSideEffectTrader:
                def __init__(self):
                    self.plan_calls = 0
                    self.open_calls = 0

                def close_dry_run_position_if_triggered(self):
                    return None

                def sync_state_with_exchange(self):
                    return SimpleNamespace(
                        has_position=False,
                        closed_state=None,
                        pending_detail=None,
                        pending_resolved=False,
                    )

                def build_breadth_recovery_margin_capped_trade_plan(
                    self, *args, **kwargs
                ):
                    self.plan_calls += 1
                    raise AssertionError("N15 incomplete context reached plan")

                def open_long_plan_with_protection(self, *args, **kwargs):
                    self.open_calls += 1
                    raise AssertionError("N15 incomplete context reached order")

            class NoSideEffectPaper:
                def __init__(self):
                    self.open_calls = 0

                def close_triggered_open_trades(self, *args, **kwargs):
                    return []

                def open_trade(self, *args, **kwargs):
                    self.open_calls += 1
                    raise AssertionError("N15 incomplete context reached paper")

            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(
                dry_run=True, symbol_cooldown_hours=4
            )
            bot.logger = logging.getLogger("n15_main_boundary_wait")
            bot.client = Client()
            bot.recorder = recorder
            bot.monitor = Monitor()
            bot.trader = NoSideEffectTrader()
            bot.paper_trader = NoSideEffectPaper()
            bot.strategy_scheduler = real_scheduler
            bot.strategies = (N15_STRATEGY,)
            bot.state = SimpleNamespace(load=lambda: None, clear=lambda: None)

            with patch(
                "trading_bot.main.time.time",
                return_value=(checked + INTERVAL) / 1000,
            ):
                bot._run_once_multi_strategy()
            self.assertEqual(bot.trader.plan_calls, 0)
            self.assertEqual(bot.trader.open_calls, 0)
            self.assertEqual(bot.paper_trader.open_calls, 0)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT * FROM n15_market_snapshots"
                ).fetchall(), frozen_before)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM n15_entry_states"
                ).fetchone()[0], 0)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type='n15_snapshot_quarantined'"
                ).fetchone()[0], 0)
                current_scan_id = connection.execute(
                    "SELECT current_scan_id FROM strategy_signal_current "
                    "WHERE singleton_id=1"
                ).fetchone()[0]
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM strategy_signals "
                    "WHERE scan_id=? AND reason='N15_MARKET_CONTEXT_INSUFFICIENT'",
                    (current_scan_id,),
                ).fetchone()[0], 100)

            bot.client.raws = second_raws
            with patch(
                "trading_bot.main.time.time",
                return_value=(checked + INTERVAL) / 1000,
            ):
                bot._run_once_multi_strategy()
            self.assertEqual(bot.trader.plan_calls, 0)
            self.assertEqual(bot.trader.open_calls, 0)
            self.assertEqual(bot.paper_trader.open_calls, 0)
            with recorder._connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT symbol,status,reason FROM n15_entry_states"
                ).fetchall(), [(
                    "S001USDT",
                    "MISSED",
                    "HISTORICAL_N15_ENTRY_MISSED",
                )])
                self.assertEqual(connection.execute(
                    "SELECT e_time FROM n15_market_snapshots"
                ).fetchall(), [(
                    str(second_raws[items[0].symbol][-1][0]),
                )])
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type='n15_snapshot_quarantined'"
                ).fetchone()[0], 0)
                for table in (
                    "strategy_paper_trades",
                    "strategy_live_links",
                    "trade_reviews",
                ):
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM " + table
                    ).fetchone()[0], 0)

    def test_main_state_reread_failure_blocks_before_any_live_attempt(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, real_scheduler = scheduler(tmpdir)
            items, raws, checked = full_batch()
            for symbol in ("WIN1USDT", "WIN2USDT"):
                trade_id = recorder.open_strategy_paper_trade(
                    "N15", symbol, "100", "99", "105", "", {}, {}
                )
                recorder.close_strategy_paper_trade(
                    trade_id, "WIN", "TAKE_PROFIT", "105", "5"
                )
            class Client:
                def get_klines(self, symbol):
                    return raws[symbol]
                def get_klines_for_interval(self, *args, **kwargs):
                    return []
                def get_aggregate_trades(self, *args, **kwargs):
                    return []
            class Monitor:
                def scan_for_strategies(self, volume_top_n):
                    return StrategyMarketScan(100, [], items)
            class FailingTrader:
                live_calls = 0
                def close_dry_run_position_if_triggered(self):
                    return None
                def sync_state_with_exchange(self):
                    return SimpleNamespace(has_position=False, closed_state=None)
                def build_breadth_recovery_margin_capped_trade_plan(
                    self, symbol, entry, p, risk_reward, **kwargs
                ):
                    return TradePlan(
                        symbol=symbol, leverage=10, quantity=Decimal("1"),
                        entry_price=entry, stop_loss_price=p,
                        take_profit_price=entry + Decimal("5") * (entry - p),
                        stop_loss_pct=(entry - p) / entry,
                        take_profit_pct=Decimal("5") * (entry - p) / entry,
                        amplitude_24h_pct=Decimal("0"),
                        high_24h_price=Decimal("0"), low_24h_price=Decimal("0"),
                        risk_amount=entry - p, notional_value=entry,
                        required_margin=entry / Decimal("10"),
                        balance=Decimal("1000"),
                        stop_mode="breadth_recovery_margin_capped",
                        structure_id=kwargs["structure_id"],
                        entry_min_price=kwargs["entry_min_price"],
                        entry_max_price=kwargs["entry_max_price"],
                    )
                def open_long_plan_with_protection(self, plan):
                    self.live_calls += 1
                    raise BinanceAPIError("forced journal failure")
            class UnreadableState:
                calls = 0
                def load(self):
                    self.calls += 1
                    if self.calls == 1:
                        return None
                    raise OSError("forced main state read failure")
            trader = FailingTrader()
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=True)
            bot.logger = logging.getLogger("n15_main_unreadable_state")
            bot.client = Client()
            bot.recorder = recorder
            bot.paper_trader = PaperTrader(
                recorder, bot.logger, clock_ms=lambda: checked
            )
            bot.monitor = Monitor()
            bot.trader = trader
            bot.strategy_scheduler = real_scheduler
            bot.strategies = (N15_STRATEGY,)
            bot.state = UnreadableState()
            with patch("trading_bot.main.time.time", return_value=checked / 1000):
                bot._run_once_multi_strategy()
            self.assertEqual(trader.live_calls, 0)
            self.assertIsNone(recorder.get_open_strategy_paper_trade("N15"))
            with recorder._connect() as connection:
                event = connection.execute(
                    "SELECT payload_json FROM events "
                    "WHERE event_type='strategy_live_order_local_state_unreadable'"
                ).fetchone()
                execution_rows = connection.execute(
                    "SELECT "
                    "(SELECT COUNT(*) FROM trade_reviews), "
                    "(SELECT COUNT(*) FROM strategy_live_links)"
                ).fetchone()
            self.assertIsNone(event)
            self.assertEqual(execution_rows, (0, 0))

    def test_real_main_scheduler_preserves_n15_audits_without_new_execution(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, real_scheduler = scheduler(tmpdir)
            items, first_raws, first_checked = full_batch()

            def append_setup(source, start_index, target_rank):
                result = deepcopy(source)
                for item in items:
                    rank = item.quote_volume_rank
                    raw = result[item.symbol]
                    if rank <= 40:
                        b = kline(start_index, "100", "100.3", "99.8", "100.2")
                    else:
                        b = kline(start_index, "100", "100.2", "99.7", "99.8")
                    if rank <= 60:
                        low = "99.7" if rank < target_rank else "99.8"
                        c = kline(start_index + 1, "100", "101", low, "100.8", "80", "41.6")
                    else:
                        c = kline(start_index + 1, "100", "100.2", "99.7", "99.8", "80", "40")
                    e = kline(start_index + 2, "100.8", "101", "99.8", "100.8")
                    raw.extend((b, c, e))
                    result[item.symbol] = raw[-122:]
                return result

            second_raws = append_setup(first_raws, 122, 2)
            third_raws = append_setup(second_raws, 125, 3)
            fourth_raws = append_setup(third_raws, 128, 4)
            checked_values = [
                first_checked,
                BASE + 124 * INTERVAL + 30_000,
                BASE + 127 * INTERVAL + 30_000,
                BASE + 130 * INTERVAL + 30_000,
            ]
            raw_rounds = [first_raws, second_raws, third_raws, fourth_raws]
            current_round = [0]

            class MainClient:
                def __init__(self):
                    self.calls = []
                def get_klines(self, symbol):
                    self.calls.append((current_round[0], symbol))
                    return raw_rounds[current_round[0]][symbol]
                def get_klines_for_interval(self, *args, **kwargs):
                    start = kwargs.get("start_time_ms")
                    if start is None and len(args) >= 4:
                        start = args[3]
                    start = int(start or 0)
                    return [[
                        start, "100", "1000", "100", "1000", "1",
                        start + 59_999,
                    ]]
                def get_aggregate_trades(self, symbol, start_time_ms, end_time_ms):
                    return [{"T": start_time_ms, "a": 1, "p": "1000"}]
            class MainMonitor:
                def scan_for_strategies(self, volume_top_n):
                    return StrategyMarketScan(100, [], items)
            class MainTrader:
                def __init__(self, state_store):
                    self.live_symbols = []
                    self.state_store = state_store
                    self.live_closed = False
                def close_dry_run_position_if_triggered(self):
                    if current_round[0] != 3 or self.live_closed:
                        return None
                    state = self.state_store.load()
                    if state is None:
                        return None
                    self.live_closed = True
                    self.state_store.clear()
                    entry = Decimal(state.entry_price)
                    stop = Decimal(state.stop_loss_price)
                    quantity = Decimal(state.quantity)
                    return DryRunCloseResult(
                        state=state,
                        exit_reason="STOP_LOSS",
                        exit_price=stop,
                        mark_price=stop,
                        pnl_amount=(stop - entry) * quantity,
                        pnl_pct=(stop - entry) / entry,
                        balance_before=Decimal("1000"),
                        balance_after=Decimal("999"),
                    )
                def sync_state_with_exchange(self):
                    return SimpleNamespace(has_position=True, closed_state=None)
                def build_breadth_recovery_margin_capped_trade_plan(
                    self, symbol, entry, p, risk_reward, **kwargs
                ):
                    return TradePlan(
                        symbol=symbol, leverage=10, quantity=Decimal("1"),
                        entry_price=entry, stop_loss_price=p,
                        take_profit_price=entry + Decimal("5") * (entry - p),
                        stop_loss_pct=(entry - p) / entry,
                        take_profit_pct=Decimal("5") * (entry - p) / entry,
                        amplitude_24h_pct=Decimal("0"),
                        high_24h_price=Decimal("0"), low_24h_price=Decimal("0"),
                        risk_amount=entry - p, notional_value=entry,
                        required_margin=entry / Decimal("10"),
                        balance=Decimal("1000"),
                        stop_mode="breadth_recovery_margin_capped",
                        structure_id=kwargs["structure_id"],
                        entry_min_price=kwargs["entry_min_price"],
                        entry_max_price=kwargs["entry_max_price"],
                    )
                def open_long_plan_with_protection(self, plan):
                    self.live_symbols.append(plan.symbol)
                    raise AssertionError("inactive N15 must not open a live position")

            client = MainClient()
            now = [checked_values[0]]
            state_store = StateStore(str(Path(tmpdir) / "state.json"))
            trader = MainTrader(state_store)
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(
                dry_run=True, symbol_cooldown_hours=4
            )
            bot.logger = logging.getLogger("n15_real_main_three_rounds")
            bot.client = client
            bot.recorder = recorder
            bot.paper_trader = PaperTrader(
                recorder, bot.logger, clock_ms=lambda: now[0]
            )
            bot.monitor = MainMonitor()
            bot.trader = trader
            bot.strategy_scheduler = real_scheduler
            bot.strategies = (N15_STRATEGY,)
            bot.state = state_store

            current_round[0] = 0
            with patch("trading_bot.main.time.time", return_value=now[0] / 1000):
                bot._run_once_multi_strategy()
            self.assertIsNone(recorder.get_open_strategy_paper_trade("N15"))
            current_round[0] = 1
            now[0] = checked_values[1]
            with patch("trading_bot.main.time.time", return_value=now[0] / 1000):
                bot._run_once_multi_strategy()
            state = recorder.get_strategy_state("N15")
            self.assertEqual(state.paper_trade_count, 0)
            self.assertEqual(state.consecutive_wins, 0)
            self.assertFalse(state.live_eligible)
            self.assertIsNone(recorder.get_open_strategy_paper_trade("N15"))
            current_round[0] = 2
            now[0] = checked_values[2]
            with patch("trading_bot.main.time.time", return_value=now[0] / 1000):
                bot._run_once_multi_strategy()
            self.assertEqual(trader.live_symbols, [])
            self.assertIsNone(state_store.load())
            current_round[0] = 3
            now[0] = checked_values[3]
            with patch("trading_bot.main.time.time", return_value=now[0] / 1000):
                bot._run_once_multi_strategy()
            self.assertEqual(trader.live_symbols, [])
            self.assertIsNone(recorder.get_open_strategy_paper_trade("N15"))
            final_state = recorder.get_strategy_state("N15")
            self.assertEqual(final_state.paper_trade_count, 0)
            self.assertEqual(final_state.consecutive_wins, 0)
            self.assertFalse(final_state.live_eligible)
            for index in range(4):
                symbols = [symbol for round_index, symbol in client.calls if round_index == index]
                self.assertEqual(len(symbols), 100)
                self.assertEqual(len(set(symbols)), 100)
            with recorder._connect() as connection:
                rows = connection.execute(
                    "SELECT symbol,detail_json FROM strategy_passed_signal_audits "
                    "WHERE strategy_id='N15' AND passed=1 ORDER BY id"
                ).fetchall()
                current_rows = connection.execute(
                    "SELECT scan_id,symbol,detail_json FROM strategy_signals "
                    "WHERE strategy_id='N15' AND passed=1 ORDER BY id"
                ).fetchall()
                ordinary_scan_ids = connection.execute(
                    "SELECT DISTINCT scan_id FROM strategy_signals "
                    "WHERE strategy_id='N15' ORDER BY scan_id"
                ).fetchall()
                ordinary_count = connection.execute(
                    "SELECT COUNT(*) FROM strategy_signals "
                    "WHERE strategy_id='N15'"
                ).fetchone()[0]
                terminals = connection.execute(
                    "SELECT detail_json FROM n15_entry_states "
                    "WHERE strategy_id='N15' ORDER BY CAST(e_time AS INTEGER)"
                ).fetchall()
                paper_rows = connection.execute(
                    "SELECT symbol,orders_json FROM strategy_paper_trades "
                    "WHERE strategy_id='N15' ORDER BY id"
                ).fetchall()
            self.assertEqual([row[0] for row in rows], [
                "S001USDT", "S002USDT", "S003USDT", "S004USDT",
            ])
            self.assertEqual(
                [(row[0], row[1]) for row in current_rows],
                [(recorder.current_strategy_signal_scan_id(), "S004USDT")],
            )
            self.assertEqual(
                ordinary_scan_ids,
                [(recorder.current_strategy_signal_scan_id(),)],
            )
            self.assertEqual(ordinary_count, len(items))
            for expected_rank, (_, detail_json) in enumerate(rows, 1):
                detail = json.loads(detail_json)
                self.assertEqual(
                    detail["structure"]["quote_volume_rank"], expected_rank
                )
                self.assertEqual(
                    detail["structure"]["candidate_universe"],
                    "quote_volume_top_frozen_n15",
                )
            self.assertEqual(len(terminals), 4)
            for expected_rank, (detail_json,) in enumerate(terminals, 1):
                detail = json.loads(detail_json)
                self.assertEqual(
                    detail["structure"]["quote_volume_rank"], expected_rank
                )
                self.assertEqual(
                    detail["structure"]["candidate_universe"],
                    "quote_volume_top_frozen_n15",
                )
            self.assertEqual(paper_rows, [])

    def _deadline_crossing_plan(self, trader, deadline):
        plan = trader.build_breadth_recovery_margin_capped_trade_plan(
            "N15USDT", Decimal("100.8"), Decimal("99.8"), Decimal("5"),
            entry_min_price=Decimal("100.8"),
            entry_max_price=Decimal("101.3"),
            structure_id="n15-cas",
        )
        return replace(
            plan,
            entry_deadline_ms=deadline,
            structure_context={
                "strategy_id": "N15", "structure_id": "n15-cas",
            },
        )

    def test_deadline_cas_does_not_delete_replaced_n14_pending(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            deadline = 1_730_000_820_000
            path = str(Path(tmpdir) / "state.json")
            replacement_holder = {}
            class ReplacingStore(StateStore):
                def compare_and_clear(self, expected):
                    replacement = replace(
                        expected,
                        symbol="N14USDT",
                        orders={
                            "strategy": {
                                "strategy_id": "N14",
                                "structure_id": "n14-other",
                            },
                            "execution_pending": {
                                "phase": "MARKET_ORDER_SUBMITTING",
                                "client_order_id": "other-client-id",
                            },
                        },
                    )
                    replacement_holder["state"] = replacement
                    StateStore(path).save(replacement)
                    return super().compare_and_clear(expected)
            store = ReplacingStore(path)
            client = FakeLiveExecutionClient({}, leverage=50)
            values = iter((deadline - 1, deadline))
            trader = Trader(
                client, live_test_config(), store,
                logging.getLogger("n15_cas_replaced"),
                clock_ms=lambda: next(values),
            )
            plan = self._deadline_crossing_plan(trader, deadline)
            with self.assertRaisesRegex(
                BinanceAPIError, "ENTRY_WINDOW_EXPIRED_PENDING_CHANGED"
            ):
                trader.open_long_plan_with_protection(plan)
            self.assertEqual(client.market_calls, [])
            self.assertEqual(store.load(), replacement_holder["state"])

    def test_deadline_cas_fsync_failure_restores_visible_n15_pending(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            deadline = 1_730_000_820_000
            class FailingClearStore(StateStore):
                def compare_and_clear(self, expected):
                    original = self._fsync_parent_directory
                    self._fsync_parent_directory = lambda: (_ for _ in ()).throw(
                        OSError("forced directory fsync failure")
                    )
                    try:
                        return super().compare_and_clear(expected)
                    finally:
                        self._fsync_parent_directory = original
            store = FailingClearStore(str(Path(tmpdir) / "state.json"))
            client = FakeLiveExecutionClient({}, leverage=50)
            values = iter((deadline - 1, deadline))
            trader = Trader(
                client, live_test_config(), store,
                logging.getLogger("n15_cas_fsync"),
                clock_ms=lambda: next(values),
            )
            plan = self._deadline_crossing_plan(trader, deadline)
            with self.assertRaisesRegex(
                BinanceAPIError, "ENTRY_WINDOW_EXPIRED_PENDING_CLEAR_FAILED"
            ):
                trader.open_long_plan_with_protection(plan)
            self.assertEqual(client.market_calls, [])
            retained = store.load()
            self.assertIsNotNone(retained)
            self.assertEqual(
                retained.orders["strategy"]["strategy_id"], "N15"
            )

    def test_pre_submit_journal_fsync_error_is_wrapped_and_pending_visible(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            class SaveThenFailStore(StateStore):
                def save(self, state):
                    super().save(state)
                    raise OSError("forced journal directory fsync failure")
            store = SaveThenFailStore(str(Path(tmpdir) / "state.json"))
            client = FakeLiveExecutionClient({}, leverage=50)
            trader = Trader(
                client, live_test_config(), store,
                logging.getLogger("n15_journal_fsync"),
            )
            plan = self._deadline_crossing_plan(
                trader, 9_999_999_999_999
            )
            with self.assertRaisesRegex(
                BinanceAPIError, "MARKET_ORDER_PRE_SUBMIT_JOURNAL_FAILED"
            ):
                trader.open_long_plan_with_protection(plan)
            self.assertEqual(client.market_calls, [])
            retained = StateStore(str(Path(tmpdir) / "state.json")).load()
            self.assertIsNotNone(retained)
            self.assertEqual(
                retained.orders["strategy"]["strategy_id"], "N15"
            )

    def test_expired_journal_initial_load_error_is_wrapped_and_pending_visible(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            deadline = 1_730_000_820_000
            class FailNextLoadStore(StateStore):
                fail_next_load = False
                def save(self, state):
                    super().save(state)
                    self.fail_next_load = True
                def load(self):
                    if self.fail_next_load:
                        self.fail_next_load = False
                        raise OSError("forced pending read failure")
                    return super().load()
            path = str(Path(tmpdir) / "state.json")
            store = FailNextLoadStore(path)
            client = FakeLiveExecutionClient({}, leverage=50)
            values = iter((deadline - 1, deadline))
            trader = Trader(
                client, live_test_config(), store,
                logging.getLogger("n15_pending_read"),
                clock_ms=lambda: next(values),
            )
            plan = self._deadline_crossing_plan(trader, deadline)
            with self.assertRaisesRegex(
                BinanceAPIError, "ENTRY_WINDOW_EXPIRED_PENDING_READ_FAILED"
            ):
                trader.open_long_plan_with_protection(plan)
            self.assertEqual(client.market_calls, [])
            self.assertIsNotNone(StateStore(path).load())

    def test_deadline_crossing_during_journal_never_submits_buy_or_leaves_pending(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            deadline = 1_730_000_120_000
            client = FakeLiveExecutionClient(
                {"status": "FILLED", "avgPrice": "100.8", "executedQty": "1"},
                leverage=50,
            )
            class CapturingStateStore(StateStore):
                def __init__(self, path):
                    super().__init__(path)
                    self.saved = []
                def save(self, state):
                    super().save(state)
                    self.saved.append(state)
            store = CapturingStateStore(str(Path(tmpdir) / "state.json"))
            clock_values = iter((deadline - 1, deadline))
            trader = Trader(
                client, live_test_config(), store,
                logging.getLogger("n15_deadline_after_journal"),
                clock_ms=lambda: next(clock_values),
            )
            plan = trader.build_breadth_recovery_margin_capped_trade_plan(
                "N15USDT", Decimal("100.8"), Decimal("99.8"), Decimal("5"),
                entry_min_price=Decimal("100.8"),
                entry_max_price=Decimal("101.3"),
                structure_id="n15-structure",
            )
            plan = replace(
                plan,
                entry_deadline_ms=deadline,
                structure_context={
                    "strategy_id": "N15",
                    "structure_id": "n15-structure",
                },
            )
            with self.assertRaises(EntryWindowExpiredError):
                trader.open_long_plan_with_protection(plan)
            self.assertEqual(client.market_calls, [])
            self.assertIsNone(store.load())
            self.assertEqual(len(store.saved), 1)
            first = store.saved[0]
            self.assertEqual(first.orders["strategy"]["strategy_id"], "N15")
            self.assertEqual(
                first.orders["execution_pending"]["phase"],
                "MARKET_ORDER_SUBMITTING",
            )

    def test_first_success_state_is_already_owned_by_n15(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"status": "FILLED", "avgPrice": "100.8", "executedQty": "1"},
                leverage=50,
            )
            store = StateStore(str(Path(tmpdir) / "state.json"))
            trader = Trader(
                client, live_test_config(), store,
                logging.getLogger("n15_success_ownership"),
            )
            plan = trader.build_breadth_recovery_margin_capped_trade_plan(
                "N15USDT", Decimal("100.8"), Decimal("99.8"), Decimal("5"),
                entry_min_price=Decimal("100.8"),
                entry_max_price=Decimal("101.3"),
                structure_id="n15-success",
            )
            client.open_response["executedQty"] = str(plan.quantity)
            client.position_quantities = [str(plan.quantity)]
            plan = replace(
                plan,
                structure_context={
                    "strategy_id": "N15",
                    "structure_id": "n15-success",
                    "quote_volume_rank": 7,
                    "candidate_universe": "quote_volume_top_frozen_n15",
                },
            )
            state = trader.open_long_plan_with_protection(plan)
            self.assertEqual(state.orders["strategy"]["strategy_id"], "N15")
            self.assertEqual(
                store.load().orders["strategy"]["strategy_id"], "N15"
            )
            self.assertEqual(
                store.load().orders["plan"]["structure_context"]["quote_volume_rank"],
                7,
            )

    def test_main_plan_uses_frozen_entry_p_and_deadline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, sched = scheduler(tmpdir)
            items, raws, checked = full_batch()
            scan_id = recorder.begin_scan(len(items), items, True)
            self.assertIsNotNone(scan_id)
            signal = sched.evaluate(scan_id, {"quote_volume_top": items}, raws, checked_at_ms=checked).passed_signals[0]
            base = TradePlan(
                symbol=signal.candidate.symbol, leverage=20, quantity=Decimal("1"),
                entry_price=Decimal("100.8"), stop_loss_price=Decimal("99.79"),
                take_profit_price=Decimal("105.85"), stop_loss_pct=Decimal("0.01"),
                take_profit_pct=Decimal("0.05"), amplitude_24h_pct=Decimal("0"),
                high_24h_price=Decimal("0"), low_24h_price=Decimal("0"),
                risk_amount=Decimal("1"), notional_value=Decimal("100"),
                required_margin=Decimal("5"), balance=Decimal("1000"),
                stop_mode="breadth_recovery_margin_capped",
            )
            class FakeTrader:
                def build_breadth_recovery_margin_capped_trade_plan(self, *args, **kwargs):
                    self.args = args, kwargs
                    return replace(base, entry_min_price=kwargs["entry_min_price"], entry_max_price=kwargs["entry_max_price"])
            bot = TradingBot.__new__(TradingBot)
            bot.trader = FakeTrader()
            plan = bot._build_strategy_plan(signal)
            structure = signal.analysis.structure
            self.assertEqual(bot.trader.args[0][1], structure.entry.close)
            self.assertEqual(bot.trader.args[0][2], structure.p)
            self.assertEqual(plan.entry_deadline_ms, structure.entry.open_time_ms + 120_000)
            self.assertEqual(plan.structure_context["strategy_id"], "N15")
            self.assertEqual(
                plan.structure_context["quote_volume_rank"],
                signal.candidate.quote_volume_rank,
            )

    def test_stop_range_five_r_low_leverage_and_actual_fill_cleanup(self):
        entry_min = Decimal("100.8")
        entry_max = Decimal("101.3")
        allowed_min = entry_min * Decimal("0.995")
        allowed_max = entry_max * Decimal("1.005")
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                RuleConstrainedClient(leverage=10), test_config(str(Path(tmpdir) / "account.json")),
                StateStore(str(Path(tmpdir) / "state.json")), logging.getLogger("n15_trader"),
            )
            plan = trader.build_breadth_recovery_margin_capped_trade_plan(
                "N15USDT", Decimal("100.8"), Decimal("99.8"), Decimal("5"),
                entry_min_price=entry_min, entry_max_price=entry_max,
            )
            trader._assert_structured_actual_entry_range(plan, allowed_min)
            trader._assert_structured_actual_entry_range(plan, allowed_max)
            self.assertLessEqual(plan.actual_risk_amount, plan.target_risk_amount)
            self.assertGreaterEqual(
                (plan.take_profit_price - plan.entry_price) / (plan.entry_price - plan.stop_loss_price),
                Decimal("5"),
            )
        for actual_entry in (
            allowed_min - Decimal("0.0001"),
            allowed_max + Decimal("0.0001"),
        ):
            with self.subTest(actual_entry=actual_entry), tempfile.TemporaryDirectory() as tmpdir:
                client = FakeLiveExecutionClient(
                    {"status": "FILLED", "avgPrice": str(actual_entry), "executedQty": "10"},
                    leverage=50, position_quantities=["10", "0"],
                    close_side_effects=[{"status": "FILLED", "executedQty": "10"}],
                )
                trader = Trader(client, live_test_config(), StateStore(str(Path(tmpdir) / "state.json")), logging.getLogger("n15_fill"))
                plan = trader.build_breadth_recovery_margin_capped_trade_plan(
                    "N15USDT", Decimal("100.8"), Decimal("99.8"), Decimal("5"),
                    entry_min_price=entry_min, entry_max_price=entry_max,
                )
                client.open_response["executedQty"] = str(plan.quantity)
                client.position_quantities = [str(plan.quantity), "0"]
                client.close_side_effects = [{"status": "FILLED", "executedQty": str(plan.quantity)}]
                with self.assertRaisesRegex(BinanceAPIError, "N15_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE"):
                    trader.open_long_plan_with_protection(plan)
                self.assertEqual(client.protection_calls, [])
                self.assertEqual(len(client.close_calls), 1)


if __name__ == "__main__":
    unittest.main()
