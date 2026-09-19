from __future__ import annotations

from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests.recorder_test_utils import make_test_recorder
from tests.test_n17 import BASE_TIME, INTERVAL_MS, n17_fixture
from tests.test_n18 import BASE_TIME as N18_BASE_TIME, n18_fixture
from tests.test_n19 import analyze as analyze_n19, n19_fixture
from trading_bot.monitor import FundingCandidate
from trading_bot.n17_analyzer import analyze_n17_range_support_rebound
from trading_bot.n18_analyzer import analyze_n18_ascending_triangle_breakout
from trading_bot.strategy_scheduler import (
    N20BatchContext,
    StrategyScheduler,
    StrategySignalDecision,
)
import trading_bot.strategy_scheduler as scheduler_module
from trading_bot.strategies import (
    N17_STRATEGY,
    N18_STRATEGY,
    N19_STRATEGY,
    load_all_strategies,
)


CURRENT_SYMBOLS = tuple(f"T{rank:03d}USDT" for rank in range(1, 101))
N17_OFFBOARD = ("LAUSDT", "REUSDT", "ZHIPUUSDT")
N18_OFFBOARD = ("ZHIPUUSDT",)


def _candidates() -> list[FundingCandidate]:
    return [
        FundingCandidate(
            symbol,
            None,
            Decimal("100"),
            quote_volume=Decimal("1000000"),
            quote_volume_rank=rank,
            candidate_universe="quote_volume_top",
        )
        for rank, symbol in enumerate(CURRENT_SYMBOLS, start=1)
    ]


def _shift_to_current_open_time(
    rows: list[list[str | int]],
    current_open_time_ms: int,
) -> list[list[str | int]]:
    shifted = deepcopy(rows)
    delta = current_open_time_ms - int(shifted[-1][0])
    for row in shifted:
        row[0] = int(row[0]) + delta
        if type(row[6]) is int and row[6] > 0:
            row[6] += delta
    return shifted


def _n18_rows_on_shared_axis(
    current_open_time_ms: int = BASE_TIME + 121 * INTERVAL_MS,
) -> list[list[str | int]]:
    rows = deepcopy(n18_fixture())
    shift = BASE_TIME - N18_BASE_TIME
    for row in rows:
        row[0] = int(row[0]) + shift
    return _shift_to_current_open_time(rows, current_open_time_ms)


def _raw_by_symbol(
    current_open_time_ms: int = BASE_TIME + 121 * INTERVAL_MS,
) -> dict[str, list[list[str | int]]]:
    ordinary = _shift_to_current_open_time(
        n17_fixture(), current_open_time_ms
    )
    raw = {symbol: deepcopy(ordinary) for symbol in CURRENT_SYMBOLS}
    raw.update({symbol: deepcopy(ordinary) for symbol in N17_OFFBOARD})
    raw["ZHIPUUSDT"] = _n18_rows_on_shared_axis(current_open_time_ms)
    return raw


def _install_offboard_states(
    recorder,
    current_open_time_ms: int = BASE_TIME + 121 * INTERVAL_MS,
) -> dict[tuple[str, str], str]:
    state_hashes: dict[tuple[str, str], str] = {}
    n17_rows = _shift_to_current_open_time(
        n17_fixture(), current_open_time_ms
    )
    for rank, symbol in enumerate(N17_OFFBOARD, start=7):
        analysis = analyze_n17_range_support_rebound(
            symbol,
            n17_rows,
            quote_volume_rank=rank,
            checked_at_ms=current_open_time_ms + 60_000,
        )
        if analysis.state_record is None:
            raise AssertionError("N17 offboard fixture did not form a state")
        if recorder.record_n17_state(analysis.state_record) != "INSERTED":
            raise AssertionError("N17 offboard state was not inserted")
        state_hashes[("N17", symbol)] = analysis.state_record.evidence_sha256
    rows = _n18_rows_on_shared_axis(current_open_time_ms)
    analysis = analyze_n18_ascending_triangle_breakout(
        "ZHIPUUSDT",
        rows,
        quote_volume_rank=10,
        checked_at_ms=current_open_time_ms + 1_000,
    )
    if analysis.state_record is None:
        raise AssertionError("N18 offboard fixture did not form a state")
    if recorder.record_n18_state(analysis.state_record) != "INSERTED":
        raise AssertionError("N18 offboard state was not inserted")
    state_hashes[("N18", "ZHIPUUSDT")] = analysis.state_record.evidence_sha256
    return state_hashes


def _evaluate_round(
    scheduler: StrategyScheduler,
    scan_id: int,
    candidates: list[FundingCandidate],
    raw_by_symbol: dict[str, list[list[str | int]]],
    evaluated_offboard: list[tuple[str, str]],
    fault: tuple[str, str, str] | None = None,
):
    def reject(strategy, candidate, raw, _checked_at_ms, *arguments):
        proposals = arguments[14]
        if strategy.strategy_id in {"N17", "N18", "N19"}:
            proposals.append(
                scheduler._history_coverage_proposal(
                    strategy.strategy_id,
                    candidate.symbol,
                    raw[candidate.symbol],
                    122,
                )
            )
        if candidate.symbol not in CURRENT_SYMBOLS:
            evaluated_offboard.append((strategy.strategy_id, candidate.symbol))
        reason = (
            "N18_STRUCTURE_CONSUMED"
            if strategy.strategy_id == "N18"
            else f"{strategy.strategy_id}_STRUCTURE_NOT_FOUND"
        )
        if fault is not None and (strategy.strategy_id, candidate.symbol) == (
            fault[0],
            fault[1],
        ):
            reason = fault[2]
        return scheduler._rejected(strategy, candidate, reason)

    deferred = {
        symbol: "N20_MARKET_CONTEXT_DEFERRED_NEW_MEMBER"
        for symbol in CURRENT_SYMBOLS
    }
    with patch.object(
        scheduler,
        "_build_n12_batch_context",
        return_value=SimpleNamespace(),
    ), patch.object(
        scheduler,
        "_build_n13_batch_context",
        return_value=SimpleNamespace(complete=False),
    ), patch.object(
        scheduler,
        "_build_n14_batch_context",
        return_value=SimpleNamespace(current_open_time_ms=None, snapshots={}),
    ), patch.object(
        scheduler,
        "_build_n15_batch_context",
        return_value=SimpleNamespace(complete=False, snapshot=None),
    ), patch.object(
        scheduler,
        "_build_n20_batch_context",
        return_value=N20BatchContext(None, True, deferred_reasons=deferred),
    ), patch.object(
        scheduler, "_probe_n14_candidate", return_value=None
    ), patch.object(
        scheduler, "_evaluate_candidate", side_effect=reject
    ):
        return scheduler.evaluate(
            scan_id,
            {
                "quote_volume_top": candidates,
                "negative_funding": [],
            },
            raw_by_symbol,
            checked_at_ms=BASE_TIME + 121 * INTERVAL_MS + 60_000,
            micro_windows={},
        )


class OffboardOrdinarySignalBoundaryTests(unittest.TestCase):
    def _new_runtime(self, directory: str):
        recorder = make_test_recorder(
            Path(directory) / "review.sqlite3",
            logging.getLogger("offboard-ordinary-boundary"),
        )
        strategies = tuple(
            strategy
            for strategy in load_all_strategies()
            if 6 <= int(strategy.strategy_id[1:]) <= 25
        )
        recorder.upsert_strategy_definitions(load_all_strategies())
        return recorder, StrategyScheduler(
            strategies, 96, recorder, logging.getLogger()
        )

    def test_two_rounds_keep_offboard_lifecycle_but_publish_exact_top100(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder, scheduler = self._new_runtime(directory)
            original_state_hashes = _install_offboard_states(recorder)
            candidates = _candidates()
            raw_by_symbol = _raw_by_symbol()
            evaluated_offboard: list[tuple[str, str]] = []
            for expected_scan in (1, 2):
                scan_id = recorder.begin_scan(854, candidates, dry_run=True)
                self.assertEqual(scan_id, expected_scan)
                result = _evaluate_round(
                    scheduler,
                    scan_id,
                    candidates,
                    raw_by_symbol,
                    evaluated_offboard,
                )
                self.assertTrue(result.signal_batch_published)
                self.assertEqual(len(result.signals), 2_000)
                self.assertEqual(result.passed_signals, [])
                self.assertEqual(result.live_candidates, [])
                with closing(sqlite3.connect(recorder.db_file)) as connection:
                    counts = dict(
                        connection.execute(
                            "SELECT strategy_id,COUNT(*) FROM strategy_signals "
                            "WHERE scan_id=? GROUP BY strategy_id",
                            (scan_id,),
                        )
                    )
                    self.assertEqual(
                        counts,
                        {
                            f"N{number:02d}": 100
                            for number in range(6, 26)
                        },
                    )
                    self.assertEqual(sum(counts.values()), 2_000)
                    offboard_signals = connection.execute(
                        "SELECT strategy_id,symbol FROM strategy_signals "
                        "WHERE scan_id=? AND symbol NOT IN (%s) "
                        "ORDER BY strategy_id,symbol"
                        % ",".join("?" for _ in CURRENT_SYMBOLS),
                        (scan_id, *CURRENT_SYMBOLS),
                    ).fetchall()
                    self.assertEqual(offboard_signals, [])
                    for (strategy_id, symbol), evidence_sha in (
                        original_state_hashes.items()
                    ):
                        table = (
                            "n17_range_support_states"
                            if strategy_id == "N17"
                            else "n18_triangle_states"
                        )
                        self.assertEqual(
                            connection.execute(
                                f"SELECT evidence_sha256 FROM {table} "
                                "WHERE strategy_id=? AND symbol=?",
                                (strategy_id, symbol),
                            ).fetchone(),
                            (evidence_sha,),
                        )
                        self.assertEqual(
                            connection.execute(
                                "SELECT COUNT(*) FROM "
                                "history_coverage_publication_receipts "
                                "WHERE strategy_id=? AND symbol=?",
                                (strategy_id, symbol),
                            ).fetchone(),
                            (1,),
                        )
            self.assertEqual(
                set(evaluated_offboard),
                {
                    ("N17", "LAUSDT"),
                    ("N17", "REUSDT"),
                    ("N17", "ZHIPUUSDT"),
                    ("N18", "ZHIPUUSDT"),
                },
            )

    def test_top100_global_cooldown_keeps_n17_n19_coverage_and_publishes(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("top100-global-cooldown-coverage"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            scheduler = StrategyScheduler(
                tuple(
                    strategy
                    for strategy in load_all_strategies()
                    if strategy.strategy_id in {"N17", "N18", "N19"}
                ),
                96,
                recorder,
                logging.getLogger("top100-global-cooldown-coverage"),
            )
            candidates = _candidates()
            raw_by_symbol = {
                symbol: deepcopy(n17_fixture()) for symbol in CURRENT_SYMBOLS
            }
            cooldown_symbol = CURRENT_SYMBOLS[58]
            self.assertTrue(
                recorder.set_symbol_cooldown(
                    cooldown_symbol,
                    datetime.now(timezone.utc) + timedelta(hours=4),
                    "STOP_LOSS",
                    6,
                )
            )

            for expected_scan in (1, 2):
                scan_id = recorder.begin_scan(854, candidates, dry_run=True)
                self.assertEqual(scan_id, expected_scan)
                result = scheduler.evaluate(
                    scan_id,
                    {
                        "quote_volume_top": candidates,
                        "negative_funding": [],
                    },
                    raw_by_symbol,
                    checked_at_ms=BASE_TIME + 121 * INTERVAL_MS + 60_000,
                    micro_windows={},
                )
                self.assertTrue(result.signal_batch_published)
                self.assertEqual(len(result.signals), 300)
                cooldown_signals = [
                    signal
                    for signal in result.signals
                    if signal.candidate.symbol == cooldown_symbol
                ]
                self.assertEqual(
                    {signal.strategy.strategy_id for signal in cooldown_signals},
                    {"N17", "N18", "N19"},
                )
                self.assertTrue(
                    all(
                        signal.reason.startswith(
                            "GLOBAL_SYMBOL_COOLDOWN_UNTIL:"
                        )
                        and not signal.passed
                        for signal in cooldown_signals
                    )
                )
                self.assertNotIn(
                    cooldown_symbol,
                    {
                        signal.candidate.symbol
                        for signal in result.passed_signals
                    },
                )
                self.assertNotIn(
                    cooldown_symbol,
                    {
                        candidate.signal.candidate.symbol
                        for candidate in result.live_candidates
                    },
                )
                with closing(sqlite3.connect(recorder.db_file)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT strategy_id,COUNT(*) FROM strategy_signals "
                            "WHERE scan_id=? GROUP BY strategy_id "
                            "ORDER BY strategy_id",
                            (scan_id,),
                        ).fetchall(),
                        [("N17", 100), ("N18", 100), ("N19", 100)],
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT strategy_id,COUNT(*) FROM "
                            "history_coverage_publication_receipts "
                            "WHERE source_scan_id=? GROUP BY strategy_id "
                            "ORDER BY strategy_id",
                            (scan_id,),
                        ).fetchall(),
                        (
                            [("N17", 100), ("N18", 100), ("N19", 100)]
                            if expected_scan == 1
                            else []
                        ),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT strategy_id,COUNT(*) FROM "
                            "history_coverage_epoch_heads "
                            "GROUP BY strategy_id ORDER BY strategy_id"
                        ).fetchall(),
                        [("N17", 100), ("N18", 100), ("N19", 100)],
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_signals "
                            "WHERE scan_id=? AND symbol=? AND passed=1",
                            (scan_id, cooldown_symbol),
                        ).fetchone(),
                        (0,),
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

    def test_strategy_symbol_cooldown_is_deferred_until_coverage_is_ready(self):
        cases = (
            (N17_STRATEGY, "N17_STRATEGY", n17_fixture()),
            (N18_STRATEGY, "N18_STRATEGY", _n18_rows_on_shared_axis()),
            (N19_STRATEGY, "N19_STRATEGY", n19_fixture()),
        )
        for original, module_name, rows in cases:
            with self.subTest(strategy_id=original.strategy_id), tempfile.TemporaryDirectory() as directory:
                recorder = make_test_recorder(
                    Path(directory) / "review.sqlite3",
                    logging.getLogger("strategy-symbol-cooldown-coverage"),
                )
                recorder.upsert_strategy_definitions(load_all_strategies())
                strategy = replace(original, loss_symbol_cooldown_hours=1)
                scheduler = StrategyScheduler(
                    (strategy,), 96, recorder, logging.getLogger()
                )
                symbol = "COOLDOWNUSDT"
                candidate = FundingCandidate(
                    symbol,
                    None,
                    Decimal("100"),
                    quote_volume=Decimal("1000000"),
                    quote_volume_rank=1,
                    candidate_universe="quote_volume_top",
                )
                raw_by_symbol = {symbol: deepcopy(rows)}
                market_symbols: tuple[str, ...] = ()
                if original.strategy_id == "N19":
                    market_symbols = CURRENT_SYMBOLS
                    raw_by_symbol.update(
                        {
                            item: deepcopy(rows)
                            for item in CURRENT_SYMBOLS
                        }
                    )
                proposals = []
                with patch.object(
                    scheduler_module, module_name, strategy
                ), patch.object(
                    recorder,
                    "active_strategy_symbol_cooldown",
                    return_value="2030-01-01T00:00:00+00:00",
                ) as cooldown_read:
                    decision = scheduler._evaluate_candidate(
                        strategy,
                        candidate,
                        raw_by_symbol,
                        BASE_TIME + 121 * INTERVAL_MS + 60_000,
                        None,
                        None,
                        n17_states_by_symbol={},
                        n19_states_by_symbol={},
                        n19_market_symbols=market_symbols,
                        n18_states_by_symbol={},
                        history_coverage_proposals=proposals,
                        global_cooldowns={},
                        global_cooldown_scope=frozenset(raw_by_symbol),
                        n19_market_context_cache={},
                        history_source_presence={
                            item: frozenset() for item in raw_by_symbol
                        },
                    )
                self.assertEqual(cooldown_read.call_count, 1)
                self.assertFalse(decision.passed)
                self.assertEqual(
                    decision.reason,
                    "SYMBOL_COOLDOWN_UNTIL:2030-01-01T00:00:00+00:00",
                )
                self.assertEqual(
                    [(proposal.strategy_id, proposal.symbol) for proposal in proposals],
                    [(original.strategy_id, symbol)],
                )

    def test_recorder_cooldown_proposal_owner_contract_is_exact(self):
        candidates = _candidates()
        source = n17_fixture()
        symbol = CURRENT_SYMBOLS[58]
        base_proposal = StrategyScheduler._history_coverage_proposal(
            "N17", symbol, source, 122
        )
        cases = {
            "missing": (),
            "duplicate": (base_proposal, base_proposal),
            "forged_current_owner": (
                base_proposal,
                replace(
                    base_proposal,
                    symbol=CURRENT_SYMBOLS[59],
                    source_sha256="f" * 64,
                ),
            ),
            "valid": (base_proposal,),
        }
        for name, proposals in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                recorder = make_test_recorder(
                    Path(directory) / "review.sqlite3",
                    logging.getLogger("cooldown-proposal-owner-contract"),
                )
                recorder.upsert_strategy_definitions(load_all_strategies())
                scan_id = recorder.begin_scan(854, candidates, dry_run=True)
                self.assertIsInstance(
                    recorder.record_strategy_signal(
                        scan_id,
                        "N17",
                        symbol,
                        "",
                        (),
                        "",
                        False,
                        False,
                        "REJECTED",
                        "GLOBAL_SYMBOL_COOLDOWN_UNTIL:"
                        "2030-01-01T00:00:00+00:00",
                    ),
                    int,
                )
                published = recorder.publish_strategy_signal_batch(
                    scan_id, 1, proposals
                )
                self.assertEqual(published, name == "valid")
                with closing(sqlite3.connect(recorder.db_file)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT current_scan_id FROM "
                            "strategy_signal_current WHERE singleton_id=1"
                        ).fetchone(),
                        (scan_id if name == "valid" else None,),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM "
                            "history_coverage_publication_receipts"
                        ).fetchone(),
                        (1 if name == "valid" else 0,),
                    )
                    self.assertEqual(
                        tuple(
                            connection.execute(
                                f"SELECT COUNT(*) FROM {table}"
                            ).fetchone()[0]
                            for table in (
                                "strategy_paper_trades",
                                "strategy_live_links",
                                "trade_reviews",
                            )
                        ),
                        (0, 0, 0),
                    )

    def test_offboard_integrity_failures_block_round_and_next_scan_recovers(self):
        faults = (
            ("N17", "LAUSDT", "N17_STATE_READ_FAILED"),
            ("N17", "REUSDT", "N17_STATE_PERSIST_FAILED"),
            ("N17", "ZHIPUUSDT", "N17_FROZEN_EVIDENCE_INVALID"),
            ("N18", "ZHIPUUSDT", "N18_STATE_INCONSISTENT"),
        )
        for fault in faults:
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                recorder = make_test_recorder(
                    Path(directory) / "review.sqlite3",
                    logging.getLogger("offboard-integrity-boundary"),
                )
                strategies = tuple(
                    strategy
                    for strategy in load_all_strategies()
                    if 6 <= int(strategy.strategy_id[1:]) <= 25
                )
                recorder.upsert_strategy_definitions(load_all_strategies())
                original_state_hashes = _install_offboard_states(recorder)
                scheduler = StrategyScheduler(
                    strategies, 96, recorder, logging.getLogger()
                )
                candidates = _candidates()
                raw_by_symbol = _raw_by_symbol()
                evaluated: list[tuple[str, str]] = []

                first_scan = recorder.begin_scan(854, candidates, dry_run=True)
                first = _evaluate_round(
                    scheduler,
                    first_scan,
                    candidates,
                    raw_by_symbol,
                    evaluated,
                )
                self.assertTrue(first.signal_batch_published)
                self.assertEqual(len(first.signals), 2_000)

                with closing(sqlite3.connect(recorder.db_file)) as connection:
                    protected_before = tuple(
                        connection.execute(
                            f"SELECT COUNT(*) FROM {table}"
                        ).fetchone()[0]
                        for table in (
                            "strategy_paper_trades",
                            "strategy_live_links",
                            "trade_reviews",
                            "events",
                            "strategy_passed_signal_audits",
                            "strategy_passed_structure_ledger",
                        )
                    )
                    receipts_before = connection.execute(
                        "SELECT COUNT(*) FROM "
                        "history_coverage_publication_receipts"
                    ).fetchone()[0]

                failed_scan = recorder.begin_scan(854, candidates, dry_run=True)
                failed = _evaluate_round(
                    scheduler,
                    failed_scan,
                    candidates,
                    raw_by_symbol,
                    evaluated,
                    fault,
                )
                self.assertFalse(failed.signal_batch_published)
                self.assertEqual(failed.passed_signals, [])
                self.assertEqual(failed.live_candidates, [])
                self.assertIn(
                    fault,
                    {
                        (item.strategy_id, item.symbol, item.code)
                        for item in failed.signal_audit_failures
                    },
                )
                with closing(sqlite3.connect(recorder.db_file)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT current_scan_id FROM "
                            "strategy_signal_current WHERE singleton_id=1"
                        ).fetchone(),
                        (first_scan,),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT scan_id,state,recorded_count,expected_count "
                            "FROM strategy_signal_batches ORDER BY scan_id"
                        ).fetchall(),
                        [
                            (first_scan, "CURRENT", 2_000, 2_000),
                            (failed_scan, "STAGING", 2_000, None),
                        ],
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_signals "
                            "WHERE scan_id=? AND symbol IN (?,?,?)",
                            (failed_scan, *N17_OFFBOARD),
                        ).fetchone(),
                        (0,),
                    )
                    self.assertEqual(
                        tuple(
                            connection.execute(
                                f"SELECT COUNT(*) FROM {table}"
                            ).fetchone()[0]
                            for table in (
                                "strategy_paper_trades",
                                "strategy_live_links",
                                "trade_reviews",
                                "events",
                                "strategy_passed_signal_audits",
                                "strategy_passed_structure_ledger",
                            )
                        ),
                        protected_before,
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM "
                            "history_coverage_publication_receipts"
                        ).fetchone(),
                        (receipts_before,),
                    )
                    for (strategy_id, symbol), evidence_sha in (
                        original_state_hashes.items()
                    ):
                        table = (
                            "n17_range_support_states"
                            if strategy_id == "N17"
                            else "n18_triangle_states"
                        )
                        self.assertEqual(
                            connection.execute(
                                f"SELECT evidence_sha256 FROM {table} "
                                "WHERE strategy_id=? AND symbol=?",
                                (strategy_id, symbol),
                            ).fetchone(),
                            (evidence_sha,),
                        )

                recovered_scan = recorder.begin_scan(
                    854, candidates, dry_run=True
                )
                recovered = _evaluate_round(
                    scheduler,
                    recovered_scan,
                    candidates,
                    raw_by_symbol,
                    evaluated,
                )
                self.assertTrue(recovered.signal_batch_published)
                self.assertEqual(len(recovered.signals), 2_000)
                with closing(sqlite3.connect(recorder.db_file)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT scan_id,state,recorded_count,expected_count "
                            "FROM strategy_signal_batches"
                        ).fetchall(),
                        [(recovered_scan, "CURRENT", 2_000, 2_000)],
                    )

    def test_recorder_rejects_the_old_2004_signal_shape_before_current_moves(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder, scheduler = self._new_runtime(directory)
            state_hashes = _install_offboard_states(recorder)
            candidates = _candidates()
            raw_by_symbol = _raw_by_symbol()
            scan_id = recorder.begin_scan(854, candidates, dry_run=True)
            with patch.object(
                scheduler_module,
                "_OFFBOARD_LIFECYCLE_ONLY_STRATEGIES",
                frozenset(),
            ):
                result = _evaluate_round(
                    scheduler, scan_id, candidates, raw_by_symbol, []
                )
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(len(result.signals), 2_004)
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id FROM strategy_signal_current "
                        "WHERE singleton_id=1"
                    ).fetchone(),
                    (None,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT state,recorded_count,expected_count FROM "
                        "strategy_signal_batches WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    ("STAGING", 2_004, None),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM "
                        "history_coverage_publication_receipts"
                    ).fetchone(),
                    (0,),
                )
                for (strategy_id, symbol), evidence_sha in state_hashes.items():
                    table = (
                        "n17_range_support_states"
                        if strategy_id == "N17"
                        else "n18_triangle_states"
                    )
                    self.assertEqual(
                        connection.execute(
                            f"SELECT evidence_sha256 FROM {table} "
                            "WHERE strategy_id=? AND symbol=?",
                            (strategy_id, symbol),
                        ).fetchone(),
                        (evidence_sha,),
                    )

    def test_offboard_coverage_requires_a_real_durable_lifecycle_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder, scheduler = self._new_runtime(directory)
            candidates = _candidates()
            raw_by_symbol = _raw_by_symbol()
            fake_n17 = tuple(
                SimpleNamespace(
                    strategy_id="N17",
                    symbol=symbol,
                    quote_volume_rank=rank,
                    structure_id="f" * 24,
                    family_id="e" * 24,
                )
                for rank, symbol in enumerate(N17_OFFBOARD, start=7)
            )
            fake_n18 = (
                SimpleNamespace(
                    strategy_id="N18",
                    symbol="ZHIPUUSDT",
                    quote_volume_rank=10,
                    structure_id="d" * 24,
                    family_id="c" * 24,
                    episode_id="c" * 24,
                ),
            )
            scan_id = recorder.begin_scan(854, candidates, dry_run=True)
            with patch.object(
                recorder, "get_active_n17_states", return_value=fake_n17
            ), patch.object(
                recorder,
                "get_latest_n17_states",
                return_value={item.symbol: item for item in fake_n17},
            ), patch.object(
                recorder, "get_active_n18_states", return_value=fake_n18
            ), patch.object(
                recorder,
                "get_latest_n18_states",
                return_value={item.symbol: item for item in fake_n18},
            ):
                result = _evaluate_round(
                    scheduler, scan_id, candidates, raw_by_symbol, []
                )
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(len(result.signals), 2_000)
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT current_scan_id FROM strategy_signal_current "
                        "WHERE singleton_id=1"
                    ).fetchone(),
                    (None,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM "
                        "history_coverage_publication_receipts"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT state,recorded_count,expected_count FROM "
                        "strategy_signal_batches WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    ("STAGING", 2_000, None),
                )

    def test_n19_offboard_state_is_evaluated_and_receipted_without_a_signal(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("n19-offboard-ordinary-boundary"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            n19_state = analyze_n19().state_record
            self.assertIsNotNone(n19_state)
            self.assertEqual(recorder.record_n19_state(n19_state), "INSERTED")
            scheduler = StrategyScheduler(
                tuple(
                    strategy
                    for strategy in load_all_strategies()
                    if strategy.strategy_id == "N19"
                ),
                96,
                recorder,
                logging.getLogger("n19-offboard-ordinary-boundary"),
            )
            candidates = _candidates()
            raw_by_symbol = {
                symbol: deepcopy(n19_fixture()) for symbol in CURRENT_SYMBOLS
            }
            raw_by_symbol[n19_state.symbol] = deepcopy(n19_fixture())
            evaluated: list[tuple[str, str]] = []
            scan_id = recorder.begin_scan(854, candidates, dry_run=True)
            result = _evaluate_round(
                scheduler, scan_id, candidates, raw_by_symbol, evaluated
            )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(len(result.signals), 100)
            self.assertNotIn(
                n19_state.symbol,
                {item.candidate.symbol for item in result.signals},
            )
            self.assertIn(("N19", n19_state.symbol), evaluated)
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    (100,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id=? AND symbol=?",
                        (scan_id, n19_state.symbol),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM "
                        "history_coverage_publication_receipts "
                        "WHERE strategy_id='N19' AND symbol=?",
                        (n19_state.symbol,),
                    ).fetchone(),
                    (1,),
                )

    def test_offboard_passed_setup_cannot_take_the_current_top100_representative(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("offboard-representative-boundary"),
            )
            recorder.upsert_strategy_definitions(load_all_strategies())
            state_hashes = _install_offboard_states(recorder)
            current_open_time_ms = BASE_TIME + 122 * INTERVAL_MS
            n17_rows = _shift_to_current_open_time(
                n17_fixture(), current_open_time_ms
            )
            current_analysis = analyze_n17_range_support_rebound(
                CURRENT_SYMBOLS[0],
                n17_rows,
                quote_volume_rank=100,
                checked_at_ms=current_open_time_ms + 60_000,
            )
            offboard_analysis = analyze_n17_range_support_rebound(
                N17_OFFBOARD[0],
                n17_rows,
                quote_volume_rank=1,
                checked_at_ms=current_open_time_ms + 60_000,
            )
            self.assertTrue(current_analysis.passed)
            self.assertTrue(offboard_analysis.passed)
            self.assertEqual(
                recorder.record_n17_state(current_analysis.state_record),
                "INSERTED",
            )
            scheduler = StrategyScheduler(
                tuple(
                    strategy
                    for strategy in load_all_strategies()
                    if strategy.strategy_id == "N17"
                ),
                96,
                recorder,
                logging.getLogger("offboard-representative-boundary"),
            )
            candidates = _candidates()
            raw_by_symbol = _raw_by_symbol(current_open_time_ms)

            def coverage_only_reject(
                strategy, candidate, raw, _checked_at_ms, *arguments
            ):
                arguments[14].append(
                    scheduler._history_coverage_proposal(
                        "N17", candidate.symbol, raw[candidate.symbol], 122
                    )
                )
                return scheduler._rejected(
                    strategy, candidate, "N17_STRUCTURE_NOT_FOUND"
                )

            baseline_scan_id = recorder.begin_scan(
                854, candidates, dry_run=True
            )
            with patch.object(
                scheduler,
                "_evaluate_candidate",
                side_effect=coverage_only_reject,
            ):
                baseline = scheduler.evaluate(
                    baseline_scan_id,
                    {"quote_volume_top": candidates, "negative_funding": []},
                    raw_by_symbol,
                    checked_at_ms=current_open_time_ms + 60_000,
                )
            self.assertTrue(baseline.signal_batch_published)
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                receipt_rows_before = connection.execute(
                    "SELECT * FROM history_coverage_publication_receipts "
                    "WHERE strategy_id='N17' AND symbol IN (?,?,?) "
                    "ORDER BY symbol,source_scan_id",
                    N17_OFFBOARD,
                ).fetchall()
                head_rows_before = connection.execute(
                    "SELECT * FROM history_coverage_epoch_heads "
                    "WHERE strategy_id='N17' AND symbol IN (?,?,?) "
                    "ORDER BY symbol",
                    N17_OFFBOARD,
                ).fetchall()
            self.assertEqual(len(receipt_rows_before), len(N17_OFFBOARD))
            self.assertEqual(len(head_rows_before), len(N17_OFFBOARD))

            def analyze_or_reject(
                strategy, candidate, raw, _checked_at_ms, *arguments
            ):
                arguments[14].append(
                    scheduler._history_coverage_proposal(
                        "N17", candidate.symbol, raw[candidate.symbol], 122
                    )
                )
                if candidate.symbol == CURRENT_SYMBOLS[0]:
                    return StrategySignalDecision(
                        strategy,
                        candidate,
                        current_analysis,
                        True,
                        "PASSED",
                        "PASSED",
                    )
                if candidate.symbol == N17_OFFBOARD[0]:
                    return StrategySignalDecision(
                        strategy,
                        candidate,
                        offboard_analysis,
                        True,
                        "PASSED",
                        "PASSED",
                    )
                return scheduler._rejected(
                    strategy, candidate, "N17_STRUCTURE_NOT_FOUND"
                )

            scan_id = recorder.begin_scan(854, candidates, dry_run=True)
            with patch.object(
                scheduler, "_evaluate_candidate", side_effect=analyze_or_reject
            ):
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": candidates, "negative_funding": []},
                    raw_by_symbol,
                    checked_at_ms=current_open_time_ms + 60_000,
                )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(len(result.signals), 100)
            self.assertEqual(
                [item.candidate.symbol for item in result.passed_signals],
                [CURRENT_SYMBOLS[0]],
            )
            self.assertEqual(
                [item.signal.candidate.symbol for item in result.live_candidates],
                [CURRENT_SYMBOLS[0]],
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT decision,reason FROM strategy_signals "
                        "WHERE scan_id=? AND strategy_id='N17' AND symbol=?",
                        (scan_id, CURRENT_SYMBOLS[0]),
                    ).fetchone(),
                    ("PASSED", "PASSED"),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id=? AND symbol=?",
                        (scan_id, N17_OFFBOARD[0]),
                    ).fetchone(),
                    (0,),
                )
                for (strategy_id, symbol), evidence_sha in state_hashes.items():
                    table = (
                        "n17_range_support_states"
                        if strategy_id == "N17"
                        else "n18_triangle_states"
                    )
                    self.assertEqual(
                        connection.execute(
                            f"SELECT evidence_sha256 FROM {table} "
                            "WHERE strategy_id=? AND symbol=?",
                            (strategy_id, symbol),
                        ).fetchone(),
                        (evidence_sha,),
                    )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM history_coverage_publication_receipts "
                        "WHERE strategy_id='N17' AND symbol IN (?,?,?) "
                        "ORDER BY symbol,source_scan_id",
                        N17_OFFBOARD,
                    ).fetchall(),
                    receipt_rows_before,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM history_coverage_epoch_heads "
                        "WHERE strategy_id='N17' AND symbol IN (?,?,?) "
                        "ORDER BY symbol",
                        N17_OFFBOARD,
                    ).fetchall(),
                    head_rows_before,
                )


if __name__ == "__main__":
    unittest.main()
