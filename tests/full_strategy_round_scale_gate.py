"""Production-shaped full scheduler-round performance release gate.

This gate is deliberately outside unittest discovery.  It combines the real
851-row market candidate builder, one shared 122-row Kline input for every
Top100 member, all enabled N01-N25 definitions, 2,000 ordinary decisions, a Review
database retaining 10,251 coverage receipts, and the real atomic batch
publisher.  ``--kline-delay-ms`` adds deterministic per-request latency through
the real bounded main-loop Kline fetcher, so the release gate measures the
network critical path without making external requests.
"""

from __future__ import annotations

import argparse
from contextlib import closing, ExitStack
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import resource
import sqlite3
import statistics
import sys
import tempfile
import threading
import time
from unittest.mock import patch

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from tests.authorized_legacy_witness_scale_gate import (  # noqa: E402
    _add_receipts,
    _inflate_review,
)
from tests.test_authorized_legacy_witness import (  # noqa: E402
    AuthorizedLegacyV3WitnessTests,
)
from tests.test_micro_strategies import (  # noqa: E402
    OPEN_TIME,
    authenticated_top100_observations,
    observation,
)
from tests.test_offboard_signal_boundary import (  # noqa: E402
    N17_OFFBOARD,
    N18_OFFBOARD,
    _install_offboard_states,
    _raw_by_symbol as _offboard_raw_by_symbol,
)
from tests.test_n20 import market_fixture  # noqa: E402
from trading_bot.monitor import FundingCandidate  # noqa: E402
from trading_bot.n15_snapshot import build_n15_snapshot  # noqa: E402
from trading_bot.micro_observation import (  # noqa: E402
    MicroObservationSampler,
)
from trading_bot.micro_analyzer import analyze_n21  # noqa: E402
from trading_bot.monitor import FundingMonitor  # noqa: E402
from trading_bot.main import TradingBot  # noqa: E402
from trading_bot.recorder import ReviewRecorder  # noqa: E402
from trading_bot.signal_retention import (  # noqa: E402
    inspect_authorized_legacy_v3_witness,
    install_authorized_legacy_v3_witness,
)
from trading_bot.strategies import N15_STRATEGY, load_all_strategies  # noqa: E402
import trading_bot.n19_analyzer as n19_module  # noqa: E402
import trading_bot.n20_analyzer as n20_module  # noqa: E402
import trading_bot.strategy_scheduler as scheduler_module  # noqa: E402
from trading_bot.strategy_scheduler import StrategyScheduler  # noqa: E402


class _MarketClient:
    def __init__(self, symbols: tuple[str, ...]):
        extras = tuple(f"X{index:04d}USDT" for index in range(751))
        self.symbols = symbols + extras

    def get_premium_index(self):
        return [
            {
                "symbol": symbol,
                "lastFundingRate": "0",
                "markPrice": "100",
                "indexPrice": "100",
            }
            for symbol in self.symbols
        ]

    def get_tradable_usdt_perpetual_symbols(self):
        return set(self.symbols)

    def get_24hr_tickers(self):
        return [
            {
                "symbol": symbol,
                "quoteVolume": str(1_000_000_000 - index),
                "lastPrice": "100",
            }
            for index, symbol in enumerate(self.symbols)
        ]


class _DelayedKlineClient:
    def __init__(self, rows_by_symbol, delay_seconds):
        self.rows_by_symbol = rows_by_symbol
        self.delay_seconds = delay_seconds
        self.lock = threading.Lock()
        self.calls = []
        self.active = 0
        self.max_active = 0

    def get_klines(self, symbol):
        with self.lock:
            self.calls.append(symbol)
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            if self.delay_seconds:
                time.sleep(self.delay_seconds)
            return self.rows_by_symbol[symbol]
        finally:
            with self.lock:
                self.active -= 1

def _symbols() -> tuple[str, ...]:
    return tuple(
        "BTCUSDT"
        if rank == 1
        else "ETHUSDT"
        if rank == 2
        else "龙虾USDT"
        if rank == 74
        else f"S{rank:03d}USDT"
        for rank in range(1, 101)
    )


def _raw_by_symbol(symbols: tuple[str, ...]):
    fixture_symbols, fixture = market_fixture(through=121)
    source_current = int(fixture[fixture_symbols[0]][-1][0])
    shift = OPEN_TIME - source_current
    result = {}
    for index, symbol in enumerate(symbols):
        rows = deepcopy(fixture[fixture_symbols[index]])
        for row in rows:
            row[0] = int(row[0]) + shift
            row[6] = int(row[6]) + shift
        result[symbol] = rows
    return result


def _micro_sample(
    symbols: tuple[str, ...],
    ordinal: int,
):
    sample = {}
    index = min(ordinal, 4) - 1
    extra = max(0, ordinal - 4)
    default_premium = str(
        Decimal(("-0.00012", "-0.00010", "-0.00008", "-0.00006")[index])
        + Decimal("0.00002") * extra
    )
    for rank, symbol in enumerate(symbols, start=1):
        values = {
            "close": "100",
            "high": "101",
            "low": "99",
            "quote": str(800 + ordinal * 100),
            "trades": 80 + ordinal * 10,
            "taker": str(400 + ordinal * 50),
            "premium": default_premium,
        }
        if rank == 7:
            values.update({
                "close": str(Decimal(
                    ("100", "100.05", "100.10", "100.30")[index]
                ) + Decimal("0.20") * extra),
                "high": str(Decimal("101") + Decimal("0.20") * extra),
                "low": "99.5",
                "quote": str(Decimal(
                    ("1000", "1100", "1200", "1300")[index]
                ) + Decimal("100") * extra),
                "trades": (100, 110, 120, 130)[index] + 10 * extra,
                "taker": str(Decimal(
                    ("500", "550", "602", "658")[index]
                ) + Decimal("58") * extra),
            })
        elif rank == 8:
            values.update({
                "close": str(Decimal(
                    ("100", "99.95", "100.05", "100.40")[index]
                ) + Decimal("0.30") * extra),
                "high": str(Decimal(
                    ("100.8", "100.8", "100.8", "101.1")[index]
                ) + Decimal("0.30") * extra),
                "low": "99.4",
                "quote": str(Decimal(
                    ("1000", "1100", "1225", "1381.25")[index]
                ) + Decimal("150") * extra),
                "trades": (100, 110, 120, 140)[index] + 20 * extra,
                "taker": str(Decimal(
                    ("500", "540", "605", "692.5")[index]
                ) + Decimal("90") * extra),
            })
        elif rank == 10:
            values.update({
                "close": str(Decimal(
                    ("100", "100", "99.95", "100.20")[index]
                ) + Decimal("0.20") * extra),
                "high": str(Decimal(
                    ("101", "101", "101", "101.1")[index]
                ) + Decimal("0.20") * extra),
                "low": "99.4",
                "quote": str(Decimal(
                    ("900", "1000", "1100", "1220")[index]
                ) + Decimal("120") * extra),
                "trades": (90, 100, 110, 125)[index] + 15 * extra,
                "taker": str(Decimal(
                    ("450", "500", "540", "610")[index]
                ) + Decimal("70") * extra),
                "premium": str(Decimal(
                    ("-0.00036", "-0.00030", "-0.00024", "-0.00018")[index]
                ) + Decimal("0.00006") * extra),
            })
        sample[symbol] = observation(
            symbol,
            ordinal,
            ordinal,
            OPEN_TIME + 1 + (ordinal - 1) * 60_000,
            rank=rank,
            **values,
        )
    return authenticated_top100_observations(sample)


def _timed_wrapper(name, target, totals, counts):
    def wrapped(*args, **kwargs):
        started = time.perf_counter()
        try:
            return target(*args, **kwargs)
        finally:
            totals[name] = totals.get(name, 0.0) + (
                time.perf_counter() - started
            )
            counts[name] = counts.get(name, 0) + 1

    return wrapped


def _instrument_attribute(stack, owner, name, totals, counts):
    target = getattr(owner, name)
    setattr(owner, name, _timed_wrapper(name, target, totals, counts))
    stack.callback(setattr, owner, name, target)


def _rss_peak_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


class _ResourceProbe:
    """Bounded sampler for release-gate process and SQLite resource peaks."""

    def __init__(self, database: Path, ledger: Path):
        self._database = database
        self._ledger = ledger
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="full-round-resource-probe",
            daemon=True,
        )
        self.fd_peak = 0
        self.thread_peak = 0
        self.wal_bytes_peak = 0

    @staticmethod
    def _fd_count() -> int:
        for directory in ("/proc/self/fd", "/dev/fd"):
            try:
                return len(os.listdir(directory))
            except OSError:
                continue
        raise RuntimeError("resource gate cannot count file descriptors")

    def _sample(self) -> None:
        self.fd_peak = max(self.fd_peak, self._fd_count())
        self.thread_peak = max(self.thread_peak, threading.active_count())
        self.wal_bytes_peak = max(
            self.wal_bytes_peak,
            *(path.stat().st_size if path.exists() else 0 for path in (
                Path(str(self._database) + "-wal"),
                Path(str(self._ledger) + "-wal"),
            )),
        )

    def _run(self) -> None:
        while not self._stop.wait(0.005):
            self._sample()

    def start(self) -> None:
        self._sample()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        if self._thread.is_alive():
            raise RuntimeError("resource gate probe did not stop")
        self._sample()


def _seed_real_permanent_history(
    recorder: ReviewRecorder,
    micro_sampler: MicroObservationSampler,
    symbols: tuple[str, ...],
    permanent_claim_count: int,
    micro_analysis_count: int = 500,
) -> None:
    """Fill the actual permanent hot-path tables, not only payload events."""

    if permanent_claim_count < 1 or micro_analysis_count < 500:
        raise ValueError("permanent history scale fixture is too small")
    now = "2026-08-12T00:00:00+00:00"
    audits = []
    ledgers = []
    for index in range(permanent_claim_count):
        source_signal_id = 20_000_000 + index
        source_scan_id = 30_000_000 + index
        structure_id = f"{index:024x}"
        symbol = f"P{index:06d}USDT"
        evidence_sha256 = f"{index + 1:064x}"
        audits.append((
            source_signal_id, source_scan_id, "N06", symbol,
            "", "[]", "", 1, 1, "PASSED", "PASSED",
            structure_id, "{}", now, evidence_sha256, now, "ACTIVE",
        ))
        ledgers.append((
            "N06", symbol, structure_id, source_signal_id,
            source_scan_id, now, evidence_sha256, now, "ACTIVE",
        ))

    windows = micro_sampler.snapshot()
    analysis = analyze_n21(windows[symbols[6]], OPEN_TIME + 180_001)
    if not analysis.passed or analysis.structure is None:
        raise RuntimeError("micro permanent history fixture is not PASSED")
    micro_rows = []
    for index in range(micro_analysis_count):
        symbol = f"M{index:06d}USDT"
        source_scan_id = 40_000_000 + index
        structure_id = hashlib.sha256(symbol.encode("utf-8")).hexdigest()[:24]
        evidence = deepcopy(analysis.evidence)
        evidence["symbol"] = symbol
        evidence["structure_id"] = structure_id
        for item in evidence["observations"]:
            item[0] = symbol
        evidence["observations"][-1][1] = source_scan_id
        evidence_json = json.dumps(
            evidence,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        micro_rows.append((
            source_scan_id,
            "N21",
            symbol,
            structure_id,
            analysis.structure.kline_open_time_ms,
            analysis.structure.confirmation_observed_at_ms,
            analysis.structure.deadline_ms,
            evidence_json,
            hashlib.sha256(evidence_json.encode("utf-8")).hexdigest(),
            now,
        ))

    with recorder._connect() as connection:
        connection.executemany(
            "INSERT INTO strategy_passed_signal_audits("
            "source_signal_id,source_scan_id,strategy_id,symbol,funding_rate,"
            "matched_patterns,trend_slope,current_bullish,passed,decision,reason,"
            "structure_id,detail_json,signal_created_at,evidence_sha256,created_at,"
            "claim_state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            audits,
        )
        connection.executemany(
            "INSERT INTO strategy_passed_structure_ledger("
            "strategy_id,symbol,structure_id,source_signal_id,source_scan_id,"
            "source_signal_created_at,evidence_sha256,created_at,claim_state) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            ledgers,
        )
        connection.executemany(
            "INSERT INTO micro_passed_analyses("
            "source_scan_id,strategy_id,symbol,structure_id,kline_open_time_ms,"
            "confirmation_observed_at_ms,deadline_ms,evidence_json,"
            "evidence_sha256,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            micro_rows,
        )


def _database_snapshot(database: Path) -> dict[str, object]:
    uri = f"{database.resolve().as_uri()}?mode=ro"
    with closing(
        sqlite3.connect(uri, uri=True, isolation_level=None)
    ) as connection:
        batches = connection.execute(
            """
            SELECT state, COUNT(*), COALESCE(SUM(recorded_count), 0),
                   COALESCE(SUM(expected_count), 0)
            FROM strategy_signal_batches
            GROUP BY state ORDER BY state
            """
        ).fetchall()
        current_by_strategy = connection.execute(
            """
            SELECT strategy_id, COUNT(*)
            FROM strategy_signals AS signal
            JOIN strategy_signal_batches AS batch
              ON batch.scan_id = signal.scan_id
            WHERE batch.state = 'CURRENT'
            GROUP BY strategy_id ORDER BY strategy_id
            """
        ).fetchall()
        current_passed = connection.execute(
            """
            SELECT COUNT(*)
            FROM strategy_signals AS signal
            JOIN strategy_signal_batches AS batch
              ON batch.scan_id = signal.scan_id
            WHERE batch.state = 'CURRENT' AND signal.decision = 'PASSED'
            """
        ).fetchone()[0]
        counts = {
            table: connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in (
                "history_coverage_publication_receipts",
                "n15_snapshot_terminal_receipts",
                "strategy_passed_signal_audits",
                "strategy_passed_structure_ledger",
                "micro_passed_analyses",
                "n13_rotation_states",
                "strategy_paper_trades",
                "strategy_live_links",
                "trade_reviews",
                "events",
            )
        }
        current_scan = connection.execute(
            "SELECT current_scan_id FROM strategy_signal_current WHERE singleton_id=1"
        ).fetchone()[0]
    return {
        "batches": tuple(tuple(row) for row in batches),
        "current_by_strategy": dict(current_by_strategy),
        "current_passed": current_passed,
        "counts": counts,
        "current_scan": current_scan,
    }


def _expected_analysis_counts(
    active_global_cooldown_members: int = 0,
) -> dict[str, int]:
    if (
        type(active_global_cooldown_members) is not int
        or not 0 <= active_global_cooldown_members <= 100
    ):
        raise ValueError("active cooldown fixture count is invalid")
    expected = {"analyze_symbol": 0}
    for strategy_number in range(6, 20):
        expected_name = {
            6: "analyze_n06_double_break_pullback",
            7: "analyze_n07_p1_retest",
            8: "analyze_n08_range_five_bullish",
            9: "analyze_n09_slow_decline_half_retrace",
            10: "analyze_n10_volume_liquidity_sweep_reclaim",
            11: "analyze_n11_volatility_squeeze_breakout_retest",
            12: "analyze_n12_relative_strength_first_pullback",
            13: "analyze_n13_vwap_rotation",
            14: "analyze_n14_sell_pressure_decay_reversal",
            15: "analyze_n15_breadth_recovery_leader",
            16: "analyze_n16_mature_trend_support",
            17: "analyze_n17_range_support_rebound",
            18: "analyze_n18_ascending_triangle_breakout",
            19: "analyze_n19_staircase_exhaustion_reversal",
        }[strategy_number]
        expected[expected_name] = (
            200 - active_global_cooldown_members
            if strategy_number == 14
            else 100 - active_global_cooldown_members
            if strategy_number == 15
            else 100
            if strategy_number in {17, 18, 19}
            else 100 - active_global_cooldown_members
        )
    expected["analyze_n20_market_episode"] = 1
    expected["analyze_micro_strategy"] = 500
    return expected


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-mib", type=int, default=305)
    parser.add_argument("--receipt-count", type=int, default=10_251)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--max-round-seconds", type=float, default=30.0)
    parser.add_argument("--kline-delay-ms", type=int, default=0)
    args = parser.parse_args()
    if (
        args.database_mib < 1
        or args.receipt_count < 1
        or args.rounds < 3
        or args.max_round_seconds <= 0
        or args.kline_delay_ms < 0
    ):
        raise SystemExit("full strategy round scale arguments are invalid")

    case = AuthorizedLegacyV3WitnessTests("runTest")
    logger = logging.getLogger("full-strategy-round-scale-gate")
    symbols = _symbols()
    raw_by_symbol = _raw_by_symbol(symbols)
    micro_sampler = MicroObservationSampler(boot_id="boot")
    for ordinal in range(1, 5):
        if not micro_sampler.commit_sample(
            _micro_sample(symbols, ordinal)
        ):
            raise RuntimeError("micro sampler scale fixture did not warm")
    durations: list[float] = []
    phase_rows: list[dict[str, object]] = []

    with tempfile.TemporaryDirectory(
        prefix="full-strategy-round-scale-"
    ) as root:
        database, ledger, evidence_sha = case._v3_fixture(root)
        _add_receipts(database, args.receipt_count)
        _inflate_review(database, args.database_mib)
        mocked_record = case._mock_record(evidence_sha)
        with patch(
            "trading_bot.n19_analyzer.decode_n19_state_evidence",
            return_value=mocked_record,
        ):
            plan = inspect_authorized_legacy_v3_witness(database, ledger)
            install_authorized_legacy_v3_witness(
                database, ledger, plan.to_jsonable()
            )
            recorder = ReviewRecorder(
                database,
                logger,
                n16_claim_ledger_file=ledger,
            )
            strategies = load_all_strategies()
            recorder.upsert_strategy_definitions(strategies)
            cooldown_symbol = symbols[58]
            if not recorder.set_symbol_cooldown(
                cooldown_symbol,
                datetime.now(timezone.utc) + timedelta(hours=4),
                "STOP_LOSS",
                6,
            ):
                raise RuntimeError("unable to seed active global cooldown")
            offboard_state_hashes = _install_offboard_states(
                recorder, OPEN_TIME
            )
            offboard_raw = _offboard_raw_by_symbol(OPEN_TIME)
            for offboard_symbol in set(N17_OFFBOARD) | set(N18_OFFBOARD):
                raw_by_symbol[offboard_symbol] = offboard_raw[offboard_symbol]
            n15_offboard = "HFTUSDT"
            n15_frozen_symbols = (*symbols[:-1], n15_offboard)
            n15_frozen_candidates = [
                FundingCandidate(
                    symbol,
                    None,
                    Decimal("100"),
                    quote_volume=Decimal(1_000_000_000 - rank),
                    quote_volume_rank=rank,
                    candidate_universe="quote_volume_top",
                )
                for rank, symbol in enumerate(n15_frozen_symbols, start=1)
            ]
            raw_by_symbol[n15_offboard] = deepcopy(raw_by_symbol[symbols[-1]])
            n15_old_open_time = OPEN_TIME - 900_000
            n15_old_raw = {}
            for symbol in n15_frozen_symbols:
                current_raw = raw_by_symbol[symbol]
                seed = deepcopy(current_raw[0])
                seed[0] -= 900_000
                seed[6] -= 900_000
                n15_old_raw[symbol] = [seed, *deepcopy(current_raw[:-1])]
            for index in (5, 7, 10):
                raw_by_symbol[n15_offboard][-1][index] = "0"
            n15_payload, _ = build_n15_snapshot(
                N15_STRATEGY,
                n15_frozen_candidates,
                n15_old_raw,
            )
            if recorder.record_n15_market_snapshot(
                "N15", str(n15_old_open_time), n15_payload
            ) != "OK":
                raise RuntimeError("unable to seed N15 offboard snapshot")
            permanent_claim_count = max(500, args.receipt_count)
            _seed_real_permanent_history(
                recorder,
                micro_sampler,
                symbols,
                permanent_claim_count,
            )
            scheduler = StrategyScheduler(strategies, 96, recorder, logger)
            monitor = FundingMonitor(
                _MarketClient(symbols), Decimal("0.01"), logger
            )
            abandoned_scan_id = recorder.begin_scan(851, (), True)
            if type(abandoned_scan_id) is not int:
                raise RuntimeError("unable to prepare abandoned STAGING batch")
            initial_snapshot = _database_snapshot(database)
            if (
                initial_snapshot["batches"]
                != (("CURRENT", 1, 1, 1), ("STAGING", 1, 0, 0))
            ):
                raise RuntimeError("abandoned STAGING fixture is invalid")
            protected_before = dict(initial_snapshot["counts"])
            first_published_receipt_count: int | None = None
            cumulative_published_passed = 0
            evaluated_offboard: set[tuple[str, str]] = set()

            analysis_names = (
                "analyze_symbol",
                "analyze_n06_double_break_pullback",
                "analyze_n07_p1_retest",
                "analyze_n08_range_five_bullish",
                "analyze_n09_slow_decline_half_retrace",
                "analyze_n10_volume_liquidity_sweep_reclaim",
                "analyze_n11_volatility_squeeze_breakout_retest",
                "analyze_n12_relative_strength_first_pullback",
                "analyze_n13_vwap_rotation",
                "analyze_n14_sell_pressure_decay_reversal",
                "analyze_n15_breadth_recovery_leader",
                "analyze_n16_mature_trend_support",
                "analyze_n17_range_support_rebound",
                "analyze_n18_ascending_triangle_breakout",
                "analyze_n19_staircase_exhaustion_reversal",
                "analyze_n20_market_episode",
                "analyze_micro_strategy",
            )
            for _round in range(args.rounds):
                if _round:
                    ordinal = 4 + _round
                    if not micro_sampler.commit_sample(
                        _micro_sample(symbols, ordinal)
                    ):
                        raise RuntimeError(
                            "micro sampler scale fixture did not advance"
                        )
                totals: dict[str, float] = {}
                counts: dict[str, int] = {}
                cpu_started = time.process_time()
                thread_count_before = threading.active_count()
                fd_count_before = _ResourceProbe._fd_count()
                rss_before = _rss_peak_bytes()
                round_started = time.perf_counter()
                market_started = time.perf_counter()
                market = monitor.scan_for_strategies(100)
                market_seconds = time.perf_counter() - market_started
                if (
                    market.scanned_count != 851
                    or len(market.funding_candidates) != 0
                    or len(market.volume_candidates) != 100
                ):
                    raise RuntimeError("full round market shape is invalid")
                groups = {
                    "negative_funding": market.funding_candidates,
                    "quote_volume_top": market.volume_candidates,
                }
                delayed_client = _DelayedKlineClient(
                    raw_by_symbol,
                    args.kline_delay_ms / 1000,
                )
                fetch_bot = TradingBot.__new__(TradingBot)
                fetch_bot.client = delayed_client
                fetch_bot._kline_request_slots = threading.BoundedSemaphore(10)
                kline_started = time.perf_counter()
                (
                    round_raw_by_symbol,
                    round_observed_at_ms,
                    round_failures,
                ) = fetch_bot._fetch_strategy_kline_generation(
                    tuple(sorted(raw_by_symbol))
                )
                kline_seconds = time.perf_counter() - kline_started
                if (
                    round_failures
                    or set(round_raw_by_symbol) != set(raw_by_symbol)
                    or set(round_observed_at_ms) != set(raw_by_symbol)
                    or len(delayed_client.calls) != len(raw_by_symbol)
                    or set(delayed_client.calls) != set(raw_by_symbol)
                    or delayed_client.max_active > 10
                    or (
                        args.kline_delay_ms > 0
                        and len(raw_by_symbol) > 1
                        and delayed_client.max_active < 2
                    )
                ):
                    raise RuntimeError("bounded Kline scale generation failed")
                scan_id = recorder.begin_scan(
                    market.scanned_count, market.all_candidates, True
                )
                if type(scan_id) is not int:
                    raise RuntimeError("full round scan did not begin")
                with ExitStack() as stack:
                    resource_probe = _ResourceProbe(database, ledger)
                    resource_probe.start()
                    stack.callback(resource_probe.stop)
                    original_evaluate_candidate = scheduler._evaluate_candidate

                    def evaluate_with_offboard_boundary(
                        strategy, candidate, *arguments
                    ):
                        if (
                            strategy.strategy_id == "N15"
                            and candidate.symbol == n15_offboard
                        ):
                            evaluated_offboard.add(("N15", n15_offboard))
                        if candidate.symbol in set(N17_OFFBOARD) | set(
                            N18_OFFBOARD
                        ):
                            proposals = arguments[16]
                            if strategy.strategy_id in {"N17", "N18", "N19"}:
                                proposals.append(
                                    scheduler._history_coverage_proposal(
                                        strategy.strategy_id,
                                        candidate.symbol,
                                        arguments[0][candidate.symbol],
                                        122,
                                    )
                                )
                            evaluated_offboard.add(
                                (strategy.strategy_id, candidate.symbol)
                            )
                            return scheduler._rejected(
                                strategy,
                                candidate,
                                (
                                    "N18_STRUCTURE_CONSUMED"
                                    if strategy.strategy_id == "N18"
                                    else f"{strategy.strategy_id}_STRUCTURE_NOT_FOUND"
                                ),
                            )
                        return original_evaluate_candidate(
                            strategy, candidate, *arguments
                        )

                    stack.enter_context(
                        patch.object(
                            scheduler,
                            "_evaluate_candidate",
                            new=evaluate_with_offboard_boundary,
                        )
                    )
                    for name in analysis_names:
                        _instrument_attribute(
                            stack,
                            scheduler_module,
                            name,
                            totals,
                            counts,
                        )
                    for name in (
                        "active_symbol_cooldowns",
                        "active_strategy_symbol_cooldowns",
                        "record_micro_passed_analyses",
                        "record_strategy_signals",
                        "publish_strategy_signal_batch",
                        "_connect",
                        "_open_identity_attested_runtime_connection",
                        "_attest_legacy_witness_pair",
                        "_attest_n16_claim_ledger_after_catalog_attestation",
                        "_attest_n16_ledger_before_review_commit",
                    ):
                        _instrument_attribute(
                            stack,
                            recorder,
                            name,
                            totals,
                            counts,
                        )
                    _instrument_attribute(
                        stack,
                        recorder.n16_claim_ledger,
                        "attest",
                        totals,
                        counts,
                    )
                    for name in (
                        "runtime_attestation_scope",
                        "runtime_attestation_transaction",
                        "commit_attested_review",
                    ):
                        _instrument_attribute(
                            stack,
                            recorder.n16_claim_ledger,
                            name,
                            totals,
                            counts,
                        )
                    _instrument_attribute(
                        stack,
                        n19_module,
                        "build_n19_market_context",
                        totals,
                        counts,
                    )
                    _instrument_attribute(
                        stack,
                        n20_module,
                        "compress_n20_evidence",
                        totals,
                        counts,
                    )
                    scheduler_started = time.perf_counter()
                    lease_holder = []
                    micro_freeze_started = []
                    micro_confirmation_ages = []

                    def provide_micro_windows():
                        micro_freeze_started.append(time.perf_counter())
                        lease = micro_sampler.freeze_for_scan(
                            scan_id=scan_id,
                            current_symbols=symbols,
                            fallback_observations={},
                            captured_at_ms=(
                                OPEN_TIME + 100
                                + (3 + _round) * 60_000
                            ),
                        )
                        lease_holder.append(lease)
                        latest_observed_at_ms = max(
                            window.observations[-1].observed_at_ms
                            for window in lease.windows.values()
                        )
                        micro_confirmation_ages.append(
                            (
                                lease.captured_at_ms
                                - latest_observed_at_ms
                            )
                            / 1000.0
                        )
                        return lease.windows, lease.captured_at_ms

                    result = scheduler.evaluate(
                        scan_id,
                        groups,
                        round_raw_by_symbol,
                        checked_at_ms=OPEN_TIME + 180_001,
                        micro_window_provider=provide_micro_windows,
                        micro_context_complete=True,
                        authenticated_symbols=market.authenticated_symbols,
                        authenticated_symbols_sha256=(
                            market.authenticated_symbols_sha256
                        ),
                    )
                    scheduler_seconds = (
                        time.perf_counter() - scheduler_started
                    )
                    freeze_to_current_seconds = (
                        time.perf_counter() - micro_freeze_started[0]
                        if len(micro_freeze_started) == 1
                        else float("inf")
                    )
                    confirmation_to_current_seconds = (
                        micro_confirmation_ages[0]
                        + freeze_to_current_seconds
                        if len(micro_confirmation_ages) == 1
                        and micro_confirmation_ages[0] >= 0
                        else float("inf")
                    )
                    if (
                        len(lease_holder) != 1
                        or not micro_sampler.confirm(
                            lease_holder[0], current_scan_id=scan_id
                        )
                    ):
                        raise RuntimeError(
                            "micro sampler scale lease did not confirm"
                        )
                duration = time.perf_counter() - round_started
                cpu_seconds = time.process_time() - cpu_started
                thread_count_after = threading.active_count()
                rss_after = _rss_peak_bytes()
                ledger_transactions = counts.get(
                    "runtime_attestation_transaction", 0
                )
                n16_transaction_attestations = counts.get(
                    "_attest_n16_claim_ledger_after_catalog_attestation", 0
                )
                legacy_pair_attestations = counts.get(
                    "_attest_legacy_witness_pair", 0
                )
                commit_attestations = counts.get(
                    "_attest_n16_ledger_before_review_commit", 0
                )
                commit_transactions = counts.get(
                    "commit_attested_review", 0
                )
                if (
                    not result.signal_batch_published
                    or len(result.signals) != 2_000
                    or micro_sampler.frame_count > 4
                    or micro_sampler.n22_market_context_entry_count > 400
                    or sum(
                        len(window.observations)
                        for window in micro_sampler.snapshot().values()
                    ) > 400
                    or counts.get("active_symbol_cooldowns") != 1
                    or counts.get("active_strategy_symbol_cooldowns") != 1
                    or counts.get("_open_identity_attested_runtime_connection") != 2
                    or counts.get("runtime_attestation_scope") != 1
                    or not 1 <= ledger_transactions <= 70
                    or not 1 <= commit_transactions <= 35
                    or commit_attestations != commit_transactions
                    or counts.get("_connect", 0) > 35
                    or abs(
                        n16_transaction_attestations - ledger_transactions
                    ) > 1
                    or legacy_pair_attestations
                    != (
                        n16_transaction_attestations
                        + commit_attestations
                        + 1
                    )
                    or abs(
                        counts.get("attest")
                        - (legacy_pair_attestations + 1)
                    ) > 1
                    or counts.get("record_micro_passed_analyses", 0) > 1
                    or counts.get("record_strategy_signals") != 1
                    or counts.get("publish_strategy_signal_batch") != 1
                    or thread_count_after != thread_count_before
                    or resource_probe.fd_peak > fd_count_before + 12
                    or resource_probe.thread_peak > thread_count_before + 2
                    or resource_probe.wal_bytes_peak > 128 * 1024 * 1024
                    or rss_after - rss_before > 256 * 1024 * 1024
                    or {
                        name: counts.get(name, 0)
                        for name in analysis_names
                    }
                    != _expected_analysis_counts(1)
                ):
                    raise RuntimeError(
                        "full strategy round failed: "
                        + json.dumps(
                            {
                                "round": _round,
                                "published": result.signal_batch_published,
                                "signals": len(result.signals),
                                "counts": counts,
                                "expected": _expected_analysis_counts(1),
                                "resources": {
                                    "fd_before": fd_count_before,
                                    "fd_peak": resource_probe.fd_peak,
                                    "thread_before": thread_count_before,
                                    "thread_after": thread_count_after,
                                    "thread_peak": resource_probe.thread_peak,
                                    "wal_bytes_peak": resource_probe.wal_bytes_peak,
                                    "rss_delta": rss_after - rss_before,
                                },
                            },
                            sort_keys=True,
                        )
                    )
                if any(
                    signal.strategy.strategy_id in {
                        "N21", "N22", "N23", "N24", "N25"
                    }
                    and signal.reason.endswith(
                        "MICRO_OBSERVATION_COLD_START"
                    )
                    for signal in result.signals
                ):
                    raise RuntimeError(
                        "independent micro sampler remained cold"
                    )
                snapshot = _database_snapshot(database)
                expected_strategy_counts = {
                    f"N{number:02d}": 100
                    for number in range(6, 26)
                }
                if (
                    snapshot["batches"] != (("CURRENT", 1, 2_000, 2_000),)
                    or snapshot["current_by_strategy"]
                    != expected_strategy_counts
                    or snapshot["current_scan"] != scan_id
                ):
                    raise RuntimeError(
                        "CURRENT/STAGING or per-strategy signal shape is invalid"
                    )
                if any(
                    row[2]
                    in set(N17_OFFBOARD) | set(N18_OFFBOARD) | {n15_offboard}
                    for row in recorder.list_current_strategy_signals()
                ):
                    raise RuntimeError(
                        "offboard member entered the ordinary CURRENT batch"
                    )
                with recorder._read_only_runtime_snapshot() as connection:
                    if connection.execute(
                        "SELECT COUNT(*) FROM n15_snapshot_terminal_receipts "
                        "WHERE strategy_id='N15' AND e_time=?",
                        (str(n15_old_open_time),),
                    ).fetchone() != (1,):
                        raise RuntimeError(
                            "N15 offboard snapshot was not permanently closed"
                        )
                    if connection.execute(
                        "SELECT COUNT(*) FROM n15_market_snapshots "
                        "WHERE strategy_id='N15' AND e_time=?",
                        (str(n15_old_open_time),),
                    ).fetchone() != (0,):
                        raise RuntimeError(
                            "N15 closed snapshot remained active"
                        )
                cooldown_rows = [
                    row
                    for row in recorder.list_current_strategy_signals()
                    if row[2] == cooldown_symbol
                    and row[1] in {"N17", "N18", "N19"}
                ]
                if (
                    len(cooldown_rows) != 3
                    or {row[1] for row in cooldown_rows}
                    != {"N17", "N18", "N19"}
                    or any(
                        row[3] != 0
                        or row[4] != "REJECTED"
                        or not row[5].startswith(
                            "GLOBAL_SYMBOL_COOLDOWN_UNTIL:"
                        )
                        for row in cooldown_rows
                    )
                ):
                    raise RuntimeError(
                        "active cooldown ordinary decisions are invalid"
                    )
                with recorder._read_only_runtime_snapshot() as connection:
                    if connection.execute(
                        "SELECT COUNT(*) FROM history_coverage_epoch_heads "
                        "WHERE symbol=? AND strategy_id IN "
                        "('N17','N18','N19')",
                        (cooldown_symbol,),
                    ).fetchone() != (3,):
                        raise RuntimeError(
                            "active cooldown coverage owners are incomplete"
                        )
                    for (strategy_id, symbol), evidence_sha in (
                        offboard_state_hashes.items()
                    ):
                        table = (
                            "n17_range_support_states"
                            if strategy_id == "N17"
                            else "n18_triangle_states"
                        )
                        if connection.execute(
                            f"SELECT evidence_sha256 FROM {table} "
                            "WHERE strategy_id=? AND symbol=?",
                            (strategy_id, symbol),
                        ).fetchone() != (evidence_sha,):
                            raise RuntimeError(
                                "offboard lifecycle state changed unexpectedly"
                            )
                        if connection.execute(
                            "SELECT COUNT(*) FROM "
                            "history_coverage_publication_receipts "
                            "WHERE strategy_id=? AND symbol=?",
                            (strategy_id, symbol),
                        ).fetchone() != (1,):
                            raise RuntimeError(
                                "offboard coverage receipt is incomplete"
                            )
                current_counts = dict(snapshot["counts"])
                if (
                    protected_before["strategy_passed_signal_audits"]
                    < permanent_claim_count
                    or protected_before["strategy_passed_structure_ledger"]
                    < permanent_claim_count
                    or protected_before["micro_passed_analyses"] < 500
                    or current_counts["strategy_paper_trades"]
                    != protected_before["strategy_paper_trades"]
                    or current_counts["strategy_live_links"]
                    != protected_before["strategy_live_links"]
                    or current_counts["trade_reviews"]
                    != protected_before["trade_reviews"]
                ):
                    raise RuntimeError(
                        "full strategy round produced execution side effects"
                    )
                if current_counts["events"] != (
                    protected_before["events"] + _round + 1
                ):
                    raise RuntimeError(
                        "global cooldown audit event count is invalid"
                    )
                with recorder._read_only_runtime_snapshot() as connection:
                    if connection.execute(
                        "SELECT COUNT(*) FROM events WHERE event_type=? "
                        "AND symbol=?",
                        ("global_symbol_cooldown_skip", cooldown_symbol),
                    ).fetchone() != (_round + 1,):
                        raise RuntimeError(
                            "global cooldown audit event is incomplete"
                        )
                passed_count = int(snapshot["current_passed"])
                if _round == 0 and passed_count < 3:
                    raise RuntimeError(
                        "full strategy round did not exercise PASSED publication"
                    )
                cumulative_published_passed += passed_count
                if (
                    current_counts["strategy_passed_signal_audits"]
                    - protected_before["strategy_passed_signal_audits"]
                    != cumulative_published_passed
                    or current_counts["strategy_passed_structure_ledger"]
                    - protected_before["strategy_passed_structure_ledger"]
                    != cumulative_published_passed
                    or current_counts["micro_passed_analyses"]
                    < protected_before["micro_passed_analyses"]
                    or current_counts["n13_rotation_states"] <= 0
                    or current_counts["history_coverage_publication_receipts"]
                    < protected_before["history_coverage_publication_receipts"]
                    or current_counts["n15_snapshot_terminal_receipts"]
                    != protected_before["n15_snapshot_terminal_receipts"] + 1
                ):
                    raise RuntimeError(
                        "permanent audit, ledger, or receipt graph is invalid: "
                        + json.dumps(
                            {
                                "round": _round,
                                "passed": passed_count,
                                "before": protected_before,
                                "current": current_counts,
                            },
                            sort_keys=True,
                        )
                    )
                receipt_count = current_counts[
                    "history_coverage_publication_receipts"
                ]
                if first_published_receipt_count is None:
                    first_published_receipt_count = receipt_count
                elif receipt_count != first_published_receipt_count:
                    raise RuntimeError(
                        "NO_CHANGE coverage receipts grew across identical rounds"
                    )
                durations.append(duration)
                phase_rows.append(
                    {
                        "market_seconds": market_seconds,
                        "kline_seconds": kline_seconds,
                        "kline_request_count": len(delayed_client.calls),
                        "kline_max_concurrency": delayed_client.max_active,
                        "scheduler_seconds": scheduler_seconds,
                        "cpu_seconds": cpu_seconds,
                        "rss_peak_before": rss_before,
                        "rss_peak_after": rss_after,
                        "rss_peak_delta": rss_after - rss_before,
                        "fd_count_before": fd_count_before,
                        "fd_peak": resource_probe.fd_peak,
                        "thread_peak": resource_probe.thread_peak,
                        "wal_bytes_peak": resource_probe.wal_bytes_peak,
                        "thread_count_before": thread_count_before,
                        "thread_count_after": thread_count_after,
                        "analyzer_seconds": sum(
                            totals.get(name, 0.0)
                            for name in analysis_names
                        ),
                        "batch_write_seconds": totals.get(
                            "record_strategy_signals", 0.0
                        ),
                        "publish_seconds": totals.get(
                            "publish_strategy_signal_batch", 0.0
                        ),
                        "freeze_to_current_seconds": freeze_to_current_seconds,
                        "confirmation_to_current_seconds": (
                            confirmation_to_current_seconds
                        ),
                        "remaining_entry_window_seconds": (
                            120.0 - confirmation_to_current_seconds
                        ),
                        "current_passed": passed_count,
                        "permanent_counts": current_counts,
                        "counts": counts,
                    }
                )

            ordered = sorted(durations)
            p90 = ordered[max(0, math.ceil(len(ordered) * 0.90) - 1)]
            current = recorder.list_current_strategy_signals()
            with recorder._read_only_runtime_snapshot() as connection:
                unicode_strategy_rows = connection.execute(
                    "SELECT strategy_id FROM strategy_signals "
                    "WHERE symbol='龙虾USDT' ORDER BY strategy_id"
                ).fetchall()
                unicode_coverage_rows = connection.execute(
                    "SELECT strategy_id FROM "
                    "history_coverage_publication_receipts "
                    "WHERE symbol='龙虾USDT' ORDER BY strategy_id"
                ).fetchall()
                micro_passed_strategies = connection.execute(
                    "SELECT DISTINCT strategy_id FROM micro_passed_analyses "
                    "ORDER BY strategy_id"
                ).fetchall()
            if (
                len(current) != 2_000
                or unicode_strategy_rows
                != [("N%02d" % index,) for index in range(6, 26)]
                or unicode_coverage_rows
                != [("N17",), ("N18",), ("N19",)]
                or micro_passed_strategies
                != [("N21",), ("N23",), ("N25",)]
                or evaluated_offboard
                != {
                    ("N17", "LAUSDT"),
                    ("N17", "REUSDT"),
                    ("N17", "ZHIPUUSDT"),
                    ("N18", "ZHIPUUSDT"),
                }
                or cumulative_published_passed < 3
                or max(
                    float(row["confirmation_to_current_seconds"])
                    for row in phase_rows
                ) > 60.0
                or p90 > args.max_round_seconds
                or p90 + 60 > 150
            ):
                raise RuntimeError(
                    "full strategy round performance gate failed"
                )
            print(
                json.dumps(
                    {
                        "database_bytes": database.stat().st_size,
                        "receipt_count": args.receipt_count,
                        "permanent_claim_count": permanent_claim_count,
                        "seeded_micro_passed_analysis_count": 500,
                        "kline_delay_ms": args.kline_delay_ms,
                        "scanned_count": 851,
                        "candidate_count": 100,
                        "signal_count": 2_000,
                        "rounds": args.rounds,
                        "round_seconds": durations,
                        "median_seconds": statistics.median(durations),
                        "p90_seconds": p90,
                        "start_to_start_with_sleep_p90_seconds": p90 + 60,
                        "phase_rows": phase_rows,
                        "current_signal_count": len(current),
                    },
                    sort_keys=True,
                )
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
