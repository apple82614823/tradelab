from __future__ import annotations

import json
import re
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from .analyzer import AnalysisResult, analyze_symbol
from .binance_client import BinanceAPIError, BinanceFuturesClient
from .config import Config, load_config
from .exchange_symbol import (
    attest_authenticated_symbol_set,
    authenticated_symbol_set_sha256,
    canonical_exchange_symbol,
)
from .instance_lock import InstanceLock
from .logger import setup_logging
from .monitor import FundingMonitor
from .micro_observation import (
    MicroObservationCache,
    MicroObservationError,
    MicroObservationLease,
    MicroObservationSample,
    MicroObservationSampler,
    MicroSampleIdentity,
    build_micro_observation,
    build_universe_sha256,
)
from .micro_analyzer import MicroAnalysisResult, MICRO_STRATEGY_IDS
from .n06_analyzer import N06AnalysisResult
from .n07_analyzer import N07AnalysisResult
from .n08_analyzer import N08AnalysisResult
from .n09_analyzer import N09AnalysisResult
from .n10_analyzer import N10AnalysisResult
from .n11_analyzer import N11AnalysisResult
from .n12_analyzer import N12AnalysisResult
from .n13_analyzer import N13AnalysisResult
from .n14_analyzer import N14AnalysisResult
from .n15_analyzer import N15AnalysisResult
from .n16_analyzer import N16AnalysisResult
from .n17_analyzer import N17AnalysisResult
from .n18_analyzer import N18AnalysisResult
from .n19_analyzer import N19AnalysisResult
from .n20_analyzer import N20AnalysisResult
from .paper_trader import PaperTrader
from .precision import decimal_to_api
from .recorder import PaperTradePersistenceError, ReviewRecorder
from .state import PositionState, StateStore
from .strategies import load_active_strategies
from .strategy_scheduler import (
    StrategyScheduler,
    StrategySignalDecision,
)
from .trader import EntryWindowExpiredError, LiveCloseResolution, TradePlan, Trader


_LOCAL_STATE_UNSET = object()
_N16_STOP_MODE = "trend_support_continuation_margin_capped"
_N17_STOP_MODE = "range_support_absorption_margin_capped"
_N18_STOP_MODE = "ascending_triangle_breakout_margin_capped"
_N19_STOP_MODE = "staircase_exhaustion_margin_capped"
_N20_STOP_MODE = "relative_strength_recovery_margin_capped"
_MICRO_STOP_MODE = "micro_observation_margin_capped"
_NO_LIVE_RESULT_CLEANUP_REASONS = {
    "EXECUTION_CLEANUP_RESOLVED_WITHOUT_TRADE_RESULT",
    "EMERGENCY_CLEANUP_CONFIRMED_FLAT_WITHOUT_LIVE_RESULT",
}
_KLINE_INTERVAL_MS = 900_000
_KLINE_BOUNDARY_RETRY_LIMIT = 2
_KLINE_BOUNDARY_RETRY_DELAY_SECONDS = 1
_MICRO_KLINE_MAX_WORKERS = 10
_STRATEGY_KLINE_MAX_WORKERS = 10
_MICRO_SAMPLE_COLLECTION_BUDGET_SECONDS = 45
_MICRO_SAMPLE_WAIT_SLICE_SECONDS = 0.25


_AUDIT_CREDENTIAL_NAME = (
    r"(?:api[_-]?(?:key|secret)|secret|token|signature)"
)
_AUDIT_NEXT_FIELD = (
    r"(?=\s+[A-Za-z][A-Za-z0-9_.-]{0,63}\s*[:=]|[,;&}\]]|$)"
)


def _next_fixed_poll_tick(
    previous_tick: float,
    observed_at: float,
    interval_seconds: int,
) -> float:
    """Return the first fixed-grid tick strictly after ``observed_at``."""

    if (
        type(previous_tick) not in {int, float}
        or type(observed_at) not in {int, float}
        or type(interval_seconds) is not int
        or interval_seconds <= 0
        or observed_at < previous_tick
    ):
        raise ValueError("fixed poll cadence input is invalid")
    missed = int((observed_at - previous_tick) // interval_seconds) + 1
    return previous_tick + missed * interval_seconds


def _stale_kline_snapshot_symbols(
    raw_klines_by_symbol,
    kline_observed_at_ms,
    snapshot_observed_at_ms: int,
) -> tuple[str, ...] | None:
    """Return rows that are exactly one 15m generation behind the snapshot.

    ``None`` means the inputs cannot prove the narrow exchange-boundary case;
    the ordinary analyzers retain authority over all other malformed inputs.
    An empty tuple proves every classified row belongs to the same live
    generation at the completed snapshot time.
    """

    if (
        type(raw_klines_by_symbol) is not dict
        or type(kline_observed_at_ms) is not dict
        or type(snapshot_observed_at_ms) is not int
        or snapshot_observed_at_ms <= 0
        or not raw_klines_by_symbol
        or set(raw_klines_by_symbol) != set(kline_observed_at_ms)
    ):
        return None
    target_open_time_ms = (
        snapshot_observed_at_ms // _KLINE_INTERVAL_MS * _KLINE_INTERVAL_MS
    )
    stale: list[str] = []
    for symbol in sorted(raw_klines_by_symbol):
        rows = raw_klines_by_symbol[symbol]
        observed_at_ms = kline_observed_at_ms[symbol]
        if (
            type(symbol) is not str
            or not symbol
            or type(rows) not in (list, tuple)
            or not rows
            or type(observed_at_ms) is not int
            or observed_at_ms <= 0
        ):
            return None
        row = rows[-1]
        if type(row) not in (list, tuple) or len(row) <= 6:
            return None
        try:
            open_time_ms = int(str(row[0]))
            close_time_ms = int(str(row[6]))
        except (TypeError, ValueError):
            return None
        if (
            str(open_time_ms) != str(row[0])
            or str(close_time_ms) != str(row[6])
            or close_time_ms != open_time_ms + _KLINE_INTERVAL_MS - 1
        ):
            return None
        if open_time_ms == target_open_time_ms:
            if not open_time_ms <= observed_at_ms <= close_time_ms:
                return None
            continue
        if (
            open_time_ms == target_open_time_ms - _KLINE_INTERVAL_MS
            and close_time_ms == target_open_time_ms - 1
        ):
            stale.append(symbol)
            continue
        return None
    return tuple(stale)


def _unready_live_kline_value_symbols(
    raw_klines_by_symbol,
    snapshot_observed_at_ms: int,
) -> tuple[str, ...] | None:
    """Return target-generation live rows that N20 cannot strictly parse.

    The exchange can expose a new 15m axis before its cumulative volume
    counters are initialized.  Only the current target row is classified
    here; malformed closed history remains under the analyzers' ordinary
    fail-closed authority.
    """

    if (
        type(raw_klines_by_symbol) is not dict
        or type(snapshot_observed_at_ms) is not int
        or snapshot_observed_at_ms <= 0
        or not raw_klines_by_symbol
    ):
        return None
    target_open_time_ms = (
        snapshot_observed_at_ms // _KLINE_INTERVAL_MS * _KLINE_INTERVAL_MS
    )
    unready: list[str] = []
    for symbol in sorted(raw_klines_by_symbol):
        rows = raw_klines_by_symbol[symbol]
        if (
            type(symbol) is not str
            or not symbol
            or type(rows) not in (list, tuple)
            or not rows
            or type(rows[-1]) not in (list, tuple)
        ):
            return None
        row = rows[-1]
        try:
            open_time_ms = int(str(row[0]))
        except (IndexError, TypeError, ValueError):
            return None
        if open_time_ms != target_open_time_ms:
            continue
        try:
            values = tuple(
                Decimal(str(row[index]))
                for index in (1, 2, 3, 4, 5, 7, 10)
            )
            open_price, high, low, close, base, quote, taker = values
            trade_count = row[8]
            trade_count_exact = type(trade_count) is int
        except (
            IndexError,
            TypeError,
            ValueError,
            ArithmeticError,
        ):
            unready.append(symbol)
            continue
        if (
            any(not value.is_finite() for value in values)
            or min(open_price, high, low, close) <= 0
            or high < max(open_price, close, low)
            or low > min(open_price, close, high)
            or base <= 0
            or quote <= 0
            or not trade_count_exact
            or trade_count < 0
            or not Decimal("0") <= taker <= quote
        ):
            unready.append(symbol)
    return tuple(unready)


def _redact_audit_credentials(text: str) -> str:
    """Redact bounded credential formats while retaining adjacent diagnostics."""

    # URL query values have an unambiguous '&'/'#' boundary and must be
    # handled before the generic key/value forms below.
    text = re.sub(
        rf"(?i)([?&]{_AUDIT_CREDENTIAL_NAME}=)([^&#\s]*)",
        lambda match: match.group(1) + "[REDACTED]",
        text,
    )
    # Quoted Authorization values may contain the scheme and whitespace.
    text = re.sub(
        r"(?i)(\bauthorization\s*[:=]\s*)(?P<quote>[\"'])"
        r".*?(?P=quote)",
        lambda match: (
            match.group(1)
            + match.group("quote")
            + "[REDACTED]"
            + match.group("quote")
        ),
        text,
    )
    # Unquoted Bearer/Basic credentials are one RFC-style token.  Redacting
    # only that token preserves following symbol/reason diagnostics.
    text = re.sub(
        r"(?i)(\bauthorization\s*[:=]\s*)"
        r"(?:bearer|basic)\s+([^,;\s}\]&]+)",
        lambda match: match.group(1) + "[REDACTED]",
        text,
    )
    text = re.sub(
        r"(?i)(\bauthorization\s*[:=]\s*)"
        r"(?!\[REDACTED\])([^,;\s}\]&]+)",
        lambda match: match.group(1) + "[REDACTED]",
        text,
    )
    # JSON/header-style quoted values can contain spaces.  Optional quotes
    # around the field name cover both JSON and conventional headers.
    text = re.sub(
        rf"(?i)((?:[\"'])?\b{_AUDIT_CREDENTIAL_NAME}\b"
        rf"(?:[\"'])?\s*[:=]\s*)(?P<quote>[\"']).*?(?P=quote)",
        lambda match: (
            match.group(1)
            + match.group("quote")
            + "[REDACTED]"
            + match.group("quote")
        ),
        text,
    )
    # An unquoted token value is the one supported legacy multi-word form.
    # Stop at punctuation or the next structured key rather than swallowing
    # unrelated symbol/reason fields.
    text = re.sub(
        rf"(?i)(\btoken\b\s*[:=]\s*)"
        rf"(?![\"']|\[REDACTED\])(.*?)"
        + _AUDIT_NEXT_FIELD,
        lambda match: match.group(1) + "[REDACTED]",
        text,
    )
    # Other unquoted credentials remain single tokens.  Values containing
    # spaces must use quotes, which were handled above.
    text = re.sub(
        rf"(?i)(\b{_AUDIT_CREDENTIAL_NAME}\b\s*[:=]\s*)"
        r"(?![\"']|\[REDACTED\])([^,;\s}\]&]+)",
        lambda match: match.group(1) + "[REDACTED]",
        text,
    )
    return text


def _bounded_audit_error(error: BaseException, limit: int = 2048) -> str:
    """Return bounded printable diagnostics without affecting reason codes."""

    message = "".join(
        character if character.isprintable() else " "
        for character in str(error)
    )
    text = _redact_audit_credentials(
        f"{type(error).__name__}: {message}"
    )
    if len(text) > limit:
        marker = "...[TRUNCATED]"
        text = (
            text[: limit - len(marker)] + marker
            if limit >= len(marker)
            else text[:limit]
        )
    return text


class _N16PlanIntegrityError(RuntimeError):
    """Raised when a published N16 claim and its execution plan diverge."""


class _N17PlanIntegrityError(RuntimeError):
    """Raised when a published N17 claim and its execution plan diverge."""


class _N19PlanIntegrityError(RuntimeError):
    """Raised when a published N19 claim and its execution plan diverge."""


class _N18PlanIntegrityError(RuntimeError):
    """Raised when a published N18 claim and its execution plan diverge."""


class _N20PlanIntegrityError(RuntimeError):
    """Raised when a published N20 winner and execution plan diverge."""


class _MicroPlanIntegrityError(RuntimeError):
    """Raised when a published N21-N25 claim and plan diverge."""


class TradingBot:
    def __init__(self, config: Config = None, instance_lock: InstanceLock = None):
        self.config = config or load_config()
        self.instance_lock = instance_lock or InstanceLock(self.config.instance_lock_file)
        self._owns_instance_lock = False
        if not self.instance_lock.acquired:
            self.instance_lock.acquire()
            self._owns_instance_lock = True
        try:
            self.logger = setup_logging(self.config.log_file)
            # The independent N16 lifecycle ledger and Review DB are jointly
            # attested before constructing any exchange-facing component.
            # This keeps a cross-generation or unresolved two-phase claim
            # fail-closed ahead of all paper/live/exchange side effects.
            self.recorder = ReviewRecorder(
                self.config.review_db_file,
                self.logger,
                n16_claim_ledger_file=self.config.n16_claim_ledger_file,
            )
            self.client = BinanceFuturesClient(self.config, self.logger)
            self.state = StateStore(self.config.state_file)
            self.monitor = FundingMonitor(self.client, self.config.funding_rate_abs_threshold, self.logger)
            self.trader = Trader(self.client, self.config, self.state, self.logger)
            self.strategies = load_active_strategies()
            self.micro_observation_cache = MicroObservationCache()
            self.micro_observation_sampler = MicroObservationSampler()
            self._kline_request_slots = threading.BoundedSemaphore(
                _STRATEGY_KLINE_MAX_WORKERS
            )
            self._micro_sampler_client = None
            self._micro_sampler_monitor = None
            self._poll_stop_event = threading.Event()
            self.recorder.upsert_strategy_definitions(self.strategies)
            self.paper_trader = PaperTrader(self.recorder, self.logger)
            self.strategy_scheduler = StrategyScheduler(
                self.strategies,
                self.config.trend_window,
                self.recorder,
                self.logger,
            )
        except Exception:
            if self._owns_instance_lock:
                self.instance_lock.release()
            raise

    def close(self) -> None:
        poll_stop_event = getattr(self, "_poll_stop_event", None)
        if poll_stop_event is not None:
            poll_stop_event.set()
        if not self._stop_micro_observation_sampler():
            raise RuntimeError(
                "Micro observation sampler ownership remains active"
            )
        if self._owns_instance_lock:
            self.instance_lock.release()

    def _micro_sampler_error(self, error_type: str) -> None:
        self.logger.warning(
            "Micro observation sampling failed closed; only authenticated "
            "last-good frames within the strict freshness boundary remain | "
            "error_type=%s",
            error_type,
        )

    def _start_micro_observation_sampler(self) -> bool:
        sampler = getattr(self, "micro_observation_sampler", None)
        if sampler is None or not any(
            strategy.strategy_id in MICRO_STRATEGY_IDS
            for strategy in self.strategies
        ):
            return True
        if sampler.is_running:
            return True
        public_config = replace(
            self.config,
            api_key="",
            api_secret="",
            dry_run=True,
            live_confirmation="",
            request_retries=1,
            request_retry_delay_seconds=0,
            network_reconnect_delay_seconds=0,
            request_timeout_seconds=min(
                self.config.request_timeout_seconds,
                3,
            ),
        )
        self._micro_sampler_client = BinanceFuturesClient(
            public_config,
            self.logger,
        )
        self._micro_sampler_monitor = FundingMonitor(
            self._micro_sampler_client,
            public_config.funding_rate_abs_threshold,
            self.logger,
        )
        return sampler.start(
            self._collect_micro_observation_sample,
            on_error=self._micro_sampler_error,
        )

    def _stop_micro_observation_sampler(self) -> bool:
        sampler = getattr(self, "micro_observation_sampler", None)
        if sampler is None:
            return True
        stopped = sampler.stop(timeout_seconds=15)
        if not stopped and hasattr(self, "logger"):
            self.logger.error(
                "Micro observation sampler did not stop within its bounded "
                "shutdown window"
            )
        if stopped:
            self._micro_sampler_monitor = None
            self._micro_sampler_client = None
        return stopped

    def _collect_micro_observation_sample(
        self,
        identity: MicroSampleIdentity,
        stop_event,
    ):
        """Collect one public, read-only, exact Top100 micro generation."""

        monitor = self._micro_sampler_monitor
        client = self._micro_sampler_client
        if monitor is None or client is None or stop_event.is_set():
            raise MicroObservationError("micro sampler is not available")
        collection_deadline = (
            time.monotonic() + _MICRO_SAMPLE_COLLECTION_BUDGET_SECONDS
        )
        market_scan = monitor.scan_for_strategies(100)
        top100 = list(market_scan.volume_candidates)
        ranked = tuple(
            (candidate.symbol, candidate.quote_volume_rank)
            for candidate in top100
            if type(candidate.quote_volume_rank) is int
        )
        universe_sha256 = build_universe_sha256(ranked)
        continuation_ranked = identity.continuation_ranked_symbols
        continuation_universe_sha256 = (
            identity.continuation_universe_sha256
        )
        if continuation_ranked:
            if (
                build_universe_sha256(continuation_ranked)
                != continuation_universe_sha256
            ):
                raise MicroObservationError(
                    "micro continuation universe is inconsistent"
                )
        elif continuation_universe_sha256 is not None:
            raise MicroObservationError(
                "micro continuation universe is inconsistent"
            )
        if type(market_scan.premium_observed_at_ms) is not int:
            raise MicroObservationError(
                "micro premium observation time is unavailable"
            )
        if time.monotonic() >= collection_deadline:
            raise MicroObservationError("micro market snapshot exceeded its budget")
        current_symbols = tuple(candidate.symbol for candidate in top100)
        if len(current_symbols) != 100 or len(set(current_symbols)) != 100:
            raise MicroObservationError("micro current Top100 is incomplete")
        raw_klines_by_symbol, kline_observed_at_ms = (
            self._fetch_micro_kline_generation(
                current_symbols,
                stop_event,
                collection_deadline,
            )
        )

        retry_count = 0
        authenticated_snapshot_observed_at_ms = None
        while True:
            snapshot_observed_at_ms = int(time.time() * 1000)
            axis_unready_symbols = _stale_kline_snapshot_symbols(
                raw_klines_by_symbol,
                kline_observed_at_ms,
                snapshot_observed_at_ms,
            )
            value_unready_symbols = _unready_live_kline_value_symbols(
                raw_klines_by_symbol,
                snapshot_observed_at_ms,
            )
            if (
                axis_unready_symbols is None
                or value_unready_symbols is None
            ):
                raise MicroObservationError(
                    "micro Kline snapshot is not classifiable"
                )
            retry_symbols = tuple(sorted(
                set(axis_unready_symbols) | set(value_unready_symbols)
            ))
            if not retry_symbols:
                authenticated_snapshot_observed_at_ms = (
                    snapshot_observed_at_ms
                )
                break
            if retry_count >= _KLINE_BOUNDARY_RETRY_LIMIT:
                raise MicroObservationError(
                    "micro Kline boundary snapshot is not ready"
                )
            retry_count += 1
            if stop_event.wait(_KLINE_BOUNDARY_RETRY_DELAY_SECONDS):
                raise MicroObservationError("micro sampler is stopping")
            retry_klines, retry_observed_at_ms = (
                self._fetch_micro_kline_generation(
                    retry_symbols,
                    stop_event,
                    collection_deadline,
                )
            )
            raw_klines_by_symbol.update(retry_klines)
            kline_observed_at_ms.update(retry_observed_at_ms)

        if authenticated_snapshot_observed_at_ms is None:
            raise MicroObservationError(
                "micro Kline snapshot was not authenticated"
            )
        observations = {}
        for candidate in top100:
            symbol = candidate.symbol
            premium_row = market_scan.premium_by_symbol.get(symbol)
            if premium_row is None:
                raise MicroObservationError(
                    "micro premium member is unavailable"
                )
            observations[symbol] = build_micro_observation(
                symbol=symbol,
                scan_id=identity.sample_ordinal,
                generation=identity.generation,
                boot_id=identity.boot_id,
                observed_at_ms=kline_observed_at_ms[symbol],
                premium_observed_at_ms=market_scan.premium_observed_at_ms,
                raw_klines=raw_klines_by_symbol[symbol],
                premium_row=premium_row,
                quote_volume_rank=candidate.quote_volume_rank,
                universe_sha256=universe_sha256,
                )
        continuation_observations = {}
        departed_ranked = tuple(
            (symbol, rank)
            for symbol, rank in continuation_ranked
            if symbol not in observations
        )
        if len(departed_ranked) > 100:
            raise MicroObservationError(
                "micro Kline continuation exceeds its bound"
            )
        if departed_ranked:
            assert continuation_universe_sha256 is not None
            continuation_reason = None
            departed_symbols = tuple(
                symbol for symbol, _rank in departed_ranked
            )
            if any(
                market_scan.premium_by_symbol.get(symbol) is None
                for symbol in departed_symbols
            ):
                continuation_reason = "PREMIUM_UNAVAILABLE"
            else:
                if stop_event.is_set():
                    raise MicroObservationError("micro sampler is stopping")
                try:
                    (
                        departed_klines,
                        departed_observed_at_ms,
                    ) = self._fetch_micro_kline_generation(
                        departed_symbols,
                        stop_event,
                        collection_deadline,
                    )
                    departed_axis_unready = _stale_kline_snapshot_symbols(
                        departed_klines,
                        departed_observed_at_ms,
                        authenticated_snapshot_observed_at_ms,
                    )
                    departed_value_unready = (
                        _unready_live_kline_value_symbols(
                            departed_klines,
                            authenticated_snapshot_observed_at_ms,
                        )
                    )
                    if (
                        departed_axis_unready is None
                        or departed_value_unready is None
                    ):
                        continuation_reason = "IDENTITY_INVALID"
                    elif departed_axis_unready or departed_value_unready:
                        continuation_reason = "SNAPSHOT_NOT_READY"
                    else:
                        continuation_klines = {
                            **raw_klines_by_symbol,
                            **departed_klines,
                        }
                        continuation_observed_at_ms = {
                            **kline_observed_at_ms,
                            **departed_observed_at_ms,
                        }
                        candidate_continuation = {}
                        for symbol, rank in continuation_ranked:
                            premium_row = (
                                market_scan.premium_by_symbol.get(symbol)
                            )
                            if premium_row is None:
                                raise MicroObservationError(
                                    "micro continuation premium is incomplete"
                                )
                            candidate_continuation[symbol] = (
                                build_micro_observation(
                                    symbol=symbol,
                                    scan_id=identity.sample_ordinal,
                                    generation=identity.generation,
                                    boot_id=identity.boot_id,
                                    observed_at_ms=(
                                        continuation_observed_at_ms[symbol]
                                    ),
                                    premium_observed_at_ms=(
                                        market_scan.premium_observed_at_ms
                                    ),
                                    raw_klines=continuation_klines[symbol],
                                    premium_row=premium_row,
                                    quote_volume_rank=rank,
                                    universe_sha256=(
                                        continuation_universe_sha256
                                    ),
                                )
                            )
                        continuation_observations = candidate_continuation
                except MicroObservationError:
                    if stop_event.is_set():
                        raise
                    continuation_reason = "OPTIONAL_KLINE_UNAVAILABLE"
            if continuation_reason is not None:
                logger = getattr(self, "logger", None)
                if logger is not None:
                    logger.warning(
                        "Micro N22 continuation unavailable; committing the "
                        "authenticated current Top100 frame without interval "
                        "proof | reason=%s departed_count=%s",
                        continuation_reason,
                        len(departed_symbols),
                    )
        return MicroObservationSample(
            observations,
            continuation_observations,
        )

    def _fetch_micro_kline_generation(
        self,
        symbols,
        stop_event,
        deadline_monotonic: float,
    ):
        """Fetch one exact symbol set with bounded I/O concurrency and time."""

        client = self._micro_sampler_client
        symbol_tuple = tuple(symbols)
        if (
            client is None
            or not symbol_tuple
            or len(set(symbol_tuple)) != len(symbol_tuple)
            or any(type(symbol) is not str or not symbol for symbol in symbol_tuple)
        ):
            raise MicroObservationError("micro Kline generation is invalid")
        if stop_event.is_set():
            raise MicroObservationError("micro sampler is stopping")
        remaining = deadline_monotonic - time.monotonic()
        if remaining <= 0:
            raise MicroObservationError("micro Kline generation exceeded its budget")

        executor = ThreadPoolExecutor(
            max_workers=min(_MICRO_KLINE_MAX_WORKERS, len(symbol_tuple)),
            thread_name_prefix="micro-kline",
        )
        futures = {}
        rows_by_symbol = {}
        observed_at_ms = {}
        failed = True
        slots = getattr(self, "_kline_request_slots", None)
        if slots is None:
            slots = threading.BoundedSemaphore(_STRATEGY_KLINE_MAX_WORKERS)

        def fetch(symbol):
            with slots:
                return client.get_klines(symbol)

        try:
            for symbol in symbol_tuple:
                if stop_event.is_set():
                    raise MicroObservationError("micro sampler is stopping")
                futures[executor.submit(fetch, symbol)] = symbol
            pending = set(futures)
            while pending:
                if stop_event.is_set():
                    raise MicroObservationError("micro sampler is stopping")
                remaining = deadline_monotonic - time.monotonic()
                if remaining <= 0:
                    raise MicroObservationError(
                        "micro Kline generation exceeded its budget"
                    )
                done, pending = wait(
                    pending,
                    timeout=min(_MICRO_SAMPLE_WAIT_SLICE_SECONDS, remaining),
                    return_when=FIRST_COMPLETED,
                )
                for future in done:
                    symbol = futures[future]
                    try:
                        rows_by_symbol[symbol] = future.result()
                    except Exception as exc:
                        raise MicroObservationError(
                            "micro Kline generation request failed"
                        ) from exc
                    observed_at_ms[symbol] = int(time.time() * 1000)
            if set(rows_by_symbol) != set(symbol_tuple):
                raise MicroObservationError(
                    "micro Kline generation is incomplete"
                )
            if time.monotonic() > deadline_monotonic:
                raise MicroObservationError(
                    "micro Kline generation exceeded its budget"
                )
            failed = False
            return rows_by_symbol, observed_at_ms
        finally:
            if failed:
                for future in futures:
                    future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)

    def _set_symbol_cooldown(self, symbol: str, exit_reason: str, trade_id: int | None) -> None:
        cooldown_until = datetime.now(timezone.utc) + timedelta(hours=self.config.symbol_cooldown_hours)
        cooldown_recorded = self.recorder.set_symbol_cooldown(symbol, cooldown_until, exit_reason, trade_id)
        if not cooldown_recorded:
            raise RuntimeError(f"Failed to set symbol cooldown for {symbol}")
        self.logger.info(
            "Symbol cooldown set | symbol=%s until=%s hours=%s reason=%s trade_id=%s",
            symbol,
            cooldown_until.isoformat(),
            self.config.symbol_cooldown_hours,
            exit_reason,
            trade_id,
        )

    def run_once(self) -> None:
        if self.config.multi_strategy_enabled:
            self._run_once_multi_strategy()
        else:
            self._run_once_single_strategy()

    def _signal_batch_is_current(self, scan_id: int) -> bool:
        try:
            current_scan_id = self.recorder.current_strategy_signal_scan_id()
        except Exception as exc:
            self.logger.error(
                "Unable to attest current strategy signal batch; blocking "
                "execution | scan_id=%s error=%s",
                scan_id,
                exc,
            )
            return False
        if type(current_scan_id) is not int or current_scan_id != scan_id:
            self.logger.error(
                "Strategy signal batch is not the current published batch; "
                "blocking execution | scan_id=%s current_scan_id=%s",
                scan_id,
                current_scan_id,
            )
            return False
        return True

    @staticmethod
    def _n16_plan_with_published_identity(
        signal: StrategySignalDecision,
        plan: TradePlan,
    ) -> TradePlan:
        """Bind and validate every bidirectional N16 execution marker."""

        analysis = signal.analysis
        structure_context = (
            dict(plan.structure_context)
            if type(plan.structure_context) is dict
            else None
        )
        context_strategy_id = (
            structure_context.get("strategy_id")
            if structure_context is not None
            else None
        )
        n16_marked = any(
            (
                signal.strategy.strategy_id == "N16",
                signal.strategy.stop_mode == _N16_STOP_MODE,
                plan.stop_mode == _N16_STOP_MODE,
                context_strategy_id == "N16",
            )
        )
        if not n16_marked:
            return plan
        if (
            signal.strategy.strategy_id != "N16"
            or signal.strategy.stop_mode != _N16_STOP_MODE
            or type(signal.signal_id) is not int
            or signal.signal_id <= 0
            or not isinstance(analysis, N16AnalysisResult)
            or type(analysis.structure_id) is not str
            or plan.symbol != signal.candidate.symbol
            or plan.structure_id != analysis.structure_id
            or plan.stop_mode != _N16_STOP_MODE
            or structure_context is None
            or context_strategy_id != "N16"
        ):
            raise _N16PlanIntegrityError(
                "N16_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
            )
        context_signal_id = structure_context.get("signal_id")
        if (
            structure_context.get("structure_id") != analysis.structure_id
            or (
                context_signal_id is not None
                and (
                    type(context_signal_id) is not int
                    or context_signal_id != signal.signal_id
                )
            )
        ):
            raise _N16PlanIntegrityError(
                "N16_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
            )
        structure_context["signal_id"] = signal.signal_id
        bound = replace(plan, structure_context=structure_context)
        if (
            bound.structure_context.get("strategy_id") != "N16"
            or type(bound.structure_context.get("signal_id")) is not int
            or bound.structure_context.get("signal_id") != signal.signal_id
            or bound.structure_context.get("structure_id")
            != analysis.structure_id
        ):
            raise _N16PlanIntegrityError(
                "N16_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
            )
        return bound

    @staticmethod
    def _n17_plan_with_published_identity(
        signal: StrategySignalDecision,
        plan: TradePlan,
    ) -> TradePlan:
        analysis = signal.analysis
        context = (
            dict(plan.structure_context)
            if type(plan.structure_context) is dict
            else None
        )
        marked = any(
            (
                signal.strategy.strategy_id == "N17",
                signal.strategy.stop_mode == _N17_STOP_MODE,
                plan.stop_mode == _N17_STOP_MODE,
                context is not None and context.get("strategy_id") == "N17",
            )
        )
        if not marked:
            return plan
        if (
            signal.strategy.strategy_id != "N17"
            or signal.strategy.stop_mode != _N17_STOP_MODE
            or type(signal.signal_id) is not int
            or signal.signal_id <= 0
            or not isinstance(analysis, N17AnalysisResult)
            or type(analysis.structure_id) is not str
            or len(analysis.structure_id) != 24
            or plan.symbol != signal.candidate.symbol
            or plan.structure_id != analysis.structure_id
            or plan.stop_mode != _N17_STOP_MODE
            or context is None
            or context.get("strategy_id") != "N17"
            or context.get("rule_version") != "N17_V1"
            or context.get("structure_id") != analysis.structure_id
        ):
            raise _N17PlanIntegrityError(
                "N17_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
            )
        existing_signal_id = context.get("signal_id")
        if existing_signal_id is not None and (
            type(existing_signal_id) is not int
            or existing_signal_id != signal.signal_id
        ):
            raise _N17PlanIntegrityError(
                "N17_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
            )
        context["signal_id"] = signal.signal_id
        return replace(plan, structure_context=context)

    @staticmethod
    def _n19_plan_with_published_identity(
        signal: StrategySignalDecision,
        plan: TradePlan,
    ) -> TradePlan:
        analysis = signal.analysis
        context = (
            dict(plan.structure_context)
            if type(plan.structure_context) is dict
            else None
        )
        marked = any(
            (
                signal.strategy.strategy_id == "N19",
                signal.strategy.stop_mode == _N19_STOP_MODE,
                plan.stop_mode == _N19_STOP_MODE,
                context is not None and context.get("strategy_id") == "N19",
            )
        )
        if not marked:
            return plan
        if (
            signal.strategy.strategy_id != "N19"
            or signal.strategy.stop_mode != _N19_STOP_MODE
            or type(signal.signal_id) is not int
            or signal.signal_id <= 0
            or not isinstance(analysis, N19AnalysisResult)
            or type(analysis.structure_id) is not str
            or len(analysis.structure_id) != 24
            or plan.symbol != signal.candidate.symbol
            or plan.structure_id != analysis.structure_id
            or plan.stop_mode != _N19_STOP_MODE
            or context is None
            or context.get("strategy_id") != "N19"
            or context.get("rule_version") != "N19_V1"
            or context.get("structure_id") != analysis.structure_id
        ):
            raise _N19PlanIntegrityError(
                "N19_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
            )
        existing_signal_id = context.get("signal_id")
        if existing_signal_id is not None and (
            type(existing_signal_id) is not int
            or existing_signal_id != signal.signal_id
        ):
            raise _N19PlanIntegrityError(
                "N19_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
            )
        context["signal_id"] = signal.signal_id
        return replace(plan, structure_context=context)

    @staticmethod
    def _n18_plan_with_published_identity(
        signal: StrategySignalDecision,
        plan: TradePlan,
    ) -> TradePlan:
        analysis = signal.analysis
        context = (
            dict(plan.structure_context)
            if type(plan.structure_context) is dict else None
        )
        marked = any((
            signal.strategy.strategy_id == "N18",
            signal.strategy.stop_mode == _N18_STOP_MODE,
            plan.stop_mode == _N18_STOP_MODE,
            context is not None and context.get("strategy_id") == "N18",
        ))
        if not marked:
            return plan
        if (
            signal.strategy.strategy_id != "N18"
            or signal.strategy.stop_mode != _N18_STOP_MODE
            or type(signal.signal_id) is not int or signal.signal_id <= 0
            or not isinstance(analysis, N18AnalysisResult)
            or type(analysis.structure_id) is not str
            or len(analysis.structure_id) != 24
            or plan.symbol != signal.candidate.symbol
            or plan.structure_id != analysis.structure_id
            or plan.stop_mode != _N18_STOP_MODE
            or context is None
            or context.get("strategy_id") != "N18"
            or context.get("rule_version") != "N18_V1"
            or context.get("structure_id") != analysis.structure_id
        ):
            raise _N18PlanIntegrityError(
                "N18_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
            )
        existing_signal_id = context.get("signal_id")
        if existing_signal_id is not None and (
            type(existing_signal_id) is not int
            or existing_signal_id != signal.signal_id
        ):
            raise _N18PlanIntegrityError(
                "N18_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
            )
        context["signal_id"] = signal.signal_id
        return replace(plan, structure_context=context)

    @staticmethod
    def _n20_plan_with_published_identity(
        signal: StrategySignalDecision,
        plan: TradePlan,
    ) -> TradePlan:
        analysis = signal.analysis
        context = dict(plan.structure_context) if type(plan.structure_context) is dict else None
        marked = any((
            signal.strategy.strategy_id == "N20",
            signal.strategy.stop_mode == _N20_STOP_MODE,
            plan.stop_mode == _N20_STOP_MODE,
            context is not None and context.get("strategy_id") == "N20",
        ))
        if not marked:
            return plan
        if (
            signal.strategy.strategy_id != "N20"
            or signal.strategy.stop_mode != _N20_STOP_MODE
            or type(signal.signal_id) is not int or signal.signal_id <= 0
            or not isinstance(analysis, N20AnalysisResult)
            or type(analysis.structure_id) is not str or len(analysis.structure_id) != 24
            or analysis.winner is None or analysis.winner.symbol != signal.candidate.symbol
            or plan.symbol != signal.candidate.symbol
            or plan.structure_id != analysis.structure_id
            or plan.stop_mode != _N20_STOP_MODE
            or context is None or context.get("strategy_id") != "N20"
            or context.get("rule_version") != "N20_V1"
            or context.get("structure_id") != analysis.structure_id
        ):
            raise _N20PlanIntegrityError(
                "N20_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
            )
        existing_signal_id = context.get("signal_id")
        if existing_signal_id is not None and (
            type(existing_signal_id) is not int or existing_signal_id != signal.signal_id
        ):
            raise _N20PlanIntegrityError(
                "N20_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
            )
        context["signal_id"] = signal.signal_id
        return replace(plan, structure_context=context)

    @staticmethod
    def _micro_plan_with_published_identity(
        signal: StrategySignalDecision,
        plan: TradePlan,
    ) -> TradePlan:
        analysis = signal.analysis
        context = (
            dict(plan.structure_context)
            if type(plan.structure_context) is dict
            else None
        )
        markers = (
            signal.strategy.strategy_id in MICRO_STRATEGY_IDS,
            signal.strategy.stop_mode == _MICRO_STOP_MODE,
            plan.stop_mode == _MICRO_STOP_MODE,
            context is not None
            and context.get("strategy_id") in MICRO_STRATEGY_IDS,
        )
        if not any(markers):
            return plan
        if (
            not all(markers)
            or type(signal.signal_id) is not int
            or signal.signal_id <= 0
            or not isinstance(analysis, MicroAnalysisResult)
            or not analysis.passed
            or analysis.structure is None
            or analysis.structure.strategy_id != signal.strategy.strategy_id
            or analysis.structure.symbol != signal.candidate.symbol
            or type(analysis.structure_id) is not str
            or len(analysis.structure_id) != 24
            or plan.symbol != signal.candidate.symbol
            or plan.structure_id != analysis.structure_id
            or context is None
            or context.get("strategy_id") != signal.strategy.strategy_id
            or context.get("rule_version")
            != f"{signal.strategy.strategy_id}_V1"
            or context.get("structure_id") != analysis.structure_id
        ):
            raise _MicroPlanIntegrityError(
                "MICRO_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
            )
        existing_signal_id = context.get("signal_id")
        if existing_signal_id is not None and (
            type(existing_signal_id) is not int
            or existing_signal_id != signal.signal_id
        ):
            raise _MicroPlanIntegrityError(
                "MICRO_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
            )
        context["signal_id"] = signal.signal_id
        return replace(plan, structure_context=context)

    def _n16_execution_boundary_ready(self) -> bool:
        """Attest the durable publication phase before any side effect."""

        checker = getattr(self.recorder, "assert_n16_execution_ready", None)
        # Production construction always uses ReviewRecorder.  Lightweight
        # test doubles predating the independent ledger may omit the method;
        # never let a real recorder bypass it.
        if checker is None:
            if isinstance(self.recorder, ReviewRecorder):
                self.logger.error(
                    "N16 publication boundary is unavailable; blocking this round."
                )
                return False
            return True
        try:
            checker()
            return True
        except Exception as exc:
            self.logger.error(
                "N16 publication boundary is unresolved; blocking all paper, "
                "live and exchange activity for this round | error=%s",
                exc,
            )
            return False

    def _n16_current_claims_ready(
        self,
        scan_id: int,
        passed_signals: list[StrategySignalDecision],
    ) -> bool:
        identities = []
        for signal in passed_signals:
            strategy_id_marked = signal.strategy.strategy_id == "N16"
            stop_mode_marked = signal.strategy.stop_mode == _N16_STOP_MODE
            if strategy_id_marked != stop_mode_marked:
                self.logger.error(
                    "N16 current execution markers conflict; blocking this round."
                )
                return False
            if not strategy_id_marked:
                continue
            analysis = signal.analysis
            structure_id = (
                analysis.structure_id
                if isinstance(analysis, N16AnalysisResult)
                else None
            )
            if (
                type(signal.signal_id) is not int
                or signal.signal_id <= 0
                or type(signal.candidate.symbol) is not str
                or type(structure_id) is not str
            ):
                self.logger.error(
                    "N16 current execution claim identity is invalid; "
                    "blocking this round."
                )
                return False
            identities.append(
                (signal.signal_id, signal.candidate.symbol, structure_id)
            )
        checker = getattr(
            self.recorder, "assert_n16_current_scan_claims", None
        )
        if checker is None:
            if isinstance(self.recorder, ReviewRecorder):
                self.logger.error(
                    "N16 current claim attestation is unavailable; "
                    "blocking this round."
                )
                return False
            return True
        try:
            checker(scan_id, tuple(identities))
            return True
        except Exception as exc:
            self.logger.error(
                "N16 current claim attestation failed; blocking all paper, "
                "live and exchange activity | scan_id=%s error=%s",
                scan_id,
                exc,
            )
            return False

    def _n16_local_state_claim_identity(
        self,
        local_state,
    ) -> tuple[int, str, str, bool] | None:
        """Return one strict N16 identity or reject conflicting N16 markers."""

        if local_state is None:
            return None
        if type(local_state) is not PositionState:
            raise RuntimeError("local execution state type is invalid")
        orders = local_state.orders
        strategy = orders.get("strategy")
        plan = orders.get("plan")
        pretrade_plan = orders.get("pretrade_plan")
        phase_keys = tuple(
            key
            for key in (
                "execution_pending",
                "execution_cleanup_resolved",
                "emergency_cleanup_pending",
            )
            if key in orders
        )
        phase_payloads = tuple(orders.get(key) for key in phase_keys)
        strategy_id = (
            strategy.get("strategy_id") if type(strategy) is dict else None
        )
        marker_values = [strategy_id]
        stop_modes = []
        candidate_claim_identities = []
        if type(strategy) is dict:
            candidate_claim_identities.append(
                (strategy.get("signal_id"), strategy.get("structure_id"))
            )
        for payload in (plan, pretrade_plan) + phase_payloads:
            if type(payload) is dict:
                marker_values.append(payload.get("strategy_id"))
                stop_modes.append(payload.get("stop_mode"))
                candidate_claim_identities.append(
                    (payload.get("signal_id"), payload.get("structure_id"))
                )
                context = payload.get("structure_context")
                if type(context) is dict:
                    marker_values.append(context.get("strategy_id"))
                    candidate_claim_identities.append(
                        (
                            context.get("signal_id"),
                            context.get("structure_id"),
                        )
                    )
        n16_marked = "N16" in marker_values or (
            "trend_support_continuation_margin_capped" in stop_modes
        )
        if not n16_marked:
            marker_checker = getattr(
                self.recorder,
                "n16_execution_identity_is_claimed",
                None,
            )
            if marker_checker is not None:
                for signal_marker, structure_marker in candidate_claim_identities:
                    if signal_marker is None and structure_marker is None:
                        continue
                    if marker_checker(signal_marker, structure_marker):
                        n16_marked = True
                        break
        if not n16_marked:
            return None
        if (
            type(local_state.symbol) is not str
            or not local_state.symbol
            or local_state.symbol.strip() != local_state.symbol
            or len(local_state.symbol) > 64
            or type(local_state.leverage) is not int
            or local_state.leverage <= 0
            or type(local_state.dry_run) is not bool
            or type(local_state.opened_at) is not str
            or not 1 <= len(local_state.opened_at) <= 64
            or type(local_state.orders) is not dict
        ):
            raise RuntimeError("N16 local position state shape is invalid")
        try:
            opened_at = datetime.fromisoformat(
                local_state.opened_at.replace("Z", "+00:00")
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "N16 local position timestamp is invalid"
            ) from exc
        if (
            opened_at.tzinfo is None
            or opened_at.astimezone(timezone.utc).isoformat()
            != local_state.opened_at
        ):
            raise RuntimeError("N16 local position timestamp is invalid")
        for field_name in (
            "quantity",
            "entry_price",
            "stop_loss_price",
            "take_profit_price",
        ):
            raw_value = getattr(local_state, field_name)
            if type(raw_value) is not str or not 1 <= len(raw_value) <= 128:
                raise RuntimeError(
                    "N16 local position numeric evidence is invalid"
                )
            try:
                number = Decimal(raw_value)
            except Exception as exc:
                raise RuntimeError(
                    "N16 local position numeric evidence is invalid"
                ) from exc
            if (
                not number.is_finite()
                or (
                    number != 0
                    if field_name == "quantity"
                    and phase_keys == ("execution_cleanup_resolved",)
                    else number <= 0
                )
                or decimal_to_api(number) != raw_value
            ):
                raise RuntimeError(
                    "N16 local position numeric evidence is invalid"
                )
        final_order_keys = (
            "pretrade_plan",
            "open",
            "stop",
            "take_profit",
        )
        if len(phase_keys) > 1:
            raise RuntimeError("N16 local execution phases conflict")
        if phase_keys:
            if any(key in orders for key in final_order_keys):
                raise RuntimeError(
                    "N16 pending execution contains final order evidence"
                )
        elif any(
            key not in orders or type(orders.get(key)) is not dict
            for key in final_order_keys
        ):
            raise RuntimeError("N16 final live execution evidence is incomplete")
        if (
            type(strategy) is not dict
            or strategy_id != "N16"
            or type(local_state.symbol) is not str
            or type(strategy.get("signal_id")) is not int
            or strategy["signal_id"] <= 0
            or type(strategy.get("structure_id")) is not str
            or len(strategy["structure_id"]) != 24
        ):
            raise RuntimeError("N16 local execution strategy identity is invalid")
        signal_id = strategy["signal_id"]
        structure_id = strategy["structure_id"]

        def validate_plan_payload(payload, label: str) -> None:
            if type(payload) is not dict:
                raise RuntimeError(
                    f"N16 local execution {label} identity conflicts"
                )
            context = payload.get("structure_context")
            if (
                payload.get("stop_mode")
                != "trend_support_continuation_margin_capped"
                or payload.get("structure_id") != structure_id
                or (
                    "strategy_id" in payload
                    and payload.get("strategy_id") != "N16"
                )
                or (
                    "signal_id" in payload
                    and (
                        type(payload.get("signal_id")) is not int
                        or payload.get("signal_id") != signal_id
                    )
                )
                or type(context) is not dict
                or context.get("strategy_id") != "N16"
                or type(context.get("signal_id")) is not int
                or context.get("signal_id") != signal_id
                or context.get("structure_id") != structure_id
            ):
                raise RuntimeError(
                    f"N16 local execution {label} identity conflicts"
                )

        validate_plan_payload(plan, "plan")
        if pretrade_plan is not None:
            validate_plan_payload(pretrade_plan, "pretrade plan")
        for payload in phase_payloads:
            if type(payload) is not dict or (
                payload.get("strategy_id") != "N16"
                or type(payload.get("signal_id")) is not int
                or payload.get("signal_id") != signal_id
                or payload.get("structure_id") != structure_id
                or payload.get("stop_mode")
                != "trend_support_continuation_margin_capped"
            ):
                raise RuntimeError("N16 local execution phase identity conflicts")
        return signal_id, local_state.symbol, structure_id, bool(phase_keys)

    def _n16_local_execution_state_ready(
        self,
        local_state=_LOCAL_STATE_UNSET,
    ) -> bool:
        """Authenticate one exact N16 local pending/live state."""

        if local_state is _LOCAL_STATE_UNSET:
            try:
                local_state = self.state.load()
            except Exception as exc:
                self.logger.error(
                    "Local execution state cannot be read before N16 claim "
                    "attestation; blocking this round | error=%s",
                    exc,
                )
                return False
        active_checker = getattr(
            self.recorder, "n16_active_live_claim_identity", None
        )
        active_identity_unavailable = active_checker is None
        if active_identity_unavailable:
            if isinstance(self.recorder, ReviewRecorder):
                self.logger.error(
                    "N16 active live claim attestation is unavailable; "
                    "blocking this round."
                )
                return False
            active_live_identity = None
        else:
            try:
                active_live_identity = active_checker()
            except Exception as exc:
                self.logger.error(
                    "N16 active live claim attestation failed; blocking all "
                    "paper, live and exchange activity | error=%s",
                    exc,
                )
                return False
        if local_state is None:
            return active_live_identity is None
        link_marker_checker = getattr(
            self.recorder,
            "n16_live_link_marker_for_state",
            None,
        )
        if link_marker_checker is None:
            if isinstance(self.recorder, ReviewRecorder):
                self.logger.error(
                    "N16 local live-link reverse attestation is unavailable; "
                    "blocking this round."
                )
                return False
            local_live_link_is_n16 = False
        else:
            try:
                local_live_link_is_n16 = link_marker_checker(
                    local_state.symbol,
                    local_state.opened_at,
                )
            except Exception as exc:
                self.logger.error(
                    "N16 local live-link reverse attestation failed; blocking "
                    "all paper, live and exchange activity | error=%s",
                    exc,
                )
                return False
        try:
            identity = self._n16_local_state_claim_identity(local_state)
        except Exception as exc:
            self.logger.error(
                "N16 local execution markers are inconsistent; blocking all "
                "paper, live and exchange activity | error=%s",
                exc,
            )
            return False
        if identity is None:
            if active_live_identity is not None or local_live_link_is_n16:
                self.logger.error(
                    "N16 live claim has no matching local state identity; "
                    "blocking this round."
                )
                return False
            return True
        signal_id, symbol, structure_id, pending_phase = identity
        checker = getattr(
            self.recorder, "assert_n16_execution_claim", None
        )
        if checker is None:
            if isinstance(self.recorder, ReviewRecorder):
                self.logger.error(
                    "N16 local execution claim attestation is unavailable; "
                    "blocking this round."
                )
                return False
            return True
        try:
            checker(signal_id, symbol, structure_id)
            expected_identity = (signal_id, symbol, structure_id)
            if active_live_identity is None:
                if pending_phase and local_live_link_is_n16:
                    raise RuntimeError(
                        "N16 pending local state conflicts with a durable live link"
                    )
                if not pending_phase and not active_identity_unavailable:
                    inspector = getattr(
                        self.recorder,
                        "inspect_strategy_live_finalization",
                        None,
                    )
                    if inspector is None:
                        raise RuntimeError(
                            "N16 final recovery attestation is unavailable"
                        )
                    evidence = inspector(
                        local_state,
                        "N16",
                        allow_dry_run_pending=True,
                    )
                    evidence_status = getattr(evidence, "status", None)
                    if evidence_status == "FINAL":
                        if not local_live_link_is_n16:
                            raise RuntimeError(
                                "N16 final state is missing its durable link marker"
                            )
                    elif evidence_status == "REVIEW_ONLY_OPENED":
                        if not local_live_link_is_n16:
                            raise RuntimeError(
                                "N16 Review-only state is missing its durable marker"
                            )
                    elif evidence_status == "MISSING_OPEN_AUDIT":
                        if local_live_link_is_n16:
                            raise RuntimeError(
                                "N16 missing-audit state conflicts with durable evidence"
                            )
                    else:
                        raise RuntimeError(
                            "N16 local live state lacks exact recoverable evidence"
                        )
            elif active_live_identity != expected_identity:
                raise RuntimeError(
                    "N16 active live claim and local state disagree"
                )
            else:
                inspector = getattr(
                    self.recorder,
                    "inspect_strategy_live_finalization",
                    None,
                )
                if inspector is None:
                    raise RuntimeError(
                        "N16 active live state attestation is unavailable"
                    )
                evidence = inspector(
                    local_state,
                    "N16",
                    allow_dry_run_pending=True,
                )
                if getattr(evidence, "status", None) not in {
                    "PENDING",
                    "REVIEW_ONLY_OPENED",
                    "RECOVERABLE_FINAL_REVIEW",
                }:
                    raise RuntimeError(
                        "N16 active live claim does not match local state"
                    )
            return True
        except Exception as exc:
            self.logger.error(
                "N16 local execution claim attestation failed; blocking all "
                "paper, live and exchange activity | error=%s",
                exc,
            )
            return False

    def _attested_local_execution_state(self):
        try:
            local_state = self.state.load()
        except Exception as exc:
            self.logger.error(
                "Local execution state cannot be read immediately before "
                "reconciliation; blocking this round | error=%s",
                exc,
            )
            return False, None
        ready = (
            self._n16_local_execution_state_ready(local_state)
            and self._n17_local_execution_state_ready(local_state)
            and self._n18_local_execution_state_ready(local_state)
            and self._n19_local_execution_state_ready(local_state)
            and self._n20_local_execution_state_ready(local_state)
            and self._micro_local_execution_state_ready(local_state)
        )
        if ready and local_state is not None and type(local_state.orders) is dict:
            cleanup_keys = tuple(
                key for key in (
                    "execution_cleanup_resolved", "emergency_cleanup_pending"
                ) if key in local_state.orders
            )
            strategy = local_state.orders.get("strategy")
            if cleanup_keys and type(strategy) is dict:
                strategy_id = strategy.get("strategy_id")
                try:
                    if type(strategy_id) is not str:
                        raise RuntimeError("cleanup strategy identity is invalid")
                    self.recorder.assert_no_active_live_link_for_cleanup(
                        strategy_id, local_state.symbol
                    )
                except Exception as exc:
                    self.logger.error(
                        "Pre-live cleanup graph attestation failed; blocking all "
                        "paper, live and exchange activity | error=%s",
                        exc,
                    )
                    ready = False
        return ready, local_state

    def _n17_local_execution_state_ready(self, local_state) -> bool:
        if local_state is None:
            return True
        try:
            orders = local_state.orders
            if type(orders) is not dict:
                return True
            strategy = orders.get("strategy")
            plan = orders.get("plan")
            pretrade = orders.get("pretrade_plan")
            phase_keys = tuple(
                key
                for key in (
                    "execution_pending",
                    "execution_cleanup_resolved",
                    "emergency_cleanup_pending",
                )
                if key in orders
            )
            payloads = tuple(
                item
                for item in (strategy, plan, pretrade)
                + tuple(orders.get(key) for key in phase_keys)
                if type(item) is dict
            )
            marked = any(item.get("strategy_id") == "N17" for item in payloads)
            marked = marked or any(
                item.get("stop_mode") == _N17_STOP_MODE for item in payloads
            )
            for item in (plan, pretrade):
                context = item.get("structure_context") if type(item) is dict else None
                if type(context) is dict and context.get("strategy_id") == "N17":
                    marked = True
            if not marked:
                return True
            if type(strategy) is not dict or strategy.get("strategy_id") != "N17":
                raise RuntimeError("N17 local strategy marker conflicts")
            signal_id = strategy.get("signal_id")
            structure_id = strategy.get("structure_id")
            if (
                type(signal_id) is not int
                or signal_id <= 0
                or type(structure_id) is not str
                or len(structure_id) != 24
                or len(phase_keys) > 1
            ):
                raise RuntimeError("N17 local claim identity is invalid")
            for label, item in (("plan", plan), ("pretrade_plan", pretrade)):
                if item is None:
                    if label == "plan":
                        raise RuntimeError("N17 local plan is missing")
                    continue
                context = item.get("structure_context") if type(item) is dict else None
                if (
                    type(item) is not dict
                    or item.get("stop_mode") != _N17_STOP_MODE
                    or item.get("structure_id") != structure_id
                    or type(context) is not dict
                    or context.get("strategy_id") != "N17"
                    or context.get("rule_version") != "N17_V1"
                    or context.get("signal_id") != signal_id
                    or context.get("structure_id") != structure_id
                ):
                    raise RuntimeError(f"N17 local {label} identity conflicts")
            for key in phase_keys:
                phase = orders[key]
                if (
                    type(phase) is not dict
                    or phase.get("strategy_id") != "N17"
                    or phase.get("signal_id") != signal_id
                    or phase.get("structure_id") != structure_id
                    or phase.get("stop_mode") != _N17_STOP_MODE
                ):
                    raise RuntimeError("N17 local phase identity conflicts")
            self.recorder.assert_n17_execution_claim(
                signal_id, local_state.symbol, structure_id
            )
            return True
        except Exception as exc:
            self.logger.error(
                "N17 local execution claim attestation failed; blocking all "
                "paper, live and exchange activity | error=%s",
                exc,
            )
            return False

    def _n19_local_execution_state_ready(self, local_state) -> bool:
        if local_state is None:
            return True
        try:
            orders = local_state.orders
            if type(orders) is not dict:
                return True
            strategy = orders.get("strategy")
            plan = orders.get("plan")
            pretrade = orders.get("pretrade_plan")
            phase_keys = tuple(
                key
                for key in (
                    "execution_pending",
                    "execution_cleanup_resolved",
                    "emergency_cleanup_pending",
                )
                if key in orders
            )
            payloads = tuple(
                item
                for item in (strategy, plan, pretrade)
                + tuple(orders.get(key) for key in phase_keys)
                if type(item) is dict
            )
            marked = any(item.get("strategy_id") == "N19" for item in payloads)
            marked = marked or any(
                item.get("stop_mode") == _N19_STOP_MODE for item in payloads
            )
            for item in (plan, pretrade):
                context = item.get("structure_context") if type(item) is dict else None
                if type(context) is dict and context.get("strategy_id") == "N19":
                    marked = True
            if not marked:
                return True
            if type(strategy) is not dict or strategy.get("strategy_id") != "N19":
                raise RuntimeError("N19 local strategy marker conflicts")
            signal_id = strategy.get("signal_id")
            structure_id = strategy.get("structure_id")
            if (
                type(signal_id) is not int
                or signal_id <= 0
                or type(structure_id) is not str
                or len(structure_id) != 24
                or len(phase_keys) > 1
            ):
                raise RuntimeError("N19 local claim identity is invalid")
            for label, item in (("plan", plan), ("pretrade_plan", pretrade)):
                if item is None:
                    if label == "plan":
                        raise RuntimeError("N19 local plan is missing")
                    continue
                context = item.get("structure_context") if type(item) is dict else None
                if (
                    type(item) is not dict
                    or item.get("stop_mode") != _N19_STOP_MODE
                    or item.get("structure_id") != structure_id
                    or type(context) is not dict
                    or context.get("strategy_id") != "N19"
                    or context.get("rule_version") != "N19_V1"
                    or type(context.get("signal_id")) is not int
                    or context.get("signal_id") != signal_id
                    or context.get("structure_id") != structure_id
                ):
                    raise RuntimeError(f"N19 local {label} identity conflicts")
            for key in phase_keys:
                phase = orders[key]
                if (
                    type(phase) is not dict
                    or phase.get("strategy_id") != "N19"
                    or type(phase.get("signal_id")) is not int
                    or phase.get("signal_id") != signal_id
                    or phase.get("structure_id") != structure_id
                    or phase.get("stop_mode") != _N19_STOP_MODE
                ):
                    raise RuntimeError("N19 local phase identity conflicts")
            self.recorder.assert_n19_execution_claim(
                signal_id, local_state.symbol, structure_id
            )
            return True
        except Exception as exc:
            self.logger.error(
                "N19 local execution claim attestation failed; blocking all "
                "paper, live and exchange activity | error=%s",
                exc,
            )
            return False

    def _n20_local_execution_state_ready(self, local_state) -> bool:
        if local_state is None:
            return True
        try:
            orders = local_state.orders
            if type(orders) is not dict:
                return True
            strategy = orders.get("strategy")
            plan = orders.get("plan")
            pretrade = orders.get("pretrade_plan")
            phase_keys = tuple(
                key for key in (
                    "execution_pending", "execution_cleanup_resolved",
                    "emergency_cleanup_pending",
                ) if key in orders
            )
            payloads = tuple(
                item for item in (strategy, plan, pretrade)
                + tuple(orders.get(key) for key in phase_keys)
                if type(item) is dict
            )
            marked = any(item.get("strategy_id") == "N20" for item in payloads)
            marked = marked or any(item.get("stop_mode") == _N20_STOP_MODE for item in payloads)
            for item in (plan, pretrade):
                context = item.get("structure_context") if type(item) is dict else None
                if type(context) is dict and context.get("strategy_id") == "N20":
                    marked = True
            if not marked:
                return True
            if type(strategy) is not dict or strategy.get("strategy_id") != "N20":
                raise RuntimeError("N20 local strategy marker conflicts")
            signal_id = strategy.get("signal_id")
            structure_id = strategy.get("structure_id")
            if (
                type(signal_id) is not int or signal_id <= 0
                or type(structure_id) is not str or len(structure_id) != 24
                or len(phase_keys) > 1
            ):
                raise RuntimeError("N20 local claim identity is invalid")
            for label, item in (("plan", plan), ("pretrade_plan", pretrade)):
                if item is None:
                    if label == "plan":
                        raise RuntimeError("N20 local plan is missing")
                    continue
                context = item.get("structure_context") if type(item) is dict else None
                if (
                    type(item) is not dict or item.get("stop_mode") != _N20_STOP_MODE
                    or item.get("structure_id") != structure_id
                    or type(context) is not dict
                    or context.get("strategy_id") != "N20"
                    or context.get("rule_version") != "N20_V1"
                    or type(context.get("signal_id")) is not int
                    or context.get("signal_id") != signal_id
                    or context.get("structure_id") != structure_id
                ):
                    raise RuntimeError(f"N20 local {label} identity conflicts")
            for key in phase_keys:
                phase = orders[key]
                if (
                    type(phase) is not dict or phase.get("strategy_id") != "N20"
                    or type(phase.get("signal_id")) is not int
                    or phase.get("signal_id") != signal_id
                    or phase.get("structure_id") != structure_id
                    or phase.get("stop_mode") != _N20_STOP_MODE
                ):
                    raise RuntimeError("N20 local phase identity conflicts")
            self.recorder.assert_n20_execution_claim(
                signal_id, local_state.symbol, structure_id
            )
            return True
        except Exception as exc:
            self.logger.error(
                "N20 local execution claim attestation failed; blocking all "
                "paper, live and exchange activity | error=%s",
                exc,
            )
            return False

    def _micro_local_execution_state_ready(self, local_state) -> bool:
        if local_state is None:
            return True
        try:
            orders = local_state.orders
            if type(orders) is not dict:
                return True
            strategy = orders.get("strategy")
            plan = orders.get("plan")
            pretrade = orders.get("pretrade_plan")
            phase_keys = tuple(
                key
                for key in (
                    "execution_pending",
                    "execution_cleanup_resolved",
                    "emergency_cleanup_pending",
                )
                if key in orders
            )
            payloads = tuple(
                item
                for item in (strategy, plan, pretrade)
                + tuple(orders.get(key) for key in phase_keys)
                if type(item) is dict
            )
            marked_ids = {
                item.get("strategy_id")
                for item in payloads
                if item.get("strategy_id") in MICRO_STRATEGY_IDS
            }
            marked = bool(marked_ids) or any(
                item.get("stop_mode") == _MICRO_STOP_MODE for item in payloads
            )
            for item in (plan, pretrade):
                context = (
                    item.get("structure_context")
                    if type(item) is dict
                    else None
                )
                if (
                    type(context) is dict
                    and context.get("strategy_id") in MICRO_STRATEGY_IDS
                ):
                    marked = True
                    marked_ids.add(context["strategy_id"])
            if not marked:
                return True
            if len(marked_ids) != 1:
                raise RuntimeError("micro local strategy marker conflicts")
            strategy_id = next(iter(marked_ids))
            if (
                type(strategy) is not dict
                or strategy.get("strategy_id") != strategy_id
            ):
                raise RuntimeError("micro local strategy marker conflicts")
            signal_id = strategy.get("signal_id")
            structure_id = strategy.get("structure_id")
            if (
                type(signal_id) is not int
                or signal_id <= 0
                or type(structure_id) is not str
                or len(structure_id) != 24
                or len(phase_keys) > 1
            ):
                raise RuntimeError("micro local claim identity is invalid")
            for label, item in (("plan", plan), ("pretrade_plan", pretrade)):
                if item is None:
                    if label == "plan":
                        raise RuntimeError("micro local plan is missing")
                    continue
                context = (
                    item.get("structure_context")
                    if type(item) is dict
                    else None
                )
                if (
                    type(item) is not dict
                    or item.get("stop_mode") != _MICRO_STOP_MODE
                    or item.get("structure_id") != structure_id
                    or type(context) is not dict
                    or context.get("strategy_id") != strategy_id
                    or context.get("rule_version") != f"{strategy_id}_V1"
                    or type(context.get("signal_id")) is not int
                    or context.get("signal_id") != signal_id
                    or context.get("structure_id") != structure_id
                ):
                    raise RuntimeError(f"micro local {label} identity conflicts")
            for key in phase_keys:
                phase = orders[key]
                if (
                    type(phase) is not dict
                    or phase.get("strategy_id") != strategy_id
                    or type(phase.get("signal_id")) is not int
                    or phase.get("signal_id") != signal_id
                    or phase.get("structure_id") != structure_id
                    or phase.get("stop_mode") != _MICRO_STOP_MODE
                ):
                    raise RuntimeError("micro local phase identity conflicts")
            self.recorder.assert_micro_execution_claim(
                signal_id,
                strategy_id,
                local_state.symbol,
                structure_id,
            )
            return True
        except Exception as exc:
            self.logger.error(
                "N21-N25 local execution claim attestation failed; blocking "
                "all paper, live and exchange activity | error=%s",
                exc,
            )
            return False

    def _n18_local_execution_state_ready(self, local_state) -> bool:
        if local_state is None:
            return True
        try:
            orders = local_state.orders
            if type(orders) is not dict:
                return True
            strategy = orders.get("strategy")
            plan = orders.get("plan")
            pretrade = orders.get("pretrade_plan")
            phase_keys = tuple(
                key for key in (
                    "execution_pending", "execution_cleanup_resolved",
                    "emergency_cleanup_pending",
                ) if key in orders
            )
            payloads = tuple(
                item for item in (strategy, plan, pretrade)
                + tuple(orders.get(key) for key in phase_keys)
                if type(item) is dict
            )
            marked = any(item.get("strategy_id") == "N18" for item in payloads)
            marked = marked or any(
                item.get("stop_mode") == _N18_STOP_MODE for item in payloads
            )
            for item in (plan, pretrade):
                context = item.get("structure_context") if type(item) is dict else None
                if type(context) is dict and context.get("strategy_id") == "N18":
                    marked = True
            if not marked:
                return True
            if type(strategy) is not dict or strategy.get("strategy_id") != "N18":
                raise RuntimeError("N18 local strategy marker conflicts")
            signal_id = strategy.get("signal_id")
            structure_id = strategy.get("structure_id")
            if (
                type(signal_id) is not int or signal_id <= 0
                or type(structure_id) is not str or len(structure_id) != 24
                or len(phase_keys) > 1
            ):
                raise RuntimeError("N18 local claim identity is invalid")
            for label, item in (("plan", plan), ("pretrade_plan", pretrade)):
                if item is None:
                    if label == "plan":
                        raise RuntimeError("N18 local plan is missing")
                    continue
                context = item.get("structure_context") if type(item) is dict else None
                if (
                    type(item) is not dict
                    or item.get("stop_mode") != _N18_STOP_MODE
                    or item.get("structure_id") != structure_id
                    or type(context) is not dict
                    or context.get("strategy_id") != "N18"
                    or context.get("rule_version") != "N18_V1"
                    or type(context.get("signal_id")) is not int
                    or context.get("signal_id") != signal_id
                    or context.get("structure_id") != structure_id
                ):
                    raise RuntimeError(f"N18 local {label} identity conflicts")
            for key in phase_keys:
                phase = orders[key]
                if (
                    type(phase) is not dict
                    or phase.get("strategy_id") != "N18"
                    or type(phase.get("signal_id")) is not int
                    or phase.get("signal_id") != signal_id
                    or phase.get("structure_id") != structure_id
                    or phase.get("stop_mode") != _N18_STOP_MODE
                ):
                    raise RuntimeError("N18 local phase identity conflicts")
            self.recorder.assert_n18_execution_claim(
                signal_id, local_state.symbol, structure_id
            )
            return True
        except Exception as exc:
            self.logger.error(
                "N18 local execution claim attestation failed; blocking all "
                "paper, live and exchange activity | error=%s", exc,
            )
            return False

    def _clear_finalized_n16_state_before_reconciliation(
        self,
        local_state,
    ) -> bool:
        """Complete an already-finalized N16 crash recovery without exchange I/O."""

        if local_state is None:
            return False
        try:
            identity = self._n16_local_state_claim_identity(local_state)
            if identity is None or identity[3]:
                return False
            evidence = self.recorder.inspect_strategy_live_finalization(
                local_state,
                "N16",
                allow_dry_run_pending=True,
            )
            if evidence.status not in {"FINAL", "RECOVERABLE_FINAL_REVIEW"}:
                return False
            blocked = self._complete_n16_final_evidence(local_state, evidence)
            if not blocked:
                self.logger.info(
                    "Recovered previously finalized N16 result without exchange "
                    "I/O | symbol=%s",
                    local_state.symbol,
                )
            return True
        except Exception as exc:
            self.logger.error(
                "N16 final recovery is blocked; state retained and all side "
                "effects disabled | symbol=%s error=%s",
                getattr(local_state, "symbol", None),
                exc,
            )
            return True

    def _settle_n16_dry_finalization(
        self,
        state,
        finalization,
        *,
        expected_balance_before: Decimal | None = None,
    ) -> None:
        if not state.dry_run:
            return
        if finalization is None:
            raise RuntimeError("N16 dry finalization evidence is missing")
        try:
            pnl_amount = Decimal(finalization.pnl_amount)
            balance_after = Decimal(finalization.balance_after)
        except Exception as exc:
            raise RuntimeError("N16 dry settlement evidence is invalid") from exc
        if (
            not pnl_amount.is_finite()
            or not balance_after.is_finite()
            or balance_after < 0
        ):
            raise RuntimeError("N16 dry settlement evidence is non-finite")
        balance_before = balance_after - pnl_amount
        if not balance_before.is_finite() or balance_before < 0:
            raise RuntimeError("N16 dry settlement baseline is invalid")
        if expected_balance_before is not None and (
            type(expected_balance_before) is not Decimal
            or not expected_balance_before.is_finite()
            or expected_balance_before < 0
            or expected_balance_before != balance_before
        ):
            raise RuntimeError("N16 dry settlement baseline conflicts")
        settle = getattr(self.trader, "settle_dry_run_balance_once", None)
        if settle is None:
            raise RuntimeError("N16 dry settlement gate is unavailable")
        settled = settle(
            finalization.trade_id,
            balance_before,
            balance_after,
        )
        if settled != balance_after:
            raise RuntimeError("N16 dry settlement acknowledgement conflicts")

    def _complete_n16_final_evidence(self, state, evidence) -> bool:
        """Finish one exact N16 FINAL/RECOVERABLE edge without exchange I/O."""

        finalization = evidence.finalization
        if evidence.status == "RECOVERABLE_FINAL_REVIEW":
            required = (
                evidence.result,
                evidence.exit_reason,
                evidence.exit_price,
                evidence.mark_price,
                evidence.pnl_amount,
                evidence.pnl_pct,
                evidence.balance_after,
            )
            if any(value is None for value in required):
                raise RuntimeError("N16 recoverable final evidence is incomplete")
            cooldown_until = datetime.now(timezone.utc) + timedelta(
                hours=self.config.symbol_cooldown_hours
            )
            if state.dry_run:
                finalization = self.recorder.finalize_strategy_dry_run_result(
                    state=state,
                    strategy_id="N16",
                    result=evidence.result,
                    exit_reason=evidence.exit_reason,
                    exit_price=evidence.exit_price,
                    mark_price=evidence.mark_price,
                    pnl_amount=evidence.pnl_amount,
                    pnl_pct=evidence.pnl_pct,
                    balance_after=evidence.balance_after,
                    cooldown_until=cooldown_until,
                )
            else:
                finalization = self.recorder.finalize_strategy_live_result(
                    state=state,
                    strategy_id="N16",
                    result=evidence.result,
                    exit_reason=evidence.exit_reason,
                    exit_price=evidence.exit_price,
                    mark_price=evidence.mark_price,
                    pnl_amount=evidence.pnl_amount,
                    pnl_pct=evidence.pnl_pct,
                    balance_after=evidence.balance_after,
                    detail={"source": "RECOVERED_FINAL_TRADE_REVIEW"},
                    cooldown_until=cooldown_until,
                )
            if finalization is None:
                raise RuntimeError("N16 recoverable finalization did not commit")
            confirmed = self.recorder.inspect_strategy_live_finalization(
                state,
                "N16",
                allow_dry_run_pending=True,
            )
            if (
                confirmed.status != "FINAL"
                or confirmed.finalization is None
                or confirmed.finalization.trade_id != finalization.trade_id
            ):
                raise RuntimeError("N16 finalization acknowledgement is indeterminate")
            finalization = confirmed.finalization
        elif evidence.status != "FINAL" or finalization is None:
            raise RuntimeError("N16 durable final evidence is incomplete")

        self._settle_n16_dry_finalization(state, finalization)
        return self._compare_and_clear_finalized_strategy_state(
            state,
            "N16",
            "DURABLE_FINAL_EVIDENCE",
        )

    def _repair_n16_live_audit_before_reconciliation(self, local_state):
        """Repair only exact N16 MISSING/REVIEW_ONLY crash edges.

        The initial gate is read-only.  The recorder then re-attests the
        independent ledger and exact committed claim inside the same
        ``BEGIN IMMEDIATE`` transaction that creates the missing Review/link.
        A fresh read-only READY/active/PENDING attestation is required after
        commit before any dry-run or exchange reconciliation is allowed.
        """

        try:
            if local_state is None:
                return True, local_state
            identity = self._n16_local_state_claim_identity(local_state)
            if identity is None or identity[3]:
                return True, local_state
            evidence = self.recorder.inspect_strategy_live_finalization(
                local_state,
                "N16",
                allow_dry_run_pending=True,
            )
            if evidence.status not in {
                "MISSING_OPEN_AUDIT",
                "REVIEW_ONLY_OPENED",
            }:
                return True, local_state
            claim = self.recorder.claim_strategy_live_open_audit(
                local_state,
                "N16",
            )
            if claim is None:
                raise RuntimeError(
                    "N16 live-audit recovery did not commit"
                )
            ready, reloaded_state = self._attested_local_execution_state()
            if not ready or reloaded_state is None:
                raise RuntimeError(
                    "N16 live-audit recovery could not be re-attested"
                )
            active_identity = self.recorder.n16_active_live_claim_identity()
            expected_identity = identity[:3]
            final_evidence = self.recorder.inspect_strategy_live_finalization(
                reloaded_state,
                "N16",
                allow_dry_run_pending=True,
            )
            if (
                active_identity != expected_identity
                or final_evidence.status != "PENDING"
            ):
                raise RuntimeError(
                    "N16 live-audit recovery acknowledgement is indeterminate"
                )
            return True, reloaded_state
        except Exception as exc:
            self.logger.error(
                "N16 live-audit recovery is blocked; retaining state and "
                "blocking all side effects | symbol=%s error=%s",
                getattr(local_state, "symbol", None),
                exc,
            )
            return False, local_state

    def _prepare_n16_local_recovery_before_scan(self) -> bool:
        """Resolve/attest N16 local crash evidence before market scanning."""

        try:
            ready, local_state = self._attested_local_execution_state()
            if not ready:
                return False
            if self._clear_finalized_n16_state_before_reconciliation(local_state):
                return False
            repaired, _local_state = (
                self._repair_n16_live_audit_before_reconciliation(local_state)
            )
            return repaired
        except Exception as exc:
            self.logger.error(
                "N16 pre-scan recovery failed closed; state retained | error=%s",
                exc,
            )
            return False

    def _close_dry_run_with_attested_state(self, local_state):
        if isinstance(self.trader, Trader):
            defer_settlement = False
            if local_state is not None and local_state.dry_run:
                identity = self._n16_local_state_claim_identity(local_state)
                defer_settlement = identity is not None and not identity[3]
            return self.trader.close_dry_run_position_if_triggered(
                expected_local_state=local_state,
                defer_settlement=defer_settlement,
            )
        return self.trader.close_dry_run_position_if_triggered()

    def _finalize_n16_dry_close(self, close_result) -> bool:
        identity = self._n16_local_state_claim_identity(close_result.state)
        if identity is None or identity[3] or not close_result.state.dry_run:
            return False
        financial_values = (
            close_result.exit_price,
            close_result.mark_price,
            close_result.pnl_amount,
            close_result.pnl_pct,
            close_result.balance_before,
            close_result.balance_after,
        )
        if (
            any(
                type(value) is not Decimal or not value.is_finite()
                for value in financial_values
            )
            or close_result.exit_price <= 0
            or close_result.mark_price <= 0
            or close_result.balance_before < 0
            or close_result.balance_after < 0
            or close_result.balance_before + close_result.pnl_amount
            != close_result.balance_after
        ):
            self.logger.error(
                "N16 dry close arithmetic is inconsistent; retaining state."
            )
            return False
        result = self._live_result_from_exit_reason(close_result.exit_reason)
        if result not in {"WIN", "LOSS"}:
            return False
        finalization = self.recorder.finalize_strategy_dry_run_result(
            state=close_result.state,
            strategy_id="N16",
            result=result,
            exit_reason=close_result.exit_reason,
            exit_price=decimal_to_api(close_result.exit_price),
            mark_price=decimal_to_api(close_result.mark_price),
            pnl_amount=decimal_to_api(close_result.pnl_amount),
            pnl_pct=decimal_to_api(close_result.pnl_pct),
            balance_after=decimal_to_api(close_result.balance_after),
            cooldown_until=datetime.now(timezone.utc)
            + timedelta(hours=self.config.symbol_cooldown_hours),
        )
        if finalization is None:
            return False
        confirmed = self.recorder.inspect_strategy_live_finalization(
            close_result.state,
            "N16",
            allow_dry_run_pending=True,
        )
        if (
            confirmed.status != "FINAL"
            or confirmed.finalization is None
            or confirmed.finalization.trade_id != finalization.trade_id
            or confirmed.finalization.pnl_amount
            != decimal_to_api(close_result.pnl_amount)
            or confirmed.finalization.balance_after
            != decimal_to_api(close_result.balance_after)
        ):
            return False
        self._settle_n16_dry_finalization(
            close_result.state,
            confirmed.finalization,
            expected_balance_before=close_result.balance_before,
        )
        return not self._compare_and_clear_finalized_strategy_state(
            close_result.state,
            "N16",
            "ATOMIC_DRY_FINALIZER",
        )

    def _sync_with_attested_state(self, local_state):
        if isinstance(self.trader, Trader):
            return self.trader.sync_state_with_exchange(
                expected_local_state=local_state
            )
        return self.trader.sync_state_with_exchange()

    def _live_result_from_exit_reason(self, exit_reason: str) -> str | None:
        if exit_reason == "TAKE_PROFIT":
            return "WIN"
        if exit_reason == "STOP_LOSS":
            return "LOSS"
        return None

    def _strategy_id_from_state(self, state) -> str | None:
        strategy_id = state.orders.get("strategy", {}).get("strategy_id") if isinstance(state.orders, dict) else None
        return str(strategy_id) if strategy_id else None

    def _record_strategy_live_result_from_state(self, state, result: str, trade_id: int | None) -> bool:
        strategy_id = self._strategy_id_from_state(state)
        if strategy_id is None:
            return False
        return self.recorder.record_strategy_live_result(
            strategy_id=strategy_id,
            result=result,
            symbol=state.symbol,
            trade_review_id=trade_id,
            opened_at=state.opened_at,
        )

    def _record_resolved_exchange_close(
        self,
        state,
        resolution: LiveCloseResolution,
    ) -> int | None:
        trade_id = self.recorder.record_trade_close(
            state,
            resolution.exit_reason,
            decimal_to_api(resolution.exit_price) if resolution.exit_price is not None else "",
            "",
            "",
            "",
            "",
            detail=resolution.detail,
        )
        if trade_id is None:
            raise RuntimeError(
                "Live close audit persistence failed; retaining local state for recovery."
            )
        self._set_symbol_cooldown(state.symbol, resolution.exit_reason, trade_id)
        return trade_id

    def _compare_and_clear_finalized_strategy_state(
        self,
        state,
        strategy_id: str,
        source: str,
    ) -> bool:
        """Clear only the exact recovered state after durable final evidence.

        ``True`` means recovery remains blocked and the caller must retain the
        state.  A replacement state must never be cleared by a stale recovery.
        """
        try:
            state_cleared = self.state.compare_and_clear(state)
        except Exception:
            self.logger.exception(
                "Strategy live finalization committed but state clear failed; "
                "state retained | strategy=%s symbol=%s source=%s",
                strategy_id,
                state.symbol,
                source,
            )
            return True
        if not state_cleared:
            self.logger.error(
                "Strategy live finalization committed but state changed; "
                "state retained | strategy=%s symbol=%s source=%s",
                strategy_id,
                state.symbol,
                source,
            )
            return True
        return False

    def _handle_closed_strategy_live_state(self, state) -> bool:
        strategy_id = self._strategy_id_from_state(state)
        if strategy_id is None:
            resolution = LiveCloseResolution(
                "MANUAL_OR_EXTERNAL_CLOSE",
                "MANUAL_OR_EXTERNAL_CLOSE",
                None,
                {"reason": "POSITION_HAS_NO_STRATEGY_ID"},
            )
            self._record_resolved_exchange_close(state, resolution)
            self.state.clear()
            return False

        if (
            isinstance(state.orders, dict)
            and isinstance(state.orders.get("execution_pending"), dict)
        ):
            self.recorder.mark_strategy_live_result_pending(
                strategy_id,
                "MARKET_ORDER_EXECUTION_UNKNOWN",
            )
            self.recorder.record_event(
                "strategy_market_order_execution_pending",
                {
                    "strategy_id": strategy_id,
                    "detail": state.orders["execution_pending"],
                },
                state.symbol,
            )
            self.logger.error(
                "Market order execution remains unknown; local evidence retained | "
                "strategy=%s symbol=%s",
                strategy_id,
                state.symbol,
            )
            return True

        final_evidence = self.recorder.inspect_strategy_live_finalization(
            state,
            strategy_id,
            allow_dry_run_pending=(strategy_id == "N16"),
        )
        if (
            strategy_id == "N16"
            and final_evidence.status in {"FINAL", "RECOVERABLE_FINAL_REVIEW"}
        ):
            try:
                return self._complete_n16_final_evidence(state, final_evidence)
            except Exception as exc:
                self.logger.error(
                    "N16 final recovery failed closed; state retained without "
                    "exchange I/O | symbol=%s error=%s",
                    state.symbol,
                    exc,
                )
                return True
        if final_evidence.status == "FINAL":
            blocked = self._compare_and_clear_finalized_strategy_state(
                state,
                strategy_id,
                "DURABLE_FINAL_EVIDENCE",
            )
            if not blocked:
                self.logger.info(
                    "Recovered previously finalized strategy live result without "
                    "re-querying the exchange | strategy=%s symbol=%s trade_id=%s",
                    strategy_id,
                    state.symbol,
                    final_evidence.finalization.trade_id
                    if final_evidence.finalization is not None
                    else None,
                )
            return blocked
        if final_evidence.status == "RECOVERABLE_FINAL_REVIEW":
            if (
                final_evidence.result is None
                or final_evidence.exit_reason is None
                or final_evidence.exit_price is None
            ):
                self.logger.error(
                    "Recoverable final review lacks strict result evidence; state retained | "
                    "strategy=%s symbol=%s",
                    strategy_id,
                    state.symbol,
                )
                return True
            finalization = self.recorder.finalize_strategy_live_result(
                state=state,
                strategy_id=strategy_id,
                result=final_evidence.result,
                exit_reason=final_evidence.exit_reason,
                exit_price=final_evidence.exit_price,
                detail={"source": "RECOVERED_FINAL_TRADE_REVIEW"},
                cooldown_until=datetime.now(timezone.utc)
                + timedelta(hours=self.config.symbol_cooldown_hours),
            )
            if finalization is None:
                self.logger.error(
                    "Final trade review recovery could not be atomically completed; "
                    "state retained | strategy=%s symbol=%s",
                    strategy_id,
                    state.symbol,
                )
                return True
            return self._compare_and_clear_finalized_strategy_state(
                state,
                strategy_id,
                "RECOVERED_FINAL_TRADE_REVIEW",
            )

        if final_evidence.status in {
            "MISSING_OPEN_AUDIT",
            "REVIEW_ONLY_OPENED",
        }:
            live_open_claim = self.recorder.claim_strategy_live_open_audit(
                state,
                strategy_id,
            )
            if live_open_claim is None:
                self.logger.error(
                    "Strategy live-open audit claim is blocked; state retained without "
                    "exchange query | strategy=%s symbol=%s opened_at=%s evidence=%s",
                    strategy_id,
                    state.symbol,
                    state.opened_at,
                    final_evidence.status,
                )
                return True
            final_evidence = self.recorder.inspect_strategy_live_finalization(
                state,
                strategy_id,
                allow_dry_run_pending=(strategy_id == "N16"),
            )
        if final_evidence.status != "PENDING":
            self.logger.error(
                "Strategy live finalization evidence is blocked; state retained "
                "without exchange query | strategy=%s symbol=%s status=%s detail=%s",
                strategy_id,
                state.symbol,
                final_evidence.status,
                final_evidence.detail,
            )
            return True

        resolution = self.trader.resolve_closed_live_position(state)
        if resolution.result in {"WIN", "LOSS"}:
            cooldown_until = datetime.now(timezone.utc) + timedelta(
                hours=self.config.symbol_cooldown_hours
            )
            finalization = self.recorder.finalize_strategy_live_result(
                state=state,
                strategy_id=strategy_id,
                result=resolution.result,
                exit_reason=resolution.exit_reason,
                exit_price=decimal_to_api(resolution.exit_price)
                if resolution.exit_price is not None
                else "",
                detail=resolution.detail,
                cooldown_until=cooldown_until,
            )
            if finalization is None:
                self.logger.error(
                    "Atomic strategy live finalization failed; local state retained | "
                    "strategy=%s symbol=%s result=%s",
                    strategy_id,
                    state.symbol,
                    resolution.result,
                )
                return True
            if self._compare_and_clear_finalized_strategy_state(
                state,
                strategy_id,
                "ATOMIC_FINALIZER",
            ):
                return True
            self.logger.info(
                "Strategy live result confirmed | strategy=%s symbol=%s result=%s reason=%s "
                "idempotent=%s",
                strategy_id,
                state.symbol,
                resolution.result,
                resolution.exit_reason,
                finalization.idempotent,
            )
            return False

        if resolution.result == "LIVE_RESULT_PENDING":
            current_strategy_state = self.recorder.get_strategy_state(strategy_id)
            if not current_strategy_state.live_result_pending:
                self.recorder.mark_strategy_live_result_pending(
                    strategy_id, resolution.exit_reason
                )
                self.recorder.record_event(
                    "strategy_live_result_alarm",
                    {
                        "strategy_id": strategy_id,
                        "result": resolution.result,
                        "reason": resolution.exit_reason,
                        "detail": resolution.detail,
                    },
                    state.symbol,
                )
        else:
            self._record_resolved_exchange_close(state, resolution)
            self.recorder.mark_strategy_live_result_pending(strategy_id, resolution.exit_reason)
            self.recorder.record_event(
                "strategy_live_result_alarm",
                {
                    "strategy_id": strategy_id,
                    "result": resolution.result,
                    "reason": resolution.exit_reason,
                    "detail": resolution.detail,
                },
                state.symbol,
            )
        self.logger.error(
            "Strategy live result pending; strategy remains blocked | strategy=%s symbol=%s result=%s",
            strategy_id,
            state.symbol,
            resolution.result,
        )
        return True

    def _strategy_volume_top_n(self) -> int:
        return max(
            (
                strategy.volume_top_n or 0
                for strategy in self.strategies
                if strategy.market_filter == "quote_volume_top"
            ),
            default=0,
        )

    def _signal_plan_cache_key(self, signal: StrategySignalDecision) -> tuple[str, str, str]:
        structure_id = ""
        if isinstance(
            signal.analysis,
            (
                N06AnalysisResult,
                N07AnalysisResult,
                N08AnalysisResult,
                N09AnalysisResult,
                N10AnalysisResult,
                N11AnalysisResult,
                N12AnalysisResult,
                N13AnalysisResult,
                N14AnalysisResult,
                N15AnalysisResult,
                N16AnalysisResult,
                N17AnalysisResult,
                N18AnalysisResult,
                N19AnalysisResult,
                N20AnalysisResult,
                MicroAnalysisResult,
            ),
        ):
            structure_id = signal.analysis.structure_id or ""
        return signal.strategy.stop_mode, signal.candidate.symbol, structure_id

    def _build_strategy_plan(self, signal: StrategySignalDecision) -> TradePlan:
        strategy = signal.strategy
        candidate = signal.candidate
        if strategy.stop_mode == "amplitude":
            return self.trader.build_trade_plan(candidate.symbol, candidate.mark_price)
        if strategy.stop_mode == "structure_p1":
            analysis = signal.analysis
            if not isinstance(analysis, N06AnalysisResult) or analysis.structure is None:
                raise BinanceAPIError(f"Missing N06 structure for {candidate.symbol}")
            plan = self.trader.build_structure_trade_plan(
                candidate.symbol,
                candidate.mark_price,
                analysis.structure.p1,
                strategy.risk_reward_ratio,
                structure_id=analysis.structure.structure_id,
            )
            if analysis.current_open_time is None:
                raise BinanceAPIError(f"Missing N06 entry candle time for {candidate.symbol}")
            try:
                entry_candle_open_time_ms = int(Decimal(analysis.current_open_time))
            except (ArithmeticError, TypeError, ValueError) as exc:
                raise BinanceAPIError(
                    f"Invalid N06 entry candle time for {candidate.symbol}: "
                    f"{analysis.current_open_time}"
                ) from exc
            return replace(
                plan,
                entry_candle_open_time_ms=entry_candle_open_time_ms,
                entry_deadline_ms=(
                    entry_candle_open_time_ms + strategy.entry_window_seconds * 1000
                ),
            )
        if strategy.stop_mode == "structure_p1_margin_capped":
            analysis = signal.analysis
            if not isinstance(analysis, N07AnalysisResult) or analysis.structure is None:
                raise BinanceAPIError(f"Missing N07 structure for {candidate.symbol}")
            return self.trader.build_margin_capped_structure_trade_plan(
                candidate.symbol,
                candidate.mark_price,
                analysis.structure.p1,
                strategy.risk_reward_ratio,
                structure_id=analysis.structure.structure_id,
            )
        if strategy.stop_mode == "amplitude_margin_capped":
            analysis = signal.analysis
            if not isinstance(analysis, N08AnalysisResult) or analysis.structure is None:
                raise BinanceAPIError(f"Missing N08 range structure for {candidate.symbol}")
            plan = self.trader.build_amplitude_margin_capped_trade_plan(
                candidate.symbol,
                candidate.mark_price,
                strategy.risk_reward_ratio,
                structure_id=analysis.structure.structure_id,
            )
            if analysis.current_open_time is None:
                raise BinanceAPIError(f"Missing N08 entry candle time for {candidate.symbol}")
            try:
                entry_candle_open_time_ms = int(Decimal(analysis.current_open_time))
            except (ArithmeticError, TypeError, ValueError) as exc:
                raise BinanceAPIError(
                    f"Invalid N08 entry candle time for {candidate.symbol}: "
                    f"{analysis.current_open_time}"
                ) from exc
            return replace(
                plan,
                entry_candle_open_time_ms=entry_candle_open_time_ms,
                entry_deadline_ms=(
                    entry_candle_open_time_ms + strategy.entry_window_seconds * 1000
                ),
            )
        if strategy.stop_mode == "s1_target_margin_capped":
            analysis = signal.analysis
            if not isinstance(analysis, N09AnalysisResult) or analysis.structure is None:
                raise BinanceAPIError(f"Missing N09 slow-decline structure for {candidate.symbol}")
            return self.trader.build_s1_target_margin_capped_trade_plan(
                candidate.symbol,
                candidate.mark_price,
                analysis.structure.s1,
                structure_id=analysis.structure.structure_id,
            )
        if strategy.stop_mode == "sweep_low_tick_margin_capped":
            analysis = signal.analysis
            if not isinstance(analysis, N10AnalysisResult) or analysis.structure is None:
                raise BinanceAPIError(f"Missing N10 sweep structure for {candidate.symbol}")
            plan = self.trader.build_sweep_low_margin_capped_trade_plan(
                candidate.symbol,
                analysis.structure.e.close,
                analysis.structure.w.low,
                strategy.risk_reward_ratio,
                structure_id=analysis.structure.structure_id,
                entry_min_price=analysis.structure.w.high,
                entry_max_price=analysis.structure.entry_upper_price,
            )
            return replace(
                plan,
                entry_candle_open_time_ms=analysis.structure.e.open_time_ms,
                entry_deadline_ms=(
                    analysis.structure.e.open_time_ms
                    + strategy.entry_window_seconds * 1000
                ),
            )
        if strategy.stop_mode == "breakout_retest_margin_capped":
            analysis = signal.analysis
            if (
                not isinstance(analysis, N11AnalysisResult)
                or analysis.structure is None
                or analysis.structure.retest is None
                or analysis.structure.entry is None
                or analysis.structure.entry_upper_price is None
            ):
                raise BinanceAPIError(
                    f"Missing N11 breakout-retest structure for {candidate.symbol}"
                )
            plan = self.trader.build_breakout_retest_margin_capped_trade_plan(
                candidate.symbol,
                analysis.structure.entry.close,
                analysis.structure.retest.low,
                strategy.risk_reward_ratio,
                structure_id=analysis.structure.structure_id,
                entry_min_price=analysis.structure.breakout_level,
                entry_max_price=analysis.structure.entry_upper_price,
            )
            return replace(
                plan,
                entry_candle_open_time_ms=analysis.structure.entry.open_time_ms,
                entry_deadline_ms=(
                    analysis.structure.entry.open_time_ms
                    + strategy.entry_window_seconds * 1000
                ),
            )
        if strategy.stop_mode == "relative_strength_pullback_margin_capped":
            analysis = signal.analysis
            if (
                not isinstance(analysis, N12AnalysisResult)
                or analysis.structure is None
                or analysis.structure.entry is None
            ):
                raise BinanceAPIError(
                    f"Missing N12 relative-strength pullback structure for {candidate.symbol}"
                )
            plan = self.trader.build_relative_strength_pullback_margin_capped_trade_plan(
                candidate.symbol,
                analysis.structure.entry.close,
                analysis.structure.p,
                strategy.risk_reward_ratio,
                structure_id=analysis.structure.structure_id,
                entry_min_price=analysis.structure.entry_min_price,
                entry_max_price=analysis.structure.entry_max_price,
            )
            return replace(
                plan,
                entry_candle_open_time_ms=analysis.structure.entry.open_time_ms,
                entry_deadline_ms=(
                    analysis.structure.entry.open_time_ms
                    + strategy.entry_window_seconds * 1000
                ),
            )
        if strategy.stop_mode == "vwap_rotation_margin_capped":
            analysis = signal.analysis
            if not isinstance(analysis, N13AnalysisResult) or analysis.structure is None or analysis.structure.entry is None:
                raise BinanceAPIError(f"Missing N13 VWAP rotation structure for {candidate.symbol}")
            plan = self.trader.build_vwap_rotation_margin_capped_trade_plan(
                candidate.symbol, analysis.structure.entry.close, analysis.structure.p,
                strategy.risk_reward_ratio, structure_id=analysis.structure.structure_id,
                entry_min_price=analysis.structure.entry_min_price,
                entry_max_price=analysis.structure.entry_max_price,
            )
            return replace(
                plan,
                entry_candle_open_time_ms=analysis.structure.entry.open_time_ms,
                entry_deadline_ms=analysis.structure.entry.open_time_ms + strategy.entry_window_seconds * 1000,
            )
        if strategy.stop_mode == "sell_pressure_decay_margin_capped":
            analysis = signal.analysis
            if (
                not isinstance(analysis, N14AnalysisResult)
                or analysis.structure is None
                or analysis.structure.entry is None
            ):
                raise BinanceAPIError(
                    f"Missing N14 sell-pressure structure for {candidate.symbol}"
                )
            structure = analysis.structure
            plan = self.trader.build_sell_pressure_decay_margin_capped_trade_plan(
                candidate.symbol,
                structure.entry.close,
                structure.p,
                strategy.risk_reward_ratio,
                structure_id=structure.structure_id,
                entry_min_price=structure.entry_min_price,
                entry_max_price=structure.entry_max_price,
            )
            return replace(
                plan,
                entry_candle_open_time_ms=structure.entry.open_time_ms,
                entry_deadline_ms=(
                    structure.entry.open_time_ms
                    + strategy.entry_window_seconds * 1000
                ),
                structure_context={
                    "strategy_id": strategy.strategy_id,
                    "structure_id": structure.structure_id,
                    "s_open_time_ms": structure.s.open_time_ms,
                    "a_open_time_ms": structure.a.open_time_ms,
                    "c_open_time_ms": structure.c.open_time_ms,
                    "p": str(structure.p),
                    "atr_s_reference": str(structure.atr_s_reference),
                    "atr_c": str(structure.atr_c),
                    "volume_median_s": str(structure.volume_median_s),
                    "quote_volume_rank_at_s": structure.quote_volume_rank,
                },
            )
        if strategy.stop_mode == "breadth_recovery_margin_capped":
            analysis = signal.analysis
            if (
                not isinstance(analysis, N15AnalysisResult)
                or analysis.structure is None
            ):
                raise BinanceAPIError(
                    f"Missing N15 breadth-recovery structure for {candidate.symbol}"
                )
            structure = analysis.structure
            plan = self.trader.build_breadth_recovery_margin_capped_trade_plan(
                candidate.symbol,
                structure.entry.close,
                structure.p,
                strategy.risk_reward_ratio,
                structure_id=structure.structure_id,
                entry_min_price=structure.entry_min_price,
                entry_max_price=structure.entry_max_price,
            )
            return replace(
                plan,
                entry_candle_open_time_ms=structure.entry.open_time_ms,
                entry_deadline_ms=(
                    structure.entry.open_time_ms
                    + strategy.entry_window_seconds * 1000
                ),
                structure_context={
                    "strategy_id": strategy.strategy_id,
                    "structure_id": structure.structure_id,
                    "b_open_time_ms": structure.b.open_time_ms,
                    "c_open_time_ms": structure.c.open_time_ms,
                    "p": str(structure.p),
                    "atr_b_pre": str(structure.atr_b_pre),
                    "atr_c_pre": str(structure.atr_c_pre),
                    "rank_b": structure.rank_b,
                    "rank_c": structure.rank_c,
                    "quote_volume_rank": structure.quote_volume_rank,
                    "candidate_universe": candidate.candidate_universe,
                },
            )
        if strategy.stop_mode == "trend_support_continuation_margin_capped":
            analysis = signal.analysis
            if (
                not isinstance(analysis, N16AnalysisResult)
                or analysis.symbol != candidate.symbol
                or analysis.structure is None
                or analysis.structure.symbol != candidate.symbol
                or analysis.structure.c is None
                or analysis.structure.entry is None
                or analysis.structure.structure_id is None
                or analysis.structure.atr_c is None
                or analysis.structure.entry_min_price is None
                or analysis.structure.entry_max_price is None
                or analysis.state_record is None
                or analysis.state_record.strategy_id != "N16"
                or analysis.state_record.symbol != candidate.symbol
                or analysis.state_record.structure_id
                != analysis.structure.structure_id
                or analysis.state_record.stage != "CONFIRMED"
                or analysis.state_record.reason != "PASSED"
            ):
                raise BinanceAPIError(
                    f"Missing N16 trend-support structure for {candidate.symbol}"
                )
            structure = analysis.structure
            plan = self.trader.build_trend_support_continuation_margin_capped_trade_plan(
                candidate.symbol,
                structure.entry.close,
                structure.p,
                strategy.risk_reward_ratio,
                structure_id=structure.structure_id,
                entry_min_price=structure.entry_min_price,
                entry_max_price=structure.entry_max_price,
            )
            return replace(
                plan,
                entry_candle_open_time_ms=structure.entry.open_time_ms,
                entry_deadline_ms=(
                    structure.entry.open_time_ms
                    + strategy.entry_window_seconds * 1000
                ),
                structure_context={
                    "strategy_id": strategy.strategy_id,
                    "rule_version": "N16_V1",
                    "structure_id": structure.structure_id,
                    "episode_id": structure.episode_id,
                    "l1_open_time_ms": structure.l1.open_time_ms,
                    "h1_open_time_ms": structure.h1.open_time_ms,
                    "l2_open_time_ms": structure.l2.open_time_ms,
                    "h2_open_time_ms": structure.h2.open_time_ms,
                    "a_open_time_ms": structure.a.open_time_ms,
                    "c_open_time_ms": structure.c.open_time_ms,
                    "p": str(structure.p),
                    "atr_c": str(structure.atr_c),
                    "evidence_sha256": analysis.state_record.evidence_sha256,
                    "entry_observation": structure.entry.to_jsonable(False),
                    "quote_volume_rank": analysis.quote_volume_rank,
                    "candidate_universe": candidate.candidate_universe,
                },
            )
        if strategy.stop_mode == _N17_STOP_MODE:
            analysis = signal.analysis
            if (
                not isinstance(analysis, N17AnalysisResult)
                or analysis.symbol != candidate.symbol
                or analysis.structure is None
                or analysis.structure.symbol != candidate.symbol
                or analysis.structure.confirmation is None
                or analysis.structure.entry is None
                or analysis.structure.atr_confirmation is None
                or analysis.structure.entry_min is None
                or analysis.structure.entry_max is None
                or analysis.state_record is None
                or analysis.state_record.strategy_id != "N17"
                or analysis.state_record.symbol != candidate.symbol
                or analysis.state_record.structure_id
                != analysis.structure.structure_id
                or analysis.state_record.stage != "CONFIRMED"
                or analysis.state_record.reason != "PASSED"
            ):
                raise BinanceAPIError(
                    f"Missing N17 range-support structure for {candidate.symbol}"
                )
            structure = analysis.structure
            plan = self.trader.build_range_support_absorption_margin_capped_trade_plan(
                candidate.symbol,
                structure.entry.close,
                structure.touch.low,
                strategy.risk_reward_ratio,
                structure_id=structure.structure_id,
                entry_min_price=structure.entry_min,
                entry_max_price=structure.entry_max,
            )
            return replace(
                plan,
                entry_candle_open_time_ms=structure.entry.open_time_ms,
                entry_deadline_ms=(
                    structure.entry.open_time_ms
                    + strategy.entry_window_seconds * 1000
                ),
                structure_context={
                    "strategy_id": "N17",
                    "rule_version": "N17_V1",
                    "structure_id": structure.structure_id,
                    "family_id": analysis.state_record.family_id,
                    "box_start_time_ms": structure.box.start_time_ms,
                    "box_end_time_ms": structure.box.end_time_ms,
                    "touch_open_time_ms": structure.touch.open_time_ms,
                    "absorption_open_time_ms": structure.absorption.open_time_ms,
                    "confirmation_open_time_ms": structure.confirmation.open_time_ms,
                    "touch_low": str(structure.touch.low),
                    "lower": str(structure.box.lower),
                    "atr_confirmation": str(structure.atr_confirmation),
                    "evidence_sha256": analysis.state_record.evidence_sha256,
                    "entry_observation": structure.entry.to_jsonable(),
                    "quote_volume_rank": analysis.quote_volume_rank,
                    "candidate_universe": candidate.candidate_universe,
                },
            )
        if strategy.stop_mode == _N19_STOP_MODE:
            analysis = signal.analysis
            if (
                not isinstance(analysis, N19AnalysisResult)
                or analysis.symbol != candidate.symbol
                or analysis.structure is None
                or analysis.structure.symbol != candidate.symbol
                or analysis.structure.c is None
                or analysis.structure.entry is None
                or analysis.structure.atr_c is None
                or analysis.structure.entry_min is None
                or analysis.structure.entry_max is None
                or analysis.structure.structure_id is None
                or analysis.state_record is None
                or analysis.state_record.strategy_id != "N19"
                or analysis.state_record.symbol != candidate.symbol
                or analysis.state_record.structure_id
                != analysis.structure.structure_id
                or analysis.state_record.stage != "CONFIRMED"
                or analysis.state_record.reason != "PASSED"
            ):
                raise BinanceAPIError(
                    f"Missing N19 staircase-exhaustion structure for {candidate.symbol}"
                )
            structure = analysis.structure
            plan = self.trader.build_staircase_exhaustion_margin_capped_trade_plan(
                candidate.symbol,
                structure.entry.close,
                structure.x.low,
                strategy.risk_reward_ratio,
                structure_id=structure.structure_id,
                entry_min_price=structure.entry_min,
                entry_max_price=structure.entry_max,
            )
            return replace(
                plan,
                entry_candle_open_time_ms=structure.entry.open_time_ms,
                entry_deadline_ms=(
                    structure.entry.open_time_ms
                    + strategy.entry_window_seconds * 1000
                ),
                structure_context={
                    "strategy_id": "N19",
                    "rule_version": "N19_V1",
                    "structure_id": structure.structure_id,
                    "family_id": analysis.state_record.family_id,
                    "s_open_time_ms": structure.s.open_time_ms,
                    "l1_open_time_ms": structure.l1.open_time_ms,
                    "r1_open_time_ms": structure.r1.open_time_ms,
                    "l2_open_time_ms": structure.l2.open_time_ms,
                    "r2_open_time_ms": structure.r2.open_time_ms,
                    "x_open_time_ms": structure.x.open_time_ms,
                    "c_open_time_ms": structure.c.open_time_ms,
                    "x_low": str(structure.x.low),
                    "atr_c": str(structure.atr_c),
                    "evidence_sha256": analysis.state_record.evidence_sha256,
                    "entry_observation": structure.entry.to_jsonable(),
                    "quote_volume_rank": analysis.quote_volume_rank,
                    "candidate_universe": candidate.candidate_universe,
                },
            )
        if strategy.stop_mode == _N18_STOP_MODE:
            analysis = signal.analysis
            if (
                not isinstance(analysis, N18AnalysisResult)
                or analysis.symbol != candidate.symbol
                or analysis.structure is None
                or analysis.structure.symbol != candidate.symbol
                or analysis.structure.b is None
                or analysis.structure.entry is None
                or analysis.structure.atr_b is None
                or analysis.structure.entry_min is None
                or analysis.structure.entry_max is None
                or analysis.structure.structure_id is None
                or analysis.state_record is None
                or analysis.state_record.strategy_id != "N18"
                or analysis.state_record.symbol != candidate.symbol
                or analysis.state_record.structure_id != analysis.structure.structure_id
                or analysis.state_record.stage != "CONFIRMED"
                or analysis.state_record.reason != "PASSED"
            ):
                raise BinanceAPIError(
                    f"Missing N18 ascending-triangle structure for {candidate.symbol}"
                )
            structure = analysis.structure
            plan = self.trader.build_ascending_triangle_breakout_margin_capped_trade_plan(
                candidate.symbol,
                structure.entry.close,
                structure.l3.low,
                strategy.risk_reward_ratio,
                structure_id=structure.structure_id,
                entry_min_price=structure.entry_min,
                entry_max_price=structure.entry_max,
            )
            return replace(
                plan,
                entry_candle_open_time_ms=structure.entry.open_time_ms,
                entry_deadline_ms=(
                    structure.entry.open_time_ms + strategy.entry_window_seconds * 1000
                ),
                structure_context={
                    "strategy_id": "N18",
                    "rule_version": "N18_V1",
                    "structure_id": structure.structure_id,
                    "family_id": analysis.state_record.family_id,
                    "l1_open_time_ms": structure.l1.open_time_ms,
                    "h1_open_time_ms": structure.h1.open_time_ms,
                    "l2_open_time_ms": structure.l2.open_time_ms,
                    "h2_open_time_ms": structure.h2.open_time_ms,
                    "l3_open_time_ms": structure.l3.open_time_ms,
                    "a_open_time_ms": structure.a.open_time_ms,
                    "b_open_time_ms": structure.b.open_time_ms,
                    "l3_low": str(structure.l3.low),
                    "resistance": str(structure.resistance),
                    "atr_b": str(structure.atr_b),
                    "evidence_sha256": analysis.state_record.evidence_sha256,
                    "entry_observation": structure.entry.to_jsonable(),
                    "quote_volume_rank": analysis.quote_volume_rank,
                    "candidate_universe": candidate.candidate_universe,
                },
            )
        if strategy.stop_mode == _N20_STOP_MODE:
            analysis = signal.analysis
            if (
                not isinstance(analysis, N20AnalysisResult)
                or analysis.symbol != candidate.symbol
                or analysis.winner is None
                or analysis.winner.symbol != candidate.symbol
                or analysis.state_record is None
                or analysis.state_record.strategy_id != "N20"
                or analysis.state_record.stage != "CONFIRMED"
                or analysis.state_record.reason != "PASSED"
                or analysis.state_record.structure_id != analysis.winner.structure_id
            ):
                raise BinanceAPIError(
                    f"Missing N20 relative-strength winner for {candidate.symbol}"
                )
            winner = analysis.winner
            plan = self.trader.build_n20_relative_strength_recovery_margin_capped_trade_plan(
                candidate.symbol,
                winner.entry.close,
                winner.p,
                strategy.risk_reward_ratio,
                structure_id=winner.structure_id,
                entry_min_price=winner.entry_min,
                entry_max_price=winner.entry_max,
            )
            return replace(
                plan,
                entry_candle_open_time_ms=winner.entry.open_time_ms,
                entry_deadline_ms=(
                    winner.entry.open_time_ms + strategy.entry_window_seconds * 1000
                ),
                structure_context={
                    "strategy_id": "N20", "rule_version": "N20_V1",
                    "structure_id": winner.structure_id,
                    "episode_id": winner.episode_id,
                    "c_open_time_ms": winner.c.open_time_ms,
                    "p_open_time_ms": winner.p_open_time_ms,
                    "p": str(winner.p), "atr_c": str(winner.atr_c),
                    "evidence_sha256": analysis.state_record.encoded[2],
                    "entry_observation": winner.entry.to_jsonable(),
                    "quote_volume_rank": winner.quote_volume_rank,
                    "baseline_rank": winner.baseline_rank,
                    "resilience_rank": winner.resilience_rank,
                    "candidate_universe": candidate.candidate_universe,
                },
            )
        if strategy.stop_mode == _MICRO_STOP_MODE:
            analysis = signal.analysis
            if (
                strategy.strategy_id not in MICRO_STRATEGY_IDS
                or not isinstance(analysis, MicroAnalysisResult)
                or not analysis.passed
                or analysis.structure is None
                or analysis.evidence is None
                or analysis.structure.strategy_id != strategy.strategy_id
                or analysis.structure.symbol != candidate.symbol
            ):
                raise BinanceAPIError(
                    f"Missing {strategy.strategy_id} micro structure for "
                    f"{candidate.symbol}"
                )
            structure = analysis.structure
            plan = self.trader.build_micro_observation_margin_capped_trade_plan(
                strategy.strategy_id,
                candidate.symbol,
                structure.entry_price,
                structure.structural_low,
                strategy.risk_reward_ratio,
                structure_id=structure.structure_id,
                entry_min_price=structure.entry_min,
                entry_max_price=structure.entry_max,
            )
            return replace(
                plan,
                entry_candle_open_time_ms=structure.kline_open_time_ms,
                entry_deadline_ms=structure.deadline_ms,
                structure_context={
                    "strategy_id": strategy.strategy_id,
                    "rule_version": f"{strategy.strategy_id}_V1",
                    "structure_id": structure.structure_id,
                    "kline_open_time_ms": structure.kline_open_time_ms,
                    "confirmation_observed_at_ms": (
                        structure.confirmation_observed_at_ms
                    ),
                    "deadline_ms": structure.deadline_ms,
                    "structural_low": str(structure.structural_low),
                    "atr": str(structure.atr),
                    "source_chain_sha256": structure.source_chain_sha256,
                    "quote_volume_rank": structure.quote_volume_rank,
                    "candidate_universe": candidate.candidate_universe,
                },
            )
        raise BinanceAPIError(f"Unsupported stop mode: {strategy.stop_mode}")

    def _trade_plan_detail(self, plan: TradePlan) -> dict[str, object]:
        return {
            "entry_price": decimal_to_api(plan.entry_price),
            "entry_min": decimal_to_api(plan.entry_min_price)
            if plan.entry_min_price is not None
            else None,
            "entry_max": decimal_to_api(plan.entry_max_price)
            if plan.entry_max_price is not None
            else None,
            "actual_entry": decimal_to_api(plan.actual_entry_price)
            if plan.actual_entry_price is not None
            else None,
            "stop_loss_price": decimal_to_api(plan.stop_loss_price),
            "take_profit_price": decimal_to_api(plan.take_profit_price),
            "stop_loss_pct": decimal_to_api(plan.stop_loss_pct),
            "take_profit_pct": decimal_to_api(plan.take_profit_pct),
            "amplitude_24h_pct": decimal_to_api(plan.amplitude_24h_pct),
            "high_24h_price": decimal_to_api(plan.high_24h_price),
            "low_24h_price": decimal_to_api(plan.low_24h_price),
            "risk_amount": decimal_to_api(plan.risk_amount),
            "quantity": decimal_to_api(plan.quantity),
            "notional_value": decimal_to_api(plan.notional_value),
            "required_margin": decimal_to_api(plan.required_margin),
            "balance": decimal_to_api(plan.balance),
            "leverage": plan.leverage,
            "stop_mode": plan.stop_mode,
            "risk_reward_ratio": decimal_to_api(plan.risk_reward_ratio),
            "structure_id": plan.structure_id,
            "structure_stop_price": decimal_to_api(plan.structure_stop_price)
            if plan.structure_stop_price is not None
            else None,
            "structure_target_price": decimal_to_api(plan.structure_target_price)
            if plan.structure_target_price is not None
            else None,
            "target_risk_amount": decimal_to_api(plan.target_risk_amount)
            if plan.target_risk_amount is not None
            else None,
            "actual_risk_amount": decimal_to_api(plan.actual_risk_amount)
            if plan.actual_risk_amount is not None
            else None,
            "risk_capped_by_margin": plan.risk_capped_by_margin,
            "pretrade_quantity": decimal_to_api(plan.pretrade_quantity)
            if plan.pretrade_quantity is not None
            else None,
            "executed_quantity": decimal_to_api(plan.executed_quantity)
            if plan.executed_quantity is not None
            else None,
            "final_protected_quantity": decimal_to_api(plan.final_protected_quantity)
            if plan.final_protected_quantity is not None
            else None,
            "post_fill_actual_risk_amount": decimal_to_api(plan.post_fill_actual_risk_amount)
            if plan.post_fill_actual_risk_amount is not None
            else None,
            "post_fill_required_margin": decimal_to_api(plan.post_fill_required_margin)
            if plan.post_fill_required_margin is not None
            else None,
            "reduced_after_fill": plan.reduced_after_fill,
            "entry_candle_open_time_ms": plan.entry_candle_open_time_ms,
            "entry_deadline_ms": plan.entry_deadline_ms,
            "structure_context": plan.structure_context,
        }

    def _record_entry_window_expired(
        self,
        scan_id: int | None,
        signal: StrategySignalDecision,
        error: EntryWindowExpiredError,
        path: str,
    ) -> bool:
        detail = {
            "entry_window_path": path,
            "entry_window_now_ms": error.now_ms,
            "entry_deadline_ms": error.entry_deadline_ms,
        }
        overlay_saved = self.recorder.update_strategy_signal(
            signal.signal_id,
            decision="ENTRY_WINDOW_EXPIRED",
            reason="ENTRY_WINDOW_EXPIRED_BEFORE_ORDER",
            detail=detail,
        )
        event_saved = bool(overlay_saved) and self.recorder.record_event(
            "strategy_entry_window_expired_before_order",
            {
                "scan_id": scan_id,
                "strategy_id": signal.strategy.strategy_id,
                "signal_id": signal.signal_id,
                **detail,
            },
            signal.candidate.symbol,
        )
        self.logger.warning(
            "%s entry window expired before %s open | symbol=%s now_ms=%s deadline_ms=%s",
            signal.strategy.strategy_id,
            path,
            signal.candidate.symbol,
            error.now_ms,
            error.entry_deadline_ms,
        )
        return bool(overlay_saved and event_saved)

    def _paper_trade_detail(
        self,
        scan_id: int | None,
        signal: StrategySignalDecision,
        plan: TradePlan,
    ) -> dict:
        analysis = signal.analysis
        detail = {
            "scan_id": scan_id,
            "signal_id": signal.signal_id,
            "strategy_name": signal.strategy.name,
            "candidate_universe": signal.candidate.candidate_universe,
            "quote_volume": str(signal.candidate.quote_volume)
            if signal.candidate.quote_volume is not None
            else None,
            "quote_volume_rank": signal.candidate.quote_volume_rank,
            **self._trade_plan_detail(plan),
        }
        if isinstance(
            analysis,
            (
                N06AnalysisResult,
                N07AnalysisResult,
                N08AnalysisResult,
                N09AnalysisResult,
                N10AnalysisResult,
                N11AnalysisResult,
                N12AnalysisResult,
                N13AnalysisResult,
                N14AnalysisResult,
                N15AnalysisResult,
                N16AnalysisResult,
                N17AnalysisResult,
                N18AnalysisResult,
                N19AnalysisResult,
                N20AnalysisResult,
                MicroAnalysisResult,
            ),
        ):
            detail.update(analysis.detail_json())
        else:
            detail["matched_patterns"] = list(analysis.matched_patterns if analysis else ())
        return detail

    def _permanent_signal_audit_snapshot(
        self,
        signal: StrategySignalDecision,
    ) -> dict[str, object]:
        analysis = signal.analysis
        detail: dict[str, object] = {
            "signal_id": signal.signal_id,
            "strategy_id": signal.strategy.strategy_id,
            "strategy_name": signal.strategy.name,
            "symbol": signal.candidate.symbol,
            "decision": signal.decision,
            "reason": signal.reason,
            "candidate_universe": signal.candidate.candidate_universe,
            "quote_volume": str(signal.candidate.quote_volume)
            if signal.candidate.quote_volume is not None
            else None,
            "quote_volume_rank": signal.candidate.quote_volume_rank,
        }
        if isinstance(
            analysis,
            (
                N06AnalysisResult,
                N07AnalysisResult,
                N08AnalysisResult,
                N09AnalysisResult,
                N10AnalysisResult,
                N11AnalysisResult,
                N12AnalysisResult,
                N13AnalysisResult,
                N14AnalysisResult,
                N15AnalysisResult,
                N16AnalysisResult,
                N17AnalysisResult,
                N18AnalysisResult,
                N19AnalysisResult,
                N20AnalysisResult,
                MicroAnalysisResult,
            ),
        ):
            detail["analysis"] = analysis.detail_json()
        elif isinstance(analysis, AnalysisResult):
            detail["analysis"] = analysis.detail
            detail["matched_patterns"] = list(analysis.matched_patterns)
        else:
            detail["analysis"] = None
        return detail

    def _reconcile_single_strategy_after_publication(self) -> bool:
        ready, local_state = self._attested_local_execution_state()
        if not ready:
            return False
        if self._clear_finalized_n16_state_before_reconciliation(local_state):
            return False
        repaired, local_state = (
            self._repair_n16_live_audit_before_reconciliation(local_state)
        )
        if not repaired:
            return False
        try:
            close_result = self._close_dry_run_with_attested_state(local_state)
        except BinanceAPIError as exc:
            self.logger.error(
                "Local state changed before dry-run reconciliation; blocking "
                "this round | error=%s",
                exc,
            )
            return False
        if close_result:
            try:
                close_identity = self._n16_local_state_claim_identity(
                    close_result.state
                )
            except Exception as exc:
                self.logger.error(
                    "Dry-run close identity is invalid; blocking this round | error=%s",
                    exc,
                )
                return False
            if close_identity is not None:
                try:
                    finalized = self._finalize_n16_dry_close(close_result)
                except Exception as exc:
                    self.logger.error(
                        "N16 dry finalization failed closed; state retained | error=%s",
                        exc,
                    )
                    return False
                if not finalized:
                    self.logger.error(
                        "N16 dry finalization was not fully acknowledged; state retained"
                    )
                    return False
                self.logger.info(
                    "N16 dry-run position atomically finalized | symbol=%s reason=%s",
                    close_result.state.symbol,
                    close_result.exit_reason,
                )
                ready, local_state = self._attested_local_execution_state()
                if not ready:
                    return False
            else:
                trade_id = self.recorder.record_trade_close(
                    close_result.state,
                    close_result.exit_reason,
                    decimal_to_api(close_result.exit_price),
                    decimal_to_api(close_result.mark_price),
                    decimal_to_api(close_result.pnl_amount),
                    decimal_to_api(close_result.pnl_pct),
                    decimal_to_api(close_result.balance_after),
                )
                self._set_symbol_cooldown(
                    close_result.state.symbol, close_result.exit_reason, trade_id
                )
                self.logger.info(
                    "Dry-run position closed | symbol=%s reason=%s exit=%s mark=%s pnl=%s balance_after=%s",
                    close_result.state.symbol,
                    close_result.exit_reason,
                    decimal_to_api(close_result.exit_price),
                    decimal_to_api(close_result.mark_price),
                    decimal_to_api(close_result.pnl_amount),
                    decimal_to_api(close_result.balance_after),
                )
                ready, local_state = self._attested_local_execution_state()
                if not ready:
                    return False

        try:
            sync_result = self._sync_with_attested_state(local_state)
        except BinanceAPIError as exc:
            self.logger.error(
                "Local state changed before exchange reconciliation; blocking "
                "this round | error=%s",
                exc,
            )
            return False
        pending_detail = getattr(sync_result, "pending_detail", None)
        pending_resolved = bool(
            getattr(sync_result, "pending_resolved", False)
        )
        if pending_detail is not None:
            local_pending_state = local_state
            pending_strategy_id = (
                self._strategy_id_from_state(local_pending_state)
                if local_pending_state is not None
                else None
            )
            pending_event_saved = self.recorder.record_event(
                "execution_pending_resolved"
                if pending_resolved
                else "execution_pending",
                {
                    "strategy_id": pending_strategy_id,
                    "resolved": pending_resolved,
                    "detail": pending_detail,
                },
                local_pending_state.symbol
                if local_pending_state is not None
                else None,
            )
            resolved_reason = str(
                pending_detail.get("reason", "")
            )
            pending_state_saved = resolved_reason == (
                "MARKET_ORDER_CONFIRMED_NOT_EXECUTED"
            )
            if (
                pending_resolved
                and not pending_state_saved
                and pending_strategy_id is not None
            ):
                if resolved_reason in _NO_LIVE_RESULT_CLEANUP_REASONS:
                    pending_state_saved = True
                else:
                    pending_state_saved = (
                        self.recorder.mark_strategy_live_result_pending(
                            pending_strategy_id,
                            resolved_reason or "EXECUTION_CLEANUP_RESOLVED",
                        )
                    )
            if (
                pending_resolved
                and pending_event_saved
                and pending_state_saved
            ):
                try:
                    pending_cleared = bool(
                        local_pending_state is not None
                        and self.state.compare_and_clear(
                            local_pending_state
                        )
                    )
                except Exception as exc:
                    pending_cleared = False
                    self.logger.error(
                        "Resolved execution state CAS failed; state retained | "
                        "error=%s",
                        exc,
                    )
                if not pending_cleared:
                    self.logger.error(
                        "Resolved execution state changed before CAS clear; "
                        "state retained"
                    )
            self.logger.error(
                "Execution reconciliation handled; skipping this scan | "
                "resolved=%s reason=%s",
                pending_resolved,
                resolved_reason,
            )
            return False
        if sync_result.closed_state:
            trade_id = self.recorder.record_trade_close(
                sync_result.closed_state,
                "EXCHANGE_POSITION_CLOSED",
                "",
                "",
                "",
                "",
                "",
            )
            self._set_symbol_cooldown(sync_result.closed_state.symbol, "EXCHANGE_POSITION_CLOSED", trade_id)
            self.logger.info(
                "Live position closed on exchange | symbol=%s cooldown_hours=%s",
                sync_result.closed_state.symbol,
                self.config.symbol_cooldown_hours,
            )
            self.state.clear()

        if sync_result.has_position:
            self.recorder.record_event("skip_existing_position")
            self.logger.info("Existing position detected, skipping scan.")
            return False

        return True

    def _run_once_single_strategy(self) -> None:
        if not self._n16_execution_boundary_ready():
            return
        if not self._prepare_n16_local_recovery_before_scan():
            return

        scanned_count, candidates = self.monitor.scan()
        candidate_symbols = [item.symbol for item in candidates]
        scan_id = self.recorder.begin_scan(scanned_count, candidates, self.config.dry_run)
        if scan_id is None:
            self.logger.error(
                "Signal scan audit could not begin; blocking this entire round."
            )
            return
        self.logger.info(
            "Funding scan | scanned=%s candidates=%s",
            scanned_count,
            ",".join(candidate_symbols) or "-",
        )

        opened = False
        scan_completion_allowed = True
        analysis_details: list[str] = []
        try:
            pending_executions: list[
                tuple[FundingCandidate, AnalysisResult]
            ] = []
            signal_audit_complete = True
            recorded_signal_count = 0
            for candidate in candidates:
                cooldown = self.recorder.active_symbol_cooldown(candidate.symbol)
                if cooldown:
                    self.recorder.record_event(
                        "symbol_cooldown_skip",
                        {
                            "scan_id": scan_id,
                            "cooldown_until": cooldown.cooldown_until,
                            "reason": cooldown.reason,
                            "source_trade_id": cooldown.source_trade_id,
                        },
                        candidate.symbol,
                    )
                    analysis_details.append(f"{candidate.symbol}:cooldown_until={cooldown.cooldown_until}")
                    self.logger.info(
                        "Cooldown skip | symbol=%s until=%s reason=%s trade_id=%s",
                        candidate.symbol,
                        cooldown.cooldown_until,
                        cooldown.reason,
                        cooldown.source_trade_id,
                    )
                    cooldown_result = AnalysisResult(
                        symbol=candidate.symbol,
                        passed=False,
                        trend_slope=Decimal("0"),
                        pattern=None,
                        current_bullish=False,
                        detail=(
                            "symbol cooldown active until "
                            f"{cooldown.cooldown_until}: {cooldown.reason}"
                        ),
                    )
                    cooldown_signal_id = self.recorder.record_signal(
                        scan_id,
                        candidate,
                        cooldown_result,
                        "VOID",
                    )
                    if cooldown_signal_id is None:
                        signal_audit_complete = False
                        self.logger.error(
                            "Void signal audit failed; blocking this entire "
                            "round | symbol=%s",
                            candidate.symbol,
                        )
                        break
                    recorded_signal_count += 1
                    continue

                raw_klines = self.client.get_klines(candidate.symbol)
                result = analyze_symbol(
                    candidate.symbol,
                    raw_klines,
                    candidate.mark_price,
                    self.config.trend_window,
                )
                analysis_details.append(f"{candidate.symbol}:{result.detail}")
                self.logger.info("Kline analysis | %s | %s", candidate.symbol, result.detail)

                signal_id = self.recorder.record_signal(
                    scan_id,
                    candidate,
                    result,
                    "PASSED" if result.passed else "REJECTED",
                )
                if signal_id is None:
                    signal_audit_complete = False
                    self.logger.error(
                        "Signal audit failed; blocking this entire round | "
                        "symbol=%s decision=%s",
                        candidate.symbol,
                        "PASSED" if result.passed else "REJECTED",
                    )
                    break
                recorded_signal_count += 1
                if result.passed:
                    pending_executions.append((candidate, result))

            if not signal_audit_complete:
                return
            if recorded_signal_count <= 0:
                self.logger.info(
                    "No single-strategy decisions were generated; retaining "
                    "the previous current signal batch."
                )
                return
            try:
                signal_batch_published = (
                    self.recorder.publish_strategy_signal_batch(
                        scan_id,
                        recorded_signal_count,
                    )
                )
            except Exception as exc:
                signal_batch_published = False
                self.logger.error(
                    "Single-strategy signal batch publish failed; blocking "
                    "all execution | scan_id=%s error=%s",
                    scan_id,
                    exc,
                )
            if (
                not signal_batch_published
                or not self._signal_batch_is_current(scan_id)
            ):
                if not self._n16_execution_boundary_ready():
                    # An indeterminate two-phase publication must remain
                    # untouched for explicit resolution.  Even scan
                    # completion is a Review write and could obscure the
                    # exact STAGING/PREPARED evidence.
                    scan_completion_allowed = False
                self.logger.error(
                    "Single-strategy signal batch was not atomically "
                    "published; blocking all order execution for this round | "
                    "scan_id=%s",
                    scan_id,
                )
                return

            if not self._reconcile_single_strategy_after_publication():
                return

            # Preserve the legacy candidate and fallback order exactly, but
            # execute it only after every decision in this round is durable
            # and the complete batch is the current published generation.
            for candidate, _result in pending_executions:
                try:
                    position_state = self.trader.open_long_with_protection(candidate.symbol, candidate.mark_price)
                except BinanceAPIError as exc:
                    self.recorder.record_trade_failure(scan_id, candidate.symbol, str(exc), self.config.dry_run)
                    try:
                        failed_order_state = self.state.load()
                    except Exception as state_exc:
                        self.recorder.record_event(
                            "single_strategy_order_state_read_failed",
                            {
                                "scan_id": scan_id,
                                "order_error": str(exc),
                                "state_error": str(state_exc),
                            },
                            candidate.symbol,
                        )
                        self.logger.error(
                            "Order failed and local state cannot be verified; "
                            "ending this scan | symbol=%s order_error=%s "
                            "state_error=%s",
                            candidate.symbol,
                            exc,
                            state_exc,
                        )
                        break
                    if failed_order_state is not None:
                        self.recorder.record_event(
                            "single_strategy_order_local_pending",
                            {
                                "scan_id": scan_id,
                                "order_error": str(exc),
                                "orders": failed_order_state.orders,
                            },
                            candidate.symbol,
                        )
                        self.logger.error(
                            "Order failed with durable local state; ending this "
                            "scan | symbol=%s error=%s",
                            candidate.symbol,
                            exc,
                        )
                        break
                    self.logger.error(
                        "Order failed, skipping this candidate | symbol=%s error=%s",
                        candidate.symbol,
                        exc,
                    )
                    continue

                opened = True
                self.recorder.record_trade_open(scan_id, position_state)
                self.logger.info(
                    "Opened long | symbol=%s entry=%s stop=%s take_profit=%s dry_run=%s",
                    position_state.symbol,
                    position_state.entry_price,
                    position_state.stop_loss_price,
                    position_state.take_profit_price,
                    position_state.dry_run,
                )
                break
        finally:
            if scan_completion_allowed:
                self.recorder.complete_scan(scan_id, opened)

        self.logger.info(
            "Poll result | scanned=%s candidates=%s analysis=%s opened=%s",
            scanned_count,
            ",".join(candidate_symbols) or "-",
            " || ".join(analysis_details) or "-",
            opened,
        )

    def _reconcile_multi_strategy_after_publication(self):
        ready, local_state = self._attested_local_execution_state()
        if not ready:
            return None
        if self._clear_finalized_n16_state_before_reconciliation(local_state):
            return None
        repaired, local_state = (
            self._repair_n16_live_audit_before_reconciliation(local_state)
        )
        if not repaired:
            return None
        local_orders = (
            local_state.orders
            if local_state is not None and type(local_state.orders) is dict
            else {}
        )
        local_execution_pending = any(
            key in local_orders
            for key in (
                "execution_pending",
                "execution_cleanup_resolved",
                "emergency_cleanup_pending",
            )
        )
        # Paper close/checkpoint persistence is part of the round's durable
        # pre-execution gate.  Complete it before dry/live reconciliation so
        # a failed close/qualification transaction cannot be followed by
        # leverage, order, protection, or a later paper open.  A durable local
        # execution phase is higher priority: it must reach its exact recovery
        # classifier without touching paper state first.
        paper_close_results = []
        if not local_execution_pending:
            try:
                paper_close_results = (
                    self.paper_trader.close_triggered_open_trades(
                        lambda symbol, start_time_ms: (
                            self.client.get_klines_for_interval(
                                symbol,
                                "1m",
                                limit=1500,
                                start_time_ms=start_time_ms,
                            )
                        ),
                        lambda symbol, start_time_ms, end_time_ms: (
                            self.client.get_aggregate_trades(
                                symbol,
                                start_time_ms,
                                end_time_ms,
                            )
                        ),
                    )
                )
            except PaperTradePersistenceError as exc:
                self.logger.error(
                    "Paper lifecycle persistence failed; blocking all execution "
                    "for this round | error=%s",
                    exc,
                )
                return None
        if paper_close_results:
            self.logger.info(
                "Paper trade close sweep | closed=%s",
                len(paper_close_results),
            )
        try:
            close_result = self._close_dry_run_with_attested_state(local_state)
        except BinanceAPIError as exc:
            self.logger.error(
                "Local state changed before strategy dry-run reconciliation; "
                "blocking this round | error=%s",
                exc,
            )
            return None
        if close_result:
            try:
                close_identity = self._n16_local_state_claim_identity(
                    close_result.state
                )
            except Exception as exc:
                self.logger.error(
                    "Dry-run close identity is invalid; blocking this round | error=%s",
                    exc,
                )
                return None
            if close_identity is not None:
                try:
                    finalized = self._finalize_n16_dry_close(close_result)
                except Exception as exc:
                    self.logger.error(
                        "N16 dry finalization failed closed; state retained | error=%s",
                        exc,
                    )
                    return None
                if not finalized:
                    self.logger.error(
                        "N16 dry finalization was not fully acknowledged; state retained"
                    )
                    return None
            else:
                trade_id = self.recorder.record_trade_close(
                    close_result.state,
                    close_result.exit_reason,
                    decimal_to_api(close_result.exit_price),
                    decimal_to_api(close_result.mark_price),
                    decimal_to_api(close_result.pnl_amount),
                    decimal_to_api(close_result.pnl_pct),
                    decimal_to_api(close_result.balance_after),
                )
                self._set_symbol_cooldown(
                    close_result.state.symbol, close_result.exit_reason, trade_id
                )
                self._record_strategy_live_result_from_state(
                    close_result.state,
                    self._live_result_from_exit_reason(close_result.exit_reason),
                    trade_id,
                )
            self.logger.info(
                "Dry-run strategy/live slot closed | symbol=%s reason=%s pnl=%s",
                close_result.state.symbol,
                close_result.exit_reason,
                decimal_to_api(close_result.pnl_amount),
            )
            ready, local_state = self._attested_local_execution_state()
            if not ready:
                return None

        try:
            sync_result = self._sync_with_attested_state(local_state)
        except BinanceAPIError as exc:
            self.logger.error(
                "Local state changed before strategy exchange reconciliation; "
                "blocking this round | error=%s",
                exc,
            )
            return None
        pending_detail = getattr(sync_result, "pending_detail", None)
        pending_resolved = bool(
            getattr(sync_result, "pending_resolved", False)
        )
        strategy_execution_blocked: set[str] = set()
        local_strategy_state = local_state
        if local_strategy_state is not None:
            local_strategy_id = self._strategy_id_from_state(local_strategy_state)
            if local_strategy_id is not None:
                strategy_execution_blocked.add(local_strategy_id)
        live_result_blocked = False
        if pending_detail is not None:
            pending_strategy_id = (
                self._strategy_id_from_state(local_strategy_state)
                if local_strategy_state is not None
                else None
            )
            if pending_strategy_id is not None:
                strategy_execution_blocked.add(pending_strategy_id)
            pending_audit_saved = self.recorder.record_event(
                "strategy_execution_pending_resolved"
                if pending_resolved
                else "strategy_execution_pending",
                {
                    "strategy_id": pending_strategy_id,
                    "resolved": pending_resolved,
                    "detail": pending_detail,
                },
                local_strategy_state.symbol
                if local_strategy_state is not None
                else None,
            )
            resolved_reason = str(
                pending_detail.get("reason", "")
            )
            pending_state_saved = True
            if (
                pending_resolved
                and resolved_reason != "MARKET_ORDER_CONFIRMED_NOT_EXECUTED"
            ):
                if resolved_reason in _NO_LIVE_RESULT_CLEANUP_REASONS:
                    pending_state_saved = True
                else:
                    pending_state_saved = bool(
                        pending_strategy_id is not None
                        and self.recorder.mark_strategy_live_result_pending(
                            pending_strategy_id,
                            resolved_reason or "EXECUTION_CLEANUP_RESOLVED",
                        )
                    )
            if (
                pending_resolved
                and pending_audit_saved
                and pending_state_saved
            ):
                try:
                    pending_cleared = bool(
                        local_strategy_state is not None
                        and self.state.compare_and_clear(
                            local_strategy_state
                        )
                    )
                except Exception as exc:
                    pending_cleared = False
                    self.logger.error(
                        "Resolved execution state CAS failed; state retained | "
                        "error=%s",
                        exc,
                    )
                if not pending_cleared:
                    self.logger.error(
                        "Resolved execution state changed before CAS clear; "
                        "state retained"
                    )
            self.logger.error(
                "Strategy execution reconciliation handled; skipping this scan | "
                "strategy=%s resolved=%s reason=%s",
                pending_strategy_id,
                pending_resolved,
                resolved_reason,
            )
            return None
        if sync_result.closed_state:
            live_result_blocked = self._handle_closed_strategy_live_state(sync_result.closed_state)

        return sync_result, strategy_execution_blocked, live_result_blocked

    def _attest_market_scan_symbols(self, market_scan):
        candidates = tuple(market_scan.all_candidates)
        candidate_symbols = tuple(
            canonical_exchange_symbol(candidate.symbol)
            for candidate in candidates
        )
        authenticated_symbols = getattr(
            market_scan, "authenticated_symbols", frozenset()
        )
        authenticated_sha256 = getattr(
            market_scan, "authenticated_symbols_sha256", None
        )
        if isinstance(self.monitor, FundingMonitor):
            registry = attest_authenticated_symbol_set(
                authenticated_symbols,
                authenticated_sha256,
            )
        else:
            # Unit/integration adapters have no exchangeInfo transport.  They
            # still traverse the exact structural boundary; production's
            # FundingMonitor must always provide the authenticated registry.
            registry = frozenset(candidate_symbols)
            authenticated_sha256 = authenticated_symbol_set_sha256(registry)
        if any(symbol not in registry for symbol in candidate_symbols):
            raise ValueError(
                "market candidate is absent from authenticated exchangeInfo"
            )
        return registry, authenticated_sha256

    def _fetch_strategy_kline_generation(self, symbols):
        """Fetch the exact shared Kline set once with bounded I/O parallelism."""

        symbol_tuple = tuple(symbols)
        if (
            not symbol_tuple
            or len(symbol_tuple) != len(set(symbol_tuple))
            or any(type(symbol) is not str or not symbol for symbol in symbol_tuple)
        ):
            raise ValueError("strategy Kline generation is invalid")
        executor = ThreadPoolExecutor(
            max_workers=min(_STRATEGY_KLINE_MAX_WORKERS, len(symbol_tuple)),
            thread_name_prefix="strategy-kline",
        )
        slots = getattr(self, "_kline_request_slots", None)
        if slots is None:
            slots = threading.BoundedSemaphore(_STRATEGY_KLINE_MAX_WORKERS)

        def fetch(symbol):
            with slots:
                return self.client.get_klines(symbol)

        futures = {}
        rows_by_symbol = {}
        observed_at_ms = {}
        failures = []
        try:
            for symbol in symbol_tuple:
                futures[executor.submit(fetch, symbol)] = symbol
            pending = set(futures)
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    symbol = futures[future]
                    try:
                        rows_by_symbol[symbol] = future.result()
                    except BinanceAPIError as exc:
                        failures.append((symbol, exc))
                    observed_at_ms[symbol] = int(time.time() * 1000)
        finally:
            executor.shutdown(wait=True, cancel_futures=True)
        return rows_by_symbol, observed_at_ms, tuple(
            sorted(failures, key=lambda item: item[0])
        )

    def _run_once_multi_strategy(self) -> None:
        round_started_monotonic = time.monotonic()
        if not self._n16_execution_boundary_ready():
            return
        if not self._prepare_n16_local_recovery_before_scan():
            return

        market_started_monotonic = time.monotonic()
        market_scan = self.monitor.scan_for_strategies(self._strategy_volume_top_n())
        try:
            authenticated_symbols, authenticated_symbols_sha256 = (
                self._attest_market_scan_symbols(market_scan)
            )
        except Exception as exc:
            self.logger.error(
                "Authenticated exchange symbol boundary failed closed before "
                "scan | error=%s",
                exc,
            )
            return
        market_finished_monotonic = time.monotonic()
        candidate_groups = {
            "negative_funding": market_scan.funding_candidates,
            "quote_volume_top": market_scan.volume_candidates,
        }
        checked_at_ms = int(time.time() * 1000)
        # Only the authoritative current market universe owns the strict live
        # axis/value readiness gate.  Frozen lifecycle members are fetched in
        # the same de-duplicated request set, but their historical analyzers
        # must decide whether a halted/settling row can close or must wait;
        # one departed member must never prevent begin_scan for all 25
        # strategies.
        live_required_symbol_set = {
            item.symbol for item in market_scan.all_candidates
        }
        candidate_symbol_set = set(live_required_symbol_set)
        if any(strategy.strategy_id == "N14" for strategy in self.strategies):
            try:
                current_open_time_ms = checked_at_ms // 900_000 * 900_000
                stale_result = self.recorder.expire_stale_n14_active_episodes(
                    "N14",
                    current_open_time_ms,
                )
                if stale_result != "OK":
                    raise RuntimeError(stale_result)
                candidate_symbol_set.update(
                    self.recorder.get_required_n14_snapshot_symbols(
                        "N14",
                        current_open_time_ms,
                    )
                )
            except Exception as exc:
                self.logger.warning(
                    "Unable to restore N14 frozen members; N14 will fail closed: %s",
                    exc,
                )
        if any(strategy.strategy_id == "N15" for strategy in self.strategies):
            try:
                candidate_symbol_set.update(
                    self.recorder.get_pending_n15_snapshot_symbols("N15")
                )
            except Exception as exc:
                self.logger.warning(
                    "Unable to restore N15 frozen members; N15 will fail closed: %s",
                    exc,
                )
        if any(strategy.strategy_id == "N16" for strategy in self.strategies):
            try:
                candidate_symbol_set.update(
                    self.recorder.get_required_n16_episode_symbols("N16")
                )
            except Exception as exc:
                self.logger.warning(
                    "Unable to restore N16 frozen members; N16 will fail closed: %s",
                    exc,
                )
        if any(strategy.strategy_id == "N17" for strategy in self.strategies):
            try:
                candidate_symbol_set.update(
                    self.recorder.get_required_n17_family_symbols()
                )
            except Exception as exc:
                self.logger.warning(
                    "Unable to restore N17 frozen members; N17 will fail closed: %s",
                    exc,
                )
        if any(strategy.strategy_id == "N19" for strategy in self.strategies):
            try:
                candidate_symbol_set.update(
                    self.recorder.get_required_n19_family_symbols()
                )
            except Exception as exc:
                self.logger.warning(
                    "Unable to restore N19 frozen members; N19 will fail closed: %s",
                    exc,
                )
                # The dropped member is outside the instantaneous Top100, so
                # the scheduler cannot rediscover this missing fetch later.
                # Abort before begin_scan and before every side effect.
                return
        if any(strategy.strategy_id == "N18" for strategy in self.strategies):
            try:
                candidate_symbol_set.update(
                    self.recorder.get_required_n18_family_symbols()
                )
            except Exception as exc:
                self.logger.warning(
                    "Unable to restore N18 frozen members; N18 will fail closed: %s",
                    exc,
                )
                return
        if any(strategy.strategy_id == "N20" for strategy in self.strategies):
            try:
                candidate_symbol_set.update(
                    self.recorder.get_required_n20_frozen_symbols()
                )
            except Exception as exc:
                self.logger.warning(
                    "Unable to restore N20 frozen members; N20 will fail closed: %s",
                    exc,
                )
                return
        candidate_symbols = sorted(candidate_symbol_set)
        kline_started_monotonic = time.monotonic()
        if candidate_symbols:
            (
                raw_klines_by_symbol,
                kline_observed_at_ms,
                kline_fetch_failures_tuple,
            ) = self._fetch_strategy_kline_generation(candidate_symbols)
        else:
            raw_klines_by_symbol = {}
            kline_observed_at_ms = {}
            kline_fetch_failures_tuple = ()
        kline_fetch_failures = list(kline_fetch_failures_tuple)
        for symbol, exc in kline_fetch_failures:
            self.logger.error(
                "Kline fetch failed | symbol=%s error=%s", symbol, exc
            )
        kline_finished_monotonic = time.monotonic()

        micro_enabled = any(
            strategy.strategy_id in MICRO_STRATEGY_IDS
            for strategy in self.strategies
        )
        live_required_fetch_failures = [
            item for item in kline_fetch_failures
            if item[0] in live_required_symbol_set
        ]
        boundary_retry_count = 0
        authenticated_snapshot_observed_at_ms = None
        if (
            micro_enabled
            and not live_required_fetch_failures
            and live_required_symbol_set
        ):
            while True:
                snapshot_observed_at_ms = int(time.time() * 1000)
                live_raw_klines = {
                    symbol: raw_klines_by_symbol[symbol]
                    for symbol in live_required_symbol_set
                    if symbol in raw_klines_by_symbol
                }
                live_observed_at_ms = {
                    symbol: kline_observed_at_ms[symbol]
                    for symbol in live_required_symbol_set
                    if symbol in kline_observed_at_ms
                }
                axis_unready_symbols = _stale_kline_snapshot_symbols(
                    live_raw_klines,
                    live_observed_at_ms,
                    snapshot_observed_at_ms,
                )
                value_unready_symbols = _unready_live_kline_value_symbols(
                    live_raw_klines,
                    snapshot_observed_at_ms,
                )
                if (
                    axis_unready_symbols is None
                    or value_unready_symbols is None
                ):
                    break
                retry_symbols = tuple(sorted(
                    set(axis_unready_symbols) | set(value_unready_symbols)
                ))
                if not retry_symbols:
                    authenticated_snapshot_observed_at_ms = (
                        snapshot_observed_at_ms
                    )
                    break
                if boundary_retry_count >= _KLINE_BOUNDARY_RETRY_LIMIT:
                    micro_cache = getattr(
                        self, "micro_observation_cache", None
                    )
                    if micro_cache is not None:
                        micro_cache.clear()
                    self.logger.warning(
                        "Kline boundary snapshot is not ready; N21-N25 and "
                        "all execution fail closed before scan | "
                        "axis_not_ready=%s value_not_ready=%s attempts=%s",
                        len(axis_unready_symbols),
                        len(value_unready_symbols),
                        boundary_retry_count + 1,
                    )
                    return
                boundary_retry_count += 1
                time.sleep(_KLINE_BOUNDARY_RETRY_DELAY_SECONDS)
                retry_failed = False
                (
                    retry_rows,
                    retry_observed_at_ms,
                    retry_failures,
                ) = self._fetch_strategy_kline_generation(retry_symbols)
                raw_klines_by_symbol.update(retry_rows)
                kline_observed_at_ms.update(retry_observed_at_ms)
                for symbol, exc in retry_failures:
                    retry_failed = True
                    kline_fetch_failures.append((symbol, exc))
                    self.logger.error(
                        "Kline boundary refresh failed | symbol=%s error=%s",
                        symbol,
                        exc,
                    )
                if retry_failed:
                    break
            if authenticated_snapshot_observed_at_ms is not None:
                checked_at_ms = authenticated_snapshot_observed_at_ms
                lifecycle_raw_klines = {
                    symbol: raw_klines_by_symbol[symbol]
                    for symbol in set(raw_klines_by_symbol).difference(
                        live_required_symbol_set
                    )
                }
                lifecycle_value_unready = (
                    _unready_live_kline_value_symbols(
                        lifecycle_raw_klines,
                        authenticated_snapshot_observed_at_ms,
                    )
                    if lifecycle_raw_klines
                    else ()
                )
                if lifecycle_value_unready:
                    self.logger.warning(
                        "Frozen lifecycle Kline has no initialized live "
                        "value; readiness is scoped to its owning strategy | "
                        "count=%s symbols=%s",
                        len(lifecycle_value_unready),
                        ",".join(lifecycle_value_unready[:8]),
                    )

        snapshot_ready_monotonic = time.monotonic()
        scan_id = self.recorder.begin_scan(
            market_scan.scanned_count,
            market_scan.all_candidates,
            self.config.dry_run,
        )
        if scan_id is None:
            self.logger.error(
                "Strategy signal scan audit could not begin; blocking this entire round."
            )
            return
        self.logger.info(
            "Multi-strategy market scan | scanned=%s funding_candidates=%s volume_candidates=%s",
            market_scan.scanned_count,
            len(market_scan.funding_candidates),
            len(market_scan.volume_candidates),
        )

        opened_live = False
        scan_completion_allowed = True
        micro_sampler = getattr(self, "micro_observation_sampler", None)
        micro_sampler_active = bool(
            micro_enabled
            and micro_sampler is not None
            and micro_sampler.is_running
        )
        micro_lease: MicroObservationLease | None = None
        micro_frozen_monotonic: float | None = None
        micro_lease_resolved = False
        try:
            for symbol, exc in kline_fetch_failures:
                self.recorder.record_event(
                    "strategy_kline_fetch_failed",
                    {"scan_id": scan_id, "error": str(exc)},
                    symbol,
                )

            micro_cache = getattr(self, "micro_observation_cache", None)
            if micro_cache is None:
                micro_cache = MicroObservationCache()
                self.micro_observation_cache = micro_cache
            micro_proposal = None
            micro_context_complete = True
            micro_context_failure_code = None
            micro_context_failure_symbol = None
            sampler_fallback_factory = None
            try:
                if not micro_enabled:
                    raise StopIteration
                top100 = market_scan.volume_candidates
                ranked = tuple(
                    (candidate.symbol, candidate.quote_volume_rank)
                    for candidate in top100
                    if type(candidate.quote_volume_rank) is int
                )
                universe_sha256 = build_universe_sha256(ranked)
                if type(market_scan.premium_observed_at_ms) is not int:
                    micro_context_failure_code = "MICRO_PREMIUM_TIME_UNAVAILABLE"
                    raise MicroObservationError(
                        "premium observation time is unavailable"
                    )
                for candidate in top100:
                    if (
                        candidate.symbol not in raw_klines_by_symbol
                        or candidate.symbol not in kline_observed_at_ms
                        or candidate.symbol not in market_scan.premium_by_symbol
                    ):
                        micro_context_failure_code = (
                            "MICRO_TOP100_SOURCE_INCOMPLETE"
                        )
                        micro_context_failure_symbol = candidate.symbol
                        raise MicroObservationError(
                            "Top100 micro source is incomplete"
                        )

                def build_observations(
                    observation_scan_id,
                    target_generation,
                    observation_boot_id,
                ):
                    nonlocal micro_context_failure_code
                    nonlocal micro_context_failure_symbol
                    result = {}
                    for candidate in top100:
                        symbol = candidate.symbol
                        try:
                            result[symbol] = build_micro_observation(
                                symbol=symbol,
                                scan_id=observation_scan_id,
                                generation=target_generation,
                                boot_id=observation_boot_id,
                                observed_at_ms=kline_observed_at_ms[symbol],
                                premium_observed_at_ms=(
                                    market_scan.premium_observed_at_ms
                                ),
                                raw_klines=raw_klines_by_symbol[symbol],
                                premium_row=(
                                    market_scan.premium_by_symbol[symbol]
                                ),
                                quote_volume_rank=(
                                    candidate.quote_volume_rank
                                ),
                                universe_sha256=universe_sha256,
                            )
                        except Exception:
                            micro_context_failure_code = (
                                "MICRO_OBSERVATION_BUILD_FAILED"
                            )
                            micro_context_failure_symbol = symbol
                            raise
                    return result

                if micro_sampler_active:
                    def sampler_fallback_factory(identity):
                        return build_observations(
                            identity.sample_ordinal,
                            identity.generation,
                            identity.boot_id,
                        )
                else:
                    observations = build_observations(
                        scan_id,
                        micro_cache.generation + 1,
                        micro_cache.boot_id,
                    )
                    micro_context_failure_code = "MICRO_CACHE_PROPOSAL_FAILED"
                    micro_context_failure_symbol = None
                    micro_proposal = micro_cache.propose(scan_id, observations)
                micro_context_failure_code = None
            except StopIteration:
                pass
            except Exception as exc:
                micro_context_complete = False
                micro_cache.clear()
                self.logger.warning(
                    "Micro observation proposal is incomplete; N21-N25 and "
                    "all execution fail closed | scan_id=%s error=%s",
                    scan_id,
                    exc,
                )

            ordered_micro_symbols = tuple(
                candidate.symbol
                for candidate in sorted(
                    market_scan.volume_candidates,
                    key=lambda item: (
                        item.quote_volume_rank,
                        item.symbol,
                    ),
                )
            )

            def provide_micro_windows():
                nonlocal micro_lease, micro_frozen_monotonic
                if (
                    not micro_sampler_active
                    or micro_sampler is None
                ):
                    raise MicroObservationError(
                        "micro sampler is not active"
                    )
                if micro_lease is None:
                    micro_lease = micro_sampler.freeze_for_scan(
                        scan_id=scan_id,
                        current_symbols=ordered_micro_symbols,
                        fallback_observations={},
                        fallback_factory=sampler_fallback_factory,
                        captured_at_ms=int(time.time() * 1000),
                    )
                    micro_frozen_monotonic = time.monotonic()
                return micro_lease.windows, micro_lease.captured_at_ms

            scheduler_kwargs = {"checked_at_ms": checked_at_ms}
            if isinstance(self.strategy_scheduler, StrategyScheduler):
                scheduler_kwargs.update(
                    authenticated_symbols=authenticated_symbols,
                    authenticated_symbols_sha256=(
                        authenticated_symbols_sha256
                    ),
                    micro_windows=(
                        micro_proposal.windows
                        if micro_proposal is not None
                        else {}
                    ),
                    micro_context_complete=micro_context_complete,
                    micro_context_failure_code=micro_context_failure_code,
                    micro_context_failure_symbol=micro_context_failure_symbol,
                )
                if micro_sampler_active and micro_context_complete:
                    scheduler_kwargs["micro_window_provider"] = (
                        provide_micro_windows
                    )
            scheduler_started_monotonic = time.monotonic()
            scheduler_result = self.strategy_scheduler.evaluate(
                scan_id,
                candidate_groups,
                raw_klines_by_symbol,
                **scheduler_kwargs,
            )
            scheduler_finished_monotonic = time.monotonic()
            self.logger.info(
                "Strategy round phase timing | scan_id=%s "
                "recovery_seconds=%.3f market_seconds=%.3f "
                "kline_seconds=%.3f readiness_seconds=%.3f "
                "pre_scheduler_seconds=%.3f scheduler_seconds=%.3f "
                "micro_freeze_to_publish_seconds=%.3f "
                "elapsed_seconds=%.3f "
                "kline_symbols=%s kline_workers=%s "
                "exchange_symbol_set_sha256=%s",
                scan_id,
                market_started_monotonic - round_started_monotonic,
                market_finished_monotonic - market_started_monotonic,
                kline_finished_monotonic - kline_started_monotonic,
                snapshot_ready_monotonic - kline_finished_monotonic,
                scheduler_started_monotonic - snapshot_ready_monotonic,
                scheduler_finished_monotonic - scheduler_started_monotonic,
                (
                    scheduler_finished_monotonic - micro_frozen_monotonic
                    if micro_frozen_monotonic is not None
                    else -1.0
                ),
                scheduler_finished_monotonic - round_started_monotonic,
                len(candidate_symbols),
                min(_STRATEGY_KLINE_MAX_WORKERS, len(candidate_symbols)),
                authenticated_symbols_sha256,
            )
            if (
                not scheduler_result.signal_batch_published
                or not self._signal_batch_is_current(scan_id)
            ):
                micro_cache.clear()
                if micro_sampler_active and micro_sampler is not None:
                    micro_sampler.abort(micro_lease)
                    micro_lease_resolved = True
                self.logger.error(
                    "Strategy signal batch was not atomically published; "
                    "blocking all plan, paper and live execution for this round | "
                    "scan_id=%s audit_failures=%s audit_failure_overflow=%s",
                    scan_id,
                    json.dumps(
                        [
                            failure.log_payload()
                            for failure in getattr(
                                scheduler_result, "signal_audit_failures", ()
                            )
                        ],
                        ensure_ascii=True,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    getattr(
                        scheduler_result, "signal_audit_failure_overflow", 0
                    ),
                )
                if not self._n16_execution_boundary_ready():
                    scan_completion_allowed = False
                else:
                    self.recorder.record_event(
                        "strategy_signal_batch_execution_blocked",
                        {"scan_id": scan_id},
                    )
                return

            micro_cache_committed = not micro_enabled
            if micro_sampler_active and micro_sampler is not None:
                try:
                    micro_cache_committed = bool(
                        micro_lease is not None
                        and micro_sampler.confirm(
                            micro_lease,
                            current_scan_id=scan_id,
                        )
                    )
                except Exception as exc:
                    micro_cache_committed = False
                    self.logger.warning(
                        "Micro observation sampler publication CAS raised; "
                        "treating the generation as ambiguous | "
                        "scan_id=%s error=%s",
                        scan_id,
                        exc,
                    )
                micro_lease_resolved = True
            elif micro_enabled:
                try:
                    micro_cache_committed = bool(
                        micro_proposal is not None
                        and micro_context_complete
                        and micro_cache.commit(
                            micro_proposal,
                            current_scan_id=scan_id,
                        )
                    )
                except Exception as exc:
                    micro_cache_committed = False
                    self.logger.warning(
                        "Micro observation cache publication CAS raised; "
                        "treating the cache generation as ambiguous | "
                        "scan_id=%s error=%s",
                        scan_id,
                        exc,
                    )
            if not micro_cache_committed:
                micro_cache.clear()
                self.logger.error(
                    "Micro observation cache publication CAS failed; blocking "
                    "all plan, paper and live execution for this round | "
                    "scan_id=%s",
                    scan_id,
                )
                return

            if not self._n16_current_claims_ready(
                scan_id,
                scheduler_result.passed_signals,
            ):
                return
            if micro_frozen_monotonic is not None:
                freeze_to_execution = (
                    time.monotonic() - micro_frozen_monotonic
                )
                self.logger.info(
                    "Micro publication execution boundary timing | "
                    "scan_id=%s freeze_to_execution_seconds=%.3f "
                    "remaining_entry_window_seconds=%.3f",
                    scan_id,
                    freeze_to_execution,
                    120.0 - freeze_to_execution,
                )

            plan_cache: dict[tuple[str, str, str], TradePlan] = {}
            plans_by_signal_identity: dict[int, TradePlan] = {}
            for signal in scheduler_result.passed_signals:
                if signal.signal_id is None:
                    self.recorder.record_event(
                        "strategy_execution_blocked_missing_signal_audit",
                        {
                            "scan_id": scan_id,
                            "strategy_id": signal.strategy.strategy_id,
                            "reason": "SIGNAL_AUDIT_PERSIST_FAILED",
                        },
                        signal.candidate.symbol,
                    )
                    continue
                try:
                    signal_n16_marked = (
                        signal.strategy.strategy_id == "N16"
                        or signal.strategy.stop_mode == _N16_STOP_MODE
                    )
                    if signal_n16_marked:
                        if (
                            signal.strategy.strategy_id != "N16"
                            or signal.strategy.stop_mode != _N16_STOP_MODE
                            or type(signal.signal_id) is not int
                            or signal.signal_id <= 0
                            or not isinstance(
                                signal.analysis, N16AnalysisResult
                            )
                            or type(signal.analysis.structure_id) is not str
                        ):
                            raise _N16PlanIntegrityError(
                                "N16_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
                            )
                    cache_key = self._signal_plan_cache_key(signal)
                    if cache_key not in plan_cache:
                        built_plan = self._build_strategy_plan(signal)
                        built_plan = self._n16_plan_with_published_identity(
                            signal,
                            built_plan,
                        )
                        built_plan = self._n17_plan_with_published_identity(
                            signal,
                            built_plan,
                        )
                        built_plan = self._n19_plan_with_published_identity(
                            signal,
                            built_plan,
                        )
                        built_plan = self._n18_plan_with_published_identity(
                            signal,
                            built_plan,
                        )
                        built_plan = self._n20_plan_with_published_identity(
                            signal,
                            built_plan,
                        )
                        built_plan = self._micro_plan_with_published_identity(
                            signal,
                            built_plan,
                        )
                        plan_cache[cache_key] = built_plan
                    plan = plan_cache[cache_key]
                    plan = self._n16_plan_with_published_identity(
                        signal,
                        plan,
                    )
                    plan = self._n17_plan_with_published_identity(
                        signal,
                        plan,
                    )
                    plan = self._n19_plan_with_published_identity(
                        signal,
                        plan,
                    )
                    plan = self._n18_plan_with_published_identity(
                        signal,
                        plan,
                    )
                    plan = self._n20_plan_with_published_identity(
                        signal,
                        plan,
                    )
                    plan = self._micro_plan_with_published_identity(
                        signal,
                        plan,
                    )
                    if signal.strategy.strategy_id == "N16":
                        try:
                            self.recorder.assert_n16_execution_claim(
                                signal.signal_id,
                                plan.symbol,
                                plan.structure_id,
                            )
                        except Exception as exc:
                            raise _N16PlanIntegrityError(
                                "N16_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
                            ) from exc
                    if signal.strategy.strategy_id == "N17":
                        try:
                            self.recorder.assert_n17_execution_claim(
                                signal.signal_id,
                                plan.symbol,
                                plan.structure_id,
                            )
                        except Exception as exc:
                            raise _N17PlanIntegrityError(
                                "N17_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
                            ) from exc
                    if signal.strategy.strategy_id == "N19":
                        try:
                            self.recorder.assert_n19_execution_claim(
                                signal.signal_id,
                                plan.symbol,
                                plan.structure_id,
                            )
                        except Exception as exc:
                            raise _N19PlanIntegrityError(
                                "N19_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
                            ) from exc
                    if signal.strategy.strategy_id == "N18":
                        try:
                            self.recorder.assert_n18_execution_claim(
                                signal.signal_id,
                                plan.symbol,
                                plan.structure_id,
                            )
                        except Exception as exc:
                            raise _N18PlanIntegrityError(
                                "N18_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
                            ) from exc
                    if signal.strategy.strategy_id == "N20":
                        try:
                            self.recorder.assert_n20_execution_claim(
                                signal.signal_id,
                                plan.symbol,
                                plan.structure_id,
                            )
                        except Exception as exc:
                            raise _N20PlanIntegrityError(
                                "N20_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
                            ) from exc
                    if signal.strategy.strategy_id in MICRO_STRATEGY_IDS:
                        try:
                            self.recorder.assert_micro_execution_claim(
                                signal.signal_id,
                                signal.strategy.strategy_id,
                                plan.symbol,
                                plan.structure_id,
                            )
                        except Exception as exc:
                            raise _MicroPlanIntegrityError(
                                "MICRO_PLAN_PUBLISHED_CLAIM_IDENTITY_CONFLICT"
                            ) from exc
                except (
                    _N16PlanIntegrityError,
                    _N17PlanIntegrityError,
                    _N18PlanIntegrityError,
                    _N19PlanIntegrityError,
                    _N20PlanIntegrityError,
                    _MicroPlanIntegrityError,
                ) as exc:
                    strategy_id = signal.strategy.strategy_id
                    overlay_saved = self.recorder.update_strategy_signal(
                        signal.signal_id,
                        decision="EXECUTION_BLOCKED",
                        reason=str(exc),
                        detail={"plan_integrity_error": str(exc)},
                    )
                    if overlay_saved:
                        self.recorder.record_event(
                            "strategy_plan_claim_integrity_blocked",
                            {
                                "scan_id": scan_id,
                                "strategy_id": strategy_id,
                                "signal_id": signal.signal_id,
                                "error": str(exc),
                            },
                            signal.candidate.symbol,
                        )
                    self.logger.error(
                        "%s plan identity conflicts with its published claim; "
                        "blocking every paper, live and exchange side effect "
                        "for this round | symbol=%s signal_id=%s overlay_saved=%s",
                        strategy_id,
                        signal.candidate.symbol,
                        signal.signal_id,
                        overlay_saved,
                    )
                    return
                except BinanceAPIError as exc:
                    audit_error = _bounded_audit_error(exc)
                    overlay_saved = self.recorder.update_strategy_signal(
                        signal.signal_id,
                        decision="PLAN_REJECTED",
                        reason="TRADE_PLAN_INVALID",
                        detail={"plan_error": audit_error},
                    )
                    if not overlay_saved:
                        self.logger.error(
                            "Strategy plan rejection overlay failed; blocking "
                            "every paper, live and exchange side effect for "
                            "this round | strategy=%s symbol=%s signal_id=%s",
                            signal.strategy.strategy_id,
                            signal.candidate.symbol,
                            signal.signal_id,
                        )
                        return
                    self.recorder.record_event(
                        "strategy_trade_plan_failed",
                        {
                            "scan_id": scan_id,
                            "strategy_id": signal.strategy.strategy_id,
                            "signal_id": signal.signal_id,
                            "error": audit_error,
                        },
                        signal.candidate.symbol,
                    )
                    self.logger.error(
                        "Strategy plan failed | strategy=%s symbol=%s error=%s",
                        signal.strategy.strategy_id,
                        signal.candidate.symbol,
                        audit_error,
                    )
                    continue

                plans_by_signal_identity[id(signal)] = plan
                if not self.recorder.update_strategy_signal(
                    signal.signal_id,
                    detail=self._trade_plan_detail(plan),
                ):
                    self.logger.error(
                        "Strategy plan overlay audit failed; blocking every "
                        "paper, live and exchange side effect for this round | "
                        "strategy=%s symbol=%s signal_id=%s",
                        signal.strategy.strategy_id,
                        signal.candidate.symbol,
                        signal.signal_id,
                    )
                    return

            reconciliation = self._reconcile_multi_strategy_after_publication()
            if reconciliation is None:
                return
            (
                sync_result,
                strategy_execution_blocked,
                live_result_blocked,
            ) = reconciliation

            live_blocked = sync_result.has_position or live_result_blocked
            # The complete decision batch is published before live
            # reconciliation.  N01-N05 no longer use paper performance as a
            # qualification gate; durable activity/recovery ownership still
            # blocks duplicate execution.
            valid_live_candidates = []
            for live_candidate in scheduler_result.live_candidates:
                signal = live_candidate.signal
                if (
                    signal.signal_id is None
                    or id(signal) not in plans_by_signal_identity
                ):
                    continue
                strategy_id = signal.strategy.strategy_id
                if self.recorder.strategy_activity_mode(strategy_id) == "IDLE":
                    valid_live_candidates.append(live_candidate)
            live_candidate = self.strategy_scheduler.choose_live_candidate(
                valid_live_candidates,
                live_blocked=live_blocked,
            )
            reserved_live_signal_identity: int | None = None
            if live_candidate is None:
                if live_blocked and valid_live_candidates:
                    self.recorder.record_event(
                        "strategy_live_skip_existing_position",
                        {"scan_id": scan_id, "candidate_count": len(valid_live_candidates)},
                    )
            else:
                signal = live_candidate.signal
                reserved_live_signal_identity = id(signal)
                strategy = signal.strategy
                candidate = signal.candidate
                plan = plans_by_signal_identity[id(signal)]
                try:
                    position_state = self.trader.open_long_plan_with_protection(plan)
                except EntryWindowExpiredError as exc:
                    if not self._record_entry_window_expired(
                        scan_id, signal, exc, "LIVE"
                    ):
                        return
                except BinanceAPIError as exc:
                    audit_error = _bounded_audit_error(exc)
                    try:
                        failed_order_state = self.state.load()
                    except Exception as state_exc:
                        failed_order_state = None
                        strategy_execution_blocked.add(strategy.strategy_id)
                        self.recorder.mark_strategy_live_result_pending(
                            strategy.strategy_id,
                            "LOCAL_EXECUTION_STATE_UNREADABLE",
                        )
                        self.recorder.record_event(
                            "strategy_live_order_local_state_unreadable",
                            {
                                "scan_id": scan_id,
                                "strategy_id": strategy.strategy_id,
                                "signal_id": signal.signal_id,
                                "order_error": audit_error,
                                "state_error": _bounded_audit_error(state_exc),
                                "pending_uncertain": True,
                            },
                            candidate.symbol,
                        )
                        self.logger.error(
                            "Live order failed and local execution state is unreadable; "
                            "strategy blocked | strategy=%s symbol=%s state_error=%s",
                            strategy.strategy_id,
                            candidate.symbol,
                            _bounded_audit_error(state_exc),
                        )
                    if (
                        failed_order_state is not None
                        and self._strategy_id_from_state(failed_order_state)
                        == strategy.strategy_id
                    ):
                        strategy_execution_blocked.add(strategy.strategy_id)
                        self.recorder.record_event(
                            "strategy_live_order_local_pending",
                            {
                                "scan_id": scan_id,
                                "strategy_id": strategy.strategy_id,
                                "signal_id": signal.signal_id,
                                "orders": failed_order_state.orders,
                            },
                            candidate.symbol,
                        )
                    self.recorder.record_trade_failure(
                        scan_id, candidate.symbol, audit_error,
                        self.config.dry_run,
                    )
                    overlay_saved = self.recorder.update_strategy_signal(
                        signal.signal_id,
                        decision="LIVE_ORDER_FAILED",
                        reason="LIVE_ORDER_FAILED",
                        detail={"live_order_error": audit_error},
                    )
                    if not overlay_saved:
                        self.logger.error(
                            "Live order failure overlay was not durably saved; "
                            "strategy remains blocked | strategy=%s symbol=%s "
                            "signal_id=%s",
                            strategy.strategy_id,
                            candidate.symbol,
                            signal.signal_id,
                        )
                        return
                    self.recorder.record_event(
                        "strategy_live_order_failed",
                        {
                            "scan_id": scan_id,
                            "strategy_id": strategy.strategy_id,
                            "signal_id": signal.signal_id,
                            "error": audit_error,
                            "paper_fallback": False,
                        },
                        candidate.symbol,
                    )
                    self.logger.error(
                        "Strategy live order failed | strategy=%s symbol=%s error=%s",
                        strategy.strategy_id,
                        candidate.symbol,
                        audit_error,
                    )
                else:
                    structure_id = (
                        signal.analysis.structure_id
                        if isinstance(
                            signal.analysis,
                            (
                                N06AnalysisResult,
                                N07AnalysisResult,
                                N08AnalysisResult,
                                N09AnalysisResult,
                                N10AnalysisResult,
                                N11AnalysisResult,
                                N12AnalysisResult,
                                N13AnalysisResult,
                                N14AnalysisResult,
                                N15AnalysisResult,
                                N16AnalysisResult,
                                N17AnalysisResult,
                                N18AnalysisResult,
                                N19AnalysisResult,
                                N20AnalysisResult,
                                MicroAnalysisResult,
                            ),
                        )
                        else None
                    )
                    orders = dict(position_state.orders)
                    orders["strategy"] = {
                        "strategy_id": strategy.strategy_id,
                        "strategy_name": strategy.name,
                        "signal_id": signal.signal_id,
                        "structure_id": structure_id,
                        "quote_volume_rank": candidate.quote_volume_rank,
                        "candidate_universe": candidate.candidate_universe,
                        "signal_audit": self._permanent_signal_audit_snapshot(signal),
                    }
                    position_state = replace(position_state, orders=orders)
                    self.state.save(position_state)
                    trade_id = self.recorder.record_trade_open(scan_id, position_state)
                    if trade_id is None:
                        live_link_id = None
                    elif strategy.strategy_id == "N16":
                        strict_claim = self.recorder.claim_strategy_live_open_audit(
                            position_state,
                            "N16",
                        )
                        live_link_id = (
                            strict_claim.live_link_id
                            if strict_claim is not None
                            else None
                        )
                    else:
                        live_link_id = self.recorder.record_strategy_live_open(
                            strategy.strategy_id,
                            trade_id,
                            position_state.symbol,
                            position_state.opened_at,
                        )
                    if trade_id is None or live_link_id is None:
                        strategy_execution_blocked.add(strategy.strategy_id)
                        self.recorder.mark_strategy_live_result_pending(
                            strategy.strategy_id,
                            "LIVE_OPEN_AUDIT_PERSIST_FAILED",
                        )
                        pending_overlay_saved = self.recorder.update_strategy_signal(
                            signal.signal_id,
                            decision="LIVE_OPEN_AUDIT_PENDING",
                            reason="LIVE_OPEN_AUDIT_PERSIST_FAILED",
                            detail={
                                "trade_review_id": trade_id,
                                "live_link_id": live_link_id,
                            },
                        )
                        if not pending_overlay_saved:
                            self.logger.error(
                                "Live open pending overlay was not durably "
                                "saved; execution remains blocked | "
                                "strategy=%s symbol=%s signal_id=%s",
                                strategy.strategy_id,
                                position_state.symbol,
                                signal.signal_id,
                            )
                            return
                        self.recorder.record_event(
                            "strategy_live_open_audit_failed",
                            {
                                "scan_id": scan_id,
                                "strategy_id": strategy.strategy_id,
                                "signal_id": signal.signal_id,
                                "trade_review_id": trade_id,
                                "live_link_id": live_link_id,
                            },
                            position_state.symbol,
                        )
                        opened_live = True
                        self.logger.error(
                            "Live position opened but audit persistence failed; "
                            "strategy is blocked | strategy=%s symbol=%s",
                            strategy.strategy_id,
                            position_state.symbol,
                        )
                    else:
                        live_detail = {
                            "trade_review_id": trade_id,
                            "paper_fallback": False,
                        }
                        if strategy.stop_mode in {
                            "structure_p1_margin_capped",
                            "amplitude_margin_capped",
                            "s1_target_margin_capped",
                            "sweep_low_tick_margin_capped",
                            "breakout_retest_margin_capped",
                            "relative_strength_pullback_margin_capped",
                            "vwap_rotation_margin_capped",
                            "sell_pressure_decay_margin_capped",
                            "breadth_recovery_margin_capped",
                            "trend_support_continuation_margin_capped",
                            _N17_STOP_MODE,
                            _N18_STOP_MODE,
                            _N19_STOP_MODE,
                            _N20_STOP_MODE,
                            _MICRO_STOP_MODE,
                        }:
                            live_detail.update(
                                {
                                    "execution_plan": orders.get("plan", {}),
                                    "post_fill_adjustment": orders.get(
                                        "post_fill_adjustment", {}
                                    ),
                                }
                            )
                        live_overlay_saved = self.recorder.update_strategy_signal(
                            signal.signal_id,
                            decision="LIVE_OPENED",
                            reason="LIVE_OPENED",
                            detail=live_detail,
                        )
                        if not live_overlay_saved:
                            self.logger.error(
                                "Live open overlay was not durably saved; "
                                "execution remains blocked | strategy=%s "
                                "symbol=%s signal_id=%s",
                                strategy.strategy_id,
                                position_state.symbol,
                                signal.signal_id,
                            )
                            return
                        opened_live = True
                        self.logger.info(
                            "Strategy live slot opened | strategy=%s symbol=%s "
                            "entry=%s dry_run=%s",
                            strategy.strategy_id,
                            position_state.symbol,
                            position_state.entry_price,
                            position_state.dry_run,
                        )

        finally:
            if (
                micro_sampler_active
                and micro_sampler is not None
                and not micro_lease_resolved
            ):
                micro_sampler.abort(micro_lease)
            if scan_completion_allowed:
                self.recorder.complete_scan(scan_id, opened_live)

        self.logger.info(
            "Multi-strategy poll result | scanned=%s candidates=%s live_opened=%s",
            market_scan.scanned_count,
            ",".join(candidate_symbols) or "-",
            opened_live,
        )

    def run_forever(self) -> None:
        mode = "DRY_RUN" if self.config.dry_run else "LIVE"
        self.logger.info(
            "Trading bot started | mode=%s base_url=%s active_strategies=N01-N05",
            mode,
            self.config.base_url,
        )
        try:
            if not self._start_micro_observation_sampler():
                raise RuntimeError(
                    "Micro observation sampler ownership could not be acquired"
                )
            poll_stop_event = getattr(self, "_poll_stop_event", None)
            if poll_stop_event is None:
                poll_stop_event = threading.Event()
                self._poll_stop_event = poll_stop_event
            next_tick = time.monotonic()
            while not poll_stop_event.is_set():
                try:
                    self.run_once()
                except KeyboardInterrupt:
                    self.logger.info("Trading bot stopped by user.")
                    raise
                except Exception:
                    self.logger.exception("Main loop error; continuing after delay.")
                if poll_stop_event.is_set():
                    break
                observed_at = time.monotonic()
                next_tick = _next_fixed_poll_tick(
                    next_tick,
                    observed_at,
                    self.config.poll_interval_seconds,
                )
                poll_stop_event.wait(max(0.0, next_tick - time.monotonic()))
        finally:
            self.close()


def main() -> None:
    config = load_config()
    with InstanceLock(config.instance_lock_file) as instance_lock:
        TradingBot(config=config, instance_lock=instance_lock).run_forever()


if __name__ == "__main__":
    main()
