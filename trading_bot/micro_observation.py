from __future__ import annotations

from dataclasses import dataclass, replace as dataclass_replace
from decimal import Decimal, InvalidOperation
import hashlib
import json
import secrets
import threading
import time
from types import MappingProxyType
from typing import Any, Callable, Mapping, Sequence


MICRO_RULE_VERSION = "MICRO_OBSERVATION_V1"
MICRO_MIN_INTERVAL_MS = 45_000
MICRO_MAX_INTERVAL_MS = 150_000
MICRO_MAX_OBSERVATIONS_PER_SYMBOL = 4
MICRO_MAX_TOTAL_OBSERVATIONS = 400
MICRO_PREMIUM_MAX_SKEW_MS = 75_000
FIFTEEN_MINUTES_MS = 900_000


class MicroObservationError(RuntimeError):
    pass


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def strict_decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool):
        raise MicroObservationError(f"{name} is not a finite decimal")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise MicroObservationError(f"{name} is not a finite decimal") from exc
    if not result.is_finite():
        raise MicroObservationError(f"{name} is not a finite decimal")
    return result


def strict_int(value: Any, name: str) -> int:
    if type(value) is not int:
        raise MicroObservationError(f"{name} is not an integer")
    return value


@dataclass(frozen=True)
class MicroObservation:
    symbol: str
    scan_id: int
    generation: int
    boot_id: str
    observed_at_ms: int
    premium_observed_at_ms: int
    kline_open_time_ms: int
    kline_close_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    quote_volume: Decimal
    trade_count: int
    taker_buy_quote: Decimal
    mark_price: Decimal
    index_price: Decimal
    funding_rate: Decimal
    premium: Decimal
    quote_volume_rank: int
    universe_sha256: str
    atr: Decimal
    quote_rate_baseline: Decimal
    source_sha256: str
    rule_version: str = MICRO_RULE_VERSION

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "atr": str(self.atr), "boot_id": self.boot_id,
            "close": str(self.close), "funding_rate": str(self.funding_rate),
            "generation": self.generation, "high": str(self.high),
            "index_price": str(self.index_price),
            "kline_close_time_ms": self.kline_close_time_ms,
            "kline_open_time_ms": self.kline_open_time_ms,
            "low": str(self.low), "mark_price": str(self.mark_price),
            "observed_at_ms": self.observed_at_ms, "open": str(self.open),
            "premium": str(self.premium),
            "premium_observed_at_ms": self.premium_observed_at_ms,
            "quote_rate_baseline": str(self.quote_rate_baseline),
            "quote_volume": str(self.quote_volume),
            "quote_volume_rank": self.quote_volume_rank,
            "rule_version": self.rule_version, "scan_id": self.scan_id,
            "symbol": self.symbol,
            "taker_buy_quote": str(self.taker_buy_quote),
            "trade_count": self.trade_count,
            "universe_sha256": self.universe_sha256,
        }


@dataclass(frozen=True)
class MicroIncrement:
    before: MicroObservation
    after: MicroObservation
    interval_ms: int
    quote_rate: Decimal
    trade_rate: Decimal
    delta_ratio: Decimal
    price_return: Decimal
    premium_change: Decimal


@dataclass(frozen=True)
class MicroObservationWindow:
    symbol: str
    observations: tuple[MicroObservation, ...]
    reset_reason: str | None = None

    @property
    def increments(self) -> tuple[MicroIncrement, ...]:
        return tuple(derive_increment(a, b) for a, b in
                     zip(self.observations, self.observations[1:]))


@dataclass(frozen=True)
class MicroObservationProposal:
    expected_generation: int
    target_generation: int
    scan_id: int
    boot_id: str
    windows: Mapping[str, MicroObservationWindow]
    source_sha256: str


@dataclass(frozen=True)
class MicroSampleIdentity:
    sample_ordinal: int
    generation: int
    boot_id: str
    continuation_ranked_symbols: tuple[tuple[str, int], ...] = ()
    continuation_universe_sha256: str | None = None
    lifecycle_epoch: int = 0


@dataclass(frozen=True)
class MicroObservationSample(Mapping[str, MicroObservation]):
    """One current Top100 frame plus an optional prior-cohort continuation."""

    observations: Mapping[str, MicroObservation]
    continuation_observations: Mapping[str, MicroObservation]

    def __getitem__(self, key: str) -> MicroObservation:
        return self.observations[key]

    def __iter__(self):
        return iter(self.observations)

    def __len__(self) -> int:
        return len(self.observations)


@dataclass(frozen=True)
class N22MarketReturnEvidence:
    symbol: str
    before_source_sha256: str
    after_source_sha256: str
    price_return: Decimal
    observed_at_ms: int


@dataclass(frozen=True)
class N22MarketContext:
    """Compact proof for one complete Top100 interval used by N22."""

    before_generation: int
    after_generation: int
    boot_id: str
    universe_sha256: str
    entries: tuple[N22MarketReturnEvidence, ...]
    confirmation_observed_at_ms: int
    source_sha256: str


@dataclass(frozen=True)
class MicroObservationLease:
    expected_generation: int
    scan_id: int
    boot_id: str
    captured_at_ms: int
    windows: Mapping[str, MicroObservationWindow]
    source_sha256: str
    lifecycle_epoch: int = 0


@dataclass(frozen=True)
class MicroObservationFrame:
    """One immutable, fully authenticated Top100 sampler generation."""

    generation: int
    sample_ordinal: int
    boot_id: str
    universe_sha256: str
    observations: Mapping[str, MicroObservation]
    n22_market_context: N22MarketContext | None
    source_sha256: str


class MicroObservationContext(Mapping[str, MicroObservationWindow]):
    """Lease-local windows plus exact historical Top100 generation cohorts."""

    def __init__(
        self,
        windows: Mapping[str, MicroObservationWindow],
        cohorts: Mapping[tuple[int, str], Mapping[str, MicroObservation]],
        n22_market_contexts: Mapping[
            tuple[int, int, str], N22MarketContext
        ] | None = None,
    ) -> None:
        self._windows = MappingProxyType(dict(windows))
        self._cohorts = MappingProxyType({
            key: MappingProxyType(dict(value))
            for key, value in cohorts.items()
        })
        self._n22_market_contexts = MappingProxyType(dict(
            n22_market_contexts or {}
        ))

    def __getitem__(self, key: str) -> MicroObservationWindow:
        return self._windows[key]

    def __iter__(self):
        return iter(self._windows)

    def __len__(self) -> int:
        return len(self._windows)

    def cohort(
        self,
        generation: int,
        universe_sha256: str,
    ) -> Mapping[str, MicroObservation] | None:
        return self._cohorts.get((generation, universe_sha256))

    def prior_observation(
        self,
        symbol: str,
        generation: int,
    ) -> MicroObservation | None:
        candidates = [
            item
            for cohort in self._cohorts.values()
            for member, item in cohort.items()
            if member == symbol and item.generation < generation
        ]
        return max(candidates, key=lambda item: item.generation, default=None)

    def n22_market_context(
        self,
        before_generation: int,
        after_generation: int,
        universe_sha256: str,
    ) -> N22MarketContext | None:
        return self._n22_market_contexts.get((
            before_generation,
            after_generation,
            universe_sha256,
        ))

    @property
    def cohort_count(self) -> int:
        return len(self._cohorts)

    @property
    def n22_market_context_count(self) -> int:
        return len(self._n22_market_contexts)


def validate_micro_observation(item: MicroObservation) -> None:
    """Authenticate one immutable cache item without trusting its constructor."""

    if type(item) is not MicroObservation:
        raise MicroObservationError("micro observation type is invalid")
    if (
        type(item.symbol) is not str
        or not item.symbol
        or type(item.scan_id) is not int
        or item.scan_id <= 0
        or type(item.generation) is not int
        or item.generation <= 0
        or type(item.boot_id) is not str
        or not item.boot_id
        or type(item.observed_at_ms) is not int
        or type(item.premium_observed_at_ms) is not int
        or type(item.kline_open_time_ms) is not int
        or type(item.kline_close_time_ms) is not int
        or item.kline_close_time_ms
        != item.kline_open_time_ms + FIFTEEN_MINUTES_MS - 1
        or not item.kline_open_time_ms
        <= item.observed_at_ms
        <= item.kline_close_time_ms
        or abs(item.observed_at_ms - item.premium_observed_at_ms)
        > MICRO_PREMIUM_MAX_SKEW_MS
        or type(item.quote_volume_rank) is not int
        or not 1 <= item.quote_volume_rank <= 100
        or type(item.universe_sha256) is not str
        or len(item.universe_sha256) != 64
        or any(character not in "0123456789abcdef" for character in item.universe_sha256)
        or item.rule_version != MICRO_RULE_VERSION
    ):
        raise MicroObservationError("micro observation identity is invalid")
    decimals = (
        item.open,
        item.high,
        item.low,
        item.close,
        item.quote_volume,
        item.taker_buy_quote,
        item.mark_price,
        item.index_price,
        item.funding_rate,
        item.premium,
        item.atr,
        item.quote_rate_baseline,
    )
    if any(type(value) is not Decimal or not value.is_finite() for value in decimals):
        raise MicroObservationError("micro observation decimal is invalid")
    if (
        min(item.open, item.high, item.low, item.close) <= 0
        or item.high < max(item.open, item.close, item.low)
        or item.low > min(item.open, item.close, item.high)
        or item.quote_volume < 0
        or type(item.trade_count) is not int
        or item.trade_count < 0
        or not Decimal(0) <= item.taker_buy_quote <= item.quote_volume
        or item.mark_price <= 0
        or item.index_price <= 0
        or item.premium != (item.mark_price - item.index_price) / item.index_price
        or item.atr <= 0
        or item.quote_rate_baseline <= 0
    ):
        raise MicroObservationError("micro observation value is invalid")
    expected = hashlib.sha256(
        canonical_json(item.canonical_payload()).encode("utf-8")
    ).hexdigest()
    if type(item.source_sha256) is not str or item.source_sha256 != expected:
        raise MicroObservationError("micro observation source hash conflicts")


def _closed_metrics(raw_klines: Sequence[Sequence[Any]]) -> tuple[Decimal, Decimal]:
    if len(raw_klines) < 22:
        raise MicroObservationError("closed ATR/volume source is incomplete")
    closed = raw_klines[:-1]
    ranges: list[Decimal] = []
    prior_close: Decimal | None = None
    quote_volumes: list[Decimal] = []
    for row in closed:
        if len(row) < 11:
            raise MicroObservationError("kline row is incomplete")
        high, low, close = (strict_decimal(row[i], name) for i, name in
                            ((2, "high"), (3, "low"), (4, "close")))
        quote = strict_decimal(row[7], "quote volume")
        if low <= 0 or high < max(low, close) or quote <= 0:
            raise MicroObservationError("closed kline is invalid")
        tr = high - low
        if prior_close is not None:
            tr = max(tr, abs(high - prior_close), abs(low - prior_close))
        ranges.append(tr)
        quote_volumes.append(quote)
        prior_close = close
    period = 14
    atr = sum(ranges[1:period + 1], Decimal(0)) / Decimal(period)
    for value in ranges[period + 1:]:
        atr = (atr * Decimal(period - 1) + value) / Decimal(period)
    latest20 = sorted(quote_volumes[-20:])
    median_quote = (latest20[9] + latest20[10]) / Decimal(2)
    quote_rate_baseline = median_quote / Decimal(900)
    if atr <= 0 or quote_rate_baseline <= 0:
        raise MicroObservationError("closed ATR/volume metric is invalid")
    return atr, quote_rate_baseline


def build_universe_sha256(ranked_symbols: Sequence[tuple[str, int]]) -> str:
    if (
        len(ranked_symbols) != 100
        or any(
            type(symbol) is not str
            or not symbol
            or type(rank) is not int
            for symbol, rank in ranked_symbols
        )
        or len({s for s, _ in ranked_symbols}) != 100
        or {r for _, r in ranked_symbols} != set(range(1, 101))
    ):
        raise MicroObservationError("Top100 universe is incomplete")
    return hashlib.sha256(canonical_json(sorted(ranked_symbols,
                                                 key=lambda x: x[1])).encode()).hexdigest()


def _authenticate_top100_observations(
    observations: Mapping[str, MicroObservation],
) -> str:
    """Authenticate the complete ranked universe without trusting its hash."""

    if (
        len(observations) != 100
        or any(type(symbol) is not str or not symbol for symbol in observations)
    ):
        raise MicroObservationError("proposal Top100 is incomplete")
    ranked: list[tuple[str, int]] = []
    for symbol, item in observations.items():
        validate_micro_observation(item)
        if item.symbol != symbol:
            raise MicroObservationError("proposal Top100 symbol is inconsistent")
        ranked.append((item.symbol, item.quote_volume_rank))
    universe_sha256 = build_universe_sha256(tuple(ranked))
    if any(
        item.universe_sha256 != universe_sha256
        for item in observations.values()
    ):
        raise MicroObservationError("proposal Top100 digest is inconsistent")
    return universe_sha256


def build_micro_observation(*, symbol: str, scan_id: int, generation: int,
                            boot_id: str, observed_at_ms: int,
                            premium_observed_at_ms: int,
                            raw_klines: Sequence[Sequence[Any]],
                            premium_row: Mapping[str, Any],
                            quote_volume_rank: int,
                            universe_sha256: str) -> MicroObservation:
    if not symbol or premium_row.get("symbol") != symbol:
        raise MicroObservationError("micro source symbol is inconsistent")
    for value, name in ((scan_id, "scan_id"), (generation, "generation"),
                        (observed_at_ms, "observed_at_ms"),
                        (premium_observed_at_ms, "premium_observed_at_ms")):
        strict_int(value, name)
    if abs(observed_at_ms - premium_observed_at_ms) > MICRO_PREMIUM_MAX_SKEW_MS:
        raise MicroObservationError("premium snapshot is stale")
    if type(quote_volume_rank) is not int or not 1 <= quote_volume_rank <= 100:
        raise MicroObservationError("Top100 rank is invalid")
    if len(universe_sha256) != 64 or not raw_klines:
        raise MicroObservationError("micro source identity is invalid")
    row = raw_klines[-1]
    if len(row) < 11:
        raise MicroObservationError("current kline row is incomplete")
    open_time, close_time = strict_int(row[0], "open time"), strict_int(row[6], "close time")
    if close_time != open_time + FIFTEEN_MINUTES_MS - 1:
        raise MicroObservationError("current kline close time is invalid")
    if not open_time <= observed_at_ms <= close_time:
        raise MicroObservationError("current kline is not live at observation")
    values = [strict_decimal(row[i], name) for i, name in
              ((1, "open"), (2, "high"), (3, "low"), (4, "close"),
               (7, "quote volume"), (10, "taker buy quote"))]
    open_price, high, low, close, quote_volume, taker = values
    trades = strict_int(row[8], "trade count")
    if (min(open_price, high, low, close) <= 0 or high < max(open_price, close, low)
            or low > min(open_price, close, high) or quote_volume < 0
            or trades < 0 or not Decimal(0) <= taker <= quote_volume):
        raise MicroObservationError("current cumulative kline is invalid")
    mark = strict_decimal(premium_row.get("markPrice"), "mark price")
    index = strict_decimal(premium_row.get("indexPrice"), "index price")
    funding = strict_decimal(premium_row.get("lastFundingRate"), "funding rate")
    if mark <= 0 or index <= 0:
        raise MicroObservationError("premium price source is invalid")
    premium = (mark - index) / index
    atr, baseline = _closed_metrics(raw_klines)
    unsigned = dict(symbol=symbol, scan_id=scan_id, generation=generation,
                    boot_id=boot_id, observed_at_ms=observed_at_ms,
                    premium_observed_at_ms=premium_observed_at_ms,
                    kline_open_time_ms=open_time, kline_close_time_ms=close_time,
                    open=str(open_price), high=str(high), low=str(low), close=str(close),
                    quote_volume=str(quote_volume), trade_count=trades,
                    taker_buy_quote=str(taker), mark_price=str(mark),
                    index_price=str(index), funding_rate=str(funding),
                    premium=str(premium), quote_volume_rank=quote_volume_rank,
                    universe_sha256=universe_sha256, atr=str(atr),
                    quote_rate_baseline=str(baseline), rule_version=MICRO_RULE_VERSION)
    digest = hashlib.sha256(canonical_json(unsigned).encode()).hexdigest()
    return MicroObservation(symbol, scan_id, generation, boot_id, observed_at_ms,
        premium_observed_at_ms, open_time, close_time, open_price, high, low, close,
        quote_volume, trades, taker, mark, index, funding, premium,
        quote_volume_rank, universe_sha256, atr, baseline, digest)


def derive_increment(before: MicroObservation, after: MicroObservation) -> MicroIncrement:
    validate_micro_observation(before)
    validate_micro_observation(after)
    if before.symbol != after.symbol or before.boot_id != after.boot_id:
        raise MicroObservationError("observation identity changed")
    if before.kline_open_time_ms != after.kline_open_time_ms:
        raise MicroObservationError("observation crossed 15m boundary")
    # Independently sampled observations can be bound by one later CURRENT
    # strategy batch.  In that case their publication scan id is equal while
    # the sampler generation remains the strict source ordering authority.
    if after.scan_id < before.scan_id or after.generation <= before.generation:
        raise MicroObservationError("observation order is invalid")
    dt = after.observed_at_ms - before.observed_at_ms
    if not MICRO_MIN_INTERVAL_MS <= dt <= MICRO_MAX_INTERVAL_MS:
        raise MicroObservationError("observation interval is invalid")
    if after.high < before.high or after.low > before.low:
        raise MicroObservationError("cumulative extrema regressed")
    dq = after.quote_volume - before.quote_volume
    dtaker = after.taker_buy_quote - before.taker_buy_quote
    dn = after.trade_count - before.trade_count
    if dq <= 0 or dn < 0 or not 0 <= dtaker <= dq:
        raise MicroObservationError("cumulative counters regressed")
    seconds = Decimal(dt) / Decimal(1000)
    return MicroIncrement(before, after, dt, dq / seconds, Decimal(dn) / seconds,
                          (Decimal(2) * dtaker - dq) / dq,
                          after.close / before.close - Decimal(1),
                          after.premium - before.premium)


def _n22_market_context_payload(
    context: N22MarketContext,
) -> dict[str, Any]:
    return {
        "after_generation": context.after_generation,
        "before_generation": context.before_generation,
        "boot_id": context.boot_id,
        "confirmation_observed_at_ms": context.confirmation_observed_at_ms,
        "entries": [
            [
                entry.symbol,
                entry.before_source_sha256,
                entry.after_source_sha256,
                str(entry.price_return),
                entry.observed_at_ms,
            ]
            for entry in context.entries
        ],
        "universe_sha256": context.universe_sha256,
    }


def validate_n22_market_context(context: N22MarketContext) -> None:
    if (
        type(context) is not N22MarketContext
        or type(context.before_generation) is not int
        or context.before_generation <= 0
        or type(context.after_generation) is not int
        or context.after_generation <= context.before_generation
        or type(context.boot_id) is not str
        or not context.boot_id
        or type(context.universe_sha256) is not str
        or len(context.universe_sha256) != 64
        or any(
            character not in "0123456789abcdef"
            for character in context.universe_sha256
        )
        or type(context.entries) is not tuple
        or len(context.entries) != 100
        or type(context.confirmation_observed_at_ms) is not int
        or context.confirmation_observed_at_ms <= 0
    ):
        raise MicroObservationError("N22 market context identity is invalid")
    symbols: set[str] = set()
    observed_times: list[int] = []
    for entry in context.entries:
        if (
            type(entry) is not N22MarketReturnEvidence
            or type(entry.symbol) is not str
            or not entry.symbol
            or entry.symbol in symbols
            or type(entry.before_source_sha256) is not str
            or len(entry.before_source_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in entry.before_source_sha256
            )
            or type(entry.after_source_sha256) is not str
            or len(entry.after_source_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in entry.after_source_sha256
            )
            or type(entry.price_return) is not Decimal
            or not entry.price_return.is_finite()
            or type(entry.observed_at_ms) is not int
            or entry.observed_at_ms <= 0
        ):
            raise MicroObservationError("N22 market context entry is invalid")
        symbols.add(entry.symbol)
        observed_times.append(entry.observed_at_ms)
    if (
        tuple(entry.symbol for entry in context.entries)
        != tuple(sorted(symbols))
        or context.confirmation_observed_at_ms != max(observed_times)
    ):
        raise MicroObservationError("N22 market context ordering is invalid")
    expected = hashlib.sha256(canonical_json(
        _n22_market_context_payload(context)
    ).encode("utf-8")).hexdigest()
    if type(context.source_sha256) is not str or context.source_sha256 != expected:
        raise MicroObservationError("N22 market context hash conflicts")


class MicroObservationCache:
    """Immutable proposal plus generation-CAS; only CURRENT scans can commit."""
    def __init__(self, *, boot_id: str | None = None) -> None:
        self._boot_id = boot_id or secrets.token_hex(16)
        self._generation = 0
        self._scan_id: int | None = None
        self._windows: dict[str, MicroObservationWindow] = {}

    @property
    def boot_id(self) -> str: return self._boot_id
    @property
    def generation(self) -> int: return self._generation
    @property
    def scan_id(self) -> int | None: return self._scan_id
    def snapshot(self) -> Mapping[str, MicroObservationWindow]:
        return MappingProxyType(dict(self._windows))
    def clear(self) -> None:
        self._windows, self._scan_id = {}, None
        self._generation += 1

    def propose(self, scan_id: int,
                observations: Mapping[str, MicroObservation]) -> MicroObservationProposal:
        if type(scan_id) is not int or scan_id <= 0 or (self._scan_id is not None and scan_id <= self._scan_id):
            raise MicroObservationError("proposal scan id is invalid")
        _authenticate_top100_observations(observations)
        target = self._generation + 1
        windows: dict[str, MicroObservationWindow] = {}
        for symbol, current in sorted(observations.items()):
            validate_micro_observation(current)
            if (current.scan_id, current.generation, current.boot_id) != (scan_id, target, self._boot_id):
                raise MicroObservationError("proposal generation is invalid")
            old = self._windows.get(symbol)
            values, reason = (current,), "MICRO_OBSERVATION_COLD_START"
            if old and old.observations:
                try:
                    derive_increment(old.observations[-1], current)
                except MicroObservationError as exc:
                    reason = str(exc)
                else:
                    values = (*old.observations[-3:], current)
                    reason = None
            windows[symbol] = MicroObservationWindow(symbol, values, reason)
        if sum(len(w.observations) for w in windows.values()) > MICRO_MAX_TOTAL_OBSERVATIONS:
            raise MicroObservationError("micro cache exceeds 400 observations")
        digest = hashlib.sha256(canonical_json({s: [o.source_sha256 for o in w.observations]
            for s, w in windows.items()}).encode()).hexdigest()
        return MicroObservationProposal(self._generation, target, scan_id, self._boot_id,
                                        MappingProxyType(windows), digest)

    def commit(self, proposal: MicroObservationProposal, *, current_scan_id: int) -> bool:
        if (proposal.boot_id != self._boot_id
                or proposal.expected_generation != self._generation
                or proposal.target_generation != self._generation + 1
                or proposal.scan_id != current_scan_id
                or (self._scan_id is not None and proposal.scan_id <= self._scan_id)):
            self.clear()
            return False
        self._windows, self._generation, self._scan_id = (
            dict(proposal.windows), proposal.target_generation, proposal.scan_id)
        return True


class MicroObservationSampler:
    """Single-owner, bounded in-memory sampler with publication leases.

    The worker never touches SQLite and cannot perform exchange actions.  A
    full strategy scan receives an immutable lease close to N21-N25
    evaluation.  The worker may append newer complete frames while that lease
    is held; confirmation authenticates the frozen lease rather than a mutable
    global generation.
    """

    def __init__(
        self,
        *,
        boot_id: str | None = None,
        sample_interval_ms: int = 60_000,
    ) -> None:
        if (
            type(sample_interval_ms) is not int
            or not MICRO_MIN_INTERVAL_MS
            <= sample_interval_ms
            <= MICRO_MAX_INTERVAL_MS
        ):
            raise MicroObservationError("sample interval is invalid")
        self._boot_id = boot_id or secrets.token_hex(16)
        self._sample_interval_ms = sample_interval_ms
        self._sample_ordinal = 0
        self._generation = 0
        self._frames: tuple[MicroObservationFrame, ...] = ()
        self._lock = threading.RLock()
        self._lease: MicroObservationLease | None = None
        self._source_failed = False
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        # Epoch zero is the deterministic, threadless fixture lifecycle used
        # by direct proposal tests.  Every real worker start receives a new
        # positive epoch.  Once stop begins, leases and commits remain blocked
        # until a subsequent start establishes another positive epoch.
        self._lifecycle_epoch = 0
        self._active_lifecycle_epoch: int | None = None
        self._lease_enabled = True

    @property
    def boot_id(self) -> str:
        return self._boot_id

    @property
    def generation(self) -> int:
        with self._lock:
            return self._generation

    @property
    def frame_count(self) -> int:
        with self._lock:
            return len(self._frames)

    @property
    def n22_market_context_entry_count(self) -> int:
        with self._lock:
            return sum(
                len(frame.n22_market_context.entries)
                for frame in self._frames
                if frame.n22_market_context is not None
            )

    @property
    def is_running(self) -> bool:
        with self._lock:
            return bool(
                self._active_lifecycle_epoch is not None
                and self._thread is not None
                and self._thread.is_alive()
            )

    def snapshot(self) -> Mapping[str, MicroObservationWindow]:
        with self._lock:
            if not self._frames:
                return MappingProxyType({})
            symbols = tuple(self._frames[-1].observations)
            return MappingProxyType(self._windows_for_symbols(symbols))

    def next_identity(self) -> MicroSampleIdentity:
        with self._lock:
            continuation_ranked_symbols: tuple[tuple[str, int], ...] = ()
            continuation_universe_sha256 = None
            if self._frames:
                latest = self._frames[-1]
                continuation_ranked_symbols = tuple(sorted(
                    (
                        (symbol, item.quote_volume_rank)
                        for symbol, item in latest.observations.items()
                    ),
                    key=lambda item: item[1],
                ))
                continuation_universe_sha256 = latest.universe_sha256
            return MicroSampleIdentity(
                self._sample_ordinal + 1,
                self._generation + 1,
                self._boot_id,
                continuation_ranked_symbols,
                continuation_universe_sha256,
                (
                    self._active_lifecycle_epoch
                    if self._active_lifecycle_epoch is not None
                    else 0
                ),
            )

    @staticmethod
    def _frame_digest(
        generation: int,
        sample_ordinal: int,
        boot_id: str,
        universe_sha256: str,
        observations: Mapping[str, MicroObservation],
        n22_market_context: N22MarketContext | None,
    ) -> str:
        return hashlib.sha256(canonical_json({
            "boot_id": boot_id,
            "generation": generation,
            "sample_ordinal": sample_ordinal,
            "universe_sha256": universe_sha256,
            "observations": [
                [symbol, observations[symbol].source_sha256]
                for symbol in sorted(observations)
            ],
            "n22_market_context_sha256": (
                n22_market_context.source_sha256
                if n22_market_context is not None
                else None
            ),
        }).encode("utf-8")).hexdigest()

    def _windows_for_symbols(
        self,
        symbols: Sequence[str],
    ) -> dict[str, MicroObservationWindow]:
        windows: dict[str, MicroObservationWindow] = {}
        for symbol in symbols:
            values: tuple[MicroObservation, ...] = ()
            reason: str | None = "MICRO_OBSERVATION_COLD_START"
            for frame in self._frames:
                current = frame.observations.get(symbol)
                if current is None:
                    continue
                if values:
                    try:
                        derive_increment(values[-1], current)
                    except MicroObservationError as exc:
                        values, reason = (current,), str(exc)
                    else:
                        values = (*values[-3:], current)
                        reason = None
                else:
                    values = (current,)
            if values:
                windows[symbol] = MicroObservationWindow(
                    symbol, values, reason,
                )
        return windows

    @staticmethod
    def _context_digest(context: MicroObservationContext) -> str:
        return hashlib.sha256(canonical_json(
            {"windows": {
                symbol: [
                    item.source_sha256
                    for item in window.observations
                ]
                for symbol, window in sorted(context.items())
            }, "cohorts": [
                [generation, universe, [
                    [symbol, item.source_sha256]
                    for symbol, item in sorted(cohort.items())
                ]]
                for (generation, universe), cohort
                in sorted(context._cohorts.items())
            ], "n22_market_contexts": [
                [
                    before_generation,
                    after_generation,
                    universe,
                    market_context.source_sha256,
                ]
                for (
                    before_generation,
                    after_generation,
                    universe,
                ), market_context
                in sorted(context._n22_market_contexts.items())
            ]}
        ).encode("utf-8")).hexdigest()

    @staticmethod
    def _build_n22_market_context(
        before_frame: MicroObservationFrame,
        after_observations: Mapping[str, MicroObservation],
    ) -> N22MarketContext:
        if set(after_observations) != set(before_frame.observations):
            raise MicroObservationError(
                "N22 continuation Top100 is incomplete"
            )
        entries: list[N22MarketReturnEvidence] = []
        for symbol in sorted(before_frame.observations):
            before = before_frame.observations[symbol]
            after = after_observations[symbol]
            increment = derive_increment(before, after)
            entries.append(N22MarketReturnEvidence(
                symbol,
                before.source_sha256,
                after.source_sha256,
                increment.price_return,
                after.observed_at_ms,
            ))
        unsigned = N22MarketContext(
            before_frame.generation,
            next(iter(after_observations.values())).generation,
            before_frame.boot_id,
            before_frame.universe_sha256,
            tuple(entries),
            max(entry.observed_at_ms for entry in entries),
            "",
        )
        context = dataclass_replace(
            unsigned,
            source_sha256=hashlib.sha256(canonical_json(
                _n22_market_context_payload(unsigned)
            ).encode("utf-8")).hexdigest(),
        )
        validate_n22_market_context(context)
        return context

    def commit_sample(
        self,
        sample: Mapping[str, MicroObservation] | MicroObservationSample,
        *,
        identity: MicroSampleIdentity | None = None,
    ) -> bool:
        with self._lock:
            if not self._lease_enabled:
                self.mark_failed("sample lifecycle is stopped")
                return False
            if type(sample) is MicroObservationSample:
                observations = sample.observations
                continuation_observations = sample.continuation_observations
            else:
                observations = sample
                continuation_observations = {}
            if len(observations) != 100:
                self.mark_failed("sample Top100 is incomplete")
                return False
            expected = self.next_identity()
            try:
                if self._active_lifecycle_epoch is not None:
                    if type(identity) is not MicroSampleIdentity:
                        raise MicroObservationError(
                            "active sample lifecycle identity is missing"
                        )
                if identity is not None and (
                    type(identity) is not MicroSampleIdentity
                    or (
                        identity.sample_ordinal,
                        identity.generation,
                        identity.boot_id,
                        identity.lifecycle_epoch,
                    )
                    != (
                        expected.sample_ordinal,
                        expected.generation,
                        expected.boot_id,
                        expected.lifecycle_epoch,
                    )
                ):
                    raise MicroObservationError(
                        "sample lifecycle identity is inconsistent"
                    )
                ordinals = {
                    item.scan_id for item in observations.values()
                }
                generations = {
                    item.generation for item in observations.values()
                }
                boot_ids = {
                    item.boot_id for item in observations.values()
                }
                if (
                    ordinals != {expected.sample_ordinal}
                    or generations != {expected.generation}
                    or boot_ids != {expected.boot_id}
                ):
                    raise MicroObservationError(
                        "sample identity is inconsistent"
                    )
                universe_sha256 = _authenticate_top100_observations(
                    observations
                )
                n22_market_context = None
                if self._frames:
                    previous = self._frames[-1]
                    if continuation_observations:
                        continuation_universe = (
                            _authenticate_top100_observations(
                                continuation_observations
                            )
                        )
                        if (
                            continuation_universe
                            != previous.universe_sha256
                            or set(continuation_observations)
                            != set(previous.observations)
                            or any(
                                (
                                    item.scan_id,
                                    item.generation,
                                    item.boot_id,
                                    item.quote_volume_rank,
                                    item.universe_sha256,
                                ) != (
                                    expected.sample_ordinal,
                                    expected.generation,
                                    expected.boot_id,
                                    previous.observations[symbol].quote_volume_rank,
                                    previous.universe_sha256,
                                )
                                for symbol, item
                                in continuation_observations.items()
                            )
                        ):
                            raise MicroObservationError(
                                "sample continuation identity is inconsistent"
                            )
                        n22_after = continuation_observations
                    elif set(previous.observations).issubset(observations):
                        n22_after = {
                            symbol: observations[symbol]
                            for symbol in previous.observations
                        }
                    else:
                        n22_after = None
                    if n22_after is not None:
                        try:
                            n22_market_context = (
                                self._build_n22_market_context(
                                    previous,
                                    n22_after,
                                )
                            )
                        except MicroObservationError:
                            # A valid complete current frame may legitimately
                            # cross a 15m boundary or reset after an interval
                            # gap.  Preserve that frame while withholding the
                            # interval-specific N22 market proof.
                            n22_market_context = None
                frame_observations = MappingProxyType(dict(observations))
                frame = MicroObservationFrame(
                    expected.generation,
                    expected.sample_ordinal,
                    expected.boot_id,
                    universe_sha256,
                    frame_observations,
                    n22_market_context,
                    self._frame_digest(
                        expected.generation,
                        expected.sample_ordinal,
                        expected.boot_id,
                        universe_sha256,
                        frame_observations,
                        n22_market_context,
                    ),
                )
            except Exception:
                self.mark_failed("sample proposal is invalid")
                return False
            self._frames = (*self._frames[-3:], frame)
            self._sample_ordinal = expected.sample_ordinal
            self._generation = expected.generation
            self._source_failed = False
            return True

    @staticmethod
    def _rebind_observation(
        item: MicroObservation,
        *,
        scan_id: int,
        boot_id: str,
    ) -> MicroObservation:
        unsigned = dataclass_replace(
            item,
            scan_id=scan_id,
            boot_id=boot_id,
            source_sha256="",
        )
        return dataclass_replace(
            unsigned,
            source_sha256=hashlib.sha256(
                canonical_json(unsigned.canonical_payload()).encode("utf-8")
            ).hexdigest(),
        )

    def freeze_for_scan(
        self,
        *,
        scan_id: int,
        current_symbols: Sequence[str],
        fallback_observations: Mapping[str, MicroObservation] | None,
        fallback_factory: Callable[
            [MicroSampleIdentity], Mapping[str, MicroObservation]
        ] | None = None,
        captured_at_ms: int,
    ) -> MicroObservationLease:
        if type(scan_id) is not int or scan_id <= 0:
            raise MicroObservationError("lease scan id is invalid")
        if type(captured_at_ms) is not int or captured_at_ms <= 0:
            raise MicroObservationError("lease capture time is invalid")
        symbols = tuple(current_symbols)
        if (
            len(symbols) != 100
            or any(type(symbol) is not str or not symbol for symbol in symbols)
            or len(set(symbols)) != 100
        ):
            raise MicroObservationError("lease Top100 is incomplete")
        with self._lock:
            if not self._lease_enabled:
                raise MicroObservationError(
                    "micro sampler lifecycle is stopped"
                )
            if self._lease is not None:
                raise MicroObservationError("micro sampler already has a lease")
            lifecycle_epoch = (
                self._active_lifecycle_epoch
                if self._active_lifecycle_epoch is not None
                else 0
            )
            expected_universe_sha256 = build_universe_sha256(tuple(
                (symbol, rank)
                for rank, symbol in enumerate(symbols, start=1)
            ))
            latest_frame = self._frames[-1] if self._frames else None
            factory_required = bool(
                fallback_factory is not None
                and (
                    latest_frame is None
                    or latest_frame.universe_sha256
                    != expected_universe_sha256
                    or set(latest_frame.observations) != set(symbols)
                    or any(
                        captured_at_ms < item.observed_at_ms
                        or captured_at_ms - item.observed_at_ms
                        > MICRO_MAX_INTERVAL_MS
                        for item in latest_frame.observations.values()
                    )
                )
            )
            if factory_required:
                if fallback_observations:
                    raise MicroObservationError(
                        "micro sampler fallback source is ambiguous"
                    )
                identity = self.next_identity()
                try:
                    generated = fallback_factory(identity)
                except Exception as exc:
                    raise MicroObservationError(
                        "micro sampler fallback construction failed"
                    ) from exc
                if not isinstance(generated, Mapping):
                    raise MicroObservationError(
                        "micro sampler fallback identity is invalid"
                    )
                try:
                    generated_universe = _authenticate_top100_observations(
                        generated
                    )
                except MicroObservationError as exc:
                    raise MicroObservationError(
                        "micro sampler fallback identity is invalid"
                    ) from exc
                if (
                    set(generated) != set(symbols)
                    or generated_universe != expected_universe_sha256
                    or any(
                        (
                            item.scan_id,
                            item.generation,
                            item.boot_id,
                            item.quote_volume_rank,
                        )
                        != (
                            identity.sample_ordinal,
                            identity.generation,
                            identity.boot_id,
                            rank,
                        )
                        for rank, symbol in enumerate(symbols, start=1)
                        for item in (generated[symbol],)
                    )
                    or any(
                        captured_at_ms < item.observed_at_ms
                        or captured_at_ms - item.observed_at_ms
                        > MICRO_MAX_INTERVAL_MS
                        for item in generated.values()
                    )
                    or not self.commit_sample(generated, identity=identity)
                ):
                    raise MicroObservationError(
                        "micro sampler fallback identity is invalid"
                    )
            cached = self._windows_for_symbols(symbols)
            missing_symbols = tuple(
                symbol for symbol in symbols if symbol not in cached
            )
            resolved_fallback = fallback_observations or {}
            if missing_symbols:
                try:
                    fallback_universe_sha256 = (
                        _authenticate_top100_observations(
                            resolved_fallback
                        )
                    )
                except MicroObservationError as exc:
                    raise MicroObservationError(
                        "micro sampler fallback identity is invalid"
                    ) from exc
                expected_generation = self._generation + 1
                if (
                    set(resolved_fallback) != set(symbols)
                    or fallback_universe_sha256
                    != expected_universe_sha256
                    or any(
                        (
                            item.scan_id,
                            item.generation,
                            item.boot_id,
                            item.quote_volume_rank,
                        )
                        != (
                            scan_id,
                            expected_generation,
                            self._boot_id,
                            rank,
                        )
                        for rank, symbol in enumerate(symbols, start=1)
                        for item in (resolved_fallback[symbol],)
                    )
                ):
                    raise MicroObservationError(
                        "micro sampler fallback identity is invalid"
                    )
            windows: dict[str, MicroObservationWindow] = {}
            for symbol in symbols:
                source_window = cached.get(symbol)
                if source_window is None:
                    fallback = resolved_fallback.get(symbol)
                    if fallback is None:
                        raise MicroObservationError(
                            "micro sampler current member is unavailable"
                        )
                    validate_micro_observation(fallback)
                    source_window = MicroObservationWindow(
                        symbol,
                        (fallback,),
                        "MICRO_OBSERVATION_COLD_START",
                    )
                rebound = tuple(
                    self._rebind_observation(
                        item,
                        scan_id=scan_id,
                        boot_id=self._boot_id,
                    )
                    for item in source_window.observations
                )
                window = MicroObservationWindow(
                    symbol,
                    rebound,
                    source_window.reset_reason,
                )
                # Recompute every adjacent increment after rebinding so a
                # malformed cached window cannot cross the publication gate.
                window.increments
                windows[symbol] = window
            if sum(len(window.observations) for window in windows.values()) > 400:
                raise MicroObservationError("micro sampler lease exceeds bound")
            latest_observed_at_ms = max(
                item.observed_at_ms
                for window in windows.values()
                for item in window.observations
            )
            if captured_at_ms < latest_observed_at_ms:
                raise MicroObservationError(
                    "micro sampler lease capture time is invalid"
                )
            if captured_at_ms - latest_observed_at_ms > MICRO_MAX_INTERVAL_MS:
                raise MicroObservationError(
                    "micro sampler last-good frame is stale"
                )
            if any(
                captured_at_ms
                - window.observations[-1].observed_at_ms
                > MICRO_MAX_INTERVAL_MS
                for window in windows.values()
            ):
                raise MicroObservationError(
                    "micro sampler member last-good frame is stale"
                )
            cohorts: dict[
                tuple[int, str], dict[str, MicroObservation]
            ] = {}
            n22_market_contexts: dict[
                tuple[int, int, str], N22MarketContext
            ] = {}
            for frame in self._frames:
                rebound_cohort = {
                    symbol: self._rebind_observation(
                        item,
                        scan_id=scan_id,
                        boot_id=self._boot_id,
                    )
                    for symbol, item in frame.observations.items()
                }
                cohorts[(frame.generation, frame.universe_sha256)] = (
                    rebound_cohort
                )
                if frame.n22_market_context is not None:
                    validate_n22_market_context(frame.n22_market_context)
                    market_context = frame.n22_market_context
                    key = (
                        market_context.before_generation,
                        market_context.after_generation,
                        market_context.universe_sha256,
                    )
                    if key in n22_market_contexts:
                        raise MicroObservationError(
                            "N22 market context identity is duplicated"
                        )
                    n22_market_contexts[key] = market_context
            if missing_symbols:
                fallback_generation = next(iter(
                    resolved_fallback.values()
                )).generation
                fallback_universe = next(iter(
                    resolved_fallback.values()
                )).universe_sha256
                cohorts[(fallback_generation, fallback_universe)] = {
                    symbol: self._rebind_observation(
                        item,
                        scan_id=scan_id,
                        boot_id=self._boot_id,
                    )
                    for symbol, item in resolved_fallback.items()
                }
            context = MicroObservationContext(
                windows,
                cohorts,
                n22_market_contexts,
            )
            digest = self._context_digest(context)
            lease = MicroObservationLease(
                self._generation,
                scan_id,
                self._boot_id,
                captured_at_ms,
                context,
                digest,
                lifecycle_epoch,
            )
            self._lease = lease
            return lease

    def confirm(
        self,
        lease: MicroObservationLease,
        *,
        current_scan_id: int,
    ) -> bool:
        with self._lock:
            lifecycle_epoch = (
                self._active_lifecycle_epoch
                if self._active_lifecycle_epoch is not None
                else 0
            )
            valid = (
                type(lease) is MicroObservationLease
                and self._lease_enabled
                and self._lease is lease
                and lease.scan_id == current_scan_id
                and lease.boot_id == self._boot_id
                and lease.lifecycle_epoch == lifecycle_epoch
                and type(lease.windows) is MicroObservationContext
                and lease.source_sha256 == self._context_digest(lease.windows)
            )
            self._lease = None
            return valid

    def abort(self, lease: MicroObservationLease | None = None) -> None:
        with self._lock:
            if (
                (self._lease is None and lease is not None)
                or (
                    self._lease is not None
                    and (
                        type(lease) is not MicroObservationLease
                        or lease is not self._lease
                    )
                )
            ):
                raise MicroObservationError(
                    "micro sampler lease identity is invalid"
                )
            active = self._lease
            self._lease = None
            if active is None:
                self._frames = ()
            else:
                self._frames = tuple(
                    frame for frame in self._frames
                    if frame.generation > active.expected_generation
                )
            self._source_failed = False

    def mark_failed(self, _reason: str) -> None:
        with self._lock:
            self._source_failed = True

    def _next_collection_due(
        self,
        previous_due: float,
        completed_at: float,
        *,
        succeeded: bool,
    ) -> float:
        scheduled = previous_due + self._sample_interval_ms / 1000
        if succeeded:
            return max(
                scheduled,
                completed_at + MICRO_MIN_INTERVAL_MS / 1000,
            )
        return max(
            scheduled,
            completed_at + MICRO_MIN_INTERVAL_MS / 1000,
        )

    def _retire_worker_lifecycle(
        self,
        lifecycle_epoch: int,
        owner: threading.Thread,
    ) -> None:
        """Retire only the worker that still owns the active lifecycle."""
        with self._lock:
            if (
                self._active_lifecycle_epoch != lifecycle_epoch
                or self._thread is not owner
            ):
                return
            self._active_lifecycle_epoch = None
            self._lease_enabled = False
            self._lease = None

    def start(
        self,
        collector: Callable[
            [MicroSampleIdentity, threading.Event],
            Mapping[str, MicroObservation],
        ],
        *,
        thread_name: str = "micro-observation-sampler",
        on_error: Callable[[str], None] | None = None,
    ) -> bool:
        if not callable(collector):
            raise MicroObservationError("micro sampler collector is invalid")
        with self._lock:
            if (
                self._active_lifecycle_epoch is not None
                or (
                    self._thread is not None
                    and self._thread.is_alive()
                )
            ):
                return False
            self._lifecycle_epoch += 1
            lifecycle_epoch = self._lifecycle_epoch
            self._active_lifecycle_epoch = lifecycle_epoch
            self._lease_enabled = True
            self._lease = None
            self._frames = ()
            self._source_failed = False
            self._stop_event.clear()

            def run() -> None:
                owner = threading.current_thread()
                try:
                    next_due = time.monotonic()
                    while not self._stop_event.is_set():
                        remaining = next_due - time.monotonic()
                        if remaining > 0 and self._stop_event.wait(remaining):
                            break
                        identity = self.next_identity()
                        if identity.lifecycle_epoch != lifecycle_epoch:
                            break
                        sample_succeeded = False
                        try:
                            observations = collector(identity, self._stop_event)
                            if self._stop_event.is_set():
                                break
                            committed = self.commit_sample(
                                observations,
                                identity=identity,
                            )
                            if not committed:
                                raise MicroObservationError(
                                    "micro sample commit was rejected"
                                )
                            sample_succeeded = True
                        except Exception as exc:
                            with self._lock:
                                self._sample_ordinal = max(
                                    self._sample_ordinal,
                                    identity.sample_ordinal,
                                )
                                self._generation = max(
                                    self._generation,
                                    identity.generation,
                                )
                            self.mark_failed("micro sample collection failed")
                            if on_error is not None:
                                on_error(type(exc).__name__)
                        now = time.monotonic()
                        next_due = self._next_collection_due(
                            next_due,
                            now,
                            succeeded=sample_succeeded,
                        )
                finally:
                    self._retire_worker_lifecycle(lifecycle_epoch, owner)

            thread = threading.Thread(
                target=run,
                name=thread_name,
                daemon=True,
            )
            self._thread = thread
            try:
                thread.start()
            except BaseException:
                if (
                    self._active_lifecycle_epoch == lifecycle_epoch
                    and self._thread is thread
                ):
                    self._active_lifecycle_epoch = None
                    self._lease_enabled = False
                    self._lease = None
                    self._frames = ()
                    self._source_failed = False
                    self._thread = None
                    self._stop_event.set()
                raise
            return True

    def stop(self, *, timeout_seconds: float = 15) -> bool:
        if timeout_seconds <= 0:
            raise MicroObservationError("micro sampler stop timeout is invalid")
        with self._lock:
            thread = self._thread
            self._lease_enabled = False
            self._active_lifecycle_epoch = None
            self._stop_event.set()
        if thread is not None and thread.ident is not None:
            thread.join(timeout_seconds)
        stopped = thread is None or not thread.is_alive()
        with self._lock:
            if stopped and self._thread is thread:
                self._thread = None
                self._lease = None
                self._frames = ()
                self._source_failed = False
        return stopped
