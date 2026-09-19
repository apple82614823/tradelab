from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
import time
from types import MappingProxyType
from typing import Any, Mapping

from .binance_client import BinanceFuturesClient
from .exchange_symbol import (
    authenticated_symbol_set_sha256,
    canonical_exchange_symbol,
)


@dataclass(frozen=True)
class FundingCandidate:
    symbol: str
    funding_rate: Decimal | None
    mark_price: Decimal
    quote_volume: Decimal | None = None
    quote_volume_rank: int | None = None
    candidate_universe: str = "negative_funding"


@dataclass(frozen=True)
class StrategyMarketScan:
    scanned_count: int
    funding_candidates: list[FundingCandidate]
    volume_candidates: list[FundingCandidate]
    premium_by_symbol: Mapping[str, Mapping[str, Any]] = field(
        default_factory=lambda: MappingProxyType({})
    )
    premium_observed_at_ms: int | None = None
    authenticated_symbols: frozenset[str] = field(default_factory=frozenset)
    authenticated_symbols_sha256: str | None = None

    @property
    def all_candidates(self) -> list[FundingCandidate]:
        return [*self.funding_candidates, *self.volume_candidates]


class FundingMonitor:
    def __init__(self, client: BinanceFuturesClient, abs_threshold: Decimal, logger):
        self.client = client
        self.abs_threshold = abs_threshold
        self.logger = logger

    def scan(self) -> tuple[int, list[FundingCandidate]]:
        rows = self.client.get_premium_index()
        return len(rows), self._funding_candidates(rows)

    def scan_for_strategies(self, volume_top_n: int) -> StrategyMarketScan:
        if type(volume_top_n) is not int or volume_top_n < 0:
            raise ValueError("volume_top_n must be a non-negative integer")
        premium_rows = self.client.get_premium_index()
        premium_observed_at_ms = int(time.time() * 1000)
        premium_by_symbol: dict[str, Mapping[str, Any]] = {}
        for row in premium_rows:
            symbol = row.get("symbol")
            if isinstance(symbol, str) and symbol and symbol not in premium_by_symbol:
                premium_by_symbol[symbol] = MappingProxyType(dict(row))
        raw_tradable_symbols = self.client.get_tradable_usdt_perpetual_symbols()
        tradable_symbols = frozenset(
            canonical_exchange_symbol(symbol)
            for symbol in raw_tradable_symbols
        )
        tradable_symbols_sha256 = authenticated_symbol_set_sha256(
            tradable_symbols
        )
        funding_candidates = self._funding_candidates(
            premium_rows,
            tradable_symbols=tradable_symbols,
        )
        ranked_rows: list[tuple[str, Decimal, dict[str, Any]]] = []
        if volume_top_n:
            for row in self.client.get_24hr_tickers():
                symbol = row.get("symbol")
                if type(symbol) is not str:
                    continue
                if symbol not in tradable_symbols:
                    continue
                quote_volume = Decimal(str(row.get("quoteVolume", "0")))
                ranked_rows.append((symbol, quote_volume, row))

        ranked_rows.sort(key=lambda item: (-item[1], item[0]))
        volume_candidates: list[FundingCandidate] = []
        for rank, (symbol, quote_volume, ticker) in enumerate(ranked_rows[:volume_top_n], start=1):
            volume_candidates.append(
                FundingCandidate(
                    symbol=symbol,
                    funding_rate=None,
                    mark_price=Decimal(str(ticker.get("lastPrice", "0"))),
                    quote_volume=quote_volume,
                    quote_volume_rank=rank,
                    candidate_universe="quote_volume_top",
                )
            )

        return StrategyMarketScan(
            scanned_count=len(premium_rows),
            funding_candidates=funding_candidates,
            volume_candidates=volume_candidates,
            premium_by_symbol=MappingProxyType(premium_by_symbol),
            premium_observed_at_ms=premium_observed_at_ms,
            authenticated_symbols=tradable_symbols,
            authenticated_symbols_sha256=tradable_symbols_sha256,
        )

    def _funding_candidates(
        self,
        rows: list[dict[str, Any]],
        tradable_symbols: frozenset[str] | None = None,
    ) -> list[FundingCandidate]:
        candidates: list[FundingCandidate] = []
        for row in rows:
            symbol = row.get("symbol", "")
            if not symbol.endswith("USDT"):
                continue
            if tradable_symbols is not None and symbol not in tradable_symbols:
                continue
            funding_rate = Decimal(str(row.get("lastFundingRate", "0")))
            if funding_rate <= -self.abs_threshold:
                candidates.append(
                    FundingCandidate(
                        symbol=symbol,
                        funding_rate=funding_rate,
                        mark_price=Decimal(str(row.get("markPrice", "0"))),
                        candidate_universe="negative_funding",
                    )
                )
        candidates.sort(key=lambda item: item.funding_rate if item.funding_rate is not None else Decimal("0"))
        return candidates
