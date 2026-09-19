from __future__ import annotations

import hashlib
import json
import unicodedata
from collections.abc import Iterable


EXCHANGE_SYMBOL_MAX_CODEPOINTS = 32
EXCHANGE_SYMBOL_MAX_UTF8_BYTES = 96
EXCHANGE_SYMBOL_SET_MAX_ITEMS = 10_000


def canonical_exchange_symbol(value: object) -> str:
    """Validate one exact, display-preserving exchange symbol.

    The exchangeInfo membership check is the authority for whether a symbol is
    tradable.  This function supplies the independent bounded text boundary:
    exact normalized UTF-8 letters/numbers followed by the ASCII quote asset.
    It intentionally never transliterates or rewrites the exchange spelling.
    """

    if type(value) is not str or not 5 <= len(value) <= EXCHANGE_SYMBOL_MAX_CODEPOINTS:
        raise ValueError("exchange symbol is not canonical")
    try:
        encoded = value.encode("utf-8", "strict")
    except UnicodeError as exc:
        raise ValueError("exchange symbol is not canonical") from exc
    if (
        len(encoded) > EXCHANGE_SYMBOL_MAX_UTF8_BYTES
        or unicodedata.normalize("NFKC", value) != value
        or not value.endswith("USDT")
    ):
        raise ValueError("exchange symbol is not canonical")
    base = value[:-4]
    if not base:
        raise ValueError("exchange symbol is not canonical")
    for character in base:
        if character.isascii():
            if character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789":
                raise ValueError("exchange symbol is not canonical")
            continue
        if not unicodedata.category(character).startswith(("L", "N")):
            raise ValueError("exchange symbol is not canonical")
    return value


def exchange_symbol_sha256(symbol: object) -> str:
    canonical = canonical_exchange_symbol(symbol)
    return hashlib.sha256(
        b"BINANCE-USDT-PERPETUAL-SYMBOL-V1\x00" + canonical.encode("utf-8")
    ).hexdigest()


def authenticated_symbol_set_sha256(symbols: Iterable[object]) -> str:
    if isinstance(symbols, (str, bytes, bytearray)):
        raise ValueError("authenticated exchange symbol set is invalid")
    canonical = []
    for symbol in symbols:
        if len(canonical) >= EXCHANGE_SYMBOL_SET_MAX_ITEMS:
            raise ValueError("authenticated exchange symbol set is too large")
        canonical.append(canonical_exchange_symbol(symbol))
    if not canonical:
        raise ValueError("authenticated exchange symbol set is empty")
    if len(canonical) != len(set(canonical)):
        raise ValueError("authenticated exchange symbol set contains duplicates")
    payload = json.dumps(
        sorted(canonical),
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(
        b"BINANCE-USDT-PERPETUAL-SET-V1\x00" + payload
    ).hexdigest()


def attest_authenticated_symbol_set(
    symbols: Iterable[object],
    expected_sha256: object,
) -> frozenset[str]:
    if isinstance(symbols, (str, bytes, bytearray)):
        raise ValueError("authenticated exchange symbol set is invalid")
    canonical_items = []
    for symbol in symbols:
        if len(canonical_items) >= EXCHANGE_SYMBOL_SET_MAX_ITEMS:
            raise ValueError("authenticated exchange symbol set is too large")
        canonical_items.append(canonical_exchange_symbol(symbol))
    canonical = tuple(canonical_items)
    if (
        type(expected_sha256) is not str
        or len(expected_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_sha256)
        or authenticated_symbol_set_sha256(canonical) != expected_sha256
    ):
        raise ValueError("authenticated exchange symbol set digest conflicts")
    return frozenset(canonical)
