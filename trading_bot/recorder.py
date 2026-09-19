from __future__ import annotations

import json
import hashlib
import math
import os
import sqlite3
import stat
import threading
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from .analyzer import AnalysisResult
from .monitor import FundingCandidate
from .precision import decimal_to_api
from .state import PositionState


_RUNTIME_SQLITE_CONNECTION_LOCK = threading.RLock()
_N16_MAINTENANCE_INSTALL_TOKEN = object()


def _expected_database_fd_increment_is_attested(
    before: Mapping[int, tuple[int, int]],
    after: Mapping[int, tuple[int, int]],
    expected_identity: tuple[int, int],
) -> bool:
    """Prove one new descriptor for the expected inode, ignoring other files."""

    before_expected = {
        descriptor
        for descriptor, identity in before.items()
        if identity == expected_identity
    }
    after_expected = {
        descriptor
        for descriptor, identity in after.items()
        if identity == expected_identity
    }
    return (
        before_expected.issubset(after_expected)
        and len(after_expected) == len(before_expected) + 1
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_utc_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def canonical_utc_datetime(value: Any) -> datetime:
    """Return one exact built-in UTC ISO-8601 timestamp or fail closed."""

    if type(value) is not str or not value:
        raise ValueError("timestamp must be a non-empty built-in string")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp must carry the UTC offset")
    normalized = parsed.astimezone(timezone.utc)
    if normalized.isoformat() != value:
        raise ValueError("timestamp is not canonical UTC ISO-8601")
    return normalized


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)


def strict_json_dumps(value: Any) -> str:
    """Serialize persisted execution evidence without coercion or NaN."""

    item_count = [0]

    def validate(item: Any, depth: int, active: set[int]) -> None:
        item_count[0] += 1
        if item_count[0] > 50_000 or depth > 64:
            raise ValueError("strict JSON evidence exceeds its bounded shape")
        if item is None or type(item) in {str, int, bool}:
            return
        if type(item) is float:
            if not math.isfinite(item):
                raise ValueError("strict JSON evidence contains a non-finite float")
            return
        if type(item) not in {dict, list}:
            raise TypeError("strict JSON evidence contains an unsupported type")
        identity = id(item)
        if identity in active:
            raise ValueError("strict JSON evidence contains a cycle")
        active.add(identity)
        try:
            if type(item) is list:
                for child in item:
                    validate(child, depth + 1, active)
            else:
                for key, child in item.items():
                    if type(key) is not str:
                        raise TypeError("strict JSON object keys must be strings")
                    validate(child, depth + 1, active)
        finally:
            active.remove(identity)

    validate(value, 0, set())

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        allow_nan=False,
        separators=(",", ":"),
    )


def _canonical_decimal_evidence(
    raw_value: Any,
    *,
    allow_empty: bool,
    require_positive: bool,
    require_nonnegative: bool = False,
) -> Decimal | None:
    """Validate one persisted financial string without Decimal coercion."""

    if type(raw_value) is not str or len(raw_value) > 128:
        raise ValueError("financial evidence must be a bounded built-in string")
    if raw_value == "":
        if allow_empty:
            return None
        raise ValueError("financial evidence must not be empty")
    unsigned = raw_value[1:] if raw_value.startswith("-") else raw_value
    if not unsigned or raw_value.startswith("+") or unsigned.count(".") > 1:
        raise ValueError("financial evidence is not canonical fixed-point")
    integer_part, separator, fraction_part = unsigned.partition(".")
    if (
        not integer_part.isascii()
        or not integer_part.isdecimal()
        or (len(integer_part) > 1 and integer_part.startswith("0"))
        or (
            separator
            and (
                not fraction_part
                or not fraction_part.isascii()
                or not fraction_part.isdecimal()
                or fraction_part.endswith("0")
            )
        )
    ):
        raise ValueError("financial evidence is not canonical fixed-point")
    try:
        number = Decimal(raw_value)
    except Exception as exc:
        raise ValueError("financial evidence is not decimal") from exc
    if (
        not number.is_finite()
        or decimal_to_api(number) != raw_value
        or (number == 0 and number.is_signed())
        or (require_positive and number <= 0)
        or (require_nonnegative and number < 0)
    ):
        raise ValueError("financial evidence is not canonical and finite")
    return number


def _validate_terminal_financial_evidence(
    *,
    exit_reason: Any,
    exit_price: Any,
    mark_price: Any,
    pnl_amount: Any,
    pnl_pct: Any,
    balance_after: Any,
    dry_run: bool,
) -> None:
    if exit_reason not in {"STOP_LOSS", "TAKE_PROFIT"}:
        raise ValueError("terminal financial evidence has an invalid reason")
    _canonical_decimal_evidence(
        exit_price,
        allow_empty=False,
        require_positive=True,
    )
    optional = not dry_run
    _canonical_decimal_evidence(
        mark_price,
        allow_empty=optional,
        require_positive=True,
    )
    _canonical_decimal_evidence(
        pnl_amount,
        allow_empty=optional,
        require_positive=False,
    )
    _canonical_decimal_evidence(
        pnl_pct,
        allow_empty=optional,
        require_positive=False,
    )
    _canonical_decimal_evidence(
        balance_after,
        allow_empty=optional,
        require_positive=False,
        require_nonnegative=True,
    )


def _n16_terminal_event_identity_valid(
    raw_payload: Any,
    trade_review_id: Any,
    event_type: Any,
    occurred_at: Any,
    symbol: Any,
) -> int:
    """SQLite trigger predicate for every post-install close event."""

    if (
        type(raw_payload) is not str
        or type(trade_review_id) is not int
        or trade_review_id <= 0
        or event_type not in {
            "n16_dry_run_position_closed",
            "n16_live_position_closed",
        }
        or type(event_type) is not str
        or type(occurred_at) is not str
        or type(symbol) is not str
        or not 1 <= len(symbol) <= 64
    ):
        return 0
    try:
        canonical_utc_datetime(occurred_at)
    except (TypeError, ValueError):
        return 0

    def reject_duplicate_keys(
        pairs: list[tuple[str, Any]],
    ) -> dict[str, Any]:
        parsed: dict[str, Any] = {}
        for key, value in pairs:
            if type(key) is not str or key in parsed:
                raise ValueError("duplicate terminal event key")
            parsed[key] = value
        return parsed

    try:
        payload = json.loads(
            raw_payload,
            object_pairs_hook=reject_duplicate_keys,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError("non-finite terminal event value: %s" % value)
            ),
        )
    except (TypeError, ValueError):
        return 0
    required = {
        "trade_id",
        "exit_reason",
        "exit_price",
        "mark_price",
        "pnl_amount",
        "pnl_pct",
        "balance_after",
    }
    allowed = required | {"resolution_detail"}
    if (
        type(payload) is not dict
        or not required.issubset(payload)
        or not set(payload).issubset(allowed)
        or type(payload.get("trade_id")) is not int
        or payload.get("trade_id") != trade_review_id
        or any(
            type(payload.get(key)) is not str
            for key in required - {"trade_id"}
        )
        or (
            "resolution_detail" in payload
            and type(payload["resolution_detail"]) is not dict
        )
    ):
        return 0
    try:
        strict_json_dumps(payload)
        _validate_terminal_financial_evidence(
            exit_reason=payload["exit_reason"],
            exit_price=payload["exit_price"],
            mark_price=payload["mark_price"],
            pnl_amount=payload["pnl_amount"],
            pnl_pct=payload["pnl_pct"],
            balance_after=payload["balance_after"],
            dry_run=(event_type == "n16_dry_run_position_closed"),
        )
    except (TypeError, ValueError):
        return 0
    return 1


def _n13_json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
        sort_keys=True,
        allow_nan=False,
    )


_N13_REUSED_DETAIL_MAX_BYTES = 4_096
_N13_REUSED_DETAIL_MAX_DEPTH = 16
_N13_REUSED_DETAIL_MAX_ITEMS = 1_024
_N13_REUSED_DETAIL_MAX_CONTAINER = 256
_N13_REUSED_DETAIL_MAX_STRING = 2_048
_N13_REUSED_DETAIL_MAX_INT = 9_223_372_036_854_775_807

_PASSED_STRUCTURE_LEDGER_STRATEGIES = frozenset(
    {
        "N06", "N07", "N08", "N11", "N12", "N16", "N17", "N18", "N19", "N20",
        "N21", "N22", "N23", "N24", "N25",
    }
)
_LEGACY_SINGLE_STRATEGY_ID = "LEGACY_SINGLE"
_SUPPORTED_STRATEGY_IDS = frozenset(
    {"N%02d" % number for number in range(1, 26)}
    | {_LEGACY_SINGLE_STRATEGY_ID}
)
_SIGNAL_MANIFEST_SEED = "0" * 64
_STRATEGY_SIGNAL_DETAIL_MAX_BYTES = 65_536
_STRATEGY_SIGNAL_DETAIL_MAX_DEPTH = 32
_STRATEGY_SIGNAL_DETAIL_MAX_ITEMS = 20_000


def _strict_strategy_signal_retention_row(
    row: Any,
) -> tuple[Any, ...]:
    if type(row) not in (tuple, list) or len(row) != 8:
        raise RuntimeError("strategy signal retention row is invalid")
    value = tuple(row)
    current, active, state, cutoff, count, passed, manifest, origin = value
    normal_marker = bool(
        type(cutoff) is int
        and cutoff > 0
        and type(count) is int
        and count > 0
        and type(passed) is int
        and 0 <= passed <= count
        and type(manifest) is str
        and len(manifest) == 64
        and not any(character not in "0123456789abcdef" for character in manifest)
    )
    if value == (None, 0, "PENDING", None, None, None, None, None):
        return value
    if (
        type(current) is int
        and current > 0
        and active == 0
        and type(active) is int
        and state == "BACKFILLED"
        and origin == "LEGACY"
        and normal_marker
    ):
        return value
    if state == "COMPLETE" and type(active) is int and active == 1:
        if (
            origin == "GENESIS"
            and (current is None or (type(current) is int and current > 0))
            and type(cutoff) is int
            and cutoff == 0
            and type(count) is int
            and count == 0
            and type(passed) is int
            and passed == 0
            and type(manifest) is str
            and manifest == _SIGNAL_MANIFEST_SEED
        ):
            return value
        if (
            origin == "LEGACY"
            and type(current) is int
            and current > 0
            and normal_marker
        ):
            return value
    raise RuntimeError("strategy signal retention identity is inconsistent")


def _strategy_signal_retention_row(
    connection: sqlite3.Connection,
) -> tuple[Any, ...]:
    row = connection.execute(
        """
        SELECT current_scan_id, retention_active, migration_state,
               migration_cutoff_signal_id, source_signal_count,
               source_passed_count, source_manifest_sha256, retention_origin
        FROM strategy_signal_current WHERE singleton_id = 1
        """
    ).fetchone()
    return _strict_strategy_signal_retention_row(row)


def _strict_signal_text(value: Any, name: str, maximum: int = 256) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > maximum
        or value.strip() != value
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(f"{name} must be a canonical built-in string")
    return value


def _strategy_signal_evidence_sha256(payload: dict[str, Any]) -> str:
    encoded = json_dumps(payload).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _strict_strategy_signal_detail_value(
    value: Any,
    *,
    depth: int = 0,
    item_count: list[int] | None = None,
    active_containers: set[int] | None = None,
) -> None:
    if item_count is None:
        item_count = [0]
    if active_containers is None:
        active_containers = set()
    item_count[0] += 1
    if (
        depth > _STRATEGY_SIGNAL_DETAIL_MAX_DEPTH
        or item_count[0] > _STRATEGY_SIGNAL_DETAIL_MAX_ITEMS
    ):
        raise ValueError("strategy signal detail exceeds structural limits")
    if value is None or type(value) is bool or type(value) is str:
        return
    if type(value) is int:
        if abs(value) > 9_223_372_036_854_775_807:
            raise ValueError("strategy signal detail integer is out of range")
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("strategy signal detail contains non-finite data")
        return
    if type(value) not in {dict, list}:
        raise ValueError("strategy signal detail must contain built-in JSON values")
    identity = id(value)
    if identity in active_containers:
        raise ValueError("strategy signal detail contains a cycle")
    active_containers.add(identity)
    try:
        if type(value) is list:
            for item in value:
                _strict_strategy_signal_detail_value(
                    item,
                    depth=depth + 1,
                    item_count=item_count,
                    active_containers=active_containers,
                )
            return
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("strategy signal detail keys must be built-in strings")
            _strict_strategy_signal_detail_value(
                item,
                depth=depth + 1,
                item_count=item_count,
                active_containers=active_containers,
            )
    finally:
        active_containers.remove(identity)


def _strict_strategy_signal_detail_json(payload: Any) -> str:
    if type(payload) is not dict:
        raise ValueError("strategy signal detail must be a built-in dict")
    _strict_strategy_signal_detail_value(payload)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        allow_nan=False,
    )
    if len(encoded.encode("utf-8")) > _STRATEGY_SIGNAL_DETAIL_MAX_BYTES:
        raise ValueError("strategy signal detail exceeds byte limit")
    return encoded


def _load_strict_strategy_signal_detail_json(value: Any) -> dict[str, Any]:
    if (
        type(value) is not str
        or len(value.encode("utf-8")) > _STRATEGY_SIGNAL_DETAIL_MAX_BYTES
    ):
        raise ValueError("strategy signal detail JSON is invalid or oversized")

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        parsed: dict[str, Any] = {}
        for key, item in pairs:
            if type(key) is not str or key in parsed:
                raise ValueError("strategy signal detail JSON has invalid keys")
            parsed[key] = item
        return parsed

    def reject_constant(value: str) -> Any:
        raise ValueError(f"strategy signal detail JSON constant is invalid: {value}")

    parsed = json.loads(
        value,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=reject_constant,
    )
    _strict_strategy_signal_detail_value(parsed)
    if type(parsed) is not dict:
        raise ValueError("strategy signal detail JSON must contain an object")
    return parsed


def _validated_strategy_signal_row(
    row: tuple[Any, ...] | sqlite3.Row,
    scan_id: int,
) -> tuple[Any, ...]:
    values = tuple(row)
    if (
        len(values) != 14
        or type(values[0]) is not int
        or values[0] <= 0
        or type(values[1]) is not int
        or values[1] != scan_id
        or type(values[2]) is not str
        or values[2] not in _SUPPORTED_STRATEGY_IDS
        or type(values[3]) is not str
        or type(values[4]) is not str
        or type(values[5]) is not str
        or type(values[6]) is not str
        or type(values[7]) is not int
        or values[7] not in (0, 1)
        or type(values[8]) is not int
        or values[8] not in (0, 1)
        or type(values[9]) is not str
        or type(values[10]) is not str
        or (values[11] is not None and type(values[11]) is not str)
        or type(values[12]) is not str
        or type(values[13]) is not str
    ):
        raise RuntimeError("strategy signal batch row is invalid")
    try:
        _strict_signal_text(values[9], "decision", 128)
        _strict_signal_text(values[10], "reason", 512)
        _load_strict_strategy_signal_detail_json(values[12])
    except (TypeError, ValueError) as exc:
        raise RuntimeError("strategy signal execution overlay is invalid") from exc
    return values


def _strategy_signal_row_evidence(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "scan_id": row[1],
        "strategy_id": row[2],
        "symbol": row[3],
        "funding_rate": row[4],
        "matched_patterns": row[5],
        "trend_slope": row[6],
        "current_bullish": row[7],
        "passed": row[8],
        "decision": row[9],
        "reason": row[10],
        "structure_id": row[11],
        "detail_json": row[12],
        "created_at": row[13],
    }


def _strategy_signal_batch_snapshot(
    connection: sqlite3.Connection,
    scan_id: int,
) -> tuple[int, int | None, int | None, str]:
    """Rebuild a staging batch identity from its durable ordinary rows."""

    manifest_sha256 = _SIGNAL_MANIFEST_SEED
    count = 0
    first_signal_id: int | None = None
    last_signal_id: int | None = None
    rows = connection.execute(
        """
        SELECT id, scan_id, strategy_id, symbol, funding_rate,
               matched_patterns, trend_slope, current_bullish, passed,
               decision, reason, structure_id, detail_json, created_at
        FROM strategy_signals WHERE scan_id = ? ORDER BY id
        """,
        (scan_id,),
    )
    for row in rows:
        row = _validated_strategy_signal_row(row, scan_id)
        evidence_sha256 = _strategy_signal_evidence_sha256(
            _strategy_signal_row_evidence(row)
        )
        manifest_sha256 = hashlib.sha256(
            f"{manifest_sha256}:{evidence_sha256}".encode("ascii")
        ).hexdigest()
        count += 1
        if first_signal_id is None:
            first_signal_id = row[0]
        last_signal_id = row[0]
    return count, first_signal_id, last_signal_id, manifest_sha256


def _strategy_signal_expected_count_value(count: int) -> int | None:
    """Map the public zero-row batch count onto the existing nullable schema."""

    if type(count) is not int or count < 0:
        raise RuntimeError("strategy signal batch count is invalid")
    return count if count > 0 else None


def _validated_active_passed_audit(
    audit: tuple[Any, ...] | sqlite3.Row,
) -> tuple[tuple[Any, ...], str]:
    values = tuple(audit)
    if (
        len(values) != 17
        or type(values[0]) is not int
        or values[0] <= 0
        or (
            values[1] is not None
            and (type(values[1]) is not int or values[1] <= 0)
        )
        or type(values[2]) is not str
        or type(values[3]) is not str
        or type(values[4]) is not str
        or type(values[5]) is not str
        or type(values[6]) is not str
        or type(values[7]) is not int
        or values[7] not in (0, 1)
        or type(values[8]) is not int
        or values[8] != 1
        or type(values[9]) is not str
        or type(values[10]) is not str
        or (values[11] is not None and type(values[11]) is not str)
        or type(values[12]) is not str
        or type(values[13]) is not str
        or type(values[14]) is not str
        or len(values[14]) != 64
        or any(character not in "0123456789abcdef" for character in values[14])
        or type(values[15]) is not str
        or values[16] != "ACTIVE"
    ):
        raise RuntimeError("strategy passed audit publication evidence is invalid")
    evidence_sha256 = _strategy_signal_evidence_sha256(
        {
            "scan_id": values[1],
            "strategy_id": values[2],
            "symbol": values[3],
            "funding_rate": values[4],
            "matched_patterns": values[5],
            "trend_slope": values[6],
            "current_bullish": values[7],
            "passed": values[8],
            "decision": values[9],
            "reason": values[10],
            "structure_id": values[11],
            "detail_json": values[12],
            "created_at": values[13],
        }
    )
    if evidence_sha256 != values[14]:
        raise RuntimeError("strategy passed audit publication hash mismatch")
    return values, evidence_sha256


def _validate_active_structure_ledger(
    connection: sqlite3.Connection,
    ledger: tuple[Any, ...] | sqlite3.Row,
    strategy_id: str,
    symbol: str,
    structure_id: str,
) -> None:
    values = tuple(ledger)
    if (
        len(values) != 10
        or type(values[0]) is not int
        or values[0] <= 0
        or type(values[1]) is not str
        or values[1] != strategy_id
        or type(values[2]) is not str
        or values[2] != symbol
        or type(values[3]) is not str
        or values[3] != structure_id
        or type(values[4]) is not int
        or values[4] <= 0
        or (
            values[5] is not None
            and (type(values[5]) is not int or values[5] <= 0)
        )
        or type(values[6]) is not str
        or type(values[7]) is not str
        or len(values[7]) != 64
        or any(character not in "0123456789abcdef" for character in values[7])
        or type(values[8]) is not str
        or values[9] != "ACTIVE"
    ):
        raise RuntimeError("strategy passed ledger publication evidence is invalid")
    source_audit = connection.execute(
        """
        SELECT source_signal_id, source_scan_id, strategy_id, symbol,
               funding_rate, matched_patterns, trend_slope, current_bullish,
               passed, decision, reason, structure_id, detail_json,
               signal_created_at, evidence_sha256, created_at, claim_state
        FROM strategy_passed_signal_audits
        WHERE source_signal_id = ?
        """,
        (values[4],),
    ).fetchone()
    if source_audit is None:
        raise RuntimeError("strategy passed ledger source audit is missing")
    source_audit, evidence_sha256 = _validated_active_passed_audit(source_audit)
    if (
        source_audit[1] != values[5]
        or source_audit[2] != values[1]
        or source_audit[3] != values[2]
        or source_audit[11] != values[3]
        or source_audit[13] != values[6]
        or evidence_sha256 != values[7]
    ):
        raise RuntimeError("strategy passed ledger source identity mismatch")


def _attest_permanent_execution_claim(
    connection: sqlite3.Connection,
    signal_id: int,
    strategy_id: str,
    symbol: str,
    structure_id: str,
) -> tuple[int, dict[str, Any], str]:
    """Authenticate a published claim without retention-deletable rows.

    ``strategy_signals`` is intentionally absent: after a later CURRENT batch
    is published, retention removes the old ordinary row.  The ACTIVE audit is
    a self-contained, hash-authenticated copy of the original source signal;
    the permanent structure ledger independently binds its source id, scan,
    timestamp and hash.  Strategy-specific callers must additionally attest
    their immutable lifecycle evidence.
    """

    audit = connection.execute(
        """
        SELECT source_signal_id,source_scan_id,strategy_id,symbol,
               funding_rate,matched_patterns,trend_slope,current_bullish,
               passed,decision,reason,structure_id,detail_json,
               signal_created_at,evidence_sha256,created_at,claim_state
        FROM strategy_passed_signal_audits
        WHERE source_signal_id=?
        """,
        (signal_id,),
    ).fetchone()
    if audit is None:
        raise RuntimeError("permanent execution audit is missing")
    audit, evidence_sha256 = _validated_active_passed_audit(audit)
    if (
        audit[0] != signal_id
        or type(audit[1]) is not int
        or audit[1] <= 0
        or audit[2:4] != (strategy_id, symbol)
        or audit[8:12] != (1, "PASSED", "PASSED", structure_id)
    ):
        raise RuntimeError("permanent execution audit identity conflicts")
    ledger = connection.execute(
        """
        SELECT id,strategy_id,symbol,structure_id,source_signal_id,
               source_scan_id,source_signal_created_at,evidence_sha256,
               created_at,claim_state
        FROM strategy_passed_structure_ledger
        WHERE source_signal_id=?
        """,
        (signal_id,),
    ).fetchone()
    if ledger is None:
        raise RuntimeError("permanent execution ledger is missing")
    _validate_active_structure_ledger(
        connection, ledger, strategy_id, symbol, structure_id
    )
    if (
        ledger[4] != signal_id
        or ledger[5] != audit[1]
        or ledger[6] != audit[13]
        or ledger[7] != evidence_sha256
    ):
        raise RuntimeError("permanent execution ledger identity conflicts")
    try:
        detail = _load_strict_strategy_signal_detail_json(audit[12])
    except (TypeError, ValueError) as exc:
        raise RuntimeError("permanent execution detail is invalid") from exc
    return audit[1], detail, evidence_sha256


def _attest_terminal_execution_derivation(
    detail: dict[str, Any],
    terminal_evidence: dict[str, Any],
    allowed_stage_reasons: Mapping[str, frozenset[str]],
    *,
    strategy_id: str,
) -> None:
    """Bind a legal post-claim terminal state to its signed PASSED shape."""

    original = detail.get("structure")
    terminal = terminal_evidence.get("structure")
    reason = terminal_evidence.get("reason")
    stage = terminal_evidence.get("stage")
    if (
        type(original) is not dict
        or type(terminal) is not dict
        or type(stage) is not str
        or stage not in allowed_stage_reasons
        or type(reason) is not str
        or reason not in allowed_stage_reasons[stage]
    ):
        raise RuntimeError("terminal execution derivation is invalid")
    if strategy_id == "N19":
        structure_matches = original == terminal
    elif strategy_id == "N18":
        original_base = dict(original)
        original_entry = original_base.pop("entry", None)
        terminal_base = dict(terminal)
        terminal_entry = terminal_base.pop("entry", None)
        historical_cutoff = (
            original_base.get("b", {}).get("open_time_ms")
            if type(original_base.get("b")) is dict
            else None
        )
        terminal_entry_matches = (
            terminal_entry is None
            and terminal_evidence.get("terminal_cutoff_time_ms")
            == historical_cutoff
        ) or (
            type(original_entry) is dict
            and type(terminal_entry) is dict
            and terminal_entry.get("open_time_ms")
            == original_entry.get("open_time_ms")
            and terminal_evidence.get("terminal_cutoff_time_ms")
            == terminal_entry.get("open_time_ms")
        )
        structure_matches = (
            type(original_entry) is dict
            and type(original_entry.get("open_time_ms")) is int
            and terminal_base == original_base
            and terminal_entry_matches
        )
    else:
        raise RuntimeError("terminal execution strategy is invalid")
    if not structure_matches:
        raise RuntimeError("terminal execution structure conflicts")


def _strategy_signal_published_snapshot(
    connection: sqlite3.Connection,
    scan_id: int,
) -> tuple[int, int | None, int | None, str]:
    """Rebuild immutable publication evidence after CURRENT rows are enriched.

    PASSED rows may receive execution status in decision/reason/detail_json after
    publication.  Their original raw decision is therefore reconstructed from
    the immutable ACTIVE audit.  REJECTED rows are never enriched and remain an
    exact ordinary-row commitment.
    """

    rows = connection.execute(
        """
        SELECT id, scan_id, strategy_id, symbol, funding_rate,
               matched_patterns, trend_slope, current_bullish, passed,
               decision, reason, structure_id, detail_json, created_at
        FROM strategy_signals WHERE scan_id = ? ORDER BY id
        """,
        (scan_id,),
    ).fetchall()
    audits = connection.execute(
        """
        SELECT source_signal_id, source_scan_id, strategy_id, symbol,
               funding_rate, matched_patterns, trend_slope, current_bullish,
               passed, decision, reason, structure_id, detail_json,
               signal_created_at, evidence_sha256, created_at, claim_state
        FROM strategy_passed_signal_audits
        WHERE source_scan_id = ?
        ORDER BY source_signal_id
        """,
        (scan_id,),
    ).fetchall()
    audit_by_signal_id: dict[int, tuple[tuple[Any, ...], str]] = {}
    for audit in audits:
        validated_audit, evidence_sha256 = _validated_active_passed_audit(audit)
        source_signal_id = validated_audit[0]
        if source_signal_id in audit_by_signal_id:
            raise RuntimeError("duplicate strategy passed audit publication evidence")
        audit_by_signal_id[source_signal_id] = (
            validated_audit,
            evidence_sha256,
        )

    manifest_sha256 = _SIGNAL_MANIFEST_SEED
    first_signal_id: int | None = None
    last_signal_id: int | None = None
    expected_ledger_keys: dict[tuple[str, str], str] = {}
    used_audit_ids: set[int] = set()
    for raw_row in rows:
        row = _validated_strategy_signal_row(raw_row, scan_id)
        if row[8] == 1:
            audit_entry = audit_by_signal_id.get(row[0])
            if audit_entry is None:
                raise RuntimeError("strategy passed publication audit is missing")
            audit, evidence_sha256 = audit_entry
            immutable_indexes = (0, 1, 2, 3, 4, 5, 6, 7, 8, 11, 13)
            if tuple(row[index] for index in immutable_indexes) != tuple(
                audit[index] for index in immutable_indexes
            ):
                raise RuntimeError("strategy passed ordinary identity changed")
            used_audit_ids.add(row[0])
            if row[2] in _PASSED_STRUCTURE_LEDGER_STRATEGIES:
                if type(row[11]) is not str or not row[11]:
                    raise RuntimeError("strategy passed structure identity is missing")
                key = (row[2], row[11])
                existing_symbol = expected_ledger_keys.get(key)
                if existing_symbol is not None and existing_symbol != row[3]:
                    raise RuntimeError("strategy passed structure symbol conflict")
                expected_ledger_keys[key] = row[3]
                if row[2] == "N16":
                    _validate_n16_passed_lifecycle(
                        connection, row[3], row[11], audit[12]
                    )
                    _validate_n16_consumption_seal(
                        connection, row[3], row[11]
                    )
        else:
            evidence_sha256 = _strategy_signal_evidence_sha256(
                _strategy_signal_row_evidence(row)
            )
        manifest_sha256 = hashlib.sha256(
            f"{manifest_sha256}:{evidence_sha256}".encode("ascii")
        ).hexdigest()
        if first_signal_id is None:
            first_signal_id = row[0]
        last_signal_id = row[0]

    if used_audit_ids != set(audit_by_signal_id):
        raise RuntimeError("strategy passed publication audit count mismatch")

    for (strategy_id, structure_id), symbol in expected_ledger_keys.items():
        ledger_rows = connection.execute(
            """
            SELECT id, strategy_id, symbol, structure_id, source_signal_id,
                   source_scan_id, source_signal_created_at, evidence_sha256,
                   created_at, claim_state
            FROM strategy_passed_structure_ledger
            WHERE strategy_id = ? AND structure_id = ?
            """,
            (strategy_id, structure_id),
        ).fetchall()
        if len(ledger_rows) != 1:
            raise RuntimeError("strategy passed ledger publication count mismatch")
        _validate_active_structure_ledger(
            connection,
            ledger_rows[0],
            strategy_id,
            symbol,
            structure_id,
        )

    sourced_ledgers = connection.execute(
        """
        SELECT strategy_id, symbol, structure_id
        FROM strategy_passed_structure_ledger
        WHERE source_scan_id = ?
        """,
        (scan_id,),
    ).fetchall()
    for ledger_key in sourced_ledgers:
        if (
            len(ledger_key) != 3
            or type(ledger_key[0]) is not str
            or type(ledger_key[1]) is not str
            or type(ledger_key[2]) is not str
            or expected_ledger_keys.get((ledger_key[0], ledger_key[2]))
            != ledger_key[1]
        ):
            raise RuntimeError("unexpected strategy passed ledger publication evidence")

    return len(rows), first_signal_id, last_signal_id, manifest_sha256


def _strategy_signal_claim_snapshot(
    connection: sqlite3.Connection,
    scan_id: int,
    claim_state: str,
) -> tuple[int, int]:
    """Strictly bind PASSED audit/ledger claims to one ordinary signal batch."""

    if claim_state not in {"STAGED", "ACTIVE"}:
        raise RuntimeError("invalid strategy signal claim state")
    passed_rows = connection.execute(
        """
        SELECT id, scan_id, strategy_id, symbol, funding_rate,
               matched_patterns, trend_slope, current_bullish, passed,
               decision, reason, structure_id, detail_json, created_at
        FROM strategy_signals
        WHERE scan_id = ? AND passed = 1
        ORDER BY id
        """,
        (scan_id,),
    ).fetchall()
    audit_rows = connection.execute(
        """
        SELECT id, source_signal_id, source_scan_id, strategy_id, symbol,
               funding_rate, matched_patterns, trend_slope, current_bullish,
               passed, decision, reason, structure_id, detail_json,
               signal_created_at, evidence_sha256, created_at, claim_state
        FROM strategy_passed_signal_audits
        WHERE source_scan_id = ?
        ORDER BY source_signal_id
        """,
        (scan_id,),
    ).fetchall()
    if len(audit_rows) != len(passed_rows):
        raise RuntimeError("strategy passed audit claim count mismatch")

    expected_ledgers: list[tuple[tuple[Any, ...], str]] = []
    for signal, audit in zip(passed_rows, audit_rows):
        if (
            len(signal) != 14
            or type(signal[0]) is not int
            or signal[0] <= 0
            or type(signal[1]) is not int
            or signal[1] != scan_id
            or type(signal[2]) is not str
            or type(signal[3]) is not str
            or type(signal[4]) is not str
            or type(signal[5]) is not str
            or type(signal[6]) is not str
            or type(signal[7]) is not int
            or signal[7] not in (0, 1)
            or type(signal[8]) is not int
            or signal[8] != 1
            or type(signal[9]) is not str
            or type(signal[10]) is not str
            or (signal[11] is not None and type(signal[11]) is not str)
            or type(signal[12]) is not str
            or type(signal[13]) is not str
        ):
            raise RuntimeError("strategy passed signal claim source is invalid")
        evidence_sha256 = _strategy_signal_evidence_sha256(
            {
                "scan_id": signal[1],
                "strategy_id": signal[2],
                "symbol": signal[3],
                "funding_rate": signal[4],
                "matched_patterns": signal[5],
                "trend_slope": signal[6],
                "current_bullish": signal[7],
                "passed": signal[8],
                "decision": signal[9],
                "reason": signal[10],
                "structure_id": signal[11],
                "detail_json": signal[12],
                "created_at": signal[13],
            }
        )
        expected_audit = tuple(signal) + (evidence_sha256,)
        if (
            len(audit) != 18
            or type(audit[0]) is not int
            or audit[0] <= 0
            or tuple(audit[1:16]) != expected_audit
            or type(audit[16]) is not str
            or audit[17] != claim_state
        ):
            raise RuntimeError("strategy passed audit claim identity mismatch")
        if signal[2] in _PASSED_STRUCTURE_LEDGER_STRATEGIES:
            if type(signal[11]) is not str or not signal[11]:
                raise RuntimeError("strategy structure claim identity is missing")
            expected_ledgers.append((signal, evidence_sha256))
            if signal[2] == "N16":
                _validate_n16_passed_lifecycle(
                    connection,
                    signal[3],
                    signal[11],
                    signal[12],
                )
                seal = _n16_consumption_seal_row(connection, signal[11])
                if claim_state == "STAGED" and seal is not None:
                    raise RuntimeError(
                        "N16 STAGED claim already has a consumption seal"
                    )
                if claim_state == "ACTIVE":
                    _validate_n16_consumption_seal(
                        connection, signal[3], signal[11]
                    )

    ledger_rows = connection.execute(
        """
        SELECT id, strategy_id, symbol, structure_id, source_signal_id,
               source_scan_id, source_signal_created_at, evidence_sha256,
               created_at, claim_state
        FROM strategy_passed_structure_ledger
        WHERE source_scan_id = ?
        ORDER BY source_signal_id
        """,
        (scan_id,),
    ).fetchall()
    if len(ledger_rows) != len(expected_ledgers):
        raise RuntimeError("strategy passed ledger claim count mismatch")
    for (signal, evidence_sha256), ledger in zip(expected_ledgers, ledger_rows):
        expected_ledger = (
            signal[2],
            signal[3],
            signal[11],
            signal[0],
            signal[1],
            signal[13],
            evidence_sha256,
        )
        if (
            len(ledger) != 10
            or type(ledger[0]) is not int
            or ledger[0] <= 0
            or tuple(ledger[1:8]) != expected_ledger
            or type(ledger[8]) is not str
            or ledger[9] != claim_state
        ):
            raise RuntimeError("strategy passed ledger claim identity mismatch")
    return len(passed_rows), len(expected_ledgers)


def _micro_staged_lifecycle_snapshot(
    connection: sqlite3.Connection,
    scan_id: int,
) -> int:
    """Authenticate the exact unpublished micro execution claims for a batch."""

    from .micro_schema import (
        MICRO_STRATEGY_IDS,
        validate_micro_staged_lifecycle_graph,
    )

    if type(scan_id) is not int or scan_id <= 0:
        raise RuntimeError("micro staging scan identity is invalid")
    signals = connection.execute(
        "SELECT id,strategy_id,symbol,structure_id FROM strategy_signals "
        "WHERE scan_id=? AND passed=1 AND strategy_id IN "
        "('N21','N22','N23','N24','N25') ORDER BY id",
        (scan_id,),
    ).fetchall()
    rows = validate_micro_staged_lifecycle_graph(connection, scan_id)
    expected = [
        (signal_id, strategy_id, symbol, structure_id, scan_id, "STAGED")
        for signal_id, strategy_id, symbol, structure_id in signals
    ]
    if rows != tuple(expected) or any(
        row[1] not in MICRO_STRATEGY_IDS for row in rows
    ):
        raise RuntimeError("micro staged lifecycle graph conflicts")
    return len(rows)


def _delete_staged_strategy_signal_batch(
    connection: sqlite3.Connection,
    scan_id: int,
    recorded_count: int,
    first_id: int | None,
    last_id: int | None,
) -> tuple[int, int, int]:
    """Delete one fully authenticated unpublished execution graph.

    Intrinsic ``micro_passed_analyses`` are deliberately not touched.
    """

    passed_claim_count, ledger_claim_count = _strategy_signal_claim_snapshot(
        connection, scan_id, "STAGED"
    )
    micro_claim_count = _micro_staged_lifecycle_snapshot(connection, scan_id)
    removed_micro = connection.execute(
        "DELETE FROM micro_strategy_lifecycle "
        "WHERE source_scan_id=? AND claim_state='STAGED'",
        (scan_id,),
    )
    if removed_micro.rowcount != micro_claim_count:
        raise RuntimeError("micro staged lifecycle cleanup mismatch")
    removed_ledgers = connection.execute(
        "DELETE FROM strategy_passed_structure_ledger "
        "WHERE source_scan_id=? AND claim_state='STAGED'",
        (scan_id,),
    )
    if removed_ledgers.rowcount != ledger_claim_count:
        raise RuntimeError("strategy signal staged ledger cleanup mismatch")
    removed_audits = connection.execute(
        "DELETE FROM strategy_passed_signal_audits "
        "WHERE source_scan_id=? AND claim_state='STAGED'",
        (scan_id,),
    )
    if removed_audits.rowcount != passed_claim_count:
        raise RuntimeError("strategy signal staged audit cleanup mismatch")
    if recorded_count:
        if type(first_id) is not int or type(last_id) is not int:
            raise RuntimeError("invalid strategy signal staging range")
        removed_signals = connection.execute(
            "DELETE FROM strategy_signals "
            "WHERE scan_id=? AND id BETWEEN ? AND ?",
            (scan_id, first_id, last_id),
        )
        if removed_signals.rowcount != recorded_count:
            raise RuntimeError("strategy signal staging cleanup count mismatch")
    removed_batch = connection.execute(
        "DELETE FROM strategy_signal_batches "
        "WHERE scan_id=? AND state='STAGING'",
        (scan_id,),
    )
    if removed_batch.rowcount != 1:
        raise RuntimeError("strategy signal staging batch cleanup mismatch")
    return passed_claim_count, ledger_claim_count, micro_claim_count


def _validate_unpublished_genesis_evidence(
    connection: sqlite3.Connection,
) -> None:
    batches = connection.execute(
        """
        SELECT scan_id, state, recorded_count, expected_count,
               first_signal_id, last_signal_id, manifest_sha256, completed_at
        FROM strategy_signal_batches ORDER BY scan_id
        """
    ).fetchall()
    if len(batches) > 1:
        raise RuntimeError("unpublished genesis has multiple signal batches")
    if not batches:
        if connection.execute(
            "SELECT 1 FROM strategy_signals LIMIT 1"
        ).fetchone() or connection.execute(
            "SELECT 1 FROM strategy_passed_signal_audits LIMIT 1"
        ).fetchone() or connection.execute(
            "SELECT 1 FROM strategy_passed_structure_ledger LIMIT 1"
        ).fetchone():
            raise RuntimeError("unpublished genesis has orphaned evidence")
        return
    batch = batches[0]
    if (
        len(batch) != 8
        or type(batch[0]) is not int
        or batch[0] <= 0
        or batch[1] != "STAGING"
        or type(batch[2]) is not int
        or batch[2] < 0
        or batch[3] is not None
        or type(batch[6]) is not str
        or len(batch[6]) != 64
        or batch[7] is not None
    ):
        raise RuntimeError("unpublished genesis staging batch is invalid")
    actual = _strategy_signal_batch_snapshot(connection, batch[0])
    if actual != (batch[2], batch[4], batch[5], batch[6]):
        raise RuntimeError("unpublished genesis staging identity mismatch")
    _strategy_signal_claim_snapshot(connection, batch[0], "STAGED")
    signal_sources = connection.execute(
        "SELECT DISTINCT scan_id FROM strategy_signals"
    ).fetchall()
    claim_sources = connection.execute(
        """
        SELECT source_scan_id FROM strategy_passed_signal_audits
        UNION
        SELECT source_scan_id FROM strategy_passed_structure_ledger
        """
    ).fetchall()
    if signal_sources not in ([], [(batch[0],)]) or any(
        type(row) not in (tuple, list)
        or len(row) != 1
        or row[0] != batch[0]
        for row in claim_sources
    ):
        raise RuntimeError("unpublished genesis evidence has an orphaned source")


def _validate_complete_strategy_signal_graph(
    connection: sqlite3.Connection,
    retention: tuple[Any, ...] | None = None,
) -> None:
    """Certify ownership of every ordinary signal in one COMPLETE graph.

    Permanent ACTIVE audit/ledger rows intentionally outlive their ordinary
    source batch and are not constrained here.  PENDING/BACKFILLED migration
    states are likewise validated by the offline maintenance workflow because
    they legitimately contain legacy ordinary rows without batch ownership.
    """

    retention = retention or _strategy_signal_retention_row(connection)
    if retention[1:3] != (1, "COMPLETE"):
        raise RuntimeError("strategy signal batch graph is not active")
    orphan = connection.execute(
        """
        SELECT signal.id, signal.scan_id
        FROM strategy_signals AS signal
        LEFT JOIN strategy_signal_batches AS batch
          ON batch.scan_id = signal.scan_id
        WHERE typeof(signal.scan_id) != 'integer'
           OR signal.scan_id <= 0
           OR batch.scan_id IS NULL
        LIMIT 1
        """
    ).fetchone()
    if orphan is not None:
        raise RuntimeError("ordinary strategy signal has no batch owner")
    batches = connection.execute(
        """
        SELECT scan_id, state, recorded_count, expected_count,
               first_signal_id, last_signal_id, manifest_sha256, completed_at
        FROM strategy_signal_batches ORDER BY scan_id
        """
    ).fetchall()
    current_batches = [batch for batch in batches if batch[1] == "CURRENT"]
    staging_batches = [batch for batch in batches if batch[1] == "STAGING"]
    if len(current_batches) > 1 or len(staging_batches) > 1 or (
        len(current_batches) + len(staging_batches) != len(batches)
    ):
        raise RuntimeError("strategy signal batch graph cardinality is invalid")

    current_scan_id = retention[0]
    if current_scan_id is None:
        if retention[7] != "GENESIS" or current_batches:
            raise RuntimeError("unpublished strategy signal graph is invalid")
        _validate_unpublished_genesis_evidence(connection)
        return
    if (
        type(current_scan_id) is not int
        or current_scan_id <= 0
        or len(current_batches) != 1
        or current_batches[0][0] != current_scan_id
    ):
        raise RuntimeError("current strategy signal batch owner is invalid")

    current = current_batches[0]
    current_expected_count = _strategy_signal_expected_count_value(current[2])
    if (
        len(current) != 8
        or type(current[0]) is not int
        or type(current[2]) is not int
        or current[2] < 0
        or current[3] != current_expected_count
        or (
            current[2] > 0
            and (
                type(current[4]) is not int
                or current[4] <= 0
                or type(current[5]) is not int
                or current[5] < current[4]
            )
        )
        or (current[2] == 0 and current[4:6] != (None, None))
        or type(current[6]) is not str
        or len(current[6]) != 64
        or type(current[7]) is not str
        or not current[7]
        or _strategy_signal_published_snapshot(connection, current_scan_id)
        != (current[2], current[4], current[5], current[6])
    ):
        raise RuntimeError("current strategy signal batch graph is invalid")

    staging_scan_ids = set()
    for staging in staging_batches:
        if (
            len(staging) != 8
            or type(staging[0]) is not int
            or staging[0] <= 0
            or type(staging[2]) is not int
            or staging[2] < 0
            or staging[3] is not None
            or type(staging[6]) is not str
            or len(staging[6]) != 64
            or staging[7] is not None
            or _strategy_signal_batch_snapshot(connection, staging[0])
            != (staging[2], staging[4], staging[5], staging[6])
        ):
            raise RuntimeError("staging strategy signal batch graph is invalid")
        _strategy_signal_claim_snapshot(connection, staging[0], "STAGED")
        staging_scan_ids.add(staging[0])

    staged_claim_sources = connection.execute(
        """
        SELECT source_scan_id FROM strategy_passed_signal_audits
        WHERE claim_state = 'STAGED'
        UNION
        SELECT source_scan_id FROM strategy_passed_structure_ledger
        WHERE claim_state = 'STAGED'
        """
    ).fetchall()
    if any(
        type(row) not in (tuple, list)
        or len(row) != 1
        or type(row[0]) is not int
        or row[0] not in staging_scan_ids
        for row in staged_claim_sources
    ):
        raise RuntimeError("staged strategy signal claim has no batch owner")


def _n13_reused_detail_fallback() -> dict[str, Any]:
    return {
        "audit_detail_omitted": True,
        "audit_detail_status": "OVERSIZE_OR_INVALID",
    }


def _n13_reused_detail_is_strict_json(
    value: Any,
    *,
    depth: int = 0,
    item_count: list[int] | None = None,
    active_containers: set[int] | None = None,
) -> bool:
    if item_count is None:
        item_count = [0]
    if active_containers is None:
        active_containers = set()
    item_count[0] += 1
    if (
        item_count[0] > _N13_REUSED_DETAIL_MAX_ITEMS
        or depth > _N13_REUSED_DETAIL_MAX_DEPTH
    ):
        return False
    if value is None or type(value) is bool:
        return True
    if type(value) is int:
        return abs(value) <= _N13_REUSED_DETAIL_MAX_INT
    if type(value) is float:
        return math.isfinite(value)
    if type(value) is str:
        return len(value) <= _N13_REUSED_DETAIL_MAX_STRING
    if type(value) not in {dict, list}:
        return False
    identity = id(value)
    if (
        identity in active_containers
        or len(value) > _N13_REUSED_DETAIL_MAX_CONTAINER
    ):
        return False
    active_containers.add(identity)
    try:
        if type(value) is list:
            return all(
                _n13_reused_detail_is_strict_json(
                    item,
                    depth=depth + 1,
                    item_count=item_count,
                    active_containers=active_containers,
                )
                for item in value
            )
        for key, item in value.items():
            if (
                type(key) is not str
                or not key
                or len(key) > 128
                or key == "metric_context_bars"
                or not _n13_reused_detail_is_strict_json(
                    item,
                    depth=depth + 1,
                    item_count=item_count,
                    active_containers=active_containers,
                )
            ):
                return False
        return True
    finally:
        active_containers.remove(identity)


def _bounded_n13_reused_detail(value: Any) -> dict[str, Any]:
    if (
        type(value) is not dict
        or not _n13_reused_detail_is_strict_json(value)
    ):
        return _n13_reused_detail_fallback()
    try:
        encoded = json_dumps(value).encode("utf-8")
    except (TypeError, ValueError):
        return _n13_reused_detail_fallback()
    return (
        value
        if len(encoded) <= _N13_REUSED_DETAIL_MAX_BYTES
        else _n13_reused_detail_fallback()
    )


def _n13_strict_json_loads(value: Any) -> Any:
    if type(value) is not str:
        raise ValueError("N13 state JSON must be a built-in string")

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        parsed: dict[str, Any] = {}
        for key, item in pairs:
            if key in parsed:
                raise ValueError(f"duplicate N13 state JSON key: {key}")
            parsed[key] = item
        return parsed

    def reject_nonstandard_constant(constant: str) -> Any:
        raise ValueError(f"non-standard N13 state JSON constant: {constant}")

    return json.loads(
        value,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=reject_nonstandard_constant,
    )


def _canonical_n15_e_time(value: str) -> int:
    if type(value) is not str or not value:
        raise ValueError("N15 e_time invalid")
    parsed = int(value)
    if str(parsed) != value or parsed <= 0 or parsed % 900_000 != 0:
        raise ValueError("N15 e_time invalid")
    return parsed


@dataclass(frozen=True)
class SymbolCooldown:
    symbol: str
    cooldown_until: str
    reason: str
    source_trade_id: int | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class StrategyState:
    strategy_id: str
    consecutive_wins: int
    paper_trade_count: int
    win_count: int
    loss_count: int
    win_rate: str
    live_eligible: bool
    last_trade_result: str | None
    last_trade_closed_at: str | None
    updated_at: str
    live_result_pending: bool = False


@dataclass(frozen=True)
class StrategyPaperTrade:
    id: int
    strategy_id: str
    symbol: str
    opened_at: str
    last_checked_at: str | None
    closed_at: str | None
    entry_price: str
    stop_loss_price: str
    take_profit_price: str
    result: str
    exit_reason: str | None
    r_multiple: str | None
    funding_rate: str
    orders_json: str
    detail_json: str


class PaperTradePersistenceError(RuntimeError):
    """A paper lifecycle write could not be durably confirmed."""


@dataclass(frozen=True)
class StrategyLiveFinalization:
    trade_id: int
    idempotent: bool
    pnl_amount: str = ""
    balance_after: str = ""


@dataclass(frozen=True)
class StrategyLiveOpenClaim:
    trade_review_id: int
    review_created: bool
    link_created: bool


@dataclass(frozen=True)
class StrategyLiveFinalizationEvidence:
    status: str
    finalization: StrategyLiveFinalization | None = None
    result: str | None = None
    exit_reason: str | None = None
    exit_price: str | None = None
    mark_price: str | None = None
    pnl_amount: str | None = None
    pnl_pct: str | None = None
    balance_after: str | None = None
    detail: str = ""


@dataclass(frozen=True)
class N08StructureState:
    id: int
    strategy_id: str
    symbol: str
    structure_id: str
    range_start_time: str
    range_end_time: str
    upper_reference: str
    lower_reference: str
    upper_tolerance_boundary: str
    lower_tolerance_boundary: str
    first_streak_start_time: str
    first_streak_end_time: str
    status: str
    reason: str
    reset_open_time: str | None
    detail_json: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class N08HistoryCoverage:
    strategy_id: str
    symbol: str
    continuous_from_open_time: str
    continuous_until_open_time: str
    last_response_first_open_time: str
    last_response_last_open_time: str
    last_gap_from_open_time: str | None
    last_gap_to_open_time: str | None
    updated_at: str


@dataclass(frozen=True)
class HistoryCoverageProposal:
    """One bounded N17/N18/N19 coverage transition for atomic publication."""

    strategy_id: str
    symbol: str
    source_start_time_ms: int
    covered_through_time_ms: int
    current_open_time_ms: int
    source_sha256: str
    # A narrowly-scoped N19 CONFIRMED -> historical MISSED transition travels
    # with an explicit canonical identity.  Every field participates in
    # equality and diagnostics; omitting or replacing the payload can no
    # longer compare equal to the proposal that advanced CURRENT.
    n19_terminal_family_id: Any = None
    n19_terminal_structure_id: Any = None
    n19_terminal_evidence_sha256: Any = None
    n19_terminal_state_record: Any = None


HISTORY_COVERAGE_STRATEGY_IDS = frozenset({"N17", "N18", "N19"})


def history_coverage_signal_requires_proposal(
    strategy_id: str,
    decision: str,
    reason: str,
) -> bool:
    """Return the canonical ordinary-signal ownership requirement.

    A newly listed Top100 member with fewer than 122 rows cannot truthfully
    create coverage.  Every other ordinary N17-N19 decision -- including
    trading cooldowns -- must have a proposal, so eligibility cannot hide a
    missing or failed durable market-data transition.
    """

    if (
        strategy_id not in HISTORY_COVERAGE_STRATEGY_IDS
        or type(decision) is not str
        or type(reason) is not str
    ):
        raise ValueError("history coverage signal contract is invalid")
    insufficient_reason = f"{strategy_id}_HISTORY_SOURCE_INSUFFICIENT"
    if reason == insufficient_reason:
        if decision != "REJECTED":
            raise ValueError(
                "history coverage insufficient signal decision is invalid"
            )
        return False
    return True


@dataclass(frozen=True)
class StrategySignalBatchWriteResult:
    """Atomic result for one scheduler round's ordinary signal rows."""

    signal_ids: tuple[int, ...] = ()
    failed_index: int | None = None

    @property
    def complete(self) -> bool:
        return self.failed_index is None and bool(self.signal_ids)


@dataclass(frozen=True)
class _PreparedStrategySignal:
    scan_id: int
    strategy_id: str
    symbol: str
    funding_rate: str
    matched_patterns_json: str
    trend_slope: str
    current_bullish: bool
    passed: bool
    decision: str
    reason: str
    structure_id: str | None
    detail_payload: dict[str, Any]
    detail_json: str
    micro_evidence_json: str | None
    micro_evidence_sha256: str | None
    created_at: str
    evidence_sha256: str


@dataclass(frozen=True)
class N09StructureState:
    id: int
    strategy_id: str
    symbol: str
    structure_id: str
    s1_time: str
    s1_price: str
    l_time: str
    l_price: str
    first_touch_time: str
    status: str
    reason: str
    detail_json: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class N10StructureState:
    id: int
    strategy_id: str
    symbol: str
    structure_id: str
    support_start_time: str
    support_end_time: str
    support_price: str
    w_time: str
    c_time: str
    e_time: str
    status: str
    reason: str
    detail_json: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class StrategyStructureTerminalState:
    id: int
    strategy_id: str
    symbol: str
    structure_id: str
    status: str
    reason: str
    detail_json: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class N16TrendSupportState:
    id: int
    strategy_id: str
    symbol: str
    episode_id: str
    structure_id: str | None
    stage: str
    reason: str
    quote_volume_rank: int
    evidence_json: str
    evidence_sha256: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class N17RangeSupportState:
    id: int
    strategy_id: str
    symbol: str
    family_id: str
    structure_id: str
    stage: str
    reason: str
    quote_volume_rank: int
    box_start_time_ms: int
    box_end_time_ms: int
    reset_after_time_ms: int | None
    evidence_json: str
    evidence_sha256: str
    created_at: str
    updated_at: str


def _validate_n17_lifecycle_state_rows(connection: sqlite3.Connection) -> None:
    """Strictly reproduce every durable N17 row from its own frozen source."""

    from .n17_analyzer import decode_n17_state_evidence

    rows = connection.execute(
        "SELECT strategy_id,symbol,family_id,structure_id,stage,reason,"
        "quote_volume_rank,box_start_time_ms,box_end_time_ms,"
        "reset_after_time_ms,evidence_json,evidence_sha256 "
        "FROM n17_range_support_states ORDER BY id"
    )
    for row in rows:
        if (
            len(row) != 12
            or type(row[10]) is not str
            or type(row[11]) is not str
            or hashlib.sha256(row[10].encode("utf-8")).hexdigest() != row[11]
        ):
            raise RuntimeError("N17 lifecycle outer evidence is invalid")
        decoded = decode_n17_state_evidence(
            row[10], expected_symbol=row[1]
        )
        if (
            row[0] != decoded.strategy_id
            or row[1] != decoded.symbol
            or row[2] != decoded.family_id
            or row[3] != decoded.structure_id
            or row[4] != decoded.stage
            or row[5] != decoded.reason
            or row[6] != decoded.quote_volume_rank
            or row[7] != decoded.box_start_time_ms
            or row[8] != decoded.box_end_time_ms
            or row[9] != decoded.reset_after_time_ms
            or row[11] != decoded.evidence_sha256
        ):
            raise RuntimeError("N17 lifecycle columns conflict with evidence")


@dataclass(frozen=True)
class N19StaircaseState:
    id: int
    strategy_id: str
    symbol: str
    family_id: str
    structure_id: str | None
    stage: str
    reason: str
    quote_volume_rank: int
    s_open_time_ms: int
    x_open_time_ms: int
    reset_after_time_ms: int | None
    evidence_json: str
    evidence_sha256: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class N18TriangleState:
    id: int
    strategy_id: str
    symbol: str
    family_id: str
    structure_id: str | None
    stage: str
    reason: str
    quote_volume_rank: int
    l1_open_time_ms: int
    a_open_time_ms: int
    terminal_cutoff_time_ms: int | None
    evidence_json: str
    evidence_sha256: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class N20MarketEpisodeState:
    id: int
    strategy_id: str
    episode_id: str
    stage: str
    reason: str
    m0_open_time_ms: int
    d1_open_time_ms: int
    c_open_time_ms: int | None
    winner_symbol: str | None
    structure_id: str | None
    terminal_cutoff_time_ms: int | None
    evidence_blob: bytes
    evidence_size: int
    evidence_sha256: str
    created_at: str
    updated_at: str


PAPER_TRADE_VOID_CONFIRMATION = "VOID_RULE_VERSION_INVALIDATED"
PAPER_TRADE_VOID_REASON = "RULE_VERSION_INVALIDATED"


class _N13StateInconsistentError(RuntimeError):
    pass


class _N13EpisodeAlreadyConsumedError(RuntimeError):
    pass


class _N16StateInconsistentError(RuntimeError):
    pass


@dataclass(frozen=True)
class _N16PublishReviewBaseline:
    retention_row: tuple[Any, ...]
    batch_row: tuple[Any, ...]
    batch_snapshot: tuple[Any, ...]
    claim_snapshot: tuple[int, int]
    n16_claims: tuple[tuple[Any, ...], ...]
    review_summary: Any
    publication_baseline: Any


def _type_sensitive_row_equal(left: Any, right: Any) -> bool:
    if type(left) not in (tuple, list) or type(right) not in (tuple, list):
        return type(left) is type(right) and left == right
    if len(left) != len(right):
        return False
    return all(
        type(left_item) is type(right_item) and left_item == right_item
        for left_item, right_item in zip(left, right)
    )


def _n16_typed_snapshot_sha256(value: Any) -> str:
    def encode(item: Any) -> Any:
        if item is None:
            return ["none"]
        if type(item) is int:
            return ["int", str(item)]
        if type(item) is str:
            return ["str", item]
        if type(item) is tuple:
            return ["tuple", [encode(child) for child in item]]
        if type(item) is list:
            return ["list", [encode(child) for child in item]]
        raise RuntimeError("N16 publication snapshot contains an invalid type")

    payload = json.dumps(
        encode(value),
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


_N16_TABLES = (
    "n16_trend_support_states",
    "n16_lifecycle_guard",
    "n16_consumption_seals",
    "n16_first_claim_witness",
    "strategy_lifecycle_installations",
)
_N16_AUTOINCREMENT_TABLES = (
    "n16_trend_support_states",
    "n16_consumption_seals",
)
_N16_GUARD_MUTATION_FUNCTION = "_n16_guard_mutation_authorized"
_N16_CLAIM_CHAIN_ADVANCE_FUNCTION = "_n16_claim_chain_advance"
_COVERAGE_EPOCH_MUTATION_FUNCTION = "_coverage_epoch_mutation_authorized"
_N16_CLAIM_CHAIN_SEED = "0" * 64
_N16_EVENTS_TABLE_SQL = """
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL,
    event_type TEXT NOT NULL,
    symbol TEXT,
    payload_json TEXT NOT NULL
, trade_review_id INTEGER)
""".strip()
_N16_PREINSTALL_EVENTS_TABLE_SQL = """
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL,
    event_type TEXT NOT NULL,
    symbol TEXT,
    payload_json TEXT NOT NULL
)
""".strip()
_N16_SHARED_TABLE_SQL = {
    "trade_reviews": """
        CREATE TABLE trade_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id INTEGER,
            opened_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            quantity TEXT,
            entry_price TEXT,
            stop_loss_price TEXT,
            take_profit_price TEXT,
            amplitude_24h_pct TEXT,
            high_24h_price TEXT,
            low_24h_price TEXT,
            stop_loss_pct TEXT,
            take_profit_pct TEXT,
            risk_amount TEXT,
            target_risk_amount TEXT,
            actual_risk_amount TEXT,
            risk_capped_by_margin INTEGER,
            pretrade_quantity TEXT,
            executed_quantity TEXT,
            final_protected_quantity TEXT,
            post_fill_actual_risk_amount TEXT,
            post_fill_required_margin TEXT,
            reduced_after_fill INTEGER,
            notional_value TEXT,
            required_margin TEXT,
            balance TEXT,
            leverage INTEGER,
            dry_run INTEGER NOT NULL,
            status TEXT NOT NULL,
            error TEXT,
            orders_json TEXT NOT NULL,
            closed_at TEXT,
            exit_reason TEXT,
            exit_price TEXT,
            close_mark_price TEXT,
            realized_pnl TEXT,
            realized_pnl_pct TEXT,
            balance_after_close TEXT,
            FOREIGN KEY(scan_id) REFERENCES scans(id)
        )
    """.strip(),
    "strategy_live_links": """
        CREATE TABLE strategy_live_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id TEXT NOT NULL,
            trade_review_id INTEGER,
            symbol TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            closed_at TEXT,
            result TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(trade_review_id) REFERENCES trade_reviews(id)
        )
    """.strip(),
    "strategy_paper_trades": """
        CREATE TABLE strategy_paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            last_checked_at TEXT,
            closed_at TEXT,
            entry_price TEXT NOT NULL,
            stop_loss_price TEXT NOT NULL,
            take_profit_price TEXT NOT NULL,
            result TEXT NOT NULL,
            exit_reason TEXT,
            r_multiple TEXT,
            funding_rate TEXT NOT NULL,
            orders_json TEXT NOT NULL,
            detail_json TEXT NOT NULL
        )
    """.strip(),
    "strategy_states": """
        CREATE TABLE strategy_states (
            strategy_id TEXT PRIMARY KEY,
            consecutive_wins INTEGER NOT NULL DEFAULT 0,
            paper_trade_count INTEGER NOT NULL DEFAULT 0,
            win_count INTEGER NOT NULL DEFAULT 0,
            loss_count INTEGER NOT NULL DEFAULT 0,
            win_rate TEXT NOT NULL DEFAULT '0',
            live_eligible INTEGER NOT NULL DEFAULT 0,
            live_result_pending INTEGER NOT NULL DEFAULT 0,
            last_trade_result TEXT,
            last_trade_closed_at TEXT,
            updated_at TEXT NOT NULL
        )
    """.strip(),
    "symbol_cooldowns": """
        CREATE TABLE symbol_cooldowns (
            symbol TEXT PRIMARY KEY,
            cooldown_until TEXT NOT NULL,
            reason TEXT NOT NULL,
            source_trade_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """.strip(),
}
_N16_SHARED_TABLE_XINFO = {
    "trade_reviews": (
        ("id", "INTEGER", 0, None, 1, 0),
        ("scan_id", "INTEGER", 0, None, 0, 0),
        ("opened_at", "TEXT", 1, None, 0, 0),
        ("symbol", "TEXT", 1, None, 0, 0),
        ("side", "TEXT", 1, None, 0, 0),
        ("quantity", "TEXT", 0, None, 0, 0),
        ("entry_price", "TEXT", 0, None, 0, 0),
        ("stop_loss_price", "TEXT", 0, None, 0, 0),
        ("take_profit_price", "TEXT", 0, None, 0, 0),
        ("amplitude_24h_pct", "TEXT", 0, None, 0, 0),
        ("high_24h_price", "TEXT", 0, None, 0, 0),
        ("low_24h_price", "TEXT", 0, None, 0, 0),
        ("stop_loss_pct", "TEXT", 0, None, 0, 0),
        ("take_profit_pct", "TEXT", 0, None, 0, 0),
        ("risk_amount", "TEXT", 0, None, 0, 0),
        ("target_risk_amount", "TEXT", 0, None, 0, 0),
        ("actual_risk_amount", "TEXT", 0, None, 0, 0),
        ("risk_capped_by_margin", "INTEGER", 0, None, 0, 0),
        ("pretrade_quantity", "TEXT", 0, None, 0, 0),
        ("executed_quantity", "TEXT", 0, None, 0, 0),
        ("final_protected_quantity", "TEXT", 0, None, 0, 0),
        ("post_fill_actual_risk_amount", "TEXT", 0, None, 0, 0),
        ("post_fill_required_margin", "TEXT", 0, None, 0, 0),
        ("reduced_after_fill", "INTEGER", 0, None, 0, 0),
        ("notional_value", "TEXT", 0, None, 0, 0),
        ("required_margin", "TEXT", 0, None, 0, 0),
        ("balance", "TEXT", 0, None, 0, 0),
        ("leverage", "INTEGER", 0, None, 0, 0),
        ("dry_run", "INTEGER", 1, None, 0, 0),
        ("status", "TEXT", 1, None, 0, 0),
        ("error", "TEXT", 0, None, 0, 0),
        ("orders_json", "TEXT", 1, None, 0, 0),
        ("closed_at", "TEXT", 0, None, 0, 0),
        ("exit_reason", "TEXT", 0, None, 0, 0),
        ("exit_price", "TEXT", 0, None, 0, 0),
        ("close_mark_price", "TEXT", 0, None, 0, 0),
        ("realized_pnl", "TEXT", 0, None, 0, 0),
        ("realized_pnl_pct", "TEXT", 0, None, 0, 0),
        ("balance_after_close", "TEXT", 0, None, 0, 0),
    ),
    "strategy_live_links": (
        ("id", "INTEGER", 0, None, 1, 0),
        ("strategy_id", "TEXT", 1, None, 0, 0),
        ("trade_review_id", "INTEGER", 0, None, 0, 0),
        ("symbol", "TEXT", 1, None, 0, 0),
        ("opened_at", "TEXT", 1, None, 0, 0),
        ("closed_at", "TEXT", 0, None, 0, 0),
        ("result", "TEXT", 0, None, 0, 0),
        ("created_at", "TEXT", 1, None, 0, 0),
    ),
    "strategy_paper_trades": (
        ("id", "INTEGER", 0, None, 1, 0),
        ("strategy_id", "TEXT", 1, None, 0, 0),
        ("symbol", "TEXT", 1, None, 0, 0),
        ("opened_at", "TEXT", 1, None, 0, 0),
        ("last_checked_at", "TEXT", 0, None, 0, 0),
        ("closed_at", "TEXT", 0, None, 0, 0),
        ("entry_price", "TEXT", 1, None, 0, 0),
        ("stop_loss_price", "TEXT", 1, None, 0, 0),
        ("take_profit_price", "TEXT", 1, None, 0, 0),
        ("result", "TEXT", 1, None, 0, 0),
        ("exit_reason", "TEXT", 0, None, 0, 0),
        ("r_multiple", "TEXT", 0, None, 0, 0),
        ("funding_rate", "TEXT", 1, None, 0, 0),
        ("orders_json", "TEXT", 1, None, 0, 0),
        ("detail_json", "TEXT", 1, None, 0, 0),
    ),
    "strategy_states": (
        ("strategy_id", "TEXT", 0, None, 1, 0),
        ("consecutive_wins", "INTEGER", 1, "0", 0, 0),
        ("paper_trade_count", "INTEGER", 1, "0", 0, 0),
        ("win_count", "INTEGER", 1, "0", 0, 0),
        ("loss_count", "INTEGER", 1, "0", 0, 0),
        ("win_rate", "TEXT", 1, "'0'", 0, 0),
        ("live_eligible", "INTEGER", 1, "0", 0, 0),
        ("live_result_pending", "INTEGER", 1, "0", 0, 0),
        ("last_trade_result", "TEXT", 0, None, 0, 0),
        ("last_trade_closed_at", "TEXT", 0, None, 0, 0),
        ("updated_at", "TEXT", 1, None, 0, 0),
    ),
    "symbol_cooldowns": (
        ("symbol", "TEXT", 0, None, 1, 0),
        ("cooldown_until", "TEXT", 1, None, 0, 0),
        ("reason", "TEXT", 1, None, 0, 0),
        ("source_trade_id", "INTEGER", 0, None, 0, 0),
        ("created_at", "TEXT", 1, None, 0, 0),
        ("updated_at", "TEXT", 1, None, 0, 0),
    ),
}
_N16_MIGRATED_TRADE_REVIEWS_TABLE_SQL = """
    CREATE TABLE trade_reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_id INTEGER,
        opened_at TEXT NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        quantity TEXT,
        entry_price TEXT,
        stop_loss_price TEXT,
        take_profit_price TEXT,
        amplitude_24h_pct TEXT,
        high_24h_price TEXT,
        low_24h_price TEXT,
        stop_loss_pct TEXT,
        take_profit_pct TEXT,
        risk_amount TEXT,
        notional_value TEXT,
        required_margin TEXT,
        balance TEXT,
        leverage INTEGER,
        dry_run INTEGER NOT NULL,
        status TEXT NOT NULL,
        error TEXT,
        orders_json TEXT NOT NULL, target_risk_amount TEXT,
        actual_risk_amount TEXT,
        risk_capped_by_margin INTEGER,
        pretrade_quantity TEXT,
        executed_quantity TEXT,
        final_protected_quantity TEXT,
        post_fill_actual_risk_amount TEXT,
        post_fill_required_margin TEXT,
        reduced_after_fill INTEGER,
        closed_at TEXT,
        exit_reason TEXT,
        exit_price TEXT,
        close_mark_price TEXT,
        realized_pnl TEXT,
        realized_pnl_pct TEXT,
        balance_after_close TEXT,
        FOREIGN KEY(scan_id) REFERENCES scans(id)
    )
""".strip()
_N16_MIGRATED_TRADE_REVIEWS_XINFO = (
    ("id", "INTEGER", 0, None, 1, 0),
    ("scan_id", "INTEGER", 0, None, 0, 0),
    ("opened_at", "TEXT", 1, None, 0, 0),
    ("symbol", "TEXT", 1, None, 0, 0),
    ("side", "TEXT", 1, None, 0, 0),
    ("quantity", "TEXT", 0, None, 0, 0),
    ("entry_price", "TEXT", 0, None, 0, 0),
    ("stop_loss_price", "TEXT", 0, None, 0, 0),
    ("take_profit_price", "TEXT", 0, None, 0, 0),
    ("amplitude_24h_pct", "TEXT", 0, None, 0, 0),
    ("high_24h_price", "TEXT", 0, None, 0, 0),
    ("low_24h_price", "TEXT", 0, None, 0, 0),
    ("stop_loss_pct", "TEXT", 0, None, 0, 0),
    ("take_profit_pct", "TEXT", 0, None, 0, 0),
    ("risk_amount", "TEXT", 0, None, 0, 0),
    ("notional_value", "TEXT", 0, None, 0, 0),
    ("required_margin", "TEXT", 0, None, 0, 0),
    ("balance", "TEXT", 0, None, 0, 0),
    ("leverage", "INTEGER", 0, None, 0, 0),
    ("dry_run", "INTEGER", 1, None, 0, 0),
    ("status", "TEXT", 1, None, 0, 0),
    ("error", "TEXT", 0, None, 0, 0),
    ("orders_json", "TEXT", 1, None, 0, 0),
    ("target_risk_amount", "TEXT", 0, None, 0, 0),
    ("actual_risk_amount", "TEXT", 0, None, 0, 0),
    ("risk_capped_by_margin", "INTEGER", 0, None, 0, 0),
    ("pretrade_quantity", "TEXT", 0, None, 0, 0),
    ("executed_quantity", "TEXT", 0, None, 0, 0),
    ("final_protected_quantity", "TEXT", 0, None, 0, 0),
    ("post_fill_actual_risk_amount", "TEXT", 0, None, 0, 0),
    ("post_fill_required_margin", "TEXT", 0, None, 0, 0),
    ("reduced_after_fill", "INTEGER", 0, None, 0, 0),
    ("closed_at", "TEXT", 0, None, 0, 0),
    ("exit_reason", "TEXT", 0, None, 0, 0),
    ("exit_price", "TEXT", 0, None, 0, 0),
    ("close_mark_price", "TEXT", 0, None, 0, 0),
    ("realized_pnl", "TEXT", 0, None, 0, 0),
    ("realized_pnl_pct", "TEXT", 0, None, 0, 0),
    ("balance_after_close", "TEXT", 0, None, 0, 0),
)
_N16_LEGACY_PAPER_TABLE_SQL = """
    CREATE TABLE strategy_paper_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        opened_at TEXT NOT NULL,
        closed_at TEXT,
        entry_price TEXT NOT NULL,
        stop_loss_price TEXT NOT NULL,
        take_profit_price TEXT NOT NULL,
        result TEXT NOT NULL,
        exit_reason TEXT,
        r_multiple TEXT,
        funding_rate TEXT NOT NULL,
        orders_json TEXT NOT NULL,
        detail_json TEXT NOT NULL,
        last_checked_at TEXT
    )
""".strip()
_N16_MIGRATED_PAPER_TABLE_SQL = """
    CREATE TABLE strategy_paper_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        opened_at TEXT NOT NULL,
        closed_at TEXT,
        entry_price TEXT NOT NULL,
        stop_loss_price TEXT NOT NULL,
        take_profit_price TEXT NOT NULL,
        result TEXT NOT NULL,
        exit_reason TEXT,
        r_multiple TEXT,
        funding_rate TEXT NOT NULL,
        orders_json TEXT NOT NULL,
        detail_json TEXT NOT NULL
    , last_checked_at TEXT)
""".strip()
_N16_PRE_LASTCHECK_PAPER_TABLE_SQL = """
    CREATE TABLE strategy_paper_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        opened_at TEXT NOT NULL,
        closed_at TEXT,
        entry_price TEXT NOT NULL,
        stop_loss_price TEXT NOT NULL,
        take_profit_price TEXT NOT NULL,
        result TEXT NOT NULL,
        exit_reason TEXT,
        r_multiple TEXT,
        funding_rate TEXT NOT NULL,
        orders_json TEXT NOT NULL,
        detail_json TEXT NOT NULL
    )
""".strip()
_N16_LEGACY_PAPER_TABLE_XINFO = (
    ("id", "INTEGER", 0, None, 1, 0),
    ("strategy_id", "TEXT", 1, None, 0, 0),
    ("symbol", "TEXT", 1, None, 0, 0),
    ("opened_at", "TEXT", 1, None, 0, 0),
    ("closed_at", "TEXT", 0, None, 0, 0),
    ("entry_price", "TEXT", 1, None, 0, 0),
    ("stop_loss_price", "TEXT", 1, None, 0, 0),
    ("take_profit_price", "TEXT", 1, None, 0, 0),
    ("result", "TEXT", 1, None, 0, 0),
    ("exit_reason", "TEXT", 0, None, 0, 0),
    ("r_multiple", "TEXT", 0, None, 0, 0),
    ("funding_rate", "TEXT", 1, None, 0, 0),
    ("orders_json", "TEXT", 1, None, 0, 0),
    ("detail_json", "TEXT", 1, None, 0, 0),
    ("last_checked_at", "TEXT", 0, None, 0, 0),
)
_N16_PRE_LASTCHECK_PAPER_TABLE_XINFO = (
    _N16_LEGACY_PAPER_TABLE_XINFO[:-1]
)
_N16_LEGACY_STRATEGY_STATES_TABLE_SQL = """
    CREATE TABLE strategy_states (
        strategy_id TEXT PRIMARY KEY,
        consecutive_wins INTEGER NOT NULL DEFAULT 0,
        paper_trade_count INTEGER NOT NULL DEFAULT 0,
        win_count INTEGER NOT NULL DEFAULT 0,
        loss_count INTEGER NOT NULL DEFAULT 0,
        win_rate TEXT NOT NULL DEFAULT '0',
        live_eligible INTEGER NOT NULL DEFAULT 0,
        last_trade_result TEXT,
        last_trade_closed_at TEXT,
        updated_at TEXT NOT NULL
    )
""".strip()
_N16_LEGACY_STRATEGY_STATES_XINFO = (
    ("strategy_id", "TEXT", 0, None, 1, 0),
    ("consecutive_wins", "INTEGER", 1, "0", 0, 0),
    ("paper_trade_count", "INTEGER", 1, "0", 0, 0),
    ("win_count", "INTEGER", 1, "0", 0, 0),
    ("loss_count", "INTEGER", 1, "0", 0, 0),
    ("win_rate", "TEXT", 1, "'0'", 0, 0),
    ("live_eligible", "INTEGER", 1, "0", 0, 0),
    ("last_trade_result", "TEXT", 0, None, 0, 0),
    ("last_trade_closed_at", "TEXT", 0, None, 0, 0),
    ("updated_at", "TEXT", 1, None, 0, 0),
)
_N16_MIGRATED_STRATEGY_STATES_TABLE_SQL = """
    CREATE TABLE strategy_states (
        strategy_id TEXT PRIMARY KEY,
        consecutive_wins INTEGER NOT NULL DEFAULT 0,
        paper_trade_count INTEGER NOT NULL DEFAULT 0,
        win_count INTEGER NOT NULL DEFAULT 0,
        loss_count INTEGER NOT NULL DEFAULT 0,
        win_rate TEXT NOT NULL DEFAULT '0',
        live_eligible INTEGER NOT NULL DEFAULT 0,
        last_trade_result TEXT,
        last_trade_closed_at TEXT,
        updated_at TEXT NOT NULL
    , live_result_pending INTEGER NOT NULL DEFAULT 0)
""".strip()
_N16_MIGRATED_STRATEGY_STATES_XINFO = (
    _N16_LEGACY_STRATEGY_STATES_XINFO
    + (("live_result_pending", "INTEGER", 1, "0", 0, 0),)
)
_N16_SHARED_TABLE_FOREIGN_KEYS = {
    "trade_reviews": (
        (0, 0, "scans", "scan_id", "id", "NO ACTION", "NO ACTION", "NONE"),
    ),
    "strategy_live_links": (
        (
            0,
            0,
            "trade_reviews",
            "trade_review_id",
            "id",
            "NO ACTION",
            "NO ACTION",
            "NONE",
        ),
    ),
    "strategy_paper_trades": (),
    "strategy_states": (),
    "symbol_cooldowns": (),
}
_N16_STATE_TABLE_SQL = """
CREATE TABLE n16_trend_support_states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N16'),
    symbol TEXT NOT NULL CHECK(length(symbol) BETWEEN 1 AND 64),
    episode_id TEXT NOT NULL CHECK(
        length(episode_id) = 24
        AND episode_id NOT GLOB '*[^0-9a-f]*'
    ),
    structure_id TEXT CHECK(
        structure_id IS NULL OR (
            length(structure_id) = 24
            AND structure_id NOT GLOB '*[^0-9a-f]*'
        )
    ),
    stage TEXT NOT NULL CHECK(stage IN (
        'TOUCH_LOCKED', 'CONFIRMING', 'CONFIRMED',
        'MISSED', 'INVALID', 'EXPIRED'
    )),
    reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 128),
    quote_volume_rank INTEGER NOT NULL CHECK(
        typeof(quote_volume_rank) = 'integer'
        AND quote_volume_rank BETWEEN 1 AND 100
    ),
    evidence_json TEXT NOT NULL CHECK(
        length(CAST(evidence_json AS BLOB)) < 131072
    ),
    evidence_sha256 TEXT NOT NULL CHECK(
        length(evidence_sha256) = 64
        AND evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(strategy_id, episode_id)
)
""".strip()
_N16_GUARD_TABLE_SQL = """
CREATE TABLE n16_lifecycle_guard (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    schema_version INTEGER NOT NULL CHECK(
        typeof(schema_version) = 'integer' AND schema_version = 1
    ),
    rule_version TEXT NOT NULL CHECK(rule_version = 'N16_V1'),
    guard_sha256 TEXT NOT NULL CHECK(
        guard_sha256 = 'd262585be43bf3dc7137d98e84770d672b8d82d40c369724a1ac70e211b52332'
    ),
    catalog_schema_version INTEGER NOT NULL CHECK(
        typeof(catalog_schema_version) = 'integer'
        AND catalog_schema_version > 0
    ),
    sealed_claim_count INTEGER NOT NULL CHECK(
        typeof(sealed_claim_count) = 'integer'
        AND sealed_claim_count >= 0
    ),
    confirmed_chain_sha256 TEXT NOT NULL CHECK(
        length(confirmed_chain_sha256) = 64
        AND confirmed_chain_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    CHECK(
        (sealed_claim_count = 0 AND confirmed_chain_sha256 =
        '0000000000000000000000000000000000000000000000000000000000000000')
        OR (sealed_claim_count > 0 AND confirmed_chain_sha256 !=
        '0000000000000000000000000000000000000000000000000000000000000000')
    )
)
""".strip()
_N16_ROOT_TABLE_SQL = """
CREATE TABLE strategy_lifecycle_installations (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N16'),
    schema_version INTEGER NOT NULL CHECK(
        typeof(schema_version) = 'integer' AND schema_version = 1
    ),
    rule_version TEXT NOT NULL CHECK(rule_version = 'N16_V1'),
    guard_sha256 TEXT NOT NULL CHECK(
        guard_sha256 = 'd262585be43bf3dc7137d98e84770d672b8d82d40c369724a1ac70e211b52332'
    ),
    catalog_schema_version INTEGER NOT NULL CHECK(
        typeof(catalog_schema_version) = 'integer'
        AND catalog_schema_version > 0
    ),
    sealed_claim_count INTEGER NOT NULL CHECK(
        typeof(sealed_claim_count) = 'integer'
        AND sealed_claim_count >= 0
    ),
    confirmed_chain_sha256 TEXT NOT NULL CHECK(
        length(confirmed_chain_sha256) = 64
        AND confirmed_chain_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    installed_at TEXT NOT NULL CHECK(length(installed_at) BETWEEN 1 AND 64),
    CHECK(
        (sealed_claim_count = 0 AND confirmed_chain_sha256 =
        '0000000000000000000000000000000000000000000000000000000000000000')
        OR (sealed_claim_count > 0 AND confirmed_chain_sha256 !=
        '0000000000000000000000000000000000000000000000000000000000000000')
    )
)
""".strip()
_N16_GUARD_FIXED_ROW = (
    1,
    1,
    "N16_V1",
    "d262585be43bf3dc7137d98e84770d672b8d82d40c369724a1ac70e211b52332",
)
_N16_SEAL_TABLE_SQL = """
CREATE TABLE n16_consumption_seals (
    seal_ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
    source_signal_id INTEGER NOT NULL UNIQUE CHECK(
        typeof(source_signal_id) = 'integer' AND source_signal_id > 0
    ),
    schema_version INTEGER NOT NULL CHECK(
        typeof(schema_version) = 'integer' AND schema_version = 1
    ),
    rule_version TEXT NOT NULL CHECK(rule_version = 'N16_V1'),
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N16'),
    source_scan_id INTEGER NOT NULL CHECK(
        typeof(source_scan_id) = 'integer' AND source_scan_id > 0
    ),
    audit_id INTEGER NOT NULL UNIQUE CHECK(
        typeof(audit_id) = 'integer' AND audit_id > 0
    ),
    ledger_id INTEGER NOT NULL UNIQUE CHECK(
        typeof(ledger_id) = 'integer' AND ledger_id > 0
    ),
    state_id INTEGER NOT NULL UNIQUE CHECK(
        typeof(state_id) = 'integer' AND state_id > 0
    ),
    symbol TEXT NOT NULL CHECK(length(symbol) BETWEEN 1 AND 64),
    episode_id TEXT NOT NULL UNIQUE CHECK(
        length(episode_id) = 24
        AND episode_id NOT GLOB '*[^0-9a-f]*'
    ),
    structure_id TEXT NOT NULL UNIQUE CHECK(
        length(structure_id) = 24
        AND structure_id NOT GLOB '*[^0-9a-f]*'
    ),
    signal_evidence_sha256 TEXT NOT NULL CHECK(
        length(signal_evidence_sha256) = 64
        AND signal_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    state_evidence_sha256 TEXT NOT NULL CHECK(
        length(state_evidence_sha256) = 64
        AND state_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    signal_created_at TEXT NOT NULL,
    claim_created_at TEXT NOT NULL
)
""".strip()
_N16_WITNESS_TABLE_SQL = """
CREATE TABLE n16_first_claim_witness (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    seal_ordinal INTEGER NOT NULL CHECK(
        typeof(seal_ordinal) = 'integer' AND seal_ordinal = 1
    ),
    source_signal_id INTEGER NOT NULL CHECK(
        typeof(source_signal_id) = 'integer' AND source_signal_id > 0
    ),
    schema_version INTEGER NOT NULL CHECK(
        typeof(schema_version) = 'integer' AND schema_version = 1
    ),
    rule_version TEXT NOT NULL CHECK(rule_version = 'N16_V1'),
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N16'),
    source_scan_id INTEGER NOT NULL CHECK(
        typeof(source_scan_id) = 'integer' AND source_scan_id > 0
    ),
    audit_id INTEGER NOT NULL CHECK(
        typeof(audit_id) = 'integer' AND audit_id > 0
    ),
    ledger_id INTEGER NOT NULL CHECK(
        typeof(ledger_id) = 'integer' AND ledger_id > 0
    ),
    state_id INTEGER NOT NULL CHECK(
        typeof(state_id) = 'integer' AND state_id > 0
    ),
    symbol TEXT NOT NULL CHECK(length(symbol) BETWEEN 1 AND 64),
    episode_id TEXT NOT NULL CHECK(
        length(episode_id) = 24
        AND episode_id NOT GLOB '*[^0-9a-f]*'
    ),
    structure_id TEXT NOT NULL CHECK(
        length(structure_id) = 24
        AND structure_id NOT GLOB '*[^0-9a-f]*'
    ),
    signal_evidence_sha256 TEXT NOT NULL CHECK(
        length(signal_evidence_sha256) = 64
        AND signal_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    state_evidence_sha256 TEXT NOT NULL CHECK(
        length(state_evidence_sha256) = 64
        AND state_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    signal_created_at TEXT NOT NULL,
    claim_created_at TEXT NOT NULL
)
""".strip()
_N16_INDEX_SQL = {
    "idx_n16_trend_support_active": (
        "CREATE INDEX idx_n16_trend_support_active "
        "ON n16_trend_support_states(strategy_id, stage, symbol)"
    ),
    "idx_n16_trend_support_structure": (
        "CREATE UNIQUE INDEX idx_n16_trend_support_structure "
        "ON n16_trend_support_states(strategy_id, structure_id) "
        "WHERE structure_id IS NOT NULL"
    ),
    "idx_n16_live_link_state_key": (
        "CREATE INDEX idx_n16_live_link_state_key "
        "ON strategy_live_links(symbol, opened_at)"
    ),
    "idx_n16_live_link_review": (
        "CREATE INDEX idx_n16_live_link_review "
        "ON strategy_live_links(trade_review_id)"
    ),
    "idx_n16_trade_review_state_key": (
        "CREATE INDEX idx_n16_trade_review_state_key "
        "ON trade_reviews(symbol, opened_at)"
    ),
    "idx_n16_review_pending_rows": (
        "CREATE INDEX idx_n16_review_pending_rows "
        "ON trade_reviews(id) WHERE status IN ("
        "'OPENED', 'CLOSED_LIVE_RESULT_PENDING')"
    ),
    "idx_n16_paper_open_rows": (
        "CREATE INDEX idx_n16_paper_open_rows "
        "ON strategy_paper_trades(id) WHERE result = 'OPEN'"
    ),
    "idx_n16_live_open_rows": (
        "CREATE INDEX idx_n16_live_open_rows "
        "ON strategy_live_links(id) WHERE closed_at IS NULL"
    ),
    "idx_n16_terminal_event_trade": (
        "CREATE UNIQUE INDEX idx_n16_terminal_event_trade "
        "ON events(trade_review_id) WHERE event_type IN ("
        "'n16_dry_run_position_closed', 'n16_live_position_closed')"
    ),
}
_N16_SHARED_INDEX_SQL = {
    "trade_reviews": {
        "idx_trade_reviews_symbol_time": (
            "CREATE INDEX idx_trade_reviews_symbol_time "
            "ON trade_reviews(symbol, opened_at)"
        ),
        "idx_n16_trade_review_state_key": _N16_INDEX_SQL[
            "idx_n16_trade_review_state_key"
        ],
        "idx_n16_review_pending_rows": _N16_INDEX_SQL[
            "idx_n16_review_pending_rows"
        ],
    },
    "strategy_live_links": {
        "idx_strategy_live_result": (
            "CREATE INDEX idx_strategy_live_result "
            "ON strategy_live_links(strategy_id, symbol, result, closed_at)"
        ),
        "idx_n16_live_link_state_key": _N16_INDEX_SQL[
            "idx_n16_live_link_state_key"
        ],
        "idx_n16_live_link_review": _N16_INDEX_SQL[
            "idx_n16_live_link_review"
        ],
        "idx_n16_live_open_rows": _N16_INDEX_SQL[
            "idx_n16_live_open_rows"
        ],
    },
    "strategy_paper_trades": {
        "idx_strategy_paper_open": (
            "CREATE INDEX idx_strategy_paper_open "
            "ON strategy_paper_trades(strategy_id, result)"
        ),
        "idx_strategy_paper_symbol_result": (
            "CREATE INDEX idx_strategy_paper_symbol_result "
            "ON strategy_paper_trades(strategy_id, symbol, result, closed_at)"
        ),
        "idx_n16_paper_open_rows": _N16_INDEX_SQL[
            "idx_n16_paper_open_rows"
        ],
    },
    "strategy_states": {
        "sqlite_autoindex_strategy_states_1": None,
    },
    "symbol_cooldowns": {
        "sqlite_autoindex_symbol_cooldowns_1": None,
        "idx_symbol_cooldowns_until": (
            "CREATE INDEX idx_symbol_cooldowns_until "
            "ON symbol_cooldowns(cooldown_until)"
        ),
    },
}
_N16_SHARED_INDEX_METADATA = {
    "idx_trade_reviews_symbol_time": (
        (0, "c", 0),
        (
            (0, 3, "symbol", 0, "BINARY", 1),
            (1, 2, "opened_at", 0, "BINARY", 1),
            (2, -1, None, 0, "BINARY", 0),
        ),
    ),
    "idx_n16_trade_review_state_key": (
        (0, "c", 0),
        (
            (0, 3, "symbol", 0, "BINARY", 1),
            (1, 2, "opened_at", 0, "BINARY", 1),
            (2, -1, None, 0, "BINARY", 0),
        ),
    ),
    "idx_n16_review_pending_rows": (
        (0, "c", 1),
        (
            (0, 0, "id", 0, "BINARY", 1),
            (1, -1, None, 0, "BINARY", 0),
        ),
    ),
    "idx_strategy_live_result": (
        (0, "c", 0),
        (
            (0, 1, "strategy_id", 0, "BINARY", 1),
            (1, 3, "symbol", 0, "BINARY", 1),
            (2, 6, "result", 0, "BINARY", 1),
            (3, 5, "closed_at", 0, "BINARY", 1),
            (4, -1, None, 0, "BINARY", 0),
        ),
    ),
    "idx_n16_live_link_state_key": (
        (0, "c", 0),
        (
            (0, 3, "symbol", 0, "BINARY", 1),
            (1, 4, "opened_at", 0, "BINARY", 1),
            (2, -1, None, 0, "BINARY", 0),
        ),
    ),
    "idx_n16_live_link_review": (
        (0, "c", 0),
        (
            (0, 2, "trade_review_id", 0, "BINARY", 1),
            (1, -1, None, 0, "BINARY", 0),
        ),
    ),
    "idx_n16_live_open_rows": (
        (0, "c", 1),
        (
            (0, 0, "id", 0, "BINARY", 1),
            (1, -1, None, 0, "BINARY", 0),
        ),
    ),
    "idx_strategy_paper_open": (
        (0, "c", 0),
        (
            (0, 1, "strategy_id", 0, "BINARY", 1),
            (1, 9, "result", 0, "BINARY", 1),
            (2, -1, None, 0, "BINARY", 0),
        ),
    ),
    "idx_strategy_paper_symbol_result": (
        (0, "c", 0),
        (
            (0, 1, "strategy_id", 0, "BINARY", 1),
            (1, 2, "symbol", 0, "BINARY", 1),
            (2, 9, "result", 0, "BINARY", 1),
            (3, 5, "closed_at", 0, "BINARY", 1),
            (4, -1, None, 0, "BINARY", 0),
        ),
    ),
    "idx_n16_paper_open_rows": (
        (0, "c", 1),
        (
            (0, 0, "id", 0, "BINARY", 1),
            (1, -1, None, 0, "BINARY", 0),
        ),
    ),
    "sqlite_autoindex_strategy_states_1": (
        (1, "pk", 0),
        (
            (0, 0, "strategy_id", 0, "BINARY", 1),
            (1, -1, None, 0, "BINARY", 0),
        ),
    ),
    "sqlite_autoindex_symbol_cooldowns_1": (
        (1, "pk", 0),
        (
            (0, 0, "symbol", 0, "BINARY", 1),
            (1, -1, None, 0, "BINARY", 0),
        ),
    ),
    "idx_symbol_cooldowns_until": (
        (0, "c", 0),
        (
            (0, 1, "cooldown_until", 0, "BINARY", 1),
            (1, -1, None, 0, "BINARY", 0),
        ),
    ),
}
_N16_LEGACY_PAPER_INDEX_XINFO = {
    "idx_strategy_paper_open": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 8, "result", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
    "idx_strategy_paper_symbol_result": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 2, "symbol", 0, "BINARY", 1),
        (2, 8, "result", 0, "BINARY", 1),
        (3, 4, "closed_at", 0, "BINARY", 1),
        (4, -1, None, 0, "BINARY", 0),
    ),
    "idx_n16_paper_open_rows": (
        (0, 0, "id", 0, "BINARY", 1),
        (1, -1, None, 0, "BINARY", 0),
    ),
}
_N16_EVENT_INDEX_SQL = {
    "idx_events_type_time": (
        "CREATE INDEX idx_events_type_time "
        "ON events(event_type, occurred_at)"
    ),
    "idx_n16_terminal_event_trade": _N16_INDEX_SQL[
        "idx_n16_terminal_event_trade"
    ],
}
_N16_TRIGGER_SQL = {
    "trg_n16_terminal_event_identity": """
        CREATE TRIGGER trg_n16_terminal_event_identity
        BEFORE INSERT ON events
        WHEN EXISTS (
            SELECT 1 FROM events AS existing
            WHERE existing.id = NEW.id
              AND (
                  existing.event_type IN (
                      'n16_dry_run_position_closed',
                      'n16_live_position_closed'
                  )
                  OR existing.trade_review_id IS NOT NULL
              )
        ) OR (
            NEW.event_type IN (
                'n16_dry_run_position_closed', 'n16_live_position_closed'
            )
            AND (
                typeof(NEW.trade_review_id) != 'integer'
                OR NEW.trade_review_id <= 0
                OR typeof(NEW.symbol) != 'text'
                OR length(NEW.symbol) NOT BETWEEN 1 AND 64
                OR json_valid(NEW.payload_json) != 1
                OR json_type(NEW.payload_json) != 'object'
                OR (SELECT COUNT(*) FROM json_each(NEW.payload_json))
                    NOT IN (7, 8)
                OR (SELECT COUNT(*) FROM json_each(NEW.payload_json)) !=
                   (SELECT COUNT(DISTINCT key) FROM json_each(NEW.payload_json))
                OR EXISTS (
                    SELECT 1
                    FROM json_tree(NEW.payload_json)
                    WHERE parent IS NOT NULL AND key IS NOT NULL
                    GROUP BY parent, key
                    HAVING COUNT(*) != 1
                )
                OR EXISTS (
                    SELECT 1 FROM json_each(NEW.payload_json)
                    WHERE key NOT IN (
                        'trade_id', 'exit_reason', 'exit_price', 'mark_price',
                        'pnl_amount', 'pnl_pct', 'balance_after',
                        'resolution_detail'
                    )
                )
                OR json_type(NEW.payload_json, '$.trade_id') IS NOT 'integer'
                OR json_extract(NEW.payload_json, '$.trade_id')
                    != NEW.trade_review_id
                OR json_type(NEW.payload_json, '$.exit_reason') IS NOT 'text'
                OR json_type(NEW.payload_json, '$.exit_price') IS NOT 'text'
                OR json_type(NEW.payload_json, '$.mark_price') IS NOT 'text'
                OR json_type(NEW.payload_json, '$.pnl_amount') IS NOT 'text'
                OR json_type(NEW.payload_json, '$.pnl_pct') IS NOT 'text'
                OR json_type(NEW.payload_json, '$.balance_after') IS NOT 'text'
                OR (
                    json_type(NEW.payload_json, '$.resolution_detail') IS NOT NULL
                    AND json_type(
                        NEW.payload_json, '$.resolution_detail'
                    ) != 'object'
                )
                OR NOT EXISTS (
                    SELECT 1
                    FROM trade_reviews AS review
                    JOIN strategy_live_links AS live
                      ON live.trade_review_id = review.id
                    JOIN n16_consumption_seals AS seal
                      ON seal.source_signal_id = COALESCE(
                          json_extract(
                              review.orders_json, '$.strategy.signal_id'
                          ),
                          json_extract(
                              review.orders_json,
                              '$.state_orders.strategy.signal_id'
                          )
                      )
                     AND seal.structure_id = COALESCE(
                          json_extract(
                              review.orders_json, '$.strategy.structure_id'
                          ),
                          json_extract(
                              review.orders_json,
                              '$.state_orders.strategy.structure_id'
                          )
                      )
                     AND seal.symbol = review.symbol
                    WHERE review.id = NEW.trade_review_id
                      AND review.symbol = NEW.symbol
                      AND review.closed_at = NEW.occurred_at
                      AND review.status = 'CLOSED_' ||
                          json_extract(NEW.payload_json, '$.exit_reason')
                      AND review.exit_reason =
                          json_extract(NEW.payload_json, '$.exit_reason')
                      AND review.exit_price =
                          json_extract(NEW.payload_json, '$.exit_price')
                      AND review.close_mark_price =
                          json_extract(NEW.payload_json, '$.mark_price')
                      AND review.realized_pnl =
                          json_extract(NEW.payload_json, '$.pnl_amount')
                      AND review.realized_pnl_pct =
                          json_extract(NEW.payload_json, '$.pnl_pct')
                      AND review.balance_after_close =
                          json_extract(NEW.payload_json, '$.balance_after')
                      AND json_type(review.orders_json, '$.close') = 'object'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM json_tree(review.orders_json)
                          WHERE parent IS NOT NULL AND key IS NOT NULL
                          GROUP BY parent, key
                          HAVING COUNT(*) != 1
                      )
                      AND json(json_remove(NEW.payload_json, '$.trade_id')) =
                          json(json_extract(review.orders_json, '$.close'))
                      AND review.dry_run = CASE NEW.event_type
                          WHEN 'n16_dry_run_position_closed' THEN 1 ELSE 0 END
                      AND json_valid(review.orders_json) = 1
                      AND COALESCE(
                          json_extract(
                              review.orders_json, '$.strategy.strategy_id'
                          ),
                          json_extract(
                              review.orders_json,
                              '$.state_orders.strategy.strategy_id'
                          )
                      ) = 'N16'
                      AND live.strategy_id = 'N16'
                      AND live.symbol = review.symbol
                      AND live.opened_at = review.opened_at
                      AND (
                          (live.closed_at IS NULL AND live.result IS NULL)
                          OR (
                              live.closed_at = NEW.occurred_at
                              AND live.closed_at = review.closed_at
                              AND live.result = CASE json_extract(
                                  NEW.payload_json, '$.exit_reason'
                              )
                                  WHEN 'TAKE_PROFIT' THEN 'WIN'
                                  WHEN 'STOP_LOSS' THEN 'LOSS'
                                  ELSE NULL
                              END
                          )
                      )
                )
            )
        ) OR (
            NEW.event_type NOT IN (
                'n16_dry_run_position_closed', 'n16_live_position_closed'
            )
            AND NEW.trade_review_id IS NOT NULL
        )
        BEGIN SELECT RAISE(ABORT, 'terminal event identity is invalid'); END
    """.strip(),
    "trg_n16_terminal_event_identity_update": """
        CREATE TRIGGER trg_n16_terminal_event_identity_update
        BEFORE UPDATE ON events
        WHEN OLD.event_type IN (
            'n16_dry_run_position_closed', 'n16_live_position_closed'
        ) OR OLD.trade_review_id IS NOT NULL
        OR NEW.event_type IN (
            'n16_dry_run_position_closed', 'n16_live_position_closed'
        ) OR NEW.trade_review_id IS NOT NULL
        OR (
            NEW.id IS NOT OLD.id
            AND EXISTS (
                SELECT 1 FROM events AS existing
                WHERE existing.id = NEW.id
                  AND existing.id != OLD.id
                  AND (
                      existing.event_type IN (
                          'n16_dry_run_position_closed',
                          'n16_live_position_closed'
                      )
                      OR existing.trade_review_id IS NOT NULL
                  )
            )
        )
        BEGIN SELECT RAISE(ABORT, 'N16 terminal event is immutable'); END
    """.strip(),
    "trg_n16_terminal_event_identity_delete": """
        CREATE TRIGGER trg_n16_terminal_event_identity_delete
        BEFORE DELETE ON events
        WHEN OLD.event_type IN (
            'n16_dry_run_position_closed', 'n16_live_position_closed'
        ) OR OLD.trade_review_id IS NOT NULL
        BEGIN SELECT RAISE(ABORT, 'N16 terminal event is immutable'); END
    """.strip(),
    "trg_n16_root_no_replace": """
        CREATE TRIGGER trg_n16_root_no_replace
        BEFORE INSERT ON strategy_lifecycle_installations
        WHEN EXISTS (
            SELECT 1 FROM strategy_lifecycle_installations
            WHERE singleton_id = NEW.singleton_id OR strategy_id = NEW.strategy_id
        )
        BEGIN SELECT RAISE(ABORT, 'N16 lifecycle root replacement is forbidden'); END
    """.strip(),
    "trg_n16_root_no_update": """
        CREATE TRIGGER trg_n16_root_no_update
        BEFORE UPDATE ON strategy_lifecycle_installations
        WHEN NOT (
            OLD.singleton_id = 1
            AND OLD.strategy_id = 'N16'
            AND OLD.schema_version = 1
            AND OLD.rule_version = 'N16_V1'
            AND OLD.guard_sha256 =
                'd262585be43bf3dc7137d98e84770d672b8d82d40c369724a1ac70e211b52332'
            AND NEW.singleton_id IS OLD.singleton_id
            AND NEW.strategy_id IS OLD.strategy_id
            AND NEW.schema_version IS OLD.schema_version
            AND NEW.rule_version IS OLD.rule_version
            AND NEW.guard_sha256 IS OLD.guard_sha256
            AND NEW.installed_at IS OLD.installed_at
            AND (
                (
                    NEW.catalog_schema_version IS OLD.catalog_schema_version
                    AND NEW.sealed_claim_count = OLD.sealed_claim_count + 1
                    AND NEW.sealed_claim_count = (
                        SELECT sealed_claim_count FROM n16_lifecycle_guard
                        WHERE singleton_id = 1
                    )
                    AND NEW.confirmed_chain_sha256 = (
                        SELECT confirmed_chain_sha256
                        FROM n16_lifecycle_guard WHERE singleton_id = 1
                    )
                    AND _n16_guard_mutation_authorized(
                        'seal', NEW.catalog_schema_version,
                        NEW.sealed_claim_count
                    ) = 1
                )
                OR (
                    NEW.sealed_claim_count IS OLD.sealed_claim_count
                    AND NEW.confirmed_chain_sha256
                        IS OLD.confirmed_chain_sha256
                    AND NEW.catalog_schema_version != OLD.catalog_schema_version
                    AND NEW.catalog_schema_version = (
                        SELECT catalog_schema_version FROM n16_lifecycle_guard
                        WHERE singleton_id = 1
                    )
                    AND NEW.sealed_claim_count = (
                        SELECT sealed_claim_count FROM n16_lifecycle_guard
                        WHERE singleton_id = 1
                    )
                    AND NEW.confirmed_chain_sha256 = (
                        SELECT confirmed_chain_sha256
                        FROM n16_lifecycle_guard WHERE singleton_id = 1
                    )
                    AND _n16_guard_mutation_authorized(
                        'catalog', NEW.catalog_schema_version,
                        NEW.sealed_claim_count
                    ) = 1
                )
            )
        )
        BEGIN SELECT RAISE(ABORT, 'N16 lifecycle root update is invalid'); END
    """.strip(),
    "trg_n16_root_no_delete": """
        CREATE TRIGGER trg_n16_root_no_delete
        BEFORE DELETE ON strategy_lifecycle_installations
        BEGIN SELECT RAISE(ABORT, 'N16 lifecycle root is immutable'); END
    """.strip(),
    "trg_n16_guard_no_replace": """
        CREATE TRIGGER trg_n16_guard_no_replace
        BEFORE INSERT ON n16_lifecycle_guard
        WHEN EXISTS (
            SELECT 1 FROM n16_lifecycle_guard
            WHERE singleton_id = NEW.singleton_id
        )
        BEGIN SELECT RAISE(ABORT, 'N16 lifecycle guard replacement is forbidden'); END
    """.strip(),
    "trg_n16_guard_no_update": """
        CREATE TRIGGER trg_n16_guard_no_update
        BEFORE UPDATE ON n16_lifecycle_guard
        WHEN NOT (
            OLD.singleton_id = 1
            AND OLD.schema_version = 1
            AND OLD.rule_version = 'N16_V1'
            AND OLD.guard_sha256 =
                'd262585be43bf3dc7137d98e84770d672b8d82d40c369724a1ac70e211b52332'
            AND NEW.singleton_id IS OLD.singleton_id
            AND NEW.schema_version IS OLD.schema_version
            AND NEW.rule_version IS OLD.rule_version
            AND NEW.guard_sha256 IS OLD.guard_sha256
            AND typeof(OLD.confirmed_chain_sha256) = 'text'
            AND typeof(NEW.confirmed_chain_sha256) = 'text'
            AND typeof(OLD.sealed_claim_count) = 'integer'
            AND typeof(NEW.sealed_claim_count) = 'integer'
            AND (
                (
                    NEW.catalog_schema_version IS OLD.catalog_schema_version
                    AND NEW.sealed_claim_count = OLD.sealed_claim_count + 1
                    AND NEW.sealed_claim_count = (
                        SELECT COUNT(*) FROM n16_consumption_seals
                    )
                    AND NEW.confirmed_chain_sha256 = (
                        SELECT _n16_claim_chain_advance(
                            OLD.confirmed_chain_sha256,
                            seal_ordinal, source_signal_id, schema_version,
                            rule_version, strategy_id, source_scan_id,
                            audit_id, ledger_id, state_id, symbol, episode_id,
                            structure_id, signal_evidence_sha256,
                            state_evidence_sha256, signal_created_at,
                            claim_created_at
                        )
                        FROM n16_consumption_seals
                        WHERE seal_ordinal = NEW.sealed_claim_count
                    )
                    AND EXISTS (
                        SELECT 1 FROM n16_first_claim_witness
                        WHERE singleton_id = 1 AND seal_ordinal = 1
                    )
                    AND OLD.sealed_claim_count = (
                        SELECT sealed_claim_count
                        FROM strategy_lifecycle_installations
                        WHERE singleton_id = 1 AND strategy_id = 'N16'
                    )
                    AND OLD.confirmed_chain_sha256 = (
                        SELECT confirmed_chain_sha256
                        FROM strategy_lifecycle_installations
                        WHERE singleton_id = 1 AND strategy_id = 'N16'
                    )
                    AND _n16_guard_mutation_authorized(
                        'seal', NEW.catalog_schema_version,
                        NEW.sealed_claim_count
                    ) = 1
                )
                OR (
                    NEW.sealed_claim_count IS OLD.sealed_claim_count
                    AND NEW.confirmed_chain_sha256
                        IS OLD.confirmed_chain_sha256
                    AND NEW.catalog_schema_version != OLD.catalog_schema_version
                    AND OLD.catalog_schema_version = (
                        SELECT catalog_schema_version
                        FROM strategy_lifecycle_installations
                        WHERE singleton_id = 1 AND strategy_id = 'N16'
                    )
                    AND OLD.sealed_claim_count = (
                        SELECT sealed_claim_count
                        FROM strategy_lifecycle_installations
                        WHERE singleton_id = 1 AND strategy_id = 'N16'
                    )
                    AND OLD.confirmed_chain_sha256 = (
                        SELECT confirmed_chain_sha256
                        FROM strategy_lifecycle_installations
                        WHERE singleton_id = 1 AND strategy_id = 'N16'
                    )
                    AND _n16_guard_mutation_authorized(
                        'catalog', NEW.catalog_schema_version,
                        NEW.sealed_claim_count
                    ) = 1
                )
            )
        )
        BEGIN SELECT RAISE(ABORT, 'N16 lifecycle guard update is invalid'); END
    """.strip(),
    "trg_n16_guard_no_delete": """
        CREATE TRIGGER trg_n16_guard_no_delete
        BEFORE DELETE ON n16_lifecycle_guard
        BEGIN SELECT RAISE(ABORT, 'N16 lifecycle guard is immutable'); END
    """.strip(),
    "trg_n16_state_no_replace": """
        CREATE TRIGGER trg_n16_state_no_replace
        BEFORE INSERT ON n16_trend_support_states
        WHEN EXISTS (
            SELECT 1 FROM n16_trend_support_states AS existing
            WHERE existing.id = NEW.id
               OR existing.episode_id = NEW.episode_id
               OR (
                    NEW.structure_id IS NOT NULL
                    AND existing.structure_id = NEW.structure_id
               )
        )
        BEGIN SELECT RAISE(ABORT, 'N16 lifecycle state replacement is forbidden'); END
    """.strip(),
    "trg_n16_seal_no_replace": """
        CREATE TRIGGER trg_n16_seal_no_replace
        BEFORE INSERT ON n16_consumption_seals
        WHEN EXISTS (
            SELECT 1 FROM n16_consumption_seals AS existing
            WHERE existing.seal_ordinal = NEW.seal_ordinal
               OR existing.source_signal_id = NEW.source_signal_id
               OR existing.audit_id = NEW.audit_id
               OR existing.ledger_id = NEW.ledger_id
               OR existing.state_id = NEW.state_id
               OR existing.episode_id = NEW.episode_id
               OR existing.structure_id = NEW.structure_id
        )
        BEGIN SELECT RAISE(ABORT, 'N16 consumption seal replacement is forbidden'); END
    """.strip(),
    "trg_n16_seal_validate_insert": """
        CREATE TRIGGER trg_n16_seal_validate_insert
        BEFORE INSERT ON n16_consumption_seals
        WHEN NOT EXISTS (
            SELECT 1
            FROM n16_trend_support_states AS state
            JOIN strategy_passed_signal_audits AS audit
              ON audit.id = NEW.audit_id
            JOIN strategy_passed_structure_ledger AS ledger
              ON ledger.id = NEW.ledger_id
            WHERE state.id = NEW.state_id
              AND NEW.schema_version = 1
              AND NEW.rule_version = 'N16_V1'
              AND NEW.strategy_id = 'N16'
              AND state.strategy_id = 'N16'
              AND state.symbol = NEW.symbol
              AND state.episode_id = NEW.episode_id
              AND state.structure_id = NEW.structure_id
              AND state.stage = 'CONFIRMED'
              AND state.reason = 'PASSED'
              AND state.evidence_sha256 = NEW.state_evidence_sha256
              AND audit.source_signal_id = NEW.source_signal_id
              AND audit.strategy_id = 'N16'
              AND audit.symbol = NEW.symbol
              AND audit.structure_id = NEW.structure_id
              AND audit.source_scan_id = NEW.source_scan_id
              AND audit.signal_created_at = NEW.signal_created_at
              AND audit.evidence_sha256 = NEW.signal_evidence_sha256
              AND audit.claim_state = 'ACTIVE'
              AND ledger.source_signal_id = NEW.source_signal_id
              AND ledger.strategy_id = 'N16'
              AND ledger.symbol = NEW.symbol
              AND ledger.structure_id = NEW.structure_id
              AND ledger.source_scan_id = NEW.source_scan_id
              AND ledger.source_signal_created_at = NEW.signal_created_at
              AND ledger.evidence_sha256 = NEW.signal_evidence_sha256
              AND ledger.created_at = NEW.claim_created_at
              AND ledger.claim_state = 'ACTIVE'
        )
        BEGIN SELECT RAISE(ABORT, 'N16 consumption seal graph is invalid'); END
    """.strip(),
    "trg_n16_seal_no_update": """
        CREATE TRIGGER trg_n16_seal_no_update
        BEFORE UPDATE ON n16_consumption_seals
        BEGIN SELECT RAISE(ABORT, 'N16 consumption seal is immutable'); END
    """.strip(),
    "trg_n16_seal_no_delete": """
        CREATE TRIGGER trg_n16_seal_no_delete
        BEFORE DELETE ON n16_consumption_seals
        BEGIN SELECT RAISE(ABORT, 'N16 consumption seal is immutable'); END
    """.strip(),
    "trg_n16_witness_no_replace": """
        CREATE TRIGGER trg_n16_witness_no_replace
        BEFORE INSERT ON n16_first_claim_witness
        WHEN EXISTS (SELECT 1 FROM n16_first_claim_witness)
        BEGIN SELECT RAISE(ABORT, 'N16 first claim witness replacement is forbidden'); END
    """.strip(),
    "trg_n16_witness_validate_insert": """
        CREATE TRIGGER trg_n16_witness_validate_insert
        BEFORE INSERT ON n16_first_claim_witness
        WHEN NOT EXISTS (
            SELECT 1 FROM n16_consumption_seals AS seal
            WHERE seal.seal_ordinal = 1
              AND NEW.singleton_id = 1
              AND NEW.seal_ordinal = seal.seal_ordinal
              AND NEW.source_signal_id = seal.source_signal_id
              AND NEW.schema_version = seal.schema_version
              AND NEW.rule_version = seal.rule_version
              AND NEW.strategy_id = seal.strategy_id
              AND NEW.source_scan_id = seal.source_scan_id
              AND NEW.audit_id = seal.audit_id
              AND NEW.ledger_id = seal.ledger_id
              AND NEW.state_id = seal.state_id
              AND NEW.symbol = seal.symbol
              AND NEW.episode_id = seal.episode_id
              AND NEW.structure_id = seal.structure_id
              AND NEW.signal_evidence_sha256 = seal.signal_evidence_sha256
              AND NEW.state_evidence_sha256 = seal.state_evidence_sha256
              AND NEW.signal_created_at = seal.signal_created_at
              AND NEW.claim_created_at = seal.claim_created_at
        )
        BEGIN SELECT RAISE(ABORT, 'N16 first claim witness is invalid'); END
    """.strip(),
    "trg_n16_witness_no_update": """
        CREATE TRIGGER trg_n16_witness_no_update
        BEFORE UPDATE ON n16_first_claim_witness
        BEGIN SELECT RAISE(ABORT, 'N16 first claim witness is immutable'); END
    """.strip(),
    "trg_n16_witness_no_delete": """
        CREATE TRIGGER trg_n16_witness_no_delete
        BEFORE DELETE ON n16_first_claim_witness
        BEGIN SELECT RAISE(ABORT, 'N16 first claim witness is immutable'); END
    """.strip(),
    "trg_n16_state_no_update_after_seal": """
        CREATE TRIGGER trg_n16_state_no_update_after_seal
        BEFORE UPDATE ON n16_trend_support_states
        WHEN EXISTS (
            SELECT 1 FROM n16_consumption_seals WHERE state_id = OLD.id
        )
        BEGIN SELECT RAISE(ABORT, 'N16 consumed lifecycle is immutable'); END
    """.strip(),
    "trg_n16_state_no_delete_after_seal": """
        CREATE TRIGGER trg_n16_state_no_delete_after_seal
        BEFORE DELETE ON n16_trend_support_states
        WHEN EXISTS (
            SELECT 1 FROM n16_consumption_seals WHERE state_id = OLD.id
        )
        BEGIN SELECT RAISE(ABORT, 'N16 consumed lifecycle is immutable'); END
    """.strip(),
    "trg_n16_audit_no_active_insert": """
        CREATE TRIGGER trg_n16_audit_no_active_insert
        BEFORE INSERT ON strategy_passed_signal_audits
        WHEN NEW.strategy_id = 'N16' AND NEW.claim_state = 'ACTIVE'
        BEGIN SELECT RAISE(ABORT, 'N16 ACTIVE audit requires staged publication'); END
    """.strip(),
    "trg_n16_audit_no_replace": """
        CREATE TRIGGER trg_n16_audit_no_replace
        BEFORE INSERT ON strategy_passed_signal_audits
        WHEN EXISTS (
            SELECT 1 FROM strategy_passed_signal_audits AS existing
            WHERE existing.strategy_id = 'N16'
              AND (
                    existing.id = NEW.id
                    OR existing.source_signal_id = NEW.source_signal_id
              )
        )
        BEGIN SELECT RAISE(ABORT, 'N16 audit replacement is forbidden'); END
    """.strip(),
    "trg_n16_audit_validate_staged_insert": """
        CREATE TRIGGER trg_n16_audit_validate_staged_insert
        BEFORE INSERT ON strategy_passed_signal_audits
        WHEN NEW.strategy_id = 'N16' AND NEW.claim_state = 'STAGED'
         AND NOT EXISTS (
            SELECT 1
            FROM strategy_signals AS signal
            JOIN n16_trend_support_states AS state
              ON state.structure_id = NEW.structure_id
            WHERE signal.id = NEW.source_signal_id
              AND signal.scan_id = NEW.source_scan_id
              AND signal.strategy_id = 'N16'
              AND signal.symbol = NEW.symbol
              AND signal.funding_rate = NEW.funding_rate
              AND signal.matched_patterns = NEW.matched_patterns
              AND signal.trend_slope = NEW.trend_slope
              AND signal.current_bullish = NEW.current_bullish
              AND signal.passed = NEW.passed
              AND signal.decision = NEW.decision
              AND signal.reason = NEW.reason
              AND signal.structure_id = NEW.structure_id
              AND signal.detail_json = NEW.detail_json
              AND signal.created_at = NEW.signal_created_at
              AND state.strategy_id = 'N16'
              AND state.symbol = NEW.symbol
              AND state.structure_id = NEW.structure_id
              AND state.stage = 'CONFIRMED'
              AND state.reason = 'PASSED'
         )
        BEGIN SELECT RAISE(ABORT, 'N16 staged audit graph is invalid'); END
    """.strip(),
    "trg_n16_audit_no_identity_adoption": """
        CREATE TRIGGER trg_n16_audit_no_identity_adoption
        BEFORE UPDATE ON strategy_passed_signal_audits
        WHEN OLD.strategy_id != 'N16' AND NEW.strategy_id = 'N16'
        BEGIN SELECT RAISE(ABORT, 'N16 audit identity cannot be adopted'); END
    """.strip(),
    "trg_n16_audit_staged_update_guard": """
        CREATE TRIGGER trg_n16_audit_staged_update_guard
        BEFORE UPDATE ON strategy_passed_signal_audits
        WHEN OLD.strategy_id = 'N16' AND OLD.claim_state = 'STAGED'
         AND (
            NEW.claim_state != 'ACTIVE'
            OR NEW.id IS NOT OLD.id
            OR NEW.source_signal_id IS NOT OLD.source_signal_id
            OR NEW.source_scan_id IS NOT OLD.source_scan_id
            OR NEW.strategy_id IS NOT OLD.strategy_id
            OR NEW.symbol IS NOT OLD.symbol
            OR NEW.funding_rate IS NOT OLD.funding_rate
            OR NEW.matched_patterns IS NOT OLD.matched_patterns
            OR NEW.trend_slope IS NOT OLD.trend_slope
            OR NEW.current_bullish IS NOT OLD.current_bullish
            OR NEW.passed IS NOT OLD.passed
            OR NEW.decision IS NOT OLD.decision
            OR NEW.reason IS NOT OLD.reason
            OR NEW.structure_id IS NOT OLD.structure_id
            OR NEW.detail_json IS NOT OLD.detail_json
            OR NEW.signal_created_at IS NOT OLD.signal_created_at
            OR NEW.evidence_sha256 IS NOT OLD.evidence_sha256
            OR NEW.created_at IS NOT OLD.created_at
         )
        BEGIN SELECT RAISE(ABORT, 'N16 staged audit activation is invalid'); END
    """.strip(),
    "trg_n16_audit_activate_graph": """
        CREATE TRIGGER trg_n16_audit_activate_graph
        BEFORE UPDATE OF claim_state ON strategy_passed_signal_audits
        WHEN OLD.strategy_id = 'N16' AND OLD.claim_state = 'STAGED'
         AND NEW.claim_state = 'ACTIVE'
         AND NOT EXISTS (
            SELECT 1
            FROM strategy_passed_structure_ledger AS ledger
            JOIN n16_trend_support_states AS state
              ON state.structure_id = ledger.structure_id
            WHERE ledger.strategy_id = 'N16'
              AND ledger.source_signal_id = OLD.source_signal_id
              AND ledger.source_scan_id = OLD.source_scan_id
              AND ledger.symbol = OLD.symbol
              AND ledger.structure_id = OLD.structure_id
              AND ledger.source_signal_created_at = OLD.signal_created_at
              AND ledger.evidence_sha256 = OLD.evidence_sha256
              AND ledger.claim_state = 'STAGED'
              AND state.strategy_id = 'N16'
              AND state.symbol = OLD.symbol
              AND state.structure_id = OLD.structure_id
              AND state.stage = 'CONFIRMED'
              AND state.reason = 'PASSED'
         )
        BEGIN SELECT RAISE(ABORT, 'N16 audit activation graph is invalid'); END
    """.strip(),
    "trg_n16_audit_no_update_after_active": """
        CREATE TRIGGER trg_n16_audit_no_update_after_active
        BEFORE UPDATE ON strategy_passed_signal_audits
        WHEN (OLD.strategy_id = 'N16' AND OLD.claim_state = 'ACTIVE')
          OR EXISTS (
            SELECT 1 FROM n16_consumption_seals WHERE audit_id = OLD.id
          )
        BEGIN SELECT RAISE(ABORT, 'N16 active audit is immutable'); END
    """.strip(),
    "trg_n16_audit_no_delete_after_active": """
        CREATE TRIGGER trg_n16_audit_no_delete_after_active
        BEFORE DELETE ON strategy_passed_signal_audits
        WHEN (OLD.strategy_id = 'N16' AND OLD.claim_state = 'ACTIVE')
          OR EXISTS (
            SELECT 1 FROM n16_consumption_seals WHERE audit_id = OLD.id
          )
        BEGIN SELECT RAISE(ABORT, 'N16 active audit is immutable'); END
    """.strip(),
    "trg_n16_ledger_no_active_insert": """
        CREATE TRIGGER trg_n16_ledger_no_active_insert
        BEFORE INSERT ON strategy_passed_structure_ledger
        WHEN NEW.strategy_id = 'N16' AND NEW.claim_state = 'ACTIVE'
        BEGIN SELECT RAISE(ABORT, 'N16 ACTIVE ledger requires staged publication'); END
    """.strip(),
    "trg_n16_ledger_no_replace": """
        CREATE TRIGGER trg_n16_ledger_no_replace
        BEFORE INSERT ON strategy_passed_structure_ledger
        WHEN EXISTS (
            SELECT 1 FROM strategy_passed_structure_ledger AS existing
            WHERE existing.strategy_id = 'N16'
              AND (
                    existing.id = NEW.id
                    OR existing.source_signal_id = NEW.source_signal_id
                    OR existing.structure_id = NEW.structure_id
              )
        )
        BEGIN SELECT RAISE(ABORT, 'N16 ledger replacement is forbidden'); END
    """.strip(),
    "trg_n16_ledger_validate_staged_insert": """
        CREATE TRIGGER trg_n16_ledger_validate_staged_insert
        BEFORE INSERT ON strategy_passed_structure_ledger
        WHEN NEW.strategy_id = 'N16' AND NEW.claim_state = 'STAGED'
         AND NOT EXISTS (
            SELECT 1
            FROM strategy_passed_signal_audits AS audit
            JOIN n16_trend_support_states AS state
              ON state.structure_id = NEW.structure_id
            WHERE audit.source_signal_id = NEW.source_signal_id
              AND audit.source_scan_id = NEW.source_scan_id
              AND audit.strategy_id = 'N16'
              AND audit.symbol = NEW.symbol
              AND audit.structure_id = NEW.structure_id
              AND audit.signal_created_at = NEW.source_signal_created_at
              AND audit.evidence_sha256 = NEW.evidence_sha256
              AND audit.claim_state = 'STAGED'
              AND state.strategy_id = 'N16'
              AND state.symbol = NEW.symbol
              AND state.structure_id = NEW.structure_id
              AND state.stage = 'CONFIRMED'
              AND state.reason = 'PASSED'
         )
        BEGIN SELECT RAISE(ABORT, 'N16 staged ledger graph is invalid'); END
    """.strip(),
    "trg_n16_ledger_no_identity_adoption": """
        CREATE TRIGGER trg_n16_ledger_no_identity_adoption
        BEFORE UPDATE ON strategy_passed_structure_ledger
        WHEN OLD.strategy_id != 'N16' AND NEW.strategy_id = 'N16'
        BEGIN SELECT RAISE(ABORT, 'N16 ledger identity cannot be adopted'); END
    """.strip(),
    "trg_n16_ledger_staged_update_guard": """
        CREATE TRIGGER trg_n16_ledger_staged_update_guard
        BEFORE UPDATE ON strategy_passed_structure_ledger
        WHEN OLD.strategy_id = 'N16' AND OLD.claim_state = 'STAGED'
         AND (
            NEW.claim_state != 'ACTIVE'
            OR NEW.id IS NOT OLD.id
            OR NEW.strategy_id IS NOT OLD.strategy_id
            OR NEW.symbol IS NOT OLD.symbol
            OR NEW.structure_id IS NOT OLD.structure_id
            OR NEW.source_signal_id IS NOT OLD.source_signal_id
            OR NEW.source_scan_id IS NOT OLD.source_scan_id
            OR NEW.source_signal_created_at IS NOT OLD.source_signal_created_at
            OR NEW.evidence_sha256 IS NOT OLD.evidence_sha256
            OR NEW.created_at IS NOT OLD.created_at
         )
        BEGIN SELECT RAISE(ABORT, 'N16 staged ledger activation is invalid'); END
    """.strip(),
    "trg_n16_ledger_activate_graph": """
        CREATE TRIGGER trg_n16_ledger_activate_graph
        BEFORE UPDATE OF claim_state ON strategy_passed_structure_ledger
        WHEN OLD.strategy_id = 'N16' AND OLD.claim_state = 'STAGED'
         AND NEW.claim_state = 'ACTIVE'
         AND NOT EXISTS (
            SELECT 1
            FROM strategy_passed_signal_audits AS audit
            JOIN n16_trend_support_states AS state
              ON state.structure_id = OLD.structure_id
            WHERE audit.source_signal_id = OLD.source_signal_id
              AND audit.source_scan_id = OLD.source_scan_id
              AND audit.strategy_id = 'N16'
              AND audit.symbol = OLD.symbol
              AND audit.structure_id = OLD.structure_id
              AND audit.signal_created_at = OLD.source_signal_created_at
              AND audit.evidence_sha256 = OLD.evidence_sha256
              AND audit.claim_state = 'ACTIVE'
              AND state.strategy_id = 'N16'
              AND state.symbol = OLD.symbol
              AND state.structure_id = OLD.structure_id
              AND state.stage = 'CONFIRMED'
              AND state.reason = 'PASSED'
         )
        BEGIN SELECT RAISE(ABORT, 'N16 ledger activation graph is invalid'); END
    """.strip(),
    "trg_n16_ledger_seal_after_active": """
        CREATE TRIGGER trg_n16_ledger_seal_after_active
        AFTER UPDATE OF claim_state ON strategy_passed_structure_ledger
        WHEN OLD.strategy_id = 'N16' AND OLD.claim_state = 'STAGED'
         AND NEW.claim_state = 'ACTIVE'
        BEGIN
            INSERT INTO n16_consumption_seals (
                source_signal_id, schema_version, rule_version, strategy_id,
                source_scan_id, audit_id, ledger_id, state_id, symbol,
                episode_id, structure_id, signal_evidence_sha256,
                state_evidence_sha256, signal_created_at, claim_created_at
            )
            SELECT NEW.source_signal_id, 1, 'N16_V1', 'N16',
                   NEW.source_scan_id, audit.id, NEW.id, state.id, NEW.symbol,
                   state.episode_id, NEW.structure_id, NEW.evidence_sha256,
                   state.evidence_sha256, NEW.source_signal_created_at,
                   NEW.created_at
            FROM strategy_passed_signal_audits AS audit
            JOIN n16_trend_support_states AS state
              ON state.structure_id = NEW.structure_id
            WHERE audit.source_signal_id = NEW.source_signal_id
              AND audit.claim_state = 'ACTIVE'
              AND state.strategy_id = 'N16';
            INSERT INTO n16_first_claim_witness (
                singleton_id, seal_ordinal, source_signal_id,
                schema_version, rule_version, strategy_id, source_scan_id,
                audit_id, ledger_id, state_id, symbol, episode_id,
                structure_id, signal_evidence_sha256,
                state_evidence_sha256, signal_created_at, claim_created_at
            )
            SELECT 1, seal.seal_ordinal, seal.source_signal_id,
                   seal.schema_version, seal.rule_version, seal.strategy_id,
                   seal.source_scan_id, seal.audit_id, seal.ledger_id,
                   seal.state_id, seal.symbol, seal.episode_id,
                   seal.structure_id, seal.signal_evidence_sha256,
                   seal.state_evidence_sha256, seal.signal_created_at,
                   seal.claim_created_at
            FROM n16_consumption_seals AS seal
            WHERE seal.seal_ordinal = 1
              AND NOT EXISTS (SELECT 1 FROM n16_first_claim_witness);
            UPDATE n16_lifecycle_guard
            SET sealed_claim_count = sealed_claim_count + 1,
                confirmed_chain_sha256 = (
                    SELECT _n16_claim_chain_advance(
                        n16_lifecycle_guard.confirmed_chain_sha256,
                        seal.seal_ordinal, seal.source_signal_id,
                        seal.schema_version, seal.rule_version,
                        seal.strategy_id, seal.source_scan_id, seal.audit_id,
                        seal.ledger_id, seal.state_id, seal.symbol,
                        seal.episode_id, seal.structure_id,
                        seal.signal_evidence_sha256,
                        seal.state_evidence_sha256, seal.signal_created_at,
                        seal.claim_created_at
                    )
                    FROM n16_consumption_seals AS seal
                    WHERE seal.ledger_id = NEW.id
                )
            WHERE singleton_id = 1;
            UPDATE strategy_lifecycle_installations
            SET sealed_claim_count = sealed_claim_count + 1,
                confirmed_chain_sha256 = (
                    SELECT confirmed_chain_sha256
                    FROM n16_lifecycle_guard WHERE singleton_id = 1
                )
            WHERE singleton_id = 1 AND strategy_id = 'N16';
        END
    """.strip(),
    "trg_n16_ledger_no_update_after_active": """
        CREATE TRIGGER trg_n16_ledger_no_update_after_active
        BEFORE UPDATE ON strategy_passed_structure_ledger
        WHEN (OLD.strategy_id = 'N16' AND OLD.claim_state = 'ACTIVE')
          OR EXISTS (
            SELECT 1 FROM n16_consumption_seals WHERE ledger_id = OLD.id
          )
        BEGIN SELECT RAISE(ABORT, 'N16 active ledger is immutable'); END
    """.strip(),
    "trg_n16_ledger_no_delete_after_active": """
        CREATE TRIGGER trg_n16_ledger_no_delete_after_active
        BEFORE DELETE ON strategy_passed_structure_ledger
        WHEN (OLD.strategy_id = 'N16' AND OLD.claim_state = 'ACTIVE')
          OR EXISTS (
            SELECT 1 FROM n16_consumption_seals WHERE ledger_id = OLD.id
          )
        BEGIN SELECT RAISE(ABORT, 'N16 active ledger is immutable'); END
    """.strip(),
}
_N16_RETENTION_TRIGGER_TABLES = {
    "trg_n16_audit_no_active_insert": "strategy_passed_signal_audits",
    "trg_n16_audit_no_replace": "strategy_passed_signal_audits",
    "trg_n16_audit_validate_staged_insert": "strategy_passed_signal_audits",
    "trg_n16_audit_no_identity_adoption": "strategy_passed_signal_audits",
    "trg_n16_audit_staged_update_guard": "strategy_passed_signal_audits",
    "trg_n16_audit_activate_graph": "strategy_passed_signal_audits",
    "trg_n16_audit_no_update_after_active": "strategy_passed_signal_audits",
    "trg_n16_audit_no_delete_after_active": "strategy_passed_signal_audits",
    "trg_n16_ledger_no_active_insert": "strategy_passed_structure_ledger",
    "trg_n16_ledger_no_replace": "strategy_passed_structure_ledger",
    "trg_n16_ledger_validate_staged_insert": "strategy_passed_structure_ledger",
    "trg_n16_ledger_no_identity_adoption": "strategy_passed_structure_ledger",
    "trg_n16_ledger_staged_update_guard": "strategy_passed_structure_ledger",
    "trg_n16_ledger_activate_graph": "strategy_passed_structure_ledger",
    "trg_n16_ledger_seal_after_active": "strategy_passed_structure_ledger",
    "trg_n16_ledger_no_update_after_active": "strategy_passed_structure_ledger",
    "trg_n16_ledger_no_delete_after_active": "strategy_passed_structure_ledger",
}
_N16_STATE_COLUMNS = (
    ("id", "INTEGER", 0, None, 1),
    ("strategy_id", "TEXT", 1, None, 0),
    ("symbol", "TEXT", 1, None, 0),
    ("episode_id", "TEXT", 1, None, 0),
    ("structure_id", "TEXT", 0, None, 0),
    ("stage", "TEXT", 1, None, 0),
    ("reason", "TEXT", 1, None, 0),
    ("quote_volume_rank", "INTEGER", 1, None, 0),
    ("evidence_json", "TEXT", 1, None, 0),
    ("evidence_sha256", "TEXT", 1, None, 0),
    ("created_at", "TEXT", 1, None, 0),
    ("updated_at", "TEXT", 1, None, 0),
)
_N16_GUARD_COLUMNS = (
    ("singleton_id", "INTEGER", 0, None, 1),
    ("schema_version", "INTEGER", 1, None, 0),
    ("rule_version", "TEXT", 1, None, 0),
    ("guard_sha256", "TEXT", 1, None, 0),
    ("catalog_schema_version", "INTEGER", 1, None, 0),
    ("sealed_claim_count", "INTEGER", 1, None, 0),
    ("confirmed_chain_sha256", "TEXT", 1, None, 0),
)
_N16_ROOT_COLUMNS = (
    ("singleton_id", "INTEGER", 0, None, 1),
    ("strategy_id", "TEXT", 1, None, 0),
    ("schema_version", "INTEGER", 1, None, 0),
    ("rule_version", "TEXT", 1, None, 0),
    ("guard_sha256", "TEXT", 1, None, 0),
    ("catalog_schema_version", "INTEGER", 1, None, 0),
    ("sealed_claim_count", "INTEGER", 1, None, 0),
    ("confirmed_chain_sha256", "TEXT", 1, None, 0),
    ("installed_at", "TEXT", 1, None, 0),
)
_N16_SEAL_COLUMNS = (
    ("seal_ordinal", "INTEGER", 0, None, 1),
    ("source_signal_id", "INTEGER", 1, None, 0),
    ("schema_version", "INTEGER", 1, None, 0),
    ("rule_version", "TEXT", 1, None, 0),
    ("strategy_id", "TEXT", 1, None, 0),
    ("source_scan_id", "INTEGER", 1, None, 0),
    ("audit_id", "INTEGER", 1, None, 0),
    ("ledger_id", "INTEGER", 1, None, 0),
    ("state_id", "INTEGER", 1, None, 0),
    ("symbol", "TEXT", 1, None, 0),
    ("episode_id", "TEXT", 1, None, 0),
    ("structure_id", "TEXT", 1, None, 0),
    ("signal_evidence_sha256", "TEXT", 1, None, 0),
    ("state_evidence_sha256", "TEXT", 1, None, 0),
    ("signal_created_at", "TEXT", 1, None, 0),
    ("claim_created_at", "TEXT", 1, None, 0),
)
_N16_WITNESS_COLUMNS = (
    ("singleton_id", "INTEGER", 0, None, 1),
    ("seal_ordinal", "INTEGER", 1, None, 0),
    ("source_signal_id", "INTEGER", 1, None, 0),
    ("schema_version", "INTEGER", 1, None, 0),
    ("rule_version", "TEXT", 1, None, 0),
    ("strategy_id", "TEXT", 1, None, 0),
    ("source_scan_id", "INTEGER", 1, None, 0),
    ("audit_id", "INTEGER", 1, None, 0),
    ("ledger_id", "INTEGER", 1, None, 0),
    ("state_id", "INTEGER", 1, None, 0),
    ("symbol", "TEXT", 1, None, 0),
    ("episode_id", "TEXT", 1, None, 0),
    ("structure_id", "TEXT", 1, None, 0),
    ("signal_evidence_sha256", "TEXT", 1, None, 0),
    ("state_evidence_sha256", "TEXT", 1, None, 0),
    ("signal_created_at", "TEXT", 1, None, 0),
    ("claim_created_at", "TEXT", 1, None, 0),
)
def _n16_normalized_sql(value: Any) -> str:
    if type(value) is not str:
        raise RuntimeError("N16 schema SQL is missing")
    normalized: list[str] = []
    in_literal = False
    index = 0
    while index < len(value):
        character = value[index]
        if character == "'":
            normalized.append(character)
            if in_literal and index + 1 < len(value) and value[index + 1] == "'":
                normalized.append("'")
                index += 2
                continue
            in_literal = not in_literal
        elif in_literal:
            normalized.append(character)
        elif character.isspace():
            if normalized and normalized[-1] != " ":
                normalized.append(" ")
        else:
            normalized.append(character.lower())
        index += 1
    if in_literal:
        raise RuntimeError("N16 schema SQL has an unterminated literal")
    return "".join(normalized).strip()


def _verify_n16_shared_execution_schema(
    connection: sqlite3.Connection,
) -> None:
    """Certify every shared table that can authorize an N16 side effect.

    These are intentionally existing N01-N15 tables, not N16-owned storage.
    N16 nevertheless depends on their exact constraints and lookup indexes.
    A same-column replacement with a hostile CHECK, generated column, trigger,
    expression index, or partial predicate must therefore be rejected by the
    read-only startup preflight before catalog refresh or any Review write.
    """

    for table, expected_sql in _N16_SHARED_TABLE_SQL.items():
        rows = connection.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_schema "
            "WHERE lower(name)=lower(?)",
            (table,),
        ).fetchall()
        actual_xinfo = tuple(
            tuple(row[1:7])
            for row in connection.execute(
                'PRAGMA table_xinfo("%s")' % table
            ).fetchall()
        )
        migrated_trade_reviews = (
            table == "trade_reviews"
            and len(rows) == 1
            and type(rows[0][3]) is str
            and _n16_normalized_sql(rows[0][3])
            == _n16_normalized_sql(
                _N16_MIGRATED_TRADE_REVIEWS_TABLE_SQL
            )
            and actual_xinfo == _N16_MIGRATED_TRADE_REVIEWS_XINFO
        )
        legacy_paper = (
            table == "strategy_paper_trades"
            and len(rows) == 1
            and type(rows[0][3]) is str
            and _n16_normalized_sql(rows[0][3])
            == _n16_normalized_sql(_N16_LEGACY_PAPER_TABLE_SQL)
            and actual_xinfo == _N16_LEGACY_PAPER_TABLE_XINFO
        )
        migrated_paper = (
            table == "strategy_paper_trades"
            and len(rows) == 1
            and type(rows[0][3]) is str
            and _n16_normalized_sql(rows[0][3])
            == _n16_normalized_sql(_N16_MIGRATED_PAPER_TABLE_SQL)
            and actual_xinfo == _N16_LEGACY_PAPER_TABLE_XINFO
        )
        migrated_strategy_states = (
            table == "strategy_states"
            and len(rows) == 1
            and type(rows[0][3]) is str
            and _n16_normalized_sql(rows[0][3])
            == _n16_normalized_sql(
                _N16_MIGRATED_STRATEGY_STATES_TABLE_SQL
            )
            and actual_xinfo == _N16_MIGRATED_STRATEGY_STATES_XINFO
        )
        if (
            len(rows) != 1
            or tuple(rows[0][:3]) != ("table", table, table)
            or type(rows[0][3]) is not str
            or (
                not migrated_trade_reviews
                and not legacy_paper
                and not migrated_paper
                and not migrated_strategy_states
                and _n16_normalized_sql(rows[0][3])
                != _n16_normalized_sql(expected_sql)
            )
        ):
            raise RuntimeError(
                "N16 shared execution table is inconsistent: %s" % table
            )
        if (
            not migrated_trade_reviews
            and not legacy_paper
            and not migrated_paper
            and not migrated_strategy_states
            and actual_xinfo != _N16_SHARED_TABLE_XINFO[table]
        ):
            raise RuntimeError(
                "N16 shared execution columns are inconsistent: %s" % table
            )
        actual_foreign_keys = tuple(
            tuple(row)
            for row in connection.execute(
                'PRAGMA foreign_key_list("%s")' % table
            ).fetchall()
        )
        if actual_foreign_keys != _N16_SHARED_TABLE_FOREIGN_KEYS[table]:
            raise RuntimeError(
                "N16 shared execution foreign keys are inconsistent: %s"
                % table
            )

        expected_indexes = _N16_SHARED_INDEX_SQL[table]
        index_rows = connection.execute(
            'PRAGMA index_list("%s")' % table
        ).fetchall()
        index_identity = {
            row[1]: tuple(row[2:5])
            for row in index_rows
            if type(row[1]) is str
        }
        if set(index_identity) != set(expected_indexes):
            raise RuntimeError(
                "N16 shared execution index catalog is inconsistent: %s"
                % table
            )
        for name, expected_index_sql in expected_indexes.items():
            expected_identity, expected_xinfo = _N16_SHARED_INDEX_METADATA[name]
            if legacy_paper or migrated_paper:
                expected_xinfo = _N16_LEGACY_PAPER_INDEX_XINFO[name]
            index_sql_rows = connection.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                "WHERE lower(name)=lower(?)",
                (name,),
            ).fetchall()
            actual_index_xinfo = tuple(
                tuple(row[:6])
                for row in connection.execute(
                    'PRAGMA index_xinfo("%s")' % name
                ).fetchall()
            )
            sql_matches = (
                index_sql_rows[0][3] is None
                if expected_index_sql is None and len(index_sql_rows) == 1
                else (
                    expected_index_sql is not None
                    and len(index_sql_rows) == 1
                    and type(index_sql_rows[0][3]) is str
                    and _n16_normalized_sql(index_sql_rows[0][3])
                    == _n16_normalized_sql(expected_index_sql)
                )
            )
            if (
                index_identity.get(name) != expected_identity
                or len(index_sql_rows) != 1
                or tuple(index_sql_rows[0][:3]) != ("index", name, table)
                or not sql_matches
                or actual_index_xinfo != expected_xinfo
            ):
                raise RuntimeError(
                    "N16 shared execution index is inconsistent: %s" % name
                )

        triggers = connection.execute(
            "SELECT name, sql FROM sqlite_schema "
            "WHERE type='trigger' AND tbl_name=? ORDER BY name",
            (table,),
        ).fetchall()
        if triggers:
            raise RuntimeError(
                "N16 shared execution trigger catalog is inconsistent: %s"
                % table
            )


def _verify_n16_preinstall_shared_execution_schema(
    connection: sqlite3.Connection,
) -> None:
    """Certify existing N01-N15 shared tables before either N16 file changes."""

    legacy_indexes = {
        "trade_reviews": {"idx_trade_reviews_symbol_time"},
        "strategy_live_links": {"idx_strategy_live_result"},
        "strategy_paper_trades": {
            "idx_strategy_paper_open",
            "idx_strategy_paper_symbol_result",
        },
        "strategy_states": {"sqlite_autoindex_strategy_states_1"},
        "symbol_cooldowns": {
            "sqlite_autoindex_symbol_cooldowns_1",
            "idx_symbol_cooldowns_until",
        },
    }
    for table, expected_sql in _N16_SHARED_TABLE_SQL.items():
        rows = connection.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema "
            "WHERE lower(name)=lower(?)",
            (table,),
        ).fetchall()
        if not rows:
            continue
        actual_xinfo = tuple(
            tuple(row[1:7])
            for row in connection.execute(
                'PRAGMA table_xinfo("%s")' % table
            ).fetchall()
        )
        migrated_trade_reviews = (
            table == "trade_reviews"
            and len(rows) == 1
            and type(rows[0][3]) is str
            and _n16_normalized_sql(rows[0][3])
            == _n16_normalized_sql(
                _N16_MIGRATED_TRADE_REVIEWS_TABLE_SQL
            )
            and actual_xinfo == _N16_MIGRATED_TRADE_REVIEWS_XINFO
        )
        legacy_paper = (
            table == "strategy_paper_trades"
            and len(rows) == 1
            and type(rows[0][3]) is str
            and _n16_normalized_sql(rows[0][3])
            == _n16_normalized_sql(_N16_LEGACY_PAPER_TABLE_SQL)
            and actual_xinfo == _N16_LEGACY_PAPER_TABLE_XINFO
        )
        migrated_paper = (
            table == "strategy_paper_trades"
            and len(rows) == 1
            and type(rows[0][3]) is str
            and _n16_normalized_sql(rows[0][3])
            == _n16_normalized_sql(_N16_MIGRATED_PAPER_TABLE_SQL)
            and actual_xinfo == _N16_LEGACY_PAPER_TABLE_XINFO
        )
        pre_lastcheck_paper = (
            table == "strategy_paper_trades"
            and len(rows) == 1
            and type(rows[0][3]) is str
            and _n16_normalized_sql(rows[0][3])
            == _n16_normalized_sql(_N16_PRE_LASTCHECK_PAPER_TABLE_SQL)
            and actual_xinfo == _N16_PRE_LASTCHECK_PAPER_TABLE_XINFO
        )
        legacy_strategy_states = (
            table == "strategy_states"
            and len(rows) == 1
            and type(rows[0][3]) is str
            and (
                (
                    _n16_normalized_sql(rows[0][3])
                    == _n16_normalized_sql(
                        _N16_LEGACY_STRATEGY_STATES_TABLE_SQL
                    )
                    and actual_xinfo == _N16_LEGACY_STRATEGY_STATES_XINFO
                )
                or (
                    _n16_normalized_sql(rows[0][3])
                    == _n16_normalized_sql(
                        _N16_MIGRATED_STRATEGY_STATES_TABLE_SQL
                    )
                    and actual_xinfo == _N16_MIGRATED_STRATEGY_STATES_XINFO
                )
            )
        )
        if (
            len(rows) != 1
            or tuple(rows[0][:3]) != ("table", table, table)
            or type(rows[0][3]) is not str
            or (
                not migrated_trade_reviews
                and not legacy_paper
                and not migrated_paper
                and not pre_lastcheck_paper
                and not legacy_strategy_states
                and (
                    _n16_normalized_sql(rows[0][3])
                    != _n16_normalized_sql(expected_sql)
                    or actual_xinfo != _N16_SHARED_TABLE_XINFO[table]
                )
            )
            or tuple(
                tuple(row)
                for row in connection.execute(
                    'PRAGMA foreign_key_list("%s")' % table
                ).fetchall()
            )
            != _N16_SHARED_TABLE_FOREIGN_KEYS[table]
        ):
            raise RuntimeError(
                "pre-N16 shared execution table is inconsistent: %s" % table
            )
        expected_names = legacy_indexes[table]
        index_rows = connection.execute(
            'PRAGMA index_list("%s")' % table
        ).fetchall()
        identity = {
            row[1]: tuple(row[2:5])
            for row in index_rows
            if type(row[1]) is str
        }
        if set(identity) != expected_names:
            raise RuntimeError(
                "pre-N16 shared execution indexes are inconsistent: %s" % table
            )
        for name in expected_names:
            expected_identity, expected_xinfo = _N16_SHARED_INDEX_METADATA[name]
            if legacy_paper or migrated_paper or pre_lastcheck_paper:
                expected_xinfo = _N16_LEGACY_PAPER_INDEX_XINFO[name]
            sql_rows = connection.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_schema "
                "WHERE lower(name)=lower(?)",
                (name,),
            ).fetchall()
            expected_index_sql = _N16_SHARED_INDEX_SQL[table][name]
            sql_matches = (
                sql_rows[0][3] is None
                if expected_index_sql is None and len(sql_rows) == 1
                else (
                    expected_index_sql is not None
                    and len(sql_rows) == 1
                    and type(sql_rows[0][3]) is str
                    and _n16_normalized_sql(sql_rows[0][3])
                    == _n16_normalized_sql(expected_index_sql)
                )
            )
            if (
                identity.get(name) != expected_identity
                or len(sql_rows) != 1
                or tuple(sql_rows[0][:3]) != ("index", name, table)
                or not sql_matches
                or tuple(
                    tuple(row[:6])
                    for row in connection.execute(
                        'PRAGMA index_xinfo("%s")' % name
                    ).fetchall()
                )
                != expected_xinfo
            ):
                raise RuntimeError(
                    "pre-N16 shared execution index is inconsistent: %s" % name
                )
        if connection.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='trigger' AND tbl_name=? "
            "LIMIT 1",
            (table,),
        ).fetchone() is not None:
            raise RuntimeError(
                "pre-N16 shared execution trigger is inconsistent: %s" % table
            )


def _verify_n16_schema(
    connection: sqlite3.Connection,
    *,
    allow_absent: bool,
) -> bool:
    owned_names = {
        *_N16_TABLES,
        *_N16_INDEX_SQL.keys(),
        *_N16_TRIGGER_SQL.keys(),
    }
    n16_catalog_rows = connection.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_schema "
        "WHERE sql IS NOT NULL AND ("
        "lower(name) GLOB 'n16*' OR lower(name) GLOB 'idx_n16*' "
        "OR lower(name) GLOB 'trg_n16*' OR lower(tbl_name) GLOB 'n16*' "
        "OR lower(name) = 'strategy_lifecycle_installations' "
        "OR lower(tbl_name) = 'strategy_lifecycle_installations'"
        ") ORDER BY type, name"
    ).fetchall()
    for object_type, name, table_name, sql in n16_catalog_rows:
        if (
            type(object_type) is not str
            or type(name) is not str
            or type(table_name) is not str
            or type(sql) is not str
            or name not in owned_names
        ):
            raise RuntimeError("N16 lifecycle catalog contains an unknown object")
    catalog_rows = connection.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_schema "
        "WHERE lower(name) IN (%s)" % ",".join("?" for _ in owned_names),
        tuple(name.lower() for name in sorted(owned_names)),
    ).fetchall()
    for object_type, name, table_name, sql in catalog_rows:
        if (
            type(object_type) is not str
            or type(name) is not str
            or type(table_name) is not str
            or (sql is not None and type(sql) is not str)
            or name not in owned_names
        ):
            raise RuntimeError("N16 lifecycle schema object identity is inconsistent")
    if not catalog_rows:
        if any(
            type(row[1]) is str and row[1].lower() == "trade_review_id"
            for row in connection.execute("PRAGMA table_xinfo(events)").fetchall()
        ):
            raise RuntimeError(
                "pre-N16 Review contains a partial terminal event identity schema"
            )
        if allow_absent:
            return False
        raise RuntimeError("N16 lifecycle schema is absent")
    if {row[1] for row in catalog_rows} != owned_names:
        raise RuntimeError("N16 lifecycle schema is partial")
    if {
        row[1] for row in catalog_rows if row[0] == "table"
    } != set(_N16_TABLES):
        raise RuntimeError("N16 lifecycle table identity is inconsistent")

    def columns(table: str) -> tuple[tuple[Any, ...], ...]:
        return tuple(
            (row[1], row[2], row[3], row[4], row[5])
            for row in connection.execute(
                'PRAGMA table_info("%s")' % table
            ).fetchall()
        )

    table_specs = {
        "n16_trend_support_states": (
            _N16_STATE_COLUMNS,
            _N16_STATE_TABLE_SQL,
        ),
        "n16_lifecycle_guard": (
            _N16_GUARD_COLUMNS,
            _N16_GUARD_TABLE_SQL,
        ),
        "n16_consumption_seals": (
            _N16_SEAL_COLUMNS,
            _N16_SEAL_TABLE_SQL,
        ),
        "n16_first_claim_witness": (
            _N16_WITNESS_COLUMNS,
            _N16_WITNESS_TABLE_SQL,
        ),
        "strategy_lifecycle_installations": (
            _N16_ROOT_COLUMNS,
            _N16_ROOT_TABLE_SQL,
        ),
    }
    for table, (expected_columns, expected_sql) in table_specs.items():
        if columns(table) != expected_columns:
            raise RuntimeError("N16 lifecycle table columns are inconsistent: %s" % table)
        table_sql = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
        if (
            table_sql is None
            or _n16_normalized_sql(table_sql[0])
            != _n16_normalized_sql(expected_sql)
        ):
            raise RuntimeError(
                "N16 lifecycle table constraints are inconsistent: %s" % table
            )
        table_xinfo = tuple(
            (row[1], row[2], row[3], row[4], row[5], row[6])
            for row in connection.execute(
                'PRAGMA table_xinfo("%s")' % table
            ).fetchall()
        )
        if table_xinfo != tuple(item + (0,) for item in expected_columns):
            raise RuntimeError(
                "N16 lifecycle hidden columns are inconsistent: %s" % table
            )
        if connection.execute(
            'PRAGMA foreign_key_list("%s")' % table
        ).fetchall():
            raise RuntimeError(
                "N16 lifecycle tables must not have foreign keys: %s" % table
            )
    event_xinfo = connection.execute("PRAGMA table_xinfo(events)").fetchall()
    if tuple(tuple(row[1:7]) for row in event_xinfo) != (
        ("id", "INTEGER", 0, None, 1, 0),
        ("occurred_at", "TEXT", 1, None, 0, 0),
        ("event_type", "TEXT", 1, None, 0, 0),
        ("symbol", "TEXT", 0, None, 0, 0),
        ("payload_json", "TEXT", 1, None, 0, 0),
        ("trade_review_id", "INTEGER", 0, None, 0, 0),
    ):
        raise RuntimeError("N16 terminal event columns are inconsistent")
    event_trade_columns = [
        tuple(row[1:7])
        for row in event_xinfo
        if type(row[1]) is str and row[1].lower() == "trade_review_id"
    ]
    if event_trade_columns != [
        ("trade_review_id", "INTEGER", 0, None, 0, 0)
    ]:
        raise RuntimeError("N16 terminal event identity column is inconsistent")
    event_table_sql = connection.execute(
        "SELECT sql FROM sqlite_schema WHERE type='table' AND name='events'"
    ).fetchall()
    if (
        len(event_table_sql) != 1
        or type(event_table_sql[0][0]) is not str
        or _n16_normalized_sql(event_table_sql[0][0])
        != _n16_normalized_sql(_N16_EVENTS_TABLE_SQL)
    ):
        raise RuntimeError("N16 terminal event table constraints are inconsistent")
    if connection.execute("PRAGMA foreign_key_list(events)").fetchall():
        raise RuntimeError("N16 terminal event table must not have foreign keys")
    event_index_rows = connection.execute("PRAGMA index_list(events)").fetchall()
    event_index_identity = {
        row[1]: tuple(row[2:5]) for row in event_index_rows
        if type(row[1]) is str
    }
    if event_index_identity != {
        "idx_events_type_time": (0, "c", 0),
        "idx_n16_terminal_event_trade": (1, "c", 1),
    }:
        raise RuntimeError("N16 terminal event indexes are inconsistent")
    event_index_xinfo = {
        "idx_events_type_time": (
            (0, 2, "event_type", 0, "BINARY", 1),
            (1, 1, "occurred_at", 0, "BINARY", 1),
            (2, -1, None, 0, "BINARY", 0),
        ),
        "idx_n16_terminal_event_trade": (
            (0, 5, "trade_review_id", 0, "BINARY", 1),
            (1, -1, None, 0, "BINARY", 0),
        ),
    }
    for index_name, expected_sql in _N16_EVENT_INDEX_SQL.items():
        index_sql = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type='index' AND name=?",
            (index_name,),
        ).fetchall()
        actual_xinfo = tuple(
            tuple(row[:6])
            for row in connection.execute(
                'PRAGMA index_xinfo("%s")' % index_name
            ).fetchall()
        )
        if (
            len(index_sql) != 1
            or type(index_sql[0][0]) is not str
            or _n16_normalized_sql(index_sql[0][0])
            != _n16_normalized_sql(expected_sql)
            or actual_xinfo != event_index_xinfo[index_name]
        ):
            raise RuntimeError(
                "N16 terminal event index metadata is inconsistent: %s"
                % index_name
            )
    guard_rows = connection.execute(
        "SELECT singleton_id, schema_version, rule_version, guard_sha256, "
        "catalog_schema_version, sealed_claim_count, confirmed_chain_sha256 "
        "FROM n16_lifecycle_guard ORDER BY singleton_id"
    ).fetchall()
    if (
        len(guard_rows) != 1
        or tuple(guard_rows[0][:4]) != _N16_GUARD_FIXED_ROW
        or type(guard_rows[0][4]) is not int
        or guard_rows[0][4] <= 0
        or type(guard_rows[0][5]) is not int
        or guard_rows[0][5] < 0
        or type(guard_rows[0][6]) is not str
        or len(guard_rows[0][6]) != 64
        or any(
            character not in "0123456789abcdef"
            for character in guard_rows[0][6]
        )
    ):
        raise RuntimeError("N16 lifecycle guard identity is inconsistent")
    root_rows = connection.execute(
        "SELECT singleton_id, strategy_id, schema_version, rule_version, "
        "guard_sha256, catalog_schema_version, sealed_claim_count, "
        "confirmed_chain_sha256, "
        "installed_at FROM strategy_lifecycle_installations "
        "ORDER BY singleton_id"
    ).fetchall()
    if (
        len(root_rows) != 1
        or tuple(root_rows[0][:5])
        != (
            1,
            "N16",
            1,
            "N16_V1",
            _N16_GUARD_FIXED_ROW[3],
        )
        or type(root_rows[0][5]) is not int
        or root_rows[0][5] <= 0
        or type(root_rows[0][6]) is not int
        or root_rows[0][6] < 0
        or type(root_rows[0][7]) is not str
        or len(root_rows[0][7]) != 64
        or any(
            character not in "0123456789abcdef"
            for character in root_rows[0][7]
        )
        or type(root_rows[0][8]) is not str
        or not root_rows[0][8]
        or root_rows[0][5] != guard_rows[0][4]
        or root_rows[0][6] != guard_rows[0][5]
        or root_rows[0][7] != guard_rows[0][6]
    ):
        raise RuntimeError("N16 lifecycle installation root is inconsistent")
    sequence_rows = connection.execute(
        "SELECT seq FROM sqlite_sequence "
        "WHERE name = 'n16_consumption_seals'"
    ).fetchall()
    if sequence_rows == []:
        seal_sequence = 0
    elif (
        len(sequence_rows) == 1
        and type(sequence_rows[0][0]) is int
        and sequence_rows[0][0] >= 0
    ):
        seal_sequence = sequence_rows[0][0]
    else:
        raise RuntimeError("N16 consumption sequence is inconsistent")
    if guard_rows[0][5] != seal_sequence:
        raise RuntimeError("N16 lifecycle high-water is inconsistent")
    witness_rows = connection.execute(
        "SELECT singleton_id, seal_ordinal, source_signal_id, schema_version, "
        "rule_version, strategy_id, source_scan_id, audit_id, ledger_id, "
        "state_id, symbol, episode_id, structure_id, "
        "signal_evidence_sha256, state_evidence_sha256, signal_created_at, "
        "claim_created_at FROM n16_first_claim_witness "
        "ORDER BY singleton_id"
    ).fetchall()
    if guard_rows[0][5] == 0:
        if witness_rows:
            raise RuntimeError("N16 first claim witness is inconsistent")
    else:
        if len(witness_rows) != 1:
            raise RuntimeError("N16 first claim witness is inconsistent")
        witness = witness_rows[0]
        if (
            type(witness) not in (tuple, list)
            or len(witness) != 17
            or type(witness[0]) is not int
            or witness[0] != 1
            or type(witness[1]) is not int
            or witness[1] != 1
            or any(
                type(witness[index]) is not int or witness[index] <= 0
                for index in (2, 6, 7, 8, 9)
            )
            or type(witness[3]) is not int
            or witness[3] != 1
            or type(witness[4]) is not str
            or witness[4] != "N16_V1"
            or type(witness[5]) is not str
            or witness[5] != "N16"
            or type(witness[10]) is not str
            or not 1 <= len(witness[10]) <= 64
            or any(
                type(witness[index]) is not str
                or len(witness[index]) != 24
                or any(
                    character not in "0123456789abcdef"
                    for character in witness[index]
                )
                for index in (11, 12)
            )
            or any(
                type(witness[index]) is not str
                or len(witness[index]) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in witness[index]
                )
                for index in (13, 14)
            )
            or any(
                type(witness[index]) is not str or not witness[index]
                for index in (15, 16)
            )
        ):
            raise RuntimeError("N16 first claim witness is inconsistent")
        first_seals = connection.execute(
            "SELECT seal_ordinal, source_signal_id, schema_version, "
            "rule_version, strategy_id, source_scan_id, audit_id, ledger_id, "
            "state_id, symbol, episode_id, structure_id, "
            "signal_evidence_sha256, state_evidence_sha256, "
            "signal_created_at, claim_created_at "
            "FROM n16_consumption_seals WHERE seal_ordinal = 1"
        ).fetchall()
        if len(first_seals) != 1 or any(
            type(witness[index + 1]) is not type(first_seals[0][index])
            or witness[index + 1] != first_seals[0][index]
            for index in range(16)
        ):
            raise RuntimeError("N16 first claim witness is inconsistent")
    current_schema_version_row = connection.execute(
        "PRAGMA schema_version"
    ).fetchone()
    if (
        current_schema_version_row is None
        or len(current_schema_version_row) != 1
        or type(current_schema_version_row[0]) is not int
        or current_schema_version_row[0] <= 0
    ):
        raise RuntimeError("N16 lifecycle catalog generation is invalid")

    for name, expected_sql in _N16_INDEX_SQL.items():
        rows = connection.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_schema "
            "WHERE lower(name) = lower(?)",
            (name,),
        ).fetchall()
        if (
            len(rows) != 1
            or rows[0][0] != "index"
            or rows[0][1] != name
            or rows[0][2]
            not in _N16_TABLES
            + (
                "events",
                "strategy_live_links",
                "strategy_paper_trades",
                "trade_reviews",
            )
            or _n16_normalized_sql(rows[0][3])
            != _n16_normalized_sql(expected_sql)
        ):
            raise RuntimeError("N16 lifecycle index is inconsistent: %s" % name)
    trend_index_list = connection.execute(
        "PRAGMA index_list(n16_trend_support_states)"
    ).fetchall()
    trend_index_identity = {
        row[1]: (row[2], row[3], row[4]) for row in trend_index_list
    }
    expected_trend_index_identity = {
        "idx_n16_trend_support_active": (0, "c", 0),
        "idx_n16_trend_support_structure": (1, "c", 1),
        "sqlite_autoindex_n16_trend_support_states_1": (1, "u", 0),
    }
    if trend_index_identity != expected_trend_index_identity:
        raise RuntimeError("N16 lifecycle index catalog is inconsistent")

    # Certify every named N16 index through all three SQLite views.  Exact SQL
    # alone is insufficient because a hostile same-name index can differ in
    # uniqueness, origin, partial status, collation, sort order, expression
    # use, or auxiliary rowid layout.
    expected_named_index_metadata = {
        "idx_n16_trend_support_active": (
            "n16_trend_support_states",
            (0, "c", 0),
            (
                (0, 1, "strategy_id", 0, "BINARY", 1),
                (1, 5, "stage", 0, "BINARY", 1),
                (2, 2, "symbol", 0, "BINARY", 1),
                (3, -1, None, 0, "BINARY", 0),
            ),
        ),
        "idx_n16_trend_support_structure": (
            "n16_trend_support_states",
            (1, "c", 1),
            (
                (0, 1, "strategy_id", 0, "BINARY", 1),
                (1, 4, "structure_id", 0, "BINARY", 1),
                (2, -1, None, 0, "BINARY", 0),
            ),
        ),
        "idx_n16_live_link_state_key": (
            "strategy_live_links",
            (0, "c", 0),
            (
                (0, 3, "symbol", 0, "BINARY", 1),
                (1, 4, "opened_at", 0, "BINARY", 1),
                (2, -1, None, 0, "BINARY", 0),
            ),
        ),
        "idx_n16_live_link_review": (
            "strategy_live_links",
            (0, "c", 0),
            (
                (0, 2, "trade_review_id", 0, "BINARY", 1),
                (1, -1, None, 0, "BINARY", 0),
            ),
        ),
        "idx_n16_trade_review_state_key": (
            "trade_reviews",
            (0, "c", 0),
            (
                (0, 3, "symbol", 0, "BINARY", 1),
                (1, 2, "opened_at", 0, "BINARY", 1),
                (2, -1, None, 0, "BINARY", 0),
            ),
        ),
        "idx_n16_review_pending_rows": (
            "trade_reviews",
            (0, "c", 1),
            (
                (0, 0, "id", 0, "BINARY", 1),
                (1, -1, None, 0, "BINARY", 0),
            ),
        ),
        "idx_n16_paper_open_rows": (
            "strategy_paper_trades",
            (0, "c", 1),
            (
                (0, 0, "id", 0, "BINARY", 1),
                (1, -1, None, 0, "BINARY", 0),
            ),
        ),
        "idx_n16_live_open_rows": (
            "strategy_live_links",
            (0, "c", 1),
            (
                (0, 0, "id", 0, "BINARY", 1),
                (1, -1, None, 0, "BINARY", 0),
            ),
        ),
        "idx_n16_terminal_event_trade": (
            "events",
            (1, "c", 1),
            (
                (0, 5, "trade_review_id", 0, "BINARY", 1),
                (1, -1, None, 0, "BINARY", 0),
            ),
        ),
    }
    for name, (table, expected_identity, expected_xinfo) in (
        expected_named_index_metadata.items()
    ):
        matching = [
            row
            for row in connection.execute(
                'PRAGMA index_list("%s")' % table
            ).fetchall()
            if row[1] == name
        ]
        if (
            len(matching) != 1
            or tuple(matching[0][2:5]) != expected_identity
        ):
            raise RuntimeError("N16 index identity is inconsistent: %s" % name)
        actual_xinfo = tuple(
            tuple(row[:6])
            for row in connection.execute(
                'PRAGMA index_xinfo("%s")' % name
            ).fetchall()
        )
        if actual_xinfo != expected_xinfo:
            raise RuntimeError("N16 index metadata is inconsistent: %s" % name)

    seal_indexes = connection.execute(
        "PRAGMA index_list(n16_consumption_seals)"
    ).fetchall()
    seal_index_identity = {
        row[1]: (row[2], row[3], row[4]) for row in seal_indexes
    }
    expected_seal_keys = {
        ("source_signal_id",),
        ("audit_id",),
        ("ledger_id",),
        ("state_id",),
        ("episode_id",),
        ("structure_id",),
    }
    if (
        len(seal_index_identity) != 6
        or any(value != (1, "u", 0) for value in seal_index_identity.values())
    ):
        raise RuntimeError("N16 consumption seal indexes are inconsistent")
    actual_seal_keys = {
        tuple(
            row[2]
            for row in connection.execute(
                'PRAGMA index_xinfo("%s")' % name
            ).fetchall()
            if row[5] == 1
        )
        for name in seal_index_identity
    }
    if actual_seal_keys != expected_seal_keys:
        raise RuntimeError("N16 consumption seal unique keys are inconsistent")
    if connection.execute(
        "PRAGMA index_list(n16_lifecycle_guard)"
    ).fetchall():
        raise RuntimeError("N16 lifecycle guard indexes are inconsistent")
    if connection.execute(
        "PRAGMA index_list(strategy_lifecycle_installations)"
    ).fetchall():
        raise RuntimeError("N16 lifecycle root indexes are inconsistent")
    if connection.execute(
        "PRAGMA index_list(n16_first_claim_witness)"
    ).fetchall():
        raise RuntimeError("N16 first claim witness indexes are inconsistent")

    trigger_rows = connection.execute(
        "SELECT name, tbl_name, sql FROM sqlite_schema "
        "WHERE type = 'trigger' AND ("
        "name GLOB 'trg_n16_*' OR tbl_name IN ("
        "'n16_trend_support_states', 'n16_lifecycle_guard', "
        "'n16_consumption_seals', "
        "'n16_first_claim_witness', "
        "'strategy_lifecycle_installations')) ORDER BY name"
    ).fetchall()
    if {row[0] for row in trigger_rows} != set(_N16_TRIGGER_SQL):
        raise RuntimeError("N16 lifecycle trigger catalog is inconsistent")
    for name, _table, sql in trigger_rows:
        if _n16_normalized_sql(sql) != _n16_normalized_sql(
            _N16_TRIGGER_SQL[name]
        ):
            raise RuntimeError("N16 lifecycle trigger is inconsistent: %s" % name)
    event_trigger_rows = connection.execute(
        "SELECT name, sql FROM sqlite_schema "
        "WHERE type='trigger' AND tbl_name='events' ORDER BY name"
    ).fetchall()
    expected_event_triggers = {
        "trg_n16_terminal_event_identity",
        "trg_n16_terminal_event_identity_update",
        "trg_n16_terminal_event_identity_delete",
    }
    if {row[0] for row in event_trigger_rows} != expected_event_triggers:
        raise RuntimeError("N16 terminal event trigger catalog is inconsistent")
    for name, sql in event_trigger_rows:
        if (
            type(sql) is not str
            or _n16_normalized_sql(sql)
            != _n16_normalized_sql(_N16_TRIGGER_SQL[name])
        ):
            raise RuntimeError("N16 terminal event trigger is inconsistent: %s" % name)

    _verify_n16_shared_execution_schema(connection)

    tables = connection.execute(
        "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name"
    ).fetchall()
    for table_row in tables:
        table_name = table_row[0]
        if type(table_name) is not str:
            raise RuntimeError("SQLite table catalog is invalid")
        for foreign_key in connection.execute(
            'PRAGMA foreign_key_list("%s")' % table_name
        ).fetchall():
            if (
                type(foreign_key[2]) is str
                and foreign_key[2].lower()
                in {name.lower() for name in _N16_TABLES}
            ):
                raise RuntimeError(
                    "N16 lifecycle tables must not have incoming foreign keys"
                )
    return True


def _install_n16_schema(connection: sqlite3.Connection) -> None:
    if _verify_n16_schema(connection, allow_absent=True):
        return
    connection.execute("SAVEPOINT install_n16_schema")
    try:
        event_columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(events)").fetchall()
        }
        if "trade_review_id" not in event_columns:
            connection.execute(
                "ALTER TABLE events ADD COLUMN trade_review_id INTEGER"
            )
        connection.execute(_N16_STATE_TABLE_SQL)
        connection.execute(_N16_GUARD_TABLE_SQL)
        connection.execute(_N16_SEAL_TABLE_SQL)
        connection.execute(_N16_WITNESS_TABLE_SQL)
        connection.execute(_N16_ROOT_TABLE_SQL)
        for sql in _N16_INDEX_SQL.values():
            connection.execute(sql)
        for sql in _N16_TRIGGER_SQL.values():
            connection.execute(sql)
        catalog_schema_version = connection.execute(
            "PRAGMA schema_version"
        ).fetchone()
        if (
            catalog_schema_version is None
            or len(catalog_schema_version) != 1
            or type(catalog_schema_version[0]) is not int
            or catalog_schema_version[0] <= 0
        ):
            raise RuntimeError("N16 lifecycle catalog generation is invalid")
        installed_at = utc_now()
        connection.execute(
            "INSERT INTO n16_lifecycle_guard ("
            "singleton_id, schema_version, rule_version, guard_sha256, "
            "catalog_schema_version, sealed_claim_count, "
            "confirmed_chain_sha256"
            ") VALUES (?, ?, ?, ?, ?, ?, ?)",
            _N16_GUARD_FIXED_ROW
            + (catalog_schema_version[0], 0, _N16_CLAIM_CHAIN_SEED),
        )
        connection.execute(
            "INSERT INTO strategy_lifecycle_installations ("
            "singleton_id, strategy_id, schema_version, rule_version, "
            "guard_sha256, catalog_schema_version, sealed_claim_count, "
            "confirmed_chain_sha256, "
            "installed_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                1,
                "N16",
                1,
                "N16_V1",
                _N16_GUARD_FIXED_ROW[3],
                catalog_schema_version[0],
                0,
                _N16_CLAIM_CHAIN_SEED,
                installed_at,
            ),
        )
        _verify_n16_schema(connection, allow_absent=False)
        connection.execute("RELEASE SAVEPOINT install_n16_schema")
    except Exception:
        connection.execute("ROLLBACK TO SAVEPOINT install_n16_schema")
        connection.execute("RELEASE SAVEPOINT install_n16_schema")
        raise


def _n16_table_exists(connection: sqlite3.Connection, table: str) -> bool:
    row = connection.execute(
        "SELECT type, name FROM sqlite_schema WHERE lower(name) = lower(?)",
        (table,),
    ).fetchall()
    if not row:
        return False
    if row != [("table", table)]:
        raise RuntimeError("N16 trace table identity is inconsistent: %s" % table)
    return True


def _n16_preinstall_trace_exists(connection: sqlite3.Connection) -> bool:
    """Prove that an absent N16 schema is genuinely pre-N16.

    This check is used only once, before the bounded empty schema install.  A
    database that contains any N16 definition, publication, execution, state,
    event, or sequence trace is post-N16 and must never be guessed/repaired by
    normal startup.
    """

    strategy_tables = (
        "strategy_definitions",
        "strategy_signals",
        "strategy_passed_signal_audits",
        "strategy_passed_structure_ledger",
        "strategy_paper_trades",
        "strategy_states",
        "strategy_live_links",
        "strategy_structure_terminal_states",
    )
    for table in strategy_tables:
        if _n16_table_exists(connection, table):
            row = connection.execute(
                'SELECT 1 FROM "%s" WHERE strategy_id = \'N16\' LIMIT 1'
                % table
            ).fetchone()
            if row is not None:
                return True
    if _n16_table_exists(connection, "events"):
        event = connection.execute(
            "SELECT 1 FROM events WHERE lower(event_type) GLOB 'n16*' LIMIT 1"
        ).fetchone()
        if event is not None:
            return True
    if _n16_table_exists(connection, "sqlite_sequence"):
        sequence = connection.execute(
            "SELECT 1 FROM sqlite_sequence WHERE name IN (%s) LIMIT 1"
            % ",".join("?" for _ in _N16_AUTOINCREMENT_TABLES),
            _N16_AUTOINCREMENT_TABLES,
        ).fetchone()
        if sequence is not None:
            return True
    return False


def _n16_runtime_schema_status(connection: sqlite3.Connection) -> str:
    if _verify_n16_schema(connection, allow_absent=True):
        guard_schema_version = connection.execute(
            "SELECT catalog_schema_version FROM n16_lifecycle_guard "
            "WHERE singleton_id = 1"
        ).fetchone()
        current_schema_version = _n16_catalog_schema_version(connection)
        if (
            guard_schema_version is None
            or len(guard_schema_version) != 1
            or type(guard_schema_version[0]) is not int
        ):
            raise RuntimeError("N16 lifecycle catalog generation is invalid")
        # Any catalog generation drift requires a full permanent-graph
        # attestation.  A ReviewRecorder write connection may then advance the
        # two independent roots under its connection-local authorization
        # function.  Read-only maintenance never mutates the attestation.
        if current_schema_version != guard_schema_version[0]:
            _validate_n16_permanent_graph(connection)
        return "CURRENT"
    # Ordinary startup must remain a bounded, read-only classification.  A
    # schema-absent Review file is never installed here: the constructor will
    # reject PRE_N16 below.  The stopped-service maintenance entrypoint owns
    # the intentionally O(n) proof that no N16 business trace exists before it
    # creates either side of the Review/claim-ledger pair.
    return "PRE_N16"


def _n16_catalog_schema_version(connection: sqlite3.Connection) -> int:
    row = connection.execute("PRAGMA schema_version").fetchone()
    if row is None or len(row) != 1 or type(row[0]) is not int or row[0] <= 0:
        raise RuntimeError("N16 lifecycle catalog generation is invalid")
    return row[0]


def _n16_attested_catalog_schema_version(
    connection: sqlite3.Connection,
) -> int:
    rows = connection.execute(
        "SELECT catalog_schema_version FROM n16_lifecycle_guard "
        "WHERE singleton_id = 1"
    ).fetchall()
    if (
        len(rows) != 1
        or type(rows[0][0]) is not int
        or rows[0][0] <= 0
    ):
        raise RuntimeError("N16 lifecycle catalog generation is invalid")
    return rows[0][0]


def _validated_n16_consumption_seal(row: Any) -> tuple[Any, ...]:
    if type(row) not in (tuple, list) or len(row) != 15:
        raise RuntimeError("N16 consumption seal is invalid")
    value = tuple(row)
    if (
        type(value[0]) is not int
        or value[0] <= 0
        or value[1] != 1
        or type(value[1]) is not int
        or value[2] != "N16_V1"
        or type(value[2]) is not str
        or value[3] != "N16"
        or type(value[3]) is not str
        or any(type(value[index]) is not int or value[index] <= 0
               for index in (4, 5, 6, 7))
        or type(value[8]) is not str
        or not value[8]
        or any(
            type(value[index]) is not str
            or len(value[index]) != 24
            or any(
                character not in "0123456789abcdef"
                for character in value[index]
            )
            for index in (9, 10)
        )
        or any(
            type(value[index]) is not str
            or len(value[index]) != 64
            or any(
                character not in "0123456789abcdef"
                for character in value[index]
            )
            for index in (11, 12)
        )
        or type(value[13]) is not str
        or not value[13]
        or type(value[14]) is not str
        or not value[14]
    ):
        raise RuntimeError("N16 consumption seal identity is invalid")
    return value


def _n16_consumption_seal_row(
    connection: sqlite3.Connection,
    structure_id: str,
) -> tuple[Any, ...] | None:
    rows = connection.execute(
        """
        SELECT source_signal_id, schema_version, rule_version, strategy_id,
               source_scan_id, audit_id, ledger_id, state_id, symbol,
               episode_id, structure_id, signal_evidence_sha256,
               state_evidence_sha256, signal_created_at, claim_created_at
        FROM n16_consumption_seals
        WHERE strategy_id = 'N16' AND structure_id = ?
        """,
        (structure_id,),
    ).fetchall()
    if len(rows) > 1:
        raise RuntimeError("N16 consumption seal is not unique")
    return _validated_n16_consumption_seal(rows[0]) if rows else None


def _validate_n16_consumption_seal(
    connection: sqlite3.Connection,
    symbol: str,
    structure_id: str,
) -> tuple[Any, ...]:
    seal = _n16_consumption_seal_row(connection, structure_id)
    if seal is None:
        raise RuntimeError("N16 ACTIVE claim consumption seal is missing")
    state_rows = connection.execute(
        """
        SELECT id, strategy_id, symbol, episode_id, structure_id,
               stage, reason, quote_volume_rank, evidence_json,
               evidence_sha256, created_at, updated_at
        FROM n16_trend_support_states WHERE id = ?
        """,
        (seal[7],),
    ).fetchall()
    if len(state_rows) != 1:
        raise RuntimeError("N16 consumed lifecycle state is missing")
    state = _validated_n16_state_row(state_rows[0])
    ledger_rows = connection.execute(
        """
        SELECT id, strategy_id, symbol, structure_id, source_signal_id,
               source_scan_id, source_signal_created_at, evidence_sha256,
               created_at, claim_state
        FROM strategy_passed_structure_ledger
        WHERE strategy_id = 'N16' AND structure_id = ?
        """,
        (structure_id,),
    ).fetchall()
    if len(ledger_rows) != 1:
        raise RuntimeError("N16 ACTIVE ledger is missing")
    ledger = ledger_rows[0]
    _validate_active_structure_ledger(
        connection, ledger, "N16", symbol, structure_id
    )
    audit_rows = connection.execute(
        """
        SELECT id, source_signal_id, source_scan_id, strategy_id, symbol,
               funding_rate, matched_patterns, trend_slope, current_bullish,
               passed, decision, reason, structure_id, detail_json,
               signal_created_at, evidence_sha256, created_at, claim_state
        FROM strategy_passed_signal_audits WHERE source_signal_id = ?
        """,
        (seal[0],),
    ).fetchall()
    if len(audit_rows) != 1:
        raise RuntimeError("N16 ACTIVE audit is missing")
    audit_id = audit_rows[0][0]
    audit, signal_evidence_sha256 = _validated_active_passed_audit(
        audit_rows[0][1:]
    )
    if (
        seal[8] != symbol
        or seal[10] != structure_id
        or state.id != seal[7]
        or state.symbol != seal[8]
        or state.episode_id != seal[9]
        or state.structure_id != seal[10]
        or state.stage != "CONFIRMED"
        or state.reason != "PASSED"
        or state.evidence_sha256 != seal[12]
        or ledger[0] != seal[6]
        or ledger[4] != seal[0]
        or ledger[5] != seal[4]
        or ledger[6] != seal[13]
        or ledger[7] != seal[11]
        or ledger[8] != seal[14]
        or audit_id != seal[5]
        or audit[0] != seal[0]
        or audit[1] != seal[4]
        or audit[2] != "N16"
        or audit[3] != seal[8]
        or audit[11] != seal[10]
        or audit[13] != seal[13]
        or audit[15] != seal[14]
        or signal_evidence_sha256 != seal[11]
    ):
        raise RuntimeError("N16 permanent consumption graph conflicts")
    _validate_n16_passed_lifecycle(
        connection, symbol, structure_id, audit[12]
    )
    return seal


def _validate_n16_permanent_graph(connection: sqlite3.Connection) -> None:
    """Re-attest historical N16 claims after any non-VACUUM DDL drift.

    The normal startup path is O(1): exact catalog plus the installation
    schema cookie (or the one-cookie VACUUM INTO copy).  A wider cookie change
    means some DDL occurred, so every permanent N16 seal is revalidated once
    before a write connection may enable WAL.  This catches a trigger that was
    dropped, followed by row damage and an exact trigger recreation, without
    imposing an unbounded scan on ordinary restarts.
    """

    from .n16_claim_ledger import (
        N16_CLAIM_CHAIN_SEED,
        advance_review_claim_chain,
        validate_review_claim,
    )

    active_counts = connection.execute(
        "SELECT "
        "(SELECT COUNT(*) FROM strategy_passed_signal_audits "
        " WHERE strategy_id='N16' AND claim_state='ACTIVE'), "
        "(SELECT COUNT(*) FROM strategy_passed_structure_ledger "
        " WHERE strategy_id='N16' AND claim_state='ACTIVE'), "
        "(SELECT COUNT(*) FROM n16_consumption_seals), "
        "(SELECT sealed_claim_count FROM n16_lifecycle_guard "
        " WHERE singleton_id=1), "
        "(SELECT sealed_claim_count FROM strategy_lifecycle_installations "
        " WHERE singleton_id=1 AND strategy_id='N16')"
    ).fetchone()
    if (
        active_counts is None
        or len(active_counts) != 5
        or any(type(value) is not int or value < 0 for value in active_counts)
        or active_counts[0] != active_counts[1]
        or active_counts[0] != active_counts[2]
        or active_counts[0] != active_counts[3]
        or active_counts[0] != active_counts[4]
    ):
        raise RuntimeError("N16 permanent consumption graph count conflicts")
    seals = connection.execute(
        "SELECT seal_ordinal, source_signal_id, schema_version, rule_version, "
        "strategy_id, source_scan_id, audit_id, ledger_id, state_id, symbol, "
        "episode_id, structure_id, signal_evidence_sha256, "
        "state_evidence_sha256, signal_created_at, claim_created_at "
        "FROM n16_consumption_seals ORDER BY seal_ordinal"
    ).fetchall()
    if len(seals) != active_counts[2]:
        raise RuntimeError("N16 permanent consumption graph is incomplete")
    chain = N16_CLAIM_CHAIN_SEED
    for expected_ordinal, row in enumerate(seals, 1):
        try:
            claim = validate_review_claim(row)
        except Exception as exc:
            raise RuntimeError("N16 permanent consumption identity is invalid")
        if claim[0] != expected_ordinal:
            raise RuntimeError("N16 permanent claim order is inconsistent")
        _validate_n16_consumption_seal(connection, claim[9], claim[11])
        chain = advance_review_claim_chain(chain, claim)
    heads = connection.execute(
        "SELECT "
        "(SELECT confirmed_chain_sha256 FROM n16_lifecycle_guard "
        " WHERE singleton_id=1), "
        "(SELECT confirmed_chain_sha256 "
        " FROM strategy_lifecycle_installations "
        " WHERE singleton_id=1 AND strategy_id='N16')"
    ).fetchone()
    if heads != (chain, chain):
        raise RuntimeError("N16 permanent claim chain conflicts")


def _n16_json_payload_has_marker(raw_value: Any, label: str) -> bool:
    """Strictly classify one bounded generic JSON payload for N16 markers."""

    if type(raw_value) is not str or len(raw_value.encode("utf-8")) > 1_048_576:
        raise RuntimeError("N16 %s JSON is invalid" % label)

    def strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result = {}  # type: dict[str, Any]
        for key, value in pairs:
            if type(key) is not str or key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        value = json.loads(
            raw_value,
            object_pairs_hook=strict_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite JSON number")
            ),
        )
    except (TypeError, ValueError) as exc:
        raise RuntimeError("N16 %s JSON is invalid" % label) from exc
    seen = 0

    def walk(item: Any, depth: int) -> bool:
        nonlocal seen
        seen += 1
        if depth > 32 or seen > 50_000:
            raise RuntimeError("N16 %s JSON is unbounded" % label)
        if type(item) is dict:
            for marker_key in ("strategy_id", "stop_mode", "rule_version"):
                if (
                    marker_key in item
                    and item[marker_key] is not None
                    and type(item[marker_key]) is not str
                ):
                    raise RuntimeError("N16 %s marker is invalid" % label)
            if item.get("strategy_id") == "N16":
                return True
            if (
                item.get("stop_mode")
                == "trend_support_continuation_margin_capped"
                or item.get("rule_version") == "N16_V1"
            ):
                return True
            return any(walk(child, depth + 1) for child in item.values())
        if type(item) is list:
            return any(walk(child, depth + 1) for child in item)
        if type(item) is float and not math.isfinite(item):
            raise RuntimeError("N16 %s JSON is invalid" % label)
        if type(item) not in {str, int, float, bool, type(None)}:
            raise RuntimeError("N16 %s JSON is invalid" % label)
        return False

    return walk(value, 0)


def _validate_n16_preinstall_review_clean(
    connection: sqlite3.Connection,
) -> None:
    """Stopped-service full preinstall scan before any N16 write.

    Ordinary startup intentionally does not call this O(n) JSON scan.  It is
    reserved for the explicit maintenance install while the service is
    stopped and before the independent ledger or Review schema is created.
    """

    _verify_n16_preinstall_shared_execution_schema(connection)

    if _n16_table_exists(connection, "events"):
        event_xinfo = tuple(
            tuple(row[1:7])
            for row in connection.execute("PRAGMA table_xinfo(events)").fetchall()
        )
        if event_xinfo != (
            ("id", "INTEGER", 0, None, 1, 0),
            ("occurred_at", "TEXT", 1, None, 0, 0),
            ("event_type", "TEXT", 1, None, 0, 0),
            ("symbol", "TEXT", 0, None, 0, 0),
            ("payload_json", "TEXT", 1, None, 0, 0),
        ):
            raise RuntimeError("pre-N16 events columns are inconsistent")
        event_sql = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name='events'"
        ).fetchall()
        if (
            len(event_sql) != 1
            or type(event_sql[0][0]) is not str
            or _n16_normalized_sql(event_sql[0][0])
            != _n16_normalized_sql(_N16_PREINSTALL_EVENTS_TABLE_SQL)
            or connection.execute("PRAGMA foreign_key_list(events)").fetchall()
        ):
            raise RuntimeError("pre-N16 events constraints are inconsistent")
        event_indexes = connection.execute("PRAGMA index_list(events)").fetchall()
        if (
            len(event_indexes) != 1
            or event_indexes[0][1] != "idx_events_type_time"
            or tuple(event_indexes[0][2:5]) != (0, "c", 0)
        ):
            raise RuntimeError("pre-N16 events indexes are inconsistent")
        legacy_index_sql = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type='index' "
            "AND name='idx_events_type_time'"
        ).fetchall()
        legacy_index_xinfo = tuple(
            tuple(row[:6])
            for row in connection.execute(
                "PRAGMA index_xinfo(idx_events_type_time)"
            ).fetchall()
        )
        if (
            len(legacy_index_sql) != 1
            or type(legacy_index_sql[0][0]) is not str
            or _n16_normalized_sql(legacy_index_sql[0][0])
            != _n16_normalized_sql(_N16_EVENT_INDEX_SQL["idx_events_type_time"])
            or legacy_index_xinfo
            != (
                (0, 2, "event_type", 0, "BINARY", 1),
                (1, 1, "occurred_at", 0, "BINARY", 1),
                (2, -1, None, 0, "BINARY", 0),
            )
        ):
            raise RuntimeError("pre-N16 events index metadata is inconsistent")
        if connection.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='trigger' "
            "AND tbl_name='events' LIMIT 1"
        ).fetchone() is not None:
            raise RuntimeError("pre-N16 events triggers are inconsistent")
    if _n16_preinstall_trace_exists(connection):
        raise RuntimeError("pre-N16 Review contains N16 lifecycle evidence")
    if _n16_table_exists(connection, "events"):
        for event_type, payload_json in connection.execute(
            "SELECT event_type,payload_json FROM events ORDER BY id"
        ):
            if (
                type(event_type) is not str
                or not event_type
                or event_type.lower().startswith("n16")
                or _n16_json_payload_has_marker(payload_json, "event")
            ):
                raise RuntimeError("pre-N16 Review contains event evidence")
    if _n16_table_exists(connection, "trade_reviews"):
        for (orders_json,) in connection.execute(
            "SELECT orders_json FROM trade_reviews ORDER BY id"
        ):
            if _n16_json_payload_has_marker(orders_json, "trade review"):
                raise RuntimeError("pre-N16 Review contains trade evidence")


def _validate_n16_installing_review_empty(
    connection: sqlite3.Connection,
) -> None:
    """Prove a CURRENT Review file is a pristine interrupted install.

    Installation roots/guards are expected to exist at this point.  Every
    business/lifecycle trace outside those fixed metadata rows must still be
    absent; a legitimate interrupted first install cannot have created an N16
    episode, signal, execution, terminal event, claim, or sequence history.
    """

    _validate_n16_permanent_graph(connection)
    strategy_tables = (
        "strategy_definitions",
        "strategy_signals",
        "strategy_passed_signal_audits",
        "strategy_passed_structure_ledger",
        "strategy_paper_trades",
        "strategy_states",
        "strategy_live_links",
        "strategy_structure_terminal_states",
    )
    for table in strategy_tables:
        if _n16_table_exists(connection, table) and connection.execute(
            'SELECT 1 FROM "%s" WHERE strategy_id = \'N16\' LIMIT 1'
            % table
        ).fetchone() is not None:
            raise RuntimeError(
                "N16 interrupted installation contains business evidence"
            )
    for table in (
        "n16_trend_support_states",
        "n16_consumption_seals",
        "n16_first_claim_witness",
    ):
        if connection.execute(
            'SELECT 1 FROM "%s" LIMIT 1' % table
        ).fetchone() is not None:
            raise RuntimeError(
                "N16 interrupted installation contains lifecycle evidence"
            )

    for event_type, payload_json in connection.execute(
        "SELECT event_type,payload_json FROM events ORDER BY id"
    ):
        if (
            type(event_type) is not str
            or not event_type
            or event_type.lower().startswith("n16")
            or _n16_json_payload_has_marker(payload_json, "event")
        ):
            raise RuntimeError(
                "N16 interrupted installation contains event evidence"
            )
    for (orders_json,) in connection.execute(
        "SELECT orders_json FROM trade_reviews ORDER BY id"
    ):
        if _n16_json_payload_has_marker(orders_json, "trade review"):
            raise RuntimeError(
                "N16 interrupted installation contains trade evidence"
            )
    sequence_rows = connection.execute(
        "SELECT name,seq FROM sqlite_sequence WHERE name IN (%s)"
        % ",".join("?" for _ in _N16_AUTOINCREMENT_TABLES),
        _N16_AUTOINCREMENT_TABLES,
    ).fetchall()
    if sequence_rows:
        raise RuntimeError(
            "N16 interrupted installation contains sequence evidence"
        )


def _n16_review_claim_row(
    connection: sqlite3.Connection,
    ordinal: int,
) -> tuple[Any, ...] | None:
    from .n16_claim_ledger import validate_review_claim

    if type(ordinal) is not int or ordinal <= 0:
        raise RuntimeError("N16 claim ordinal is invalid")
    rows = connection.execute(
        "SELECT seal_ordinal, source_signal_id, schema_version, rule_version, "
        "strategy_id, source_scan_id, audit_id, ledger_id, state_id, symbol, "
        "episode_id, structure_id, signal_evidence_sha256, "
        "state_evidence_sha256, signal_created_at, claim_created_at "
        "FROM n16_consumption_seals WHERE seal_ordinal = ?",
        (ordinal,),
    ).fetchall()
    if len(rows) > 1:
        raise RuntimeError("N16 claim ordinal is not unique")
    return validate_review_claim(rows[0]) if rows else None


def _n16_review_claim_summary(
    connection: sqlite3.Connection,
):
    _verify_n16_schema(connection, allow_absent=False)
    return _n16_review_claim_summary_after_schema_attestation(connection)


def _n16_review_claim_summary_after_schema_attestation(
    connection: sqlite3.Connection,
):
    """Read the fixed high-water summary after this catalog was certified.

    Callers must either have completed the full N16 schema verification on
    this connection or have compared ``PRAGMA schema_version`` with the
    recorder's previously certified catalog generation.  The summary itself
    is deliberately re-read on every operation; it is never cached.
    """

    from .n16_claim_ledger import (
        N16ReviewClaimSummary,
        review_claim_sha256,
        review_install_sha256,
    )

    root_rows = connection.execute(
        "SELECT strategy_id,schema_version,rule_version,guard_sha256,"
        "installed_at,singleton_id FROM strategy_lifecycle_installations "
        "WHERE singleton_id=1 AND strategy_id='N16'"
    ).fetchall()
    guard_rows = connection.execute(
        "SELECT sealed_claim_count, confirmed_chain_sha256 "
        "FROM n16_lifecycle_guard "
        "WHERE singleton_id=1"
    ).fetchall()
    root_chain_rows = connection.execute(
        "SELECT confirmed_chain_sha256 "
        "FROM strategy_lifecycle_installations "
        "WHERE singleton_id=1 AND strategy_id='N16'"
    ).fetchall()
    if (
        len(root_rows) != 1
        or len(guard_rows) != 1
        or len(root_chain_rows) != 1
        or type(guard_rows[0][0]) is not int
        or guard_rows[0][0] < 0
        or type(guard_rows[0][1]) is not str
        or len(guard_rows[0][1]) != 64
        or any(
            character not in "0123456789abcdef"
            for character in guard_rows[0][1]
        )
        or root_chain_rows[0][0] != guard_rows[0][1]
    ):
        raise RuntimeError("N16 lifecycle high-water is invalid")
    count = guard_rows[0][0]
    first = _n16_review_claim_row(connection, 1) if count else None
    last = _n16_review_claim_row(connection, count) if count else None
    if (count and (first is None or last is None)) or (
        not count and (first is not None or last is not None)
    ):
        raise RuntimeError("N16 lifecycle high-water evidence is incomplete")
    return N16ReviewClaimSummary(
        review_install_sha256=review_install_sha256(root_rows[0]),
        confirmed_claim_count=count,
        first_claim_sha256=(review_claim_sha256(first) if first else None),
        last_claim_sha256=(review_claim_sha256(last) if last else None),
        confirmed_chain_sha256=guard_rows[0][1],
    )


def _n16_all_review_claims(
    connection: sqlite3.Connection,
) -> tuple[tuple[Any, ...], ...]:
    from .n16_claim_ledger import validate_review_claim

    rows = connection.execute(
        "SELECT seal_ordinal, source_signal_id, schema_version, rule_version, "
        "strategy_id, source_scan_id, audit_id, ledger_id, state_id, symbol, "
        "episode_id, structure_id, signal_evidence_sha256, "
        "state_evidence_sha256, signal_created_at, claim_created_at "
        "FROM n16_consumption_seals ORDER BY seal_ordinal"
    ).fetchall()
    claims = tuple(validate_review_claim(row) for row in rows)
    if any(claim[0] != expected for expected, claim in enumerate(claims, 1)):
        raise RuntimeError("N16 lifecycle claim ordinals are not contiguous")
    return claims


def _n16_staged_review_claims(
    connection: sqlite3.Connection,
    scan_id: int,
    base_count: int,
) -> tuple[tuple[Any, ...], ...]:
    from .n16_claim_ledger import validate_review_claim

    if (
        type(scan_id) is not int
        or scan_id <= 0
        or type(base_count) is not int
        or base_count < 0
    ):
        raise RuntimeError("N16 staged claim identity is invalid")
    rows = connection.execute(
        """
        SELECT audit.source_signal_id, 1, 'N16_V1', 'N16',
               audit.source_scan_id, audit.id, ledger.id, state.id,
               state.symbol, state.episode_id, state.structure_id,
               audit.evidence_sha256, state.evidence_sha256,
               audit.signal_created_at, ledger.created_at, audit.detail_json
        FROM strategy_passed_structure_ledger AS ledger
        JOIN strategy_passed_signal_audits AS audit
          ON audit.source_signal_id=ledger.source_signal_id
         AND audit.strategy_id=ledger.strategy_id
         AND audit.structure_id=ledger.structure_id
         AND audit.claim_state=ledger.claim_state
        JOIN n16_trend_support_states AS state
          ON state.strategy_id=ledger.strategy_id
         AND state.symbol=ledger.symbol
         AND state.structure_id=ledger.structure_id
        WHERE ledger.strategy_id='N16'
          AND ledger.source_scan_id=?
          AND ledger.claim_state='STAGED'
        ORDER BY audit.source_signal_id
        """,
        (scan_id,),
    ).fetchall()
    claims = []
    for offset, row in enumerate(rows, 1):
        if type(row) not in (tuple, list) or len(row) != 16:
            raise RuntimeError("N16 staged claim shape is invalid")
        _validate_n16_passed_lifecycle(
            connection,
            row[8],
            row[10],
            row[15],
        )
        claim = validate_review_claim(
            (base_count + offset,) + tuple(row[:15])
        )
        claims.append(claim)
    return tuple(claims)


def _n16_review_publication_baseline(
    connection: sqlite3.Connection,
    scan_id: int,
    retention_row: tuple[Any, ...],
    batch_row: tuple[Any, ...],
    batch_snapshot: tuple[Any, ...],
    claim_snapshot: tuple[int, int],
    n16_claims: tuple[tuple[Any, ...], ...],
):
    from .n16_claim_ledger import N16ReviewPublicationBaseline

    if (
        type(scan_id) is not int
        or scan_id <= 0
        or type(retention_row) is not tuple
        or len(retention_row) != 9
        or type(batch_row) is not tuple
        or len(batch_row) != 10
        or type(batch_snapshot) is not tuple
        or type(claim_snapshot) is not tuple
        or len(claim_snapshot) != 2
        or type(n16_claims) is not tuple
    ):
        raise RuntimeError("N16 publication baseline shape is invalid")
    current_scan_id = retention_row[0]
    if current_scan_id is None:
        current_batch = None
        current_manifest = _SIGNAL_MANIFEST_SEED
    else:
        if type(current_scan_id) is not int or current_scan_id <= 0:
            raise RuntimeError("N16 publication current pointer is invalid")
        current_batch = connection.execute(
            "SELECT scan_id,state,recorded_count,expected_count,"
            "first_signal_id,last_signal_id,manifest_sha256,completed_at,"
            "created_at,updated_at FROM strategy_signal_batches "
            "WHERE scan_id=?",
            (current_scan_id,),
        ).fetchone()
        if (
            current_batch is None
            or len(current_batch) != 10
            or current_batch[0] != current_scan_id
            or current_batch[1] != "CURRENT"
            or type(current_batch[6]) is not str
            or len(current_batch[6]) != 64
            or any(
                character not in "0123456789abcdef"
                for character in current_batch[6]
            )
        ):
            raise RuntimeError("N16 publication current batch is invalid")
        current_batch = tuple(current_batch)
        current_manifest = current_batch[6]
    return N16ReviewPublicationBaseline(
        current_scan_id=current_scan_id,
        current_manifest_sha256=current_manifest,
        retention_sha256=_n16_typed_snapshot_sha256(
            (retention_row, current_batch, batch_row)
        ),
        staging_sha256=_n16_staging_publication_sha256(
            batch_row, batch_snapshot, retention_row
        ),
        passed_claims_sha256=_n16_passed_claims_snapshot_sha256(
            connection, scan_id, claim_snapshot, n16_claims
        ),
    )


def _n16_staging_publication_sha256(
    batch_row: tuple[Any, ...],
    batch_snapshot: tuple[Any, ...],
    retention_row: tuple[Any, ...],
) -> str:
    if (
        type(batch_row) is not tuple
        or len(batch_row) != 10
        or type(batch_snapshot) is not tuple
        or len(batch_snapshot) != 4
        or type(retention_row) is not tuple
        or len(retention_row) != 9
    ):
        raise RuntimeError("N16 staging publication identity is invalid")
    # Only fields that remain immutable after STAGING is promoted to CURRENT
    # enter this digest.  State/completion/update fields are separately
    # constrained by the classifier for the phase being proven.
    stable_batch = (
        batch_row[0],
        batch_row[2],
        batch_row[4],
        batch_row[5],
        batch_row[6],
        batch_row[8],
    )
    # CURRENT pointer and updated_at legitimately change at promotion.  Every
    # other retention provenance field is immutable across the publication and
    # must remain part of both SAFE_ABORT and SAFE_COMMIT classification.
    retention_marker = (
        retention_row[1],
        retention_row[2],
        retention_row[3],
        retention_row[4],
        retention_row[5],
        retention_row[6],
        retention_row[8],
    )
    return _n16_typed_snapshot_sha256(
        (stable_batch, batch_snapshot, retention_marker)
    )


def _n16_passed_claims_snapshot_sha256(
    connection: sqlite3.Connection,
    scan_id: int,
    claim_snapshot: tuple[int, int],
    n16_claims: tuple[tuple[Any, ...], ...],
) -> str:
    if (
        type(scan_id) is not int
        or scan_id <= 0
        or type(claim_snapshot) is not tuple
        or len(claim_snapshot) != 2
        or type(n16_claims) is not tuple
    ):
        raise RuntimeError("N16 passed claim snapshot identity is invalid")
    audit_rows = tuple(
        tuple(row)
        for row in connection.execute(
            "SELECT id,source_signal_id,source_scan_id,strategy_id,symbol,"
            "funding_rate,matched_patterns,trend_slope,current_bullish,passed,"
            "decision,reason,structure_id,detail_json,signal_created_at,"
            "evidence_sha256,created_at "
            "FROM strategy_passed_signal_audits "
            "WHERE source_scan_id=? ORDER BY source_signal_id",
            (scan_id,),
        ).fetchall()
    )
    ledger_rows = tuple(
        tuple(row)
        for row in connection.execute(
            "SELECT id,strategy_id,symbol,structure_id,source_signal_id,"
            "source_scan_id,source_signal_created_at,evidence_sha256,"
            "created_at "
            "FROM strategy_passed_structure_ledger "
            "WHERE source_scan_id=? ORDER BY source_signal_id",
            (scan_id,),
        ).fetchall()
    )
    return _n16_typed_snapshot_sha256(
        (claim_snapshot, audit_rows, ledger_rows, n16_claims)
    )


def _classify_prepared_n16_review_connection(
    connection: sqlite3.Connection,
    claim_ledger: Any,
    prepared: Any,
) -> tuple[str, Any | None]:
    """Classify a pending publication without mutating either database."""

    from .n16_claim_ledger import N16PreparedPublication

    if type(prepared) is not N16PreparedPublication:
        return "UNKNOWN", None
    try:
        external_claims, target_summary = claim_ledger.prepared_target(
            prepared
        )
        retention_row = connection.execute(
            "SELECT current_scan_id, retention_active, migration_state, "
            "migration_cutoff_signal_id, source_signal_count, "
            "source_passed_count, source_manifest_sha256, updated_at, "
            "retention_origin FROM strategy_signal_current "
            "WHERE singleton_id=1"
        ).fetchone()
        batch_row = connection.execute(
            "SELECT scan_id,state,recorded_count,expected_count,"
            "first_signal_id,last_signal_id,manifest_sha256,completed_at,"
            "created_at,updated_at FROM strategy_signal_batches "
            "WHERE scan_id=?",
            (prepared.scan_id,),
        ).fetchone()
        if (
            retention_row is None
            or len(retention_row) != 9
            or batch_row is None
            or len(batch_row) != 10
        ):
            return "UNKNOWN", None
        retention_row = tuple(retention_row)
        batch_row = tuple(batch_row)
        review_summary = _n16_review_claim_summary(connection)

        if review_summary == prepared.base_summary:
            retention = (
                retention_row[0], retention_row[1], retention_row[2],
                retention_row[3], retention_row[4], retention_row[5],
                retention_row[6], retention_row[8],
            )
            if retention_row[0] == prepared.scan_id:
                return "UNKNOWN", None
            _validate_complete_strategy_signal_graph(connection, retention)
            if batch_row[1] != "STAGING" or batch_row[7] is not None:
                return "UNKNOWN", None
            batch_snapshot = _strategy_signal_batch_snapshot(
                connection, prepared.scan_id
            )
            claim_snapshot = _strategy_signal_claim_snapshot(
                connection, prepared.scan_id, "STAGED"
            )
            staged_claims = _n16_staged_review_claims(
                connection,
                prepared.scan_id,
                prepared.base_summary.confirmed_claim_count,
            )
            active = connection.execute(
                "SELECT "
                "(SELECT COUNT(*) FROM strategy_passed_signal_audits "
                " WHERE strategy_id='N16' AND source_scan_id=? "
                " AND claim_state='ACTIVE'), "
                "(SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                " WHERE strategy_id='N16' AND source_scan_id=? "
                " AND claim_state='ACTIVE'), "
                "(SELECT COUNT(*) FROM n16_consumption_seals "
                " WHERE seal_ordinal>?)",
                (
                    prepared.scan_id,
                    prepared.scan_id,
                    prepared.base_summary.confirmed_claim_count,
                ),
            ).fetchone()
            recomputed = _n16_review_publication_baseline(
                connection,
                prepared.scan_id,
                retention_row,
                batch_row,
                tuple(batch_snapshot),
                tuple(claim_snapshot),
                tuple(staged_claims),
            )
            if (
                staged_claims == external_claims
                and active == (0, 0, 0)
                and recomputed == prepared.review_baseline
            ):
                return "SAFE_ABORT", prepared.base_summary
            return "UNKNOWN", None

        if review_summary == target_summary:
            retention = (
                retention_row[0], retention_row[1], retention_row[2],
                retention_row[3], retention_row[4], retention_row[5],
                retention_row[6], retention_row[8],
            )
            if retention_row[0] != prepared.scan_id:
                return "UNKNOWN", None
            _validate_complete_strategy_signal_graph(connection, retention)
            if (
                batch_row[1] != "CURRENT"
                or batch_row[2] != batch_row[3]
                or batch_row[6] != prepared.batch_sha256
                or type(batch_row[7]) is not str
                or not batch_row[7]
            ):
                return "UNKNOWN", None
            batch_snapshot = _strategy_signal_published_snapshot(
                connection, prepared.scan_id
            )
            if (
                _n16_staging_publication_sha256(
                    batch_row, tuple(batch_snapshot), retention_row
                )
                != prepared.review_baseline.staging_sha256
            ):
                return "UNKNOWN", None
            claim_snapshot = _strategy_signal_claim_snapshot(
                connection, prepared.scan_id, "ACTIVE"
            )
            review_claims = tuple(
                _n16_review_claim_row(
                    connection,
                    prepared.base_summary.confirmed_claim_count + offset,
                )
                for offset in range(1, prepared.claim_count + 1)
            )
            if (
                review_claims != external_claims
                or _n16_passed_claims_snapshot_sha256(
                    connection,
                    prepared.scan_id,
                    tuple(claim_snapshot),
                    tuple(review_claims),
                )
                != prepared.review_baseline.passed_claims_sha256
            ):
                return "UNKNOWN", None
            staged = connection.execute(
                "SELECT "
                "(SELECT COUNT(*) FROM strategy_passed_signal_audits "
                " WHERE source_scan_id=? AND claim_state='STAGED'), "
                "(SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                " WHERE source_scan_id=? AND claim_state='STAGED')",
                (prepared.scan_id, prepared.scan_id),
            ).fetchone()
            if staged == (0, 0):
                return "SAFE_COMMIT", target_summary
    except Exception:
        return "UNKNOWN", None
    return "UNKNOWN", None


def _validated_n16_state_row(row: Any) -> N16TrendSupportState:
    from .n16_analyzer import loads_n16_state_envelope

    if type(row) not in (tuple, list) or len(row) != 12:
        raise RuntimeError("N16 state row is invalid")
    value = tuple(row)
    if (
        type(value[0]) is not int
        or value[0] <= 0
        or value[1] != "N16"
        or type(value[1]) is not str
        or type(value[2]) is not str
        or not value[2]
        or type(value[3]) is not str
        or (value[4] is not None and type(value[4]) is not str)
        or type(value[5]) is not str
        or type(value[6]) is not str
        or type(value[7]) is not int
        or type(value[8]) is not str
        or type(value[9]) is not str
        or type(value[10]) is not str
        or type(value[11]) is not str
    ):
        raise RuntimeError("N16 state row identity is invalid")
    decoded = loads_n16_state_envelope(value[8], "N16", value[2])
    if (
        decoded.episode_id != value[3]
        or decoded.structure_id != value[4]
        or decoded.stage != value[5]
        or decoded.reason != value[6]
        or decoded.quote_volume_rank != value[7]
        or decoded.canonical_sha256 != value[9]
    ):
        raise RuntimeError("N16 state evidence does not match its row")
    return N16TrendSupportState(*value)


def _validate_n16_passed_lifecycle(
    connection: sqlite3.Connection,
    symbol: str,
    structure_id: str,
    detail_json: str,
) -> N16TrendSupportState:
    from .n16_analyzer import validate_n16_passed_signal_detail

    rows = connection.execute(
        """
        SELECT id, strategy_id, symbol, episode_id, structure_id,
               stage, reason, quote_volume_rank, evidence_json,
               evidence_sha256, created_at, updated_at
        FROM n16_trend_support_states
        WHERE strategy_id = 'N16' AND structure_id = ?
        """,
        (structure_id,),
    ).fetchall()
    if len(rows) != 1:
        raise RuntimeError("N16 PASSED lifecycle state is not unique")
    state = _validated_n16_state_row(rows[0])
    if (
        state.symbol != symbol
        or state.stage != "CONFIRMED"
        or state.reason != "PASSED"
    ):
        raise RuntimeError("N16 PASSED lifecycle state is not confirmed")
    detail = _load_strict_strategy_signal_detail_json(detail_json)
    validate_n16_passed_signal_detail(
        detail,
        expected_symbol=symbol,
        expected_structure_id=structure_id,
        expected_evidence_json=state.evidence_json,
    )
    return state


def _n16_state_progresses(old: Any, new: Any) -> bool:
    from .n16_analyzer import decode_n16_state_envelope

    old_decoded = decode_n16_state_envelope(old, "N16")
    new_decoded = decode_n16_state_envelope(new, "N16", old_decoded.symbol)
    if (
        old_decoded.episode_id != new_decoded.episode_id
        or old_decoded.quote_volume_rank != new_decoded.quote_volume_rank
        or old_decoded.config != new_decoded.config
        or old_decoded.seed_current_open_time_ms
        != new_decoded.seed_current_open_time_ms
        or len(new_decoded.metric_source) < len(old_decoded.metric_source)
        or any(
            left.to_jsonable(False) != right.to_jsonable(False)
            for left, right in zip(
                old_decoded.metric_source, new_decoded.metric_source
            )
        )
    ):
        return False
    immutable_summary_keys = {
        "trend_id",
        "episode_id",
        "l1",
        "h1",
        "l2",
        "h2",
        "a",
        "atr_h2",
        "atr_a",
        "ema20_a",
        "ema50_a",
        "ema50_slope_reference",
        "up_leg_efficiency",
        "up_leg_volume_median",
        "pullback_volume_median",
        "pullback_volume_ratio",
    }
    if any(
        old_decoded.summary[key] != new_decoded.summary[key]
        for key in immutable_summary_keys
    ):
        return False
    if (
        old_decoded.qualified_observation is not None
        and old_decoded.qualified_observation
        != new_decoded.qualified_observation
    ):
        return False
    if (
        old_decoded.stage in {"MISSED", "INVALID", "EXPIRED"}
        and old_decoded.qualified_observation
        != new_decoded.qualified_observation
    ):
        return False
    if (
        old_decoded.structure_id is not None
        and old_decoded.structure_id != new_decoded.structure_id
    ):
        return False
    if old_decoded.summary["c"] is not None:
        for key in (
            "structure_id",
            "c",
            "atr_c",
            "ema20_c",
            "ema50_c",
            "p",
            "pullback_depth",
            "c_close_location",
            "c_taker_buy_ratio",
            "c_volume_multiple",
            "entry_min_price",
            "entry_max_price",
        ):
            if old_decoded.summary[key] != new_decoded.summary[key]:
                return False
    allowed = {
        "TOUCH_LOCKED": {
            "TOUCH_LOCKED", "CONFIRMING", "CONFIRMED",
            "MISSED", "INVALID", "EXPIRED",
        },
        "CONFIRMING": {
            "CONFIRMING", "CONFIRMED", "MISSED", "INVALID", "EXPIRED",
        },
        "CONFIRMED": {
            "CONFIRMED", "MISSED", "INVALID", "EXPIRED",
        },
        "MISSED": {"MISSED"},
        "INVALID": {"INVALID"},
        "EXPIRED": {"EXPIRED"},
    }
    return new_decoded.stage in allowed.get(old_decoded.stage, set())


def _n16_terminal_episode_identity_matches(old_decoded: Any, new_decoded: Any) -> bool:
    from .n16_analyzer import (
        _n16_candle_monotonic_extension,
    )

    if (
        old_decoded.stage not in {"MISSED", "INVALID", "EXPIRED"}
        or old_decoded.episode_id != new_decoded.episode_id
        or old_decoded.config != new_decoded.config
    ):
        return False
    anchor_keys = {
        "trend_id", "episode_id", "l1", "h1", "l2", "h2", "a",
    }
    if any(
        old_decoded.summary[key] != new_decoded.summary[key]
        for key in anchor_keys
    ):
        return False

    old_source = {
        item.open_time_ms: item for item in old_decoded.metric_source
    }
    new_source = {
        item.open_time_ms: item for item in new_decoded.metric_source
    }
    overlap = set(old_source).intersection(new_source)
    anchor_times = {
        old_decoded.summary[key]["open_time_ms"]
        for key in ("l1", "h1", "l2", "h2", "a")
    }
    if not anchor_times.issubset(overlap):
        return False
    projected_entry_time = (
        old_decoded.terminal_entry.open_time_ms
        if old_decoded.terminal_entry is not None
        and old_decoded.terminal_entry.open_time_ms in overlap
        else None
    )
    for open_time_ms in overlap:
        old_candle = old_source[open_time_ms]
        new_candle = new_source[open_time_ms]
        if open_time_ms == projected_entry_time:
            if not _n16_candle_monotonic_extension(old_candle, new_candle):
                return False
        elif old_candle.to_jsonable(False) != new_candle.to_jsonable(False):
            return False
    return True


def _n16_terminal_replay_matches(old: Any, new: Any) -> bool:
    from .n16_analyzer import decode_n16_state_envelope

    old_decoded = decode_n16_state_envelope(old, "N16")
    new_decoded = decode_n16_state_envelope(new, "N16", old_decoded.symbol)
    return bool(
        new_decoded.stage in {"MISSED", "INVALID", "EXPIRED"}
        and _n16_terminal_episode_identity_matches(old_decoded, new_decoded)
    )


def _n16_terminal_consumes_active_replay(old: Any, new: Any) -> bool:
    """Keep an immutable terminal episode from being revived by a fresh scan.

    A still-forming E candle may first close above the frozen entry ceiling and
    later fall back into the entry range.  A rolling 122-bar seed may likewise
    recompute EMA/ATR enough for a strictly valid terminal episode to look
    active again.  Both envelopes are fully validated before this comparison;
    only the immutable episode anchors and byte-identical overlapping raw
    candles are projected.  The first terminal audit always remains immutable.
    """
    from .n16_analyzer import decode_n16_state_envelope

    old_decoded = decode_n16_state_envelope(old, "N16")
    new_decoded = decode_n16_state_envelope(new, "N16", old_decoded.symbol)
    return (
        new_decoded.stage not in {"MISSED", "INVALID", "EXPIRED"}
        and _n16_terminal_episode_identity_matches(old_decoded, new_decoded)
    )


def _n13_rotation_guard_is_valid(value: Any) -> bool:
    if type(value) is not tuple or len(value) != 10:
        return False
    if type(value[0]) is not int or value[0] <= 0:
        return False
    if value[4] is not None and type(value[4]) is not str:
        return False
    return all(
        type(value[index]) is str
        for index in (1, 2, 3, 5, 6, 7, 8, 9)
    )


def _n13_rotation_guards_equal(actual: Any, expected: Any) -> bool:
    return (
        _n13_rotation_guard_is_valid(actual)
        and _n13_rotation_guard_is_valid(expected)
        and all(
            type(left) is type(right) and left == right
            for left, right in zip(actual, expected)
        )
    )


class _N14StateInconsistentError(RuntimeError):
    pass


class _N14EpisodeAlreadyConsumedError(RuntimeError):
    pass


class _N15StateInconsistentError(RuntimeError):
    pass


_N14_ACTIVE_STAGES = {"S_LOCKED": 1, "A_CONFIRMED": 2, "C_CONFIRMED": 3}
_N14_ABSORPTION_TERMINAL_REASONS = {
    "N14_ABSORPTION_VOLUME_TOO_LOW",
    "N14_ABSORPTION_TAKER_BUY_TOO_HIGH",
    "N14_ABSORPTION_RANGE_TOO_LARGE",
    "N14_ABSORPTION_LOW_TOO_LOW",
    "N14_ABSORPTION_CLOSE_TOO_LOW",
}
_N14_CONFIRMATION_TERMINAL_REASONS = {
    "N14_CONFIRMATION_LOW_BROKE_P",
    "N14_CONFIRMATION_NOT_FOUND",
}
_N14_C_TERMINAL_REASONS = {
    "PASSED",
    "N14_MARKET_CASCADE_NOT_STABILIZED",
    "N14_ENTRY_PRICE_TOO_EXTENDED",
    "N14_ENTRY_LOW_BROKE_P",
    "N14_ENTRY_WINDOW_EXPIRED",
}


def _n14_canonical_hash(unsigned: dict[str, Any]) -> str:
    canonical = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _decode_n14_active_envelope(
    value: Any,
    strategy_id: str,
    symbol: str,
    s_time: str,
) -> dict[str, Any]:
    expected = {
        "schema_version",
        "strategy_id",
        "symbol",
        "s_time",
        "stage",
        "config_signature",
        "evidence",
        "canonical_sha256",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise _N14StateInconsistentError
    unsigned = {key: value[key] for key in expected - {"canonical_sha256"}}
    evidence = value["evidence"]
    expected_evidence = {
        "locked_stage",
        "s",
        "a",
        "c",
        "p",
        "atr_s_reference",
        "volume_median_s",
        "market_scenario",
        "structure_id",
        "entry_min_price",
        "entry_max_price",
        "failed_confirmation_reasons",
        "bullish_breadth_c",
        "cascade_gate",
    }
    if not isinstance(evidence, dict):
        raise _N14StateInconsistentError
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["strategy_id"] != strategy_id
        or value["symbol"] != symbol
        or value["s_time"] != s_time
        or value["stage"] not in _N14_ACTIVE_STAGES
        or evidence.get("locked_stage") != value["stage"]
        or not isinstance(value["config_signature"], str)
        or not value["config_signature"]
        or set(evidence) != expected_evidence
        or not isinstance(evidence["s"], dict)
        or evidence["s"].get("open_time_ms") != int(s_time)
        or not isinstance(evidence["failed_confirmation_reasons"], list)
        or any(not isinstance(item, str) for item in evidence["failed_confirmation_reasons"])
        or value["canonical_sha256"] != _n14_canonical_hash(unsigned)
    ):
        raise _N14StateInconsistentError
    stage = value["stage"]
    common_valid = (
        isinstance(evidence.get("s"), dict)
        and evidence.get("atr_s_reference") is not None
        and evidence.get("volume_median_s") is not None
        and isinstance(evidence.get("market_scenario"), dict)
    )
    if stage == "S_LOCKED":
        valid_stage_evidence = common_valid and all(
            evidence.get(key) is None
            for key in (
                "a",
                "c",
                "p",
                "structure_id",
                "entry_min_price",
                "entry_max_price",
                "bullish_breadth_c",
                "cascade_gate",
            )
        )
    elif stage == "A_CONFIRMED":
        valid_stage_evidence = (
            common_valid
            and isinstance(evidence.get("a"), dict)
            and evidence.get("p") is not None
            and all(
                evidence.get(key) is None
                for key in (
                    "c",
                    "structure_id",
                    "entry_min_price",
                    "entry_max_price",
                    "bullish_breadth_c",
                    "cascade_gate",
                )
            )
        )
    else:
        valid_stage_evidence = (
            common_valid
            and isinstance(evidence.get("a"), dict)
            and isinstance(evidence.get("c"), dict)
            and evidence.get("p") is not None
            and isinstance(evidence.get("structure_id"), str)
            and bool(evidence["structure_id"])
            and evidence.get("entry_min_price") is not None
            and evidence.get("entry_max_price") is not None
            and evidence.get("bullish_breadth_c") is not None
            and evidence.get("cascade_gate")
            in {"BREADTH_MIN", "IMPROVEMENT"}
        )
    if not valid_stage_evidence:
        raise _N14StateInconsistentError
    return value


def _decode_n14_terminal_envelope(
    value: Any,
    strategy_id: str,
    symbol: str,
    s_time: str,
) -> dict[str, Any]:
    expected = {
        "schema_version",
        "strategy_id",
        "symbol",
        "s_time",
        "structure_id",
        "status",
        "reason",
        "config_signature",
        "evidence",
        "canonical_sha256",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise _N14StateInconsistentError
    unsigned = {key: value[key] for key in expected - {"canonical_sha256"}}
    evidence = value.get("evidence")
    expected_evidence = {
        "locked_stage",
        "s",
        "a",
        "c",
        "p",
        "atr_s_reference",
        "volume_median_s",
        "market_scenario",
        "structure_id",
        "entry_min_price",
        "entry_max_price",
        "failed_confirmation_reasons",
        "bullish_breadth_c",
        "cascade_gate",
    }
    if (
        value["schema_version"] != 1
        or type(value["schema_version"]) is not int
        or value["strategy_id"] != strategy_id
        or value["symbol"] != symbol
        or value["s_time"] != s_time
        or not isinstance(value["config_signature"], str)
        or not value["config_signature"]
        or not isinstance(evidence, dict)
        or set(evidence) != expected_evidence
        or evidence.get("locked_stage") not in _N14_ACTIVE_STAGES
        or not isinstance(evidence.get("s"), dict)
        or evidence["s"].get("open_time_ms") != int(s_time)
        or evidence.get("atr_s_reference") is None
        or evidence.get("volume_median_s") is None
        or not isinstance(evidence.get("market_scenario"), dict)
        or not isinstance(evidence.get("failed_confirmation_reasons"), list)
        or any(
            not isinstance(item, str)
            for item in evidence.get("failed_confirmation_reasons", [])
        )
        or value["canonical_sha256"] != _n14_canonical_hash(unsigned)
    ):
        raise _N14StateInconsistentError
    status = value.get("status")
    reason = value.get("reason")
    locked_stage = evidence["locked_stage"]
    breadth_c = evidence.get("bullish_breadth_c")
    cascade_gate = evidence.get("cascade_gate")
    if breadth_c is not None:
        try:
            parsed_breadth_c = Decimal(str(breadth_c))
        except Exception as exc:
            raise _N14StateInconsistentError from exc
        if (
            not parsed_breadth_c.is_finite()
            or not Decimal("0") <= parsed_breadth_c <= Decimal("1")
        ):
            raise _N14StateInconsistentError
    elif cascade_gate is not None:
        raise _N14StateInconsistentError
    if status not in {"INVALID", "MISSED", "CONSUMED"}:
        raise _N14StateInconsistentError
    if reason == "N14_SYSTEMIC_CRASH_VETO":
        valid = (
            status == "INVALID"
            and locked_stage == "S_LOCKED"
            and all(
                evidence.get(key) is None
                for key in (
                    "a", "c", "p", "structure_id", "entry_min_price",
                    "entry_max_price", "bullish_breadth_c", "cascade_gate",
                )
            )
        )
    elif reason in _N14_ABSORPTION_TERMINAL_REASONS:
        valid = (
            status == "INVALID"
            and locked_stage == "S_LOCKED"
            and isinstance(evidence.get("a"), dict)
            and all(
                evidence.get(key) is None
                for key in (
                    "c", "p", "structure_id", "entry_min_price",
                    "entry_max_price", "bullish_breadth_c", "cascade_gate",
                )
            )
        )
    elif reason in _N14_CONFIRMATION_TERMINAL_REASONS:
        valid = (
            status == "INVALID"
            and locked_stage == "A_CONFIRMED"
            and isinstance(evidence.get("a"), dict)
            and evidence.get("p") is not None
            and all(
                evidence.get(key) is None
                for key in (
                    "c", "structure_id", "entry_min_price",
                    "entry_max_price", "bullish_breadth_c", "cascade_gate",
                )
            )
        )
    elif reason in _N14_C_TERMINAL_REASONS:
        valid = (
            locked_stage == "C_CONFIRMED"
            and isinstance(evidence.get("a"), dict)
            and isinstance(evidence.get("c"), dict)
            and evidence.get("p") is not None
            and isinstance(value.get("structure_id"), str)
            and bool(value["structure_id"])
            and evidence.get("structure_id") == value["structure_id"]
            and status == ("CONSUMED" if reason == "PASSED" else "MISSED")
            and breadth_c is not None
            and cascade_gate in {"BREADTH_MIN", "IMPROVEMENT", "FAILED"}
            and (
                cascade_gate == "FAILED"
                if reason == "N14_MARKET_CASCADE_NOT_STABILIZED"
                else cascade_gate in {"BREADTH_MIN", "IMPROVEMENT"}
            )
        )
    elif reason == "HISTORICAL_N14_ENTRY_MISSED":
        valid = status == "MISSED"
        if locked_stage == "A_CONFIRMED":
            valid = (
                valid
                and isinstance(evidence.get("a"), dict)
                and evidence.get("p") is not None
                and all(
                    evidence.get(key) is None
                    for key in (
                        "c", "structure_id", "entry_min_price",
                        "entry_max_price", "bullish_breadth_c", "cascade_gate",
                    )
                )
            )
        elif locked_stage == "C_CONFIRMED":
            valid = (
                valid
                and isinstance(evidence.get("a"), dict)
                and isinstance(evidence.get("c"), dict)
                and evidence.get("p") is not None
                and isinstance(value.get("structure_id"), str)
                and bool(value["structure_id"])
                and evidence.get("structure_id") == value["structure_id"]
                and evidence.get("entry_min_price") is not None
                and evidence.get("entry_max_price") is not None
                and breadth_c is not None
                and cascade_gate in {"BREADTH_MIN", "IMPROVEMENT"}
            )
        elif locked_stage == "S_LOCKED":
            valid = valid and all(
                evidence.get(key) is None
                for key in (
                    "a", "c", "p", "structure_id", "entry_min_price",
                    "entry_max_price", "bullish_breadth_c", "cascade_gate",
                )
            )
    else:
        valid = False
    if not valid:
        raise _N14StateInconsistentError
    return value


def _n14_evidence_progresses(
    old_envelope: dict[str, Any],
    new_envelope: dict[str, Any],
) -> bool:
    if old_envelope["config_signature"] != new_envelope["config_signature"]:
        return False
    old = old_envelope["evidence"]
    new = new_envelope["evidence"]
    old_locked_stage = old.get("locked_stage")
    new_locked_stage = new.get("locked_stage")
    if (
        old_locked_stage not in _N14_ACTIVE_STAGES
        or new_locked_stage not in _N14_ACTIVE_STAGES
        or _N14_ACTIVE_STAGES[new_locked_stage]
        < _N14_ACTIVE_STAGES[old_locked_stage]
    ):
        return False
    for key in (
        "s",
        "atr_s_reference",
        "volume_median_s",
        "market_scenario",
    ):
        if old.get(key) != new.get(key):
            return False
    for key in (
        "a",
        "c",
        "p",
        "structure_id",
        "entry_min_price",
        "entry_max_price",
        "bullish_breadth_c",
        "cascade_gate",
    ):
        if old.get(key) is not None and old.get(key) != new.get(key):
            return False
    old_reasons = old.get("failed_confirmation_reasons", [])
    new_reasons = new.get("failed_confirmation_reasons", [])
    if new_reasons[: len(old_reasons)] != old_reasons:
        return False
    return True


class ReviewRecorder:
    def __init__(
        self,
        db_file: str,
        logger,
        n16_claim_ledger_file: str | os.PathLike[str],
        *,
        _n16_maintenance_token: object | None = None,
        _n16_claim_ledger_instance=None,
        _n16_database_scope=None,
        _strategy_schema_install_target: str | None = None,
    ):
        if _n16_maintenance_token not in (
            None,
            _N16_MAINTENANCE_INSTALL_TOKEN,
        ):
            raise TypeError("N16 maintenance token is invalid")
        self._n16_maintenance_install = (
            _n16_maintenance_token is _N16_MAINTENANCE_INSTALL_TOKEN
        )
        if _strategy_schema_install_target not in {
            None, "N17", "N18", "N19", "N20", "MICRO", "COVERAGE_EPOCH"
        }:
            raise TypeError("strategy schema maintenance target is invalid")
        if (
            _strategy_schema_install_target is not None
            and not self._n16_maintenance_install
        ):
            raise TypeError("strategy schema maintenance target requires a token")
        self._n17_maintenance_install = (
            _strategy_schema_install_target == "N17"
        )
        self._n19_maintenance_install = (
            _strategy_schema_install_target == "N19"
        )
        self._n18_maintenance_install = (
            _strategy_schema_install_target == "N18"
        )
        self._n20_maintenance_install = (
            _strategy_schema_install_target == "N20"
        )
        self._micro_maintenance_install = (
            _strategy_schema_install_target == "MICRO"
        )
        self._coverage_epoch_maintenance_install = (
            _strategy_schema_install_target == "COVERAGE_EPOCH"
        )
        requested_db_file = Path(os.path.abspath(os.fspath(db_file)))
        if _n16_database_scope is not None:
            if not self._n16_maintenance_install:
                raise TypeError(
                    "Review database scope is maintenance-only"
                )
            if requested_db_file != _n16_database_scope.path:
                raise RuntimeError(
                    "Review database path differs from maintenance scope"
                )
            _n16_database_scope.validate_before_open(requested_db_file)
            self.db_file = requested_db_file
        else:
            self.db_file = self._canonical_runtime_database_path(
                requested_db_file
            )
        self.logger = logger
        from .n16_claim_ledger import N16PermanentClaimLedger

        requested_claim_ledger = Path(
            os.path.abspath(os.fspath(n16_claim_ledger_file))
        )
        if requested_claim_ledger == self.db_file:
            raise RuntimeError("N16 claim ledger must differ from Review DB")
        self.n16_claim_ledger_file = requested_claim_ledger
        if _n16_claim_ledger_instance is not None:
            if (
                not self._n16_maintenance_install
                or not isinstance(
                    _n16_claim_ledger_instance,
                    N16PermanentClaimLedger,
                )
                or _n16_claim_ledger_instance.path
                != requested_claim_ledger
            ):
                raise TypeError(
                    "N16 maintenance ledger instance is invalid"
                )
            self.n16_claim_ledger = _n16_claim_ledger_instance
        else:
            self.n16_claim_ledger = N16PermanentClaimLedger(
                requested_claim_ledger
            )
        self._n16_database_scope = _n16_database_scope
        self._runtime_connection_lock = _RUNTIME_SQLITE_CONNECTION_LOCK
        self._runtime_open_connection_descriptors: dict[int, int] = {}
        # One scheduler round owns one identity-attested Review connection.
        # Nested recorder operations reuse it only on the owning thread; the
        # scope is discarded before the next round so no path/inode/catalog
        # result can leak across generations.
        self._runtime_round_scope = threading.local()
        self._runtime_parent_identity = (
            _n16_database_scope.parent_identity
            if _n16_database_scope is not None
            else self._runtime_parent_file_identity()
        )
        if (
            not os.path.lexists(str(self.db_file))
            and not self._n16_maintenance_install
        ):
            raise RuntimeError(
                "new Review DB requires explicit stopped-service N16 maintenance "
                "installation"
            )
        self._runtime_database_identity = (
            _n16_database_scope.main_identity
            if _n16_database_scope is not None
            else self._prepare_runtime_database_file()
        )
        self._runtime_n16_schema_status: str | None = None
        self._runtime_n16_prewrite_verified = False
        self._runtime_n16_attested_schema_version: int | None = None
        self._runtime_n16_catalog_refresh_version: int | None = None
        self._runtime_n16_install_prepared = False
        self._runtime_n17_schema_status: str | None = None
        self._runtime_n19_schema_status: str | None = None
        self._runtime_n18_schema_status: str | None = None
        self._runtime_n20_schema_status: str | None = None
        self._runtime_micro_schema_status: str | None = None
        self._runtime_coverage_epoch_schema_status: str | None = None
        self._runtime_family_seal_schema_status: str | None = None
        self._runtime_n15_terminal_schema_status: str | None = None
        self._runtime_family_graph_startup_validated = False
        self._runtime_family_graph_generation: int | None = None
        self._runtime_family_graph_active_generation: int | None = None
        self._coverage_epoch_mutation_scope: (
            tuple[str, int | None, int | None, str | None, frozenset[tuple[str, str]]]
            | None
        ) = None
        self._legacy_reset_mutation_scope: (
            tuple[str, int, str] | None
        ) = None
        self._n15_snapshot_terminal_scope: (
            tuple[str, str, str] | None
        ) = None
        self._protected_generation_pair_after_commit: (
            tuple[int, int, int, str] | None
        ) = None
        self._n16_publication_pair_after_commit = None
        self._init_db()

    @classmethod
    def _open_for_n16_maintenance(
        cls,
        db_file: str,
        logger,
        n16_claim_ledger,
        *,
        database_scope=None,
    ) -> "ReviewRecorder":
        """Private stopped-service construction used only by maintenance."""

        return cls(
            db_file,
            logger,
            n16_claim_ledger_file=n16_claim_ledger.path,
            _n16_maintenance_token=_N16_MAINTENANCE_INSTALL_TOKEN,
            _n16_claim_ledger_instance=n16_claim_ledger,
            _n16_database_scope=database_scope,
        )

    @classmethod
    def _open_for_n17_maintenance(
        cls,
        db_file: str,
        logger,
        n16_claim_ledger,
        *,
        database_scope=None,
    ) -> "ReviewRecorder":
        """Private stopped-service N16-to-N17 schema upgrade entry."""

        return cls(
            db_file,
            logger,
            n16_claim_ledger_file=n16_claim_ledger.path,
            _n16_maintenance_token=_N16_MAINTENANCE_INSTALL_TOKEN,
            _n16_claim_ledger_instance=n16_claim_ledger,
            _n16_database_scope=database_scope,
            _strategy_schema_install_target="N17",
        )

    @classmethod
    def _open_for_n19_maintenance(
        cls,
        db_file: str,
        logger,
        n16_claim_ledger,
        *,
        database_scope=None,
    ) -> "ReviewRecorder":
        """Private stopped-service N17-to-N19 schema upgrade entry."""

        return cls(
            db_file,
            logger,
            n16_claim_ledger_file=n16_claim_ledger.path,
            _n16_maintenance_token=_N16_MAINTENANCE_INSTALL_TOKEN,
            _n16_claim_ledger_instance=n16_claim_ledger,
            _n16_database_scope=database_scope,
            _strategy_schema_install_target="N19",
        )

    @classmethod
    def _open_for_n18_maintenance(
        cls,
        db_file: str,
        logger,
        n16_claim_ledger,
        *,
        database_scope=None,
    ) -> "ReviewRecorder":
        """Private stopped-service N19-to-N18 schema upgrade entry."""

        return cls(
            db_file,
            logger,
            n16_claim_ledger_file=n16_claim_ledger.path,
            _n16_maintenance_token=_N16_MAINTENANCE_INSTALL_TOKEN,
            _n16_claim_ledger_instance=n16_claim_ledger,
            _n16_database_scope=database_scope,
            _strategy_schema_install_target="N18",
        )

    @classmethod
    def _open_for_n20_maintenance(
        cls,
        db_file: str,
        logger,
        n16_claim_ledger,
        *,
        database_scope=None,
    ) -> "ReviewRecorder":
        """Private stopped-service N18-to-N20 schema upgrade entry."""

        return cls(
            db_file,
            logger,
            n16_claim_ledger_file=n16_claim_ledger.path,
            _n16_maintenance_token=_N16_MAINTENANCE_INSTALL_TOKEN,
            _n16_claim_ledger_instance=n16_claim_ledger,
            _n16_database_scope=database_scope,
            _strategy_schema_install_target="N20",
        )

    @classmethod
    def _open_for_micro_maintenance(
        cls,
        db_file: str,
        logger,
        n16_claim_ledger,
        *,
        database_scope=None,
    ) -> "ReviewRecorder":
        """Private stopped-service N20-to-N21-N25 schema upgrade entry."""

        return cls(
            db_file,
            logger,
            n16_claim_ledger_file=n16_claim_ledger.path,
            _n16_maintenance_token=_N16_MAINTENANCE_INSTALL_TOKEN,
            _n16_claim_ledger_instance=n16_claim_ledger,
            _n16_database_scope=database_scope,
            _strategy_schema_install_target="MICRO",
        )

    @classmethod
    def _open_for_coverage_epoch_maintenance(
        cls,
        db_file: str,
        logger,
        n16_claim_ledger,
        *,
        database_scope=None,
    ) -> "ReviewRecorder":
        """Private stopped-service coverage-epoch schema upgrade entry."""

        return cls(
            db_file,
            logger,
            n16_claim_ledger_file=n16_claim_ledger.path,
            _n16_maintenance_token=_N16_MAINTENANCE_INSTALL_TOKEN,
            _n16_claim_ledger_instance=n16_claim_ledger,
            _n16_database_scope=database_scope,
            _strategy_schema_install_target="COVERAGE_EPOCH",
        )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        scoped_connection = getattr(
            self._runtime_round_scope, "connection", None
        )
        if scoped_connection is not None:
            # Preserve the historical per-operation transaction boundary even
            # though the expensive FD/inode/catalog/ledger attestation belongs
            # to the enclosing round.  A failed operation rolls back only its
            # own work; a successful protected-generation mutation advances
            # the independent ledger before another operation may proceed.
            with self._runtime_connection_lock:
                if getattr(self._runtime_round_scope, "write_depth", 0):
                    raise RuntimeError(
                        "strategy round Review write transaction is already active"
                    )
                self._attest_round_write_connection(scoped_connection)
                self._runtime_round_scope.write_depth = 1
                operation_failed = False
                try:
                    try:
                        with self.n16_claim_ledger.runtime_attestation_transaction():
                            self._attest_n16_claim_ledger_after_catalog_attestation(
                                scoped_connection
                            )
                            parent_change_token = (
                                self._runtime_parent_change_token()
                            )
                            self._runtime_round_scope.write_parent_change_token = (
                                parent_change_token
                            )
                            yield scoped_connection
                            # Prove the exact retained FD and the
                            # rename-sensitive parent token before either the
                            # ledger read snapshot or Review write may commit.
                            self._attest_round_write_connection(
                                scoped_connection,
                                allow_pending_pair=True,
                            )
                            expected_parent_change_token = getattr(
                                self._runtime_round_scope,
                                "write_parent_change_token",
                                parent_change_token,
                            )
                            if self._runtime_parent_change_token() != (
                                expected_parent_change_token
                            ):
                                raise RuntimeError(
                                    "strategy round Review parent changed during "
                                    "write transaction"
                                )
                        # Exiting the ledger context above first ends its read
                        # snapshot.  Pin a new query-only ledger snapshot and
                        # hold the ledger write mutex while the exact current
                        # ledger/Review pair authorizes the Review commit.
                        if scoped_connection.in_transaction:
                            self.n16_claim_ledger.commit_attested_review(
                                scoped_connection,
                                self,
                            )
                    except BaseException:
                        scoped_connection.rollback()
                        raise
                    pending_n16_publication = (
                        self._n16_publication_pair_after_commit
                    )
                    if pending_n16_publication is not None:
                        prepared_publication, committed_summary = (
                            pending_n16_publication
                        )
                        self.n16_claim_ledger.commit_prepared(
                            prepared_publication,
                            committed_summary,
                            utc_now(),
                        )
                        self._n16_publication_pair_after_commit = None
                    self._complete_protected_generation_pair(scoped_connection)
                    self._attest_round_write_connection(scoped_connection)
                    # The independent ledger pairing above is allowed to create
                    # or retire its own WAL sidecars in the shared parent
                    # directory.  Re-prove the Review path/FD closure here, but
                    # do not compare the pre-write directory ctime across that
                    # separate, authenticated ledger transaction.  Transient
                    # Review parent/main/sidecar replacement during the Review
                    # transaction itself was already rejected before commit by
                    # the exact token comparison above.
                    self._runtime_parent_change_token()
                    with self.n16_claim_ledger.runtime_attestation_transaction():
                        self._attest_n16_claim_ledger_after_catalog_attestation(
                            scoped_connection
                        )
                except BaseException:
                    operation_failed = True
                    raise
                finally:
                    self._n16_publication_pair_after_commit = None
                    self._protected_generation_pair_after_commit = None
                    self._runtime_round_scope.write_parent_change_token = None
                    self._runtime_round_scope.write_depth = 0
                    if not operation_failed:
                        self._attest_round_write_connection(scoped_connection)
            return
        with self._runtime_connection_lock:
            if self._n16_database_scope is not None:
                self._n16_database_scope.validate_before_open(self.db_file)
            self._revalidate_runtime_database_file()
            self._validated_runtime_sidecars()
            previous_active_generation = (
                self._runtime_family_graph_active_generation
            )
            self._runtime_family_graph_active_generation = None
            # The database was either attested or securely created with
            # openat(O_EXCL|O_NOFOLLOW) during construction.  Never let a
            # pathname swap turn a later runtime open into SQLite's default
            # read-write-create operation against an external directory.
            uri = self.db_file.as_uri() + "?mode=rw"
            connection = self._open_identity_attested_runtime_connection(uri)
            paired_generation_after_commit = None
            try:
                connection.create_function(
                    _N16_GUARD_MUTATION_FUNCTION,
                    3,
                    self._authorize_n16_guard_mutation,
                )
                connection.create_function(
                    _N16_CLAIM_CHAIN_ADVANCE_FUNCTION,
                    17,
                    self._advance_n16_claim_chain,
                )
                connection.create_function(
                    _COVERAGE_EPOCH_MUTATION_FUNCTION,
                    6,
                    self._authorize_coverage_epoch_mutation,
                )
                if not self._runtime_n16_prewrite_verified:
                    n16_status = _n16_runtime_schema_status(connection)
                    if (
                        self._runtime_n16_schema_status
                        not in {"PRE_N16", "CURRENT"}
                        or n16_status != self._runtime_n16_schema_status
                    ):
                        raise RuntimeError(
                            "N16 lifecycle schema changed after read-only preflight"
                        )
                    if n16_status == "CURRENT":
                        from .n17_schema import n17_schema_status

                        current_n17_status = n17_schema_status(connection)
                        if (
                            self._runtime_n17_schema_status
                            not in {"PRE_N17", "CURRENT"}
                            or current_n17_status
                            != self._runtime_n17_schema_status
                        ):
                            raise RuntimeError(
                                "N17 lifecycle schema changed after read-only preflight"
                            )
                        if (
                            current_n17_status == "PRE_N17"
                            and not self._n17_maintenance_install
                        ):
                            raise RuntimeError(
                                "pre-N17 Review requires explicit maintenance"
                            )
                        if current_n17_status == "CURRENT":
                            from .n19_schema import n19_schema_status

                            current_n19_status = n19_schema_status(connection)
                            if (
                                self._runtime_n19_schema_status
                                not in {"PRE_N19", "CURRENT"}
                                or current_n19_status
                                != self._runtime_n19_schema_status
                            ):
                                raise RuntimeError(
                                    "N19 lifecycle schema changed after read-only preflight"
                                )
                            if (
                                current_n19_status == "PRE_N19"
                                and not (
                                    self._n17_maintenance_install
                                    or self._n19_maintenance_install
                                )
                            ):
                                raise RuntimeError(
                                    "pre-N19 Review requires explicit maintenance"
                                )
                            if current_n19_status == "CURRENT":
                                from .n18_schema import n18_schema_status

                                current_n18_status = n18_schema_status(connection)
                                if (
                                    self._runtime_n18_schema_status
                                    not in {"PRE_N18", "CURRENT"}
                                    or current_n18_status
                                    != self._runtime_n18_schema_status
                                ):
                                    raise RuntimeError(
                                        "N18 lifecycle schema changed after read-only preflight"
                                    )
                                if (
                                    current_n18_status == "PRE_N18"
                                    and not (
                                        self._n19_maintenance_install
                                        or self._n18_maintenance_install
                                    )
                                ):
                                    raise RuntimeError(
                                        "pre-N18 Review requires explicit maintenance"
                                    )
                                if current_n18_status == "CURRENT":
                                    from .n20_schema import n20_schema_status

                                    current_n20_status = n20_schema_status(connection)
                                    if (
                                        self._runtime_n20_schema_status
                                        not in {"PRE_N20", "CURRENT"}
                                        or current_n20_status
                                        != self._runtime_n20_schema_status
                                    ):
                                        raise RuntimeError(
                                            "N20 lifecycle schema changed after read-only preflight"
                                        )
                                    if (
                                        current_n20_status == "PRE_N20"
                                        and not (
                                            self._n18_maintenance_install
                                            or self._n20_maintenance_install
                                        )
                                    ):
                                        raise RuntimeError(
                                            "pre-N20 Review requires explicit maintenance"
                                        )
                                    if current_n20_status == "CURRENT":
                                        from .micro_schema import micro_schema_status

                                        current_micro_status = micro_schema_status(connection)
                                        if (
                                            self._runtime_micro_schema_status
                                            not in {"PRE_MICRO", "CURRENT"}
                                            or current_micro_status
                                            != self._runtime_micro_schema_status
                                        ):
                                            raise RuntimeError(
                                                "N21-N25 lifecycle schema changed "
                                                "after read-only preflight"
                                            )
                                        if (
                                            current_micro_status == "PRE_MICRO"
                                            and not (
                                                self._n20_maintenance_install
                                                or self._micro_maintenance_install
                                            )
                                        ):
                                            raise RuntimeError(
                                                "pre-N21-N25 Review requires explicit "
                                                "maintenance"
                                            )
                                        if current_micro_status == "CURRENT":
                                            from .coverage_epoch_schema import (
                                                coverage_epoch_schema_status,
                                            )

                                            current_epoch_status = (
                                                coverage_epoch_schema_status(connection)
                                            )
                                            if (
                                                self._runtime_coverage_epoch_schema_status
                                                not in {
                                                    "PRE_EPOCH",
                                                    "PRE_TERMINAL_RECEIPT",
                                                    "AUTHORIZED_LEGACY_V3",
                                                    "CURRENT",
                                                }
                                                or current_epoch_status
                                                != self._runtime_coverage_epoch_schema_status
                                            ):
                                                raise RuntimeError(
                                                    "history coverage epoch schema "
                                                    "changed after read-only preflight"
                                                )
                                            if (
                                                current_epoch_status
                                                in {
                                                    "PRE_EPOCH",
                                                    "PRE_TERMINAL_RECEIPT",
                                                }
                                                and not (
                                                    self._micro_maintenance_install
                                                    or self._coverage_epoch_maintenance_install
                                                )
                                            ):
                                                raise RuntimeError(
                                                    "pre-epoch Review requires explicit "
                                                    "maintenance"
                                                )
                        # A catalog-generation refresh is itself a Review write.
                        # Re-attest the independent state ledger first so a
                        # coordinated-looking rollback inside the Review file
                        # cannot be normalized into a new empty generation.
                        self._attest_n16_claim_ledger_after_catalog_attestation(
                            connection
                        )
                        self._refresh_n16_catalog_generation(connection)
                        self._runtime_n16_prewrite_verified = True
                        self._runtime_n16_attested_schema_version = (
                            _n16_catalog_schema_version(connection)
                        )
                else:
                    current_catalog_version = _n16_catalog_schema_version(
                        connection
                    )
                    if self._runtime_n16_attested_schema_version is None:
                        raise RuntimeError(
                            "N16 lifecycle startup attestation is missing"
                        )
                    if (
                        current_catalog_version
                        != self._runtime_n16_attested_schema_version
                    ):
                        if _n16_runtime_schema_status(connection) != "CURRENT":
                            raise RuntimeError(
                                "N16 lifecycle schema changed after startup "
                                "attestation"
                            )
                        from .n17_schema import n17_schema_status

                        if n17_schema_status(connection) != "CURRENT":
                            raise RuntimeError(
                                "N17 lifecycle schema changed after startup attestation"
                            )
                        from .n19_schema import n19_schema_status

                        if n19_schema_status(connection) != "CURRENT":
                            raise RuntimeError(
                                "N19 lifecycle schema changed after startup attestation"
                            )
                        from .n18_schema import n18_schema_status

                        if n18_schema_status(connection) != "CURRENT":
                            raise RuntimeError(
                                "N18 lifecycle schema changed after startup attestation"
                            )
                        from .n20_schema import n20_schema_status

                        if n20_schema_status(connection) != "CURRENT":
                            raise RuntimeError(
                                "N20 lifecycle schema changed after startup attestation"
                            )
                        from .micro_schema import micro_schema_status

                        if micro_schema_status(connection) != "CURRENT":
                            raise RuntimeError(
                                "N21-N25 lifecycle schema changed after startup "
                                "attestation"
                            )
                        from .coverage_epoch_schema import (
                            coverage_epoch_schema_status,
                        )

                        if coverage_epoch_schema_status(connection) not in {
                            "CURRENT",
                            "AUTHORIZED_LEGACY_V3",
                        }:
                            raise RuntimeError(
                                "history coverage epoch schema changed after "
                                "startup attestation"
                            )
                        from .coverage_family_seal import (
                            family_seal_schema_status,
                        )

                        if family_seal_schema_status(connection) != "CURRENT":
                            raise RuntimeError(
                                "N19 family seal schema changed after startup "
                                "attestation"
                            )
                        from .n15_terminal_schema import (
                            n15_terminal_schema_status,
                        )

                        if n15_terminal_schema_status(connection) != "CURRENT":
                            raise RuntimeError(
                                "N15 terminal receipt schema changed after "
                                "startup attestation"
                            )
                        self._attest_n16_claim_ledger_after_catalog_attestation(
                            connection
                        )
                        self._refresh_n16_catalog_generation(connection)
                        self._runtime_n16_attested_schema_version = (
                            current_catalog_version
                        )
                if not (
                    self._n16_maintenance_install
                    and self._runtime_n16_install_prepared
                ):
                    # Every recorder write is gated by the durable ledger
                    # phase.  In particular, a PREPARED publication left by an
                    # acknowledgement loss cannot be followed by paper/live
                    # audit writes in the same process.
                    self._attest_n16_claim_ledger_after_catalog_attestation(
                        connection
                    )
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA foreign_keys=ON")
                try:
                    yield connection
                    if connection.in_transaction:
                        if not (
                            self._n16_maintenance_install
                            and self._runtime_n16_install_prepared
                        ):
                            self.n16_claim_ledger.commit_attested_review(
                                connection,
                                self,
                            )
                        else:
                            connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
                paired_generation_after_commit = (
                    self._complete_protected_generation_pair(connection)
                )
            finally:
                self._n16_publication_pair_after_commit = None
                self._protected_generation_pair_after_commit = None
                self._close_identity_attested_runtime_connection(connection)
                self._runtime_family_graph_active_generation = (
                    paired_generation_after_commit
                    if paired_generation_after_commit is not None
                    else self._runtime_family_graph_active_generation
                    if self._runtime_family_graph_active_generation is not None
                    else previous_active_generation
                )
                if self._n16_database_scope is not None:
                    self._n16_database_scope.refresh_after_close(self.db_file)

    def _complete_protected_generation_pair(
        self,
        connection: sqlite3.Connection,
    ) -> int | None:
        pair = self._protected_generation_pair_after_commit
        if pair is None:
            return None
        (
            expected_generation,
            generation,
            review_schema_version,
            family_catalog_sha256_value,
        ) = pair
        current = connection.execute(
            "SELECT generation FROM history_coverage_protected_generation "
            "WHERE singleton_id=1"
        ).fetchone()
        if current != (generation,):
            raise RuntimeError(
                "N19 protected Review generation changed before ledger pairing"
            )
        self.n16_claim_ledger.advance_protected_generation_highwater(
            expected_generation=expected_generation,
            generation=generation,
            review_schema_version=review_schema_version,
            family_catalog_sha256=family_catalog_sha256_value,
            now=utc_now(),
        )
        if getattr(
            self.n16_claim_ledger._runtime_attestation_scope,
            "connection",
            None,
        ) is not None:
            # Reuse the round-owned, identity-attested ledger FD, but pin a new
            # read snapshot after the independent RW advance.  Opening another
            # maintenance-style descriptor while this FD is retained is both
            # unnecessary and defeats its exact-increment proof.
            with self.n16_claim_ledger.runtime_attestation_transaction():
                persisted_highwater = (
                    self.n16_claim_ledger.protected_generation_highwater()
                )
        else:
            persisted_highwater = (
                self.n16_claim_ledger.protected_generation_highwater()
            )
        if persisted_highwater != (
            generation,
            review_schema_version,
            family_catalog_sha256_value,
        ):
            raise RuntimeError("N19 protected generation pair did not persist")
        self._runtime_family_graph_generation = generation
        self._runtime_family_graph_active_generation = generation
        if getattr(self._runtime_round_scope, "connection", None) is not None:
            self._runtime_round_scope.generation = generation
        self._protected_generation_pair_after_commit = None
        return generation

    def _attest_round_write_connection(
        self,
        connection: sqlite3.Connection,
        *,
        allow_pending_pair: bool = False,
    ) -> None:
        """Re-prove one retained Review FD and its pinned catalog boundary."""

        if self._n16_database_scope is not None:
            self._n16_database_scope.validate_before_open(self.db_file)
        self._revalidate_opened_runtime_connection(connection)
        expected_schema = getattr(
            self._runtime_round_scope, "schema_version", None
        )
        expected_generation = getattr(
            self._runtime_round_scope, "generation", None
        )
        schema = connection.execute("PRAGMA schema_version").fetchone()
        generation = connection.execute(
            "SELECT generation FROM history_coverage_protected_generation "
            "WHERE singleton_id=1"
        ).fetchone()
        trusted_generation = self._runtime_family_graph_generation
        observed_generation = (
            generation[0]
            if type(generation) in (tuple, list)
            and len(generation) == 1
            and type(generation[0]) is int
            else None
        )
        pending_pair = self._protected_generation_pair_after_commit
        pending_generation_is_exact = (
            allow_pending_pair
            and type(pending_pair) is tuple
            and len(pending_pair) == 4
            and pending_pair[0] == expected_generation
            and pending_pair[1] == observed_generation
            and pending_pair[2] == expected_schema
        )
        if (
            type(expected_schema) is not int
            or schema != (expected_schema,)
        ):
            raise RuntimeError(
                "N19 protected Review catalog changed outside maintenance"
            )
        if (
            type(expected_generation) is not int
            or (
                observed_generation != expected_generation
                and not pending_generation_is_exact
            )
            or type(trusted_generation) is not int
            or expected_generation != trusted_generation
        ):
            raise RuntimeError(
                "strategy round Review write boundary changed"
            )

    @contextmanager
    def strategy_round_runtime_scope(self) -> Iterator[sqlite3.Connection]:
        """Own one RW connection and one fixed query-only snapshot per round."""

        if getattr(self._runtime_round_scope, "connection", None) is not None:
            raise RuntimeError("strategy round Review scope is already active")
        with self._connect() as connection, \
                self.n16_claim_ledger.runtime_attestation_scope():
            generation = connection.execute(
                "SELECT generation FROM history_coverage_protected_generation "
                "WHERE singleton_id=1"
            ).fetchone()
            schema = connection.execute("PRAGMA schema_version").fetchone()
            if (
                generation is None
                or len(generation) != 1
                or type(generation[0]) is not int
                or generation[0] < 0
                or schema is None
                or len(schema) != 1
                or type(schema[0]) is not int
                or schema[0] <= 0
            ):
                raise RuntimeError("strategy round Review identity is invalid")
            wal_details, _shm_details, _journal_details = (
                self._validated_runtime_sidecars()
            )
            options = (
                "mode=ro"
                if wal_details is not None and wal_details[2] > 0
                else "mode=ro&immutable=1"
            )
            read_connection = self._open_identity_attested_runtime_connection(
                "%s?%s" % (self.db_file.as_uri(), options)
            )
            round_lock_released = False
            try:
                read_connection.execute("PRAGMA query_only=ON")
                if read_connection.execute("PRAGMA query_only").fetchone() != (1,):
                    raise RuntimeError(
                        "strategy round Review snapshot is not query-only"
                    )
                read_connection.execute("BEGIN")
                # The first reads pin one SQLite snapshot before any nested
                # short write transaction can advance the Review database.
                read_generation = read_connection.execute(
                    "SELECT generation FROM "
                    "history_coverage_protected_generation WHERE singleton_id=1"
                ).fetchone()
                read_schema = read_connection.execute(
                    "PRAGMA schema_version"
                ).fetchone()
                if read_generation != generation or read_schema != schema:
                    raise RuntimeError(
                        "strategy round Review snapshot identity conflicts"
                    )
                self._runtime_round_scope.connection = connection
                self._runtime_round_scope.read_connection = read_connection
                self._runtime_round_scope.generation = generation[0]
                self._runtime_round_scope.schema_version = schema[0]
                self._runtime_round_scope.write_depth = 0
                # The process-global lock is required while authenticating and
                # opening both descriptors, but must not cover the complete
                # scheduler.  Nested writes reacquire it for their short
                # transaction/pairing boundary; independent recorders can then
                # reach their own concurrency gates without a round-wide lock.
                self._runtime_connection_lock.release()
                round_lock_released = True
                yield connection
                self._runtime_connection_lock.acquire()
                round_lock_released = False
                current_generation = connection.execute(
                    "SELECT generation FROM "
                    "history_coverage_protected_generation WHERE singleton_id=1"
                ).fetchone()
                current_schema = connection.execute(
                    "PRAGMA schema_version"
                ).fetchone()
                trusted_generation = self._runtime_family_graph_generation
                if current_schema != schema:
                    raise RuntimeError(
                        "N19 protected Review catalog changed outside maintenance"
                    )
                if (
                    current_generation is None
                    or len(current_generation) != 1
                    or type(current_generation[0]) is not int
                    or type(trusted_generation) is not int
                    or current_generation[0] != trusted_generation
                ):
                    raise RuntimeError(
                        "strategy round Review generation changed without pairing"
                    )
            finally:
                if round_lock_released:
                    self._runtime_connection_lock.acquire()
                self._runtime_round_scope.connection = None
                self._runtime_round_scope.read_connection = None
                self._runtime_round_scope.generation = None
                self._runtime_round_scope.schema_version = None
                self._runtime_round_scope.write_depth = 0
                try:
                    read_connection.rollback()
                finally:
                    self._close_identity_attested_runtime_connection(
                        read_connection
                    )

    @contextmanager
    def _read_only_runtime_snapshot(self) -> Iterator[sqlite3.Connection]:
        """Open one identity-attested Review snapshot without write pragmas."""

        scoped_connection = getattr(
            self._runtime_round_scope, "read_connection", None
        )
        if scoped_connection is not None:
            if (
                getattr(self._runtime_round_scope, "write_depth", 0)
                or not scoped_connection.in_transaction
                or scoped_connection.execute(
                    "PRAGMA query_only"
                ).fetchone() != (1,)
            ):
                raise RuntimeError(
                    "strategy round Review snapshot boundary is invalid"
                )
            yield scoped_connection
            return
        with self._runtime_connection_lock:
            if self._n16_database_scope is not None:
                self._n16_database_scope.validate_before_open(self.db_file)
            self._revalidate_runtime_database_file()
            wal_details, _shm_details, _journal_details = (
                self._validated_runtime_sidecars()
            )
            options = (
                "mode=ro"
                if wal_details is not None and wal_details[2] > 0
                else "mode=ro&immutable=1"
            )
            previous_active_generation = (
                self._runtime_family_graph_active_generation
            )
            self._runtime_family_graph_active_generation = None
            connection = self._open_identity_attested_runtime_connection(
                "%s?%s" % (self.db_file.as_uri(), options)
            )
            try:
                connection.execute("PRAGMA query_only=ON")
                if connection.execute("PRAGMA query_only").fetchone() != (1,):
                    raise RuntimeError("Review snapshot is not query-only")
                connection.execute("BEGIN")
                yield connection
            finally:
                try:
                    connection.rollback()
                finally:
                    self._close_identity_attested_runtime_connection(
                        connection
                    )
                    self._runtime_family_graph_active_generation = (
                        previous_active_generation
                    )
                    if self._n16_database_scope is not None:
                        self._n16_database_scope.refresh_after_close(
                            self.db_file
                        )

    def _classify_prepared_n16_review(
        self,
        prepared: Any,
    ) -> tuple[str, Any | None]:
        """Classify a pending publication from one attested RO snapshot."""

        with self._read_only_runtime_snapshot() as connection:
            return _classify_prepared_n16_review_connection(
                connection, self.n16_claim_ledger, prepared
            )

    def _abort_prepared_n16_if_review_unchanged(
        self,
        prepared: Any,
    ) -> bool:
        classification, _summary = self._classify_prepared_n16_review(
            prepared
        )
        if classification != "SAFE_ABORT":
            return False
        # The classifier holds the process-wide runtime lock only while its
        # RO connection is open.  Re-enter it here and classify once more,
        # then keep that lock through compensation so no in-process writer can
        # advance Review between proof and abort.
        with self._runtime_connection_lock:
            classification, _summary = self._classify_prepared_n16_review(
                prepared
            )
            if classification != "SAFE_ABORT":
                return False
            self.n16_claim_ledger.abort_prepared(prepared, utc_now())
            return True

    def _authorize_n16_guard_mutation(
        self,
        mode: Any,
        catalog_schema_version: Any,
        sealed_claim_count: Any,
    ) -> int:
        if (
            type(mode) is not str
            or type(catalog_schema_version) is not int
            or catalog_schema_version <= 0
            or type(sealed_claim_count) is not int
            or sealed_claim_count < 0
        ):
            return 0
        if mode == "seal":
            return 1
        if (
            mode == "catalog"
            and catalog_schema_version
            == self._runtime_n16_catalog_refresh_version
        ):
            return 1
        return 0

    @staticmethod
    def _advance_n16_claim_chain(previous: Any, *claim: Any) -> str:
        from .n16_claim_ledger import advance_review_claim_chain

        if len(claim) != 16:
            raise RuntimeError("N16 claim chain input is invalid")
        return advance_review_claim_chain(previous, claim)

    def _authorize_coverage_epoch_mutation(
        self,
        action: Any,
        strategy_id: Any,
        symbol: Any,
        source_scan_id: Any,
        batch_expected_count: Any,
        batch_manifest_sha256: Any,
    ) -> int:
        if action in {
            "n15_snapshot_terminal_insert",
            "n15_snapshot_terminal_delete",
        }:
            archive_scope = self._n15_snapshot_terminal_scope
            return int(
                archive_scope is not None
                and strategy_id == "N15"
                and symbol == archive_scope[0]
                and source_scan_id is None
                and type(batch_expected_count) is int
                and str(batch_expected_count) == archive_scope[1]
                and batch_manifest_sha256 == archive_scope[2]
            )
        if action == "legacy_reset_insert":
            reset_scope = self._legacy_reset_mutation_scope
            return int(
                reset_scope is not None
                and strategy_id == "N19"
                and symbol == reset_scope[0]
                and source_scan_id is None
                and type(batch_expected_count) is int
                and batch_expected_count == reset_scope[1]
                and batch_manifest_sha256 == reset_scope[2]
            )
        scope = self._coverage_epoch_mutation_scope
        if (
            scope is None
            or type(action) is not str
            or type(strategy_id) is not str
            or type(symbol) is not str
        ):
            return 0
        mode, scan_id, expected_count, manifest, keys = scope
        if mode == "INSTALL":
            return int(
                self._coverage_epoch_maintenance_install
                and action in {
                    "chain_insert",
                    "head_insert",
                    "installation_insert",
                    "family_seal_insert",
                    "legacy_witness_insert",
                    "v3_anchor_insert",
                    "family_installation_insert",
                    "n15_snapshot_terminal_install",
                }
                and source_scan_id is None
            )
        if (
            mode != "PUBLISH"
            or type(scan_id) is not int
            or type(expected_count) is not int
            or type(manifest) is not str
            or (strategy_id, symbol) not in keys
            or action not in {
                "chain_insert",
                "head_insert",
                "head_update",
                "receipt_insert",
                "terminal_bundle_insert",
                "terminal_receipt_insert",
                "terminal_binding_overlay_insert",
                "family_seal_insert",
                "mirror_insert",
                "mirror_update",
            }
        ):
            return 0
        if action == "terminal_binding_overlay_insert":
            return int(
                type(source_scan_id) is int
                and source_scan_id == scan_id
                and batch_expected_count is None
                and type(batch_manifest_sha256) is str
                and len(batch_manifest_sha256) == 64
            )
        if action in {
            "receipt_insert",
            "terminal_bundle_insert",
            "terminal_receipt_insert",
        }:
            return int(
                type(source_scan_id) is int
                and source_scan_id == scan_id
                and type(batch_expected_count) is int
                and batch_expected_count == expected_count
                and type(batch_manifest_sha256) is str
                and batch_manifest_sha256 == manifest
            )
        if action == "chain_insert":
            return int(
                type(source_scan_id) is int and source_scan_id == scan_id
            )
        return int(source_scan_id is None)

    def _refresh_n16_catalog_generation(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        current = _n16_catalog_schema_version(connection)
        if current == _n16_attested_catalog_schema_version(connection):
            return
        # The read-only preflight and this exact write connection have both
        # re-attested the full graph before this point.  Only this connection
        # exposes the authorization function, and only for the exact target
        # catalog generation during this short transaction.
        self._runtime_n16_catalog_refresh_version = current
        try:
            connection.execute("BEGIN IMMEDIATE")
            guard = connection.execute(
                "UPDATE n16_lifecycle_guard "
                "SET catalog_schema_version = ? WHERE singleton_id = 1",
                (current,),
            )
            if guard.rowcount != 1:
                raise RuntimeError("N16 lifecycle guard refresh conflicted")
            root = connection.execute(
                "UPDATE strategy_lifecycle_installations "
                "SET catalog_schema_version = ? "
                "WHERE singleton_id = 1 AND strategy_id = 'N16'",
                (current,),
            )
            if root.rowcount != 1:
                raise RuntimeError("N16 lifecycle root refresh conflicted")
            _verify_n16_schema(connection, allow_absent=False)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            self._runtime_n16_catalog_refresh_version = None

    def _refresh_n16_catalog_generation_in_transaction(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        """Refresh the N16 catalog witness inside an existing atomic upgrade."""

        current = _n16_catalog_schema_version(connection)
        if current == _n16_attested_catalog_schema_version(connection):
            return
        self._runtime_n16_catalog_refresh_version = current
        try:
            guard = connection.execute(
                "UPDATE n16_lifecycle_guard SET catalog_schema_version=? "
                "WHERE singleton_id=1",
                (current,),
            )
            root = connection.execute(
                "UPDATE strategy_lifecycle_installations "
                "SET catalog_schema_version=? "
                "WHERE singleton_id=1 AND strategy_id='N16'",
                (current,),
            )
            if guard.rowcount != 1 or root.rowcount != 1:
                raise RuntimeError("N16 catalog witness refresh conflicted")
            _verify_n16_schema(connection, allow_absent=False)
        finally:
            self._runtime_n16_catalog_refresh_version = None

    @staticmethod
    def _canonical_runtime_database_path(database: Path) -> Path:
        if database.name in ("", ".", ".."):
            raise RuntimeError("ReviewRecorder database filename is invalid")
        parent = database.parent
        if not os.path.lexists(str(parent)):
            raise RuntimeError(
                "ReviewRecorder database parent must be pre-created"
            )

        current = Path(parent.anchor)
        for component in parent.parts[1:]:
            candidate = current / component
            details = os.lstat(str(candidate))
            if stat.S_ISLNK(details.st_mode):
                parent_details = os.lstat(str(current))
                trusted_system_alias = (
                    details.st_uid == 0
                    and stat.S_ISDIR(parent_details.st_mode)
                    and parent_details.st_uid == 0
                    and parent_details.st_mode & 0o022 == 0
                )
                if not trusted_system_alias:
                    raise RuntimeError(
                        "ReviewRecorder database parent chain contains a symlink"
                    )
            elif not stat.S_ISDIR(details.st_mode):
                raise RuntimeError(
                    "ReviewRecorder database parent chain is not a directory"
                )
            current = candidate

        resolved_parent = parent.resolve(strict=True)
        current = Path(resolved_parent.anchor)
        for component in resolved_parent.parts[1:]:
            current = current / component
            details = os.lstat(str(current))
            if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
                raise RuntimeError(
                    "ReviewRecorder resolved database parent is not a real directory"
                )
        return resolved_parent / database.name

    def _runtime_parent_file_identity(self) -> tuple[int, int]:
        details = os.lstat(str(self.db_file.parent))
        if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
            raise RuntimeError(
                "ReviewRecorder database parent must be a real directory"
            )
        return int(details.st_dev), int(details.st_ino)

    def _runtime_parent_change_token(self) -> tuple[int, int, int]:
        """Return an identity plus rename-sensitive token for the DB parent.

        A retained SQLite FD remains bound to the original database if its
        parent is renamed away and restored during a transaction.  Endpoint
        pathname checks alone cannot see that ABA event.  Directory ctime is
        advanced by the rename/replacement operations covered by this runtime
        boundary, while normal writes through an already-open WAL do not
        mutate the parent directory entry set.
        """

        details = os.lstat(str(self.db_file.parent))
        if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
            raise RuntimeError(
                "ReviewRecorder database parent must be a real directory"
            )
        token = (
            int(details.st_dev),
            int(details.st_ino),
            int(details.st_ctime_ns),
        )
        if token[:2] != self._runtime_parent_identity:
            raise RuntimeError("ReviewRecorder database parent identity changed")
        return token

    def _before_round_ledger_mutation(self) -> None:
        """Bracket an authenticated ledger mutation inside a Review write."""

        if getattr(self._runtime_round_scope, "connection", None) is None:
            return
        expected = getattr(
            self._runtime_round_scope,
            "write_parent_change_token",
            None,
        )
        if expected is None or self._runtime_parent_change_token() != expected:
            raise RuntimeError(
                "strategy round Review parent changed before ledger mutation"
            )

    def _after_round_ledger_mutation(self) -> None:
        """Adopt only the parent token produced by the bracketed ledger write."""

        if getattr(self._runtime_round_scope, "connection", None) is None:
            return
        if getattr(
            self._runtime_round_scope,
            "write_parent_change_token",
            None,
        ) is None:
            raise RuntimeError(
                "strategy round Review ledger mutation boundary is invalid"
            )
        self._runtime_round_scope.write_parent_change_token = (
            self._runtime_parent_change_token()
        )

    def _open_runtime_parent_fd(self) -> int:
        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(str(self.db_file.parent), flags)
        except OSError as exc:
            raise RuntimeError(
                "ReviewRecorder database parent cannot be opened safely"
            ) from exc
        try:
            details = os.fstat(descriptor)
            if (
                not stat.S_ISDIR(details.st_mode)
                or (int(details.st_dev), int(details.st_ino))
                != self._runtime_parent_identity
            ):
                raise RuntimeError(
                    "ReviewRecorder database parent identity changed"
                )
        except Exception:
            os.close(descriptor)
            raise
        return descriptor

    @staticmethod
    def _runtime_regular_fd_snapshot() -> dict[int, tuple[int, int]]:
        descriptor_names = None
        # Prefer Linux's authoritative procfs table.  macOS falls back to its
        # native /dev/fd view.
        for directory in ("/proc/self/fd", "/dev/fd"):
            try:
                descriptor_names = os.listdir(directory)
                break
            except OSError:
                continue
        if descriptor_names is None:
            raise RuntimeError(
                "ReviewRecorder cannot attest opened SQLite file descriptors"
            )
        snapshot = {}
        for name in descriptor_names:
            try:
                descriptor = int(name)
                details = os.fstat(descriptor)
            except (OSError, TypeError, ValueError):
                continue
            if stat.S_ISREG(details.st_mode):
                snapshot[descriptor] = (
                    int(details.st_dev),
                    int(details.st_ino),
                )
        return snapshot

    @staticmethod
    def _runtime_regular_file_identity(
        path: Path,
        label: str,
    ) -> tuple[int, int, int]:
        details = os.lstat(str(path))
        if (
            stat.S_ISLNK(details.st_mode)
            or not stat.S_ISREG(details.st_mode)
            or details.st_nlink != 1
        ):
            raise RuntimeError(
                "%s must be a non-linked regular Binance file" % label
            )
        return int(details.st_dev), int(details.st_ino), int(details.st_size)

    def _validated_runtime_sidecars(self):
        details = []
        for suffix in ("-wal", "-shm", "-journal"):
            path = Path(str(self.db_file) + suffix)
            if not os.path.lexists(str(path)):
                details.append(None)
                continue
            identity = self._runtime_regular_file_identity(
                path, "SQLite sidecar %s" % path.name
            )
            details.append(identity)
        if details[2] is not None:
            raise RuntimeError(
                "SQLite rollback journal requires explicit maintenance "
                "before ReviewRecorder startup"
            )
        return tuple(details)

    def _prepare_runtime_database_file(self) -> tuple[int, int]:
        database_exists = os.path.lexists(str(self.db_file))
        database_identity = None
        database_size = 0
        if database_exists:
            database = self._runtime_regular_file_identity(
                self.db_file, "ReviewRecorder database"
            )
            database_identity = database[:2]
            database_size = database[2]
        sidecars = self._validated_runtime_sidecars()
        if (not database_exists or database_size == 0) and any(
            item is not None for item in sidecars
        ):
            raise RuntimeError(
                "new or empty ReviewRecorder database has unexpected sidecars"
            )
        if database_identity is not None:
            return database_identity

        parent_descriptor = self._open_runtime_parent_fd()
        try:
            flags = os.O_CREAT | os.O_EXCL | os.O_RDWR
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(
                self.db_file.name,
                flags,
                0o600,
                dir_fd=parent_descriptor,
            )
            try:
                details = os.fstat(descriptor)
                if not stat.S_ISREG(details.st_mode) or details.st_nlink != 1:
                    raise RuntimeError(
                        "new ReviewRecorder database is not a private regular file"
                    )
                created_identity = (int(details.st_dev), int(details.st_ino))
            finally:
                os.close(descriptor)
        finally:
            os.close(parent_descriptor)
        current = self._runtime_regular_file_identity(
            self.db_file, "new ReviewRecorder database"
        )
        if current[:2] != created_identity:
            raise RuntimeError("new ReviewRecorder database identity changed")
        return created_identity

    def _revalidate_runtime_database_file(self) -> tuple[int, int, int]:
        if self._runtime_parent_file_identity() != self._runtime_parent_identity:
            raise RuntimeError("ReviewRecorder database parent identity changed")
        current = self._runtime_regular_file_identity(
            self.db_file, "ReviewRecorder database"
        )
        if current[:2] != self._runtime_database_identity:
            raise RuntimeError("ReviewRecorder database identity changed")
        return current

    def _validate_opened_runtime_connection(
        self,
        connection: sqlite3.Connection,
        descriptor_snapshot: dict[int, tuple[int, int]],
    ) -> int:
        opened_descriptors = self._runtime_regular_fd_snapshot()
        if not _expected_database_fd_increment_is_attested(
            descriptor_snapshot,
            opened_descriptors,
            self._runtime_database_identity,
        ):
            raise RuntimeError(
                "opened ReviewRecorder database descriptor is not attested"
            )
        before_expected = {
            descriptor
            for descriptor, identity in descriptor_snapshot.items()
            if identity == self._runtime_database_identity
        }
        after_expected = {
            descriptor
            for descriptor, identity in opened_descriptors.items()
            if identity == self._runtime_database_identity
        }
        added = after_expected - before_expected
        if len(added) != 1:
            raise RuntimeError(
                "opened ReviewRecorder descriptor identity is ambiguous"
            )
        descriptor = next(iter(added))
        rows = connection.execute("PRAGMA database_list").fetchall()
        main_rows = [
            row
            for row in rows
            if type(row) in (tuple, list)
            and len(row) == 3
            and row[1] == "main"
        ]
        if (
            len(main_rows) != 1
            or type(main_rows[0][0]) is not int
            or type(main_rows[0][2]) is not str
            or not main_rows[0][2]
        ):
            raise RuntimeError("opened ReviewRecorder database identity is invalid")
        opened = self._runtime_regular_file_identity(
            Path(main_rows[0][2]), "opened ReviewRecorder database"
        )
        if opened[:2] != self._runtime_database_identity:
            raise RuntimeError("opened ReviewRecorder database is not attested")
        self._revalidate_runtime_database_file()
        self._validated_runtime_sidecars()
        return descriptor

    def _revalidate_opened_runtime_connection(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        """Bind one retained SQLite handle to its original main-file FD."""

        descriptor = self._runtime_open_connection_descriptors.get(
            id(connection)
        )
        if type(descriptor) is not int:
            raise RuntimeError(
                "opened ReviewRecorder descriptor proof is missing"
            )
        try:
            details = os.fstat(descriptor)
        except OSError as exc:
            raise RuntimeError(
                "opened ReviewRecorder descriptor is unavailable"
            ) from exc
        if (
            not stat.S_ISREG(details.st_mode)
            or (int(details.st_dev), int(details.st_ino))
            != self._runtime_database_identity
        ):
            raise RuntimeError(
                "opened ReviewRecorder descriptor identity changed"
            )
        # No SQLite operation precedes the fstat proof above.  database_list
        # then binds that proven descriptor back to SQLite's current main DB.
        rows = connection.execute("PRAGMA database_list").fetchall()
        main_rows = [
            row for row in rows
            if type(row) in (tuple, list) and len(row) == 3 and row[1] == "main"
        ]
        if (
            len(main_rows) != 1
            or type(main_rows[0][2]) is not str
            or not main_rows[0][2]
        ):
            raise RuntimeError(
                "opened ReviewRecorder database identity is invalid"
            )
        opened = self._runtime_regular_file_identity(
            Path(main_rows[0][2]), "opened ReviewRecorder database"
        )
        if opened[:2] != self._runtime_database_identity:
            raise RuntimeError("opened ReviewRecorder database is not attested")
        self._revalidate_runtime_database_file()
        self._validated_runtime_sidecars()

    def _close_identity_attested_runtime_connection(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        try:
            connection.close()
        finally:
            self._runtime_open_connection_descriptors.pop(id(connection), None)

    def _open_identity_attested_runtime_connection(
        self,
        uri: str,
    ) -> sqlite3.Connection:
        """Open the Review database and attest its new FD before any SQL."""

        descriptor_snapshot = self._runtime_regular_fd_snapshot()
        connection = sqlite3.connect(uri, uri=True)
        try:
            # This method first compares process descriptors using fstat.  Its
            # PRAGMA database_list check is reached only after the newly opened
            # descriptor is uniquely bound to the expected Review inode.
            descriptor = self._validate_opened_runtime_connection(
                connection,
                descriptor_snapshot,
            )
            self._runtime_open_connection_descriptors[id(connection)] = descriptor
        except BaseException:
            connection.close()
            raise
        return connection

    def _init_db(self) -> None:
        self._runtime_n16_schema_status = (
            self._assert_existing_retention_schema_is_runtime_compatible()
        )
        if self._runtime_n16_schema_status == "PRE_N16":
            if not self._n16_maintenance_install:
                raise RuntimeError(
                    "pre-N16 Review DB requires explicit stopped-service "
                    "maintenance installation"
                )
            if (
                not self.n16_claim_ledger.exists
                or self.n16_claim_ledger.metadata_phase() != "INSTALLING"
                or not self._runtime_n16_install_prepared
            ):
                raise RuntimeError(
                    "N16 maintenance requires an exact INSTALLING ledger"
                )
        if (
            self._runtime_n16_schema_status == "CURRENT"
            and self._runtime_n17_schema_status == "PRE_N17"
        ):
            if not self._n17_maintenance_install:
                raise RuntimeError(
                    "pre-N17 Review requires explicit stopped-service maintenance"
                )
            from .n17_schema import install_n17_schema

            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    install_n17_schema(connection, utc_now())
                    self._refresh_n16_catalog_generation_in_transaction(
                        connection
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
                self._runtime_n17_schema_status = "CURRENT"
                self._runtime_n16_attested_schema_version = (
                    _n16_catalog_schema_version(connection)
                )
                from .n19_schema import n19_schema_status

                self._runtime_n19_schema_status = n19_schema_status(connection)
        if (
            self._runtime_n16_schema_status == "CURRENT"
            and self._runtime_n17_schema_status == "CURRENT"
            and self._runtime_n19_schema_status == "PRE_N19"
        ):
            if not self._n19_maintenance_install:
                if not self._n17_maintenance_install:
                    raise RuntimeError(
                        "pre-N19 Review requires explicit stopped-service maintenance"
                    )
            else:
                from .n19_schema import install_n19_schema

                with self._connect() as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    try:
                        install_n19_schema(connection, utc_now())
                        self._refresh_n16_catalog_generation_in_transaction(
                            connection
                        )
                        connection.commit()
                    except BaseException:
                        connection.rollback()
                        raise
                    self._runtime_n19_schema_status = "CURRENT"
                    self._runtime_n16_attested_schema_version = (
                        _n16_catalog_schema_version(connection)
                    )
                    from .n18_schema import n18_schema_status

                    self._runtime_n18_schema_status = n18_schema_status(connection)
        if (
            self._runtime_n16_schema_status == "CURRENT"
            and self._runtime_n17_schema_status == "CURRENT"
            and self._runtime_n19_schema_status == "CURRENT"
            and self._runtime_n18_schema_status == "PRE_N18"
        ):
            if not self._n18_maintenance_install:
                if not self._n19_maintenance_install:
                    raise RuntimeError(
                        "pre-N18 Review requires explicit stopped-service maintenance"
                    )
            else:
                from .n18_schema import install_n18_schema

                with self._connect() as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    try:
                        install_n18_schema(connection, utc_now())
                        self._refresh_n16_catalog_generation_in_transaction(
                            connection
                        )
                        connection.commit()
                    except BaseException:
                        connection.rollback()
                        raise
                    self._runtime_n18_schema_status = "CURRENT"
                    self._runtime_n16_attested_schema_version = (
                        _n16_catalog_schema_version(connection)
                    )
                    from .n20_schema import n20_schema_status

                    self._runtime_n20_schema_status = n20_schema_status(connection)
        if (
            self._runtime_n16_schema_status == "CURRENT"
            and self._runtime_n17_schema_status == "CURRENT"
            and self._runtime_n19_schema_status == "CURRENT"
            and self._runtime_n18_schema_status == "CURRENT"
            and self._runtime_n20_schema_status == "PRE_N20"
        ):
            if not self._n20_maintenance_install:
                if not self._n18_maintenance_install:
                    raise RuntimeError(
                        "pre-N20 Review requires explicit stopped-service maintenance"
                    )
            else:
                from .n20_schema import install_n20_schema

                with self._connect() as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    try:
                        install_n20_schema(connection, utc_now())
                        self._refresh_n16_catalog_generation_in_transaction(connection)
                        connection.commit()
                    except BaseException:
                        connection.rollback()
                        raise
                    self._runtime_n20_schema_status = "CURRENT"
                    self._runtime_n16_attested_schema_version = (
                        _n16_catalog_schema_version(connection)
                    )
                    from .micro_schema import micro_schema_status

                    self._runtime_micro_schema_status = micro_schema_status(connection)
        if (
            self._runtime_n16_schema_status == "CURRENT"
            and self._runtime_n17_schema_status == "CURRENT"
            and self._runtime_n19_schema_status == "CURRENT"
            and self._runtime_n18_schema_status == "CURRENT"
            and self._runtime_n20_schema_status == "CURRENT"
            and self._runtime_micro_schema_status == "PRE_MICRO"
        ):
            if not self._micro_maintenance_install:
                if not self._n20_maintenance_install:
                    raise RuntimeError(
                        "pre-N21-N25 Review requires explicit stopped-service maintenance"
                    )
            else:
                from .micro_schema import install_micro_schema

                with self._connect() as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    try:
                        # The micro DDL must never commit on top of a broken
                        # predecessor graph.  Revalidate in the same write
                        # transaction immediately before the first schema write.
                        _validate_n17_lifecycle_state_rows(connection)
                        install_micro_schema(connection, utc_now())
                        self._refresh_n16_catalog_generation_in_transaction(connection)
                        connection.commit()
                    except BaseException:
                        connection.rollback()
                        raise
                    self._runtime_micro_schema_status = "CURRENT"
                    self._runtime_n16_attested_schema_version = (
                        _n16_catalog_schema_version(connection)
                    )
                    from .coverage_epoch_schema import (
                        coverage_epoch_schema_status,
                        validate_coverage_epoch_graph,
                    )

                    self._runtime_coverage_epoch_schema_status = (
                        coverage_epoch_schema_status(connection)
                    )
                    from .coverage_family_seal import (
                        family_seal_schema_status,
                    )

                    self._runtime_family_seal_schema_status = (
                        family_seal_schema_status(connection)
                    )
        if (
            self._runtime_n16_schema_status == "CURRENT"
            and self._runtime_n17_schema_status == "CURRENT"
            and self._runtime_n19_schema_status == "CURRENT"
            and self._runtime_n18_schema_status == "CURRENT"
            and self._runtime_n20_schema_status == "CURRENT"
            and self._runtime_micro_schema_status == "CURRENT"
            and self._runtime_coverage_epoch_schema_status
            in {"PRE_EPOCH", "PRE_TERMINAL_RECEIPT"}
        ):
            if self._micro_maintenance_install:
                pass
            elif not self._coverage_epoch_maintenance_install:
                raise RuntimeError(
                    "history coverage epoch schema requires explicit "
                    "stopped-service maintenance"
                )
            else:
                from .coverage_epoch_schema import install_coverage_epoch_schema

                with self._connect() as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    try:
                        self._coverage_epoch_mutation_scope = (
                            "INSTALL", None, None, None, frozenset()
                        )
                        install_coverage_epoch_schema(connection, utc_now())
                        self._refresh_n16_catalog_generation_in_transaction(connection)
                        connection.commit()
                    except BaseException:
                        connection.rollback()
                        raise
                    finally:
                        self._coverage_epoch_mutation_scope = None
                    self._runtime_coverage_epoch_schema_status = "CURRENT"
                    self._runtime_n16_attested_schema_version = (
                        _n16_catalog_schema_version(connection)
                    )
        if (
            self._runtime_n16_schema_status == "CURRENT"
            and self._runtime_n17_schema_status == "CURRENT"
            and self._runtime_n19_schema_status == "CURRENT"
            and self._runtime_n18_schema_status == "CURRENT"
            and self._runtime_n20_schema_status == "CURRENT"
            and self._runtime_micro_schema_status == "CURRENT"
            and self._runtime_coverage_epoch_schema_status
            in {"CURRENT", "AUTHORIZED_LEGACY_V3"}
            and self._runtime_family_seal_schema_status == "PRE_FAMILY_SEAL"
        ):
            if not self._coverage_epoch_maintenance_install:
                raise RuntimeError(
                    "N19 family seal schema requires explicit stopped-service "
                    "maintenance"
                )
            from .coverage_family_seal import install_family_seal_schema

            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    self._coverage_epoch_mutation_scope = (
                        "INSTALL", None, None, None, frozenset()
                    )
                    install_family_seal_schema(connection, utc_now())
                    self._refresh_n16_catalog_generation_in_transaction(connection)
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
                finally:
                    self._coverage_epoch_mutation_scope = None
                self._runtime_family_seal_schema_status = "CURRENT"
                self._runtime_n16_attested_schema_version = (
                    _n16_catalog_schema_version(connection)
                )
                from .n15_terminal_schema import n15_terminal_schema_status

                self._runtime_n15_terminal_schema_status = (
                    n15_terminal_schema_status(
                        connection, validate_graph=False
                    )
                )
        if (
            self._runtime_family_seal_schema_status == "CURRENT"
            and self._runtime_n15_terminal_schema_status
            == "PRE_N15_TERMINAL"
        ):
            if not self._coverage_epoch_maintenance_install:
                raise RuntimeError(
                    "N15 terminal receipt schema requires explicit "
                    "stopped-service maintenance"
                )
            from .n15_terminal_schema import install_n15_terminal_schema

            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    self._coverage_epoch_mutation_scope = (
                        "INSTALL", None, None, None, frozenset()
                    )
                    install_n15_terminal_schema(connection, utc_now())
                    self._refresh_n16_catalog_generation_in_transaction(
                        connection
                    )
                    generation = connection.execute(
                        "SELECT generation FROM "
                        "history_coverage_protected_generation "
                        "WHERE singleton_id=1"
                    ).fetchone()
                    schema = connection.execute(
                        "PRAGMA schema_version"
                    ).fetchone()
                    expected_generation = (
                        self._runtime_family_graph_active_generation
                    )
                    if (
                        generation is None
                        or len(generation) != 1
                        or type(generation[0]) is not int
                        or type(expected_generation) is not int
                        or generation[0] != expected_generation
                        or schema is None
                        or len(schema) != 1
                        or type(schema[0]) is not int
                        or schema[0] <= 0
                    ):
                        raise RuntimeError(
                            "N15 terminal installation generation conflicts"
                        )
                    from .coverage_family_seal import (
                        family_seal_catalog_sha256,
                    )

                    if (
                        self.n16_claim_ledger.protected_generation_highwater()
                        is not None
                    ):
                        self._protected_generation_pair_after_commit = (
                            expected_generation,
                            expected_generation,
                            schema[0],
                            family_seal_catalog_sha256(connection),
                        )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
                finally:
                    self._coverage_epoch_mutation_scope = None
                self._runtime_n15_terminal_schema_status = "CURRENT"
                self._runtime_n16_attested_schema_version = (
                    _n16_catalog_schema_version(connection)
                )
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS scans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    mode TEXT NOT NULL,
                    scanned_count INTEGER NOT NULL,
                    candidate_count INTEGER NOT NULL,
                    candidates_json TEXT NOT NULL,
                    opened INTEGER NOT NULL DEFAULT 0,
                    note TEXT
                );

                CREATE TABLE IF NOT EXISTS signal_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id INTEGER,
                    reviewed_at TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    funding_rate TEXT NOT NULL,
                    mark_price TEXT NOT NULL,
                    trend_slope TEXT NOT NULL,
                    pattern TEXT,
                    current_bullish INTEGER NOT NULL,
                    passed INTEGER NOT NULL,
                    decision TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    error TEXT,
                    FOREIGN KEY(scan_id) REFERENCES scans(id)
                );

                CREATE TABLE IF NOT EXISTS trade_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id INTEGER,
                    opened_at TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity TEXT,
                    entry_price TEXT,
                    stop_loss_price TEXT,
                    take_profit_price TEXT,
                    amplitude_24h_pct TEXT,
                    high_24h_price TEXT,
                    low_24h_price TEXT,
                    stop_loss_pct TEXT,
                    take_profit_pct TEXT,
                    risk_amount TEXT,
                    target_risk_amount TEXT,
                    actual_risk_amount TEXT,
                    risk_capped_by_margin INTEGER,
                    pretrade_quantity TEXT,
                    executed_quantity TEXT,
                    final_protected_quantity TEXT,
                    post_fill_actual_risk_amount TEXT,
                    post_fill_required_margin TEXT,
                    reduced_after_fill INTEGER,
                    notional_value TEXT,
                    required_margin TEXT,
                    balance TEXT,
                    leverage INTEGER,
                    dry_run INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    orders_json TEXT NOT NULL,
                    FOREIGN KEY(scan_id) REFERENCES scans(id)
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    symbol TEXT,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS symbol_cooldowns (
                    symbol TEXT PRIMARY KEY,
                    cooldown_until TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    source_trade_id INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS strategy_definitions (
                    strategy_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    config_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS strategy_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id INTEGER,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    funding_rate TEXT NOT NULL,
                    matched_patterns TEXT NOT NULL,
                    trend_slope TEXT NOT NULL,
                    current_bullish INTEGER NOT NULL,
                    passed INTEGER NOT NULL,
                    decision TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    structure_id TEXT,
                    detail_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(scan_id) REFERENCES scans(id)
                );

                CREATE TABLE IF NOT EXISTS strategy_signal_batches (
                    scan_id INTEGER PRIMARY KEY,
                    state TEXT NOT NULL CHECK(state IN ('STAGING', 'CURRENT')),
                    recorded_count INTEGER NOT NULL DEFAULT 0
                        CHECK(typeof(recorded_count) = 'integer' AND recorded_count >= 0),
                    expected_count INTEGER
                        CHECK(expected_count IS NULL OR (
                            typeof(expected_count) = 'integer' AND expected_count > 0
                        )),
                    first_signal_id INTEGER,
                    last_signal_id INTEGER,
                    manifest_sha256 TEXT NOT NULL,
                    completed_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(scan_id) REFERENCES scans(id)
                );

                CREATE TABLE IF NOT EXISTS strategy_signal_current (
                    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
                    current_scan_id INTEGER,
                    retention_active INTEGER NOT NULL DEFAULT 0
                        CHECK(retention_active IN (0, 1)),
                    migration_state TEXT NOT NULL DEFAULT 'PENDING'
                        CHECK(migration_state IN ('PENDING', 'BACKFILLED', 'COMPLETE')),
                    migration_cutoff_signal_id INTEGER,
                    source_signal_count INTEGER,
                    source_passed_count INTEGER,
                    source_manifest_sha256 TEXT,
                    updated_at TEXT NOT NULL,
                    retention_origin TEXT
                        CHECK(retention_origin IN ('LEGACY', 'GENESIS')),
                    FOREIGN KEY(current_scan_id) REFERENCES scans(id)
                );

                CREATE TABLE IF NOT EXISTS strategy_passed_signal_audits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_signal_id INTEGER NOT NULL UNIQUE
                        CHECK(typeof(source_signal_id) = 'integer' AND source_signal_id > 0),
                    source_scan_id INTEGER,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    funding_rate TEXT NOT NULL,
                    matched_patterns TEXT NOT NULL,
                    trend_slope TEXT NOT NULL,
                    current_bullish INTEGER NOT NULL CHECK(current_bullish IN (0, 1)),
                    passed INTEGER NOT NULL CHECK(passed = 1),
                    decision TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    structure_id TEXT,
                    detail_json TEXT NOT NULL,
                    signal_created_at TEXT NOT NULL,
                    evidence_sha256 TEXT NOT NULL CHECK(
                        length(evidence_sha256) = 64
                        AND evidence_sha256 NOT GLOB '*[^0-9a-f]*'
                    ),
                    created_at TEXT NOT NULL,
                    claim_state TEXT NOT NULL DEFAULT 'ACTIVE'
                        CHECK(claim_state IN ('STAGED', 'ACTIVE'))
                );

                CREATE TABLE IF NOT EXISTS strategy_passed_structure_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    structure_id TEXT NOT NULL,
                    source_signal_id INTEGER NOT NULL UNIQUE
                        CHECK(typeof(source_signal_id) = 'integer' AND source_signal_id > 0),
                    source_scan_id INTEGER,
                    source_signal_created_at TEXT NOT NULL,
                    evidence_sha256 TEXT NOT NULL CHECK(
                        length(evidence_sha256) = 64
                        AND evidence_sha256 NOT GLOB '*[^0-9a-f]*'
                    ),
                    created_at TEXT NOT NULL,
                    claim_state TEXT NOT NULL DEFAULT 'ACTIVE'
                        CHECK(claim_state IN ('STAGED', 'ACTIVE')),
                    UNIQUE(strategy_id, structure_id)
                );

                CREATE TABLE IF NOT EXISTS strategy_paper_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    opened_at TEXT NOT NULL,
                    last_checked_at TEXT,
                    closed_at TEXT,
                    entry_price TEXT NOT NULL,
                    stop_loss_price TEXT NOT NULL,
                    take_profit_price TEXT NOT NULL,
                    result TEXT NOT NULL,
                    exit_reason TEXT,
                    r_multiple TEXT,
                    funding_rate TEXT NOT NULL,
                    orders_json TEXT NOT NULL,
                    detail_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS strategy_states (
                    strategy_id TEXT PRIMARY KEY,
                    consecutive_wins INTEGER NOT NULL DEFAULT 0,
                    paper_trade_count INTEGER NOT NULL DEFAULT 0,
                    win_count INTEGER NOT NULL DEFAULT 0,
                    loss_count INTEGER NOT NULL DEFAULT 0,
                    win_rate TEXT NOT NULL DEFAULT '0',
                    live_eligible INTEGER NOT NULL DEFAULT 0,
                    live_result_pending INTEGER NOT NULL DEFAULT 0,
                    last_trade_result TEXT,
                    last_trade_closed_at TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS strategy_live_links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    trade_review_id INTEGER,
                    symbol TEXT NOT NULL,
                    opened_at TEXT NOT NULL,
                    closed_at TEXT,
                    result TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(trade_review_id) REFERENCES trade_reviews(id)
                );

                CREATE TABLE IF NOT EXISTS n08_structure_states (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    structure_id TEXT NOT NULL,
                    range_start_time TEXT NOT NULL,
                    range_end_time TEXT NOT NULL,
                    upper_reference TEXT NOT NULL,
                    lower_reference TEXT NOT NULL,
                    upper_tolerance_boundary TEXT NOT NULL,
                    lower_tolerance_boundary TEXT NOT NULL,
                    first_streak_start_time TEXT NOT NULL,
                    first_streak_end_time TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    reset_open_time TEXT,
                    detail_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(strategy_id, structure_id)
                );

                CREATE TABLE IF NOT EXISTS n08_history_coverage (
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    continuous_from_open_time TEXT NOT NULL,
                    continuous_until_open_time TEXT NOT NULL,
                    last_response_first_open_time TEXT NOT NULL,
                    last_response_last_open_time TEXT NOT NULL,
                    last_gap_from_open_time TEXT,
                    last_gap_to_open_time TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(strategy_id, symbol)
                );

                CREATE TABLE IF NOT EXISTS n09_structure_states (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    structure_id TEXT NOT NULL,
                    s1_time TEXT NOT NULL,
                    s1_price TEXT NOT NULL,
                    l_time TEXT NOT NULL,
                    l_price TEXT NOT NULL,
                    first_touch_time TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(strategy_id, structure_id)
                );

                CREATE TABLE IF NOT EXISTS n10_structure_states (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    structure_id TEXT NOT NULL,
                    support_start_time TEXT NOT NULL,
                    support_end_time TEXT NOT NULL,
                    support_price TEXT NOT NULL,
                    w_time TEXT NOT NULL,
                    c_time TEXT NOT NULL,
                    e_time TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(strategy_id, structure_id)
                );

                CREATE TABLE IF NOT EXISTS n12_stage_states (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    l_time TEXT NOT NULL,
                    h_time TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(strategy_id, symbol, l_time, h_time)
                );

                CREATE TABLE IF NOT EXISTS n12_rank_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    current_open_time TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(strategy_id, current_open_time)
                );

                CREATE TABLE IF NOT EXISTS n13_market_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    current_open_time TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(strategy_id, current_open_time)
                );

                CREATE TABLE IF NOT EXISTS n13_rotation_states (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    t_time TEXT NOT NULL,
                    structure_id TEXT,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(strategy_id, symbol, t_time)
                );

                CREATE TABLE IF NOT EXISTS n14_market_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    s_time TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(strategy_id, s_time)
                );

                CREATE TABLE IF NOT EXISTS n14_active_episodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    s_time TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(strategy_id, symbol, s_time)
                );

                CREATE TABLE IF NOT EXISTS n14_sell_impact_states (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    s_time TEXT NOT NULL,
                    structure_id TEXT,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(strategy_id, symbol, s_time)
                );

                CREATE TABLE IF NOT EXISTS n15_market_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    e_time TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(strategy_id, e_time)
                );

                CREATE TABLE IF NOT EXISTS n15_entry_states (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    e_time TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    structure_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(strategy_id, e_time)
                );

                CREATE TABLE IF NOT EXISTS strategy_structure_terminal_states (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    structure_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(strategy_id, structure_id)
                );

                CREATE INDEX IF NOT EXISTS idx_signal_reviews_symbol_time
                    ON signal_reviews(symbol, reviewed_at);
                CREATE INDEX IF NOT EXISTS idx_trade_reviews_symbol_time
                    ON trade_reviews(symbol, opened_at);
                CREATE INDEX IF NOT EXISTS idx_events_type_time
                    ON events(event_type, occurred_at);
                CREATE INDEX IF NOT EXISTS idx_symbol_cooldowns_until
                    ON symbol_cooldowns(cooldown_until);
                CREATE INDEX IF NOT EXISTS idx_strategy_signals_strategy_symbol
                    ON strategy_signals(strategy_id, symbol, created_at);
                CREATE INDEX IF NOT EXISTS idx_strategy_paper_open
                    ON strategy_paper_trades(strategy_id, result);
                CREATE INDEX IF NOT EXISTS idx_n12_stage_symbol
                    ON n12_stage_states(strategy_id, symbol, status);
                CREATE INDEX IF NOT EXISTS idx_n12_rank_snapshot_time
                    ON n12_rank_snapshots(strategy_id, current_open_time);
                CREATE INDEX IF NOT EXISTS idx_n13_rotation_symbol
                    ON n13_rotation_states(strategy_id, symbol, status);
                CREATE INDEX IF NOT EXISTS idx_n14_sell_impact_symbol
                    ON n14_sell_impact_states(strategy_id, symbol, status);
                CREATE INDEX IF NOT EXISTS idx_n14_active_episode_symbol
                    ON n14_active_episodes(strategy_id, symbol, stage);
                CREATE INDEX IF NOT EXISTS idx_n15_entry_symbol
                    ON n15_entry_states(strategy_id, symbol, status);
                CREATE INDEX IF NOT EXISTS idx_strategy_paper_symbol_result
                    ON strategy_paper_trades(strategy_id, symbol, result, closed_at);
                CREATE INDEX IF NOT EXISTS idx_strategy_live_result
                    ON strategy_live_links(strategy_id, symbol, result, closed_at);
                CREATE INDEX IF NOT EXISTS idx_n08_structure_state_active
                    ON n08_structure_states(strategy_id, symbol, status);
                CREATE INDEX IF NOT EXISTS idx_n09_structure_s1
                    ON n09_structure_states(strategy_id, symbol, s1_time, status);
                CREATE INDEX IF NOT EXISTS idx_n10_structure_symbol_time
                    ON n10_structure_states(strategy_id, symbol, w_time, status);
                CREATE INDEX IF NOT EXISTS idx_strategy_structure_terminal_lookup
                    ON strategy_structure_terminal_states(strategy_id, symbol, status);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_signal_one_staging
                    ON strategy_signal_batches(state) WHERE state = 'STAGING';
                CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_signal_one_current
                    ON strategy_signal_batches(state) WHERE state = 'CURRENT';
                CREATE INDEX IF NOT EXISTS idx_passed_structure_symbol
                    ON strategy_passed_structure_ledger(strategy_id, symbol, structure_id);
                """
            )
            self._ensure_trade_columns(connection)
            self._ensure_strategy_columns(connection)
            self._ensure_paper_trade_columns(connection)
            self._ensure_strategy_state_columns(connection)
            self._ensure_passed_claim_columns(connection)
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_strategy_signals_structure
                ON strategy_signals(strategy_id, structure_id, passed)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_passed_audit_claim_batch
                ON strategy_passed_signal_audits(source_scan_id, claim_state)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_passed_ledger_claim_batch
                ON strategy_passed_structure_ledger(source_scan_id, claim_state)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_passed_audit_claim_state_scan
                ON strategy_passed_signal_audits(claim_state, source_scan_id)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_passed_ledger_claim_state_scan
                ON strategy_passed_structure_ledger(claim_state, source_scan_id)
                """
            )
            now = utc_now()
            connection.execute(
                """
                INSERT OR IGNORE INTO strategy_signal_current (
                    singleton_id, current_scan_id, retention_active,
                    migration_state, updated_at
                ) VALUES (1, NULL, 0, 'PENDING', ?)
                """,
                (now,),
            )
            retention = connection.execute(
                """
                SELECT current_scan_id, retention_active, migration_state,
                       migration_cutoff_signal_id, source_signal_count,
                       source_passed_count, source_manifest_sha256,
                       retention_origin
                FROM strategy_signal_current WHERE singleton_id = 1
                """
            ).fetchone()
            has_legacy_signals = connection.execute(
                "SELECT 1 FROM strategy_signals LIMIT 1"
            ).fetchone()
            has_retention_evidence = connection.execute(
                """
                SELECT 1 FROM (
                    SELECT 1 AS present FROM strategy_signal_batches
                    UNION ALL
                    SELECT 1 FROM strategy_passed_signal_audits
                    UNION ALL
                    SELECT 1 FROM strategy_passed_structure_ledger
                ) LIMIT 1
                """
            ).fetchone()
            retention_sequences = connection.execute(
                """
                SELECT name, seq FROM sqlite_sequence
                WHERE name IN (
                    'strategy_signals',
                    'strategy_passed_signal_audits',
                    'strategy_passed_structure_ledger'
                )
                """
            ).fetchall()
            retention_is_empty = (
                has_legacy_signals is None
                and has_retention_evidence is None
                and retention_sequences == []
            )
            if retention is None or len(retention) != 8:
                raise RuntimeError("strategy signal retention row is invalid")
            genesis_candidates = {
                (None, 0, "PENDING", None, None, None, None, None),
                (None, 1, "COMPLETE", None, None, None, None, None),
                (
                    None,
                    1,
                    "COMPLETE",
                    0,
                    0,
                    0,
                    _SIGNAL_MANIFEST_SEED,
                    None,
                ),
            }
            if retention in genesis_candidates and retention_is_empty:
                connection.execute(
                    """
                    UPDATE strategy_signal_current
                    SET retention_active = 1, migration_state = 'COMPLETE',
                        migration_cutoff_signal_id = 0,
                        source_signal_count = 0, source_passed_count = 0,
                        source_manifest_sha256 = ?, retention_origin = 'GENESIS',
                        updated_at = ?
                    WHERE singleton_id = 1
                    """,
                    (_SIGNAL_MANIFEST_SEED, now),
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_strategy_signals_scan_id
                    ON strategy_signals(scan_id)
                    """
                )
            final_retention = _strategy_signal_retention_row(connection)
            if final_retention[7] == "GENESIS" and final_retention[0] is None:
                _validate_unpublished_genesis_evidence(connection)
            _install_n16_schema(connection)
            self._runtime_n16_schema_status = "CURRENT"
            self._runtime_n16_prewrite_verified = True
            self._runtime_n16_attested_schema_version = (
                _n16_catalog_schema_version(connection)
            )
        if self._runtime_n16_install_prepared:
            with self._connect() as connection:
                _validate_n16_installing_review_empty(connection)
                summary = _n16_review_claim_summary(connection)
                if _n16_all_review_claims(connection):
                    raise RuntimeError(
                        "N16 installation Review baseline is not empty"
                    )
            self.n16_claim_ledger.attest_installing_empty()
            self.n16_claim_ledger.confirm_install(summary, utc_now())
            self._runtime_n16_install_prepared = False

    def _ensure_trade_columns(self, connection: sqlite3.Connection) -> None:
        existing = {
            row[1]
            for row in connection.execute("PRAGMA table_info(trade_reviews)").fetchall()
        }
        columns = {
            "amplitude_24h_pct": "TEXT",
            "high_24h_price": "TEXT",
            "low_24h_price": "TEXT",
            "stop_loss_pct": "TEXT",
            "take_profit_pct": "TEXT",
            "risk_amount": "TEXT",
            "target_risk_amount": "TEXT",
            "actual_risk_amount": "TEXT",
            "risk_capped_by_margin": "INTEGER",
            "pretrade_quantity": "TEXT",
            "executed_quantity": "TEXT",
            "final_protected_quantity": "TEXT",
            "post_fill_actual_risk_amount": "TEXT",
            "post_fill_required_margin": "TEXT",
            "reduced_after_fill": "INTEGER",
            "notional_value": "TEXT",
            "required_margin": "TEXT",
            "balance": "TEXT",
            "closed_at": "TEXT",
            "exit_reason": "TEXT",
            "exit_price": "TEXT",
            "close_mark_price": "TEXT",
            "realized_pnl": "TEXT",
            "realized_pnl_pct": "TEXT",
            "balance_after_close": "TEXT",
        }
        for column, column_type in columns.items():
            if column not in existing:
                connection.execute(f"ALTER TABLE trade_reviews ADD COLUMN {column} {column_type}")

    def _ensure_strategy_columns(self, connection: sqlite3.Connection) -> None:
        existing = {
            row[1]
            for row in connection.execute("PRAGMA table_info(strategy_signals)").fetchall()
        }
        columns = {
            "structure_id": "TEXT",
            "detail_json": "TEXT NOT NULL DEFAULT '{}'",
        }
        for column, column_type in columns.items():
            if column not in existing:
                connection.execute(f"ALTER TABLE strategy_signals ADD COLUMN {column} {column_type}")

    def _ensure_paper_trade_columns(self, connection: sqlite3.Connection) -> None:
        existing = {
            row[1]
            for row in connection.execute("PRAGMA table_info(strategy_paper_trades)").fetchall()
        }
        if "last_checked_at" not in existing:
            connection.execute("ALTER TABLE strategy_paper_trades ADD COLUMN last_checked_at TEXT")

    def _ensure_strategy_state_columns(self, connection: sqlite3.Connection) -> None:
        existing = {
            row[1]
            for row in connection.execute("PRAGMA table_info(strategy_states)").fetchall()
        }
        if "live_result_pending" not in existing:
            connection.execute(
                "ALTER TABLE strategy_states "
                "ADD COLUMN live_result_pending INTEGER NOT NULL DEFAULT 0"
            )

    def _ensure_passed_claim_columns(self, connection: sqlite3.Connection) -> None:
        for table in (
            "strategy_passed_signal_audits",
            "strategy_passed_structure_ledger",
        ):
            existing = {
                row[1]
                for row in connection.execute(
                    f"PRAGMA table_info({table})"
                ).fetchall()
            }
            if "claim_state" not in existing:
                connection.execute(
                    f"ALTER TABLE {table} ADD COLUMN claim_state TEXT NOT NULL "
                    "DEFAULT 'ACTIVE' CHECK(claim_state IN ('STAGED', 'ACTIVE'))"
                )

    def _assert_existing_retention_schema_is_runtime_compatible(self) -> str:
        database = self._revalidate_runtime_database_file()
        wal_details, shm_details, _journal_details = (
            self._validated_runtime_sidecars()
        )
        if database[2] == 0:
            if wal_details is not None or shm_details is not None:
                raise RuntimeError(
                    "empty ReviewRecorder database has unexpected sidecars"
                )
            if self.n16_claim_ledger.exists:
                if (
                    self._n16_maintenance_install
                    and self.n16_claim_ledger.metadata_phase() == "INSTALLING"
                ):
                    self._runtime_n16_install_prepared = True
                    return "PRE_N16"
                raise RuntimeError(
                    "N16 claim ledger exists without a Review installation"
                )
            return "PRE_N16"
        # With no sidecars, immutable mode safely reads a checkpointed main
        # database even when its header still records WAL journal mode.  An
        # empty WAL (and any validated residual SHM) has no newer pages either.
        # Only a non-empty validated WAL requires ordinary read-only mode; in
        # that case SQLite may legitimately update this Binance DB's own SHM.
        uri_options = (
            "mode=ro"
            if wal_details is not None and wal_details[2] > 0
            else "mode=ro&immutable=1"
        )
        uri = f"{self.db_file.resolve().as_uri()}?{uri_options}"
        # Import before the guarded connection so an opened-identity failure
        # cannot be obscured by an unbound exception class in the handler.
        from .signal_retention import (
            SignalRetentionMaintenanceError,
            _retention_schema_version,
            _strict_absent_retention_history_is_empty,
            _strict_genesis_history_is_empty,
            _validate_retention_dependencies,
            _verify_retention_schema,
        )

        # Keep the complete read-only preflight connection lifecycle under the
        # same process-wide lock as normal recorder connections.  Releasing the
        # lock after descriptor attestation but before query/close would let a
        # concurrent constructor snapshot a descriptor that is then closed and
        # reused underneath it.
        with self._runtime_connection_lock:
            connection = self._open_identity_attested_runtime_connection(uri)
            try:
                # Schema-absent databases are never runnable.  Classify N16
                # from catalog/PRAGMA evidence first and reject (or hand the
                # already-attested INSTALLING pair to explicit maintenance)
                # before retention validation can touch any permanent
                # business-history table.  CURRENT databases still execute
                # the complete retention and N16 graph preflight below.
                n16_status = _n16_runtime_schema_status(connection)
                if n16_status == "PRE_N16":
                    return self._attest_n16_claim_ledger_prewrite(
                        connection, n16_status
                    )
                version = _retention_schema_version(connection)
                if version == "ABSENT":
                    _validate_retention_dependencies(connection)
                    if not _strict_absent_retention_history_is_empty(connection):
                        raise RuntimeError(
                            "legacy strategy signal history requires explicit "
                            "retention maintenance before ReviewRecorder startup"
                        )
                    return self._attest_n16_claim_ledger_prewrite(
                        connection, n16_status
                    )
                if version != "CURRENT":
                    raise RuntimeError(
                        "strategy signal retention schema requires explicit "
                        "maintenance upgrade before ReviewRecorder startup"
                    )
                _verify_retention_schema(connection)
                _validate_retention_dependencies(connection)
                retention = _strategy_signal_retention_row(connection)
                if retention[2] == "BACKFILLED":
                    raise RuntimeError(
                        "BACKFILLED strategy signal retention requires explicit "
                        "maintenance completion before ReviewRecorder startup"
                    )
                if (
                    retention[2] == "PENDING"
                    and not _strict_genesis_history_is_empty(connection)
                ):
                    raise RuntimeError(
                        "PENDING strategy signal retention contains legacy "
                        "history and requires explicit maintenance"
                    )
                if retention[2] == "COMPLETE":
                    _validate_complete_strategy_signal_graph(
                        connection, retention
                    )
                return self._attest_n16_claim_ledger_prewrite(
                    connection, n16_status
                )
            except SignalRetentionMaintenanceError as exc:
                raise RuntimeError(
                    "strategy signal retention schema requires explicit "
                    "maintenance verification before ReviewRecorder startup"
                ) from exc
            finally:
                self._close_identity_attested_runtime_connection(connection)

    def _attest_n16_claim_ledger_prewrite(
        self,
        connection: sqlite3.Connection,
        n16_status: str,
    ) -> str:
        if n16_status == "PRE_N16":
            if self._n16_maintenance_install:
                if (
                    not self.n16_claim_ledger.exists
                    or self.n16_claim_ledger.metadata_phase() != "INSTALLING"
                ):
                    raise RuntimeError(
                        "pre-N16 maintenance requires an INSTALLING claim ledger"
                    )
                self._runtime_n16_install_prepared = True
                return n16_status
            if self.n16_claim_ledger.exists:
                raise RuntimeError(
                    "N16 claim ledger exists for a pre-N16 Review database"
                )
            return n16_status
        if n16_status != "CURRENT":
            raise RuntimeError("N16 lifecycle installation status is invalid")
        if not self.n16_claim_ledger.exists:
            raise RuntimeError(
                "N16 claim ledger is missing from a post-N16 Review database; "
                "explicit stopped-service bootstrap is required"
            )
        try:
            summary = _n16_review_claim_summary(connection)
            if (
                self._n16_maintenance_install
                and self.n16_claim_ledger.metadata_phase() == "INSTALLING"
            ):
                if summary.confirmed_claim_count != 0:
                    raise RuntimeError(
                        "INSTALLING ledger cannot confirm non-empty Review claims"
                    )
                self._runtime_n16_install_prepared = True
                return n16_status
            self.n16_claim_ledger.attest(summary)
        except Exception as exc:
            raise RuntimeError(
                "N16 Review and independent claim ledger are inconsistent"
            ) from exc
        from .n17_schema import n17_schema_status

        self._runtime_n17_schema_status = n17_schema_status(connection)
        if (
            self._runtime_n17_schema_status == "PRE_N17"
            and not self._n17_maintenance_install
        ):
            raise RuntimeError(
                "pre-N17 Review DB requires explicit stopped-service "
                "strategy schema maintenance"
            )
        if self._runtime_n17_schema_status not in {"PRE_N17", "CURRENT"}:
            raise RuntimeError("N17 lifecycle installation status is invalid")
        if self._runtime_n17_schema_status == "CURRENT":
            from .n19_schema import n19_schema_status

            self._runtime_n19_schema_status = n19_schema_status(connection)
            if (
                self._runtime_n19_schema_status == "PRE_N19"
                and not (
                    self._n17_maintenance_install
                    or self._n19_maintenance_install
                )
            ):
                raise RuntimeError(
                    "pre-N19 Review DB requires explicit stopped-service "
                    "strategy schema maintenance"
                )
            if self._runtime_n19_schema_status not in {"PRE_N19", "CURRENT"}:
                raise RuntimeError("N19 lifecycle installation status is invalid")
            if self._runtime_n19_schema_status == "CURRENT":
                from .n18_schema import n18_schema_status

                self._runtime_n18_schema_status = n18_schema_status(connection)
                if (
                    self._runtime_n18_schema_status == "PRE_N18"
                    and not (
                        self._n19_maintenance_install
                        or self._n18_maintenance_install
                    )
                ):
                    raise RuntimeError(
                        "pre-N18 Review DB requires explicit stopped-service "
                        "strategy schema maintenance"
                    )
                if self._runtime_n18_schema_status not in {"PRE_N18", "CURRENT"}:
                    raise RuntimeError("N18 lifecycle installation status is invalid")
                if self._runtime_n18_schema_status == "CURRENT":
                    from .n20_schema import (
                        n20_schema_status,
                        validate_n20_episode_graph,
                    )

                    self._runtime_n20_schema_status = n20_schema_status(connection)
                    if (
                        self._runtime_n20_schema_status == "PRE_N20"
                        and not (
                            self._n18_maintenance_install
                            or self._n20_maintenance_install
                        )
                    ):
                        raise RuntimeError(
                            "pre-N20 Review DB requires explicit stopped-service "
                            "strategy schema maintenance"
                        )
                    if self._runtime_n20_schema_status not in {"PRE_N20", "CURRENT"}:
                        raise RuntimeError("N20 lifecycle installation status is invalid")
                    if self._runtime_n20_schema_status == "CURRENT":
                        validate_n20_episode_graph(connection)
                        from .micro_schema import micro_schema_status

                        self._runtime_micro_schema_status = micro_schema_status(
                            connection
                        )
                        if (
                            self._runtime_micro_schema_status == "PRE_MICRO"
                            and not (
                                self._n20_maintenance_install
                                or self._micro_maintenance_install
                            )
                        ):
                            raise RuntimeError(
                                "pre-N21-N25 Review DB requires explicit "
                                "stopped-service strategy schema maintenance"
                            )
                        if self._runtime_micro_schema_status not in {
                            "PRE_MICRO", "CURRENT"
                        }:
                            raise RuntimeError(
                                "N21-N25 lifecycle installation status is invalid"
                            )
                        if self._runtime_micro_schema_status == "CURRENT":
                            from .coverage_epoch_schema import (
                                coverage_epoch_schema_status,
                            )

                            self._runtime_coverage_epoch_schema_status = (
                                coverage_epoch_schema_status(connection)
                            )
                            from .coverage_family_seal import (
                                family_seal_schema_status,
                            )

                            self._runtime_family_seal_schema_status = (
                                family_seal_schema_status(
                                    connection, validate_graph=False
                                )
                            )
                            if (
                                self._runtime_coverage_epoch_schema_status
                                in {"PRE_EPOCH", "PRE_TERMINAL_RECEIPT"}
                                and not (
                                    self._micro_maintenance_install
                                    or self._coverage_epoch_maintenance_install
                                )
                            ):
                                raise RuntimeError(
                                    "history coverage epoch schema requires "
                                    "explicit stopped-service maintenance"
                                )
                            if (
                                self._runtime_coverage_epoch_schema_status
                                in {"PRE_EPOCH", "PRE_TERMINAL_RECEIPT"}
                                and self._runtime_family_seal_schema_status
                                != "PRE_FAMILY_SEAL"
                            ):
                                raise RuntimeError(
                                    "history coverage family seal is half-installed "
                                    "and requires explicit repair"
                                )
                            if self._runtime_coverage_epoch_schema_status not in {
                                "PRE_EPOCH",
                                "PRE_TERMINAL_RECEIPT",
                                "AUTHORIZED_LEGACY_V3",
                                "CURRENT",
                            }:
                                raise RuntimeError(
                                    "history coverage epoch installation status "
                                    "is invalid"
                                )
                            if (
                                self._runtime_family_seal_schema_status
                                == "PRE_FAMILY_SEAL"
                                and not self._coverage_epoch_maintenance_install
                            ):
                                raise RuntimeError(
                                    "N19 family seal schema requires explicit "
                                    "stopped-service maintenance"
                                )
                            if self._runtime_family_seal_schema_status not in {
                                "PRE_FAMILY_SEAL",
                                "CURRENT",
                            }:
                                raise RuntimeError(
                                    "N19 family seal installation status is invalid"
                                )
                            if (
                                self._runtime_family_seal_schema_status
                                == "CURRENT"
                            ):
                                from .n15_terminal_schema import (
                                    n15_terminal_schema_status,
                                )

                                self._runtime_n15_terminal_schema_status = (
                                    n15_terminal_schema_status(
                                        connection, validate_graph=False
                                    )
                                )
                                if (
                                    self._runtime_n15_terminal_schema_status
                                    == "PRE_N15_TERMINAL"
                                    and not self._coverage_epoch_maintenance_install
                                ):
                                    raise RuntimeError(
                                        "N15 terminal receipt schema requires "
                                        "explicit stopped-service maintenance"
                                    )
                                if self._runtime_n15_terminal_schema_status not in {
                                    "PRE_N15_TERMINAL", "CURRENT"
                                }:
                                    raise RuntimeError(
                                        "N15 terminal receipt installation status "
                                        "is invalid"
                                    )
                                self._attest_legacy_witness_pair(connection)
        return n16_status

    def _attest_legacy_witness_pair(
        self,
        connection: sqlite3.Connection,
        *,
        allow_pending_protected_generation: bool = False,
    ) -> None:
        from .coverage_family_seal import (
            family_seal_catalog_sha256,
            validate_family_seal_graph,
        )

        # Exact schema/xinfo/index/trigger attestation is performed at
        # construction and again whenever the catalog schema version changes.
        # Rebuilding the reference catalog for every ordinary signal write
        # would make the hot path proportional to the number of signals even
        # when the catalog is unchanged.  The per-connection gate below still
        # binds the current schema cookie and exact catalog digest to the
        # independent ledger high-water before consulting protected data.
        if self._runtime_family_seal_schema_status != "CURRENT":
            raise RuntimeError("N19 family seal schema is incomplete")
        if self._runtime_n15_terminal_schema_status != "CURRENT":
            if not self._coverage_epoch_maintenance_install:
                raise RuntimeError("N15 terminal receipt schema is incomplete")
        started_snapshot = not connection.in_transaction
        if started_snapshot:
            connection.execute("BEGIN")
        validated_generation = None
        review_schema_version = None
        family_catalog_sha256_value = None
        try:
            schema_version_row = connection.execute(
                "PRAGMA schema_version"
            ).fetchone()
            if (
                schema_version_row is None
                or len(schema_version_row) != 1
                or type(schema_version_row[0]) is not int
                or schema_version_row[0] <= 0
            ):
                raise RuntimeError(
                    "N19 protected Review schema version is invalid"
                )
            review_schema_version = schema_version_row[0]
            family_catalog_sha256_value = family_seal_catalog_sha256(
                connection
            )
            generation_row = connection.execute(
                "SELECT singleton_id,generation "
                "FROM history_coverage_protected_generation"
            ).fetchall()
            if (
                len(generation_row) != 1
                or type(generation_row[0][0]) is not int
                or generation_row[0][0] != 1
                or type(generation_row[0][1]) is not int
                or generation_row[0][1] < 0
            ):
                raise RuntimeError("N19 protected generation is invalid")
            observed_generation = generation_row[0][1]
            if (
                self._runtime_family_graph_generation is not None
                and observed_generation
                < self._runtime_family_graph_generation
            ):
                raise RuntimeError(
                    "N19 protected generation rolled back in this process"
                )
            ledger_highwater = (
                self.n16_claim_ledger.protected_generation_highwater()
            )
            if ledger_highwater is None:
                if not self._coverage_epoch_maintenance_install:
                    raise RuntimeError(
                        "N19 protected generation ledger highwater is missing"
                    )
                validate_family_seal_graph(connection)
                requires_full_validation = True
            else:
                (
                    ledger_generation,
                    ledger_schema_version,
                    ledger_catalog_sha256,
                ) = ledger_highwater
                if (
                    review_schema_version != ledger_schema_version
                    or family_catalog_sha256_value
                    != ledger_catalog_sha256
                ):
                    raise RuntimeError(
                        "N19 protected Review catalog changed outside maintenance"
                    )
                if observed_generation < ledger_generation:
                    raise RuntimeError(
                        "N19 protected generation is below its ledger highwater"
                    )
                pending_pair = self._protected_generation_pair_after_commit
                pending_generation_is_exact = (
                    allow_pending_protected_generation
                    and type(pending_pair) is tuple
                    and len(pending_pair) == 4
                    and pending_pair
                    == (
                        ledger_generation,
                        observed_generation,
                        review_schema_version,
                        family_catalog_sha256_value,
                    )
                )
                if (
                    observed_generation > ledger_generation
                    and not pending_generation_is_exact
                ):
                    raise RuntimeError(
                        "N19 protected generation is above its ledger highwater"
                    )
                requires_full_validation = False
            witnesses = connection.execute(
                "SELECT review_plan_sha256,witness_sha256,"
                "pre_review_canonical_sha256 "
                "FROM history_coverage_n19_legacy_unbound_witnesses "
                "ORDER BY witness_id"
            ).fetchall()
            mirror = self.n16_claim_ledger.legacy_witness_mirror()
            if mirror is None:
                raise RuntimeError(
                    "N16 ledger legacy witness mirror schema is missing"
                )
            if not witnesses:
                if mirror[0] != "EMPTY":
                    raise RuntimeError(
                        "N16 ledger contains an unpaired legacy witness mirror"
                    )
            elif (
                len(witnesses) != 1
                or mirror[0] != "COMMITTED"
                or tuple(witnesses[0])
                != (mirror[1], mirror[2], mirror[3])
                or mirror[5] != self.n16_claim_ledger.ledger_uuid()
            ):
                raise RuntimeError(
                    "Review and N16 ledger legacy witness evidence disagree"
                )
            after_generation = connection.execute(
                "SELECT generation FROM history_coverage_protected_generation "
                "WHERE singleton_id=1"
            ).fetchone()
            if after_generation != (observed_generation,):
                raise RuntimeError(
                    "N19 protected generation changed during attestation"
                )
            after_schema_version = connection.execute(
                "PRAGMA schema_version"
            ).fetchone()
            if after_schema_version != (review_schema_version,):
                raise RuntimeError(
                    "N19 protected Review schema changed during attestation"
                )
            if (
                family_seal_catalog_sha256(connection)
                != family_catalog_sha256_value
            ):
                raise RuntimeError(
                    "N19 protected Review catalog changed during attestation"
                )
            if requires_full_validation:
                validated_generation = observed_generation
        finally:
            if started_snapshot:
                connection.rollback()
        if validated_generation is None:
            validated_generation = observed_generation
        if (
            validated_generation is not None
            and not (
                allow_pending_protected_generation
                and ledger_highwater is not None
                and validated_generation > ledger_highwater[0]
            )
        ):
            self._runtime_family_graph_startup_validated = True
            # The trusted high-water is the exact in-database protected
            # generation observed by this successfully validated SQLite
            # snapshot.  Connection close and file signatures never advance it.
            self._runtime_family_graph_generation = validated_generation
            self._runtime_family_graph_active_generation = validated_generation

    def _attest_n16_claim_ledger(
        self,
        connection: sqlite3.Connection,
    ):
        summary = _n16_review_claim_summary(connection)
        self.n16_claim_ledger.attest(summary)
        if self._runtime_family_seal_schema_status == "CURRENT":
            self._attest_legacy_witness_pair(connection)
        return summary

    def _attest_n16_claim_ledger_after_catalog_attestation(
        self,
        connection: sqlite3.Connection,
    ):
        """Attest the live ledger without repeating certified catalog work."""

        summary = _n16_review_claim_summary_after_schema_attestation(
            connection
        )
        self.n16_claim_ledger.attest(summary)
        if self._runtime_family_seal_schema_status == "CURRENT":
            self._attest_legacy_witness_pair(connection)
        return summary

    def _attest_n16_ledger_before_review_commit(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        """Authorize one Review commit from one fresh ledger snapshot."""

        summary = _n16_review_claim_summary_after_schema_attestation(
            connection
        )
        pending_publication = self._n16_publication_pair_after_commit
        if pending_publication is None:
            self.n16_claim_ledger.attest(summary)
        else:
            prepared, committed_summary = pending_publication
            observed = self.n16_claim_ledger.prepared_publication()
            if observed != prepared or summary != committed_summary:
                raise RuntimeError(
                    "N16 prepared publication changed before Review commit"
                )
            _claims, target_summary = (
                self.n16_claim_ledger.prepared_target(prepared)
            )
            if target_summary != committed_summary:
                raise RuntimeError(
                    "N16 prepared publication target conflicts with Review"
                )
        if self._runtime_family_seal_schema_status == "CURRENT":
            self._attest_legacy_witness_pair(
                connection,
                allow_pending_protected_generation=True,
            )

    def assert_n16_execution_ready(self) -> None:
        """Fail closed before any paper/live/exchange side effect.

        Ordinary rounds use the independent ledger's fixed-size READY/high-
        water attestation plus exact O(log n) checks for the bounded set of
        active N16 paper/live claims.  Historical full-chain comparison is a
        stopped-service maintenance responsibility, not an unbounded startup
        scan.
        """

        with self._read_only_runtime_snapshot() as connection:
            if self._runtime_n16_schema_status != "CURRENT":
                raise RuntimeError("N16 lifecycle installation is incomplete")
            self._attest_n16_claim_ledger(connection)
            self._attest_n16_open_execution_claims(connection)

    def n16_active_live_claim_identity(
        self,
    ) -> tuple[int, str, str] | None:
        """Return the sole exact active N16 live identity, if one exists.

        This is intentionally separate from paper-trade evidence.  Main uses
        it to bind the unique local PositionState to the durable live link
        before any exchange reconciliation, while pre-review pending order
        phases may legitimately have no live link yet.
        """

        with self._read_only_runtime_snapshot() as connection:
            self._attest_n16_claim_ledger(connection)
            return self._attest_n16_open_execution_claims(connection)

    def _n16_review_claim_marker(
        self,
        connection: sqlite3.Connection,
        symbol: str,
        raw_orders: Any,
        *,
        allow_pending_close: bool = False,
        allow_terminal_close: bool,
    ) -> tuple[int, str] | None:
        """Resolve one Review payload against the independent N16 ledger.

        The strategy marker is decoded before N16's strict signal/structure
        contract is applied.  This preserves legacy N01-N15 Review-only crash
        recovery where structure_id may legitimately be null, while an
        explicit or ledger-backed N16 identity remains exact and fail-closed.
        """

        try:
            payload_strategy_id, payload = self._decode_n16_execution_claim(
                raw_orders,
                nested_strategy=True,
                allow_pending_close=allow_pending_close,
                allow_terminal_close=allow_terminal_close,
            )
        except RuntimeError:
            if _n16_json_payload_has_marker(raw_orders, "live Review"):
                raise RuntimeError("N16 Review identity is malformed")
            return None

        signal_id = payload.get("signal_id")
        structure_id = payload.get("structure_id")
        complete_identity = (
            type(signal_id) is int
            and signal_id > 0
            and type(structure_id) is str
            and len(structure_id) == 24
            and not any(
                character not in "0123456789abcdef"
                for character in structure_id
            )
        )
        if payload_strategy_id != "N16" and not complete_identity:
            return None
        if not complete_identity:
            raise RuntimeError("N16 Review claim identity is invalid")
        by_structure = self.n16_claim_ledger.committed_claim(structure_id)
        by_signal = (
            self.n16_claim_ledger.committed_claim_by_source_signal_id(
                signal_id
            )
        )
        if (
            by_structure is not None
            and by_signal is not None
            and by_structure != by_signal
        ):
            raise RuntimeError("N16 Review claim markers conflict")
        if (
            payload_strategy_id != "N16"
            and by_structure is None
            and by_signal is None
        ):
            return None
        if payload_strategy_id != "N16":
            raise RuntimeError("N16 Review strategy marker conflicts")
        self._attest_n16_committed_structure_claim(
            connection,
            symbol,
            structure_id,
            source_signal_id=signal_id,
        )
        return signal_id, structure_id

    def n16_live_link_marker_for_state(
        self,
        symbol: str,
        opened_at: str,
    ) -> bool:
        """Detect an N16 live-link marker for one exact local state key.

        This reverse lookup starts from the N16-owned
        ``strategy_live_links(symbol, opened_at)`` index and then resolves the
        Review row by primary key.  It is deliberately independent of the mutable
        strategy marker stored in ``position.json``: a local state whose N16
        labels were erased must still be stopped when its Review/live link or
        external permanent claim identifies it as N16.
        """

        _strict_signal_text(symbol, "N16 local live symbol", 64)
        _strict_signal_text(opened_at, "N16 local live opened_at", 64)
        try:
            parsed_opened_at = parse_utc_datetime(opened_at)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("N16 local live opened_at is invalid") from exc
        if parsed_opened_at.isoformat() != opened_at:
            raise RuntimeError("N16 local live opened_at is not canonical")
        with self._read_only_runtime_snapshot() as connection:
            self._attest_n16_claim_ledger(connection)
            link_key_rows = connection.execute(
                "SELECT id "
                "FROM strategy_live_links INDEXED BY "
                "idx_n16_live_link_state_key "
                "WHERE symbol=? AND opened_at=? ORDER BY id LIMIT 2",
                (symbol, opened_at),
            ).fetchall()
            if len(link_key_rows) > 1:
                raise RuntimeError("N16 local live-link key is ambiguous")
            review_key_rows = connection.execute(
                "SELECT id FROM trade_reviews INDEXED BY "
                "idx_n16_trade_review_state_key "
                "WHERE symbol=? AND opened_at=? ORDER BY id LIMIT 2",
                (symbol, opened_at),
            ).fetchall()
            if len(review_key_rows) > 1:
                raise RuntimeError("N16 local Review key is ambiguous")
            review_link_rows = []
            if review_key_rows:
                review_link_rows = connection.execute(
                    "SELECT id FROM strategy_live_links INDEXED BY "
                    "idx_n16_live_link_review WHERE trade_review_id=? "
                    "ORDER BY id LIMIT 2",
                    (review_key_rows[0][0],),
                ).fetchall()
                if len(review_link_rows) > 1:
                    raise RuntimeError("N16 local Review has ambiguous live links")
            candidate_link_ids = {
                row[0] for row in link_key_rows + review_link_rows
            }
            if len(candidate_link_ids) > 1:
                raise RuntimeError("N16 local Review/live key sets conflict")
            if not candidate_link_ids:
                if review_key_rows:
                    orphan_review = connection.execute(
                        "SELECT orders_json FROM trade_reviews WHERE id=?",
                        (review_key_rows[0][0],),
                    ).fetchall()
                    if len(orphan_review) != 1:
                        raise RuntimeError("N16 local Review row is missing")
                    return self._n16_review_claim_marker(
                        connection,
                        symbol,
                        orphan_review[0][0],
                        allow_terminal_close=True,
                    ) is not None
                return False
            candidate_link_id = next(iter(candidate_link_ids))
            link_rows = connection.execute(
                "SELECT id,strategy_id,trade_review_id,symbol,opened_at "
                "FROM strategy_live_links WHERE id=?",
                (candidate_link_id,),
            ).fetchall()
            if len(link_rows) != 1:
                raise RuntimeError("N16 local live-link row is missing")
            (
                link_id,
                link_strategy_id,
                linked_review_id,
                link_symbol,
                link_opened_at,
            ) = link_rows[0]
            if (
                type(link_id) is not int
                or link_id <= 0
                or type(linked_review_id) is not int
                or linked_review_id <= 0
                or type(link_strategy_id) is not str
                or type(link_symbol) is not str
                or link_symbol != symbol
                or type(link_opened_at) is not str
                or link_opened_at != opened_at
            ):
                raise RuntimeError("N16 local live-link row is invalid")
            review_rows = connection.execute(
                "SELECT id,symbol,opened_at,orders_json FROM trade_reviews "
                "WHERE id=?",
                (linked_review_id,),
            ).fetchall()
            if len(review_rows) != 1:
                raise RuntimeError("N16 local live-link Review row is missing")
            review_id, review_symbol, review_opened_at, orders_json = (
                review_rows[0]
            )
            if (
                type(review_id) is not int
                or review_id != linked_review_id
                or type(review_symbol) is not str
                or review_symbol != symbol
                or type(review_opened_at) is not str
                or review_opened_at != opened_at
            ):
                raise RuntimeError("N16 local live-link Review key conflicts")
            if (
                review_key_rows
                and review_key_rows[0][0] != linked_review_id
            ):
                raise RuntimeError("N16 local Review/live edge conflicts")
            try:
                payload_strategy_id, signal_id, structure_id = (
                    self._n16_execution_claim_components(
                        orders_json,
                        nested_strategy=True,
                        allow_terminal_close=True,
                    )
                )
            except RuntimeError:
                if link_strategy_id == "N16":
                    raise
                return False
            by_structure = self.n16_claim_ledger.committed_claim(
                structure_id
            )
            by_signal = (
                self.n16_claim_ledger.committed_claim_by_source_signal_id(
                    signal_id
                )
            )
            if (
                by_structure is not None
                and by_signal is not None
                and by_structure != by_signal
            ):
                raise RuntimeError("N16 local live claim markers conflict")
            is_n16 = (
                link_strategy_id == "N16"
                or payload_strategy_id == "N16"
                or by_structure is not None
                or by_signal is not None
            )
            if not is_n16:
                return False
            if link_strategy_id != "N16" or payload_strategy_id != "N16":
                raise RuntimeError("N16 local live-link identity conflicts")
            self._attest_n16_committed_structure_claim(
                connection,
                symbol,
                structure_id,
                source_signal_id=signal_id,
            )
            return True

    @staticmethod
    def _decode_n16_execution_claim(
        raw_json: Any,
        *,
        nested_strategy: bool,
        allow_pending_close: bool = False,
        allow_terminal_close: bool = False,
    ) -> tuple[str | None, dict[str, Any]]:
        if type(raw_json) is not str or not 2 <= len(raw_json) <= 262_144:
            raise RuntimeError("N16 execution claim JSON is invalid")

        def reject_duplicate_keys(
            pairs: list[tuple[str, Any]],
        ) -> dict[str, Any]:
            result = {}  # type: dict[str, Any]
            for key, value in pairs:
                if type(key) is not str or key in result:
                    raise ValueError("duplicate or invalid N16 execution key")
                result[key] = value
            return result

        try:
            payload = json.loads(
                raw_json,
                object_pairs_hook=reject_duplicate_keys,
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError("N16 execution claim JSON is invalid") from exc
        if type(payload) is not dict:
            raise RuntimeError("N16 execution claim payload is invalid")
        if nested_strategy:
            close_payload = payload.get("close")
            if allow_pending_close:
                required_close_keys = {
                    "exit_reason",
                    "exit_price",
                    "mark_price",
                    "pnl_amount",
                    "pnl_pct",
                    "balance_after",
                }
                if (
                    type(close_payload) is not dict
                    or set(close_payload) != required_close_keys
                    or close_payload.get("exit_reason")
                    != "LIVE_RESULT_PENDING"
                    or any(
                        type(close_payload.get(key)) is not str
                        for key in required_close_keys
                    )
                    or any(
                        close_payload.get(key) != ""
                        for key in required_close_keys
                        if key != "exit_reason"
                    )
                ):
                    raise RuntimeError(
                        "N16 pending live close audit is invalid"
                    )
            elif allow_terminal_close:
                if "close" in payload and type(close_payload) is not dict:
                    raise RuntimeError("N16 terminal live close audit is invalid")
            elif "close" in payload:
                raise RuntimeError("N16 open live claim contains close evidence")
            if "state_orders" in payload:
                expected_keys = (
                    {"state_orders", "close"}
                    if allow_pending_close or "close" in payload
                    else {"state_orders"}
                )
                if payload.keys() != expected_keys:
                    raise RuntimeError(
                        "N16 live claim wrapper is ambiguous"
                    )
                payload = payload.get("state_orders")
                if type(payload) is not dict:
                    raise RuntimeError(
                        "N16 live claim state orders are invalid"
                    )
            payload = payload.get("strategy")
            if type(payload) is not dict:
                raise RuntimeError("N16 live claim identity is missing")
            strategy_id = payload.get("strategy_id")
            if (
                type(strategy_id) is not str
                or not 1 <= len(strategy_id) <= 64
            ):
                raise RuntimeError("N16 live claim strategy is invalid")
            return strategy_id, payload
        return None, payload

    @classmethod
    def _n16_execution_claim_components(
        cls,
        raw_json: Any,
        *,
        nested_strategy: bool,
        allow_pending_close: bool = False,
        allow_terminal_close: bool = False,
    ) -> tuple[str | None, int, str]:
        strategy_id, payload = cls._decode_n16_execution_claim(
            raw_json,
            nested_strategy=nested_strategy,
            allow_pending_close=allow_pending_close,
            allow_terminal_close=allow_terminal_close,
        )
        signal_id = payload.get("signal_id")
        structure_id = payload.get("structure_id")
        if (
            type(signal_id) is not int
            or signal_id <= 0
            or type(structure_id) is not str
            or len(structure_id) != 24
            or any(
                character not in "0123456789abcdef"
                for character in structure_id
            )
        ):
            raise RuntimeError("N16 execution claim identity is invalid")
        return strategy_id, signal_id, structure_id

    @classmethod
    def _n16_execution_claim_identity(
        cls,
        raw_json: Any,
        *,
        nested_strategy: bool,
        allow_pending_close: bool = False,
    ) -> tuple[int, str]:
        strategy_id, signal_id, structure_id = (
            cls._n16_execution_claim_components(
                raw_json,
                nested_strategy=nested_strategy,
                allow_pending_close=allow_pending_close,
            )
        )
        if nested_strategy and strategy_id != "N16":
            raise RuntimeError("N16 live claim identity is missing")
        return signal_id, structure_id

    def _attest_n16_open_execution_claims(
        self,
        connection: sqlite3.Connection,
    ) -> tuple[int, str, str] | None:
        paper_rows = connection.execute(
            "SELECT strategy_id,symbol,detail_json "
            "FROM strategy_paper_trades INDEXED BY idx_n16_paper_open_rows "
            "WHERE result='OPEN' AND id>0 ORDER BY id"
        ).fetchall()
        pending_review_rows = connection.execute(
            "SELECT id,symbol,opened_at,dry_run,status,closed_at,exit_reason,"
            "exit_price,close_mark_price,realized_pnl,realized_pnl_pct,"
            "balance_after_close,orders_json "
            "FROM trade_reviews INDEXED BY idx_n16_review_pending_rows "
            "WHERE status IN ('OPENED','CLOSED_LIVE_RESULT_PENDING') "
            "AND id>0 ORDER BY id"
        ).fetchall()
        live_rows = connection.execute(
            "SELECT link.id,link.strategy_id,link.trade_review_id,"
            "link.symbol,link.opened_at,link.result,review.id,"
            "review.symbol,review.opened_at,review.dry_run,review.status,"
            "review.closed_at,review.exit_reason,review.exit_price,"
            "review.close_mark_price,review.realized_pnl,"
            "review.realized_pnl_pct,review.balance_after_close,"
            "review.orders_json "
            "FROM strategy_live_links AS link INDEXED BY "
            "idx_n16_live_open_rows "
            "LEFT JOIN trade_reviews AS review ON review.id=link.trade_review_id "
            "WHERE link.closed_at IS NULL AND link.id>0 "
            "ORDER BY link.id"
        ).fetchall()

        def external_claim_marker(
            signal_id: int,
            structure_id: str,
        ) -> bool:
            by_structure = self.n16_claim_ledger.committed_claim(
                structure_id
            )
            by_signal = (
                self.n16_claim_ledger.committed_claim_by_source_signal_id(
                    signal_id
                )
            )
            if (
                by_structure is not None
                and by_signal is not None
                and by_structure != by_signal
            ):
                raise RuntimeError("N16 execution claim markers conflict")
            return by_structure is not None or by_signal is not None

        active_n16_claim_count = 0
        active_n16_live_identity = None
        linked_n16_review_ids: set[int] = set()
        for table_strategy_id, symbol, detail_json in paper_rows:
            try:
                _payload_strategy, signal_id, structure_id = (
                    self._n16_execution_claim_components(
                        detail_json,
                        nested_strategy=False,
                    )
                )
            except RuntimeError:
                if table_strategy_id == "N16":
                    raise
                continue
            marker = external_claim_marker(signal_id, structure_id)
            if table_strategy_id != "N16" and not marker:
                continue
            if type(table_strategy_id) is not str or table_strategy_id != "N16":
                raise RuntimeError("N16 paper claim strategy marker conflicts")
            if type(symbol) is not str or not symbol:
                raise RuntimeError("N16 paper claim symbol is invalid")
            self._attest_n16_committed_structure_claim(
                connection,
                symbol,
                structure_id,
                source_signal_id=signal_id,
            )
            active_n16_claim_count += 1

        for review in pending_review_rows:
            (
                review_id,
                review_symbol,
                review_opened_at,
                review_dry_run,
                review_status,
                review_closed_at,
                review_exit_reason,
                review_exit_price,
                review_close_mark_price,
                review_realized_pnl,
                review_realized_pnl_pct,
                review_balance_after_close,
                orders_json,
            ) = review
            open_review = (
                review_status == "OPENED"
                and review_closed_at is None
                and review_exit_reason is None
                and (
                    review_exit_price,
                    review_close_mark_price,
                    review_realized_pnl,
                    review_realized_pnl_pct,
                    review_balance_after_close,
                )
                == (None, None, None, None, None)
            )
            pending_review = (
                review_status == "CLOSED_LIVE_RESULT_PENDING"
                and type(review_closed_at) is str
                and bool(review_closed_at)
                and review_exit_reason == "LIVE_RESULT_PENDING"
                and (
                    review_exit_price,
                    review_close_mark_price,
                    review_realized_pnl,
                    review_realized_pnl_pct,
                    review_balance_after_close,
                )
                == ("", "", "", "", "")
            )
            if type(review_symbol) is not str or not review_symbol:
                raise RuntimeError("active Review symbol is invalid")
            marker = self._n16_review_claim_marker(
                connection,
                review_symbol,
                orders_json,
                allow_pending_close=pending_review,
                allow_terminal_close=False,
            )
            if marker is None:
                continue
            signal_id, structure_id = marker
            if (
                type(review_id) is not int
                or review_id <= 0
                or type(review_opened_at) is not str
                or not review_opened_at
                or type(review_dry_run) is not int
                or review_dry_run not in (0, 1)
                or not (open_review or pending_review)
            ):
                raise RuntimeError("N16 active Review identity is invalid")
            edges = connection.execute(
                "SELECT id,strategy_id,symbol,opened_at,closed_at,result "
                "FROM strategy_live_links INDEXED BY "
                "idx_n16_live_link_review WHERE trade_review_id=? "
                "ORDER BY id LIMIT 2",
                (review_id,),
            ).fetchall()
            if len(edges) > 1:
                raise RuntimeError("N16 active Review has ambiguous live links")
            review_identity = (signal_id, review_symbol, structure_id)
            if not edges:
                if (
                    active_n16_live_identity is not None
                    and active_n16_live_identity != review_identity
                ):
                    raise RuntimeError("multiple active N16 live claims")
                active_n16_live_identity = review_identity
                active_n16_claim_count += 1
                continue
            (
                edge_id,
                edge_strategy_id,
                edge_symbol,
                edge_opened_at,
                edge_closed_at,
                edge_result,
            ) = edges[0]
            if (
                type(edge_id) is not int
                or edge_id <= 0
                or edge_strategy_id != "N16"
                or edge_symbol != review_symbol
                or edge_opened_at != review_opened_at
                or edge_closed_at is not None
                or edge_result is not None
            ):
                raise RuntimeError("N16 active Review/live edge is invalid")
            linked_n16_review_ids.add(review_id)

        for (
            link_id,
            link_strategy_id,
            linked_review_id,
            link_symbol,
            link_opened_at,
            link_result,
            review_id,
            review_symbol,
            review_opened_at,
            review_dry_run,
            review_status,
            review_closed_at,
            review_exit_reason,
            review_exit_price,
            review_close_mark_price,
            review_realized_pnl,
            review_realized_pnl_pct,
            review_balance_after_close,
            orders_json,
        ) in live_rows:
            if (
                type(link_id) is not int
                or link_id <= 0
                or type(link_strategy_id) is not str
                or type(link_symbol) is not str
                or not link_symbol
                or type(link_opened_at) is not str
                or not link_opened_at
                or link_result is not None
            ):
                raise RuntimeError("active live-link identity is invalid")
            if review_id is None:
                if link_strategy_id == "N16":
                    raise RuntimeError(
                        "N16 active live link is missing its Review edge"
                    )
                continue
            open_review = (
                review_status == "OPENED"
                and review_closed_at is None
                and review_exit_reason is None
                and (
                    review_exit_price,
                    review_close_mark_price,
                    review_realized_pnl,
                    review_realized_pnl_pct,
                    review_balance_after_close,
                )
                == (None, None, None, None, None)
            )
            pending_review = (
                review_status == "CLOSED_LIVE_RESULT_PENDING"
                and type(review_closed_at) is str
                and bool(review_closed_at)
                and review_exit_reason == "LIVE_RESULT_PENDING"
                and (
                    review_exit_price,
                    review_close_mark_price,
                    review_realized_pnl,
                    review_realized_pnl_pct,
                    review_balance_after_close,
                )
                == ("", "", "", "", "")
            )
            terminal_candidate = (
                review_status in {"CLOSED_STOP_LOSS", "CLOSED_TAKE_PROFIT"}
                and type(review_closed_at) is str
                and bool(review_closed_at)
                and review_exit_reason in {"STOP_LOSS", "TAKE_PROFIT"}
            )
            marker = self._n16_review_claim_marker(
                connection,
                review_symbol,
                orders_json,
                allow_pending_close=pending_review,
                allow_terminal_close=terminal_candidate,
            )
            if marker is None:
                if link_strategy_id == "N16":
                    raise RuntimeError("N16 active Review identity is missing")
                continue
            signal_id, structure_id = marker
            terminal_review = False
            if terminal_candidate:
                if type(review_dry_run) is not int or review_dry_run not in (0, 1):
                    raise RuntimeError(
                        "N16 recoverable Review dry-run marker is invalid"
                    )
                expected_result = (
                    "WIN" if review_exit_reason == "TAKE_PROFIT" else "LOSS"
                )
                stored_close_payload = self._strict_review_close_payload(
                    orders_json,
                    (
                        review_exit_reason,
                        review_exit_price,
                        review_close_mark_price,
                        review_realized_pnl,
                        review_realized_pnl_pct,
                        review_balance_after_close,
                    ),
                    allow_resolution_detail=True,
                )
                try:
                    _validate_terminal_financial_evidence(
                        exit_reason=review_exit_reason,
                        exit_price=review_exit_price,
                        mark_price=review_close_mark_price,
                        pnl_amount=review_realized_pnl,
                        pnl_pct=review_realized_pnl_pct,
                        balance_after=review_balance_after_close,
                        dry_run=bool(review_dry_run),
                    )
                except (TypeError, ValueError) as exc:
                    raise RuntimeError(
                        "N16 recoverable Review financial evidence is invalid"
                    ) from exc
                event_count, event_time = (
                    self._terminal_live_close_event_evidence(
                        connection,
                        review_symbol,
                        review_id,
                        review_exit_reason,
                        stored_close_payload,
                        self._terminal_close_event_type(
                            "N16", bool(review_dry_run)
                        ),
                        require_n16_index=True,
                    )
                )
                try:
                    opened_time = canonical_utc_datetime(review_opened_at)
                    closed_time = canonical_utc_datetime(review_closed_at)
                except (TypeError, ValueError) as exc:
                    raise RuntimeError(
                        "N16 recoverable Review time is invalid"
                    ) from exc
                terminal_review = (
                    stored_close_payload is not None
                    and expected_result in {"WIN", "LOSS"}
                    and event_count == 1
                    and event_time == review_closed_at
                    and opened_time <= closed_time
                )
            if (
                link_strategy_id != "N16"
                or type(linked_review_id) is not int
                or linked_review_id <= 0
                or type(review_id) is not int
                or review_id != linked_review_id
                or type(review_symbol) is not str
                or review_symbol != link_symbol
                or type(review_opened_at) is not str
                or review_opened_at != link_opened_at
                or type(review_dry_run) is not int
                or review_dry_run not in (0, 1)
                or type(review_status) is not str
                or not (open_review or pending_review or terminal_review)
                or (
                    not terminal_review
                    and review_id not in linked_n16_review_ids
                )
            ):
                raise RuntimeError("N16 live claim identity is invalid")
            live_identity = (signal_id, link_symbol, structure_id)
            if (
                active_n16_live_identity is not None
                and active_n16_live_identity != live_identity
            ):
                raise RuntimeError("multiple active N16 live claims")
            active_n16_live_identity = live_identity
            active_n16_claim_count += 1
        if active_n16_claim_count > 1:
            raise RuntimeError("multiple active N16 execution claims")
        return active_n16_live_identity

    def assert_n16_current_scan_claims(
        self,
        scan_id: int,
        expected_claims: tuple[tuple[int, str, str], ...],
    ) -> None:
        """Attest this round's bounded N16 execution claims before execution."""

        if (
            type(scan_id) is not int
            or scan_id <= 0
            or type(expected_claims) is not tuple
            or len(expected_claims) > 1
        ):
            raise RuntimeError("N16 current claim set is invalid")
        normalized = []
        for value in expected_claims:
            if type(value) is not tuple or len(value) != 3:
                raise RuntimeError("N16 current claim identity is invalid")
            signal_id, symbol, structure_id = value
            if type(signal_id) is not int or signal_id <= 0:
                raise RuntimeError("N16 current signal identity is invalid")
            _strict_signal_text(symbol, "N16 current symbol", 64)
            _strict_signal_text(
                structure_id, "N16 current structure_id", 24
            )
            normalized.append(value)
        with self._read_only_runtime_snapshot() as connection:
            self._attest_n16_claim_ledger(connection)
            retention = _strategy_signal_retention_row(connection)
            if retention[0] != scan_id or retention[1:3] != (1, "COMPLETE"):
                raise RuntimeError("N16 current batch pointer conflicts")
            _validate_complete_strategy_signal_graph(connection, retention)
            actual = connection.execute(
                "SELECT source_signal_id,symbol,structure_id "
                "FROM strategy_passed_structure_ledger "
                "WHERE strategy_id='N16' AND source_scan_id=? "
                "AND claim_state='ACTIVE' ORDER BY source_signal_id",
                (scan_id,),
            ).fetchall()
            if actual != normalized:
                raise RuntimeError("N16 current claim set conflicts")
            for signal_id, symbol, structure_id in normalized:
                claim = self._attest_n16_committed_structure_claim(
                    connection,
                    symbol,
                    structure_id,
                    source_signal_id=signal_id,
                )
                if claim[5] != scan_id:
                    raise RuntimeError("N16 current claim scan conflicts")

    def assert_n16_execution_claim(
        self,
        signal_id: int,
        symbol: str,
        structure_id: str,
    ) -> None:
        """Authenticate one state-file N16 execution claim exactly."""

        if type(signal_id) is not int or signal_id <= 0:
            raise RuntimeError("N16 execution signal identity is invalid")
        _strict_signal_text(symbol, "N16 execution symbol", 64)
        _strict_signal_text(
            structure_id, "N16 execution structure_id", 24
        )
        with self._read_only_runtime_snapshot() as connection:
            self._attest_n16_claim_ledger(connection)
            self._attest_n16_committed_structure_claim(
                connection,
                symbol,
                structure_id,
                source_signal_id=signal_id,
            )

    def assert_n17_execution_claim(
        self,
        signal_id: int,
        symbol: str,
        structure_id: str,
    ) -> None:
        """Authenticate one exact published N17 claim and lifecycle row."""

        from .n17_analyzer import decode_n17_state_evidence
        from .n17_schema import n17_schema_status

        if type(signal_id) is not int or signal_id <= 0:
            raise RuntimeError("N17 execution signal identity is invalid")
        _strict_signal_text(symbol, "N17 execution symbol", 64)
        _strict_signal_text(structure_id, "N17 execution structure_id", 24)
        with self._read_only_runtime_snapshot() as connection:
            self._attest_n16_claim_ledger(connection)
            if n17_schema_status(connection) != "CURRENT":
                raise RuntimeError("N17 lifecycle schema is not current")
            _scan_id, detail, _signal_sha256 = (
                _attest_permanent_execution_claim(
                    connection, signal_id, "N17", symbol, structure_id
                )
            )
            rows = connection.execute(
                "SELECT strategy_id,symbol,family_id,structure_id,stage,reason,"
                "quote_volume_rank,box_start_time_ms,box_end_time_ms,"
                "reset_after_time_ms,evidence_json,evidence_sha256 "
                "FROM n17_range_support_states WHERE strategy_id='N17' "
                "AND symbol=? AND structure_id=?",
                (symbol, structure_id),
            ).fetchall()
            if len(rows) != 1:
                raise RuntimeError("N17 execution claim graph is incomplete")
            row = rows[0]
            try:
                decoded = decode_n17_state_evidence(
                    row[10], expected_symbol=symbol
                )
            except Exception as exc:
                raise RuntimeError("N17 execution evidence is invalid") from exc
            if (
                row[:6]
                != (
                    "N17", symbol, decoded.family_id, structure_id,
                    "CONFIRMED", "PASSED",
                )
                or type(row[6]) is not int
                or type(row[7]) is not int
                or type(row[8]) is not int
                or (row[9] is not None and type(row[9]) is not int)
                or tuple(row[6:10])
                != (
                    decoded.quote_volume_rank,
                    decoded.box_start_time_ms,
                    decoded.box_end_time_ms,
                    decoded.reset_after_time_ms,
                )
                or decoded.structure_id != structure_id
                or decoded.stage != "CONFIRMED"
                or decoded.reason != "PASSED"
                or hashlib.sha256(row[10].encode("utf-8")).hexdigest()
                != row[11]
                or detail.get("state_stage") != "CONFIRMED"
                or detail.get("evidence_sha256") != row[11]
                or detail.get("structure_id") != structure_id
            ):
                raise RuntimeError("N17 execution claim graph conflicts")

    def assert_n19_execution_claim(
        self,
        signal_id: int,
        symbol: str,
        structure_id: str,
    ) -> None:
        """Authenticate one exact published N19 claim and lifecycle row."""

        from .n19_analyzer import decode_n19_state_evidence
        from .n19_schema import n19_schema_status

        if type(signal_id) is not int or signal_id <= 0:
            raise RuntimeError("N19 execution signal identity is invalid")
        _strict_signal_text(symbol, "N19 execution symbol", 64)
        _strict_signal_text(structure_id, "N19 execution structure_id", 24)
        with self._read_only_runtime_snapshot() as connection:
            self._attest_n16_claim_ledger(connection)
            if n19_schema_status(connection) != "CURRENT":
                raise RuntimeError("N19 lifecycle schema is not current")
            _scan_id, detail, _signal_sha256 = (
                _attest_permanent_execution_claim(
                    connection, signal_id, "N19", symbol, structure_id
                )
            )
            rows = connection.execute(
                "SELECT strategy_id,symbol,family_id,structure_id,stage,reason,"
                "quote_volume_rank,s_open_time_ms,x_open_time_ms,"
                "reset_after_time_ms,evidence_json,evidence_sha256 "
                "FROM n19_staircase_states WHERE strategy_id='N19' "
                "AND symbol=? AND structure_id=?",
                (symbol, structure_id),
            ).fetchall()
            if len(rows) != 1:
                raise RuntimeError("N19 execution claim graph is incomplete")
            row = rows[0]
            try:
                decoded = decode_n19_state_evidence(
                    row[10], expected_symbol=symbol
                )
            except Exception as exc:
                raise RuntimeError("N19 execution evidence is invalid") from exc
            allowed_stage = row[4:6] == ("CONFIRMED", "PASSED")
            if row[4] in {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}:
                _attest_terminal_execution_derivation(
                    detail,
                    decoded.evidence,
                    {
                        "MISSED": frozenset({
                            "N19_HISTORICAL_ENTRY_MISSED",
                            "N19_ENTRY_PRICE_ABOVE_MAX",
                        }),
                        "INVALID": frozenset({"N19_ENTRY_BROKE_X_LOW"}),
                        "EXPIRED": frozenset({"N19_ENTRY_WINDOW_EXPIRED"}),
                    },
                    strategy_id="N19",
                )
                allowed_stage = True
            if (
                row[:4] != ("N19", symbol, decoded.family_id, structure_id)
                or row[4:6] != (decoded.stage, decoded.reason)
                or not allowed_stage
                or type(row[6]) is not int
                or type(row[7]) is not int
                or type(row[8]) is not int
                or (row[9] is not None and type(row[9]) is not int)
                or tuple(row[6:10])
                != (
                    decoded.quote_volume_rank,
                    decoded.s_open_time_ms,
                    decoded.x_open_time_ms,
                    decoded.reset_after_time_ms,
                )
                or decoded.structure_id != structure_id
                or hashlib.sha256(row[10].encode("utf-8")).hexdigest()
                != row[11]
                or detail.get("state_stage") != "CONFIRMED"
                or type(detail.get("evidence_sha256")) is not str
                or (
                    row[4] == "CONFIRMED"
                    and detail.get("evidence_sha256") != row[11]
                )
                or detail.get("structure_id") != structure_id
            ):
                raise RuntimeError("N19 execution claim graph conflicts")

    def assert_n18_execution_claim(
        self,
        signal_id: int,
        symbol: str,
        structure_id: str,
    ) -> None:
        """Authenticate one exact published N18 claim and lifecycle row."""

        from .n18_analyzer import decode_n18_state_evidence
        from .n18_schema import n18_schema_status

        if type(signal_id) is not int or signal_id <= 0:
            raise RuntimeError("N18 execution signal identity is invalid")
        _strict_signal_text(symbol, "N18 execution symbol", 64)
        _strict_signal_text(structure_id, "N18 execution structure_id", 24)
        with self._read_only_runtime_snapshot() as connection:
            self._attest_n16_claim_ledger(connection)
            if n18_schema_status(connection) != "CURRENT":
                raise RuntimeError("N18 lifecycle schema is not current")
            _scan_id, detail, _signal_sha256 = (
                _attest_permanent_execution_claim(
                    connection, signal_id, "N18", symbol, structure_id
                )
            )
            rows = connection.execute(
                "SELECT strategy_id,symbol,family_id,structure_id,stage,reason,"
                "quote_volume_rank,l1_open_time_ms,a_open_time_ms,"
                "terminal_cutoff_time_ms,evidence_json,evidence_sha256 "
                "FROM n18_triangle_states WHERE strategy_id='N18' "
                "AND symbol=? AND structure_id=?",
                (symbol, structure_id),
            ).fetchall()
            if len(rows) != 1:
                raise RuntimeError("N18 execution claim graph is incomplete")
            row = rows[0]
            try:
                decoded = decode_n18_state_evidence(
                    row[10], expected_symbol=symbol
                )
            except Exception as exc:
                raise RuntimeError("N18 execution evidence is invalid") from exc
            allowed_stage = row[4:6] == ("CONFIRMED", "PASSED")
            if row[4] in {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}:
                _attest_terminal_execution_derivation(
                    detail,
                    decoded.evidence,
                    {
                        "MISSED": frozenset({
                            "N18_HISTORICAL_ENTRY_MISSED",
                            "N18_ENTRY_PRICE_ABOVE_MAX",
                        }),
                        "INVALID": frozenset({
                            "N18_ENTRY_TRIANGLE_INVALIDATED",
                            "N18_ENTRY_BREAKOUT_NOT_HELD",
                        }),
                        "EXPIRED": frozenset({"N18_ENTRY_WINDOW_EXPIRED"}),
                    },
                    strategy_id="N18",
                )
                allowed_stage = True
            if (
                row[:4] != ("N18", symbol, decoded.family_id, structure_id)
                or row[4:6] != (decoded.stage, decoded.reason)
                or not allowed_stage
                or type(row[6]) is not int
                or type(row[7]) is not int
                or type(row[8]) is not int
                or (row[9] is not None and type(row[9]) is not int)
                or tuple(row[6:10])
                != (
                    decoded.quote_volume_rank,
                    decoded.l1_open_time_ms,
                    decoded.a_open_time_ms,
                    decoded.terminal_cutoff_time_ms,
                )
                or decoded.structure_id != structure_id
                or hashlib.sha256(row[10].encode("utf-8")).hexdigest()
                != row[11]
                or detail.get("state_stage") != "CONFIRMED"
                or type(detail.get("evidence_sha256")) is not str
                or (
                    row[4] == "CONFIRMED"
                    and detail.get("evidence_sha256") != row[11]
                )
                or detail.get("structure_id") != structure_id
            ):
                raise RuntimeError("N18 execution claim graph conflicts")

    def assert_n20_execution_claim(
        self,
        signal_id: int,
        symbol: str,
        structure_id: str,
    ) -> None:
        """Authenticate one exact published N20 winner and episode row."""

        from .n20_analyzer import (
            N20StateRecord,
            canonical_sha256,
            decode_n20_state_evidence,
        )
        from .n20_schema import n20_schema_status

        if type(signal_id) is not int or signal_id <= 0:
            raise RuntimeError("N20 execution signal identity is invalid")
        _strict_signal_text(symbol, "N20 execution symbol", 64)
        _strict_signal_text(structure_id, "N20 execution structure_id", 24)
        with self._read_only_runtime_snapshot() as connection:
            self._attest_n16_claim_ledger(connection)
            if n20_schema_status(connection) != "CURRENT":
                raise RuntimeError("N20 lifecycle schema is not current")
            _scan_id, detail, _signal_sha256 = (
                _attest_permanent_execution_claim(
                    connection, signal_id, "N20", symbol, structure_id
                )
            )
            rows = connection.execute(
                "SELECT strategy_id,episode_id,stage,reason,m0_open_time_ms,"
                "d1_open_time_ms,c_open_time_ms,winner_symbol,structure_id,"
                "terminal_cutoff_time_ms,evidence_blob,evidence_size,"
                "evidence_sha256 FROM n20_market_episodes "
                "WHERE strategy_id='N20' AND winner_symbol=? "
                "AND structure_id=?",
                (symbol, structure_id),
            ).fetchall()
            if len(rows) != 1:
                raise RuntimeError("N20 execution claim graph is incomplete")
            row = rows[0]
            try:
                blob = row[10] if type(row[10]) is bytes else bytes(row[10])
                decoded = decode_n20_state_evidence(blob, row[11], row[12])
            except Exception as exc:
                raise RuntimeError("N20 execution evidence is invalid") from exc
            confirmed_digest = row[12]
            allowed_stage = row[2:4] == ("CONFIRMED", "PASSED")
            if row[2:4] == ("CONSUMED", "N20_EPISODE_CONSUMED"):
                try:
                    winner = decoded.evidence["winner"]
                    entry = winner["entry"]
                    if (
                        type(entry) is not dict
                        or type(entry.get("open_time_ms")) is not int
                        or row[9] != entry["open_time_ms"]
                    ):
                        raise ValueError("N20 consumed cutoff conflicts")
                    confirmed_evidence = dict(decoded.evidence)
                    confirmed_evidence["stage"] = "CONFIRMED"
                    confirmed_evidence["reason"] = "PASSED"
                    confirmed_evidence["terminal_cutoff_time_ms"] = None
                    unsigned = dict(confirmed_evidence)
                    unsigned.pop("canonical_sha256", None)
                    confirmed_evidence["canonical_sha256"] = (
                        canonical_sha256(unsigned)
                    )
                    confirmed = N20StateRecord(
                        "N20", decoded.episode_id, "CONFIRMED", "PASSED",
                        decoded.m0_open_time_ms, decoded.d1_open_time_ms,
                        decoded.c_open_time_ms, symbol, structure_id, None,
                        confirmed_evidence,
                    )
                    confirmed_blob, confirmed_size, confirmed_digest = (
                        confirmed.encoded
                    )
                    reconstructed = decode_n20_state_evidence(
                        confirmed_blob, confirmed_size, confirmed_digest
                    )
                    if (
                        reconstructed.stage != "CONFIRMED"
                        or reconstructed.reason != "PASSED"
                        or reconstructed.structure_id != structure_id
                        or reconstructed.winner_symbol != symbol
                    ):
                        raise ValueError("N20 confirmed reconstruction conflicts")
                    allowed_stage = True
                except Exception as exc:
                    raise RuntimeError(
                        "N20 consumed execution derivation is invalid"
                    ) from exc
            if (
                type(row[4]) is not int
                or type(row[5]) is not int
                or type(row[6]) is not int
                or (row[9] is not None and type(row[9]) is not int)
                or type(row[11]) is not int
                or type(row[12]) is not str
                or tuple(row[:10])
                != (
                    "N20", decoded.episode_id, decoded.stage, decoded.reason,
                    decoded.m0_open_time_ms, decoded.d1_open_time_ms,
                    decoded.c_open_time_ms, symbol, structure_id,
                    decoded.terminal_cutoff_time_ms,
                )
                or not allowed_stage
                or decoded.strategy_id != "N20"
                or decoded.winner_symbol != symbol
                or decoded.structure_id != structure_id
                or detail.get("episode_evidence_sha256") != confirmed_digest
                or detail.get("structure_id") != structure_id
            ):
                raise RuntimeError("N20 execution claim graph conflicts")

    def assert_micro_execution_claim(
        self,
        signal_id: int,
        strategy_id: str,
        symbol: str,
        structure_id: str,
    ) -> None:
        """Authenticate one exact active N21-N25 claim in bounded lookups."""

        from .micro_schema import MICRO_STRATEGY_IDS, micro_schema_status

        if type(signal_id) is not int or signal_id <= 0:
            raise RuntimeError("micro execution signal identity is invalid")
        if strategy_id not in MICRO_STRATEGY_IDS:
            raise RuntimeError("micro execution strategy identity is invalid")
        _strict_signal_text(symbol, "micro execution symbol", 64)
        _strict_signal_text(structure_id, "micro execution structure_id", 24)
        with self._read_only_runtime_snapshot() as connection:
            self._attest_n16_claim_ledger(connection)
            if micro_schema_status(connection) != "CURRENT":
                raise RuntimeError("N21-N25 lifecycle schema is not current")
            scan_id, detail, _signal_sha256 = (
                _attest_permanent_execution_claim(
                    connection, signal_id, strategy_id, symbol, structure_id
                )
            )
            rows = connection.execute(
                "SELECT source_scan_id,strategy_id,symbol,structure_id,"
                "claim_state,evidence_json,evidence_sha256 "
                "FROM micro_strategy_lifecycle WHERE source_signal_id=?",
                (signal_id,),
            ).fetchall()
            if len(rows) != 1:
                raise RuntimeError("micro execution claim graph is incomplete")
            row = rows[0]
            expected = (scan_id, strategy_id, symbol, structure_id, "ACTIVE")
            try:
                lifecycle_evidence = _load_strict_strategy_signal_detail_json(
                    row[5]
                )
                canonical = json.dumps(
                    lifecycle_evidence,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
            except (TypeError, ValueError) as exc:
                raise RuntimeError("micro execution evidence is invalid") from exc
            analyses = connection.execute(
                "SELECT evidence_json,evidence_sha256 FROM micro_passed_analyses "
                "WHERE source_scan_id=? AND strategy_id=? AND symbol=? "
                "AND structure_id=?",
                (scan_id, strategy_id, symbol, structure_id),
            ).fetchall()
            if (
                type(row[0]) is not int
                or tuple(row[:5]) != expected
                or canonical != row[5]
                or hashlib.sha256(row[5].encode("utf-8")).hexdigest()
                != row[6]
                or lifecycle_evidence.get("strategy_id") != strategy_id
                or lifecycle_evidence.get("symbol") != symbol
                or lifecycle_evidence.get("structure_id") != structure_id
                or detail.get("evidence") != lifecycle_evidence
                or analyses != [(row[5], row[6])]
            ):
                raise RuntimeError("micro execution claim graph conflicts")

    def assert_no_active_live_link_for_cleanup(
        self,
        strategy_id: str,
        symbol: str,
    ) -> None:
        """Require a pre-live cleanup state to have no durable live edge."""

        _strict_signal_text(strategy_id, "cleanup strategy_id", 16)
        _strict_signal_text(symbol, "cleanup symbol", 64)
        with self._read_only_runtime_snapshot() as connection:
            self._attest_n16_claim_ledger(connection)
            rows = connection.execute(
                "SELECT id,trade_review_id FROM strategy_live_links "
                "WHERE strategy_id=? AND symbol=? AND closed_at IS NULL "
                "ORDER BY id LIMIT 2",
                (strategy_id, symbol),
            ).fetchall()
            if rows:
                raise RuntimeError(
                    "pre-live cleanup conflicts with an active live link"
                )

    def n16_execution_identity_is_claimed(
        self,
        signal_id: Any,
        structure_id: Any,
    ) -> bool:
        """Classify redundant state markers against the independent ledger.

        This bounded O(log n) lookup is intentionally independent of the
        mutable strategy marker stored in ``position.json``.  If either side
        points at N16, both identities must resolve to the same committed
        claim; numeric coercion and one-sided matches are rejected.
        """

        valid_signal = type(signal_id) is int and signal_id > 0
        valid_structure = (
            type(structure_id) is str
            and len(structure_id) == 24
            and all(
                character in "0123456789abcdef"
                for character in structure_id
            )
        )
        with self._read_only_runtime_snapshot() as connection:
            self._attest_n16_claim_ledger(connection)
            by_signal = (
                self.n16_claim_ledger.committed_claim_by_source_signal_id(
                    signal_id
                )
                if valid_signal
                else None
            )
            by_structure = (
                self.n16_claim_ledger.committed_claim(structure_id)
                if valid_structure
                else None
            )
        if by_signal is None and by_structure is None:
            return False
        if (
            not valid_signal
            or not valid_structure
            or by_signal is None
            or by_structure is None
            or by_signal != by_structure
        ):
            raise RuntimeError("N16 execution identity markers conflict")
        return True

    def begin_scan(self, scanned_count: int, candidates: list[FundingCandidate], dry_run: bool) -> int | None:
        try:
            payload = [
                {
                    "symbol": candidate.symbol,
                    "funding_rate": str(candidate.funding_rate) if candidate.funding_rate is not None else "",
                    "mark_price": str(candidate.mark_price),
                    "candidate_universe": candidate.candidate_universe,
                    "quote_volume": str(candidate.quote_volume) if candidate.quote_volume is not None else None,
                    "quote_volume_rank": candidate.quote_volume_rank,
                }
                for candidate in candidates
            ]
            with self._connect() as connection:
                self._attest_n16_claim_ledger(connection)
                connection.execute("BEGIN IMMEDIATE")
                retention = _strategy_signal_retention_row(connection)
                if retention[1:3] != (1, "COMPLETE"):
                    raise RuntimeError(
                        "strategy signal retention maintenance is required"
                    )
                _validate_complete_strategy_signal_graph(connection, retention)
                staging_rows = connection.execute(
                    """
                    SELECT scan_id, recorded_count, first_signal_id, last_signal_id
                    FROM strategy_signal_batches WHERE state = 'STAGING'
                    """
                ).fetchall()
                if len(staging_rows) > 1:
                    raise RuntimeError("multiple strategy signal staging batches")
                # _validate_complete_strategy_signal_graph above already
                # performs the one global orphan-STAGED ownership proof using
                # the authenticated claim_state-leading indexes.  Do not scan
                # the same permanent tables a second time in this hot path.
                for staging_scan_id, recorded_count, first_id, last_id in staging_rows:
                    if type(staging_scan_id) is not int or type(recorded_count) is not int:
                        raise RuntimeError("invalid strategy signal staging batch")
                    staging_batch = connection.execute(
                        """
                        SELECT manifest_sha256 FROM strategy_signal_batches
                        WHERE scan_id = ? AND state = 'STAGING'
                        """,
                        (staging_scan_id,),
                    ).fetchone()
                    actual = _strategy_signal_batch_snapshot(
                        connection, staging_scan_id
                    )
                    if (
                        staging_batch is None
                        or type(staging_batch[0]) is not str
                        or actual
                        != (recorded_count, first_id, last_id, staging_batch[0])
                    ):
                        raise RuntimeError(
                            "strategy signal staging batch identity mismatch"
                        )
                    _delete_staged_strategy_signal_batch(
                        connection,
                        staging_scan_id,
                        recorded_count,
                        first_id,
                        last_id,
                    )
                cursor = connection.execute(
                    """
                    INSERT INTO scans (
                        started_at, mode, scanned_count, candidate_count, candidates_json
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        utc_now(),
                        "DRY_RUN" if dry_run else "LIVE",
                        scanned_count,
                        len(candidates),
                        json_dumps(payload),
                    ),
                )
                scan_id = int(cursor.lastrowid)
                now = utc_now()
                connection.execute(
                    """
                    INSERT INTO strategy_signal_batches (
                        scan_id, state, recorded_count, expected_count,
                        first_signal_id, last_signal_id, manifest_sha256,
                        completed_at, created_at, updated_at
                    ) VALUES (?, 'STAGING', 0, NULL, NULL, NULL, ?, NULL, ?, ?)
                    """,
                    (scan_id, _SIGNAL_MANIFEST_SEED, now, now),
                )
                _validate_complete_strategy_signal_graph(connection, retention)
                return scan_id
        except Exception as exc:
            self.logger.warning("Review DB write failed while beginning scan: %s", exc)
            return None

    def complete_scan(self, scan_id: int | None, opened: bool, note: str | None = None) -> None:
        if scan_id is None:
            return
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    "UPDATE scans SET completed_at = ?, opened = ?, note = ? WHERE id = ?",
                    (utc_now(), int(opened), note, scan_id),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("scan completion identity mismatch")
                batch = connection.execute(
                    """
                    SELECT state, recorded_count
                    FROM strategy_signal_batches WHERE scan_id = ?
                    """,
                    (scan_id,),
                ).fetchone()
                if batch == ("STAGING", 0):
                    connection.execute(
                        "DELETE FROM strategy_signal_batches WHERE scan_id = ?",
                        (scan_id,),
                    )
        except Exception as exc:
            self.logger.warning("Review DB write failed while completing scan: %s", exc)

    def upsert_strategy_definitions(self, strategies: list[Any] | tuple[Any, ...]) -> None:
        now = utc_now()
        try:
            with self._connect() as connection:
                for strategy in strategies:
                    config_payload = (
                        strategy.to_jsonable()
                        if hasattr(strategy, "to_jsonable")
                        else {
                            "strategy_id": strategy.strategy_id,
                            "name": strategy.name,
                            "allowed_patterns": list(strategy.allowed_patterns),
                            "funding_threshold": str(strategy.funding_threshold),
                            "loss_symbol_cooldown_hours": strategy.loss_symbol_cooldown_hours,
                            "enabled": strategy.enabled,
                        }
                    )
                    connection.execute(
                        """
                        INSERT INTO strategy_definitions (
                            strategy_id, name, enabled, config_json, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(strategy_id) DO UPDATE SET
                            name = excluded.name,
                            enabled = excluded.enabled,
                            config_json = excluded.config_json,
                            updated_at = excluded.updated_at
                        """,
                        (
                            strategy.strategy_id,
                            strategy.name,
                            int(strategy.enabled),
                            json_dumps(config_payload),
                            now,
                            now,
                        ),
                    )
                    self._ensure_strategy_state(connection, strategy.strategy_id, now)
        except Exception as exc:
            raise RuntimeError(
                "Review DB write failed while upserting strategy definitions"
            ) from exc

    def _ensure_strategy_state(
        self,
        connection: sqlite3.Connection,
        strategy_id: str,
        now: str | None = None,
    ) -> None:
        timestamp = now or utc_now()
        connection.execute(
            """
            INSERT OR IGNORE INTO strategy_states (
                strategy_id, consecutive_wins, paper_trade_count, win_count,
                loss_count, win_rate, live_eligible, updated_at
            ) VALUES (?, 0, 0, 0, 0, '0', 0, ?)
            """,
            (strategy_id, timestamp),
        )

    def get_strategy_state(self, strategy_id: str) -> StrategyState:
        now = utc_now()
        with self._connect() as connection:
            self._ensure_strategy_state(connection, strategy_id, now)
            row = connection.execute(
                """
                SELECT strategy_id, consecutive_wins, paper_trade_count,
                       win_count, loss_count, win_rate, live_eligible, live_result_pending,
                       last_trade_result, last_trade_closed_at, updated_at
                FROM strategy_states
                WHERE strategy_id = ?
                """,
                (strategy_id,),
            ).fetchone()
        return StrategyState(
            strategy_id=str(row[0]),
            consecutive_wins=int(row[1]),
            paper_trade_count=int(row[2]),
            win_count=int(row[3]),
            loss_count=int(row[4]),
            win_rate=str(row[5]),
            live_eligible=bool(row[6]),
            live_result_pending=bool(row[7]),
            last_trade_result=str(row[8]) if row[8] is not None else None,
            last_trade_closed_at=str(row[9]) if row[9] is not None else None,
            updated_at=str(row[10]),
        )

    def strategy_effectiveness_status(
        self,
        strategy_id: str,
        minimum_closed_samples: int = 30,
    ) -> dict[str, Any]:
        if minimum_closed_samples < 1:
            raise ValueError("minimum_closed_samples must be positive")
        state = self.get_strategy_state(strategy_id)
        closed_samples = state.paper_trade_count
        return {
            "strategy_id": strategy_id,
            "closed_samples": closed_samples,
            "minimum_closed_samples": minimum_closed_samples,
            "status": (
                "INSUFFICIENT_SAMPLE"
                if closed_samples < minimum_closed_samples
                else "READY_FOR_EVALUATION"
            ),
            "win_rate": state.win_rate,
        }

    @staticmethod
    def _n16_state_row(
        connection: sqlite3.Connection,
        strategy_id: str,
        episode_id: str,
    ) -> N16TrendSupportState | None:
        row = connection.execute(
            """
            SELECT id, strategy_id, symbol, episode_id, structure_id,
                   stage, reason, quote_volume_rank, evidence_json,
                   evidence_sha256, created_at, updated_at
            FROM n16_trend_support_states
            WHERE strategy_id = ? AND episode_id = ?
            """,
            (strategy_id, episode_id),
        ).fetchone()
        return _validated_n16_state_row(row) if row is not None else None

    def _n16_state_is_consumed(
        self,
        connection: sqlite3.Connection,
        state: N16TrendSupportState,
    ) -> bool:
        if state.structure_id is None:
            return False
        seal = _n16_consumption_seal_row(connection, state.structure_id)
        ledger = connection.execute(
            """
            SELECT id, strategy_id, symbol, structure_id, source_signal_id,
                   source_scan_id, source_signal_created_at, evidence_sha256,
                   created_at, claim_state
            FROM strategy_passed_structure_ledger
            WHERE strategy_id = 'N16' AND structure_id = ?
            """,
            (state.structure_id,),
        ).fetchone()
        if ledger is None:
            external = self.n16_claim_ledger.committed_claim_sha256(
                state.structure_id
            )
            if seal is not None or external is not None:
                raise RuntimeError(
                    "N16 permanent claim is detached from its Review ledger"
                )
            return False
        if len(ledger) != 10 or ledger[9] not in {"STAGED", "ACTIVE"}:
            raise RuntimeError("N16 passed ledger is invalid")
        if ledger[2] != state.symbol:
            raise RuntimeError("N16 passed ledger symbol conflicts with state")
        if ledger[9] == "STAGED":
            if seal is not None:
                raise RuntimeError("N16 staged claim has a consumption seal")
            return False
        if seal is None:
            raise RuntimeError("N16 active ledger has no consumption seal")
        _validate_active_structure_ledger(
            connection,
            ledger,
            "N16",
            state.symbol,
            state.structure_id,
        )
        audit = connection.execute(
            "SELECT detail_json FROM strategy_passed_signal_audits "
            "WHERE source_signal_id = ? AND claim_state = 'ACTIVE'",
            (ledger[4],),
        ).fetchone()
        if (
            audit is None
            or len(audit) != 1
            or type(audit[0]) is not str
        ):
            raise RuntimeError("N16 active passed audit detail is invalid")
        _validate_n16_passed_lifecycle(
            connection,
            state.symbol,
            state.structure_id,
            audit[0],
        )
        _validate_n16_consumption_seal(
            connection, state.symbol, state.structure_id
        )
        try:
            self._attest_n16_committed_structure_claim(
                connection,
                state.symbol,
                state.structure_id,
                source_signal_id=ledger[4],
            )
        except Exception as exc:
            raise _N16StateInconsistentError(
                "N16 Review claim and independent ledger conflict"
            ) from exc
        return True

    def _attest_n16_committed_structure_claim(
        self,
        connection: sqlite3.Connection,
        symbol: str,
        structure_id: str,
        *,
        source_signal_id: int | None = None,
    ) -> tuple[Any, ...]:
        _strict_signal_text(symbol, "N16 symbol", 64)
        _strict_signal_text(structure_id, "N16 structure_id", 24)
        if source_signal_id is not None and (
            type(source_signal_id) is not int or source_signal_id <= 0
        ):
            raise RuntimeError("N16 source signal identity is invalid")
        rows = connection.execute(
            "SELECT seal_ordinal FROM n16_consumption_seals "
            "WHERE strategy_id='N16' AND symbol=? AND structure_id=?",
            (symbol, structure_id),
        ).fetchall()
        if (
            len(rows) != 1
            or type(rows[0]) not in (tuple, list)
            or len(rows[0]) != 1
            or type(rows[0][0]) is not int
            or rows[0][0] <= 0
        ):
            raise RuntimeError("N16 consumption seal ordinal is invalid")
        claim = _n16_review_claim_row(connection, rows[0][0])
        if (
            claim is None
            or claim[9] != symbol
            or claim[11] != structure_id
            or (
                source_signal_id is not None
                and claim[1] != source_signal_id
            )
        ):
            raise RuntimeError("N16 Review claim identity conflicts")
        self.n16_claim_ledger.attest_committed_claim(claim)
        return claim

    def get_n16_state(
        self,
        strategy_id: str,
        episode_id: str,
    ) -> N16TrendSupportState | None:
        if strategy_id != "N16" or type(strategy_id) is not str:
            raise ValueError("N16 strategy identity is invalid")
        _strict_signal_text(episode_id, "N16 episode_id", 24)
        with self._connect() as connection:
            state = self._n16_state_row(connection, strategy_id, episode_id)
            if state is not None and self._n16_state_is_consumed(connection, state):
                return replace(state, stage="CONSUMED", reason="PASSED")
            return state

    def get_active_n16_states(
        self,
        strategy_id: str = "N16",
    ) -> list[N16TrendSupportState]:
        if strategy_id != "N16" or type(strategy_id) is not str:
            raise ValueError("N16 strategy identity is invalid")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, strategy_id, symbol, episode_id, structure_id,
                       stage, reason, quote_volume_rank, evidence_json,
                       evidence_sha256, created_at, updated_at
                FROM n16_trend_support_states
                WHERE strategy_id = ? AND stage IN (
                    'TOUCH_LOCKED', 'CONFIRMING', 'CONFIRMED'
                )
                ORDER BY quote_volume_rank, symbol, episode_id
                """,
                (strategy_id,),
            ).fetchall()
            states = [_validated_n16_state_row(row) for row in rows]
            return [
                state
                for state in states
                if not self._n16_state_is_consumed(connection, state)
            ]

    def get_required_n16_episode_symbols(
        self,
        strategy_id: str = "N16",
    ) -> tuple[str, ...]:
        return tuple(
            sorted({state.symbol for state in self.get_active_n16_states(strategy_id)})
        )

    def record_n16_state(self, record: Any) -> str:
        from .n16_analyzer import (
            N16StateRecord,
            decode_n16_state_envelope,
        )

        try:
            if type(record) is not N16StateRecord:
                raise ValueError("N16 state record type is invalid")
            if (
                record.strategy_id != "N16"
                or type(record.strategy_id) is not str
                or type(record.symbol) is not str
                or not record.symbol
                or type(record.episode_id) is not str
                or type(record.structure_id) not in {str, type(None)}
                or type(record.stage) is not str
                or record.stage == "CONSUMED"
                or type(record.reason) is not str
                or type(record.quote_volume_rank) is not int
                or type(record.evidence) is not dict
            ):
                raise ValueError("N16 state record identity is invalid")
            decoded = decode_n16_state_envelope(
                record.evidence, "N16", record.symbol
            )
            if (
                decoded.episode_id != record.episode_id
                or decoded.structure_id != record.structure_id
                or decoded.stage != record.stage
                or decoded.reason != record.reason
                or decoded.quote_volume_rank != record.quote_volume_rank
            ):
                raise ValueError("N16 state record conflicts with evidence")
            evidence_json = record.evidence_json
            evidence_sha256 = record.evidence_sha256
        except (ArithmeticError, TypeError, ValueError):
            return "N16_STATE_INCONSISTENT"

        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = self._n16_state_row(
                    connection, record.strategy_id, record.episode_id
                )
                now = utc_now()
                if existing is None:
                    if record.structure_id is not None:
                        permanent = connection.execute(
                            """
                            SELECT
                              (SELECT COUNT(*) FROM n16_consumption_seals
                               WHERE structure_id = ?),
                              (SELECT COUNT(*)
                               FROM strategy_passed_structure_ledger
                               WHERE strategy_id = 'N16' AND structure_id = ?)
                            """,
                            (record.structure_id, record.structure_id),
                        ).fetchone()
                        external_claim = (
                            self.n16_claim_ledger.committed_claim_sha256(
                                record.structure_id
                            )
                        )
                        if permanent != (0, 0) or external_claim is not None:
                            raise _N16StateInconsistentError(
                                "N16 permanent claim has no lifecycle state"
                            )
                    cursor = connection.execute(
                        """
                        INSERT INTO n16_trend_support_states (
                            strategy_id, symbol, episode_id, structure_id,
                            stage, reason, quote_volume_rank, evidence_json,
                            evidence_sha256, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record.strategy_id,
                            record.symbol,
                            record.episode_id,
                            record.structure_id,
                            record.stage,
                            record.reason,
                            record.quote_volume_rank,
                            evidence_json,
                            evidence_sha256,
                            now,
                            now,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise RuntimeError("N16 state insert did not write one row")
                    return "INSERTED"
                if self._n16_state_is_consumed(connection, existing):
                    return "N16_EPISODE_CONSUMED"
                if (
                    existing.symbol == record.symbol
                    and existing.structure_id == record.structure_id
                    and existing.stage == record.stage
                    and existing.reason == record.reason
                    and existing.quote_volume_rank == record.quote_volume_rank
                    and existing.evidence_json == evidence_json
                    and existing.evidence_sha256 == evidence_sha256
                ):
                    return "UNCHANGED"
                old_evidence = json.loads(existing.evidence_json)
                if _n16_terminal_consumes_active_replay(
                    old_evidence,
                    record.evidence,
                ):
                    return "N16_EPISODE_CONSUMED"
                if _n16_terminal_replay_matches(
                    old_evidence,
                    record.evidence,
                ):
                    return "UNCHANGED"
                if (
                    existing.symbol != record.symbol
                    or existing.quote_volume_rank != record.quote_volume_rank
                    or not _n16_state_progresses(old_evidence, record.evidence)
                ):
                    raise _N16StateInconsistentError(
                        "N16 lifecycle evidence conflicts"
                    )
                update = connection.execute(
                    """
                    UPDATE n16_trend_support_states
                    SET structure_id = ?, stage = ?, reason = ?,
                        evidence_json = ?, evidence_sha256 = ?, updated_at = ?
                    WHERE id = ? AND evidence_sha256 = ?
                    """,
                    (
                        record.structure_id,
                        record.stage,
                        record.reason,
                        evidence_json,
                        evidence_sha256,
                        now,
                        existing.id,
                        existing.evidence_sha256,
                    ),
                )
                if update.rowcount != 1:
                    raise RuntimeError("N16 state update identity mismatch")
                return "UPDATED"
        except _N16StateInconsistentError:
            return "N16_STATE_INCONSISTENT"
        except Exception as exc:
            self.logger.warning("N16 state persistence failed: %s", exc)
            return "N16_STATE_PERSIST_FAILED"

    @staticmethod
    def _n17_state_from_row(row: Any) -> N17RangeSupportState:
        if type(row) not in (tuple, list) or len(row) != 15:
            raise RuntimeError("N17 lifecycle row shape is invalid")
        return N17RangeSupportState(*row)

    def get_active_n17_states(
        self, strategy_id: str = "N17"
    ) -> list[N17RangeSupportState]:
        if strategy_id != "N17" or type(strategy_id) is not str:
            raise ValueError("N17 strategy identity is invalid")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id,strategy_id,symbol,family_id,structure_id,stage,reason,
                       quote_volume_rank,box_start_time_ms,box_end_time_ms,
                       reset_after_time_ms,evidence_json,evidence_sha256,
                       created_at,updated_at
                FROM n17_range_support_states
                WHERE strategy_id='N17'
                  AND stage IN ('TOUCH_LOCKED','CONFIRMING','CONFIRMED')
                  AND NOT EXISTS (
                    SELECT 1 FROM strategy_passed_structure_ledger AS consumed
                    WHERE consumed.strategy_id='N17'
                      AND consumed.structure_id=
                          n17_range_support_states.structure_id
                      AND consumed.claim_state='ACTIVE'
                  )
                ORDER BY quote_volume_rank,symbol,family_id
                """
            ).fetchall()
            return [self._n17_state_from_row(row) for row in rows]

    def get_required_n17_family_symbols(self) -> tuple[str, ...]:
        return tuple(sorted({state.symbol for state in self.get_active_n17_states()}))

    def get_latest_n17_states(
        self, symbols: list[str] | tuple[str, ...] | set[str]
    ) -> dict[str, N17RangeSupportState]:
        """Return at most one latest durable family per requested symbol.

        The caller supplies the bounded shared-market symbol set.  The
        certified composite index makes each lookup independent of permanent
        lifecycle history size.
        """

        if type(symbols) not in {list, tuple, set} or len(symbols) > 200:
            raise ValueError("N17 latest-state symbol set is invalid")
        ordered = sorted(set(symbols))
        if any(type(symbol) is not str or not symbol for symbol in ordered):
            raise ValueError("N17 latest-state symbol is invalid")
        result: dict[str, N17RangeSupportState] = {}
        with self._read_only_runtime_snapshot() as connection:
            for symbol in ordered:
                rows = connection.execute(
                    """
                    SELECT id,strategy_id,symbol,family_id,structure_id,stage,reason,
                           quote_volume_rank,box_start_time_ms,box_end_time_ms,
                           reset_after_time_ms,evidence_json,evidence_sha256,
                           created_at,updated_at
                    FROM n17_range_support_states
                         INDEXED BY idx_n17_range_support_symbol_latest
                    WHERE strategy_id='N17' AND symbol=?
                      AND NOT EXISTS (
                        SELECT 1
                        FROM strategy_passed_structure_ledger AS consumed
                        WHERE consumed.strategy_id='N17'
                          AND consumed.structure_id=
                              n17_range_support_states.structure_id
                          AND consumed.claim_state='ACTIVE'
                      )
                    ORDER BY box_end_time_ms DESC,id DESC LIMIT 1
                    """,
                    (symbol,),
                ).fetchall()
                if rows:
                    result[symbol] = self._n17_state_from_row(rows[0])
        return result

    def record_n17_state(self, record: Any) -> str:
        from .n17_analyzer import (
            N17StateRecord,
            _canonical_json,
            _sha256_json,
            decode_n17_state_evidence,
        )

        try:
            if type(record) is not N17StateRecord:
                raise ValueError("N17 state record type is invalid")
            if (
                record.strategy_id != "N17"
                or type(record.symbol) is not str
                or type(record.family_id) is not str
                or len(record.family_id) != 24
                or type(record.structure_id) is not str
                or len(record.structure_id) != 24
                or record.stage
                not in {
                    "TOUCH_LOCKED",
                    "CONFIRMING",
                    "CONFIRMED",
                    "CONSUMED",
                    "MISSED",
                    "INVALID",
                    "EXPIRED",
                }
                or type(record.quote_volume_rank) is not int
                or not 1 <= record.quote_volume_rank <= 100
                or type(record.evidence) is not dict
                or record.evidence.get("strategy_id") != "N17"
                or record.evidence.get("symbol") != record.symbol
                or record.evidence.get("family_id") != record.family_id
                or record.evidence.get("structure_id") != record.structure_id
                or record.evidence.get("stage") != record.stage
                or record.evidence.get("reason") != record.reason
                or record.evidence.get("reset_after_time_ms")
                != record.reset_after_time_ms
            ):
                raise ValueError("N17 state identity conflicts with evidence")
            unsigned = dict(record.evidence)
            claimed_hash = unsigned.pop("canonical_sha256", None)
            if (
                type(claimed_hash) is not str
                or claimed_hash != _sha256_json(unsigned)
                or record.evidence_sha256 != _sha256_json(record.evidence)
                or len(_canonical_json(record.evidence).encode("utf-8")) >= 131072
            ):
                raise ValueError("N17 state evidence digest is invalid")
            decoded = decode_n17_state_evidence(
                record.evidence_json,
                expected_symbol=record.symbol,
            )
            if (
                decoded.family_id != record.family_id
                or decoded.structure_id != record.structure_id
                or decoded.stage != record.stage
                or decoded.reason != record.reason
                or decoded.quote_volume_rank != record.quote_volume_rank
                or decoded.box_start_time_ms != record.box_start_time_ms
                or decoded.box_end_time_ms != record.box_end_time_ms
                or decoded.reset_after_time_ms != record.reset_after_time_ms
                or decoded.evidence_sha256 != record.evidence_sha256
            ):
                raise ValueError("N17 decoded state identity conflicts with record")
        except Exception:
            return "N17_STATE_INCONSISTENT"
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing_row = connection.execute(
                    """
                    SELECT id,strategy_id,symbol,family_id,structure_id,stage,reason,
                           quote_volume_rank,box_start_time_ms,box_end_time_ms,
                           reset_after_time_ms,evidence_json,evidence_sha256,
                           created_at,updated_at
                    FROM n17_range_support_states
                    WHERE strategy_id='N17' AND family_id=?
                    """,
                    (record.family_id,),
                ).fetchone()
                consumed = connection.execute(
                    "SELECT 1 FROM strategy_passed_structure_ledger "
                    "WHERE strategy_id='N17' AND structure_id=? "
                    "AND claim_state='ACTIVE' LIMIT 1",
                    (record.structure_id,),
                ).fetchone()
                if consumed is not None:
                    return "N17_STRUCTURE_CONSUMED"
                now = utc_now()
                if existing_row is None:
                    if record.reset_after_time_ms is not None:
                        return "N17_STATE_INCONSISTENT"
                    connection.execute(
                        """
                        INSERT INTO n17_range_support_states (
                            strategy_id,symbol,family_id,structure_id,stage,reason,
                            quote_volume_rank,box_start_time_ms,box_end_time_ms,
                            reset_after_time_ms,evidence_json,evidence_sha256,
                            created_at,updated_at
                        ) VALUES ('N17',?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            record.symbol,
                            record.family_id,
                            record.structure_id,
                            record.stage,
                            record.reason,
                            record.quote_volume_rank,
                            record.box_start_time_ms,
                            record.box_end_time_ms,
                            record.reset_after_time_ms,
                            record.evidence_json,
                            record.evidence_sha256,
                            now,
                            now,
                        ),
                    )
                    return "INSERTED"
                existing = self._n17_state_from_row(existing_row)
                if (
                    existing.symbol != record.symbol
                    or existing.structure_id != record.structure_id
                    or existing.box_start_time_ms != record.box_start_time_ms
                    or existing.box_end_time_ms != record.box_end_time_ms
                    or existing.quote_volume_rank != record.quote_volume_rank
                ):
                    return "N17_STATE_INCONSISTENT"
                if existing.evidence_sha256 == record.evidence_sha256:
                    return "UNCHANGED"
                if existing.stage in {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}:
                    if (
                        existing.stage == record.stage
                        and existing.reason == record.reason
                        and existing.reset_after_time_ms is None
                        and type(record.reset_after_time_ms) is int
                        and record.reset_after_time_ms > existing.box_end_time_ms
                    ):
                        changed = connection.execute(
                            "UPDATE n17_range_support_states SET "
                            "reset_after_time_ms=?,evidence_json=?,evidence_sha256=?,"
                            "updated_at=? WHERE id=? AND reset_after_time_ms IS NULL "
                            "AND evidence_sha256=?",
                            (
                                record.reset_after_time_ms,
                                record.evidence_json,
                                record.evidence_sha256,
                                now,
                                existing.id,
                                existing.evidence_sha256,
                            ),
                        )
                        if changed.rowcount != 1:
                            raise RuntimeError("N17 reset evidence update conflicted")
                        return "UPDATED"
                    return "N17_STRUCTURE_CONSUMED"
                progression = {
                    "TOUCH_LOCKED": {"CONFIRMING", "CONFIRMED", "CONSUMED", "MISSED", "INVALID", "EXPIRED"},
                    "CONFIRMING": {"CONFIRMED", "CONSUMED", "MISSED", "INVALID", "EXPIRED"},
                    "CONFIRMED": {"CONSUMED", "MISSED", "INVALID", "EXPIRED"},
                }
                if record.stage not in progression.get(existing.stage, set()):
                    return "N17_STATE_INCONSISTENT"
                changed = connection.execute(
                    """
                    UPDATE n17_range_support_states
                    SET stage=?,reason=?,reset_after_time_ms=?,evidence_json=?,
                        evidence_sha256=?,updated_at=?
                    WHERE id=? AND evidence_sha256=?
                    """,
                    (
                        record.stage,
                        record.reason,
                        record.reset_after_time_ms,
                        record.evidence_json,
                        record.evidence_sha256,
                        now,
                        existing.id,
                        existing.evidence_sha256,
                    ),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("N17 lifecycle update conflicted")
                return "UPDATED"
        except Exception as exc:
            self.logger.warning("N17 state persistence failed: %s", exc)
            return "N17_STATE_PERSIST_FAILED"

    @staticmethod
    def _history_coverage_contract(
        strategy_id: str,
    ) -> tuple[str, str, str, tuple[str, ...]]:
        contracts = {
            "N17": (
                "n17_history_coverage",
                "n17_range_support_states",
                "box_start_time_ms",
                ("TOUCH_LOCKED", "CONFIRMING", "CONFIRMED"),
            ),
            "N18": (
                "n18_history_coverage",
                "n18_triangle_states",
                "l1_open_time_ms",
                (
                    "TRIANGLE_ARMED",
                    "ABSORPTION_LOCKED",
                    "BREAKOUT_PENDING",
                    "CONFIRMED",
                ),
            ),
            "N19": (
                "n19_history_coverage",
                "n19_staircase_states",
                "s_open_time_ms",
                ("EXHAUSTION_LOCKED", "CONFIRMATION_PENDING", "CONFIRMED"),
            ),
        }
        if type(strategy_id) is not str or strategy_id not in contracts:
            raise ValueError("history coverage strategy identity is invalid")
        return contracts[strategy_id]

    @staticmethod
    def _history_coverage_active_suffix(
        strategy_id: str,
        state_table: str,
    ) -> str:
        if strategy_id != "N17":
            return ""
        return (
            " AND NOT EXISTS ("
            "SELECT 1 FROM strategy_passed_structure_ledger AS consumed "
            "WHERE consumed.strategy_id='N17' "
            f"AND consumed.structure_id={state_table}.structure_id "
            "AND consumed.claim_state='ACTIVE')"
        )

    @staticmethod
    def _validate_history_coverage_proposal(
        proposal: HistoryCoverageProposal,
    ) -> None:
        if (
            type(proposal) is not HistoryCoverageProposal
            or proposal.strategy_id not in {"N17", "N18", "N19"}
            or type(proposal.symbol) is not str
            or not proposal.symbol
            or len(proposal.symbol) > 32
            or type(proposal.source_start_time_ms) is not int
            or proposal.source_start_time_ms <= 0
            or type(proposal.covered_through_time_ms) is not int
            or proposal.covered_through_time_ms
            != proposal.source_start_time_ms + (120 * 900_000)
            or type(proposal.current_open_time_ms) is not int
            or proposal.current_open_time_ms
            != proposal.covered_through_time_ms + 900_000
            or type(proposal.source_sha256) is not str
            or len(proposal.source_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in proposal.source_sha256
            )
        ):
            raise ValueError("history coverage proposal is invalid")
        if (
            any(
                item is not None
                for item in (
                    proposal.n19_terminal_family_id,
                    proposal.n19_terminal_structure_id,
                    proposal.n19_terminal_evidence_sha256,
                    proposal.n19_terminal_state_record,
                )
            )
            and proposal.strategy_id != "N19"
        ):
            raise ValueError(
                "history coverage terminal transition strategy is invalid"
            )
        terminal_items = (
            proposal.n19_terminal_family_id,
            proposal.n19_terminal_structure_id,
            proposal.n19_terminal_evidence_sha256,
            proposal.n19_terminal_state_record,
        )
        if any(item is None for item in terminal_items) != all(
            item is None for item in terminal_items
        ):
            raise ValueError(
                "history coverage terminal transition identity is incomplete"
            )
        if proposal.n19_terminal_state_record is not None and (
            type(proposal.n19_terminal_family_id) is not str
            or len(proposal.n19_terminal_family_id) != 24
            or type(proposal.n19_terminal_structure_id) is not str
            or len(proposal.n19_terminal_structure_id) != 24
            or type(proposal.n19_terminal_evidence_sha256) is not str
            or len(proposal.n19_terminal_evidence_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in proposal.n19_terminal_evidence_sha256
            )
        ):
            raise ValueError(
                "history coverage terminal transition identity is invalid"
            )

    def _attest_n19_coverage_terminal_transition(
        self,
        connection: sqlite3.Connection,
        proposal: HistoryCoverageProposal,
    ) -> tuple[str, str, str] | None:
        record = proposal.n19_terminal_state_record
        if record is None:
            return None
        from .n19_analyzer import (
            N19StateRecord,
            _historical_missed_record_from_confirmed,
            decode_n19_state_evidence,
        )

        if (
            proposal.strategy_id != "N19"
            or type(record) is not N19StateRecord
            or record.strategy_id != "N19"
            or record.symbol != proposal.symbol
            or record.family_id != proposal.n19_terminal_family_id
            or record.structure_id != proposal.n19_terminal_structure_id
            or record.evidence_sha256
            != proposal.n19_terminal_evidence_sha256
            or record.stage != "MISSED"
            or record.reason != "N19_HISTORICAL_ENTRY_MISSED"
            or record.reset_after_time_ms is not None
        ):
            raise RuntimeError(
                "N19 coverage terminal transition identity is invalid"
            )
        try:
            decoded_record = decode_n19_state_evidence(
                record.evidence_json,
                expected_symbol=proposal.symbol,
            )
        except Exception as exc:
            raise RuntimeError(
                "N19 coverage terminal transition evidence is invalid"
            ) from exc
        if decoded_record != record:
            raise RuntimeError(
                "N19 coverage terminal transition is not canonical"
            )
        structure = decoded_record.evidence.get("structure")
        c = structure.get("c") if type(structure) is dict else None
        c_open_time_ms = c.get("open_time_ms") if type(c) is dict else None
        if (
            type(c_open_time_ms) is not int
            or c_open_time_ms <= 0
            or c_open_time_ms % 900_000
            or proposal.covered_through_time_ms <= c_open_time_ms
            or (
                proposal.covered_through_time_ms - c_open_time_ms
            ) % 900_000
        ):
            raise RuntimeError(
                "N19 coverage terminal transition is not historical"
            )
        existing_row = connection.execute(
            """
            SELECT id,strategy_id,symbol,family_id,structure_id,stage,reason,
                   quote_volume_rank,s_open_time_ms,x_open_time_ms,
                   reset_after_time_ms,evidence_json,evidence_sha256,
                   created_at,updated_at
            FROM n19_staircase_states
            WHERE strategy_id='N19' AND family_id=?
            """,
            (record.family_id,),
        ).fetchone()
        if existing_row is None:
            raise RuntimeError(
                "N19 coverage terminal transition source is missing"
            )
        existing = self._n19_state_from_row(existing_row)
        try:
            decoded_existing = decode_n19_state_evidence(
                existing.evidence_json,
                expected_symbol=proposal.symbol,
            )
        except Exception as exc:
            raise RuntimeError(
                "N19 coverage terminal transition source is invalid"
            ) from exc
        if (
            existing.strategy_id != "N19"
            or existing.symbol != proposal.symbol
            or existing.family_id != record.family_id
            or type(existing.quote_volume_rank) is not int
            or type(existing.s_open_time_ms) is not int
            or type(existing.x_open_time_ms) is not int
            or type(existing.evidence_sha256) is not str
            or decoded_existing.family_id != existing.family_id
            or decoded_existing.structure_id != existing.structure_id
            or decoded_existing.stage != existing.stage
            or decoded_existing.reason != existing.reason
            or decoded_existing.quote_volume_rank
            != existing.quote_volume_rank
            or decoded_existing.s_open_time_ms != existing.s_open_time_ms
            or decoded_existing.x_open_time_ms != existing.x_open_time_ms
            or decoded_existing.reset_after_time_ms
            != existing.reset_after_time_ms
            or decoded_existing.evidence_sha256 != existing.evidence_sha256
        ):
            raise RuntimeError(
                "N19 coverage terminal transition source conflicts"
            )
        if existing.evidence_sha256 == record.evidence_sha256:
            if decoded_existing != record:
                raise RuntimeError(
                    "N19 coverage terminal transition replay conflicts"
                )
            return ("APPLIED", record.family_id, record.evidence_sha256)
        if existing.stage != "CONFIRMED":
            raise RuntimeError(
                "N19 coverage terminal transition source is not confirmed"
            )
        try:
            expected = _historical_missed_record_from_confirmed(
                decoded_existing,
                record.evidence.get("source"),
            )
        except Exception as exc:
            raise RuntimeError(
                "N19 coverage terminal transition cannot be derived"
            ) from exc
        if expected != record:
            raise RuntimeError(
                "N19 coverage terminal transition derivation conflicts"
            )
        return ("PENDING", record.family_id, existing.evidence_sha256)

    @staticmethod
    def _n19_terminal_publication_identity_exists(
        connection: sqlite3.Connection,
        proposal: HistoryCoverageProposal,
    ) -> bool:
        record = proposal.n19_terminal_state_record
        if record is None:
            return False
        rows = connection.execute(
            "SELECT source_scan_id FROM "
            "history_coverage_n19_terminal_receipts "
            "INDEXED BY idx_history_coverage_n19_terminal_identity "
            "WHERE strategy_id='N19' AND symbol=? AND family_id=? "
            "LIMIT 2",
            (
                proposal.symbol,
                record.family_id,
            ),
        ).fetchall()
        if len(rows) > 1:
            raise RuntimeError(
                "N19 terminal publication identity is duplicated"
            )
        return bool(rows)

    def _history_coverage_gap_is_blocked(
        self,
        connection: sqlite3.Connection,
        proposal: HistoryCoverageProposal,
        state_table: str,
        anchor_column: str,
        active_stages: tuple[str, ...],
        terminal_transition: tuple[str, str, str] | None,
    ) -> bool:
        placeholders = ",".join("?" for _ in active_stages)
        active_suffix = self._history_coverage_active_suffix(
            proposal.strategy_id,
            state_table,
        )
        if proposal.strategy_id == "N19":
            active_rows = connection.execute(
                f"SELECT family_id FROM {state_table} "
                f"WHERE strategy_id=? AND symbol=? "
                f"AND stage IN ({placeholders}) "
                f"AND {anchor_column} < ?{active_suffix}",
                (
                    proposal.strategy_id,
                    proposal.symbol,
                    *active_stages,
                    proposal.source_start_time_ms,
                ),
            ).fetchall()
            if not active_rows:
                return False
            return not (
                terminal_transition is not None
                and terminal_transition[0] == "PENDING"
                and len(active_rows) == 1
                and active_rows[0][0] == terminal_transition[1]
            )
        return (
            connection.execute(
                f"SELECT 1 FROM {state_table} "
                f"WHERE strategy_id=? AND symbol=? "
                f"AND stage IN ({placeholders}) "
                f"AND {anchor_column} < ?{active_suffix} LIMIT 1",
                (
                    proposal.strategy_id,
                    proposal.symbol,
                    *active_stages,
                    proposal.source_start_time_ms,
                ),
            ).fetchone()
            is not None
        )

    def _attest_n19_terminal_transition_signal(
        self,
        connection: sqlite3.Connection,
        scan_id: int,
        proposal: HistoryCoverageProposal,
        *,
        ordinary_signal_required: bool,
    ) -> None:
        rows = connection.execute(
            """
            SELECT passed,decision,reason,structure_id,detail_json
            FROM strategy_signals
            WHERE scan_id=? AND strategy_id='N19' AND symbol=?
            ORDER BY id LIMIT 2
            """,
            (scan_id, proposal.symbol),
        ).fetchall()
        if not rows and not ordinary_signal_required:
            # A dropped frozen member has no ordinary signal by contract.  Its
            # canonical terminal record is still bound by the permanent N19
            # terminal receipt, coverage receipt, family seal and the current
            # batch manifest in the same publication transaction.
            return
        if len(rows) != 1 or not ordinary_signal_required:
            raise RuntimeError(
                "N19 coverage terminal transition signal is not unique"
            )
        row = tuple(rows[0])
        if (
            len(row) != 5
            or type(row[0]) is not int
            or row[0] not in (0, 1)
            or type(row[1]) is not str
            or type(row[2]) is not str
            or (row[3] is not None and type(row[3]) is not str)
            or type(row[4]) is not str
        ):
            raise RuntimeError(
                "N19 coverage terminal transition signal is invalid"
            )
        try:
            detail = _load_strict_strategy_signal_detail_json(row[4])
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "N19 coverage terminal transition signal detail is invalid"
            ) from exc
        record = proposal.n19_terminal_state_record
        if record is None:
            if (
                row[2] == "N19_HISTORICAL_ENTRY_MISSED"
                or detail.get("reason") == "N19_HISTORICAL_ENTRY_MISSED"
            ):
                raise RuntimeError(
                    "N19 historical signal is missing its terminal transition"
                )
            return
        if (
            row[0] != 0
            or row[1] != "REJECTED"
            or row[2] != "N19_HISTORICAL_ENTRY_MISSED"
            or row[3] != record.structure_id
            or type(detail.get("schema_version")) is not int
            or detail["schema_version"] != 1
            or detail.get("rule_version") != "N19_V1"
            or detail.get("reason") != row[2]
            or detail.get("structure_id") != record.structure_id
            or detail.get("state_stage") != "MISSED"
            or detail.get("evidence_sha256") != record.evidence_sha256
            or type(detail.get("quote_volume_rank")) is not int
            or detail["quote_volume_rank"] != record.quote_volume_rank
            or detail.get("current_open_time")
            != str(proposal.current_open_time_ms)
        ):
            raise RuntimeError(
                "N19 coverage terminal transition signal conflicts"
            )

    def _apply_n19_coverage_terminal_transition(
        self,
        connection: sqlite3.Connection,
        proposal: HistoryCoverageProposal,
        now: str,
    ) -> tuple[str, str, str] | None:
        transition = self._attest_n19_coverage_terminal_transition(
            connection,
            proposal,
        )
        if transition is None or transition[0] == "APPLIED":
            return transition
        record = proposal.n19_terminal_state_record
        changed = connection.execute(
            """
            UPDATE n19_staircase_states
            SET stage=?,reason=?,reset_after_time_ms=?,
                evidence_json=?,evidence_sha256=?,updated_at=?
            WHERE strategy_id='N19' AND family_id=? AND stage='CONFIRMED'
              AND evidence_sha256=?
            """,
            (
                record.stage,
                record.reason,
                record.reset_after_time_ms,
                record.evidence_json,
                record.evidence_sha256,
                now,
                record.family_id,
                transition[2],
            ),
        )
        if changed.rowcount != 1:
            raise RuntimeError(
                "N19 coverage terminal transition conflicted"
            )
        return ("APPLIED", transition[1], record.evidence_sha256)

    def prepare_history_coverage_proposal(
        self,
        proposal: HistoryCoverageProposal,
    ) -> str:
        """Classify one transition without writing.

        A non-overlapping fixed 122-bar window may start a new bounded epoch
        only when no active family can span the unobserved interval.  The
        publication transaction repeats this proof before it writes either
        coverage or the immutable gap witness.
        """

        self._validate_history_coverage_proposal(proposal)
        (
            table,
            state_table,
            anchor_column,
            active_stages,
        ) = self._history_coverage_contract(proposal.strategy_id)
        with self._read_only_runtime_snapshot() as connection:
            if self._n19_terminal_publication_identity_exists(
                connection, proposal
            ):
                return "INCONSISTENT"
            terminal_transition = (
                self._attest_n19_coverage_terminal_transition(
                    connection,
                    proposal,
                )
            )
            existing = connection.execute(
                "SELECT epoch_start_time_ms,covered_through_time_ms,"
                "source_sha256,epoch_ordinal,chain_head_sha256 "
                "FROM history_coverage_epoch_heads "
                "WHERE strategy_id=? AND symbol=?",
                (proposal.strategy_id, proposal.symbol),
            ).fetchone()
            if existing is None:
                legacy = connection.execute(
                    f"SELECT 1 FROM {table} WHERE strategy_id=? AND symbol=?",
                    (proposal.strategy_id, proposal.symbol),
                ).fetchone()
                if legacy is not None:
                    return "INCONSISTENT"
                return "NEW"
            if (
                type(existing[0]) is not int
                or existing[0] <= 0
                or type(existing[1]) is not int
                or existing[1] < existing[0]
                or type(existing[2]) is not str
                or len(existing[2]) != 64
                or type(existing[3]) is not int
                or existing[3] <= 0
                or type(existing[4]) is not str
                or len(existing[4]) != 64
            ):
                return "INCONSISTENT"
            if (
                proposal.source_start_time_ms < existing[0]
                or proposal.covered_through_time_ms < existing[1]
            ):
                return "INCONSISTENT"
            from .coverage_epoch_schema import attest_coverage_epoch_owner

            try:
                owner = attest_coverage_epoch_owner(
                    connection, proposal.strategy_id, proposal.symbol
                )
            except RuntimeError:
                return "INCONSISTENT"
            if owner is None:
                return "INCONSISTENT"
            _, latest_receipt, chain_tip = owner
            durable_source_start = (
                chain_tip[0]
                if latest_receipt is None
                else latest_receipt[1]
            )
            if (
                proposal.source_start_time_ms == durable_source_start
                and proposal.covered_through_time_ms == existing[1]
                and proposal.source_sha256 == existing[2]
            ):
                return "NO_CHANGE"
            if proposal.source_start_time_ms <= existing[1] + 900_000:
                return "CONTIGUOUS"
            return (
                "GAP_BLOCKED"
                if self._history_coverage_gap_is_blocked(
                    connection,
                    proposal,
                    state_table,
                    anchor_column,
                    active_stages,
                    terminal_transition,
                )
                else "GAP"
            )

    def history_coverage_presence(
        self,
        symbols: set[str] | frozenset[str],
    ) -> dict[str, frozenset[str]]:
        """Return exact durable N17-N19 owners for bounded short histories."""

        if (
            type(symbols) not in {set, frozenset}
            or not symbols
            or len(symbols) > 200
            or any(type(symbol) is not str or not symbol for symbol in symbols)
        ):
            raise ValueError("history coverage presence symbol set is invalid")
        ordered = sorted(symbols)
        placeholders = ",".join("?" for _ in ordered)
        heads: dict[str, set[str]] = {symbol: set() for symbol in ordered}
        mirrors: dict[str, set[str]] = {symbol: set() for symbol in ordered}
        states: dict[str, set[str]] = {symbol: set() for symbol in ordered}
        with self._read_only_runtime_snapshot() as connection:
            rows = connection.execute(
                "SELECT kind,strategy_id,symbol FROM ("
                "SELECT 'HEAD' AS kind,strategy_id,symbol "
                "FROM history_coverage_epoch_heads "
                f"WHERE strategy_id IN ('N17','N18','N19') AND symbol IN ({placeholders}) "
                "UNION ALL SELECT 'MIRROR','N17',symbol FROM n17_history_coverage "
                f"WHERE strategy_id='N17' AND symbol IN ({placeholders}) "
                "UNION ALL SELECT 'MIRROR','N18',symbol FROM n18_history_coverage "
                f"WHERE strategy_id='N18' AND symbol IN ({placeholders}) "
                "UNION ALL SELECT 'MIRROR','N19',symbol FROM n19_history_coverage "
                f"WHERE strategy_id='N19' AND symbol IN ({placeholders}) "
                "UNION ALL SELECT 'STATE','N17',symbol FROM n17_range_support_states "
                f"WHERE strategy_id='N17' AND symbol IN ({placeholders}) GROUP BY symbol "
                "UNION ALL SELECT 'STATE','N18',symbol FROM n18_triangle_states "
                f"WHERE strategy_id='N18' AND symbol IN ({placeholders}) GROUP BY symbol "
                "UNION ALL SELECT 'STATE','N19',symbol FROM n19_staircase_states "
                f"WHERE strategy_id='N19' AND symbol IN ({placeholders}) GROUP BY symbol"
                ") ORDER BY kind,strategy_id,symbol",
                tuple(ordered) * 7,
            ).fetchall()
            destinations = {
                "HEAD": heads,
                "MIRROR": mirrors,
                "STATE": states,
            }
            for kind, strategy_id, symbol in rows:
                if (
                    kind not in destinations
                    or strategy_id not in {"N17", "N18", "N19"}
                    or symbol not in heads
                ):
                    raise RuntimeError(
                        "history coverage presence identity is invalid"
                    )
                destinations[kind][symbol].add(strategy_id)
        if any(
            heads[symbol] != mirrors[symbol]
            or not states[symbol].issubset(heads[symbol])
            for symbol in ordered
        ):
            raise RuntimeError("history coverage presence graph is inconsistent")
        return {
            symbol: frozenset(strategy_ids)
            for symbol, strategy_ids in heads.items()
        }

    def _apply_history_coverage_proposals(
        self,
        connection: sqlite3.Connection,
        scan_id: int,
        expected_count: int,
        batch_manifest_sha256: str,
        proposals: tuple[HistoryCoverageProposal, ...],
    ) -> None:
        if (
            type(proposals) is not tuple
            or len(proposals) > 600
            or type(expected_count) is not int
            or expected_count < 0
            or type(batch_manifest_sha256) is not str
            or len(batch_manifest_sha256) != 64
        ):
            raise RuntimeError("history coverage proposal collection is invalid")
        keys: set[tuple[str, str]] = set()
        now = utc_now()
        publication_has_terminal_binding = any(
            row[1] == "terminal_binding_sha256"
            for row in connection.execute(
                "PRAGMA table_xinfo(history_coverage_publication_receipts)"
            )
        )

        def insert_receipt(
            proposal: HistoryCoverageProposal,
            epoch_ordinal: int,
            epoch_start_time_ms: int,
            chain_head_sha256: str,
            publication_ordinal: int,
            previous_receipt_sha256: str | None,
        ) -> str:
            from .coverage_epoch_schema import (
                coverage_publication_receipt_sha256,
                n19_terminal_publication_binding_sha256,
            )

            terminal_binding_sha256 = None
            terminal_record = proposal.n19_terminal_state_record
            if terminal_record is not None:
                terminal_binding_sha256 = (
                    n19_terminal_publication_binding_sha256(
                        scan_id,
                        proposal.symbol,
                        terminal_record.family_id,
                        terminal_record.structure_id,
                        terminal_record.evidence_json,
                        terminal_record.evidence_sha256,
                        expected_count,
                        batch_manifest_sha256,
                        epoch_ordinal,
                        epoch_start_time_ms,
                        chain_head_sha256,
                    )
                )
            receipt_sha = coverage_publication_receipt_sha256(
                scan_id,
                proposal.strategy_id,
                proposal.symbol,
                proposal.source_start_time_ms,
                proposal.covered_through_time_ms,
                proposal.source_sha256,
                epoch_ordinal,
                epoch_start_time_ms,
                chain_head_sha256,
                publication_ordinal,
                previous_receipt_sha256,
                expected_count,
                batch_manifest_sha256,
                None if terminal_record is not None else terminal_binding_sha256,
            )
            values = (
                scan_id,
                proposal.strategy_id,
                proposal.symbol,
                proposal.source_start_time_ms,
                proposal.covered_through_time_ms,
                proposal.source_sha256,
                epoch_ordinal,
                epoch_start_time_ms,
                chain_head_sha256,
                publication_ordinal,
                previous_receipt_sha256,
                expected_count,
                batch_manifest_sha256,
            )
            if terminal_record is not None:
                insert_n19_terminal_bundle(
                    proposal,
                    receipt_sha,
                    epoch_ordinal,
                    epoch_start_time_ms,
                    chain_head_sha256,
                    publication_ordinal,
                    previous_receipt_sha256,
                )
                return receipt_sha
            if publication_has_terminal_binding:
                inserted = connection.execute(
                    "INSERT INTO history_coverage_publication_receipts VALUES "
                    "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    values + (None, receipt_sha, now),
                )
            else:
                inserted = connection.execute(
                    "INSERT INTO history_coverage_publication_receipts VALUES "
                    "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    values + (receipt_sha, now),
                )
            if inserted.rowcount != 1:
                raise RuntimeError("coverage publication receipt was not sealed")
            return receipt_sha

        def insert_n19_terminal_bundle(
            proposal: HistoryCoverageProposal,
            coverage_receipt_sha256: str,
            epoch_ordinal: int,
            epoch_start_time_ms: int,
            chain_head_sha256: str,
            publication_ordinal: int,
            previous_receipt_sha256: str | None,
        ) -> None:
            record = proposal.n19_terminal_state_record
            if record is None:
                return
            transition = self._attest_n19_coverage_terminal_transition(
                connection,
                proposal,
            )
            if transition is None or transition[0] != "PENDING":
                raise RuntimeError(
                    "N19 terminal bundle source transition is unavailable"
                )
            from .coverage_epoch_schema import (
                n19_terminal_publication_binding_sha256,
                n19_terminal_publication_receipt_sha256,
            )
            from .coverage_family_seal import (
                PROVED_TERMINAL_DOMAIN,
                family_seal_sha256,
                terminal_bundle_sha256,
            )

            receipt_sha256 = n19_terminal_publication_receipt_sha256(
                scan_id,
                proposal.symbol,
                record.family_id,
                record.structure_id,
                record.evidence_json,
                record.evidence_sha256,
                expected_count,
                batch_manifest_sha256,
                coverage_receipt_sha256,
                epoch_ordinal,
                epoch_start_time_ms,
                chain_head_sha256,
            )
            terminal_binding_sha256 = n19_terminal_publication_binding_sha256(
                scan_id,
                proposal.symbol,
                record.family_id,
                record.structure_id,
                record.evidence_json,
                record.evidence_sha256,
                expected_count,
                batch_manifest_sha256,
                epoch_ordinal,
                epoch_start_time_ms,
                chain_head_sha256,
            )
            seal_sha256 = family_seal_sha256(
                proposal.symbol,
                record.family_id,
                record.structure_id,
                record.evidence_sha256,
                PROVED_TERMINAL_DOMAIN,
                receipt_sha256,
            )
            bundle_sha256 = terminal_bundle_sha256(
                scan_id,
                proposal.symbol,
                record.family_id,
                record.structure_id,
                transition[2],
                record.evidence_json,
                record.evidence_sha256,
                expected_count,
                batch_manifest_sha256,
                proposal.source_start_time_ms,
                proposal.covered_through_time_ms,
                proposal.source_sha256,
                publication_ordinal,
                previous_receipt_sha256,
                coverage_receipt_sha256,
                epoch_ordinal,
                epoch_start_time_ms,
                chain_head_sha256,
                receipt_sha256,
                terminal_binding_sha256,
                seal_sha256,
            )
            inserted = connection.execute(
                "INSERT INTO history_coverage_n19_terminal_bundles VALUES "
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    scan_id,
                    "N19",
                    proposal.symbol,
                    record.family_id,
                    record.structure_id,
                    transition[2],
                    record.evidence_json,
                    record.evidence_sha256,
                    expected_count,
                    batch_manifest_sha256,
                    proposal.source_start_time_ms,
                    proposal.covered_through_time_ms,
                    proposal.source_sha256,
                    publication_ordinal,
                    previous_receipt_sha256,
                    coverage_receipt_sha256,
                    epoch_ordinal,
                    epoch_start_time_ms,
                    chain_head_sha256,
                    receipt_sha256,
                    terminal_binding_sha256,
                    seal_sha256,
                    bundle_sha256,
                    now,
                ),
            )
            if inserted.rowcount != 1:
                raise RuntimeError(
                    "N19 terminal publication bundle was not sealed"
                )

        for proposal in proposals:
            self._validate_history_coverage_proposal(proposal)
            key = (proposal.strategy_id, proposal.symbol)
            if key in keys:
                raise RuntimeError("history coverage proposal is duplicated")
            keys.add(key)
            (
                table,
                state_table,
                anchor_column,
                active_stages,
            ) = self._history_coverage_contract(proposal.strategy_id)
            signal = connection.execute(
                "SELECT 1 FROM strategy_signals "
                "WHERE scan_id=? AND strategy_id=? AND symbol=? LIMIT 1",
                (scan_id, proposal.strategy_id, proposal.symbol),
            ).fetchone()
            if signal is None:
                top100_symbols = self._scan_exact_quote_volume_top_symbols(
                    connection, scan_id
                )
                state_owner = connection.execute(
                    f"SELECT 1 FROM {state_table} "
                    "WHERE strategy_id=? AND symbol=? LIMIT 1",
                    (proposal.strategy_id, proposal.symbol),
                ).fetchone()
                if (
                    top100_symbols is None
                    or proposal.symbol in top100_symbols
                    or state_owner is None
                ):
                    raise RuntimeError(
                        "history coverage proposal has no staging or "
                        "offboard lifecycle owner"
                    )
            terminal_transition = (
                self._attest_n19_coverage_terminal_transition(
                    connection, proposal
                )
            )
            existing_row = connection.execute(
                "SELECT epoch_start_time_ms,covered_through_time_ms,"
                "source_sha256,epoch_ordinal,chain_head_sha256,"
                "publication_count,latest_receipt_sha256 "
                "FROM history_coverage_epoch_heads "
                "WHERE strategy_id=? AND symbol=?",
                key,
            ).fetchone()
            if existing_row is None:
                legacy = connection.execute(
                    f"SELECT 1 FROM {table} WHERE strategy_id=? AND symbol=?",
                    key,
                ).fetchone()
                if legacy is not None:
                    raise RuntimeError(
                        "legacy coverage has no epoch head"
                    )
                from .coverage_epoch_schema import (
                    coverage_epoch_chain_sha256,
                )

                chain_sha = coverage_epoch_chain_sha256(
                    proposal.strategy_id,
                    proposal.symbol,
                    1,
                    proposal.source_start_time_ms,
                    proposal.covered_through_time_ms,
                    proposal.source_sha256,
                    None,
                    None,
                    None,
                    scan_id,
                )
                connection.execute(
                    f"INSERT INTO {table} "
                    "(strategy_id,symbol,source_start_time_ms,"
                    "covered_through_time_ms,source_sha256,updated_at) "
                    "VALUES (?,?,?,?,?,?)",
                    (
                        proposal.strategy_id,
                        proposal.symbol,
                        proposal.source_start_time_ms,
                        proposal.covered_through_time_ms,
                        proposal.source_sha256,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO history_coverage_epoch_chain VALUES "
                    "(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        proposal.strategy_id,
                        proposal.symbol,
                        1,
                        proposal.source_start_time_ms,
                        proposal.covered_through_time_ms,
                        proposal.source_sha256,
                        None,
                        None,
                        None,
                        chain_sha,
                        scan_id,
                        now,
                    ),
                )
                receipt_sha = insert_receipt(
                    proposal,
                    1,
                    proposal.source_start_time_ms,
                    chain_sha,
                    1,
                    None,
                )
                self._apply_n19_coverage_terminal_transition(
                    connection, proposal, now
                )
                connection.execute(
                    "INSERT INTO history_coverage_epoch_heads VALUES "
                    "(?,?,?,?,?,?,?,?,?,?)",
                    (
                        proposal.strategy_id,
                        proposal.symbol,
                        1,
                        proposal.source_start_time_ms,
                        proposal.covered_through_time_ms,
                        proposal.source_sha256,
                        chain_sha,
                        1,
                        receipt_sha,
                        now,
                    ),
                )
                continue
            existing = tuple(existing_row)
            if (
                type(existing[0]) is not int
                or existing[0] <= 0
                or type(existing[1]) is not int
                or existing[1] < existing[0]
                or type(existing[2]) is not str
                or len(existing[2]) != 64
                or type(existing[3]) is not int
                or existing[3] <= 0
                or type(existing[4]) is not str
                or len(existing[4]) != 64
                or type(existing[5]) is not int
                or existing[5] < 0
                or (
                    existing[6] is not None
                    and (
                        type(existing[6]) is not str
                        or len(existing[6]) != 64
                    )
                )
                or (
                    (existing[5] == 0 and existing[6] is not None)
                    or (existing[5] > 0 and existing[6] is None)
                )
                or proposal.source_start_time_ms < existing[0]
                or proposal.covered_through_time_ms < existing[1]
            ):
                raise RuntimeError("history coverage baseline is inconsistent")
            legacy = connection.execute(
                f"SELECT source_start_time_ms,covered_through_time_ms,"
                f"source_sha256 FROM {table} "
                "WHERE strategy_id=? AND symbol=?",
                key,
            ).fetchone()
            if (
                legacy is None
                or type(legacy[0]) is not int
                or legacy[0] > existing[0]
                or tuple(legacy[1:]) != existing[1:3]
            ):
                raise RuntimeError("legacy coverage mirror is inconsistent")
            from .coverage_epoch_schema import attest_coverage_epoch_owner

            owner = attest_coverage_epoch_owner(
                connection,
                proposal.strategy_id,
                proposal.symbol,
            )
            if owner is None:
                raise RuntimeError("history coverage owner is unavailable")
            _, latest_receipt, chain_tip = owner
            durable_source_start = (
                chain_tip[0]
                if latest_receipt is None
                else latest_receipt[1]
            )
            if (
                proposal.source_start_time_ms == durable_source_start
                and proposal.covered_through_time_ms == existing[1]
                and proposal.source_sha256 == existing[2]
            ):
                # A repeated scan of the exact same source window is a
                # publication NO_CHANGE.  Its signal/proposal ownership is
                # still attested, but immutable coverage evidence does not
                # grow.  A terminal transition is different: its batch
                # manifest must remain permanently bound to the state
                # migration, so only publication metadata advances.
                if terminal_transition is not None:
                    next_publication_ordinal = existing[5] + 1
                    receipt_sha = insert_receipt(
                        proposal,
                        existing[3],
                        existing[0],
                        existing[4],
                        next_publication_ordinal,
                        existing[6],
                    )
                    self._apply_n19_coverage_terminal_transition(
                        connection, proposal, now
                    )
                    head_changed = connection.execute(
                        "UPDATE history_coverage_epoch_heads SET "
                        "publication_count=?,latest_receipt_sha256=?,"
                        "updated_at=? WHERE strategy_id=? AND symbol=? "
                        "AND epoch_ordinal=? AND epoch_start_time_ms=? "
                        "AND covered_through_time_ms=? AND source_sha256=? "
                        "AND chain_head_sha256=? AND publication_count=? "
                        "AND latest_receipt_sha256 IS ?",
                        (
                            next_publication_ordinal,
                            receipt_sha,
                            now,
                            proposal.strategy_id,
                            proposal.symbol,
                            existing[3],
                            existing[0],
                            existing[1],
                            existing[2],
                            existing[4],
                            existing[5],
                            existing[6],
                        ),
                    )
                    if head_changed.rowcount != 1:
                        raise RuntimeError(
                            "coverage terminal publication receipt conflicted"
                        )
                continue
            is_gap = proposal.source_start_time_ms > existing[1] + 900_000
            next_publication_ordinal = existing[5] + 1
            if is_gap:
                if self._history_coverage_gap_is_blocked(
                    connection,
                    proposal,
                    state_table,
                    anchor_column,
                    active_stages,
                    terminal_transition,
                ):
                    raise RuntimeError(
                        f"{proposal.strategy_id}_HISTORY_COVERAGE_GAP_BLOCKED"
                    )
                from .coverage_epoch_schema import (
                    coverage_epoch_chain_sha256,
                )
                next_ordinal = existing[3] + 1
                chain_sha = coverage_epoch_chain_sha256(
                    proposal.strategy_id,
                    proposal.symbol,
                    next_ordinal,
                    proposal.source_start_time_ms,
                    proposal.covered_through_time_ms,
                    proposal.source_sha256,
                    existing[1],
                    existing[2],
                    existing[4],
                    scan_id,
                )
                connection.execute(
                    "INSERT INTO history_coverage_epoch_chain VALUES "
                    "(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        proposal.strategy_id,
                        proposal.symbol,
                        next_ordinal,
                        proposal.source_start_time_ms,
                        proposal.covered_through_time_ms,
                        proposal.source_sha256,
                        existing[1],
                        existing[2],
                        existing[4],
                        chain_sha,
                        scan_id,
                        now,
                    ),
                )
                receipt_sha = insert_receipt(
                    proposal,
                    next_ordinal,
                    proposal.source_start_time_ms,
                    chain_sha,
                    next_publication_ordinal,
                    existing[6],
                )
                self._apply_n19_coverage_terminal_transition(
                    connection, proposal, now
                )
                head_changed = connection.execute(
                    "UPDATE history_coverage_epoch_heads SET "
                    "epoch_ordinal=?,epoch_start_time_ms=?,"
                    "covered_through_time_ms=?,source_sha256=?,"
                    "chain_head_sha256=?,publication_count=?,"
                    "latest_receipt_sha256=?,updated_at=? "
                    "WHERE strategy_id=? AND symbol=? "
                    "AND epoch_ordinal=? AND epoch_start_time_ms=? "
                    "AND covered_through_time_ms=? AND source_sha256=? "
                    "AND chain_head_sha256=? AND publication_count=? "
                    "AND latest_receipt_sha256 IS ?",
                    (
                        next_ordinal,
                        proposal.source_start_time_ms,
                        proposal.covered_through_time_ms,
                        proposal.source_sha256,
                        chain_sha,
                        next_publication_ordinal,
                        receipt_sha,
                        now,
                        proposal.strategy_id,
                        proposal.symbol,
                        existing[3],
                        existing[0],
                        existing[1],
                        existing[2],
                        existing[4],
                        existing[5],
                        existing[6],
                    ),
                )
                if head_changed.rowcount != 1:
                    raise RuntimeError("coverage epoch head transition conflicted")
            else:
                receipt_sha = insert_receipt(
                    proposal,
                    existing[3],
                    existing[0],
                    existing[4],
                    next_publication_ordinal,
                    existing[6],
                )
                self._apply_n19_coverage_terminal_transition(
                    connection, proposal, now
                )
                head_changed = connection.execute(
                    "UPDATE history_coverage_epoch_heads SET "
                    "covered_through_time_ms=?,source_sha256=?,"
                    "publication_count=?,latest_receipt_sha256=?,updated_at=? "
                    "WHERE strategy_id=? AND symbol=? "
                    "AND epoch_ordinal=? AND epoch_start_time_ms=? "
                    "AND covered_through_time_ms=? AND source_sha256=? "
                    "AND chain_head_sha256=? AND publication_count=? "
                    "AND latest_receipt_sha256 IS ?",
                    (
                        proposal.covered_through_time_ms,
                        proposal.source_sha256,
                        next_publication_ordinal,
                        receipt_sha,
                        now,
                        proposal.strategy_id,
                        proposal.symbol,
                        existing[3],
                        existing[0],
                        existing[1],
                        existing[2],
                        existing[4],
                        existing[5],
                        existing[6],
                    ),
                )
                if head_changed.rowcount != 1:
                    raise RuntimeError("coverage epoch head update conflicted")
            changed = connection.execute(
                f"UPDATE {table} SET covered_through_time_ms=?,"
                "source_sha256=?,updated_at=? "
                "WHERE strategy_id=? AND symbol=? "
                "AND source_start_time_ms=? AND covered_through_time_ms=? "
                "AND source_sha256=?",
                (
                    proposal.covered_through_time_ms,
                    proposal.source_sha256,
                    now,
                    proposal.strategy_id,
                    proposal.symbol,
                    legacy[0],
                    existing[1],
                    existing[2],
                ),
            )
            if changed.rowcount != 1:
                raise RuntimeError("history coverage transition conflicted")

    @staticmethod
    def _scan_exact_quote_volume_top_symbols(
        connection: sqlite3.Connection,
        scan_id: int,
    ) -> frozenset[str] | None:
        row = connection.execute(
            "SELECT candidates_json FROM scans WHERE id=?",
            (scan_id,),
        ).fetchone()
        if row is None or len(row) != 1 or type(row[0]) is not str:
            raise RuntimeError("scan candidate snapshot is missing")

        def reject_duplicate_keys(
            pairs: list[tuple[str, Any]],
        ) -> dict[str, Any]:
            parsed: dict[str, Any] = {}
            for key, value in pairs:
                if type(key) is not str or key in parsed:
                    raise ValueError("duplicate scan candidate key")
                parsed[key] = value
            return parsed

        try:
            payload = json.loads(
                row[0],
                object_pairs_hook=reject_duplicate_keys,
                parse_constant=lambda value: (_ for _ in ()).throw(
                    ValueError(
                        "non-finite scan candidate value: %s" % value
                    )
                ),
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError("scan candidate snapshot is invalid") from exc
        if type(payload) is not list or len(payload) > 1_000:
            raise RuntimeError("scan candidate snapshot shape is invalid")
        ranked: list[tuple[str, int]] = []
        for item in payload:
            if type(item) is not dict:
                raise RuntimeError("scan candidate row is invalid")
            if item.get("candidate_universe") != "quote_volume_top":
                continue
            symbol = item.get("symbol")
            rank = item.get("quote_volume_rank")
            if (
                type(symbol) is not str
                or not symbol
                or len(symbol) > 32
                or type(rank) is not int
                or not 1 <= rank <= 100
            ):
                return None
            ranked.append((symbol, rank))
        symbols = [item[0] for item in ranked]
        ranks = [item[1] for item in ranked]
        if (
            len(ranked) != 100
            or len(set(symbols)) != 100
            or set(ranks) != set(range(1, 101))
        ):
            return None
        return frozenset(symbols)

    def _attest_history_coverage_publication(
        self,
        connection: sqlite3.Connection,
        scan_id: int,
        expected_count: int,
        batch_manifest_sha256: str,
        proposals: tuple[HistoryCoverageProposal, ...],
        *,
        published: bool,
    ) -> frozenset[tuple[str, str]]:
        if type(proposals) is not tuple or len(proposals) > 600:
            raise RuntimeError("history coverage proposal collection is invalid")
        signal_rows = connection.execute(
            "SELECT strategy_id,symbol,decision,reason FROM strategy_signals "
            "WHERE scan_id=? AND strategy_id IN ('N17','N18','N19') "
            "ORDER BY strategy_id,symbol",
            (scan_id,),
        ).fetchall()
        signal_keys = [(row[0], row[1]) for row in signal_rows]
        if (
            any(
                type(row[0]) is not str
                or type(row[1]) is not str
                or type(row[2]) is not str
                or type(row[3]) is not str
                for row in signal_rows
            )
            or len(set(signal_keys)) != len(signal_keys)
        ):
            raise RuntimeError("history coverage signal owner set is invalid")
        try:
            required_signal_keys = {
                (row[0], row[1])
                for row in signal_rows
                if history_coverage_signal_requires_proposal(
                    row[0], row[2], row[3]
                )
            }
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc
        top100_symbols = self._scan_exact_quote_volume_top_symbols(
            connection, scan_id
        )
        if top100_symbols is not None and any(
            symbol not in top100_symbols for _, symbol in signal_keys
        ):
            raise RuntimeError(
                "history coverage ordinary signal is outside current Top100"
            )
        proposal_keys: list[tuple[str, str]] = []
        for proposal in proposals:
            self._validate_history_coverage_proposal(proposal)
            proposal_keys.append((proposal.strategy_id, proposal.symbol))
        proposal_key_set = set(proposal_keys)
        if len(proposal_key_set) != len(proposal_keys) or not (
            required_signal_keys.issubset(proposal_key_set)
        ):
            raise RuntimeError(
                "history coverage proposal owner set conflicts with signal batch"
            )
        offboard_proposal_keys = proposal_key_set - required_signal_keys
        if offboard_proposal_keys:
            if top100_symbols is None:
                raise RuntimeError(
                    "offboard history coverage lacks an exact Top100 snapshot"
                )
            for strategy_id, symbol in offboard_proposal_keys:
                if symbol in top100_symbols:
                    raise RuntimeError(
                        "current Top100 coverage proposal lacks its signal"
                    )
                _table, state_table, _anchor, _active = (
                    self._history_coverage_contract(strategy_id)
                )
                state_count = connection.execute(
                    f"SELECT COUNT(*) FROM {state_table} "
                    "WHERE strategy_id=? AND symbol=?",
                    (strategy_id, symbol),
                ).fetchone()
                if (
                    state_count is None
                    or len(state_count) != 1
                    or type(state_count[0]) is not int
                    or state_count[0] < 1
                ):
                    raise RuntimeError(
                        "offboard history coverage lifecycle owner is missing"
                    )
        from .coverage_epoch_schema import attest_coverage_epoch_owner

        for proposal in proposals:
            if (
                not published
                and self._n19_terminal_publication_identity_exists(
                    connection, proposal
                )
            ):
                raise RuntimeError(
                    "N19 terminal publication identity is already sealed"
                )
            self._attest_n19_coverage_terminal_transition(
                connection,
                proposal,
            )
            if proposal.strategy_id == "N19":
                self._attest_n19_terminal_transition_signal(
                    connection,
                    scan_id,
                    proposal,
                    ordinary_signal_required=(
                        (proposal.strategy_id, proposal.symbol)
                        in required_signal_keys
                    ),
                )
            attest_coverage_epoch_owner(
                connection,
                proposal.strategy_id,
                proposal.symbol,
                allow_absent=not published,
            )
        if published:
            publication_has_terminal_binding = any(
                row[1] == "terminal_binding_sha256"
                for row in connection.execute(
                    "PRAGMA table_xinfo("
                    "history_coverage_publication_receipts)"
                )
            )
            receipt_rows = connection.execute(
                "SELECT strategy_id,symbol,source_start_time_ms,"
                "covered_through_time_ms,source_sha256,"
                "batch_expected_count,batch_manifest_sha256,"
                + (
                    "terminal_binding_sha256,"
                    if publication_has_terminal_binding
                    else "NULL AS terminal_binding_sha256,"
                )
                + "receipt_sha256 "
                "FROM history_coverage_publication_receipts "
                "WHERE source_scan_id=? ORDER BY strategy_id,symbol",
                (scan_id,),
            ).fetchall()
            receipt_by_key = {
                (row[0], row[1]): tuple(row) for row in receipt_rows
            }
            if (
                len(receipt_by_key) != len(receipt_rows)
                or set(receipt_by_key) - set(proposal_keys)
            ):
                raise RuntimeError(
                    "published history coverage receipt set conflicts"
                )
            overlay_rows = connection.execute(
                "SELECT coverage_receipt_sha256,source_scan_id,"
                "strategy_id,symbol,terminal_binding_sha256,"
                "terminal_receipt_sha256 "
                "FROM history_coverage_authorized_terminal_bindings "
                "WHERE source_scan_id=? ORDER BY strategy_id,symbol",
                (scan_id,),
            ).fetchall()
            overlay_by_receipt: dict[str, tuple[Any, ...]] = {
                row[0]: tuple(row) for row in overlay_rows
            }
            if len(overlay_by_receipt) != len(overlay_rows):
                raise RuntimeError(
                    "published N19 terminal binding set conflicts"
                )
            terminal_rows = connection.execute(
                "SELECT strategy_id,symbol,family_id,structure_id,"
                "terminal_evidence_json,terminal_evidence_sha256,"
                "batch_expected_count,batch_manifest_sha256,"
                "coverage_receipt_sha256,result_epoch_ordinal,"
                "result_epoch_start_time_ms,result_chain_head_sha256,"
                "receipt_sha256 "
                "FROM history_coverage_n19_terminal_receipts "
                "WHERE source_scan_id=? ORDER BY strategy_id,symbol",
                (scan_id,),
            ).fetchall()
            terminal_by_key = {
                (row[0], row[1]): tuple(row) for row in terminal_rows
            }
            expected_terminal_keys = {
                ("N19", proposal.symbol)
                for proposal in proposals
                if proposal.n19_terminal_state_record is not None
            }
            if (
                len(terminal_by_key) != len(terminal_rows)
                or set(terminal_by_key) != expected_terminal_keys
            ):
                raise RuntimeError(
                    "published N19 terminal receipt set conflicts"
                )
            for proposal in proposals:
                key = (proposal.strategy_id, proposal.symbol)
                owner = attest_coverage_epoch_owner(
                    connection, proposal.strategy_id, proposal.symbol
                )
                if owner is None:
                    raise RuntimeError(
                        "published history coverage owner is missing"
                    )
                head, latest_receipt, chain_tip = owner
                if head[2:4] != (
                    proposal.covered_through_time_ms,
                    proposal.source_sha256,
                ):
                    raise RuntimeError(
                        "published history coverage head is incomplete"
                    )
                current_receipt = receipt_by_key.get(key)
                if (
                    proposal.n19_terminal_state_record is not None
                    and current_receipt is None
                ):
                    raise RuntimeError(
                        "published N19 terminal transition receipt is missing"
                    )
                if current_receipt is not None:
                    expected_terminal_binding = None
                    terminal_record = proposal.n19_terminal_state_record
                    if terminal_record is not None:
                        from .coverage_epoch_schema import (
                            n19_terminal_publication_binding_sha256,
                        )

                        expected_terminal_binding = (
                            n19_terminal_publication_binding_sha256(
                                scan_id,
                                proposal.symbol,
                                terminal_record.family_id,
                                terminal_record.structure_id,
                                terminal_record.evidence_json,
                                terminal_record.evidence_sha256,
                                expected_count,
                                batch_manifest_sha256,
                                head[0],
                                head[1],
                                head[4],
                            )
                        )
                    effective_terminal_binding = current_receipt[7]
                    overlay = overlay_by_receipt.get(current_receipt[8])
                    if overlay is not None:
                        if overlay[1:4] != (
                            scan_id,
                            proposal.strategy_id,
                            proposal.symbol,
                        ):
                            raise RuntimeError(
                                "published N19 terminal binding owner conflicts"
                            )
                        effective_terminal_binding = overlay[4]
                    expected_receipt = (
                        proposal.strategy_id,
                        proposal.symbol,
                        proposal.source_start_time_ms,
                        proposal.covered_through_time_ms,
                        proposal.source_sha256,
                        expected_count,
                        batch_manifest_sha256,
                        None,
                        current_receipt[8],
                    )
                    if current_receipt != expected_receipt:
                        raise RuntimeError(
                            "published history coverage receipt conflicts"
                        )
                    if (
                        latest_receipt is None
                        or latest_receipt[0] != scan_id
                    ):
                        raise RuntimeError(
                            "published history coverage latest receipt conflicts"
                        )
                    terminal_row = terminal_by_key.get(key)
                    record = proposal.n19_terminal_state_record
                    if record is not None:
                        expected_terminal = (
                            "N19",
                            proposal.symbol,
                            record.family_id,
                            record.structure_id,
                            record.evidence_json,
                            record.evidence_sha256,
                            expected_count,
                            batch_manifest_sha256,
                            current_receipt[8],
                            head[0],
                            head[1],
                            head[4],
                        )
                        from .coverage_epoch_schema import (
                            n19_terminal_publication_receipt_sha256,
                        )

                        expected_terminal_receipt_sha256 = (
                            n19_terminal_publication_receipt_sha256(
                                scan_id,
                                proposal.symbol,
                                record.family_id,
                                record.structure_id,
                                record.evidence_json,
                                record.evidence_sha256,
                                expected_count,
                                batch_manifest_sha256,
                                current_receipt[8],
                                head[0],
                                head[1],
                                head[4],
                            )
                        )
                        if terminal_row != (
                            expected_terminal
                            + (expected_terminal_receipt_sha256,)
                        ):
                            raise RuntimeError(
                                "published N19 terminal receipt conflicts"
                            )
                        from .coverage_family_seal import (
                            PROVED_TERMINAL_DOMAIN,
                            family_seal_sha256,
                        )

                        seal = connection.execute(
                            "SELECT structure_id,terminal_evidence_sha256,"
                            "proof_domain,proof_sha256,seal_sha256 "
                            "FROM history_coverage_n19_family_seals "
                            "WHERE strategy_id='N19' AND symbol=? "
                            "AND family_id=?",
                            (proposal.symbol, record.family_id),
                        ).fetchone()
                        if (
                            seal is None
                            or tuple(seal[:4])
                            != (
                                record.structure_id,
                                record.evidence_sha256,
                                PROVED_TERMINAL_DOMAIN,
                                expected_terminal_receipt_sha256,
                            )
                            or seal[4]
                            != family_seal_sha256(
                                proposal.symbol,
                                record.family_id,
                                record.structure_id,
                                record.evidence_sha256,
                                PROVED_TERMINAL_DOMAIN,
                                expected_terminal_receipt_sha256,
                            )
                        ):
                            raise RuntimeError(
                                "published N19 terminal family seal conflicts"
                            )
                        if effective_terminal_binding != expected_terminal_binding:
                            raise RuntimeError(
                                "published N19 terminal binding conflicts"
                            )
                        if (
                            overlay is None
                            or overlay[5]
                            != expected_terminal_receipt_sha256
                        ):
                            raise RuntimeError(
                                "published N19 terminal binding receipt conflicts"
                            )
                    elif (
                        effective_terminal_binding is not None
                        or overlay is not None
                    ):
                        raise RuntimeError(
                            "published coverage has an extra terminal binding"
                        )
                else:
                    durable_source_start = (
                        chain_tip[0]
                        if latest_receipt is None
                        else latest_receipt[1]
                    )
                    if (
                        durable_source_start
                        != proposal.source_start_time_ms
                        or (
                            latest_receipt is not None
                            and latest_receipt[0] >= scan_id
                        )
                        or head[2] != proposal.covered_through_time_ms
                        or head[3] != proposal.source_sha256
                    ):
                        raise RuntimeError(
                            "published NO_CHANGE coverage is not durable"
                        )
        return frozenset(proposal_keys)
    def update_n17_history_coverage(
        self,
        symbol: str,
        source_start_time_ms: int,
        covered_through_time_ms: int,
        source_sha256: str,
    ) -> bool:
        # Runtime coverage may only advance with the scan publication
        # transaction through _apply_history_coverage_proposals().
        if self._runtime_coverage_epoch_schema_status in {
            "CURRENT",
            "AUTHORIZED_LEGACY_V3",
        }:
            return False
        if (
            type(symbol) is not str
            or not symbol
            or type(source_start_time_ms) is not int
            or type(covered_through_time_ms) is not int
            or covered_through_time_ms < source_start_time_ms
            or type(source_sha256) is not str
            or len(source_sha256) != 64
        ):
            return False
        try:
            with self._connect() as connection:
                now = utc_now()
                existing = connection.execute(
                    "SELECT source_start_time_ms,covered_through_time_ms,source_sha256 "
                    "FROM n17_history_coverage WHERE strategy_id='N17' AND symbol=?",
                    (symbol,),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        "INSERT INTO n17_history_coverage VALUES ('N17',?,?,?,?,?)",
                        (symbol, source_start_time_ms, covered_through_time_ms, source_sha256, now),
                    )
                    return True
                if existing == (source_start_time_ms, covered_through_time_ms, source_sha256):
                    return True
                if (
                    source_start_time_ms < existing[0]
                    or source_start_time_ms > existing[1] + 900_000
                    or covered_through_time_ms < existing[1]
                ):
                    return False
                connection.execute(
                    "UPDATE n17_history_coverage SET covered_through_time_ms=?,"
                    "source_sha256=?,updated_at=? WHERE strategy_id='N17' AND symbol=?",
                    (covered_through_time_ms, source_sha256, now, symbol),
                )
                return True
        except Exception as exc:
            self.logger.warning("N17 coverage persistence failed: %s", exc)
            return False

    @staticmethod
    def _n19_state_from_row(row: Any) -> N19StaircaseState:
        if type(row) not in (tuple, list) or len(row) != 15:
            raise RuntimeError("N19 lifecycle row shape is invalid")
        return N19StaircaseState(*row)

    def get_active_n19_states(
        self, strategy_id: str = "N19"
    ) -> list[N19StaircaseState]:
        if strategy_id != "N19" or type(strategy_id) is not str:
            raise ValueError("N19 strategy identity is invalid")
        with self._read_only_runtime_snapshot() as connection:
            rows = connection.execute(
                """
                SELECT id,strategy_id,symbol,family_id,structure_id,stage,reason,
                       quote_volume_rank,s_open_time_ms,x_open_time_ms,
                       reset_after_time_ms,evidence_json,evidence_sha256,
                       created_at,updated_at
                FROM n19_staircase_states
                WHERE strategy_id='N19'
                  AND stage IN (
                    'EXHAUSTION_LOCKED','CONFIRMATION_PENDING','CONFIRMED'
                  )
                ORDER BY quote_volume_rank,symbol,family_id
                """
            ).fetchall()
            # An ACTIVE claim consumes execution eligibility, not the frozen
            # family lifecycle.  Keep CONFIRMED families in the shared Kline
            # union until they durably reach a terminal stage; the write gate
            # below converts an unchanged ACTIVE replay to
            # N19_STRUCTURE_CONSUMED so it can never claim PASSED twice.
            return [self._n19_state_from_row(row) for row in rows]

    def get_required_n19_family_symbols(self) -> tuple[str, ...]:
        return tuple(sorted({state.symbol for state in self.get_active_n19_states()}))

    def get_latest_n19_states(
        self, symbols: list[str] | tuple[str, ...] | set[str]
    ) -> dict[str, N19StaircaseState]:
        if type(symbols) not in {list, tuple, set} or len(symbols) > 200:
            raise ValueError("N19 latest-state symbol set is invalid")
        ordered = sorted(set(symbols))
        if any(type(symbol) is not str or not symbol for symbol in ordered):
            raise ValueError("N19 latest-state symbol is invalid")
        result: dict[str, N19StaircaseState] = {}
        with self._read_only_runtime_snapshot() as connection:
            for symbol in ordered:
                rows = connection.execute(
                    """
                    SELECT id,strategy_id,symbol,family_id,structure_id,stage,reason,
                           quote_volume_rank,s_open_time_ms,x_open_time_ms,
                           reset_after_time_ms,evidence_json,evidence_sha256,
                           created_at,updated_at
                    FROM n19_staircase_states
                         INDEXED BY idx_n19_staircase_symbol_latest
                    WHERE strategy_id='N19' AND symbol=?
                    ORDER BY x_open_time_ms DESC,id DESC LIMIT 1
                    """,
                    (symbol,),
                ).fetchall()
                if rows:
                    result[symbol] = self._n19_state_from_row(rows[0])
        return result

    def record_n19_state(self, record: Any) -> str:
        from .n19_analyzer import (
            N19StateRecord,
            _canonical_json,
            _record_with_validated_closed_entry,
            _sha256_json,
            decode_n19_state_evidence,
        )

        try:
            if type(record) is not N19StateRecord:
                raise ValueError("N19 state record type is invalid")
            if (
                record.strategy_id != "N19"
                or type(record.symbol) is not str
                or type(record.family_id) is not str
                or len(record.family_id) != 24
                or (
                    record.structure_id is not None
                    and (
                        type(record.structure_id) is not str
                        or len(record.structure_id) != 24
                    )
                )
                or record.stage not in {
                    "EXHAUSTION_LOCKED", "CONFIRMATION_PENDING", "CONFIRMED",
                    "CONSUMED", "MISSED", "INVALID", "EXPIRED",
                }
                or (record.stage == "CONFIRMED" and record.structure_id is None)
                or type(record.quote_volume_rank) is not int
                or not 1 <= record.quote_volume_rank <= 100
                or type(record.s_open_time_ms) is not int
                or type(record.x_open_time_ms) is not int
                or record.x_open_time_ms <= record.s_open_time_ms
                or type(record.evidence) is not dict
                or record.evidence.get("strategy_id") != "N19"
                or record.evidence.get("symbol") != record.symbol
                or record.evidence.get("family_id") != record.family_id
                or record.evidence.get("structure_id") != record.structure_id
                or record.evidence.get("stage") != record.stage
                or record.evidence.get("reason") != record.reason
                or record.evidence.get("reset_after_time_ms")
                != record.reset_after_time_ms
            ):
                raise ValueError("N19 state identity conflicts with evidence")
            unsigned = dict(record.evidence)
            claimed_hash = unsigned.pop("canonical_sha256", None)
            if (
                type(claimed_hash) is not str
                or claimed_hash != _sha256_json(unsigned)
                or record.evidence_sha256 != _sha256_json(record.evidence)
                or len(_canonical_json(record.evidence).encode("utf-8")) > 131072
            ):
                raise ValueError("N19 state evidence digest is invalid")
            decoded = decode_n19_state_evidence(
                record.evidence_json,
                expected_symbol=record.symbol,
            )
            if (
                decoded.family_id != record.family_id
                or decoded.structure_id != record.structure_id
                or decoded.stage != record.stage
                or decoded.reason != record.reason
                or decoded.quote_volume_rank != record.quote_volume_rank
                or decoded.s_open_time_ms != record.s_open_time_ms
                or decoded.x_open_time_ms != record.x_open_time_ms
                or decoded.reset_after_time_ms != record.reset_after_time_ms
            ):
                raise ValueError("N19 decoded evidence conflicts with state")
        except Exception:
            return "N19_STATE_INCONSISTENT"
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing_row = connection.execute(
                    """
                    SELECT id,strategy_id,symbol,family_id,structure_id,stage,reason,
                           quote_volume_rank,s_open_time_ms,x_open_time_ms,
                           reset_after_time_ms,evidence_json,evidence_sha256,
                           created_at,updated_at
                    FROM n19_staircase_states
                    WHERE strategy_id='N19' AND family_id=?
                    """,
                    (record.family_id,),
                ).fetchone()
                now = utc_now()
                if existing_row is None:
                    if (
                        record.reason == "N19_HISTORICAL_ENTRY_MISSED"
                        and record.reset_after_time_ms is None
                    ):
                        # This transition is a publication event, not an
                        # independent lifecycle insert.  It must be committed
                        # with its signal, receipt, seal, and CURRENT pointer.
                        return "N19_STATE_INCONSISTENT"
                    if record.reset_after_time_ms is not None:
                        return "N19_STATE_INCONSISTENT"
                    if record.structure_id is not None:
                        consumed = connection.execute(
                            "SELECT 1 FROM strategy_passed_structure_ledger "
                            "WHERE strategy_id='N19' AND structure_id=? "
                            "AND claim_state='ACTIVE' LIMIT 1",
                            (record.structure_id,),
                        ).fetchone()
                        if consumed is not None:
                            return "N19_STRUCTURE_CONSUMED"
                    connection.execute(
                        """
                        INSERT INTO n19_staircase_states (
                            strategy_id,symbol,family_id,structure_id,stage,reason,
                            quote_volume_rank,s_open_time_ms,x_open_time_ms,
                            reset_after_time_ms,evidence_json,evidence_sha256,
                            created_at,updated_at
                        ) VALUES ('N19',?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            record.symbol, record.family_id, record.structure_id,
                            record.stage, record.reason, record.quote_volume_rank,
                            record.s_open_time_ms, record.x_open_time_ms,
                            record.reset_after_time_ms, record.evidence_json,
                            record.evidence_sha256, now, now,
                        ),
                    )
                    return "INSERTED"
                existing = self._n19_state_from_row(existing_row)
                if (
                    existing.symbol != record.symbol
                    or existing.s_open_time_ms != record.s_open_time_ms
                    or existing.x_open_time_ms != record.x_open_time_ms
                    or existing.quote_volume_rank != record.quote_volume_rank
                    or (
                        existing.structure_id is not None
                        and existing.structure_id != record.structure_id
                    )
                ):
                    return "N19_STATE_INCONSISTENT"
                consumed = None
                if record.structure_id is not None:
                    consumed = connection.execute(
                        "SELECT 1 FROM strategy_passed_structure_ledger "
                        "WHERE strategy_id='N19' AND structure_id=? "
                        "AND claim_state='ACTIVE' LIMIT 1",
                        (record.structure_id,),
                    ).fetchone()
                if existing.evidence_sha256 == record.evidence_sha256:
                    if consumed is not None and existing.stage not in {
                        "CONSUMED", "MISSED", "INVALID", "EXPIRED"
                    }:
                        return "N19_STRUCTURE_CONSUMED"
                    return "UNCHANGED"
                if (
                    record.reason == "N19_HISTORICAL_ENTRY_MISSED"
                    and record.reset_after_time_ms is None
                ):
                    # An exact replay above is harmless.  Any distinct
                    # CONFIRMED -> historical transition must use the atomic
                    # publication path so a permanent proof is created.
                    return "N19_STATE_INCONSISTENT"
                if existing.stage in {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}:
                    try:
                        decoded_existing = decode_n19_state_evidence(
                            existing.evidence_json,
                            expected_symbol=record.symbol,
                        )
                        terminal_cutoff = decoded_existing.evidence[
                            "terminal_cutoff_time_ms"
                        ]
                        existing_source = decoded_existing.evidence["source"]
                        updated_source = record.evidence["source"]
                        existing_reset_evidence = decoded_existing.evidence.get(
                            "reset_evidence"
                        )
                        updated_reset_evidence = record.evidence.get(
                            "reset_evidence"
                        )
                        evolved_existing = _record_with_validated_closed_entry(
                            decoded_existing,
                            updated_source[: len(existing_source)],
                        )
                        evolved_existing_source = evolved_existing.evidence[
                            "source"
                        ]
                        immutable_existing = dict(evolved_existing.evidence)
                        immutable_updated = dict(record.evidence)
                        for payload in (immutable_existing, immutable_updated):
                            payload.pop("canonical_sha256", None)
                            payload.pop("reset_after_time_ms", None)
                            payload.pop("source", None)
                            payload.pop("reset_evidence", None)
                        continuous_reset_is_exact = (
                            type(terminal_cutoff) is int
                            and type(record.reset_after_time_ms) is int
                            and record.reset_after_time_ms > terminal_cutoff
                            and type(existing_source) is list
                            and type(updated_source) is list
                            and len(updated_source) >= len(existing_source)
                            and updated_source[: len(existing_source)]
                            == evolved_existing_source
                            and existing_reset_evidence is None
                            and updated_reset_evidence is None
                            and immutable_updated == immutable_existing
                        )
                        disconnected_reset_is_exact = (
                            type(terminal_cutoff) is int
                            and type(record.reset_after_time_ms) is int
                            and record.reset_after_time_ms > terminal_cutoff
                            and type(existing_source) is list
                            and updated_source == existing_source
                            and existing_reset_evidence is None
                            and type(updated_reset_evidence) is dict
                            and immutable_updated == immutable_existing
                        )
                        reset_update_is_exact = (
                            continuous_reset_is_exact
                            or disconnected_reset_is_exact
                        )
                    except Exception:
                        reset_update_is_exact = False
                    if (
                        existing.stage == record.stage
                        and existing.reason == record.reason
                        and existing.reset_after_time_ms is None
                        and reset_update_is_exact
                    ):
                        witness = connection.execute(
                            "SELECT state_row_sha256 FROM "
                            "history_coverage_n19_legacy_unbound_witnesses "
                            "WHERE strategy_id='N19' AND symbol=? "
                            "AND family_id=? AND structure_id=?",
                            (
                                record.symbol,
                                record.family_id,
                                record.structure_id,
                            ),
                        ).fetchone()
                        if witness is not None:
                            from .coverage_family_seal import (
                                typed_row_sha256,
                            )

                            successor_row = list(existing_row)
                            successor_row[10] = record.reset_after_time_ms
                            successor_row[11] = record.evidence_json
                            successor_row[12] = record.evidence_sha256
                            successor_row[14] = now
                            successor_state_sha256 = typed_row_sha256(
                                tuple(successor_row)
                            )
                            self._legacy_reset_mutation_scope = (
                                record.symbol,
                                record.reset_after_time_ms,
                                record.evidence_sha256,
                            )
                            try:
                                inserted = connection.execute(
                                    "INSERT INTO "
                                    "history_coverage_n19_legacy_reset_successors "
                                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                                    (
                                        "N19",
                                        record.symbol,
                                        record.family_id,
                                        record.structure_id,
                                        witness[0],
                                        record.reset_after_time_ms,
                                        record.evidence_json,
                                        record.evidence_sha256,
                                        successor_state_sha256,
                                        now,
                                    ),
                                )
                                if inserted.rowcount != 1:
                                    raise RuntimeError(
                                        "N19 legacy reset successor was not sealed"
                                    )
                            finally:
                                self._legacy_reset_mutation_scope = None
                        changed = connection.execute(
                            "UPDATE n19_staircase_states SET "
                            "reset_after_time_ms=?,evidence_json=?,evidence_sha256=?,"
                            "updated_at=? WHERE id=? AND reset_after_time_ms IS NULL "
                            "AND evidence_sha256=?",
                            (
                                record.reset_after_time_ms, record.evidence_json,
                                record.evidence_sha256, now, existing.id,
                                existing.evidence_sha256,
                            ),
                        )
                        if changed.rowcount != 1:
                            raise RuntimeError("N19 reset evidence update conflicted")
                        generation_row = connection.execute(
                            "SELECT generation FROM "
                            "history_coverage_protected_generation "
                            "WHERE singleton_id=1"
                        ).fetchone()
                        schema_row = connection.execute(
                            "PRAGMA schema_version"
                        ).fetchone()
                        expected_generation = (
                            self._runtime_family_graph_active_generation
                        )
                        if (
                            type(expected_generation) is not int
                            or generation_row is None
                            or len(generation_row) != 1
                            or type(generation_row[0]) is not int
                            or generation_row[0] < expected_generation
                            or schema_row is None
                            or len(schema_row) != 1
                            or type(schema_row[0]) is not int
                            or schema_row[0] <= 0
                            or self._protected_generation_pair_after_commit
                            is not None
                        ):
                            raise RuntimeError(
                                "N19 reset protected generation is inconsistent"
                            )
                        if generation_row[0] > expected_generation:
                            from .coverage_family_seal import (
                                family_seal_catalog_sha256,
                            )

                            self._protected_generation_pair_after_commit = (
                                expected_generation,
                                generation_row[0],
                                schema_row[0],
                                family_seal_catalog_sha256(connection),
                            )
                        return "UPDATED"
                    return "N19_STRUCTURE_CONSUMED"
                allow_consumed_terminal_progression = (
                    consumed is not None
                    and existing.stage == "CONFIRMED"
                    and record.stage in {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}
                    and existing.structure_id == record.structure_id
                )
                if consumed is not None and not allow_consumed_terminal_progression:
                    return "N19_STRUCTURE_CONSUMED"
                progression = {
                    "EXHAUSTION_LOCKED": {
                        "CONFIRMATION_PENDING", "CONFIRMED", "CONSUMED",
                        "MISSED", "INVALID", "EXPIRED",
                    },
                    "CONFIRMATION_PENDING": {
                        "CONFIRMED", "CONSUMED", "MISSED", "INVALID", "EXPIRED",
                    },
                    "CONFIRMED": {"CONSUMED", "MISSED", "INVALID", "EXPIRED"},
                }
                if record.stage not in progression.get(existing.stage, set()):
                    return "N19_STATE_INCONSISTENT"
                changed = connection.execute(
                    """
                    UPDATE n19_staircase_states
                    SET structure_id=?,stage=?,reason=?,reset_after_time_ms=?,
                        evidence_json=?,evidence_sha256=?,updated_at=?
                    WHERE id=? AND evidence_sha256=?
                    """,
                    (
                        record.structure_id, record.stage, record.reason,
                        record.reset_after_time_ms, record.evidence_json,
                        record.evidence_sha256, now, existing.id,
                        existing.evidence_sha256,
                    ),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("N19 lifecycle update conflicted")
                return "UPDATED"
        except Exception as exc:
            self.logger.warning("N19 state persistence failed: %s", exc)
            return "N19_STATE_PERSIST_FAILED"

    def update_n19_history_coverage(
        self,
        symbol: str,
        source_start_time_ms: int,
        covered_through_time_ms: int,
        source_sha256: str,
    ) -> bool:
        # Runtime coverage may only advance with the scan publication
        # transaction through _apply_history_coverage_proposals().
        if self._runtime_coverage_epoch_schema_status in {
            "CURRENT",
            "AUTHORIZED_LEGACY_V3",
        }:
            return False
        if (
            type(symbol) is not str
            or not symbol
            or type(source_start_time_ms) is not int
            or type(covered_through_time_ms) is not int
            or covered_through_time_ms < source_start_time_ms
            or type(source_sha256) is not str
            or len(source_sha256) != 64
        ):
            return False
        try:
            with self._connect() as connection:
                now = utc_now()
                existing = connection.execute(
                    "SELECT source_start_time_ms,covered_through_time_ms,source_sha256 "
                    "FROM n19_history_coverage WHERE strategy_id='N19' AND symbol=?",
                    (symbol,),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        "INSERT INTO n19_history_coverage VALUES ('N19',?,?,?,?,?)",
                        (symbol, source_start_time_ms, covered_through_time_ms, source_sha256, now),
                    )
                    return True
                if existing == (source_start_time_ms, covered_through_time_ms, source_sha256):
                    return True
                if (
                    source_start_time_ms < existing[0]
                    or source_start_time_ms > existing[1] + 900_000
                    or covered_through_time_ms < existing[1]
                ):
                    return False
                connection.execute(
                    "UPDATE n19_history_coverage SET covered_through_time_ms=?,"
                    "source_sha256=?,updated_at=? WHERE strategy_id='N19' AND symbol=?",
                    (covered_through_time_ms, source_sha256, now, symbol),
                )
                return True
        except Exception as exc:
            self.logger.warning("N19 coverage persistence failed: %s", exc)
            return False

    @staticmethod
    def _n18_state_from_row(row: Any) -> N18TriangleState:
        if type(row) not in (tuple, list) or len(row) != 15:
            raise RuntimeError("N18 lifecycle row shape is invalid")
        return N18TriangleState(*row)

    def get_active_n18_states(
        self, strategy_id: str = "N18"
    ) -> list[N18TriangleState]:
        if type(strategy_id) is not str or strategy_id != "N18":
            raise ValueError("N18 strategy identity is invalid")
        with self._read_only_runtime_snapshot() as connection:
            rows = connection.execute(
                """
                SELECT id,strategy_id,symbol,family_id,structure_id,stage,reason,
                       quote_volume_rank,l1_open_time_ms,a_open_time_ms,
                       terminal_cutoff_time_ms,evidence_json,evidence_sha256,
                       created_at,updated_at
                FROM n18_triangle_states
                WHERE strategy_id='N18'
                  AND stage IN (
                    'TRIANGLE_ARMED','ABSORPTION_LOCKED',
                    'BREAKOUT_PENDING','CONFIRMED'
                  )
                ORDER BY quote_volume_rank,symbol,family_id
                """
            ).fetchall()
            return [self._n18_state_from_row(row) for row in rows]

    def get_required_n18_family_symbols(self) -> tuple[str, ...]:
        return tuple(sorted({state.symbol for state in self.get_active_n18_states()}))

    def get_latest_n18_states(
        self, symbols: list[str] | tuple[str, ...] | set[str]
    ) -> dict[str, N18TriangleState]:
        if type(symbols) not in {list, tuple, set} or len(symbols) > 200:
            raise ValueError("N18 latest-state symbol set is invalid")
        ordered = sorted(set(symbols))
        if any(type(symbol) is not str or not symbol for symbol in ordered):
            raise ValueError("N18 latest-state symbol is invalid")
        result: dict[str, N18TriangleState] = {}
        with self._read_only_runtime_snapshot() as connection:
            for symbol in ordered:
                row = connection.execute(
                    """
                    SELECT id,strategy_id,symbol,family_id,structure_id,stage,reason,
                           quote_volume_rank,l1_open_time_ms,a_open_time_ms,
                           terminal_cutoff_time_ms,evidence_json,evidence_sha256,
                           created_at,updated_at
                    FROM n18_triangle_states
                         INDEXED BY idx_n18_triangle_symbol_latest
                    WHERE strategy_id='N18' AND symbol=?
                    ORDER BY a_open_time_ms DESC,id DESC LIMIT 1
                    """,
                    (symbol,),
                ).fetchone()
                if row is not None:
                    result[symbol] = self._n18_state_from_row(row)
        return result

    def record_n18_state(self, record: Any) -> str:
        from .n18_analyzer import (
            N18StateRecord,
            _canonical_json,
            _sha256_json,
            decode_n18_state_evidence,
            validate_n18_source_progression,
        )

        terminal = {"CONSUMED", "MISSED", "INVALID", "EXPIRED"}
        active = {
            "TRIANGLE_ARMED", "ABSORPTION_LOCKED", "BREAKOUT_PENDING",
            "CONFIRMED",
        }
        try:
            if type(record) is not N18StateRecord:
                raise ValueError("N18 state record type is invalid")
            if (
                record.strategy_id != "N18"
                or type(record.symbol) is not str
                or type(record.family_id) is not str
                or len(record.family_id) != 24
                or (
                    record.structure_id is not None
                    and (type(record.structure_id) is not str or len(record.structure_id) != 24)
                )
                or record.stage not in active | terminal
                or (record.stage == "CONFIRMED" and record.structure_id is None)
                or type(record.quote_volume_rank) is not int
                or not 1 <= record.quote_volume_rank <= 100
                or type(record.l1_open_time_ms) is not int
                or type(record.a_open_time_ms) is not int
                or record.a_open_time_ms <= record.l1_open_time_ms
                or type(record.evidence) is not dict
                or record.evidence.get("strategy_id") != "N18"
                or record.evidence.get("symbol") != record.symbol
                or record.evidence.get("family_id") != record.family_id
                or record.evidence.get("structure_id") != record.structure_id
                or record.evidence.get("stage") != record.stage
                or record.evidence.get("reason") != record.reason
                or record.evidence.get("terminal_cutoff_time_ms")
                != record.terminal_cutoff_time_ms
            ):
                raise ValueError("N18 state identity conflicts with evidence")
            unsigned = dict(record.evidence)
            claimed_hash = unsigned.pop("canonical_sha256", None)
            if (
                type(claimed_hash) is not str
                or claimed_hash != _sha256_json(unsigned)
                or record.evidence_sha256 != _sha256_json(record.evidence)
                or len(_canonical_json(record.evidence).encode("utf-8")) > 131072
            ):
                raise ValueError("N18 state evidence digest is invalid")
            decoded = decode_n18_state_evidence(
                record.evidence_json, expected_symbol=record.symbol
            )
            if (
                decoded.family_id != record.family_id
                or decoded.structure_id != record.structure_id
                or decoded.stage != record.stage
                or decoded.reason != record.reason
                or decoded.quote_volume_rank != record.quote_volume_rank
                or decoded.l1_open_time_ms != record.l1_open_time_ms
                or decoded.a_open_time_ms != record.a_open_time_ms
                or decoded.terminal_cutoff_time_ms != record.terminal_cutoff_time_ms
            ):
                raise ValueError("N18 decoded evidence conflicts with state")
        except Exception:
            return "N18_STATE_INCONSISTENT"
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    """
                    SELECT id,strategy_id,symbol,family_id,structure_id,stage,reason,
                           quote_volume_rank,l1_open_time_ms,a_open_time_ms,
                           terminal_cutoff_time_ms,evidence_json,evidence_sha256,
                           created_at,updated_at
                    FROM n18_triangle_states
                    WHERE strategy_id='N18' AND family_id=?
                    """,
                    (record.family_id,),
                ).fetchone()
                now = utc_now()
                if row is None:
                    if record.structure_id is not None:
                        consumed = connection.execute(
                            "SELECT 1 FROM strategy_passed_structure_ledger "
                            "WHERE strategy_id='N18' AND structure_id=? "
                            "AND claim_state='ACTIVE' LIMIT 1",
                            (record.structure_id,),
                        ).fetchone()
                        if consumed is not None:
                            return "N18_STRUCTURE_CONSUMED"
                    connection.execute(
                        """
                        INSERT INTO n18_triangle_states (
                            strategy_id,symbol,family_id,structure_id,stage,reason,
                            quote_volume_rank,l1_open_time_ms,a_open_time_ms,
                            terminal_cutoff_time_ms,evidence_json,evidence_sha256,
                            created_at,updated_at
                        ) VALUES ('N18',?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            record.symbol, record.family_id, record.structure_id,
                            record.stage, record.reason, record.quote_volume_rank,
                            record.l1_open_time_ms, record.a_open_time_ms,
                            record.terminal_cutoff_time_ms, record.evidence_json,
                            record.evidence_sha256, now, now,
                        ),
                    )
                    return "INSERTED"
                existing = self._n18_state_from_row(row)
                if (
                    existing.symbol != record.symbol
                    or existing.l1_open_time_ms != record.l1_open_time_ms
                    or existing.a_open_time_ms != record.a_open_time_ms
                    or existing.quote_volume_rank != record.quote_volume_rank
                    or (
                        existing.structure_id is not None
                        and existing.structure_id != record.structure_id
                    )
                ):
                    return "N18_STATE_INCONSISTENT"
                consumed = None
                if record.structure_id is not None:
                    consumed = connection.execute(
                        "SELECT 1 FROM strategy_passed_structure_ledger "
                        "WHERE strategy_id='N18' AND structure_id=? "
                        "AND claim_state='ACTIVE' LIMIT 1",
                        (record.structure_id,),
                    ).fetchone()
                if existing.evidence_sha256 == record.evidence_sha256:
                    if consumed is not None and existing.stage not in terminal:
                        return "N18_STRUCTURE_CONSUMED"
                    return "UNCHANGED"
                try:
                    decoded_existing = decode_n18_state_evidence(
                        existing.evidence_json,
                        expected_symbol=existing.symbol,
                    )
                    validate_n18_source_progression(
                        decoded_existing,
                        decoded,
                    )
                except Exception as exc:
                    self.logger.warning(
                        "N18 state progression conflict | symbol=%s "
                        "family_id=%s existing_stage=%s proposed_stage=%s "
                        "error_type=%s",
                        record.symbol,
                        record.family_id,
                        existing.stage,
                        record.stage,
                        type(exc).__name__,
                    )
                    return "N18_STATE_INCONSISTENT"
                if existing.stage in terminal:
                    return "N18_STRUCTURE_CONSUMED"
                allow_consumed_terminal = (
                    consumed is not None
                    and existing.stage == "CONFIRMED"
                    and record.stage in terminal
                    and existing.structure_id == record.structure_id
                )
                if consumed is not None and not allow_consumed_terminal:
                    return "N18_STRUCTURE_CONSUMED"
                progression = {
                    "TRIANGLE_ARMED": active | terminal,
                    "ABSORPTION_LOCKED": {
                        "ABSORPTION_LOCKED", "BREAKOUT_PENDING", "CONFIRMED",
                    } | terminal,
                    "BREAKOUT_PENDING": {
                        "BREAKOUT_PENDING", "CONFIRMED",
                    } | terminal,
                    "CONFIRMED": {"CONFIRMED"} | terminal,
                }
                if record.stage not in progression.get(existing.stage, set()):
                    return "N18_STATE_INCONSISTENT"
                changed = connection.execute(
                    """
                    UPDATE n18_triangle_states
                    SET structure_id=?,stage=?,reason=?,terminal_cutoff_time_ms=?,
                        evidence_json=?,evidence_sha256=?,updated_at=?
                    WHERE id=? AND evidence_sha256=?
                    """,
                    (
                        record.structure_id, record.stage, record.reason,
                        record.terminal_cutoff_time_ms, record.evidence_json,
                        record.evidence_sha256, now, existing.id,
                        existing.evidence_sha256,
                    ),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("N18 lifecycle update conflicted")
                return "UPDATED"
        except Exception as exc:
            self.logger.warning("N18 state persistence failed: %s", exc)
            return "N18_STATE_PERSIST_FAILED"

    def update_n18_history_coverage(
        self,
        symbol: str,
        source_start_time_ms: int,
        covered_through_time_ms: int,
        source_sha256: str,
    ) -> bool:
        # Runtime coverage may only advance with the scan publication
        # transaction through _apply_history_coverage_proposals().
        if self._runtime_coverage_epoch_schema_status in {
            "CURRENT",
            "AUTHORIZED_LEGACY_V3",
        }:
            return False
        if (
            type(symbol) is not str or not symbol
            or type(source_start_time_ms) is not int
            or type(covered_through_time_ms) is not int
            or covered_through_time_ms < source_start_time_ms
            or type(source_sha256) is not str or len(source_sha256) != 64
        ):
            return False
        try:
            with self._connect() as connection:
                now = utc_now()
                existing = connection.execute(
                    "SELECT source_start_time_ms,covered_through_time_ms,source_sha256 "
                    "FROM n18_history_coverage WHERE strategy_id='N18' AND symbol=?",
                    (symbol,),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        "INSERT INTO n18_history_coverage VALUES ('N18',?,?,?,?,?)",
                        (symbol, source_start_time_ms, covered_through_time_ms,
                         source_sha256, now),
                    )
                    return True
                if existing == (source_start_time_ms, covered_through_time_ms, source_sha256):
                    return True
                if (
                    source_start_time_ms < existing[0]
                    or source_start_time_ms > existing[1] + 900_000
                    or covered_through_time_ms < existing[1]
                ):
                    return False
                connection.execute(
                    "UPDATE n18_history_coverage SET covered_through_time_ms=?,"
                    "source_sha256=?,updated_at=? WHERE strategy_id='N18' AND symbol=?",
                    (covered_through_time_ms, source_sha256, now, symbol),
                )
                return True
        except Exception as exc:
            self.logger.warning("N18 coverage persistence failed: %s", exc)
            return False

    @staticmethod
    def _n20_state_from_row(row: Any) -> N20MarketEpisodeState:
        if type(row) not in (tuple, list) or len(row) != 16:
            raise RuntimeError("N20 episode row shape is invalid")
        value = list(row)
        if type(value[11]) is not bytes:
            value[11] = bytes(value[11])
        return N20MarketEpisodeState(*value)

    def get_active_n20_episode(self) -> N20MarketEpisodeState | None:
        with self._read_only_runtime_snapshot() as connection:
            rows = connection.execute(
                """
                SELECT id,strategy_id,episode_id,stage,reason,m0_open_time_ms,
                       d1_open_time_ms,c_open_time_ms,winner_symbol,structure_id,
                       terminal_cutoff_time_ms,evidence_blob,evidence_size,
                       evidence_sha256,created_at,updated_at
                FROM n20_market_episodes INDEXED BY idx_n20_episode_active
                WHERE strategy_id='N20' AND stage IN (
                    'BULL_CONTEXT_FROZEN','PULLBACK_ACTIVE','RECOVERY_FROZEN',
                    'ENTRY_WAITING','CONFIRMED'
                )
                ORDER BY d1_open_time_ms DESC,id DESC LIMIT 2
                """
            ).fetchall()
            if len(rows) > 1:
                raise RuntimeError("multiple active N20 market episodes exist")
            return self._n20_state_from_row(rows[0]) if rows else None

    def get_latest_n20_episode(self) -> N20MarketEpisodeState | None:
        with self._read_only_runtime_snapshot() as connection:
            row = connection.execute(
                """
                SELECT id,strategy_id,episode_id,stage,reason,m0_open_time_ms,
                       d1_open_time_ms,c_open_time_ms,winner_symbol,structure_id,
                       terminal_cutoff_time_ms,evidence_blob,evidence_size,
                       evidence_sha256,created_at,updated_at
                FROM n20_market_episodes INDEXED BY idx_n20_episode_latest
                WHERE strategy_id='N20'
                ORDER BY d1_open_time_ms DESC,id DESC LIMIT 1
                """
            ).fetchone()
            return self._n20_state_from_row(row) if row is not None else None

    def get_required_n20_frozen_symbols(self) -> tuple[str, ...]:
        from .n20_analyzer import decode_n20_state_evidence

        state = self.get_active_n20_episode()
        if state is None:
            return ()
        decoded = decode_n20_state_evidence(
            state.evidence_blob, state.evidence_size, state.evidence_sha256
        )
        if decoded.episode_id != state.episode_id or decoded.stage != state.stage:
            raise RuntimeError("N20 active episode evidence conflicts")
        return tuple(item["symbol"] for item in decoded.evidence["members"])

    def record_n20_episode(self, record: Any) -> str:
        from .n20_analyzer import (
            N20StateRecord,
            decode_n20_state_evidence,
            validate_n20_stage_reason,
            validate_n20_source_progression,
        )

        active = {
            "BULL_CONTEXT_FROZEN", "PULLBACK_ACTIVE", "RECOVERY_FROZEN",
            "ENTRY_WAITING", "CONFIRMED",
        }
        terminal = {"CONSUMED", "MISSED", "INVALID", "CRASH_VETO", "EXPIRED"}
        try:
            if type(record) is not N20StateRecord or record.strategy_id != "N20":
                raise ValueError("N20 state record type is invalid")
            validate_n20_stage_reason(record.stage, record.reason)
            if (
                type(record.episode_id) is not str
                or type(record.m0_open_time_ms) is not int
                or type(record.d1_open_time_ms) is not int
                or (
                    record.c_open_time_ms is not None
                    and type(record.c_open_time_ms) is not int
                )
                or (
                    record.terminal_cutoff_time_ms is not None
                    and type(record.terminal_cutoff_time_ms) is not int
                )
                or (
                    record.winner_symbol is not None
                    and type(record.winner_symbol) is not str
                )
                or (
                    record.structure_id is not None
                    and type(record.structure_id) is not str
                )
            ):
                raise ValueError("N20 state record identity types are invalid")
            blob, size, digest = record.encoded
            decoded = decode_n20_state_evidence(blob, size, digest)
            if (
                decoded.episode_id != record.episode_id
                or decoded.stage != record.stage or decoded.reason != record.reason
                or decoded.m0_open_time_ms != record.m0_open_time_ms
                or decoded.d1_open_time_ms != record.d1_open_time_ms
                or decoded.c_open_time_ms != record.c_open_time_ms
                or decoded.winner_symbol != record.winner_symbol
                or decoded.structure_id != record.structure_id
                or decoded.terminal_cutoff_time_ms != record.terminal_cutoff_time_ms
                or record.stage not in active | terminal
                or (record.stage == "CONFIRMED" and record.structure_id is None)
            ):
                raise ValueError("N20 state identity conflicts with evidence")
        except Exception:
            return "N20_STATE_INCONSISTENT"
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                rows = connection.execute(
                    """
                    SELECT id,strategy_id,episode_id,stage,reason,m0_open_time_ms,
                           d1_open_time_ms,c_open_time_ms,winner_symbol,structure_id,
                           terminal_cutoff_time_ms,evidence_blob,evidence_size,
                           evidence_sha256,created_at,updated_at
                    FROM n20_market_episodes WHERE strategy_id='N20'
                    ORDER BY d1_open_time_ms DESC,id DESC LIMIT 2
                    """
                ).fetchall()
                existing = None
                for row in rows:
                    item = self._n20_state_from_row(row)
                    if item.episode_id == record.episode_id:
                        existing = item
                        break
                now = utc_now()
                if existing is None:
                    if rows:
                        latest = self._n20_state_from_row(rows[0])
                        if latest.stage in active:
                            return "N20_STATE_INCONSISTENT"
                        if (
                            latest.terminal_cutoff_time_ms is None
                            or record.d1_open_time_ms <= latest.terminal_cutoff_time_ms
                        ):
                            return "N20_EPISODE_CONSUMED"
                    if record.structure_id is not None and connection.execute(
                        "SELECT 1 FROM strategy_passed_structure_ledger "
                        "WHERE strategy_id='N20' AND structure_id=? "
                        "AND claim_state='ACTIVE' LIMIT 1",
                        (record.structure_id,),
                    ).fetchone() is not None:
                        return "N20_EPISODE_CONSUMED"
                    connection.execute(
                        """
                        INSERT INTO n20_market_episodes (
                            strategy_id,episode_id,stage,reason,m0_open_time_ms,
                            d1_open_time_ms,c_open_time_ms,winner_symbol,structure_id,
                            terminal_cutoff_time_ms,evidence_blob,evidence_size,
                            evidence_sha256,created_at,updated_at
                        ) VALUES ('N20',?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            record.episode_id, record.stage, record.reason,
                            record.m0_open_time_ms, record.d1_open_time_ms,
                            record.c_open_time_ms, record.winner_symbol,
                            record.structure_id, record.terminal_cutoff_time_ms,
                            sqlite3.Binary(blob), size, digest, now, now,
                        ),
                    )
                    return "INSERTED"
                if (
                    existing.m0_open_time_ms != record.m0_open_time_ms
                    or existing.d1_open_time_ms != record.d1_open_time_ms
                    or (existing.c_open_time_ms is not None and existing.c_open_time_ms != record.c_open_time_ms)
                    or (existing.winner_symbol is not None and existing.winner_symbol != record.winner_symbol)
                    or (existing.structure_id is not None and existing.structure_id != record.structure_id)
                ):
                    return "N20_STATE_INCONSISTENT"
                try:
                    decoded_existing = decode_n20_state_evidence(
                        existing.evidence_blob,
                        existing.evidence_size,
                        existing.evidence_sha256,
                    )
                    validate_n20_source_progression(
                        decoded_existing.evidence["source"],
                        decoded.evidence["source"],
                    )
                except Exception:
                    return "N20_STATE_INCONSISTENT"
                consumed = None
                if record.structure_id is not None:
                    consumed = connection.execute(
                        "SELECT 1 FROM strategy_passed_structure_ledger "
                        "WHERE strategy_id='N20' AND structure_id=? "
                        "AND claim_state='ACTIVE' LIMIT 1",
                        (record.structure_id,),
                    ).fetchone()
                if existing.evidence_sha256 == digest:
                    if consumed is not None and existing.stage not in terminal:
                        return "N20_EPISODE_CONSUMED"
                    return "UNCHANGED"
                if existing.stage in terminal:
                    return "N20_EPISODE_CONSUMED"
                if consumed is not None and record.stage not in terminal:
                    return "N20_EPISODE_CONSUMED"
                progression = {
                    "BULL_CONTEXT_FROZEN": active | terminal,
                    "PULLBACK_ACTIVE": active | terminal,
                    "RECOVERY_FROZEN": {"ENTRY_WAITING", "CONFIRMED"} | terminal,
                    "ENTRY_WAITING": {"CONFIRMED"} | terminal,
                    "CONFIRMED": terminal,
                }
                if record.stage not in progression.get(existing.stage, set()):
                    return "N20_STATE_INCONSISTENT"
                changed = connection.execute(
                    """
                    UPDATE n20_market_episodes
                    SET stage=?,reason=?,c_open_time_ms=?,winner_symbol=?,
                        structure_id=?,terminal_cutoff_time_ms=?,evidence_blob=?,
                        evidence_size=?,evidence_sha256=?,updated_at=?
                    WHERE id=? AND evidence_sha256=?
                    """,
                    (
                        record.stage, record.reason, record.c_open_time_ms,
                        record.winner_symbol, record.structure_id,
                        record.terminal_cutoff_time_ms, sqlite3.Binary(blob), size,
                        digest, now, existing.id, existing.evidence_sha256,
                    ),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("N20 episode update conflicted")
                return "UPDATED"
        except Exception as exc:
            self.logger.warning("N20 episode persistence failed: %s", exc)
            return "N20_STATE_PERSIST_FAILED"

    def _prepare_strategy_signal(
        self,
        scan_id: int | None,
        strategy_id: str,
        symbol: str,
        funding_rate: str,
        matched_patterns: list[str] | tuple[str, ...],
        trend_slope: str,
        current_bullish: bool,
        passed: bool,
        decision: str,
        reason: str,
        structure_id: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> _PreparedStrategySignal:
        detail_payload = {} if detail is None else detail
        if strategy_id == "N13":
            detail_payload = _bounded_n13_reused_detail(detail_payload)
        if type(scan_id) is not int or scan_id <= 0:
            raise ValueError("strategy signal scan_id must be a positive integer")
        _strict_signal_text(strategy_id, "strategy_id", 32)
        if strategy_id not in _SUPPORTED_STRATEGY_IDS:
            raise ValueError("strategy_id is not supported")
        _strict_signal_text(symbol, "symbol", 64)
        if (
            type(funding_rate) is not str
            or len(funding_rate) > 128
            or any(ord(character) < 32 for character in funding_rate)
        ):
            raise ValueError("funding_rate must be a built-in string")
        if (
            type(trend_slope) is not str
            or len(trend_slope) > 128
            or any(ord(character) < 32 for character in trend_slope)
        ):
            raise ValueError("trend_slope must be a built-in string")
        _strict_signal_text(decision, "decision", 128)
        _strict_signal_text(reason, "reason", 512)
        if type(current_bullish) is not bool or type(passed) is not bool:
            raise ValueError("strategy signal booleans must be built-in bool")
        if type(matched_patterns) not in {list, tuple} or any(
            type(item) is not str for item in matched_patterns
        ):
            raise ValueError("matched_patterns must contain built-in strings")
        if structure_id is not None:
            _strict_signal_text(structure_id, "structure_id", 256)
        if (
            passed
            and strategy_id in _PASSED_STRUCTURE_LEDGER_STRATEGIES
            and structure_id is None
        ):
            raise ValueError(
                "passed strategy structure ledger requires structure_id"
            )
        if type(detail_payload) is not dict:
            raise ValueError("strategy signal detail must be a built-in dict")
        matched_patterns_json = json_dumps(list(matched_patterns))
        detail_json = _strict_strategy_signal_detail_json(detail_payload)
        if strategy_id == "N16" and len(detail_json.encode("utf-8")) >= 16_384:
            raise ValueError("N16 strategy signal detail exceeds 16 KiB")
        if strategy_id == "N19" and len(detail_json.encode("utf-8")) > 16_384:
            raise ValueError("N19 strategy signal detail exceeds 16 KiB")
        if strategy_id == "N18" and len(detail_json.encode("utf-8")) > 16_384:
            raise ValueError("N18 strategy signal detail exceeds 16 KiB")
        if strategy_id == "N20" and len(detail_json.encode("utf-8")) > 16_384:
            raise ValueError("N20 strategy signal detail exceeds 16 KiB")
        micro_evidence_json = None
        micro_evidence_sha256 = None
        if strategy_id in {"N21", "N22", "N23", "N24", "N25"}:
            if len(detail_json.encode("utf-8")) > 16_384:
                raise ValueError("micro strategy signal detail exceeds 16 KiB")
            if passed:
                evidence = detail_payload.get("evidence")
                if type(evidence) is not dict:
                    raise ValueError("passed micro signal evidence is missing")
                if (
                    type(evidence.get("schema_version")) is not int
                    or evidence.get("schema_version") != 1
                    or type(evidence.get("rule_version")) is not str
                    or type(evidence.get("strategy_id")) is not str
                    or evidence.get("strategy_id") != strategy_id
                    or evidence.get("symbol") != symbol
                    or evidence.get("structure_id") != structure_id
                    or type(evidence.get("kline_open_time_ms")) is not int
                    or type(evidence.get("confirmation_observed_at_ms")) is not int
                    or type(evidence.get("deadline_ms")) is not int
                    or evidence["deadline_ms"]
                    != evidence["confirmation_observed_at_ms"] + 120_000
                ):
                    raise ValueError("micro signal evidence identity conflicts")
                micro_evidence_json = json.dumps(
                    evidence,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                if len(micro_evidence_json.encode("utf-8")) > 8_192:
                    raise ValueError("micro lifecycle evidence exceeds 8 KiB")
                micro_evidence_sha256 = hashlib.sha256(
                    micro_evidence_json.encode("utf-8")
                ).hexdigest()
        created_at = utc_now()
        signal_evidence = {
            "scan_id": scan_id,
            "strategy_id": strategy_id,
            "symbol": symbol,
            "funding_rate": funding_rate,
            "matched_patterns": matched_patterns_json,
            "trend_slope": trend_slope,
            "current_bullish": int(current_bullish),
            "passed": int(passed),
            "decision": decision,
            "reason": reason,
            "structure_id": structure_id,
            "detail_json": detail_json,
            "created_at": created_at,
        }
        return _PreparedStrategySignal(
            scan_id=scan_id,
            strategy_id=strategy_id,
            symbol=symbol,
            funding_rate=funding_rate,
            matched_patterns_json=matched_patterns_json,
            trend_slope=trend_slope,
            current_bullish=current_bullish,
            passed=passed,
            decision=decision,
            reason=reason,
            structure_id=structure_id,
            detail_payload=detail_payload,
            detail_json=detail_json,
            micro_evidence_json=micro_evidence_json,
            micro_evidence_sha256=micro_evidence_sha256,
            created_at=created_at,
            evidence_sha256=_strategy_signal_evidence_sha256(signal_evidence),
        )

    def _insert_prepared_strategy_signal(
        self,
        connection: sqlite3.Connection,
        prepared: _PreparedStrategySignal,
    ) -> int:
        batch = connection.execute(
            """
            SELECT state, recorded_count, first_signal_id,
                   last_signal_id, manifest_sha256
            FROM strategy_signal_batches WHERE scan_id = ?
            """,
            (prepared.scan_id,),
        ).fetchone()
        if (
            batch is None
            or batch[0] != "STAGING"
            or type(batch[1]) is not int
            or type(batch[4]) is not str
        ):
            raise RuntimeError("strategy signal batch is not staging")
        requires_ledger = bool(
            prepared.passed
            and prepared.strategy_id in _PASSED_STRUCTURE_LEDGER_STRATEGIES
            and prepared.structure_id is not None
        )
        if requires_ledger and prepared.strategy_id == "N16":
            _validate_n16_passed_lifecycle(
                connection,
                prepared.symbol,
                prepared.structure_id,
                prepared.detail_json,
            )
            if (
                self.n16_claim_ledger.committed_claim_sha256(
                    prepared.structure_id
                )
                is not None
            ):
                raise RuntimeError("N16 structure is permanently consumed")
        if requires_ledger:
            existing = connection.execute(
                """
                SELECT symbol, source_signal_id, source_scan_id, claim_state
                FROM strategy_passed_structure_ledger
                WHERE strategy_id = ? AND structure_id = ?
                """,
                (prepared.strategy_id, prepared.structure_id),
            ).fetchone()
            if existing is not None:
                if (
                    len(existing) != 4
                    or type(existing[0]) is not str
                    or type(existing[1]) is not int
                    or existing[1] <= 0
                    or existing[3] not in {"STAGED", "ACTIVE"}
                    or (
                        existing[2] is not None
                        and (
                            type(existing[2]) is not int
                            or existing[2] <= 0
                        )
                    )
                    or (
                        existing[3] == "STAGED"
                        and existing[2] is None
                    )
                ):
                    raise RuntimeError("strategy structure claim is invalid")
                if existing[0] != prepared.symbol:
                    raise RuntimeError("strategy structure symbol conflict")
                if (
                    existing[3] != "STAGED"
                    or existing[2] != prepared.scan_id
                ):
                    raise RuntimeError("strategy structure already consumed")
                _strategy_signal_claim_snapshot(
                    connection, prepared.scan_id, "STAGED"
                )
                existing_signal = connection.execute(
                    """
                    SELECT id, scan_id, strategy_id, symbol, funding_rate,
                           matched_patterns, trend_slope, current_bullish,
                           passed, decision, reason, structure_id, detail_json,
                           created_at
                    FROM strategy_signals WHERE id = ? AND scan_id = ?
                    """,
                    (existing[1], prepared.scan_id),
                ).fetchone()
                expected_payload = (
                    prepared.scan_id,
                    prepared.strategy_id,
                    prepared.symbol,
                    prepared.funding_rate,
                    prepared.matched_patterns_json,
                    prepared.trend_slope,
                    int(prepared.current_bullish),
                    1,
                    prepared.decision,
                    prepared.reason,
                    prepared.structure_id,
                    prepared.detail_json,
                )
                if (
                    existing_signal is None
                    or len(existing_signal) != 14
                    or existing_signal[0] != existing[1]
                    or tuple(existing_signal[1:13]) != expected_payload
                    or type(existing_signal[13]) is not str
                ):
                    raise RuntimeError(
                        "strategy staged structure retry conflicts"
                    )
                return existing[1]
        cursor = connection.execute(
            """
            INSERT INTO strategy_signals (
                scan_id, strategy_id, symbol, funding_rate, matched_patterns,
                trend_slope, current_bullish, passed, decision, reason,
                structure_id, detail_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                prepared.scan_id,
                prepared.strategy_id,
                prepared.symbol,
                prepared.funding_rate,
                prepared.matched_patterns_json,
                prepared.trend_slope,
                int(prepared.current_bullish),
                int(prepared.passed),
                prepared.decision,
                prepared.reason,
                prepared.structure_id,
                prepared.detail_json,
                prepared.created_at,
            ),
        )
        signal_id = int(cursor.lastrowid)
        if prepared.micro_evidence_json is not None:
            evidence = prepared.detail_payload["evidence"]
            connection.execute(
                """
                INSERT INTO micro_strategy_lifecycle (
                    strategy_id,symbol,structure_id,kline_open_time_ms,
                    confirmation_observed_at_ms,deadline_ms,
                    source_signal_id,source_scan_id,claim_state,
                    evidence_json,evidence_sha256,created_at
                ) VALUES (?,?,?,?,?,?,?,?,'STAGED',?,?,?)
                """,
                (
                    prepared.strategy_id,
                    prepared.symbol,
                    prepared.structure_id,
                    evidence["kline_open_time_ms"],
                    evidence["confirmation_observed_at_ms"],
                    evidence["deadline_ms"],
                    signal_id,
                    prepared.scan_id,
                    prepared.micro_evidence_json,
                    prepared.micro_evidence_sha256,
                    prepared.created_at,
                ),
            )
        if prepared.passed:
            connection.execute(
                """
                INSERT INTO strategy_passed_signal_audits (
                    source_signal_id, source_scan_id, strategy_id, symbol,
                    funding_rate, matched_patterns, trend_slope,
                    current_bullish, passed, decision, reason, structure_id,
                    detail_json, signal_created_at, evidence_sha256, created_at,
                    claim_state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 'STAGED')
                """,
                (
                    signal_id,
                    prepared.scan_id,
                    prepared.strategy_id,
                    prepared.symbol,
                    prepared.funding_rate,
                    prepared.matched_patterns_json,
                    prepared.trend_slope,
                    int(prepared.current_bullish),
                    prepared.decision,
                    prepared.reason,
                    prepared.structure_id,
                    prepared.detail_json,
                    prepared.created_at,
                    prepared.evidence_sha256,
                    prepared.created_at,
                ),
            )
            if requires_ledger:
                connection.execute(
                    """
                    INSERT INTO strategy_passed_structure_ledger (
                        strategy_id, symbol, structure_id, source_signal_id,
                        source_scan_id, source_signal_created_at,
                        evidence_sha256, created_at, claim_state
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'STAGED')
                    """,
                    (
                        prepared.strategy_id,
                        prepared.symbol,
                        prepared.structure_id,
                        signal_id,
                        prepared.scan_id,
                        prepared.created_at,
                        prepared.evidence_sha256,
                        prepared.created_at,
                    ),
                )
        manifest_sha256 = hashlib.sha256(
            f"{batch[4]}:{prepared.evidence_sha256}".encode("ascii")
        ).hexdigest()
        update = connection.execute(
            """
            UPDATE strategy_signal_batches
            SET recorded_count = recorded_count + 1,
                first_signal_id = COALESCE(first_signal_id, ?),
                last_signal_id = ?, manifest_sha256 = ?, updated_at = ?
            WHERE scan_id = ? AND state = 'STAGING' AND recorded_count = ?
            """,
            (
                signal_id,
                signal_id,
                manifest_sha256,
                prepared.created_at,
                prepared.scan_id,
                batch[1],
            ),
        )
        if update.rowcount != 1:
            raise RuntimeError("strategy signal batch update conflict")
        return signal_id

    def record_strategy_signal(
        self,
        scan_id: int | None,
        strategy_id: str,
        symbol: str,
        funding_rate: str,
        matched_patterns: list[str] | tuple[str, ...],
        trend_slope: str,
        current_bullish: bool,
        passed: bool,
        decision: str,
        reason: str,
        structure_id: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> int | None:
        try:
            prepared = self._prepare_strategy_signal(
                scan_id,
                strategy_id,
                symbol,
                funding_rate,
                matched_patterns,
                trend_slope,
                current_bullish,
                passed,
                decision,
                reason,
                structure_id,
                detail,
            )
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                return self._insert_prepared_strategy_signal(
                    connection, prepared
                )
        except Exception as exc:
            self.logger.warning("Review DB write failed while recording strategy signal: %s", exc)
            return None

    def record_strategy_signals(
        self,
        scan_id: int | None,
        records: tuple[dict[str, Any], ...],
    ) -> StrategySignalBatchWriteResult:
        """Write one scheduler round's ordinary signal graph in one transaction."""

        required_keys = {
            "strategy_id",
            "symbol",
            "funding_rate",
            "matched_patterns",
            "trend_slope",
            "current_bullish",
            "passed",
            "decision",
            "reason",
            "structure_id",
            "detail",
        }
        prepared_records: list[_PreparedStrategySignal] = []
        failed_index: int | None = None
        try:
            if type(records) is not tuple or not records:
                raise ValueError(
                    "strategy signal batch records must be a non-empty tuple"
                )
            for index, record in enumerate(records):
                failed_index = index
                if type(record) is not dict or set(record) != required_keys:
                    raise ValueError(
                        "strategy signal batch record shape is invalid"
                    )
                prepared_records.append(
                    self._prepare_strategy_signal(
                        scan_id=scan_id,
                        **record,
                    )
                )
            failed_index = None
            signal_ids: list[int] = []
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                for index, prepared in enumerate(prepared_records):
                    failed_index = index
                    signal_ids.append(
                        self._insert_prepared_strategy_signal(
                            connection, prepared
                        )
                    )
                failed_index = None
            return StrategySignalBatchWriteResult(tuple(signal_ids))
        except Exception as exc:
            self.logger.warning(
                "Review DB write failed while recording strategy signal batch "
                "at index %s: %s",
                failed_index,
                exc,
            )
            return StrategySignalBatchWriteResult(
                failed_index=failed_index
            )

    @staticmethod
    def _prepare_micro_passed_analysis(
        source_scan_id: int,
        analysis: Any,
    ) -> tuple[Any, ...]:
        from .micro_analyzer import MicroAnalysisResult, MICRO_STRATEGY_IDS

        if (
            type(source_scan_id) is not int
            or source_scan_id <= 0
            or not isinstance(analysis, MicroAnalysisResult)
            or analysis.strategy_id not in MICRO_STRATEGY_IDS
            or not analysis.passed
            or analysis.structure is None
            or analysis.evidence is None
            or analysis.structure.strategy_id != analysis.strategy_id
            or analysis.structure.symbol != analysis.symbol
        ):
            raise ValueError("micro passed analysis identity is invalid")
        evidence_json = json.dumps(
            analysis.evidence,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        if len(evidence_json.encode("utf-8")) > 8_192:
            raise ValueError("micro passed analysis evidence exceeds 8 KiB")
        digest = hashlib.sha256(evidence_json.encode("utf-8")).hexdigest()
        structure = analysis.structure
        evidence = analysis.evidence
        observations = evidence.get("observations")
        if (
            type(evidence.get("schema_version")) is not int
            or evidence.get("schema_version") != 1
            or type(evidence.get("rule_version")) is not str
            or type(evidence.get("strategy_id")) is not str
            or evidence.get("strategy_id") != analysis.strategy_id
            or evidence.get("symbol") != analysis.symbol
            or evidence.get("structure_id") != structure.structure_id
            or type(evidence.get("kline_open_time_ms")) is not int
            or evidence.get("kline_open_time_ms")
            != structure.kline_open_time_ms
            or type(evidence.get("confirmation_observed_at_ms")) is not int
            or evidence.get("confirmation_observed_at_ms")
            != structure.confirmation_observed_at_ms
            or type(evidence.get("deadline_ms")) is not int
            or evidence.get("deadline_ms") != structure.deadline_ms
            or not isinstance(observations, list)
            or not observations
            or any(
                type(item) is not list
                or len(item) != 22
                or type(item[0]) is not str
                or any(
                    type(item[index]) is not int
                    for index in (1, 2, 3, 4, 5, 11, 17)
                )
                or not 1 <= item[17] <= 100
                for item in observations
            )
            or not isinstance(observations[-1], list)
            or len(observations[-1]) < 2
            or type(observations[-1][1]) is not int
            or observations[-1][1] != source_scan_id
        ):
            raise ValueError("micro passed analysis evidence identity is invalid")
        return (
            source_scan_id,
            analysis.strategy_id,
            analysis.symbol,
            structure.structure_id,
            structure.kline_open_time_ms,
            structure.confirmation_observed_at_ms,
            structure.deadline_ms,
            evidence_json,
            digest,
        )

    def record_micro_passed_analyses(
        self,
        source_scan_id: int,
        analyses: Iterable[Any],
    ) -> bool:
        """Persist one round's intrinsic micro PASSED evidence atomically."""

        try:
            prepared = tuple(
                self._prepare_micro_passed_analysis(source_scan_id, analysis)
                for analysis in analyses
            )
            identities = tuple(row[1:4] for row in prepared)
            if len(identities) != len(set(identities)):
                raise ValueError("micro passed analysis batch has duplicates")
            if not prepared:
                return True
            created_at = utc_now()
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                if connection.execute(
                    "SELECT state FROM strategy_signal_batches WHERE scan_id=?",
                    (source_scan_id,),
                ).fetchone() != ("STAGING",):
                    raise RuntimeError(
                        "micro passed analysis source batch is not STAGING"
                    )
                for row in prepared:
                    existing = connection.execute(
                        "SELECT source_scan_id,symbol,kline_open_time_ms,"
                        "confirmation_observed_at_ms,deadline_ms,evidence_json,"
                        "evidence_sha256 FROM micro_passed_analyses "
                        "WHERE source_scan_id=? AND strategy_id=? AND symbol=? "
                        "AND structure_id=?",
                        row[:4],
                    ).fetchone()
                    expected = (row[0], row[2], *row[4:])
                    if existing is not None:
                        if existing != expected:
                            raise RuntimeError(
                                "micro passed analysis evidence conflicts"
                            )
                        continue
                    connection.execute(
                        "INSERT INTO micro_passed_analyses ("
                        "source_scan_id,strategy_id,symbol,structure_id,"
                        "kline_open_time_ms,confirmation_observed_at_ms,"
                        "deadline_ms,evidence_json,evidence_sha256,created_at"
                        ") VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (*row, created_at),
                    )
            return True
        except Exception as exc:
            self.logger.warning(
                "Micro passed analysis batch persistence failed: %s", exc
            )
            return False

    def record_micro_passed_analysis(self, source_scan_id: int, analysis: Any) -> bool:
        """Backward-compatible one-item permanent micro evidence boundary."""

        return self.record_micro_passed_analyses(source_scan_id, (analysis,))

    def inspect_passed_structure(
        self,
        strategy_id: str,
        symbol: str,
        structure_id: str,
    ) -> str:
        _strict_signal_text(strategy_id, "strategy_id", 32)
        _strict_signal_text(symbol, "symbol", 64)
        _strict_signal_text(structure_id, "structure_id", 256)
        with self._connect() as connection:
            ledger = connection.execute(
                """
                SELECT id, strategy_id, symbol, structure_id, source_signal_id,
                       source_scan_id, source_signal_created_at,
                       evidence_sha256, created_at, claim_state
                FROM strategy_passed_structure_ledger
                WHERE strategy_id = ? AND structure_id = ?
                """,
                (strategy_id, structure_id),
            ).fetchone()
            retention = _strategy_signal_retention_row(connection)
            audit = None
            staging_batch = None
            ordinary = None
            n16_lifecycle_valid: bool | None = None
            if (
                ledger is not None
                and len(ledger) == 10
                and type(ledger[4]) is int
                and ledger[4] > 0
            ):
                audit = connection.execute(
                    """
                    SELECT source_signal_id, source_scan_id, strategy_id, symbol,
                           funding_rate, matched_patterns, trend_slope,
                           current_bullish, passed, decision, reason, structure_id,
                           detail_json, signal_created_at, evidence_sha256,
                           created_at, claim_state
                    FROM strategy_passed_signal_audits
                    WHERE source_signal_id = ?
                    """,
                    (ledger[4],),
                ).fetchone()
                if (
                    strategy_id == "N16"
                    and audit is not None
                    and len(audit) == 17
                    and type(audit[12]) is str
                ):
                    try:
                        _validate_n16_passed_lifecycle(
                            connection,
                            symbol,
                            structure_id,
                            audit[12],
                        )
                        if ledger[9] == "ACTIVE":
                            _validate_n16_consumption_seal(
                                connection, symbol, structure_id
                            )
                            self._attest_n16_committed_structure_claim(
                                connection,
                                symbol,
                                structure_id,
                                source_signal_id=ledger[4],
                            )
                        elif _n16_consumption_seal_row(
                            connection, structure_id
                        ) is not None:
                            raise RuntimeError(
                                "N16 staged claim has a consumption seal"
                            )
                        n16_lifecycle_valid = True
                    except Exception:
                        n16_lifecycle_valid = False
                if ledger[9] == "STAGED":
                    staging_batch = connection.execute(
                        """
                        SELECT state FROM strategy_signal_batches
                        WHERE scan_id = ?
                        """,
                        (ledger[5],),
                    ).fetchone()
                    ordinary = connection.execute(
                        """
                        SELECT id, scan_id, strategy_id, symbol, funding_rate,
                               matched_patterns, trend_slope, current_bullish,
                               passed, decision, reason, structure_id,
                               detail_json, created_at
                        FROM strategy_signals
                        WHERE id = ? AND scan_id = ?
                        """,
                        (ledger[4], ledger[5]),
                    ).fetchone()
        if retention[1:3] != (1, "COMPLETE"):
            return "INCONSISTENT"
        if strategy_id == "N16" and n16_lifecycle_valid is not True:
            return "INCONSISTENT"
        if ledger is None:
            return "MISSING"
        if (
            len(ledger) != 10
            or type(ledger[0]) is not int
            or ledger[0] <= 0
            or type(ledger[1]) is not str
            or type(ledger[2]) is not str
            or type(ledger[3]) is not str
            or type(ledger[4]) is not int
            or ledger[4] <= 0
            or (
                ledger[5] is not None
                and (type(ledger[5]) is not int or ledger[5] <= 0)
            )
            or type(ledger[6]) is not str
            or type(ledger[7]) is not str
            or len(ledger[7]) != 64
            or any(
                character not in "0123456789abcdef"
                for character in ledger[7]
            )
            or type(ledger[8]) is not str
            or ledger[9] not in {"STAGED", "ACTIVE"}
            or ledger[1] != strategy_id
            or ledger[2] != symbol
            or ledger[3] != structure_id
        ):
            return "INCONSISTENT"
        if (
            audit is None
            or len(audit) != 17
            or type(audit[0]) is not int
            or audit[0] <= 0
            or (
                audit[1] is not None
                and (type(audit[1]) is not int or audit[1] <= 0)
            )
            or type(audit[2]) is not str
            or type(audit[3]) is not str
            or type(audit[4]) is not str
            or type(audit[5]) is not str
            or type(audit[6]) is not str
            or type(audit[7]) is not int
            or audit[7] not in (0, 1)
            or type(audit[8]) is not int
            or audit[8] != 1
            or type(audit[9]) is not str
            or type(audit[10]) is not str
            or type(audit[11]) is not str
            or type(audit[12]) is not str
            or type(audit[13]) is not str
            or type(audit[14]) is not str
            or len(audit[14]) != 64
            or any(
                character not in "0123456789abcdef"
                for character in audit[14]
            )
            or type(audit[15]) is not str
            or audit[16] not in {"STAGED", "ACTIVE"}
        ):
            return "INCONSISTENT"
        evidence_sha256 = _strategy_signal_evidence_sha256(
            {
                "scan_id": audit[1],
                "strategy_id": audit[2],
                "symbol": audit[3],
                "funding_rate": audit[4],
                "matched_patterns": audit[5],
                "trend_slope": audit[6],
                "current_bullish": audit[7],
                "passed": audit[8],
                "decision": audit[9],
                "reason": audit[10],
                "structure_id": audit[11],
                "detail_json": audit[12],
                "created_at": audit[13],
            }
        )
        if (
            audit[0] != ledger[4]
            or audit[1] != ledger[5]
            or audit[2] != ledger[1]
            or audit[3] != ledger[2]
            or audit[11] != ledger[3]
            or audit[13] != ledger[6]
            or audit[14] != ledger[7]
            or audit[14] != evidence_sha256
            or audit[16] != ledger[9]
        ):
            return "INCONSISTENT"
        if ledger[9] == "STAGED":
            if (
                staging_batch != ("STAGING",)
                or ordinary is None
                or len(ordinary) != 14
                or tuple(ordinary) + (evidence_sha256,) != tuple(audit[:15])
            ):
                return "INCONSISTENT"
        return "CONSUMED"

    def has_strategy_structure_signal(
        self,
        strategy_id: str,
        structure_id: str,
        symbol: str | None = None,
    ) -> bool:
        if symbol is None:
            _strict_signal_text(strategy_id, "strategy_id", 32)
            _strict_signal_text(structure_id, "structure_id", 256)
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT symbol FROM strategy_passed_structure_ledger
                    WHERE strategy_id = ? AND structure_id = ?
                    """,
                    (strategy_id, structure_id),
                ).fetchall()
            if len(rows) > 1 or (
                rows and (type(rows[0][0]) is not str or not rows[0][0])
            ):
                raise RuntimeError("strategy passed structure ledger is inconsistent")
            if not rows:
                return False
            symbol = rows[0][0]
        status = self.inspect_passed_structure(strategy_id, symbol, structure_id)
        if status == "INCONSISTENT":
            raise RuntimeError("strategy passed structure ledger is inconsistent")
        return status == "CONSUMED"

    def current_strategy_signal_scan_id(self) -> int | None:
        with self._connect() as connection:
            connection.execute("BEGIN")
            row = _strategy_signal_retention_row(connection)
            if row[1:3] == (1, "COMPLETE"):
                _validate_complete_strategy_signal_graph(connection, row)
        if row[1:3] != (1, "COMPLETE"):
            return None
        if row[0] is None:
            return None
        if type(row[0]) is not int or row[0] <= 0:
            raise RuntimeError("invalid current strategy signal scan")
        return row[0]

    def list_current_strategy_signals(self) -> list[tuple[Any, ...]]:
        with self._connect() as connection:
            connection.execute("BEGIN")
            retention = _strategy_signal_retention_row(connection)
            if retention[1:3] == (1, "COMPLETE"):
                _validate_complete_strategy_signal_graph(
                    connection, retention
                )
            if retention[0] is None and retention[1:3] == (1, "COMPLETE"):
                current_batches = connection.execute(
                    "SELECT COUNT(*) FROM strategy_signal_batches "
                    "WHERE state = 'CURRENT'"
                ).fetchone()
                if current_batches != (0,):
                    raise RuntimeError("current strategy signal pointer is missing")
                return []
            if (
                type(retention[0]) is not int
                or retention[0] <= 0
                or retention[1:3] != (1, "COMPLETE")
            ):
                raise RuntimeError("current strategy signal pointer is invalid")
            scan_id = retention[0]
            batch = connection.execute(
                """
                SELECT state, recorded_count, expected_count,
                       first_signal_id, last_signal_id, manifest_sha256,
                       completed_at
                FROM strategy_signal_batches WHERE scan_id = ?
                """,
                (scan_id,),
            ).fetchone()
            rows = connection.execute(
                """
                SELECT id, strategy_id, symbol, passed, decision, reason,
                       structure_id, detail_json, created_at
                FROM strategy_signals WHERE scan_id = ? ORDER BY id
                """,
                (scan_id,),
            ).fetchall()
            expected_count_value = (
                _strategy_signal_expected_count_value(batch[1])
                if batch is not None and type(batch[1]) is int
                else None
            )
            if (
                batch is None
                or batch[0] != "CURRENT"
                or type(batch[1]) is not int
                or batch[1] < 0
                or batch[2] != expected_count_value
                or (
                    batch[1] > 0
                    and (
                        type(batch[3]) is not int
                        or type(batch[4]) is not int
                    )
                )
                or (batch[1] == 0 and batch[3:5] != (None, None))
                or type(batch[5]) is not str
                or len(batch[5]) != 64
                or type(batch[6]) is not str
                or not batch[6]
                or len(rows) != batch[1]
                or (
                    batch[1] > 0
                    and (
                        type(rows[0][0]) is not int
                        or type(rows[-1][0]) is not int
                        or rows[0][0] != batch[3]
                        or rows[-1][0] != batch[4]
                    )
                )
                or _strategy_signal_published_snapshot(connection, scan_id)
                != (batch[1], batch[3], batch[4], batch[5])
            ):
                raise RuntimeError("current strategy signal batch is incomplete")
            return rows

    def publish_strategy_signal_batch(
        self,
        scan_id: int | None,
        expected_count: int,
        history_coverage_proposals: tuple[HistoryCoverageProposal, ...] = (),
    ) -> bool:
        if (
            type(scan_id) is not int
            or scan_id <= 0
            or type(expected_count) is not int
            or expected_count < 0
        ):
            return False
        prepared_n16 = None
        n16_base_summary = None
        n16_review_baseline = None
        n16_committed_summary = None
        family_generation_before_publish = None
        family_generation_after_publish = None
        family_review_schema_version = None
        family_catalog_sha256_value = None
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                retention = _strategy_signal_retention_row(connection)
                retention_baseline_row = connection.execute(
                    "SELECT current_scan_id, retention_active, "
                    "migration_state, migration_cutoff_signal_id, "
                    "source_signal_count, source_passed_count, "
                    "source_manifest_sha256, updated_at, retention_origin "
                    "FROM strategy_signal_current WHERE singleton_id=1"
                ).fetchone()
                if (
                    retention_baseline_row is None
                    or len(retention_baseline_row) != 9
                    or not _type_sensitive_row_equal(
                        (
                            retention_baseline_row[0],
                            retention_baseline_row[1],
                            retention_baseline_row[2],
                            retention_baseline_row[3],
                            retention_baseline_row[4],
                            retention_baseline_row[5],
                            retention_baseline_row[6],
                            retention_baseline_row[8],
                        ),
                        retention,
                    )
                ):
                    raise RuntimeError(
                        "strategy signal retention baseline is invalid"
                    )
                if retention[1:3] != (1, "COMPLETE"):
                    raise RuntimeError("strategy signal retention is not active")
                _validate_complete_strategy_signal_graph(connection, retention)
                unpublished_genesis = (
                    retention[0] is None and retention[7] == "GENESIS"
                )
                if retention[0] is None and not unpublished_genesis:
                    raise RuntimeError(
                        "strategy signal genesis marker is invalid"
                    )
                batch = connection.execute(
                    """
                    SELECT state, recorded_count, expected_count,
                           first_signal_id, last_signal_id, manifest_sha256,
                           completed_at
                    FROM strategy_signal_batches WHERE scan_id = ?
                    """,
                    (scan_id,),
                ).fetchone()
                if batch is None:
                    raise RuntimeError("strategy signal batch missing")
                if batch[0] == "CURRENT" and retention[0] == scan_id:
                    self._attest_n16_claim_ledger(connection)
                    from .coverage_epoch_schema import (
                        coverage_epoch_schema_status,
                        validate_coverage_epoch_graph,
                    )

                    if coverage_epoch_schema_status(
                        connection, validate_graph=False
                    ) not in {"CURRENT", "AUTHORIZED_LEGACY_V3"}:
                        raise RuntimeError(
                            "coverage epoch generation is not CURRENT"
                        )
                    # A CURRENT retry is an exceptional confirmation path,
                    # not the per-minute publication hot path.  Re-attest the
                    # complete permanent graph before reporting idempotent
                    # success so unrelated missing/interior evidence cannot
                    # be hidden by an affected-owner check.
                    validate_coverage_epoch_graph(connection)
                    from .coverage_family_seal import (
                        validate_family_seal_graph,
                    )

                    validate_family_seal_graph(connection)
                    self._attest_history_coverage_publication(
                        connection,
                        scan_id,
                        expected_count,
                        batch[5],
                        history_coverage_proposals,
                        published=True,
                    )
                    actual = _strategy_signal_published_snapshot(
                        connection, scan_id
                    )
                    passed_count = connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id = ? AND passed = 1",
                        (scan_id,),
                    ).fetchone()
                    active_audits = connection.execute(
                        """
                        SELECT COUNT(*) FROM strategy_passed_signal_audits
                        WHERE source_scan_id = ? AND claim_state = 'ACTIVE'
                        """,
                        (scan_id,),
                    ).fetchone()
                    staged_audits = connection.execute(
                        """
                        SELECT COUNT(*) FROM strategy_passed_signal_audits
                        WHERE source_scan_id = ? AND claim_state = 'STAGED'
                        """,
                        (scan_id,),
                    ).fetchone()
                    staged_ledgers = connection.execute(
                        """
                        SELECT COUNT(*) FROM strategy_passed_structure_ledger
                        WHERE source_scan_id = ? AND claim_state = 'STAGED'
                        """,
                        (scan_id,),
                    ).fetchone()
                    return bool(
                        batch[1] == expected_count
                        and batch[2]
                        == _strategy_signal_expected_count_value(expected_count)
                        and actual
                        == (expected_count, batch[3], batch[4], batch[5])
                        and type(batch[5]) is str
                        and len(batch[5]) == 64
                        and type(batch[6]) is str
                        and bool(batch[6])
                        and passed_count == active_audits
                        and staged_audits == (0,)
                        and staged_ledgers == (0,)
                    )
                if (
                    batch[0] != "STAGING"
                    or batch[1] != expected_count
                    or batch[2] is not None
                    or (
                        expected_count > 0
                        and (
                            type(batch[3]) is not int
                            or type(batch[4]) is not int
                            or batch[3] <= 0
                            or batch[4] < batch[3]
                        )
                    )
                    or (
                        expected_count == 0
                        and batch[3:5] != (None, None)
                    )
                    or type(batch[5]) is not str
                    or len(batch[5]) != 64
                    or batch[6] is not None
                ):
                    raise RuntimeError("strategy signal batch is incomplete")
                actual = _strategy_signal_batch_snapshot(connection, scan_id)
                if actual != (batch[1], batch[3], batch[4], batch[5]):
                    raise RuntimeError("strategy signal staging evidence mismatch")
                passed_claim_count, ledger_claim_count = (
                    _strategy_signal_claim_snapshot(connection, scan_id, "STAGED")
                )
                n16_base_summary = self._attest_n16_claim_ledger(connection)
                family_generation_before_publish = (
                    self._runtime_family_graph_generation
                )
                from .coverage_epoch_schema import (
                    coverage_epoch_schema_status,
                )

                coverage_generation = coverage_epoch_schema_status(
                    connection, validate_graph=False
                )
                if coverage_generation not in {
                    "CURRENT",
                    "AUTHORIZED_LEGACY_V3",
                }:
                    raise RuntimeError(
                        "coverage epoch generation is not CURRENT"
                    )
                from .coverage_family_seal import (
                    family_seal_schema_status,
                )

                if family_seal_schema_status(
                    connection, validate_graph=False
                ) != "CURRENT":
                    raise RuntimeError(
                        "N19 family seal generation is not CURRENT"
                    )
                coverage_keys = self._attest_history_coverage_publication(
                    connection,
                    scan_id,
                    expected_count,
                    batch[5],
                    history_coverage_proposals,
                    published=False,
                )
                self._coverage_epoch_mutation_scope = (
                    "PUBLISH",
                    scan_id,
                    expected_count,
                    batch[5],
                    coverage_keys,
                )
                try:
                    self._apply_history_coverage_proposals(
                        connection,
                        scan_id,
                        expected_count,
                        batch[5],
                        history_coverage_proposals,
                    )
                finally:
                    self._coverage_epoch_mutation_scope = None
                n16_claims = _n16_staged_review_claims(
                    connection,
                    scan_id,
                    n16_base_summary.confirmed_claim_count,
                )
                batch_baseline_row = connection.execute(
                    "SELECT scan_id,state,recorded_count,expected_count,"
                    "first_signal_id,last_signal_id,manifest_sha256,"
                    "completed_at,created_at,updated_at "
                    "FROM strategy_signal_batches WHERE scan_id=?",
                    (scan_id,),
                ).fetchone()
                if (
                    batch_baseline_row is None
                    or len(batch_baseline_row) != 10
                    or batch_baseline_row[0] != scan_id
                    or not _type_sensitive_row_equal(
                        batch_baseline_row[1:8], batch
                    )
                ):
                    raise RuntimeError(
                        "strategy signal batch baseline is invalid"
                    )
                n16_review_baseline = _N16PublishReviewBaseline(
                    retention_row=tuple(retention_baseline_row),
                    batch_row=tuple(batch_baseline_row),
                    batch_snapshot=tuple(actual),
                    claim_snapshot=(passed_claim_count, ledger_claim_count),
                    n16_claims=n16_claims,
                    review_summary=n16_base_summary,
                    publication_baseline=_n16_review_publication_baseline(
                        connection,
                        scan_id,
                        tuple(retention_baseline_row),
                        tuple(batch_baseline_row),
                        tuple(actual),
                        (passed_claim_count, ledger_claim_count),
                        n16_claims,
                    ),
                )
                self._before_round_ledger_mutation()
                prepared_n16 = self.n16_claim_ledger.prepare_publication(
                    n16_base_summary,
                    n16_review_baseline.publication_baseline,
                    scan_id,
                    batch[5],
                    n16_claims,
                    utc_now(),
                )
                self._after_round_ledger_mutation()
                activated_audits = connection.execute(
                    """
                    UPDATE strategy_passed_signal_audits
                    SET claim_state = 'ACTIVE'
                    WHERE source_scan_id = ? AND claim_state = 'STAGED'
                    """,
                    (scan_id,),
                )
                if activated_audits.rowcount != passed_claim_count:
                    raise RuntimeError(
                        "strategy passed audit claim activation mismatch"
                    )
                activated_ledgers = connection.execute(
                    """
                    UPDATE strategy_passed_structure_ledger
                    SET claim_state = 'ACTIVE'
                    WHERE source_scan_id = ? AND claim_state = 'STAGED'
                      AND strategy_id != 'N16'
                    """,
                    (scan_id,),
                )
                if (
                    activated_ledgers.rowcount
                    != ledger_claim_count - len(n16_claims)
                ):
                    raise RuntimeError(
                        "strategy passed ledger claim activation mismatch"
                    )
                expected_micro_claims = connection.execute(
                    "SELECT COUNT(*) FROM micro_strategy_lifecycle "
                    "WHERE source_scan_id=? AND claim_state='STAGED'",
                    (scan_id,),
                ).fetchone()[0]
                activated_micro = connection.execute(
                    "UPDATE micro_strategy_lifecycle SET claim_state='ACTIVE' "
                    "WHERE source_scan_id=? AND claim_state='STAGED'",
                    (scan_id,),
                )
                if activated_micro.rowcount != expected_micro_claims:
                    raise RuntimeError(
                        "micro lifecycle claim activation mismatch"
                    )
                # N16 seal ordinals are part of the permanent cross-database
                # chain identity.  Activate them in the exact order used by
                # the prepared external ledger; never depend on SQLite's
                # unspecified trigger order for a multi-row UPDATE.
                for expected_claim in n16_claims:
                    activated_n16 = connection.execute(
                        "UPDATE strategy_passed_structure_ledger "
                        "SET claim_state='ACTIVE' "
                        "WHERE id=? AND source_signal_id=? "
                        "AND source_scan_id=? AND strategy_id='N16' "
                        "AND structure_id=? AND claim_state='STAGED'",
                        (
                            expected_claim[7],
                            expected_claim[1],
                            expected_claim[5],
                            expected_claim[11],
                        ),
                    )
                    if activated_n16.rowcount != 1:
                        raise RuntimeError(
                            "N16 passed ledger activation order conflicts"
                        )
                _strategy_signal_claim_snapshot(connection, scan_id, "ACTIVE")
                for expected_claim in n16_claims:
                    if (
                        _n16_review_claim_row(
                            connection, expected_claim[0]
                        )
                        != expected_claim
                    ):
                        raise RuntimeError(
                            "N16 Review claim differs from prepared publication"
                        )
                old_scan_id = retention[0]
                if old_scan_id is not None:
                    if type(old_scan_id) is not int or old_scan_id <= 0:
                        raise RuntimeError("current strategy signal pointer invalid")
                    old_batch = connection.execute(
                        """
                        SELECT state, recorded_count, expected_count,
                               first_signal_id, last_signal_id,
                               manifest_sha256, completed_at
                        FROM strategy_signal_batches WHERE scan_id = ?
                        """,
                        (old_scan_id,),
                    ).fetchone()
                    old_expected_count = (
                        _strategy_signal_expected_count_value(old_batch[1])
                        if old_batch is not None
                        and type(old_batch[1]) is int
                        else None
                    )
                    if (
                        old_batch is None
                        or old_batch[0] != "CURRENT"
                        or type(old_batch[1]) is not int
                        or old_batch[1] < 0
                        or old_batch[2] != old_expected_count
                        or (
                            old_batch[1] > 0
                            and (
                                type(old_batch[3]) is not int
                                or type(old_batch[4]) is not int
                            )
                        )
                        or (
                            old_batch[1] == 0
                            and old_batch[3:5] != (None, None)
                        )
                        or type(old_batch[5]) is not str
                        or len(old_batch[5]) != 64
                        or type(old_batch[6]) is not str
                        or not old_batch[6]
                    ):
                        raise RuntimeError("current strategy signal batch invalid")
                    old_actual = _strategy_signal_published_snapshot(
                        connection, old_scan_id
                    )
                    if old_actual != (
                        old_batch[1],
                        old_batch[3],
                        old_batch[4],
                        old_batch[5],
                    ):
                        raise RuntimeError(
                            "current strategy signal batch identity mismatch"
                        )
                    if old_batch[1] > 0:
                        deleted = connection.execute(
                            """
                            DELETE FROM strategy_signals
                            WHERE scan_id = ? AND id BETWEEN ? AND ?
                            """,
                            (old_scan_id, old_batch[3], old_batch[4]),
                        )
                        if deleted.rowcount != old_batch[1]:
                            raise RuntimeError(
                                "current strategy signal cleanup mismatch"
                            )
                    removed_batch = connection.execute(
                        "DELETE FROM strategy_signal_batches WHERE scan_id = ?",
                        (old_scan_id,),
                    )
                    if removed_batch.rowcount != 1:
                        raise RuntimeError("current strategy signal batch cleanup failed")
                now = utc_now()
                promoted = connection.execute(
                    """
                    UPDATE strategy_signal_batches
                    SET state = 'CURRENT', expected_count = ?, completed_at = ?,
                        updated_at = ?
                    WHERE scan_id = ? AND state = 'STAGING'
                    """,
                    (
                        _strategy_signal_expected_count_value(expected_count),
                        now,
                        now,
                        scan_id,
                    ),
                )
                if promoted.rowcount != 1:
                    raise RuntimeError("strategy signal batch promotion failed")
                pointer = connection.execute(
                    """
                    UPDATE strategy_signal_current
                    SET current_scan_id = ?, updated_at = ?
                    WHERE singleton_id = 1 AND current_scan_id IS ?
                      AND retention_active = ? AND migration_state = ?
                      AND migration_cutoff_signal_id IS ?
                      AND source_signal_count IS ?
                      AND source_passed_count IS ?
                      AND source_manifest_sha256 IS ?
                      AND retention_origin IS ?
                    """,
                    (scan_id, now) + retention,
                )
                if pointer.rowcount != 1:
                    raise RuntimeError("strategy signal current pointer update failed")
                final_pointer = _strategy_signal_retention_row(connection)
                final_batches = connection.execute(
                    """
                    SELECT scan_id, state, recorded_count, expected_count,
                           first_signal_id, last_signal_id, manifest_sha256,
                           completed_at
                    FROM strategy_signal_batches
                    WHERE state IN ('CURRENT', 'STAGING')
                    ORDER BY scan_id
                    """
                ).fetchall()
                final_actual = _strategy_signal_batch_snapshot(
                    connection, scan_id
                )
                if (
                    final_pointer != (scan_id,) + retention[1:]
                    or final_batches
                    != [
                        (
                            scan_id,
                            "CURRENT",
                            expected_count,
                            _strategy_signal_expected_count_value(
                                expected_count
                            ),
                            batch[3],
                            batch[4],
                            batch[5],
                            now,
                        )
                    ]
                    or final_actual
                    != (expected_count, batch[3], batch[4], batch[5])
                ):
                    raise RuntimeError(
                        "strategy signal final publication identity mismatch"
                    )
                _strategy_signal_claim_snapshot(
                    connection, scan_id, "ACTIVE"
                )
                _validate_complete_strategy_signal_graph(
                    connection, final_pointer
                )
                self._attest_history_coverage_publication(
                    connection,
                    scan_id,
                    expected_count,
                    batch[5],
                    history_coverage_proposals,
                    published=True,
                )
                generation_row = connection.execute(
                    "SELECT generation "
                    "FROM history_coverage_protected_generation "
                    "WHERE singleton_id=1"
                ).fetchone()
                if (
                    type(generation_row) not in (tuple, list)
                    or len(generation_row) != 1
                    or type(generation_row[0]) is not int
                    or generation_row[0] < 0
                ):
                    raise RuntimeError(
                        "N19 protected generation is invalid after publication"
                    )
                family_generation_after_publish = generation_row[0]
                family_schema_row = connection.execute(
                    "PRAGMA schema_version"
                ).fetchone()
                if (
                    family_schema_row is None
                    or len(family_schema_row) != 1
                    or type(family_schema_row[0]) is not int
                    or family_schema_row[0] <= 0
                ):
                    raise RuntimeError(
                        "N19 protected Review schema version is invalid"
                    )
                family_review_schema_version = family_schema_row[0]
                from .coverage_family_seal import (
                    family_seal_catalog_sha256,
                )

                family_catalog_sha256_value = (
                    family_seal_catalog_sha256(connection)
                )
                n16_committed_summary = _n16_review_claim_summary(connection)
                if (
                    family_generation_after_publish
                    > family_generation_before_publish
                ):
                    self._protected_generation_pair_after_commit = (
                        family_generation_before_publish,
                        family_generation_after_publish,
                        family_review_schema_version,
                        family_catalog_sha256_value,
                    )
                if prepared_n16 is not None:
                    self._n16_publication_pair_after_commit = (
                        prepared_n16,
                        n16_committed_summary,
                    )
            if prepared_n16 is not None:
                if n16_committed_summary is None:
                    raise RuntimeError(
                        "N16 committed Review summary is unavailable"
                    )
                self.n16_claim_ledger.commit_prepared(
                    prepared_n16,
                    n16_committed_summary,
                    utc_now(),
                )
            if family_generation_after_publish is not None:
                if (
                    family_generation_before_publish is None
                    or family_review_schema_version is None
                    or family_catalog_sha256_value is None
                    or family_generation_after_publish
                    < family_generation_before_publish
                ):
                    raise RuntimeError(
                        "N19 protected publication generation is inconsistent"
                    )
                if (
                    family_generation_after_publish
                    > family_generation_before_publish
                    and self._runtime_family_graph_generation
                    != family_generation_after_publish
                ):
                    self.n16_claim_ledger.advance_protected_generation_highwater(
                        expected_generation=family_generation_before_publish,
                        generation=family_generation_after_publish,
                        review_schema_version=family_review_schema_version,
                        family_catalog_sha256=family_catalog_sha256_value,
                        now=utc_now(),
                    )
                # The affected-owner proof and Review commit precede this
                # independent ledger advance.  A failed/ambiguous ledger
                # advance is never accepted as a trusted runtime generation.
                self._runtime_family_graph_generation = (
                    family_generation_after_publish
                )
                self._runtime_family_graph_active_generation = (
                    family_generation_after_publish
                )
            return True
        except Exception as exc:
            if (
                prepared_n16 is not None
                and n16_base_summary is not None
                and n16_review_baseline is not None
            ):
                try:
                    safely_aborted = self._abort_prepared_n16_if_review_unchanged(
                        prepared_n16
                    )
                    if not safely_aborted:
                        self.logger.error(
                            "N16 independent claim preparation remains "
                            "unresolved after an ambiguous Review result"
                        )
                except Exception as cleanup_exc:
                    self.logger.error(
                        "N16 independent claim preparation remains unresolved: %s",
                        cleanup_exc,
                    )
            self.logger.warning(
                "Review DB write failed while publishing strategy signal batch: %s",
                exc,
            )
            return False

    def get_strategy_structure_terminal_state(
        self,
        strategy_id: str,
        structure_id: str,
    ) -> StrategyStructureTerminalState | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id, strategy_id, symbol, structure_id, status, reason,
                       detail_json, created_at, updated_at
                FROM strategy_structure_terminal_states
                WHERE strategy_id = ? AND structure_id = ?
                """,
                (strategy_id, structure_id),
            ).fetchone()
        return StrategyStructureTerminalState(*row) if row is not None else None

    def record_strategy_structure_terminal(
        self,
        strategy_id: str,
        symbol: str,
        structure_id: str,
        status: str,
        reason: str,
        detail: dict[str, Any],
    ) -> bool:
        if status not in {"MISSED", "INVALID", "TRADED", "CONSUMED"}:
            raise ValueError(f"unsupported terminal structure status: {status}")
        now = utc_now()
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO strategy_structure_terminal_states (
                        strategy_id, symbol, structure_id, status, reason,
                        detail_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        strategy_id,
                        symbol,
                        structure_id,
                        status,
                        reason,
                        json_dumps(detail),
                        now,
                        now,
                    ),
                )
            return True
        except Exception as exc:
            self.logger.warning(
                "Review DB write failed while recording terminal structure: %s",
                exc,
            )
            return False

    def get_active_n08_structure_states(
        self,
        strategy_id: str,
        symbol: str,
    ) -> list[N08StructureState]:
        with self._read_only_runtime_snapshot() as connection:
            rows = connection.execute(
                """
                SELECT id, strategy_id, symbol, structure_id, range_start_time,
                       range_end_time, upper_reference, lower_reference,
                       upper_tolerance_boundary, lower_tolerance_boundary,
                       first_streak_start_time, first_streak_end_time, status,
                       reason, reset_open_time, detail_json, created_at, updated_at
                FROM n08_structure_states
                WHERE strategy_id = ? AND symbol = ? AND status = 'CONSUMED'
                ORDER BY id ASC
                """,
                (strategy_id, symbol),
            ).fetchall()
        return [N08StructureState(*row) for row in rows]

    def get_current_n08_structure_states(
        self,
        strategy_id: str,
        symbol: str,
    ) -> list[N08StructureState]:
        """Read N08 rows after a same-round write on the retained RW FD.

        The round's ordinary read snapshot is deliberately immutable.  This
        narrowly scoped read-after-write path is used only after an N08
        historical state INSERT has committed and therefore performs the full
        short-transaction ledger/FD attestation before exposing that new row.
        """

        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, strategy_id, symbol, structure_id, range_start_time,
                       range_end_time, upper_reference, lower_reference,
                       upper_tolerance_boundary, lower_tolerance_boundary,
                       first_streak_start_time, first_streak_end_time, status,
                       reason, reset_open_time, detail_json, created_at, updated_at
                FROM n08_structure_states
                WHERE strategy_id = ? AND symbol = ? AND status = 'CONSUMED'
                ORDER BY id ASC
                """,
                (strategy_id, symbol),
            ).fetchall()
        return [N08StructureState(*row) for row in rows]

    def has_n08_structure_state(self, strategy_id: str, structure_id: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM n08_structure_states
                WHERE strategy_id = ? AND structure_id = ?
                LIMIT 1
                """,
                (strategy_id, structure_id),
            ).fetchone()
        return row is not None

    def get_n08_history_coverage(
        self,
        strategy_id: str,
        symbol: str,
    ) -> N08HistoryCoverage | None:
        with self._read_only_runtime_snapshot() as connection:
            row = connection.execute(
                """
                SELECT strategy_id, symbol, continuous_from_open_time,
                       continuous_until_open_time, last_response_first_open_time,
                       last_response_last_open_time, last_gap_from_open_time,
                       last_gap_to_open_time, updated_at
                FROM n08_history_coverage
                WHERE strategy_id = ? AND symbol = ?
                """,
                (strategy_id, symbol),
            ).fetchone()
        return N08HistoryCoverage(*row) if row is not None else None

    def upsert_n08_history_coverage(
        self,
        coverage: N08HistoryCoverage,
    ) -> bool:
        return self.upsert_n08_history_coverages((coverage,))

    def upsert_n08_history_coverages(
        self,
        coverages: Iterable[N08HistoryCoverage],
    ) -> bool:
        """Persist one round's N08 coverage watermarks in one transaction."""

        try:
            rows = tuple(coverages)
            identities = {
                (coverage.strategy_id, coverage.symbol)
                for coverage in rows
            }
            if len(identities) != len(rows):
                raise ValueError("duplicate N08 coverage identity")
            if not rows:
                return True
            with self._connect() as connection:
                connection.executemany(
                    """
                    INSERT INTO n08_history_coverage (
                        strategy_id, symbol, continuous_from_open_time,
                        continuous_until_open_time, last_response_first_open_time,
                        last_response_last_open_time, last_gap_from_open_time,
                        last_gap_to_open_time, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(strategy_id, symbol) DO UPDATE SET
                        continuous_from_open_time = excluded.continuous_from_open_time,
                        continuous_until_open_time = excluded.continuous_until_open_time,
                        last_response_first_open_time = excluded.last_response_first_open_time,
                        last_response_last_open_time = excluded.last_response_last_open_time,
                        last_gap_from_open_time = excluded.last_gap_from_open_time,
                        last_gap_to_open_time = excluded.last_gap_to_open_time,
                        updated_at = excluded.updated_at
                    """,
                    tuple(
                        (
                            coverage.strategy_id,
                            coverage.symbol,
                            coverage.continuous_from_open_time,
                            coverage.continuous_until_open_time,
                            coverage.last_response_first_open_time,
                            coverage.last_response_last_open_time,
                            coverage.last_gap_from_open_time,
                            coverage.last_gap_to_open_time,
                            coverage.updated_at,
                        )
                        for coverage in rows
                    ),
                )
            return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while updating N08 coverage: %s", exc)
            return False

    def record_n08_structure_consumed(
        self,
        strategy_id: str,
        symbol: str,
        structure_id: str,
        range_start_time: str,
        range_end_time: str,
        upper_reference: str,
        lower_reference: str,
        upper_tolerance_boundary: str,
        lower_tolerance_boundary: str,
        first_streak_start_time: str,
        first_streak_end_time: str,
        reason: str,
        detail: dict[str, Any],
        reset_open_time: str | None = None,
    ) -> bool:
        now = utc_now()
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO n08_structure_states (
                        strategy_id, symbol, structure_id, range_start_time,
                        range_end_time, upper_reference, lower_reference,
                        upper_tolerance_boundary, lower_tolerance_boundary,
                        first_streak_start_time, first_streak_end_time,
                        status, reason, reset_open_time, detail_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'CONSUMED', ?, ?, ?, ?, ?)
                    """,
                    (
                        strategy_id,
                        symbol,
                        structure_id,
                        range_start_time,
                        range_end_time,
                        upper_reference,
                        lower_reference,
                        upper_tolerance_boundary,
                        lower_tolerance_boundary,
                        first_streak_start_time,
                        first_streak_end_time,
                        reason,
                        reset_open_time,
                        json_dumps(detail),
                        now,
                        now,
                    ),
                )
            return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while consuming N08 structure: %s", exc)
            return False

    def get_n09_structure_state_for_s1(
        self,
        strategy_id: str,
        symbol: str,
        s1_time: str,
    ) -> N09StructureState | None:
        with self._read_only_runtime_snapshot() as connection:
            row = connection.execute(
                """
                SELECT id, strategy_id, symbol, structure_id, s1_time, s1_price,
                       l_time, l_price, first_touch_time, status, reason,
                       detail_json, created_at, updated_at
                FROM n09_structure_states
                WHERE strategy_id = ? AND symbol = ? AND s1_time = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (strategy_id, symbol, s1_time),
            ).fetchone()
        return N09StructureState(*row) if row is not None else None

    def record_n09_structure_consumed(
        self,
        strategy_id: str,
        symbol: str,
        structure_id: str,
        s1_time: str,
        s1_price: str,
        l_time: str,
        l_price: str,
        first_touch_time: str,
        reason: str,
        detail: dict[str, Any],
    ) -> bool:
        now = utc_now()
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO n09_structure_states (
                        strategy_id, symbol, structure_id, s1_time, s1_price,
                        l_time, l_price, first_touch_time, status, reason,
                        detail_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'CONSUMED', ?, ?, ?, ?)
                    """,
                    (
                        strategy_id,
                        symbol,
                        structure_id,
                        s1_time,
                        s1_price,
                        l_time,
                        l_price,
                        first_touch_time,
                        reason,
                        json_dumps(detail),
                        now,
                        now,
                    ),
                )
            return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while consuming N09 structure: %s", exc)
            return False

    def get_n10_structure_state(
        self,
        strategy_id: str,
        structure_id: str,
    ) -> N10StructureState | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id, strategy_id, symbol, structure_id,
                       support_start_time, support_end_time, support_price,
                       w_time, c_time, e_time, status, reason, detail_json,
                       created_at, updated_at
                FROM n10_structure_states
                WHERE strategy_id = ? AND structure_id = ?
                LIMIT 1
                """,
                (strategy_id, structure_id),
            ).fetchone()
        return N10StructureState(*row) if row is not None else None

    def record_n10_structure_consumed(
        self,
        strategy_id: str,
        symbol: str,
        structure_id: str,
        support_start_time: str,
        support_end_time: str,
        support_price: str,
        w_time: str,
        c_time: str,
        e_time: str,
        reason: str,
        detail: dict[str, Any],
    ) -> bool:
        now = utc_now()
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO n10_structure_states (
                        strategy_id, symbol, structure_id,
                        support_start_time, support_end_time, support_price,
                        w_time, c_time, e_time, status, reason, detail_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'CONSUMED', ?, ?, ?, ?)
                    """,
                    (
                        strategy_id,
                        symbol,
                        structure_id,
                        support_start_time,
                        support_end_time,
                        support_price,
                        w_time,
                        c_time,
                        e_time,
                        reason,
                        json_dumps(detail),
                        now,
                        now,
                    ),
                )
            return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while consuming N10 structure: %s", exc)
            return False

    def get_n12_stage_state(
        self,
        strategy_id: str,
        symbol: str,
        l_time: str,
        h_time: str,
    ) -> sqlite3.Row | None:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT id, strategy_id, symbol, l_time, h_time, status, reason,
                       detail_json, created_at, updated_at
                FROM n12_stage_states
                WHERE strategy_id = ? AND symbol = ? AND l_time = ? AND h_time = ?
                LIMIT 1
                """,
                (strategy_id, symbol, l_time, h_time),
            ).fetchone()

    def get_n12_rank_snapshot(
        self,
        strategy_id: str,
        current_open_time: str,
    ) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT payload_json
                FROM n12_rank_snapshots
                WHERE strategy_id = ? AND current_open_time = ?
                LIMIT 1
                """,
                (strategy_id, current_open_time),
            ).fetchone()
        if row is None:
            return None
        payload = json.loads(row[0])
        if not isinstance(payload, dict):
            raise ValueError("N12 rank snapshot payload must be an object")
        return payload

    def record_n12_rank_snapshot(
        self,
        strategy_id: str,
        current_open_time: str,
        payload: dict[str, Any],
    ) -> bool:
        now = utc_now()
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO n12_rank_snapshots (
                        strategy_id, current_open_time, payload_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        strategy_id,
                        current_open_time,
                        json_dumps(payload),
                        now,
                        now,
                    ),
                )
                connection.execute(
                    """
                    DELETE FROM n12_rank_snapshots
                    WHERE strategy_id = ? AND current_open_time != ?
                    """,
                    (strategy_id, current_open_time),
                )
            return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while freezing N12 rank: %s", exc)
            return False

    def get_n13_market_snapshot(self, strategy_id: str, current_open_time: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload_json FROM n13_market_snapshots WHERE strategy_id=? AND current_open_time=?",
                (strategy_id, current_open_time),
            ).fetchone()
        if row is None:
            return None
        payload = json.loads(row[0])
        if not isinstance(payload, dict):
            raise ValueError("N13 snapshot payload invalid")
        return payload

    def record_n13_market_snapshot(self, strategy_id: str, current_open_time: str, payload: dict[str, Any]) -> bool:
        now = utc_now()
        try:
            with self._connect() as connection:
                connection.execute(
                    "INSERT INTO n13_market_snapshots(strategy_id,current_open_time,payload_json,created_at,updated_at) VALUES(?,?,?,?,?)",
                    (strategy_id, current_open_time, json_dumps(payload), now, now),
                )
                connection.execute(
                    "DELETE FROM n13_market_snapshots WHERE strategy_id=? AND current_open_time!=?",
                    (strategy_id, current_open_time),
                )
            return True
        except Exception as exc:
            self.logger.warning("N13 snapshot write failed: %s", exc)
            return False

    def get_n13_rotation_state(self, strategy_id: str, symbol: str, t_time: str):
        with self._read_only_runtime_snapshot() as connection:
            return connection.execute(
                "SELECT id,strategy_id,symbol,t_time,structure_id,status,reason,"
                "detail_json,created_at,updated_at FROM n13_rotation_states "
                "WHERE strategy_id=? AND symbol=? AND t_time=?",
                (strategy_id, symbol, t_time),
            ).fetchone()

    def record_n13_rotation_state(self, strategy_id: str, symbol: str, t_time: str, structure_id: str | None, status: str, reason: str, detail: dict[str, Any]) -> bool:
        result = self.record_n13_rotation_states_atomically(
            [
                {
                    "strategy_id": strategy_id,
                    "symbol": symbol,
                    "t_time": t_time,
                    "structure_id": structure_id,
                    "status": status,
                    "reason": reason,
                    "detail": detail,
                }
            ]
        )
        return result == "OK"

    def record_n13_rotation_states_atomically(
        self,
        records: list[dict[str, Any]],
        *,
        existing_guards: list[tuple[Any, ...]] | None = None,
    ) -> str:
        """Persist one N13 evaluation as an all-or-nothing, exact-idempotent batch."""
        if existing_guards is None:
            guards: list[tuple[Any, ...]] = []
        elif type(existing_guards) is list:
            guards = existing_guards
        else:
            self.logger.warning(
                "N13 state conflict detected while validating atomic guards"
            )
            return "N13_STATE_INCONSISTENT"
        if not records and not guards:
            return "OK"
        now = utc_now()
        try:
            unique_records: list[dict[str, Any]] = []
            records_by_key: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
            signatures_by_key: dict[tuple[Any, Any, Any], tuple[Any, ...]] = {}
            for record in records:
                key = (
                    record["strategy_id"],
                    record["symbol"],
                    record["t_time"],
                )
                try:
                    detail_json = _n13_json_dumps(record["detail"])
                except (TypeError, ValueError):
                    raise _N13StateInconsistentError
                signature = (
                    record.get("structure_id"),
                    record["status"],
                    record["reason"],
                    detail_json,
                )
                if key in records_by_key:
                    if signatures_by_key[key] != signature:
                        raise _N13StateInconsistentError
                    if record.get("require_new"):
                        records_by_key[key]["require_new"] = True
                    continue
                normalized = dict(record)
                normalized["_detail_json"] = detail_json
                records_by_key[key] = normalized
                signatures_by_key[key] = signature
                unique_records.append(normalized)
            guards_by_key: dict[tuple[Any, Any, Any], tuple[Any, ...]] = {}
            for guard in guards:
                if not _n13_rotation_guard_is_valid(guard):
                    raise _N13StateInconsistentError
                key = (guard[1], guard[2], guard[3])
                if key in records_by_key:
                    raise _N13StateInconsistentError
                if key in guards_by_key:
                    if not _n13_rotation_guards_equal(
                        guards_by_key[key], guard
                    ):
                        raise _N13StateInconsistentError
                    continue
                guards_by_key[key] = guard
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                for guard in guards_by_key.values():
                    existing_guard = connection.execute(
                        "SELECT id,strategy_id,symbol,t_time,structure_id,"
                        "status,reason,detail_json,created_at,updated_at "
                        "FROM n13_rotation_states WHERE strategy_id=? "
                        "AND symbol=? AND t_time=?",
                        (guard[1], guard[2], guard[3]),
                    ).fetchone()
                    if not _n13_rotation_guards_equal(
                        existing_guard, guard
                    ):
                        raise _N13StateInconsistentError
                for record in unique_records:
                    strategy_id = record["strategy_id"]
                    symbol = record["symbol"]
                    t_time = record["t_time"]
                    structure_id = record.get("structure_id")
                    status = record["status"]
                    reason = record["reason"]
                    detail_json = record["_detail_json"]
                    existing = connection.execute(
                        """
                        SELECT structure_id, status, reason, detail_json
                        FROM n13_rotation_states
                        WHERE strategy_id=? AND symbol=? AND t_time=?
                        """,
                        (strategy_id, symbol, t_time),
                    ).fetchone()
                    if existing is not None:
                        try:
                            existing_detail_json = _n13_json_dumps(
                                _n13_strict_json_loads(existing[3])
                            )
                        except (TypeError, ValueError):
                            raise _N13StateInconsistentError
                        if (
                            tuple(existing[:3]) != (structure_id, status, reason)
                            or existing_detail_json != detail_json
                        ):
                            raise _N13StateInconsistentError
                        if record.get("require_new"):
                            raise _N13EpisodeAlreadyConsumedError
                        continue
                    connection.execute(
                        """
                        INSERT INTO n13_rotation_states (
                            strategy_id, symbol, t_time, structure_id,
                            status, reason, detail_json, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            strategy_id,
                            symbol,
                            t_time,
                            structure_id,
                            status,
                            reason,
                            detail_json,
                            now,
                            now,
                        ),
                    )
            return "OK"
        except _N13EpisodeAlreadyConsumedError:
            return "N13_EPISODE_CONSUMED"
        except _N13StateInconsistentError:
            self.logger.warning("N13 state conflict detected while writing atomic batch")
            return "N13_STATE_INCONSISTENT"
        except Exception as exc:
            self.logger.warning("N13 atomic state write failed: %s", exc)
            return "N13_STATE_PERSIST_FAILED"

    def get_n14_market_snapshot(
        self,
        strategy_id: str,
        s_time: str,
    ) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT payload_json FROM n14_market_snapshots
                WHERE strategy_id=? AND s_time=?
                """,
                (strategy_id, s_time),
            ).fetchone()
        if row is None:
            return None
        payload = json.loads(row[0])
        if not isinstance(payload, dict):
            raise ValueError("N14 snapshot payload invalid")
        return payload

    def get_n15_market_snapshot(
        self,
        strategy_id: str,
        e_time: str,
    ) -> dict[str, Any] | None:
        parsed_e_time = _canonical_n15_e_time(e_time)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload_json,payload_sha256 FROM n15_market_snapshots "
                "WHERE strategy_id=? AND e_time=?",
                (strategy_id, e_time),
            ).fetchone()
        if row is None:
            return None
        from .n15_terminal_schema import n15_snapshot_payload_sha256
        if row[1] != n15_snapshot_payload_sha256(row[0]):
            raise ValueError("N15 snapshot payload hash invalid")
        payload = json.loads(row[0])
        from .n15_snapshot import validate_n15_snapshot_envelope
        validate_n15_snapshot_envelope(payload, strategy_id, parsed_e_time)
        return payload

    def get_pending_n15_snapshot_symbols(
        self, strategy_id: str
    ) -> set[str]:
        symbols: set[str] = set()
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT e_time,payload_json,payload_sha256 "
                "FROM n15_market_snapshots "
                "WHERE strategy_id=?",
                (strategy_id,),
            ).fetchall()
        from .n15_terminal_schema import n15_snapshot_payload_sha256
        for e_time, payload_json, payload_sha256 in rows:
            if payload_sha256 != n15_snapshot_payload_sha256(payload_json):
                raise ValueError("N15_STATE_INCONSISTENT")
            parsed_e_time = _canonical_n15_e_time(str(e_time))
            payload = json.loads(payload_json)
            from .n15_snapshot import validate_n15_snapshot_envelope
            snapshot = validate_n15_snapshot_envelope(
                payload, strategy_id, parsed_e_time
            )
            symbols.update(snapshot.rows)
        return symbols

    def record_n15_market_snapshot(
        self,
        strategy_id: str,
        e_time: str,
        payload: dict[str, Any],
    ) -> str:
        now = utc_now()
        try:
            from .n15_snapshot import validate_n15_snapshot_envelope
            try:
                parsed_e_time = _canonical_n15_e_time(e_time)
                validate_n15_snapshot_envelope(
                    payload, strategy_id, parsed_e_time
                )
            except (TypeError, ValueError) as exc:
                raise _N15StateInconsistentError from exc
            payload_json = json_dumps(payload)
            from .n15_terminal_schema import n15_snapshot_payload_sha256
            payload_sha256 = n15_snapshot_payload_sha256(payload_json)
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT payload_json,payload_sha256 "
                    "FROM n15_market_snapshots "
                    "WHERE strategy_id=? AND e_time=?",
                    (strategy_id, e_time),
                ).fetchone()
                if existing is not None:
                    try:
                        existing_json = json_dumps(json.loads(existing[0]))
                    except (TypeError, ValueError) as exc:
                        raise _N15StateInconsistentError from exc
                    if (
                        existing_json != payload_json
                        or existing[1] != payload_sha256
                    ):
                        raise _N15StateInconsistentError
                    return "OK"
                connection.execute(
                    "INSERT INTO n15_market_snapshots("
                    "strategy_id,e_time,payload_json,created_at,updated_at,"
                    "payload_sha256) VALUES(?,?,?,?,?,?)",
                    (strategy_id, e_time, payload_json, now, now, payload_sha256),
                )
            return "OK"
        except _N15StateInconsistentError:
            return "N15_STATE_INCONSISTENT"
        except Exception as exc:
            self.logger.warning("N15 snapshot write failed: %s", exc)
            return "N15_STATE_PERSIST_FAILED"

    def upgrade_n15_market_snapshot(
        self,
        strategy_id: str,
        e_time: str,
        expected_legacy_payload: dict[str, Any],
        upgraded_payload: dict[str, Any],
    ) -> str:
        now = utc_now()
        try:
            from .n15_snapshot import (
                N15_SNAPSHOT_SCHEMA_VERSION,
                validate_n15_snapshot_upgrade_pair,
            )
            parsed_e_time = _canonical_n15_e_time(e_time)
            if (
                type(expected_legacy_payload) is not dict
                or expected_legacy_payload.get("schema_version") != 1
                or type(upgraded_payload) is not dict
                or upgraded_payload.get("schema_version")
                != N15_SNAPSHOT_SCHEMA_VERSION
            ):
                raise _N15StateInconsistentError
            try:
                validate_n15_snapshot_upgrade_pair(
                    expected_legacy_payload,
                    upgraded_payload,
                    strategy_id,
                    parsed_e_time,
                )
            except (TypeError, ValueError) as exc:
                raise _N15StateInconsistentError from exc
            expected_json = json_dumps(expected_legacy_payload)
            upgraded_json = json_dumps(upgraded_payload)
            from .n15_terminal_schema import n15_snapshot_payload_sha256
            expected_sha256 = n15_snapshot_payload_sha256(expected_json)
            upgraded_sha256 = n15_snapshot_payload_sha256(upgraded_json)
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT payload_json,payload_sha256 "
                    "FROM n15_market_snapshots "
                    "WHERE strategy_id=? AND e_time=?",
                    (strategy_id, e_time),
                ).fetchone()
                if existing is None:
                    raise _N15StateInconsistentError
                try:
                    existing_json = json_dumps(json.loads(existing[0]))
                except (TypeError, ValueError) as exc:
                    raise _N15StateInconsistentError from exc
                if (
                    existing_json != expected_json
                    or existing[1] != expected_sha256
                ):
                    raise _N15StateInconsistentError
                cursor = connection.execute(
                    "UPDATE n15_market_snapshots "
                    "SET payload_json=?,payload_sha256=?,updated_at=? "
                    "WHERE strategy_id=? AND e_time=?",
                    (upgraded_json, upgraded_sha256, now, strategy_id, e_time),
                )
                if cursor.rowcount != 1:
                    raise _N15StateInconsistentError
            return "OK"
        except _N15StateInconsistentError:
            return "N15_STATE_INCONSISTENT"
        except Exception as exc:
            self.logger.warning("N15 snapshot upgrade failed: %s", exc)
            return "N15_STATE_PERSIST_FAILED"

    def get_n15_entry_state(self, strategy_id: str, e_time: str):
        _canonical_n15_e_time(e_time)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT id,strategy_id,e_time,symbol,structure_id,status,reason,detail_json "
                "FROM n15_entry_states WHERE strategy_id=? AND e_time=?",
                (strategy_id, e_time),
            ).fetchone()
        if row is not None:
            from .n15_analyzer import validate_n15_state_envelope
            try:
                detail = json.loads(row[7])
                validate_n15_state_envelope(
                    row[1], row[2], row[3], row[4], row[5], row[6], detail
                )
            except (TypeError, ValueError) as exc:
                raise ValueError("N15_STATE_INCONSISTENT") from exc
        return row

    def list_n15_market_snapshots(self, strategy_id: str) -> list[tuple[str, dict[str, Any]]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT e_time,payload_json,payload_sha256 "
                "FROM n15_market_snapshots "
                "WHERE strategy_id=? ORDER BY CAST(e_time AS INTEGER)",
                (strategy_id,),
            ).fetchall()
        result = []
        from .n15_terminal_schema import n15_snapshot_payload_sha256
        for e_time, payload_json, payload_sha256 in rows:
            try:
                if payload_sha256 != n15_snapshot_payload_sha256(payload_json):
                    raise ValueError
                parsed_e_time = _canonical_n15_e_time(str(e_time))
                payload = json.loads(payload_json)
                from .n15_snapshot import validate_n15_snapshot_envelope
                validate_n15_snapshot_envelope(
                    payload, strategy_id, parsed_e_time
                )
            except (TypeError, ValueError) as exc:
                deleted = self.delete_n15_market_snapshot(
                    strategy_id, str(e_time)
                )
                self.record_event(
                    (
                        "n15_snapshot_quarantined"
                        if deleted else "n15_snapshot_quarantine_failed"
                    ),
                    {
                        "strategy_id": strategy_id,
                        "e_time": str(e_time),
                        "reason": "N15_SNAPSHOT_ENVELOPE_INVALID",
                        "retained": not deleted,
                    },
                )
                raise ValueError("N15 snapshot payload quarantined") from exc
            result.append((str(e_time), payload))
        return result

    def delete_n15_market_snapshot(
        self, strategy_id: str, e_time: str
    ) -> bool:
        try:
            with self._connect() as connection:
                connection.execute(
                    "DELETE FROM n15_market_snapshots "
                    "WHERE strategy_id=? AND e_time=?",
                    (strategy_id, e_time),
                )
            return True
        except Exception as exc:
            self.logger.warning("N15 snapshot quarantine failed: %s", exc)
            return False

    @staticmethod
    def _confirm_n15_snapshot_archive(
        connection: sqlite3.Connection,
        identities: tuple[tuple[str, str], ...],
    ) -> None:
        from .n15_terminal_schema import validate_n15_terminal_receipt

        for strategy_id, e_time in identities:
            validate_n15_terminal_receipt(connection, strategy_id, e_time)

    def delete_n15_market_snapshots_before(
        self, strategy_id: str, current_e_time: str
    ) -> bool:
        """Permanently close old N15 snapshots without losing their evidence.

        The terminal receipt INSERT is the only statement authorized to remove
        the active snapshot.  Its AFTER trigger performs that removal, so a
        caught statement error cannot leave either a receipt-only or a
        delete-only half state.  Winner entry state creation, receipt creation,
        graph confirmation, and active-row removal share this transaction.
        """

        try:
            parsed_current = _canonical_n15_e_time(current_e_time)
            if strategy_id != "N15":
                raise _N15StateInconsistentError
            from .n15_analyzer import (
                historical_n15_missed_detail,
                n15_structure_id,
                validate_n15_state_envelope,
            )
            from .n15_snapshot import validate_n15_snapshot_envelope
            from .n15_terminal_schema import (
                build_n15_terminal_receipt,
                n15_snapshot_payload_sha256,
            )

            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                expected_generation = self._runtime_family_graph_active_generation
                if type(expected_generation) is not int or expected_generation < 0:
                    raise _N15StateInconsistentError
                before = connection.execute(
                    "SELECT generation FROM history_coverage_protected_generation "
                    "WHERE singleton_id=1"
                ).fetchone()
                if (
                    before is None
                    or len(before) != 1
                    or type(before[0]) is not int
                    or before[0] != expected_generation
                    or self._protected_generation_pair_after_commit is not None
                ):
                    raise _N15StateInconsistentError
                rows = connection.execute(
                    "SELECT e_time,payload_json,payload_sha256 "
                    "FROM n15_market_snapshots WHERE strategy_id=? "
                    "AND CAST(e_time AS INTEGER) < ? "
                    "ORDER BY CAST(e_time AS INTEGER)",
                    (strategy_id, parsed_current),
                ).fetchall()
                now = utc_now()
                for e_time, payload_json, payload_sha256 in rows:
                    try:
                        parsed_e_time = _canonical_n15_e_time(e_time)
                        if (
                            type(payload_json) is not str
                            or payload_sha256
                            != n15_snapshot_payload_sha256(payload_json)
                        ):
                            raise ValueError
                        payload = json.loads(payload_json)
                        snapshot = validate_n15_snapshot_envelope(
                            payload, strategy_id, parsed_e_time
                        )
                    except (TypeError, ValueError) as exc:
                        raise _N15StateInconsistentError from exc
                    winner_symbol = snapshot.winner_symbol
                    entry_state = None
                    if winner_symbol is not None:
                        winner = snapshot.rows[winner_symbol]
                        structure_id = n15_structure_id(
                            strategy_id,
                            winner_symbol,
                            winner.b.open_time_ms,
                            winner.c.open_time_ms,
                        )
                        state = connection.execute(
                            "SELECT symbol,structure_id,status,reason,detail_json "
                            "FROM n15_entry_states "
                            "WHERE strategy_id=? AND e_time=?",
                            (strategy_id, e_time),
                        ).fetchone()
                        if state is None:
                            detail = historical_n15_missed_detail(
                                strategy_id,
                                parsed_e_time,
                                winner_symbol,
                                structure_id,
                                winner,
                            )
                            validate_n15_state_envelope(
                                strategy_id,
                                e_time,
                                winner_symbol,
                                structure_id,
                                "MISSED",
                                "HISTORICAL_N15_ENTRY_MISSED",
                                detail,
                            )
                            detail_json = json_dumps(detail)
                            inserted = connection.execute(
                                "INSERT INTO n15_entry_states("
                                "strategy_id,e_time,symbol,structure_id,status,"
                                "reason,detail_json,created_at,updated_at) "
                                "VALUES(?,?,?,?,?,?,?,?,?)",
                                (
                                    strategy_id,
                                    e_time,
                                    winner_symbol,
                                    structure_id,
                                    "MISSED",
                                    "HISTORICAL_N15_ENTRY_MISSED",
                                    detail_json,
                                    now,
                                    now,
                                ),
                            )
                            if inserted.rowcount != 1:
                                raise _N15StateInconsistentError
                            entry_state = (
                                winner_symbol,
                                structure_id,
                                "MISSED",
                                "HISTORICAL_N15_ENTRY_MISSED",
                                detail_json,
                            )
                        else:
                            entry_state = tuple(state)
                            try:
                                detail = json.loads(entry_state[4])
                                validate_n15_state_envelope(
                                    strategy_id,
                                    e_time,
                                    entry_state[0],
                                    entry_state[1],
                                    entry_state[2],
                                    entry_state[3],
                                    detail,
                                )
                            except (TypeError, ValueError) as exc:
                                raise _N15StateInconsistentError from exc
                            if (
                                entry_state[0] != winner_symbol
                                or entry_state[1] != structure_id
                            ):
                                raise _N15StateInconsistentError
                    elif connection.execute(
                        "SELECT 1 FROM n15_entry_states "
                        "WHERE strategy_id=? AND e_time=?",
                        (strategy_id, e_time),
                    ).fetchone() is not None:
                        raise _N15StateInconsistentError

                    receipt = build_n15_terminal_receipt(
                        strategy_id=strategy_id,
                        e_time=e_time,
                        winner_symbol=winner_symbol,
                        payload_json=payload_json,
                        entry_state=entry_state,
                        closed_at=now,
                    )
                    self._n15_snapshot_terminal_scope = (
                        winner_symbol or "NO_WINNER",
                        e_time,
                        receipt[14],
                    )
                    try:
                        inserted = connection.execute(
                            "INSERT INTO n15_snapshot_terminal_receipts "
                            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            receipt,
                        )
                    finally:
                        self._n15_snapshot_terminal_scope = None
                    if inserted.rowcount != 1:
                        raise _N15StateInconsistentError
                    if connection.execute(
                        "SELECT 1 FROM n15_market_snapshots "
                        "WHERE strategy_id=? AND e_time=?",
                        (strategy_id, e_time),
                    ).fetchone() is not None:
                        raise _N15StateInconsistentError

                self._confirm_n15_snapshot_archive(
                    connection,
                    tuple((strategy_id, str(row[0])) for row in rows),
                )
                after = connection.execute(
                    "SELECT generation FROM history_coverage_protected_generation "
                    "WHERE singleton_id=1"
                ).fetchone()
                if (
                    after is None
                    or len(after) != 1
                    or type(after[0]) is not int
                    or after[0] != expected_generation + len(rows)
                ):
                    raise _N15StateInconsistentError
                if rows:
                    from .coverage_family_seal import (
                        family_seal_catalog_sha256,
                    )

                    schema = connection.execute(
                        "PRAGMA schema_version"
                    ).fetchone()
                    if (
                        schema is None
                        or len(schema) != 1
                        or type(schema[0]) is not int
                        or schema[0] <= 0
                    ):
                        raise _N15StateInconsistentError
                    self._protected_generation_pair_after_commit = (
                        expected_generation,
                        after[0],
                        schema[0],
                        family_seal_catalog_sha256(connection),
                    )
            return True
        except _N15StateInconsistentError:
            self.logger.warning("N15 snapshot terminal archive is inconsistent")
            return False
        except Exception as exc:
            self.logger.warning("N15 snapshot terminal archive failed: %s", exc)
            return False

    def record_n15_entry_state(
        self,
        strategy_id: str,
        e_time: str,
        symbol: str,
        structure_id: str,
        status: str,
        reason: str,
        detail: dict[str, Any],
    ) -> str:
        now = utc_now()
        try:
            from .n15_analyzer import validate_n15_state_envelope
            try:
                _canonical_n15_e_time(e_time)
                validate_n15_state_envelope(
                    strategy_id, e_time, symbol, structure_id,
                    status, reason, detail,
                )
            except (TypeError, ValueError) as exc:
                raise _N15StateInconsistentError from exc
            detail_json = json_dumps(detail)
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT symbol,structure_id,status,reason,detail_json "
                    "FROM n15_entry_states WHERE strategy_id=? AND e_time=?",
                    (strategy_id, e_time),
                ).fetchone()
                expected = (symbol, structure_id, status, reason, detail_json)
                if existing is not None:
                    if tuple(existing) != expected:
                        raise _N15StateInconsistentError
                    return "EXISTS"
                connection.execute(
                    "INSERT INTO n15_entry_states("
                    "strategy_id,e_time,symbol,structure_id,status,reason,detail_json,"
                    "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        strategy_id, e_time, symbol, structure_id, status,
                        reason, detail_json, now, now,
                    ),
                )
            return "INSERTED"
        except _N15StateInconsistentError:
            return "N15_STATE_INCONSISTENT"
        except Exception as exc:
            self.logger.warning("N15 state write failed: %s", exc)
            return "N15_STATE_PERSIST_FAILED"

    def record_n14_market_snapshot(
        self,
        strategy_id: str,
        s_time: str,
        payload: dict[str, Any],
    ) -> str:
        now = utc_now()
        try:
            from .n14_snapshot import decode_n14_snapshot

            decoded = decode_n14_snapshot(payload, strategy_id)
            if str(decoded.s_open_time_ms) != str(s_time):
                raise _N14StateInconsistentError
            payload_json = json_dumps(payload)
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT payload_json FROM n14_market_snapshots
                    WHERE strategy_id=? AND s_time=?
                    """,
                    (strategy_id, s_time),
                ).fetchone()
                if existing is not None:
                    try:
                        existing_json = json_dumps(json.loads(existing[0]))
                    except (TypeError, ValueError):
                        return "N14_STATE_INCONSISTENT"
                    return (
                        "OK"
                        if existing_json == payload_json
                        else "N14_STATE_INCONSISTENT"
                    )
                connection.execute(
                    """
                    INSERT INTO n14_market_snapshots(
                        strategy_id,s_time,payload_json,created_at,updated_at
                    ) VALUES(?,?,?,?,?)
                    """,
                    (
                        strategy_id,
                        s_time,
                        payload_json,
                        now,
                        now,
                    ),
                )
            return "OK"
        except (TypeError, ValueError, _N14StateInconsistentError):
            self.logger.warning("N14 snapshot payload is inconsistent")
            return "N14_STATE_INCONSISTENT"
        except Exception as exc:
            self.logger.warning("N14 snapshot write failed: %s", exc)
            return "N14_STATE_PERSIST_FAILED"

    def list_n14_market_snapshots(self, strategy_id: str) -> list[tuple[str, str]]:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT s_time,payload_json FROM n14_market_snapshots
                WHERE strategy_id=? ORDER BY CAST(s_time AS INTEGER)
                """,
                (strategy_id,),
            ).fetchall()

    def get_active_n14_snapshot_symbols(
        self,
        strategy_id: str,
        current_open_time_ms: int,
    ) -> set[str]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT snapshot.s_time,snapshot.payload_json
                FROM n14_market_snapshots snapshot
                WHERE snapshot.strategy_id=? AND EXISTS (
                    SELECT 1 FROM n14_active_episodes active
                    WHERE active.strategy_id=snapshot.strategy_id
                      AND active.s_time=snapshot.s_time
                )
                """,
                (strategy_id,),
            ).fetchall()
        symbols: set[str] = set()
        from .n14_snapshot import INTERVAL_MS, decode_n14_snapshot

        for s_time_value, payload_json in rows:
            s_time = int(s_time_value)
            if (
                s_time >= current_open_time_ms
                or s_time < current_open_time_ms - 4 * INTERVAL_MS
            ):
                raise ValueError("N14 active snapshot lifecycle invalid")
            payload = json.loads(payload_json)
            snapshot = decode_n14_snapshot(payload, strategy_id)
            if snapshot.s_open_time_ms != s_time:
                raise ValueError("N14 active snapshot identity invalid")
            symbols.update(snapshot.rows)
        return symbols

    def get_required_n14_snapshot_symbols(
        self,
        strategy_id: str,
        current_open_time_ms: int,
    ) -> set[str]:
        """Return every frozen N14 member needed by the current batch.

        The scheduler always restores the exact latest S snapshot when it
        already exists, even before any symbol has an active episode.  It also
        restores every snapshot that still owns an active episode.  Market
        data collection must use that same union or an intra-candle top-100
        membership change can omit one immutable frozen member and make the
        whole N14 context incomplete.
        """

        from .n14_snapshot import INTERVAL_MS, decode_n14_snapshot

        if type(strategy_id) is not str or not strategy_id:
            raise ValueError("N14 strategy identity invalid")
        if (
            type(current_open_time_ms) is not int
            or current_open_time_ms <= INTERVAL_MS
            or current_open_time_ms % INTERVAL_MS != 0
        ):
            raise ValueError("N14 current candle identity invalid")

        latest_s_time = current_open_time_ms - INTERVAL_MS

        def canonical_s_time(value: object) -> int:
            if type(value) is not str or not value or not value.isdigit():
                raise ValueError("N14 snapshot time invalid")
            parsed = int(value)
            if (
                parsed <= 0
                or str(parsed) != value
                or parsed % INTERVAL_MS != 0
            ):
                raise ValueError("N14 snapshot time invalid")
            return parsed

        payloads: dict[int, str] = {}
        with self._connect() as connection:
            latest = connection.execute(
                """
                SELECT payload_json FROM n14_market_snapshots
                WHERE strategy_id=? AND s_time=?
                """,
                (strategy_id, str(latest_s_time)),
            ).fetchone()
            if latest is not None:
                if len(latest) != 1 or type(latest[0]) is not str:
                    raise ValueError("N14 latest snapshot payload invalid")
                payloads[latest_s_time] = latest[0]

            active_rows = connection.execute(
                """
                SELECT active.s_time,snapshot.payload_json
                FROM (
                    SELECT DISTINCT strategy_id,s_time
                    FROM n14_active_episodes
                    WHERE strategy_id=?
                ) active
                LEFT JOIN n14_market_snapshots snapshot
                  ON snapshot.strategy_id=active.strategy_id
                 AND snapshot.s_time=active.s_time
                ORDER BY CAST(active.s_time AS INTEGER)
                """,
                (strategy_id,),
            ).fetchall()

        for s_time_value, payload_json in active_rows:
            s_time = canonical_s_time(s_time_value)
            if (
                s_time >= current_open_time_ms
                or s_time < current_open_time_ms - 4 * INTERVAL_MS
            ):
                raise ValueError("N14 active snapshot lifecycle invalid")
            if type(payload_json) is not str:
                raise ValueError("N14 active snapshot missing")
            payloads[s_time] = payload_json

        symbols: set[str] = set()
        for s_time, payload_json in sorted(payloads.items()):
            try:
                payload = json.loads(payload_json)
            except (TypeError, ValueError) as exc:
                raise ValueError("N14 snapshot payload invalid") from exc
            if type(payload) is not dict:
                raise ValueError("N14 snapshot payload invalid")
            snapshot = decode_n14_snapshot(payload, strategy_id)
            if snapshot.s_open_time_ms != s_time:
                raise ValueError("N14 snapshot identity invalid")
            symbols.update(snapshot.rows)
        return symbols

    def list_n14_active_snapshot_times(self, strategy_id: str) -> set[str]:
        with self._connect() as connection:
            return {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT DISTINCT s_time FROM n14_active_episodes
                    WHERE strategy_id=?
                    """,
                    (strategy_id,),
                ).fetchall()
            }

    def expire_stale_n14_active_episodes(
        self,
        strategy_id: str,
        current_open_time_ms: int,
        maximum_lifecycle_bars: int = 4,
    ) -> str:
        cutoff = int(current_open_time_ms) - maximum_lifecycle_bars * 900_000
        now = utc_now()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                rows = connection.execute(
                    """
                    SELECT symbol,s_time,stage,detail_json
                    FROM n14_active_episodes
                    WHERE strategy_id=? AND CAST(s_time AS INTEGER)<?
                    ORDER BY CAST(s_time AS INTEGER),symbol
                    """,
                    (strategy_id, cutoff),
                ).fetchall()
                for symbol, s_time_value, stage, detail_json in rows:
                    s_time = str(s_time_value)
                    try:
                        active = _decode_n14_active_envelope(
                            json.loads(detail_json), strategy_id, symbol, s_time
                        )
                    except (TypeError, ValueError):
                        raise _N14StateInconsistentError
                    if active["stage"] != stage:
                        raise _N14StateInconsistentError
                    unsigned = {
                        "schema_version": 1,
                        "strategy_id": strategy_id,
                        "symbol": symbol,
                        "s_time": s_time,
                        "structure_id": active["evidence"].get("structure_id"),
                        "status": "MISSED",
                        "reason": "HISTORICAL_N14_ENTRY_MISSED",
                        "config_signature": active["config_signature"],
                        "evidence": active["evidence"],
                    }
                    terminal = {
                        **unsigned,
                        "canonical_sha256": _n14_canonical_hash(unsigned),
                    }
                    _decode_n14_terminal_envelope(
                        terminal, strategy_id, symbol, s_time
                    )
                    terminal_json = json_dumps(terminal)
                    existing = connection.execute(
                        """
                        SELECT structure_id,status,reason,detail_json
                        FROM n14_sell_impact_states
                        WHERE strategy_id=? AND symbol=? AND s_time=?
                        """,
                        (strategy_id, symbol, s_time),
                    ).fetchone()
                    expected = (
                        unsigned["structure_id"],
                        unsigned["status"],
                        unsigned["reason"],
                        terminal_json,
                    )
                    if existing is not None:
                        try:
                            existing_terminal = _decode_n14_terminal_envelope(
                                json.loads(existing[3]), strategy_id, symbol, s_time
                            )
                        except (TypeError, ValueError):
                            raise _N14StateInconsistentError
                        if (
                            existing[0],
                            existing[1],
                            existing[2],
                            json_dumps(existing_terminal),
                        ) != expected:
                            raise _N14StateInconsistentError
                    else:
                        connection.execute(
                            """
                            INSERT INTO n14_sell_impact_states(
                                strategy_id,symbol,s_time,structure_id,status,
                                reason,detail_json,created_at,updated_at
                            ) VALUES(?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                strategy_id,
                                symbol,
                                s_time,
                                unsigned["structure_id"],
                                unsigned["status"],
                                unsigned["reason"],
                                terminal_json,
                                now,
                                now,
                            ),
                        )
                    connection.execute(
                        """
                        DELETE FROM n14_active_episodes
                        WHERE strategy_id=? AND symbol=? AND s_time=?
                        """,
                        (strategy_id, symbol, s_time),
                    )
            return "OK"
        except _N14StateInconsistentError:
            self.logger.warning("N14 stale active state is inconsistent")
            return "N14_STATE_INCONSISTENT"
        except Exception as exc:
            self.logger.warning("N14 stale active cleanup failed: %s", exc)
            return "N14_STATE_PERSIST_FAILED"

    def delete_expired_n14_market_snapshots(
        self,
        strategy_id: str,
        minimum_s_time: str,
    ) -> bool:
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    DELETE FROM n14_market_snapshots
                    WHERE strategy_id=? AND CAST(s_time AS INTEGER)<?
                      AND NOT EXISTS (
                        SELECT 1 FROM n14_active_episodes active
                        WHERE active.strategy_id=n14_market_snapshots.strategy_id
                          AND active.s_time=n14_market_snapshots.s_time
                      )
                    """,
                    (strategy_id, int(minimum_s_time)),
                )
            return True
        except Exception as exc:
            self.logger.warning("N14 snapshot cleanup failed: %s", exc)
            return False

    def get_n14_active_episode(
        self,
        strategy_id: str,
        symbol: str,
        s_time: str,
    ):
        with self._read_only_runtime_snapshot() as connection:
            return connection.execute(
                """
                SELECT id,strategy_id,symbol,s_time,stage,detail_json
                FROM n14_active_episodes
                WHERE strategy_id=? AND symbol=? AND s_time=?
                """,
                (strategy_id, symbol, s_time),
            ).fetchone()

    def get_validated_n14_active_episode(
        self,
        strategy_id: str,
        symbol: str,
        s_time: str,
    ) -> dict[str, Any] | None:
        row = self.get_n14_active_episode(strategy_id, symbol, s_time)
        if row is None:
            return None
        try:
            payload = json.loads(row[5])
            envelope = _decode_n14_active_envelope(
                payload, strategy_id, symbol, s_time
            )
        except (TypeError, ValueError, _N14StateInconsistentError) as exc:
            raise ValueError("N14 active episode is inconsistent") from exc
        if row[4] != envelope["stage"]:
            raise ValueError("N14 active episode stage is inconsistent")
        return envelope

    def upsert_n14_active_episode(
        self,
        strategy_id: str,
        symbol: str,
        s_time: str,
        stage: str,
        detail: dict[str, Any],
    ) -> str:
        return self.record_n14_state_batch_atomically(
            [],
            [
                {
                    "strategy_id": strategy_id,
                    "symbol": symbol,
                    "s_time": s_time,
                    "stage": stage,
                    "detail": detail,
                }
            ],
        )

    def get_n14_sell_impact_state(
        self,
        strategy_id: str,
        symbol: str,
        s_time: str,
    ):
        with self._read_only_runtime_snapshot() as connection:
            return connection.execute(
                """
                SELECT id,strategy_id,symbol,s_time,structure_id,status,reason,detail_json
                FROM n14_sell_impact_states
                WHERE strategy_id=? AND symbol=? AND s_time=?
                """,
                (strategy_id, symbol, s_time),
            ).fetchone()

    def list_recent_n14_sell_impact_states(
        self,
        strategy_id: str,
        symbol: str,
        minimum_s_time: str,
    ) -> list[tuple[Any, ...]]:
        with self._read_only_runtime_snapshot() as connection:
            return connection.execute(
                """
                SELECT id,strategy_id,symbol,s_time,structure_id,status,reason,detail_json
                FROM n14_sell_impact_states
                WHERE strategy_id=? AND symbol=? AND CAST(s_time AS INTEGER)>=?
                ORDER BY CAST(s_time AS INTEGER) DESC
                """,
                (strategy_id, symbol, int(minimum_s_time)),
            ).fetchall()

    def record_n14_sell_impact_states_atomically(
        self,
        records: list[dict[str, Any]],
    ) -> str:
        return self.record_n14_state_batch_atomically(records, [])

    def record_n14_state_batch_atomically(
        self,
        terminal_records: list[dict[str, Any]],
        active_records: list[dict[str, Any]],
    ) -> str:
        if not terminal_records and not active_records:
            return "OK"
        now = utc_now()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                for record in terminal_records:
                    strategy_id = record["strategy_id"]
                    symbol = record["symbol"]
                    s_time = record["s_time"]
                    structure_id = record.get("structure_id")
                    status = record["status"]
                    reason = record["reason"]
                    detail_json = json_dumps(record["detail"])
                    terminal_envelope = _decode_n14_terminal_envelope(
                        record["detail"], strategy_id, symbol, s_time
                    )
                    if (
                        terminal_envelope["structure_id"] != structure_id
                        or terminal_envelope["status"] != status
                        or terminal_envelope["reason"] != reason
                    ):
                        raise _N14StateInconsistentError
                    active_row = connection.execute(
                        """
                        SELECT stage,detail_json FROM n14_active_episodes
                        WHERE strategy_id=? AND symbol=? AND s_time=?
                        """,
                        (strategy_id, symbol, s_time),
                    ).fetchone()
                    if active_row is not None:
                        try:
                            active_payload = json.loads(active_row[1])
                        except (TypeError, ValueError):
                            raise _N14StateInconsistentError
                        active_envelope = _decode_n14_active_envelope(
                            active_payload, strategy_id, symbol, s_time
                        )
                        if (
                            active_row[0] != active_envelope["stage"]
                            or not _n14_evidence_progresses(
                                active_envelope, terminal_envelope
                            )
                        ):
                            raise _N14StateInconsistentError
                    existing = connection.execute(
                        """
                        SELECT structure_id,status,reason,detail_json
                        FROM n14_sell_impact_states
                        WHERE strategy_id=? AND symbol=? AND s_time=?
                        """,
                        (strategy_id, symbol, s_time),
                    ).fetchone()
                    if existing is not None:
                        try:
                            existing_payload = json.loads(existing[3])
                            _decode_n14_terminal_envelope(
                                existing_payload, strategy_id, symbol, s_time
                            )
                            existing_detail_json = json_dumps(existing_payload)
                        except (TypeError, ValueError):
                            raise _N14StateInconsistentError
                        if (
                            tuple(existing[:3]) != (structure_id, status, reason)
                            or existing_detail_json != detail_json
                        ):
                            raise _N14StateInconsistentError
                        if record.get("require_new"):
                            raise _N14EpisodeAlreadyConsumedError
                        continue
                    connection.execute(
                        """
                        INSERT INTO n14_sell_impact_states(
                            strategy_id,symbol,s_time,structure_id,status,reason,
                            detail_json,created_at,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            strategy_id,
                            symbol,
                            s_time,
                            structure_id,
                            status,
                            reason,
                            detail_json,
                            now,
                            now,
                        ),
                    )
                    connection.execute(
                        """
                        DELETE FROM n14_active_episodes
                        WHERE strategy_id=? AND symbol=? AND s_time=?
                        """,
                        (strategy_id, symbol, s_time),
                    )
                for record in active_records:
                    strategy_id = record["strategy_id"]
                    symbol = record["symbol"]
                    s_time = record["s_time"]
                    stage = record["stage"]
                    if stage not in _N14_ACTIVE_STAGES:
                        raise ValueError("N14 active stage invalid")
                    if connection.execute(
                        """
                        SELECT 1 FROM n14_sell_impact_states
                        WHERE strategy_id=? AND symbol=? AND s_time=?
                        """,
                        (strategy_id, symbol, s_time),
                    ).fetchone() is not None:
                        raise _N14EpisodeAlreadyConsumedError
                    detail_json = json_dumps(record["detail"])
                    new_envelope = _decode_n14_active_envelope(
                        record["detail"], strategy_id, symbol, s_time
                    )
                    if new_envelope["stage"] != stage:
                        raise _N14StateInconsistentError
                    existing = connection.execute(
                        """
                        SELECT stage,detail_json FROM n14_active_episodes
                        WHERE strategy_id=? AND symbol=? AND s_time=?
                        """,
                        (strategy_id, symbol, s_time),
                    ).fetchone()
                    if existing is None:
                        connection.execute(
                            """
                            INSERT INTO n14_active_episodes(
                                strategy_id,symbol,s_time,stage,detail_json,
                                created_at,updated_at
                            ) VALUES(?,?,?,?,?,?,?)
                            """,
                            (
                                strategy_id,
                                symbol,
                                s_time,
                                stage,
                                detail_json,
                                now,
                                now,
                            ),
                        )
                        continue
                    old_stage, old_detail_json = existing
                    try:
                        old_payload = json.loads(old_detail_json)
                        old_envelope = _decode_n14_active_envelope(
                            old_payload, strategy_id, symbol, s_time
                        )
                        old_detail_json = json_dumps(old_payload)
                    except (TypeError, ValueError):
                        raise _N14StateInconsistentError
                    if old_stage == stage:
                        if old_detail_json == detail_json:
                            continue
                        if not _n14_evidence_progresses(old_envelope, new_envelope):
                            raise _N14StateInconsistentError
                    elif (
                        old_stage not in _N14_ACTIVE_STAGES
                        or _N14_ACTIVE_STAGES[old_stage] >= _N14_ACTIVE_STAGES[stage]
                        or not _n14_evidence_progresses(old_envelope, new_envelope)
                    ):
                        raise _N14StateInconsistentError
                    connection.execute(
                        """
                        UPDATE n14_active_episodes
                        SET stage=?,detail_json=?,updated_at=?
                        WHERE strategy_id=? AND symbol=? AND s_time=?
                        """,
                        (
                            stage,
                            detail_json,
                            now,
                            strategy_id,
                            symbol,
                            s_time,
                        ),
                    )
            return "OK"
        except _N14EpisodeAlreadyConsumedError:
            return "N14_EPISODE_CONSUMED"
        except _N14StateInconsistentError:
            self.logger.warning("N14 state conflict detected while writing atomic batch")
            return "N14_STATE_INCONSISTENT"
        except Exception as exc:
            self.logger.warning("N14 atomic state write failed: %s", exc)
            return "N14_STATE_PERSIST_FAILED"

    def record_n12_stage_terminal(
        self,
        strategy_id: str,
        symbol: str,
        l_time: str,
        h_time: str,
        status: str,
        reason: str,
        detail: dict[str, Any],
    ) -> bool:
        if status not in {"MISSED", "INVALID", "CONSUMED"}:
            raise ValueError(f"unsupported N12 stage status: {status}")
        now = utc_now()
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO n12_stage_states (
                        strategy_id, symbol, l_time, h_time, status, reason,
                        detail_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        strategy_id,
                        symbol,
                        l_time,
                        h_time,
                        status,
                        reason,
                        json_dumps(detail),
                        now,
                        now,
                    ),
                )
            return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while consuming N12 stage: %s", exc)
            return False

    def record_n12_stage_and_structure_terminal(
        self,
        strategy_id: str,
        symbol: str,
        l_time: str,
        h_time: str,
        structure_id: str,
        status: str,
        reason: str,
        detail: dict[str, Any],
    ) -> bool:
        if status not in {"MISSED", "INVALID", "CONSUMED"}:
            raise ValueError(f"unsupported N12 terminal status: {status}")
        now = utc_now()
        detail_json = json_dumps(detail)
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO n12_stage_states (
                        strategy_id, symbol, l_time, h_time, status, reason,
                        detail_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        strategy_id, symbol, l_time, h_time, status, reason,
                        detail_json, now, now,
                    ),
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO strategy_structure_terminal_states (
                        strategy_id, symbol, structure_id, status, reason,
                        detail_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        strategy_id, symbol, structure_id, status, reason,
                        detail_json, now, now,
                    ),
                )
            return True
        except Exception as exc:
            self.logger.warning(
                "Review DB write failed while atomically recording N12 stage/structure: %s",
                exc,
            )
            return False

    def reconcile_n12_stage_and_structure_terminal(
        self,
        strategy_id: str,
        symbol: str,
        l_time: str,
        h_time: str,
        structure_id: str,
    ) -> str:
        now = utc_now()
        try:
            with self._connect() as connection:
                stage = connection.execute(
                    """
                    SELECT status, reason, detail_json
                    FROM n12_stage_states
                    WHERE strategy_id = ? AND symbol = ? AND l_time = ? AND h_time = ?
                    LIMIT 1
                    """,
                    (strategy_id, symbol, l_time, h_time),
                ).fetchone()
                terminal = connection.execute(
                    """
                    SELECT status, reason, detail_json
                    FROM strategy_structure_terminal_states
                    WHERE strategy_id = ? AND structure_id = ?
                    LIMIT 1
                    """,
                    (strategy_id, structure_id),
                ).fetchone()
                if stage is None and terminal is None:
                    return "MISSING"
                if stage is not None and terminal is not None:
                    return "CONSISTENT" if tuple(stage) == tuple(terminal) else "INCONSISTENT"
                if stage is not None:
                    try:
                        stage_detail = json.loads(stage[2])
                    except (TypeError, ValueError):
                        return "INCONSISTENT"
                    if isinstance(stage_detail, dict) and "stage_event" in stage_detail:
                        return "STAGE_ONLY_TERMINAL"
                    stored_structure_id = None
                    if isinstance(stage_detail, dict):
                        stored_structure_id = stage_detail.get("structure_id")
                        if stored_structure_id is None:
                            structure_detail = stage_detail.get("structure")
                            if isinstance(structure_detail, dict):
                                stored_structure_id = structure_detail.get("structure_id")
                        if stored_structure_id is None:
                            historical = stage_detail.get("historical_backfill")
                            if isinstance(historical, dict):
                                structure_detail = historical.get("structure")
                                if isinstance(structure_detail, dict):
                                    stored_structure_id = structure_detail.get("structure_id")
                    if stored_structure_id != structure_id:
                        return "INCONSISTENT"
                    connection.execute(
                        """
                        INSERT INTO strategy_structure_terminal_states (
                            strategy_id, symbol, structure_id, status, reason,
                            detail_json, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            strategy_id, symbol, structure_id,
                            stage[0], stage[1], stage[2], now, now,
                        ),
                    )
                else:
                    connection.execute(
                        """
                        INSERT INTO n12_stage_states (
                            strategy_id, symbol, l_time, h_time, status, reason,
                            detail_json, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            strategy_id, symbol, l_time, h_time,
                            terminal[0], terminal[1], terminal[2], now, now,
                        ),
                    )
            return "REPAIRED"
        except Exception as exc:
            self.logger.warning(
                "Review DB write failed while reconciling N12 stage/structure: %s",
                exc,
            )
            return "FAILED"

    def retire_n08_structure_state(
        self,
        state_id: int,
        reset_open_time: str,
        reason: str,
    ) -> bool:
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE n08_structure_states
                    SET status = 'RETIRED', reset_open_time = ?, reason = ?, updated_at = ?
                    WHERE id = ? AND status = 'CONSUMED'
                    """,
                    (reset_open_time, reason, utc_now(), state_id),
                )
            return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while retiring N08 structure: %s", exc)
            return False

    def mark_n08_structure_reset(
        self,
        state_id: int,
        reset_open_time: str,
        reason: str,
    ) -> bool:
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE n08_structure_states
                    SET reset_open_time = COALESCE(reset_open_time, ?),
                        reason = ?, updated_at = ?
                    WHERE id = ? AND status = 'CONSUMED'
                    """,
                    (reset_open_time, reason, utc_now(), state_id),
                )
            return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while marking N08 reset: %s", exc)
            return False

    def update_strategy_signal(
        self,
        signal_id: int | None,
        decision: str | None = None,
        reason: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> bool:
        if signal_id is None:
            return False
        try:
            if type(signal_id) is not int or signal_id <= 0:
                raise ValueError("strategy signal id must be a positive built-in integer")
            if decision is not None:
                _strict_signal_text(decision, "decision", 128)
            if reason is not None:
                _strict_signal_text(reason, "reason", 512)
            if detail is not None and type(detail) is not dict:
                raise ValueError("strategy signal overlay detail must be a built-in dict")
            with self._connect() as connection:
                retention = _strategy_signal_retention_row(connection)
                if retention[1:3] != (1, "COMPLETE"):
                    raise RuntimeError(
                        "strategy signal overlay requires active retention"
                    )
                row = connection.execute(
                    """
                    SELECT signals.strategy_id, signals.decision,
                           signals.reason, signals.detail_json,
                           signals.passed, batches.state,
                           current.current_scan_id,
                           current.retention_active,
                           current.migration_state,
                           signals.scan_id
                    FROM strategy_signals AS signals
                    JOIN strategy_signal_batches AS batches
                      ON batches.scan_id = signals.scan_id
                    JOIN strategy_signal_current AS current
                      ON current.singleton_id = 1
                    WHERE signals.id = ?
                    """,
                    (signal_id,),
                ).fetchone()
                if row is None:
                    raise RuntimeError("strategy signal overlay row is missing")
                if (
                    len(row) != 10
                    or type(row[0]) is not str
                    or type(row[1]) is not str
                    or type(row[2]) is not str
                    or type(row[3]) is not str
                    or row[4] != 1
                    or row[5] != "CURRENT"
                    or type(row[6]) is not int
                    or row[6] <= 0
                    or row[7:9] != (1, "COMPLETE")
                    or row[6] != retention[0]
                    or type(row[9]) is not int
                    or row[9] != row[6]
                ):
                    raise RuntimeError(
                        "strategy signal overlay requires a published PASSED row"
                    )
                _strict_signal_text(row[1], "decision", 128)
                _strict_signal_text(row[2], "reason", 512)
                detail_payload = _load_strict_strategy_signal_detail_json(row[3])
                detail_payload.update(detail or {})
                if str(row[0]) == "N13":
                    detail_payload = _bounded_n13_reused_detail(
                        detail_payload
                    )
                detail_json = _strict_strategy_signal_detail_json(detail_payload)
                updated = connection.execute(
                    """
                    UPDATE strategy_signals
                    SET decision = ?, reason = ?, detail_json = ?
                    WHERE id = ?
                    """,
                    (
                        decision if decision is not None else row[1],
                        reason if reason is not None else row[2],
                        detail_json,
                        signal_id,
                    ),
                )
                if updated.rowcount != 1:
                    raise RuntimeError("strategy signal overlay update identity mismatch")
            return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while updating strategy signal: %s", exc)
            return False

    def get_open_strategy_paper_trade(self, strategy_id: str) -> StrategyPaperTrade | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id, strategy_id, symbol, opened_at, last_checked_at, closed_at,
                       entry_price, stop_loss_price, take_profit_price, result, exit_reason,
                       r_multiple, funding_rate, orders_json, detail_json
                FROM strategy_paper_trades
                WHERE strategy_id = ? AND result = 'OPEN'
                ORDER BY id DESC
                LIMIT 1
                """,
                (strategy_id,),
            ).fetchone()
        return self._paper_trade_from_row(row) if row else None

    def get_open_strategy_paper_trades(self) -> list[StrategyPaperTrade]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, strategy_id, symbol, opened_at, last_checked_at, closed_at,
                       entry_price, stop_loss_price, take_profit_price, result, exit_reason,
                       r_multiple, funding_rate, orders_json, detail_json
                FROM strategy_paper_trades
                WHERE result = 'OPEN'
                ORDER BY id ASC
                """
            ).fetchall()
        return [self._paper_trade_from_row(row) for row in rows]

    def strategy_activity_mode(self, strategy_id: str) -> str:
        with self._connect() as connection:
            live_open = connection.execute(
                """
                SELECT 1 FROM strategy_live_links
                WHERE strategy_id = ? AND closed_at IS NULL
                LIMIT 1
                """,
                (strategy_id,),
            ).fetchone()
            if live_open is not None:
                return "LIVE_OPEN"

            paper_open = connection.execute(
                """
                SELECT 1 FROM strategy_paper_trades
                WHERE strategy_id = ? AND result = 'OPEN'
                LIMIT 1
                """,
                (strategy_id,),
            ).fetchone()
            if paper_open is not None:
                return "PAPER_OPEN"
            pending = connection.execute(
                "SELECT live_result_pending FROM strategy_states WHERE strategy_id = ?",
                (strategy_id,),
            ).fetchone()
            if pending is not None and bool(pending[0]):
                return "LIVE_OPEN"
        return "IDLE"

    def _paper_trade_from_row(self, row: sqlite3.Row | tuple[Any, ...]) -> StrategyPaperTrade:
        return StrategyPaperTrade(
            id=int(row[0]),
            strategy_id=str(row[1]),
            symbol=str(row[2]),
            opened_at=str(row[3]),
            last_checked_at=str(row[4]) if row[4] is not None else None,
            closed_at=str(row[5]) if row[5] is not None else None,
            entry_price=str(row[6]),
            stop_loss_price=str(row[7]),
            take_profit_price=str(row[8]),
            result=str(row[9]),
            exit_reason=str(row[10]) if row[10] is not None else None,
            r_multiple=str(row[11]) if row[11] is not None else None,
            funding_rate=str(row[12]),
            orders_json=str(row[13]),
            detail_json=str(row[14]),
        )

    def open_strategy_paper_trade(
        self,
        strategy_id: str,
        symbol: str,
        entry_price: str,
        stop_loss_price: str,
        take_profit_price: str,
        funding_rate: str,
        orders: dict[str, Any],
        detail: dict[str, Any],
        opened_at: str | None = None,
    ) -> int | None:
        detail_payload = (
            _bounded_n13_reused_detail(detail)
            if strategy_id == "N13"
            else detail
        )
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT id FROM strategy_paper_trades
                    WHERE strategy_id = ? AND result = 'OPEN'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (strategy_id,),
                ).fetchone()
                if existing is not None:
                    return None
                self._ensure_strategy_state(connection, strategy_id)
                cursor = connection.execute(
                    """
                    INSERT INTO strategy_paper_trades (
                        strategy_id, symbol, opened_at, entry_price, stop_loss_price,
                        take_profit_price, result, funding_rate, orders_json, detail_json
                    ) VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?)
                    """,
                    (
                        strategy_id,
                        symbol,
                        opened_at or utc_now(),
                        entry_price,
                        stop_loss_price,
                        take_profit_price,
                        funding_rate,
                        json_dumps(orders),
                        json_dumps(detail_payload),
                    ),
                )
                trade_id = int(cursor.lastrowid)
                if trade_id <= 0:
                    raise RuntimeError("paper trade insert identity is invalid")
                return trade_id
        except Exception as exc:
            self.logger.warning("Review DB write failed while opening strategy paper trade: %s", exc)
            raise PaperTradePersistenceError(
                "strategy paper open could not be confirmed"
            ) from exc

    def update_strategy_paper_last_checked(
        self,
        trade_id: int,
        checked_at: str,
    ) -> bool:
        try:
            with self._connect() as connection:
                updated = connection.execute(
                    """
                    UPDATE strategy_paper_trades
                    SET last_checked_at = ?
                    WHERE id = ? AND result = 'OPEN'
                    """,
                    (checked_at, trade_id),
                )
                if updated.rowcount != 1:
                    raise RuntimeError(
                        "paper checkpoint requires one OPEN trade"
                    )
            return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while updating paper checkpoint: %s", exc)
            return False

    def close_strategy_paper_trade(
        self,
        trade_id: int,
        result: str,
        exit_reason: str,
        exit_price: str,
        r_multiple: str,
        detail: dict[str, Any] | None = None,
    ) -> bool:
        if result not in {"WIN", "LOSS"}:
            raise ValueError("strategy paper trade result must be WIN or LOSS")
        closed_at = utc_now()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    """
                    SELECT strategy_id, detail_json, result
                    FROM strategy_paper_trades WHERE id = ?
                    """,
                    (trade_id,),
                ).fetchone()
                if row is None or row[2] != "OPEN":
                    return False
                strategy_id = str(row[0])
                try:
                    detail_payload = json.loads(row[1] or "{}")
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        "paper trade detail JSON is invalid"
                    ) from exc
                if type(detail_payload) is not dict:
                    raise RuntimeError("paper trade detail is not an object")
                detail_payload["close"] = detail or {}
                detail_payload["exit_price"] = exit_price
                if strategy_id == "N13":
                    detail_payload = _bounded_n13_reused_detail(
                        detail_payload
                    )
                updated = connection.execute(
                    """
                    UPDATE strategy_paper_trades
                    SET closed_at = ?, result = ?, exit_reason = ?, r_multiple = ?,
                        detail_json = ?
                    WHERE id = ? AND result = 'OPEN'
                    """,
                    (
                        closed_at,
                        result,
                        exit_reason,
                        r_multiple,
                        json_dumps(detail_payload),
                        trade_id,
                    ),
                )
                if updated.rowcount != 1:
                    raise RuntimeError(
                        "paper close requires one OPEN trade"
                    )
                self._apply_strategy_trade_result(connection, strategy_id, result, closed_at, paper_trade=True)
            return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while closing strategy paper trade: %s", exc)
            return False

    def void_strategy_paper_trade(
        self,
        trade_id: int,
        strategy_id: str,
        symbol: str,
        confirmation: str,
    ) -> bool:
        """Void one explicitly identified legacy paper trade without changing statistics."""
        if confirmation != PAPER_TRADE_VOID_CONFIRMATION:
            raise ValueError("explicit paper trade void confirmation is required")
        normalized_symbol = symbol.upper()
        closed_at = utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT strategy_id, symbol, result, exit_reason, detail_json
                FROM strategy_paper_trades
                WHERE id = ?
                """,
                (trade_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"paper trade {trade_id} does not exist")
            actual_strategy, actual_symbol, result, exit_reason, detail_json = row
            if str(actual_strategy) != strategy_id or str(actual_symbol).upper() != normalized_symbol:
                raise ValueError("paper trade strategy or symbol does not match")
            if result == "VOID" and exit_reason == PAPER_TRADE_VOID_REASON:
                return False
            if result != "OPEN":
                raise ValueError(f"paper trade {trade_id} is not OPEN")
            try:
                detail = json.loads(detail_json or "{}")
            except json.JSONDecodeError:
                detail = {}
            detail["void"] = {
                "reason": PAPER_TRADE_VOID_REASON,
                "voided_at": closed_at,
                "statistics_changed": False,
            }
            if strategy_id == "N13":
                detail = _bounded_n13_reused_detail(detail)
            connection.execute(
                """
                UPDATE strategy_paper_trades
                SET closed_at = ?, result = 'VOID', exit_reason = ?,
                    r_multiple = NULL, detail_json = ?
                WHERE id = ? AND result = 'OPEN'
                """,
                (closed_at, PAPER_TRADE_VOID_REASON, json_dumps(detail), trade_id),
            )
            connection.execute(
                """
                INSERT INTO events (occurred_at, event_type, symbol, payload_json)
                VALUES (?, 'strategy_paper_trade_voided', ?, ?)
                """,
                (
                    closed_at,
                    normalized_symbol,
                    json_dumps(
                        {
                            "trade_id": trade_id,
                            "strategy_id": strategy_id,
                            "reason": PAPER_TRADE_VOID_REASON,
                            "statistics_changed": False,
                        }
                    ),
                ),
            )
        return True

    def _apply_strategy_trade_result(
        self,
        connection: sqlite3.Connection,
        strategy_id: str,
        result: str,
        closed_at: str,
        paper_trade: bool,
    ) -> None:
        self._ensure_strategy_state(connection, strategy_id, closed_at)
        row = connection.execute(
            """
            SELECT consecutive_wins, paper_trade_count, win_count, loss_count,
                   live_eligible, live_result_pending
            FROM strategy_states
            WHERE strategy_id = ?
            """,
            (strategy_id,),
        ).fetchone()
        consecutive_wins = int(row[0])
        paper_trade_count = int(row[1])
        win_count = int(row[2])
        loss_count = int(row[3])
        live_eligible = bool(row[4])
        live_result_pending = bool(row[5])

        if paper_trade:
            paper_trade_count += 1
            if result == "WIN":
                win_count += 1
            else:
                loss_count += 1
        elif result == "LOSS":
            consecutive_wins = 0
            live_eligible = False
            live_result_pending = False
        else:
            live_eligible = True
            live_result_pending = False

        win_rate = Decimal(win_count) / Decimal(paper_trade_count) if paper_trade_count else Decimal("0")
        state_result = "LIVE_RESULT_PENDING" if paper_trade and live_result_pending else result
        connection.execute(
            """
            UPDATE strategy_states
            SET consecutive_wins = ?, paper_trade_count = ?, win_count = ?,
                loss_count = ?, win_rate = ?, live_eligible = ?,
                live_result_pending = ?, last_trade_result = ?,
                last_trade_closed_at = ?, updated_at = ?
            WHERE strategy_id = ?
            """,
            (
                consecutive_wins,
                paper_trade_count,
                win_count,
                loss_count,
                str(win_rate),
                int(live_eligible),
                int(live_result_pending),
                state_result,
                closed_at,
                closed_at,
                strategy_id,
            ),
        )

    def mark_strategy_live_result_pending(
        self,
        strategy_id: str,
        reason: str,
    ) -> bool:
        now = utc_now()
        try:
            with self._connect() as connection:
                self._ensure_strategy_state(connection, strategy_id, now)
                connection.execute(
                    """
                    UPDATE strategy_states
                    SET live_eligible = 0, live_result_pending = 1,
                        last_trade_result = 'LIVE_RESULT_PENDING', updated_at = ?
                    WHERE strategy_id = ?
                    """,
                    (now, strategy_id),
                )
                connection.execute(
                    "INSERT INTO events (occurred_at, event_type, payload_json) VALUES (?, ?, ?)",
                    (
                        now,
                        "strategy_live_result_pending",
                        json_dumps({"strategy_id": strategy_id, "reason": reason}),
                    ),
                )
            return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while marking live result pending: %s", exc)
            return False

    def active_strategy_symbol_cooldown(
        self,
        strategy_id: str,
        symbol: str,
        cooldown_hours: int,
        now: datetime | None = None,
    ) -> str | None:
        now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT closed_at FROM strategy_paper_trades
                WHERE strategy_id = ? AND symbol = ? AND result = 'LOSS' AND closed_at IS NOT NULL
                UNION ALL
                SELECT closed_at FROM strategy_live_links
                WHERE strategy_id = ? AND symbol = ? AND result = 'LOSS' AND closed_at IS NOT NULL
                """,
                (strategy_id, symbol, strategy_id, symbol),
            ).fetchall()
        if not rows:
            return None
        latest_loss = max(parse_utc_datetime(str(row[0])) for row in rows)
        cooldown_until = latest_loss + timedelta(hours=cooldown_hours)
        if cooldown_until <= now_utc:
            return None
        return cooldown_until.isoformat()

    def active_strategy_symbol_cooldowns(
        self,
        requirements: Mapping[tuple[str, str], int],
        now: datetime | None = None,
    ) -> dict[tuple[str, str], str]:
        """Read one round's strategy/symbol loss cooldowns in one snapshot."""

        if type(requirements) is not dict:
            requirements = dict(requirements)
        if any(
            type(key) is not tuple
            or len(key) != 2
            or type(key[0]) is not str
            or not key[0]
            or type(key[1]) is not str
            or not key[1]
            or type(hours) is not int
            or hours <= 0
            for key, hours in requirements.items()
        ):
            raise ValueError(
                "strategy symbol cooldown snapshot identity is invalid"
            )
        if not requirements:
            return {}
        now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        strategies = tuple(sorted({key[0] for key in requirements}))
        symbols = tuple(sorted({key[1] for key in requirements}))
        strategy_placeholders = ",".join("?" for _ in strategies)
        latest: dict[tuple[str, str], datetime] = {}
        with self._read_only_runtime_snapshot() as connection:
            # 100 current symbols plus the bounded frozen lifecycle union fit
            # below SQLite's host-parameter limit in production.  Chunking
            # retains the same already-pinned query-only snapshot.
            symbol_chunk_size = max(1, 500 - len(strategies))
            for offset in range(0, len(symbols), symbol_chunk_size):
                chunk = symbols[offset : offset + symbol_chunk_size]
                symbol_placeholders = ",".join("?" for _ in chunk)
                parameters = (*strategies, *chunk)
                rows = connection.execute(
                    "SELECT strategy_id,symbol,closed_at FROM "
                    "strategy_paper_trades WHERE result='LOSS' "
                    f"AND strategy_id IN ({strategy_placeholders}) "
                    f"AND symbol IN ({symbol_placeholders}) "
                    "AND closed_at IS NOT NULL UNION ALL "
                    "SELECT strategy_id,symbol,closed_at FROM "
                    "strategy_live_links WHERE result='LOSS' "
                    f"AND strategy_id IN ({strategy_placeholders}) "
                    f"AND symbol IN ({symbol_placeholders}) "
                    "AND closed_at IS NOT NULL",
                    (*parameters, *parameters),
                ).fetchall()
                for strategy_id, symbol, closed_at in rows:
                    key = (strategy_id, symbol)
                    if key not in requirements:
                        continue
                    closed = parse_utc_datetime(str(closed_at))
                    previous = latest.get(key)
                    if previous is None or closed > previous:
                        latest[key] = closed
        active = {}
        for key, closed in latest.items():
            cooldown_until = closed + timedelta(hours=requirements[key])
            if cooldown_until > now_utc:
                active[key] = cooldown_until.isoformat()
        return active

    def record_strategy_live_open(
        self,
        strategy_id: str,
        trade_review_id: int | None,
        symbol: str,
        opened_at: str,
    ) -> int | None:
        if strategy_id == "N16":
            self.logger.warning(
                "N16 live-open audit requires the strict claim transaction"
            )
            return None
        try:
            with self._connect() as connection:
                self._ensure_strategy_state(connection, strategy_id)
                cursor = connection.execute(
                    """
                    INSERT INTO strategy_live_links (
                        strategy_id, trade_review_id, symbol, opened_at, created_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (strategy_id, trade_review_id, symbol, opened_at, utc_now()),
                )
                return int(cursor.lastrowid)
        except Exception as exc:
            self.logger.warning("Review DB write failed while recording strategy live open: %s", exc)
            return None

    @staticmethod
    def _strict_positive_exchange_id(value: Any) -> int | None:
        if type(value) is int and value > 0:
            return value
        if type(value) is str and value.isascii() and value.isdecimal():
            normalized = int(value)
            if normalized > 0 and str(normalized) == value:
                return normalized
        return None

    @staticmethod
    def _strict_client_order_id(value: Any) -> str | None:
        if (
            type(value) is str
            and 1 <= len(value) <= 36
            and value.isascii()
            and all(character.isalnum() or character in "._-:/" for character in value)
        ):
            return value
        return None

    @staticmethod
    def _strict_identity_string(value: Any, expected: str) -> bool:
        return type(value) is str and value == expected

    @classmethod
    def _validate_live_open_claim_state(
        cls,
        state: PositionState,
        strategy_id: str,
        *,
        allow_dry_run: bool = False,
    ) -> dict[str, Any]:
        string_fields = (
            state.symbol,
            state.quantity,
            state.entry_price,
            state.stop_loss_price,
            state.take_profit_price,
            state.opened_at,
        )
        if (
            type(strategy_id) is not str
            or not strategy_id
            or any(type(value) is not str or not value for value in string_fields)
            or type(state.leverage) is not int
            or state.leverage <= 0
            or type(state.dry_run) is not bool
            or (state.dry_run and not allow_dry_run)
            or type(state.orders) is not dict
            or "close" in state.orders
        ):
            raise RuntimeError("Invalid live state identity for audit claim")
        if not cls._strict_json_equal(state.orders, state.orders):
            raise RuntimeError("Live state orders are not strict finite JSON evidence")
        try:
            quantity = Decimal(state.quantity)
            entry_price = Decimal(state.entry_price)
            stop_loss_price = Decimal(state.stop_loss_price)
            take_profit_price = Decimal(state.take_profit_price)
        except (ArithmeticError, ValueError) as exc:
            raise RuntimeError("Invalid live state decimal evidence") from exc
        if (
            not all(
                value.is_finite() and value > 0
                for value in (
                    quantity,
                    entry_price,
                    stop_loss_price,
                    take_profit_price,
                )
            )
            or not stop_loss_price < entry_price < take_profit_price
        ):
            raise RuntimeError("Invalid live state price/quantity ordering")
        try:
            opened_at = datetime.fromisoformat(state.opened_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise RuntimeError("Invalid live state opened_at") from exc
        if (
            opened_at.tzinfo is None
            or opened_at.astimezone(timezone.utc).isoformat() != state.opened_at
        ):
            raise RuntimeError("Live state opened_at is not canonical UTC")

        pending_keys = {
            "execution_pending",
            "emergency_cleanup_pending",
            "protection_cleanup_pending",
        }
        if any(
            key in pending_keys or key.endswith("_cleanup_pending")
            for key in state.orders
        ):
            raise RuntimeError("Pending execution/cleanup state cannot be claimed as open")

        required_orders = {
            name: state.orders.get(name)
            for name in ("strategy", "plan", "open", "stop", "take_profit")
        }
        if any(type(value) is not dict or not value for value in required_orders.values()):
            raise RuntimeError("Live state is missing complete built-in order evidence")
        strategy_order = required_orders["strategy"]
        plan = required_orders["plan"]
        open_order = required_orders["open"]
        stop_order = required_orders["stop"]
        take_profit_order = required_orders["take_profit"]
        if (
            type(strategy_order.get("strategy_id")) is not str
            or strategy_order.get("strategy_id") != strategy_id
        ):
            raise RuntimeError("Live state strategy identity mismatch")

        if state.dry_run:
            if (
                set(open_order)
                != {
                    "symbol",
                    "side",
                    "type",
                    "origQty",
                    "clientOrderId",
                    "dryRun",
                }
                or set(stop_order)
                != {
                    "symbol",
                    "type",
                    "triggerPrice",
                    "clientAlgoId",
                    "dryRun",
                }
                or set(take_profit_order)
                != {
                    "symbol",
                    "type",
                    "triggerPrice",
                    "clientAlgoId",
                    "dryRun",
                }
                or open_order.get("symbol") != state.symbol
                or open_order.get("side") != "BUY"
                or open_order.get("type") != "MARKET"
                or open_order.get("origQty") != state.quantity
                or open_order.get("dryRun") is not True
                or stop_order.get("symbol") != state.symbol
                or stop_order.get("type") != "STOP_MARKET"
                or stop_order.get("triggerPrice") != state.stop_loss_price
                or stop_order.get("dryRun") is not True
                or take_profit_order.get("symbol") != state.symbol
                or take_profit_order.get("type") != "TAKE_PROFIT_MARKET"
                or take_profit_order.get("triggerPrice")
                != state.take_profit_price
                or take_profit_order.get("dryRun") is not True
                or plan.get("entry_price") != state.entry_price
                or plan.get("stop_loss_price") != state.stop_loss_price
                or plan.get("take_profit_price") != state.take_profit_price
                or plan.get("executed_quantity") != state.quantity
                or plan.get("final_protected_quantity") != state.quantity
            ):
                raise RuntimeError("Dry-run live state order identity mismatch")
            client_ids = (
                cls._strict_client_order_id(open_order.get("clientOrderId")),
                cls._strict_client_order_id(stop_order.get("clientAlgoId")),
                cls._strict_client_order_id(
                    take_profit_order.get("clientAlgoId")
                ),
            )
            if any(value is None for value in client_ids) or len(
                set(client_ids)
            ) != 3:
                raise RuntimeError(
                    "Dry-run live state client identities are invalid"
                )
            return plan

        open_order_id = cls._strict_positive_exchange_id(open_order.get("orderId"))
        open_client_id = cls._strict_client_order_id(open_order.get("clientOrderId"))
        try:
            open_executed_quantity = Decimal(str(open_order.get("executedQty")))
        except (ArithmeticError, ValueError) as exc:
            raise RuntimeError("Live state market-order executed quantity invalid") from exc
        recovered_from_position = open_order.get("executionRecoveredFromPosition") is True
        if (
            open_client_id is None
            or not cls._strict_identity_string(open_order.get("symbol"), state.symbol)
            or not cls._strict_identity_string(open_order.get("side"), "BUY")
            or not cls._strict_identity_string(
                open_order.get("type", open_order.get("origType")), "MARKET"
            )
            or not cls._strict_identity_string(open_order.get("status"), "FILLED")
            or not open_executed_quantity.is_finite()
            or open_executed_quantity <= 0
            or quantity > open_executed_quantity
            or (
                open_order_id is None
                and not (
                    "orderId" not in open_order
                    and recovered_from_position
                )
            )
        ):
            raise RuntimeError("Live state market-order identity mismatch")
        for plan_key, expected_value in (
            ("executed_quantity", open_executed_quantity),
            ("final_protected_quantity", quantity),
        ):
            plan_value = plan.get(plan_key)
            if plan_value is None:
                continue
            try:
                parsed_plan_value = Decimal(str(plan_value))
            except (ArithmeticError, ValueError) as exc:
                raise RuntimeError(
                    f"Live state plan {plan_key} is invalid"
                ) from exc
            if (
                not parsed_plan_value.is_finite()
                or parsed_plan_value <= 0
                or parsed_plan_value != expected_value
            ):
                raise RuntimeError(
                    f"Live state plan {plan_key} conflicts with order/state evidence"
                )

        protection_client_ids: list[str] = []
        for role, order, expected_type in (
            ("stop", stop_order, "STOP_MARKET"),
            ("take_profit", take_profit_order, "TAKE_PROFIT_MARKET"),
        ):
            algo_id = (
                cls._strict_positive_exchange_id(order.get("algoId"))
                if "algoId" in order
                else None
            )
            client_algo_id = cls._strict_client_order_id(order.get("clientAlgoId"))
            raw_status_present = "algoStatus" in order or "status" in order
            raw_status = order.get("algoStatus", order.get("status"))
            if (
                ("algoId" in order and algo_id is None)
                or client_algo_id is None
                or not cls._strict_identity_string(order.get("symbol"), state.symbol)
                or not cls._strict_identity_string(order.get("side"), "SELL")
                or not cls._strict_identity_string(order.get("orderType"), expected_type)
                or not cls._strict_identity_string(order.get("algoType"), "CONDITIONAL")
                or order.get("closePosition") is not True
                or (
                    raw_status_present
                    and (
                        type(raw_status) is not str
                        or raw_status != "NEW"
                    )
                )
            ):
                raise RuntimeError(f"Live state {role} protection identity mismatch")
            protection_client_ids.append(client_algo_id)
        if len({open_client_id, *protection_client_ids}) != 3:
            raise RuntimeError("Live state order client identities are not unique")
        return plan

    @staticmethod
    def _live_open_review_core(
        state: PositionState,
        plan: dict[str, Any],
    ) -> tuple[Any, ...]:
        return (
            state.opened_at,
            state.symbol,
            "BUY",
            state.quantity,
            state.entry_price,
            state.stop_loss_price,
            state.take_profit_price,
            plan.get("amplitude_24h_pct"),
            plan.get("high_24h_price"),
            plan.get("low_24h_price"),
            plan.get("stop_loss_pct"),
            plan.get("take_profit_pct"),
            plan.get("risk_amount"),
            plan.get("target_risk_amount"),
            plan.get("actual_risk_amount"),
            int(bool(plan.get("risk_capped_by_margin"))),
            plan.get("pretrade_quantity"),
            plan.get("executed_quantity"),
            plan.get("final_protected_quantity"),
            plan.get("post_fill_actual_risk_amount"),
            plan.get("post_fill_required_margin"),
            int(bool(plan.get("reduced_after_fill"))),
            plan.get("notional_value"),
            plan.get("required_margin"),
            plan.get("balance"),
            state.leverage,
            int(state.dry_run),
        )

    @staticmethod
    def _strict_json_equal(left: Any, right: Any) -> bool:
        if type(left) is not type(right):
            return False
        if type(left) is dict:
            if (
                not all(type(key) is str for key in left)
                or not all(type(key) is str for key in right)
                or left.keys() != right.keys()
            ):
                return False
            return all(
                ReviewRecorder._strict_json_equal(left[key], right[key])
                for key in left
            )
        if type(left) is list:
            return len(left) == len(right) and all(
                ReviewRecorder._strict_json_equal(left_item, right_item)
                for left_item, right_item in zip(left, right)
            )
        if type(left) is float:
            return math.isfinite(left) and math.isfinite(right) and left == right
        if type(left) in {str, int, bool, type(None)}:
            return left == right
        return False

    @staticmethod
    def _strict_review_orders_document(raw_orders: Any) -> dict[str, Any] | None:
        def reject_duplicate_keys(
            pairs: list[tuple[str, Any]],
        ) -> dict[str, Any]:
            result = {}  # type: dict[str, Any]
            for key, value in pairs:
                if type(key) is not str or key in result:
                    raise ValueError("duplicate Review order key")
                result[key] = value
            return result

        try:
            value = json.loads(
                raw_orders,
                object_pairs_hook=reject_duplicate_keys,
                parse_constant=lambda _value: (_ for _ in ()).throw(
                    ValueError("non-finite Review order value")
                ),
            )
        except (TypeError, ValueError):
            return None
        if type(value) is not dict or not ReviewRecorder._strict_json_equal(
            value, value
        ):
            return None
        return value

    @staticmethod
    def _strict_review_close_payload(
        raw_orders: Any,
        expected_values: tuple[str, str, str, str, str, str],
        *,
        allow_resolution_detail: bool,
    ) -> dict[str, Any] | None:
        saved_orders = ReviewRecorder._strict_review_orders_document(raw_orders)
        if saved_orders is None or type(saved_orders.get("close")) is not dict:
            return None
        close_payload = saved_orders["close"]
        required_keys = {
            "exit_reason",
            "exit_price",
            "mark_price",
            "pnl_amount",
            "pnl_pct",
            "balance_after",
        }
        allowed_keys = set(required_keys)
        if allow_resolution_detail:
            allowed_keys.add("resolution_detail")
        if (
            not required_keys.issubset(close_payload)
            or not set(close_payload).issubset(allowed_keys)
            or tuple(
                close_payload[key]
                for key in (
                    "exit_reason",
                    "exit_price",
                    "mark_price",
                    "pnl_amount",
                    "pnl_pct",
                    "balance_after",
                )
            )
            != expected_values
            or any(
                type(close_payload[key]) is not str for key in required_keys
            )
            or (
                "resolution_detail" in close_payload
                and type(close_payload["resolution_detail"]) is not dict
            )
        ):
            return None
        return close_payload

    @staticmethod
    def _review_orders_match_state(
        raw_orders: Any,
        state_orders: dict[str, Any],
        *,
        require_close: bool,
        expected_close: tuple[str, str, str, str, str, str] | None = None,
        allow_resolution_detail: bool = False,
    ) -> bool:
        saved_orders = ReviewRecorder._strict_review_orders_document(raw_orders)
        if saved_orders is None:
            return False
        has_close = "close" in saved_orders
        if has_close != require_close:
            return False
        if has_close:
            if expected_close is None or ReviewRecorder._strict_review_close_payload(
                raw_orders,
                expected_close,
                allow_resolution_detail=allow_resolution_detail,
            ) is None:
                return False
            saved_orders = dict(saved_orders)
            saved_orders.pop("close")
        return (
            ReviewRecorder._strict_json_equal(saved_orders, state_orders)
            or ReviewRecorder._strict_json_equal(
                saved_orders,
                {"state_orders": state_orders},
            )
        )

    @classmethod
    def _open_review_matches_state(
        cls,
        review: tuple[Any, ...],
        state: PositionState,
        *,
        expected_status: str,
    ) -> bool:
        plan = state.orders.get("plan")
        if type(plan) is not dict:
            return False
        if tuple(review[1:28]) != cls._live_open_review_core(state, plan):
            return False
        if review[28] != expected_status or review[29] is not None:
            return False
        if expected_status == "OPENED":
            return (
                all(value is None for value in review[31:38])
                and cls._review_orders_match_state(
                    review[30], state.orders, require_close=False
                )
            )
        if expected_status == "CLOSED_LIVE_RESULT_PENDING":
            return (
                bool(review[31])
                and review[32] == "LIVE_RESULT_PENDING"
                and tuple(review[33:38]) == ("", "", "", "", "")
                and cls._review_orders_match_state(
                    review[30],
                    state.orders,
                    require_close=True,
                    expected_close=(
                        "LIVE_RESULT_PENDING",
                        "",
                        "",
                        "",
                        "",
                        "",
                    ),
                    allow_resolution_detail=True,
                )
            )
        return False

    def _attest_n16_live_execution_identity(
        self,
        connection: sqlite3.Connection,
        state: PositionState,
    ) -> tuple[tuple[Any, ...], tuple[Any, ...], tuple[Any, ...]]:
        """Bind one live state to its permanent claim, Review, and live link."""

        plan = self._validate_live_open_claim_state(
            state,
            "N16",
            allow_dry_run=True,
        )
        _strategy_id, signal_id, structure_id = (
            self._n16_execution_claim_components(
                strict_json_dumps(state.orders),
                nested_strategy=True,
            )
        )
        self._attest_n16_claim_ledger(connection)
        claim = self._attest_n16_committed_structure_claim(
            connection,
            state.symbol,
            structure_id,
            source_signal_id=signal_id,
        )
        reviews = connection.execute(
            """
            SELECT id, opened_at, symbol, side, quantity, entry_price,
                   stop_loss_price, take_profit_price, amplitude_24h_pct,
                   high_24h_price, low_24h_price, stop_loss_pct,
                   take_profit_pct, risk_amount, target_risk_amount,
                   actual_risk_amount, risk_capped_by_margin,
                   pretrade_quantity, executed_quantity,
                   final_protected_quantity, post_fill_actual_risk_amount,
                   post_fill_required_margin, reduced_after_fill,
                   notional_value, required_margin, balance, leverage,
                   dry_run, status, error, orders_json, closed_at,
                   exit_reason, exit_price, close_mark_price,
                   realized_pnl, realized_pnl_pct, balance_after_close
            FROM trade_reviews
            WHERE symbol=? AND dry_run=? AND opened_at=?
            ORDER BY id
            """,
            (state.symbol, int(state.dry_run), state.opened_at),
        ).fetchall()
        if len(reviews) != 1:
            raise RuntimeError("N16 live Review identity is not unique")
        review = tuple(reviews[0])
        if tuple(review[1:28]) != self._live_open_review_core(state, plan):
            raise RuntimeError("N16 live Review core identity conflicts")
        if review[28] == "OPENED":
            orders_match = self._review_orders_match_state(
                review[30], state.orders, require_close=False
            )
        elif review[28] == "CLOSED_LIVE_RESULT_PENDING":
            orders_match = self._review_orders_match_state(
                review[30],
                state.orders,
                require_close=True,
                expected_close=(
                    "LIVE_RESULT_PENDING",
                    "",
                    "",
                    "",
                    "",
                    "",
                ),
                allow_resolution_detail=True,
            )
        elif review[28] in {"CLOSED_STOP_LOSS", "CLOSED_TAKE_PROFIT"}:
            orders_match = self._review_orders_match_state(
                review[30],
                state.orders,
                require_close=True,
                expected_close=tuple(review[32:38]),
                allow_resolution_detail=True,
            )
        else:
            orders_match = False
        if not orders_match or review[29] is not None:
            raise RuntimeError("N16 live Review order identity conflicts")

        links = connection.execute(
            """
            SELECT id,strategy_id,trade_review_id,symbol,opened_at,
                   closed_at,result,created_at
            FROM strategy_live_links
            WHERE symbol=? AND opened_at=?
            ORDER BY id
            """,
            (state.symbol, state.opened_at),
        ).fetchall()
        if len(links) != 1:
            raise RuntimeError("N16 live link identity is not unique")
        link = tuple(links[0])
        if (
            type(link[0]) is not int
            or link[0] <= 0
            or link[1] != "N16"
            or link[2] != review[0]
            or link[3] != state.symbol
            or link[4] != state.opened_at
        ):
            raise RuntimeError("N16 live link identity conflicts")
        opened_time = canonical_utc_datetime(state.opened_at)
        created_time = canonical_utc_datetime(link[7])
        upper_time = (
            canonical_utc_datetime(link[5])
            if link[5] is not None
            else datetime.now(timezone.utc)
        )
        if not opened_time <= created_time <= upper_time:
            raise RuntimeError("N16 live link time ordering conflicts")
        return review, link, claim

    def claim_strategy_live_open_audit(
        self,
        state: PositionState,
        strategy_id: str,
    ) -> StrategyLiveOpenClaim | None:
        """Atomically recover the live-open review/link after a process crash.

        The local state is the authority for open-order evidence only.  This
        method never invents a close result and accepts no ambiguous or
        terminal audit row.
        """
        try:
            plan = self._validate_live_open_claim_state(
                state,
                strategy_id,
                allow_dry_run=(strategy_id == "N16"),
            )
            n16_claim_identity = None
            if strategy_id == "N16":
                _payload_strategy, signal_id, structure_id = (
                    self._n16_execution_claim_components(
                        json_dumps(state.orders),
                        nested_strategy=True,
                    )
                )
                n16_claim_identity = (
                    signal_id,
                    state.symbol,
                    structure_id,
                )
        except Exception as exc:
            self.logger.warning("Refusing invalid live-open audit claim: %s", exc)
            return None
        expected_review_identity = (
            *self._live_open_review_core(state, plan),
            "OPENED",
            None,
        )
        recovered_orders = {"state_orders": state.orders}
        claim_time = utc_now()
        try:
            opened_time = canonical_utc_datetime(state.opened_at)
            claim_created_time = canonical_utc_datetime(claim_time)
            if opened_time > claim_created_time:
                raise ValueError("live-open claim precedes the position open time")
        except (TypeError, ValueError) as exc:
            self.logger.warning("Refusing invalid live-open claim time: %s", exc)
            return None

        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                if n16_claim_identity is not None:
                    self._attest_n16_claim_ledger(connection)
                    self._attest_n16_committed_structure_claim(
                        connection,
                        n16_claim_identity[1],
                        n16_claim_identity[2],
                        source_signal_id=n16_claim_identity[0],
                    )
                definitions = connection.execute(
                    "SELECT strategy_id FROM strategy_definitions WHERE strategy_id = ?",
                    (strategy_id,),
                ).fetchall()
                strategy_states = connection.execute(
                    "SELECT strategy_id FROM strategy_states WHERE strategy_id = ?",
                    (strategy_id,),
                ).fetchall()
                if len(definitions) != 1 or definitions[0][0] != strategy_id:
                    raise RuntimeError(
                        f"Live-open claim strategy is not exactly registered: {strategy_id}"
                    )
                if len(strategy_states) != 1 or strategy_states[0][0] != strategy_id:
                    raise RuntimeError(
                        f"Live-open claim strategy state is missing: {strategy_id}"
                    )
                reviews = connection.execute(
                    """
                    SELECT id, opened_at, symbol, side, quantity, entry_price,
                           stop_loss_price, take_profit_price, amplitude_24h_pct,
                           high_24h_price, low_24h_price, stop_loss_pct,
                           take_profit_pct, risk_amount, target_risk_amount,
                           actual_risk_amount, risk_capped_by_margin,
                           pretrade_quantity, executed_quantity,
                           final_protected_quantity, post_fill_actual_risk_amount,
                           post_fill_required_margin, reduced_after_fill,
                           notional_value, required_margin, balance, leverage,
                           dry_run, status, error, orders_json, closed_at,
                           exit_reason, exit_price, close_mark_price,
                           realized_pnl, realized_pnl_pct, balance_after_close
                    FROM trade_reviews
                    WHERE symbol = ? AND opened_at = ? AND dry_run = ?
                    ORDER BY id ASC
                    """,
                    (state.symbol, state.opened_at, int(state.dry_run)),
                ).fetchall()
                if len(reviews) > 1:
                    raise RuntimeError(
                        "Ambiguous trade reviews for live-open claim: "
                        f"symbol={state.symbol} opened_at={state.opened_at} "
                        f"count={len(reviews)}"
                    )

                review_created = False
                if not reviews:
                    cursor = connection.execute(
                        """
                        INSERT INTO trade_reviews (
                            scan_id, opened_at, symbol, side, quantity, entry_price,
                            stop_loss_price, take_profit_price, amplitude_24h_pct,
                            high_24h_price, low_24h_price, stop_loss_pct,
                            take_profit_pct, risk_amount, target_risk_amount,
                            actual_risk_amount, risk_capped_by_margin,
                            pretrade_quantity, executed_quantity,
                            final_protected_quantity, post_fill_actual_risk_amount,
                            post_fill_required_margin, reduced_after_fill,
                            notional_value, required_margin, balance, leverage,
                            dry_run, status, error, orders_json
                        ) VALUES (
                            NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,
                        (*expected_review_identity, json_dumps(recovered_orders)),
                    )
                    trade_review_id = int(cursor.lastrowid)
                    if trade_review_id <= 0:
                        raise RuntimeError("Live-open claim inserted an invalid trade review id")
                    review_created = True
                else:
                    review = reviews[0]
                    trade_review_id = review[0]
                    if type(trade_review_id) is not int or trade_review_id <= 0:
                        raise RuntimeError(
                            f"Live-open claim found invalid trade review id: {trade_review_id!r}"
                        )
                    if not self._open_review_matches_state(
                        review,
                        state,
                        expected_status="OPENED",
                    ):
                        raise RuntimeError(
                            "Trade review is not an exact pristine OPENED audit: "
                            f"trade_review_id={trade_review_id}"
                        )

                links = connection.execute(
                    """
                    SELECT id, strategy_id, trade_review_id, symbol, opened_at,
                           closed_at, result, created_at
                    FROM strategy_live_links
                    WHERE symbol = ? AND opened_at = ?
                    ORDER BY id ASC
                    """,
                    (state.symbol, state.opened_at),
                ).fetchall()
                if len(links) > 1:
                    raise RuntimeError(
                        "Ambiguous live links for live-open claim: "
                        f"strategy={strategy_id} symbol={state.symbol} "
                        f"opened_at={state.opened_at} count={len(links)}"
                    )
                link_created = False
                if not links:
                    link_cursor = connection.execute(
                        """
                        INSERT INTO strategy_live_links (
                            strategy_id, trade_review_id, symbol, opened_at, created_at
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            strategy_id,
                            trade_review_id,
                            state.symbol,
                            state.opened_at,
                            claim_time,
                        ),
                    )
                    if int(link_cursor.lastrowid) <= 0:
                        raise RuntimeError("Live-open claim inserted an invalid live link id")
                    link_created = True
                else:
                    (
                        link_id,
                        linked_strategy_id,
                        linked_review_id,
                        linked_symbol,
                        linked_opened_at,
                        closed_at,
                        result,
                        created_at,
                    ) = links[0]
                    if (
                        type(link_id) is not int
                        or link_id <= 0
                        or linked_strategy_id != strategy_id
                        or linked_review_id != trade_review_id
                        or linked_symbol != state.symbol
                        or linked_opened_at != state.opened_at
                        or closed_at is not None
                        or result is not None
                        or type(created_at) is not str
                        or not created_at
                    ):
                        raise RuntimeError(
                            "Live link conflicts with exact live-open claim: "
                            f"link_id={link_id} trade_review_id={linked_review_id!r}"
                        )
                    if not (
                        opened_time
                        <= canonical_utc_datetime(created_at)
                        <= claim_created_time
                    ):
                        raise RuntimeError(
                            "Live link timestamp conflicts with the exact claim"
                        )

                return StrategyLiveOpenClaim(
                    trade_review_id,
                    review_created=review_created,
                    link_created=link_created,
                )
        except Exception as exc:
            self.logger.warning("Atomic live-open audit claim failed: %s", exc)
            return None

    def record_strategy_live_result(
        self,
        strategy_id: str,
        result: str,
        symbol: str | None = None,
        trade_review_id: int | None = None,
        opened_at: str | None = None,
        closed_at: str | None = None,
    ) -> bool:
        """Close one already-audited legacy/dry-run live link safely.

        The live finalizer is the only path for real exchange recovery.  This
        compatibility method remains for the dry-run close path, but must not
        turn a missing or ambiguous link into a strategy-state result.
        """
        if result not in {"WIN", "LOSS"}:
            raise ValueError("strategy live result must be WIN or LOSS")
        if (
            symbol is None
            or type(trade_review_id) is not int
            or trade_review_id <= 0
            or type(opened_at) is not str
            or not opened_at
        ):
            self.logger.warning(
                "Refusing strategy live result without strict link identity: "
                "strategy=%s symbol=%s trade_review_id=%r opened_at=%r",
                strategy_id,
                symbol,
                trade_review_id,
                opened_at,
            )
            return False
        close_time = closed_at or utc_now()
        expected_review_status = (
            "CLOSED_TAKE_PROFIT" if result == "WIN" else "CLOSED_STOP_LOSS"
        )
        try:
            with self._connect() as connection:
                links = connection.execute(
                    """
                    SELECT id, closed_at, result
                    FROM strategy_live_links
                    WHERE strategy_id = ? AND symbol = ? AND opened_at = ?
                          AND trade_review_id = ?
                    ORDER BY id ASC
                    """,
                    (strategy_id, symbol, opened_at, trade_review_id),
                ).fetchall()
                if len(links) != 1:
                    raise RuntimeError(
                        "Expected exactly one strict strategy live link: "
                        f"strategy={strategy_id} symbol={symbol} "
                        f"opened_at={opened_at} trade_review_id={trade_review_id} "
                        f"count={len(links)}"
                    )
                link_id, link_closed_at, link_result = links[0]
                reviews = connection.execute(
                    """
                    SELECT id, status, closed_at
                    FROM trade_reviews
                    WHERE id = ? AND symbol = ? AND opened_at = ?
                    """,
                    (trade_review_id, symbol, opened_at),
                ).fetchall()
                if (
                    len(reviews) != 1
                    or reviews[0][1] != expected_review_status
                    or not reviews[0][2]
                ):
                    raise RuntimeError(
                        "Strategy live result review evidence is missing or conflicting: "
                        f"trade_review_id={trade_review_id}"
                    )
                strategy_rows = connection.execute(
                    "SELECT strategy_id FROM strategy_states WHERE strategy_id = ?",
                    (strategy_id,),
                ).fetchall()
                if len(strategy_rows) != 1:
                    raise RuntimeError(
                        f"Expected exactly one strategy state: {strategy_id}"
                    )
                if link_closed_at is not None or link_result is not None:
                    if link_closed_at is not None and link_result == result:
                        return True
                    raise RuntimeError(
                        "Strategy live link has conflicting final result: "
                        f"link_id={link_id} result={link_result!r}"
                    )
                link_update = connection.execute(
                    """
                    UPDATE strategy_live_links
                    SET closed_at = ?, result = ?
                    WHERE id = ? AND strategy_id = ? AND symbol = ? AND opened_at = ?
                          AND trade_review_id = ? AND closed_at IS NULL AND result IS NULL
                    """,
                    (
                        close_time,
                        result,
                        link_id,
                        strategy_id,
                        symbol,
                        opened_at,
                        trade_review_id,
                    ),
                )
                if link_update.rowcount != 1:
                    raise RuntimeError(
                        f"Strategy live link update did not affect exactly one row: {link_id}"
                    )
                self._apply_strategy_trade_result(
                    connection,
                    strategy_id,
                    result,
                    close_time,
                    paper_trade=False,
                )
            return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while recording strategy live result: %s", exc)
            return False

    def finalize_strategy_live_result(
        self,
        state: PositionState,
        strategy_id: str,
        result: str,
        exit_reason: str,
        exit_price: str,
        detail: dict[str, Any],
        cooldown_until: datetime,
        *,
        mark_price: str = "",
        pnl_amount: str = "",
        pnl_pct: str = "",
        balance_after: str = "",
        _allow_dry_run: bool = False,
    ) -> StrategyLiveFinalization | None:
        if state.dry_run and not _allow_dry_run:
            raise ValueError("strategy live finalization requires a live state")
        if _allow_dry_run and not state.dry_run:
            raise ValueError("dry-run strategy finalization requires a dry-run state")
        if result not in {"WIN", "LOSS"}:
            raise ValueError("strategy live finalization result must be WIN or LOSS")
        if (result, exit_reason) not in {
            ("WIN", "TAKE_PROFIT"),
            ("LOSS", "STOP_LOSS"),
        }:
            raise ValueError(
                "strategy live finalization result and exit reason disagree: "
                f"result={result} exit_reason={exit_reason}"
            )

        if (
            type(detail) is not dict
            or any(
                type(value) is not str
                for value in (
                    exit_price,
                    mark_price,
                    pnl_amount,
                    pnl_pct,
                    balance_after,
                )
            )
        ):
            self.logger.warning("Refusing non-strict live finalization payload")
            return None
        close_time = utc_now()
        status = f"CLOSED_{exit_reason}"
        close_payload = {
            "exit_reason": exit_reason,
            "exit_price": exit_price,
            "mark_price": mark_price,
            "pnl_amount": pnl_amount,
            "pnl_pct": pnl_pct,
            "balance_after": balance_after,
            "resolution_detail": detail,
        }
        try:
            close_datetime = canonical_utc_datetime(close_time)
            opened_datetime = canonical_utc_datetime(state.opened_at)
            if strategy_id == "N16":
                self._validate_live_open_claim_state(
                    state,
                    "N16",
                    allow_dry_run=True,
                )
                self._n16_execution_claim_components(
                    strict_json_dumps(state.orders),
                    nested_strategy=True,
                )
            if type(cooldown_until) is not datetime:
                raise ValueError("cooldown time must be a built-in datetime")
            if cooldown_until.tzinfo is None:
                raise ValueError("cooldown time must be timezone-aware")
            cooldown_until_text = cooldown_until.astimezone(
                timezone.utc
            ).isoformat()
            cooldown_datetime = canonical_utc_datetime(cooldown_until_text)
            if opened_datetime > close_datetime or cooldown_datetime <= close_datetime:
                raise ValueError("live finalization time ordering is invalid")
            strict_json_dumps(close_payload)
            strict_json_dumps(state.orders)
            _validate_terminal_financial_evidence(
                exit_reason=exit_reason,
                exit_price=exit_price,
                mark_price=mark_price,
                pnl_amount=pnl_amount,
                pnl_pct=pnl_pct,
                balance_after=balance_after,
                dry_run=state.dry_run,
            )
        except (TypeError, ValueError) as exc:
            self.logger.warning(
                "Refusing non-finite live finalization evidence: %s", exc
            )
            return None
        try:
            with self._connect() as connection:
                if strategy_id == "N16":
                    self._attest_n16_live_execution_identity(
                        connection,
                        state,
                    )
                links = connection.execute(
                    """
                    SELECT id, trade_review_id, closed_at, result, created_at
                    FROM strategy_live_links
                    WHERE strategy_id = ? AND symbol = ? AND opened_at = ?
                    ORDER BY id ASC
                    """,
                    (strategy_id, state.symbol, state.opened_at),
                ).fetchall()
                if len(links) != 1:
                    raise RuntimeError(
                        "Expected exactly one strategy live link for finalization: "
                        f"strategy={strategy_id} symbol={state.symbol} "
                        f"opened_at={state.opened_at} count={len(links)}"
                    )
                (
                    link_id,
                    trade_review_id,
                    link_closed_at,
                    link_result,
                    link_created_at,
                ) = links[0]
                if type(trade_review_id) is not int or trade_review_id <= 0:
                    raise RuntimeError(
                        "Strategy live link has no valid trade_review_id: "
                        f"link_id={link_id} trade_review_id={trade_review_id!r}"
                    )
                try:
                    link_created_datetime = canonical_utc_datetime(
                        link_created_at
                    )
                except (TypeError, ValueError) as exc:
                    raise RuntimeError(
                        "Strategy live link creation time is invalid"
                    ) from exc
                if not (
                    opened_datetime
                    <= link_created_datetime
                    <= close_datetime
                ):
                    raise RuntimeError(
                        "Strategy live link time ordering is invalid"
                    )

                reviews = connection.execute(
                    """
                    SELECT id, status, closed_at, exit_reason, exit_price,
                           close_mark_price, realized_pnl, realized_pnl_pct,
                           balance_after_close, orders_json
                    FROM trade_reviews
                    WHERE symbol = ? AND dry_run = ? AND opened_at = ?
                    ORDER BY id ASC
                    """,
                    (state.symbol, int(state.dry_run), state.opened_at),
                ).fetchall()
                if len(reviews) != 1 or reviews[0][0] != trade_review_id:
                    raise RuntimeError(
                        "Strategy live link review identity mismatch: "
                        f"link_id={link_id} trade_review_id={trade_review_id}"
                    )
                review = reviews[0]
                event_close_payload = close_payload
                final_pnl_amount = pnl_amount
                final_balance_after = balance_after
                if review[1] in {"CLOSED_STOP_LOSS", "CLOSED_TAKE_PROFIT"}:
                    if review[1] != status or review[3] != exit_reason:
                        raise RuntimeError(
                            "Final trade review reason conflicts with live finalization"
                        )
                    _validate_terminal_financial_evidence(
                        exit_reason=review[3],
                        exit_price=review[4],
                        mark_price=review[5],
                        pnl_amount=review[6],
                        pnl_pct=review[7],
                        balance_after=review[8],
                        dry_run=state.dry_run,
                    )
                    stored_close_payload = self._strict_review_close_payload(
                        review[9],
                        tuple(review[3:9]),
                        allow_resolution_detail=True,
                    )
                    if stored_close_payload is None:
                        raise RuntimeError(
                            "Final trade review close payload is invalid"
                        )
                    event_close_payload = stored_close_payload
                    final_pnl_amount = review[6]
                    final_balance_after = review[8]
                (
                    terminal_close_events,
                    terminal_event_occurred_at,
                ) = self._terminal_live_close_event_evidence(
                    connection,
                    state.symbol,
                    trade_review_id,
                    exit_reason,
                    event_close_payload,
                    self._terminal_close_event_type(strategy_id, state.dry_run),
                    require_n16_index=(strategy_id == "N16"),
                )

                state_row = connection.execute(
                    """
                    SELECT consecutive_wins, live_eligible, live_result_pending,
                           last_trade_result, last_trade_closed_at, updated_at
                    FROM strategy_states
                    WHERE strategy_id = ?
                    """,
                    (strategy_id,),
                ).fetchall()
                if len(state_row) != 1:
                    raise RuntimeError(
                        f"Expected strategy state for live finalization: {strategy_id}"
                    )

                if link_closed_at is not None or link_result is not None:
                    final_state_matches = (
                        int(state_row[0][0]) == 0
                        and not bool(state_row[0][1])
                        and not bool(state_row[0][2])
                        and state_row[0][3] == "LOSS"
                        if result == "LOSS"
                        else (
                            bool(state_row[0][1])
                            and not bool(state_row[0][2])
                            and state_row[0][3] == "WIN"
                        )
                    )
                    cooldown = connection.execute(
                        """
                        SELECT cooldown_until, reason, source_trade_id,
                               created_at, updated_at
                        FROM symbol_cooldowns
                        WHERE symbol = ?
                        """,
                        (state.symbol,),
                    ).fetchall()
                    final_time_matches = False
                    cooldown_time_matches = False
                    try:
                        review_closed = canonical_utc_datetime(review[2])
                        link_closed = canonical_utc_datetime(link_closed_at)
                        event_closed = canonical_utc_datetime(
                            terminal_event_occurred_at
                        )
                        state_closed = canonical_utc_datetime(state_row[0][4])
                        state_updated = canonical_utc_datetime(state_row[0][5])
                        final_time_matches = (
                            review[2]
                            == link_closed_at
                            == terminal_event_occurred_at
                            == state_row[0][4]
                            and review_closed
                            == link_closed
                            == event_closed
                            == state_closed
                            and state_updated == state_closed
                            and opened_datetime
                            <= link_created_datetime
                            <= review_closed
                        )
                        if len(cooldown) == 1:
                            cooldown_until_value = canonical_utc_datetime(
                                cooldown[0][0]
                            )
                            cooldown_created = canonical_utc_datetime(
                                cooldown[0][3]
                            )
                            cooldown_updated = canonical_utc_datetime(
                                cooldown[0][4]
                            )
                            cooldown_time_matches = (
                                cooldown_until_value > review_closed
                                and cooldown_created == review_closed
                                and cooldown_updated == review_closed
                            )
                    except (TypeError, ValueError):
                        final_time_matches = False
                        cooldown_time_matches = False
                    if (
                        link_closed_at is None
                        or link_result != result
                        or not review[2]
                        or self._strict_review_close_payload(
                            review[9],
                            tuple(review[3:9]),
                            allow_resolution_detail=True,
                        )
                        is None
                        or not final_state_matches
                        or not final_time_matches
                        or len(cooldown) != 1
                        or cooldown[0][1] != exit_reason
                        or cooldown[0][2] != trade_review_id
                        or not cooldown_time_matches
                        or terminal_close_events != 1
                    ):
                        raise RuntimeError(
                            "Conflicting or incomplete final live evidence: "
                            f"strategy={strategy_id} symbol={state.symbol}"
                        )
                    return StrategyLiveFinalization(
                        trade_review_id,
                        idempotent=True,
                        pnl_amount=final_pnl_amount,
                        balance_after=final_balance_after,
                    )

                review_was_final = False
                authoritative_event_exists = terminal_close_events == 1
                if review[1] in {"OPENED", "CLOSED_LIVE_RESULT_PENDING"}:
                    if terminal_close_events != 0:
                        raise RuntimeError(
                            "Pending live review already has terminal close event: "
                            f"trade_review_id={trade_review_id}"
                        )
                    review_update = connection.execute(
                        """
                        UPDATE trade_reviews
                        SET status = ?, closed_at = ?, exit_reason = ?, exit_price = ?,
                            close_mark_price = ?, realized_pnl = ?, realized_pnl_pct = ?,
                            balance_after_close = ?, orders_json = ?
                        WHERE id = ? AND status = ?
                        """,
                        (
                            status,
                            close_time,
                            exit_reason,
                            exit_price,
                            mark_price,
                            pnl_amount,
                            pnl_pct,
                            balance_after,
                            self._orders_json_with_close(review[9], close_payload),
                            trade_review_id,
                            review[1],
                        ),
                    )
                    if review_update.rowcount != 1:
                        raise RuntimeError(
                            f"Live review update did not affect exactly one row: {trade_review_id}"
                        )
                elif review[1] == status and review[2]:
                    recoverable_event_count = terminal_close_events
                    recoverable_event_time = terminal_event_occurred_at
                    if (
                        recoverable_event_count != 1
                        or recoverable_event_time != review[2]
                    ):
                        raise RuntimeError(
                            "Final trade review event evidence is invalid"
                        )
                    # A previous process durably wrote the close audit/event and
                    # then crashed before closing the exact live link.  Reuse it.
                    review_was_final = True
                    try:
                        recoverable_closed_datetime = canonical_utc_datetime(
                            review[2]
                        )
                        if not (
                            opened_datetime
                            <= link_created_datetime
                            <= recoverable_closed_datetime
                        ):
                            raise ValueError(
                                "recoverable close precedes the live link"
                            )
                    except (TypeError, ValueError) as exc:
                        raise RuntimeError(
                            "Final trade review close time is invalid"
                        ) from exc
                    strict_existing_close = self._strict_review_close_payload(
                        review[9],
                        tuple(review[3:9]),
                        allow_resolution_detail=True,
                    )
                    if strict_existing_close is None:
                        raise RuntimeError(
                            "Final trade review close payload is invalid"
                        )
                    close_time = review[2]
                else:
                    raise RuntimeError(
                        "Conflicting final trade review before live finalization: "
                        f"trade_review_id={trade_review_id} status={review[1]}"
                    )

                link_update = connection.execute(
                    """
                    UPDATE strategy_live_links
                    SET closed_at = ?, result = ?
                    WHERE id = ? AND strategy_id = ? AND symbol = ? AND opened_at = ?
                          AND trade_review_id = ? AND closed_at IS NULL AND result IS NULL
                    """,
                    (
                        close_time,
                        result,
                        link_id,
                        strategy_id,
                        state.symbol,
                        state.opened_at,
                        trade_review_id,
                    ),
                )
                if link_update.rowcount != 1:
                    raise RuntimeError(
                        f"Live link update did not affect exactly one row: {link_id}"
                    )

                if result == "LOSS":
                    state_update = connection.execute(
                        """
                        UPDATE strategy_states
                        SET consecutive_wins = 0, live_eligible = 0,
                            live_result_pending = 0, last_trade_result = 'LOSS',
                            last_trade_closed_at = ?, updated_at = ?
                        WHERE strategy_id = ?
                        """,
                        (close_time, close_time, strategy_id),
                    )
                else:
                    state_update = connection.execute(
                        """
                        UPDATE strategy_states
                        SET live_eligible = 1, live_result_pending = 0,
                            last_trade_result = 'WIN', last_trade_closed_at = ?,
                            updated_at = ?
                        WHERE strategy_id = ?
                        """,
                        (close_time, close_time, strategy_id),
                    )
                if state_update.rowcount != 1:
                    raise RuntimeError(
                        f"Strategy state update did not affect exactly one row: {strategy_id}"
                    )

                existing_cooldowns = connection.execute(
                    """
                    SELECT cooldown_until, source_trade_id
                    FROM symbol_cooldowns
                    WHERE symbol = ?
                    """,
                    (state.symbol,),
                ).fetchall()
                if len(existing_cooldowns) > 1:
                    raise RuntimeError(
                        "Expected at most one existing symbol cooldown: "
                        f"symbol={state.symbol} existing={existing_cooldowns}"
                    )
                if existing_cooldowns and existing_cooldowns[0][1] != trade_review_id:
                    try:
                        existing_until = datetime.fromisoformat(
                            str(existing_cooldowns[0][0])
                        )
                        opened_at = datetime.fromisoformat(state.opened_at)
                        if existing_until.tzinfo is None or opened_at.tzinfo is None:
                            raise ValueError("naive timestamp")
                    except (TypeError, ValueError) as exc:
                        raise RuntimeError(
                            "Existing symbol cooldown has an invalid ownership boundary: "
                            f"symbol={state.symbol} existing={existing_cooldowns}"
                        ) from exc
                    if existing_until > opened_at:
                        raise RuntimeError(
                            "Existing symbol cooldown belongs to an active different trade: "
                            f"symbol={state.symbol} existing={existing_cooldowns} "
                            f"opened_at={state.opened_at} trade_review_id={trade_review_id}"
                        )

                cooldown_already_final = (
                    review_was_final
                    and strategy_id != "N16"
                    and len(existing_cooldowns) == 1
                    and existing_cooldowns[0][1] == trade_review_id
                    and connection.execute(
                        "SELECT reason FROM symbol_cooldowns WHERE symbol = ?",
                        (state.symbol,),
                    ).fetchone()[0]
                    == exit_reason
                )
                cooldown_payload = {
                    "cooldown_until": cooldown_until_text,
                    "reason": exit_reason,
                    "source_trade_id": trade_review_id,
                }
                if not cooldown_already_final:
                    created_at_update = (
                        "created_at = excluded.created_at, "
                        if strategy_id == "N16"
                        else ""
                    )
                    cooldown_update = connection.execute(
                        ("""
                        INSERT INTO symbol_cooldowns (
                            symbol, cooldown_until, reason, source_trade_id, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(symbol) DO UPDATE SET
                            cooldown_until = excluded.cooldown_until,
                            reason = excluded.reason,
                            source_trade_id = excluded.source_trade_id,
                            """ + created_at_update + """
                            updated_at = excluded.updated_at
                        """),
                        (
                            state.symbol,
                            cooldown_until_text,
                            exit_reason,
                            trade_review_id,
                            close_time,
                            close_time,
                        ),
                    )
                    if cooldown_update.rowcount != 1:
                        raise RuntimeError(
                            "Symbol cooldown upsert did not affect exactly one row: "
                            f"symbol={state.symbol} rowcount={cooldown_update.rowcount}"
                        )
                if not authoritative_event_exists:
                    persisted_close_payload = dict(event_close_payload)
                    persisted_close_payload["trade_id"] = trade_review_id
                    connection.execute(
                        """
                        INSERT INTO events (
                            occurred_at, event_type, symbol, payload_json,
                            trade_review_id
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            close_time,
                            self._terminal_close_event_type(
                                strategy_id, state.dry_run
                            ),
                            state.symbol,
                            strict_json_dumps(persisted_close_payload),
                            trade_review_id if strategy_id == "N16" else None,
                        ),
                    )
                if not cooldown_already_final:
                    connection.execute(
                        """
                        INSERT INTO events (occurred_at, event_type, symbol, payload_json)
                        VALUES (?, ?, ?, ?)
                        """,
                        (
                            close_time,
                            "symbol_cooldown_set",
                            state.symbol,
                            strict_json_dumps(cooldown_payload),
                        ),
                    )
                return StrategyLiveFinalization(
                    trade_review_id,
                    idempotent=False,
                    pnl_amount=final_pnl_amount,
                    balance_after=final_balance_after,
                )
        except Exception as exc:
            self.logger.warning("Atomic live finalization failed: %s", exc)
            return None

    def finalize_strategy_dry_run_result(
        self,
        state: PositionState,
        strategy_id: str,
        result: str,
        exit_reason: str,
        exit_price: str,
        mark_price: str,
        pnl_amount: str,
        pnl_pct: str,
        balance_after: str,
        cooldown_until: datetime,
    ) -> StrategyLiveFinalization | None:
        """Atomically persist every N16 dry-run terminal DB edge."""

        return self.finalize_strategy_live_result(
            state=state,
            strategy_id=strategy_id,
            result=result,
            exit_reason=exit_reason,
            exit_price=exit_price,
            detail={"source": "N16_DRY_RUN_CLOSE"},
            cooldown_until=cooldown_until,
            mark_price=mark_price,
            pnl_amount=pnl_amount,
            pnl_pct=pnl_pct,
            balance_after=balance_after,
            _allow_dry_run=True,
        )

    @staticmethod
    def _orders_json_with_close(raw_orders: Any, close_payload: dict[str, Any]) -> str:
        orders = ReviewRecorder._strict_review_orders_document(raw_orders or "{}")
        if orders is None:
            raise RuntimeError("Existing trade review orders_json must be an object")
        orders["close"] = close_payload
        try:
            return strict_json_dumps(orders)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Final trade review evidence is not strict JSON") from exc

    @staticmethod
    def _terminal_close_event_type(strategy_id: str, dry_run: bool) -> str:
        if strategy_id == "N16":
            return (
                "n16_dry_run_position_closed"
                if dry_run
                else "n16_live_position_closed"
            )
        return "dry_run_position_closed" if dry_run else "live_position_closed"

    @staticmethod
    def _terminal_live_close_event_evidence(
        connection: sqlite3.Connection,
        symbol: str,
        trade_review_id: int,
        exit_reason: str,
        expected_close_payload: dict[str, Any] | None = None,
        event_type: str = "live_position_closed",
        require_n16_index: bool = False,
    ) -> tuple[int, str | None]:
        if require_n16_index:
            rows = connection.execute(
                """
                SELECT occurred_at, event_type, symbol, payload_json
                FROM events INDEXED BY idx_n16_terminal_event_trade
                WHERE trade_review_id = ?
                  AND event_type IN (
                      'n16_dry_run_position_closed',
                      'n16_live_position_closed'
                  )
                ORDER BY id ASC
                LIMIT 2
                """,
                (trade_review_id,),
            ).fetchall()
        else:
            rows = [
                (occurred_at, event_type, symbol, payload_json)
                for occurred_at, payload_json in connection.execute(
                    """
                    SELECT occurred_at,payload_json FROM events
                    WHERE event_type = ? AND symbol = ?
                    """,
                    (event_type, symbol),
                ).fetchall()
            ]
        count = 0
        matching_occurred_at = None
        for occurred_at, actual_event_type, actual_symbol, payload_json in rows:
            if require_n16_index and (
                actual_event_type != event_type or actual_symbol != symbol
            ):
                return -1, None
            payload = ReviewRecorder._strict_review_orders_document(
                payload_json
            )
            if payload is None:
                if require_n16_index:
                    return -1, None
                continue
            if payload.get("trade_id") == trade_review_id:
                if (
                    not require_n16_index
                    and payload.get("exit_reason") == "LIVE_RESULT_PENDING"
                    and exit_reason != "LIVE_RESULT_PENDING"
                ):
                    continue
                try:
                    canonical_utc_datetime(occurred_at)
                except (TypeError, ValueError):
                    return -1, None
                if expected_close_payload is not None:
                    expected_event = dict(expected_close_payload)
                    expected_event["trade_id"] = trade_review_id
                    if not ReviewRecorder._strict_json_equal(
                        payload, expected_event
                    ):
                        return -1, None
                elif payload.get("exit_reason") != exit_reason:
                    return -1, None
                count += 1
                matching_occurred_at = occurred_at
            elif require_n16_index:
                return -1, None
        return count, matching_occurred_at

    @staticmethod
    def _terminal_live_close_event_count(
        connection: sqlite3.Connection,
        symbol: str,
        trade_review_id: int,
        exit_reason: str,
        expected_close_payload: dict[str, Any] | None = None,
        event_type: str = "live_position_closed",
        require_n16_index: bool = False,
    ) -> int:
        return ReviewRecorder._terminal_live_close_event_evidence(
            connection,
            symbol,
            trade_review_id,
            exit_reason,
            expected_close_payload,
            event_type,
            require_n16_index,
        )[0]

    def inspect_strategy_live_finalization(
        self,
        state: PositionState,
        strategy_id: str,
        *,
        allow_dry_run_pending: bool = False,
    ) -> StrategyLiveFinalizationEvidence:
        if state.dry_run and not allow_dry_run_pending:
            return StrategyLiveFinalizationEvidence("BLOCKED", detail="DRY_RUN_STATE")
        try:
            with self._connect() as connection:
                reviews = connection.execute(
                    """
                    SELECT id, opened_at, symbol, side, quantity, entry_price,
                           stop_loss_price, take_profit_price, amplitude_24h_pct,
                           high_24h_price, low_24h_price, stop_loss_pct,
                           take_profit_pct, risk_amount, target_risk_amount,
                           actual_risk_amount, risk_capped_by_margin,
                           pretrade_quantity, executed_quantity,
                           final_protected_quantity, post_fill_actual_risk_amount,
                           post_fill_required_margin, reduced_after_fill,
                           notional_value, required_margin, balance, leverage,
                           dry_run, status, error, orders_json, closed_at,
                           exit_reason, exit_price, close_mark_price,
                           realized_pnl, realized_pnl_pct, balance_after_close
                    FROM trade_reviews
                    WHERE symbol = ? AND dry_run = ? AND opened_at = ?
                    ORDER BY id ASC
                    """,
                    (state.symbol, int(state.dry_run), state.opened_at),
                ).fetchall()
                links = connection.execute(
                    """
                    SELECT id, strategy_id, trade_review_id, symbol, opened_at,
                           closed_at, result, created_at
                    FROM strategy_live_links
                    WHERE symbol = ? AND opened_at = ?
                    ORDER BY id ASC
                    """,
                    (state.symbol, state.opened_at),
                ).fetchall()
                if not reviews and not links:
                    return StrategyLiveFinalizationEvidence(
                        "MISSING_OPEN_AUDIT"
                    )
                if len(reviews) != 1:
                    return StrategyLiveFinalizationEvidence(
                        "BLOCKED", detail=f"TRADE_REVIEW_COUNT={len(reviews)}"
                    )
                review = reviews[0]
                trade_review_id = review[0]
                if type(trade_review_id) is not int or trade_review_id <= 0:
                    return StrategyLiveFinalizationEvidence(
                        "BLOCKED", detail="TRADE_REVIEW_ID_INVALID"
                    )
                if not links:
                    if self._open_review_matches_state(
                        review,
                        state,
                        expected_status="OPENED",
                    ):
                        return StrategyLiveFinalizationEvidence(
                            "REVIEW_ONLY_OPENED"
                        )
                    return StrategyLiveFinalizationEvidence(
                        "BLOCKED", detail="ZERO_LINK_NON_PRISTINE_OPEN_REVIEW"
                    )
                if len(links) != 1:
                    return StrategyLiveFinalizationEvidence(
                        "BLOCKED", detail=f"GLOBAL_LIVE_LINK_COUNT={len(links)}"
                    )
                (
                    link_id,
                    linked_strategy_id,
                    linked_review_id,
                    linked_symbol,
                    linked_opened_at,
                    link_closed_at,
                    link_result,
                    link_created_at,
                ) = links[0]
                if (
                    type(link_id) is not int
                    or link_id <= 0
                    or linked_strategy_id != strategy_id
                    or linked_review_id != trade_review_id
                    or linked_symbol != state.symbol
                    or linked_opened_at != state.opened_at
                    or type(link_created_at) is not str
                    or not link_created_at
                ):
                    return StrategyLiveFinalizationEvidence(
                        "BLOCKED", detail="GLOBAL_LIVE_LINK_IDENTITY_MISMATCH"
                    )
                try:
                    opened_datetime = canonical_utc_datetime(state.opened_at)
                    link_created_datetime = canonical_utc_datetime(
                        link_created_at
                    )
                    if opened_datetime > link_created_datetime:
                        raise ValueError("live link predates its position")
                except (TypeError, ValueError):
                    return StrategyLiveFinalizationEvidence(
                        "BLOCKED", detail="GLOBAL_LIVE_LINK_CREATED_AT_INVALID"
                    )
                definitions = connection.execute(
                    "SELECT strategy_id FROM strategy_definitions WHERE strategy_id = ?",
                    (strategy_id,),
                ).fetchall()
                strategy_rows = connection.execute(
                    """
                    SELECT consecutive_wins, live_eligible, live_result_pending,
                           last_trade_result, last_trade_closed_at, updated_at
                    FROM strategy_states WHERE strategy_id = ?
                    """,
                    (strategy_id,),
                ).fetchall()
                if (
                    len(definitions) != 1
                    or definitions[0][0] != strategy_id
                    or len(strategy_rows) != 1
                ):
                    return StrategyLiveFinalizationEvidence(
                        "BLOCKED", detail="STRATEGY_REGISTRATION_OR_STATE_INVALID"
                    )
                try:
                    self._validate_live_open_claim_state(
                        state,
                        strategy_id,
                        allow_dry_run=allow_dry_run_pending,
                    )
                except Exception:
                    return StrategyLiveFinalizationEvidence(
                        "BLOCKED", detail="LIVE_STATE_ORDER_IDENTITY_INVALID"
                    )
                if review[28] in {"OPENED", "CLOSED_LIVE_RESULT_PENDING"}:
                    review_matches = self._open_review_matches_state(
                        review,
                        state,
                        expected_status=str(review[28]),
                    )
                    if link_closed_at is None and link_result is None:
                        if review_matches:
                            return StrategyLiveFinalizationEvidence("PENDING")
                        return StrategyLiveFinalizationEvidence(
                            "BLOCKED", detail="PENDING_REVIEW_IDENTITY_MISMATCH"
                        )
                    return StrategyLiveFinalizationEvidence(
                        "BLOCKED", detail="PENDING_REVIEW_LINK_CONFLICT"
                    )
                if review[28] not in {"CLOSED_STOP_LOSS", "CLOSED_TAKE_PROFIT"}:
                    return StrategyLiveFinalizationEvidence(
                        "BLOCKED", detail="UNSUPPORTED_FINAL_REVIEW_STATUS"
                    )
                plan = state.orders.get("plan")
                expected_close_values = (
                    review[32],
                    review[33],
                    review[34],
                    review[35],
                    review[36],
                    review[37],
                )
                strict_close_payload = self._strict_review_close_payload(
                    review[30],
                    expected_close_values,
                    allow_resolution_detail=True,
                )
                final_review_identity_matches = (
                    type(plan) is dict
                    and tuple(review[1:28]) == self._live_open_review_core(state, plan)
                    and review[29] is None
                    and strict_close_payload is not None
                    and self._review_orders_match_state(
                        review[30],
                        state.orders,
                        require_close=True,
                        expected_close=expected_close_values,
                        allow_resolution_detail=True,
                    )
                )
                expected_result = (
                    "LOSS" if review[28] == "CLOSED_STOP_LOSS" else "WIN"
                )
                expected_reason = (
                    "STOP_LOSS" if expected_result == "LOSS" else "TAKE_PROFIT"
                )
                _validate_terminal_financial_evidence(
                    exit_reason=review[32],
                    exit_price=review[33],
                    mark_price=review[34],
                    pnl_amount=review[35],
                    pnl_pct=review[36],
                    balance_after=review[37],
                    dry_run=state.dry_run,
                )
                terminal_close_events, terminal_event_occurred_at = (
                    self._terminal_live_close_event_evidence(
                    connection,
                    state.symbol,
                    trade_review_id,
                    expected_reason,
                    strict_close_payload,
                    self._terminal_close_event_type(
                        strategy_id, state.dry_run
                    ),
                    require_n16_index=(strategy_id == "N16"),
                    )
                )
                state_matches = (
                    int(strategy_rows[0][0]) == 0
                    and not bool(strategy_rows[0][1])
                    and not bool(strategy_rows[0][2])
                    and strategy_rows[0][3] == "LOSS"
                    if expected_result == "LOSS"
                    else (
                        bool(strategy_rows[0][1])
                        and not bool(strategy_rows[0][2])
                        and strategy_rows[0][3] == "WIN"
                    )
                )
                if link_closed_at is None and link_result is None:
                    recoverable_event_count = terminal_close_events
                    recoverable_event_time = terminal_event_occurred_at
                    recoverable_time_valid = False
                    try:
                        recoverable_closed = canonical_utc_datetime(review[31])
                        recoverable_event = canonical_utc_datetime(
                            recoverable_event_time
                        )
                        recoverable_opened = canonical_utc_datetime(state.opened_at)
                        recoverable_time_valid = (
                            review[31] == recoverable_event_time
                            and recoverable_opened
                            <= recoverable_closed
                            == recoverable_event
                        )
                    except (TypeError, ValueError):
                        recoverable_time_valid = False
                    if (
                        final_review_identity_matches
                        and review[31]
                        and review[32] == expected_reason
                        and recoverable_event_count == 1
                        and recoverable_time_valid
                    ):
                        return StrategyLiveFinalizationEvidence(
                            "RECOVERABLE_FINAL_REVIEW",
                            result=expected_result,
                            exit_reason=expected_reason,
                            exit_price=str(review[33] or ""),
                            mark_price=str(review[34] or ""),
                            pnl_amount=str(review[35] or ""),
                            pnl_pct=str(review[36] or ""),
                            balance_after=str(review[37] or ""),
                        )
                    return StrategyLiveFinalizationEvidence(
                        "BLOCKED", detail="FINAL_REVIEW_LINK_OR_STATE_CONFLICT"
                    )
                cooldown = connection.execute(
                    """
                    SELECT cooldown_until, reason, source_trade_id,
                           created_at, updated_at
                    FROM symbol_cooldowns WHERE symbol = ?
                    """,
                    (state.symbol,),
                ).fetchall()
                timestamps_match = False
                cooldown_time_valid = False
                try:
                    review_closed = canonical_utc_datetime(review[31])
                    link_closed = canonical_utc_datetime(link_closed_at)
                    event_closed = canonical_utc_datetime(
                        terminal_event_occurred_at
                    )
                    state_closed = canonical_utc_datetime(strategy_rows[0][4])
                    state_updated = canonical_utc_datetime(strategy_rows[0][5])
                    opened = canonical_utc_datetime(state.opened_at)
                    link_created = canonical_utc_datetime(link_created_at)
                    timestamps_match = (
                        review[31]
                        == link_closed_at
                        == terminal_event_occurred_at
                        == strategy_rows[0][4]
                        and opened <= link_created <= review_closed
                        and state_updated == state_closed
                        and review_closed == link_closed == event_closed == state_closed
                    )
                    if len(cooldown) == 1:
                        cooldown_until = canonical_utc_datetime(cooldown[0][0])
                        cooldown_created = canonical_utc_datetime(cooldown[0][3])
                        cooldown_updated = canonical_utc_datetime(cooldown[0][4])
                        cooldown_time_valid = (
                            cooldown_until > review_closed
                            and cooldown_created == review_closed
                            and cooldown_updated == review_closed
                        )
                except (TypeError, ValueError):
                    timestamps_match = False
                    cooldown_time_valid = False
                if (
                    not final_review_identity_matches
                    or not review[31]
                    or review[32] != expected_reason
                    or link_closed_at is None
                    or link_result != expected_result
                    or not state_matches
                    or not timestamps_match
                    or len(cooldown) != 1
                    or cooldown[0][1] != expected_reason
                    or cooldown[0][2] != trade_review_id
                    or not cooldown_time_valid
                    or terminal_close_events != 1
                ):
                    return StrategyLiveFinalizationEvidence(
                        "BLOCKED", detail="FINAL_EVIDENCE_INCOMPLETE_OR_CONFLICTING"
                    )
                return StrategyLiveFinalizationEvidence(
                    "FINAL",
                    finalization=StrategyLiveFinalization(
                        trade_review_id,
                        idempotent=True,
                        pnl_amount=str(review[35] or ""),
                        balance_after=str(review[37] or ""),
                    ),
                )
        except Exception as exc:
            self.logger.warning("Unable to inspect strategy live finalization: %s", exc)
            return StrategyLiveFinalizationEvidence("BLOCKED", detail=str(exc))

    def record_signal(
        self,
        scan_id: int | None,
        candidate: FundingCandidate,
        result: AnalysisResult,
        decision: str,
        error: str | None = None,
    ) -> int | None:
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    INSERT INTO signal_reviews (
                        scan_id, reviewed_at, symbol, funding_rate, mark_price,
                        trend_slope, pattern, current_bullish, passed, decision, detail, error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        scan_id,
                        utc_now(),
                        candidate.symbol,
                        str(candidate.funding_rate),
                        str(candidate.mark_price),
                        str(result.trend_slope),
                        result.pattern,
                        int(result.current_bullish),
                        int(result.passed),
                        decision,
                        result.detail,
                        error,
                    ),
                )
                legacy_review_id = int(cursor.lastrowid)

            # The legacy single-strategy table remains available for its
            # existing diagnostics, but it is not a publication boundary.
            # Every legacy decision must also enter the ordinary signal batch
            # so main can atomically publish the complete round before an
            # order is attempted.  This path is intentionally identified as
            # LEGACY_SINGLE rather than N01: the legacy analyzer permits the
            # historical A/B/C pattern set, while formal N01 has its own
            # strategy contract.
            signal_id = self.record_strategy_signal(
                scan_id=scan_id,
                strategy_id=_LEGACY_SINGLE_STRATEGY_ID,
                symbol=candidate.symbol,
                funding_rate=(
                    str(candidate.funding_rate)
                    if candidate.funding_rate is not None
                    else ""
                ),
                matched_patterns=result.matched_patterns,
                trend_slope=str(result.trend_slope),
                current_bullish=bool(result.current_bullish),
                passed=bool(result.passed),
                decision=decision,
                reason=decision,
                detail={
                    "legacy_signal_review_id": legacy_review_id,
                    "analysis_detail": result.detail,
                    "pattern": result.pattern,
                    "mark_price": str(candidate.mark_price),
                    "candidate_universe": candidate.candidate_universe,
                    "quote_volume": (
                        str(candidate.quote_volume)
                        if candidate.quote_volume is not None
                        else None
                    ),
                    "quote_volume_rank": candidate.quote_volume_rank,
                },
            )
            if signal_id is None:
                raise RuntimeError(
                    "legacy signal did not enter the strategy signal batch"
                )
            return signal_id
        except Exception as exc:
            self.logger.warning("Review DB write failed while recording signal: %s", exc)
            return None

    def record_trade_open(self, scan_id: int | None, state: PositionState) -> int | None:
        try:
            plan = state.orders.get("plan", {})
            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    INSERT INTO trade_reviews (
                        scan_id, opened_at, symbol, side, quantity, entry_price,
                        stop_loss_price, take_profit_price, amplitude_24h_pct,
                        high_24h_price, low_24h_price, stop_loss_pct,
                        take_profit_pct, risk_amount, target_risk_amount,
                        actual_risk_amount, risk_capped_by_margin, pretrade_quantity,
                        executed_quantity, final_protected_quantity,
                        post_fill_actual_risk_amount, post_fill_required_margin,
                        reduced_after_fill, notional_value, required_margin, balance,
                        leverage, dry_run, status, error, orders_json
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (
                        scan_id,
                        state.opened_at,
                        state.symbol,
                        "BUY",
                        state.quantity,
                        state.entry_price,
                        state.stop_loss_price,
                        state.take_profit_price,
                        plan.get("amplitude_24h_pct"),
                        plan.get("high_24h_price"),
                        plan.get("low_24h_price"),
                        plan.get("stop_loss_pct"),
                        plan.get("take_profit_pct"),
                        plan.get("risk_amount"),
                        plan.get("target_risk_amount"),
                        plan.get("actual_risk_amount"),
                        int(bool(plan.get("risk_capped_by_margin"))),
                        plan.get("pretrade_quantity"),
                        plan.get("executed_quantity"),
                        plan.get("final_protected_quantity"),
                        plan.get("post_fill_actual_risk_amount"),
                        plan.get("post_fill_required_margin"),
                        int(bool(plan.get("reduced_after_fill"))),
                        plan.get("notional_value"),
                        plan.get("required_margin"),
                        plan.get("balance"),
                        state.leverage,
                        int(state.dry_run),
                        "OPENED",
                        None,
                        json_dumps(state.orders),
                    ),
                )
                return int(cursor.lastrowid)
        except Exception as exc:
            self.logger.warning("Review DB write failed while recording trade open: %s", exc)
            return None

    def record_trade_failure(self, scan_id: int | None, symbol: str, error: str, dry_run: bool) -> None:
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO trade_reviews (
                        scan_id, opened_at, symbol, side, dry_run, status, error, orders_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        scan_id,
                        utc_now(),
                        symbol,
                        "BUY",
                        int(dry_run),
                        "FAILED",
                        error,
                        "{}",
                    ),
                )
        except Exception as exc:
            self.logger.warning("Review DB write failed while recording trade failure: %s", exc)

    def record_trade_close(
        self,
        state: PositionState,
        exit_reason: str,
        exit_price: str,
        mark_price: str,
        pnl_amount: str,
        pnl_pct: str,
        balance_after: str,
        detail: dict[str, Any] | None = None,
    ) -> int | None:
        try:
            if detail is not None and type(detail) is not dict:
                raise ValueError("trade close detail must be a built-in object")
            closed_at = utc_now()
            status = f"CLOSED_{exit_reason}"
            payload = {
                "exit_reason": exit_reason,
                "exit_price": exit_price,
                "mark_price": mark_price,
                "pnl_amount": pnl_amount,
                "pnl_pct": pnl_pct,
                "balance_after": balance_after,
            }
            if detail is not None:
                payload["resolution_detail"] = detail
            # Reject non-finite values and implicit string coercion before the
            # first transaction.  A partially written close must never become
            # unreadable to the strict recovery inspector.
            closed_datetime = canonical_utc_datetime(closed_at)
            opened_datetime = canonical_utc_datetime(state.opened_at)
            if opened_datetime > closed_datetime:
                raise ValueError("trade close precedes the position open time")
            strict_json_dumps(payload)
            strict_json_dumps(state.orders)
            if exit_reason in {"STOP_LOSS", "TAKE_PROFIT"}:
                _validate_terminal_financial_evidence(
                    exit_reason=exit_reason,
                    exit_price=exit_price,
                    mark_price=mark_price,
                    pnl_amount=pnl_amount,
                    pnl_pct=pnl_pct,
                    balance_after=balance_after,
                    dry_run=state.dry_run,
                )
            strategy_payload = state.orders.get("strategy")
            is_n16_terminal = (
                exit_reason != "LIVE_RESULT_PENDING"
                and type(strategy_payload) is dict
                and strategy_payload.get("strategy_id") == "N16"
            )
            if is_n16_terminal:
                self._validate_live_open_claim_state(
                    state,
                    "N16",
                    allow_dry_run=True,
                )
                self._n16_execution_claim_components(
                    strict_json_dumps(state.orders),
                    nested_strategy=True,
                )
            with self._connect() as connection:
                if is_n16_terminal:
                    self._attest_n16_live_execution_identity(
                        connection,
                        state,
                    )
                rows = connection.execute(
                    """
                    SELECT id, status, exit_reason, exit_price, close_mark_price,
                           realized_pnl, realized_pnl_pct, balance_after_close,
                           orders_json, closed_at
                    FROM trade_reviews
                    WHERE symbol = ? AND dry_run = ? AND opened_at = ?
                    ORDER BY id DESC
                    """,
                    (state.symbol, int(state.dry_run), state.opened_at),
                ).fetchall()
                if len(rows) > 1:
                    raise RuntimeError(
                        "Ambiguous close audit rows for "
                        f"{state.symbol} opened_at={state.opened_at}"
                    )
                row = rows[0] if rows else None
                if is_n16_terminal:
                    if row is None:
                        raise RuntimeError(
                            "N16 terminal close requires an existing live-open review"
                        )
                    live_links = connection.execute(
                        """
                        SELECT id, strategy_id, trade_review_id, symbol,
                               opened_at, closed_at, result
                        FROM strategy_live_links
                        WHERE symbol=? AND opened_at=?
                        ORDER BY id
                        """,
                        (state.symbol, state.opened_at),
                    ).fetchall()
                    if (
                        len(live_links) != 1
                        or type(live_links[0][0]) is not int
                        or live_links[0][0] <= 0
                        or live_links[0][1] != "N16"
                        or live_links[0][2] != row[0]
                        or live_links[0][3] != state.symbol
                        or live_links[0][4] != state.opened_at
                        or live_links[0][5] is not None
                        or live_links[0][6] is not None
                    ):
                        raise RuntimeError(
                            "N16 terminal close lacks an exact active live link"
                        )
                if row is None:
                    cursor = connection.execute(
                        """
                        INSERT INTO trade_reviews (
                            opened_at, symbol, side, quantity, entry_price,
                            stop_loss_price, take_profit_price, leverage, dry_run,
                            status, closed_at, exit_reason, exit_price,
                            close_mark_price, realized_pnl, realized_pnl_pct,
                            balance_after_close, orders_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            state.opened_at,
                            state.symbol,
                            "BUY",
                            state.quantity,
                            state.entry_price,
                            state.stop_loss_price,
                            state.take_profit_price,
                            state.leverage,
                            int(state.dry_run),
                            status,
                            closed_at,
                            exit_reason,
                            exit_price,
                            mark_price,
                            pnl_amount,
                            pnl_pct,
                            balance_after,
                            strict_json_dumps(
                                {"close": payload, "state_orders": state.orders}
                            ),
                        ),
                    )
                    trade_id = int(cursor.lastrowid)
                elif row[1] in {"OPENED", "CLOSED_LIVE_RESULT_PENDING"}:
                    trade_id = int(row[0])
                    orders = self._strict_review_orders_document(row[8] or "{}")
                    if orders is None:
                        raise RuntimeError("Existing close Review JSON is invalid")
                    orders["close"] = payload
                    connection.execute(
                        """
                        UPDATE trade_reviews
                        SET status = ?, closed_at = ?, exit_reason = ?, exit_price = ?,
                            close_mark_price = ?, realized_pnl = ?, realized_pnl_pct = ?,
                            balance_after_close = ?, orders_json = ?
                        WHERE id = ?
                        """,
                        (
                            status,
                            closed_at,
                            exit_reason,
                            exit_price,
                            mark_price,
                            pnl_amount,
                            pnl_pct,
                            balance_after,
                            strict_json_dumps(orders),
                            row[0],
                        ),
                    )
                else:
                    existing_close = (
                        row[1], row[2], row[3], row[4], row[5], row[6], row[7]
                    )
                    requested_close = (
                        status,
                        exit_reason,
                        exit_price,
                        mark_price,
                        pnl_amount,
                        pnl_pct,
                        balance_after,
                    )
                    if existing_close != requested_close:
                        raise RuntimeError(
                            "Conflicting final close audit for "
                            f"{state.symbol} opened_at={state.opened_at}: "
                            f"existing={existing_close} requested={requested_close}"
                        )
                    if is_n16_terminal:
                        stored_close_payload = self._strict_review_close_payload(
                            row[8],
                            tuple(row[2:8]),
                            allow_resolution_detail=True,
                        )
                        _validate_terminal_financial_evidence(
                            exit_reason=row[2],
                            exit_price=row[3],
                            mark_price=row[4],
                            pnl_amount=row[5],
                            pnl_pct=row[6],
                            balance_after=row[7],
                            dry_run=state.dry_run,
                        )
                        event_count, event_time = (
                            self._terminal_live_close_event_evidence(
                                connection,
                                state.symbol,
                                int(row[0]),
                                row[2],
                                stored_close_payload,
                                self._terminal_close_event_type(
                                    "N16", state.dry_run
                                ),
                                require_n16_index=True,
                            )
                        )
                        if (
                            stored_close_payload is None
                            or event_count != 1
                            or row[9] != event_time
                            or canonical_utc_datetime(row[9])
                            != canonical_utc_datetime(event_time)
                        ):
                            raise RuntimeError(
                                "N16 terminal close replay evidence is inconsistent"
                            )
                    return int(row[0])
                payload["trade_id"] = trade_id
                event_type = self._terminal_close_event_type(
                    "N16" if is_n16_terminal else "",
                    state.dry_run,
                )
                connection.execute(
                    "INSERT INTO events (occurred_at, event_type, symbol, payload_json, trade_review_id) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        closed_at,
                        event_type,
                        state.symbol,
                        strict_json_dumps(payload),
                        trade_id if is_n16_terminal else None,
                    ),
                )
                return trade_id
        except Exception as exc:
            self.logger.warning("Review DB write failed while recording trade close: %s", exc)
            return None

    def set_symbol_cooldown(
        self,
        symbol: str,
        cooldown_until: datetime,
        reason: str,
        source_trade_id: int | None,
    ) -> bool:
        try:
            now = utc_now()
            cooldown_until_text = cooldown_until.astimezone(timezone.utc).isoformat()
            payload = {
                "cooldown_until": cooldown_until_text,
                "reason": reason,
                "source_trade_id": source_trade_id,
            }
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO symbol_cooldowns (
                        symbol, cooldown_until, reason, source_trade_id, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        cooldown_until = excluded.cooldown_until,
                        reason = excluded.reason,
                        source_trade_id = excluded.source_trade_id,
                        updated_at = excluded.updated_at
                    """,
                    (symbol, cooldown_until_text, reason, source_trade_id, now, now),
                )
                connection.execute(
                    "INSERT INTO events (occurred_at, event_type, symbol, payload_json) VALUES (?, ?, ?, ?)",
                    (now, "symbol_cooldown_set", symbol, json_dumps(payload)),
                )
                return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while setting symbol cooldown: %s", exc)
            return False

    def active_symbol_cooldown(
        self,
        symbol: str,
        now: datetime | None = None,
    ) -> SymbolCooldown | None:
        return self.active_symbol_cooldowns((symbol,), now).get(symbol)

    def active_symbol_cooldowns(
        self,
        symbols: Iterable[str],
        now: datetime | None = None,
    ) -> dict[str, SymbolCooldown]:
        """Read one round's global cooldowns from one authenticated snapshot."""

        requested = tuple(sorted(set(symbols)))
        if any(type(symbol) is not str or not symbol for symbol in requested):
            raise ValueError("symbol cooldown snapshot identity is invalid")
        if not requested:
            return {}
        now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        rows: list[sqlite3.Row | tuple[Any, ...]] = []
        with self._read_only_runtime_snapshot() as connection:
            # The snapshot helper pins one query-only SQLite read view.  Every
            # chunk therefore observes the same cooldown generation.
            # Keep every statement comfortably below SQLite's conservative
            # host-parameter limit while retaining a single authenticated
            # connection and snapshot for the whole scheduler round.
            for offset in range(0, len(requested), 500):
                chunk = requested[offset : offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                rows.extend(
                    connection.execute(
                        """
                        SELECT symbol, cooldown_until, reason, source_trade_id,
                               created_at, updated_at
                        FROM symbol_cooldowns
                        WHERE symbol IN (%s)
                        """
                        % placeholders,
                        chunk,
                    ).fetchall()
                )
        active: dict[str, SymbolCooldown] = {}
        for row in rows:
            symbol = str(row[0])
            cooldown_until = parse_utc_datetime(str(row[1]))
            if cooldown_until <= now_utc:
                continue
            active[symbol] = SymbolCooldown(
                symbol=symbol,
                cooldown_until=str(row[1]),
                reason=str(row[2]),
                source_trade_id=(
                    int(row[3]) if row[3] is not None else None
                ),
                created_at=str(row[4]),
                updated_at=str(row[5]),
            )
        return active

    def record_event(self, event_type: str, payload: dict[str, Any] | None = None, symbol: str | None = None) -> bool:
        try:
            clean_payload = payload or {}
            with self._connect() as connection:
                connection.execute(
                    "INSERT INTO events (occurred_at, event_type, symbol, payload_json) VALUES (?, ?, ?, ?)",
                    (utc_now(), event_type, symbol, json_dumps(clean_payload)),
                )
            return True
        except Exception as exc:
            self.logger.warning("Review DB write failed while recording event: %s", exc)
            return False

    def latest_summary(self) -> dict[str, Any]:
        with self._connect() as connection:
            scan_count = connection.execute("SELECT COUNT(*) FROM scans").fetchone()[0]
            signal_count = connection.execute("SELECT COUNT(*) FROM signal_reviews").fetchone()[0]
            trade_count = connection.execute("SELECT COUNT(*) FROM trade_reviews").fetchone()[0]
            latest_scan = connection.execute(
                "SELECT id, started_at, scanned_count, candidate_count, opened FROM scans ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return {
            "scan_count": scan_count,
            "signal_count": signal_count,
            "trade_count": trade_count,
            "latest_scan": tuple(latest_scan) if latest_scan else None,
        }
