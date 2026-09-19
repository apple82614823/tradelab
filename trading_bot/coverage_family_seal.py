from __future__ import annotations

import hashlib
import heapq
import json
import sqlite3
import struct
import tempfile
from dataclasses import dataclass
from typing import Any, BinaryIO, Iterable, Optional, Sequence


FAMILY_SEAL_SCHEMA_VERSION = 5
FAMILY_SEAL_RULE_VERSION = "N19_PROTECTED_GENERATION_V5"
AUTHORIZED_LEGACY_DOMAIN = "AUTHORIZED_LEGACY_V3_UNBOUND"
PROVED_TERMINAL_DOMAIN = "NORMAL_TERMINAL_RECEIPT"
# Public-source synthetic fixture identity only.  It must not be used with a
# private production database and does not change strategy parameters,
# trading behavior, or the generic fail-closed checks below.
AUTHORIZED_SYMBOL = "EXAMPLEUSDT"
AUTHORIZED_FAMILY_ID = "0123456789abcdef01234567"
AUTHORIZED_STRUCTURE_ID = "89abcdef0123456789abcdef"
AUTHORIZED_STATEMENT_SHA256 = (
    "2dd9bf0e9a19d4464ff53ddb9bba35a850306499d12d88a34257541dcd0b04a1"
)
AUTHORIZED_GAP_REASON = "V3_TERMINAL_PUBLICATION_BINDING_NOT_PERSISTED"


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _valid_hash(value: Any) -> bool:
    return bool(
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _typed_value(value: Any) -> Any:
    if value is None:
        return {"type": "null", "value": None}
    if type(value) is int:
        return {"type": "int", "value": str(value)}
    if type(value) is float:
        return {"type": "float", "value": value.hex()}
    if type(value) is str:
        return {"type": "str", "value": value}
    if type(value) is bytes:
        return {"type": "bytes", "value": value.hex()}
    raise RuntimeError("Review canonical value type is unsupported")


def typed_row_sha256(row: Sequence[Any]) -> str:
    if type(row) not in (tuple, list):
        raise RuntimeError("typed Review row is invalid")
    return _sha256_json([_typed_value(value) for value in row])


def typed_row_json(row: Sequence[Any]) -> str:
    if type(row) not in (tuple, list):
        raise RuntimeError("typed Review row is invalid")
    return _canonical_json([_typed_value(value) for value in row])


def review_catalog_sha256(connection: sqlite3.Connection) -> str:
    rows = tuple(
        tuple(row)
        for row in connection.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name,tbl_name"
        )
    )
    if any(
        len(row) != 4
        or any(type(value) is not str for value in row)
        for row in rows
    ):
        raise RuntimeError("Review catalog snapshot is invalid")
    return _sha256_json(
        {
            "application_id": connection.execute(
                "PRAGMA application_id"
            ).fetchone()[0],
            "user_version": connection.execute(
                "PRAGMA user_version"
            ).fetchone()[0],
            "objects": [
                [row[0], row[1], row[2], _normalized_sql(row[3])]
                for row in rows
            ],
        }
    )


@dataclass(frozen=True)
class ReviewSnapshotDigests:
    full_snapshot_sha256: str
    canonical_sha256: str
    catalog_sha256: str


def _write_snapshot_digest_chunk(
    values: list[bytes],
) -> BinaryIO:
    values.sort()
    stream = tempfile.TemporaryFile(
        mode="w+b",
        prefix="review-digest-chunk-",
    )
    try:
        for value in values:
            if len(value) != 32 or stream.write(value) != 32:
                raise RuntimeError("Review snapshot digest spool is invalid")
        stream.flush()
        stream.seek(0)
        return stream
    except BaseException:
        stream.close()
        raise


def _iter_snapshot_digest_chunk(stream: BinaryIO):
    stream.seek(0)
    while True:
        value = stream.read(32)
        if not value:
            return
        if len(value) != 32:
            raise RuntimeError("Review snapshot digest spool is truncated")
        yield value


def review_snapshot_digests(
    connection: sqlite3.Connection,
) -> ReviewSnapshotDigests:
    """Return exact Review commitments with bounded resident memory.

    The old implementation accumulated every typed row JSON string and then
    serialized the entire database a second time.  This version commits each
    row to a fixed 32-byte type-sensitive digest, sorts bounded chunks on a
    private file-backed spool, and merges those chunks into a domain-separated
    table multiset commitment.  The canonical digest is computed during the
    same source scan, preserving its existing rowid-ordered contract.
    """

    tables = tuple(
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    )
    if any(type(table) is not str or not table for table in tables):
        raise RuntimeError("Review table snapshot is invalid")
    catalog_sha256 = review_catalog_sha256(connection)
    full_digest = hashlib.sha256()
    full_digest.update(b"REVIEW_FULL_SNAPSHOT_V2\x00")
    full_digest.update(bytes.fromhex(catalog_sha256))
    full_digest.update(struct.pack(">Q", len(tables)))
    canonical_digest = hashlib.sha256()
    chunk_limit = 4096
    for table in tables:
        escaped = table.replace('"', '""')
        table_bytes = table.encode("utf-8")
        full_digest.update(struct.pack(">Q", len(table_bytes)))
        full_digest.update(table_bytes)
        canonical_digest.update(
            ("TABLE|" + table + "\n").encode("utf-8")
        )
        chunks: list[BinaryIO] = []
        try:
            values: list[bytes] = []
            row_count = 0
            for raw_row in connection.execute(
                'SELECT * FROM "%s" ORDER BY rowid' % escaped
            ):
                row = tuple(raw_row)
                canonical_digest.update(
                    typed_row_sha256(row).encode("ascii")
                )
                canonical_digest.update(b"\n")
                values.append(
                    bytes.fromhex(
                        typed_row_sha256(
                            _review_snapshot_row(table, row)
                        )
                    )
                )
                row_count += 1
                if len(values) == chunk_limit:
                    chunks.append(_write_snapshot_digest_chunk(values))
                    values = []
            if values:
                chunks.append(_write_snapshot_digest_chunk(values))
            full_digest.update(struct.pack(">Q", row_count))
            iterators = [
                _iter_snapshot_digest_chunk(stream) for stream in chunks
            ]
            merged = heapq.merge(*iterators) if iterators else ()
            merged_count = 0
            for value in merged:
                full_digest.update(value)
                merged_count += 1
            if merged_count != row_count:
                raise RuntimeError("Review snapshot row count changed")
        finally:
            for stream in chunks:
                stream.close()
    return ReviewSnapshotDigests(
        full_snapshot_sha256=full_digest.hexdigest(),
        canonical_sha256=canonical_digest.hexdigest(),
        catalog_sha256=catalog_sha256,
    )


def review_full_snapshot_sha256(connection: sqlite3.Connection) -> str:
    return review_snapshot_digests(connection).full_snapshot_sha256


def _review_snapshot_row(
    table: str, row: tuple[Any, ...]
) -> tuple[Any, ...]:
    catalog_indexes = {
        "n16_lifecycle_guard": 4,
        "strategy_lifecycle_installations": 5,
        "history_coverage_family_seal_installation": 3,
    }
    index = catalog_indexes.get(table)
    if index is None:
        return row
    if len(row) <= index or type(row[index]) is not int or row[index] <= 0:
        raise RuntimeError("Review catalog generation row is invalid")
    normalized = list(row)
    normalized[index] = 0
    return tuple(normalized)


def _untyped_row(value: Any) -> tuple[Any, ...]:
    if type(value) is not list:
        raise RuntimeError("typed Review row JSON is invalid")
    result: list[Any] = []
    for item in value:
        if (
            type(item) is not dict
            or set(item) != {"type", "value"}
            or type(item["type"]) is not str
        ):
            raise RuntimeError("typed Review row value is invalid")
        kind = item["type"]
        raw = item["value"]
        if kind == "null" and raw is None:
            result.append(None)
        elif kind == "int" and type(raw) is str:
            parsed = int(raw)
            if str(parsed) != raw:
                raise RuntimeError("typed Review integer is not canonical")
            result.append(parsed)
        elif kind == "float" and type(raw) is str:
            result.append(float.fromhex(raw))
        elif kind == "str" and type(raw) is str:
            result.append(raw)
        elif kind == "bytes" and type(raw) is str:
            result.append(bytes.fromhex(raw))
        else:
            raise RuntimeError("typed Review row type is unsupported")
    return tuple(result)


def review_canonical_sha256(
    connection: sqlite3.Connection,
    *,
    excluded_tables: Iterable[str] = (),
) -> str:
    excluded = set(excluded_tables)
    tables = [
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name COLLATE BINARY"
        )
        if row[0] not in excluded
    ]
    digest = hashlib.sha256()
    for table in tables:
        if type(table) is not str:
            raise RuntimeError("Review canonical table name is invalid")
        escaped = table.replace('"', '""')
        digest.update(("TABLE|" + table + "\n").encode("utf-8"))
        for row in connection.execute(
            'SELECT * FROM "%s" ORDER BY rowid' % escaped
        ):
            digest.update(typed_row_sha256(tuple(row)).encode("ascii"))
            digest.update(b"\n")
    return digest.hexdigest()


def ordered_v3_receipt_set_sha256(
    rows: Iterable[Sequence[Any]],
) -> str:
    digest = hashlib.sha256()
    digest.update(b"V3_RECEIPT_MULTISET_V2\x00")
    count = 0
    chunks: list[BinaryIO] = []
    try:
        values: list[bytes] = []
        for raw_row in rows:
            values.append(
                bytes.fromhex(typed_row_sha256(tuple(raw_row)))
            )
            count += 1
            if len(values) == 4096:
                chunks.append(_write_snapshot_digest_chunk(values))
                values = []
        if values:
            chunks.append(_write_snapshot_digest_chunk(values))
        iterators = [
            _iter_snapshot_digest_chunk(stream) for stream in chunks
        ]
        merged = heapq.merge(*iterators) if iterators else ()
        merged_count = 0
        for value in merged:
            digest.update(value)
            merged_count += 1
        if merged_count != count:
            raise RuntimeError("v3 receipt digest count changed")
    finally:
        for stream in chunks:
            stream.close()
    digest.update(struct.pack(">Q", count))
    return digest.hexdigest()


def coverage_graph_sha256(
    chain_rows: Sequence[Sequence[Any]],
    head_rows: Sequence[Sequence[Any]],
    installation_row: Sequence[Any],
) -> str:
    return _sha256_json(
        {
            "chain": [
                [_typed_value(value) for value in tuple(row)]
                for row in chain_rows
            ],
            "heads": [
                [_typed_value(value) for value in tuple(row)]
                for row in head_rows
            ],
            "installation": [
                _typed_value(value) for value in tuple(installation_row)
            ],
        }
    )


@dataclass(frozen=True)
class AuthorizedLegacyWitnessPlan:
    symbol: str
    family_id: str
    structure_id: str
    terminal_evidence_sha256: str
    state_row_sha256: str
    receipt_count: int
    receipt_set_sha256: str
    coverage_graph_sha256: str
    coverage_catalog_sha256: str
    review_canonical_sha256: str
    authorization_sha256: str
    review_plan_sha256: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "family_id": self.family_id,
            "structure_id": self.structure_id,
            "terminal_evidence_sha256": self.terminal_evidence_sha256,
            "state_row_sha256": self.state_row_sha256,
            "receipt_count": self.receipt_count,
            "receipt_set_sha256": self.receipt_set_sha256,
            "coverage_graph_sha256": self.coverage_graph_sha256,
            "coverage_catalog_sha256": self.coverage_catalog_sha256,
            "review_canonical_sha256": self.review_canonical_sha256,
            "authorization_sha256": self.authorization_sha256,
            "review_plan_sha256": self.review_plan_sha256,
        }


def authorized_witness_plan_sha256(
    symbol: str,
    family_id: str,
    structure_id: str,
    terminal_evidence_sha256: str,
    state_row_sha256: str,
    receipt_count: int,
    receipt_set_sha256: str,
    graph_sha256: str,
    catalog_sha256: str,
    canonical_review_sha256: str,
    authorization_sha256: str,
) -> str:
    return _sha256_json(
        {
            "rule_version": FAMILY_SEAL_RULE_VERSION,
            "domain": AUTHORIZED_LEGACY_DOMAIN,
            "symbol": symbol,
            "family_id": family_id,
            "structure_id": structure_id,
            "terminal_evidence_sha256": terminal_evidence_sha256,
            "state_row_sha256": state_row_sha256,
            "receipt_count": receipt_count,
            "receipt_set_sha256": receipt_set_sha256,
            "coverage_graph_sha256": graph_sha256,
            "coverage_catalog_sha256": catalog_sha256,
            "review_canonical_sha256": canonical_review_sha256,
            "authorization_sha256": authorization_sha256,
            "gap_reason": AUTHORIZED_GAP_REASON,
        }
    )


def authorized_witness_sha256(
    plan: AuthorizedLegacyWitnessPlan,
    terminal_evidence_json: str,
) -> str:
    return _sha256_json(
        {
            **plan.to_jsonable(),
            "terminal_evidence_json_sha256": hashlib.sha256(
                terminal_evidence_json.encode("utf-8")
            ).hexdigest(),
            "gap_reason": AUTHORIZED_GAP_REASON,
        }
    )


def family_seal_sha256(
    symbol: str,
    family_id: str,
    structure_id: str,
    evidence_sha256: str,
    proof_domain: str,
    proof_sha256: str,
) -> str:
    return _sha256_json(
        {
            "rule_version": FAMILY_SEAL_RULE_VERSION,
            "strategy_id": "N19",
            "symbol": symbol,
            "family_id": family_id,
            "structure_id": structure_id,
            "terminal_evidence_sha256": evidence_sha256,
            "proof_domain": proof_domain,
            "proof_sha256": proof_sha256,
        }
    )


def terminal_bundle_sha256(
    source_scan_id: int,
    symbol: str,
    family_id: str,
    structure_id: str,
    prior_evidence_sha256: str,
    terminal_evidence_json: str,
    terminal_evidence_sha256: str,
    batch_expected_count: int,
    batch_manifest_sha256: str,
    source_start_time_ms: int,
    covered_through_time_ms: int,
    source_sha256: str,
    publication_ordinal: int,
    previous_receipt_sha256: Optional[str],
    coverage_receipt_sha256: str,
    result_epoch_ordinal: int,
    result_epoch_start_time_ms: int,
    result_chain_head_sha256: str,
    terminal_receipt_sha256: str,
    terminal_binding_sha256: str,
    family_seal_sha256_value: str,
) -> str:
    return _sha256_json(
        {
            "rule_version": FAMILY_SEAL_RULE_VERSION,
            "source_scan_id": source_scan_id,
            "strategy_id": "N19",
            "symbol": symbol,
            "family_id": family_id,
            "structure_id": structure_id,
            "prior_evidence_sha256": prior_evidence_sha256,
            "terminal_evidence_json": terminal_evidence_json,
            "terminal_evidence_sha256": terminal_evidence_sha256,
            "batch_expected_count": batch_expected_count,
            "batch_manifest_sha256": batch_manifest_sha256,
            "source_start_time_ms": source_start_time_ms,
            "covered_through_time_ms": covered_through_time_ms,
            "source_sha256": source_sha256,
            "publication_ordinal": publication_ordinal,
            "previous_receipt_sha256": previous_receipt_sha256,
            "coverage_receipt_sha256": coverage_receipt_sha256,
            "result_epoch_ordinal": result_epoch_ordinal,
            "result_epoch_start_time_ms": result_epoch_start_time_ms,
            "result_chain_head_sha256": result_chain_head_sha256,
            "terminal_receipt_sha256": terminal_receipt_sha256,
            "terminal_binding_sha256": terminal_binding_sha256,
            "family_seal_sha256": family_seal_sha256_value,
        }
    )


FAMILY_SEAL_TABLE_SQL = """
CREATE TABLE history_coverage_n19_family_seals (
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N19'),
    symbol TEXT NOT NULL CHECK(length(symbol) BETWEEN 5 AND 32),
    family_id TEXT NOT NULL CHECK(
        length(family_id) = 24 AND family_id NOT GLOB '*[^0-9a-f]*'
    ),
    structure_id TEXT NOT NULL CHECK(
        length(structure_id) = 24 AND structure_id NOT GLOB '*[^0-9a-f]*'
    ),
    terminal_evidence_sha256 TEXT NOT NULL CHECK(
        length(terminal_evidence_sha256) = 64
        AND terminal_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    proof_domain TEXT NOT NULL CHECK(
        proof_domain IN (
            'NORMAL_TERMINAL_RECEIPT',
            'AUTHORIZED_LEGACY_V3_UNBOUND'
        )
    ),
    proof_sha256 TEXT NOT NULL UNIQUE CHECK(
        length(proof_sha256) = 64
        AND proof_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    seal_sha256 TEXT NOT NULL UNIQUE CHECK(
        length(seal_sha256) = 64
        AND seal_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    sealed_at TEXT NOT NULL,
    PRIMARY KEY(strategy_id,symbol,family_id)
)
""".strip()


LEGACY_WITNESS_TABLE_SQL = """
CREATE TABLE history_coverage_n19_legacy_unbound_witnesses (
    witness_id INTEGER PRIMARY KEY CHECK(
        typeof(witness_id) = 'integer' AND witness_id = 1
    ),
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N19'),
    symbol TEXT NOT NULL CHECK(symbol = 'EXAMPLEUSDT'),
    family_id TEXT NOT NULL CHECK(family_id = '0123456789abcdef01234567'),
    structure_id TEXT NOT NULL CHECK(structure_id = '89abcdef0123456789abcdef'),
    terminal_stage TEXT NOT NULL CHECK(terminal_stage = 'MISSED'),
    terminal_reason TEXT NOT NULL CHECK(
        terminal_reason = 'N19_HISTORICAL_ENTRY_MISSED'
    ),
    terminal_evidence_json TEXT NOT NULL,
    terminal_evidence_sha256 TEXT NOT NULL CHECK(
        length(terminal_evidence_sha256) = 64
        AND terminal_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    state_row_sha256 TEXT NOT NULL CHECK(
        length(state_row_sha256) = 64
        AND state_row_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    v3_receipt_count INTEGER NOT NULL CHECK(
        typeof(v3_receipt_count) = 'integer' AND v3_receipt_count > 0
    ),
    v3_receipt_set_sha256 TEXT NOT NULL CHECK(
        length(v3_receipt_set_sha256) = 64
        AND v3_receipt_set_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    v3_coverage_graph_sha256 TEXT NOT NULL CHECK(
        length(v3_coverage_graph_sha256) = 64
        AND v3_coverage_graph_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    v3_catalog_sha256 TEXT NOT NULL CHECK(
        length(v3_catalog_sha256) = 64
        AND v3_catalog_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    pre_review_canonical_sha256 TEXT NOT NULL CHECK(
        length(pre_review_canonical_sha256) = 64
        AND pre_review_canonical_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    review_plan_sha256 TEXT NOT NULL UNIQUE CHECK(
        length(review_plan_sha256) = 64
        AND review_plan_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    authorization_sha256 TEXT NOT NULL CHECK(
        authorization_sha256 =
        '2dd9bf0e9a19d4464ff53ddb9bba35a850306499d12d88a34257541dcd0b04a1'
    ),
    gap_reason TEXT NOT NULL CHECK(
        gap_reason = 'V3_TERMINAL_PUBLICATION_BINDING_NOT_PERSISTED'
    ),
    witness_sha256 TEXT NOT NULL UNIQUE CHECK(
        length(witness_sha256) = 64
        AND witness_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    witnessed_at TEXT NOT NULL
)
""".strip()


FAMILY_INSTALLATION_TABLE_SQL = f"""
CREATE TABLE history_coverage_family_seal_installation (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    schema_version INTEGER NOT NULL CHECK(
        typeof(schema_version) = 'integer'
        AND schema_version = {FAMILY_SEAL_SCHEMA_VERSION}
    ),
    rule_version TEXT NOT NULL CHECK(
        rule_version = '{FAMILY_SEAL_RULE_VERSION}'
    ),
    catalog_schema_version INTEGER NOT NULL CHECK(
        typeof(catalog_schema_version) = 'integer'
        AND catalog_schema_version > 0
    ),
    catalog_sha256 TEXT NOT NULL CHECK(
        length(catalog_sha256) = 64
        AND catalog_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    installed_at TEXT NOT NULL
)
""".strip()


PROTECTED_GENERATION_TABLE_SQL = """
CREATE TABLE history_coverage_protected_generation (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    generation INTEGER NOT NULL CHECK(
        typeof(generation) = 'integer' AND generation >= 0
    )
)
""".strip()


TERMINAL_BINDING_OVERLAY_TABLE_SQL = """
CREATE TABLE history_coverage_authorized_terminal_bindings (
    coverage_receipt_sha256 TEXT PRIMARY KEY CHECK(
        length(coverage_receipt_sha256) = 64
        AND coverage_receipt_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    source_scan_id INTEGER NOT NULL CHECK(
        typeof(source_scan_id) = 'integer' AND source_scan_id > 0
    ),
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N19'),
    symbol TEXT NOT NULL CHECK(length(symbol) BETWEEN 5 AND 32),
    terminal_binding_sha256 TEXT NOT NULL CHECK(
        length(terminal_binding_sha256) = 64
        AND terminal_binding_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    terminal_receipt_sha256 TEXT NOT NULL UNIQUE CHECK(
        length(terminal_receipt_sha256) = 64
        AND terminal_receipt_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    created_at TEXT NOT NULL,
    UNIQUE(source_scan_id,strategy_id,symbol)
)
""".strip()


V3_SNAPSHOT_ANCHOR_TABLE_SQL = """
CREATE TABLE history_coverage_v3_snapshot_anchors (
    anchor_kind TEXT NOT NULL CHECK(
        anchor_kind IN ('RECEIPT','CHAIN','HEAD','INSTALLATION')
    ),
    anchor_identity TEXT NOT NULL CHECK(length(anchor_identity) BETWEEN 1 AND 256),
    typed_row_json TEXT NOT NULL,
    typed_row_sha256 TEXT NOT NULL UNIQUE CHECK(
        length(typed_row_sha256) = 64
        AND typed_row_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    anchored_at TEXT NOT NULL,
    PRIMARY KEY(anchor_kind,anchor_identity)
)
""".strip()


LEGACY_RESET_SUCCESSOR_TABLE_SQL = """
CREATE TABLE history_coverage_n19_legacy_reset_successors (
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N19'),
    symbol TEXT NOT NULL CHECK(symbol = 'EXAMPLEUSDT'),
    family_id TEXT NOT NULL CHECK(family_id = '0123456789abcdef01234567'),
    structure_id TEXT NOT NULL CHECK(structure_id = '89abcdef0123456789abcdef'),
    original_state_row_sha256 TEXT NOT NULL CHECK(
        length(original_state_row_sha256) = 64
        AND original_state_row_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    reset_after_time_ms INTEGER NOT NULL CHECK(
        typeof(reset_after_time_ms) = 'integer' AND reset_after_time_ms > 0
    ),
    successor_evidence_json TEXT NOT NULL,
    successor_evidence_sha256 TEXT NOT NULL CHECK(
        length(successor_evidence_sha256) = 64
        AND successor_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    successor_state_row_sha256 TEXT NOT NULL UNIQUE CHECK(
        length(successor_state_row_sha256) = 64
        AND successor_state_row_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    created_at TEXT NOT NULL,
    PRIMARY KEY(strategy_id,symbol,family_id)
)
""".strip()


TERMINAL_BUNDLE_TABLE_SQL = """
CREATE TABLE history_coverage_n19_terminal_bundles (
    source_scan_id INTEGER NOT NULL CHECK(
        typeof(source_scan_id) = 'integer' AND source_scan_id > 0
    ),
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N19'),
    symbol TEXT NOT NULL CHECK(length(symbol) BETWEEN 5 AND 32),
    family_id TEXT NOT NULL CHECK(
        length(family_id) = 24 AND family_id NOT GLOB '*[^0-9a-f]*'
    ),
    structure_id TEXT NOT NULL CHECK(
        length(structure_id) = 24 AND structure_id NOT GLOB '*[^0-9a-f]*'
    ),
    prior_evidence_sha256 TEXT NOT NULL CHECK(
        length(prior_evidence_sha256) = 64
        AND prior_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    terminal_evidence_json TEXT NOT NULL,
    terminal_evidence_sha256 TEXT NOT NULL CHECK(
        length(terminal_evidence_sha256) = 64
        AND terminal_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    batch_expected_count INTEGER NOT NULL CHECK(
        typeof(batch_expected_count) = 'integer' AND batch_expected_count > 0
    ),
    batch_manifest_sha256 TEXT NOT NULL CHECK(
        length(batch_manifest_sha256) = 64
        AND batch_manifest_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    source_start_time_ms INTEGER NOT NULL CHECK(
        typeof(source_start_time_ms) = 'integer' AND source_start_time_ms > 0
    ),
    covered_through_time_ms INTEGER NOT NULL CHECK(
        typeof(covered_through_time_ms) = 'integer'
        AND covered_through_time_ms >= source_start_time_ms
    ),
    source_sha256 TEXT NOT NULL CHECK(
        length(source_sha256) = 64
        AND source_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    publication_ordinal INTEGER NOT NULL CHECK(
        typeof(publication_ordinal) = 'integer' AND publication_ordinal > 0
    ),
    previous_receipt_sha256 TEXT CHECK(
        previous_receipt_sha256 IS NULL
        OR (
            length(previous_receipt_sha256) = 64
            AND previous_receipt_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    coverage_receipt_sha256 TEXT NOT NULL UNIQUE CHECK(
        length(coverage_receipt_sha256) = 64
        AND coverage_receipt_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    result_epoch_ordinal INTEGER NOT NULL CHECK(
        typeof(result_epoch_ordinal) = 'integer' AND result_epoch_ordinal > 0
    ),
    result_epoch_start_time_ms INTEGER NOT NULL CHECK(
        typeof(result_epoch_start_time_ms) = 'integer'
        AND result_epoch_start_time_ms > 0
    ),
    result_chain_head_sha256 TEXT NOT NULL CHECK(
        length(result_chain_head_sha256) = 64
        AND result_chain_head_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    terminal_receipt_sha256 TEXT NOT NULL UNIQUE CHECK(
        length(terminal_receipt_sha256) = 64
        AND terminal_receipt_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    terminal_binding_sha256 TEXT NOT NULL CHECK(
        length(terminal_binding_sha256) = 64
        AND terminal_binding_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    family_seal_sha256 TEXT NOT NULL UNIQUE CHECK(
        length(family_seal_sha256) = 64
        AND family_seal_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    bundle_sha256 TEXT NOT NULL UNIQUE CHECK(
        length(bundle_sha256) = 64
        AND bundle_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    published_at TEXT NOT NULL,
    PRIMARY KEY(source_scan_id,strategy_id,symbol),
    UNIQUE(strategy_id,symbol,family_id)
)
""".strip()


INDEX_SQL = {
    "idx_history_coverage_n19_family_seal_identity": (
        "CREATE UNIQUE INDEX idx_history_coverage_n19_family_seal_identity "
        "ON history_coverage_n19_family_seals(strategy_id,symbol,family_id)"
    ),
    "idx_history_coverage_n19_legacy_witness_identity": (
        "CREATE UNIQUE INDEX idx_history_coverage_n19_legacy_witness_identity "
        "ON history_coverage_n19_legacy_unbound_witnesses("
        "strategy_id,symbol,family_id)"
    ),
    "idx_history_coverage_authorized_terminal_owner": (
        "CREATE UNIQUE INDEX idx_history_coverage_authorized_terminal_owner "
        "ON history_coverage_authorized_terminal_bindings("
        "source_scan_id,strategy_id,symbol)"
    ),
    "idx_history_coverage_v3_anchor_identity": (
        "CREATE UNIQUE INDEX idx_history_coverage_v3_anchor_identity "
        "ON history_coverage_v3_snapshot_anchors(anchor_kind,anchor_identity)"
    ),
    "idx_history_coverage_n19_legacy_reset_identity": (
        "CREATE UNIQUE INDEX idx_history_coverage_n19_legacy_reset_identity "
        "ON history_coverage_n19_legacy_reset_successors("
        "strategy_id,symbol,family_id)"
    ),
    "idx_history_coverage_n19_terminal_bundle_identity": (
        "CREATE UNIQUE INDEX idx_history_coverage_n19_terminal_bundle_identity "
        "ON history_coverage_n19_terminal_bundles("
        "strategy_id,symbol,family_id)"
    ),
}


TRIGGER_SQL = {
    "trg_history_coverage_protected_generation_no_replace": """
CREATE TRIGGER trg_history_coverage_protected_generation_no_replace
BEFORE INSERT ON history_coverage_protected_generation
WHEN EXISTS(
  SELECT 1 FROM history_coverage_protected_generation WHERE singleton_id=1
)
BEGIN SELECT RAISE(ABORT, 'protected generation replacement is forbidden'); END
""".strip(),
    "trg_history_coverage_protected_generation_transition": """
CREATE TRIGGER trg_history_coverage_protected_generation_transition
BEFORE UPDATE ON history_coverage_protected_generation
WHEN NEW.singleton_id!=OLD.singleton_id
 OR typeof(NEW.generation)!='integer'
 OR NEW.generation!=OLD.generation+1
BEGIN SELECT RAISE(ABORT, 'protected generation transition is invalid'); END
""".strip(),
    "trg_history_coverage_protected_generation_no_delete": """
CREATE TRIGGER trg_history_coverage_protected_generation_no_delete
BEFORE DELETE ON history_coverage_protected_generation
BEGIN SELECT RAISE(ABORT, 'protected generation is permanent'); END
""".strip(),
    "trg_history_coverage_n19_terminal_bundle_insert_authorized": """
CREATE TRIGGER trg_history_coverage_n19_terminal_bundle_insert_authorized
BEFORE INSERT ON history_coverage_n19_terminal_bundles
WHEN _coverage_epoch_mutation_authorized(
  'terminal_bundle_insert',NEW.strategy_id,NEW.symbol,NEW.source_scan_id,
  NEW.batch_expected_count,NEW.batch_manifest_sha256
) != 1
BEGIN SELECT RAISE(ABORT, 'N19 terminal bundle insert is unauthorized'); END
""".strip(),
    "trg_history_coverage_n19_terminal_bundle_no_replace": """
CREATE TRIGGER trg_history_coverage_n19_terminal_bundle_no_replace
BEFORE INSERT ON history_coverage_n19_terminal_bundles
WHEN EXISTS(
  SELECT 1 FROM history_coverage_n19_terminal_bundles
  WHERE source_scan_id=NEW.source_scan_id
    AND strategy_id=NEW.strategy_id AND symbol=NEW.symbol
  UNION ALL
  SELECT 1 FROM history_coverage_n19_terminal_bundles
  WHERE strategy_id=NEW.strategy_id AND symbol=NEW.symbol
    AND family_id=NEW.family_id
)
BEGIN SELECT RAISE(ABORT, 'N19 terminal bundle replacement is forbidden'); END
""".strip(),
    "trg_history_coverage_n19_terminal_bundle_no_update": """
CREATE TRIGGER trg_history_coverage_n19_terminal_bundle_no_update
BEFORE UPDATE ON history_coverage_n19_terminal_bundles
BEGIN SELECT RAISE(ABORT, 'N19 terminal bundles are immutable'); END
""".strip(),
    "trg_history_coverage_n19_terminal_bundle_no_delete": """
CREATE TRIGGER trg_history_coverage_n19_terminal_bundle_no_delete
BEFORE DELETE ON history_coverage_n19_terminal_bundles
BEGIN SELECT RAISE(ABORT, 'N19 terminal bundles are permanent'); END
""".strip(),
    "trg_history_coverage_n19_terminal_requires_bundle": """
CREATE TRIGGER trg_history_coverage_n19_terminal_requires_bundle
BEFORE INSERT ON history_coverage_n19_terminal_receipts
WHEN NOT EXISTS(
  SELECT 1 FROM history_coverage_n19_terminal_bundles AS bundle
  WHERE bundle.source_scan_id=NEW.source_scan_id
    AND bundle.strategy_id=NEW.strategy_id
    AND bundle.symbol=NEW.symbol
    AND bundle.family_id=NEW.family_id
    AND bundle.structure_id=NEW.structure_id
    AND bundle.terminal_evidence_json=NEW.terminal_evidence_json
    AND bundle.terminal_evidence_sha256=NEW.terminal_evidence_sha256
    AND bundle.batch_expected_count=NEW.batch_expected_count
    AND bundle.batch_manifest_sha256=NEW.batch_manifest_sha256
    AND bundle.coverage_receipt_sha256=NEW.coverage_receipt_sha256
    AND bundle.result_epoch_ordinal=NEW.result_epoch_ordinal
    AND bundle.result_epoch_start_time_ms=NEW.result_epoch_start_time_ms
    AND bundle.result_chain_head_sha256=NEW.result_chain_head_sha256
    AND bundle.terminal_receipt_sha256=NEW.receipt_sha256
    AND bundle.published_at=NEW.published_at
)
BEGIN SELECT RAISE(ABORT, 'N19 terminal receipt requires one atomic bundle'); END
""".strip(),
    "trg_history_coverage_terminal_publication_requires_bundle": """
CREATE TRIGGER trg_history_coverage_terminal_publication_requires_bundle
BEFORE INSERT ON history_coverage_publication_receipts
WHEN NEW.terminal_binding_sha256 IS NOT NULL AND NOT EXISTS(
  SELECT 1 FROM history_coverage_n19_terminal_bundles AS bundle
  WHERE bundle.source_scan_id=NEW.source_scan_id
    AND bundle.strategy_id=NEW.strategy_id
    AND bundle.symbol=NEW.symbol
    AND bundle.source_start_time_ms=NEW.source_start_time_ms
    AND bundle.covered_through_time_ms=NEW.covered_through_time_ms
    AND bundle.source_sha256=NEW.source_sha256
    AND bundle.result_epoch_ordinal=NEW.result_epoch_ordinal
    AND bundle.result_epoch_start_time_ms=NEW.result_epoch_start_time_ms
    AND bundle.result_chain_head_sha256=NEW.result_chain_head_sha256
    AND bundle.publication_ordinal=NEW.publication_ordinal
    AND bundle.previous_receipt_sha256 IS NEW.previous_receipt_sha256
    AND bundle.batch_expected_count=NEW.batch_expected_count
    AND bundle.batch_manifest_sha256=NEW.batch_manifest_sha256
    AND bundle.terminal_binding_sha256=NEW.terminal_binding_sha256
    AND bundle.coverage_receipt_sha256=NEW.receipt_sha256
    AND bundle.published_at=NEW.published_at
)
BEGIN SELECT RAISE(
  ABORT, 'terminal-bound coverage receipt requires one atomic bundle'
); END
""".strip(),
    "trg_history_coverage_n19_terminal_bundle_apply": """
CREATE TRIGGER trg_history_coverage_n19_terminal_bundle_apply
AFTER INSERT ON history_coverage_n19_terminal_bundles
BEGIN
  INSERT INTO history_coverage_publication_receipts (
    source_scan_id,strategy_id,symbol,source_start_time_ms,
    covered_through_time_ms,source_sha256,result_epoch_ordinal,
    result_epoch_start_time_ms,result_chain_head_sha256,
    publication_ordinal,previous_receipt_sha256,batch_expected_count,
    batch_manifest_sha256,receipt_sha256,published_at
  ) VALUES (
    NEW.source_scan_id,NEW.strategy_id,NEW.symbol,
    NEW.source_start_time_ms,NEW.covered_through_time_ms,NEW.source_sha256,
    NEW.result_epoch_ordinal,NEW.result_epoch_start_time_ms,
    NEW.result_chain_head_sha256,NEW.publication_ordinal,
    NEW.previous_receipt_sha256,NEW.batch_expected_count,
    NEW.batch_manifest_sha256,NEW.coverage_receipt_sha256,NEW.published_at
  );
  INSERT INTO history_coverage_n19_terminal_receipts VALUES (
    NEW.source_scan_id,NEW.strategy_id,NEW.symbol,NEW.family_id,
    NEW.structure_id,NEW.terminal_evidence_json,
    NEW.terminal_evidence_sha256,NEW.batch_expected_count,
    NEW.batch_manifest_sha256,NEW.coverage_receipt_sha256,
    NEW.result_epoch_ordinal,NEW.result_epoch_start_time_ms,
    NEW.result_chain_head_sha256,NEW.terminal_receipt_sha256,
    NEW.published_at
  );
  INSERT INTO history_coverage_n19_family_seals VALUES (
    NEW.strategy_id,NEW.symbol,NEW.family_id,NEW.structure_id,
    NEW.terminal_evidence_sha256,'NORMAL_TERMINAL_RECEIPT',
    NEW.terminal_receipt_sha256,NEW.family_seal_sha256,NEW.published_at
  );
  INSERT INTO history_coverage_authorized_terminal_bindings VALUES (
    NEW.coverage_receipt_sha256,NEW.source_scan_id,NEW.strategy_id,
    NEW.symbol,NEW.terminal_binding_sha256,
    NEW.terminal_receipt_sha256,NEW.published_at
  );
  UPDATE n19_staircase_states
  SET stage='MISSED',reason='N19_HISTORICAL_ENTRY_MISSED',
      reset_after_time_ms=NULL,evidence_json=NEW.terminal_evidence_json,
      evidence_sha256=NEW.terminal_evidence_sha256,
      updated_at=NEW.published_at
  WHERE strategy_id=NEW.strategy_id AND symbol=NEW.symbol
    AND family_id=NEW.family_id AND structure_id=NEW.structure_id
    AND stage='CONFIRMED'
    AND evidence_sha256=NEW.prior_evidence_sha256;
  SELECT CASE WHEN changes()!=1 THEN
    RAISE(ABORT, 'N19 terminal bundle state transition conflicted') END;
END
""".strip(),
    "trg_history_coverage_n19_family_seal_insert_authorized": """
CREATE TRIGGER trg_history_coverage_n19_family_seal_insert_authorized
BEFORE INSERT ON history_coverage_n19_family_seals
WHEN _coverage_epoch_mutation_authorized(
  'family_seal_insert',NEW.strategy_id,NEW.symbol,NULL,NULL,NEW.seal_sha256
) != 1
BEGIN SELECT RAISE(ABORT, 'N19 family seal insert is unauthorized'); END
""".strip(),
    "trg_history_coverage_n19_family_seal_no_replace": """
CREATE TRIGGER trg_history_coverage_n19_family_seal_no_replace
BEFORE INSERT ON history_coverage_n19_family_seals
WHEN EXISTS(
  SELECT 1 FROM history_coverage_n19_family_seals
  WHERE strategy_id=NEW.strategy_id AND symbol=NEW.symbol
    AND family_id=NEW.family_id
)
BEGIN SELECT RAISE(ABORT, 'N19 family seal replacement is forbidden'); END
""".strip(),
    "trg_history_coverage_n19_family_seal_no_update": """
CREATE TRIGGER trg_history_coverage_n19_family_seal_no_update
BEFORE UPDATE ON history_coverage_n19_family_seals
BEGIN SELECT RAISE(ABORT, 'N19 family seals are immutable'); END
""".strip(),
    "trg_history_coverage_n19_family_seal_no_delete": """
CREATE TRIGGER trg_history_coverage_n19_family_seal_no_delete
BEFORE DELETE ON history_coverage_n19_family_seals
BEGIN SELECT RAISE(ABORT, 'N19 family seals are permanent'); END
""".strip(),
    "trg_history_coverage_n19_legacy_witness_insert_authorized": """
CREATE TRIGGER trg_history_coverage_n19_legacy_witness_insert_authorized
BEFORE INSERT ON history_coverage_n19_legacy_unbound_witnesses
WHEN _coverage_epoch_mutation_authorized(
  'legacy_witness_insert',NEW.strategy_id,NEW.symbol,NULL,
  NEW.v3_receipt_count,NEW.witness_sha256
) != 1
BEGIN SELECT RAISE(ABORT, 'N19 legacy witness insert is unauthorized'); END
""".strip(),
    "trg_history_coverage_n19_legacy_witness_no_replace": """
CREATE TRIGGER trg_history_coverage_n19_legacy_witness_no_replace
BEFORE INSERT ON history_coverage_n19_legacy_unbound_witnesses
WHEN EXISTS(
  SELECT 1 FROM history_coverage_n19_legacy_unbound_witnesses
  WHERE witness_id=1 OR (
    strategy_id=NEW.strategy_id AND symbol=NEW.symbol
    AND family_id=NEW.family_id
  )
)
BEGIN SELECT RAISE(ABORT, 'N19 legacy witness replacement is forbidden'); END
""".strip(),
    "trg_history_coverage_n19_legacy_witness_no_normal_proof": """
CREATE TRIGGER trg_history_coverage_n19_legacy_witness_no_normal_proof
BEFORE INSERT ON history_coverage_n19_legacy_unbound_witnesses
WHEN EXISTS(
  SELECT 1 FROM history_coverage_n19_terminal_receipts
  WHERE strategy_id=NEW.strategy_id AND symbol=NEW.symbol
    AND family_id=NEW.family_id
)
BEGIN SELECT RAISE(ABORT, 'N19 legacy and normal proof domains conflict'); END
""".strip(),
    "trg_history_coverage_n19_terminal_no_legacy_proof": """
CREATE TRIGGER trg_history_coverage_n19_terminal_no_legacy_proof
BEFORE INSERT ON history_coverage_n19_terminal_receipts
WHEN EXISTS(
  SELECT 1 FROM history_coverage_n19_legacy_unbound_witnesses
  WHERE strategy_id=NEW.strategy_id AND symbol=NEW.symbol
    AND family_id=NEW.family_id
)
BEGIN SELECT RAISE(ABORT, 'N19 normal and legacy proof domains conflict'); END
""".strip(),
    "trg_history_coverage_n19_legacy_witness_no_update": """
CREATE TRIGGER trg_history_coverage_n19_legacy_witness_no_update
BEFORE UPDATE ON history_coverage_n19_legacy_unbound_witnesses
BEGIN SELECT RAISE(ABORT, 'N19 legacy witnesses are immutable'); END
""".strip(),
    "trg_history_coverage_n19_legacy_witness_no_delete": """
CREATE TRIGGER trg_history_coverage_n19_legacy_witness_no_delete
BEFORE DELETE ON history_coverage_n19_legacy_unbound_witnesses
BEGIN SELECT RAISE(ABORT, 'N19 legacy witnesses are permanent'); END
""".strip(),
    "trg_history_coverage_n19_legacy_witness_state_update_guard": """
CREATE TRIGGER trg_history_coverage_n19_legacy_witness_state_update_guard
BEFORE UPDATE ON n19_staircase_states
WHEN OLD.strategy_id='N19' AND OLD.symbol='EXAMPLEUSDT'
 AND OLD.family_id='0123456789abcdef01234567'
 AND EXISTS(
   SELECT 1 FROM history_coverage_n19_legacy_unbound_witnesses
   WHERE witness_id=1
 )
 AND NOT (
   OLD.structure_id IS NEW.structure_id
   AND OLD.stage=NEW.stage AND OLD.reason=NEW.reason
   AND OLD.quote_volume_rank=NEW.quote_volume_rank
   AND OLD.s_open_time_ms=NEW.s_open_time_ms
   AND OLD.x_open_time_ms=NEW.x_open_time_ms
   AND OLD.created_at=NEW.created_at
   AND OLD.reset_after_time_ms IS NULL
   AND NEW.reset_after_time_ms IS NOT NULL
   AND EXISTS(
     SELECT 1 FROM history_coverage_n19_legacy_reset_successors AS r
     WHERE r.strategy_id=OLD.strategy_id AND r.symbol=OLD.symbol
       AND r.family_id=OLD.family_id AND r.structure_id=OLD.structure_id
       AND r.reset_after_time_ms=NEW.reset_after_time_ms
       AND r.successor_evidence_json=NEW.evidence_json
       AND r.successor_evidence_sha256=NEW.evidence_sha256
   )
 )
BEGIN SELECT RAISE(ABORT, 'authorized N19 legacy state update is forbidden'); END
""".strip(),
    "trg_history_coverage_n19_historical_state_no_insert": """
CREATE TRIGGER trg_history_coverage_n19_historical_state_no_insert
BEFORE INSERT ON n19_staircase_states
WHEN NEW.strategy_id='N19'
 AND NEW.reason='N19_HISTORICAL_ENTRY_MISSED'
BEGIN SELECT RAISE(ABORT, 'N19 historical terminal insertion is forbidden'); END
""".strip(),
    "trg_history_coverage_n19_historical_state_update_authorized": """
CREATE TRIGGER trg_history_coverage_n19_historical_state_update_authorized
BEFORE UPDATE ON n19_staircase_states
WHEN NEW.strategy_id='N19'
 AND NEW.reason='N19_HISTORICAL_ENTRY_MISSED'
 AND OLD.reason!='N19_HISTORICAL_ENTRY_MISSED'
 AND NOT EXISTS(
   SELECT 1 FROM history_coverage_n19_terminal_receipts AS receipt
   JOIN history_coverage_publication_receipts AS coverage
     ON coverage.receipt_sha256=receipt.coverage_receipt_sha256
    AND coverage.source_scan_id=receipt.source_scan_id
    AND coverage.strategy_id=receipt.strategy_id
    AND coverage.symbol=receipt.symbol
    AND coverage.batch_expected_count=receipt.batch_expected_count
    AND coverage.batch_manifest_sha256=receipt.batch_manifest_sha256
    AND coverage.result_epoch_ordinal=receipt.result_epoch_ordinal
    AND coverage.result_epoch_start_time_ms=
        receipt.result_epoch_start_time_ms
    AND coverage.result_chain_head_sha256=
        receipt.result_chain_head_sha256
   JOIN history_coverage_n19_family_seals AS seal
     ON seal.strategy_id=receipt.strategy_id
    AND seal.symbol=receipt.symbol
    AND seal.family_id=receipt.family_id
    AND seal.structure_id=receipt.structure_id
    AND seal.terminal_evidence_sha256=
        receipt.terminal_evidence_sha256
    AND seal.proof_domain='NORMAL_TERMINAL_RECEIPT'
    AND seal.proof_sha256=receipt.receipt_sha256
   JOIN history_coverage_authorized_terminal_bindings AS binding
     ON binding.coverage_receipt_sha256=
        receipt.coverage_receipt_sha256
    AND binding.source_scan_id=receipt.source_scan_id
    AND binding.strategy_id=receipt.strategy_id
    AND binding.symbol=receipt.symbol
    AND binding.terminal_receipt_sha256=receipt.receipt_sha256
   WHERE receipt.strategy_id=NEW.strategy_id
     AND receipt.symbol=NEW.symbol
     AND receipt.family_id=NEW.family_id
     AND receipt.structure_id=NEW.structure_id
     AND receipt.terminal_evidence_json=NEW.evidence_json
     AND receipt.terminal_evidence_sha256=NEW.evidence_sha256
 )
BEGIN SELECT RAISE(ABORT, 'N19 historical terminal update is unauthorized'); END
""".strip(),
    "trg_history_coverage_authorized_binding_insert_authorized": """
CREATE TRIGGER trg_history_coverage_authorized_binding_insert_authorized
BEFORE INSERT ON history_coverage_authorized_terminal_bindings
WHEN _coverage_epoch_mutation_authorized(
  'terminal_binding_overlay_insert',NEW.strategy_id,NEW.symbol,
  NEW.source_scan_id,NULL,NEW.terminal_binding_sha256
) != 1
BEGIN SELECT RAISE(ABORT, 'terminal binding overlay insert is unauthorized'); END
""".strip(),
    "trg_history_coverage_authorized_binding_requires_bundle": """
CREATE TRIGGER trg_history_coverage_authorized_binding_requires_bundle
BEFORE INSERT ON history_coverage_authorized_terminal_bindings
WHEN NOT EXISTS(
  SELECT 1 FROM history_coverage_n19_terminal_bundles AS bundle
  WHERE bundle.coverage_receipt_sha256=NEW.coverage_receipt_sha256
    AND bundle.source_scan_id=NEW.source_scan_id
    AND bundle.strategy_id=NEW.strategy_id
    AND bundle.symbol=NEW.symbol
    AND bundle.terminal_binding_sha256=NEW.terminal_binding_sha256
    AND bundle.terminal_receipt_sha256=NEW.terminal_receipt_sha256
    AND bundle.published_at=NEW.created_at
)
BEGIN SELECT RAISE(
  ABORT, 'terminal binding overlay requires one atomic bundle'
); END
""".strip(),
    "trg_history_coverage_authorized_binding_no_replace": """
CREATE TRIGGER trg_history_coverage_authorized_binding_no_replace
BEFORE INSERT ON history_coverage_authorized_terminal_bindings
WHEN EXISTS(
  SELECT 1 FROM history_coverage_authorized_terminal_bindings
  WHERE coverage_receipt_sha256=NEW.coverage_receipt_sha256
     OR terminal_receipt_sha256=NEW.terminal_receipt_sha256
     OR (source_scan_id=NEW.source_scan_id
         AND strategy_id=NEW.strategy_id AND symbol=NEW.symbol)
)
BEGIN SELECT RAISE(ABORT, 'terminal binding overlay replacement is forbidden'); END
""".strip(),
    "trg_history_coverage_authorized_binding_no_update": """
CREATE TRIGGER trg_history_coverage_authorized_binding_no_update
BEFORE UPDATE ON history_coverage_authorized_terminal_bindings
BEGIN SELECT RAISE(ABORT, 'terminal binding overlays are immutable'); END
""".strip(),
    "trg_history_coverage_authorized_binding_no_delete": """
CREATE TRIGGER trg_history_coverage_authorized_binding_no_delete
BEFORE DELETE ON history_coverage_authorized_terminal_bindings
BEGIN SELECT RAISE(ABORT, 'terminal binding overlays are permanent'); END
""".strip(),
    "trg_history_coverage_v3_anchor_insert_authorized": """
CREATE TRIGGER trg_history_coverage_v3_anchor_insert_authorized
BEFORE INSERT ON history_coverage_v3_snapshot_anchors
WHEN _coverage_epoch_mutation_authorized(
  'v3_anchor_insert','N19',NEW.anchor_kind,NULL,NULL,NEW.typed_row_sha256
) != 1
BEGIN SELECT RAISE(ABORT, 'v3 snapshot anchor insert is unauthorized'); END
""".strip(),
    "trg_history_coverage_v3_anchor_no_replace": """
CREATE TRIGGER trg_history_coverage_v3_anchor_no_replace
BEFORE INSERT ON history_coverage_v3_snapshot_anchors
WHEN EXISTS(
  SELECT 1 FROM history_coverage_v3_snapshot_anchors
  WHERE anchor_kind=NEW.anchor_kind AND anchor_identity=NEW.anchor_identity
)
BEGIN SELECT RAISE(ABORT, 'v3 snapshot anchor replacement is forbidden'); END
""".strip(),
    "trg_history_coverage_v3_anchor_no_update": """
CREATE TRIGGER trg_history_coverage_v3_anchor_no_update
BEFORE UPDATE ON history_coverage_v3_snapshot_anchors
BEGIN SELECT RAISE(ABORT, 'v3 snapshot anchors are immutable'); END
""".strip(),
    "trg_history_coverage_v3_anchor_no_delete": """
CREATE TRIGGER trg_history_coverage_v3_anchor_no_delete
BEFORE DELETE ON history_coverage_v3_snapshot_anchors
BEGIN SELECT RAISE(ABORT, 'v3 snapshot anchors are permanent'); END
""".strip(),
    "trg_history_coverage_n19_legacy_reset_insert_authorized": """
CREATE TRIGGER trg_history_coverage_n19_legacy_reset_insert_authorized
BEFORE INSERT ON history_coverage_n19_legacy_reset_successors
WHEN _coverage_epoch_mutation_authorized(
  'legacy_reset_insert',NEW.strategy_id,NEW.symbol,NULL,
  NEW.reset_after_time_ms,NEW.successor_evidence_sha256
) != 1
BEGIN SELECT RAISE(ABORT, 'legacy reset successor insert is unauthorized'); END
""".strip(),
    "trg_history_coverage_n19_legacy_reset_no_replace": """
CREATE TRIGGER trg_history_coverage_n19_legacy_reset_no_replace
BEFORE INSERT ON history_coverage_n19_legacy_reset_successors
WHEN EXISTS(
  SELECT 1 FROM history_coverage_n19_legacy_reset_successors
  WHERE strategy_id=NEW.strategy_id AND symbol=NEW.symbol
    AND family_id=NEW.family_id
)
BEGIN SELECT RAISE(ABORT, 'legacy reset successor replacement is forbidden'); END
""".strip(),
    "trg_history_coverage_n19_legacy_reset_no_update": """
CREATE TRIGGER trg_history_coverage_n19_legacy_reset_no_update
BEFORE UPDATE ON history_coverage_n19_legacy_reset_successors
BEGIN SELECT RAISE(ABORT, 'legacy reset successors are immutable'); END
""".strip(),
    "trg_history_coverage_n19_legacy_reset_no_delete": """
CREATE TRIGGER trg_history_coverage_n19_legacy_reset_no_delete
BEFORE DELETE ON history_coverage_n19_legacy_reset_successors
BEGIN SELECT RAISE(ABORT, 'legacy reset successors are permanent'); END
""".strip(),
    "trg_history_coverage_family_install_insert_authorized": """
CREATE TRIGGER trg_history_coverage_family_install_insert_authorized
BEFORE INSERT ON history_coverage_family_seal_installation
WHEN _coverage_epoch_mutation_authorized(
  'family_installation_insert','N19','INSTALLATION',NULL,NULL,NEW.catalog_sha256
) != 1
BEGIN SELECT RAISE(ABORT, 'N19 family seal installation is unauthorized'); END
""".strip(),
    "trg_history_coverage_family_install_no_replace": """
CREATE TRIGGER trg_history_coverage_family_install_no_replace
BEFORE INSERT ON history_coverage_family_seal_installation
WHEN EXISTS(
  SELECT 1 FROM history_coverage_family_seal_installation
  WHERE singleton_id=1
)
BEGIN SELECT RAISE(ABORT, 'N19 family seal installation replacement is forbidden'); END
""".strip(),
    "trg_history_coverage_family_install_no_update": """
CREATE TRIGGER trg_history_coverage_family_install_no_update
BEFORE UPDATE ON history_coverage_family_seal_installation
BEGIN SELECT RAISE(ABORT, 'N19 family seal installation is immutable'); END
""".strip(),
    "trg_history_coverage_family_install_no_delete": """
CREATE TRIGGER trg_history_coverage_family_install_no_delete
BEFORE DELETE ON history_coverage_family_seal_installation
BEGIN SELECT RAISE(ABORT, 'N19 family seal installation is permanent'); END
""".strip(),
}

_PROTECTED_GENERATION_EVENTS = (
    ("terminal_bundle_insert", "history_coverage_n19_terminal_bundles", "INSERT", ""),
    ("family_seal_insert", "history_coverage_n19_family_seals", "INSERT", ""),
    (
        "legacy_witness_insert",
        "history_coverage_n19_legacy_unbound_witnesses",
        "INSERT",
        "",
    ),
    (
        "terminal_binding_insert",
        "history_coverage_authorized_terminal_bindings",
        "INSERT",
        "",
    ),
    ("snapshot_anchor_insert", "history_coverage_v3_snapshot_anchors", "INSERT", ""),
    (
        "legacy_reset_insert",
        "history_coverage_n19_legacy_reset_successors",
        "INSERT",
        "",
    ),
    (
        "terminal_receipt_insert",
        "history_coverage_n19_terminal_receipts",
        "INSERT",
        "",
    ),
    (
        "publication_receipt_insert",
        "history_coverage_publication_receipts",
        "INSERT",
        "",
    ),
    ("epoch_chain_insert", "history_coverage_epoch_chain", "INSERT", ""),
    ("epoch_head_insert", "history_coverage_epoch_heads", "INSERT", ""),
    ("epoch_head_update", "history_coverage_epoch_heads", "UPDATE", ""),
    (
        "historical_state_insert",
        "n19_staircase_states",
        "INSERT",
        "WHEN NEW.strategy_id='N19' "
        "AND NEW.reason='N19_HISTORICAL_ENTRY_MISSED'",
    ),
    (
        "historical_state_update",
        "n19_staircase_states",
        "UPDATE",
        "WHEN (NEW.strategy_id='N19' "
        "AND NEW.reason='N19_HISTORICAL_ENTRY_MISSED') "
        "OR (OLD.strategy_id='N19' "
        "AND OLD.reason='N19_HISTORICAL_ENTRY_MISSED')",
    ),
)
for _event_name, _event_table, _event_operation, _event_when in (
    _PROTECTED_GENERATION_EVENTS
):
    TRIGGER_SQL[
        "trg_history_coverage_generation_%s" % _event_name
    ] = (
        "CREATE TRIGGER trg_history_coverage_generation_%s "
        "AFTER %s ON %s %s "
        "BEGIN UPDATE history_coverage_protected_generation "
        "SET generation=generation+1 WHERE singleton_id=1; END"
        % (_event_name, _event_operation, _event_table, _event_when)
    )

TABLES = (
    "history_coverage_protected_generation",
    "history_coverage_n19_terminal_bundles",
    "history_coverage_n19_family_seals",
    "history_coverage_n19_legacy_unbound_witnesses",
    "history_coverage_authorized_terminal_bindings",
    "history_coverage_v3_snapshot_anchors",
    "history_coverage_n19_legacy_reset_successors",
    "history_coverage_family_seal_installation",
)
OBJECTS = {*TABLES, *INDEX_SQL, *TRIGGER_SQL}
_TERMINAL_PUBLICATION_GUARD = (
    "trg_history_coverage_terminal_publication_requires_bundle"
)


def _expected_objects(connection: sqlite3.Connection) -> set[str]:
    expected = set(OBJECTS)
    if not any(
        row[1] == "terminal_binding_sha256"
        for row in connection.execute(
            "PRAGMA table_xinfo(history_coverage_publication_receipts)"
        )
    ):
        expected.remove(_TERMINAL_PUBLICATION_GUARD)
    return expected


def _normalized_sql(value: Any) -> str:
    if type(value) is not str:
        raise RuntimeError("N19 family seal schema SQL is missing")
    return "".join(value.lower().split()).replace('"', "").replace("`", "")


def _owned_objects(connection: sqlite3.Connection) -> dict[str, tuple[str, str]]:
    rows = connection.execute(
        "SELECT type,name,sql FROM sqlite_schema "
        "WHERE name='history_coverage_protected_generation' "
        "OR name LIKE 'trg_history_coverage_protected_generation_%' "
        "OR name LIKE 'trg_history_coverage_generation_%' "
        "OR name='history_coverage_family_seal_installation' "
        "OR name LIKE 'trg_history_coverage_family_install_%' "
        "OR name LIKE 'history_coverage_n19_terminal_bundle%' "
        "OR name LIKE 'history_coverage_n19_family_seal%' "
        "OR name LIKE 'history_coverage_n19_legacy_unbound%' "
        "OR name LIKE 'history_coverage_authorized_terminal_binding%' "
        "OR name LIKE 'history_coverage_v3_snapshot_anchor%' "
        "OR name LIKE 'history_coverage_n19_legacy_reset_successor%' "
        "OR name LIKE 'idx_history_coverage_n19_family%' "
        "OR name LIKE 'idx_history_coverage_n19_terminal_bundle%' "
        "OR name LIKE 'idx_history_coverage_n19_legacy%' "
        "OR name LIKE 'idx_history_coverage_authorized%' "
        "OR name LIKE 'idx_history_coverage_v3%' "
        "OR name LIKE 'trg_history_coverage_n19_family%' "
        "OR name LIKE 'trg_history_coverage_n19_terminal_bundle%' "
        "OR name='trg_history_coverage_n19_terminal_requires_bundle' "
        "OR name='trg_history_coverage_terminal_publication_requires_bundle' "
        "OR name LIKE 'trg_history_coverage_n19_legacy%' "
        "OR name LIKE 'trg_history_coverage_n19_historical%' "
        "OR name LIKE 'trg_history_coverage_authorized%' "
        "OR name LIKE 'trg_history_coverage_v3%' "
        "OR name='trg_history_coverage_n19_terminal_no_legacy_proof'"
    ).fetchall()
    result: dict[str, tuple[str, str]] = {}
    for row in rows:
        if (
            type(row) not in (tuple, list)
            or len(row) != 3
            or any(type(value) is not str for value in row)
            or row[1] in result
        ):
            raise RuntimeError("N19 family seal catalog is invalid")
        result[row[1]] = (row[0], row[2])
    return result


def _legacy_family_seal_catalog_sha256(
    connection: sqlite3.Connection,
) -> str:
    objects = _expected_objects(connection)
    placeholders = ",".join("?" for _ in objects)
    rows = connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema "
        f"WHERE name IN ({placeholders}) ORDER BY type,name",
        tuple(sorted(objects)),
    ).fetchall()
    if len(rows) != len(objects):
        raise RuntimeError("N19 family seal catalog is incomplete")
    return hashlib.sha256(
        "\n".join(
            "|".join((row[0], row[1], row[2], _normalized_sql(row[3])))
            for row in rows
        ).encode("utf-8")
    ).hexdigest()


def family_seal_catalog_sha256(connection: sqlite3.Connection) -> str:
    """Bind the N19 catalog and the separately versioned N15 archive.

    Existing v5 installations retain their original immutable catalog root.
    The independent N15 installation contributes a second exact catalog only
    after explicit stopped-service installation; the N16 ledger high-water
    then binds the composite digest and Review schema cookie as one pair.
    """

    legacy = _legacy_family_seal_catalog_sha256(connection)
    from .n15_terminal_schema import n15_terminal_schema_status

    status = n15_terminal_schema_status(connection, validate_graph=False)
    if status == "PRE_N15_TERMINAL":
        return legacy
    if status != "CURRENT":
        raise RuntimeError("N15 terminal catalog generation is invalid")
    from .n15_terminal_schema import n15_terminal_catalog_sha256

    return hashlib.sha256(
        ("FAMILY_AND_N15_V1|" + legacy + "|" +
         n15_terminal_catalog_sha256(connection)).encode("ascii")
    ).hexdigest()


def family_seal_schema_status(
    connection: sqlite3.Connection,
    *,
    validate_graph: bool = True,
) -> str:
    owned = _owned_objects(connection)
    if not owned:
        return "PRE_FAMILY_SEAL"
    expected_objects = _expected_objects(connection)
    if set(owned) != expected_objects:
        raise RuntimeError("N19 family seal schema is partial")
    expected = {
        "history_coverage_protected_generation": (
            PROTECTED_GENERATION_TABLE_SQL
        ),
        "history_coverage_n19_terminal_bundles": TERMINAL_BUNDLE_TABLE_SQL,
        "history_coverage_n19_family_seals": FAMILY_SEAL_TABLE_SQL,
        "history_coverage_n19_legacy_unbound_witnesses": (
            LEGACY_WITNESS_TABLE_SQL
        ),
        "history_coverage_authorized_terminal_bindings": (
            TERMINAL_BINDING_OVERLAY_TABLE_SQL
        ),
        "history_coverage_v3_snapshot_anchors": (
            V3_SNAPSHOT_ANCHOR_TABLE_SQL
        ),
        "history_coverage_n19_legacy_reset_successors": (
            LEGACY_RESET_SUCCESSOR_TABLE_SQL
        ),
        "history_coverage_family_seal_installation": (
            FAMILY_INSTALLATION_TABLE_SQL
        ),
        **INDEX_SQL,
        **TRIGGER_SQL,
    }
    if _TERMINAL_PUBLICATION_GUARD not in expected_objects:
        expected.pop(_TERMINAL_PUBLICATION_GUARD)
    expected_types = {
        **{name: "table" for name in TABLES},
        **{name: "index" for name in INDEX_SQL},
        **{name: "trigger" for name in TRIGGER_SQL},
    }
    for name, sql in expected.items():
        if (
            owned[name][0] != expected_types[name]
            or _normalized_sql(owned[name][1]) != _normalized_sql(sql)
        ):
            raise RuntimeError(
                "N19 family seal object is inconsistent: %s" % name
            )
    reference = sqlite3.connect(":memory:")
    try:
        from .coverage_epoch_schema import (
            EPOCH_TABLE_SQL,
            HEAD_TABLE_SQL,
            N19_TERMINAL_PUBLICATION_TABLE_SQL,
            PUBLICATION_TABLE_SQL,
        )
        from .n19_schema import N19_STATE_TABLE_SQL

        reference.execute(N19_STATE_TABLE_SQL)
        reference.execute(EPOCH_TABLE_SQL)
        reference.execute(HEAD_TABLE_SQL)
        reference.execute(PUBLICATION_TABLE_SQL)
        reference.execute(N19_TERMINAL_PUBLICATION_TABLE_SQL)
        for sql in (
            PROTECTED_GENERATION_TABLE_SQL,
            TERMINAL_BUNDLE_TABLE_SQL,
            FAMILY_SEAL_TABLE_SQL,
            LEGACY_WITNESS_TABLE_SQL,
            TERMINAL_BINDING_OVERLAY_TABLE_SQL,
            V3_SNAPSHOT_ANCHOR_TABLE_SQL,
            LEGACY_RESET_SUCCESSOR_TABLE_SQL,
            FAMILY_INSTALLATION_TABLE_SQL,
            *INDEX_SQL.values(),
            *TRIGGER_SQL.values(),
        ):
            reference.execute(sql)
        for table in TABLES:
            actual_xinfo = tuple(
                tuple(row)
                for row in connection.execute(
                    'PRAGMA table_xinfo("%s")' % table
                )
            )
            expected_xinfo = tuple(
                tuple(row)
                for row in reference.execute(
                    'PRAGMA table_xinfo("%s")' % table
                )
            )
            if actual_xinfo != expected_xinfo:
                raise RuntimeError(
                    "N19 family seal table metadata conflicts: %s" % table
                )
            if connection.execute(
                'PRAGMA foreign_key_list("%s")' % table
            ).fetchall():
                raise RuntimeError(
                    "N19 family seal table has an unexpected foreign key"
                )
            actual_indexes = {
                row[1]: (row[2], row[3], row[4])
                for row in connection.execute(
                    'PRAGMA index_list("%s")' % table
                )
            }
            expected_indexes = {
                row[1]: (row[2], row[3], row[4])
                for row in reference.execute(
                    'PRAGMA index_list("%s")' % table
                )
            }
            if actual_indexes != expected_indexes:
                raise RuntimeError(
                    "N19 family seal index set conflicts: %s" % table
                )
            for index_name in expected_indexes:
                actual_index = tuple(
                    tuple(row)
                    for row in connection.execute(
                        'PRAGMA index_xinfo("%s")' % index_name
                    )
                )
                expected_index = tuple(
                    tuple(row)
                    for row in reference.execute(
                        'PRAGMA index_xinfo("%s")' % index_name
                    )
                )
                if actual_index != expected_index:
                    raise RuntimeError(
                        "N19 family seal index metadata conflicts: %s"
                        % index_name
                    )
            actual_triggers = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_schema "
                    "WHERE type='trigger' AND tbl_name=?",
                    (table,),
                )
            }
            expected_triggers = {
                row[0]
                for row in reference.execute(
                    "SELECT name FROM sqlite_schema "
                    "WHERE type='trigger' AND tbl_name=?",
                    (table,),
                )
            }
            if actual_triggers != expected_triggers:
                raise RuntimeError(
                    "N19 family seal trigger set conflicts: %s" % table
                )
    finally:
        reference.close()
    lowered_tables = {table.casefold() for table in TABLES}
    for (table_name,) in connection.execute(
        "SELECT name FROM sqlite_schema WHERE type='table'"
    ).fetchall():
        if type(table_name) is not str:
            raise RuntimeError("N19 family seal catalog table name is invalid")
        escaped = table_name.replace('"', '""')
        for foreign_key in connection.execute(
            f'PRAGMA foreign_key_list("{escaped}")'
        ).fetchall():
            if (
                len(foreign_key) > 2
                and type(foreign_key[2]) is str
                and foreign_key[2].casefold() in lowered_tables
            ):
                raise RuntimeError(
                    "N19 family seal table has an incoming foreign key"
                )
    row = connection.execute(
        "SELECT schema_version,rule_version,catalog_schema_version,"
        "catalog_sha256 FROM history_coverage_family_seal_installation "
        "WHERE singleton_id=1"
    ).fetchone()
    if (
        row is None
        or type(row[0]) is not int
        or row[0] != FAMILY_SEAL_SCHEMA_VERSION
        or row[1] != FAMILY_SEAL_RULE_VERSION
        or type(row[2]) is not int
        or row[2] <= 0
        or row[3] != _legacy_family_seal_catalog_sha256(connection)
    ):
        raise RuntimeError("N19 family seal installation is inconsistent")
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
        raise RuntimeError("N19 protected generation is inconsistent")
    if validate_graph:
        try:
            validate_family_seal_graph(connection)
        except sqlite3.Error as exc:
            raise RuntimeError(
                "N19 family seal graph dependency is missing"
            ) from exc
    return "CURRENT"


def install_family_seal_schema(
    connection: sqlite3.Connection,
    installed_at: str,
) -> None:
    if family_seal_schema_status(connection) == "CURRENT":
        return
    for sql in (
        PROTECTED_GENERATION_TABLE_SQL,
        TERMINAL_BUNDLE_TABLE_SQL,
        FAMILY_SEAL_TABLE_SQL,
        LEGACY_WITNESS_TABLE_SQL,
        TERMINAL_BINDING_OVERLAY_TABLE_SQL,
        V3_SNAPSHOT_ANCHOR_TABLE_SQL,
        LEGACY_RESET_SUCCESSOR_TABLE_SQL,
        FAMILY_INSTALLATION_TABLE_SQL,
        *INDEX_SQL.values(),
        *(
            sql
            for name, sql in TRIGGER_SQL.items()
            if name in _expected_objects(connection)
        ),
    ):
        connection.execute(sql)
    connection.execute(
        "INSERT INTO history_coverage_protected_generation VALUES (1,0)"
    )
    for row in connection.execute(
        "SELECT symbol,family_id,structure_id,terminal_evidence_sha256,"
        "receipt_sha256,published_at "
        "FROM history_coverage_n19_terminal_receipts "
        "ORDER BY strategy_id,symbol,family_id"
    ):
        insert_family_seal(
            connection,
            symbol=row[0],
            family_id=row[1],
            structure_id=row[2],
            evidence_sha256=row[3],
            proof_domain=PROVED_TERMINAL_DOMAIN,
            proof_sha256=row[4],
            sealed_at=row[5],
        )
    schema_version = connection.execute("PRAGMA schema_version").fetchone()[0]
    catalog_sha256 = _legacy_family_seal_catalog_sha256(connection)
    connection.execute(
        "INSERT INTO history_coverage_family_seal_installation "
        "VALUES (1,?,?,?,?,?)",
        (
            FAMILY_SEAL_SCHEMA_VERSION,
            FAMILY_SEAL_RULE_VERSION,
            schema_version,
            catalog_sha256,
            installed_at,
        ),
    )


def insert_family_seal(
    connection: sqlite3.Connection,
    *,
    symbol: str,
    family_id: str,
    structure_id: str,
    evidence_sha256: str,
    proof_domain: str,
    proof_sha256: str,
    sealed_at: str,
) -> str:
    seal_sha = family_seal_sha256(
        symbol,
        family_id,
        structure_id,
        evidence_sha256,
        proof_domain,
        proof_sha256,
    )
    connection.execute(
        "INSERT INTO history_coverage_n19_family_seals VALUES "
        "('N19',?,?,?,?,?,?,?,?)",
        (
            symbol,
            family_id,
            structure_id,
            evidence_sha256,
            proof_domain,
            proof_sha256,
            seal_sha,
            sealed_at,
        ),
    )
    return seal_sha


def insert_authorized_legacy_witness(
    connection: sqlite3.Connection,
    *,
    plan: AuthorizedLegacyWitnessPlan,
    terminal_evidence_json: str,
    witnessed_at: str,
) -> str:
    if (
        plan.symbol != AUTHORIZED_SYMBOL
        or plan.family_id != AUTHORIZED_FAMILY_ID
        or plan.structure_id != AUTHORIZED_STRUCTURE_ID
        or plan.authorization_sha256 != AUTHORIZED_STATEMENT_SHA256
        or plan.review_plan_sha256
        != authorized_witness_plan_sha256(
            plan.symbol,
            plan.family_id,
            plan.structure_id,
            plan.terminal_evidence_sha256,
            plan.state_row_sha256,
            plan.receipt_count,
            plan.receipt_set_sha256,
            plan.coverage_graph_sha256,
            plan.coverage_catalog_sha256,
            plan.review_canonical_sha256,
            plan.authorization_sha256,
        )
        or type(terminal_evidence_json) is not str
        or type(witnessed_at) is not str
        or not witnessed_at
    ):
        raise RuntimeError("authorized N19 legacy witness plan is invalid")
    from .n19_analyzer import decode_n19_state_evidence

    record = decode_n19_state_evidence(
        terminal_evidence_json,
        expected_symbol=plan.symbol,
    )
    if (
        record.stage != "MISSED"
        or record.reason != "N19_HISTORICAL_ENTRY_MISSED"
        or record.family_id != plan.family_id
        or record.structure_id != plan.structure_id
        or record.evidence_sha256 != plan.terminal_evidence_sha256
    ):
        raise RuntimeError("authorized N19 legacy witness evidence conflicts")
    witness_sha = authorized_witness_sha256(plan, terminal_evidence_json)
    inserted = connection.execute(
        "INSERT INTO history_coverage_n19_legacy_unbound_witnesses "
        "VALUES (1,'N19',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            plan.symbol,
            plan.family_id,
            plan.structure_id,
            record.stage,
            record.reason,
            terminal_evidence_json,
            plan.terminal_evidence_sha256,
            plan.state_row_sha256,
            plan.receipt_count,
            plan.receipt_set_sha256,
            plan.coverage_graph_sha256,
            plan.coverage_catalog_sha256,
            plan.review_canonical_sha256,
            plan.review_plan_sha256,
            plan.authorization_sha256,
            AUTHORIZED_GAP_REASON,
            witness_sha,
            witnessed_at,
        ),
    )
    if inserted.rowcount != 1:
        raise RuntimeError("authorized N19 legacy witness was not inserted")
    return witness_sha


def insert_v3_snapshot_anchors(
    connection: sqlite3.Connection,
    *,
    anchored_at: str,
) -> None:
    if type(anchored_at) is not str or not anchored_at:
        raise RuntimeError("v3 snapshot anchor time is invalid")
    collections = (
        (
            "RECEIPT",
            "SELECT * FROM history_coverage_publication_receipts "
            "ORDER BY publication_ordinal,source_scan_id,strategy_id,symbol",
        ),
        (
            "CHAIN",
            "SELECT * FROM history_coverage_epoch_chain "
            "ORDER BY strategy_id,symbol,epoch_ordinal",
        ),
        (
            "HEAD",
            "SELECT * FROM history_coverage_epoch_heads "
            "ORDER BY strategy_id,symbol",
        ),
        (
            "INSTALLATION",
            "SELECT * FROM history_coverage_epoch_installation "
            "ORDER BY singleton_id",
        ),
    )
    for kind, sql in collections:
        row_count = 0
        for raw_row in connection.execute(sql):
            row_count += 1
            row = tuple(raw_row)
            row_json = typed_row_json(row)
            row_sha = typed_row_sha256(row)
            inserted = connection.execute(
                "INSERT INTO history_coverage_v3_snapshot_anchors "
                "VALUES (?,?,?,?,?)",
                (
                    kind,
                    _v3_snapshot_anchor_identity(kind, row),
                    row_json,
                    row_sha,
                    anchored_at,
                ),
            )
            if inserted.rowcount != 1:
                raise RuntimeError("v3 snapshot anchor was not inserted")
        if kind == "INSTALLATION" and row_count != 1:
            raise RuntimeError("v3 installation anchor source is invalid")


def _v3_snapshot_anchor_identity(
    kind: str,
    row: tuple[Any, ...],
) -> str:
    if type(kind) is not str or type(row) is not tuple:
        raise RuntimeError("v3 snapshot anchor identity input is invalid")
    if kind == "RECEIPT":
        if (
            len(row) != 15
            or type(row[0]) is not int
            or row[0] <= 0
            or type(row[1]) is not str
            or type(row[2]) is not str
        ):
            raise RuntimeError("v3 receipt anchor identity is invalid")
        return "%d|%s|%s" % (row[0], row[1], row[2])
    if kind == "CHAIN":
        if (
            len(row) != 12
            or type(row[0]) is not str
            or type(row[1]) is not str
            or type(row[2]) is not int
            or row[2] <= 0
        ):
            raise RuntimeError("v3 chain anchor identity is invalid")
        return "%s|%s|%d" % (row[0], row[1], row[2])
    if kind == "HEAD":
        if (
            len(row) != 10
            or type(row[0]) is not str
            or type(row[1]) is not str
        ):
            raise RuntimeError("v3 head anchor identity is invalid")
        return "%s|%s" % (row[0], row[1])
    if kind == "INSTALLATION":
        if len(row) != 6 or type(row[0]) is not int or row[0] != 1:
            raise RuntimeError("v3 installation anchor identity is invalid")
        return str(row[0])
    raise RuntimeError("v3 snapshot anchor kind is invalid")


def _validated_v3_snapshot_anchors(
    connection: sqlite3.Connection,
) -> dict[str, list[tuple[Any, ...]]]:
    anchored: dict[str, list[tuple[Any, ...]]] = {
        "RECEIPT": [],
        "CHAIN": [],
        "HEAD": [],
        "INSTALLATION": [],
    }
    identities: set[tuple[str, str]] = set()
    installation_time = connection.execute(
        "SELECT installed_at FROM history_coverage_family_seal_installation "
        "WHERE singleton_id=1"
    ).fetchone()
    if (
        installation_time is None
        or len(installation_time) != 1
        or type(installation_time[0]) is not str
        or not installation_time[0]
    ):
        raise RuntimeError("v3 snapshot anchor installation time is invalid")
    for raw in connection.execute(
        "SELECT anchor_kind,anchor_identity,typed_row_json,"
        "typed_row_sha256,anchored_at "
        "FROM history_coverage_v3_snapshot_anchors "
        "ORDER BY anchor_kind,anchor_identity"
    ):
        row = tuple(raw)
        if (
            len(row) != 5
            or row[0] not in anchored
            or type(row[1]) is not str
            or not row[1]
            or type(row[2]) is not str
            or not _valid_hash(row[3])
            or type(row[4]) is not str
            or not row[4]
            or (row[0], row[1]) in identities
        ):
            raise RuntimeError("v3 snapshot anchor is invalid")
        try:
            decoded = json.loads(row[2])
            value = _untyped_row(decoded)
        except Exception as exc:
            raise RuntimeError("v3 snapshot anchor payload is invalid") from exc
        if (
            typed_row_json(value) != row[2]
            or typed_row_sha256(value) != row[3]
            or _v3_snapshot_anchor_identity(row[0], value) != row[1]
            or row[4] != installation_time[0]
        ):
            raise RuntimeError("v3 snapshot anchor digest conflicts")
        identities.add((row[0], row[1]))
        anchored[row[0]].append(value)
    return anchored


def _attest_v3_snapshot_boundary(
    connection: sqlite3.Connection,
    witness: tuple[Any, ...],
) -> None:
    from .coverage_epoch_schema import (
        _LEGACY_V3_OBJECTS,
        _coverage_epoch_catalog_sha256_for_objects,
    )

    anchored = _validated_v3_snapshot_anchors(connection)
    receipts = tuple(anchored["RECEIPT"])
    chains = tuple(anchored["CHAIN"])
    heads = tuple(anchored["HEAD"])
    installation = tuple(anchored["INSTALLATION"])
    if (
        len(receipts) != witness[9]
        or ordered_v3_receipt_set_sha256(receipts) != witness[10]
        or len(installation) != 1
        or coverage_graph_sha256(chains, heads, installation[0])
        != witness[11]
        or _coverage_epoch_catalog_sha256_for_objects(
            connection, _LEGACY_V3_OBJECTS
        )
        != witness[12]
    ):
        raise RuntimeError("authorized v3 snapshot commitment conflicts")
    current_receipts = {
        (row[0], row[1], row[2]): tuple(row)
        for row in connection.execute(
            "SELECT * FROM history_coverage_publication_receipts"
        )
    }
    for row in receipts:
        if current_receipts.get((row[0], row[1], row[2])) != row:
            raise RuntimeError("authorized v3 receipt anchor conflicts")
    current_chain = {
        (row[0], row[1], row[2]): tuple(row)
        for row in connection.execute(
            "SELECT * FROM history_coverage_epoch_chain"
        )
    }
    for row in chains:
        if current_chain.get((row[0], row[1], row[2])) != row:
            raise RuntimeError("authorized v3 chain anchor conflicts")
    current_heads = {
        (row[0], row[1]): tuple(row)
        for row in connection.execute(
            "SELECT * FROM history_coverage_epoch_heads"
        )
    }
    for old in heads:
        current = current_heads.get((old[0], old[1]))
        same_published_state = (
            current is not None and current[:9] == old[:9]
        )
        latest_receipt_time = None
        if (
            current is not None
            and not same_published_state
            and current[8] is not None
        ):
            latest_receipt = connection.execute(
                "SELECT published_at FROM "
                "history_coverage_publication_receipts "
                "WHERE receipt_sha256=?",
                (current[8],),
            ).fetchone()
            if latest_receipt is not None and len(latest_receipt) == 1:
                latest_receipt_time = latest_receipt[0]
        if (
            current is None
            or current[2] < old[2]
            or current[4] < old[4]
            or current[7] < old[7]
            or (
                current[2] == old[2]
                and (
                    current[3] != old[3]
                    or current[6] != old[6]
                )
            )
            or (same_published_state and current[9] != old[9])
            or (
                not same_published_state
                and (
                    type(latest_receipt_time) is not str
                    or current[9] != latest_receipt_time
                )
            )
        ):
            raise RuntimeError("authorized v3 head anchor conflicts")
    current_installation = connection.execute(
        "SELECT * FROM history_coverage_epoch_installation "
        "WHERE singleton_id=1"
    ).fetchall()
    if current_installation != [installation[0]]:
        raise RuntimeError("authorized v3 installation anchor conflicts")


def validate_family_seal_graph(connection: sqlite3.Connection) -> None:
    if not _owned_objects(connection):
        raise RuntimeError("N19 family seal schema is absent")
    from .n19_analyzer import decode_n19_state_evidence

    terminal_rows = connection.execute(
        "SELECT strategy_id,symbol,family_id,structure_id,"
        "terminal_evidence_json,terminal_evidence_sha256,receipt_sha256 "
        "FROM history_coverage_n19_terminal_receipts "
        "ORDER BY strategy_id,symbol,family_id"
    ).fetchall()
    installation_time_row = connection.execute(
        "SELECT installed_at FROM history_coverage_family_seal_installation "
        "WHERE singleton_id=1"
    ).fetchone()
    if (
        installation_time_row is None
        or len(installation_time_row) != 1
        or type(installation_time_row[0]) is not str
        or not installation_time_row[0]
    ):
        raise RuntimeError("N19 terminal bundle installation time is invalid")
    bundle_rows = connection.execute(
        "SELECT source_scan_id,strategy_id,symbol,family_id,structure_id,"
        "prior_evidence_sha256,terminal_evidence_json,"
        "terminal_evidence_sha256,batch_expected_count,"
        "batch_manifest_sha256,source_start_time_ms,"
        "covered_through_time_ms,source_sha256,publication_ordinal,"
        "previous_receipt_sha256,coverage_receipt_sha256,"
        "result_epoch_ordinal,result_epoch_start_time_ms,"
        "result_chain_head_sha256,terminal_receipt_sha256,"
        "terminal_binding_sha256,family_seal_sha256,bundle_sha256,"
        "published_at FROM history_coverage_n19_terminal_bundles "
        "ORDER BY source_scan_id,strategy_id,symbol"
    ).fetchall()
    publication_has_terminal_binding = any(
        row[1] == "terminal_binding_sha256"
        for row in connection.execute(
            "PRAGMA table_xinfo(history_coverage_publication_receipts)"
        )
    )
    coverage_by_receipt = {
        row[14]: tuple(row)
        for row in connection.execute(
            "SELECT source_scan_id,strategy_id,symbol,source_start_time_ms,"
            "covered_through_time_ms,source_sha256,result_epoch_ordinal,"
            "result_epoch_start_time_ms,result_chain_head_sha256,"
            "publication_ordinal,previous_receipt_sha256,"
            "batch_expected_count,batch_manifest_sha256,"
            + (
                "terminal_binding_sha256,"
                if publication_has_terminal_binding
                else "NULL AS terminal_binding_sha256,"
            )
            + "receipt_sha256,published_at "
            "FROM history_coverage_publication_receipts "
            "WHERE receipt_sha256 IN ("
            "SELECT coverage_receipt_sha256 "
            "FROM history_coverage_n19_terminal_bundles)"
        )
    }
    bundles_by_receipt: dict[str, tuple[Any, ...]] = {}
    for raw_bundle in bundle_rows:
        bundle = tuple(raw_bundle)
        if (
            len(bundle) != 24
            or type(bundle[0]) is not int
            or bundle[0] <= 0
            or bundle[1] != "N19"
            or type(bundle[2]) is not str
            or not 5 <= len(bundle[2]) <= 32
            or type(bundle[3]) is not str
            or len(bundle[3]) != 24
            or type(bundle[4]) is not str
            or len(bundle[4]) != 24
            or any(
                not _valid_hash(bundle[index])
                for index in (5, 7, 9, 12, 15, 18, 19, 20, 21, 22)
            )
            or type(bundle[6]) is not str
            or type(bundle[8]) is not int
            or bundle[8] <= 0
            or type(bundle[10]) is not int
            or bundle[10] <= 0
            or type(bundle[11]) is not int
            or bundle[11] < bundle[10]
            or type(bundle[13]) is not int
            or bundle[13] <= 0
            or (
                bundle[14] is not None
                and not _valid_hash(bundle[14])
            )
            or type(bundle[16]) is not int
            or bundle[16] <= 0
            or type(bundle[17]) is not int
            or bundle[17] <= 0
            or type(bundle[23]) is not str
            or not bundle[23]
            or bundle[19] in bundles_by_receipt
            or bundle[22] != terminal_bundle_sha256(
                bundle[0],
                bundle[2],
                bundle[3],
                bundle[4],
                bundle[5],
                bundle[6],
                bundle[7],
                bundle[8],
                bundle[9],
                bundle[10],
                bundle[11],
                bundle[12],
                bundle[13],
                bundle[14],
                bundle[15],
                bundle[16],
                bundle[17],
                bundle[18],
                bundle[19],
                bundle[20],
                bundle[21],
            )
        ):
            raise RuntimeError("N19 terminal bundle is invalid")
        coverage = coverage_by_receipt.get(bundle[15])
        if coverage != (
            bundle[0],
            bundle[1],
            bundle[2],
            bundle[10],
            bundle[11],
            bundle[12],
            bundle[16],
            bundle[17],
            bundle[18],
            bundle[13],
            bundle[14],
            bundle[8],
            bundle[9],
            None,
            bundle[15],
            bundle[23],
        ):
            raise RuntimeError("N19 terminal bundle coverage receipt conflicts")
        bundles_by_receipt[bundle[19]] = bundle
    terminal_full_rows = tuple(
        tuple(row)
        for row in connection.execute(
            "SELECT source_scan_id,strategy_id,symbol,family_id,structure_id,"
            "terminal_evidence_json,terminal_evidence_sha256,"
            "batch_expected_count,batch_manifest_sha256,"
            "coverage_receipt_sha256,result_epoch_ordinal,"
            "result_epoch_start_time_ms,result_chain_head_sha256,"
            "receipt_sha256,published_at "
            "FROM history_coverage_n19_terminal_receipts "
            "ORDER BY source_scan_id,strategy_id,symbol"
        )
    )
    for terminal in terminal_full_rows:
        bundle = bundles_by_receipt.get(terminal[13])
        if bundle is None:
            if terminal[14] >= installation_time_row[0]:
                raise RuntimeError(
                    "N19 post-install terminal receipt has no atomic bundle"
                )
            continue
        if (
            bundle[:5] != terminal[:5]
            or bundle[6:10] != terminal[5:9]
            or bundle[15] != terminal[9]
            or bundle[16:19] != terminal[10:13]
            or bundle[19] != terminal[13]
            or bundle[23] != terminal[14]
        ):
            raise RuntimeError("N19 terminal bundle receipt conflicts")
    if len(bundles_by_receipt) > len(terminal_full_rows):
        raise RuntimeError("N19 terminal bundle proof set is incomplete")
    witness_rows = connection.execute(
        "SELECT strategy_id,symbol,family_id,structure_id,terminal_stage,"
        "terminal_reason,terminal_evidence_json,terminal_evidence_sha256,"
        "state_row_sha256,v3_receipt_count,v3_receipt_set_sha256,"
        "v3_coverage_graph_sha256,v3_catalog_sha256,"
        "pre_review_canonical_sha256,review_plan_sha256,"
        "authorization_sha256,gap_reason,witness_sha256 "
        "FROM history_coverage_n19_legacy_unbound_witnesses "
        "ORDER BY witness_id"
    ).fetchall()
    coverage_installation = connection.execute(
        "SELECT schema_version FROM history_coverage_epoch_installation "
        "WHERE singleton_id=1"
    ).fetchall()
    authorized_generation = (
        coverage_installation == [(3,)]
        and connection.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='table' "
            "AND name='history_coverage_n19_terminal_receipts'"
        ).fetchone()
        == (1,)
    )
    if (
        authorized_generation
        and len(witness_rows) != 1
    ) or (
        not authorized_generation
        and len(witness_rows) != 0
    ):
        raise RuntimeError(
            "N19 authorized legacy witness generation is inconsistent"
        )
    if not authorized_generation and (
        connection.execute(
            "SELECT 1 FROM history_coverage_v3_snapshot_anchors LIMIT 1"
        ).fetchone()
        is not None
        or connection.execute(
            "SELECT 1 FROM "
            "history_coverage_n19_legacy_reset_successors LIMIT 1"
        ).fetchone()
        is not None
    ):
        raise RuntimeError(
            "N19 authorized legacy support evidence is out of generation"
        )
    seals = {
        (row[0], row[1], row[2]): tuple(row)
        for row in connection.execute(
            "SELECT strategy_id,symbol,family_id,structure_id,"
            "terminal_evidence_sha256,proof_domain,proof_sha256,"
            "seal_sha256 FROM history_coverage_n19_family_seals "
            "ORDER BY strategy_id,symbol,family_id"
        )
    }
    if len(seals) != len(terminal_rows) + len(witness_rows):
        raise RuntimeError("N19 family seal proof set is incomplete")
    proof_keys = {
        (row[1], row[2], row[3]) for row in terminal_rows
    } | {
        (row[1], row[2], row[3]) for row in witness_rows
    }
    historical_states = connection.execute(
        "SELECT symbol,family_id,structure_id,evidence_json,evidence_sha256 "
        "FROM n19_staircase_states WHERE strategy_id='N19' "
        "AND reason='N19_HISTORICAL_ENTRY_MISSED'"
    ).fetchall()
    if len(historical_states) != len(proof_keys):
        raise RuntimeError("N19 historical terminal proof set is incomplete")
    for raw_state in historical_states:
        state = tuple(raw_state)
        if (
            len(state) != 5
            or tuple(state[:3]) not in proof_keys
            or type(state[3]) is not str
            or not _valid_hash(state[4])
        ):
            raise RuntimeError("N19 historical terminal state proof conflicts")
        try:
            decoded_state = decode_n19_state_evidence(
                state[3], expected_symbol=state[0]
            )
        except Exception as exc:
            raise RuntimeError(
                "N19 historical terminal state evidence is invalid"
            ) from exc
        if (
            decoded_state.family_id != state[1]
            or decoded_state.structure_id != state[2]
            or decoded_state.evidence_sha256 != state[4]
            or decoded_state.stage != "MISSED"
            or decoded_state.reason != "N19_HISTORICAL_ENTRY_MISSED"
        ):
            raise RuntimeError("N19 historical terminal state proof conflicts")
    for row in terminal_rows:
        key = tuple(row[:3])
        seal = seals.get(key)
        if (
            seal is None
            or seal[3] != row[3]
            or seal[4] != row[5]
            or seal[5] != PROVED_TERMINAL_DOMAIN
            or seal[6] != row[6]
            or seal[7]
            != family_seal_sha256(
                row[1], row[2], row[3], row[5], seal[5], row[6]
            )
        ):
            raise RuntimeError("N19 normal terminal family seal conflicts")
    overlays = {
        row[0]: tuple(row)
        for row in connection.execute(
            "SELECT coverage_receipt_sha256,source_scan_id,strategy_id,symbol,"
            "terminal_binding_sha256,terminal_receipt_sha256 "
            "FROM history_coverage_authorized_terminal_bindings "
            "ORDER BY source_scan_id,strategy_id,symbol"
        )
    }
    if len(overlays) != len(terminal_rows):
        raise RuntimeError("authorized terminal binding proof set is incomplete")
    from .coverage_epoch_schema import (
        n19_terminal_publication_binding_sha256,
    )

    for terminal in connection.execute(
        "SELECT source_scan_id,strategy_id,symbol,family_id,structure_id,"
        "terminal_evidence_json,terminal_evidence_sha256,"
        "batch_expected_count,batch_manifest_sha256,"
        "coverage_receipt_sha256,result_epoch_ordinal,"
        "result_epoch_start_time_ms,result_chain_head_sha256,"
        "receipt_sha256 "
        "FROM history_coverage_n19_terminal_receipts"
    ):
        expected_binding = n19_terminal_publication_binding_sha256(
            terminal[0],
            terminal[2],
            terminal[3],
            terminal[4],
            terminal[5],
            terminal[6],
            terminal[7],
            terminal[8],
            terminal[10],
            terminal[11],
            terminal[12],
        )
        overlay = overlays.get(terminal[9])
        if overlay != (
            terminal[9],
            terminal[0],
            terminal[1],
            terminal[2],
            expected_binding,
            terminal[13],
        ):
            raise RuntimeError(
                "authorized terminal binding overlay conflicts"
            )
    for raw in witness_rows:
        row = tuple(raw)
        if (
            len(row) != 18
            or row[0] != "N19"
            or row[1] != AUTHORIZED_SYMBOL
            or row[2] != AUTHORIZED_FAMILY_ID
            or row[3] != AUTHORIZED_STRUCTURE_ID
            or row[4] != "MISSED"
            or row[5] != "N19_HISTORICAL_ENTRY_MISSED"
            or type(row[6]) is not str
            or not _valid_hash(row[7])
            or not _valid_hash(row[8])
            or type(row[9]) is not int
            or row[9] <= 0
            or any(not _valid_hash(value) for value in row[10:15])
            or row[15] != AUTHORIZED_STATEMENT_SHA256
            or row[16] != AUTHORIZED_GAP_REASON
            or not _valid_hash(row[17])
        ):
            raise RuntimeError("N19 legacy witness is invalid")
        record = decode_n19_state_evidence(row[6], expected_symbol=row[1])
        if (
            record.family_id != row[2]
            or record.structure_id != row[3]
            or record.stage != row[4]
            or record.reason != row[5]
            or record.evidence_sha256 != row[7]
        ):
            raise RuntimeError("N19 legacy witness evidence conflicts")
        plan = AuthorizedLegacyWitnessPlan(
            row[1], row[2], row[3], row[7], row[8], row[9], row[10],
            row[11], row[12], row[13], row[15], row[14],
        )
        if row[17] != authorized_witness_sha256(plan, row[6]):
            raise RuntimeError("N19 legacy witness digest conflicts")
        _attest_v3_snapshot_boundary(connection, row)
        seal = seals.get((row[0], row[1], row[2]))
        if (
            seal is None
            or seal[3] != row[3]
            or seal[4] != row[7]
            or seal[5] != AUTHORIZED_LEGACY_DOMAIN
            or seal[6] != row[17]
            or seal[7]
            != family_seal_sha256(
                row[1], row[2], row[3], row[7], seal[5], row[17]
            )
        ):
            raise RuntimeError("N19 legacy witness family seal conflicts")
        state = connection.execute(
            "SELECT * FROM n19_staircase_states "
            "WHERE strategy_id='N19' AND symbol=? AND family_id=?",
            (row[1], row[2]),
        ).fetchall()
        if len(state) != 1:
            raise RuntimeError("N19 legacy witness state row conflicts")
        current_state_sha = typed_row_sha256(tuple(state[0]))
        if current_state_sha == row[8]:
            reset_rows = connection.execute(
                "SELECT 1 FROM history_coverage_n19_legacy_reset_successors"
            ).fetchall()
            if reset_rows:
                raise RuntimeError(
                    "N19 legacy reset successor has no state descendant"
                )
            continue
        reset_rows = connection.execute(
            "SELECT strategy_id,symbol,family_id,structure_id,"
            "original_state_row_sha256,reset_after_time_ms,"
            "successor_evidence_json,successor_evidence_sha256,"
            "successor_state_row_sha256 "
            "FROM history_coverage_n19_legacy_reset_successors"
        ).fetchall()
        if len(reset_rows) != 1:
            raise RuntimeError("N19 legacy reset successor is missing")
        reset = tuple(reset_rows[0])
        try:
            successor = decode_n19_state_evidence(
                reset[6], expected_symbol=row[1]
            )
        except Exception as exc:
            raise RuntimeError(
                "N19 legacy reset successor evidence is invalid"
            ) from exc
        terminal_cutoff = record.evidence.get("terminal_cutoff_time_ms")
        if (
            reset[:5]
            != ("N19", row[1], row[2], row[3], row[8])
            or type(reset[5]) is not int
            or type(terminal_cutoff) is not int
            or reset[5] <= terminal_cutoff
            or successor.family_id != row[2]
            or successor.structure_id != row[3]
            or successor.stage != row[4]
            or successor.reason != row[5]
            or successor.reset_after_time_ms != reset[5]
            or successor.evidence_sha256 != reset[7]
            or reset[8] != current_state_sha
            or tuple(state[0])[10] != reset[5]
            or tuple(state[0])[11] != reset[6]
            or tuple(state[0])[12] != reset[7]
        ):
            raise RuntimeError("N19 legacy reset successor conflicts")

    from .n15_terminal_schema import (
        n15_terminal_schema_status,
        validate_n15_terminal_graph,
    )

    if n15_terminal_schema_status(
        connection, validate_graph=False
    ) == "CURRENT":
        validate_n15_terminal_graph(connection)
