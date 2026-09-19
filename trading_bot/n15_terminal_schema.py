from __future__ import annotations

import hashlib
import json
import sqlite3
import zlib
from datetime import datetime, timezone
from typing import Any


N15_TERMINAL_SCHEMA_VERSION = 1
N15_TERMINAL_RULE_VERSION = "N15_SNAPSHOT_TERMINAL_V1"
N15_NO_WINNER_CLOSE_REASON = "N15_NO_WINNER_SNAPSHOT_CLOSED"
N15_WINNER_CLOSE_REASON = "N15_WINNER_SNAPSHOT_CLOSED"


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _snapshot_json(value: Any) -> str:
    """Match the already-frozen N15 payload encoding byte for byte."""

    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
        sort_keys=True,
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _valid_hash(value: Any) -> bool:
    return bool(
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _normalized_sql(value: Any) -> str:
    if type(value) is not str:
        raise RuntimeError("N15 terminal schema SQL is missing")
    return "".join(value.lower().split()).replace('"', "").replace("`", "")


def n15_snapshot_payload_sha256(payload_json: str) -> str:
    if type(payload_json) is not str:
        raise RuntimeError("N15 snapshot payload text is invalid")
    return _sha256_bytes(payload_json.encode("utf-8"))


def n15_terminal_receipt_sha256(
    strategy_id: str,
    e_time: str,
    winner_symbol: str | None,
    snapshot_payload_sha256: str,
    snapshot_blob_sha256: str,
    close_reason: str,
    entry_symbol: str | None,
    entry_structure_id: str | None,
    entry_status: str | None,
    entry_reason: str | None,
    entry_detail_sha256: str | None,
    closed_at: str,
) -> str:
    return _sha256_bytes(
        _canonical_json(
            {
                "schema_version": N15_TERMINAL_SCHEMA_VERSION,
                "rule_version": N15_TERMINAL_RULE_VERSION,
                "strategy_id": strategy_id,
                "e_time": e_time,
                "winner_symbol": winner_symbol,
                "snapshot_payload_sha256": snapshot_payload_sha256,
                "snapshot_blob_sha256": snapshot_blob_sha256,
                "close_reason": close_reason,
                "entry_symbol": entry_symbol,
                "entry_structure_id": entry_structure_id,
                "entry_status": entry_status,
                "entry_reason": entry_reason,
                "entry_detail_sha256": entry_detail_sha256,
                "closed_at": closed_at,
            }
        ).encode("utf-8")
    )


N15_TERMINAL_RECEIPT_TABLE_SQL = """
CREATE TABLE n15_snapshot_terminal_receipts (
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N15'),
    e_time TEXT NOT NULL CHECK(
        length(e_time) BETWEEN 1 AND 32
        AND e_time NOT GLOB '*[^0-9]*'
    ),
    winner_symbol TEXT CHECK(
        winner_symbol IS NULL OR length(winner_symbol) BETWEEN 5 AND 32
    ),
    snapshot_payload_blob BLOB NOT NULL CHECK(
        typeof(snapshot_payload_blob) = 'blob'
        AND length(snapshot_payload_blob) BETWEEN 1 AND 2097152
    ),
    snapshot_payload_size INTEGER NOT NULL CHECK(
        typeof(snapshot_payload_size) = 'integer'
        AND snapshot_payload_size BETWEEN 1 AND 8388608
    ),
    snapshot_payload_sha256 TEXT NOT NULL CHECK(
        length(snapshot_payload_sha256) = 64
        AND snapshot_payload_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    snapshot_blob_sha256 TEXT NOT NULL CHECK(
        length(snapshot_blob_sha256) = 64
        AND snapshot_blob_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    close_reason TEXT NOT NULL CHECK(close_reason IN (
        'N15_NO_WINNER_SNAPSHOT_CLOSED',
        'N15_WINNER_SNAPSHOT_CLOSED'
    )),
    entry_symbol TEXT,
    entry_structure_id TEXT,
    entry_status TEXT,
    entry_reason TEXT,
    entry_detail_json TEXT,
    entry_detail_sha256 TEXT,
    receipt_sha256 TEXT NOT NULL UNIQUE CHECK(
        length(receipt_sha256) = 64
        AND receipt_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    closed_at TEXT NOT NULL,
    PRIMARY KEY(strategy_id,e_time),
    CHECK(
        (winner_symbol IS NULL
         AND close_reason = 'N15_NO_WINNER_SNAPSHOT_CLOSED'
         AND entry_symbol IS NULL AND entry_structure_id IS NULL
         AND entry_status IS NULL AND entry_reason IS NULL
         AND entry_detail_json IS NULL AND entry_detail_sha256 IS NULL)
        OR
        (winner_symbol IS NOT NULL
         AND close_reason = 'N15_WINNER_SNAPSHOT_CLOSED'
         AND entry_symbol = winner_symbol
         AND length(entry_structure_id) = 24
         AND entry_structure_id NOT GLOB '*[^0-9a-f]*'
         AND entry_status IN ('CONSUMED','MISSED')
         AND length(entry_reason) BETWEEN 1 AND 128
         AND entry_detail_json IS NOT NULL
         AND length(entry_detail_sha256) = 64
         AND entry_detail_sha256 NOT GLOB '*[^0-9a-f]*')
    )
)
""".strip()


N15_TERMINAL_INSTALLATION_TABLE_SQL = f"""
CREATE TABLE n15_snapshot_terminal_installation (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    schema_version INTEGER NOT NULL CHECK(
        typeof(schema_version) = 'integer'
        AND schema_version = {N15_TERMINAL_SCHEMA_VERSION}
    ),
    rule_version TEXT NOT NULL CHECK(
        rule_version = '{N15_TERMINAL_RULE_VERSION}'
    ),
    legacy_entry_state_count INTEGER NOT NULL CHECK(
        typeof(legacy_entry_state_count) = 'integer'
        AND legacy_entry_state_count >= 0
    ),
    legacy_entry_state_sha256 TEXT NOT NULL CHECK(
        length(legacy_entry_state_sha256) = 64
        AND legacy_entry_state_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    catalog_sha256 TEXT NOT NULL CHECK(
        length(catalog_sha256) = 64
        AND catalog_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    installed_at TEXT NOT NULL
)
""".strip()


N15_TERMINAL_INDEX_SQL = {
    "idx_n15_snapshot_terminal_winner": (
        "CREATE INDEX idx_n15_snapshot_terminal_winner "
        "ON n15_snapshot_terminal_receipts("
        "strategy_id,winner_symbol,e_time)"
    ),
}


N15_TERMINAL_TRIGGER_SQL = {
    "trg_n15_snapshot_terminal_insert_authorized": """
CREATE TRIGGER trg_n15_snapshot_terminal_insert_authorized
BEFORE INSERT ON n15_snapshot_terminal_receipts
WHEN _coverage_epoch_mutation_authorized(
  'n15_snapshot_terminal_insert',NEW.strategy_id,
  COALESCE(NEW.winner_symbol,'NO_WINNER'),NULL,
  CAST(NEW.e_time AS INTEGER),NEW.receipt_sha256
) != 1
BEGIN SELECT RAISE(ABORT, 'N15 snapshot terminal insert is unauthorized'); END
""".strip(),
    "trg_n15_snapshot_terminal_requires_active": """
CREATE TRIGGER trg_n15_snapshot_terminal_requires_active
BEFORE INSERT ON n15_snapshot_terminal_receipts
WHEN NOT EXISTS(
  SELECT 1 FROM n15_market_snapshots AS active
  WHERE active.strategy_id=NEW.strategy_id
    AND active.e_time=NEW.e_time
    AND active.payload_sha256=NEW.snapshot_payload_sha256
)
BEGIN SELECT RAISE(ABORT, 'N15 terminal receipt lacks its active snapshot'); END
""".strip(),
    "trg_n15_snapshot_terminal_requires_entry": """
CREATE TRIGGER trg_n15_snapshot_terminal_requires_entry
BEFORE INSERT ON n15_snapshot_terminal_receipts
WHEN (
  NEW.winner_symbol IS NULL AND EXISTS(
    SELECT 1 FROM n15_entry_states AS state
    WHERE state.strategy_id=NEW.strategy_id AND state.e_time=NEW.e_time
  )
) OR (
  NEW.winner_symbol IS NOT NULL AND NOT EXISTS(
    SELECT 1 FROM n15_entry_states AS state
    WHERE state.strategy_id=NEW.strategy_id AND state.e_time=NEW.e_time
      AND state.symbol=NEW.entry_symbol
      AND state.structure_id=NEW.entry_structure_id
      AND state.status=NEW.entry_status
      AND state.reason=NEW.entry_reason
      AND state.detail_json=NEW.entry_detail_json
  )
)
BEGIN SELECT RAISE(ABORT, 'N15 terminal receipt entry proof conflicts'); END
""".strip(),
    "trg_n15_snapshot_terminal_no_replace": """
CREATE TRIGGER trg_n15_snapshot_terminal_no_replace
BEFORE INSERT ON n15_snapshot_terminal_receipts
WHEN EXISTS(
  SELECT 1 FROM n15_snapshot_terminal_receipts
  WHERE strategy_id=NEW.strategy_id AND e_time=NEW.e_time
)
BEGIN SELECT RAISE(ABORT, 'N15 terminal receipt replacement is forbidden'); END
""".strip(),
    "trg_n15_snapshot_terminal_no_update": """
CREATE TRIGGER trg_n15_snapshot_terminal_no_update
BEFORE UPDATE ON n15_snapshot_terminal_receipts
BEGIN SELECT RAISE(ABORT, 'N15 terminal receipts are immutable'); END
""".strip(),
    "trg_n15_snapshot_terminal_no_delete": """
CREATE TRIGGER trg_n15_snapshot_terminal_no_delete
BEFORE DELETE ON n15_snapshot_terminal_receipts
BEGIN SELECT RAISE(ABORT, 'N15 terminal receipts are permanent'); END
""".strip(),
    "trg_n15_snapshot_terminal_close_active": """
CREATE TRIGGER trg_n15_snapshot_terminal_close_active
AFTER INSERT ON n15_snapshot_terminal_receipts
BEGIN
  DELETE FROM n15_market_snapshots
  WHERE strategy_id=NEW.strategy_id AND e_time=NEW.e_time
    AND payload_sha256=NEW.snapshot_payload_sha256;
END
""".strip(),
    "trg_n15_market_snapshot_delete_requires_terminal": """
CREATE TRIGGER trg_n15_market_snapshot_delete_requires_terminal
BEFORE DELETE ON n15_market_snapshots
WHEN _coverage_epoch_mutation_authorized(
  'n15_snapshot_terminal_delete',OLD.strategy_id,
  COALESCE((
    SELECT receipt.winner_symbol
    FROM n15_snapshot_terminal_receipts AS receipt
    WHERE receipt.strategy_id=OLD.strategy_id AND receipt.e_time=OLD.e_time
  ),'NO_WINNER'),NULL,CAST(OLD.e_time AS INTEGER),(
    SELECT receipt.receipt_sha256
    FROM n15_snapshot_terminal_receipts AS receipt
    WHERE receipt.strategy_id=OLD.strategy_id AND receipt.e_time=OLD.e_time
  )
) != 1 OR NOT EXISTS(
  SELECT 1 FROM n15_snapshot_terminal_receipts AS receipt
  WHERE receipt.strategy_id=OLD.strategy_id
    AND receipt.e_time=OLD.e_time
    AND receipt.snapshot_payload_sha256=OLD.payload_sha256
)
BEGIN SELECT RAISE(ABORT, 'N15 active snapshot terminal deletion is unauthorized'); END
""".strip(),
    "trg_n15_entry_state_insert_requires_snapshot": """
CREATE TRIGGER trg_n15_entry_state_insert_requires_snapshot
BEFORE INSERT ON n15_entry_states
WHEN NOT EXISTS(
  SELECT 1 FROM n15_market_snapshots AS active
  WHERE active.strategy_id=NEW.strategy_id AND active.e_time=NEW.e_time
)
AND NOT EXISTS(
  SELECT 1 FROM n15_snapshot_terminal_receipts AS receipt
  WHERE receipt.strategy_id=NEW.strategy_id AND receipt.e_time=NEW.e_time
)
BEGIN SELECT RAISE(ABORT, 'N15 entry state lacks snapshot evidence'); END
""".strip(),
    "trg_n15_entry_state_no_update": """
CREATE TRIGGER trg_n15_entry_state_no_update
BEFORE UPDATE ON n15_entry_states
BEGIN SELECT RAISE(ABORT, 'N15 entry states are immutable'); END
""".strip(),
    "trg_n15_entry_state_no_delete": """
CREATE TRIGGER trg_n15_entry_state_no_delete
BEFORE DELETE ON n15_entry_states
BEGIN SELECT RAISE(ABORT, 'N15 entry states are permanent'); END
""".strip(),
    "trg_n15_snapshot_terminal_install_insert_authorized": """
CREATE TRIGGER trg_n15_snapshot_terminal_install_insert_authorized
BEFORE INSERT ON n15_snapshot_terminal_installation
WHEN _coverage_epoch_mutation_authorized(
  'n15_snapshot_terminal_install','N15','INSTALLATION',NULL,NULL,
  NEW.catalog_sha256
) != 1
BEGIN SELECT RAISE(ABORT, 'N15 terminal installation is unauthorized'); END
""".strip(),
    "trg_n15_snapshot_terminal_install_no_replace": """
CREATE TRIGGER trg_n15_snapshot_terminal_install_no_replace
BEFORE INSERT ON n15_snapshot_terminal_installation
WHEN EXISTS(
  SELECT 1 FROM n15_snapshot_terminal_installation WHERE singleton_id=1
)
BEGIN SELECT RAISE(ABORT, 'N15 terminal installation replacement is forbidden'); END
""".strip(),
    "trg_n15_snapshot_terminal_install_no_update": """
CREATE TRIGGER trg_n15_snapshot_terminal_install_no_update
BEFORE UPDATE ON n15_snapshot_terminal_installation
BEGIN SELECT RAISE(ABORT, 'N15 terminal installation is immutable'); END
""".strip(),
    "trg_n15_snapshot_terminal_install_no_delete": """
CREATE TRIGGER trg_n15_snapshot_terminal_install_no_delete
BEFORE DELETE ON n15_snapshot_terminal_installation
BEGIN SELECT RAISE(ABORT, 'N15 terminal installation is permanent'); END
""".strip(),
    "trg_n15_protected_generation_terminal_insert": """
CREATE TRIGGER trg_n15_protected_generation_terminal_insert
AFTER INSERT ON n15_snapshot_terminal_receipts
BEGIN UPDATE history_coverage_protected_generation
SET generation=generation+1 WHERE singleton_id=1; END
""".strip(),
}


N15_TERMINAL_TABLES = (
    "n15_snapshot_terminal_receipts",
    "n15_snapshot_terminal_installation",
)
N15_TERMINAL_OBJECTS = {
    *N15_TERMINAL_TABLES,
    *N15_TERMINAL_INDEX_SQL,
    *N15_TERMINAL_TRIGGER_SQL,
}


def _owned_objects(connection: sqlite3.Connection) -> dict[str, tuple[str, str]]:
    rows = connection.execute(
        "SELECT type,name,sql FROM sqlite_schema WHERE "
        "name LIKE 'n15_snapshot_terminal%' "
        "OR name LIKE 'idx_n15_snapshot_terminal%' "
        "OR name LIKE 'trg_n15_snapshot_terminal%' "
        "OR name LIKE 'trg_n15_market_snapshot_%' "
        "OR name LIKE 'trg_n15_entry_state_%' "
        "OR name='trg_n15_protected_generation_terminal_insert' "
        "ORDER BY type,name"
    ).fetchall()
    result: dict[str, tuple[str, str]] = {}
    for row in rows:
        if (
            type(row) not in (tuple, list)
            or len(row) != 3
            or any(type(value) is not str for value in row)
            or row[1] in result
        ):
            raise RuntimeError("N15 terminal catalog is invalid")
        result[row[1]] = (row[0], row[2])
    return result


def n15_terminal_catalog_sha256(connection: sqlite3.Connection) -> str:
    placeholders = ",".join("?" for _ in N15_TERMINAL_OBJECTS)
    rows = connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema "
        f"WHERE name IN ({placeholders}) ORDER BY type,name",
        tuple(sorted(N15_TERMINAL_OBJECTS)),
    ).fetchall()
    if len(rows) != len(N15_TERMINAL_OBJECTS):
        raise RuntimeError("N15 terminal catalog is incomplete")
    payload_column = tuple(
        tuple(row)
        for row in connection.execute(
            "PRAGMA table_xinfo(n15_market_snapshots)"
        )
    )
    return _sha256_bytes(
        _canonical_json(
            {
                "objects": [
                    [row[0], row[1], row[2], _normalized_sql(row[3])]
                    for row in rows
                ],
                "active_snapshot_xinfo": payload_column,
            }
        ).encode("utf-8")
    )


def _legacy_state_commitment(
    connection: sqlite3.Connection,
    installed_at: str,
) -> tuple[int, str]:
    rows = [
        tuple(row)
        for row in connection.execute(
            "SELECT id,strategy_id,e_time,symbol,structure_id,status,reason,"
            "detail_json,created_at,updated_at FROM n15_entry_states "
            "WHERE created_at<=? ORDER BY id",
            (installed_at,),
        )
    ]
    return len(rows), _sha256_bytes(_canonical_json(rows).encode("utf-8"))


def n15_terminal_schema_status(
    connection: sqlite3.Connection,
    *,
    validate_graph: bool = True,
) -> str:
    owned = _owned_objects(connection)
    columns = {
        row[1]: tuple(row)
        for row in connection.execute(
            "PRAGMA table_xinfo(n15_market_snapshots)"
        )
    }
    has_payload_sha = "payload_sha256" in columns
    if not owned:
        if has_payload_sha:
            raise RuntimeError("N15 terminal schema is half-installed")
        return "PRE_N15_TERMINAL"
    if set(owned) != N15_TERMINAL_OBJECTS or not has_payload_sha:
        raise RuntimeError("N15 terminal schema is partial")
    expected_sql = {
        "n15_snapshot_terminal_receipts": N15_TERMINAL_RECEIPT_TABLE_SQL,
        "n15_snapshot_terminal_installation": (
            N15_TERMINAL_INSTALLATION_TABLE_SQL
        ),
        **N15_TERMINAL_INDEX_SQL,
        **N15_TERMINAL_TRIGGER_SQL,
    }
    expected_types = {
        **{name: "table" for name in N15_TERMINAL_TABLES},
        **{name: "index" for name in N15_TERMINAL_INDEX_SQL},
        **{name: "trigger" for name in N15_TERMINAL_TRIGGER_SQL},
    }
    for name, sql in expected_sql.items():
        if (
            owned[name][0] != expected_types[name]
            or _normalized_sql(owned[name][1]) != _normalized_sql(sql)
        ):
            raise RuntimeError("N15 terminal object is inconsistent: %s" % name)
    root = connection.execute(
        "SELECT schema_version,rule_version,legacy_entry_state_count,"
        "legacy_entry_state_sha256,catalog_sha256,installed_at "
        "FROM n15_snapshot_terminal_installation WHERE singleton_id=1"
    ).fetchone()
    if (
        root is None
        or root[0] != N15_TERMINAL_SCHEMA_VERSION
        or root[1] != N15_TERMINAL_RULE_VERSION
        or type(root[2]) is not int
        or root[2] < 0
        or not _valid_hash(root[3])
        or root[4] != n15_terminal_catalog_sha256(connection)
        or type(root[5]) is not str
        or not root[5]
        or _legacy_state_commitment(connection, root[5]) != (root[2], root[3])
    ):
        raise RuntimeError("N15 terminal installation is inconsistent")
    if validate_graph:
        validate_n15_terminal_graph(connection)
    return "CURRENT"


def install_n15_terminal_schema(
    connection: sqlite3.Connection,
    installed_at: str,
) -> None:
    if n15_terminal_schema_status(connection, validate_graph=False) == "CURRENT":
        return
    if type(installed_at) is not str or not installed_at:
        raise RuntimeError("N15 terminal installation time is invalid")
    columns = {
        row[1] for row in connection.execute(
            "PRAGMA table_xinfo(n15_market_snapshots)"
        )
    }
    if not columns or "payload_sha256" in columns:
        raise RuntimeError("N15 active snapshot schema is not installable")
    connection.execute(
        "ALTER TABLE n15_market_snapshots ADD COLUMN payload_sha256 TEXT"
    )
    from .n15_snapshot import validate_n15_snapshot_envelope

    for row in connection.execute(
        "SELECT id,strategy_id,e_time,payload_json FROM n15_market_snapshots "
        "ORDER BY id"
    ).fetchall():
        try:
            e_time_ms = int(row[2])
            payload = json.loads(row[3])
            validate_n15_snapshot_envelope(payload, row[1], e_time_ms)
            if _snapshot_json(payload) != row[3]:
                raise ValueError
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "N15 active snapshot cannot be migrated losslessly"
            ) from exc
        payload_sha = n15_snapshot_payload_sha256(row[3])
        changed = connection.execute(
            "UPDATE n15_market_snapshots SET payload_sha256=? WHERE id=? "
            "AND payload_sha256 IS NULL",
            (payload_sha, row[0]),
        )
        if changed.rowcount != 1:
            raise RuntimeError("N15 active snapshot hash migration conflicted")
    for sql in (
        N15_TERMINAL_RECEIPT_TABLE_SQL,
        N15_TERMINAL_INSTALLATION_TABLE_SQL,
        *N15_TERMINAL_INDEX_SQL.values(),
        *N15_TERMINAL_TRIGGER_SQL.values(),
    ):
        connection.execute(sql)
    count, legacy_sha = _legacy_state_commitment(connection, installed_at)
    catalog_sha = n15_terminal_catalog_sha256(connection)
    connection.execute(
        "INSERT INTO n15_snapshot_terminal_installation VALUES "
        "(1,?,?,?,?,?,?)",
        (
            N15_TERMINAL_SCHEMA_VERSION,
            N15_TERMINAL_RULE_VERSION,
            count,
            legacy_sha,
            catalog_sha,
            installed_at,
        ),
    )
    if n15_terminal_schema_status(connection) != "CURRENT":
        raise RuntimeError("N15 terminal installation did not attest")


def decode_n15_terminal_payload(row: tuple[Any, ...]) -> tuple[str, dict[str, Any]]:
    if len(row) < 7:
        raise RuntimeError("N15 terminal receipt row is incomplete")
    blob = row[3] if type(row[3]) is bytes else bytes(row[3])
    if _sha256_bytes(blob) != row[6]:
        raise RuntimeError("N15 terminal snapshot blob hash conflicts")
    try:
        decoder = zlib.decompressobj()
        payload_bytes = decoder.decompress(blob, 8_388_609)
        if (
            len(payload_bytes) > 8_388_608
            or decoder.unconsumed_tail
            or not decoder.eof
            or decoder.unused_data
        ):
            raise ValueError
        payload_json = payload_bytes.decode("utf-8")
    except (UnicodeError, ValueError, zlib.error) as exc:
        raise RuntimeError("N15 terminal snapshot cannot be restored") from exc
    if len(payload_bytes) != row[4] or _sha256_bytes(payload_bytes) != row[5]:
        raise RuntimeError("N15 terminal snapshot payload hash conflicts")
    try:
        payload = json.loads(payload_json)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("N15 terminal snapshot JSON is invalid") from exc
    if _snapshot_json(payload) != payload_json:
        raise RuntimeError("N15 terminal snapshot JSON is not canonical")
    return payload_json, payload


def _validate_n15_terminal_receipt_row(
    connection: sqlite3.Connection,
    row: tuple[Any, ...],
) -> tuple[str, str]:
    from .n15_analyzer import n15_structure_id, validate_n15_state_envelope
    from .n15_snapshot import validate_n15_snapshot_envelope

    if len(row) != 16 or row[0] != "N15" or type(row[1]) is not str:
        raise RuntimeError("N15 terminal receipt identity is invalid")
    try:
        e_time_ms = int(row[1])
        payload_json, payload = decode_n15_terminal_payload(row)
        snapshot = validate_n15_snapshot_envelope(payload, row[0], e_time_ms)
    except Exception as exc:
        raise RuntimeError("N15 terminal snapshot evidence is invalid") from exc
    try:
        closed_at = datetime.fromisoformat(row[15])
    except (TypeError, ValueError) as exc:
        raise RuntimeError("N15 terminal close time is invalid") from exc
    if (
        snapshot.winner_symbol != row[2]
        or n15_snapshot_payload_sha256(payload_json) != row[5]
        or not _valid_hash(row[6])
        or not _valid_hash(row[14])
        or type(row[15]) is not str
        or not row[15]
        or closed_at.tzinfo is None
        or closed_at.utcoffset() != timezone.utc.utcoffset(closed_at)
        or closed_at.astimezone(timezone.utc).isoformat() != row[15]
        or connection.execute(
            "SELECT 1 FROM n15_market_snapshots "
            "WHERE strategy_id=? AND e_time=?",
            (row[0], row[1]),
        ).fetchone()
        is not None
    ):
        raise RuntimeError("N15 terminal receipt conflicts")
    entry_values = tuple(row[8:14])
    if row[2] is None:
        if row[7] != N15_NO_WINNER_CLOSE_REASON or any(
            value is not None for value in entry_values
        ):
            raise RuntimeError("N15 no-winner receipt conflicts")
        if connection.execute(
            "SELECT 1 FROM n15_entry_states WHERE strategy_id=? AND e_time=?",
            (row[0], row[1]),
        ).fetchone() is not None:
            raise RuntimeError("N15 no-winner receipt has an entry state")
    else:
        if row[7] != N15_WINNER_CLOSE_REASON or any(
            value is None for value in entry_values
        ):
            raise RuntimeError("N15 winner receipt is incomplete")
        state = connection.execute(
            "SELECT symbol,structure_id,status,reason,detail_json "
            "FROM n15_entry_states WHERE strategy_id=? AND e_time=?",
            (row[0], row[1]),
        ).fetchone()
        if state is None or tuple(state) != tuple(row[8:13]):
            raise RuntimeError("N15 winner receipt state conflicts")
        if _sha256_bytes(row[12].encode("utf-8")) != row[13]:
            raise RuntimeError("N15 winner receipt detail hash conflicts")
        try:
            detail = json.loads(row[12])
            validate_n15_state_envelope(
                row[0], row[1], row[8], row[9], row[10], row[11], detail
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError("N15 winner receipt state is invalid") from exc
        winner = snapshot.rows[row[2]]
        expected_structure = n15_structure_id(
            row[0], row[2], winner.b.open_time_ms, winner.c.open_time_ms
        )
        if row[8] != row[2] or row[9] != expected_structure:
            raise RuntimeError("N15 winner receipt structure conflicts")
    expected_receipt = n15_terminal_receipt_sha256(
        row[0], row[1], row[2], row[5], row[6], row[7],
        row[8], row[9], row[10], row[11], row[13], row[15],
    )
    if expected_receipt != row[14]:
        raise RuntimeError("N15 terminal receipt digest conflicts")
    return row[0], row[1]


def validate_n15_terminal_receipt(
    connection: sqlite3.Connection,
    strategy_id: str,
    e_time: str,
) -> None:
    row = connection.execute(
        "SELECT strategy_id,e_time,winner_symbol,snapshot_payload_blob,"
        "snapshot_payload_size,snapshot_payload_sha256,snapshot_blob_sha256,"
        "close_reason,entry_symbol,entry_structure_id,entry_status,"
        "entry_reason,entry_detail_json,entry_detail_sha256,receipt_sha256,"
        "closed_at FROM n15_snapshot_terminal_receipts "
        "WHERE strategy_id=? AND e_time=?",
        (strategy_id, e_time),
    ).fetchone()
    if row is None:
        raise RuntimeError("N15 terminal receipt is missing")
    if _validate_n15_terminal_receipt_row(connection, tuple(row)) != (
        strategy_id, e_time
    ):
        raise RuntimeError("N15 terminal receipt identity conflicts")


def validate_n15_terminal_graph(connection: sqlite3.Connection) -> None:
    from .n15_analyzer import n15_structure_id, validate_n15_state_envelope
    from .n15_snapshot import validate_n15_snapshot_envelope

    rows = connection.execute(
        "SELECT strategy_id,e_time,winner_symbol,snapshot_payload_blob,"
        "snapshot_payload_size,snapshot_payload_sha256,snapshot_blob_sha256,"
        "close_reason,entry_symbol,entry_structure_id,entry_status,"
        "entry_reason,entry_detail_json,entry_detail_sha256,receipt_sha256,"
        "closed_at FROM n15_snapshot_terminal_receipts "
        "ORDER BY strategy_id,CAST(e_time AS INTEGER)"
    )
    receipt_times: set[tuple[str, str]] = set()
    for raw in rows:
        row = tuple(raw)
        identity = _validate_n15_terminal_receipt_row(connection, row)
        if identity in receipt_times:
            raise RuntimeError("N15 terminal receipt digest conflicts")
        receipt_times.add(identity)

    for active in connection.execute(
        "SELECT strategy_id,e_time,payload_json,payload_sha256 "
        "FROM n15_market_snapshots ORDER BY id"
    ):
        if (
            not _valid_hash(active[3])
            or n15_snapshot_payload_sha256(active[2]) != active[3]
            or (active[0], active[1]) in receipt_times
        ):
            raise RuntimeError("N15 active snapshot identity conflicts")

    installation = connection.execute(
        "SELECT installed_at FROM n15_snapshot_terminal_installation "
        "WHERE singleton_id=1"
    ).fetchone()
    if installation is None or type(installation[0]) is not str:
        raise RuntimeError("N15 terminal installation root is missing")
    installed_at = installation[0]
    for state in connection.execute(
        "SELECT strategy_id,e_time,symbol,structure_id,status,reason,"
        "detail_json,created_at FROM n15_entry_states ORDER BY id"
    ):
        if type(state[7]) is not str:
            raise RuntimeError("N15 entry state time is invalid")
        active = connection.execute(
            "SELECT payload_json,payload_sha256 FROM n15_market_snapshots "
            "WHERE strategy_id=? AND e_time=?",
            (state[0], state[1]),
        ).fetchone()
        receipt = connection.execute(
            "SELECT winner_symbol,entry_symbol,entry_structure_id,entry_status,"
            "entry_reason,entry_detail_json FROM "
            "n15_snapshot_terminal_receipts WHERE strategy_id=? AND e_time=?",
            (state[0], state[1]),
        ).fetchone()
        if state[7] <= installed_at:
            continue
        if receipt is not None:
            if tuple(receipt[1:]) != tuple(state[2:7]):
                raise RuntimeError("N15 terminal entry reverse proof conflicts")
            continue
        if active is None:
            raise RuntimeError("N15 entry state lacks snapshot proof")
        try:
            if active[1] != n15_snapshot_payload_sha256(active[0]):
                raise ValueError
            active_payload = json.loads(active[0])
            active_snapshot = validate_n15_snapshot_envelope(
                active_payload, state[0], int(state[1])
            )
            detail = json.loads(state[6])
            validate_n15_state_envelope(
                state[0], state[1], state[2], state[3], state[4], state[5],
                detail,
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError("N15 active entry proof is invalid") from exc
        if active_snapshot.winner_symbol != state[2]:
            raise RuntimeError("N15 active entry winner conflicts")
        winner = active_snapshot.rows[state[2]]
        if state[3] != n15_structure_id(
            state[0], state[2], winner.b.open_time_ms, winner.c.open_time_ms
        ):
            raise RuntimeError("N15 active entry structure conflicts")


def build_n15_terminal_receipt(
    *,
    strategy_id: str,
    e_time: str,
    winner_symbol: str | None,
    payload_json: str,
    entry_state: tuple[Any, ...] | None,
    closed_at: str,
) -> tuple[Any, ...]:
    payload_bytes = payload_json.encode("utf-8")
    blob = zlib.compress(payload_bytes, level=9)
    payload_sha = _sha256_bytes(payload_bytes)
    blob_sha = _sha256_bytes(blob)
    if winner_symbol is None:
        close_reason = N15_NO_WINNER_CLOSE_REASON
        entry_values = (None, None, None, None, None, None)
    else:
        if entry_state is None or len(entry_state) != 5:
            raise RuntimeError("N15 winner terminal state is missing")
        close_reason = N15_WINNER_CLOSE_REASON
        detail_json = entry_state[4]
        entry_values = (
            entry_state[0], entry_state[1], entry_state[2], entry_state[3],
            detail_json, _sha256_bytes(detail_json.encode("utf-8")),
        )
    receipt_sha = n15_terminal_receipt_sha256(
        strategy_id, e_time, winner_symbol, payload_sha, blob_sha,
        close_reason, entry_values[0], entry_values[1], entry_values[2],
        entry_values[3], entry_values[5], closed_at,
    )
    return (
        strategy_id, e_time, winner_symbol, sqlite3.Binary(blob),
        len(payload_bytes), payload_sha, blob_sha, close_reason,
        *entry_values, receipt_sha, closed_at,
    )
