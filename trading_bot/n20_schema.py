from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any


N20_SCHEMA_VERSION = 1
N20_RULE_VERSION = "N20_V1"


def _normalized_sql(value: str) -> str:
    return "".join(value.lower().split()).replace('"', "").replace("`", "")


N20_EPISODE_TABLE_SQL = """
CREATE TABLE n20_market_episodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N20'),
    episode_id TEXT NOT NULL CHECK(
        length(episode_id) = 24 AND episode_id NOT GLOB '*[^0-9a-f]*'
    ),
    stage TEXT NOT NULL CHECK(stage IN (
        'BULL_CONTEXT_FROZEN','PULLBACK_ACTIVE','RECOVERY_FROZEN',
        'ENTRY_WAITING','CONFIRMED','CONSUMED','MISSED','INVALID',
        'CRASH_VETO','EXPIRED'
    )),
    reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 128),
    m0_open_time_ms INTEGER NOT NULL CHECK(
        typeof(m0_open_time_ms) = 'integer' AND m0_open_time_ms > 0
    ),
    d1_open_time_ms INTEGER NOT NULL CHECK(
        typeof(d1_open_time_ms) = 'integer'
        AND d1_open_time_ms = m0_open_time_ms + 900000
    ),
    c_open_time_ms INTEGER CHECK(
        c_open_time_ms IS NULL OR (
            typeof(c_open_time_ms) = 'integer'
            AND c_open_time_ms >= d1_open_time_ms + 1800000
        )
    ),
    winner_symbol TEXT CHECK(
        winner_symbol IS NULL OR length(winner_symbol) BETWEEN 5 AND 32
    ),
    structure_id TEXT CHECK(
        structure_id IS NULL OR (
            length(structure_id) = 24
            AND structure_id NOT GLOB '*[^0-9a-f]*'
        )
    ),
    terminal_cutoff_time_ms INTEGER CHECK(
        terminal_cutoff_time_ms IS NULL OR (
            typeof(terminal_cutoff_time_ms) = 'integer'
            AND terminal_cutoff_time_ms >= d1_open_time_ms
        )
    ),
    evidence_blob BLOB NOT NULL CHECK(
        typeof(evidence_blob) = 'blob' AND length(evidence_blob) <= 2097152
    ),
    evidence_size INTEGER NOT NULL CHECK(
        typeof(evidence_size) = 'integer'
        AND evidence_size > 0 AND evidence_size <= 8388608
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

_GUARD_DIGEST = hashlib.sha256(
    (N20_RULE_VERSION + "|bull-market-leader-episode-v1").encode("utf-8")
).hexdigest()

N20_INSTALLATION_TABLE_SQL = """
CREATE TABLE n20_lifecycle_installation (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N20'),
    schema_version INTEGER NOT NULL CHECK(
        typeof(schema_version) = 'integer' AND schema_version = 1
    ),
    rule_version TEXT NOT NULL CHECK(rule_version = 'N20_V1'),
    guard_sha256 TEXT NOT NULL CHECK(guard_sha256 = '%s'),
    catalog_schema_version INTEGER NOT NULL CHECK(
        typeof(catalog_schema_version) = 'integer'
        AND catalog_schema_version > 0
    ),
    installed_at TEXT NOT NULL
)
""".strip() % _GUARD_DIGEST

N20_INDEX_SQL = {
    "idx_n20_episode_active": (
        "CREATE INDEX idx_n20_episode_active ON n20_market_episodes("
        "strategy_id,stage,d1_open_time_ms DESC,id DESC) WHERE stage IN ("
        "'BULL_CONTEXT_FROZEN','PULLBACK_ACTIVE','RECOVERY_FROZEN',"
        "'ENTRY_WAITING','CONFIRMED')"
    ),
    "idx_n20_episode_identity": (
        "CREATE UNIQUE INDEX idx_n20_episode_identity "
        "ON n20_market_episodes(strategy_id,episode_id)"
    ),
    "idx_n20_episode_structure": (
        "CREATE UNIQUE INDEX idx_n20_episode_structure "
        "ON n20_market_episodes(strategy_id,structure_id) "
        "WHERE structure_id IS NOT NULL"
    ),
    "idx_n20_episode_latest": (
        "CREATE INDEX idx_n20_episode_latest ON n20_market_episodes("
        "strategy_id,d1_open_time_ms DESC,id DESC)"
    ),
}

N20_TRIGGER_SQL = {
    "trg_n20_episode_no_delete": """
CREATE TRIGGER trg_n20_episode_no_delete
BEFORE DELETE ON n20_market_episodes
BEGIN SELECT RAISE(ABORT, 'N20 episode evidence is permanent'); END
""".strip(),
    "trg_n20_episode_identity_immutable": """
CREATE TRIGGER trg_n20_episode_identity_immutable
BEFORE UPDATE ON n20_market_episodes
WHEN OLD.strategy_id != NEW.strategy_id
  OR OLD.episode_id != NEW.episode_id
  OR OLD.m0_open_time_ms != NEW.m0_open_time_ms
  OR OLD.d1_open_time_ms != NEW.d1_open_time_ms
  OR OLD.created_at != NEW.created_at
  OR (OLD.c_open_time_ms IS NOT NULL AND OLD.c_open_time_ms IS NOT NEW.c_open_time_ms)
  OR (OLD.winner_symbol IS NOT NULL AND OLD.winner_symbol IS NOT NEW.winner_symbol)
  OR (OLD.structure_id IS NOT NULL AND OLD.structure_id IS NOT NEW.structure_id)
  OR OLD.stage IN ('CONSUMED','MISSED','INVALID','CRASH_VETO','EXPIRED')
BEGIN SELECT RAISE(ABORT, 'N20 episode identity is immutable'); END
""".strip(),
    "trg_n20_installation_immutable": """
CREATE TRIGGER trg_n20_installation_immutable
BEFORE UPDATE ON n20_lifecycle_installation
BEGIN SELECT RAISE(ABORT, 'N20 installation is immutable'); END
""".strip(),
    "trg_n20_installation_no_delete": """
CREATE TRIGGER trg_n20_installation_no_delete
BEFORE DELETE ON n20_lifecycle_installation
BEGIN SELECT RAISE(ABORT, 'N20 installation is permanent'); END
""".strip(),
}

N20_TABLES = ("n20_market_episodes", "n20_lifecycle_installation")

_EXPECTED_INDEX_LIST = {
    "n20_market_episodes": {
        "idx_n20_episode_active": (0, "c", 1),
        "idx_n20_episode_identity": (1, "c", 0),
        "idx_n20_episode_structure": (1, "c", 1),
        "idx_n20_episode_latest": (0, "c", 0),
        "sqlite_autoindex_n20_market_episodes_1": (1, "u", 0),
    },
    "n20_lifecycle_installation": {},
}

_EXPECTED_INDEX_XINFO = {
    "idx_n20_episode_active": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 3, "stage", 0, "BINARY", 1),
        (2, 6, "d1_open_time_ms", 1, "BINARY", 1),
        (3, 0, "id", 1, "BINARY", 1),
        (4, -1, None, 0, "BINARY", 0),
    ),
    "idx_n20_episode_identity": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 2, "episode_id", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
    "idx_n20_episode_structure": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 9, "structure_id", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
    "idx_n20_episode_latest": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 6, "d1_open_time_ms", 1, "BINARY", 1),
        (2, 0, "id", 1, "BINARY", 1),
        (3, -1, None, 0, "BINARY", 0),
    ),
}


def _owned_names() -> set[str]:
    return {*N20_TABLES, *N20_INDEX_SQL, *N20_TRIGGER_SQL}


def n20_schema_status(connection: sqlite3.Connection) -> str:
    rows = connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE sql IS NOT NULL "
        "AND (lower(name) GLOB 'n20*' OR lower(name) GLOB 'idx_n20*' "
        "OR lower(name) GLOB 'trg_n20*' OR lower(tbl_name) GLOB 'n20*') "
        "ORDER BY type,name"
    ).fetchall()
    if not rows:
        return "PRE_N20"
    if {row[1] for row in rows} != _owned_names():
        raise RuntimeError("N20 lifecycle schema is partial or contains unknown objects")
    expected_tables = {
        "n20_market_episodes": N20_EPISODE_TABLE_SQL,
        "n20_lifecycle_installation": N20_INSTALLATION_TABLE_SQL,
    }
    for table, expected in expected_tables.items():
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?", (table,)
        ).fetchall()
        if (
            len(row) != 1 or type(row[0][0]) is not str
            or _normalized_sql(row[0][0]) != _normalized_sql(expected)
            or connection.execute('PRAGMA foreign_key_list("%s")' % table).fetchall()
        ):
            raise RuntimeError("N20 lifecycle table is inconsistent: %s" % table)
    for table, expected in _EXPECTED_INDEX_LIST.items():
        actual = {
            row[1]: tuple(row[2:5])
            for row in connection.execute('PRAGMA index_list("%s")' % table)
            if len(row) >= 5 and type(row[1]) is str
        }
        if actual != expected:
            raise RuntimeError("N20 lifecycle index set is inconsistent: %s" % table)
    for name, expected in N20_INDEX_SQL.items():
        row = connection.execute(
            "SELECT tbl_name,sql FROM sqlite_schema WHERE type='index' AND name=?", (name,)
        ).fetchall()
        if (
            len(row) != 1 or row[0][0] != "n20_market_episodes"
            or type(row[0][1]) is not str
            or _normalized_sql(row[0][1]) != _normalized_sql(expected)
        ):
            raise RuntimeError("N20 lifecycle index is inconsistent: %s" % name)
        xinfo = tuple(
            tuple(item[:6]) for item in
            connection.execute('PRAGMA index_xinfo("%s")' % name).fetchall()
        )
        if xinfo != _EXPECTED_INDEX_XINFO[name]:
            raise RuntimeError("N20 lifecycle index metadata is inconsistent: %s" % name)
    for name, expected in N20_TRIGGER_SQL.items():
        row = connection.execute(
            "SELECT tbl_name,sql FROM sqlite_schema WHERE type='trigger' AND name=?", (name,)
        ).fetchall()
        table = "n20_lifecycle_installation" if "installation" in name else "n20_market_episodes"
        if (
            len(row) != 1 or row[0][0] != table or type(row[0][1]) is not str
            or _normalized_sql(row[0][1]) != _normalized_sql(expected)
        ):
            raise RuntimeError("N20 lifecycle trigger is inconsistent: %s" % name)
    root = connection.execute(
        "SELECT singleton_id,strategy_id,schema_version,rule_version,guard_sha256,"
        "catalog_schema_version,installed_at FROM n20_lifecycle_installation"
    ).fetchall()
    if (
        len(root) != 1 or root[0][:5] != (1, "N20", 1, N20_RULE_VERSION, _GUARD_DIGEST)
        or type(root[0][5]) is not int or root[0][5] <= 0
        or type(root[0][6]) is not str or not root[0][6]
    ):
        raise RuntimeError("N20 lifecycle installation root is inconsistent")
    for table in N20_TABLES:
        actual = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='trigger' AND tbl_name=?", (table,)
        )}
        expected = {name for name, sql in N20_TRIGGER_SQL.items() if (" ON %s" % table) in sql}
        if actual != expected:
            raise RuntimeError("N20 lifecycle trigger set is inconsistent")
    lowered = {item.lower() for item in N20_TABLES}
    for (table,) in connection.execute("SELECT name FROM sqlite_schema WHERE type='table'"):
        for foreign_key in connection.execute('PRAGMA foreign_key_list("%s")' % table):
            if type(foreign_key[2]) is str and foreign_key[2].lower() in lowered:
                raise RuntimeError("N20 lifecycle tables have an incoming foreign key")
    return "CURRENT"


def validate_pre_n20_review_clean(connection: sqlite3.Connection) -> None:
    if n20_schema_status(connection) != "PRE_N20":
        raise RuntimeError("N20 pre-install validation requires PRE_N20 schema")
    tables = {row[0] for row in connection.execute(
        "SELECT name FROM sqlite_schema WHERE type='table'"
    ) if type(row[0]) is str}
    for table in (
        "strategy_definitions", "strategy_signals",
        "strategy_passed_signal_audits", "strategy_passed_structure_ledger",
        "strategy_paper_trades", "strategy_states", "strategy_live_links",
        "strategy_structure_terminal_states",
    ):
        if table in tables and connection.execute(
            'SELECT 1 FROM "%s" WHERE strategy_id=\'N20\' LIMIT 1' % table
        ).fetchone() is not None:
            raise RuntimeError("pre-N20 Review contains N20 lifecycle evidence")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise RuntimeError("pre-N20 JSON contains a duplicate key")
            result[key] = value
        return result

    def marked(value: Any) -> bool:
        if type(value) is dict:
            return any(
                (key == "strategy_id" and item == "N20")
                or (key == "rule_version" and item == N20_RULE_VERSION)
                or (key == "stop_mode" and item == "relative_strength_recovery_margin_capped")
                or marked(item)
                for key, item in value.items()
            )
        if type(value) is list:
            return any(marked(item) for item in value)
        return False

    for table, column in (("events", "payload_json"), ("trade_reviews", "orders_json")):
        if table not in tables:
            continue
        for (payload,) in connection.execute('SELECT "%s" FROM "%s"' % (column, table)):
            if type(payload) is not str:
                raise RuntimeError("pre-N20 JSON evidence type is invalid")
            parsed = json.loads(
                payload, object_pairs_hook=pairs,
                parse_constant=lambda value: (_ for _ in ()).throw(
                    RuntimeError("pre-N20 JSON contains a non-finite value")
                ),
            )
            if marked(parsed):
                raise RuntimeError("pre-N20 Review contains N20 JSON evidence")


def validate_n20_episode_graph(connection: sqlite3.Connection) -> None:
    """Stream and authenticate every permanent N20 row at ordinary startup."""

    from .n20_analyzer import decode_n20_state_evidence

    active = 0
    cursor = connection.execute(
        "SELECT strategy_id,episode_id,stage,reason,m0_open_time_ms,"
        "d1_open_time_ms,c_open_time_ms,winner_symbol,structure_id,"
        "terminal_cutoff_time_ms,evidence_blob,evidence_size,evidence_sha256 "
        "FROM n20_market_episodes ORDER BY id"
    )
    for row in cursor:
        if type(row) not in (tuple, list) or len(row) != 13:
            raise RuntimeError("N20 permanent episode row shape is invalid")
        blob = row[10] if type(row[10]) is bytes else bytes(row[10])
        try:
            decoded = decode_n20_state_evidence(blob, row[11], row[12])
        except Exception as exc:
            raise RuntimeError("N20 permanent episode evidence is invalid") from exc
        if tuple(row[:10]) != (
            decoded.strategy_id, decoded.episode_id, decoded.stage,
            decoded.reason, decoded.m0_open_time_ms, decoded.d1_open_time_ms,
            decoded.c_open_time_ms, decoded.winner_symbol,
            decoded.structure_id, decoded.terminal_cutoff_time_ms,
        ):
            raise RuntimeError("N20 permanent episode identity conflicts")
        if decoded.stage in {
            "BULL_CONTEXT_FROZEN", "PULLBACK_ACTIVE", "RECOVERY_FROZEN",
            "ENTRY_WAITING", "CONFIRMED",
        }:
            active += 1
    if active > 1:
        raise RuntimeError("multiple active N20 market episodes exist")


def install_n20_schema(connection: sqlite3.Connection, installed_at: str) -> None:
    if n20_schema_status(connection) != "PRE_N20":
        raise RuntimeError("N20 lifecycle installation requires exact PRE_N20 schema")
    connection.execute("SAVEPOINT install_n20_schema")
    try:
        connection.execute(N20_EPISODE_TABLE_SQL)
        connection.execute(N20_INSTALLATION_TABLE_SQL)
        for sql in N20_INDEX_SQL.values():
            connection.execute(sql)
        for sql in N20_TRIGGER_SQL.values():
            connection.execute(sql)
        catalog = connection.execute("PRAGMA schema_version").fetchone()[0]
        connection.execute(
            "INSERT INTO n20_lifecycle_installation (singleton_id,strategy_id,"
            "schema_version,rule_version,guard_sha256,catalog_schema_version,installed_at) "
            "VALUES (1,'N20',1,'N20_V1',?,?,?)",
            (_GUARD_DIGEST, catalog, installed_at),
        )
        if n20_schema_status(connection) != "CURRENT":
            raise RuntimeError("N20 lifecycle installation did not attest")
        connection.execute("RELEASE SAVEPOINT install_n20_schema")
    except BaseException:
        connection.execute("ROLLBACK TO SAVEPOINT install_n20_schema")
        connection.execute("RELEASE SAVEPOINT install_n20_schema")
        raise
