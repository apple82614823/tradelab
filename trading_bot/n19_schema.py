from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any


N19_SCHEMA_VERSION = 1
N19_RULE_VERSION = "N19_V1"


def _normalized_sql(value: str) -> str:
    return "".join(value.lower().split()).replace('"', "").replace("`", "")


N19_STATE_TABLE_SQL = """
CREATE TABLE n19_staircase_states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N19'),
    symbol TEXT NOT NULL CHECK(length(symbol) BETWEEN 5 AND 32),
    family_id TEXT NOT NULL CHECK(
        length(family_id) = 24 AND family_id NOT GLOB '*[^0-9a-f]*'
    ),
    structure_id TEXT CHECK(
        structure_id IS NULL OR (
            length(structure_id) = 24
            AND structure_id NOT GLOB '*[^0-9a-f]*'
        )
    ),
    stage TEXT NOT NULL CHECK(stage IN (
        'EXHAUSTION_LOCKED', 'CONFIRMATION_PENDING', 'CONFIRMED',
        'CONSUMED', 'MISSED', 'INVALID', 'EXPIRED'
    )),
    reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 128),
    quote_volume_rank INTEGER NOT NULL CHECK(
        typeof(quote_volume_rank) = 'integer'
        AND quote_volume_rank BETWEEN 1 AND 100
    ),
    s_open_time_ms INTEGER NOT NULL CHECK(
        typeof(s_open_time_ms) = 'integer' AND s_open_time_ms > 0
    ),
    x_open_time_ms INTEGER NOT NULL CHECK(
        typeof(x_open_time_ms) = 'integer'
        AND x_open_time_ms > s_open_time_ms
    ),
    reset_after_time_ms INTEGER CHECK(
        reset_after_time_ms IS NULL OR (
            typeof(reset_after_time_ms) = 'integer'
            AND reset_after_time_ms > x_open_time_ms
        )
    ),
    evidence_json TEXT NOT NULL CHECK(
        length(CAST(evidence_json AS BLOB)) <= 131072
    ),
    evidence_sha256 TEXT NOT NULL CHECK(
        length(evidence_sha256) = 64
        AND evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(strategy_id, family_id)
)
""".strip()

N19_COVERAGE_TABLE_SQL = """
CREATE TABLE n19_history_coverage (
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N19'),
    symbol TEXT NOT NULL CHECK(length(symbol) BETWEEN 5 AND 32),
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
    updated_at TEXT NOT NULL,
    PRIMARY KEY(strategy_id, symbol)
)
""".strip()

_GUARD_DIGEST = hashlib.sha256(
    (N19_RULE_VERSION + "|staircase-exhaustion-lifecycle-v1").encode("utf-8")
).hexdigest()

N19_INSTALLATION_TABLE_SQL = """
CREATE TABLE n19_lifecycle_installation (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N19'),
    schema_version INTEGER NOT NULL CHECK(
        typeof(schema_version) = 'integer' AND schema_version = 1
    ),
    rule_version TEXT NOT NULL CHECK(rule_version = 'N19_V1'),
    guard_sha256 TEXT NOT NULL CHECK(guard_sha256 = '%s'),
    catalog_schema_version INTEGER NOT NULL CHECK(
        typeof(catalog_schema_version) = 'integer'
        AND catalog_schema_version > 0
    ),
    installed_at TEXT NOT NULL
)
""".strip() % _GUARD_DIGEST

N19_INDEX_SQL = {
    "idx_n19_staircase_active": (
        "CREATE INDEX idx_n19_staircase_active ON n19_staircase_states("
        "strategy_id,stage,quote_volume_rank,symbol) WHERE stage IN ("
        "'EXHAUSTION_LOCKED','CONFIRMATION_PENDING','CONFIRMED')"
    ),
    "idx_n19_staircase_family": (
        "CREATE UNIQUE INDEX idx_n19_staircase_family "
        "ON n19_staircase_states(strategy_id,family_id)"
    ),
    "idx_n19_staircase_structure": (
        "CREATE UNIQUE INDEX idx_n19_staircase_structure "
        "ON n19_staircase_states(strategy_id,structure_id) "
        "WHERE structure_id IS NOT NULL"
    ),
    "idx_n19_staircase_symbol_latest": (
        "CREATE INDEX idx_n19_staircase_symbol_latest ON n19_staircase_states("
        "strategy_id,symbol,x_open_time_ms DESC,id DESC)"
    ),
    "idx_n19_coverage_symbol": (
        "CREATE UNIQUE INDEX idx_n19_coverage_symbol "
        "ON n19_history_coverage(strategy_id,symbol)"
    ),
}

N19_TRIGGER_SQL = {
    "trg_n19_state_no_delete": """
CREATE TRIGGER trg_n19_state_no_delete
BEFORE DELETE ON n19_staircase_states
BEGIN SELECT RAISE(ABORT, 'N19 lifecycle rows are permanent'); END
""".strip(),
    "trg_n19_state_identity_immutable": """
CREATE TRIGGER trg_n19_state_identity_immutable
BEFORE UPDATE ON n19_staircase_states
WHEN OLD.strategy_id != NEW.strategy_id
  OR OLD.symbol != NEW.symbol
  OR OLD.family_id != NEW.family_id
  OR OLD.s_open_time_ms != NEW.s_open_time_ms
  OR OLD.x_open_time_ms != NEW.x_open_time_ms
  OR OLD.created_at != NEW.created_at
  OR (OLD.structure_id IS NOT NULL AND OLD.structure_id IS NOT NEW.structure_id)
  OR (OLD.structure_id IS NULL AND NEW.structure_id IS NULL
      AND NEW.stage = 'CONFIRMED')
  OR (
    OLD.stage IN ('CONSUMED','MISSED','INVALID','EXPIRED')
    AND NOT (
      NEW.stage = OLD.stage
      AND NEW.reason = OLD.reason
      AND NEW.quote_volume_rank = OLD.quote_volume_rank
      AND OLD.reset_after_time_ms IS NULL
      AND NEW.reset_after_time_ms IS NOT NULL
      AND NEW.reset_after_time_ms > OLD.x_open_time_ms
    )
  )
BEGIN SELECT RAISE(ABORT, 'N19 lifecycle identity is immutable'); END
""".strip(),
    "trg_n19_coverage_no_delete": """
CREATE TRIGGER trg_n19_coverage_no_delete
BEFORE DELETE ON n19_history_coverage
BEGIN SELECT RAISE(ABORT, 'N19 coverage is permanent'); END
""".strip(),
    "trg_n19_coverage_monotonic": """
CREATE TRIGGER trg_n19_coverage_monotonic
BEFORE UPDATE ON n19_history_coverage
WHEN OLD.strategy_id != NEW.strategy_id
  OR OLD.symbol != NEW.symbol
  OR OLD.source_start_time_ms != NEW.source_start_time_ms
  OR NEW.covered_through_time_ms < OLD.covered_through_time_ms
BEGIN SELECT RAISE(ABORT, 'N19 coverage cannot regress'); END
""".strip(),
    "trg_n19_installation_immutable": """
CREATE TRIGGER trg_n19_installation_immutable
BEFORE UPDATE ON n19_lifecycle_installation
BEGIN SELECT RAISE(ABORT, 'N19 installation is immutable'); END
""".strip(),
    "trg_n19_installation_no_delete": """
CREATE TRIGGER trg_n19_installation_no_delete
BEFORE DELETE ON n19_lifecycle_installation
BEGIN SELECT RAISE(ABORT, 'N19 installation is permanent'); END
""".strip(),
}

N19_TABLES = (
    "n19_staircase_states", "n19_history_coverage",
    "n19_lifecycle_installation",
)

_EXPECTED_INDEX_LIST = {
    "n19_staircase_states": {
        "idx_n19_staircase_active": (0, "c", 1),
        "idx_n19_staircase_family": (1, "c", 0),
        "idx_n19_staircase_structure": (1, "c", 1),
        "idx_n19_staircase_symbol_latest": (0, "c", 0),
        "sqlite_autoindex_n19_staircase_states_1": (1, "u", 0),
    },
    "n19_history_coverage": {
        "idx_n19_coverage_symbol": (1, "c", 0),
        "sqlite_autoindex_n19_history_coverage_1": (1, "pk", 0),
    },
    "n19_lifecycle_installation": {},
}

_EXPECTED_INDEX_XINFO = {
    "idx_n19_staircase_active": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 5, "stage", 0, "BINARY", 1),
        (2, 7, "quote_volume_rank", 0, "BINARY", 1),
        (3, 2, "symbol", 0, "BINARY", 1),
        (4, -1, None, 0, "BINARY", 0),
    ),
    "idx_n19_staircase_family": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 3, "family_id", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
    "idx_n19_staircase_structure": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 4, "structure_id", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
    "idx_n19_staircase_symbol_latest": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 2, "symbol", 0, "BINARY", 1),
        (2, 9, "x_open_time_ms", 1, "BINARY", 1),
        (3, 0, "id", 1, "BINARY", 1),
        (4, -1, None, 0, "BINARY", 0),
    ),
    "idx_n19_coverage_symbol": (
        (0, 0, "strategy_id", 0, "BINARY", 1),
        (1, 1, "symbol", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
    "sqlite_autoindex_n19_staircase_states_1": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 3, "family_id", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
    "sqlite_autoindex_n19_history_coverage_1": (
        (0, 0, "strategy_id", 0, "BINARY", 1),
        (1, 1, "symbol", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
}


def _owned_names() -> set[str]:
    return {*N19_TABLES, *N19_INDEX_SQL, *N19_TRIGGER_SQL}


_COVERAGE_EPOCH_MIRROR_TRIGGERS = {
    "trg_history_coverage_n19_mirror_insert",
    "trg_history_coverage_n19_mirror_no_replace",
    "trg_history_coverage_n19_mirror_update",
    "trg_history_coverage_n19_mirror_delete",
}
_FAMILY_SEAL_STATE_TRIGGERS = {
    "trg_history_coverage_n19_legacy_witness_state_update_guard",
    "trg_history_coverage_n19_historical_state_no_insert",
    "trg_history_coverage_n19_historical_state_update_authorized",
}
_PROTECTED_GENERATION_STATE_TRIGGERS = {
    "trg_history_coverage_generation_historical_state_insert",
    "trg_history_coverage_generation_historical_state_update",
}


def n19_schema_status(connection: sqlite3.Connection) -> str:
    rows = connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE sql IS NOT NULL "
        "AND (lower(name) GLOB 'n19*' OR lower(name) GLOB 'idx_n19*' "
        "OR lower(name) GLOB 'trg_n19*' OR lower(tbl_name) GLOB 'n19*') "
        "ORDER BY type,name"
    ).fetchall()
    if not rows:
        return "PRE_N19"
    owned = _owned_names()
    if any(
        type(row[1]) is not str
        or row[1] not in (
            owned
            | _COVERAGE_EPOCH_MIRROR_TRIGGERS
            | _FAMILY_SEAL_STATE_TRIGGERS
            | _PROTECTED_GENERATION_STATE_TRIGGERS
        )
        for row in rows
    ):
        raise RuntimeError("N19 lifecycle catalog contains an unknown object")
    actual_names = {row[1] for row in rows}
    optional_names = actual_names & _COVERAGE_EPOCH_MIRROR_TRIGGERS
    family_optional = actual_names & _FAMILY_SEAL_STATE_TRIGGERS
    generation_optional = (
        actual_names & _PROTECTED_GENERATION_STATE_TRIGGERS
    )
    if (
        actual_names
        - optional_names
        - family_optional
        - generation_optional
        != owned
        or optional_names not in (set(), _COVERAGE_EPOCH_MIRROR_TRIGGERS)
        or family_optional not in (set(), _FAMILY_SEAL_STATE_TRIGGERS)
        or generation_optional
        not in (set(), _PROTECTED_GENERATION_STATE_TRIGGERS)
    ):
        raise RuntimeError("N19 lifecycle schema is partial")
    table_sql = {
        "n19_staircase_states": N19_STATE_TABLE_SQL,
        "n19_history_coverage": N19_COVERAGE_TABLE_SQL,
        "n19_lifecycle_installation": N19_INSTALLATION_TABLE_SQL,
    }
    for table, expected in table_sql.items():
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?", (table,)
        ).fetchall()
        if (
            len(row) != 1 or type(row[0][0]) is not str
            or _normalized_sql(row[0][0]) != _normalized_sql(expected)
            or connection.execute('PRAGMA foreign_key_list("%s")' % table).fetchall()
        ):
            raise RuntimeError("N19 lifecycle table is inconsistent: %s" % table)
    actual_lists: dict[str, dict[str, tuple[Any, ...]]] = {}
    for table, expected in _EXPECTED_INDEX_LIST.items():
        actual = {
            row[1]: tuple(row[2:5])
            for row in connection.execute('PRAGMA index_list("%s")' % table)
            if len(row) >= 5 and type(row[1]) is str
        }
        if actual != expected:
            raise RuntimeError("N19 lifecycle index set is inconsistent: %s" % table)
        actual_lists[table] = actual
    for name, expected in N19_INDEX_SQL.items():
        table = "n19_history_coverage" if name == "idx_n19_coverage_symbol" else "n19_staircase_states"
        row = connection.execute(
            "SELECT tbl_name,sql FROM sqlite_schema WHERE type='index' AND name=?", (name,)
        ).fetchall()
        if (
            len(row) != 1 or row[0][0] != table or type(row[0][1]) is not str
            or _normalized_sql(row[0][1]) != _normalized_sql(expected)
            or name not in actual_lists[table]
        ):
            raise RuntimeError("N19 lifecycle index is inconsistent: %s" % name)
    for name, expected in _EXPECTED_INDEX_XINFO.items():
        actual = tuple(tuple(row) for row in connection.execute('PRAGMA index_xinfo("%s")' % name))
        if actual != expected:
            raise RuntimeError("N19 lifecycle index metadata is inconsistent: %s" % name)
    for name, expected in N19_TRIGGER_SQL.items():
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?", (name,)
        ).fetchall()
        if len(row) != 1 or type(row[0][0]) is not str or _normalized_sql(row[0][0]) != _normalized_sql(expected):
            raise RuntimeError("N19 lifecycle trigger is inconsistent: %s" % name)
    if family_optional:
        from .coverage_family_seal import TRIGGER_SQL as FAMILY_TRIGGER_SQL

        for name in sorted(_FAMILY_SEAL_STATE_TRIGGERS):
            row = connection.execute(
                "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?",
                (name,),
            ).fetchall()
            if (
                len(row) != 1
                or type(row[0][0]) is not str
                or _normalized_sql(row[0][0])
                != _normalized_sql(FAMILY_TRIGGER_SQL[name])
            ):
                raise RuntimeError(
                    "N19 family seal state trigger is inconsistent"
                )
    root = connection.execute(
        "SELECT singleton_id,strategy_id,schema_version,rule_version,guard_sha256,"
        "catalog_schema_version,installed_at FROM n19_lifecycle_installation"
    ).fetchall()
    if (
        len(root) != 1
        or root[0][:5] != (1, "N19", 1, N19_RULE_VERSION, _GUARD_DIGEST)
        or type(root[0][5]) is not int or root[0][5] <= 0
        or type(root[0][6]) is not str or not root[0][6]
    ):
        raise RuntimeError("N19 lifecycle installation root is inconsistent")
    for table in N19_TABLES:
        actual = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='trigger' AND tbl_name=?", (table,)
        )}
        expected = {name for name, sql in N19_TRIGGER_SQL.items() if (" ON %s" % table) in sql}
        if table == "n19_history_coverage":
            optional = {
                "trg_history_coverage_n19_mirror_insert",
                "trg_history_coverage_n19_mirror_no_replace",
                "trg_history_coverage_n19_mirror_update",
                "trg_history_coverage_n19_mirror_delete",
            }
            if actual & optional:
                expected |= optional
        if (
            table == "n19_staircase_states"
            and _FAMILY_SEAL_STATE_TRIGGERS <= actual
        ):
            expected.update(_FAMILY_SEAL_STATE_TRIGGERS)
            expected.update(_PROTECTED_GENERATION_STATE_TRIGGERS)
        if actual != expected:
            raise RuntimeError("N19 lifecycle trigger set is inconsistent")
    for (table,) in connection.execute("SELECT name FROM sqlite_schema WHERE type='table'"):
        for foreign_key in connection.execute('PRAGMA foreign_key_list("%s")' % table):
            if type(foreign_key[2]) is str and foreign_key[2].lower() in {item.lower() for item in N19_TABLES}:
                raise RuntimeError("N19 lifecycle tables have an incoming foreign key")
    return "CURRENT"


def validate_pre_n19_review_clean(connection: sqlite3.Connection) -> None:
    if n19_schema_status(connection) != "PRE_N19":
        raise RuntimeError("N19 pre-install validation requires PRE_N19 schema")
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
            'SELECT 1 FROM "%s" WHERE strategy_id=\'N19\' LIMIT 1' % table
        ).fetchone() is not None:
            raise RuntimeError("pre-N19 Review contains N19 lifecycle evidence")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise RuntimeError("pre-N19 JSON contains a duplicate key")
            result[key] = value
        return result

    def marked(value: Any) -> bool:
        if type(value) is dict:
            return any(
                (key == "strategy_id" and item == "N19")
                or (key == "rule_version" and item == N19_RULE_VERSION)
                or (key == "stop_mode" and item == "staircase_exhaustion_margin_capped")
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
                raise RuntimeError("pre-N19 JSON evidence type is invalid")
            try:
                parsed = json.loads(
                    payload, object_pairs_hook=pairs,
                    parse_constant=lambda value: (_ for _ in ()).throw(
                        RuntimeError("pre-N19 JSON contains a non-finite value")
                    ),
                )
            except (TypeError, ValueError) as exc:
                raise RuntimeError("pre-N19 JSON evidence is invalid") from exc
            if marked(parsed):
                raise RuntimeError("pre-N19 Review contains N19 JSON evidence")


def install_n19_schema(connection: sqlite3.Connection, installed_at: str) -> None:
    if n19_schema_status(connection) != "PRE_N19":
        raise RuntimeError("N19 lifecycle installation requires exact PRE_N19 schema")
    connection.execute("SAVEPOINT install_n19_schema")
    try:
        for sql in (N19_STATE_TABLE_SQL, N19_COVERAGE_TABLE_SQL, N19_INSTALLATION_TABLE_SQL):
            connection.execute(sql)
        for sql in N19_INDEX_SQL.values():
            connection.execute(sql)
        for sql in N19_TRIGGER_SQL.values():
            connection.execute(sql)
        catalog = connection.execute("PRAGMA schema_version").fetchone()[0]
        connection.execute(
            "INSERT INTO n19_lifecycle_installation (singleton_id,strategy_id,"
            "schema_version,rule_version,guard_sha256,catalog_schema_version,installed_at) "
            "VALUES (1,'N19',1,'N19_V1',?,?,?)",
            (_GUARD_DIGEST, catalog, installed_at),
        )
        if n19_schema_status(connection) != "CURRENT":
            raise RuntimeError("N19 lifecycle installation did not attest")
        connection.execute("RELEASE SAVEPOINT install_n19_schema")
    except BaseException:
        connection.execute("ROLLBACK TO SAVEPOINT install_n19_schema")
        connection.execute("RELEASE SAVEPOINT install_n19_schema")
        raise
