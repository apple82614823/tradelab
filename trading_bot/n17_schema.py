from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any


N17_SCHEMA_VERSION = 1
N17_RULE_VERSION = "N17_V1"


def _normalized_sql(value: str) -> str:
    return "".join(value.lower().split()).replace('"', "").replace("`", "")


N17_STATE_TABLE_SQL = """
CREATE TABLE n17_range_support_states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N17'),
    symbol TEXT NOT NULL CHECK(length(symbol) BETWEEN 5 AND 32),
    family_id TEXT NOT NULL CHECK(
        length(family_id) = 24 AND family_id NOT GLOB '*[^0-9a-f]*'
    ),
    structure_id TEXT NOT NULL CHECK(
        length(structure_id) = 24 AND structure_id NOT GLOB '*[^0-9a-f]*'
    ),
    stage TEXT NOT NULL CHECK(stage IN (
        'TOUCH_LOCKED', 'CONFIRMING', 'CONFIRMED',
        'CONSUMED', 'MISSED', 'INVALID', 'EXPIRED'
    )),
    reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 128),
    quote_volume_rank INTEGER NOT NULL CHECK(
        typeof(quote_volume_rank) = 'integer'
        AND quote_volume_rank BETWEEN 1 AND 100
    ),
    box_start_time_ms INTEGER NOT NULL CHECK(
        typeof(box_start_time_ms) = 'integer' AND box_start_time_ms > 0
    ),
    box_end_time_ms INTEGER NOT NULL CHECK(
        typeof(box_end_time_ms) = 'integer'
        AND box_end_time_ms >= box_start_time_ms
    ),
    reset_after_time_ms INTEGER CHECK(
        reset_after_time_ms IS NULL OR (
            typeof(reset_after_time_ms) = 'integer'
            AND reset_after_time_ms > box_end_time_ms
        )
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
    UNIQUE(strategy_id, family_id),
    UNIQUE(strategy_id, structure_id)
)
""".strip()

N17_COVERAGE_TABLE_SQL = """
CREATE TABLE n17_history_coverage (
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N17'),
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
    (N17_RULE_VERSION + "|range-support-lifecycle-v1").encode("utf-8")
).hexdigest()

N17_INSTALLATION_TABLE_SQL = """
CREATE TABLE n17_lifecycle_installation (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N17'),
    schema_version INTEGER NOT NULL CHECK(
        typeof(schema_version) = 'integer' AND schema_version = 1
    ),
    rule_version TEXT NOT NULL CHECK(rule_version = 'N17_V1'),
    guard_sha256 TEXT NOT NULL CHECK(
        guard_sha256 = '%s'
    ),
    catalog_schema_version INTEGER NOT NULL CHECK(
        typeof(catalog_schema_version) = 'integer'
        AND catalog_schema_version > 0
    ),
    installed_at TEXT NOT NULL
)
""".strip() % _GUARD_DIGEST

N17_INDEX_SQL = {
    "idx_n17_range_support_active": (
        "CREATE INDEX idx_n17_range_support_active "
        "ON n17_range_support_states(strategy_id, stage, quote_volume_rank, symbol) "
        "WHERE stage IN ('TOUCH_LOCKED','CONFIRMING','CONFIRMED')"
    ),
    "idx_n17_range_support_family": (
        "CREATE UNIQUE INDEX idx_n17_range_support_family "
        "ON n17_range_support_states(strategy_id, family_id)"
    ),
    "idx_n17_range_support_structure": (
        "CREATE UNIQUE INDEX idx_n17_range_support_structure "
        "ON n17_range_support_states(strategy_id, structure_id)"
    ),
    "idx_n17_range_support_symbol_latest": (
        "CREATE INDEX idx_n17_range_support_symbol_latest "
        "ON n17_range_support_states("
        "strategy_id,symbol,box_end_time_ms DESC,id DESC)"
    ),
    "idx_n17_coverage_symbol": (
        "CREATE UNIQUE INDEX idx_n17_coverage_symbol "
        "ON n17_history_coverage(strategy_id, symbol)"
    ),
}

N17_TRIGGER_SQL = {
    "trg_n17_state_no_delete": """
CREATE TRIGGER trg_n17_state_no_delete
BEFORE DELETE ON n17_range_support_states
BEGIN SELECT RAISE(ABORT, 'N17 lifecycle rows are permanent'); END
""".strip(),
    "trg_n17_state_identity_immutable": """
CREATE TRIGGER trg_n17_state_identity_immutable
BEFORE UPDATE ON n17_range_support_states
WHEN OLD.strategy_id != NEW.strategy_id
  OR OLD.symbol != NEW.symbol
  OR OLD.family_id != NEW.family_id
  OR OLD.structure_id != NEW.structure_id
  OR OLD.box_start_time_ms != NEW.box_start_time_ms
  OR OLD.box_end_time_ms != NEW.box_end_time_ms
  OR OLD.created_at != NEW.created_at
  OR (
    OLD.stage IN ('CONSUMED','MISSED','INVALID','EXPIRED')
    AND NOT (
      NEW.stage = OLD.stage
      AND NEW.reason = OLD.reason
      AND NEW.quote_volume_rank = OLD.quote_volume_rank
      AND OLD.reset_after_time_ms IS NULL
      AND NEW.reset_after_time_ms IS NOT NULL
      AND NEW.reset_after_time_ms > OLD.box_end_time_ms
    )
  )
BEGIN SELECT RAISE(ABORT, 'N17 lifecycle identity is immutable'); END
""".strip(),
    "trg_n17_coverage_no_delete": """
CREATE TRIGGER trg_n17_coverage_no_delete
BEFORE DELETE ON n17_history_coverage
BEGIN SELECT RAISE(ABORT, 'N17 coverage is permanent'); END
""".strip(),
    "trg_n17_coverage_monotonic": """
CREATE TRIGGER trg_n17_coverage_monotonic
BEFORE UPDATE ON n17_history_coverage
WHEN OLD.strategy_id != NEW.strategy_id
  OR OLD.symbol != NEW.symbol
  OR OLD.source_start_time_ms != NEW.source_start_time_ms
  OR NEW.covered_through_time_ms < OLD.covered_through_time_ms
BEGIN SELECT RAISE(ABORT, 'N17 coverage cannot regress'); END
""".strip(),
    "trg_n17_installation_immutable": """
CREATE TRIGGER trg_n17_installation_immutable
BEFORE UPDATE ON n17_lifecycle_installation
BEGIN SELECT RAISE(ABORT, 'N17 installation is immutable'); END
""".strip(),
    "trg_n17_installation_no_delete": """
CREATE TRIGGER trg_n17_installation_no_delete
BEFORE DELETE ON n17_lifecycle_installation
BEGIN SELECT RAISE(ABORT, 'N17 installation is permanent'); END
""".strip(),
}

N17_TABLES = (
    "n17_range_support_states",
    "n17_history_coverage",
    "n17_lifecycle_installation",
)

_EXPECTED_INDEX_LIST = {
    "n17_range_support_states": {
        "idx_n17_range_support_active": (0, "c", 1),
        "idx_n17_range_support_family": (1, "c", 0),
        "idx_n17_range_support_structure": (1, "c", 0),
        "idx_n17_range_support_symbol_latest": (0, "c", 0),
        "sqlite_autoindex_n17_range_support_states_1": (1, "u", 0),
        "sqlite_autoindex_n17_range_support_states_2": (1, "u", 0),
    },
    "n17_history_coverage": {
        "idx_n17_coverage_symbol": (1, "c", 0),
        "sqlite_autoindex_n17_history_coverage_1": (1, "pk", 0),
    },
    "n17_lifecycle_installation": {},
}

_EXPECTED_INDEX_XINFO = {
    "idx_n17_range_support_active": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 5, "stage", 0, "BINARY", 1),
        (2, 7, "quote_volume_rank", 0, "BINARY", 1),
        (3, 2, "symbol", 0, "BINARY", 1),
        (4, -1, None, 0, "BINARY", 0),
    ),
    "idx_n17_range_support_family": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 3, "family_id", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
    "idx_n17_range_support_structure": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 4, "structure_id", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
    "idx_n17_range_support_symbol_latest": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 2, "symbol", 0, "BINARY", 1),
        (2, 9, "box_end_time_ms", 1, "BINARY", 1),
        (3, 0, "id", 1, "BINARY", 1),
        (4, -1, None, 0, "BINARY", 0),
    ),
    "idx_n17_coverage_symbol": (
        (0, 0, "strategy_id", 0, "BINARY", 1),
        (1, 1, "symbol", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
    "sqlite_autoindex_n17_range_support_states_1": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 3, "family_id", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
    "sqlite_autoindex_n17_range_support_states_2": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 4, "structure_id", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
    "sqlite_autoindex_n17_history_coverage_1": (
        (0, 0, "strategy_id", 0, "BINARY", 1),
        (1, 1, "symbol", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
}


def _owned_names() -> set[str]:
    return {*N17_TABLES, *N17_INDEX_SQL, *N17_TRIGGER_SQL}


_COVERAGE_EPOCH_MIRROR_TRIGGERS = {
    "trg_history_coverage_n17_mirror_insert",
    "trg_history_coverage_n17_mirror_no_replace",
    "trg_history_coverage_n17_mirror_update",
    "trg_history_coverage_n17_mirror_delete",
}


def n17_schema_status(connection: sqlite3.Connection) -> str:
    owned = _owned_names()
    rows = connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema "
        "WHERE sql IS NOT NULL AND (lower(name) GLOB 'n17*' OR lower(name) GLOB 'idx_n17*' "
        "OR lower(name) GLOB 'trg_n17*' OR lower(tbl_name) GLOB 'n17*' "
        ") ORDER BY type,name"
    ).fetchall()
    if not rows:
        return "PRE_N17"
    if any(
        type(row[1]) is not str
        or row[1] not in owned | _COVERAGE_EPOCH_MIRROR_TRIGGERS
        or type(row[3]) is not str
        for row in rows
    ):
        raise RuntimeError("N17 lifecycle catalog contains an unknown object")
    actual_names = {row[1] for row in rows}
    optional_names = actual_names & _COVERAGE_EPOCH_MIRROR_TRIGGERS
    if (
        actual_names - optional_names != owned
        or optional_names not in (set(), _COVERAGE_EPOCH_MIRROR_TRIGGERS)
    ):
        raise RuntimeError("N17 lifecycle schema is partial")
    table_sql = {
        "n17_range_support_states": N17_STATE_TABLE_SQL,
        "n17_history_coverage": N17_COVERAGE_TABLE_SQL,
        "n17_lifecycle_installation": N17_INSTALLATION_TABLE_SQL,
    }
    for table, expected in table_sql.items():
        actual = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?", (table,)
        ).fetchall()
        if (
            len(actual) != 1
            or type(actual[0][0]) is not str
            or _normalized_sql(actual[0][0]) != _normalized_sql(expected)
            or connection.execute('PRAGMA foreign_key_list("%s")' % table).fetchall()
        ):
            raise RuntimeError("N17 lifecycle table is inconsistent: %s" % table)
    actual_index_lists = {}
    for table, expected in _EXPECTED_INDEX_LIST.items():
        actual = {
            row[1]: tuple(row[2:5])
            for row in connection.execute(
                'PRAGMA index_list("%s")' % table
            ).fetchall()
            if len(row) >= 5 and type(row[1]) is str
        }
        if actual != expected:
            raise RuntimeError("N17 lifecycle index set is inconsistent: %s" % table)
        actual_index_lists[table] = actual
    for name, expected in N17_INDEX_SQL.items():
        row = connection.execute(
            "SELECT tbl_name,sql FROM sqlite_schema WHERE type='index' AND name=?",
            (name,),
        ).fetchall()
        table = "n17_history_coverage" if name == "idx_n17_coverage_symbol" else "n17_range_support_states"
        if (
            len(row) != 1
            or row[0][0] != table
            or type(row[0][1]) is not str
            or _normalized_sql(row[0][1]) != _normalized_sql(expected)
            or name not in actual_index_lists[table]
        ):
            raise RuntimeError("N17 lifecycle index is inconsistent: %s" % name)
    for name, expected in _EXPECTED_INDEX_XINFO.items():
        xinfo = tuple(
            tuple(row)
            for row in connection.execute(
                'PRAGMA index_xinfo("%s")' % name
            ).fetchall()
        )
        if xinfo != expected:
            raise RuntimeError(
                "N17 lifecycle index metadata is inconsistent: %s" % name
            )
    for name, expected in N17_TRIGGER_SQL.items():
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?", (name,)
        ).fetchall()
        if (
            len(row) != 1
            or type(row[0][0]) is not str
            or _normalized_sql(row[0][0]) != _normalized_sql(expected)
        ):
            raise RuntimeError("N17 lifecycle trigger is inconsistent: %s" % name)
    root = connection.execute(
        "SELECT singleton_id,strategy_id,schema_version,rule_version,guard_sha256,"
        "catalog_schema_version,installed_at FROM n17_lifecycle_installation"
    ).fetchall()
    if (
        len(root) != 1
        or root[0][:5]
        != (1, "N17", N17_SCHEMA_VERSION, N17_RULE_VERSION, _GUARD_DIGEST)
        or type(root[0][5]) is not int
        or root[0][5] <= 0
        or type(root[0][6]) is not str
        or not root[0][6]
    ):
        raise RuntimeError("N17 lifecycle installation root is inconsistent")
    # No external trigger or incoming FK may make a lifecycle write touch an
    # unrelated table.
    for table in N17_TABLES:
        trigger_names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type='trigger' AND tbl_name=?",
                (table,),
            ).fetchall()
        }
        expected = {
            name
            for name, sql in N17_TRIGGER_SQL.items()
            if (" ON %s" % table) in sql
        }
        if table == "n17_history_coverage":
            optional = {
                "trg_history_coverage_n17_mirror_insert",
                "trg_history_coverage_n17_mirror_no_replace",
                "trg_history_coverage_n17_mirror_update",
                "trg_history_coverage_n17_mirror_delete",
            }
            if trigger_names & optional:
                expected |= optional
        if trigger_names != expected:
            raise RuntimeError("N17 lifecycle trigger set is inconsistent: %s" % table)
    all_tables = connection.execute(
        "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name"
    ).fetchall()
    for (table,) in all_tables:
        for foreign_key in connection.execute(
            'PRAGMA foreign_key_list("%s")' % table
        ).fetchall():
            if type(foreign_key[2]) is str and foreign_key[2].lower() in {
                name.lower() for name in N17_TABLES
            }:
                raise RuntimeError("N17 lifecycle tables have an incoming foreign key")
    return "CURRENT"


def validate_pre_n17_review_clean(connection: sqlite3.Connection) -> None:
    """Prove the stopped-service Review snapshot has no N17 business trace."""

    if n17_schema_status(connection) != "PRE_N17":
        raise RuntimeError("N17 pre-install validation requires PRE_N17 schema")
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='table'"
        ).fetchall()
        if type(row[0]) is str
    }
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
        if table in tables and connection.execute(
            'SELECT 1 FROM "%s" WHERE strategy_id=\'N17\' LIMIT 1' % table
        ).fetchone() is not None:
            raise RuntimeError("pre-N17 Review contains N17 lifecycle evidence")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise RuntimeError("pre-N17 JSON contains a duplicate key")
            result[key] = value
        return result

    def marked(value: Any) -> bool:
        if type(value) is dict:
            for key, item in value.items():
                if (
                    (key == "strategy_id" and item == "N17")
                    or (key == "rule_version" and item == N17_RULE_VERSION)
                    or (
                        key == "stop_mode"
                        and item == "range_support_absorption_margin_capped"
                    )
                    or marked(item)
                ):
                    return True
        elif type(value) is list:
            return any(marked(item) for item in value)
        return False

    for table, column in (("events", "payload_json"), ("trade_reviews", "orders_json")):
        if table not in tables:
            continue
        cursor = connection.execute('SELECT "%s" FROM "%s"' % (column, table))
        for (payload,) in cursor:
            if type(payload) is not str:
                raise RuntimeError("pre-N17 JSON evidence type is invalid")
            try:
                parsed = json.loads(
                    payload,
                    object_pairs_hook=pairs,
                    parse_constant=lambda value: (_ for _ in ()).throw(
                        RuntimeError("pre-N17 JSON contains a non-finite value")
                    ),
                )
            except (TypeError, ValueError) as exc:
                raise RuntimeError("pre-N17 JSON evidence is invalid") from exc
            if marked(parsed):
                raise RuntimeError("pre-N17 Review contains N17 JSON evidence")


def install_n17_schema(connection: sqlite3.Connection, installed_at: str) -> None:
    if n17_schema_status(connection) != "PRE_N17":
        raise RuntimeError("N17 lifecycle installation requires exact pre-N17 schema")
    connection.execute("SAVEPOINT install_n17_schema")
    try:
        connection.execute(N17_STATE_TABLE_SQL)
        connection.execute(N17_COVERAGE_TABLE_SQL)
        connection.execute(N17_INSTALLATION_TABLE_SQL)
        for sql in N17_INDEX_SQL.values():
            connection.execute(sql)
        for sql in N17_TRIGGER_SQL.values():
            connection.execute(sql)
        catalog = connection.execute("PRAGMA schema_version").fetchone()[0]
        connection.execute(
            "INSERT INTO n17_lifecycle_installation ("
            "singleton_id,strategy_id,schema_version,rule_version,guard_sha256,"
            "catalog_schema_version,installed_at) VALUES (1,'N17',1,'N17_V1',?,?,?)",
            (_GUARD_DIGEST, catalog, installed_at),
        )
        if n17_schema_status(connection) != "CURRENT":
            raise RuntimeError("N17 lifecycle installation did not attest")
        connection.execute("RELEASE SAVEPOINT install_n17_schema")
    except BaseException:
        connection.execute("ROLLBACK TO SAVEPOINT install_n17_schema")
        connection.execute("RELEASE SAVEPOINT install_n17_schema")
        raise
