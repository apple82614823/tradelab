from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any


MICRO_LIFECYCLE_SCHEMA_VERSION = 1
MICRO_LIFECYCLE_RULE_VERSION = "N21_N25_V1"
MICRO_STRATEGY_IDS = ("N21", "N22", "N23", "N24", "N25")


def _normalized_sql(value: str) -> str:
    return "".join(value.lower().split()).replace('"', "").replace("`", "")


MICRO_CLAIM_TABLE_SQL = """
CREATE TABLE micro_strategy_lifecycle (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL CHECK(strategy_id IN ('N21','N22','N23','N24','N25')),
    symbol TEXT NOT NULL CHECK(length(symbol) BETWEEN 5 AND 32),
    structure_id TEXT NOT NULL CHECK(
        length(structure_id) = 24 AND structure_id NOT GLOB '*[^0-9a-f]*'
    ),
    kline_open_time_ms INTEGER NOT NULL CHECK(
        typeof(kline_open_time_ms) = 'integer' AND kline_open_time_ms > 0
    ),
    confirmation_observed_at_ms INTEGER NOT NULL CHECK(
        typeof(confirmation_observed_at_ms) = 'integer'
        AND confirmation_observed_at_ms >= kline_open_time_ms
    ),
    deadline_ms INTEGER NOT NULL CHECK(
        typeof(deadline_ms) = 'integer'
        AND deadline_ms = confirmation_observed_at_ms + 120000
    ),
    source_signal_id INTEGER NOT NULL UNIQUE CHECK(
        typeof(source_signal_id) = 'integer' AND source_signal_id > 0
    ),
    source_scan_id INTEGER NOT NULL CHECK(
        typeof(source_scan_id) = 'integer' AND source_scan_id > 0
    ),
    claim_state TEXT NOT NULL CHECK(claim_state IN ('STAGED','ACTIVE')),
    evidence_json TEXT NOT NULL CHECK(
        length(CAST(evidence_json AS BLOB)) <= 8192
    ),
    evidence_sha256 TEXT NOT NULL CHECK(
        length(evidence_sha256) = 64
        AND evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    created_at TEXT NOT NULL,
    UNIQUE(strategy_id, structure_id),
    UNIQUE(strategy_id, symbol, kline_open_time_ms)
)
""".strip()

MICRO_ANALYSIS_TABLE_SQL = """
CREATE TABLE micro_passed_analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_scan_id INTEGER NOT NULL CHECK(
        typeof(source_scan_id) = 'integer' AND source_scan_id > 0
    ),
    strategy_id TEXT NOT NULL CHECK(strategy_id IN ('N21','N22','N23','N24','N25')),
    symbol TEXT NOT NULL CHECK(length(symbol) BETWEEN 5 AND 32),
    structure_id TEXT NOT NULL CHECK(
        length(structure_id) = 24 AND structure_id NOT GLOB '*[^0-9a-f]*'
    ),
    kline_open_time_ms INTEGER NOT NULL CHECK(
        typeof(kline_open_time_ms) = 'integer' AND kline_open_time_ms > 0
    ),
    confirmation_observed_at_ms INTEGER NOT NULL CHECK(
        typeof(confirmation_observed_at_ms) = 'integer'
        AND confirmation_observed_at_ms >= kline_open_time_ms
    ),
    deadline_ms INTEGER NOT NULL CHECK(
        typeof(deadline_ms) = 'integer'
        AND deadline_ms = confirmation_observed_at_ms + 120000
    ),
    evidence_json TEXT NOT NULL CHECK(length(CAST(evidence_json AS BLOB)) <= 8192),
    evidence_sha256 TEXT NOT NULL CHECK(
        length(evidence_sha256) = 64
        AND evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    created_at TEXT NOT NULL,
    UNIQUE(source_scan_id, strategy_id, symbol, structure_id)
)
""".strip()

_GUARD_SHA256 = hashlib.sha256(
    b"N21_N25_V1|micro-observation-lifecycle-v1"
).hexdigest()

MICRO_INSTALLATION_TABLE_SQL = """
CREATE TABLE micro_lifecycle_installation (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    rule_version TEXT NOT NULL CHECK(rule_version = 'N21_N25_V1'),
    guard_sha256 TEXT NOT NULL CHECK(guard_sha256 = '%s'),
    catalog_schema_version INTEGER NOT NULL CHECK(
        typeof(catalog_schema_version) = 'integer' AND catalog_schema_version > 0
    ),
    installed_at TEXT NOT NULL
)
""".strip() % _GUARD_SHA256

MICRO_INDEX_SQL = {
    "idx_micro_lifecycle_strategy_structure": (
        "CREATE UNIQUE INDEX idx_micro_lifecycle_strategy_structure "
        "ON micro_strategy_lifecycle(strategy_id,structure_id)"
    ),
    "idx_micro_lifecycle_strategy_candle": (
        "CREATE UNIQUE INDEX idx_micro_lifecycle_strategy_candle "
        "ON micro_strategy_lifecycle(strategy_id,symbol,kline_open_time_ms)"
    ),
    "idx_micro_lifecycle_scan_state": (
        "CREATE INDEX idx_micro_lifecycle_scan_state "
        "ON micro_strategy_lifecycle(source_scan_id,claim_state,strategy_id)"
    ),
    "idx_micro_analysis_strategy_candle": (
        "CREATE INDEX idx_micro_analysis_strategy_candle "
        "ON micro_passed_analyses(strategy_id,symbol,kline_open_time_ms,source_scan_id)"
    ),
}

MICRO_TRIGGER_SQL = {
    "trg_micro_lifecycle_no_delete": """
CREATE TRIGGER trg_micro_lifecycle_no_delete
BEFORE DELETE ON micro_strategy_lifecycle
WHEN NOT (
    OLD.claim_state = 'STAGED'
    AND EXISTS (
        SELECT 1 FROM strategy_signal_batches AS batch
        WHERE batch.scan_id = OLD.source_scan_id AND batch.state = 'STAGING'
    )
)
BEGIN SELECT RAISE(ABORT, 'micro lifecycle evidence is permanent'); END
""".strip(),
    "trg_micro_lifecycle_immutable": """
CREATE TRIGGER trg_micro_lifecycle_immutable
BEFORE UPDATE ON micro_strategy_lifecycle
WHEN OLD.id != NEW.id
 OR OLD.strategy_id != NEW.strategy_id
 OR OLD.symbol != NEW.symbol
 OR OLD.structure_id != NEW.structure_id
 OR OLD.kline_open_time_ms != NEW.kline_open_time_ms
 OR OLD.confirmation_observed_at_ms != NEW.confirmation_observed_at_ms
 OR OLD.deadline_ms != NEW.deadline_ms
 OR OLD.source_signal_id != NEW.source_signal_id
 OR OLD.source_scan_id != NEW.source_scan_id
 OR OLD.evidence_json != NEW.evidence_json
 OR OLD.evidence_sha256 != NEW.evidence_sha256
 OR OLD.created_at != NEW.created_at
 OR NOT (OLD.claim_state = 'STAGED' AND NEW.claim_state = 'ACTIVE')
BEGIN SELECT RAISE(ABORT, 'micro lifecycle identity is immutable'); END
""".strip(),
    "trg_micro_analysis_no_delete": """
CREATE TRIGGER trg_micro_analysis_no_delete
BEFORE DELETE ON micro_passed_analyses
BEGIN SELECT RAISE(ABORT, 'micro passed analysis is permanent'); END
""".strip(),
    "trg_micro_analysis_immutable": """
CREATE TRIGGER trg_micro_analysis_immutable
BEFORE UPDATE ON micro_passed_analyses
BEGIN SELECT RAISE(ABORT, 'micro passed analysis is immutable'); END
""".strip(),
    "trg_micro_installation_immutable": """
CREATE TRIGGER trg_micro_installation_immutable
BEFORE UPDATE ON micro_lifecycle_installation
BEGIN SELECT RAISE(ABORT, 'micro lifecycle installation is immutable'); END
""".strip(),
    "trg_micro_installation_no_delete": """
CREATE TRIGGER trg_micro_installation_no_delete
BEFORE DELETE ON micro_lifecycle_installation
BEGIN SELECT RAISE(ABORT, 'micro lifecycle installation is permanent'); END
""".strip(),
}

MICRO_TABLES = (
    "micro_strategy_lifecycle",
    "micro_passed_analyses",
    "micro_lifecycle_installation",
)

_MICRO_TABLE_XINFO = {
    "micro_strategy_lifecycle": (
        (0, "id", "INTEGER", 0, None, 1, 0),
        (1, "strategy_id", "TEXT", 1, None, 0, 0),
        (2, "symbol", "TEXT", 1, None, 0, 0),
        (3, "structure_id", "TEXT", 1, None, 0, 0),
        (4, "kline_open_time_ms", "INTEGER", 1, None, 0, 0),
        (5, "confirmation_observed_at_ms", "INTEGER", 1, None, 0, 0),
        (6, "deadline_ms", "INTEGER", 1, None, 0, 0),
        (7, "source_signal_id", "INTEGER", 1, None, 0, 0),
        (8, "source_scan_id", "INTEGER", 1, None, 0, 0),
        (9, "claim_state", "TEXT", 1, None, 0, 0),
        (10, "evidence_json", "TEXT", 1, None, 0, 0),
        (11, "evidence_sha256", "TEXT", 1, None, 0, 0),
        (12, "created_at", "TEXT", 1, None, 0, 0),
    ),
    "micro_passed_analyses": (
        (0, "id", "INTEGER", 0, None, 1, 0),
        (1, "source_scan_id", "INTEGER", 1, None, 0, 0),
        (2, "strategy_id", "TEXT", 1, None, 0, 0),
        (3, "symbol", "TEXT", 1, None, 0, 0),
        (4, "structure_id", "TEXT", 1, None, 0, 0),
        (5, "kline_open_time_ms", "INTEGER", 1, None, 0, 0),
        (6, "confirmation_observed_at_ms", "INTEGER", 1, None, 0, 0),
        (7, "deadline_ms", "INTEGER", 1, None, 0, 0),
        (8, "evidence_json", "TEXT", 1, None, 0, 0),
        (9, "evidence_sha256", "TEXT", 1, None, 0, 0),
        (10, "created_at", "TEXT", 1, None, 0, 0),
    ),
    "micro_lifecycle_installation": (
        (0, "singleton_id", "INTEGER", 0, None, 1, 0),
        (1, "schema_version", "INTEGER", 1, None, 0, 0),
        (2, "rule_version", "TEXT", 1, None, 0, 0),
        (3, "guard_sha256", "TEXT", 1, None, 0, 0),
        (4, "catalog_schema_version", "INTEGER", 1, None, 0, 0),
        (5, "installed_at", "TEXT", 1, None, 0, 0),
    ),
}

_MICRO_INDEX_METADATA = {
    "idx_micro_lifecycle_scan_state": (
        (0, 8, "source_scan_id", 0, "BINARY", 1),
        (1, 9, "claim_state", 0, "BINARY", 1),
        (2, 1, "strategy_id", 0, "BINARY", 1),
        (3, -1, None, 0, "BINARY", 0),
    ),
    "idx_micro_lifecycle_strategy_candle": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 2, "symbol", 0, "BINARY", 1),
        (2, 4, "kline_open_time_ms", 0, "BINARY", 1),
        (3, -1, None, 0, "BINARY", 0),
    ),
    "idx_micro_lifecycle_strategy_structure": (
        (0, 1, "strategy_id", 0, "BINARY", 1),
        (1, 3, "structure_id", 0, "BINARY", 1),
        (2, -1, None, 0, "BINARY", 0),
    ),
    "idx_micro_analysis_strategy_candle": (
        (0, 2, "strategy_id", 0, "BINARY", 1),
        (1, 3, "symbol", 0, "BINARY", 1),
        (2, 5, "kline_open_time_ms", 0, "BINARY", 1),
        (3, 1, "source_scan_id", 0, "BINARY", 1),
        (4, -1, None, 0, "BINARY", 0),
    ),
}

_MICRO_NAMED_INDEX_LIST = {
    "idx_micro_lifecycle_scan_state": (0, "c", 0),
    "idx_micro_lifecycle_strategy_candle": (1, "c", 0),
    "idx_micro_lifecycle_strategy_structure": (1, "c", 0),
    "idx_micro_analysis_strategy_candle": (0, "c", 0),
}


def _owned_names() -> set[str]:
    return {*MICRO_TABLES, *MICRO_INDEX_SQL, *MICRO_TRIGGER_SQL}


def micro_schema_status(connection: sqlite3.Connection) -> str:
    rows = connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE sql IS NOT NULL "
        "AND (lower(name) GLOB 'micro_*' OR lower(name) GLOB 'idx_micro_*' "
        "OR lower(name) GLOB 'trg_micro_*' OR lower(tbl_name) GLOB 'micro_*') "
        "ORDER BY type,name"
    ).fetchall()
    if not rows:
        return "PRE_MICRO"
    if {row[1] for row in rows} != _owned_names() or any(
        type(row[3]) is not str for row in rows
    ):
        raise RuntimeError("N21-N25 lifecycle catalog is partial or unknown")
    expected_tables = {
        "micro_strategy_lifecycle": MICRO_CLAIM_TABLE_SQL,
        "micro_passed_analyses": MICRO_ANALYSIS_TABLE_SQL,
        "micro_lifecycle_installation": MICRO_INSTALLATION_TABLE_SQL,
    }
    for table, expected in expected_tables.items():
        actual = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?", (table,)
        ).fetchall()
        if (
            len(actual) != 1
            or _normalized_sql(actual[0][0]) != _normalized_sql(expected)
            or connection.execute(f'PRAGMA foreign_key_list("{table}")').fetchall()
            or tuple(connection.execute(
                f'PRAGMA table_xinfo("{table}")'
            ).fetchall()) != _MICRO_TABLE_XINFO[table]
        ):
            raise RuntimeError(f"N21-N25 lifecycle table is inconsistent: {table}")
    expected_index_names = {
        "micro_strategy_lifecycle": {
            "idx_micro_lifecycle_strategy_structure",
            "idx_micro_lifecycle_strategy_candle",
            "idx_micro_lifecycle_scan_state",
            "sqlite_autoindex_micro_strategy_lifecycle_1",
            "sqlite_autoindex_micro_strategy_lifecycle_2",
            "sqlite_autoindex_micro_strategy_lifecycle_3",
        },
        "micro_lifecycle_installation": set(),
        "micro_passed_analyses": {
            "idx_micro_analysis_strategy_candle",
            "sqlite_autoindex_micro_passed_analyses_1",
        },
    }
    for table, names in expected_index_names.items():
        rows_by_name = {
            row[1]: tuple(row[2:5])
            for row in connection.execute(f'PRAGMA index_list("{table}")').fetchall()
        }
        if set(rows_by_name) != names:
            raise RuntimeError(f"N21-N25 lifecycle index set is inconsistent: {table}")
        for name, metadata in rows_by_name.items():
            if name in _MICRO_NAMED_INDEX_LIST:
                if metadata != _MICRO_NAMED_INDEX_LIST[name]:
                    raise RuntimeError(
                        f"N21-N25 lifecycle index metadata is inconsistent: {name}"
                    )
            elif metadata != (1, "u", 0):
                raise RuntimeError(
                    f"N21-N25 lifecycle autoindex metadata is inconsistent: {name}"
                )
    for name, expected in MICRO_INDEX_SQL.items():
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type='index' AND name=?", (name,)
        ).fetchall()
        if len(row) != 1 or _normalized_sql(row[0][0]) != _normalized_sql(expected):
            raise RuntimeError(f"N21-N25 lifecycle index is inconsistent: {name}")
        xinfo = tuple(
            connection.execute(f'PRAGMA index_xinfo("{name}")').fetchall()
        )
        if xinfo != _MICRO_INDEX_METADATA[name]:
            raise RuntimeError(f"N21-N25 lifecycle index metadata is invalid: {name}")
    for name, expected in MICRO_TRIGGER_SQL.items():
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?", (name,)
        ).fetchall()
        if len(row) != 1 or _normalized_sql(row[0][0]) != _normalized_sql(expected):
            raise RuntimeError(f"N21-N25 lifecycle trigger is inconsistent: {name}")
    expected_trigger_sets = {
        "micro_strategy_lifecycle": {
            "trg_micro_lifecycle_no_delete", "trg_micro_lifecycle_immutable"
        },
        "micro_lifecycle_installation": {
            "trg_micro_installation_immutable", "trg_micro_installation_no_delete"
        },
        "micro_passed_analyses": {
            "trg_micro_analysis_no_delete", "trg_micro_analysis_immutable"
        },
    }
    for table, expected in expected_trigger_sets.items():
        actual = {
            row[0] for row in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type='trigger' AND tbl_name=?",
                (table,),
            ).fetchall()
        }
        if actual != expected:
            raise RuntimeError(f"N21-N25 lifecycle trigger set is inconsistent: {table}")
    root = connection.execute(
        "SELECT singleton_id,schema_version,rule_version,guard_sha256,"
        "catalog_schema_version,installed_at FROM micro_lifecycle_installation"
    ).fetchall()
    if (
        len(root) != 1
        or root[0][:4]
        != (1, MICRO_LIFECYCLE_SCHEMA_VERSION, MICRO_LIFECYCLE_RULE_VERSION, _GUARD_SHA256)
        or type(root[0][4]) is not int
        or root[0][4] <= 0
        or type(root[0][5]) is not str
        or not root[0][5]
    ):
        raise RuntimeError("N21-N25 lifecycle installation root is inconsistent")
    lowered = {table.lower() for table in MICRO_TABLES}
    for (table,) in connection.execute(
        "SELECT name FROM sqlite_schema WHERE type='table'"
    ).fetchall():
        for foreign_key in connection.execute(
            f'PRAGMA foreign_key_list("{table}")'
        ).fetchall():
            if type(foreign_key[2]) is str and foreign_key[2].lower() in lowered:
                raise RuntimeError("N21-N25 lifecycle has an incoming foreign key")
    return "CURRENT"


def validate_pre_micro_review_clean(connection: sqlite3.Connection) -> None:
    if micro_schema_status(connection) != "PRE_MICRO":
        raise RuntimeError("N21-N25 pre-install requires PRE_MICRO schema")
    tables = {
        row[0] for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='table'"
        ).fetchall()
    }
    for table in (
        "strategy_definitions", "strategy_signals",
        "strategy_passed_signal_audits", "strategy_passed_structure_ledger",
        "strategy_paper_trades", "strategy_states", "strategy_live_links",
        "strategy_structure_terminal_states",
    ):
        if table in tables and connection.execute(
            f'SELECT 1 FROM "{table}" WHERE strategy_id IN '
            "('N21','N22','N23','N24','N25') LIMIT 1"
        ).fetchone() is not None:
            raise RuntimeError("pre-N21-N25 Review contains lifecycle evidence")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise RuntimeError("pre-N21-N25 JSON has a duplicate key")
            result[key] = value
        return result

    def marked(value: Any) -> bool:
        if type(value) is dict:
            for key, item in value.items():
                if (
                    (key == "strategy_id" and item in MICRO_STRATEGY_IDS)
                    or (key == "rule_version" and item in {f"N{i}_V1" for i in range(21, 26)})
                    or (key == "stop_mode" and item == "micro_observation_margin_capped")
                    or marked(item)
                ):
                    return True
        elif type(value) is list:
            return any(marked(item) for item in value)
        return False

    for table, column in (("events", "payload_json"), ("trade_reviews", "orders_json")):
        if table not in tables:
            continue
        cursor = connection.execute(f'SELECT "{column}" FROM "{table}"')
        for (payload,) in cursor:
            if type(payload) is not str:
                raise RuntimeError("pre-N21-N25 JSON type is invalid")
            try:
                decoded = json.loads(payload, object_pairs_hook=pairs)
            except Exception as exc:
                raise RuntimeError("pre-N21-N25 JSON is invalid") from exc
            if marked(decoded):
                raise RuntimeError("pre-N21-N25 Review contains lifecycle evidence")


def install_micro_schema(connection: sqlite3.Connection, installed_at: str) -> None:
    if micro_schema_status(connection) != "PRE_MICRO":
        raise RuntimeError("N21-N25 lifecycle install requires PRE_MICRO schema")
    validate_pre_micro_review_clean(connection)
    connection.execute(MICRO_CLAIM_TABLE_SQL)
    connection.execute(MICRO_ANALYSIS_TABLE_SQL)
    connection.execute(MICRO_INSTALLATION_TABLE_SQL)
    for sql in MICRO_INDEX_SQL.values():
        connection.execute(sql)
    for sql in MICRO_TRIGGER_SQL.values():
        connection.execute(sql)
    catalog = connection.execute("PRAGMA schema_version").fetchone()[0]
    connection.execute(
        "INSERT INTO micro_lifecycle_installation VALUES (1,1,'N21_N25_V1',?,?,?)",
        (_GUARD_SHA256, catalog, installed_at),
    )
    if micro_schema_status(connection) != "CURRENT":
        raise RuntimeError("N21-N25 lifecycle installation did not attest")


def validate_micro_lifecycle_graph(connection: sqlite3.Connection) -> None:
    if micro_schema_status(connection) != "CURRENT":
        raise RuntimeError("N21-N25 lifecycle schema is not current")
    rows = connection.execute(
        "SELECT strategy_id,symbol,structure_id,kline_open_time_ms,"
        "confirmation_observed_at_ms,deadline_ms,source_signal_id,source_scan_id,"
        "claim_state,evidence_json,evidence_sha256 FROM micro_strategy_lifecycle "
        "ORDER BY id"
    )
    for row in rows:
        try:
            evidence = json.loads(row[9])
            canonical = json.dumps(
                evidence, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False,
            )
        except Exception as exc:
            raise RuntimeError("N21-N25 lifecycle evidence is invalid") from exc
        if (
            hashlib.sha256(canonical.encode("utf-8")).hexdigest() != row[10]
            or evidence.get("strategy_id") != row[0]
            or evidence.get("symbol") != row[1]
            or evidence.get("structure_id") != row[2]
            or evidence.get("kline_open_time_ms") != row[3]
            or evidence.get("confirmation_observed_at_ms") != row[4]
            or evidence.get("deadline_ms") != row[5]
        ):
            raise RuntimeError("N21-N25 lifecycle evidence conflicts")
        audit = connection.execute(
            "SELECT source_scan_id,strategy_id,symbol,structure_id,claim_state "
            "FROM strategy_passed_signal_audits WHERE source_signal_id=?",
            (row[6],),
        ).fetchall()
        ledger = connection.execute(
            "SELECT source_scan_id,strategy_id,symbol,structure_id,claim_state "
            "FROM strategy_passed_structure_ledger WHERE source_signal_id=?",
            (row[6],),
        ).fetchall()
        expected = [(row[7], row[0], row[1], row[2], row[8])]
        if audit != expected or ledger != expected:
            raise RuntimeError("N21-N25 permanent claim graph conflicts")
        analysis = connection.execute(
            "SELECT evidence_json,evidence_sha256 FROM micro_passed_analyses "
            "WHERE source_scan_id=? AND strategy_id=? AND symbol=? "
            "AND structure_id=?",
            (row[7], row[0], row[1], row[2]),
        ).fetchall()
        if analysis != [(row[9], row[10])]:
            raise RuntimeError("N21-N25 representative analysis graph conflicts")

    analyses = connection.execute(
        "SELECT source_scan_id,strategy_id,symbol,structure_id,"
        "kline_open_time_ms,confirmation_observed_at_ms,deadline_ms,"
        "evidence_json,evidence_sha256 FROM micro_passed_analyses ORDER BY id"
    )
    for row in analyses:
        try:
            evidence = json.loads(row[7])
            canonical = json.dumps(
                evidence,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            observations = evidence["observations"]
        except Exception as exc:
            raise RuntimeError("N21-N25 passed analysis is invalid") from exc
        if (
            type(row[0]) is not int
            or row[0] <= 0
            or not isinstance(observations, list)
            or not observations
            or not isinstance(observations[-1], list)
            or len(observations[-1]) < 6
            or observations[-1][1] != row[0]
            or hashlib.sha256(canonical.encode("utf-8")).hexdigest() != row[8]
            or evidence.get("strategy_id") != row[1]
            or evidence.get("symbol") != row[2]
            or evidence.get("structure_id") != row[3]
            or evidence.get("kline_open_time_ms") != row[4]
            or evidence.get("confirmation_observed_at_ms") != row[5]
            or evidence.get("deadline_ms") != row[6]
        ):
            raise RuntimeError("N21-N25 passed analysis evidence conflicts")


def validate_micro_staged_lifecycle_graph(
    connection: sqlite3.Connection,
    source_scan_id: int,
) -> tuple[tuple[int, str, str, str, int, str], ...]:
    """Validate only one unpublished scan's closed micro claim subgraph.

    Full permanent-history validation remains a startup/maintenance boundary.
    Runtime STAGING cleanup instead follows the indexed ``source_scan_id``
    closure and authenticates every referenced lifecycle and analysis row.
    """

    if type(source_scan_id) is not int or source_scan_id <= 0:
        raise RuntimeError("micro staging scan identity is invalid")
    if micro_schema_status(connection) != "CURRENT":
        raise RuntimeError("N21-N25 lifecycle schema is not current")
    rows = connection.execute(
        "SELECT lifecycle.source_signal_id,lifecycle.strategy_id,"
        "lifecycle.symbol,lifecycle.structure_id,lifecycle.source_scan_id,"
        "lifecycle.claim_state,lifecycle.kline_open_time_ms,"
        "lifecycle.confirmation_observed_at_ms,lifecycle.deadline_ms,"
        "lifecycle.evidence_json,lifecycle.evidence_sha256,"
        "analysis.kline_open_time_ms,analysis.confirmation_observed_at_ms,"
        "analysis.deadline_ms,analysis.evidence_json,analysis.evidence_sha256 "
        "FROM micro_strategy_lifecycle AS lifecycle "
        "INDEXED BY idx_micro_lifecycle_scan_state "
        "LEFT JOIN micro_passed_analyses AS analysis ON "
        "analysis.source_scan_id=lifecycle.source_scan_id AND "
        "analysis.strategy_id=lifecycle.strategy_id AND "
        "analysis.symbol=lifecycle.symbol AND "
        "analysis.structure_id=lifecycle.structure_id "
        "WHERE lifecycle.source_scan_id=? "
        "ORDER BY lifecycle.source_signal_id",
        (source_scan_id,),
    ).fetchall()
    authenticated: list[tuple[int, str, str, str, int, str]] = []
    for row in rows:
        if (
            len(row) != 16
            or type(row[0]) is not int
            or row[0] <= 0
            or row[1] not in MICRO_STRATEGY_IDS
            or type(row[2]) is not str
            or type(row[3]) is not str
            or type(row[4]) is not int
            or row[4] != source_scan_id
            or row[5] != "STAGED"
            or any(type(row[index]) is not int for index in (6, 7, 8))
            or type(row[9]) is not str
            or type(row[10]) is not str
            or tuple(row[11:16]) != tuple(row[6:11])
        ):
            raise RuntimeError("micro staged lifecycle graph conflicts")
        try:
            evidence = json.loads(row[9])
            canonical = json.dumps(
                evidence,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            observations = evidence["observations"]
        except Exception as exc:
            raise RuntimeError(
                "micro staged lifecycle evidence is invalid"
            ) from exc
        if (
            hashlib.sha256(canonical.encode("utf-8")).hexdigest() != row[10]
            or not isinstance(observations, list)
            or not observations
            or not isinstance(observations[-1], list)
            or len(observations[-1]) < 6
            or type(observations[-1][1]) is not int
            or observations[-1][1] != source_scan_id
            or evidence.get("strategy_id") != row[1]
            or evidence.get("symbol") != row[2]
            or evidence.get("structure_id") != row[3]
            or evidence.get("kline_open_time_ms") != row[6]
            or evidence.get("confirmation_observed_at_ms") != row[7]
            or evidence.get("deadline_ms") != row[8]
        ):
            raise RuntimeError("micro staged lifecycle evidence conflicts")
        authenticated.append(tuple(row[:6]))
    analysis_identities = connection.execute(
        "SELECT strategy_id,symbol,structure_id FROM micro_passed_analyses "
        "WHERE source_scan_id=? ORDER BY strategy_id,symbol,structure_id",
        (source_scan_id,),
    ).fetchall()
    lifecycle_identities = sorted(
        (row[1], row[2], row[3]) for row in authenticated
    )
    if analysis_identities != lifecycle_identities:
        raise RuntimeError("micro staged analysis closure conflicts")
    return tuple(authenticated)
