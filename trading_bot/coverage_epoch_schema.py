from __future__ import annotations

import hashlib
import sqlite3
from typing import Any


COVERAGE_EPOCH_SCHEMA_VERSION = 4
COVERAGE_EPOCH_RULE_VERSION = "N17_N19_COVERAGE_EPOCH_V3"
_STRATEGIES = ("N17", "N18", "N19")


def _normalized_sql(value: str) -> str:
    return "".join(value.lower().split()).replace('"', "").replace("`", "")


EPOCH_TABLE_SQL = """
CREATE TABLE history_coverage_epoch_chain (
    strategy_id TEXT NOT NULL CHECK(strategy_id IN ('N17','N18','N19')),
    symbol TEXT NOT NULL CHECK(length(symbol) BETWEEN 5 AND 32),
    epoch_ordinal INTEGER NOT NULL CHECK(
        typeof(epoch_ordinal) = 'integer' AND epoch_ordinal > 0
    ),
    source_start_time_ms INTEGER NOT NULL CHECK(
        typeof(source_start_time_ms) = 'integer' AND source_start_time_ms > 0
    ),
    initial_covered_through_time_ms INTEGER NOT NULL CHECK(
        typeof(initial_covered_through_time_ms) = 'integer'
        AND initial_covered_through_time_ms >= source_start_time_ms
    ),
    initial_source_sha256 TEXT NOT NULL CHECK(
        length(initial_source_sha256) = 64
        AND initial_source_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    prior_covered_through_time_ms INTEGER CHECK(
        prior_covered_through_time_ms IS NULL OR (
            typeof(prior_covered_through_time_ms) = 'integer'
            AND prior_covered_through_time_ms > 0
        )
    ),
    prior_source_sha256 TEXT CHECK(
        prior_source_sha256 IS NULL OR (
            length(prior_source_sha256) = 64
            AND prior_source_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    previous_chain_sha256 TEXT CHECK(
        previous_chain_sha256 IS NULL OR (
            length(previous_chain_sha256) = 64
            AND previous_chain_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    chain_sha256 TEXT NOT NULL CHECK(
        length(chain_sha256) = 64
        AND chain_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    source_scan_id INTEGER CHECK(
        source_scan_id IS NULL OR (
            typeof(source_scan_id) = 'integer' AND source_scan_id > 0
        )
    ),
    created_at TEXT NOT NULL,
    PRIMARY KEY(strategy_id,symbol,epoch_ordinal),
    UNIQUE(chain_sha256),
    CHECK(
        (epoch_ordinal = 1
         AND prior_covered_through_time_ms IS NULL
         AND prior_source_sha256 IS NULL
         AND previous_chain_sha256 IS NULL)
        OR
        (epoch_ordinal > 1
         AND prior_covered_through_time_ms IS NOT NULL
         AND prior_source_sha256 IS NOT NULL
         AND previous_chain_sha256 IS NOT NULL
         AND source_scan_id IS NOT NULL
         AND source_start_time_ms > prior_covered_through_time_ms + 900000)
    )
)
""".strip()

HEAD_TABLE_SQL = """
CREATE TABLE history_coverage_epoch_heads (
    strategy_id TEXT NOT NULL CHECK(strategy_id IN ('N17','N18','N19')),
    symbol TEXT NOT NULL CHECK(length(symbol) BETWEEN 5 AND 32),
    epoch_ordinal INTEGER NOT NULL CHECK(
        typeof(epoch_ordinal) = 'integer' AND epoch_ordinal > 0
    ),
    epoch_start_time_ms INTEGER NOT NULL CHECK(
        typeof(epoch_start_time_ms) = 'integer' AND epoch_start_time_ms > 0
    ),
    covered_through_time_ms INTEGER NOT NULL CHECK(
        typeof(covered_through_time_ms) = 'integer'
        AND covered_through_time_ms >= epoch_start_time_ms
    ),
    source_sha256 TEXT NOT NULL CHECK(
        length(source_sha256) = 64
        AND source_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    chain_head_sha256 TEXT NOT NULL CHECK(
        length(chain_head_sha256) = 64
        AND chain_head_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    publication_count INTEGER NOT NULL CHECK(
        typeof(publication_count) = 'integer' AND publication_count >= 0
    ),
    latest_receipt_sha256 TEXT CHECK(
        latest_receipt_sha256 IS NULL OR (
            length(latest_receipt_sha256) = 64
            AND latest_receipt_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    updated_at TEXT NOT NULL,
    PRIMARY KEY(strategy_id,symbol),
    CHECK(
        (publication_count = 0 AND latest_receipt_sha256 IS NULL)
        OR
        (publication_count > 0 AND latest_receipt_sha256 IS NOT NULL)
    )
)
""".strip()

INSTALLATION_TABLE_SQL = """
CREATE TABLE history_coverage_epoch_installation (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    schema_version INTEGER NOT NULL CHECK(
        typeof(schema_version) = 'integer' AND schema_version = 4
    ),
    rule_version TEXT NOT NULL CHECK(
        rule_version = 'N17_N19_COVERAGE_EPOCH_V3'
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

LEGACY_V3_INSTALLATION_TABLE_SQL = INSTALLATION_TABLE_SQL.replace(
    "schema_version = 4",
    "schema_version = 3",
)

LEGACY_V3_PUBLICATION_TABLE_SQL = """
CREATE TABLE history_coverage_publication_receipts (
    source_scan_id INTEGER NOT NULL CHECK(
        typeof(source_scan_id) = 'integer' AND source_scan_id > 0
    ),
    strategy_id TEXT NOT NULL CHECK(strategy_id IN ('N17','N18','N19')),
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
    publication_ordinal INTEGER NOT NULL CHECK(
        typeof(publication_ordinal) = 'integer' AND publication_ordinal > 0
    ),
    previous_receipt_sha256 TEXT CHECK(
        previous_receipt_sha256 IS NULL OR (
            length(previous_receipt_sha256) = 64
            AND previous_receipt_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    batch_expected_count INTEGER NOT NULL CHECK(
        typeof(batch_expected_count) = 'integer' AND batch_expected_count > 0
    ),
    batch_manifest_sha256 TEXT NOT NULL CHECK(
        length(batch_manifest_sha256) = 64
        AND batch_manifest_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    receipt_sha256 TEXT NOT NULL CHECK(
        length(receipt_sha256) = 64
        AND receipt_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    published_at TEXT NOT NULL,
    PRIMARY KEY(source_scan_id,strategy_id,symbol),
    UNIQUE(receipt_sha256),
    UNIQUE(strategy_id,symbol,publication_ordinal),
    CHECK(
        (publication_ordinal = 1 AND previous_receipt_sha256 IS NULL)
        OR
        (publication_ordinal > 1 AND previous_receipt_sha256 IS NOT NULL)
    )
)
""".strip()

PUBLICATION_TABLE_SQL = LEGACY_V3_PUBLICATION_TABLE_SQL.replace(
    """    receipt_sha256 TEXT NOT NULL CHECK(
""",
    """    terminal_binding_sha256 TEXT CHECK(
        terminal_binding_sha256 IS NULL OR (
            length(terminal_binding_sha256) = 64
            AND terminal_binding_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    receipt_sha256 TEXT NOT NULL CHECK(
""",
)

N19_TERMINAL_PUBLICATION_TABLE_SQL = """
CREATE TABLE history_coverage_n19_terminal_receipts (
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
    receipt_sha256 TEXT NOT NULL UNIQUE CHECK(
        length(receipt_sha256) = 64
        AND receipt_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    published_at TEXT NOT NULL,
    PRIMARY KEY(source_scan_id,strategy_id,symbol)
)
""".strip()

INDEX_SQL = {
    "idx_history_coverage_epoch_symbol": (
        "CREATE UNIQUE INDEX idx_history_coverage_epoch_symbol "
        "ON history_coverage_epoch_chain(strategy_id,symbol,epoch_ordinal)"
    ),
    "idx_history_coverage_epoch_head": (
        "CREATE UNIQUE INDEX idx_history_coverage_epoch_head "
        "ON history_coverage_epoch_heads(strategy_id,symbol)"
    ),
    "idx_history_coverage_receipt_owner": (
        "CREATE UNIQUE INDEX idx_history_coverage_receipt_owner "
        "ON history_coverage_publication_receipts("
        "source_scan_id,strategy_id,symbol)"
    ),
    "idx_history_coverage_n19_terminal_owner": (
        "CREATE UNIQUE INDEX idx_history_coverage_n19_terminal_owner "
        "ON history_coverage_n19_terminal_receipts("
        "source_scan_id,strategy_id,symbol)"
    ),
    "idx_history_coverage_n19_terminal_identity": (
        "CREATE UNIQUE INDEX idx_history_coverage_n19_terminal_identity "
        "ON history_coverage_n19_terminal_receipts("
        "strategy_id,symbol,family_id)"
    ),
}

TRIGGER_SQL = {
    "trg_history_coverage_epoch_no_replace": """
CREATE TRIGGER trg_history_coverage_epoch_no_replace
BEFORE INSERT ON history_coverage_epoch_chain
WHEN EXISTS(
  SELECT 1 FROM history_coverage_epoch_chain
  WHERE strategy_id=NEW.strategy_id AND symbol=NEW.symbol
    AND epoch_ordinal=NEW.epoch_ordinal
)
BEGIN SELECT RAISE(ABORT, 'coverage epoch replacement is forbidden'); END
""".strip(),
    "trg_history_coverage_epoch_no_update": """
CREATE TRIGGER trg_history_coverage_epoch_no_update
BEFORE UPDATE ON history_coverage_epoch_chain
BEGIN SELECT RAISE(ABORT, 'coverage epoch rows are immutable'); END
""".strip(),
    "trg_history_coverage_epoch_no_delete": """
CREATE TRIGGER trg_history_coverage_epoch_no_delete
BEFORE DELETE ON history_coverage_epoch_chain
BEGIN SELECT RAISE(ABORT, 'coverage epoch rows are permanent'); END
""".strip(),
    "trg_history_coverage_epoch_insert_authorized": """
CREATE TRIGGER trg_history_coverage_epoch_insert_authorized
BEFORE INSERT ON history_coverage_epoch_chain
WHEN _coverage_epoch_mutation_authorized(
  'chain_insert',NEW.strategy_id,NEW.symbol,NEW.source_scan_id,NULL,NULL
) != 1
BEGIN SELECT RAISE(ABORT, 'coverage epoch append is unauthorized'); END
""".strip(),
    "trg_history_coverage_head_no_delete": """
CREATE TRIGGER trg_history_coverage_head_no_delete
BEFORE DELETE ON history_coverage_epoch_heads
BEGIN SELECT RAISE(ABORT, 'coverage epoch heads are permanent'); END
""".strip(),
    "trg_history_coverage_head_no_replace": """
CREATE TRIGGER trg_history_coverage_head_no_replace
BEFORE INSERT ON history_coverage_epoch_heads
WHEN EXISTS(
  SELECT 1 FROM history_coverage_epoch_heads
  WHERE strategy_id=NEW.strategy_id AND symbol=NEW.symbol
)
BEGIN SELECT RAISE(ABORT, 'coverage epoch head replacement is forbidden'); END
""".strip(),
    "trg_history_coverage_head_insert_authorized": """
CREATE TRIGGER trg_history_coverage_head_insert_authorized
BEFORE INSERT ON history_coverage_epoch_heads
WHEN _coverage_epoch_mutation_authorized(
  'head_insert',NEW.strategy_id,NEW.symbol,NULL,NULL,NULL
) != 1
BEGIN SELECT RAISE(ABORT, 'coverage epoch head creation is unauthorized'); END
""".strip(),
    "trg_history_coverage_head_update_authorized": """
CREATE TRIGGER trg_history_coverage_head_update_authorized
BEFORE UPDATE ON history_coverage_epoch_heads
WHEN _coverage_epoch_mutation_authorized(
  'head_update',NEW.strategy_id,NEW.symbol,NULL,NULL,NULL
) != 1
BEGIN SELECT RAISE(ABORT, 'coverage epoch head update is unauthorized'); END
""".strip(),
    "trg_history_coverage_head_transition": """
CREATE TRIGGER trg_history_coverage_head_transition
BEFORE UPDATE ON history_coverage_epoch_heads
WHEN OLD.strategy_id != NEW.strategy_id
  OR OLD.symbol != NEW.symbol
  OR NEW.covered_through_time_ms < OLD.covered_through_time_ms
  OR NEW.publication_count != OLD.publication_count + 1
  OR NEW.latest_receipt_sha256 IS NULL
  OR NEW.latest_receipt_sha256 = OLD.latest_receipt_sha256
  OR NOT (
    (
      NEW.epoch_ordinal = OLD.epoch_ordinal
      AND NEW.epoch_start_time_ms = OLD.epoch_start_time_ms
      AND NEW.chain_head_sha256 = OLD.chain_head_sha256
    )
    OR
    (
      NEW.epoch_ordinal = OLD.epoch_ordinal + 1
      AND NEW.epoch_start_time_ms > OLD.covered_through_time_ms + 900000
      AND NEW.chain_head_sha256 != OLD.chain_head_sha256
    )
  )
BEGIN SELECT RAISE(ABORT, 'coverage epoch head transition is invalid'); END
""".strip(),
    "trg_history_coverage_installation_no_update": """
CREATE TRIGGER trg_history_coverage_installation_no_update
BEFORE UPDATE ON history_coverage_epoch_installation
BEGIN SELECT RAISE(ABORT, 'coverage epoch installation is immutable'); END
""".strip(),
    "trg_history_coverage_installation_no_replace": """
CREATE TRIGGER trg_history_coverage_installation_no_replace
BEFORE INSERT ON history_coverage_epoch_installation
WHEN EXISTS(
  SELECT 1 FROM history_coverage_epoch_installation WHERE singleton_id=1
)
BEGIN SELECT RAISE(ABORT, 'coverage epoch installation replacement is forbidden'); END
""".strip(),
    "trg_history_coverage_installation_insert_authorized": """
CREATE TRIGGER trg_history_coverage_installation_insert_authorized
BEFORE INSERT ON history_coverage_epoch_installation
WHEN _coverage_epoch_mutation_authorized(
  'installation_insert','N17','INSTALLATION',NULL,NULL,NULL
) != 1
BEGIN SELECT RAISE(ABORT, 'coverage epoch installation is unauthorized'); END
""".strip(),
    "trg_history_coverage_installation_no_delete": """
CREATE TRIGGER trg_history_coverage_installation_no_delete
BEFORE DELETE ON history_coverage_epoch_installation
BEGIN SELECT RAISE(ABORT, 'coverage epoch installation is permanent'); END
""".strip(),
    "trg_history_coverage_receipt_insert_authorized": """
CREATE TRIGGER trg_history_coverage_receipt_insert_authorized
BEFORE INSERT ON history_coverage_publication_receipts
WHEN _coverage_epoch_mutation_authorized(
  'receipt_insert',NEW.strategy_id,NEW.symbol,NEW.source_scan_id,
  NEW.batch_expected_count,NEW.batch_manifest_sha256
) != 1
BEGIN SELECT RAISE(ABORT, 'coverage publication receipt is unauthorized'); END
""".strip(),
    "trg_history_coverage_receipt_no_replace": """
CREATE TRIGGER trg_history_coverage_receipt_no_replace
BEFORE INSERT ON history_coverage_publication_receipts
WHEN EXISTS(
  SELECT 1 FROM history_coverage_publication_receipts
  WHERE source_scan_id=NEW.source_scan_id
    AND strategy_id=NEW.strategy_id AND symbol=NEW.symbol
)
BEGIN SELECT RAISE(ABORT, 'coverage publication receipt replacement is forbidden'); END
""".strip(),
    "trg_history_coverage_receipt_no_update": """
CREATE TRIGGER trg_history_coverage_receipt_no_update
BEFORE UPDATE ON history_coverage_publication_receipts
BEGIN SELECT RAISE(ABORT, 'coverage publication receipts are immutable'); END
""".strip(),
    "trg_history_coverage_receipt_no_delete": """
CREATE TRIGGER trg_history_coverage_receipt_no_delete
BEFORE DELETE ON history_coverage_publication_receipts
BEGIN SELECT RAISE(ABORT, 'coverage publication receipts are permanent'); END
""".strip(),
    "trg_history_coverage_n19_terminal_insert_authorized": """
CREATE TRIGGER trg_history_coverage_n19_terminal_insert_authorized
BEFORE INSERT ON history_coverage_n19_terminal_receipts
WHEN _coverage_epoch_mutation_authorized(
  'terminal_receipt_insert',NEW.strategy_id,NEW.symbol,NEW.source_scan_id,
  NEW.batch_expected_count,NEW.batch_manifest_sha256
) != 1
BEGIN SELECT RAISE(ABORT, 'N19 terminal publication receipt is unauthorized'); END
""".strip(),
    "trg_history_coverage_n19_terminal_no_replace": """
CREATE TRIGGER trg_history_coverage_n19_terminal_no_replace
BEFORE INSERT ON history_coverage_n19_terminal_receipts
WHEN EXISTS(
  SELECT 1 FROM history_coverage_n19_terminal_receipts
  WHERE source_scan_id=NEW.source_scan_id
    AND strategy_id=NEW.strategy_id AND symbol=NEW.symbol
  UNION ALL
  SELECT 1 FROM history_coverage_n19_terminal_receipts
  WHERE strategy_id=NEW.strategy_id AND symbol=NEW.symbol
    AND family_id=NEW.family_id
)
BEGIN SELECT RAISE(ABORT, 'N19 terminal publication receipt replacement is forbidden'); END
""".strip(),
    "trg_history_coverage_n19_terminal_no_update": """
CREATE TRIGGER trg_history_coverage_n19_terminal_no_update
BEFORE UPDATE ON history_coverage_n19_terminal_receipts
BEGIN SELECT RAISE(ABORT, 'N19 terminal publication receipts are immutable'); END
""".strip(),
    "trg_history_coverage_n19_terminal_no_delete": """
CREATE TRIGGER trg_history_coverage_n19_terminal_no_delete
BEFORE DELETE ON history_coverage_n19_terminal_receipts
BEGIN SELECT RAISE(ABORT, 'N19 terminal publication receipts are permanent'); END
""".strip(),
}

for _strategy_id in _STRATEGIES:
    _lower = _strategy_id.lower()
    _table = f"{_lower}_history_coverage"
    TRIGGER_SQL[f"trg_history_coverage_{_lower}_mirror_insert"] = (
        f"CREATE TRIGGER trg_history_coverage_{_lower}_mirror_insert\n"
        f"BEFORE INSERT ON {_table}\n"
        "WHEN _coverage_epoch_mutation_authorized("
        f"'mirror_insert',NEW.strategy_id,NEW.symbol,NULL,NULL,NULL) != 1\n"
        "BEGIN SELECT RAISE(ABORT, "
        f"'{_strategy_id} coverage mirror insert is unauthorized'); END"
    )
    TRIGGER_SQL[f"trg_history_coverage_{_lower}_mirror_no_replace"] = (
        f"CREATE TRIGGER trg_history_coverage_{_lower}_mirror_no_replace\n"
        f"BEFORE INSERT ON {_table}\n"
        "WHEN EXISTS(SELECT 1 FROM "
        f"{_table} WHERE strategy_id=NEW.strategy_id AND symbol=NEW.symbol)\n"
        "BEGIN SELECT RAISE(ABORT, "
        f"'{_strategy_id} coverage mirror replacement is forbidden'); END"
    )
    TRIGGER_SQL[f"trg_history_coverage_{_lower}_mirror_update"] = (
        f"CREATE TRIGGER trg_history_coverage_{_lower}_mirror_update\n"
        f"BEFORE UPDATE ON {_table}\n"
        "WHEN _coverage_epoch_mutation_authorized("
        f"'mirror_update',NEW.strategy_id,NEW.symbol,NULL,NULL,NULL) != 1\n"
        "BEGIN SELECT RAISE(ABORT, "
        f"'{_strategy_id} coverage mirror update is unauthorized'); END"
    )
    TRIGGER_SQL[f"trg_history_coverage_{_lower}_mirror_delete"] = (
        f"CREATE TRIGGER trg_history_coverage_{_lower}_mirror_delete\n"
        f"BEFORE DELETE ON {_table}\n"
        "BEGIN SELECT RAISE(ABORT, "
        f"'{_strategy_id} coverage mirror is permanent'); END"
    )

TABLES = (
    "history_coverage_epoch_chain",
    "history_coverage_epoch_heads",
    "history_coverage_epoch_installation",
    "history_coverage_publication_receipts",
    "history_coverage_n19_terminal_receipts",
)
_OBJECTS = {*TABLES, *INDEX_SQL, *TRIGGER_SQL}
_N19_TERMINAL_OBJECTS = {
    "history_coverage_n19_terminal_receipts",
    "idx_history_coverage_n19_terminal_owner",
    "idx_history_coverage_n19_terminal_identity",
    "trg_history_coverage_n19_terminal_insert_authorized",
    "trg_history_coverage_n19_terminal_no_replace",
    "trg_history_coverage_n19_terminal_no_update",
    "trg_history_coverage_n19_terminal_no_delete",
}
_LEGACY_V3_OBJECTS = _OBJECTS - _N19_TERMINAL_OBJECTS


def coverage_epoch_chain_sha256(
    strategy_id: str,
    symbol: str,
    epoch_ordinal: int,
    source_start_time_ms: int,
    initial_covered_through_time_ms: int,
    initial_source_sha256: str,
    prior_covered_through_time_ms: int | None,
    prior_source_sha256: str | None,
    previous_chain_sha256: str | None,
    source_scan_id: int | None,
) -> str:
    values = (
        COVERAGE_EPOCH_RULE_VERSION,
        strategy_id,
        symbol,
        str(epoch_ordinal),
        str(source_start_time_ms),
        str(initial_covered_through_time_ms),
        initial_source_sha256,
        "" if prior_covered_through_time_ms is None else str(prior_covered_through_time_ms),
        "" if prior_source_sha256 is None else prior_source_sha256,
        "" if previous_chain_sha256 is None else previous_chain_sha256,
        "" if source_scan_id is None else str(source_scan_id),
    )
    return hashlib.sha256("|".join(values).encode("utf-8")).hexdigest()


def coverage_publication_receipt_sha256(
    source_scan_id: int,
    strategy_id: str,
    symbol: str,
    source_start_time_ms: int,
    covered_through_time_ms: int,
    source_sha256: str,
    result_epoch_ordinal: int,
    result_epoch_start_time_ms: int,
    result_chain_head_sha256: str,
    publication_ordinal: int,
    previous_receipt_sha256: str | None,
    batch_expected_count: int,
    batch_manifest_sha256: str,
    terminal_binding_sha256: str | None = None,
) -> str:
    values = (
        COVERAGE_EPOCH_RULE_VERSION,
        str(source_scan_id),
        strategy_id,
        symbol,
        str(source_start_time_ms),
        str(covered_through_time_ms),
        source_sha256,
        str(result_epoch_ordinal),
        str(result_epoch_start_time_ms),
        result_chain_head_sha256,
        str(publication_ordinal),
        "" if previous_receipt_sha256 is None else previous_receipt_sha256,
        str(batch_expected_count),
        batch_manifest_sha256,
    )
    if terminal_binding_sha256 is not None:
        values += ("N19_TERMINAL_BINDING_V1", terminal_binding_sha256)
    return hashlib.sha256("|".join(values).encode("utf-8")).hexdigest()


def n19_terminal_publication_binding_sha256(
    source_scan_id: int,
    symbol: str,
    family_id: str,
    structure_id: str,
    terminal_evidence_json: str,
    terminal_evidence_sha256: str,
    batch_expected_count: int,
    batch_manifest_sha256: str,
    result_epoch_ordinal: int,
    result_epoch_start_time_ms: int,
    result_chain_head_sha256: str,
) -> str:
    evidence_json_sha256 = hashlib.sha256(
        terminal_evidence_json.encode("utf-8")
    ).hexdigest()
    values = (
        COVERAGE_EPOCH_RULE_VERSION,
        "N19_TERMINAL_BINDING_V1",
        str(source_scan_id),
        "N19",
        symbol,
        family_id,
        structure_id,
        evidence_json_sha256,
        terminal_evidence_sha256,
        str(batch_expected_count),
        batch_manifest_sha256,
        str(result_epoch_ordinal),
        str(result_epoch_start_time_ms),
        result_chain_head_sha256,
    )
    return hashlib.sha256("|".join(values).encode("utf-8")).hexdigest()


def n19_terminal_publication_receipt_sha256(
    source_scan_id: int,
    symbol: str,
    family_id: str,
    structure_id: str,
    terminal_evidence_json: str,
    terminal_evidence_sha256: str,
    batch_expected_count: int,
    batch_manifest_sha256: str,
    coverage_receipt_sha256: str,
    result_epoch_ordinal: int,
    result_epoch_start_time_ms: int,
    result_chain_head_sha256: str,
) -> str:
    binding_sha256 = n19_terminal_publication_binding_sha256(
        source_scan_id,
        symbol,
        family_id,
        structure_id,
        terminal_evidence_json,
        terminal_evidence_sha256,
        batch_expected_count,
        batch_manifest_sha256,
        result_epoch_ordinal,
        result_epoch_start_time_ms,
        result_chain_head_sha256,
    )
    values = (
        COVERAGE_EPOCH_RULE_VERSION,
        "N19_TERMINAL_PUBLICATION_V1",
        binding_sha256,
        coverage_receipt_sha256,
    )
    return hashlib.sha256("|".join(values).encode("utf-8")).hexdigest()


def _owned_objects(connection: sqlite3.Connection) -> dict[str, tuple[str, str]]:
    from .coverage_family_seal import OBJECTS as FAMILY_SEAL_OBJECTS

    rows = connection.execute(
        "SELECT type,name,sql FROM sqlite_schema "
        "WHERE name LIKE 'history_coverage_epoch_%' "
        "OR name LIKE 'history_coverage_publication_%' "
        "OR name LIKE 'history_coverage_n19_terminal_%' "
        "OR name LIKE 'idx_history_coverage_epoch_%' "
        "OR name LIKE 'idx_history_coverage_receipt_%' "
        "OR name LIKE 'idx_history_coverage_n19_terminal_%' "
        "OR name LIKE 'trg_history_coverage_%'"
    ).fetchall()
    result: dict[str, tuple[str, str]] = {}
    for row in rows:
        if type(row) in (tuple, list) and row[1] in FAMILY_SEAL_OBJECTS:
            continue
        if (
            type(row) not in (tuple, list)
            or len(row) != 3
            or type(row[0]) is not str
            or type(row[1]) is not str
            or type(row[2]) is not str
            or row[1] in result
        ):
            raise RuntimeError("coverage epoch catalog is invalid")
        result[row[1]] = (row[0], row[2])
    return result


def _coverage_epoch_catalog_sha256_for_objects(
    connection: sqlite3.Connection,
    objects: set[str],
) -> str:
    placeholders = ",".join("?" for _ in objects)
    rows = connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema "
        f"WHERE name IN ({placeholders}) ORDER BY type,name",
        tuple(sorted(objects)),
    ).fetchall()
    if len(rows) != len(objects):
        raise RuntimeError("coverage epoch catalog identity is incomplete")
    canonical: list[str] = []
    for row in rows:
        if (
            type(row) not in (tuple, list)
            or len(row) != 4
            or any(type(value) is not str for value in row)
        ):
            raise RuntimeError("coverage epoch catalog identity is invalid")
        canonical.append(
            "|".join((row[0], row[1], row[2], _normalized_sql(row[3])))
        )
    return hashlib.sha256("\n".join(canonical).encode("utf-8")).hexdigest()


def coverage_epoch_catalog_sha256(connection: sqlite3.Connection) -> str:
    return _coverage_epoch_catalog_sha256_for_objects(connection, _OBJECTS)


def coverage_epoch_schema_status(
    connection: sqlite3.Connection,
    *,
    validate_graph: bool = True,
) -> str:
    owned = _owned_objects(connection)
    if not owned:
        return "PRE_EPOCH"
    object_names = set(owned)
    legacy_v3 = object_names == _LEGACY_V3_OBJECTS
    installation_version = None
    if "history_coverage_epoch_installation" in object_names:
        version_row = connection.execute(
            "SELECT schema_version FROM history_coverage_epoch_installation "
            "WHERE singleton_id=1"
        ).fetchone()
        if (
            version_row is not None
            and len(version_row) == 1
            and type(version_row[0]) is int
        ):
            installation_version = version_row[0]
    authorized_legacy_v3 = (
        object_names == _OBJECTS and installation_version == 3
    )
    if object_names not in (_OBJECTS, _LEGACY_V3_OBJECTS):
        raise RuntimeError("coverage epoch schema is partial")
    expected_sql = {
        "history_coverage_epoch_chain": EPOCH_TABLE_SQL,
        "history_coverage_epoch_heads": HEAD_TABLE_SQL,
        "history_coverage_epoch_installation": (
            LEGACY_V3_INSTALLATION_TABLE_SQL
            if legacy_v3 or authorized_legacy_v3
            else INSTALLATION_TABLE_SQL
        ),
        "history_coverage_publication_receipts": (
            LEGACY_V3_PUBLICATION_TABLE_SQL
            if legacy_v3 or authorized_legacy_v3
            else PUBLICATION_TABLE_SQL
        ),
        "history_coverage_n19_terminal_receipts": (
            N19_TERMINAL_PUBLICATION_TABLE_SQL
        ),
        **INDEX_SQL,
        **TRIGGER_SQL,
    }
    expected_sql = {
        name: sql for name, sql in expected_sql.items() if name in object_names
    }
    expected_types = {
        **{name: "table" for name in TABLES},
        **{name: "index" for name in INDEX_SQL},
        **{name: "trigger" for name in TRIGGER_SQL},
    }
    for name, sql in expected_sql.items():
        if (
            owned[name][0] != expected_types[name]
            or _normalized_sql(owned[name][1]) != _normalized_sql(sql)
        ):
            raise RuntimeError(f"coverage epoch object is inconsistent: {name}")
    active_tables = tuple(table for table in TABLES if table in object_names)
    for table in active_tables:
        if connection.execute(f"PRAGMA foreign_key_list({table})").fetchall():
            raise RuntimeError("coverage epoch table has an unexpected foreign key")
    expected_indexes = {
        "history_coverage_epoch_chain": {
            "idx_history_coverage_epoch_symbol": (1, "c", 0),
            "sqlite_autoindex_history_coverage_epoch_chain_1": (1, "pk", 0),
            "sqlite_autoindex_history_coverage_epoch_chain_2": (1, "u", 0),
        },
        "history_coverage_epoch_heads": {
            "idx_history_coverage_epoch_head": (1, "c", 0),
            "sqlite_autoindex_history_coverage_epoch_heads_1": (1, "pk", 0),
        },
        "history_coverage_epoch_installation": {},
        "history_coverage_publication_receipts": {
            "idx_history_coverage_receipt_owner": (1, "c", 0),
            "sqlite_autoindex_history_coverage_publication_receipts_1": (
                1, "pk", 0
            ),
            "sqlite_autoindex_history_coverage_publication_receipts_2": (
                1, "u", 0
            ),
            "sqlite_autoindex_history_coverage_publication_receipts_3": (
                1, "u", 0
            ),
        },
        "history_coverage_n19_terminal_receipts": {
            "idx_history_coverage_n19_terminal_owner": (1, "c", 0),
            "idx_history_coverage_n19_terminal_identity": (1, "c", 0),
            "sqlite_autoindex_history_coverage_n19_terminal_receipts_1": (
                1, "u", 0
            ),
            "sqlite_autoindex_history_coverage_n19_terminal_receipts_2": (
                1, "u", 0
            ),
            "sqlite_autoindex_history_coverage_n19_terminal_receipts_3": (
                1, "pk", 0
            ),
        },
    }
    expected_index_xinfo = {
        "idx_history_coverage_epoch_symbol": (
            (0, 0, "strategy_id", 0, "BINARY", 1),
            (1, 1, "symbol", 0, "BINARY", 1),
            (2, 2, "epoch_ordinal", 0, "BINARY", 1),
            (3, -1, None, 0, "BINARY", 0),
        ),
        "sqlite_autoindex_history_coverage_epoch_chain_1": (
            (0, 0, "strategy_id", 0, "BINARY", 1),
            (1, 1, "symbol", 0, "BINARY", 1),
            (2, 2, "epoch_ordinal", 0, "BINARY", 1),
            (3, -1, None, 0, "BINARY", 0),
        ),
        "sqlite_autoindex_history_coverage_epoch_chain_2": (
            (0, 9, "chain_sha256", 0, "BINARY", 1),
            (1, -1, None, 0, "BINARY", 0),
        ),
        "idx_history_coverage_epoch_head": (
            (0, 0, "strategy_id", 0, "BINARY", 1),
            (1, 1, "symbol", 0, "BINARY", 1),
            (2, -1, None, 0, "BINARY", 0),
        ),
        "sqlite_autoindex_history_coverage_epoch_heads_1": (
            (0, 0, "strategy_id", 0, "BINARY", 1),
            (1, 1, "symbol", 0, "BINARY", 1),
            (2, -1, None, 0, "BINARY", 0),
        ),
        "idx_history_coverage_receipt_owner": (
            (0, 0, "source_scan_id", 0, "BINARY", 1),
            (1, 1, "strategy_id", 0, "BINARY", 1),
            (2, 2, "symbol", 0, "BINARY", 1),
            (3, -1, None, 0, "BINARY", 0),
        ),
        "sqlite_autoindex_history_coverage_publication_receipts_1": (
            (0, 0, "source_scan_id", 0, "BINARY", 1),
            (1, 1, "strategy_id", 0, "BINARY", 1),
            (2, 2, "symbol", 0, "BINARY", 1),
            (3, -1, None, 0, "BINARY", 0),
        ),
        "sqlite_autoindex_history_coverage_publication_receipts_2": (
            (0, 14, "receipt_sha256", 0, "BINARY", 1),
            (1, -1, None, 0, "BINARY", 0),
        ),
        "sqlite_autoindex_history_coverage_publication_receipts_3": (
            (0, 1, "strategy_id", 0, "BINARY", 1),
            (1, 2, "symbol", 0, "BINARY", 1),
            (2, 9, "publication_ordinal", 0, "BINARY", 1),
            (3, -1, None, 0, "BINARY", 0),
        ),
        "idx_history_coverage_n19_terminal_owner": (
            (0, 0, "source_scan_id", 0, "BINARY", 1),
            (1, 1, "strategy_id", 0, "BINARY", 1),
            (2, 2, "symbol", 0, "BINARY", 1),
            (3, -1, None, 0, "BINARY", 0),
        ),
        "idx_history_coverage_n19_terminal_identity": (
            (0, 1, "strategy_id", 0, "BINARY", 1),
            (1, 2, "symbol", 0, "BINARY", 1),
            (2, 3, "family_id", 0, "BINARY", 1),
            (3, -1, None, 0, "BINARY", 0),
        ),
        "sqlite_autoindex_history_coverage_n19_terminal_receipts_1": (
            (0, 9, "coverage_receipt_sha256", 0, "BINARY", 1),
            (1, -1, None, 0, "BINARY", 0),
        ),
        "sqlite_autoindex_history_coverage_n19_terminal_receipts_2": (
            (0, 13, "receipt_sha256", 0, "BINARY", 1),
            (1, -1, None, 0, "BINARY", 0),
        ),
        "sqlite_autoindex_history_coverage_n19_terminal_receipts_3": (
            (0, 0, "source_scan_id", 0, "BINARY", 1),
            (1, 1, "strategy_id", 0, "BINARY", 1),
            (2, 2, "symbol", 0, "BINARY", 1),
            (3, -1, None, 0, "BINARY", 0),
        ),
    }
    expected_triggers = {
        "history_coverage_epoch_chain": {
            "trg_history_coverage_epoch_no_replace",
            "trg_history_coverage_epoch_no_update",
            "trg_history_coverage_epoch_no_delete",
            "trg_history_coverage_epoch_insert_authorized",
        },
        "history_coverage_epoch_heads": {
            "trg_history_coverage_head_no_delete",
            "trg_history_coverage_head_no_replace",
            "trg_history_coverage_head_transition",
            "trg_history_coverage_head_insert_authorized",
            "trg_history_coverage_head_update_authorized",
        },
        "history_coverage_epoch_installation": {
            "trg_history_coverage_installation_no_update",
            "trg_history_coverage_installation_no_delete",
            "trg_history_coverage_installation_no_replace",
            "trg_history_coverage_installation_insert_authorized",
        },
        "history_coverage_publication_receipts": {
            "trg_history_coverage_receipt_insert_authorized",
            "trg_history_coverage_receipt_no_replace",
            "trg_history_coverage_receipt_no_update",
            "trg_history_coverage_receipt_no_delete",
        },
        "history_coverage_n19_terminal_receipts": {
            "trg_history_coverage_n19_terminal_insert_authorized",
            "trg_history_coverage_n19_terminal_no_replace",
            "trg_history_coverage_n19_terminal_no_update",
            "trg_history_coverage_n19_terminal_no_delete",
        },
    }
    if connection.execute(
        "SELECT 1 FROM sqlite_schema WHERE type='table' "
        "AND name='history_coverage_family_seal_installation'"
    ).fetchone() == (1,):
        expected_triggers["history_coverage_epoch_chain"].add(
            "trg_history_coverage_generation_epoch_chain_insert"
        )
        expected_triggers["history_coverage_epoch_heads"].update(
            {
                "trg_history_coverage_generation_epoch_head_insert",
                "trg_history_coverage_generation_epoch_head_update",
            }
        )
        expected_triggers["history_coverage_publication_receipts"].add(
            "trg_history_coverage_generation_publication_receipt_insert"
        )
        if any(
            row[1] == "terminal_binding_sha256"
            for row in connection.execute(
                "PRAGMA table_xinfo(history_coverage_publication_receipts)"
            )
        ):
            expected_triggers["history_coverage_publication_receipts"].add(
                "trg_history_coverage_terminal_publication_requires_bundle"
            )
        expected_triggers[
            "history_coverage_n19_terminal_receipts"
        ].update(
            {
                "trg_history_coverage_generation_terminal_receipt_insert",
                "trg_history_coverage_n19_terminal_no_legacy_proof",
                "trg_history_coverage_n19_terminal_requires_bundle",
            }
        )
    expected_table_sql = {
        "history_coverage_epoch_chain": EPOCH_TABLE_SQL,
        "history_coverage_epoch_heads": HEAD_TABLE_SQL,
        "history_coverage_epoch_installation": INSTALLATION_TABLE_SQL,
        "history_coverage_publication_receipts": PUBLICATION_TABLE_SQL,
        "history_coverage_n19_terminal_receipts": (
            N19_TERMINAL_PUBLICATION_TABLE_SQL
        ),
    }
    if legacy_v3 or authorized_legacy_v3:
        expected_table_sql["history_coverage_epoch_installation"] = (
            LEGACY_V3_INSTALLATION_TABLE_SQL
        )
        if legacy_v3:
            expected_table_sql.pop(
                "history_coverage_n19_terminal_receipts"
            )
        expected_table_sql["history_coverage_publication_receipts"] = (
            LEGACY_V3_PUBLICATION_TABLE_SQL
        )
        expected_index_xinfo[
            "sqlite_autoindex_history_coverage_publication_receipts_2"
        ] = (
            (0, 13, "receipt_sha256", 0, "BINARY", 1),
            (1, -1, None, 0, "BINARY", 0),
        )
    expected_connection = sqlite3.connect(":memory:")
    try:
        for table, table_sql in expected_table_sql.items():
            expected_connection.execute(table_sql)
            expected_xinfo = tuple(
                tuple(row)
                for row in expected_connection.execute(
                    f'PRAGMA table_xinfo("{table}")'
                )
            )
            actual_xinfo = tuple(
                tuple(row)
                for row in connection.execute(
                    f'PRAGMA table_xinfo("{table}")'
                )
            )
            if actual_xinfo != expected_xinfo:
                raise RuntimeError(
                    f"coverage epoch table metadata conflicts: {table}"
                )
    finally:
        expected_connection.close()
    for table in active_tables:
        indexes = {
            row[1]: (row[2], row[3], row[4])
            for row in connection.execute(f"PRAGMA index_list({table})")
        }
        if indexes != expected_indexes[table]:
            raise RuntimeError(f"coverage epoch index set conflicts: {table}")
        for index_name in indexes:
            actual_xinfo = tuple(
                tuple(row)
                for row in connection.execute(
                    f'PRAGMA index_xinfo("{index_name}")'
                )
            )
            if actual_xinfo != expected_index_xinfo[index_name]:
                raise RuntimeError(
                    f"coverage epoch index metadata conflicts: {index_name}"
                )
        triggers = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_schema "
                "WHERE type='trigger' AND tbl_name=?",
                (table,),
            )
        }
        if triggers != expected_triggers[table]:
            raise RuntimeError(f"coverage epoch trigger set conflicts: {table}")
    for strategy_id in _STRATEGIES:
        lower = strategy_id.lower()
        table = f"{lower}_history_coverage"
        expected_mirror_triggers = {
            f"trg_history_coverage_{lower}_mirror_insert",
            f"trg_history_coverage_{lower}_mirror_no_replace",
            f"trg_history_coverage_{lower}_mirror_update",
            f"trg_history_coverage_{lower}_mirror_delete",
        }
        actual_mirror_triggers = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_schema "
                "WHERE type='trigger' AND tbl_name=? "
                "AND name LIKE 'trg_history_coverage_%'",
                (table,),
            )
        }
        if actual_mirror_triggers != expected_mirror_triggers:
            raise RuntimeError(
                f"coverage epoch mirror trigger set conflicts: {table}"
            )
    lowered_tables = {table.casefold() for table in active_tables}
    for table_row in connection.execute(
        "SELECT name FROM sqlite_schema WHERE type='table'"
    ):
        table_name = table_row[0]
        if type(table_name) is not str:
            raise RuntimeError("coverage epoch catalog table name is invalid")
        if table_name.casefold() in lowered_tables:
            continue
        for foreign_key in connection.execute(
            f'PRAGMA foreign_key_list("{table_name.replace(chr(34), chr(34) * 2)}")'
        ):
            if (
                len(foreign_key) > 2
                and type(foreign_key[2]) is str
                and foreign_key[2].casefold() in lowered_tables
            ):
                raise RuntimeError(
                    "coverage epoch table has an incoming foreign key"
                )
    row = connection.execute(
        "SELECT schema_version,rule_version,catalog_schema_version,"
        "catalog_sha256 "
        "FROM history_coverage_epoch_installation WHERE singleton_id=1"
    ).fetchone()
    if (
        row is None
        or type(row[0]) is not int
        or row[0]
        != (
            3
            if legacy_v3 or authorized_legacy_v3
            else COVERAGE_EPOCH_SCHEMA_VERSION
        )
        or type(row[1]) is not str
        or row[1] != COVERAGE_EPOCH_RULE_VERSION
        or type(row[2]) is not int
        or row[2] <= 0
        or not _valid_hash(row[3])
        or row[3]
        != _coverage_epoch_catalog_sha256_for_objects(
            connection,
            (
                _LEGACY_V3_OBJECTS
                if legacy_v3 or authorized_legacy_v3
                else _OBJECTS
            ),
        )
    ):
        raise RuntimeError("coverage epoch installation identity is inconsistent")
    if validate_graph:
        validate_coverage_epoch_graph(connection)
    if legacy_v3:
        return "PRE_TERMINAL_RECEIPT"
    if authorized_legacy_v3:
        return "AUTHORIZED_LEGACY_V3"
    return "CURRENT"


def validate_pre_coverage_epoch_review(connection: sqlite3.Connection) -> None:
    if coverage_epoch_schema_status(connection) != "PRE_EPOCH":
        raise RuntimeError("coverage epoch pre-install validation requires PRE_EPOCH")
    for strategy_id in _STRATEGIES:
        table = f"{strategy_id.lower()}_history_coverage"
        for row in connection.execute(
            f"SELECT strategy_id,symbol,source_start_time_ms,"
            f"covered_through_time_ms,source_sha256 FROM {table}"
        ):
            if not _valid_legacy_row(tuple(row), strategy_id):
                raise RuntimeError("legacy history coverage row is invalid")


def _valid_hash(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _valid_legacy_row(row: tuple[Any, ...], strategy_id: str) -> bool:
    return (
        len(row) == 5
        and row[0] == strategy_id
        and type(row[1]) is str
        and 5 <= len(row[1]) <= 32
        and type(row[2]) is int
        and row[2] > 0
        and type(row[3]) is int
        and row[3] >= row[2]
        and _valid_hash(row[4])
    )


def install_coverage_epoch_schema(
    connection: sqlite3.Connection,
    installed_at: str,
) -> None:
    status = coverage_epoch_schema_status(connection)
    if status == "PRE_TERMINAL_RECEIPT":
        _upgrade_coverage_epoch_schema_v3(connection, installed_at)
        return
    validate_pre_coverage_epoch_review(connection)
    connection.execute(EPOCH_TABLE_SQL)
    connection.execute(HEAD_TABLE_SQL)
    connection.execute(INSTALLATION_TABLE_SQL)
    connection.execute(PUBLICATION_TABLE_SQL)
    connection.execute(N19_TERMINAL_PUBLICATION_TABLE_SQL)
    for sql in INDEX_SQL.values():
        connection.execute(sql)
    for sql in TRIGGER_SQL.values():
        connection.execute(sql)
    for strategy_id in _STRATEGIES:
        table = f"{strategy_id.lower()}_history_coverage"
        rows = connection.execute(
            f"SELECT strategy_id,symbol,source_start_time_ms,"
            f"covered_through_time_ms,source_sha256 FROM {table} "
            "ORDER BY symbol"
        ).fetchall()
        for raw in rows:
            row = tuple(raw)
            if not _valid_legacy_row(row, strategy_id):
                raise RuntimeError("legacy history coverage changed during install")
            chain_sha = coverage_epoch_chain_sha256(
                strategy_id, row[1], 1, row[2], row[3], row[4],
                None, None, None, None,
            )
            connection.execute(
                "INSERT INTO history_coverage_epoch_chain VALUES "
                "(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    strategy_id, row[1], 1, row[2], row[3], row[4],
                    None, None, None, chain_sha, None, installed_at,
                ),
            )
            connection.execute(
                "INSERT INTO history_coverage_epoch_heads VALUES "
                "(?,?,?,?,?,?,?,?,?,?)",
                (
                    strategy_id, row[1], 1, row[2], row[3], row[4],
                    chain_sha, 0, None, installed_at,
                ),
            )
    schema_version = connection.execute("PRAGMA schema_version").fetchone()[0]
    catalog_sha256 = coverage_epoch_catalog_sha256(connection)
    connection.execute(
        "INSERT INTO history_coverage_epoch_installation "
        "VALUES (1,4,?,?,?,?)",
        (
            COVERAGE_EPOCH_RULE_VERSION,
            schema_version,
            catalog_sha256,
            installed_at,
        ),
    )
    if coverage_epoch_schema_status(connection) != "CURRENT":
        raise RuntimeError("coverage epoch installation did not complete")


def install_authorized_legacy_v3_terminal_objects(
    connection: sqlite3.Connection,
) -> None:
    """Add only future terminal-receipt objects without rewriting V3 receipts."""

    if coverage_epoch_schema_status(connection) != "PRE_TERMINAL_RECEIPT":
        raise RuntimeError(
            "authorized legacy terminal object baseline is invalid"
        )
    connection.execute(N19_TERMINAL_PUBLICATION_TABLE_SQL)
    connection.execute(INDEX_SQL["idx_history_coverage_n19_terminal_owner"])
    connection.execute(INDEX_SQL["idx_history_coverage_n19_terminal_identity"])
    for name in (
        "trg_history_coverage_n19_terminal_insert_authorized",
        "trg_history_coverage_n19_terminal_no_replace",
        "trg_history_coverage_n19_terminal_no_update",
        "trg_history_coverage_n19_terminal_no_delete",
    ):
        connection.execute(TRIGGER_SQL[name])
    if coverage_epoch_schema_status(
        connection, validate_graph=False
    ) != "AUTHORIZED_LEGACY_V3":
        raise RuntimeError(
            "authorized legacy terminal object installation did not complete"
        )


def _upgrade_coverage_epoch_schema_v3(
    connection: sqlite3.Connection,
    installed_at: str,
) -> None:
    if coverage_epoch_schema_status(connection) != "PRE_TERMINAL_RECEIPT":
        raise RuntimeError("coverage epoch V3 upgrade baseline is invalid")
    if connection.execute(
        "SELECT 1 FROM n19_staircase_states "
        "WHERE strategy_id='N19' AND stage='MISSED' "
        "AND reason='N19_HISTORICAL_ENTRY_MISSED' LIMIT 1"
    ).fetchone() is not None:
        # V3 has no durable terminal-publication binding.  Once the ordinary
        # source batch has been retained away, the original batch manifest
        # and coverage receipt cannot be reconstructed from the terminal
        # state alone.  Refuse the explicit upgrade before its first DDL
        # instead of silently blessing an unproved historical transition.
        raise RuntimeError(
            "coverage epoch V3 N19 terminal publication proof is unavailable"
        )
    for name in (
        "trg_history_coverage_receipt_insert_authorized",
        "trg_history_coverage_receipt_no_replace",
        "trg_history_coverage_receipt_no_update",
        "trg_history_coverage_receipt_no_delete",
    ):
        connection.execute(f'DROP TRIGGER "{name}"')
    connection.execute("DROP INDEX idx_history_coverage_receipt_owner")
    connection.execute(
        "ALTER TABLE history_coverage_publication_receipts "
        "RENAME TO history_coverage_publication_receipts_v3"
    )
    connection.execute(PUBLICATION_TABLE_SQL)
    connection.execute(
        "INSERT INTO history_coverage_publication_receipts "
        "(source_scan_id,strategy_id,symbol,source_start_time_ms,"
        "covered_through_time_ms,source_sha256,result_epoch_ordinal,"
        "result_epoch_start_time_ms,result_chain_head_sha256,"
        "publication_ordinal,previous_receipt_sha256,batch_expected_count,"
        "batch_manifest_sha256,terminal_binding_sha256,receipt_sha256,"
        "published_at) "
        "SELECT source_scan_id,strategy_id,symbol,source_start_time_ms,"
        "covered_through_time_ms,source_sha256,result_epoch_ordinal,"
        "result_epoch_start_time_ms,result_chain_head_sha256,"
        "publication_ordinal,previous_receipt_sha256,batch_expected_count,"
        "batch_manifest_sha256,NULL,receipt_sha256,published_at "
        "FROM history_coverage_publication_receipts_v3"
    )
    connection.execute("DROP TABLE history_coverage_publication_receipts_v3")
    connection.execute(INDEX_SQL["idx_history_coverage_receipt_owner"])
    for name in (
        "trg_history_coverage_receipt_insert_authorized",
        "trg_history_coverage_receipt_no_replace",
        "trg_history_coverage_receipt_no_update",
        "trg_history_coverage_receipt_no_delete",
    ):
        connection.execute(TRIGGER_SQL[name])
    connection.execute(N19_TERMINAL_PUBLICATION_TABLE_SQL)
    connection.execute(INDEX_SQL["idx_history_coverage_n19_terminal_owner"])
    connection.execute(INDEX_SQL["idx_history_coverage_n19_terminal_identity"])
    for name in (
        "trg_history_coverage_n19_terminal_insert_authorized",
        "trg_history_coverage_n19_terminal_no_replace",
        "trg_history_coverage_n19_terminal_no_update",
        "trg_history_coverage_n19_terminal_no_delete",
    ):
        connection.execute(TRIGGER_SQL[name])
    for name in (
        "trg_history_coverage_installation_no_update",
        "trg_history_coverage_installation_no_delete",
        "trg_history_coverage_installation_no_replace",
        "trg_history_coverage_installation_insert_authorized",
    ):
        connection.execute(f'DROP TRIGGER "{name}"')
    connection.execute("DROP TABLE history_coverage_epoch_installation")
    connection.execute(INSTALLATION_TABLE_SQL)
    for name in (
        "trg_history_coverage_installation_no_update",
        "trg_history_coverage_installation_no_delete",
        "trg_history_coverage_installation_no_replace",
        "trg_history_coverage_installation_insert_authorized",
    ):
        connection.execute(TRIGGER_SQL[name])
    schema_version = connection.execute("PRAGMA schema_version").fetchone()[0]
    catalog_sha256 = coverage_epoch_catalog_sha256(connection)
    connection.execute(
        "INSERT INTO history_coverage_epoch_installation "
        "VALUES (1,4,?,?,?,?)",
        (
            COVERAGE_EPOCH_RULE_VERSION,
            schema_version,
            catalog_sha256,
            installed_at,
        ),
    )
    if coverage_epoch_schema_status(connection) != "CURRENT":
        raise RuntimeError("coverage epoch V3 upgrade did not complete")


def attest_coverage_epoch_owner(
    connection: sqlite3.Connection,
    strategy_id: str,
    symbol: str,
    *,
    allow_absent: bool = False,
) -> tuple[tuple[Any, ...], tuple[Any, ...] | None, tuple[Any, ...]] | None:
    """Attest one current owner without walking its permanent receipt history."""
    if strategy_id not in _STRATEGIES or type(symbol) is not str:
        raise RuntimeError("coverage epoch owner identity is invalid")
    head_row = connection.execute(
        "SELECT epoch_ordinal,epoch_start_time_ms,covered_through_time_ms,"
        "source_sha256,chain_head_sha256,publication_count,"
        "latest_receipt_sha256,updated_at "
        "FROM history_coverage_epoch_heads "
        "WHERE strategy_id=? AND symbol=?",
        (strategy_id, symbol),
    ).fetchone()
    if head_row is None:
        if allow_absent:
            return None
        raise RuntimeError("coverage epoch owner head is missing")
    head = tuple(head_row)
    if (
        len(head) != 8
        or type(head[0]) is not int
        or head[0] <= 0
        or type(head[1]) is not int
        or head[1] <= 0
        or type(head[2]) is not int
        or head[2] < head[1]
        or not _valid_hash(head[3])
        or not _valid_hash(head[4])
        or type(head[5]) is not int
        or head[5] < 0
        or (head[6] is not None and not _valid_hash(head[6]))
        or ((head[5] == 0) != (head[6] is None))
        or type(head[7]) is not str
        or not head[7]
    ):
        raise RuntimeError("coverage epoch owner head is invalid")
    chain_row = connection.execute(
        "SELECT source_start_time_ms,initial_covered_through_time_ms,"
        "initial_source_sha256,prior_covered_through_time_ms,"
        "prior_source_sha256,previous_chain_sha256,chain_sha256,"
        "source_scan_id,created_at "
        "FROM history_coverage_epoch_chain "
        "WHERE strategy_id=? AND symbol=? AND epoch_ordinal=?",
        (strategy_id, symbol, head[0]),
    ).fetchone()
    if chain_row is None:
        raise RuntimeError("coverage epoch owner chain tip is missing")
    chain = tuple(chain_row)
    if (
        len(chain) != 9
        or type(chain[0]) is not int
        or chain[0] <= 0
        or type(chain[1]) is not int
        or chain[1] < chain[0]
        or not _valid_hash(chain[2])
        or not _valid_hash(chain[6])
        or chain[6] != head[4]
        or chain[0] != head[1]
        or type(chain[8]) is not str
        or not chain[8]
        or chain[6]
        != coverage_epoch_chain_sha256(
            strategy_id,
            symbol,
            head[0],
            chain[0],
            chain[1],
            chain[2],
            chain[3],
            chain[4],
            chain[5],
            chain[7],
        )
    ):
        raise RuntimeError("coverage epoch owner chain tip is invalid")
    legacy_table = f"{strategy_id.lower()}_history_coverage"
    legacy = connection.execute(
        f"SELECT source_start_time_ms,covered_through_time_ms,"
        f"source_sha256,updated_at FROM {legacy_table} "
        "WHERE strategy_id=? AND symbol=?",
        (strategy_id, symbol),
    ).fetchone()
    if (
        legacy is None
        or type(legacy[0]) is not int
        or legacy[0] > head[1]
        or tuple(legacy[1:3]) != head[2:4]
        or type(legacy[3]) is not str
        or not legacy[3]
    ):
        raise RuntimeError("coverage epoch owner mirror is invalid")
    latest: tuple[Any, ...] | None = None
    if head[5] == 0:
        if (
            head[0] != 1
            or any(value is not None for value in chain[3:6])
            or chain[7] is not None
            or head[1:5] != (chain[0], chain[1], chain[2], chain[6])
        ):
            raise RuntimeError("coverage epoch installation owner is invalid")
    else:
        publication_has_terminal_binding = any(
            row[1] == "terminal_binding_sha256"
            for row in connection.execute(
                "PRAGMA table_xinfo(history_coverage_publication_receipts)"
            )
        )
        latest_row = connection.execute(
            "SELECT r.source_scan_id,r.source_start_time_ms,"
            "r.covered_through_time_ms,r.source_sha256,"
            "r.result_epoch_ordinal,r.result_epoch_start_time_ms,"
            "r.result_chain_head_sha256,r.publication_ordinal,"
            "r.previous_receipt_sha256,r.batch_expected_count,"
            "r.batch_manifest_sha256,"
            + (
                "r.terminal_binding_sha256,"
                if publication_has_terminal_binding
                else "NULL AS terminal_binding_sha256,"
            )
            +
            "r.receipt_sha256,s.id "
            "FROM history_coverage_publication_receipts AS r "
            "LEFT JOIN scans AS s ON s.id=r.source_scan_id "
            "WHERE r.strategy_id=? AND r.symbol=? AND r.receipt_sha256=?",
            (strategy_id, symbol, head[6]),
        ).fetchone()
        if latest_row is None:
            raise RuntimeError("coverage epoch latest receipt is missing")
        latest = tuple(latest_row)
        if (
            len(latest) != 14
            or type(latest[0]) is not int
            or latest[0] <= 0
            or type(latest[1]) is not int
            or latest[1] <= 0
            or type(latest[2]) is not int
            or latest[2] < latest[1]
            or not _valid_hash(latest[3])
            or type(latest[4]) is not int
            or type(latest[5]) is not int
            or not _valid_hash(latest[6])
            or type(latest[7]) is not int
            or latest[7] != head[5]
            or (
                latest[8] is not None
                and not _valid_hash(latest[8])
            )
            or type(latest[9]) is not int
            or latest[9] <= 0
            or not _valid_hash(latest[10])
            or (
                latest[11] is not None
                and not _valid_hash(latest[11])
            )
            or not _valid_hash(latest[12])
            or latest[13] != latest[0]
            or latest[2:7]
            != (head[2], head[3], head[0], head[1], head[4])
            or latest[12]
            != coverage_publication_receipt_sha256(
                latest[0],
                strategy_id,
                symbol,
                latest[1],
                latest[2],
                latest[3],
                latest[4],
                latest[5],
                latest[6],
                latest[7],
                latest[8],
                latest[9],
                latest[10],
                latest[11],
            )
        ):
            raise RuntimeError("coverage epoch latest receipt is invalid")
    return head, latest, chain


def validate_coverage_epoch_graph(connection: sqlite3.Connection) -> None:
    publication_has_terminal_binding = any(
        row[1] == "terminal_binding_sha256"
        for row in connection.execute(
            "PRAGMA table_xinfo(history_coverage_publication_receipts)"
        )
    )
    receipt_query = (
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
        + "receipt_sha256 "
        "FROM history_coverage_publication_receipts "
        "ORDER BY strategy_id,symbol,source_scan_id"
    )
    for raw_receipt in connection.execute(receipt_query):
        receipt = tuple(raw_receipt)
        if (
            len(receipt) != 15
            or type(receipt[0]) is not int
            or receipt[0] <= 0
            or receipt[1] not in _STRATEGIES
            or type(receipt[2]) is not str
            or not 5 <= len(receipt[2]) <= 32
            or type(receipt[3]) is not int
            or receipt[3] <= 0
            or type(receipt[4]) is not int
            or receipt[4] < receipt[3]
            or not _valid_hash(receipt[5])
            or type(receipt[6]) is not int
            or receipt[6] <= 0
            or type(receipt[7]) is not int
            or receipt[7] <= 0
            or not _valid_hash(receipt[8])
            or type(receipt[9]) is not int
            or receipt[9] <= 0
            or (
                receipt[10] is not None
                and not _valid_hash(receipt[10])
            )
            or type(receipt[11]) is not int
            or receipt[11] <= 0
            or not _valid_hash(receipt[12])
            or (
                receipt[13] is not None
                and not _valid_hash(receipt[13])
            )
            or not _valid_hash(receipt[14])
            or (
                (receipt[9] == 1 and receipt[10] is not None)
                or (receipt[9] > 1 and receipt[10] is None)
            )
            or receipt[14]
            != coverage_publication_receipt_sha256(
                *receipt[:13], receipt[13]
            )
        ):
            raise RuntimeError("coverage publication receipt is invalid")
    if connection.execute(
        "SELECT 1 FROM history_coverage_publication_receipts "
        "GROUP BY source_scan_id,strategy_id,symbol HAVING count(*) != 1 "
        "LIMIT 1"
    ).fetchone() is not None:
        raise RuntimeError("coverage publication receipt owner is duplicated")
    if connection.execute(
        "SELECT 1 FROM history_coverage_publication_receipts AS r "
        "LEFT JOIN scans AS s ON s.id=r.source_scan_id "
        "WHERE s.id IS NULL LIMIT 1"
    ).fetchone() is not None:
        raise RuntimeError("coverage publication receipt owner is missing")

    terminal_table_present = connection.execute(
        "SELECT 1 FROM sqlite_schema "
        "WHERE type='table' AND name='history_coverage_n19_terminal_receipts'"
    ).fetchone() == (1,)
    terminal_rows = (
        connection.execute(
            "SELECT source_scan_id,strategy_id,symbol,family_id,structure_id,"
            "terminal_evidence_json,terminal_evidence_sha256,"
            "batch_expected_count,batch_manifest_sha256,"
            "coverage_receipt_sha256,result_epoch_ordinal,"
            "result_epoch_start_time_ms,result_chain_head_sha256,"
            "receipt_sha256,published_at "
            "FROM history_coverage_n19_terminal_receipts "
            "ORDER BY source_scan_id,strategy_id,symbol"
        ).fetchall()
        if terminal_table_present
        else ()
    )
    binding_overlay_present = connection.execute(
        "SELECT 1 FROM sqlite_schema WHERE type='table' "
        "AND name='history_coverage_authorized_terminal_bindings'"
    ).fetchone() == (1,)
    binding_overlay_rows = (
        connection.execute(
            "SELECT coverage_receipt_sha256,source_scan_id,strategy_id,"
            "symbol,terminal_binding_sha256,terminal_receipt_sha256 "
            "FROM history_coverage_authorized_terminal_bindings "
            "ORDER BY source_scan_id,strategy_id,symbol"
        ).fetchall()
        if binding_overlay_present
        else ()
    )
    binding_overlays: dict[str, tuple[Any, ...]] = {}
    for raw_overlay in binding_overlay_rows:
        overlay = tuple(raw_overlay)
        if (
            len(overlay) != 6
            or not _valid_hash(overlay[0])
            or type(overlay[1]) is not int
            or overlay[1] <= 0
            or overlay[2] != "N19"
            or type(overlay[3]) is not str
            or not 5 <= len(overlay[3]) <= 32
            or not _valid_hash(overlay[4])
            or not _valid_hash(overlay[5])
            or overlay[0] in binding_overlays
        ):
            raise RuntimeError(
                "authorized terminal binding overlay is invalid"
            )
        binding_overlays[overlay[0]] = overlay
    from .n19_analyzer import decode_n19_state_evidence

    terminals_by_coverage: dict[str, tuple[Any, ...]] = {}
    terminal_identities: set[tuple[str, str, str]] = set()
    for raw_terminal in terminal_rows:
        terminal = tuple(raw_terminal)
        if (
            len(terminal) != 15
            or type(terminal[0]) is not int
            or terminal[0] <= 0
            or terminal[1] != "N19"
            or type(terminal[2]) is not str
            or not 5 <= len(terminal[2]) <= 32
            or type(terminal[3]) is not str
            or len(terminal[3]) != 24
            or any(character not in "0123456789abcdef" for character in terminal[3])
            or type(terminal[4]) is not str
            or len(terminal[4]) != 24
            or any(character not in "0123456789abcdef" for character in terminal[4])
            or type(terminal[5]) is not str
            or not terminal[5]
            or not _valid_hash(terminal[6])
            or type(terminal[7]) is not int
            or terminal[7] <= 0
            or not _valid_hash(terminal[8])
            or not _valid_hash(terminal[9])
            or type(terminal[10]) is not int
            or terminal[10] <= 0
            or type(terminal[11]) is not int
            or terminal[11] <= 0
            or not _valid_hash(terminal[12])
            or not _valid_hash(terminal[13])
            or type(terminal[14]) is not str
            or not terminal[14]
            or terminal[13]
            != n19_terminal_publication_receipt_sha256(
                terminal[0],
                terminal[2],
                terminal[3],
                terminal[4],
                terminal[5],
                terminal[6],
                terminal[7],
                terminal[8],
                terminal[9],
                terminal[10],
                terminal[11],
                terminal[12],
            )
        ):
            raise RuntimeError("N19 terminal publication receipt is invalid")
        try:
            record = decode_n19_state_evidence(
                terminal[5],
                expected_symbol=terminal[2],
            )
        except Exception as exc:
            raise RuntimeError(
                "N19 terminal publication evidence is invalid"
            ) from exc
        if (
            record.family_id != terminal[3]
            or record.structure_id != terminal[4]
            or record.evidence_sha256 != terminal[6]
            or record.stage != "MISSED"
            or record.reason != "N19_HISTORICAL_ENTRY_MISSED"
            or record.reset_after_time_ms is not None
        ):
            raise RuntimeError(
                "N19 terminal publication evidence conflicts"
            )
        terminal_identity = (
            terminal[1],
            terminal[2],
            terminal[3],
        )
        if terminal_identity in terminal_identities:
            raise RuntimeError(
                "N19 terminal publication identity is duplicated"
            )
        terminal_identities.add(terminal_identity)
        coverage = connection.execute(
            "SELECT source_scan_id,strategy_id,symbol,"
            "source_start_time_ms,covered_through_time_ms,source_sha256,"
            "result_epoch_ordinal,result_epoch_start_time_ms,"
            "result_chain_head_sha256,publication_ordinal,"
            "previous_receipt_sha256,batch_expected_count,"
            "batch_manifest_sha256,"
            + (
                "terminal_binding_sha256,"
                if publication_has_terminal_binding
                else "NULL AS terminal_binding_sha256,"
            )
            + "receipt_sha256 "
            "FROM history_coverage_publication_receipts "
            "WHERE source_scan_id=? AND strategy_id='N19' AND symbol=?",
            (terminal[0], terminal[2]),
        ).fetchone()
        if coverage is not None:
            coverage = tuple(coverage)
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
        overlay = binding_overlays.get(terminal[9])
        effective_binding = coverage[13] if coverage is not None else None
        if overlay is not None:
            if (
                overlay[1:4]
                != (terminal[0], terminal[1], terminal[2])
                or overlay[4] != expected_binding
                or overlay[5] != terminal[13]
            ):
                raise RuntimeError(
                    "authorized terminal binding overlay conflicts"
                )
            effective_binding = overlay[4]
        if (
            coverage is None
            or coverage[11] != terminal[7]
            or coverage[12] != terminal[8]
            or effective_binding != expected_binding
            or coverage[14] != terminal[9]
            or coverage[6] != terminal[10]
            or coverage[7] != terminal[11]
            or coverage[8] != terminal[12]
            or terminal[9] in terminals_by_coverage
        ):
            raise RuntimeError(
                "N19 terminal publication coverage receipt conflicts"
            )
        terminals_by_coverage[terminal[9]] = terminal
        state_row = connection.execute(
            "SELECT family_id,structure_id,evidence_json,evidence_sha256 "
            "FROM n19_staircase_states "
            "WHERE strategy_id='N19' AND family_id=?",
            (terminal[3],),
        ).fetchone()
        if state_row is None:
            raise RuntimeError("N19 terminal publication state is missing")
        try:
            current = decode_n19_state_evidence(
                state_row[2],
                expected_symbol=terminal[2],
            )
        except Exception as exc:
            raise RuntimeError(
                "N19 terminal publication current state is invalid"
            ) from exc
        if (
            tuple(state_row[:2]) != (terminal[3], terminal[4])
            or state_row[3] != current.evidence_sha256
            or current.family_id != terminal[3]
            or current.structure_id != terminal[4]
            or current.stage != record.stage
            or current.reason != record.reason
            or current.evidence.get("structure")
            != record.evidence.get("structure")
            or current.evidence.get("terminal_cutoff_time_ms")
            != record.evidence.get("terminal_cutoff_time_ms")
            or (
                current.evidence_sha256 != terminal[6]
                and (
                    type(current.reset_after_time_ms) is not int
                    or current.reset_after_time_ms
                    <= record.evidence["terminal_cutoff_time_ms"]
                )
            )
        ):
            raise RuntimeError("N19 terminal publication state conflicts")
    if terminal_table_present:
        legacy_witness_keys: set[tuple[str, str, str]] = set()
        if connection.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='table' "
            "AND name='history_coverage_n19_legacy_unbound_witnesses'"
        ).fetchone() == (1,):
            legacy_witness_keys = {
                (row[0], row[1], row[2])
                for row in connection.execute(
                    "SELECT symbol,family_id,structure_id FROM "
                    "history_coverage_n19_legacy_unbound_witnesses"
                )
            }
        proved_state_keys = {
            (row[2], row[3], row[4]) for row in terminal_rows
        }
        historical_state_rows = connection.execute(
            "SELECT symbol,family_id,structure_id,evidence_json,evidence_sha256 "
            "FROM n19_staircase_states "
            "WHERE strategy_id='N19' "
            "AND reason='N19_HISTORICAL_ENTRY_MISSED'"
        ).fetchall()
        for raw_state in historical_state_rows:
            state = tuple(raw_state)
            if (
                len(state) != 5
                or type(state[0]) is not str
                or type(state[1]) is not str
                or type(state[2]) is not str
                or type(state[3]) is not str
                or not _valid_hash(state[4])
            ):
                raise RuntimeError("N19 historical terminal state is invalid")
            try:
                record = decode_n19_state_evidence(
                    state[3], expected_symbol=state[0]
                )
            except Exception as exc:
                raise RuntimeError(
                    "N19 historical terminal state evidence is invalid"
                ) from exc
            key = tuple(state[:3])
            if (
                record.family_id != state[1]
                or record.structure_id != state[2]
                or record.evidence_sha256 != state[4]
                or record.stage != "MISSED"
                or record.reason != "N19_HISTORICAL_ENTRY_MISSED"
                or (
                    (key in proved_state_keys)
                    + (key in legacy_witness_keys)
                )
                != 1
            ):
                raise RuntimeError(
                    "N19 historical terminal state has no unique permanent proof"
                )
    for receipt in connection.execute(receipt_query):
        normalized = tuple(receipt)
        binding = normalized[13]
        sealed_terminal = terminals_by_coverage.get(normalized[14])
        overlay = binding_overlays.get(normalized[14])
        if (
            (overlay is None) != (sealed_terminal is None)
        ) or (
            binding is not None
            and (overlay is None or binding != overlay[4])
        ) or (
            sealed_terminal is None and binding is not None
        ):
            raise RuntimeError(
                "coverage terminal publication binding is incomplete"
            )
    if set(binding_overlays) != set(terminals_by_coverage):
        raise RuntimeError(
            "authorized terminal binding overlay set conflicts"
        )
    if not publication_has_terminal_binding:
        for receipt in connection.execute(receipt_query):
            if tuple(receipt)[13] is not None:
                raise RuntimeError(
                    "legacy coverage receipt has an unexpected terminal binding"
                )

    heads = connection.execute(
        "SELECT strategy_id,symbol,epoch_ordinal,epoch_start_time_ms,"
        "covered_through_time_ms,source_sha256,chain_head_sha256,"
        "publication_count,latest_receipt_sha256 "
        "FROM history_coverage_epoch_heads ORDER BY strategy_id,symbol"
    ).fetchall()
    head_keys: set[tuple[str, str]] = set()
    for raw_head in heads:
        head = tuple(raw_head)
        if (
            len(head) != 9
            or head[0] not in _STRATEGIES
            or type(head[1]) is not str
            or type(head[2]) is not int
            or head[2] <= 0
            or type(head[3]) is not int
            or head[3] <= 0
            or type(head[4]) is not int
            or head[4] < head[3]
            or not _valid_hash(head[5])
            or not _valid_hash(head[6])
            or type(head[7]) is not int
            or head[7] < 0
            or (
                head[8] is not None
                and not _valid_hash(head[8])
            )
            or (
                (head[7] == 0 and head[8] is not None)
                or (head[7] > 0 and head[8] is None)
            )
            or (head[0], head[1]) in head_keys
        ):
            raise RuntimeError("coverage epoch head is invalid")
        head_keys.add((head[0], head[1]))
        rows = connection.execute(
            "SELECT epoch_ordinal,source_start_time_ms,"
            "initial_covered_through_time_ms,initial_source_sha256,"
            "prior_covered_through_time_ms,prior_source_sha256,"
            "previous_chain_sha256,chain_sha256,source_scan_id "
            "FROM history_coverage_epoch_chain "
            "WHERE strategy_id=? AND symbol=? ORDER BY epoch_ordinal",
            (head[0], head[1]),
        )
        previous: tuple[Any, ...] | None = None
        chain_count = 0
        for expected_ordinal, raw_row in enumerate(rows, 1):
            chain_count = expected_ordinal
            row = tuple(raw_row)
            if (
                len(row) != 9
                or type(row[0]) is not int
                or row[0] != expected_ordinal
                or type(row[1]) is not int
                or row[1] <= 0
                or type(row[2]) is not int
                or row[2] < row[1]
                or not _valid_hash(row[3])
                or not _valid_hash(row[7])
            ):
                raise RuntimeError("coverage epoch row is invalid")
            if expected_ordinal == 1:
                if any(value is not None for value in row[4:7]):
                    raise RuntimeError("first coverage epoch ancestry is invalid")
            else:
                if (
                    previous is None
                    or type(row[4]) is not int
                    or row[4] < previous[2]
                    or not _valid_hash(row[5])
                    or row[6] != previous[7]
                    or type(row[8]) is not int
                    or row[8] <= 0
                    or row[1] <= row[4] + 900_000
                ):
                    raise RuntimeError("coverage epoch ancestry is invalid")
            expected_sha = coverage_epoch_chain_sha256(
                head[0], head[1], row[0], row[1], row[2], row[3],
                row[4], row[5], row[6], row[8],
            )
            if row[7] != expected_sha:
                raise RuntimeError("coverage epoch chain hash conflicts")
            if row[8] is not None:
                receipt = connection.execute(
                    "SELECT result_epoch_ordinal,"
                    "result_epoch_start_time_ms,result_chain_head_sha256 "
                    "FROM history_coverage_publication_receipts "
                    "WHERE source_scan_id=? AND strategy_id=? AND symbol=?",
                    (row[8], head[0], head[1]),
                ).fetchone()
                if receipt is None or tuple(receipt) != (
                    row[0], row[1], row[7]
                ):
                    raise RuntimeError(
                        "coverage epoch publication receipt conflicts"
                    )
            previous = row
        if chain_count != head[2]:
            raise RuntimeError("coverage epoch chain length conflicts")
        if (
            previous is None
            or head[3] != previous[1]
            or head[4] < previous[2]
            or head[6] != previous[7]
        ):
            raise RuntimeError("coverage epoch head does not match its chain")
        legacy_table = f"{head[0].lower()}_history_coverage"
        legacy = connection.execute(
            f"SELECT source_start_time_ms,covered_through_time_ms,source_sha256 "
            f"FROM {legacy_table} WHERE strategy_id=? AND symbol=?",
            (head[0], head[1]),
        ).fetchone()
        if (
            legacy is None
            or type(legacy[0]) is not int
            or legacy[0] > head[3]
            or tuple(legacy[1:]) != (head[4], head[5])
        ):
            raise RuntimeError("legacy coverage mirror conflicts with epoch head")
        key_receipts = connection.execute(
            "SELECT source_scan_id,strategy_id,symbol,"
            "source_start_time_ms,covered_through_time_ms,source_sha256,"
            "result_epoch_ordinal,result_epoch_start_time_ms,"
            "result_chain_head_sha256,publication_ordinal,"
            "previous_receipt_sha256,batch_expected_count,"
            "batch_manifest_sha256,"
            + (
                "terminal_binding_sha256,"
                if publication_has_terminal_binding
                else "NULL AS terminal_binding_sha256,"
            )
            + "receipt_sha256 "
            "FROM history_coverage_publication_receipts "
            "WHERE strategy_id=? AND symbol=? ORDER BY publication_ordinal",
            (head[0], head[1]),
        )
        prior_receipt: tuple[Any, ...] | None = None
        latest: tuple[Any, ...] | None = None
        receipt_count = 0
        for expected_publication_ordinal, raw_receipt in enumerate(
            key_receipts, 1
        ):
            receipt_count = expected_publication_ordinal
            receipt = tuple(raw_receipt)
            if (
                receipt[9] != expected_publication_ordinal
                or (
                    prior_receipt is None
                    and receipt[10] is not None
                )
                or (
                    prior_receipt is not None
                    and receipt[10] != prior_receipt[14]
                )
                or prior_receipt is not None
                and (
                    receipt[0] <= prior_receipt[0]
                    or receipt[3] < prior_receipt[3]
                    or receipt[4] < prior_receipt[4]
                )
            ):
                raise RuntimeError(
                    "coverage publication receipt order conflicts"
                )
            prior_receipt = receipt
            latest = receipt
        if latest is not None:
            if (
                receipt_count != head[7]
                or latest[14] != head[8]
                or latest[4] != head[4]
                or latest[5] != head[5]
                or latest[6] != head[2]
                or latest[7] != head[3]
                or latest[8] != head[6]
            ):
                raise RuntimeError(
                    "coverage epoch head has no latest publication receipt"
                )
        elif (
            previous is None
            or previous[8] is not None
            or head[7] != 0
            or head[8] is not None
            or head[2] != previous[0]
            or head[3] != previous[1]
            or head[4] != previous[2]
            or head[5] != previous[3]
            or head[6] != previous[7]
        ):
            raise RuntimeError(
                "coverage epoch runtime head has no publication receipt"
            )
    for strategy_id in _STRATEGIES:
        table = f"{strategy_id.lower()}_history_coverage"
        for row in connection.execute(
            f"SELECT strategy_id,symbol FROM {table}"
        ):
            if tuple(row) not in head_keys:
                raise RuntimeError("legacy coverage has no epoch head")
    chain_keys = {
        tuple(row)
        for row in connection.execute(
            "SELECT DISTINCT strategy_id,symbol "
            "FROM history_coverage_epoch_chain"
        )
    }
    if chain_keys != head_keys:
        raise RuntimeError("coverage epoch chain has an orphan identity")
    if connection.execute(
        "SELECT 1 FROM history_coverage_publication_receipts AS r "
        "LEFT JOIN history_coverage_epoch_heads AS h "
        "ON h.strategy_id=r.strategy_id AND h.symbol=r.symbol "
        "WHERE h.strategy_id IS NULL LIMIT 1"
    ).fetchone() is not None:
        raise RuntimeError("coverage publication receipt has no epoch head")
