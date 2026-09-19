"""Independent permanent N16 claim ledger.

The trading review database and this ledger are deliberately separate failure
domains.  The ledger is stored below the application's state directory and is
never cleared with the live-position state file.  It provides an indexed,
append-only identity for every committed N16 structure and a small two-phase
publication marker used by :class:`trading_bot.recorder.ReviewRecorder`.

This module does not attempt to protect against an actor that can coordinate a
forgery of both databases.  That stronger threat model needs an external/WORM
ledger.  Its contract is to detect a review-database-only rollback while this
state ledger remains genuine.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import stat
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence


N16_CLAIM_LEDGER_FILENAME = "n16_claim_ledger.sqlite3"
N16_CLAIM_LEDGER_SCHEMA_VERSION = 1
N16_CLAIM_LEDGER_APPLICATION_ID = 0x4E313643
N16_CLAIM_CHAIN_SEED = "0" * 64
N16_RULE_VERSION = "N16_V1"
N16_GUARD_SHA256 = (
    "d262585be43bf3dc7137d98e84770d672b8d82d40c369724a1ac70e211b52332"
)

_N16_LEDGER_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
_N16_LEDGER_CREATE_CWD_LOCK = threading.RLock()
_N16_LEDGER_RUNTIME_CONNECTION_LOCK = threading.RLock()
_N16_LEDGER_SCHEMA_REFERENCE_LOCK = threading.Lock()
_N16_LEDGER_SCHEMA_REFERENCE = None

_LEGACY_WITNESS_MIRROR_INSTALLATION_SQL = """
CREATE TABLE n16_legacy_witness_mirror_installation (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    schema_version INTEGER NOT NULL CHECK(
        typeof(schema_version) = 'integer' AND schema_version = 2
    ),
    rule_version TEXT NOT NULL CHECK(
        rule_version = 'AUTHORIZED_LEGACY_V3_UNBOUND_MIRROR_V2'
    ),
    installed_at TEXT NOT NULL
)
""".strip()

_LEGACY_WITNESS_MIRROR_SQL = """
CREATE TABLE n16_legacy_witness_mirror (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    phase TEXT NOT NULL CHECK(phase IN ('EMPTY','PREPARED','COMMITTED')),
    review_plan_sha256 TEXT,
    witness_sha256 TEXT,
    pre_review_canonical_sha256 TEXT,
    authorization_sha256 TEXT,
    ledger_uuid TEXT,
    base_review_snapshot_sha256 TEXT,
    target_review_snapshot_sha256 TEXT,
    base_catalog_sha256 TEXT,
    target_catalog_sha256 TEXT,
    prepared_at TEXT,
    committed_at TEXT,
    CHECK(
        (phase = 'EMPTY' AND review_plan_sha256 IS NULL
         AND witness_sha256 IS NULL
         AND pre_review_canonical_sha256 IS NULL
         AND authorization_sha256 IS NULL
         AND ledger_uuid IS NULL
         AND base_review_snapshot_sha256 IS NULL
         AND target_review_snapshot_sha256 IS NULL
         AND base_catalog_sha256 IS NULL
         AND target_catalog_sha256 IS NULL
         AND prepared_at IS NULL AND committed_at IS NULL)
        OR
        (phase = 'PREPARED'
         AND length(review_plan_sha256) = 64
         AND review_plan_sha256 NOT GLOB '*[^0-9a-f]*'
         AND length(witness_sha256) = 64
         AND witness_sha256 NOT GLOB '*[^0-9a-f]*'
         AND length(pre_review_canonical_sha256) = 64
         AND pre_review_canonical_sha256 NOT GLOB '*[^0-9a-f]*'
         AND authorization_sha256 =
           '2dd9bf0e9a19d4464ff53ddb9bba35a850306499d12d88a34257541dcd0b04a1'
         AND length(ledger_uuid) = 64
         AND ledger_uuid NOT GLOB '*[^0-9a-f]*'
         AND length(base_review_snapshot_sha256) = 64
         AND base_review_snapshot_sha256 NOT GLOB '*[^0-9a-f]*'
         AND length(target_review_snapshot_sha256) = 64
         AND target_review_snapshot_sha256 NOT GLOB '*[^0-9a-f]*'
         AND length(base_catalog_sha256) = 64
         AND base_catalog_sha256 NOT GLOB '*[^0-9a-f]*'
         AND length(target_catalog_sha256) = 64
         AND target_catalog_sha256 NOT GLOB '*[^0-9a-f]*'
         AND prepared_at IS NOT NULL AND committed_at IS NULL)
        OR
        (phase = 'COMMITTED'
         AND length(review_plan_sha256) = 64
         AND review_plan_sha256 NOT GLOB '*[^0-9a-f]*'
         AND length(witness_sha256) = 64
         AND witness_sha256 NOT GLOB '*[^0-9a-f]*'
         AND length(pre_review_canonical_sha256) = 64
         AND pre_review_canonical_sha256 NOT GLOB '*[^0-9a-f]*'
         AND authorization_sha256 =
           '2dd9bf0e9a19d4464ff53ddb9bba35a850306499d12d88a34257541dcd0b04a1'
         AND length(ledger_uuid) = 64
         AND ledger_uuid NOT GLOB '*[^0-9a-f]*'
         AND length(base_review_snapshot_sha256) = 64
         AND base_review_snapshot_sha256 NOT GLOB '*[^0-9a-f]*'
         AND length(target_review_snapshot_sha256) = 64
         AND target_review_snapshot_sha256 NOT GLOB '*[^0-9a-f]*'
         AND length(base_catalog_sha256) = 64
         AND base_catalog_sha256 NOT GLOB '*[^0-9a-f]*'
         AND length(target_catalog_sha256) = 64
         AND target_catalog_sha256 NOT GLOB '*[^0-9a-f]*'
         AND prepared_at IS NOT NULL AND committed_at IS NOT NULL)
    )
)
""".strip()

_LEGACY_WITNESS_MIRROR_TRIGGER_SQL = {
    "trg_n16_legacy_witness_install_no_replace": """
CREATE TRIGGER trg_n16_legacy_witness_install_no_replace
BEFORE INSERT ON n16_legacy_witness_mirror_installation
WHEN EXISTS(
  SELECT 1 FROM n16_legacy_witness_mirror_installation WHERE singleton_id=1
)
BEGIN SELECT RAISE(ABORT, 'legacy witness mirror install replacement forbidden'); END
""".strip(),
    "trg_n16_legacy_witness_install_no_update": """
CREATE TRIGGER trg_n16_legacy_witness_install_no_update
BEFORE UPDATE ON n16_legacy_witness_mirror_installation
BEGIN SELECT RAISE(ABORT, 'legacy witness mirror installation immutable'); END
""".strip(),
    "trg_n16_legacy_witness_install_no_delete": """
CREATE TRIGGER trg_n16_legacy_witness_install_no_delete
BEFORE DELETE ON n16_legacy_witness_mirror_installation
BEGIN SELECT RAISE(ABORT, 'legacy witness mirror installation permanent'); END
""".strip(),
    "trg_n16_legacy_witness_mirror_no_replace": """
CREATE TRIGGER trg_n16_legacy_witness_mirror_no_replace
BEFORE INSERT ON n16_legacy_witness_mirror
WHEN EXISTS(SELECT 1 FROM n16_legacy_witness_mirror WHERE singleton_id=1)
BEGIN SELECT RAISE(ABORT, 'legacy witness mirror replacement forbidden'); END
""".strip(),
    "trg_n16_legacy_witness_mirror_transition": """
CREATE TRIGGER trg_n16_legacy_witness_mirror_transition
BEFORE UPDATE ON n16_legacy_witness_mirror
WHEN NOT (
  (OLD.phase='EMPTY' AND NEW.phase='PREPARED'
   AND NEW.review_plan_sha256 IS NOT NULL
   AND NEW.witness_sha256 IS NOT NULL
   AND NEW.pre_review_canonical_sha256 IS NOT NULL
   AND NEW.authorization_sha256 IS NOT NULL
   AND NEW.ledger_uuid IS NOT NULL
   AND NEW.base_review_snapshot_sha256 IS NOT NULL
   AND NEW.target_review_snapshot_sha256 IS NOT NULL
   AND NEW.base_catalog_sha256 IS NOT NULL
   AND NEW.target_catalog_sha256 IS NOT NULL
   AND NEW.prepared_at IS NOT NULL AND NEW.committed_at IS NULL)
  OR
  (OLD.phase='PREPARED' AND NEW.phase='EMPTY'
   AND NEW.review_plan_sha256 IS NULL AND NEW.witness_sha256 IS NULL
   AND NEW.pre_review_canonical_sha256 IS NULL
   AND NEW.authorization_sha256 IS NULL
   AND NEW.ledger_uuid IS NULL
   AND NEW.base_review_snapshot_sha256 IS NULL
   AND NEW.target_review_snapshot_sha256 IS NULL
   AND NEW.base_catalog_sha256 IS NULL
   AND NEW.target_catalog_sha256 IS NULL
   AND NEW.prepared_at IS NULL AND NEW.committed_at IS NULL)
  OR
  (OLD.phase='PREPARED' AND NEW.phase='COMMITTED'
   AND NEW.review_plan_sha256=OLD.review_plan_sha256
   AND NEW.witness_sha256=OLD.witness_sha256
   AND NEW.pre_review_canonical_sha256=OLD.pre_review_canonical_sha256
   AND NEW.authorization_sha256=OLD.authorization_sha256
   AND NEW.ledger_uuid=OLD.ledger_uuid
   AND NEW.base_review_snapshot_sha256=OLD.base_review_snapshot_sha256
   AND NEW.target_review_snapshot_sha256=OLD.target_review_snapshot_sha256
   AND NEW.base_catalog_sha256=OLD.base_catalog_sha256
   AND NEW.target_catalog_sha256=OLD.target_catalog_sha256
   AND NEW.prepared_at=OLD.prepared_at
   AND NEW.committed_at IS NOT NULL)
)
BEGIN SELECT RAISE(ABORT, 'legacy witness mirror transition invalid'); END
""".strip(),
    "trg_n16_legacy_witness_mirror_no_delete": """
CREATE TRIGGER trg_n16_legacy_witness_mirror_no_delete
BEFORE DELETE ON n16_legacy_witness_mirror
BEGIN SELECT RAISE(ABORT, 'legacy witness mirror permanent'); END
""".strip(),
}

_PROTECTED_GENERATION_HIGHWATER_INSTALLATION_SQL = """
CREATE TABLE n16_protected_generation_highwater_installation (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    schema_version INTEGER NOT NULL CHECK(
        typeof(schema_version) = 'integer' AND schema_version = 1
    ),
    rule_version TEXT NOT NULL CHECK(
        rule_version = 'N19_PROTECTED_GENERATION_LEDGER_V1'
    ),
    installed_at TEXT NOT NULL CHECK(length(installed_at) BETWEEN 1 AND 64)
)
""".strip()

_PROTECTED_GENERATION_HIGHWATER_SQL = """
CREATE TABLE n16_protected_generation_highwater (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    generation INTEGER NOT NULL CHECK(
        typeof(generation) = 'integer' AND generation >= 0
    ),
    review_schema_version INTEGER NOT NULL CHECK(
        typeof(review_schema_version) = 'integer'
        AND review_schema_version > 0
    ),
    family_catalog_sha256 TEXT NOT NULL CHECK(
        length(family_catalog_sha256) = 64
        AND family_catalog_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    updated_at TEXT NOT NULL CHECK(length(updated_at) BETWEEN 1 AND 64)
)
""".strip()

_PROTECTED_GENERATION_HIGHWATER_TRIGGER_SQL = {
    "trg_n16_protected_generation_install_no_replace": """
CREATE TRIGGER trg_n16_protected_generation_install_no_replace
BEFORE INSERT ON n16_protected_generation_highwater_installation
WHEN EXISTS(
  SELECT 1 FROM n16_protected_generation_highwater_installation
  WHERE singleton_id=1
)
BEGIN SELECT RAISE(ABORT, 'protected generation ledger install replacement forbidden'); END
""".strip(),
    "trg_n16_protected_generation_install_no_update": """
CREATE TRIGGER trg_n16_protected_generation_install_no_update
BEFORE UPDATE ON n16_protected_generation_highwater_installation
BEGIN SELECT RAISE(ABORT, 'protected generation ledger installation immutable'); END
""".strip(),
    "trg_n16_protected_generation_install_no_delete": """
CREATE TRIGGER trg_n16_protected_generation_install_no_delete
BEFORE DELETE ON n16_protected_generation_highwater_installation
BEGIN SELECT RAISE(ABORT, 'protected generation ledger installation permanent'); END
""".strip(),
    "trg_n16_protected_generation_highwater_no_replace": """
CREATE TRIGGER trg_n16_protected_generation_highwater_no_replace
BEFORE INSERT ON n16_protected_generation_highwater
WHEN EXISTS(
  SELECT 1 FROM n16_protected_generation_highwater WHERE singleton_id=1
)
BEGIN SELECT RAISE(ABORT, 'protected generation ledger replacement forbidden'); END
""".strip(),
    "trg_n16_protected_generation_highwater_transition": """
CREATE TRIGGER trg_n16_protected_generation_highwater_transition
BEFORE UPDATE ON n16_protected_generation_highwater
WHEN NEW.singleton_id!=OLD.singleton_id
 OR typeof(NEW.generation)!='integer'
 OR NEW.generation<OLD.generation
 OR typeof(NEW.review_schema_version)!='integer'
 OR NEW.review_schema_version<OLD.review_schema_version
 OR (
      NEW.generation=OLD.generation
      AND NEW.review_schema_version=OLD.review_schema_version
    )
 OR (
      NEW.generation>OLD.generation
      AND (
        NEW.review_schema_version!=OLD.review_schema_version
        OR NEW.family_catalog_sha256!=OLD.family_catalog_sha256
      )
    )
 OR (
      NEW.generation=OLD.generation
      AND NEW.review_schema_version>OLD.review_schema_version
      AND (
        length(NEW.family_catalog_sha256)!=64
        OR NEW.family_catalog_sha256 GLOB '*[^0-9a-f]*'
      )
    )
 OR typeof(NEW.updated_at)!='text'
 OR length(NEW.updated_at)<1 OR length(NEW.updated_at)>64
BEGIN SELECT RAISE(ABORT, 'protected generation ledger transition invalid'); END
""".strip(),
    "trg_n16_protected_generation_highwater_no_delete": """
CREATE TRIGGER trg_n16_protected_generation_highwater_no_delete
BEFORE DELETE ON n16_protected_generation_highwater
BEGIN SELECT RAISE(ABORT, 'protected generation ledger highwater permanent'); END
""".strip(),
}


class N16ClaimLedgerError(RuntimeError):
    """Raised when the independent N16 lifecycle ledger is not provable."""


@dataclass
class N16ClaimLedgerFileScope:
    """One immutable main/parent proof with controlled sidecar refreshes."""

    path: Path
    main_identity: tuple[int, int] | None
    parent_identity: tuple[int, int]
    sidecar_identities: tuple[tuple[int, int] | None, ...]
    roots: tuple[Path, ...] = ()

    @classmethod
    def capture(
        cls,
        path: Path,
        *,
        allow_missing: bool,
        roots: tuple[Path, ...] = (),
    ) -> "N16ClaimLedgerFileScope":
        from .signal_retention import (
            _capture_database_sidecars,
            _directory_identity,
            _regular_file_identity,
            _validate_database_sidecars,
        )

        parent_identity = _directory_identity(
            path.parent, "N16 claim ledger parent directory"
        )
        if os.path.lexists(str(path)):
            main_identity = _regular_file_identity(path, "N16 claim ledger")
            sidecars = _capture_database_sidecars(path, "N16 claim ledger")
        else:
            if not allow_missing:
                raise N16ClaimLedgerError("N16 claim ledger is missing")
            _validate_database_sidecars(
                path, "N16 claim ledger", require_absent=True
            )
            main_identity = None
            sidecars = (None, None, None)
        scope = cls(
            path=path,
            main_identity=main_identity,
            parent_identity=parent_identity,
            sidecar_identities=sidecars,
            roots=roots,
        )
        if main_identity is None:
            scope.validate_absent()
        else:
            scope.validate_before_open(path)
        return scope

    def _validate_location(self) -> None:
        from .signal_retention import _directory_identity, _is_within

        if (
            _directory_identity(
                self.path.parent, "N16 claim ledger parent directory"
            )
            != self.parent_identity
        ):
            raise N16ClaimLedgerError(
                "N16 claim ledger parent identity changed"
            )
        if self.roots and not any(
            _is_within(root, self.path.resolve()) for root in self.roots
        ):
            raise N16ClaimLedgerError(
                "N16 claim ledger escaped the Binance allowlist"
            )

    def validate_absent(self) -> None:
        from .signal_retention import _validate_database_sidecars

        self._validate_location()
        if self.main_identity is not None or os.path.lexists(str(self.path)):
            raise N16ClaimLedgerError(
                "N16 claim ledger unexpectedly exists"
            )
        _validate_database_sidecars(
            self.path,
            "N16 claim ledger",
            require_absent=True,
        )

    def validate_before_open(self, path: Path) -> None:
        from .signal_retention import (
            _regular_file_identity,
            _validate_database_sidecars,
        )

        if path != self.path or self.main_identity is None:
            raise N16ClaimLedgerError(
                "N16 claim ledger scope is not bound to an existing file"
            )
        self._validate_location()
        if (
            _regular_file_identity(path, "N16 claim ledger")
            != self.main_identity
        ):
            raise N16ClaimLedgerError(
                "N16 claim ledger main identity changed"
            )
        _validate_database_sidecars(
            path,
            "N16 claim ledger",
            expected=self.sidecar_identities,
        )

    def bind_created(self, identity: tuple[int, int]) -> None:
        from .signal_retention import _regular_file_identity

        if (
            self.main_identity is not None
            or type(identity) is not tuple
            or len(identity) != 2
            or any(type(value) is not int or value < 0 for value in identity)
        ):
            raise N16ClaimLedgerError(
                "N16 claim ledger created identity is invalid"
            )
        self._validate_location()
        if (
            _regular_file_identity(self.path, "N16 claim ledger")
            != identity
        ):
            raise N16ClaimLedgerError(
                "N16 claim ledger created identity changed"
            )
        self.main_identity = identity
        self.sidecar_identities = (None, None, None)
        self.validate_before_open(self.path)

    def refresh_after_close(self, path: Path) -> None:
        from .signal_retention import (
            _regular_file_identity,
            _validate_database_sidecars,
        )

        if path != self.path or self.main_identity is None:
            raise N16ClaimLedgerError("N16 claim ledger scope is invalid")
        self._validate_location()
        if (
            _regular_file_identity(path, "N16 claim ledger")
            != self.main_identity
        ):
            raise N16ClaimLedgerError(
                "N16 claim ledger identity changed while open"
            )
        self.sidecar_identities = _validate_database_sidecars(
            path, "N16 claim ledger"
        )


@dataclass(frozen=True)
class N16ReviewClaimSummary:
    review_install_sha256: str
    confirmed_claim_count: int
    first_claim_sha256: str | None
    last_claim_sha256: str | None
    confirmed_chain_sha256: str


@dataclass(frozen=True)
class N16ReviewPublicationBaseline:
    current_scan_id: int | None
    current_manifest_sha256: str
    retention_sha256: str
    staging_sha256: str
    passed_claims_sha256: str


@dataclass(frozen=True)
class N16PreparedPublication:
    txn_id: str
    scan_id: int
    batch_sha256: str
    claim_count: int
    base_summary: N16ReviewClaimSummary
    review_baseline: N16ReviewPublicationBaseline


_META_TABLE_SQL = """
CREATE TABLE n16_claim_ledger_meta (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    schema_version INTEGER NOT NULL CHECK(
        typeof(schema_version) = 'integer' AND schema_version = 1
    ),
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'N16'),
    rule_version TEXT NOT NULL CHECK(rule_version = 'N16_V1'),
    guard_sha256 TEXT NOT NULL CHECK(
        guard_sha256 = 'd262585be43bf3dc7137d98e84770d672b8d82d40c369724a1ac70e211b52332'
    ),
    ledger_uuid TEXT NOT NULL CHECK(
        length(ledger_uuid) = 64 AND ledger_uuid NOT GLOB '*[^0-9a-f]*'
    ),
    phase TEXT NOT NULL CHECK(phase IN ('INSTALLING', 'READY', 'PREPARED')),
    review_install_sha256 TEXT CHECK(
        review_install_sha256 IS NULL OR (
            length(review_install_sha256) = 64
            AND review_install_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    confirmed_claim_count INTEGER NOT NULL CHECK(
        typeof(confirmed_claim_count) = 'integer'
        AND confirmed_claim_count >= 0
    ),
    confirmed_first_claim_sha256 TEXT CHECK(
        confirmed_first_claim_sha256 IS NULL OR (
            length(confirmed_first_claim_sha256) = 64
            AND confirmed_first_claim_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    confirmed_last_claim_sha256 TEXT CHECK(
        confirmed_last_claim_sha256 IS NULL OR (
            length(confirmed_last_claim_sha256) = 64
            AND confirmed_last_claim_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    confirmed_chain_sha256 TEXT NOT NULL CHECK(
        length(confirmed_chain_sha256) = 64
        AND confirmed_chain_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    pending_txn_id TEXT CHECK(
        pending_txn_id IS NULL OR (
            length(pending_txn_id) = 64
            AND pending_txn_id NOT GLOB '*[^0-9a-f]*'
        )
    ),
    pending_scan_id INTEGER CHECK(
        pending_scan_id IS NULL OR (
            typeof(pending_scan_id) = 'integer' AND pending_scan_id > 0
        )
    ),
    pending_batch_sha256 TEXT CHECK(
        pending_batch_sha256 IS NULL OR (
            length(pending_batch_sha256) = 64
            AND pending_batch_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    pending_claim_count INTEGER NOT NULL CHECK(
        typeof(pending_claim_count) = 'integer' AND pending_claim_count >= 0
    ),
    pending_target_chain_sha256 TEXT CHECK(
        pending_target_chain_sha256 IS NULL OR (
            length(pending_target_chain_sha256) = 64
            AND pending_target_chain_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    pending_base_current_scan_id INTEGER CHECK(
        pending_base_current_scan_id IS NULL OR (
            typeof(pending_base_current_scan_id) = 'integer'
            AND pending_base_current_scan_id > 0
        )
    ),
    pending_base_current_manifest_sha256 TEXT CHECK(
        pending_base_current_manifest_sha256 IS NULL OR (
            length(pending_base_current_manifest_sha256) = 64
            AND pending_base_current_manifest_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    pending_retention_sha256 TEXT CHECK(
        pending_retention_sha256 IS NULL OR (
            length(pending_retention_sha256) = 64
            AND pending_retention_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    pending_staging_sha256 TEXT CHECK(
        pending_staging_sha256 IS NULL OR (
            length(pending_staging_sha256) = 64
            AND pending_staging_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    pending_passed_claims_sha256 TEXT CHECK(
        pending_passed_claims_sha256 IS NULL OR (
            length(pending_passed_claims_sha256) = 64
            AND pending_passed_claims_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    updated_at TEXT NOT NULL CHECK(length(updated_at) BETWEEN 1 AND 64),
    CHECK(
        (
            phase = 'INSTALLING'
            AND review_install_sha256 IS NULL
            AND confirmed_claim_count = 0
            AND confirmed_first_claim_sha256 IS NULL
            AND confirmed_last_claim_sha256 IS NULL
            AND confirmed_chain_sha256 =
                '0000000000000000000000000000000000000000000000000000000000000000'
            AND pending_txn_id IS NULL
            AND pending_scan_id IS NULL
            AND pending_batch_sha256 IS NULL
            AND pending_claim_count = 0
            AND pending_target_chain_sha256 IS NULL
            AND pending_base_current_scan_id IS NULL
            AND pending_base_current_manifest_sha256 IS NULL
            AND pending_retention_sha256 IS NULL
            AND pending_staging_sha256 IS NULL
            AND pending_passed_claims_sha256 IS NULL
        ) OR (
            phase = 'READY'
            AND review_install_sha256 IS NOT NULL
            AND pending_txn_id IS NULL
            AND pending_scan_id IS NULL
            AND pending_batch_sha256 IS NULL
            AND pending_claim_count = 0
            AND pending_target_chain_sha256 IS NULL
            AND pending_base_current_scan_id IS NULL
            AND pending_base_current_manifest_sha256 IS NULL
            AND pending_retention_sha256 IS NULL
            AND pending_staging_sha256 IS NULL
            AND pending_passed_claims_sha256 IS NULL
            AND (
                (
                    confirmed_claim_count = 0
                    AND confirmed_first_claim_sha256 IS NULL
                    AND confirmed_last_claim_sha256 IS NULL
                    AND confirmed_chain_sha256 =
                        '0000000000000000000000000000000000000000000000000000000000000000'
                ) OR (
                    confirmed_claim_count > 0
                    AND confirmed_first_claim_sha256 IS NOT NULL
                    AND confirmed_last_claim_sha256 IS NOT NULL
                    AND confirmed_chain_sha256 !=
                        '0000000000000000000000000000000000000000000000000000000000000000'
                )
            )
        ) OR (
            phase = 'PREPARED'
            AND review_install_sha256 IS NOT NULL
            AND pending_txn_id IS NOT NULL
            AND pending_scan_id IS NOT NULL
            AND pending_batch_sha256 IS NOT NULL
            AND pending_claim_count > 0
            AND pending_target_chain_sha256 IS NOT NULL
            AND pending_base_current_manifest_sha256 IS NOT NULL
            AND pending_retention_sha256 IS NOT NULL
            AND pending_staging_sha256 IS NOT NULL
            AND pending_passed_claims_sha256 IS NOT NULL
            AND (
                (
                    confirmed_claim_count = 0
                    AND confirmed_first_claim_sha256 IS NULL
                    AND confirmed_last_claim_sha256 IS NULL
                    AND confirmed_chain_sha256 =
                        '0000000000000000000000000000000000000000000000000000000000000000'
                ) OR (
                    confirmed_claim_count > 0
                    AND confirmed_first_claim_sha256 IS NOT NULL
                    AND confirmed_last_claim_sha256 IS NOT NULL
                    AND confirmed_chain_sha256 !=
                        '0000000000000000000000000000000000000000000000000000000000000000'
                )
            )
        )
    )
)
""".strip()

_CLAIM_TABLE_SQL = """
CREATE TABLE n16_permanent_claims (
    claim_ordinal INTEGER PRIMARY KEY CHECK(
        typeof(claim_ordinal) = 'integer' AND claim_ordinal > 0
    ),
    txn_id TEXT NOT NULL CHECK(
        length(txn_id) = 64 AND txn_id NOT GLOB '*[^0-9a-f]*'
    ),
    claim_state TEXT NOT NULL CHECK(claim_state IN ('PREPARED', 'COMMITTED')),
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
    review_ledger_id INTEGER NOT NULL CHECK(
        typeof(review_ledger_id) = 'integer' AND review_ledger_id > 0
    ),
    state_id INTEGER NOT NULL CHECK(
        typeof(state_id) = 'integer' AND state_id > 0
    ),
    symbol TEXT NOT NULL CHECK(length(symbol) BETWEEN 1 AND 64),
    episode_id TEXT NOT NULL CHECK(
        length(episode_id) = 24 AND episode_id NOT GLOB '*[^0-9a-f]*'
    ),
    structure_id TEXT NOT NULL CHECK(
        length(structure_id) = 24 AND structure_id NOT GLOB '*[^0-9a-f]*'
    ),
    signal_evidence_sha256 TEXT NOT NULL CHECK(
        length(signal_evidence_sha256) = 64
        AND signal_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    state_evidence_sha256 TEXT NOT NULL CHECK(
        length(state_evidence_sha256) = 64
        AND state_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    signal_created_at TEXT NOT NULL CHECK(length(signal_created_at) BETWEEN 1 AND 128),
    claim_created_at TEXT NOT NULL CHECK(length(claim_created_at) BETWEEN 1 AND 128),
    claim_sha256 TEXT NOT NULL CHECK(
        length(claim_sha256) = 64 AND claim_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    chain_sha256 TEXT NOT NULL CHECK(
        length(chain_sha256) = 64 AND chain_sha256 NOT GLOB '*[^0-9a-f]*'
    )
)
""".strip()

_INDEX_SQL = {
    "idx_n16_permanent_claim_structure": (
        "CREATE UNIQUE INDEX idx_n16_permanent_claim_structure "
        "ON n16_permanent_claims(structure_id)"
    ),
    "idx_n16_permanent_claim_episode": (
        "CREATE UNIQUE INDEX idx_n16_permanent_claim_episode "
        "ON n16_permanent_claims(episode_id)"
    ),
    "idx_n16_permanent_claim_signal": (
        "CREATE UNIQUE INDEX idx_n16_permanent_claim_signal "
        "ON n16_permanent_claims(source_signal_id)"
    ),
    "idx_n16_permanent_claim_audit": (
        "CREATE UNIQUE INDEX idx_n16_permanent_claim_audit "
        "ON n16_permanent_claims(audit_id)"
    ),
    "idx_n16_permanent_claim_review_ledger": (
        "CREATE UNIQUE INDEX idx_n16_permanent_claim_review_ledger "
        "ON n16_permanent_claims(review_ledger_id)"
    ),
    "idx_n16_permanent_claim_state": (
        "CREATE UNIQUE INDEX idx_n16_permanent_claim_state "
        "ON n16_permanent_claims(state_id)"
    ),
    "idx_n16_permanent_claim_digest": (
        "CREATE UNIQUE INDEX idx_n16_permanent_claim_digest "
        "ON n16_permanent_claims(claim_sha256)"
    ),
    "idx_n16_permanent_claim_txn": (
        "CREATE INDEX idx_n16_permanent_claim_txn "
        "ON n16_permanent_claims(txn_id, claim_state, claim_ordinal)"
    ),
}

_CLAIM_SELECT = (
    "claim_ordinal, source_signal_id, schema_version, rule_version, "
    "strategy_id, source_scan_id, audit_id, review_ledger_id, state_id, "
    "symbol, episode_id, structure_id, signal_evidence_sha256, "
    "state_evidence_sha256, signal_created_at, claim_created_at"
)


def _is_lower_hex(value: Any, length: int) -> bool:
    return bool(
        type(value) is str
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )


def _normalized_sql(value: Any) -> str:
    if type(value) is not str:
        raise N16ClaimLedgerError("N16 claim ledger schema SQL is missing")
    return " ".join(value.lower().split())


def _n16_ledger_schema_reference():
    """Build one process-local reference from the frozen DDL constants."""

    global _N16_LEDGER_SCHEMA_REFERENCE
    with _N16_LEDGER_SCHEMA_REFERENCE_LOCK:
        if _N16_LEDGER_SCHEMA_REFERENCE is not None:
            return _N16_LEDGER_SCHEMA_REFERENCE
        connection = sqlite3.connect(":memory:")
        try:
            connection.execute(_META_TABLE_SQL)
            connection.execute(_CLAIM_TABLE_SQL)
            for statement in _INDEX_SQL.values():
                connection.execute(statement)
            catalog = tuple(
                (row[0], row[1], row[2], _normalized_sql(row[3]))
                for row in connection.execute(
                    "SELECT type,name,tbl_name,sql FROM sqlite_schema "
                    "WHERE name NOT LIKE 'sqlite_%' "
                    "ORDER BY type,name COLLATE BINARY"
                ).fetchall()
            )
            tables = {}
            indexes = {}
            index_columns = {}
            for table in ("n16_claim_ledger_meta", "n16_permanent_claims"):
                tables[table] = tuple(
                    tuple(row)
                    for row in connection.execute(
                        'PRAGMA table_xinfo("%s")' % table
                    ).fetchall()
                )
                listed = connection.execute(
                    'PRAGMA index_list("%s")' % table
                ).fetchall()
                indexes[table] = {
                    row[1]: (row[2], row[3], row[4]) for row in listed
                }
                for row in listed:
                    index_columns[row[1]] = tuple(
                        tuple(item)
                        for item in connection.execute(
                            'PRAGMA index_xinfo("%s")' % row[1]
                        ).fetchall()
                    )
            _N16_LEDGER_SCHEMA_REFERENCE = (
                catalog,
                tables,
                indexes,
                index_columns,
            )
            return _N16_LEDGER_SCHEMA_REFERENCE
        finally:
            connection.close()


def _legacy_witness_mirror_objects() -> set[str]:
    return {
        "n16_legacy_witness_mirror_installation",
        "n16_legacy_witness_mirror",
        *_LEGACY_WITNESS_MIRROR_TRIGGER_SQL,
    }


def _legacy_witness_mirror_status(
    connection: sqlite3.Connection,
) -> str:
    names = _legacy_witness_mirror_objects()
    owned_names = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_schema "
            "WHERE name LIKE 'n16_legacy_witness_%' "
            "OR name LIKE 'trg_n16_legacy_witness_%'"
        )
    }
    if not owned_names:
        return "PRE_MIRROR"
    if owned_names != names:
        raise N16ClaimLedgerError(
            "legacy witness mirror schema is partial or has foreign objects"
        )
    placeholders = ",".join("?" for _ in names)
    rows = connection.execute(
        "SELECT type,name,sql FROM sqlite_schema "
        f"WHERE name IN ({placeholders}) ORDER BY type,name",
        tuple(sorted(names)),
    ).fetchall()
    if {row[1] for row in rows} != names:
        raise N16ClaimLedgerError(
            "legacy witness mirror schema is partial"
        )
    expected = {
        "n16_legacy_witness_mirror_installation": (
            "table",
            _LEGACY_WITNESS_MIRROR_INSTALLATION_SQL,
        ),
        "n16_legacy_witness_mirror": (
            "table",
            _LEGACY_WITNESS_MIRROR_SQL,
        ),
        **{
            name: ("trigger", sql)
            for name, sql in _LEGACY_WITNESS_MIRROR_TRIGGER_SQL.items()
        },
    }
    for row in rows:
        if (
            row[0] != expected[row[1]][0]
            or _normalized_sql(row[2])
            != _normalized_sql(expected[row[1]][1])
        ):
            raise N16ClaimLedgerError(
                "legacy witness mirror catalog is inconsistent"
            )
    reference = sqlite3.connect(":memory:")
    try:
        reference.execute(_LEGACY_WITNESS_MIRROR_INSTALLATION_SQL)
        reference.execute(_LEGACY_WITNESS_MIRROR_SQL)
        for sql in _LEGACY_WITNESS_MIRROR_TRIGGER_SQL.values():
            reference.execute(sql)
        for table in (
            "n16_legacy_witness_mirror_installation",
            "n16_legacy_witness_mirror",
        ):
            actual = tuple(
                tuple(row)
                for row in connection.execute(
                    'PRAGMA table_xinfo("%s")' % table
                )
            )
            wanted = tuple(
                tuple(row)
                for row in reference.execute(
                    'PRAGMA table_xinfo("%s")' % table
                )
            )
            if actual != wanted:
                raise N16ClaimLedgerError(
                    "legacy witness mirror table metadata is inconsistent"
                )
            actual_indexes = {
                row[1]: tuple(row[2:5])
                for row in connection.execute(
                    'PRAGMA index_list("%s")' % table
                )
            }
            wanted_indexes = {
                row[1]: tuple(row[2:5])
                for row in reference.execute(
                    'PRAGMA index_list("%s")' % table
                )
            }
            if actual_indexes != wanted_indexes:
                raise N16ClaimLedgerError(
                    "legacy witness mirror index metadata is inconsistent"
                )
            if connection.execute(
                'PRAGMA foreign_key_list("%s")' % table
            ).fetchall():
                raise N16ClaimLedgerError(
                    "legacy witness mirror foreign key is forbidden"
                )
    finally:
        reference.close()
    installation = connection.execute(
        "SELECT schema_version,rule_version "
        "FROM n16_legacy_witness_mirror_installation "
        "WHERE singleton_id=1"
    ).fetchall()
    mirror = connection.execute(
        "SELECT phase,review_plan_sha256,witness_sha256,"
        "pre_review_canonical_sha256,authorization_sha256,"
        "ledger_uuid,base_review_snapshot_sha256,"
        "target_review_snapshot_sha256,base_catalog_sha256,"
        "target_catalog_sha256,"
        "prepared_at,committed_at "
        "FROM n16_legacy_witness_mirror WHERE singleton_id=1"
    ).fetchall()
    if (
        installation
        != [(2, "AUTHORIZED_LEGACY_V3_UNBOUND_MIRROR_V2")]
        or len(mirror) != 1
        or mirror[0][0] not in {"EMPTY", "PREPARED", "COMMITTED"}
    ):
        raise N16ClaimLedgerError(
            "legacy witness mirror state is inconsistent"
        )
    phase = mirror[0][0]
    if phase == "EMPTY":
        if any(value is not None for value in mirror[0][1:]):
            raise N16ClaimLedgerError(
                "empty legacy witness mirror contains evidence"
            )
    else:
        if (
            any(not _is_lower_hex(value, 64) for value in mirror[0][1:4])
            or mirror[0][4]
            != "2dd9bf0e9a19d4464ff53ddb9bba35a850306499d12d88a34257541dcd0b04a1"
            or any(not _is_lower_hex(value, 64) for value in mirror[0][5:10])
            or type(mirror[0][10]) is not str
            or not mirror[0][10]
            or (
                phase == "PREPARED"
                and mirror[0][11] is not None
            )
            or (
                phase == "COMMITTED"
                and (
                    type(mirror[0][11]) is not str
                    or not mirror[0][11]
                )
            )
        ):
            raise N16ClaimLedgerError(
                "legacy witness mirror evidence is inconsistent"
            )
    return phase


def _protected_generation_highwater_objects() -> set[str]:
    return {
        "n16_protected_generation_highwater_installation",
        "n16_protected_generation_highwater",
        *_PROTECTED_GENERATION_HIGHWATER_TRIGGER_SQL,
    }


def _protected_generation_highwater_status(
    connection: sqlite3.Connection,
) -> str:
    names = _protected_generation_highwater_objects()
    owned_names = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_schema "
            "WHERE name LIKE 'n16_protected_generation_%' "
            "OR name LIKE 'trg_n16_protected_generation_%'"
        )
    }
    if not owned_names:
        return "PRE_HIGHWATER"
    if owned_names != names:
        raise N16ClaimLedgerError(
            "protected generation ledger schema is partial or foreign"
        )
    placeholders = ",".join("?" for _ in names)
    rows = connection.execute(
        "SELECT type,name,sql FROM sqlite_schema "
        f"WHERE name IN ({placeholders}) ORDER BY type,name",
        tuple(sorted(names)),
    ).fetchall()
    expected = {
        "n16_protected_generation_highwater_installation": (
            "table",
            _PROTECTED_GENERATION_HIGHWATER_INSTALLATION_SQL,
        ),
        "n16_protected_generation_highwater": (
            "table",
            _PROTECTED_GENERATION_HIGHWATER_SQL,
        ),
        **{
            name: ("trigger", sql)
            for name, sql in
            _PROTECTED_GENERATION_HIGHWATER_TRIGGER_SQL.items()
        },
    }
    if {row[1] for row in rows} != names:
        raise N16ClaimLedgerError(
            "protected generation ledger schema is partial"
        )
    for row in rows:
        if (
            row[0] != expected[row[1]][0]
            or _normalized_sql(row[2])
            != _normalized_sql(expected[row[1]][1])
        ):
            raise N16ClaimLedgerError(
                "protected generation ledger catalog is inconsistent"
            )
    reference = sqlite3.connect(":memory:")
    try:
        reference.execute(
            _PROTECTED_GENERATION_HIGHWATER_INSTALLATION_SQL
        )
        reference.execute(_PROTECTED_GENERATION_HIGHWATER_SQL)
        for sql in _PROTECTED_GENERATION_HIGHWATER_TRIGGER_SQL.values():
            reference.execute(sql)
        for table in (
            "n16_protected_generation_highwater_installation",
            "n16_protected_generation_highwater",
        ):
            actual = tuple(
                tuple(row)
                for row in connection.execute(
                    f'PRAGMA table_xinfo("{table}")'
                )
            )
            wanted = tuple(
                tuple(row)
                for row in reference.execute(
                    f'PRAGMA table_xinfo("{table}")'
                )
            )
            if actual != wanted:
                raise N16ClaimLedgerError(
                    "protected generation ledger table metadata conflicts"
                )
            if connection.execute(
                f'PRAGMA foreign_key_list("{table}")'
            ).fetchall():
                raise N16ClaimLedgerError(
                    "protected generation ledger foreign key is forbidden"
                )
    finally:
        reference.close()
    installation = connection.execute(
        "SELECT schema_version,rule_version "
        "FROM n16_protected_generation_highwater_installation "
        "WHERE singleton_id=1"
    ).fetchall()
    highwater = connection.execute(
        "SELECT generation,review_schema_version,"
        "family_catalog_sha256,updated_at "
        "FROM n16_protected_generation_highwater "
        "WHERE singleton_id=1"
    ).fetchall()
    if (
        installation
        != [(1, "N19_PROTECTED_GENERATION_LEDGER_V1")]
        or len(highwater) != 1
        or type(highwater[0][0]) is not int
        or highwater[0][0] < 0
        or type(highwater[0][1]) is not int
        or highwater[0][1] <= 0
        or not _is_lower_hex(highwater[0][2], 64)
        or type(highwater[0][3]) is not str
        or not 1 <= len(highwater[0][3]) <= 64
    ):
        raise N16ClaimLedgerError(
            "protected generation ledger evidence is inconsistent"
        )
    return "CURRENT"


def validate_review_claim(value: Sequence[Any]) -> tuple[Any, ...]:
    if type(value) not in (tuple, list) or len(value) != 16:
        raise N16ClaimLedgerError("N16 review claim shape is invalid")
    claim = tuple(value)
    if (
        any(type(claim[index]) is not int or claim[index] <= 0 for index in (0, 1, 5, 6, 7, 8))
        or claim[2] != 1
        or type(claim[2]) is not int
        or claim[3] != N16_RULE_VERSION
        or type(claim[3]) is not str
        or claim[4] != "N16"
        or type(claim[4]) is not str
        or type(claim[9]) is not str
        or not 1 <= len(claim[9]) <= 64
        or not _is_lower_hex(claim[10], 24)
        or not _is_lower_hex(claim[11], 24)
        or not _is_lower_hex(claim[12], 64)
        or not _is_lower_hex(claim[13], 64)
        or type(claim[14]) is not str
        or not 1 <= len(claim[14]) <= 128
        or type(claim[15]) is not str
        or not 1 <= len(claim[15]) <= 128
    ):
        raise N16ClaimLedgerError("N16 review claim identity is invalid")
    return claim


def review_claim_sha256(value: Sequence[Any]) -> str:
    claim = validate_review_claim(value)
    encoded = json.dumps(
        list(claim), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def review_install_sha256(value: Sequence[Any]) -> str:
    if type(value) not in (tuple, list) or len(value) != 6:
        raise N16ClaimLedgerError("N16 review installation shape is invalid")
    row = tuple(value)
    if (
        row[0] != "N16"
        or type(row[0]) is not str
        or row[1] != 1
        or type(row[1]) is not int
        or row[2] != N16_RULE_VERSION
        or type(row[2]) is not str
        or row[3] != N16_GUARD_SHA256
        or type(row[3]) is not str
        or type(row[4]) is not str
        or not 1 <= len(row[4]) <= 64
        or type(row[5]) is not int
        or row[5] <= 0
    ):
        raise N16ClaimLedgerError("N16 review installation identity is invalid")
    return hashlib.sha256(
        json.dumps(list(row), separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _chain_sha256(previous: str, claim_sha256: str) -> str:
    if not _is_lower_hex(previous, 64) or not _is_lower_hex(claim_sha256, 64):
        raise N16ClaimLedgerError("N16 claim chain identity is invalid")
    return hashlib.sha256(
        (previous + ":" + claim_sha256).encode("ascii")
    ).hexdigest()


def advance_review_claim_chain(
    previous: str,
    claim: Sequence[Any],
) -> str:
    """Return the canonical next chain head for one validated Review claim."""

    return _chain_sha256(previous, review_claim_sha256(claim))


class N16PermanentClaimLedger:
    """Strict state-directory SQLite ledger for permanent N16 claims."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        file_scope: N16ClaimLedgerFileScope | None = None,
    ):
        requested = Path(os.path.abspath(os.fspath(path)))
        # Reuse the already hardened path-chain semantics without importing
        # recorder at module import time (which would form a cycle).
        from .recorder import ReviewRecorder

        self.path = ReviewRecorder._canonical_runtime_database_path(requested)
        if file_scope is None:
            file_scope = N16ClaimLedgerFileScope.capture(
                self.path,
                allow_missing=True,
            )
        elif file_scope.path != self.path:
            raise N16ClaimLedgerError(
                "N16 claim ledger path differs from its scope proof"
            )
        elif file_scope.main_identity is None:
            file_scope.validate_absent()
        else:
            file_scope.validate_before_open(self.path)
        self._file_scope = file_scope
        self._parent_identity = file_scope.parent_identity
        # A scheduler round retains one independently opened, identity-attested
        # OS descriptor.  Every Review write still opens its own SQLite ledger
        # snapshot and performs the complete ledger/catalog attestation.
        # Keeping the anchor separate from SQLite avoids pager-level FD reuse
        # when two recorder instances concurrently evaluate the same database.
        # Thread-local ownership prevents either proof from crossing threads.
        self._runtime_attestation_scope = threading.local()

    def _attest_runtime_descriptor(self) -> None:
        descriptor = getattr(
            self._runtime_attestation_scope, "descriptor", None
        )
        expected = self._file_scope.main_identity
        if type(descriptor) is not int or expected is None:
            raise N16ClaimLedgerError(
                "N16 runtime ledger descriptor proof is missing"
            )
        try:
            details = os.fstat(descriptor)
        except OSError as exc:
            raise N16ClaimLedgerError(
                "N16 runtime ledger descriptor is unavailable"
            ) from exc
        if (
            not stat.S_ISREG(details.st_mode)
            or (int(details.st_dev), int(details.st_ino)) != expected
        ):
            raise N16ClaimLedgerError(
                "N16 runtime ledger descriptor identity changed"
            )

    @contextmanager
    def runtime_attestation_scope(self) -> Iterator[None]:
        """Hold one authenticated OS ledger anchor for one scheduler round."""

        if getattr(self._runtime_attestation_scope, "descriptor", None) is not None:
            raise N16ClaimLedgerError(
                "N16 runtime ledger attestation scope is already active"
            )
        expected = self._file_scope.main_identity
        if expected is None:
            raise N16ClaimLedgerError("N16 claim ledger is missing")
        self._file_scope.validate_before_open(self.path)
        # The anchor is intentionally not a SQLite connection.  SQLite may
        # reuse a pager descriptor for two same-inode connections, making a
        # second concurrent recorder impossible to attribute uniquely.  A
        # direct O_NOFOLLOW open always gives this round its own exact inode
        # proof; each short transaction separately proves the SQLite FD.
        descriptor = None
        with _N16_LEDGER_RUNTIME_CONNECTION_LOCK:
            try:
                flags = os.O_RDONLY
                flags |= getattr(os, "O_CLOEXEC", 0)
                flags |= getattr(os, "O_NOFOLLOW", 0)
                descriptor = os.open(self.path, flags)
                details = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(details.st_mode)
                    or (int(details.st_dev), int(details.st_ino)) != expected
                ):
                    raise N16ClaimLedgerError(
                        "N16 runtime ledger descriptor is not uniquely attested"
                    )
                self._file_scope.validate_before_open(self.path)
            except BaseException:
                if descriptor is not None:
                    os.close(descriptor)
                raise
        self._runtime_attestation_scope.connection = None
        self._runtime_attestation_scope.descriptor = descriptor
        self._runtime_attestation_scope.in_snapshot = False
        try:
            self._attest_runtime_descriptor()
            yield
        finally:
            self._runtime_attestation_scope.connection = None
            self._runtime_attestation_scope.descriptor = None
            self._runtime_attestation_scope.in_snapshot = False
            os.close(descriptor)
            self._file_scope.validate_before_open(self.path)

    @contextmanager
    def runtime_attestation_transaction(self) -> Iterator[None]:
        """Open and attest one fresh ledger snapshot for one Review write."""

        if getattr(
            self._runtime_attestation_scope, "descriptor", None
        ) is None or getattr(
            self._runtime_attestation_scope, "in_snapshot", False
        ) or getattr(self._runtime_attestation_scope, "connection", None) is not None:
            raise N16ClaimLedgerError(
                "N16 runtime ledger transaction scope is invalid"
            )
        self._file_scope.validate_before_open(self.path)
        self._attest_runtime_descriptor()
        self._runtime_attestation_scope.in_snapshot = True
        from .signal_retention import _open_database

        try:
            with _open_database(
                self.path,
                read_only=True,
                expected_identity=self._file_scope.main_identity,
                file_scope=self._file_scope,
            ) as connection:
                self._runtime_attestation_scope.connection = connection
                connection.execute("PRAGMA query_only=ON")
                if connection.execute("PRAGMA query_only").fetchone() != (1,):
                    raise N16ClaimLedgerError(
                        "N16 runtime ledger snapshot is not query-only"
                    )
                connection.execute("BEGIN")
                self._verify_schema(connection)
                try:
                    yield
                finally:
                    connection.rollback()
                    self._runtime_attestation_scope.connection = None
                    self._verify_schema(connection)
        finally:
            self._runtime_attestation_scope.connection = None
            self._runtime_attestation_scope.in_snapshot = False
            self._attest_runtime_descriptor()
            self._file_scope.validate_before_open(self.path)

    @contextmanager
    def _review_commit_path_guard(
        self,
    ) -> Iterator[Callable[[sqlite3.Connection], None]]:
        """Compare the cooperative ledger namespace immediately before commit."""

        parent_descriptor = None

        def anchored_token() -> tuple[Any, ...]:
            if parent_descriptor is None:
                raise N16ClaimLedgerError(
                    "N16 Review commit parent proof is missing"
                )
            try:
                parent_details = os.fstat(parent_descriptor)
                path_parent_details = os.lstat(str(self.path.parent))
            except OSError as exc:
                raise N16ClaimLedgerError(
                    "N16 Review commit parent cannot be attested"
                ) from exc
            if (
                not stat.S_ISDIR(parent_details.st_mode)
                or stat.S_ISLNK(path_parent_details.st_mode)
                or not stat.S_ISDIR(path_parent_details.st_mode)
                or (
                    int(path_parent_details.st_dev),
                    int(path_parent_details.st_ino),
                )
                != (int(parent_details.st_dev), int(parent_details.st_ino))
            ):
                raise N16ClaimLedgerError(
                    "N16 Review commit parent identity changed"
                )

            def anchored_file(name: str, *, required: bool):
                try:
                    details = os.stat(
                        name,
                        dir_fd=parent_descriptor,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    if required:
                        raise N16ClaimLedgerError(
                            "N16 Review commit ledger main is missing"
                        )
                    return None
                except OSError as exc:
                    raise N16ClaimLedgerError(
                        "N16 Review commit ledger path cannot be attested"
                    ) from exc
                if (
                    stat.S_ISLNK(details.st_mode)
                    or not stat.S_ISREG(details.st_mode)
                    or int(details.st_nlink) != 1
                ):
                    raise N16ClaimLedgerError(
                        "N16 Review commit ledger path identity is invalid"
                    )
                return int(details.st_dev), int(details.st_ino)

            main_identity = anchored_file(self.path.name, required=True)
            sidecars = tuple(
                anchored_file(self.path.name + suffix, required=False)
                for suffix in _N16_LEDGER_SIDECAR_SUFFIXES
            )
            return (
                int(parent_details.st_dev),
                int(parent_details.st_ino),
                int(parent_details.st_ctime_ns),
                int(path_parent_details.st_ctime_ns),
                main_identity,
                sidecars,
            )

        try:
            flags = os.O_RDONLY
            flags |= getattr(os, "O_DIRECTORY", 0)
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            parent_descriptor = os.open(self.path.parent, flags)
            initial_token = anchored_token()
            if (
                initial_token[:2] != self._file_scope.parent_identity
                or initial_token[4] != self._file_scope.main_identity
            ):
                raise N16ClaimLedgerError(
                    "N16 Review commit ledger namespace conflicts with scope"
                )

            def verify(connection: sqlite3.Connection) -> None:
                from .signal_retention import _verify_sqlite_main_identity

                if getattr(
                    self._runtime_attestation_scope, "descriptor", None
                ) is not None:
                    self._attest_runtime_descriptor()
                _verify_sqlite_main_identity(
                    connection,
                    self.path,
                    self._file_scope.main_identity,
                )
                if anchored_token() != initial_token:
                    raise N16ClaimLedgerError(
                        "N16 Review commit ledger namespace changed"
                    )

            yield verify
        finally:
            if parent_descriptor is not None:
                os.close(parent_descriptor)

    def commit_attested_review(
        self,
        review_connection: sqlite3.Connection,
        review_owner: Any,
    ) -> None:
        """Authorize and commit one exact Review SQLite transaction.

        The fresh identity-attested connection takes SQLite's short RESERVED
        lock before becoming query-only.  That excludes even a separate raw
        SQLite writer until the paired Review commit decision is complete.
        The API intentionally accepts neither a callback nor a caller-owned
        context body.  It accepts only the exact ReviewRecorder that owns this
        ledger, invokes that repository-defined pair attestation, and executes
        the exact Review connection commit while the ledger mutex is held.

        Path checks detect mistakes by repository-authorized actors; they do
        not freeze the OS namespace against an uncooperative same-UID process.
        """

        if not isinstance(review_connection, sqlite3.Connection):
            raise N16ClaimLedgerError(
                "N16 Review commit requires an exact SQLite connection"
            )
        # Import lazily to keep the schema module independent at import time.
        # An exact owner type makes this a narrow repository API rather than
        # another spelling of an arbitrary callback.
        from .recorder import ReviewRecorder

        if (
            type(review_owner) is not ReviewRecorder
            or review_owner.n16_claim_ledger is not self
        ):
            raise N16ClaimLedgerError(
                "N16 Review commit requires its exact ReviewRecorder owner"
            )
        if not review_connection.in_transaction:
            raise N16ClaimLedgerError(
                "N16 Review commit requires an active Review transaction"
            )

        with _N16_LEDGER_RUNTIME_CONNECTION_LOCK:
            if (
                getattr(self._runtime_attestation_scope, "connection", None)
                is not None
                or getattr(
                    self._runtime_attestation_scope, "in_snapshot", False
                )
            ):
                raise N16ClaimLedgerError(
                    "N16 Review commit ledger snapshot is already active"
                )
            with self._open(read_only=False) as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute("PRAGMA query_only=ON")
                if connection.execute("PRAGMA query_only").fetchone() != (1,):
                    raise N16ClaimLedgerError(
                        "N16 Review commit ledger snapshot is not query-only"
                    )
                # Opening this exact authenticated connection may create its
                # own WAL/SHM files.  Freeze the namespace only after that
                # controlled setup, but before any content attestation, so an
                # authorized transient path change cannot be normalized away.
                with self._review_commit_path_guard() as verify_path:
                    self._runtime_attestation_scope.connection = connection
                    self._runtime_attestation_scope.in_snapshot = True
                    try:
                        self._verify_schema(connection)
                        review_owner._attest_n16_ledger_before_review_commit(
                            review_connection
                        )
                        if not review_connection.in_transaction:
                            raise N16ClaimLedgerError(
                                "N16 Review transaction ended before authorization"
                            )
                        self._verify_schema(connection)
                        verify_path(connection)
                        try:
                            review_connection.commit()
                        except BaseException:
                            review_connection.rollback()
                            raise
                    finally:
                        try:
                            connection.rollback()
                        finally:
                            self._runtime_attestation_scope.connection = None
                            self._runtime_attestation_scope.in_snapshot = False
                        self._verify_schema(connection)

    @staticmethod
    def _directory_identity(path: Path) -> tuple[int, int]:
        details = os.lstat(str(path))
        if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
            raise N16ClaimLedgerError(
                "N16 claim ledger parent must be a real directory"
            )
        return int(details.st_dev), int(details.st_ino)

    @property
    def exists(self) -> bool:
        if self._file_scope.main_identity is None:
            self._file_scope.validate_absent()
            return False
        self._file_scope.validate_before_open(self.path)
        return True

    @contextmanager
    def _open(self, *, read_only: bool) -> Iterator[sqlite3.Connection]:
        from .signal_retention import (
            _attested_maintenance_connection,
            _open_database,
            _verify_sqlite_main_identity,
        )

        scoped_connection = getattr(
            self._runtime_attestation_scope, "connection", None
        )
        if (
            read_only
            and scoped_connection is not None
            and getattr(
                self._runtime_attestation_scope, "in_snapshot", False
            )
        ):
            if getattr(
                self._runtime_attestation_scope, "descriptor", None
            ) is not None:
                self._attest_runtime_descriptor()
            yield scoped_connection
            return
        scope = self._file_scope
        if scope.main_identity is None:
            raise N16ClaimLedgerError("N16 claim ledger is missing")
        try:
            if not read_only:
                with _N16_LEDGER_RUNTIME_CONNECTION_LOCK:
                    # A malformed/replaced ledger must be rejected before the
                    # RW open can select WAL or create a sidecar.  The second
                    # schema check below is still performed on the exact
                    # attested RW fd.
                    with _open_database(
                        self.path,
                        read_only=True,
                        expected_identity=scope.main_identity,
                        file_scope=scope,
                    ) as preflight:
                        self._verify_schema(preflight)
                    scope.validate_before_open(self.path)
                    opener = lambda: sqlite3.connect(
                        self.path.as_uri() + "?mode=rw",
                        uri=True,
                    )
                    try:
                        with _attested_maintenance_connection(
                            opener, scope.main_identity
                        ) as connection:
                            _verify_sqlite_main_identity(
                                connection, self.path, scope.main_identity
                            )
                            self._verify_schema(connection)
                            connection.execute("PRAGMA journal_mode=WAL")
                            connection.execute("PRAGMA foreign_keys=ON")
                            connection.execute("PRAGMA busy_timeout=30000")
                            yield connection
                    finally:
                        scope.refresh_after_close(self.path)
                return
            with _open_database(
                self.path,
                read_only=True,
                expected_identity=scope.main_identity,
                file_scope=scope,
            ) as connection:
                self._verify_schema(connection)
                yield connection
        except N16ClaimLedgerError:
            raise
        except Exception as exc:
            raise N16ClaimLedgerError(
                "N16 claim ledger cannot be opened safely"
            ) from exc

    @staticmethod
    def _anchored_regular_identity(
        directory_descriptor: int,
        name: str,
        *,
        expected_links: int,
    ) -> tuple[int, int]:
        try:
            details = os.stat(
                name,
                dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise N16ClaimLedgerError(
                "N16 claim ledger staged file is unavailable"
            ) from exc
        if (
            stat.S_ISLNK(details.st_mode)
            or not stat.S_ISREG(details.st_mode)
            or int(details.st_nlink) != expected_links
        ):
            raise N16ClaimLedgerError(
                "N16 claim ledger staged file identity is invalid"
            )
        return int(details.st_dev), int(details.st_ino)

    @staticmethod
    def _require_anchored_names_absent(
        directory_descriptor: int,
        filename: str,
    ) -> None:
        for name in (filename,) + tuple(
            filename + suffix for suffix in _N16_LEDGER_SIDECAR_SUFFIXES
        ):
            try:
                os.stat(
                    name,
                    dir_fd=directory_descriptor,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise N16ClaimLedgerError(
                    "N16 claim ledger path cannot be inspected"
                ) from exc
            raise N16ClaimLedgerError(
                "N16 claim ledger path or sidecar already exists"
            )

    @classmethod
    def _secure_create_empty_file(
        cls,
        directory_descriptor: int,
        filename: str,
    ) -> tuple[int, int]:
        """Create one exact file below an already attested directory fd.

        Cleanup after a failed create is deliberately inode-bound.  A same-name
        replacement is never removed merely because it is a regular one-link
        file.
        """

        cls._require_anchored_names_absent(
            directory_descriptor,
            filename,
        )
        flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = None
        created_identity = None
        try:
            descriptor = os.open(
                filename,
                flags,
                0o600,
                dir_fd=directory_descriptor,
            )
            details = os.fstat(descriptor)
            created_identity = int(details.st_dev), int(details.st_ino)
            if not stat.S_ISREG(details.st_mode) or int(details.st_nlink) != 1:
                raise N16ClaimLedgerError(
                    "N16 claim ledger must be one regular file"
                )
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            os.fsync(directory_descriptor)
            return created_identity
        except BaseException:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            try:
                details = os.stat(
                    filename,
                    dir_fd=directory_descriptor,
                    follow_symlinks=False,
                )
                if (
                    created_identity is not None
                    and stat.S_ISREG(details.st_mode)
                    and int(details.st_nlink) == 1
                    and (int(details.st_dev), int(details.st_ino))
                    == created_identity
                ):
                    os.unlink(filename, dir_fd=directory_descriptor)
                    os.fsync(directory_descriptor)
            except OSError:
                pass
            raise

    @classmethod
    def _capture_staged_sidecars(
        cls,
        directory_descriptor: int,
        filename: str,
        owned: dict[str, tuple[int, int]],
    ) -> None:
        """Record only expected SQLite entries inside our private directory."""

        for suffix in ("-wal", "-shm", "-journal"):
            name = filename + suffix
            try:
                details = os.stat(
                    name,
                    dir_fd=directory_descriptor,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise N16ClaimLedgerError(
                    "N16 claim ledger staged sidecar cannot be inspected"
                ) from exc
            identity = int(details.st_dev), int(details.st_ino)
            if (
                stat.S_ISLNK(details.st_mode)
                or not stat.S_ISREG(details.st_mode)
                or int(details.st_nlink) != 1
            ):
                raise N16ClaimLedgerError(
                    "N16 claim ledger staged sidecar is invalid"
                )
            previous = owned.get(name)
            if previous is not None and previous != identity:
                raise N16ClaimLedgerError(
                    "N16 claim ledger staged sidecar identity changed"
                )
            owned[name] = identity

    @staticmethod
    def _remove_anchored_owned_file(
        directory_descriptor: int,
        name: str,
        identity: tuple[int, int],
    ) -> bool:
        try:
            details = os.stat(
                name,
                dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise N16ClaimLedgerError(
                "N16 claim ledger cleanup path cannot be inspected"
            ) from exc
        if (
            not stat.S_ISREG(details.st_mode)
            or int(details.st_nlink) != 1
            or (int(details.st_dev), int(details.st_ino)) != identity
        ):
            raise N16ClaimLedgerError(
                "N16 claim ledger cleanup path is not owned by this run"
            )
        os.unlink(name, dir_fd=directory_descriptor)
        return True

    @classmethod
    def _remove_created_database(
        cls,
        directory_descriptor: int,
        filename: str,
        identity: tuple[int, int],
        owned_sidecars: dict[str, tuple[int, int]],
    ) -> None:
        errors = []
        for name, expected_identity in sorted(owned_sidecars.items()):
            try:
                cls._remove_anchored_owned_file(
                    directory_descriptor,
                    name,
                    expected_identity,
                )
            except BaseException as exc:
                errors.append(exc)
        try:
            cls._remove_anchored_owned_file(
                directory_descriptor,
                filename,
                identity,
            )
        except BaseException as exc:
            errors.append(exc)
        remaining = set(os.listdir(directory_descriptor))
        if remaining:
            errors.append(
                N16ClaimLedgerError(
                    "N16 claim ledger private stage contains unowned paths: %s"
                    % ",".join(sorted(remaining))
                )
            )
        try:
            os.fsync(directory_descriptor)
        except BaseException as exc:
            errors.append(exc)
        if errors:
            raise N16ClaimLedgerError(
                "N16 claim ledger private cleanup was incomplete"
            ) from errors[0]

    def _create(
        self,
        *,
        now: str,
    ) -> None:
        if type(now) is not str or not now or len(now) > 64:
            raise N16ClaimLedgerError("N16 claim ledger timestamp is invalid")
        self._create_in_private_stage(now=now)

    def _initialize_staged_connection(
        self,
        connection: sqlite3.Connection,
        *,
        now: str,
    ) -> None:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(_META_TABLE_SQL)
        connection.execute(_CLAIM_TABLE_SQL)
        for sql in _INDEX_SQL.values():
            connection.execute(sql)
        connection.execute(
            "PRAGMA application_id = %d" % N16_CLAIM_LEDGER_APPLICATION_ID
        )
        connection.execute(
            "PRAGMA user_version = %d" % N16_CLAIM_LEDGER_SCHEMA_VERSION
        )
        connection.execute(
            """
            INSERT INTO n16_claim_ledger_meta (
                singleton_id, schema_version, strategy_id,
                rule_version, guard_sha256, ledger_uuid, phase,
                review_install_sha256, confirmed_claim_count,
                confirmed_first_claim_sha256,
                confirmed_last_claim_sha256,
                confirmed_chain_sha256, pending_txn_id,
                pending_scan_id, pending_batch_sha256,
                pending_claim_count, pending_target_chain_sha256,
                pending_base_current_scan_id,
                pending_base_current_manifest_sha256,
                pending_retention_sha256, pending_staging_sha256,
                pending_passed_claims_sha256,
                updated_at
            ) VALUES (
                1,1,'N16','N16_V1',?,?,'INSTALLING',NULL,0,NULL,NULL,?,
                NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,?
            )
            """,
            (
                N16_GUARD_SHA256,
                secrets.token_hex(32),
                N16_CLAIM_CHAIN_SEED,
                now,
            ),
        )
        self._verify_schema(connection)
        connection.commit()

    def _create_in_private_stage(
        self,
        *,
        now: str,
    ) -> None:
        """Build below an inode-anchored private directory, then publish once."""

        self._file_scope.validate_absent()

        directory_flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            directory_flags |= os.O_DIRECTORY
        if hasattr(os, "O_NOFOLLOW"):
            directory_flags |= os.O_NOFOLLOW
        parent_descriptor = None
        stage_descriptor = None
        original_cwd_descriptor = None
        stage_name = None
        identity = None
        owned_sidecars = {}  # type: dict[str, tuple[int, int]]
        cwd_changed = False
        final_maybe_linked = False
        with _N16_LEDGER_CREATE_CWD_LOCK:
            current_thread = threading.current_thread()
            if any(
                thread is not current_thread and thread.is_alive()
                for thread in threading.enumerate()
            ):
                raise N16ClaimLedgerError(
                    "N16 claim ledger installation requires one maintenance thread"
                )
            try:
                parent_descriptor = os.open(
                    str(self.path.parent),
                    directory_flags,
                )
                parent = os.fstat(parent_descriptor)
                if (
                    not stat.S_ISDIR(parent.st_mode)
                    or (int(parent.st_dev), int(parent.st_ino))
                    != self._parent_identity
                ):
                    raise N16ClaimLedgerError(
                        "N16 claim ledger parent identity changed"
                    )
                self._require_anchored_names_absent(
                    parent_descriptor,
                    self.path.name,
                )
                for _attempt in range(32):
                    stage_name = ".n16-ledger-stage-" + secrets.token_hex(16)
                    try:
                        os.mkdir(stage_name, 0o700, dir_fd=parent_descriptor)
                    except FileExistsError:
                        stage_name = None
                        continue
                    break
                if stage_name is None:
                    raise N16ClaimLedgerError(
                        "N16 claim ledger private stage cannot be allocated"
                    )
                stage_descriptor = os.open(
                    stage_name,
                    directory_flags,
                    dir_fd=parent_descriptor,
                )
                stage = os.fstat(stage_descriptor)
                if (
                    not stat.S_ISDIR(stage.st_mode)
                    or int(stage.st_dev) != int(parent.st_dev)
                    or int(stage.st_mode) & 0o077
                ):
                    raise N16ClaimLedgerError(
                        "N16 claim ledger private stage is not isolated"
                    )
                self._require_anchored_names_absent(
                    stage_descriptor,
                    self.path.name,
                )
                identity = self._secure_create_empty_file(
                    stage_descriptor,
                    self.path.name,
                )

                from .signal_retention import _attested_maintenance_connection

                original_cwd_descriptor = os.open(".", directory_flags)
                # Mark before the syscall: an injected acknowledgement loss may
                # happen after the kernel already changed the process cwd.
                cwd_changed = True
                os.fchdir(stage_descriptor)
                uri = "file:./%s?mode=rw" % self.path.name
                with _attested_maintenance_connection(
                    lambda: sqlite3.connect(uri, uri=True),
                    identity,
                ) as connection:
                    try:
                        mode_row = connection.execute(
                            "PRAGMA journal_mode=DELETE"
                        ).fetchone()
                        if mode_row != ("delete",):
                            raise N16ClaimLedgerError(
                                "N16 claim ledger staging journal mode is invalid"
                            )
                        connection.execute("PRAGMA foreign_keys=ON")
                        connection.execute("PRAGMA busy_timeout=30000")
                        self._initialize_staged_connection(
                            connection,
                            now=now,
                        )
                    except BaseException:
                        try:
                            self._capture_staged_sidecars(
                                stage_descriptor,
                                self.path.name,
                                owned_sidecars,
                            )
                        except BaseException:
                            pass
                        raise
                    else:
                        self._capture_staged_sidecars(
                            stage_descriptor,
                            self.path.name,
                            owned_sidecars,
                        )
                os.fchdir(original_cwd_descriptor)
                cwd_changed = False
                if tuple(sorted(os.listdir(stage_descriptor))) != (
                    self.path.name,
                ):
                    raise N16ClaimLedgerError(
                        "N16 claim ledger private stage contains unexpected files"
                    )
                if (
                    self._anchored_regular_identity(
                        stage_descriptor,
                        self.path.name,
                        expected_links=1,
                    )
                    != identity
                ):
                    raise N16ClaimLedgerError(
                        "N16 claim ledger staged main identity changed"
                    )
                self._require_anchored_names_absent(
                    parent_descriptor,
                    self.path.name,
                )
                # Mark before link(): a wrapper may publish and then lose its
                # acknowledgement.  Cleanup still compares the exact inode.
                final_maybe_linked = True
                os.link(
                    self.path.name,
                    self.path.name,
                    src_dir_fd=stage_descriptor,
                    dst_dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
                if (
                    self._anchored_regular_identity(
                        parent_descriptor,
                        self.path.name,
                        expected_links=2,
                    )
                    != identity
                    or self._anchored_regular_identity(
                        stage_descriptor,
                        self.path.name,
                        expected_links=2,
                    )
                    != identity
                ):
                    raise N16ClaimLedgerError(
                        "N16 claim ledger published identity is invalid"
                    )
                os.unlink(self.path.name, dir_fd=stage_descriptor)
                if (
                    self._anchored_regular_identity(
                        parent_descriptor,
                        self.path.name,
                        expected_links=1,
                    )
                    != identity
                ):
                    raise N16ClaimLedgerError(
                        "N16 claim ledger final identity is invalid"
                    )
                visible = os.lstat(str(self.path))
                visible_sidecars_absent = all(
                    not os.path.lexists(str(self.path) + suffix)
                    for suffix in _N16_LEDGER_SIDECAR_SUFFIXES
                )
                if (
                    self._directory_identity(self.path.parent)
                    != self._parent_identity
                    or stat.S_ISLNK(visible.st_mode)
                    or not stat.S_ISREG(visible.st_mode)
                    or int(visible.st_nlink) != 1
                    or (int(visible.st_dev), int(visible.st_ino)) != identity
                    or not visible_sidecars_absent
                ):
                    raise N16ClaimLedgerError(
                        "N16 claim ledger published path identity changed"
                    )
                self._file_scope.bind_created(identity)
                os.fsync(parent_descriptor)
                closing_stage_descriptor = stage_descriptor
                stage_descriptor = None
                os.close(closing_stage_descriptor)
                os.rmdir(stage_name, dir_fd=parent_descriptor)
                stage_name = None
                final_maybe_linked = False
            except BaseException as operation_error:
                cleanup_errors = []  # type: list[BaseException]
                if cwd_changed and original_cwd_descriptor is not None:
                    try:
                        os.fchdir(original_cwd_descriptor)
                        cwd_changed = False
                    except BaseException as exc:
                        cleanup_errors.append(exc)
                if (
                    final_maybe_linked
                    and parent_descriptor is not None
                    and identity is not None
                ):
                    final_identity = None
                    final_found = False
                    try:
                        details = os.stat(
                            self.path.name,
                            dir_fd=parent_descriptor,
                            follow_symlinks=False,
                        )
                        final_found = True
                        final_identity = (
                            int(details.st_dev),
                            int(details.st_ino),
                        )
                        if (
                            not stat.S_ISREG(details.st_mode)
                            or int(details.st_nlink) not in (1, 2)
                        ):
                            final_identity = None
                    except FileNotFoundError:
                        pass
                    except BaseException as exc:
                        cleanup_errors.append(exc)
                    if final_identity == identity:
                        try:
                            os.unlink(
                                self.path.name,
                                dir_fd=parent_descriptor,
                            )
                        except BaseException as exc:
                            cleanup_errors.append(exc)
                    elif final_found:
                        cleanup_errors.append(
                            N16ClaimLedgerError(
                                "N16 claim ledger final path was replaced"
                            )
                        )
                if stage_descriptor is not None and identity is not None:
                    try:
                        self._remove_created_database(
                            stage_descriptor,
                            self.path.name,
                            identity,
                            owned_sidecars,
                        )
                    except BaseException as exc:
                        cleanup_errors.append(exc)
                if stage_descriptor is not None:
                    closing_stage_descriptor = stage_descriptor
                    stage_descriptor = None
                    try:
                        os.close(closing_stage_descriptor)
                    except BaseException as exc:
                        cleanup_errors.append(exc)
                if stage_name is not None and parent_descriptor is not None:
                    try:
                        os.rmdir(stage_name, dir_fd=parent_descriptor)
                        stage_name = None
                    except FileNotFoundError:
                        stage_name = None
                    except BaseException as exc:
                        cleanup_errors.append(exc)
                if cleanup_errors:
                    raise N16ClaimLedgerError(
                        "N16 claim ledger installation failed and private cleanup was incomplete"
                    ) from operation_error
                raise operation_error.with_traceback(
                    operation_error.__traceback__
                )
            finally:
                if cwd_changed and original_cwd_descriptor is not None:
                    try:
                        os.fchdir(original_cwd_descriptor)
                    except BaseException:
                        pass
                for descriptor in (
                    stage_descriptor,
                    parent_descriptor,
                    original_cwd_descriptor,
                ):
                    if descriptor is None:
                        continue
                    try:
                        os.close(descriptor)
                    except BaseException:
                        pass

    @staticmethod
    def _insert_claim(
        connection: sqlite3.Connection,
        claim: tuple[Any, ...],
        *,
        txn_id: str,
        claim_state: str,
        claim_sha256: str,
        chain_sha256: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO n16_permanent_claims (
                claim_ordinal, txn_id, claim_state, source_signal_id,
                schema_version, rule_version, strategy_id, source_scan_id,
                audit_id, review_ledger_id, state_id, symbol, episode_id,
                structure_id, signal_evidence_sha256,
                state_evidence_sha256, signal_created_at, claim_created_at,
                claim_sha256, chain_sha256
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                claim[0],
                txn_id,
                claim_state,
            )
            + claim[1:]
            + (claim_sha256, chain_sha256),
        )

    @classmethod
    def prepare_install(
        cls,
        path: str | os.PathLike[str],
        now: str,
        *,
        file_scope: N16ClaimLedgerFileScope | None = None,
    ) -> "N16PermanentClaimLedger":
        try:
            ledger = cls(path, file_scope=file_scope)
        except N16ClaimLedgerError:
            raise
        except Exception as exc:
            raise N16ClaimLedgerError(
                "N16 claim ledger path or sidecar already exists"
            ) from exc
        ledger._create(now=now)
        return ledger

    @staticmethod
    def _validate_summary(summary: N16ReviewClaimSummary) -> None:
        if (
            type(summary) is not N16ReviewClaimSummary
            or not _is_lower_hex(summary.review_install_sha256, 64)
            or type(summary.confirmed_claim_count) is not int
            or summary.confirmed_claim_count < 0
            or not _is_lower_hex(summary.confirmed_chain_sha256, 64)
        ):
            raise N16ClaimLedgerError("N16 review claim summary is invalid")
        if summary.confirmed_claim_count == 0:
            if (
                summary.first_claim_sha256 is not None
                or summary.last_claim_sha256 is not None
                or summary.confirmed_chain_sha256 != N16_CLAIM_CHAIN_SEED
            ):
                raise N16ClaimLedgerError("N16 empty claim summary is invalid")
        elif (
            not _is_lower_hex(summary.first_claim_sha256, 64)
            or not _is_lower_hex(summary.last_claim_sha256, 64)
            or summary.confirmed_chain_sha256 == N16_CLAIM_CHAIN_SEED
        ):
            raise N16ClaimLedgerError("N16 claim high-water summary is invalid")

    @staticmethod
    def _validate_publication_baseline(
        baseline: N16ReviewPublicationBaseline,
    ) -> None:
        if (
            type(baseline) is not N16ReviewPublicationBaseline
            or (
                baseline.current_scan_id is not None
                and (
                    type(baseline.current_scan_id) is not int
                    or baseline.current_scan_id <= 0
                )
            )
            or not _is_lower_hex(baseline.current_manifest_sha256, 64)
            or not _is_lower_hex(baseline.retention_sha256, 64)
            or not _is_lower_hex(baseline.staging_sha256, 64)
            or not _is_lower_hex(baseline.passed_claims_sha256, 64)
        ):
            raise N16ClaimLedgerError(
                "N16 Review publication baseline is invalid"
            )

    @classmethod
    def _validate_prepared_publication(
        cls,
        prepared: N16PreparedPublication,
    ) -> None:
        if (
            type(prepared) is not N16PreparedPublication
            or not _is_lower_hex(prepared.txn_id, 64)
            or type(prepared.scan_id) is not int
            or prepared.scan_id <= 0
            or not _is_lower_hex(prepared.batch_sha256, 64)
            or type(prepared.claim_count) is not int
            or prepared.claim_count <= 0
        ):
            raise N16ClaimLedgerError("N16 prepared publication is invalid")
        cls._validate_summary(prepared.base_summary)
        cls._validate_publication_baseline(prepared.review_baseline)

    @staticmethod
    def _meta(connection: sqlite3.Connection) -> tuple[Any, ...]:
        rows = connection.execute(
            """
            SELECT schema_version,strategy_id,rule_version,guard_sha256,
                   ledger_uuid,phase,review_install_sha256,
                   confirmed_claim_count,confirmed_first_claim_sha256,
                   confirmed_last_claim_sha256,confirmed_chain_sha256,
                   pending_txn_id,pending_scan_id,pending_batch_sha256,
                   pending_claim_count,pending_target_chain_sha256,
                   pending_base_current_scan_id,
                   pending_base_current_manifest_sha256,
                   pending_retention_sha256,pending_staging_sha256,
                   pending_passed_claims_sha256,updated_at
            FROM n16_claim_ledger_meta WHERE singleton_id=1
            """
        ).fetchall()
        if len(rows) != 1 or type(rows[0]) not in (tuple, list):
            raise N16ClaimLedgerError("N16 claim ledger metadata is missing")
        row = tuple(rows[0])
        if (
            len(row) != 22
            or row[0] != 1
            or type(row[0]) is not int
            or row[1] != "N16"
            or type(row[1]) is not str
            or row[2] != N16_RULE_VERSION
            or type(row[2]) is not str
            or row[3] != N16_GUARD_SHA256
            or type(row[3]) is not str
            or not _is_lower_hex(row[4], 64)
            or row[5] not in {"INSTALLING", "READY", "PREPARED"}
            or type(row[7]) is not int
            or row[7] < 0
            or not _is_lower_hex(row[10], 64)
            or type(row[14]) is not int
            or row[14] < 0
            or type(row[21]) is not str
            or not row[21]
        ):
            raise N16ClaimLedgerError("N16 claim ledger metadata is invalid")
        return row

    def _verify_schema(self, connection: sqlite3.Connection) -> None:
        application_id = connection.execute("PRAGMA application_id").fetchone()
        user_version = connection.execute("PRAGMA user_version").fetchone()
        if application_id != (N16_CLAIM_LEDGER_APPLICATION_ID,) or user_version != (
            N16_CLAIM_LEDGER_SCHEMA_VERSION,
        ):
            raise N16ClaimLedgerError("N16 claim ledger file identity is invalid")
        (
            expected_catalog,
            expected_tables,
            expected_indexes,
            expected_index_columns,
        ) = _n16_ledger_schema_reference()
        actual_catalog = tuple(
            (row[0], row[1], row[2], _normalized_sql(row[3]))
            for row in connection.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_schema "
                "WHERE name NOT LIKE 'sqlite_%' "
                "AND name NOT LIKE 'n16_legacy_witness_%' "
                "AND name NOT LIKE 'trg_n16_legacy_witness_%' "
                "AND name NOT LIKE 'n16_protected_generation_%' "
                "AND name NOT LIKE 'trg_n16_protected_generation_%' "
                "ORDER BY type,name COLLATE BINARY"
            ).fetchall()
        )
        if actual_catalog != expected_catalog:
            raise N16ClaimLedgerError(
                "N16 claim ledger catalog is inconsistent"
            )
        for table in ("n16_claim_ledger_meta", "n16_permanent_claims"):
            actual_table = tuple(
                tuple(row)
                for row in connection.execute(
                    'PRAGMA table_xinfo("%s")' % table
                ).fetchall()
            )
            if actual_table != expected_tables[table]:
                raise N16ClaimLedgerError(
                    "N16 claim ledger table metadata is inconsistent"
                )
            listed = connection.execute(
                'PRAGMA index_list("%s")' % table
            ).fetchall()
            actual_indexes = {
                row[1]: (row[2], row[3], row[4]) for row in listed
            }
            if actual_indexes != expected_indexes[table]:
                raise N16ClaimLedgerError(
                    "N16 claim ledger index metadata is inconsistent"
                )
            for name in actual_indexes:
                actual_columns = tuple(
                    tuple(row)
                    for row in connection.execute(
                        'PRAGMA index_xinfo("%s")' % name
                    ).fetchall()
                )
                if actual_columns != expected_index_columns[name]:
                    raise N16ClaimLedgerError(
                        "N16 claim ledger index columns are inconsistent"
                    )
            if connection.execute(
                'PRAGMA foreign_key_list("%s")' % table
            ).fetchall():
                raise N16ClaimLedgerError(
                    "N16 claim ledger must not contain foreign keys"
                )
        self._meta(connection)
        _legacy_witness_mirror_status(connection)
        _protected_generation_highwater_status(connection)

    @staticmethod
    def _maintenance_integrity_check(connection: sqlite3.Connection) -> None:
        integrity = connection.execute("PRAGMA integrity_check").fetchall()
        foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
        if integrity != [("ok",)] or foreign_keys:
            raise N16ClaimLedgerError(
                "N16 claim ledger integrity check failed"
            )

    @staticmethod
    def _claim_digest_at(
        connection: sqlite3.Connection,
        ordinal: int,
        required_state: str,
    ) -> str | None:
        rows = connection.execute(
            "SELECT claim_sha256 FROM n16_permanent_claims "
            "WHERE claim_ordinal=? AND claim_state=?",
            (ordinal, required_state),
        ).fetchall()
        if not rows:
            return None
        if len(rows) != 1 or not _is_lower_hex(rows[0][0], 64):
            raise N16ClaimLedgerError("N16 claim ledger row is invalid")
        return rows[0][0]

    def _attest_connection(
        self,
        connection: sqlite3.Connection,
        summary: N16ReviewClaimSummary,
    ) -> tuple[Any, ...]:
        self._validate_summary(summary)
        meta = self._meta(connection)
        if meta[5] != "READY":
            raise N16ClaimLedgerError(
                "N16 claim ledger has an unresolved two-phase publication"
            )
        if (
            meta[6] != summary.review_install_sha256
            or meta[7] != summary.confirmed_claim_count
            or meta[8] != summary.first_claim_sha256
            or meta[9] != summary.last_claim_sha256
            or meta[10] != summary.confirmed_chain_sha256
        ):
            raise N16ClaimLedgerError(
                "N16 review and independent claim ledger disagree"
            )
        if summary.confirmed_claim_count == 0:
            if connection.execute(
                "SELECT 1 FROM n16_permanent_claims LIMIT 1"
            ).fetchone() is not None:
                raise N16ClaimLedgerError("N16 empty claim ledger contains rows")
            if meta[10] != N16_CLAIM_CHAIN_SEED:
                raise N16ClaimLedgerError("N16 empty claim chain is invalid")
        else:
            first = self._claim_digest_at(connection, 1, "COMMITTED")
            last = self._claim_digest_at(
                connection, summary.confirmed_claim_count, "COMMITTED"
            )
            highest = connection.execute(
                "SELECT claim_ordinal,claim_sha256,chain_sha256 "
                "FROM n16_permanent_claims ORDER BY claim_ordinal DESC LIMIT 1"
            ).fetchone()
            if (
                first != summary.first_claim_sha256
                or last != summary.last_claim_sha256
                or highest is None
                or highest[0] != summary.confirmed_claim_count
                or highest[1] != summary.last_claim_sha256
                or highest[2] != meta[10]
                or highest[2] != summary.confirmed_chain_sha256
            ):
                raise N16ClaimLedgerError(
                    "N16 claim ledger high-water is inconsistent"
                )
        return meta

    def attest(self, summary: N16ReviewClaimSummary) -> None:
        with self._open(read_only=True) as connection:
            self._attest_connection(connection, summary)

    def _attest_installing_empty_connection(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        self._maintenance_integrity_check(connection)
        meta = self._meta(connection)
        if (
            meta[5] != "INSTALLING"
            or meta[6] is not None
            or meta[7] != 0
            or meta[8] is not None
            or meta[9] is not None
            or meta[10] != N16_CLAIM_CHAIN_SEED
            or any(value is not None for value in meta[11:14])
            or meta[14] != 0
            or any(value is not None for value in meta[15:21])
            or connection.execute(
                "SELECT 1 FROM n16_permanent_claims LIMIT 1"
            ).fetchone()
            is not None
        ):
            raise N16ClaimLedgerError(
                "N16 claim ledger installation baseline is inconsistent"
            )

    def attest_installing_empty(self) -> None:
        """Prove an interrupted installation is the canonical empty phase.

        This stopped-service check is deliberately read-only and includes the
        strict catalog/index verifier performed by ``_open`` plus SQLite's
        integrity checks.  No installation confirmation may mutate metadata
        until this exact empty baseline has been proved.
        """

        with self._open(read_only=True) as connection:
            self._attest_installing_empty_connection(connection)

    def legacy_witness_mirror(self) -> tuple[Any, ...] | None:
        """Return the strictly authenticated optional witness mirror."""

        with self._open(read_only=True) as connection:
            status = _legacy_witness_mirror_status(connection)
            if status == "PRE_MIRROR":
                return None
            return tuple(
                connection.execute(
                    "SELECT phase,review_plan_sha256,witness_sha256,"
                    "pre_review_canonical_sha256,authorization_sha256,"
                    "ledger_uuid,base_review_snapshot_sha256,"
                    "target_review_snapshot_sha256,base_catalog_sha256,"
                    "target_catalog_sha256,"
                    "prepared_at,committed_at "
                    "FROM n16_legacy_witness_mirror WHERE singleton_id=1"
                ).fetchone()
            )

    def ledger_uuid(self) -> str:
        with self._open(read_only=True) as connection:
            value = self._meta(connection)[4]
            if not _is_lower_hex(value, 64):
                raise N16ClaimLedgerError("N16 ledger UUID is invalid")
            return value

    def protected_generation_highwater(
        self,
    ) -> tuple[int, int, str] | None:
        """Return the independently sealed Review generation high-water."""

        with self._open(read_only=True) as connection:
            status = _protected_generation_highwater_status(connection)
            if status == "PRE_HIGHWATER":
                return None
            row = connection.execute(
                "SELECT generation,review_schema_version,"
                "family_catalog_sha256 "
                "FROM n16_protected_generation_highwater "
                "WHERE singleton_id=1"
            ).fetchone()
            if row is None:
                raise N16ClaimLedgerError(
                    "protected generation ledger highwater is missing"
                )
            return int(row[0]), int(row[1]), str(row[2])

    def install_protected_generation_highwater(
        self,
        *,
        generation: int,
        review_schema_version: int,
        family_catalog_sha256: str,
        now: str,
    ) -> None:
        if (
            type(generation) is not int
            or generation < 0
            or type(review_schema_version) is not int
            or review_schema_version <= 0
            or not _is_lower_hex(family_catalog_sha256, 64)
            or type(now) is not str
            or not 1 <= len(now) <= 64
        ):
            raise N16ClaimLedgerError(
                "protected generation install evidence is invalid"
            )
        with self._open(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            status = _protected_generation_highwater_status(connection)
            if status == "CURRENT":
                existing = connection.execute(
                    "SELECT generation,review_schema_version,"
                    "family_catalog_sha256 "
                    "FROM n16_protected_generation_highwater "
                    "WHERE singleton_id=1"
                ).fetchone()
                if existing != (
                    generation,
                    review_schema_version,
                    family_catalog_sha256,
                ):
                    raise N16ClaimLedgerError(
                        "protected generation install conflicts"
                    )
                connection.rollback()
                return
            connection.execute(
                _PROTECTED_GENERATION_HIGHWATER_INSTALLATION_SQL
            )
            connection.execute(_PROTECTED_GENERATION_HIGHWATER_SQL)
            for sql in (
                _PROTECTED_GENERATION_HIGHWATER_TRIGGER_SQL.values()
            ):
                connection.execute(sql)
            connection.execute(
                "INSERT INTO n16_protected_generation_highwater_installation "
                "VALUES (1,1,'N19_PROTECTED_GENERATION_LEDGER_V1',?)",
                (now,),
            )
            connection.execute(
                "INSERT INTO n16_protected_generation_highwater "
                "VALUES (1,?,?,?,?)",
                (
                    generation,
                    review_schema_version,
                    family_catalog_sha256,
                    now,
                ),
            )
            if _protected_generation_highwater_status(connection) != "CURRENT":
                raise N16ClaimLedgerError(
                    "protected generation ledger installation did not attest"
                )
            connection.commit()

    def advance_protected_generation_highwater(
        self,
        *,
        expected_generation: int,
        generation: int,
        review_schema_version: int,
        family_catalog_sha256: str,
        now: str,
    ) -> None:
        if (
            type(expected_generation) is not int
            or expected_generation < 0
            or type(generation) is not int
            or generation < expected_generation
            or type(review_schema_version) is not int
            or review_schema_version <= 0
            or not _is_lower_hex(family_catalog_sha256, 64)
            or type(now) is not str
            or not 1 <= len(now) <= 64
        ):
            raise N16ClaimLedgerError(
                "protected generation advance evidence is invalid"
            )
        with self._open(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            if _protected_generation_highwater_status(connection) != "CURRENT":
                raise N16ClaimLedgerError(
                    "protected generation ledger schema is missing"
                )
            existing_before = connection.execute(
                "SELECT generation,review_schema_version,"
                "family_catalog_sha256 "
                "FROM n16_protected_generation_highwater "
                "WHERE singleton_id=1"
            ).fetchone()
            if (
                existing_before is None
                or existing_before[0] != expected_generation
                or (
                    generation == expected_generation
                    and (
                        review_schema_version <= existing_before[1]
                        or (
                            review_schema_version == existing_before[1]
                            and family_catalog_sha256 == existing_before[2]
                        )
                    )
                )
                or (
                    generation > expected_generation
                    and (
                        review_schema_version != existing_before[1]
                        or family_catalog_sha256 != existing_before[2]
                    )
                )
            ):
                raise N16ClaimLedgerError(
                    "protected generation ledger advance conflicted"
                )
            changed = connection.execute(
                "UPDATE n16_protected_generation_highwater "
                "SET generation=?,review_schema_version=?,"
                "family_catalog_sha256=?,updated_at=? "
                "WHERE singleton_id=1 AND generation=? "
                "AND review_schema_version=? "
                "AND family_catalog_sha256=?",
                (
                    generation,
                    review_schema_version,
                    family_catalog_sha256,
                    now,
                    expected_generation,
                    existing_before[1],
                    existing_before[2],
                ),
            )
            if changed.rowcount != 1:
                existing = connection.execute(
                    "SELECT generation,review_schema_version,"
                    "family_catalog_sha256 "
                    "FROM n16_protected_generation_highwater "
                    "WHERE singleton_id=1"
                ).fetchone()
                if existing == (
                    generation,
                    review_schema_version,
                    family_catalog_sha256,
                ):
                    connection.rollback()
                    return
                raise N16ClaimLedgerError(
                    "protected generation ledger advance conflicted"
                )
            if connection.execute(
                "SELECT generation,review_schema_version,"
                "family_catalog_sha256 "
                "FROM n16_protected_generation_highwater "
                "WHERE singleton_id=1"
            ).fetchone() != (
                generation,
                review_schema_version,
                family_catalog_sha256,
            ):
                raise N16ClaimLedgerError(
                    "protected generation ledger advance did not attest"
                )
            connection.commit()

    def install_legacy_witness_mirror_schema(self, now: str) -> None:
        if type(now) is not str or not now:
            raise N16ClaimLedgerError(
                "legacy witness mirror install time is invalid"
            )
        with self._open(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            if _legacy_witness_mirror_status(connection) != "PRE_MIRROR":
                connection.rollback()
                return
            connection.execute(_LEGACY_WITNESS_MIRROR_INSTALLATION_SQL)
            connection.execute(_LEGACY_WITNESS_MIRROR_SQL)
            for sql in _LEGACY_WITNESS_MIRROR_TRIGGER_SQL.values():
                connection.execute(sql)
            connection.execute(
                "INSERT INTO n16_legacy_witness_mirror_installation "
                "VALUES (1,2,'AUTHORIZED_LEGACY_V3_UNBOUND_MIRROR_V2',?)",
                (now,),
            )
            connection.execute(
                "INSERT INTO n16_legacy_witness_mirror VALUES "
                "(1,'EMPTY',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,"
                "NULL,NULL)"
            )
            if _legacy_witness_mirror_status(connection) != "EMPTY":
                raise N16ClaimLedgerError(
                    "legacy witness mirror installation did not attest"
                )
            connection.commit()

    def prepare_legacy_witness_mirror(
        self,
        *,
        review_plan_sha256: str,
        witness_sha256: str,
        pre_review_canonical_sha256: str,
        authorization_sha256: str,
        ledger_uuid: str,
        base_review_snapshot_sha256: str,
        target_review_snapshot_sha256: str,
        base_catalog_sha256: str,
        target_catalog_sha256: str,
        now: str,
    ) -> None:
        values = (
            review_plan_sha256,
            witness_sha256,
            pre_review_canonical_sha256,
        )
        if (
            any(not _is_lower_hex(value, 64) for value in values)
            or authorization_sha256
            != "2dd9bf0e9a19d4464ff53ddb9bba35a850306499d12d88a34257541dcd0b04a1"
            or type(now) is not str
            or not now
            or any(
                not _is_lower_hex(value, 64)
                for value in (
                    ledger_uuid,
                    base_review_snapshot_sha256,
                    target_review_snapshot_sha256,
                    base_catalog_sha256,
                    target_catalog_sha256,
                )
            )
        ):
            raise N16ClaimLedgerError(
                "legacy witness mirror preparation is invalid"
            )
        with self._open(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            status = _legacy_witness_mirror_status(connection)
            if status == "PRE_MIRROR":
                raise N16ClaimLedgerError(
                    "legacy witness mirror schema is absent"
                )
            current = connection.execute(
                "SELECT phase,review_plan_sha256,witness_sha256,"
                "pre_review_canonical_sha256,authorization_sha256,"
                "ledger_uuid,base_review_snapshot_sha256,"
                "target_review_snapshot_sha256,base_catalog_sha256,"
                "target_catalog_sha256 "
                "FROM n16_legacy_witness_mirror WHERE singleton_id=1"
            ).fetchone()
            expected = (
                "PREPARED",
                review_plan_sha256,
                witness_sha256,
                pre_review_canonical_sha256,
                authorization_sha256,
                ledger_uuid,
                base_review_snapshot_sha256,
                target_review_snapshot_sha256,
                base_catalog_sha256,
                target_catalog_sha256,
            )
            if tuple(current) == expected:
                connection.rollback()
                return
            if current[0] != "EMPTY":
                raise N16ClaimLedgerError(
                    "legacy witness mirror has an unresolved phase"
                )
            changed = connection.execute(
                "UPDATE n16_legacy_witness_mirror SET phase='PREPARED',"
                "review_plan_sha256=?,witness_sha256=?,"
                "pre_review_canonical_sha256=?,authorization_sha256=?,"
                "ledger_uuid=?,base_review_snapshot_sha256=?,"
                "target_review_snapshot_sha256=?,base_catalog_sha256=?,"
                "target_catalog_sha256=?,"
                "prepared_at=?,committed_at=NULL "
                "WHERE singleton_id=1 AND phase='EMPTY'",
                (
                    *values,
                    authorization_sha256,
                    ledger_uuid,
                    base_review_snapshot_sha256,
                    target_review_snapshot_sha256,
                    base_catalog_sha256,
                    target_catalog_sha256,
                    now,
                ),
            )
            if changed.rowcount != 1:
                raise N16ClaimLedgerError(
                    "legacy witness mirror preparation conflicted"
                )
            if _legacy_witness_mirror_status(connection) != "PREPARED":
                raise N16ClaimLedgerError(
                    "legacy witness mirror preparation did not attest"
                )
            connection.commit()

    def abort_legacy_witness_mirror(
        self,
        *,
        review_plan_sha256: str,
        witness_sha256: str,
        now: str,
    ) -> None:
        if (
            not _is_lower_hex(review_plan_sha256, 64)
            or not _is_lower_hex(witness_sha256, 64)
            or type(now) is not str
            or not now
        ):
            raise N16ClaimLedgerError(
                "legacy witness mirror abort identity is invalid"
            )
        with self._open(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT phase,review_plan_sha256,witness_sha256 "
                "FROM n16_legacy_witness_mirror WHERE singleton_id=1"
            ).fetchone()
            if row == ("EMPTY", None, None):
                connection.rollback()
                return
            if row != (
                "PREPARED",
                review_plan_sha256,
                witness_sha256,
            ):
                raise N16ClaimLedgerError(
                    "legacy witness mirror abort conflicts"
                )
            changed = connection.execute(
                "UPDATE n16_legacy_witness_mirror SET phase='EMPTY',"
                "review_plan_sha256=NULL,witness_sha256=NULL,"
                "pre_review_canonical_sha256=NULL,authorization_sha256=NULL,"
                "ledger_uuid=NULL,base_review_snapshot_sha256=NULL,"
                "target_review_snapshot_sha256=NULL,base_catalog_sha256=NULL,"
                "target_catalog_sha256=NULL,"
                "prepared_at=NULL,committed_at=NULL "
                "WHERE singleton_id=1 AND phase='PREPARED' "
                "AND review_plan_sha256=? AND witness_sha256=?",
                (review_plan_sha256, witness_sha256),
            )
            if changed.rowcount != 1:
                raise N16ClaimLedgerError(
                    "legacy witness mirror abort conflicted"
                )
            if _legacy_witness_mirror_status(connection) != "EMPTY":
                raise N16ClaimLedgerError(
                    "legacy witness mirror abort did not attest"
                )
            connection.commit()

    def commit_legacy_witness_mirror(
        self,
        *,
        review_plan_sha256: str,
        witness_sha256: str,
        now: str,
    ) -> None:
        if (
            not _is_lower_hex(review_plan_sha256, 64)
            or not _is_lower_hex(witness_sha256, 64)
            or type(now) is not str
            or not now
        ):
            raise N16ClaimLedgerError(
                "legacy witness mirror commit identity is invalid"
            )
        with self._open(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT phase,review_plan_sha256,witness_sha256 "
                "FROM n16_legacy_witness_mirror WHERE singleton_id=1"
            ).fetchone()
            if row == (
                "COMMITTED",
                review_plan_sha256,
                witness_sha256,
            ):
                connection.rollback()
                return
            if row != (
                "PREPARED",
                review_plan_sha256,
                witness_sha256,
            ):
                raise N16ClaimLedgerError(
                    "legacy witness mirror commit conflicts"
                )
            changed = connection.execute(
                "UPDATE n16_legacy_witness_mirror SET phase='COMMITTED',"
                "committed_at=? WHERE singleton_id=1 AND phase='PREPARED' "
                "AND review_plan_sha256=? AND witness_sha256=?",
                (now, review_plan_sha256, witness_sha256),
            )
            if changed.rowcount != 1:
                raise N16ClaimLedgerError(
                    "legacy witness mirror commit conflicted"
                )
            if _legacy_witness_mirror_status(connection) != "COMMITTED":
                raise N16ClaimLedgerError(
                    "legacy witness mirror commit did not attest"
                )
            connection.commit()

    def attest_all(
        self,
        summary: N16ReviewClaimSummary,
        review_claims: Sequence[Sequence[Any]],
    ) -> None:
        """Stopped-service full claim comparison for maintenance/VACUUM."""

        self._validate_summary(summary)
        expected = tuple(validate_review_claim(row) for row in review_claims)
        if len(expected) != summary.confirmed_claim_count or any(
            claim[0] != ordinal
            for ordinal, claim in enumerate(expected, 1)
        ):
            raise N16ClaimLedgerError(
                "N16 Review full claim sequence is invalid"
            )
        with self._open(read_only=True) as connection:
            self._maintenance_integrity_check(connection)
            meta = self._attest_connection(connection, summary)
            rows = connection.execute(
                (
                    "SELECT %s,claim_sha256,chain_sha256,claim_state,txn_id "
                    "FROM n16_permanent_claims ORDER BY claim_ordinal"
                )
                % _CLAIM_SELECT
            ).fetchall()
            if len(rows) != len(expected):
                raise N16ClaimLedgerError(
                    "N16 full claim ledger count conflicts"
                )
            actual = tuple(
                self._validated_committed_claim_row(
                    connection, meta, row
                )
                for row in rows
            )
            if actual != expected:
                raise N16ClaimLedgerError(
                    "N16 Review and state full claim ledgers disagree"
                )

    def attest_prepared_base(
        self,
        prepared: N16PreparedPublication,
        review_claims: Sequence[Sequence[Any]],
    ) -> None:
        """Read-only full audit of committed claims below one PREPARED batch.

        Publication resolution is allowed to mutate the independent ledger
        only after every already-confirmed Review claim is proved identical.
        The pending claims are authenticated separately by ``prepared_target``
        and ``commit_prepared``.
        """

        self._validate_prepared_publication(prepared)
        expected = tuple(validate_review_claim(row) for row in review_claims)
        if len(expected) != prepared.base_summary.confirmed_claim_count or any(
            claim[0] != ordinal
            for ordinal, claim in enumerate(expected, 1)
        ):
            raise N16ClaimLedgerError(
                "N16 prepared Review base claim sequence is invalid"
            )
        with self._open(read_only=True) as connection:
            self._maintenance_integrity_check(connection)
            meta = self._meta(connection)
            if (
                meta[5] != "PREPARED"
                or meta[11] != prepared.txn_id
                or meta[12] != prepared.scan_id
                or meta[13] != prepared.batch_sha256
                or meta[14] != prepared.claim_count
                or meta[6]
                != prepared.base_summary.review_install_sha256
                or meta[7]
                != prepared.base_summary.confirmed_claim_count
                or meta[8] != prepared.base_summary.first_claim_sha256
                or meta[9] != prepared.base_summary.last_claim_sha256
                or meta[10]
                != prepared.base_summary.confirmed_chain_sha256
                or meta[16] != prepared.review_baseline.current_scan_id
                or meta[17]
                != prepared.review_baseline.current_manifest_sha256
                or meta[18] != prepared.review_baseline.retention_sha256
                or meta[19] != prepared.review_baseline.staging_sha256
                or meta[20]
                != prepared.review_baseline.passed_claims_sha256
            ):
                raise N16ClaimLedgerError(
                    "N16 prepared publication base metadata conflicts"
                )
            rows = connection.execute(
                (
                    "SELECT %s,claim_sha256,chain_sha256,claim_state,txn_id "
                    "FROM n16_permanent_claims "
                    "WHERE claim_state='COMMITTED' ORDER BY claim_ordinal"
                )
                % _CLAIM_SELECT
            ).fetchall()
            if len(rows) != len(expected):
                raise N16ClaimLedgerError(
                    "N16 prepared committed base count conflicts"
                )
            actual = tuple(
                self._validated_committed_claim_row(connection, meta, row)
                for row in rows
            )
            if actual != expected:
                raise N16ClaimLedgerError(
                    "N16 prepared Review and state bases disagree"
                )

    def confirm_install(
        self,
        summary: N16ReviewClaimSummary,
        now: str,
    ) -> None:
        self._validate_summary(summary)
        if summary.confirmed_claim_count != 0:
            raise N16ClaimLedgerError("N16 installation must start at zero claims")
        with self._open(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            meta = self._meta(connection)
            if meta[5] == "READY":
                self._maintenance_integrity_check(connection)
                self._attest_connection(connection, summary)
                connection.rollback()
                return
            self._attest_installing_empty_connection(connection)
            update = connection.execute(
                "UPDATE n16_claim_ledger_meta SET phase='READY', "
                "review_install_sha256=?, updated_at=? "
                "WHERE singleton_id=1 AND phase='INSTALLING'",
                (summary.review_install_sha256, now),
            )
            if update.rowcount != 1:
                raise N16ClaimLedgerError("N16 installation confirmation conflicted")
            connection.commit()
        self.attest(summary)

    def prepare_publication(
        self,
        summary: N16ReviewClaimSummary,
        review_baseline: N16ReviewPublicationBaseline,
        scan_id: int,
        batch_sha256: str,
        claims: Sequence[Sequence[Any]],
        now: str,
    ) -> N16PreparedPublication | None:
        self._validate_summary(summary)
        self._validate_publication_baseline(review_baseline)
        if type(scan_id) is not int or scan_id <= 0:
            raise N16ClaimLedgerError("N16 publication scan identity is invalid")
        if not _is_lower_hex(batch_sha256, 64):
            raise N16ClaimLedgerError("N16 publication batch identity is invalid")
        validated = tuple(validate_review_claim(row) for row in claims)
        if not validated:
            self.attest(summary)
            return None
        for index, claim in enumerate(validated, summary.confirmed_claim_count + 1):
            if claim[0] != index or claim[5] != scan_id:
                raise N16ClaimLedgerError("N16 prepared claim order conflicts")
        txn_id = secrets.token_hex(32)
        with self._open(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            meta = self._attest_connection(connection, summary)
            chain = meta[10]
            for claim in validated:
                digest = review_claim_sha256(claim)
                chain = _chain_sha256(chain, digest)
                self._insert_claim(
                    connection,
                    claim,
                    txn_id=txn_id,
                    claim_state="PREPARED",
                    claim_sha256=digest,
                    chain_sha256=chain,
                )
            update = connection.execute(
                """
                UPDATE n16_claim_ledger_meta
                SET phase='PREPARED', pending_txn_id=?, pending_scan_id=?,
                    pending_batch_sha256=?, pending_claim_count=?,
                    pending_target_chain_sha256=?,
                    pending_base_current_scan_id=?,
                    pending_base_current_manifest_sha256=?,
                    pending_retention_sha256=?, pending_staging_sha256=?,
                    pending_passed_claims_sha256=?, updated_at=?
                WHERE singleton_id=1 AND phase='READY'
                """,
                (
                    txn_id,
                    scan_id,
                    batch_sha256,
                    len(validated),
                    chain,
                    review_baseline.current_scan_id,
                    review_baseline.current_manifest_sha256,
                    review_baseline.retention_sha256,
                    review_baseline.staging_sha256,
                    review_baseline.passed_claims_sha256,
                    now,
                ),
            )
            if update.rowcount != 1:
                raise N16ClaimLedgerError("N16 publication preparation conflicted")
            connection.commit()
        return N16PreparedPublication(
            txn_id=txn_id,
            scan_id=scan_id,
            batch_sha256=batch_sha256,
            claim_count=len(validated),
            base_summary=summary,
            review_baseline=review_baseline,
        )

    def abort_prepared(
        self,
        prepared: N16PreparedPublication,
        now: str,
    ) -> None:
        self._validate_prepared_publication(prepared)
        with self._open(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            meta = self._meta(connection)
            if meta[5] == "READY":
                connection.rollback()
                self._attest_connection(connection, prepared.base_summary)
                return
            if (
                meta[5] != "PREPARED"
                or meta[11] != prepared.txn_id
                or meta[12] != prepared.scan_id
                or meta[13] != prepared.batch_sha256
                or meta[14] != prepared.claim_count
                or meta[6]
                != prepared.base_summary.review_install_sha256
                or meta[7]
                != prepared.base_summary.confirmed_claim_count
                or meta[8] != prepared.base_summary.first_claim_sha256
                or meta[9] != prepared.base_summary.last_claim_sha256
                or meta[10]
                != prepared.base_summary.confirmed_chain_sha256
                or meta[16] != prepared.review_baseline.current_scan_id
                or meta[17]
                != prepared.review_baseline.current_manifest_sha256
                or meta[18] != prepared.review_baseline.retention_sha256
                or meta[19] != prepared.review_baseline.staging_sha256
                or meta[20]
                != prepared.review_baseline.passed_claims_sha256
            ):
                raise N16ClaimLedgerError("N16 prepared publication conflicts")
            removed = connection.execute(
                "DELETE FROM n16_permanent_claims "
                "WHERE txn_id=? AND claim_state='PREPARED'",
                (prepared.txn_id,),
            )
            if removed.rowcount != prepared.claim_count:
                raise N16ClaimLedgerError("N16 prepared claim cleanup conflicts")
            update = connection.execute(
                """
                UPDATE n16_claim_ledger_meta
                SET phase='READY', pending_txn_id=NULL, pending_scan_id=NULL,
                    pending_batch_sha256=NULL, pending_claim_count=0,
                    pending_target_chain_sha256=NULL,
                    pending_base_current_scan_id=NULL,
                    pending_base_current_manifest_sha256=NULL,
                    pending_retention_sha256=NULL,
                    pending_staging_sha256=NULL,
                    pending_passed_claims_sha256=NULL, updated_at=?
                WHERE singleton_id=1 AND phase='PREPARED'
                  AND pending_txn_id=?
                """,
                (now, prepared.txn_id),
            )
            if update.rowcount != 1:
                raise N16ClaimLedgerError("N16 preparation rollback conflicted")
            connection.commit()
        self.attest(prepared.base_summary)

    def commit_prepared(
        self,
        prepared: N16PreparedPublication,
        summary: N16ReviewClaimSummary,
        now: str,
    ) -> None:
        self._validate_prepared_publication(prepared)
        self._validate_summary(summary)
        if (
            summary.review_install_sha256
            != prepared.base_summary.review_install_sha256
            or summary.confirmed_claim_count
            != prepared.base_summary.confirmed_claim_count + prepared.claim_count
        ):
            raise N16ClaimLedgerError("N16 committed review high-water conflicts")
        with self._open(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            meta = self._meta(connection)
            if meta[5] == "READY":
                connection.rollback()
                self._attest_connection(connection, summary)
                return
            if (
                meta[5] != "PREPARED"
                or meta[11] != prepared.txn_id
                or meta[12] != prepared.scan_id
                or meta[13] != prepared.batch_sha256
                or meta[14] != prepared.claim_count
                or meta[6]
                != prepared.base_summary.review_install_sha256
                or meta[7]
                != prepared.base_summary.confirmed_claim_count
                or meta[8] != prepared.base_summary.first_claim_sha256
                or meta[9] != prepared.base_summary.last_claim_sha256
                or meta[10]
                != prepared.base_summary.confirmed_chain_sha256
                or meta[16] != prepared.review_baseline.current_scan_id
                or meta[17]
                != prepared.review_baseline.current_manifest_sha256
                or meta[18] != prepared.review_baseline.retention_sha256
                or meta[19] != prepared.review_baseline.staging_sha256
                or meta[20]
                != prepared.review_baseline.passed_claims_sha256
            ):
                raise N16ClaimLedgerError("N16 prepared publication conflicts")
            rows = connection.execute(
                "SELECT %s,claim_sha256,chain_sha256 "
                "FROM n16_permanent_claims "
                "WHERE txn_id=? AND claim_state='PREPARED' "
                "ORDER BY claim_ordinal" % _CLAIM_SELECT,
                (prepared.txn_id,),
            ).fetchall()
            all_pending = connection.execute(
                "SELECT COUNT(*),COUNT(DISTINCT txn_id) "
                "FROM n16_permanent_claims WHERE claim_state='PREPARED'"
            ).fetchone()
            chain = prepared.base_summary.confirmed_chain_sha256
            first_pending_digest = None
            last_pending_digest = None
            if (
                len(rows) != prepared.claim_count
                or all_pending != (prepared.claim_count, 1)
            ):
                raise N16ClaimLedgerError("N16 prepared claim evidence conflicts")
            for offset, row in enumerate(rows, 1):
                if type(row) not in (tuple, list) or len(row) != 18:
                    raise N16ClaimLedgerError(
                        "N16 prepared claim evidence conflicts"
                    )
                claim = validate_review_claim(row[:16])
                expected_ordinal = (
                    prepared.base_summary.confirmed_claim_count + offset
                )
                digest = review_claim_sha256(claim)
                chain = _chain_sha256(chain, digest)
                if (
                    claim[0] != expected_ordinal
                    or claim[5] != prepared.scan_id
                    or row[16] != digest
                    or row[17] != chain
                ):
                    raise N16ClaimLedgerError(
                        "N16 prepared claim chain is inconsistent"
                    )
                if first_pending_digest is None:
                    first_pending_digest = digest
                last_pending_digest = digest
            if (
                chain != meta[15]
                or chain != summary.confirmed_chain_sha256
                or last_pending_digest != summary.last_claim_sha256
                or (
                    prepared.base_summary.confirmed_claim_count == 0
                    and first_pending_digest != summary.first_claim_sha256
                )
            ):
                raise N16ClaimLedgerError("N16 prepared claim evidence conflicts")
            activated = connection.execute(
                "UPDATE n16_permanent_claims SET claim_state='COMMITTED' "
                "WHERE txn_id=? AND claim_state='PREPARED'",
                (prepared.txn_id,),
            )
            if activated.rowcount != prepared.claim_count:
                raise N16ClaimLedgerError("N16 claim confirmation count conflicts")
            first = (
                summary.first_claim_sha256
                if prepared.base_summary.confirmed_claim_count == 0
                else prepared.base_summary.first_claim_sha256
            )
            update = connection.execute(
                """
                UPDATE n16_claim_ledger_meta
                SET phase='READY', confirmed_claim_count=?,
                    confirmed_first_claim_sha256=?,
                    confirmed_last_claim_sha256=?,
                    confirmed_chain_sha256=?, pending_txn_id=NULL,
                    pending_scan_id=NULL, pending_batch_sha256=NULL,
                    pending_claim_count=0,
                    pending_target_chain_sha256=NULL,
                    pending_base_current_scan_id=NULL,
                    pending_base_current_manifest_sha256=NULL,
                    pending_retention_sha256=NULL,
                    pending_staging_sha256=NULL,
                    pending_passed_claims_sha256=NULL, updated_at=?
                WHERE singleton_id=1 AND phase='PREPARED'
                  AND pending_txn_id=?
                """,
                (
                    summary.confirmed_claim_count,
                    first,
                    summary.last_claim_sha256,
                    chain,
                    now,
                    prepared.txn_id,
                ),
            )
            if update.rowcount != 1:
                raise N16ClaimLedgerError("N16 claim confirmation conflicted")
            connection.commit()
        self.attest(summary)

    def prepared_publication(self) -> N16PreparedPublication | None:
        """Return a strictly authenticated pending token for maintenance."""

        with self._open(read_only=True) as connection:
            meta = self._meta(connection)
            if meta[5] == "READY":
                return None
            if meta[5] != "PREPARED":
                raise N16ClaimLedgerError(
                    "N16 claim ledger installation is incomplete"
                )
            summary = N16ReviewClaimSummary(
                review_install_sha256=meta[6],
                confirmed_claim_count=meta[7],
                first_claim_sha256=meta[8],
                last_claim_sha256=meta[9],
                confirmed_chain_sha256=meta[10],
            )
            baseline = N16ReviewPublicationBaseline(
                current_scan_id=meta[16],
                current_manifest_sha256=meta[17],
                retention_sha256=meta[18],
                staging_sha256=meta[19],
                passed_claims_sha256=meta[20],
            )
            prepared = N16PreparedPublication(
                txn_id=meta[11],
                scan_id=meta[12],
                batch_sha256=meta[13],
                claim_count=meta[14],
                base_summary=summary,
                review_baseline=baseline,
            )
            self._validate_prepared_publication(prepared)
            pending = connection.execute(
                "SELECT COUNT(*),MIN(claim_ordinal),MAX(claim_ordinal),"
                "MAX(chain_sha256) FILTER (WHERE claim_ordinal=("
                " SELECT MAX(claim_ordinal) FROM n16_permanent_claims "
                " WHERE txn_id=? AND claim_state='PREPARED')) "
                "FROM n16_permanent_claims "
                "WHERE txn_id=? AND claim_state='PREPARED'",
                (prepared.txn_id, prepared.txn_id),
            ).fetchone()
            expected_first = summary.confirmed_claim_count + 1
            expected_last = summary.confirmed_claim_count + prepared.claim_count
            if (
                pending is None
                or pending[0] != prepared.claim_count
                or pending[1] != expected_first
                or pending[2] != expected_last
                or pending[3] != meta[15]
            ):
                raise N16ClaimLedgerError(
                    "N16 pending claim evidence is incomplete"
                )
            return prepared

    def prepared_target(
        self,
        prepared: N16PreparedPublication,
    ) -> tuple[tuple[tuple[Any, ...], ...], N16ReviewClaimSummary]:
        """Return exact pending claims and their required Review summary."""

        self._validate_prepared_publication(prepared)
        with self._open(read_only=True) as connection:
            meta = self._meta(connection)
            if (
                meta[5] != "PREPARED"
                or meta[11] != prepared.txn_id
                or meta[12] != prepared.scan_id
                or meta[13] != prepared.batch_sha256
                or meta[14] != prepared.claim_count
                or meta[16] != prepared.review_baseline.current_scan_id
                or meta[17]
                != prepared.review_baseline.current_manifest_sha256
                or meta[18] != prepared.review_baseline.retention_sha256
                or meta[19] != prepared.review_baseline.staging_sha256
                or meta[20]
                != prepared.review_baseline.passed_claims_sha256
            ):
                raise N16ClaimLedgerError(
                    "N16 prepared publication metadata conflicts"
                )
            rows = connection.execute(
                "SELECT %s,claim_sha256,chain_sha256 "
                "FROM n16_permanent_claims "
                "WHERE txn_id=? AND claim_state='PREPARED' "
                "ORDER BY claim_ordinal" % _CLAIM_SELECT,
                (prepared.txn_id,),
            ).fetchall()
            if len(rows) != prepared.claim_count:
                raise N16ClaimLedgerError(
                    "N16 prepared claim count is incomplete"
                )
            claims = []
            chain = prepared.base_summary.confirmed_chain_sha256
            first_digest = prepared.base_summary.first_claim_sha256
            last_digest = prepared.base_summary.last_claim_sha256
            for offset, row in enumerate(rows, 1):
                if type(row) not in (tuple, list) or len(row) != 18:
                    raise N16ClaimLedgerError(
                        "N16 prepared claim row is invalid"
                    )
                claim = validate_review_claim(row[:16])
                expected_ordinal = (
                    prepared.base_summary.confirmed_claim_count + offset
                )
                digest = review_claim_sha256(claim)
                chain = _chain_sha256(chain, digest)
                if (
                    claim[0] != expected_ordinal
                    or row[16] != digest
                    or row[17] != chain
                ):
                    raise N16ClaimLedgerError(
                        "N16 prepared claim chain is inconsistent"
                    )
                if first_digest is None:
                    first_digest = digest
                last_digest = digest
                claims.append(claim)
            if chain != meta[15]:
                raise N16ClaimLedgerError(
                    "N16 prepared target chain conflicts"
                )
            return tuple(claims), N16ReviewClaimSummary(
                review_install_sha256=(
                    prepared.base_summary.review_install_sha256
                ),
                confirmed_claim_count=(
                    prepared.base_summary.confirmed_claim_count
                    + prepared.claim_count
                ),
                first_claim_sha256=first_digest,
                last_claim_sha256=last_digest,
                confirmed_chain_sha256=chain,
            )

    @staticmethod
    def _validated_committed_claim_row(
        connection: sqlite3.Connection,
        meta: tuple[Any, ...],
        row: Sequence[Any],
    ) -> tuple[Any, ...]:
        if type(row) not in (tuple, list) or len(row) != 20:
            raise N16ClaimLedgerError("N16 committed claim shape is invalid")
        claim = validate_review_claim(row[:16])
        digest = review_claim_sha256(claim)
        if (
            row[16] != digest
            or not _is_lower_hex(row[17], 64)
            or row[18] != "COMMITTED"
            or not _is_lower_hex(row[19], 64)
            or claim[0] > meta[7]
        ):
            raise N16ClaimLedgerError("N16 committed claim is inconsistent")
        if claim[0] == 1:
            previous_chain = N16_CLAIM_CHAIN_SEED
        else:
            previous = connection.execute(
                "SELECT chain_sha256 FROM n16_permanent_claims "
                "WHERE claim_ordinal=? AND claim_state='COMMITTED'",
                (claim[0] - 1,),
            ).fetchall()
            if len(previous) != 1 or not _is_lower_hex(previous[0][0], 64):
                raise N16ClaimLedgerError(
                    "N16 committed claim predecessor is inconsistent"
                )
            previous_chain = previous[0][0]
        if row[17] != _chain_sha256(previous_chain, digest):
            raise N16ClaimLedgerError("N16 committed claim chain is inconsistent")
        if claim[0] == 1 and digest != meta[8]:
            raise N16ClaimLedgerError("N16 first committed claim conflicts")
        if claim[0] == meta[7] and (
            digest != meta[9] or row[17] != meta[10]
        ):
            raise N16ClaimLedgerError("N16 committed high-water conflicts")
        return claim

    def committed_claim(
        self,
        structure_id: str,
    ) -> tuple[Any, ...] | None:
        if not _is_lower_hex(structure_id, 24):
            raise N16ClaimLedgerError("N16 structure identity is invalid")
        with self._open(read_only=True) as connection:
            meta = self._meta(connection)
            if meta[5] != "READY":
                raise N16ClaimLedgerError(
                    "N16 claim ledger has an unresolved publication"
                )
            rows = connection.execute(
                (
                    "SELECT %s,claim_sha256,chain_sha256,claim_state,txn_id "
                    "FROM n16_permanent_claims "
                    "WHERE structure_id=? AND claim_state='COMMITTED'"
                )
                % _CLAIM_SELECT,
                (structure_id,),
            ).fetchall()
            if not rows:
                return None
            if len(rows) != 1:
                raise N16ClaimLedgerError("N16 structure claim is inconsistent")
            return self._validated_committed_claim_row(
                connection, meta, rows[0]
            )

    def committed_claim_by_source_signal_id(
        self,
        source_signal_id: int,
    ) -> tuple[Any, ...] | None:
        if type(source_signal_id) is not int or source_signal_id <= 0:
            raise N16ClaimLedgerError(
                "N16 source signal identity is invalid"
            )
        with self._open(read_only=True) as connection:
            meta = self._meta(connection)
            if meta[5] != "READY":
                raise N16ClaimLedgerError(
                    "N16 claim ledger has an unresolved publication"
                )
            rows = connection.execute(
                (
                    "SELECT %s,claim_sha256,chain_sha256,claim_state,txn_id "
                    "FROM n16_permanent_claims "
                    "WHERE source_signal_id=? AND claim_state='COMMITTED'"
                )
                % _CLAIM_SELECT,
                (source_signal_id,),
            ).fetchall()
            if not rows:
                return None
            if len(rows) != 1:
                raise N16ClaimLedgerError(
                    "N16 source signal claim is inconsistent"
                )
            return self._validated_committed_claim_row(
                connection, meta, rows[0]
            )

    def attest_committed_claim(
        self,
        review_claim: Sequence[Any],
    ) -> None:
        expected = validate_review_claim(review_claim)
        actual = self.committed_claim(expected[11])
        if actual is None or actual != expected:
            raise N16ClaimLedgerError(
                "N16 Review claim and independent ledger disagree"
            )

    def committed_claim_sha256(self, structure_id: str) -> str | None:
        claim = self.committed_claim(structure_id)
        return review_claim_sha256(claim) if claim is not None else None

    def metadata_phase(self) -> str:
        with self._open(read_only=True) as connection:
            return self._meta(connection)[5]
