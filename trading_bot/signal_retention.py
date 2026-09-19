"""Offline, one-shot maintenance for bounded ``strategy_signals`` retention.

This module is deliberately scoped to the Binance application's own SQLite
database (and, through the CLI, its own instance-lock file and optional VACUUM
destination).  It never reads ``.env``, changes journald/logrotate/Python/
SQLite system configuration, controls services, or touches any other product's
ports, directories, databases, or logs.

Normal bot startup must not call this module.  The migration is intended for a
stopped-service maintenance window and is restartable after any committed
deletion batch.  ``VACUUM INTO`` is a separate, explicit operation and is never
performed by :func:`apply_signal_retention_maintenance`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import secrets
import sqlite3
import stat
import sys
import tempfile
import threading
import time
from contextlib import ExitStack, closing, contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from .instance_lock import (
    InstanceLock,
    InstanceLockError,
    validate_official_instance_lock,
)
from .exchange_symbol import canonical_exchange_symbol
from .n16_claim_ledger import N16ClaimLedgerError
from .recorder import (
    _N16_RETENTION_TRIGGER_TABLES,
    _N16_TRIGGER_SQL,
    _n16_normalized_sql,
    _n16_runtime_schema_status,
    _validate_n16_consumption_seal,
    _validate_n16_passed_lifecycle,
    _strict_strategy_signal_retention_row,
    _strategy_signal_retention_row,
    _delete_staged_strategy_signal_batch,
    _micro_staged_lifecycle_snapshot,
    _strategy_signal_batch_snapshot,
    _strategy_signal_claim_snapshot,
    _strategy_signal_published_snapshot,
    _validate_complete_strategy_signal_graph,
    _expected_database_fd_increment_is_attested,
    utc_now,
)


_LEDGER_STRATEGIES = frozenset(
    {
        "N06", "N07", "N08", "N11", "N12", "N16", "N17", "N18",
        "N19", "N20", "N21", "N22", "N23", "N24", "N25",
    }
)
_POST_LEGACY_RETENTION_STRATEGIES = frozenset(
    {"N16", "N17", "N18", "N19", "N20", "N21", "N22", "N23", "N24", "N25"}
)
_STRATEGY_IDS = frozenset(
    ["N%02d" % number for number in range(1, 26)]
    + ["LEGACY_SINGLE"]
)
_MANIFEST_SEED = "0" * 64
_SQLITE_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
_VACUUM_TARGET_CWD_LOCK = threading.Lock()
_MAINTENANCE_SQLITE_CONNECTION_LOCK = threading.RLock()
_RETENTION_MUTATION_TABLES = frozenset(
    {
        "strategy_signals",
        "strategy_signal_batches",
        "strategy_signal_current",
        "strategy_passed_signal_audits",
        "strategy_passed_structure_ledger",
    }
)


def _validate_n16_maintenance_boundary(connection: sqlite3.Connection) -> str:
    try:
        return _n16_runtime_schema_status(connection)
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N16 lifecycle schema requires explicit repair before retention maintenance"
        ) from exc


def _required_n16_claim_ledger_path(
    database: Path,
    claim_ledger_file: str,
) -> Path:
    if type(claim_ledger_file) is not str or not claim_ledger_file:
        raise SignalRetentionMaintenanceError(
            "an explicit N16 claim ledger path is required"
        )
    path = Path(claim_ledger_file)
    if not path.is_absolute() or path == database:
        raise SignalRetentionMaintenanceError(
            "N16 claim ledger must be an absolute file separate from Review"
        )
    return path


def _attest_n16_claim_boundary(
    connection: sqlite3.Connection,
    claim_ledger,
):
    """Strictly attest one CURRENT Review snapshot against the state ledger."""

    from .recorder import (
        _validate_n16_permanent_graph,
        _n16_all_review_claims,
        _n16_review_claim_summary,
    )

    if _validate_n16_maintenance_boundary(connection) != "CURRENT":
        raise SignalRetentionMaintenanceError(
            "N16 lifecycle installation is required before maintenance"
        )
    if not claim_ledger.exists:
        raise SignalRetentionMaintenanceError(
            "N16 claim ledger is required before maintenance"
        )
    try:
        _validate_n16_permanent_graph(connection)
        summary = _n16_review_claim_summary(connection)
        claim_ledger.attest_all(
            summary,
            _n16_all_review_claims(connection),
        )
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N16 Review and state claim ledger are inconsistent"
        ) from exc
    return summary


def _attest_n17_lifecycle_boundary(connection: sqlite3.Connection) -> None:
    """Require the exact stopped-service N17 lifecycle generation."""

    from .n17_schema import n17_schema_status

    try:
        if n17_schema_status(connection) != "CURRENT":
            raise RuntimeError("pre-N17 Review schema")
        _n17_evidence_repair_plan(connection, permit_legacy_repair=False)
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N17 lifecycle installation is required before maintenance"
        ) from exc


def _n17_evidence_repair_plan(
    connection: sqlite3.Connection,
    *,
    permit_legacy_repair: bool,
) -> list[tuple[int, str, Any]]:
    """Validate every N17 row and return unique legacy live-tail repairs."""

    from .n17_analyzer import (
        decode_n17_state_evidence,
        repair_n17_legacy_live_tail_evidence,
    )

    rows = connection.execute(
        "SELECT id,strategy_id,symbol,family_id,structure_id,stage,reason,"
        "quote_volume_rank,box_start_time_ms,box_end_time_ms,"
        "reset_after_time_ms,evidence_json,evidence_sha256 "
        "FROM n17_range_support_states ORDER BY id"
    )
    repairs: list[tuple[int, str, Any]] = []
    for row in rows:
        if len(row) != 13 or type(row[0]) is not int or type(row[11]) is not str:
            raise RuntimeError("N17 lifecycle row shape is invalid")
        evidence_json = row[11]
        if (
            type(row[12]) is not str
            or hashlib.sha256(evidence_json.encode("utf-8")).hexdigest()
            != row[12]
        ):
            raise RuntimeError("N17 lifecycle outer evidence hash is invalid")
        try:
            decoded = decode_n17_state_evidence(
                evidence_json, expected_symbol=row[2]
            )
        except ValueError:
            if not permit_legacy_repair:
                raise
            decoded = repair_n17_legacy_live_tail_evidence(
                evidence_json, expected_symbol=row[2]
            )
            if decoded.evidence_json == evidence_json:
                raise RuntimeError("N17 repair did not change invalid evidence")
            repairs.append((row[0], row[12], decoded))
        if (
            row[1] != decoded.strategy_id
            or row[2] != decoded.symbol
            or row[3] != decoded.family_id
            or row[4] != decoded.structure_id
            or row[5] != decoded.stage
            or row[6] != decoded.reason
            or row[7] != decoded.quote_volume_rank
            or row[8] != decoded.box_start_time_ms
            or row[9] != decoded.box_end_time_ms
            or row[10] != decoded.reset_after_time_ms
        ):
            raise RuntimeError("N17 lifecycle columns conflict with evidence")
    return repairs


def _attest_n19_lifecycle_boundary(connection: sqlite3.Connection) -> None:
    """Require the exact stopped-service N19 lifecycle generation."""

    from .n19_schema import n19_schema_status

    try:
        if n19_schema_status(connection) != "CURRENT":
            raise RuntimeError("pre-N19 Review schema")
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N19 lifecycle installation is required before maintenance"
        ) from exc


def _attest_n18_lifecycle_boundary(connection: sqlite3.Connection) -> None:
    """Require the exact stopped-service N18 lifecycle generation."""

    from .n18_schema import n18_schema_status

    try:
        if n18_schema_status(connection) != "CURRENT":
            raise RuntimeError("pre-N18 Review schema")
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N18 lifecycle installation is required before maintenance"
        ) from exc


def _attest_n20_lifecycle_boundary(connection: sqlite3.Connection) -> None:
    """Require the exact stopped-service N20 lifecycle generation."""

    from .n20_schema import n20_schema_status

    try:
        if n20_schema_status(connection) != "CURRENT":
            raise RuntimeError("pre-N20 Review schema")
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N20 lifecycle installation is required before maintenance"
        ) from exc


def _attest_micro_lifecycle_boundary(connection: sqlite3.Connection) -> None:
    """Require the exact stopped-service N21-N25 lifecycle generation."""

    from .micro_schema import (
        micro_schema_status,
        validate_micro_lifecycle_graph,
        validate_pre_micro_review_clean,
    )

    try:
        if micro_schema_status(connection) != "CURRENT":
            raise RuntimeError("pre-N21-N25 Review schema")
        # Stopped-service maintenance may scan the permanent graph.  Do this
        # before any retention/VACUUM mutation so a one-sided raw/lifecycle/
        # audit/ledger corruption cannot be carried into a compacted database.
        validate_micro_lifecycle_graph(connection)
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N21-N25 lifecycle installation is required before maintenance"
        ) from exc


def _attest_strategy_lifecycle_boundaries(
    connection: sqlite3.Connection,
    claim_ledger,
):
    from .coverage_epoch_schema import (
        coverage_epoch_schema_status,
    )
    from .coverage_family_seal import (
        family_seal_catalog_sha256,
        family_seal_schema_status,
        validate_family_seal_graph,
    )
    from .n15_terminal_schema import n15_terminal_schema_status

    summary = _attest_n16_claim_boundary(connection, claim_ledger)
    _attest_n17_lifecycle_boundary(connection)
    _attest_n19_lifecycle_boundary(connection)
    _attest_n18_lifecycle_boundary(connection)
    _attest_n20_lifecycle_boundary(connection)
    _attest_micro_lifecycle_boundary(connection)
    try:
        if coverage_epoch_schema_status(connection) not in {
            "CURRENT",
            "AUTHORIZED_LEGACY_V3",
        }:
            raise RuntimeError("coverage epoch generation is not CURRENT")
        if family_seal_schema_status(connection) != "CURRENT":
            raise RuntimeError("coverage family seal generation is not CURRENT")
        if n15_terminal_schema_status(connection) != "CURRENT":
            raise RuntimeError("N15 terminal receipt generation is not CURRENT")
        validate_family_seal_graph(connection)
        generation_row = connection.execute(
            "SELECT generation FROM history_coverage_protected_generation "
            "WHERE singleton_id=1"
        ).fetchone()
        schema_version_row = connection.execute(
            "PRAGMA schema_version"
        ).fetchone()
        highwater = claim_ledger.protected_generation_highwater()
        if (
            generation_row is None
            or len(generation_row) != 1
            or type(generation_row[0]) is not int
            or schema_version_row is None
            or len(schema_version_row) != 1
            or type(schema_version_row[0]) is not int
        ):
            raise RuntimeError(
                "coverage protected generation ledger conflicts"
            )
        review_commitment = (
            generation_row[0],
            schema_version_row[0],
            family_seal_catalog_sha256(connection),
        )
        if highwater != review_commitment:
            raise RuntimeError(
                "coverage protected generation ledger conflicts"
            )
        witnesses = connection.execute(
            "SELECT review_plan_sha256,witness_sha256,"
            "pre_review_canonical_sha256 "
            "FROM history_coverage_n19_legacy_unbound_witnesses "
            "ORDER BY witness_id"
        ).fetchall()
        mirror = claim_ledger.legacy_witness_mirror()
        if mirror is None:
            raise RuntimeError("coverage family seal mirror is absent")
        if (
            (not witnesses and mirror[0] != "EMPTY")
            or (
                witnesses
                and (
                    len(witnesses) != 1
                    or mirror[0] != "COMMITTED"
                    or tuple(witnesses[0])
                    != (mirror[1], mirror[2], mirror[3])
                    or mirror[5] != claim_ledger.ledger_uuid()
                )
            )
        ):
            raise RuntimeError("coverage family seal mirror conflicts")
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "history coverage epoch graph is inconsistent"
        ) from exc
    return summary


def _advance_protected_generation_after_explicit_maintenance(
    connection: sqlite3.Connection,
    claim_ledger,
) -> None:
    """Seal one fully validated forward-only Review catalog transition."""

    from .coverage_family_seal import (
        family_seal_catalog_sha256,
        family_seal_schema_status,
        validate_family_seal_graph,
    )

    highwater = claim_ledger.protected_generation_highwater()
    if highwater is None:
        return
    if family_seal_schema_status(
        connection, validate_graph=False
    ) != "CURRENT":
        raise SignalRetentionMaintenanceError(
            "protected generation maintenance requires exact family schema"
        )
    generation_row = connection.execute(
        "SELECT generation FROM history_coverage_protected_generation "
        "WHERE singleton_id=1"
    ).fetchone()
    schema_version_row = connection.execute(
        "PRAGMA schema_version"
    ).fetchone()
    if (
        generation_row is None
        or len(generation_row) != 1
        or type(generation_row[0]) is not int
        or schema_version_row is None
        or len(schema_version_row) != 1
        or type(schema_version_row[0]) is not int
    ):
        raise SignalRetentionMaintenanceError(
            "protected generation maintenance evidence is invalid"
        )
    target = (
        generation_row[0],
        schema_version_row[0],
        family_seal_catalog_sha256(connection),
    )
    if target == highwater:
        return
    if target[0] < highwater[0]:
        raise SignalRetentionMaintenanceError(
            "protected generation maintenance is not forward-only"
        )
    if target[0] == highwater[0]:
        if target[1] <= highwater[1]:
            raise SignalRetentionMaintenanceError(
                "protected generation maintenance is not forward-only"
            )
    elif (
        target[1] != highwater[1]
        or target[2] != highwater[2]
    ):
        # A stopped-service recovery may seal a fully validated Review
        # generation that committed before its independent-ledger
        # acknowledgement.  It must not simultaneously normalize a catalog
        # transition; catalog changes use the same-generation maintenance
        # branch above.
        raise SignalRetentionMaintenanceError(
            "protected generation maintenance pair is inconsistent"
        )
    validate_family_seal_graph(connection)
    claim_ledger.advance_protected_generation_highwater(
        expected_generation=highwater[0],
        generation=target[0],
        review_schema_version=target[1],
        family_catalog_sha256=target[2],
        now=utc_now(),
    )
    if claim_ledger.protected_generation_highwater() != target:
        raise SignalRetentionMaintenanceError(
            "protected generation maintenance commitment did not persist"
        )


def _install_n16_claim_boundary(
    database: Path,
    claim_ledger_file: Path,
    *,
    expected_database_identity: Optional[Tuple[int, int]] = None,
    database_scope: Optional["_DatabaseFileScope"] = None,
    claim_ledger_scope=None,
):
    """Explicit stopped-service N16 installation/resume entry.

    Ordinary :class:`ReviewRecorder` construction never calls this function.
    A PRE_N16 Review file may create one INSTALLING ledger and then install the
    Review schema.  CURRENT plus a missing ledger is always treated as loss,
    never as permission to bootstrap from Review history.
    """

    from .n16_claim_ledger import N16PermanentClaimLedger
    from .recorder import (
        ReviewRecorder,
        _n16_all_review_claims,
        _n16_review_claim_summary,
        _validate_n16_installing_review_empty,
        _validate_n16_preinstall_review_clean,
    )

    database = Path(database)
    claim_ledger_file = Path(claim_ledger_file)
    if not database.is_absolute() or not claim_ledger_file.is_absolute():
        raise SignalRetentionMaintenanceError(
            "N16 Review and claim ledger paths must be absolute"
        )
    if database == claim_ledger_file:
        raise SignalRetentionMaintenanceError(
            "N16 Review and claim ledger must be separate files"
        )
    ledger = N16PermanentClaimLedger(
        claim_ledger_file,
        file_scope=claim_ledger_scope,
    )
    if not os.path.lexists(str(database)):
        # The production CLI always starts from an existing, identity-attested
        # Review file.  Refuse a missing main file instead of selecting a new
        # parent after the independent ledger has been created.  A genuinely
        # fresh deployment must first provision its empty Review file through
        # the same anchored deployment boundary and then invoke maintenance.
        raise SignalRetentionMaintenanceError(
            "N16 installation requires an existing attested Review database"
        )
    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
            status = _validate_n16_maintenance_boundary(connection)
            if status == "PRE_N16":
                try:
                    _validate_n16_preinstall_review_clean(connection)
                except RuntimeError as exc:
                    raise SignalRetentionMaintenanceError(
                        "pre-N16 Review contains lifecycle evidence"
                    ) from exc
            if status == "CURRENT":
                if not os.path.lexists(str(claim_ledger_file)):
                    raise SignalRetentionMaintenanceError(
                        "post-N16 Review is missing its state claim ledger"
                    )
                phase = ledger.metadata_phase()
                if phase == "READY":
                    return _attest_n16_claim_boundary(
                        connection,
                        ledger,
                    )
                if phase == "INSTALLING":
                    # This is the only resumable post-schema installation
                    # state.  Prove both failure domains in full before the
                    # phase transition: the Review permanent graph must be
                    # canonically empty, and the independent ledger must have
                    # the exact catalog/index/integrity/empty baseline.
                    try:
                        _validate_n16_installing_review_empty(connection)
                        summary = _n16_review_claim_summary(connection)
                        claims = _n16_all_review_claims(connection)
                        ledger.attest_installing_empty()
                    except (RuntimeError, N16ClaimLedgerError) as exc:
                        raise SignalRetentionMaintenanceError(
                            "N16 interrupted installation baseline is invalid"
                        ) from exc
                    if summary.confirmed_claim_count != 0 or claims:
                        raise SignalRetentionMaintenanceError(
                            "INSTALLING ledger conflicts with Review claims"
                        )
                    try:
                        ledger.confirm_install(summary, utc_now())
                    except Exception as exc:
                        # A commit acknowledgement may be lost.  Classify the
                        # durable state instead of guessing: exact READY is a
                        # completed confirmation; exact INSTALLING is a
                        # zero-write failure.  Anything else remains blocked.
                        try:
                            if ledger.metadata_phase() == "READY":
                                _attest_n16_claim_boundary(
                                    connection,
                                    ledger,
                                )
                                raise SignalRetentionMaintenanceError(
                                    "N16 installation confirmation committed "
                                    "but its acknowledgement was unavailable; "
                                    "retry explicit maintenance"
                                ) from exc
                            ledger.attest_installing_empty()
                        except SignalRetentionMaintenanceError:
                            raise
                        except Exception as classify_exc:
                            raise SignalRetentionMaintenanceError(
                                "N16 installation confirmation is unresolved"
                            ) from classify_exc
                        raise SignalRetentionMaintenanceError(
                            "N16 installation confirmation did not complete"
                        ) from exc
                    return summary
                raise SignalRetentionMaintenanceError(
                    "N16 claim ledger requires explicit publication resolution"
                )
    if status != "PRE_N16":
        raise SignalRetentionMaintenanceError(
            "N16 lifecycle installation state is unsupported"
        )
    if ledger.exists:
        if ledger.metadata_phase() != "INSTALLING":
            raise SignalRetentionMaintenanceError(
                "pre-N16 Review requires one INSTALLING state ledger"
            )
        ledger.attest_installing_empty()
    else:
        ledger._create(now=utc_now())
        ledger.attest_installing_empty()
    try:
        recorder = ReviewRecorder._open_for_n16_maintenance(
            str(database),
            logging.getLogger("n16-lifecycle-maintenance"),
            ledger,
            database_scope=database_scope,
        )
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N16 Review installation did not complete"
        ) from exc
    if database_scope is not None:
        database_scope.refresh_after_close(database)
    try:
        with recorder._read_only_runtime_snapshot() as connection:
            summary = _n16_review_claim_summary(connection)
            claims = _n16_all_review_claims(connection)
        ledger.attest_all(summary, claims)
        return summary
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N16 installed Review could not be re-attested"
        ) from exc


def _install_n17_lifecycle_boundary(
    database: Path,
    claim_ledger_file: Path,
    *,
    expected_database_identity: Optional[Tuple[int, int]] = None,
    database_scope: Optional["_DatabaseFileScope"] = None,
    claim_ledger_scope=None,
):
    """Explicit, stopped-service upgrade from the exact N16 generation."""

    from .n16_claim_ledger import N16PermanentClaimLedger
    from .n17_schema import n17_schema_status, validate_pre_n17_review_clean
    from .recorder import ReviewRecorder

    ledger = N16PermanentClaimLedger(
        claim_ledger_file,
        file_scope=claim_ledger_scope,
    )
    if not ledger.exists or ledger.metadata_phase() != "READY":
        raise SignalRetentionMaintenanceError(
            "N17 installation requires an exact READY N16 claim ledger"
        )
    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        _attest_n16_claim_boundary(connection, ledger)
        try:
            status = n17_schema_status(connection)
        except Exception as exc:
            raise SignalRetentionMaintenanceError(
                "N17 lifecycle schema requires explicit repair"
            ) from exc
        if status == "CURRENT":
            return _attest_n16_claim_boundary(connection, ledger)
        if status != "PRE_N17":
            raise SignalRetentionMaintenanceError(
                "N17 lifecycle schema requires explicit repair"
            )
        try:
            validate_pre_n17_review_clean(connection)
        except Exception as exc:
            raise SignalRetentionMaintenanceError(
                "pre-N17 Review contains incompatible lifecycle evidence"
            ) from exc
    try:
        recorder = ReviewRecorder._open_for_n17_maintenance(
            str(database),
            logging.getLogger("n17-lifecycle-maintenance"),
            ledger,
            database_scope=database_scope,
        )
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N17 lifecycle installation did not complete"
        ) from exc
    del recorder
    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        if n17_schema_status(connection) != "CURRENT":
            raise SignalRetentionMaintenanceError(
                "N17 installed schema could not be re-attested"
            )
        return _attest_n16_claim_boundary(connection, ledger)


def _repair_n17_frozen_evidence_boundary(
    database: Path,
    claim_ledger_file: Path,
    *,
    expected_database_identity: Optional[Tuple[int, int]] = None,
    database_scope: Optional["_DatabaseFileScope"] = None,
    claim_ledger_scope=None,
):
    """Explicit stopped-service repair for the bounded legacy live-tail bug."""

    from .micro_schema import (
        micro_schema_status,
        validate_micro_lifecycle_graph,
        validate_pre_micro_review_clean,
    )
    from .n16_claim_ledger import N16PermanentClaimLedger
    from .n17_schema import N17_TRIGGER_SQL, n17_schema_status
    from .n18_schema import n18_schema_status
    from .n19_schema import n19_schema_status
    from .n20_schema import n20_schema_status

    ledger = N16PermanentClaimLedger(
        claim_ledger_file, file_scope=claim_ledger_scope
    )
    if not ledger.exists or ledger.metadata_phase() != "READY":
        raise SignalRetentionMaintenanceError(
            "N17 evidence repair requires an exact READY N16 claim ledger"
        )

    def attest_generations(connection: sqlite3.Connection) -> None:
        _attest_n16_claim_boundary(connection, ledger)
        if (
            n17_schema_status(connection) != "CURRENT"
            or n19_schema_status(connection) != "CURRENT"
            or n18_schema_status(connection) != "CURRENT"
            or n20_schema_status(connection) != "CURRENT"
        ):
            raise SignalRetentionMaintenanceError(
                "N17 evidence repair requires exact N20 predecessor generations"
            )
        micro_status = micro_schema_status(connection)
        if micro_status == "PRE_MICRO":
            validate_pre_micro_review_clean(connection)
        elif micro_status == "CURRENT":
            validate_micro_lifecycle_graph(connection)
        else:
            raise SignalRetentionMaintenanceError(
                "N17 evidence repair requires PRE_MICRO or CURRENT micro schema"
            )

    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        attest_generations(connection)
        try:
            planned = _n17_evidence_repair_plan(
                connection, permit_legacy_repair=True
            )
        except (TypeError, ValueError, RuntimeError) as exc:
            raise SignalRetentionMaintenanceError(
                "N17 frozen evidence is not uniquely repairable"
            ) from exc
        planned_identity = tuple(
            (row_id, old_sha, record.evidence_sha256)
            for row_id, old_sha, record in planned
        )

    with _open_database(
        database,
        read_only=False,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
        prewrite_validator=lambda connection: attest_generations(connection),
    ) as connection:
        connection.execute("BEGIN IMMEDIATE")
        try:
            attest_generations(connection)
            try:
                current = _n17_evidence_repair_plan(
                    connection, permit_legacy_repair=True
                )
            except (TypeError, ValueError, RuntimeError) as exc:
                raise SignalRetentionMaintenanceError(
                    "N17 frozen evidence changed or is not uniquely repairable"
                ) from exc
            current_identity = tuple(
                (row_id, old_sha, record.evidence_sha256)
                for row_id, old_sha, record in current
            )
            if current_identity != planned_identity:
                raise SignalRetentionMaintenanceError(
                    "N17 repair plan changed before the write transaction"
                )
            if current:
                connection.execute(
                    'DROP TRIGGER "trg_n17_state_identity_immutable"'
                )
                for row_id, old_sha, record in current:
                    updated = connection.execute(
                        "UPDATE n17_range_support_states SET "
                        "evidence_json=?,evidence_sha256=? "
                        "WHERE id=? AND evidence_sha256=?",
                        (
                            record.evidence_json,
                            record.evidence_sha256,
                            row_id,
                            old_sha,
                        ),
                    )
                    if updated.rowcount != 1:
                        raise SignalRetentionMaintenanceError(
                            "N17 repair row changed during maintenance"
                        )
                connection.execute(
                    N17_TRIGGER_SQL["trg_n17_state_identity_immutable"]
                )
            if n17_schema_status(connection) != "CURRENT":
                raise SignalRetentionMaintenanceError(
                    "N17 schema changed during evidence repair"
                )
            if _n17_evidence_repair_plan(
                connection, permit_legacy_repair=False
            ):
                raise SignalRetentionMaintenanceError(
                    "N17 evidence remained repairable after maintenance"
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        if planned:
            _advance_protected_generation_after_explicit_maintenance(
                connection, ledger
            )
        attest_generations(connection)
        _attest_n17_lifecycle_boundary(connection)
    return N17EvidenceRepairReport(
        mode="N17_EVIDENCE_REPAIR",
        database=str(database),
        claim_ledger=str(claim_ledger_file),
        repaired_row_count=len(planned),
        resolution="REPAIRED" if planned else "ALREADY_CURRENT",
    )


def _install_n19_lifecycle_boundary(
    database: Path,
    claim_ledger_file: Path,
    *,
    expected_database_identity: Optional[Tuple[int, int]] = None,
    database_scope: Optional["_DatabaseFileScope"] = None,
    claim_ledger_scope=None,
):
    """Explicit, stopped-service upgrade from exact N17 plus N16 READY."""

    from .n16_claim_ledger import N16PermanentClaimLedger
    from .n17_schema import n17_schema_status
    from .n19_schema import n19_schema_status, validate_pre_n19_review_clean
    from .recorder import ReviewRecorder

    ledger = N16PermanentClaimLedger(
        claim_ledger_file,
        file_scope=claim_ledger_scope,
    )
    if not ledger.exists or ledger.metadata_phase() != "READY":
        raise SignalRetentionMaintenanceError(
            "N19 installation requires an exact READY N16 claim ledger"
        )
    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        _attest_n16_claim_boundary(connection, ledger)
        try:
            if n17_schema_status(connection) != "CURRENT":
                raise RuntimeError("pre-N17 Review schema")
            status = n19_schema_status(connection)
        except Exception as exc:
            raise SignalRetentionMaintenanceError(
                "N19 lifecycle schema requires exact N17 maintenance first"
            ) from exc
        if status == "CURRENT":
            _attest_n17_lifecycle_boundary(connection)
            return _attest_n16_claim_boundary(connection, ledger)
        if status != "PRE_N19":
            raise SignalRetentionMaintenanceError(
                "N19 lifecycle schema requires explicit repair"
            )
        try:
            validate_pre_n19_review_clean(connection)
        except Exception as exc:
            raise SignalRetentionMaintenanceError(
                "pre-N19 Review contains incompatible lifecycle evidence"
            ) from exc
    try:
        recorder = ReviewRecorder._open_for_n19_maintenance(
            str(database),
            logging.getLogger("n19-lifecycle-maintenance"),
            ledger,
            database_scope=database_scope,
        )
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N19 lifecycle installation did not complete"
        ) from exc
    del recorder
    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        _attest_n17_lifecycle_boundary(connection)
        if n19_schema_status(connection) != "CURRENT":
            raise SignalRetentionMaintenanceError(
                "N19 installed schema could not be re-attested"
            )
        return _attest_n16_claim_boundary(connection, ledger)


def _install_n18_lifecycle_boundary(
    database: Path,
    claim_ledger_file: Path,
    *,
    expected_database_identity: Optional[Tuple[int, int]] = None,
    database_scope: Optional["_DatabaseFileScope"] = None,
    claim_ledger_scope=None,
):
    """Explicit, stopped-service upgrade from exact N19 plus prior generations."""

    from .n16_claim_ledger import N16PermanentClaimLedger
    from .n17_schema import n17_schema_status
    from .n18_schema import n18_schema_status, validate_pre_n18_review_clean
    from .n19_schema import n19_schema_status
    from .recorder import ReviewRecorder

    ledger = N16PermanentClaimLedger(
        claim_ledger_file,
        file_scope=claim_ledger_scope,
    )
    if not ledger.exists or ledger.metadata_phase() != "READY":
        raise SignalRetentionMaintenanceError(
            "N18 installation requires an exact READY N16 claim ledger"
        )
    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        _attest_n16_claim_boundary(connection, ledger)
        try:
            if n17_schema_status(connection) != "CURRENT":
                raise RuntimeError("pre-N17 Review schema")
            if n19_schema_status(connection) != "CURRENT":
                raise RuntimeError("pre-N19 Review schema")
            status = n18_schema_status(connection)
        except Exception as exc:
            raise SignalRetentionMaintenanceError(
                "N18 lifecycle schema requires exact N19 maintenance first"
            ) from exc
        if status == "CURRENT":
            _attest_n17_lifecycle_boundary(connection)
            _attest_n19_lifecycle_boundary(connection)
            return _attest_n16_claim_boundary(connection, ledger)
        if status != "PRE_N18":
            raise SignalRetentionMaintenanceError(
                "N18 lifecycle schema requires explicit repair"
            )
        try:
            validate_pre_n18_review_clean(connection)
        except Exception as exc:
            raise SignalRetentionMaintenanceError(
                "pre-N18 Review contains incompatible lifecycle evidence"
            ) from exc
    try:
        recorder = ReviewRecorder._open_for_n18_maintenance(
            str(database),
            logging.getLogger("n18-lifecycle-maintenance"),
            ledger,
            database_scope=database_scope,
        )
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N18 lifecycle installation did not complete"
        ) from exc
    del recorder
    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        if n18_schema_status(connection) != "CURRENT":
            raise SignalRetentionMaintenanceError(
                "N18 installed schema could not be re-attested"
            )
        _attest_n17_lifecycle_boundary(connection)
        _attest_n19_lifecycle_boundary(connection)
        return _attest_n16_claim_boundary(connection, ledger)


def _install_n20_lifecycle_boundary(
    database: Path,
    claim_ledger_file: Path,
    *,
    expected_database_identity: Optional[Tuple[int, int]] = None,
    database_scope: Optional["_DatabaseFileScope"] = None,
    claim_ledger_scope=None,
):
    """Explicit stopped-service upgrade from exact N18 to final N20."""

    from .n16_claim_ledger import N16PermanentClaimLedger
    from .n17_schema import n17_schema_status
    from .n18_schema import n18_schema_status
    from .n19_schema import n19_schema_status
    from .n20_schema import n20_schema_status, validate_pre_n20_review_clean
    from .recorder import ReviewRecorder

    ledger = N16PermanentClaimLedger(claim_ledger_file, file_scope=claim_ledger_scope)
    if not ledger.exists or ledger.metadata_phase() != "READY":
        raise SignalRetentionMaintenanceError(
            "N20 installation requires an exact READY N16 claim ledger"
        )
    with _open_database(
        database, read_only=True, expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        _attest_n16_claim_boundary(connection, ledger)
        try:
            if n17_schema_status(connection) != "CURRENT":
                raise RuntimeError("pre-N17 Review schema")
            if n19_schema_status(connection) != "CURRENT":
                raise RuntimeError("pre-N19 Review schema")
            if n18_schema_status(connection) != "CURRENT":
                raise RuntimeError("pre-N18 Review schema")
            status = n20_schema_status(connection)
        except Exception as exc:
            raise SignalRetentionMaintenanceError(
                "N20 lifecycle schema requires exact N18 maintenance first"
            ) from exc
        if status == "CURRENT":
            _attest_n17_lifecycle_boundary(connection)
            _attest_n19_lifecycle_boundary(connection)
            _attest_n18_lifecycle_boundary(connection)
            return _attest_n16_claim_boundary(connection, ledger)
        if status != "PRE_N20":
            raise SignalRetentionMaintenanceError(
                "N20 lifecycle schema requires explicit repair"
            )
        try:
            validate_pre_n20_review_clean(connection)
        except Exception as exc:
            raise SignalRetentionMaintenanceError(
                "pre-N20 Review contains incompatible lifecycle evidence"
            ) from exc
    try:
        recorder = ReviewRecorder._open_for_n20_maintenance(
            str(database), logging.getLogger("n20-lifecycle-maintenance"), ledger,
            database_scope=database_scope,
        )
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N20 lifecycle installation did not complete"
        ) from exc
    del recorder
    with _open_database(
        database, read_only=True, expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        if n20_schema_status(connection) != "CURRENT":
            raise SignalRetentionMaintenanceError(
                "N20 installed schema could not be re-attested"
            )
        _attest_n17_lifecycle_boundary(connection)
        _attest_n19_lifecycle_boundary(connection)
        _attest_n18_lifecycle_boundary(connection)
        return _attest_n16_claim_boundary(connection, ledger)


def _install_micro_lifecycle_boundary(
    database: Path,
    claim_ledger_file: Path,
    *,
    expected_database_identity: Optional[Tuple[int, int]] = None,
    database_scope: Optional["_DatabaseFileScope"] = None,
    claim_ledger_scope=None,
):
    """Explicit stopped-service upgrade from exact N20 to N21-N25."""

    from .n16_claim_ledger import N16PermanentClaimLedger
    from .n17_schema import n17_schema_status
    from .n18_schema import n18_schema_status
    from .n19_schema import n19_schema_status
    from .n20_schema import n20_schema_status
    from .micro_schema import (
        micro_schema_status,
        validate_micro_lifecycle_graph,
        validate_pre_micro_review_clean,
    )
    from .recorder import ReviewRecorder

    ledger = N16PermanentClaimLedger(
        claim_ledger_file, file_scope=claim_ledger_scope
    )
    if not ledger.exists or ledger.metadata_phase() != "READY":
        raise SignalRetentionMaintenanceError(
            "N21-N25 installation requires an exact READY N16 claim ledger"
        )
    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        _attest_n16_claim_boundary(connection, ledger)
        try:
            if n17_schema_status(connection) != "CURRENT":
                raise RuntimeError("pre-N17 Review schema")
            if n19_schema_status(connection) != "CURRENT":
                raise RuntimeError("pre-N19 Review schema")
            if n18_schema_status(connection) != "CURRENT":
                raise RuntimeError("pre-N18 Review schema")
            if n20_schema_status(connection) != "CURRENT":
                raise RuntimeError("pre-N20 Review schema")
            # This is a predecessor graph prerequisite, not an installation
            # post-check.  It must run before any micro DDL can be committed.
            _attest_n17_lifecycle_boundary(connection)
            status = micro_schema_status(connection)
        except SignalRetentionMaintenanceError:
            # Preserve the exact predecessor-graph diagnosis.  In particular,
            # operators must be told to repair N17 before installing micro
            # lifecycle tables; reporting a generic generation error obscures
            # the only safe deployment order.
            raise
        except Exception as exc:
            raise SignalRetentionMaintenanceError(
                "N21-N25 lifecycle schema requires exact N20 maintenance first"
            ) from exc
        if status == "CURRENT":
            validate_micro_lifecycle_graph(connection)
            _attest_n17_lifecycle_boundary(connection)
            _attest_n19_lifecycle_boundary(connection)
            _attest_n18_lifecycle_boundary(connection)
            _attest_n20_lifecycle_boundary(connection)
            return _attest_n16_claim_boundary(connection, ledger)
        if status != "PRE_MICRO":
            raise SignalRetentionMaintenanceError(
                "N21-N25 lifecycle schema requires explicit repair"
            )
        try:
            validate_pre_micro_review_clean(connection)
        except Exception as exc:
            raise SignalRetentionMaintenanceError(
                "pre-N21-N25 Review contains incompatible lifecycle evidence"
            ) from exc
    try:
        recorder = ReviewRecorder._open_for_micro_maintenance(
            str(database),
            logging.getLogger("micro-lifecycle-maintenance"),
            ledger,
            database_scope=database_scope,
        )
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N21-N25 lifecycle installation did not complete"
        ) from exc
    del recorder
    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        if micro_schema_status(connection) != "CURRENT":
            raise SignalRetentionMaintenanceError(
                "N21-N25 installed schema could not be re-attested"
            )
        validate_micro_lifecycle_graph(connection)
        _attest_n17_lifecycle_boundary(connection)
        _attest_n19_lifecycle_boundary(connection)
        _attest_n18_lifecycle_boundary(connection)
        _attest_n20_lifecycle_boundary(connection)
        return _attest_n16_claim_boundary(connection, ledger)


def _install_coverage_epoch_boundary(
    database: Path,
    claim_ledger_file: Path,
    *,
    expected_database_identity: Optional[Tuple[int, int]] = None,
    database_scope: Optional["_DatabaseFileScope"] = None,
    claim_ledger_scope=None,
):
    """Explicit stopped-service upgrade to immutable N17-N19 epochs."""

    from .coverage_epoch_schema import (
        coverage_epoch_schema_status,
        validate_pre_coverage_epoch_review,
    )
    from .coverage_family_seal import (
        family_seal_catalog_sha256,
        family_seal_schema_status,
    )
    from .micro_schema import micro_schema_status, validate_micro_lifecycle_graph
    from .n16_claim_ledger import N16PermanentClaimLedger
    from .n17_schema import n17_schema_status
    from .n18_schema import n18_schema_status
    from .n19_schema import n19_schema_status
    from .n20_schema import n20_schema_status, validate_n20_episode_graph
    from .n15_terminal_schema import n15_terminal_schema_status
    from .recorder import ReviewRecorder

    ledger = N16PermanentClaimLedger(
        claim_ledger_file, file_scope=claim_ledger_scope
    )
    if not ledger.exists or ledger.metadata_phase() != "READY":
        raise SignalRetentionMaintenanceError(
            "coverage epoch installation requires an exact READY N16 ledger"
        )

    def install_or_attest_generation_highwater(
        connection: sqlite3.Connection,
    ) -> None:
        generation = connection.execute(
            "SELECT generation FROM history_coverage_protected_generation "
            "WHERE singleton_id=1"
        ).fetchone()
        schema_version = connection.execute(
            "PRAGMA schema_version"
        ).fetchone()
        if (
            generation is None
            or len(generation) != 1
            or type(generation[0]) is not int
            or generation[0] < 0
            or schema_version is None
            or len(schema_version) != 1
            or type(schema_version[0]) is not int
            or schema_version[0] <= 0
        ):
            raise SignalRetentionMaintenanceError(
                "coverage protected generation is invalid"
            )
        ledger.install_protected_generation_highwater(
            generation=generation[0],
            review_schema_version=schema_version[0],
            family_catalog_sha256=family_seal_catalog_sha256(connection),
            now=utc_now(),
        )

    def attest_predecessors(connection: sqlite3.Connection) -> None:
        _attest_n16_claim_boundary(connection, ledger)
        if (
            n17_schema_status(connection) != "CURRENT"
            or n19_schema_status(connection) != "CURRENT"
            or n18_schema_status(connection) != "CURRENT"
            or n20_schema_status(connection) != "CURRENT"
            or micro_schema_status(connection) != "CURRENT"
        ):
            raise SignalRetentionMaintenanceError(
                "coverage epoch installation requires exact N17-N25 generations"
            )
        _attest_n17_lifecycle_boundary(connection)
        _attest_n19_lifecycle_boundary(connection)
        _attest_n18_lifecycle_boundary(connection)
        validate_n20_episode_graph(connection)
        validate_micro_lifecycle_graph(connection)

    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        attest_predecessors(connection)
        try:
            status = coverage_epoch_schema_status(connection)
            family_status = family_seal_schema_status(connection)
            n15_terminal_status = n15_terminal_schema_status(
                connection, validate_graph=False
            )
            if (
                status in {"CURRENT", "AUTHORIZED_LEGACY_V3"}
                and family_status == "CURRENT"
                and n15_terminal_status == "CURRENT"
            ):
                mirror = ledger.legacy_witness_mirror()
                if (
                    mirror is None
                    or (status == "CURRENT" and mirror[0] != "EMPTY")
                    or (
                        status == "AUTHORIZED_LEGACY_V3"
                        and mirror[0] != "COMMITTED"
                    )
                ):
                    raise RuntimeError(
                        "coverage family seal ledger mirror is inconsistent"
                    )
                install_or_attest_generation_highwater(connection)
                return _attest_n16_claim_boundary(connection, ledger)
            if status not in {
                "PRE_EPOCH",
                "PRE_TERMINAL_RECEIPT",
                "CURRENT",
                "AUTHORIZED_LEGACY_V3",
            }:
                raise RuntimeError("coverage epoch schema is not installable")
            if family_status not in {"PRE_FAMILY_SEAL", "CURRENT"}:
                raise RuntimeError(
                    "coverage family seal schema is not installable"
                )
            if n15_terminal_status not in {
                "PRE_N15_TERMINAL", "CURRENT"
            }:
                raise RuntimeError(
                    "N15 terminal receipt schema is not installable"
                )
            if status == "PRE_EPOCH":
                validate_pre_coverage_epoch_review(connection)
        except Exception as exc:
            raise SignalRetentionMaintenanceError(
                "coverage epoch schema requires explicit repair"
            ) from exc
    ledger.install_legacy_witness_mirror_schema(utc_now())
    try:
        recorder = ReviewRecorder._open_for_coverage_epoch_maintenance(
            str(database),
            logging.getLogger("coverage-epoch-maintenance"),
            ledger,
            database_scope=database_scope,
        )
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "coverage epoch installation did not complete"
        ) from exc
    del recorder
    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        attest_predecessors(connection)
        final_coverage_status = coverage_epoch_schema_status(connection)
        if final_coverage_status not in {
            "CURRENT", "AUTHORIZED_LEGACY_V3"
        }:
            raise SignalRetentionMaintenanceError(
                "coverage epoch schema could not be re-attested"
            )
        if family_seal_schema_status(connection) != "CURRENT":
            raise SignalRetentionMaintenanceError(
                "coverage family seal schema could not be re-attested"
            )
        if n15_terminal_schema_status(connection) != "CURRENT":
            raise SignalRetentionMaintenanceError(
                "N15 terminal receipt schema could not be re-attested"
            )
        install_or_attest_generation_highwater(connection)
        mirror = ledger.legacy_witness_mirror()
        if (
            mirror is None
            or (final_coverage_status == "CURRENT" and mirror[0] != "EMPTY")
            or (
                final_coverage_status == "AUTHORIZED_LEGACY_V3"
                and mirror[0] != "COMMITTED"
            )
        ):
            raise SignalRetentionMaintenanceError(
                "coverage family seal ledger mirror is inconsistent"
            )
        return _attest_n16_claim_boundary(connection, ledger)


def _authorized_legacy_v3_witness_plan_from_connection(
    connection: sqlite3.Connection,
    *,
    canonical_review_sha256: Optional[str] = None,
):
    """Build the one authorized v3-unbound plan without mutating Review."""

    from .coverage_epoch_schema import (
        _LEGACY_V3_OBJECTS,
        _coverage_epoch_catalog_sha256_for_objects,
        coverage_epoch_schema_status,
        validate_coverage_epoch_graph,
    )
    from .coverage_family_seal import (
        AUTHORIZED_FAMILY_ID,
        AUTHORIZED_STATEMENT_SHA256,
        AUTHORIZED_STRUCTURE_ID,
        AUTHORIZED_SYMBOL,
        AuthorizedLegacyWitnessPlan,
        authorized_witness_plan_sha256,
        coverage_graph_sha256,
        ordered_v3_receipt_set_sha256,
        review_canonical_sha256,
        typed_row_sha256,
    )
    from .n19_analyzer import decode_n19_state_evidence

    if coverage_epoch_schema_status(connection) != "PRE_TERMINAL_RECEIPT":
        raise SignalRetentionMaintenanceError(
            "authorized legacy witness requires exact v3 coverage schema"
        )
    validate_coverage_epoch_graph(connection)
    candidates = connection.execute(
        "SELECT * FROM n19_staircase_states "
        "WHERE strategy_id='N19' AND stage='MISSED' "
        "AND reason='N19_HISTORICAL_ENTRY_MISSED' "
        "ORDER BY id"
    ).fetchall()
    if len(candidates) != 1:
        raise SignalRetentionMaintenanceError(
            "authorized legacy witness candidate count must be exactly one"
        )
    state = tuple(candidates[0])
    columns = {
        row[1]: index
        for index, row in enumerate(
            connection.execute(
                "PRAGMA table_xinfo(n19_staircase_states)"
            ).fetchall()
        )
    }
    required_columns = {
        "symbol",
        "family_id",
        "structure_id",
        "stage",
        "reason",
        "evidence_json",
        "evidence_sha256",
    }
    if not required_columns <= set(columns):
        raise SignalRetentionMaintenanceError(
            "N19 legacy state shape is incomplete"
        )
    if (
        state[columns["symbol"]] != AUTHORIZED_SYMBOL
        or state[columns["family_id"]] != AUTHORIZED_FAMILY_ID
        or state[columns["structure_id"]] != AUTHORIZED_STRUCTURE_ID
        or state[columns["stage"]] != "MISSED"
        or state[columns["reason"]] != "N19_HISTORICAL_ENTRY_MISSED"
        or type(state[columns["evidence_json"]]) is not str
        or type(state[columns["evidence_sha256"]]) is not str
    ):
        raise SignalRetentionMaintenanceError(
            "N19 legacy witness identity is not authorized"
        )
    evidence_json = state[columns["evidence_json"]]
    record = decode_n19_state_evidence(
        evidence_json,
        expected_symbol=AUTHORIZED_SYMBOL,
    )
    if (
        record.family_id != AUTHORIZED_FAMILY_ID
        or record.structure_id != AUTHORIZED_STRUCTURE_ID
        or record.stage != "MISSED"
        or record.reason != "N19_HISTORICAL_ENTRY_MISSED"
        or record.evidence_sha256 != state[columns["evidence_sha256"]]
    ):
        raise SignalRetentionMaintenanceError(
            "N19 legacy state evidence conflicts"
        )
    if connection.execute(
        "SELECT 1 FROM strategy_paper_trades "
        "WHERE strategy_id='N19' AND symbol=? AND result='OPEN' LIMIT 1",
        (AUTHORIZED_SYMBOL,),
    ).fetchone():
        raise SignalRetentionMaintenanceError(
            "authorized N19 family has an open paper execution"
        )
    if connection.execute(
        "SELECT 1 FROM strategy_live_links "
        "WHERE strategy_id='N19' AND symbol=? AND closed_at IS NULL LIMIT 1",
        (AUTHORIZED_SYMBOL,),
    ).fetchone():
        raise SignalRetentionMaintenanceError(
            "authorized N19 family has an open live execution"
        )
    if connection.execute(
        "SELECT 1 FROM trade_reviews "
        "WHERE symbol=? AND status IN "
        "('OPENED','CLOSED_LIVE_RESULT_PENDING') LIMIT 1",
        (AUTHORIZED_SYMBOL,),
    ).fetchone():
        raise SignalRetentionMaintenanceError(
            "authorized N19 family has an active trade review"
        )
    if connection.execute(
        "SELECT 1 FROM strategy_passed_signal_audits "
        "WHERE strategy_id='N19' AND symbol=? AND structure_id=? "
        "AND claim_state IN ('STAGED','ACTIVE') LIMIT 1",
        (AUTHORIZED_SYMBOL, AUTHORIZED_STRUCTURE_ID),
    ).fetchone() or connection.execute(
        "SELECT 1 FROM strategy_passed_structure_ledger "
        "WHERE strategy_id='N19' AND symbol=? AND structure_id=? "
        "AND claim_state IN ('STAGED','ACTIVE') LIMIT 1",
        (AUTHORIZED_SYMBOL, AUTHORIZED_STRUCTURE_ID),
    ).fetchone():
        raise SignalRetentionMaintenanceError(
            "authorized N19 family has an execution publication claim"
        )
    receipt_count_row = connection.execute(
        "SELECT count(*) FROM history_coverage_publication_receipts"
    ).fetchone()
    if (
        receipt_count_row is None
        or len(receipt_count_row) != 1
        or type(receipt_count_row[0]) is not int
        or receipt_count_row[0] <= 0
    ):
        raise SignalRetentionMaintenanceError(
            "authorized legacy witness has no v3 publication receipts"
        )
    chain = tuple(
        tuple(row)
        for row in connection.execute(
            "SELECT * FROM history_coverage_epoch_chain "
            "ORDER BY strategy_id,symbol,epoch_ordinal"
        )
    )
    heads = tuple(
        tuple(row)
        for row in connection.execute(
            "SELECT * FROM history_coverage_epoch_heads "
            "ORDER BY strategy_id,symbol"
        )
    )
    installation = connection.execute(
        "SELECT * FROM history_coverage_epoch_installation "
        "WHERE singleton_id=1"
    ).fetchall()
    if len(installation) != 1:
        raise SignalRetentionMaintenanceError(
            "coverage epoch installation row is missing"
        )
    receipt_hash = ordered_v3_receipt_set_sha256(
        connection.execute(
            "SELECT * FROM history_coverage_publication_receipts "
            "ORDER BY publication_ordinal,source_scan_id,strategy_id,symbol"
        )
    )
    graph_hash = coverage_graph_sha256(chain, heads, installation[0])
    catalog_hash = _coverage_epoch_catalog_sha256_for_objects(
        connection,
        _LEGACY_V3_OBJECTS,
    )
    canonical_review_hash = (
        review_canonical_sha256(connection)
        if canonical_review_sha256 is None
        else canonical_review_sha256
    )
    if (
        type(canonical_review_hash) is not str
        or len(canonical_review_hash) != 64
        or any(
            character not in "0123456789abcdef"
            for character in canonical_review_hash
        )
    ):
        raise SignalRetentionMaintenanceError(
            "authorized legacy Review canonical digest is invalid"
        )
    state_hash = typed_row_sha256(state)
    evidence_hash = record.evidence_sha256
    plan_hash = authorized_witness_plan_sha256(
        AUTHORIZED_SYMBOL,
        AUTHORIZED_FAMILY_ID,
        AUTHORIZED_STRUCTURE_ID,
        evidence_hash,
        state_hash,
        receipt_count_row[0],
        receipt_hash,
        graph_hash,
        catalog_hash,
        canonical_review_hash,
        AUTHORIZED_STATEMENT_SHA256,
    )
    return (
        AuthorizedLegacyWitnessPlan(
            symbol=AUTHORIZED_SYMBOL,
            family_id=AUTHORIZED_FAMILY_ID,
            structure_id=AUTHORIZED_STRUCTURE_ID,
            terminal_evidence_sha256=evidence_hash,
            state_row_sha256=state_hash,
            receipt_count=receipt_count_row[0],
            receipt_set_sha256=receipt_hash,
            coverage_graph_sha256=graph_hash,
            coverage_catalog_sha256=catalog_hash,
            review_canonical_sha256=canonical_review_hash,
            authorization_sha256=AUTHORIZED_STATEMENT_SHA256,
            review_plan_sha256=plan_hash,
        ),
        evidence_json,
    )


def inspect_authorized_legacy_v3_witness(
    database: Path,
    claim_ledger_file: Path,
    *,
    expected_database_identity: Optional[Tuple[int, int]] = None,
    database_scope: Optional["_DatabaseFileScope"] = None,
    claim_ledger_scope=None,
):
    """Read-only stopped-service plan for the single authorized family."""

    from .n16_claim_ledger import N16PermanentClaimLedger

    ledger = N16PermanentClaimLedger(
        claim_ledger_file,
        file_scope=claim_ledger_scope,
    )
    if not ledger.exists or ledger.metadata_phase() != "READY":
        raise SignalRetentionMaintenanceError(
            "authorized legacy witness inspection requires READY N16 ledger"
        )
    mirror = ledger.legacy_witness_mirror()
    if mirror is not None and mirror[0] != "EMPTY":
        raise SignalRetentionMaintenanceError(
            "authorized legacy witness mirror has an unresolved phase"
        )
    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        _attest_n16_claim_boundary(connection, ledger)
        _attest_n17_lifecycle_boundary(connection)
        _attest_n19_lifecycle_boundary(connection)
        _attest_n18_lifecycle_boundary(connection)
        _attest_n20_lifecycle_boundary(connection)
        _attest_micro_lifecycle_boundary(connection)
        plan, _evidence = (
            _authorized_legacy_v3_witness_plan_from_connection(connection)
        )
        return plan


def _authorized_legacy_witness_report(
    mode: str,
    database: Path,
    claim_ledger_file: Path,
    plan,
    *,
    witness_sha256: Optional[str],
    ledger_phase: str,
    resolution: str,
) -> AuthorizedLegacyWitnessMaintenanceReport:
    return AuthorizedLegacyWitnessMaintenanceReport(
        mode=mode,
        database=str(database),
        claim_ledger=str(claim_ledger_file),
        symbol=plan.symbol,
        family_id=plan.family_id,
        structure_id=plan.structure_id,
        terminal_evidence_sha256=plan.terminal_evidence_sha256,
        state_row_sha256=plan.state_row_sha256,
        receipt_count=plan.receipt_count,
        receipt_set_sha256=plan.receipt_set_sha256,
        coverage_graph_sha256=plan.coverage_graph_sha256,
        coverage_catalog_sha256=plan.coverage_catalog_sha256,
        review_canonical_sha256=plan.review_canonical_sha256,
        authorization_sha256=plan.authorization_sha256,
        review_plan_sha256=plan.review_plan_sha256,
        witness_sha256=witness_sha256,
        ledger_phase=ledger_phase,
        resolution=resolution,
    )


def _require_authorized_legacy_witness_expectations(
    plan,
    expected: Dict[str, Any],
) -> None:
    actual = plan.to_jsonable()
    if set(expected) != set(actual):
        raise SignalRetentionMaintenanceError(
            "authorized legacy witness expected field set is incomplete"
        )
    for key, value in actual.items():
        if type(expected[key]) is not type(value) or expected[key] != value:
            raise SignalRetentionMaintenanceError(
                "authorized legacy witness expectation conflicts: %s" % key
            )


def install_authorized_legacy_v3_witness(
    database: Path,
    claim_ledger_file: Path,
    expected: Dict[str, Any],
    *,
    expected_database_identity: Optional[Tuple[int, int]] = None,
    database_scope: Optional["_DatabaseFileScope"] = None,
    claim_ledger_scope=None,
    progress_callback: Optional[Callable[[str, float], None]] = None,
) -> AuthorizedLegacyWitnessMaintenanceReport:
    """Install the exact authorized v3-unbound witness with two DB phases."""

    from .coverage_epoch_schema import (
        coverage_epoch_schema_status,
        install_authorized_legacy_v3_terminal_objects,
        validate_coverage_epoch_graph,
    )
    from .coverage_family_seal import (
        AUTHORIZED_LEGACY_DOMAIN,
        authorized_witness_sha256,
        family_seal_catalog_sha256,
        family_seal_schema_status,
        insert_authorized_legacy_witness,
        insert_family_seal,
        insert_v3_snapshot_anchors,
        install_family_seal_schema,
        review_catalog_sha256,
        review_snapshot_digests,
        validate_family_seal_graph,
    )
    from .n16_claim_ledger import N16PermanentClaimLedger
    from .n15_terminal_schema import (
        install_n15_terminal_schema,
        n15_terminal_schema_status,
    )
    from .recorder import (
        ReviewRecorder,
        _n16_attested_catalog_schema_version,
        _n16_catalog_schema_version,
        _n16_review_claim_summary,
        _verify_n16_schema,
    )

    ledger = N16PermanentClaimLedger(
        claim_ledger_file,
        file_scope=claim_ledger_scope,
    )
    if not ledger.exists or ledger.metadata_phase() != "READY":
        raise SignalRetentionMaintenanceError(
            "authorized legacy witness installation requires READY N16 ledger"
        )
    maintenance_logger = logging.getLogger(
        "authorized-legacy-witness-maintenance"
    )
    maintenance_started = time.monotonic()

    def report_progress(stage: str) -> None:
        elapsed = time.monotonic() - maintenance_started
        if progress_callback is not None:
            progress_callback(stage, elapsed)
        else:
            maintenance_logger.debug(
                "Authorized legacy witness maintenance stage=%s "
                "elapsed=%.3fs",
                stage,
                elapsed,
            )

    def apply_review_install(connection: sqlite3.Connection) -> None:
        catalog_target = [None]

        def authorize_guard(mode, catalog_version, sealed_count):
            return int(
                mode == "catalog"
                and type(catalog_version) is int
                and catalog_version == catalog_target[0]
                and type(sealed_count) is int
                and sealed_count >= 0
            )

        def authorize_coverage(
            action,
            strategy_id,
            symbol,
            source_scan_id,
            count,
            digest,
        ):
            if action == "family_installation_insert":
                return int(
                    strategy_id == "N19"
                    and symbol == "INSTALLATION"
                    and source_scan_id is None
                    and count is None
                    and type(digest) is str
                )
            if action == "legacy_witness_insert":
                return int(
                    strategy_id == "N19"
                    and symbol == plan.symbol
                    and source_scan_id is None
                    and count == plan.receipt_count
                    and digest == witness_sha
                )
            if action == "v3_anchor_insert":
                return int(
                    strategy_id == "N19"
                    and symbol
                    in {"RECEIPT", "CHAIN", "HEAD", "INSTALLATION"}
                    and source_scan_id is None
                    and count is None
                    and type(digest) is str
                    and len(digest) == 64
                )
            if action == "family_seal_insert":
                return int(
                    strategy_id == "N19"
                    and symbol == plan.symbol
                    and source_scan_id is None
                    and count is None
                    and type(digest) is str
                )
            if action == "n15_snapshot_terminal_install":
                return int(
                    strategy_id == "N15"
                    and symbol == "INSTALLATION"
                    and source_scan_id is None
                    and count is None
                    and type(digest) is str
                    and len(digest) == 64
                )
            return 0

        connection.create_function(
            "_n16_guard_mutation_authorized", 3, authorize_guard
        )
        connection.create_function(
            "_n16_claim_chain_advance",
            17,
            ReviewRecorder._advance_n16_claim_chain,
        )
        connection.create_function(
            "_coverage_epoch_mutation_authorized",
            6,
            authorize_coverage,
        )
        install_authorized_legacy_v3_terminal_objects(connection)
        install_family_seal_schema(connection, operation_time)
        insert_v3_snapshot_anchors(
            connection, anchored_at=operation_time
        )
        inserted_witness = insert_authorized_legacy_witness(
            connection,
            plan=plan,
            terminal_evidence_json=evidence_json,
            witnessed_at=operation_time,
        )
        if inserted_witness != witness_sha:
            raise SignalRetentionMaintenanceError(
                "authorized legacy witness digest changed"
            )
        insert_family_seal(
            connection,
            symbol=plan.symbol,
            family_id=plan.family_id,
            structure_id=plan.structure_id,
            evidence_sha256=plan.terminal_evidence_sha256,
            proof_domain=AUTHORIZED_LEGACY_DOMAIN,
            proof_sha256=witness_sha,
            sealed_at=operation_time,
        )
        # The authorized v3 upgrade is an explicit stopped-service schema
        # transition.  Install the later independent N15 terminal generation
        # in the same Review transaction so the resulting code/DB pair never
        # exposes an otherwise-valid family seal with a PRE_N15 runtime.
        install_n15_terminal_schema(connection, operation_time)
        catalog_target[0] = _n16_catalog_schema_version(connection)
        if (
            catalog_target[0]
            == _n16_attested_catalog_schema_version(connection)
        ):
            raise SignalRetentionMaintenanceError(
                "authorized legacy witness catalog did not advance"
            )
        guard = connection.execute(
            "UPDATE n16_lifecycle_guard SET catalog_schema_version=? "
            "WHERE singleton_id=1",
            (catalog_target[0],),
        )
        root = connection.execute(
            "UPDATE strategy_lifecycle_installations "
            "SET catalog_schema_version=? "
            "WHERE singleton_id=1 AND strategy_id='N16'",
            (catalog_target[0],),
        )
        if guard.rowcount != 1 or root.rowcount != 1:
            raise SignalRetentionMaintenanceError(
                "authorized legacy witness catalog seal conflicted"
            )
        _verify_n16_schema(connection, allow_absent=False)
        if (
            coverage_epoch_schema_status(
                connection,
                validate_graph=False,
            )
            != "AUTHORIZED_LEGACY_V3"
            or family_seal_schema_status(
                connection,
                validate_graph=False,
            )
            != "CURRENT"
            or n15_terminal_schema_status(connection) != "CURRENT"
        ):
            raise SignalRetentionMaintenanceError(
                "authorized legacy witness schema did not attest"
            )
        validate_coverage_epoch_graph(connection)
        validate_family_seal_graph(connection)
        if _n16_review_claim_summary(connection) is None:
            raise SignalRetentionMaintenanceError(
                "N16 Review summary is unavailable"
            )

    report_progress("SOURCE_ATTESTATION_STARTED")
    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        connection.execute("BEGIN")
        try:
            _attest_n16_claim_boundary(connection, ledger)
            base_digests = review_snapshot_digests(connection)
            plan, evidence_json = (
                _authorized_legacy_v3_witness_plan_from_connection(
                    connection,
                    canonical_review_sha256=(
                        base_digests.canonical_sha256
                    ),
                )
            )
            base_review_snapshot_sha256 = (
                base_digests.full_snapshot_sha256
            )
            base_catalog_sha256 = base_digests.catalog_sha256
            base_schema_version = connection.execute(
                "PRAGMA schema_version"
            ).fetchone()[0]
            _require_authorized_legacy_witness_expectations(plan, expected)
            witness_sha = authorized_witness_sha256(plan, evidence_json)
            operation_time = utc_now()
            report_progress("FILE_SIMULATION_BACKUP_STARTED")
            with _anchored_file_backed_review_simulation(
                connection
            ) as simulated:
                simulated.execute("PRAGMA journal_mode=OFF")
                simulated.execute("PRAGMA synchronous=OFF")
                simulated.execute("PRAGMA temp_store=FILE")
                simulated.execute("PRAGMA cache_size=-4096")
                simulated.execute("BEGIN IMMEDIATE")
                try:
                    report_progress("FILE_SIMULATION_APPLY_STARTED")
                    apply_review_install(simulated)
                    target_digests = review_snapshot_digests(simulated)
                    target_review_snapshot_sha256 = (
                        target_digests.full_snapshot_sha256
                    )
                    target_catalog_sha256 = (
                        target_digests.catalog_sha256
                    )
                finally:
                    simulated.rollback()
            report_progress("FILE_SIMULATION_COMPLETED")
        finally:
            connection.rollback()

    ledger.install_legacy_witness_mirror_schema(utc_now())
    mirror = ledger.legacy_witness_mirror()
    if mirror is None or mirror[0] != "EMPTY":
        raise SignalRetentionMaintenanceError(
            "authorized legacy witness mirror is not empty"
        )
    ledger.prepare_legacy_witness_mirror(
        review_plan_sha256=plan.review_plan_sha256,
        witness_sha256=witness_sha,
        pre_review_canonical_sha256=plan.review_canonical_sha256,
        authorization_sha256=plan.authorization_sha256,
        ledger_uuid=ledger.ledger_uuid(),
        base_review_snapshot_sha256=base_review_snapshot_sha256,
        target_review_snapshot_sha256=target_review_snapshot_sha256,
        base_catalog_sha256=base_catalog_sha256,
        target_catalog_sha256=target_catalog_sha256,
        now=operation_time,
    )

    report_progress("LEDGER_PREPARED")
    with _open_database(
        database,
        read_only=False,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        connection.execute("BEGIN IMMEDIATE")
        try:
            report_progress("REVIEW_BASE_REATTESTATION_STARTED")
            actual_base_digests = review_snapshot_digests(connection)
            if (
                actual_base_digests.full_snapshot_sha256
                != base_review_snapshot_sha256
                or actual_base_digests.canonical_sha256
                != plan.review_canonical_sha256
                or actual_base_digests.catalog_sha256
                != base_catalog_sha256
                or connection.execute(
                    "PRAGMA schema_version"
                ).fetchone()
                != (base_schema_version,)
            ):
                raise SignalRetentionMaintenanceError(
                    "authorized legacy witness Review base snapshot changed"
                )
            report_progress("REVIEW_APPLY_STARTED")
            apply_review_install(connection)
            actual_target_catalog = review_catalog_sha256(connection)
            if actual_target_catalog != target_catalog_sha256:
                raise SignalRetentionMaintenanceError(
                    "authorized legacy witness Review target snapshot changed"
                )
            connection.commit()
            report_progress("REVIEW_COMMITTED")
        except BaseException:
            connection.rollback()
            raise
    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        generation = connection.execute(
            "SELECT generation FROM history_coverage_protected_generation "
            "WHERE singleton_id=1"
        ).fetchone()
        schema_version = connection.execute(
            "PRAGMA schema_version"
        ).fetchone()
        if (
            generation is None
            or len(generation) != 1
            or type(generation[0]) is not int
            or generation[0] < 0
            or schema_version is None
            or len(schema_version) != 1
            or type(schema_version[0]) is not int
            or schema_version[0] <= 0
        ):
            raise SignalRetentionMaintenanceError(
                "authorized legacy protected generation is invalid"
            )
        ledger.install_protected_generation_highwater(
            generation=generation[0],
            review_schema_version=schema_version[0],
            family_catalog_sha256=family_seal_catalog_sha256(connection),
            now=utc_now(),
        )
    ledger.commit_legacy_witness_mirror(
        review_plan_sha256=plan.review_plan_sha256,
        witness_sha256=witness_sha,
        now=utc_now(),
    )
    report_progress("LEDGER_COMMITTED")
    return _authorized_legacy_witness_report(
        "AUTHORIZED_LEGACY_V3_WITNESS_INSTALL",
        database,
        claim_ledger_file,
        plan,
        witness_sha256=witness_sha,
        ledger_phase="COMMITTED",
        resolution="INSTALLED",
    )


def resolve_authorized_legacy_v3_witness(
    database: Path,
    claim_ledger_file: Path,
    expected: Dict[str, Any],
    resolution: str,
    *,
    expected_database_identity: Optional[Tuple[int, int]] = None,
    database_scope: Optional["_DatabaseFileScope"] = None,
    claim_ledger_scope=None,
) -> AuthorizedLegacyWitnessMaintenanceReport:
    from .coverage_epoch_schema import (
        coverage_epoch_schema_status,
        validate_coverage_epoch_graph,
    )
    from .coverage_family_seal import (
        AuthorizedLegacyWitnessPlan,
        authorized_witness_sha256,
        family_seal_catalog_sha256,
        family_seal_schema_status,
        review_catalog_sha256,
        review_full_snapshot_sha256,
        validate_family_seal_graph,
    )
    from .n16_claim_ledger import N16PermanentClaimLedger

    if resolution not in {"SAFE_ABORT", "SAFE_COMMIT"}:
        raise SignalRetentionMaintenanceError(
            "authorized legacy witness resolution is invalid"
        )
    ledger = N16PermanentClaimLedger(
        claim_ledger_file,
        file_scope=claim_ledger_scope,
    )
    mirror = ledger.legacy_witness_mirror()
    if mirror is None:
        raise SignalRetentionMaintenanceError(
            "authorized legacy witness mirror is absent"
        )
    if mirror[0] not in {"EMPTY", "PREPARED", "COMMITTED"}:
        raise SignalRetentionMaintenanceError(
            "authorized legacy witness mirror phase is invalid"
        )
    generation_highwater_evidence = None
    with _open_database(
        database,
        read_only=True,
        expected_identity=expected_database_identity,
        file_scope=database_scope,
    ) as connection:
        status = coverage_epoch_schema_status(connection)
        snapshot_sha256 = review_full_snapshot_sha256(connection)
        catalog_sha256 = review_catalog_sha256(connection)
        if status == "PRE_TERMINAL_RECEIPT":
            _attest_n16_claim_boundary(connection, ledger)
            _attest_n17_lifecycle_boundary(connection)
            _attest_n19_lifecycle_boundary(connection)
            _attest_n18_lifecycle_boundary(connection)
            _attest_n20_lifecycle_boundary(connection)
            _attest_micro_lifecycle_boundary(connection)
            plan, evidence_json = (
                _authorized_legacy_v3_witness_plan_from_connection(
                    connection
                )
            )
            witness_sha = authorized_witness_sha256(plan, evidence_json)
            classification = "SAFE_ABORT"
        elif (
            status == "AUTHORIZED_LEGACY_V3"
            and family_seal_schema_status(connection) == "CURRENT"
        ):
            _attest_n16_claim_boundary(connection, ledger)
            _attest_n17_lifecycle_boundary(connection)
            _attest_n19_lifecycle_boundary(connection)
            _attest_n18_lifecycle_boundary(connection)
            _attest_n20_lifecycle_boundary(connection)
            _attest_micro_lifecycle_boundary(connection)
            validate_coverage_epoch_graph(connection)
            validate_family_seal_graph(connection)
            row = connection.execute(
                "SELECT symbol,family_id,structure_id,"
                "terminal_evidence_sha256,state_row_sha256,"
                "v3_receipt_count,v3_receipt_set_sha256,"
                "v3_coverage_graph_sha256,v3_catalog_sha256,"
                "pre_review_canonical_sha256,authorization_sha256,"
                "review_plan_sha256,witness_sha256 "
                "FROM history_coverage_n19_legacy_unbound_witnesses "
                "WHERE witness_id=1"
            ).fetchall()
            if len(row) != 1:
                raise SignalRetentionMaintenanceError(
                    "authorized legacy Review witness is missing"
                )
            value = tuple(row[0])
            plan = AuthorizedLegacyWitnessPlan(*value[:12])
            witness_sha = value[12]
            classification = "SAFE_COMMIT"
            generation_row = connection.execute(
                "SELECT generation "
                "FROM history_coverage_protected_generation "
                "WHERE singleton_id=1"
            ).fetchone()
            schema_version_row = connection.execute(
                "PRAGMA schema_version"
            ).fetchone()
            if (
                generation_row is None
                or len(generation_row) != 1
                or type(generation_row[0]) is not int
                or generation_row[0] < 0
                or schema_version_row is None
                or len(schema_version_row) != 1
                or type(schema_version_row[0]) is not int
                or schema_version_row[0] <= 0
            ):
                raise SignalRetentionMaintenanceError(
                    "authorized legacy protected generation is invalid"
                )
            generation_highwater_evidence = (
                generation_row[0],
                schema_version_row[0],
                family_seal_catalog_sha256(connection),
            )
        else:
            raise SignalRetentionMaintenanceError(
                "authorized legacy witness recovery is ambiguous"
            )
    _require_authorized_legacy_witness_expectations(plan, expected)
    if mirror[0] == "EMPTY":
        if (
            resolution != "SAFE_ABORT"
            or classification != "SAFE_ABORT"
        ):
            raise SignalRetentionMaintenanceError(
                "empty legacy witness mirror is not an exact aborted base"
            )
        return _authorized_legacy_witness_report(
            "AUTHORIZED_LEGACY_V3_WITNESS_RESOLVE",
            database,
            claim_ledger_file,
            plan,
            witness_sha256=witness_sha,
            ledger_phase="EMPTY",
            resolution="ALREADY_ABORTED",
        )
    if (
        mirror[1] != plan.review_plan_sha256
        or mirror[2] != witness_sha
        or mirror[3] != plan.review_canonical_sha256
        or mirror[5] != ledger.ledger_uuid()
        or (
            classification == "SAFE_ABORT"
            and (
                snapshot_sha256 != mirror[6]
                or catalog_sha256 != mirror[8]
            )
        )
        or (
            classification == "SAFE_COMMIT"
            and (
                snapshot_sha256 != mirror[7]
                or catalog_sha256 != mirror[9]
            )
        )
    ):
        raise SignalRetentionMaintenanceError(
            "authorized legacy witness mirror conflicts with Review"
        )
    if classification != resolution:
        raise SignalRetentionMaintenanceError(
            "authorized legacy witness resolution is not safe"
        )
    if classification == "SAFE_COMMIT":
        if generation_highwater_evidence is None:
            raise SignalRetentionMaintenanceError(
                "authorized legacy protected generation is unavailable"
            )
        ledger.install_protected_generation_highwater(
            generation=generation_highwater_evidence[0],
            review_schema_version=generation_highwater_evidence[1],
            family_catalog_sha256=generation_highwater_evidence[2],
            now=utc_now(),
        )
    if mirror[0] == "COMMITTED":
        if resolution != "SAFE_COMMIT":
            raise SignalRetentionMaintenanceError(
                "committed legacy witness cannot be aborted"
            )
        return _authorized_legacy_witness_report(
            "AUTHORIZED_LEGACY_V3_WITNESS_RESOLVE",
            database,
            claim_ledger_file,
            plan,
            witness_sha256=witness_sha,
            ledger_phase="COMMITTED",
            resolution="ALREADY_COMMITTED",
        )
    if mirror[0] != "PREPARED":
        raise SignalRetentionMaintenanceError(
            "authorized legacy witness has no exact prepared recovery"
        )
    if resolution == "SAFE_ABORT":
        ledger.abort_legacy_witness_mirror(
            review_plan_sha256=plan.review_plan_sha256,
            witness_sha256=witness_sha,
            now=utc_now(),
        )
        phase = "EMPTY"
    else:
        ledger.commit_legacy_witness_mirror(
            review_plan_sha256=plan.review_plan_sha256,
            witness_sha256=witness_sha,
            now=utc_now(),
        )
        phase = "COMMITTED"
    return _authorized_legacy_witness_report(
        "AUTHORIZED_LEGACY_V3_WITNESS_RESOLVE",
        database,
        claim_ledger_file,
        plan,
        witness_sha256=witness_sha,
        ledger_phase=phase,
        resolution=resolution,
    )


def _resolve_n16_publication_boundary(
    database: Path,
    claim_ledger_file: Path,
    *,
    expected_database_identity: Optional[Tuple[int, int]] = None,
    database_scope: Optional["_DatabaseFileScope"] = None,
    claim_ledger_scope=None,
) -> Tuple[str, Any]:
    """Resolve one exact PREPARED publication under the stopped-service lock."""

    from .n16_claim_ledger import N16PermanentClaimLedger
    from .recorder import (
        _classify_prepared_n16_review_connection,
        _validate_n16_permanent_graph,
        _n16_all_review_claims,
        _n16_review_claim_summary,
    )

    database = Path(database)
    claim_ledger_file = Path(claim_ledger_file)
    if not database.is_absolute() or not claim_ledger_file.is_absolute():
        raise SignalRetentionMaintenanceError(
            "N16 Review and claim ledger paths must be absolute"
        )
    ledger = N16PermanentClaimLedger(
        claim_ledger_file,
        file_scope=claim_ledger_scope,
    )
    if not ledger.exists:
        raise SignalRetentionMaintenanceError(
            "N16 publication resolution requires the state claim ledger"
        )
    try:
        prepared = ledger.prepared_publication()
        with _open_database(
            database,
            read_only=True,
            expected_identity=expected_database_identity,
            file_scope=database_scope,
        ) as connection:
            if _validate_n16_maintenance_boundary(connection) != "CURRENT":
                raise SignalRetentionMaintenanceError(
                    "N16 publication resolution requires CURRENT Review schema"
                )
            if prepared is None:
                summary = _attest_n16_claim_boundary(
                    connection, ledger
                )
                return "READY", summary
            _validate_n16_permanent_graph(connection)
            review_claims = _n16_all_review_claims(connection)
            base_count = prepared.base_summary.confirmed_claim_count
            if len(review_claims) < base_count:
                raise SignalRetentionMaintenanceError(
                    "N16 prepared Review base claims are incomplete"
                )
            ledger.attest_prepared_base(
                prepared,
                review_claims[:base_count],
            )
            classification, summary = (
                _classify_prepared_n16_review_connection(
                    connection, ledger, prepared
                )
            )
        if classification == "SAFE_ABORT":
            ledger.abort_prepared(prepared, utc_now())
        elif classification == "SAFE_COMMIT":
            ledger.commit_prepared(prepared, summary, utc_now())
        else:
            raise SignalRetentionMaintenanceError(
                "N16 prepared publication remains ambiguous"
            )
        with _open_database(
            database,
            read_only=True,
            expected_identity=expected_database_identity,
            file_scope=database_scope,
        ) as connection:
            final_summary = _attest_n16_claim_boundary(
                connection, ledger
            )
        return classification, final_summary
    except SignalRetentionMaintenanceError:
        raise
    except Exception as exc:
        raise SignalRetentionMaintenanceError(
            "N16 prepared publication could not be resolved"
        ) from exc
_MUTABLE_TABLES = _RETENTION_MUTATION_TABLES | frozenset({"sqlite_sequence"})
_REQUIRED_RETENTION_TABLES = frozenset(
    {
        "strategy_signal_batches",
        "strategy_signal_current",
        "strategy_passed_signal_audits",
        "strategy_passed_structure_ledger",
    }
)
_SIGNAL_COLUMNS = (
    "id",
    "scan_id",
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
    "detail_json",
    "created_at",
)


class SignalRetentionMaintenanceError(RuntimeError):
    """Raised when maintenance cannot prove that deletion is safe."""


@dataclass(frozen=True)
class N16LifecycleMaintenanceReport:
    mode: str
    database: str
    claim_ledger: str
    ledger_phase: str
    confirmed_claim_count: int
    resolution: str


@dataclass(frozen=True)
class N17EvidenceRepairReport:
    mode: str
    database: str
    claim_ledger: str
    repaired_row_count: int
    resolution: str


@dataclass(frozen=True)
class AuthorizedLegacyWitnessMaintenanceReport:
    mode: str
    database: str
    claim_ledger: str
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
    witness_sha256: Optional[str]
    ledger_phase: str
    resolution: str


@dataclass(frozen=True)
class SignalRetentionReport:
    mode: str
    database: str
    keep_scan_id: Optional[int]
    latest_completed_scan_id: Optional[int]
    migration_state: str
    source_signal_count: int
    source_passed_count: int
    source_ledger_count: int
    retained_signal_count: int
    deletable_signal_count: int
    deleted_signal_count: int
    audit_count: int
    ledger_count: int
    source_manifest_sha256: str
    retained_manifest_sha256: str
    keep_attestation_verified: bool
    protected_manifest_sha256: str
    n13_rotation_sha256: Optional[str]
    strategy_signal_sequence: Optional[int]
    foreign_key_violations: int
    integrity_check: str
    schema_ready: bool
    vacuum_performed: bool = False
    vacuum_destination: Optional[str] = None
    staging_scan_id: Optional[int] = None
    staging_signal_count: int = 0
    staging_passed_claim_count: int = 0
    staging_ledger_claim_count: int = 0
    staging_micro_claim_count: int = 0
    staging_cleanup_required: bool = False
    staging_cleanup_performed: bool = False


@dataclass(frozen=True)
class _SourceSummary:
    signal_count: int
    passed_count: int
    ledger_count: int
    retained_count: int
    deletable_count: int
    cutoff_signal_id: Optional[int]
    manifest_sha256: str
    retained_manifest_sha256: str
    retained_first_id: Optional[int]
    retained_last_id: Optional[int]
    ledger_sources: Dict[Tuple[str, str], Tuple[int, str]]


@dataclass(frozen=True)
class _StagingCleanupPlan:
    scan_id: int
    recorded_count: int
    first_signal_id: Optional[int]
    last_signal_id: Optional[int]
    manifest_sha256: str
    passed_claim_count: int
    ledger_claim_count: int
    micro_claim_count: int


def _strict_text(value: Any, name: str, maximum: int) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > maximum
        or value.strip() != value
        or any(ord(character) < 32 for character in value)
    ):
        raise SignalRetentionMaintenanceError(
            "%s must be a canonical built-in string" % name
        )
    return value


def _strict_bounded_text(value: Any, name: str, maximum: int) -> str:
    if (
        type(value) is not str
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise SignalRetentionMaintenanceError(
            "%s must be a bounded built-in string" % name
        )
    return value


def _strict_json_loads(value: Any, name: str) -> Any:
    if type(value) is not str:
        raise SignalRetentionMaintenanceError(
            "%s must be a built-in JSON string" % name
        )

    def reject_duplicates(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
        parsed = {}  # type: Dict[str, Any]
        for key, item in pairs:
            if type(key) is not str or key in parsed:
                raise SignalRetentionMaintenanceError(
                    "%s contains an invalid or duplicate key" % name
                )
            parsed[key] = item
        return parsed

    def reject_constant(constant: str) -> Any:
        raise SignalRetentionMaintenanceError(
            "%s contains non-standard number %s" % (name, constant)
        )

    try:
        return json.loads(
            value,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except SignalRetentionMaintenanceError:
        raise
    except (TypeError, ValueError, RecursionError) as exc:
        raise SignalRetentionMaintenanceError("invalid %s" % name) from exc


def _validate_json_value(value: Any, name: str, depth: int = 0) -> None:
    if depth > 100:
        raise SignalRetentionMaintenanceError("%s is too deeply nested" % name)
    if value is None or type(value) is bool or type(value) is str:
        return
    if type(value) is int:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise SignalRetentionMaintenanceError("%s contains non-finite data" % name)
        return
    if type(value) is list:
        for item in value:
            _validate_json_value(item, name, depth + 1)
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise SignalRetentionMaintenanceError(
                    "%s contains a non-string key" % name
                )
            _validate_json_value(item, name, depth + 1)
        return
    raise SignalRetentionMaintenanceError("%s contains a non-JSON value" % name)


def _validate_signal_row(row: Sequence[Any]) -> Tuple[Any, ...]:
    if len(row) != len(_SIGNAL_COLUMNS):
        raise SignalRetentionMaintenanceError("strategy signal row shape is invalid")
    values = tuple(row)
    signal_id, scan_id = values[0], values[1]
    if type(signal_id) is not int or signal_id <= 0:
        raise SignalRetentionMaintenanceError("strategy signal id is invalid")
    if scan_id is not None and (type(scan_id) is not int or scan_id <= 0):
        raise SignalRetentionMaintenanceError(
            "strategy signal scan_id is invalid at id=%s" % signal_id
        )
    strategy_id = _strict_text(values[2], "strategy_id", 32)
    if strategy_id not in _STRATEGY_IDS:
        raise SignalRetentionMaintenanceError(
            "strategy signal strategy_id is unsupported at id=%s" % signal_id
        )
    symbol = _strict_text(values[3], "symbol", 64)
    try:
        canonical_exchange_symbol(symbol)
    except ValueError:
        raise SignalRetentionMaintenanceError(
            "strategy signal symbol is not a canonical USDT pair at id=%s"
            % signal_id
        )
    _strict_bounded_text(values[4], "funding_rate", 128)
    _strict_bounded_text(values[6], "trend_slope", 128)
    if type(values[7]) is not int or values[7] not in (0, 1):
        raise SignalRetentionMaintenanceError(
            "strategy signal current_bullish is invalid at id=%s" % signal_id
        )
    if type(values[8]) is not int or values[8] not in (0, 1):
        raise SignalRetentionMaintenanceError(
            "strategy signal passed is invalid at id=%s" % signal_id
        )
    _strict_text(values[9], "decision", 128)
    _strict_text(values[10], "reason", 512)
    if values[11] is not None:
        _strict_text(values[11], "structure_id", 256)
    if values[8] == 1 and strategy_id in _LEDGER_STRATEGIES and values[11] is None:
        raise SignalRetentionMaintenanceError(
            "passed ledger strategy is missing structure_id: %s signal_id=%s"
            % (strategy_id, signal_id)
        )
    _strict_text(values[13], "created_at", 128)
    patterns = _strict_json_loads(values[5], "matched_patterns")
    if type(patterns) is not list or any(type(item) is not str for item in patterns):
        raise SignalRetentionMaintenanceError(
            "matched_patterns must be a JSON list of built-in strings"
        )
    detail = _strict_json_loads(values[12], "detail_json")
    if type(detail) is not dict:
        raise SignalRetentionMaintenanceError("detail_json must be a JSON object")
    _validate_json_value(detail, "detail_json")
    return values


def _signal_evidence(row: Sequence[Any]) -> Dict[str, Any]:
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


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)


def _signal_evidence_sha256(row: Sequence[Any]) -> str:
    return hashlib.sha256(
        _canonical_json(_signal_evidence(row)).encode("utf-8")
    ).hexdigest()


def _typed_scalar(value: Any) -> List[Any]:
    if value is None:
        return ["null", None]
    if type(value) is int:
        return ["int", str(value)]
    if type(value) is float:
        if not math.isfinite(value):
            raise SignalRetentionMaintenanceError("non-finite SQLite value")
        return ["float", value.hex()]
    if type(value) is str:
        return ["str", value]
    if type(value) is bytes:
        return ["bytes", value.hex()]
    raise SignalRetentionMaintenanceError("unsupported SQLite value type")


def _update_typed_hash(digest: Any, row: Sequence[Any]) -> None:
    encoded = json.dumps(
        [_typed_scalar(value) for value in row],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)


@dataclass
class _DatabaseFileScope:
    """Mutable proof for one attested SQLite main file and its sidecars.

    SQLite itself may create, checkpoint, or remove a safe sidecar while a
    verified connection is open.  We therefore require an exact identity match
    before every open, then refresh the proof only after that controlled
    connection has closed and all remaining files pass the same isolation
    checks.
    """

    path: Path
    main_identity: Tuple[int, int]
    parent_identity: Tuple[int, int]
    sidecar_identities: Tuple[Optional[Tuple[int, int]], ...]
    roots: Tuple[Path, ...]

    def _validate_location(self, path: Path) -> None:
        if path != self.path:
            raise SignalRetentionMaintenanceError(
                "SQLite database path differs from scope proof"
            )
        if (
            _directory_identity(path.parent, "SQLite database parent directory")
            != self.parent_identity
        ):
            raise SignalRetentionMaintenanceError(
                "SQLite database parent identity changed before open"
            )
        if self.roots and not any(
            _is_within(root, path.resolve()) for root in self.roots
        ):
            raise SignalRetentionMaintenanceError(
                "SQLite database escaped the Binance allowlist"
            )

    def validate_before_open(self, path: Path) -> None:
        self._validate_location(path)
        if _regular_file_identity(path, "SQLite database") != self.main_identity:
            raise SignalRetentionMaintenanceError(
                "SQLite database identity changed before open"
            )
        _validate_database_sidecars(
            path,
            "SQLite database",
            expected=self.sidecar_identities,
        )

    def refresh_after_close(self, path: Path) -> None:
        self._validate_location(path)
        if _regular_file_identity(path, "SQLite database") != self.main_identity:
            raise SignalRetentionMaintenanceError(
                "SQLite database identity changed while open"
            )
        self.sidecar_identities = _validate_database_sidecars(
            path, "SQLite database"
        )


def _maintenance_regular_fd_snapshot() -> Dict[int, Tuple[int, int]]:
    descriptor_names = None
    # Linux exposes the authoritative per-process descriptor table directly
    # through procfs.  Prefer it over the compatibility /dev/fd alias; macOS
    # has no procfs and falls back to /dev/fd.
    for directory in ("/proc/self/fd", "/dev/fd"):
        try:
            descriptor_names = os.listdir(directory)
            break
        except OSError:
            continue
    if descriptor_names is None:
        raise SignalRetentionMaintenanceError(
            "maintenance cannot attest opened SQLite file descriptors"
        )
    snapshot = {}  # type: Dict[int, Tuple[int, int]]
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


def _validate_maintenance_opened_descriptor(
    before: Dict[int, Tuple[int, int]],
    expected_identity: Tuple[int, int],
) -> None:
    after = _maintenance_regular_fd_snapshot()
    if not _expected_database_fd_increment_is_attested(
        before,
        after,
        expected_identity,
    ):
        raise SignalRetentionMaintenanceError(
            "opened SQLite database descriptor is not attested"
        )


@contextmanager
def _attested_maintenance_connection(opener, expected_identity):
    with _MAINTENANCE_SQLITE_CONNECTION_LOCK:
        descriptor_snapshot = _maintenance_regular_fd_snapshot()
        connection = opener()
        try:
            _validate_maintenance_opened_descriptor(
                descriptor_snapshot,
                expected_identity,
            )
            yield connection
        finally:
            connection.close()


def _anchored_directory_entry_identity(
    parent_descriptor: int,
    name: str,
    label: str,
) -> Tuple[int, int]:
    try:
        details = os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except OSError as exc:
        raise SignalRetentionMaintenanceError(
            "%s is unavailable" % label
        ) from exc
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
        raise SignalRetentionMaintenanceError(
            "%s must be one non-symlink directory" % label
        )
    return int(details.st_dev), int(details.st_ino)


def _assert_anchored_directory_name(
    parent_descriptor: int,
    name: str,
    expected_identity: Tuple[int, int],
) -> None:
    if (
        _anchored_directory_entry_identity(
            parent_descriptor,
            name,
            "authorized legacy simulation directory",
        )
        != expected_identity
    ):
        raise SignalRetentionMaintenanceError(
            "authorized legacy simulation identity changed"
        )


def _find_anchored_directory_name(
    parent_descriptor: int,
    expected_identity: Tuple[int, int],
) -> str:
    matches = _anchored_directory_names(
        parent_descriptor,
        expected_identity,
    )
    if len(matches) != 1:
        raise SignalRetentionMaintenanceError(
            "authorized legacy simulation directory identity is not uniquely anchored"
        )
    return matches[0]


def _anchored_directory_names(
    parent_descriptor: int,
    expected_identity: Tuple[int, int],
) -> List[str]:
    matches = []
    try:
        names = os.listdir(parent_descriptor)
    except OSError as exc:
        raise SignalRetentionMaintenanceError(
            "authorized legacy simulation parent cannot be inspected"
        ) from exc
    for name in names:
        try:
            details = os.stat(
                name,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise SignalRetentionMaintenanceError(
                "authorized legacy simulation directory cannot be located"
            ) from exc
        if (
            stat.S_ISDIR(details.st_mode)
            and (int(details.st_dev), int(details.st_ino))
            == expected_identity
        ):
            matches.append(name)
    return matches


def _open_anchored_current_parent(
    stage_descriptor: int,
    directory_flags: int,
) -> Tuple[int, Tuple[int, int]]:
    try:
        parent_descriptor = os.open(
            "..",
            directory_flags,
            dir_fd=stage_descriptor,
        )
    except OSError as exc:
        raise SignalRetentionMaintenanceError(
            "authorized legacy simulation current parent cannot be opened"
        ) from exc
    try:
        identity = _directory_descriptor_identity(
            parent_descriptor,
            "authorized legacy simulation current parent",
        )
    except BaseException:
        os.close(parent_descriptor)
        raise
    return parent_descriptor, identity


def _current_parent_identity(
    stage_descriptor: int,
    directory_flags: int,
) -> Tuple[int, int]:
    parent_descriptor, identity = _open_anchored_current_parent(
        stage_descriptor,
        directory_flags,
    )
    try:
        return identity
    finally:
        _close_descriptor_after_cleanup(parent_descriptor)


def _anchored_stage_is_stably_absent(
    stage_descriptor: int,
    stage_identity: Tuple[int, int],
    parent_descriptor: int,
    parent_identity: Tuple[int, int],
    directory_flags: int,
) -> bool:
    if _anchored_directory_names(parent_descriptor, stage_identity):
        return False
    confirmation_descriptor, confirmation_identity = (
        _open_anchored_current_parent(
            stage_descriptor,
            directory_flags,
        )
    )
    try:
        return (
            confirmation_identity == parent_identity
            and not _anchored_directory_names(
                confirmation_descriptor,
                stage_identity,
            )
        )
    finally:
        _close_descriptor_after_cleanup(confirmation_descriptor)


def _remove_anchored_simulation_directory(
    stage_descriptor: int,
    stage_identity: Tuple[int, int],
    directory_flags: int,
) -> None:
    """Remove the private stage through its current parent, not its old path.

    An open directory descriptor follows a directory across a rename.  Each
    attempt therefore resolves ``..`` again through ``stage_descriptor`` and
    locates the one child with the original inode.  A concurrent second move
    makes either the lookup or the parent recheck fail and is retried without
    touching any same-name replacement.
    """

    last_error = None  # type: Optional[BaseException]
    for _attempt in range(16):
        if (
            _directory_descriptor_identity(
                stage_descriptor,
                "authorized legacy simulation directory",
            )
            != stage_identity
        ):
            raise SignalRetentionMaintenanceError(
                "authorized legacy simulation directory descriptor changed"
            )
        try:
            if os.listdir(stage_descriptor):
                raise SignalRetentionMaintenanceError(
                    "authorized legacy simulation directory is not empty"
                )
        except OSError as exc:
            raise SignalRetentionMaintenanceError(
                "authorized legacy simulation directory cannot be inspected"
            ) from exc

        parent_descriptor = None  # type: Optional[int]
        try:
            parent_descriptor, parent_identity = (
                _open_anchored_current_parent(
                    stage_descriptor,
                    directory_flags,
                )
            )
            try:
                anchored_name = _find_anchored_directory_name(
                    parent_descriptor,
                    stage_identity,
                )
            except SignalRetentionMaintenanceError as exc:
                last_error = exc
                continue
            if (
                _anchored_directory_entry_identity(
                    parent_descriptor,
                    anchored_name,
                    "authorized legacy simulation directory",
                )
                != stage_identity
            ):
                last_error = SignalRetentionMaintenanceError(
                    "authorized legacy simulation directory identity changed"
                )
                continue
            if (
                _current_parent_identity(
                    stage_descriptor,
                    directory_flags,
                )
                != parent_identity
            ):
                last_error = SignalRetentionMaintenanceError(
                    "authorized legacy simulation current parent changed"
                )
                continue
            try:
                os.rmdir(
                    anchored_name,
                    dir_fd=parent_descriptor,
                )
            except OSError as exc:
                last_error = exc
                if _anchored_stage_is_stably_absent(
                    stage_descriptor,
                    stage_identity,
                    parent_descriptor,
                    parent_identity,
                    directory_flags,
                ):
                    return
                continue
            if not _anchored_stage_is_stably_absent(
                stage_descriptor,
                stage_identity,
                parent_descriptor,
                parent_identity,
                directory_flags,
            ):
                last_error = SignalRetentionMaintenanceError(
                    "authorized legacy simulation directory removal is not confirmed"
                )
                continue
            return
        finally:
            if parent_descriptor is not None:
                _close_descriptor_after_cleanup(parent_descriptor)
    raise SignalRetentionMaintenanceError(
        "authorized legacy simulation directory could not be removed through "
        "its current parent"
    ) from last_error


def _remove_anchored_simulation_files(
    stage_descriptor: int,
    database_identity: Tuple[int, int],
) -> None:
    database_name = "review-simulation.sqlite3"
    allowed = {
        database_name,
        *(database_name + suffix for suffix in _SQLITE_SIDECAR_SUFFIXES),
    }
    try:
        names = set(os.listdir(stage_descriptor))
    except OSError as exc:
        raise SignalRetentionMaintenanceError(
            "authorized legacy simulation directory cannot be cleaned"
        ) from exc
    unknown = names.difference(allowed)
    if unknown:
        raise SignalRetentionMaintenanceError(
            "authorized legacy simulation directory contains unknown files"
        )
    for name in sorted(names):
        identity = _anchored_regular_file_identity(
            stage_descriptor,
            name,
            "authorized legacy simulation file",
        )
        if name == database_name and identity != database_identity:
            raise SignalRetentionMaintenanceError(
                "authorized legacy simulation database identity changed"
            )
        try:
            os.unlink(name, dir_fd=stage_descriptor)
        except OSError as exc:
            raise SignalRetentionMaintenanceError(
                "authorized legacy simulation file cleanup failed"
            ) from exc
    if os.listdir(stage_descriptor):
        raise SignalRetentionMaintenanceError(
            "authorized legacy simulation cleanup is incomplete"
        )


@contextmanager
def _anchored_file_backed_review_simulation(
    source: sqlite3.Connection,
) -> Iterator[sqlite3.Connection]:
    """Copy Review into a private, descriptor-anchored file simulation.

    Every operation on the private database is relative to descriptors opened
    before the pathname can be replaced.  Cleanup finds the original staging
    inode through the attested parent descriptor, so a same-name replacement
    and its contents are never removed.
    """

    directory_flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        directory_flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        directory_flags |= os.O_NOFOLLOW
    file_flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        file_flags |= os.O_NOFOLLOW
    database_name = "review-simulation.sqlite3"
    parent_path = Path(tempfile.gettempdir()).resolve(strict=True)
    parent_identity = _directory_identity(
        parent_path,
        "authorized legacy simulation parent",
    )
    parent_descriptor = None  # type: Optional[int]
    stage_descriptor = None  # type: Optional[int]
    original_cwd_descriptor = None  # type: Optional[int]
    stage_name = None  # type: Optional[str]
    stage_identity = None  # type: Optional[Tuple[int, int]]
    database_identity = None  # type: Optional[Tuple[int, int]]
    operation_error = None  # type: Optional[BaseException]
    operation_traceback = None
    cleanup_errors = []  # type: List[BaseException]
    try:
        parent_descriptor = os.open(str(parent_path), directory_flags)
        if (
            _directory_descriptor_identity(
                parent_descriptor,
                "authorized legacy simulation parent",
            )
            != parent_identity
        ):
            raise SignalRetentionMaintenanceError(
                "authorized legacy simulation parent identity changed"
            )
        for _attempt in range(32):
            candidate = ".authorized-legacy-review-simulation-" + secrets.token_hex(16)
            try:
                os.mkdir(candidate, 0o700, dir_fd=parent_descriptor)
            except FileExistsError:
                continue
            stage_name = candidate
            break
        if stage_name is None:
            raise SignalRetentionMaintenanceError(
                "authorized legacy simulation directory could not be allocated"
            )
        stage_descriptor = os.open(
            stage_name,
            directory_flags,
            dir_fd=parent_descriptor,
        )
        stage_details = os.fstat(stage_descriptor)
        stage_identity = (
            int(stage_details.st_dev),
            int(stage_details.st_ino),
        )
        if (
            not stat.S_ISDIR(stage_details.st_mode)
            or stage_details.st_mode & 0o077
        ):
            raise SignalRetentionMaintenanceError(
                "authorized legacy simulation directory is not isolated"
            )
        _assert_anchored_directory_name(
            parent_descriptor,
            stage_name,
            stage_identity,
        )
        database_descriptor = os.open(
            database_name,
            file_flags,
            0o600,
            dir_fd=stage_descriptor,
        )
        try:
            database_details = os.fstat(database_descriptor)
            if (
                not stat.S_ISREG(database_details.st_mode)
                or int(database_details.st_nlink) != 1
            ):
                raise SignalRetentionMaintenanceError(
                    "authorized legacy simulation database is not isolated"
                )
            database_identity = (
                int(database_details.st_dev),
                int(database_details.st_ino),
            )
        finally:
            os.close(database_descriptor)
        original_cwd_descriptor = os.open(".", directory_flags)
        with ExitStack() as stack:
            with _VACUUM_TARGET_CWD_LOCK:
                current_thread = threading.current_thread()
                if any(
                    thread is not current_thread and thread.is_alive()
                    for thread in threading.enumerate()
                ):
                    raise SignalRetentionMaintenanceError(
                        "authorized legacy simulation requires a single-threaded "
                        "maintenance process"
                    )
                cwd_changed = False
                try:
                    cwd_changed = True
                    os.fchdir(stage_descriptor)
                    simulated = stack.enter_context(
                        _attested_maintenance_connection(
                            lambda: sqlite3.connect(
                                "file:%s?mode=rw" % database_name,
                                uri=True,
                            ),
                            database_identity,
                        )
                    )
                finally:
                    if cwd_changed:
                        os.fchdir(original_cwd_descriptor)
            if (
                _anchored_regular_file_identity(
                    stage_descriptor,
                    database_name,
                    "authorized legacy simulation database",
                )
                != database_identity
            ):
                raise SignalRetentionMaintenanceError(
                    "authorized legacy simulation database identity changed"
                )
            simulated.execute("PRAGMA journal_mode=OFF")
            simulated.execute("PRAGMA synchronous=OFF")
            simulated.execute("PRAGMA temp_store=FILE")
            simulated.execute("PRAGMA cache_size=-4096")
            source.backup(simulated, pages=4096)
            _assert_anchored_directory_name(
                parent_descriptor,
                stage_name,
                stage_identity,
            )
            if (
                _anchored_regular_file_identity(
                    stage_descriptor,
                    database_name,
                    "authorized legacy simulation database",
                )
                != database_identity
            ):
                raise SignalRetentionMaintenanceError(
                    "authorized legacy simulation database identity changed"
                )
            yield simulated
            _assert_anchored_directory_name(
                parent_descriptor,
                stage_name,
                stage_identity,
            )
    except BaseException as exc:
        operation_error = exc
        operation_traceback = exc.__traceback__
    finally:
        if stage_descriptor is not None and database_identity is not None:
            try:
                _remove_anchored_simulation_files(
                    stage_descriptor,
                    database_identity,
                )
            except BaseException as exc:
                cleanup_errors.append(exc)
        if (
            stage_descriptor is not None
            and stage_identity is not None
        ):
            try:
                _remove_anchored_simulation_directory(
                    stage_descriptor,
                    stage_identity,
                    directory_flags,
                )
            except BaseException as exc:
                cleanup_errors.append(exc)
        if stage_descriptor is not None:
            try:
                _close_descriptor_after_cleanup(stage_descriptor)
            except BaseException as exc:
                cleanup_errors.append(exc)
            finally:
                stage_descriptor = None
        for descriptor in (
            original_cwd_descriptor,
            parent_descriptor,
        ):
            if descriptor is None:
                continue
            try:
                _close_descriptor_after_cleanup(descriptor)
            except BaseException as exc:
                cleanup_errors.append(exc)
    if cleanup_errors:
        raise SignalRetentionMaintenanceError(
            "authorized legacy simulation cleanup was incomplete: %s"
            % cleanup_errors[0]
        ) from operation_error
    if operation_error is not None:
        raise operation_error.with_traceback(operation_traceback)


@contextmanager
def _open_database(
    path: Path,
    read_only: bool,
    expected_identity: Optional[Tuple[int, int]] = None,
    file_scope: Optional[_DatabaseFileScope] = None,
    prewrite_validator: Optional[
        Callable[[sqlite3.Connection], None]
    ] = None,
) -> Iterator[sqlite3.Connection]:
    if file_scope is not None:
        file_scope.validate_before_open(path)
        identity = file_scope.main_identity
        sidecars = file_scope.sidecar_identities
    else:
        identity = _regular_file_identity(path, "SQLite database")
        # SQLite may inspect, truncate, unlink, or create journal sidecars
        # during sqlite3.connect itself.  Prove every pre-existing sidecar is
        # an unaliased application-owned regular file before that first call.
        sidecars = _validate_database_sidecars(path, "SQLite database")
    if expected_identity is not None and identity != expected_identity:
        raise SignalRetentionMaintenanceError(
            "SQLite database identity changed before open"
        )
    if read_only:
        wal_path = Path(str(path) + "-wal")
        wal_is_nonempty = (
            sidecars[0] is not None
            and os.lstat(str(wal_path)).st_size > 0
        )
        journal_exists = sidecars[2] is not None
        options = (
            "mode=ro"
            if wal_is_nonempty or journal_exists
            else "mode=ro&immutable=1"
        )
        opener = lambda: sqlite3.connect(
            path.as_uri() + "?" + options,
            uri=True,
        )
    else:
        # Maintenance receives an already-attested existing database.  mode=rw
        # prevents a parent-path swap from creating a new external file before
        # the opened-main identity check can run.
        opener = lambda: sqlite3.connect(
            path.as_uri() + "?mode=rw",
            uri=True,
        )
    try:
        with _attested_maintenance_connection(opener, identity) as connection:
            _verify_sqlite_main_identity(connection, path, identity)
            if read_only:
                connection.execute("PRAGMA query_only=ON")
            else:
                if prewrite_validator is not None:
                    prewrite_validator(connection)
                connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=30000")
            yield connection
    finally:
        if file_scope is not None:
            file_scope.refresh_after_close(path)


@contextmanager
def _open_immutable_database(
    path: Path,
    expected_identity: Optional[Tuple[int, int]] = None,
    file_scope: Optional[_DatabaseFileScope] = None,
) -> Iterator[sqlite3.Connection]:
    """Open one sidecar-free SQLite snapshot without any filesystem writes."""

    if file_scope is not None:
        file_scope.validate_before_open(path)
        identity = file_scope.main_identity
    else:
        identity = _regular_file_identity(path, "SQLite database")
    if expected_identity is not None and identity != expected_identity:
        raise SignalRetentionMaintenanceError(
            "SQLite database identity changed before immutable open"
        )
    _validate_database_sidecars(
        path,
        "SQLite database",
        require_absent=True,
    )
    uri = path.as_uri() + "?mode=ro&immutable=1"
    try:
        with _attested_maintenance_connection(
            lambda: sqlite3.connect(uri, uri=True),
            identity,
        ) as connection:
            _verify_sqlite_main_identity(connection, path, identity)
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=30000")
            yield connection
    finally:
        if file_scope is not None:
            file_scope.refresh_after_close(path)
        _validate_database_sidecars(
            path,
            "SQLite database",
            require_absent=True,
        )


@contextmanager
def _open_immutable_vacuum_source(
    path: Path,
    expected_identity: Optional[Tuple[int, int]] = None,
    file_scope: Optional[_DatabaseFileScope] = None,
) -> Iterator[sqlite3.Connection]:
    """Open a sidecar-free immutable source that may write only VACUUM target.

    ``query_only`` is intentionally not enabled: SQLite permits ``VACUUM
    INTO`` from a ``mode=ro&immutable=1`` source because only the destination
    is written.  No journal-mode pragma or read-write source connection is
    used.
    """

    if file_scope is not None:
        file_scope.validate_before_open(path)
        identity = file_scope.main_identity
    else:
        identity = _regular_file_identity(path, "SQLite database")
    if expected_identity is not None and identity != expected_identity:
        raise SignalRetentionMaintenanceError(
            "SQLite database identity changed before immutable VACUUM open"
        )
    _validate_database_sidecars(
        path,
        "SQLite database",
        require_absent=True,
    )
    uri = path.as_uri() + "?mode=ro&immutable=1"
    try:
        with _attested_maintenance_connection(
            lambda: sqlite3.connect(uri, uri=True),
            identity,
        ) as connection:
            _verify_sqlite_main_identity(connection, path, identity)
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=30000")
            yield connection
    finally:
        if file_scope is not None:
            file_scope.refresh_after_close(path)
        _validate_database_sidecars(
            path,
            "SQLite database",
            require_absent=True,
        )


def _regular_file_identity(path: Path, name: str) -> Tuple[int, int]:
    try:
        details = os.lstat(str(path))
    except OSError as exc:
        raise SignalRetentionMaintenanceError("%s is unavailable" % name) from exc
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        raise SignalRetentionMaintenanceError(
            "%s must be one non-symlink regular file" % name
        )
    if int(details.st_nlink) != 1:
        raise SignalRetentionMaintenanceError(
            "%s must have exactly one hard link" % name
        )
    return int(details.st_dev), int(details.st_ino)


def _database_sidecar_paths(path: Path) -> Tuple[Path, ...]:
    return tuple(Path(str(path) + suffix) for suffix in _SQLITE_SIDECAR_SUFFIXES)


def _capture_database_sidecars(
    path: Path,
    name: str,
) -> Tuple[Optional[Tuple[int, int]], ...]:
    identities = []  # type: List[Optional[Tuple[int, int]]]
    for sidecar in _database_sidecar_paths(path):
        if os.path.lexists(str(sidecar)):
            identities.append(
                _regular_file_identity(sidecar, "%s sidecar %s" % (name, sidecar.name))
            )
        else:
            identities.append(None)
    return tuple(identities)


def _validate_database_sidecars(
    path: Path,
    name: str,
    expected: Optional[Tuple[Optional[Tuple[int, int]], ...]] = None,
    require_absent: bool = False,
) -> Tuple[Optional[Tuple[int, int]], ...]:
    current = _capture_database_sidecars(path, name)
    if require_absent and any(identity is not None for identity in current):
        raise SignalRetentionMaintenanceError(
            "%s SQLite sidecars must not already exist" % name
        )
    if expected is not None and current != expected:
        raise SignalRetentionMaintenanceError(
            "%s SQLite sidecar identity changed before open" % name
        )
    return current


def _directory_identity(path: Path, name: str) -> Tuple[int, int]:
    try:
        details = os.lstat(str(path))
    except OSError as exc:
        raise SignalRetentionMaintenanceError("%s is unavailable" % name) from exc
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
        raise SignalRetentionMaintenanceError(
            "%s must be one non-symlink directory" % name
        )
    return int(details.st_dev), int(details.st_ino)


def _directory_descriptor_identity(
    descriptor: int,
    name: str,
) -> Tuple[int, int]:
    try:
        details = os.fstat(descriptor)
    except OSError as exc:
        raise SignalRetentionMaintenanceError(
            "%s descriptor is unavailable" % name
        ) from exc
    if not stat.S_ISDIR(details.st_mode):
        raise SignalRetentionMaintenanceError(
            "%s descriptor is not a directory" % name
        )
    return int(details.st_dev), int(details.st_ino)


def _close_descriptor_after_cleanup(descriptor: int) -> None:
    """Attempt one close without ever acting on a potentially reused fd."""

    # POSIX does not provide a portable way to prove that an fd number still
    # denotes the same open-file description after close() raises.  Another
    # open can already have reused the number, even for the same inode, so a
    # retry could close a descriptor owned by unrelated code.  Cleanup has
    # already finished before this helper is called; propagate the original
    # acknowledgement error but never inspect or close this number again.
    os.close(descriptor)


def _anchored_vacuum_names(filename: str) -> Tuple[str, ...]:
    if (
        type(filename) is not str
        or not filename
        or filename in (".", "..")
        or os.path.basename(filename) != filename
        or filename.startswith("file:")
        or any(character in filename for character in ("/", "\\", "?", "#", "\x00"))
        or len(os.fsencode(filename)) > 240
    ):
        raise SignalRetentionMaintenanceError(
            "VACUUM destination filename is not a strict local basename"
        )
    return tuple(filename + suffix for suffix in ("",) + _SQLITE_SIDECAR_SUFFIXES)


def _require_anchored_vacuum_names_absent(
    parent_descriptor: int,
    names: Tuple[str, ...],
) -> None:
    for name in names:
        try:
            os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise SignalRetentionMaintenanceError(
                "VACUUM destination cannot be inspected through its parent descriptor"
            ) from exc
        raise SignalRetentionMaintenanceError(
            "VACUUM destination or sidecar already exists"
        )


def _anchored_regular_file_identity(
    parent_descriptor: int,
    filename: str,
    name: str,
) -> Tuple[int, int]:
    try:
        details = os.stat(
            filename,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except OSError as exc:
        raise SignalRetentionMaintenanceError("%s is unavailable" % name) from exc
    if (
        stat.S_ISLNK(details.st_mode)
        or not stat.S_ISREG(details.st_mode)
        or int(details.st_nlink) != 1
    ):
        raise SignalRetentionMaintenanceError(
            "%s must be one non-symlink regular file" % name
        )
    return int(details.st_dev), int(details.st_ino)


def _remove_anchored_vacuum_output(
    parent_descriptor: int,
    filename: str,
    expected_identity: Tuple[int, int],
) -> None:
    current = _anchored_regular_file_identity(
        parent_descriptor,
        filename,
        "abandoned VACUUM output",
    )
    if current != expected_identity:
        raise SignalRetentionMaintenanceError(
            "abandoned VACUUM output identity changed before cleanup"
        )
    try:
        os.unlink(filename, dir_fd=parent_descriptor)
    except OSError as exc:
        raise SignalRetentionMaintenanceError(
            "abandoned VACUUM output could not be removed safely"
        ) from exc


@dataclass
class _AnchoredVacuumOutput:
    """An output whose parent descriptor stays live through all validation."""

    parent_descriptor: int
    filename: str
    identity: Tuple[int, int]
    closed: bool = False

    def commit(self) -> None:
        if self.closed:
            raise SignalRetentionMaintenanceError(
                "VACUUM output descriptor was already closed"
            )
        current = _anchored_regular_file_identity(
            self.parent_descriptor,
            self.filename,
            "validated VACUUM output",
        )
        if current != self.identity:
            raise SignalRetentionMaintenanceError(
                "VACUUM output identity changed before commit"
            )
        original_descriptor = self.parent_descriptor
        try:
            cleanup_descriptor = os.dup(original_descriptor)
        except OSError as exc:
            raise SignalRetentionMaintenanceError(
                "VACUUM output commit guard could not be created"
            ) from exc
        self.parent_descriptor = cleanup_descriptor
        try:
            os.close(original_descriptor)
        except BaseException as close_error:
            # Even when close actually completed before raising, the duplicate
            # remains anchored to the same directory inode.  A failed commit
            # can therefore remove the exact output instead of falling back to
            # the mutable pathname or leaving a compacted database behind.
            # Never inspect or retry original_descriptor: its number may now
            # belong to an unrelated open.  The independently duplicated
            # cleanup descriptor remains anchored to the attested directory.
            try:
                self.discard()
            except BaseException as cleanup_error:
                raise SignalRetentionMaintenanceError(
                    "VACUUM output could not be removed after commit "
                    "descriptor failure"
                ) from cleanup_error
            raise SignalRetentionMaintenanceError(
                "VACUUM output commit descriptor close failed"
            ) from close_error

        try:
            failure_cleanup_descriptor = os.dup(cleanup_descriptor)
        except OSError as exc:
            try:
                self.discard()
            except BaseException as cleanup_error:
                raise SignalRetentionMaintenanceError(
                    "VACUUM output could not be removed after commit "
                    "guard allocation failure"
                ) from cleanup_error
            raise SignalRetentionMaintenanceError(
                "VACUUM output commit failure guard could not be created"
            ) from exc

        # If closing the first guard loses its acknowledgement, only the
        # independent failure guard may be used for cleanup.  In particular,
        # never fstat, reuse as dir_fd, or close cleanup_descriptor again.
        self.parent_descriptor = failure_cleanup_descriptor
        try:
            os.close(cleanup_descriptor)
        except BaseException as close_error:
            try:
                self.discard()
            except BaseException as cleanup_error:
                raise SignalRetentionMaintenanceError(
                    "VACUUM output could not be removed after commit "
                    "guard failure"
                ) from cleanup_error
            raise SignalRetentionMaintenanceError(
                "VACUUM output commit guard close failed"
            ) from close_error

        # This is the last independent descriptor.  The output is already
        # committed and no failure cleanup remains to perform.  A close error
        # is necessarily acknowledgement-ambiguous; retrying or inspecting the
        # number could affect a replacement fd.  Treat the single close attempt
        # as retirement of our ownership and let this short-lived maintenance
        # process release any genuinely unclosed descriptor at process exit.
        retired_descriptor = failure_cleanup_descriptor
        self.parent_descriptor = -1
        self.closed = True
        try:
            os.close(retired_descriptor)
        except BaseException:
            pass

    def discard(self) -> None:
        if self.closed:
            return
        try:
            last_error = None  # type: Optional[BaseException]
            for _attempt in range(2):
                try:
                    _remove_anchored_vacuum_output(
                        self.parent_descriptor,
                        self.filename,
                        self.identity,
                    )
                    last_error = None
                    break
                except BaseException as exc:
                    last_error = exc
                    try:
                        os.stat(
                            self.filename,
                            dir_fd=self.parent_descriptor,
                            follow_symlinks=False,
                        )
                    except FileNotFoundError:
                        last_error = None
                        break
                    except BaseException:
                        pass
            if last_error is not None:
                raise last_error
        finally:
            retired_descriptor = self.parent_descriptor
            self.parent_descriptor = -1
            self.closed = True
            _close_descriptor_after_cleanup(retired_descriptor)


def _execute_anchored_vacuum_into(
    connection: sqlite3.Connection,
    target: Path,
    expected_parent_identity: Tuple[int, int],
) -> _AnchoredVacuumOutput:
    """Create a VACUUM output relative to an attested directory descriptor.

    ``VACUUM INTO`` only accepts a pathname.  During the stopped-service,
    single-threaded maintenance window, temporarily changing cwd to the open
    parent descriptor makes the relative filename resolve against that exact
    directory inode on both macOS and Linux.  Renaming/replacing the pathname
    cannot redirect SQLite into another system's directory.
    """

    names = _anchored_vacuum_names(target.name)
    current_thread = threading.current_thread()
    with _VACUUM_TARGET_CWD_LOCK:
        if any(
            thread is not current_thread and thread.is_alive()
            for thread in threading.enumerate()
        ):
            raise SignalRetentionMaintenanceError(
                "VACUUM target anchoring requires a single-threaded maintenance process"
            )
        directory_flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            directory_flags |= os.O_DIRECTORY
        if hasattr(os, "O_NOFOLLOW"):
            directory_flags |= os.O_NOFOLLOW
        original_cwd_descriptor = None  # type: Optional[int]
        parent_descriptor = None  # type: Optional[int]
        stage_descriptor = None  # type: Optional[int]
        stage_name = None  # type: Optional[str]
        output_identity = None  # type: Optional[Tuple[int, int]]
        final_linked = False
        cwd_changed = False
        result_handle = None  # type: Optional[_AnchoredVacuumOutput]
        try:
            original_cwd_descriptor = os.open(".", directory_flags)
            parent_descriptor = os.open(str(target.parent), directory_flags)
            if (
                _directory_descriptor_identity(
                    parent_descriptor,
                    "VACUUM destination parent",
                )
                != expected_parent_identity
            ):
                raise SignalRetentionMaintenanceError(
                    "VACUUM destination parent descriptor identity changed"
                )
            _require_anchored_vacuum_names_absent(parent_descriptor, names)

            # SQLite requires the target pathname not to exist.  A private
            # 0700 staging directory, created relative to the attested parent
            # descriptor, gives us a target that is both unguessable and safe
            # to remove even if SQLite completed the file but lost the Python
            # acknowledgement.  Publication uses linkat semantics and never
            # overwrites a name that appeared concurrently.
            for _attempt in range(32):
                candidate = ".vacuum-stage-" + secrets.token_hex(16)
                stage_name = candidate
                try:
                    os.mkdir(candidate, 0o700, dir_fd=parent_descriptor)
                except FileExistsError:
                    stage_name = None
                    continue
                break
            if stage_name is None:
                raise SignalRetentionMaintenanceError(
                    "VACUUM private staging directory could not be allocated"
                )
            stage_descriptor = os.open(
                stage_name,
                directory_flags,
                dir_fd=parent_descriptor,
            )
            stage_details = os.fstat(stage_descriptor)
            if (
                not stat.S_ISDIR(stage_details.st_mode)
                or stage_details.st_mode & 0o077
            ):
                raise SignalRetentionMaintenanceError(
                    "VACUUM private staging directory is not isolated"
                )
            _require_anchored_vacuum_names_absent(stage_descriptor, names)
            cwd_changed = True
            os.fchdir(stage_descriptor)
            connection.execute("VACUUM INTO ?", ("./" + target.name,))
            output_identity = _anchored_regular_file_identity(
                stage_descriptor,
                target.name,
                "staged VACUUM output",
            )
            if tuple(sorted(os.listdir(stage_descriptor))) != (target.name,):
                raise SignalRetentionMaintenanceError(
                    "VACUUM private staging directory contains unexpected files"
                )
            os.fchdir(original_cwd_descriptor)
            cwd_changed = False
            if (
                _directory_descriptor_identity(
                    parent_descriptor,
                    "VACUUM destination parent",
                )
                != expected_parent_identity
            ):
                raise SignalRetentionMaintenanceError(
                    "VACUUM destination parent descriptor identity changed"
                )
            _require_anchored_vacuum_names_absent(parent_descriptor, names)
            final_linked = True
            try:
                os.link(
                    target.name,
                    target.name,
                    src_dir_fd=stage_descriptor,
                    dst_dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
            except OSError as exc:
                raise SignalRetentionMaintenanceError(
                    "VACUUM output could not be published without overwrite"
                ) from exc
            os.unlink(target.name, dir_fd=stage_descriptor)
            if os.listdir(stage_descriptor):
                raise SignalRetentionMaintenanceError(
                    "VACUUM private staging directory is not empty"
                )
            closing_stage_descriptor = stage_descriptor
            stage_descriptor = None
            # Relinquish ownership in state before close().  If close really
            # succeeds and only its Python acknowledgement is lost, exception
            # cleanup must not use a newly reused fd number as a directory.
            os.close(closing_stage_descriptor)
            os.rmdir(stage_name, dir_fd=parent_descriptor)
            stage_name = None
            if (
                _anchored_regular_file_identity(
                    parent_descriptor,
                    target.name,
                    "published VACUUM output",
                )
                != output_identity
            ):
                raise SignalRetentionMaintenanceError(
                    "published VACUUM output identity changed"
                )
            if (
                _directory_identity(
                    target.parent,
                    "VACUUM destination parent directory",
                )
                != expected_parent_identity
                or _regular_file_identity(target, "VACUUM output")
                != output_identity
            ):
                raise SignalRetentionMaintenanceError(
                    "VACUUM destination path changed during anchored creation"
                )
            _validate_database_sidecars(
                target,
                "VACUUM output",
                require_absent=True,
            )
            result_handle = _AnchoredVacuumOutput(
                parent_descriptor=parent_descriptor,
                filename=target.name,
                identity=output_identity,
            )
            parent_descriptor = None
            return result_handle
        except BaseException as operation_error:
            cleanup_errors = []  # type: List[BaseException]
            if cwd_changed and original_cwd_descriptor is not None:
                restore_error = None  # type: Optional[BaseException]
                for _restore_attempt in range(2):
                    try:
                        os.fchdir(original_cwd_descriptor)
                        cwd_changed = False
                        restore_error = None
                        break
                    except BaseException as exc:
                        restore_error = exc
                if restore_error is not None:
                    cleanup_errors.append(restore_error)
            if stage_descriptor is not None:
                pending_names = set(names)
                # A single interrupted unlink must not prevent the remaining
                # private files or the published hard link from being cleaned.
                # Retry once after all siblings have been attempted.
                for _cleanup_pass in range(2):
                    for name in tuple(pending_names):
                        try:
                            os.unlink(name, dir_fd=stage_descriptor)
                        except FileNotFoundError:
                            pending_names.discard(name)
                        except BaseException as exc:
                            if _cleanup_pass == 1:
                                cleanup_errors.append(exc)
                        else:
                            pending_names.discard(name)
                try:
                    if os.listdir(stage_descriptor):
                        cleanup_errors.append(
                            SignalRetentionMaintenanceError(
                                "VACUUM private staging cleanup found unknown files"
                            )
                        )
                except BaseException as exc:
                    cleanup_errors.append(exc)
                try:
                    os.close(stage_descriptor)
                except BaseException as exc:
                    cleanup_errors.append(exc)
                finally:
                    stage_descriptor = None
            if stage_name is not None and parent_descriptor is not None:
                rmdir_error = None  # type: Optional[BaseException]
                for _rmdir_attempt in range(2):
                    try:
                        os.rmdir(stage_name, dir_fd=parent_descriptor)
                        stage_name = None
                        rmdir_error = None
                        break
                    except FileNotFoundError:
                        stage_name = None
                        rmdir_error = None
                        break
                    except BaseException as exc:
                        rmdir_error = exc
                if rmdir_error is not None:
                    cleanup_errors.append(rmdir_error)
            # Publish uses a temporary hard link.  Remove the stage link first
            # so the final file returns to nlink==1 and can pass the exact
            # identity guard used by safe cleanup.
            if (
                final_linked
                and parent_descriptor is not None
                and output_identity is not None
            ):
                final_remove_error = None  # type: Optional[BaseException]
                for _remove_attempt in range(2):
                    try:
                        _remove_anchored_vacuum_output(
                            parent_descriptor,
                            target.name,
                            output_identity,
                        )
                        final_linked = False
                        final_remove_error = None
                        break
                    except BaseException as exc:
                        final_remove_error = exc
                        try:
                            os.stat(
                                target.name,
                                dir_fd=parent_descriptor,
                                follow_symlinks=False,
                            )
                        except FileNotFoundError:
                            final_linked = False
                            final_remove_error = None
                            break
                        except BaseException:
                            pass
                if final_remove_error is not None:
                    cleanup_errors.append(final_remove_error)
            if cleanup_errors:
                raise SignalRetentionMaintenanceError(
                    "VACUUM failed and private output cleanup was incomplete: %s"
                    % cleanup_errors[0]
                ) from operation_error
            raise operation_error.with_traceback(operation_error.__traceback__)
        finally:
            finalization_errors = []  # type: List[BaseException]
            # If the first restore attempt failed, make one final attempt while
            # the original cwd descriptor is still live and report a hard
            # failure before this maintenance process can continue.
            if cwd_changed and original_cwd_descriptor is not None:
                try:
                    os.fchdir(original_cwd_descriptor)
                    cwd_changed = False
                except BaseException as exc:
                    finalization_errors.append(exc)
            for descriptor in (
                stage_descriptor,
                parent_descriptor,
                original_cwd_descriptor,
            ):
                if descriptor is None:
                    continue
                try:
                    os.close(descriptor)
                except BaseException as exc:
                    finalization_errors.append(exc)
            if finalization_errors:
                if result_handle is not None and not result_handle.closed:
                    try:
                        result_handle.discard()
                    except BaseException as exc:
                        finalization_errors.append(exc)
                raise SignalRetentionMaintenanceError(
                    "VACUUM descriptor cleanup was incomplete: %s"
                    % finalization_errors[0]
                )


def _restore_vacuum_output_schema_version(
    path: Path,
    *,
    expected_identity: Tuple[int, int],
    schema_version: int,
) -> None:
    """Restore the source's protected SQLite catalog cookie on a new copy."""

    if type(schema_version) is not int or schema_version <= 0:
        raise SignalRetentionMaintenanceError(
            "VACUUM source schema version is invalid"
        )
    flags = os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(str(path), flags)
    stream = None
    try:
        stream = os.fdopen(descriptor, "r+b", closefd=True)
        descriptor = -1
        details = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(details.st_mode)
            or details.st_nlink != 1
            or (int(details.st_dev), int(details.st_ino))
            != expected_identity
        ):
            raise SignalRetentionMaintenanceError(
                "VACUUM output descriptor identity changed"
            )
        stream.seek(0)
        header = stream.read(100)
        if (
            len(header) != 100
            or header[:16] != b"SQLite format 3\x00"
        ):
            raise SignalRetentionMaintenanceError(
                "VACUUM output SQLite header is invalid"
            )
        encoded = schema_version.to_bytes(4, "big", signed=False)
        stream.seek(40)
        if stream.write(encoded) != 4:
            raise SignalRetentionMaintenanceError(
                "VACUUM output schema version write was incomplete"
            )
        stream.flush()
        os.fsync(stream.fileno())
        stream.seek(40)
        if stream.read(4) != encoded:
            raise SignalRetentionMaintenanceError(
                "VACUUM output schema version was not restored"
            )
    finally:
        if stream is not None:
            stream.close()
        elif descriptor >= 0:
            os.close(descriptor)


def _verify_sqlite_main_identity(
    connection: sqlite3.Connection,
    path: Path,
    expected_identity: Tuple[int, int],
) -> None:
    rows = connection.execute("PRAGMA database_list").fetchall()
    main_rows = [row for row in rows if len(row) == 3 and row[1] == "main"]
    if len(main_rows) != 1 or type(main_rows[0][2]) is not str or not main_rows[0][2]:
        raise SignalRetentionMaintenanceError("SQLite main database identity is missing")
    opened_path = Path(main_rows[0][2])
    opened_identity = _regular_file_identity(opened_path, "opened SQLite database")
    current_identity = _regular_file_identity(path, "SQLite database")
    if opened_identity != expected_identity or current_identity != expected_identity:
        raise SignalRetentionMaintenanceError(
            "opened SQLite database is not the attested regular file"
        )


def _table_names(connection: sqlite3.Connection) -> List[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    if any(len(row) != 1 or type(row[0]) is not str for row in rows):
        raise SignalRetentionMaintenanceError("invalid SQLite table catalog")
    return [row[0] for row in rows]


_RETENTION_TABLE_COLUMNS = {
    "strategy_signal_batches": (
        "scan_id", "state", "recorded_count", "expected_count",
        "first_signal_id", "last_signal_id", "manifest_sha256",
        "completed_at", "created_at", "updated_at",
    ),
    "strategy_signal_current": (
        "singleton_id", "current_scan_id", "retention_active",
        "migration_state", "migration_cutoff_signal_id",
        "source_signal_count", "source_passed_count",
        "source_manifest_sha256", "updated_at", "retention_origin",
    ),
    "strategy_passed_signal_audits": (
        "id", "source_signal_id", "source_scan_id", "strategy_id", "symbol",
        "funding_rate", "matched_patterns", "trend_slope", "current_bullish",
        "passed", "decision", "reason", "structure_id", "detail_json",
        "signal_created_at", "evidence_sha256", "created_at", "claim_state",
    ),
    "strategy_passed_structure_ledger": (
        "id", "strategy_id", "symbol", "structure_id", "source_signal_id",
        "source_scan_id", "source_signal_created_at", "evidence_sha256",
        "created_at", "claim_state",
    ),
}

_RETENTION_TABLE_SQL = {
    "strategy_signal_batches": """
        CREATE TABLE strategy_signal_batches (
            scan_id INTEGER PRIMARY KEY,
            state TEXT NOT NULL CHECK(state IN ('STAGING', 'CURRENT')),
            recorded_count INTEGER NOT NULL DEFAULT 0
                CHECK(typeof(recorded_count) = 'integer' AND recorded_count >= 0),
            expected_count INTEGER CHECK(expected_count IS NULL OR (
                typeof(expected_count) = 'integer' AND expected_count > 0
            )),
            first_signal_id INTEGER,
            last_signal_id INTEGER,
            manifest_sha256 TEXT NOT NULL,
            completed_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(scan_id) REFERENCES scans(id)
        )
    """,
    "strategy_signal_current": """
        CREATE TABLE strategy_signal_current (
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
        )
    """,
    "strategy_passed_signal_audits": """
        CREATE TABLE strategy_passed_signal_audits (
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
        )
    """,
    "strategy_passed_structure_ledger": """
        CREATE TABLE strategy_passed_structure_ledger (
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
        )
    """,
}


def _normalize_schema_sql(value: Any) -> str:
    if type(value) is not str or not value:
        raise SignalRetentionMaintenanceError("SQLite schema SQL is invalid")
    output: List[str] = []
    index = 0
    quoted = False
    while index < len(value):
        character = value[index]
        if quoted:
            output.append(character)
            if character == "'":
                if index + 1 < len(value) and value[index + 1] == "'":
                    output.append("'")
                    index += 1
                else:
                    quoted = False
        elif character == "'":
            quoted = True
            output.append(character)
        elif not character.isspace():
            output.append(character.lower())
        index += 1
    if quoted:
        raise SignalRetentionMaintenanceError("SQLite schema SQL is unterminated")
    return "".join(output)


_RETENTION_TABLE_SQL_NORMALIZED = {
    table: _normalize_schema_sql(sql)
    for table, sql in _RETENTION_TABLE_SQL.items()
}
_RETENTION_ORIGIN_CLAUSE = _normalize_schema_sql(
    ", retention_origin TEXT CHECK(retention_origin IN ('LEGACY', 'GENESIS'))"
)
_RETENTION_CLAIM_CLAUSE = _normalize_schema_sql(
    ", claim_state TEXT NOT NULL DEFAULT 'ACTIVE' "
    "CHECK(claim_state IN ('STAGED', 'ACTIVE'))"
)

_RETENTION_TABLE_XINFO = {
    "strategy_signal_batches": (
        (0, "scan_id", "INTEGER", 0, None, 1, 0),
        (1, "state", "TEXT", 1, None, 0, 0),
        (2, "recorded_count", "INTEGER", 1, "0", 0, 0),
        (3, "expected_count", "INTEGER", 0, None, 0, 0),
        (4, "first_signal_id", "INTEGER", 0, None, 0, 0),
        (5, "last_signal_id", "INTEGER", 0, None, 0, 0),
        (6, "manifest_sha256", "TEXT", 1, None, 0, 0),
        (7, "completed_at", "TEXT", 0, None, 0, 0),
        (8, "created_at", "TEXT", 1, None, 0, 0),
        (9, "updated_at", "TEXT", 1, None, 0, 0),
    ),
    "strategy_signal_current": (
        (0, "singleton_id", "INTEGER", 0, None, 1, 0),
        (1, "current_scan_id", "INTEGER", 0, None, 0, 0),
        (2, "retention_active", "INTEGER", 1, "0", 0, 0),
        (3, "migration_state", "TEXT", 1, "'PENDING'", 0, 0),
        (4, "migration_cutoff_signal_id", "INTEGER", 0, None, 0, 0),
        (5, "source_signal_count", "INTEGER", 0, None, 0, 0),
        (6, "source_passed_count", "INTEGER", 0, None, 0, 0),
        (7, "source_manifest_sha256", "TEXT", 0, None, 0, 0),
        (8, "updated_at", "TEXT", 1, None, 0, 0),
        (9, "retention_origin", "TEXT", 0, None, 0, 0),
    ),
    "strategy_passed_signal_audits": (
        (0, "id", "INTEGER", 0, None, 1, 0),
        (1, "source_signal_id", "INTEGER", 1, None, 0, 0),
        (2, "source_scan_id", "INTEGER", 0, None, 0, 0),
        (3, "strategy_id", "TEXT", 1, None, 0, 0),
        (4, "symbol", "TEXT", 1, None, 0, 0),
        (5, "funding_rate", "TEXT", 1, None, 0, 0),
        (6, "matched_patterns", "TEXT", 1, None, 0, 0),
        (7, "trend_slope", "TEXT", 1, None, 0, 0),
        (8, "current_bullish", "INTEGER", 1, None, 0, 0),
        (9, "passed", "INTEGER", 1, None, 0, 0),
        (10, "decision", "TEXT", 1, None, 0, 0),
        (11, "reason", "TEXT", 1, None, 0, 0),
        (12, "structure_id", "TEXT", 0, None, 0, 0),
        (13, "detail_json", "TEXT", 1, None, 0, 0),
        (14, "signal_created_at", "TEXT", 1, None, 0, 0),
        (15, "evidence_sha256", "TEXT", 1, None, 0, 0),
        (16, "created_at", "TEXT", 1, None, 0, 0),
        (17, "claim_state", "TEXT", 1, "'ACTIVE'", 0, 0),
    ),
    "strategy_passed_structure_ledger": (
        (0, "id", "INTEGER", 0, None, 1, 0),
        (1, "strategy_id", "TEXT", 1, None, 0, 0),
        (2, "symbol", "TEXT", 1, None, 0, 0),
        (3, "structure_id", "TEXT", 1, None, 0, 0),
        (4, "source_signal_id", "INTEGER", 1, None, 0, 0),
        (5, "source_scan_id", "INTEGER", 0, None, 0, 0),
        (6, "source_signal_created_at", "TEXT", 1, None, 0, 0),
        (7, "evidence_sha256", "TEXT", 1, None, 0, 0),
        (8, "created_at", "TEXT", 1, None, 0, 0),
        (9, "claim_state", "TEXT", 1, "'ACTIVE'", 0, 0),
    ),
}

_RETENTION_TABLE_FOREIGN_KEYS = {
    "strategy_signal_batches": (
        (0, 0, "scans", "scan_id", "id", "NO ACTION", "NO ACTION", "NONE"),
    ),
    "strategy_signal_current": (
        (
            0, 0, "scans", "current_scan_id", "id",
            "NO ACTION", "NO ACTION", "NONE",
        ),
    ),
    "strategy_passed_signal_audits": (),
    "strategy_passed_structure_ledger": (),
}

_OWNED_INDEXES = {
    "idx_strategy_signal_one_staging": (
        "strategy_signal_batches", 1, 1, ("state",),
        "CREATE UNIQUE INDEX idx_strategy_signal_one_staging "
        "ON strategy_signal_batches(state) WHERE state = 'STAGING'",
    ),
    "idx_strategy_signal_one_current": (
        "strategy_signal_batches", 1, 1, ("state",),
        "CREATE UNIQUE INDEX idx_strategy_signal_one_current "
        "ON strategy_signal_batches(state) WHERE state = 'CURRENT'",
    ),
    "idx_passed_structure_symbol": (
        "strategy_passed_structure_ledger", 0, 0,
        ("strategy_id", "symbol", "structure_id"),
        "CREATE INDEX idx_passed_structure_symbol "
        "ON strategy_passed_structure_ledger(strategy_id, symbol, structure_id)",
    ),
    "idx_passed_audit_claim_batch": (
        "strategy_passed_signal_audits", 0, 0,
        ("source_scan_id", "claim_state"),
        "CREATE INDEX idx_passed_audit_claim_batch "
        "ON strategy_passed_signal_audits(source_scan_id, claim_state)",
    ),
    "idx_passed_ledger_claim_batch": (
        "strategy_passed_structure_ledger", 0, 0,
        ("source_scan_id", "claim_state"),
        "CREATE INDEX idx_passed_ledger_claim_batch "
        "ON strategy_passed_structure_ledger(source_scan_id, claim_state)",
    ),
    "idx_passed_audit_claim_state_scan": (
        "strategy_passed_signal_audits", 0, 0,
        ("claim_state", "source_scan_id"),
        "CREATE INDEX idx_passed_audit_claim_state_scan "
        "ON strategy_passed_signal_audits(claim_state, source_scan_id)",
    ),
    "idx_passed_ledger_claim_state_scan": (
        "strategy_passed_structure_ledger", 0, 0,
        ("claim_state", "source_scan_id"),
        "CREATE INDEX idx_passed_ledger_claim_state_scan "
        "ON strategy_passed_structure_ledger(claim_state, source_scan_id)",
    ),
    "idx_strategy_signals_scan_id": (
        "strategy_signals", 0, 0, ("scan_id",),
        "CREATE INDEX idx_strategy_signals_scan_id ON strategy_signals(scan_id)",
    ),
}

_RETENTION_AUTO_INDEX_KEYS = {
    "strategy_signal_batches": (),
    "strategy_signal_current": (),
    "strategy_passed_signal_audits": (("source_signal_id",),),
    "strategy_passed_structure_ledger": (
        ("source_signal_id",),
        ("strategy_id", "structure_id"),
    ),
}


def _table_column_names(
    connection: sqlite3.Connection,
    table: str,
) -> Tuple[str, ...]:
    rows = connection.execute(
        "PRAGMA table_info(%s)" % _quote_identifier(table)
    ).fetchall()
    if not rows or any(len(row) < 6 or type(row[1]) is not str for row in rows):
        raise SignalRetentionMaintenanceError(
            "invalid retention table schema: %s" % table
        )
    return tuple(row[1] for row in rows)


def _typed_equal(left: Any, right: Any) -> bool:
    if type(left) is not type(right):
        return False
    if type(left) in (tuple, list):
        return len(left) == len(right) and all(
            _typed_equal(a, b) for a, b in zip(left, right)
        )
    return bool(left == right)


def _retention_table_sql(
    connection: sqlite3.Connection,
    table: str,
) -> str:
    row = connection.execute(
        "SELECT sql FROM main.sqlite_schema WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    if (
        row is None
        or len(row) != 1
        or type(row[0]) is not str
        or not row[0]
    ):
        raise SignalRetentionMaintenanceError(
            "invalid retention table catalog: %s" % table
        )
    return _normalize_schema_sql(row[0])


def _verify_retention_table_variant(
    connection: sqlite3.Connection,
    table: str,
    *,
    include_claim: bool = True,
    include_origin: bool = True,
) -> None:
    expected_xinfo = _RETENTION_TABLE_XINFO[table]
    expected_sql = _RETENTION_TABLE_SQL_NORMALIZED[table]
    if table == "strategy_signal_current" and not include_origin:
        expected_xinfo = expected_xinfo[:-1]
        expected_sql = expected_sql.replace(_RETENTION_ORIGIN_CLAUSE, "")
    if table in {
        "strategy_passed_signal_audits",
        "strategy_passed_structure_ledger",
    } and not include_claim:
        expected_xinfo = expected_xinfo[:-1]
        expected_sql = expected_sql.replace(_RETENTION_CLAIM_CLAUSE, "")
    actual_xinfo = tuple(
        tuple(row)
        for row in connection.execute(
            "PRAGMA table_xinfo(%s)" % _quote_identifier(table)
        ).fetchall()
    )
    actual_foreign_keys = tuple(
        tuple(row)
        for row in connection.execute(
            "PRAGMA foreign_key_list(%s)" % _quote_identifier(table)
        ).fetchall()
    )
    if not _typed_equal(actual_xinfo, expected_xinfo):
        raise SignalRetentionMaintenanceError(
            "unexpected retention table metadata: %s" % table
        )
    if not _typed_equal(
        actual_foreign_keys,
        _RETENTION_TABLE_FOREIGN_KEYS[table],
    ):
        raise SignalRetentionMaintenanceError(
            "unexpected retention table foreign key: %s" % table
        )
    if _retention_table_sql(connection, table) != expected_sql:
        raise SignalRetentionMaintenanceError(
            "unexpected retention table constraints: %s" % table
        )


def _index_key_signature(
    connection: sqlite3.Connection,
    index_name: str,
    table: str,
) -> Tuple[str, ...]:
    column_ids = {
        row[1]: row[0]
        for row in connection.execute(
            "PRAGMA table_xinfo(%s)" % _quote_identifier(table)
        ).fetchall()
    }
    rows = connection.execute(
        "PRAGMA index_xinfo(%s)" % _quote_identifier(index_name)
    ).fetchall()
    if not rows:
        raise SignalRetentionMaintenanceError(
            "owned retention index is missing: %s" % index_name
        )
    keys: List[str] = []
    for offset, row in enumerate(rows):
        if type(row) not in (tuple, list) or len(row) != 6:
            raise SignalRetentionMaintenanceError(
                "owned retention index metadata is invalid: %s" % index_name
            )
        expected_auxiliary = (offset, -1, None, 0, "BINARY", 0)
        if row[5] == 0:
            if offset != len(rows) - 1 or not _typed_equal(tuple(row), expected_auxiliary):
                raise SignalRetentionMaintenanceError(
                    "owned retention index has invalid auxiliary metadata: %s"
                    % index_name
                )
            continue
        if (
            row[5] != 1
            or type(row[0]) is not int
            or row[0] != offset
            or type(row[1]) is not int
            or row[1] < 0
            or type(row[2]) is not str
            or column_ids.get(row[2]) != row[1]
            or type(row[3]) is not int
            or row[3] != 0
            or row[4] != "BINARY"
        ):
            raise SignalRetentionMaintenanceError(
                "owned retention index key is invalid: %s" % index_name
            )
        keys.append(row[2])
    return tuple(keys)


def _verify_existing_retention_indexes(
    connection: sqlite3.Connection,
    *,
    require_all: bool,
) -> None:
    table_rows: Dict[str, Dict[str, Tuple[Any, ...]]] = {}
    for table in _REQUIRED_RETENTION_TABLES | {"strategy_signals"}:
        rows = connection.execute(
            "PRAGMA index_list(%s)" % _quote_identifier(table)
        ).fetchall()
        mapped: Dict[str, Tuple[Any, ...]] = {}
        for row in rows:
            if (
                type(row) not in (tuple, list)
                or len(row) != 5
                or type(row[1]) is not str
                or type(row[2]) is not int
                or type(row[3]) is not str
                or type(row[4]) is not int
            ):
                raise SignalRetentionMaintenanceError(
                    "retention index catalog is invalid: %s" % table
                )
            mapped[row[1]] = tuple(row)
        table_rows[table] = mapped

    expected_explicit = {
        table: {
            name for name, spec in _OWNED_INDEXES.items() if spec[0] == table
        }
        for table in _REQUIRED_RETENTION_TABLES
    }
    for table in _REQUIRED_RETENTION_TABLES:
        actual_explicit = {
            name for name, row in table_rows[table].items() if row[3] == "c"
        }
        if not actual_explicit.issubset(expected_explicit[table]):
            raise SignalRetentionMaintenanceError(
                "unexpected explicit retention index: %s" % table
            )

    for name, spec in _OWNED_INDEXES.items():
        table, unique, partial, columns, expected_sql = spec
        row = table_rows[table].get(name)
        if row is None:
            if require_all:
                raise SignalRetentionMaintenanceError(
                    "owned retention index is missing: %s" % name
                )
            continue
        if row[2:] != (unique, "c", partial):
            raise SignalRetentionMaintenanceError(
                "owned retention index flags are invalid: %s" % name
            )
        if _index_key_signature(connection, name, table) != columns:
            raise SignalRetentionMaintenanceError(
                "owned retention index columns are invalid: %s" % name
            )
        catalog = connection.execute(
            "SELECT tbl_name, sql FROM main.sqlite_schema "
            "WHERE type = 'index' AND name = ?",
            (name,),
        ).fetchone()
        if (
            catalog is None
            or len(catalog) != 2
            or catalog[0] != table
            or _normalize_schema_sql(catalog[1])
            != _normalize_schema_sql(expected_sql)
        ):
            raise SignalRetentionMaintenanceError(
                "owned retention index SQL is invalid: %s" % name
            )

    for table, expected_keys in _RETENTION_AUTO_INDEX_KEYS.items():
        actual_keys: List[Tuple[str, ...]] = []
        for name, row in table_rows[table].items():
            if row[3] == "u":
                if row[2:] != (1, "u", 0):
                    raise SignalRetentionMaintenanceError(
                        "retention UNIQUE index flags are invalid: %s" % table
                    )
                actual_keys.append(_index_key_signature(connection, name, table))
            elif row[3] not in {"c"}:
                raise SignalRetentionMaintenanceError(
                    "unexpected retention autoindex origin: %s" % table
                )
        if sorted(actual_keys) != sorted(expected_keys):
            raise SignalRetentionMaintenanceError(
                "retention UNIQUE constraints are invalid: %s" % table
            )


def _validate_retention_catalog_names(connection: sqlite3.Connection) -> None:
    expected = {
        name.casefold(): ("table", name)
        for name in _REQUIRED_RETENTION_TABLES | {"strategy_signals"}
    }
    expected.update(
        {
            name.casefold(): ("index", name)
            for name in _OWNED_INDEXES
        }
    )
    rows = connection.execute(
        "SELECT type, name FROM main.sqlite_schema ORDER BY name COLLATE BINARY"
    ).fetchall()
    for row in rows:
        if (
            type(row) not in (tuple, list)
            or len(row) != 2
            or type(row[0]) is not str
            or type(row[1]) is not str
        ):
            raise SignalRetentionMaintenanceError(
                "invalid SQLite schema catalog identity"
            )
        canonical = expected.get(row[1].casefold())
        if canonical is not None and tuple(row) != canonical:
            raise SignalRetentionMaintenanceError(
                "retention catalog name or object type is non-canonical: %s"
                % row[1]
            )


def _retention_schema_version(connection: sqlite3.Connection) -> str:
    _validate_retention_catalog_names(connection)
    names = frozenset(_table_names(connection))
    present = _REQUIRED_RETENTION_TABLES & names
    if not present:
        return "ABSENT"
    if present != _REQUIRED_RETENTION_TABLES:
        raise SignalRetentionMaintenanceError(
            "retention schema is partial or mixed"
        )
    batch_columns = _table_column_names(connection, "strategy_signal_batches")
    current_columns = _table_column_names(connection, "strategy_signal_current")
    audit_columns = _table_column_names(
        connection, "strategy_passed_signal_audits"
    )
    ledger_columns = _table_column_names(
        connection, "strategy_passed_structure_ledger"
    )
    if batch_columns != _RETENTION_TABLE_COLUMNS["strategy_signal_batches"]:
        raise SignalRetentionMaintenanceError("unexpected retention batch schema")
    has_origin = current_columns == _RETENTION_TABLE_COLUMNS[
        "strategy_signal_current"
    ]
    originless = current_columns == _RETENTION_TABLE_COLUMNS[
        "strategy_signal_current"
    ][:-1]
    audit_claim = audit_columns == _RETENTION_TABLE_COLUMNS[
        "strategy_passed_signal_audits"
    ]
    ledger_claim = ledger_columns == _RETENTION_TABLE_COLUMNS[
        "strategy_passed_structure_ledger"
    ]
    audit_preclaim = audit_columns == _RETENTION_TABLE_COLUMNS[
        "strategy_passed_signal_audits"
    ][:-1]
    ledger_preclaim = ledger_columns == _RETENTION_TABLE_COLUMNS[
        "strategy_passed_structure_ledger"
    ][:-1]
    if originless and audit_preclaim and ledger_preclaim:
        version = "LEGACY_PRECLAIM"
        include_claim = False
    elif originless and audit_claim and ledger_claim:
        version = "LEGACY_ORIGINLESS"
        include_claim = True
    elif has_origin and audit_claim and ledger_claim:
        version = "CURRENT"
        include_claim = True
    else:
        raise SignalRetentionMaintenanceError(
            "retention schema version is unknown or mixed"
        )
    _verify_retention_table_variant(
        connection, "strategy_signal_batches"
    )
    _verify_retention_table_variant(
        connection,
        "strategy_signal_current",
        include_origin=has_origin,
    )
    _verify_retention_table_variant(
        connection,
        "strategy_passed_signal_audits",
        include_claim=include_claim,
    )
    _verify_retention_table_variant(
        connection,
        "strategy_passed_structure_ledger",
        include_claim=include_claim,
    )
    _verify_existing_retention_indexes(connection, require_all=False)
    return version


def _verify_retention_schema(
    connection: sqlite3.Connection,
    *,
    allow_missing_claim_state_scan_indexes: bool = False,
) -> None:
    if _retention_schema_version(connection) != "CURRENT":
        raise SignalRetentionMaintenanceError(
            "retention schema installation is incomplete"
        )
    if not allow_missing_claim_state_scan_indexes:
        _verify_existing_retention_indexes(connection, require_all=True)
        return
    _verify_existing_retention_indexes(connection, require_all=False)
    allowed_missing = {
        "idx_passed_audit_claim_state_scan",
        "idx_passed_ledger_claim_state_scan",
    }
    present = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='index'"
        ).fetchall()
        if type(row[0]) is str
    }
    missing = set(_OWNED_INDEXES) - present
    if not missing.issubset(allowed_missing):
        raise SignalRetentionMaintenanceError(
            "retention schema is missing a non-upgrade index"
        )


def _install_retention_schema(connection: sqlite3.Connection) -> None:
    """Install only the bounded-signal retention schema in one transaction."""

    existing = frozenset(_table_names(connection))
    if not {"scans", "strategy_signals"}.issubset(existing):
        raise SignalRetentionMaintenanceError("required legacy tables are missing")
    # This precheck is intentionally before BEGIN and before the singleton
    # INSERT.  A trigger on a legacy retention table must not get one chance to
    # execute merely because the schema is being upgraded.
    _validate_retention_dependencies(connection)
    schema_version = _retention_schema_version(connection)
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_signal_batches (
                scan_id INTEGER PRIMARY KEY,
                state TEXT NOT NULL CHECK(state IN ('STAGING', 'CURRENT')),
                recorded_count INTEGER NOT NULL DEFAULT 0
                    CHECK(typeof(recorded_count) = 'integer' AND recorded_count >= 0),
                expected_count INTEGER CHECK(expected_count IS NULL OR (
                    typeof(expected_count) = 'integer' AND expected_count > 0
                )),
                first_signal_id INTEGER,
                last_signal_id INTEGER,
                manifest_sha256 TEXT NOT NULL,
                completed_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(scan_id) REFERENCES scans(id)
            )
            """
        )
        connection.execute(
            """
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
            )
            """
        )
        connection.execute(
            """
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
            )
            """
        )
        connection.execute(
            """
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
            )
            """
        )

        for table in (
            "strategy_passed_signal_audits",
            "strategy_passed_structure_ledger",
        ):
            columns = _table_column_names(connection, table)
            expected = _RETENTION_TABLE_COLUMNS[table]
            if columns == expected[:-1]:
                connection.execute(
                    "ALTER TABLE %s ADD COLUMN claim_state TEXT NOT NULL "
                    "DEFAULT 'ACTIVE' CHECK(claim_state IN ('STAGED', 'ACTIVE'))"
                    % _quote_identifier(table)
                )
            elif columns != expected:
                raise SignalRetentionMaintenanceError(
                    "unexpected legacy retention schema: %s" % table
                )
        current_columns = _table_column_names(
            connection, "strategy_signal_current"
        )
        expected_current = _RETENTION_TABLE_COLUMNS[
            "strategy_signal_current"
        ]
        if current_columns == expected_current[:-1]:
            connection.execute(
                "ALTER TABLE strategy_signal_current "
                "ADD COLUMN retention_origin TEXT "
                "CHECK(retention_origin IN ('LEGACY', 'GENESIS'))"
            )
        elif current_columns != expected_current:
            raise SignalRetentionMaintenanceError(
                "unexpected legacy retention schema: strategy_signal_current"
            )

        if _retention_schema_version(connection) != "CURRENT":
            raise SignalRetentionMaintenanceError(
                "retention schema upgrade did not reach the current version"
            )
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_signal_one_staging "
            "ON strategy_signal_batches(state) WHERE state = 'STAGING'"
        )
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_signal_one_current "
            "ON strategy_signal_batches(state) WHERE state = 'CURRENT'"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_passed_structure_symbol "
            "ON strategy_passed_structure_ledger(strategy_id, symbol, structure_id)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_passed_audit_claim_batch "
            "ON strategy_passed_signal_audits(source_scan_id, claim_state)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_passed_ledger_claim_batch "
            "ON strategy_passed_structure_ledger(source_scan_id, claim_state)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_passed_audit_claim_state_scan "
            "ON strategy_passed_signal_audits(claim_state, source_scan_id)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_passed_ledger_claim_state_scan "
            "ON strategy_passed_structure_ledger(claim_state, source_scan_id)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_strategy_signals_scan_id "
            "ON strategy_signals(scan_id)"
        )
        _verify_retention_schema(connection)
        _validate_retention_dependencies(connection)
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO strategy_signal_current (
                singleton_id, current_scan_id, retention_active,
                migration_state, updated_at
            ) VALUES (1, NULL, 0, 'PENDING', ?)
            """,
            (utc_now(),),
        )
        if cursor.rowcount not in (0, 1):
            raise SignalRetentionMaintenanceError(
                "retention singleton installation conflict"
            )
        raw_state = connection.execute(
            """
            SELECT current_scan_id, retention_active, migration_state,
                   migration_cutoff_signal_id, source_signal_count,
                   source_passed_count, source_manifest_sha256,
                   retention_origin
            FROM strategy_signal_current WHERE singleton_id = 1
            """
        ).fetchone()
        if raw_state is None or len(raw_state) != 8:
            raise SignalRetentionMaintenanceError(
                "strategy signal retention row is missing"
            )
        if raw_state[7] is None and raw_state[2] in {
            "BACKFILLED",
            "COMPLETE",
        }:
            marker = raw_state[3:7]
            legacy_marker = (
                type(marker[0]) is int
                and marker[0] > 0
                and type(marker[1]) is int
                and marker[1] > 0
                and type(marker[2]) is int
                and 0 <= marker[2] <= marker[1]
                and type(marker[3]) is str
                and len(marker[3]) == 64
                and not any(
                    character not in "0123456789abcdef"
                    for character in marker[3]
                )
            )
            if legacy_marker:
                normalized = connection.execute(
                    """
                    UPDATE strategy_signal_current
                    SET retention_origin = 'LEGACY', updated_at = ?
                    WHERE singleton_id = 1 AND retention_origin IS NULL
                    """,
                    (utc_now(),),
                )
                if normalized.rowcount != 1:
                    raise SignalRetentionMaintenanceError(
                        "legacy retention origin normalization conflict"
                    )
            elif (
                raw_state[:7]
                in {
                    (None, 1, "COMPLETE", None, None, None, None),
                    (None, 1, "COMPLETE", 0, 0, 0, _MANIFEST_SEED),
                }
                and _strict_genesis_history_is_empty(connection)
            ):
                normalized = connection.execute(
                    """
                    UPDATE strategy_signal_current
                    SET migration_cutoff_signal_id = 0,
                        source_signal_count = 0, source_passed_count = 0,
                        source_manifest_sha256 = ?,
                        retention_origin = 'GENESIS', updated_at = ?
                    WHERE singleton_id = 1 AND retention_origin IS NULL
                    """,
                    (_MANIFEST_SEED, utc_now()),
                )
                if normalized.rowcount != 1:
                    raise SignalRetentionMaintenanceError(
                        "genesis retention origin normalization conflict"
                    )
        state = _retention_state(connection)
        if state[7] == "LEGACY" and state[2] == "COMPLETE":
            _complete_state_summary(connection, state[0])
        elif state[7] == "LEGACY" and state[2] == "BACKFILLED":
            _validate_backfilled_state(connection, state[0])
        _validate_retention_dependencies(connection)
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def _quote_identifier(value: str) -> str:
    return '"%s"' % value.replace('"', '""')


def _validate_retention_dependencies(connection: sqlite3.Connection) -> None:
    """Reject schema hooks that could make bounded deletion non-local.

    The maintenance operation owns exactly five mutable retention tables.  An
    incoming foreign key could cascade deletion into an unrelated table, while
    a trigger attached to any owned table could perform arbitrary side effects.
    Neither is required by this tool, so both are rejected before backfill or
    deletion begins.
    """

    table_names = _table_names(connection)
    owned = {name.casefold() for name in _RETENTION_MUTATION_TABLES}
    for catalog in ("sqlite_master", "sqlite_temp_master"):
        triggers = connection.execute(
            "SELECT name, tbl_name, sql FROM %s "
            "WHERE type = 'trigger' ORDER BY name" % catalog
        ).fetchall()
        for row in triggers:
            if (
                len(row) != 3
                or type(row[0]) is not str
                or type(row[1]) is not str
                or (row[2] is not None and type(row[2]) is not str)
            ):
                raise SignalRetentionMaintenanceError(
                    "invalid SQLite trigger catalog entry"
                )
            if row[1].casefold() in owned:
                expected_table = _N16_RETENTION_TRIGGER_TABLES.get(row[0])
                if (
                    catalog == "sqlite_master"
                    and expected_table == row[1]
                    and row[0] in _N16_TRIGGER_SQL
                    and type(row[2]) is str
                    and _n16_normalized_sql(row[2])
                    == _n16_normalized_sql(_N16_TRIGGER_SQL[row[0]])
                ):
                    continue
                raise SignalRetentionMaintenanceError(
                    "trigger attached to retention table is not allowed: %s" % row[0]
                )

    for child_table in table_names:
        foreign_keys = connection.execute(
            "PRAGMA foreign_key_list(%s)" % _quote_identifier(child_table)
        ).fetchall()
        for row in foreign_keys:
            if (
                len(row) < 8
                or type(row[0]) is not int
                or type(row[1]) is not int
                or type(row[2]) is not str
            ):
                raise SignalRetentionMaintenanceError(
                    "invalid foreign-key catalog entry for %s" % child_table
                )
            if row[2].casefold() in owned:
                raise SignalRetentionMaintenanceError(
                    "incoming foreign key to retention table is not allowed: "
                    "%s -> %s" % (child_table, row[2])
                )


def _table_full_hash(connection: sqlite3.Connection, table: str) -> str:
    if table not in _table_names(connection):
        raise SignalRetentionMaintenanceError("missing table: %s" % table)
    digest = hashlib.sha256()
    schema = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?", (table,)
    ).fetchone()
    _update_typed_hash(digest, schema or (None,))
    table_info = connection.execute(
        "PRAGMA table_info(%s)" % _quote_identifier(table)
    ).fetchall()
    if not table_info or any(len(row) < 6 or type(row[1]) is not str for row in table_info):
        raise SignalRetentionMaintenanceError("invalid table schema: %s" % table)
    primary_key = [
        row[1]
        for row in sorted(table_info, key=lambda item: item[5] or 0)
        if type(row[5]) is int and row[5] > 0
    ]
    order_columns = primary_key or [row[1] for row in table_info]
    order_clause = ", ".join(_quote_identifier(column) for column in order_columns)
    cursor = connection.execute(
        "SELECT * FROM %s ORDER BY %s"
        % (_quote_identifier(table), order_clause)
    )
    _update_typed_hash(digest, tuple(item[0] for item in cursor.description))
    for row in cursor:
        _update_typed_hash(digest, row)
    return digest.hexdigest()


def _protected_hashes(connection: sqlite3.Connection) -> Dict[str, str]:
    protected = {}  # type: Dict[str, str]
    for table in _table_names(connection):
        if table not in _MUTABLE_TABLES:
            protected[table] = _table_full_hash(connection, table)
    return protected


def _all_table_hashes(connection: sqlite3.Connection) -> Dict[str, str]:
    return {
        table: _table_full_hash(connection, table)
        for table in _table_names(connection)
    }


def _sqlite_schema_identity(
    connection: sqlite3.Connection,
) -> Tuple[str, int, int]:
    digest = hashlib.sha256()
    _update_typed_hash(digest, ("binance-sqlite-schema-v1",))
    rows = connection.execute(
        """
        SELECT type, name, tbl_name, sql
        FROM main.sqlite_schema
        ORDER BY type COLLATE BINARY, name COLLATE BINARY,
                 tbl_name COLLATE BINARY, sql COLLATE BINARY
        """
    ).fetchall()
    for row in rows:
        if (
            type(row) not in (tuple, list)
            or len(row) != 4
            or type(row[0]) is not str
            or row[0] not in {"table", "index", "view", "trigger"}
            or type(row[1]) is not str
            or type(row[2]) is not str
            or (row[3] is not None and type(row[3]) is not str)
        ):
            raise SignalRetentionMaintenanceError(
                "invalid SQLite schema catalog entry"
            )
        _update_typed_hash(digest, tuple(row))

    pragmas: List[int] = []
    for name in ("user_version", "application_id"):
        result = connection.execute("PRAGMA main.%s" % name).fetchall()
        if (
            len(result) != 1
            or type(result[0]) not in (tuple, list)
            or len(result[0]) != 1
            or type(result[0][0]) is not int
        ):
            raise SignalRetentionMaintenanceError(
                "invalid SQLite %s" % name
            )
        pragmas.append(result[0][0])
    return digest.hexdigest(), pragmas[0], pragmas[1]


def _hash_mapping(value: Dict[str, str]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _database_checks(connection: sqlite3.Connection) -> Tuple[int, str]:
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    integrity = connection.execute("PRAGMA integrity_check").fetchall()
    if any(type(row[0]) is not str for row in integrity if row):
        raise SignalRetentionMaintenanceError("invalid integrity_check result")
    integrity_text = ";".join(row[0] for row in integrity)
    if violations:
        raise SignalRetentionMaintenanceError(
            "foreign_key_check failed with %d row(s)" % len(violations)
        )
    if integrity != [("ok",)]:
        raise SignalRetentionMaintenanceError(
            "integrity_check failed: %s" % integrity_text
        )
    return 0, integrity_text


def _signal_sequence(connection: sqlite3.Connection) -> Optional[int]:
    if "sqlite_sequence" not in _table_names(connection):
        return None
    rows = connection.execute(
        "SELECT seq FROM sqlite_sequence WHERE name = 'strategy_signals'"
    ).fetchall()
    if not rows:
        return None
    if len(rows) != 1 or type(rows[0][0]) is not int or rows[0][0] < 0:
        raise SignalRetentionMaintenanceError("invalid strategy_signals sequence")
    return rows[0][0]


def _latest_completed_scan_id(connection: sqlite3.Connection) -> Optional[int]:
    row = connection.execute(
        """
        SELECT scans.id
        FROM scans
        WHERE scans.completed_at IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM strategy_signals
              WHERE strategy_signals.scan_id = scans.id
          )
        ORDER BY scans.id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    if len(row) != 1 or type(row[0]) is not int or row[0] <= 0:
        raise SignalRetentionMaintenanceError("latest completed scan id is invalid")
    return row[0]


def _iter_signal_rows(connection: sqlite3.Connection) -> Iterable[Tuple[Any, ...]]:
    sql = "SELECT %s FROM strategy_signals ORDER BY id" % ", ".join(
        _quote_identifier(column) for column in _SIGNAL_COLUMNS
    )
    for row in connection.execute(sql):
        yield _validate_signal_row(row)


def _signal_row_by_id(
    connection: sqlite3.Connection,
    signal_id: int,
) -> Tuple[Any, ...]:
    if type(signal_id) is not int or signal_id <= 0:
        raise SignalRetentionMaintenanceError("ledger source signal id is invalid")
    row = connection.execute(
        "SELECT %s FROM strategy_signals WHERE id = ?"
        % ", ".join(_quote_identifier(column) for column in _SIGNAL_COLUMNS),
        (signal_id,),
    ).fetchone()
    if row is None:
        raise SignalRetentionMaintenanceError("ledger source signal is missing")
    return _validate_signal_row(row)


def _staging_scan_hint(connection: sqlite3.Connection) -> Optional[int]:
    rows = connection.execute(
        "SELECT scan_id FROM strategy_signal_batches "
        "WHERE state='STAGING' ORDER BY scan_id"
    ).fetchall()
    if not rows:
        return None
    if (
        len(rows) != 1
        or len(rows[0]) != 1
        or type(rows[0][0]) is not int
        or rows[0][0] <= 0
    ):
        raise SignalRetentionMaintenanceError(
            "stopped-service maintenance requires one canonical STAGING owner"
        )
    return rows[0][0]


def _staging_cleanup_plan(
    connection: sqlite3.Connection,
) -> Optional[_StagingCleanupPlan]:
    """Authenticate the sole unpublished batch without mutating it."""

    retention = _strategy_signal_retention_row(connection)
    if retention[1:3] != (1, "COMPLETE"):
        return None
    try:
        _validate_complete_strategy_signal_graph(connection, retention)
    except (RuntimeError, TypeError, ValueError) as exc:
        raise SignalRetentionMaintenanceError(
            "stopped-service STAGING graph is invalid"
        ) from exc
    rows = connection.execute(
        "SELECT scan_id,recorded_count,first_signal_id,last_signal_id,"
        "manifest_sha256 FROM strategy_signal_batches "
        "WHERE state='STAGING' ORDER BY scan_id"
    ).fetchall()
    if not rows:
        return None
    if len(rows) != 1:
        raise SignalRetentionMaintenanceError(
            "stopped-service maintenance requires at most one STAGING batch"
        )
    scan_id, recorded_count, first_id, last_id, manifest = rows[0]
    if (
        type(scan_id) is not int
        or scan_id <= 0
        or type(recorded_count) is not int
        or recorded_count < 0
        or type(manifest) is not str
        or len(manifest) != 64
        or _strategy_signal_batch_snapshot(connection, scan_id)
        != (recorded_count, first_id, last_id, manifest)
    ):
        raise SignalRetentionMaintenanceError(
            "stopped-service STAGING batch identity is invalid"
        )
    try:
        passed_count, ledger_count = _strategy_signal_claim_snapshot(
            connection, scan_id, "STAGED"
        )
        micro_count = _micro_staged_lifecycle_snapshot(connection, scan_id)
    except (RuntimeError, TypeError, ValueError) as exc:
        raise SignalRetentionMaintenanceError(
            "stopped-service STAGING claim graph is invalid"
        ) from exc
    return _StagingCleanupPlan(
        scan_id=scan_id,
        recorded_count=recorded_count,
        first_signal_id=first_id,
        last_signal_id=last_id,
        manifest_sha256=manifest,
        passed_claim_count=passed_count,
        ledger_claim_count=ledger_count,
        micro_claim_count=micro_count,
    )


def _cleanup_staging_batch_transaction(
    connection: sqlite3.Connection,
    expected: _StagingCleanupPlan,
    *,
    postdelete_validator: Optional[Callable[[sqlite3.Connection], None]] = None,
) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        current = _staging_cleanup_plan(connection)
        if current != expected:
            raise SignalRetentionMaintenanceError(
                "STAGING cleanup identity changed before write"
            )
        _delete_staged_strategy_signal_batch(
            connection,
            current.scan_id,
            current.recorded_count,
            current.first_signal_id,
            current.last_signal_id,
        )
        if _staging_cleanup_plan(connection) is not None:
            raise SignalRetentionMaintenanceError(
                "STAGING cleanup did not remove the unpublished batch"
            )
        if postdelete_validator is not None:
            postdelete_validator(connection)
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def _source_summary(
    connection: sqlite3.Connection,
    keep_scan_id: int,
    *,
    legacy_cutoff_signal_id: Optional[int] = None,
    excluded_scan_ids: frozenset[int] = frozenset(),
    allow_empty_keep: bool = False,
) -> _SourceSummary:
    if type(keep_scan_id) is not int or keep_scan_id <= 0:
        raise SignalRetentionMaintenanceError("keep_scan_id must be a positive integer")
    if legacy_cutoff_signal_id is not None and (
        type(legacy_cutoff_signal_id) is not int
        or legacy_cutoff_signal_id < 0
    ):
        raise SignalRetentionMaintenanceError("legacy retention cutoff is invalid")
    digest = hashlib.sha256()
    retained_manifest = _MANIFEST_SEED
    signal_count = 0
    passed_count = 0
    ledger_count = 0
    retained_count = 0
    cutoff = None  # type: Optional[int]
    retained_first = None  # type: Optional[int]
    retained_last = None  # type: Optional[int]
    ledgers = {}  # type: Dict[Tuple[str, str], Tuple[int, str]]
    for row in _iter_signal_rows(connection):
        if row[1] in excluded_scan_ids:
            continue
        # N16+ did not exist in the first legacy source.  Once that source has
        # been sealed, later CURRENT publications legitimately contain these
        # strategies.  The immutable cutoff keeps the two evidence domains
        # separate: rows at or below it remain legacy forever, while only rows
        # strictly above it can be treated as post-cutover publications.
        if (
            row[2] in _POST_LEGACY_RETENTION_STRATEGIES
            and (
                legacy_cutoff_signal_id is None
                or row[0] <= legacy_cutoff_signal_id
            )
        ):
            raise SignalRetentionMaintenanceError(
                "legacy retention source contains impossible post-legacy strategy evidence"
            )
        signal_count += 1
        cutoff = row[0]
        evidence_sha = _signal_evidence_sha256(row)
        _update_typed_hash(digest, row)
        if row[1] == keep_scan_id:
            retained_count += 1
            retained_first = row[0] if retained_first is None else retained_first
            retained_last = row[0]
            retained_manifest = hashlib.sha256(
                (retained_manifest + ":" + evidence_sha).encode("ascii")
            ).hexdigest()
        if row[8] == 1:
            passed_count += 1
            if row[2] in _LEDGER_STRATEGIES:
                if row[11] is None:
                    raise SignalRetentionMaintenanceError(
                        "passed ledger strategy is missing structure_id: "
                        "%s signal_id=%s" % (row[2], row[0])
                    )
                key = (row[2], row[11])
                existing = ledgers.get(key)
                if existing is not None and existing[1] != row[3]:
                    raise SignalRetentionMaintenanceError(
                        "passed structure has multiple symbols: %s/%s" % key
                    )
                if existing is None:
                    ledgers[key] = (row[0], row[3])
                    ledger_count += 1
    if signal_count == 0 and retained_count == 0 and allow_empty_keep:
        return _SourceSummary(
            signal_count=0,
            passed_count=0,
            ledger_count=0,
            retained_count=0,
            deletable_count=0,
            cutoff_signal_id=None,
            manifest_sha256=digest.hexdigest(),
            retained_manifest_sha256=_MANIFEST_SEED,
            retained_first_id=None,
            retained_last_id=None,
            ledger_sources={},
        )
    if signal_count <= 0 or retained_count <= 0:
        raise SignalRetentionMaintenanceError(
            "latest completed keep scan has no strategy signals"
        )
    return _SourceSummary(
        signal_count=signal_count,
        passed_count=passed_count,
        ledger_count=ledger_count,
        retained_count=retained_count,
        deletable_count=signal_count - retained_count,
        cutoff_signal_id=cutoff,
        manifest_sha256=digest.hexdigest(),
        retained_manifest_sha256=retained_manifest,
        retained_first_id=retained_first,
        retained_last_id=retained_last,
        ledger_sources=ledgers,
    )


def _validate_keep_attestation(
    summary: _SourceSummary,
    keep_signal_count: int,
    keep_manifest_sha256: Optional[str],
) -> bool:
    if type(keep_signal_count) is not int or keep_signal_count < 0:
        raise SignalRetentionMaintenanceError(
            "keep_signal_count must be a non-negative built-in integer"
        )
    if summary.retained_count != keep_signal_count:
        raise SignalRetentionMaintenanceError(
            "keep scan signal count does not match explicit attestation"
        )
    if keep_manifest_sha256 is None:
        return False
    if (
        type(keep_manifest_sha256) is not str
        or len(keep_manifest_sha256) != 64
        or any(
            character not in "0123456789abcdef"
            for character in keep_manifest_sha256
        )
    ):
        raise SignalRetentionMaintenanceError(
            "keep_manifest_sha256 must be 64 lowercase hexadecimal characters"
        )
    if summary.retained_manifest_sha256 != keep_manifest_sha256:
        raise SignalRetentionMaintenanceError(
            "keep scan manifest does not match explicit attestation"
        )
    return True


def _validate_keep_runtime_snapshot(
    connection: sqlite3.Connection,
    keep_scan_id: int,
    summary: _SourceSummary,
    published: bool,
) -> None:
    """Apply the runtime JSON/envelope contract before any old-row delete."""

    try:
        snapshot = (
            _strategy_signal_published_snapshot(connection, keep_scan_id)
            if published
            else _strategy_signal_batch_snapshot(connection, keep_scan_id)
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        raise SignalRetentionMaintenanceError(
            "keep scan runtime publication envelope is invalid"
        ) from exc
    expected = (
        summary.retained_count,
        summary.retained_first_id,
        summary.retained_last_id,
        summary.retained_manifest_sha256,
    )
    if snapshot != expected:
        raise SignalRetentionMaintenanceError(
            "keep scan runtime publication identity mismatch"
        )


def _retention_state(connection: sqlite3.Connection) -> Tuple[Any, ...]:
    row = connection.execute(
        """
        SELECT current_scan_id, retention_active, migration_state,
               migration_cutoff_signal_id, source_signal_count,
               source_passed_count, source_manifest_sha256,
               retention_origin
        FROM strategy_signal_current WHERE singleton_id = 1
        """
    ).fetchone()
    try:
        return _strict_strategy_signal_retention_row(row)
    except (RuntimeError, TypeError, ValueError) as exc:
        raise SignalRetentionMaintenanceError(
            "strategy signal retention identity is invalid"
        ) from exc


def _originless_retention_state(
    connection: sqlite3.Connection,
) -> Tuple[Any, ...]:
    row = connection.execute(
        """
        SELECT current_scan_id, retention_active, migration_state,
               migration_cutoff_signal_id, source_signal_count,
               source_passed_count, source_manifest_sha256
        FROM strategy_signal_current WHERE singleton_id = 1
        """
    ).fetchone()
    if row == (None, 0, "PENDING", None, None, None, None):
        return tuple(row) + (None,)
    if row in {
        (None, 1, "COMPLETE", None, None, None, None),
        (None, 1, "COMPLETE", 0, 0, 0, _MANIFEST_SEED),
    } and _strict_genesis_history_is_empty(connection):
        return (None, 1, "COMPLETE", 0, 0, 0, _MANIFEST_SEED, "GENESIS")
    candidate = tuple(row or ()) + ("LEGACY",)
    try:
        return _strict_strategy_signal_retention_row(candidate)
    except (RuntimeError, TypeError, ValueError) as exc:
        raise SignalRetentionMaintenanceError(
            "originless retention identity is invalid"
        ) from exc


def _strict_genesis_history_is_empty(
    connection: sqlite3.Connection,
) -> bool:
    for table in (
        "strategy_signals",
        "strategy_signal_batches",
        "strategy_passed_signal_audits",
        "strategy_passed_structure_ledger",
    ):
        if connection.execute(
            "SELECT 1 FROM %s LIMIT 1" % _quote_identifier(table)
        ).fetchone():
            return False
    sequences = connection.execute(
        """
        SELECT name, seq FROM sqlite_sequence
        WHERE lower(name) IN (
            'strategy_signals',
            'strategy_passed_signal_audits',
            'strategy_passed_structure_ledger'
        )
        """
    ).fetchall()
    return sequences == []


def _strict_absent_retention_history_is_empty(
    connection: sqlite3.Connection,
) -> bool:
    """Prove that runtime may create the retention schema from nothing.

    Legacy signal rows or sequence history require the explicit maintenance
    backfill.  Owned index names also count as retention evidence: accepting
    one here would let ``CREATE INDEX IF NOT EXISTS`` inherit an unknown
    definition during ordinary startup.
    """

    tables = frozenset(_table_names(connection))
    if tables & _REQUIRED_RETENTION_TABLES:
        return False
    owned_indexes = connection.execute(
        "SELECT name FROM main.sqlite_schema WHERE type = 'index'"
    ).fetchall()
    if any(
        type(row) not in (tuple, list)
        or len(row) != 1
        or type(row[0]) is not str
        or row[0].casefold()
        in {name.casefold() for name in _OWNED_INDEXES}
        for row in owned_indexes
    ):
        return False
    if "strategy_signals" in tables and connection.execute(
        "SELECT 1 FROM strategy_signals LIMIT 1"
    ).fetchone():
        return False
    if "sqlite_sequence" not in tables:
        return True
    sequences = connection.execute(
        """
        SELECT name, seq FROM sqlite_sequence
        WHERE lower(name) IN (
            'strategy_signals',
            'strategy_passed_signal_audits',
            'strategy_passed_structure_ledger'
        )
        """
    ).fetchall()
    return sequences == []


def _audit_values(row: Sequence[Any], evidence_sha: str, created_at: str) -> Tuple[Any, ...]:
    return (
        row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], 1,
        row[9], row[10], row[11], row[12], row[13], evidence_sha, created_at,
        "ACTIVE",
    )


def _validate_existing_audit(row: Sequence[Any]) -> None:
    if len(row) != 18:
        raise SignalRetentionMaintenanceError("passed audit row shape is invalid")
    if type(row[0]) is not int or row[0] <= 0:
        raise SignalRetentionMaintenanceError("passed audit id is invalid")
    source = (
        row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
        row[10], row[11], row[12], row[13], row[14],
    )
    validated = _validate_signal_row(source)
    if validated[8] != 1:
        raise SignalRetentionMaintenanceError("passed audit does not contain passed=1")
    if type(row[15]) is not str or row[15] != _signal_evidence_sha256(validated):
        raise SignalRetentionMaintenanceError("passed audit evidence hash conflict")
    _strict_text(row[16], "passed audit created_at", 128)
    if type(row[17]) is not str or row[17] not in ("STAGED", "ACTIVE"):
        raise SignalRetentionMaintenanceError("passed audit claim state is invalid")


def _validate_all_audits(connection: sqlite3.Connection) -> None:
    rows = connection.execute(
        """
        SELECT id, source_signal_id, source_scan_id, strategy_id, symbol,
               funding_rate, matched_patterns, trend_slope, current_bullish,
               passed, decision, reason, structure_id, detail_json,
               signal_created_at, evidence_sha256, created_at, claim_state
        FROM strategy_passed_signal_audits ORDER BY id
        """
    )
    for row in rows:
        _validate_existing_audit(row)


def _upsert_audit(
    connection: sqlite3.Connection,
    source: Sequence[Any],
    now: str,
) -> None:
    evidence_sha = _signal_evidence_sha256(source)
    existing = connection.execute(
        """
        SELECT id, source_signal_id, source_scan_id, strategy_id, symbol,
               funding_rate, matched_patterns, trend_slope, current_bullish,
               passed, decision, reason, structure_id, detail_json,
               signal_created_at, evidence_sha256, created_at, claim_state
        FROM strategy_passed_signal_audits WHERE source_signal_id = ?
        """,
        (source[0],),
    ).fetchone()
    if existing is not None:
        _validate_existing_audit(existing)
        expected = _audit_values(source, evidence_sha, existing[16])
        if tuple(existing[1:]) != expected:
            raise SignalRetentionMaintenanceError("passed audit identity conflict")
        return
    connection.execute(
        """
        INSERT INTO strategy_passed_signal_audits (
            source_signal_id, source_scan_id, strategy_id, symbol,
            funding_rate, matched_patterns, trend_slope, current_bullish,
            passed, decision, reason, structure_id, detail_json,
            signal_created_at, evidence_sha256, created_at, claim_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        _audit_values(source, evidence_sha, now),
    )


def _validate_ledger_row(row: Sequence[Any]) -> None:
    if len(row) != 10:
        raise SignalRetentionMaintenanceError("passed structure ledger shape is invalid")
    if type(row[0]) is not int or row[0] <= 0:
        raise SignalRetentionMaintenanceError("passed structure ledger id is invalid")
    _strict_text(row[1], "ledger strategy_id", 32)
    ledger_symbol = _strict_text(row[2], "ledger symbol", 64)
    try:
        canonical_exchange_symbol(ledger_symbol)
    except ValueError as exc:
        raise SignalRetentionMaintenanceError(
            "passed structure ledger symbol is invalid"
        ) from exc
    _strict_text(row[3], "ledger structure_id", 256)
    if row[1] not in _LEDGER_STRATEGIES:
        raise SignalRetentionMaintenanceError("ledger contains an unsupported strategy")
    if type(row[4]) is not int or row[4] <= 0:
        raise SignalRetentionMaintenanceError("ledger source signal id is invalid")
    if row[5] is not None and (type(row[5]) is not int or row[5] <= 0):
        raise SignalRetentionMaintenanceError("ledger source scan id is invalid")
    _strict_text(row[6], "ledger source created_at", 128)
    if (
        type(row[7]) is not str
        or len(row[7]) != 64
        or any(character not in "0123456789abcdef" for character in row[7])
    ):
        raise SignalRetentionMaintenanceError("ledger evidence hash is invalid")
    _strict_text(row[8], "ledger created_at", 128)
    if type(row[9]) is not str or row[9] not in ("STAGED", "ACTIVE"):
        raise SignalRetentionMaintenanceError("ledger claim state is invalid")


def _upsert_ledger(
    connection: sqlite3.Connection,
    source: Sequence[Any],
    now: str,
) -> None:
    evidence_sha = _signal_evidence_sha256(source)
    existing = connection.execute(
        """
        SELECT id, strategy_id, symbol, structure_id, source_signal_id,
               source_scan_id, source_signal_created_at, evidence_sha256, created_at,
               claim_state
        FROM strategy_passed_structure_ledger
        WHERE strategy_id = ? AND structure_id = ?
        """,
        (source[2], source[11]),
    ).fetchone()
    if existing is not None:
        _validate_ledger_row(existing)
        expected = (
            source[2], source[3], source[11], source[0], source[1], source[13],
            evidence_sha, existing[8], "ACTIVE",
        )
        if tuple(existing[1:]) != expected:
            raise SignalRetentionMaintenanceError("passed structure ledger conflict")
        return
    connection.execute(
        """
        INSERT INTO strategy_passed_structure_ledger (
            strategy_id, symbol, structure_id, source_signal_id,
            source_scan_id, source_signal_created_at, evidence_sha256, created_at,
            claim_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source[2], source[3], source[11], source[0], source[1], source[13],
            evidence_sha, now, "ACTIVE",
        ),
    )


def _validate_all_ledgers(connection: sqlite3.Connection) -> None:
    seen = {}  # type: Dict[Tuple[str, str], str]
    rows = connection.execute(
        """
        SELECT id, strategy_id, symbol, structure_id, source_signal_id,
               source_scan_id, source_signal_created_at, evidence_sha256, created_at,
               claim_state
        FROM strategy_passed_structure_ledger ORDER BY id
        """
    )
    for row in rows:
        _validate_ledger_row(row)
        key = (row[1], row[3])
        if key in seen and seen[key] != row[2]:
            raise SignalRetentionMaintenanceError("ledger has multiple symbols")
        seen[key] = row[2]
        audit = connection.execute(
            """
            SELECT strategy_id, symbol, structure_id, source_scan_id,
                   signal_created_at, evidence_sha256, claim_state,
                   detail_json
            FROM strategy_passed_signal_audits WHERE source_signal_id = ?
            """,
            (row[4],),
        ).fetchone()
        if (
            audit is None
            or tuple(audit[:7])
            != (row[1], row[2], row[3], row[5], row[6], row[7], row[9])
        ):
            raise SignalRetentionMaintenanceError(
                "ledger does not match its permanent passed audit"
            )
        if row[1] == "N16":
            try:
                _validate_n16_passed_lifecycle(
                    connection,
                    row[2],
                    row[3],
                    audit[7],
                )
                if row[9] == "ACTIVE":
                    _validate_n16_consumption_seal(
                        connection,
                        row[2],
                        row[3],
                    )
                elif connection.execute(
                    "SELECT 1 FROM n16_consumption_seals "
                    "WHERE structure_id = ? LIMIT 1",
                    (row[3],),
                ).fetchone() is not None:
                    raise RuntimeError(
                        "N16 staged claim already has a permanent seal"
                    )
            except Exception as exc:
                raise SignalRetentionMaintenanceError(
                    "N16 permanent claim is not bound to its lifecycle"
                ) from exc


def _validate_preclaim_evidence(connection: sqlite3.Connection) -> None:
    audits = connection.execute(
        """
        SELECT id, source_signal_id, source_scan_id, strategy_id, symbol,
               funding_rate, matched_patterns, trend_slope, current_bullish,
               passed, decision, reason, structure_id, detail_json,
               signal_created_at, evidence_sha256, created_at
        FROM strategy_passed_signal_audits ORDER BY id
        """
    ).fetchall()
    for row in audits:
        _validate_existing_audit(tuple(row) + ("ACTIVE",))

    seen = {}  # type: Dict[Tuple[str, str], str]
    ledgers = connection.execute(
        """
        SELECT id, strategy_id, symbol, structure_id, source_signal_id,
               source_scan_id, source_signal_created_at, evidence_sha256,
               created_at
        FROM strategy_passed_structure_ledger ORDER BY id
        """
    ).fetchall()
    for row in ledgers:
        active_row = tuple(row) + ("ACTIVE",)
        _validate_ledger_row(active_row)
        key = (row[1], row[3])
        if key in seen and seen[key] != row[2]:
            raise SignalRetentionMaintenanceError("ledger has multiple symbols")
        seen[key] = row[2]
        audit = connection.execute(
            """
            SELECT strategy_id, symbol, structure_id, source_scan_id,
                   signal_created_at, evidence_sha256
            FROM strategy_passed_signal_audits WHERE source_signal_id = ?
            """,
            (row[4],),
        ).fetchone()
        if audit is None or tuple(audit) + ("ACTIVE",) != (
            row[1], row[2], row[3], row[5], row[6], row[7], "ACTIVE"
        ):
            raise SignalRetentionMaintenanceError(
                "ledger does not match its permanent passed audit"
            )


def _validate_permanent_backfill(
    connection: sqlite3.Connection,
    state: Sequence[Any],
) -> None:
    cutoff = state[3]
    source_passed_count = state[5]
    origin = state[7] if len(state) > 7 else None
    if origin == "GENESIS":
        if cutoff != 0 or source_passed_count != 0:
            raise SignalRetentionMaintenanceError("invalid genesis migration marker")
    elif origin == "LEGACY":
        if type(cutoff) is not int or cutoff <= 0:
            raise SignalRetentionMaintenanceError("invalid migration cutoff")
    else:
        raise SignalRetentionMaintenanceError("invalid retention origin")
    if type(source_passed_count) is not int or source_passed_count < 0:
        raise SignalRetentionMaintenanceError("invalid source passed count")
    _validate_all_audits(connection)
    _validate_all_ledgers(connection)
    cutoff_audits = connection.execute(
        "SELECT COUNT(*) FROM strategy_passed_signal_audits "
        "WHERE source_signal_id <= ? AND claim_state = 'ACTIVE'",
        (cutoff,),
    ).fetchone()
    if cutoff_audits != (source_passed_count,):
        raise SignalRetentionMaintenanceError(
            "permanent passed audit count does not match migration marker"
        )
    cutoff_all_audits = connection.execute(
        "SELECT COUNT(*) FROM strategy_passed_signal_audits "
        "WHERE source_signal_id <= ?",
        (cutoff,),
    ).fetchone()
    if cutoff_all_audits != cutoff_audits:
        raise SignalRetentionMaintenanceError(
            "migration source contains an unpublished passed claim"
        )
    placeholders = ",".join("?" for _ in sorted(_LEDGER_STRATEGIES))
    strategies = tuple(sorted(_LEDGER_STRATEGIES))
    conflicts = connection.execute(
        """
        SELECT strategy_id, structure_id
        FROM strategy_passed_signal_audits
        WHERE strategy_id IN (%s) AND structure_id IS NOT NULL
        GROUP BY strategy_id, structure_id
        HAVING COUNT(DISTINCT symbol) != 1
        LIMIT 1
        """ % placeholders,
        strategies,
    ).fetchone()
    if conflicts is not None:
        raise SignalRetentionMaintenanceError(
            "permanent passed audits contain a structure symbol conflict"
        )
    audit_keys = connection.execute(
        """
        SELECT strategy_id, structure_id, MIN(symbol)
        FROM strategy_passed_signal_audits
        WHERE strategy_id IN (%s) AND structure_id IS NOT NULL
        GROUP BY strategy_id, structure_id
        """ % placeholders,
        strategies,
    )
    for strategy_id, structure_id, symbol in audit_keys:
        ledger = connection.execute(
            """
            SELECT symbol, claim_state FROM strategy_passed_structure_ledger
            WHERE strategy_id = ? AND structure_id = ?
            """,
            (strategy_id, structure_id),
        ).fetchone()
        audit_state = connection.execute(
            """
            SELECT claim_state FROM strategy_passed_signal_audits
            WHERE strategy_id = ? AND structure_id = ? AND symbol = ?
            ORDER BY source_signal_id LIMIT 1
            """,
            (strategy_id, structure_id, symbol),
        ).fetchone()
        if ledger != (symbol, audit_state[0] if audit_state else None):
            raise SignalRetentionMaintenanceError(
                "permanent passed audit is missing its structure ledger"
            )


def _batch_values(
    connection: sqlite3.Connection,
    keep_scan_id: int,
    summary: _SourceSummary,
) -> Tuple[Any, ...]:
    scan = connection.execute(
        "SELECT completed_at FROM scans WHERE id = ?", (keep_scan_id,)
    ).fetchone()
    if scan is None or len(scan) != 1 or type(scan[0]) is not str or not scan[0]:
        raise SignalRetentionMaintenanceError("keep scan is not completed")
    now = utc_now()
    return (
        keep_scan_id,
        "CURRENT",
        summary.retained_count,
        summary.retained_count,
        summary.retained_first_id,
        summary.retained_last_id,
        summary.retained_manifest_sha256,
        scan[0],
        now,
        now,
    )


def _prepare_backfill(
    connection: sqlite3.Connection,
    keep_scan_id: int,
    summary: _SourceSummary,
) -> None:
    state = _retention_state(connection)
    if state[2] != "PENDING" or state[1] != 0:
        raise SignalRetentionMaintenanceError("backfill requires PENDING inactive state")
    if connection.execute("SELECT 1 FROM strategy_signal_batches LIMIT 1").fetchone():
        raise SignalRetentionMaintenanceError("unexpected strategy signal batch before backfill")
    _validate_all_audits(connection)
    _validate_all_ledgers(connection)
    now = utc_now()
    connection.execute("BEGIN IMMEDIATE")
    try:
        _validate_n16_maintenance_boundary(connection)
        _validate_retention_dependencies(connection)
        for source in _iter_signal_rows(connection):
            if source[8] == 1:
                _upsert_audit(connection, source, now)
        for source_signal_id, expected_symbol in summary.ledger_sources.values():
            source = _signal_row_by_id(connection, source_signal_id)
            if source[3] != expected_symbol:
                raise SignalRetentionMaintenanceError(
                    "ledger source symbol changed during backfill"
                )
            _upsert_ledger(connection, source, now)
        connection.execute(
            """
            INSERT INTO strategy_signal_batches (
                scan_id, state, recorded_count, expected_count,
                first_signal_id, last_signal_id, manifest_sha256,
                completed_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            _batch_values(connection, keep_scan_id, summary),
        )
        cursor = connection.execute(
            """
            UPDATE strategy_signal_current
            SET current_scan_id = ?, retention_active = 0,
                migration_state = 'BACKFILLED', migration_cutoff_signal_id = ?,
                source_signal_count = ?, source_passed_count = ?,
                source_manifest_sha256 = ?, retention_origin = 'LEGACY',
                updated_at = ?
            WHERE singleton_id = 1 AND retention_active = 0
              AND migration_state = 'PENDING'
              AND current_scan_id IS NULL AND retention_origin IS NULL
              AND migration_cutoff_signal_id IS NULL
              AND source_signal_count IS NULL
              AND source_passed_count IS NULL
              AND source_manifest_sha256 IS NULL
            """,
            (
                keep_scan_id,
                summary.cutoff_signal_id,
                summary.signal_count,
                summary.passed_count,
                summary.manifest_sha256,
                now,
            ),
        )
        if cursor.rowcount != 1:
            raise SignalRetentionMaintenanceError("backfill marker update conflict")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def _validate_backfilled_state(
    connection: sqlite3.Connection,
    keep_scan_id: int,
) -> Tuple[Any, ...]:
    state = _retention_state(connection)
    if state[0] != keep_scan_id or state[1] != 0 or state[2] != "BACKFILLED":
        raise SignalRetentionMaintenanceError("invalid BACKFILLED retention state")
    if (
        type(state[3]) is not int
        or state[3] <= 0
        or type(state[4]) is not int
        or state[4] <= 0
        or type(state[5]) is not int
        or state[5] < 0
        or state[5] > state[4]
        or type(state[6]) is not str
        or len(state[6]) != 64
        or any(character not in "0123456789abcdef" for character in state[6])
    ):
        raise SignalRetentionMaintenanceError("invalid backfill source marker")
    remaining_count = connection.execute(
        "SELECT COUNT(*) FROM strategy_signals"
    ).fetchone()
    if (
        remaining_count is None
        or type(remaining_count[0]) is not int
        or remaining_count[0] <= 0
        or remaining_count[0] > state[4]
    ):
        raise SignalRetentionMaintenanceError("backfill remaining count is invalid")
    newer = connection.execute(
        "SELECT 1 FROM strategy_signals WHERE id > ? LIMIT 1", (state[3],)
    ).fetchone()
    if newer is not None:
        raise SignalRetentionMaintenanceError("signals appeared after migration cutoff")
    batch = connection.execute(
        """
        SELECT state, recorded_count, expected_count, first_signal_id,
               last_signal_id, manifest_sha256, completed_at
        FROM strategy_signal_batches WHERE scan_id = ?
        """,
        (keep_scan_id,),
    ).fetchone()
    if (
        batch is None
        or batch[0] != "CURRENT"
        or type(batch[1]) is not int
        or batch[1] <= 0
        or batch[2] != batch[1]
        or type(batch[3]) is not int
        or type(batch[4]) is not int
        or type(batch[5]) is not str
        or len(batch[5]) != 64
        or type(batch[6]) is not str
    ):
        raise SignalRetentionMaintenanceError("backfilled current batch is invalid")
    _validate_permanent_backfill(connection, state)
    for source in _iter_signal_rows(connection):
        if source[8] != 1:
            continue
        audit = connection.execute(
            "SELECT evidence_sha256, claim_state "
            "FROM strategy_passed_signal_audits "
            "WHERE source_signal_id = ?",
            (source[0],),
        ).fetchone()
        if audit != (_signal_evidence_sha256(source), "ACTIVE"):
            raise SignalRetentionMaintenanceError("remaining passed signal audit mismatch")
        if source[2] in _LEDGER_STRATEGIES and source[11] is not None:
            ledger = connection.execute(
                """
                SELECT symbol, claim_state FROM strategy_passed_structure_ledger
                WHERE strategy_id = ? AND structure_id = ?
                """,
                (source[2], source[11]),
            ).fetchone()
            if ledger != (source[3], "ACTIVE"):
                raise SignalRetentionMaintenanceError(
                    "remaining passed structure ledger mismatch"
                )
    return state


def _delete_one_batch(
    connection: sqlite3.Connection,
    keep_scan_id: int,
    batch_size: int,
) -> int:
    connection.execute("BEGIN IMMEDIATE")
    try:
        _validate_retention_dependencies(connection)
        rows = connection.execute(
            """
            SELECT id FROM strategy_signals
            WHERE scan_id IS NULL OR scan_id != ?
            ORDER BY id LIMIT ?
            """,
            (keep_scan_id, batch_size),
        ).fetchall()
        if not rows:
            connection.commit()
            return 0
        if any(
            len(row) != 1 or type(row[0]) is not int or row[0] <= 0
            for row in rows
        ):
            raise SignalRetentionMaintenanceError("invalid deletion batch identity")
        first_id, last_id = rows[0][0], rows[-1][0]
        cursor = connection.execute(
            """
            DELETE FROM strategy_signals
            WHERE id BETWEEN ? AND ? AND (scan_id IS NULL OR scan_id != ?)
            """,
            (first_id, last_id, keep_scan_id),
        )
        if cursor.rowcount != len(rows):
            raise SignalRetentionMaintenanceError("deletion batch count mismatch")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    return len(rows)


def _finish_backfill(
    connection: sqlite3.Connection,
    keep_scan_id: int,
    expected_summary: _SourceSummary,
    expected_protected: Dict[str, str],
    expected_n13: Optional[str],
    expected_sequence: Optional[int],
    expected_checks: Tuple[int, str],
) -> Tuple[
    _SourceSummary,
    Dict[str, str],
    Optional[str],
    Optional[int],
    Tuple[int, str],
]:
    """Activate retention only after same-transaction post-update validation."""

    now = utc_now()
    connection.execute("BEGIN IMMEDIATE")
    try:
        _validate_retention_dependencies(connection)
        backfilled_state = _validate_backfilled_state(connection, keep_scan_id)
        if connection.execute(
            "SELECT 1 FROM strategy_signals "
            "WHERE scan_id IS NULL OR scan_id != ? LIMIT 1",
            (keep_scan_id,),
        ).fetchone():
            raise SignalRetentionMaintenanceError("old strategy signals remain")
        current_count = connection.execute(
            "SELECT COUNT(*) FROM strategy_signals WHERE scan_id = ?",
            (keep_scan_id,),
        ).fetchone()[0]
        batch_count = connection.execute(
            "SELECT recorded_count FROM strategy_signal_batches WHERE scan_id = ?",
            (keep_scan_id,),
        ).fetchone()
        if type(current_count) is not int or batch_count != (current_count,):
            raise SignalRetentionMaintenanceError("current signal batch count mismatch")
        cursor = connection.execute(
            """
            UPDATE strategy_signal_current
            SET retention_active = 1, migration_state = 'COMPLETE', updated_at = ?
            WHERE singleton_id = 1 AND current_scan_id = ?
              AND retention_active = 0 AND migration_state = 'BACKFILLED'
            """,
            (now, keep_scan_id),
        )
        if cursor.rowcount != 1:
            raise SignalRetentionMaintenanceError("retention activation conflict")
        expected_state = (
            keep_scan_id,
            1,
            "COMPLETE",
            backfilled_state[3],
            backfilled_state[4],
            backfilled_state[5],
            backfilled_state[6],
            "LEGACY",
        )
        if _retention_state(connection) != expected_state:
            raise SignalRetentionMaintenanceError(
                "retention identity changed during activation"
            )
        completed_summary = _complete_state_summary(connection, keep_scan_id)
        if completed_summary != expected_summary:
            raise SignalRetentionMaintenanceError(
                "current publication changed during activation"
            )
        completed_protected = _protected_hashes(connection)
        if completed_protected != expected_protected:
            raise SignalRetentionMaintenanceError(
                "protected table changed during activation"
            )
        completed_n13 = (
            _table_full_hash(connection, "n13_rotation_states")
            if "n13_rotation_states" in _table_names(connection)
            else None
        )
        if completed_n13 != expected_n13:
            raise SignalRetentionMaintenanceError(
                "N13 frozen evidence changed during activation"
            )
        completed_sequence = _signal_sequence(connection)
        if completed_sequence != expected_sequence:
            raise SignalRetentionMaintenanceError(
                "strategy signal sequence changed during activation"
            )
        completed_checks = _database_checks(connection)
        if completed_checks != expected_checks or completed_checks != (0, "ok"):
            raise SignalRetentionMaintenanceError(
                "database checks changed during activation"
            )
        _validate_n16_maintenance_boundary(connection)
        connection.commit()
        return (
            completed_summary,
            completed_protected,
            completed_n13,
            completed_sequence,
            completed_checks,
        )
    except BaseException:
        connection.rollback()
        raise


def _backfilled_final_summary(
    connection: sqlite3.Connection,
    keep_scan_id: int,
) -> _SourceSummary:
    state = _validate_backfilled_state(connection, keep_scan_id)
    summary = _source_summary(
        connection,
        keep_scan_id,
        legacy_cutoff_signal_id=state[3],
    )
    if summary.deletable_count != 0:
        raise SignalRetentionMaintenanceError("old strategy signals remain")
    try:
        published = _strategy_signal_published_snapshot(connection, keep_scan_id)
    except (RuntimeError, TypeError, ValueError) as exc:
        raise SignalRetentionMaintenanceError(
            "backfilled current publication evidence is invalid"
        ) from exc
    batch = connection.execute(
        "SELECT state, recorded_count, expected_count, first_signal_id, "
        "last_signal_id, manifest_sha256 FROM strategy_signal_batches "
        "WHERE scan_id = ?",
        (keep_scan_id,),
    ).fetchone()
    if batch != (
        "CURRENT", published[0], published[0], published[1], published[2],
        published[3],
    ):
        raise SignalRetentionMaintenanceError(
            "backfilled current publication identity mismatch"
        )
    _validate_permanent_backfill(connection, state)
    return _SourceSummary(
        signal_count=summary.signal_count,
        passed_count=summary.passed_count,
        ledger_count=summary.ledger_count,
        retained_count=summary.retained_count,
        deletable_count=0,
        cutoff_signal_id=summary.cutoff_signal_id,
        manifest_sha256=summary.manifest_sha256,
        retained_manifest_sha256=published[3],
        retained_first_id=published[1],
        retained_last_id=published[2],
        ledger_sources=summary.ledger_sources,
    )


def _complete_state_summary(
    connection: sqlite3.Connection,
    keep_scan_id: int,
    state_override: Optional[Sequence[Any]] = None,
) -> _SourceSummary:
    state = (
        tuple(state_override)
        if state_override is not None
        else _retention_state(connection)
    )
    try:
        state = _strict_strategy_signal_retention_row(state)
    except (RuntimeError, TypeError, ValueError) as exc:
        raise SignalRetentionMaintenanceError(
            "completed retention identity is invalid"
        ) from exc
    if state[0] != keep_scan_id or state[1:3] != (1, "COMPLETE"):
        raise SignalRetentionMaintenanceError("completed retention identity mismatch")
    staging_hint = (
        None
        if state_override is not None
        else _staging_scan_hint(connection)
    )
    summary = _source_summary(
        connection,
        keep_scan_id,
        legacy_cutoff_signal_id=state[3],
        excluded_scan_ids=(
            frozenset({staging_hint})
            if staging_hint is not None
            else frozenset()
        ),
        allow_empty_keep=True,
    )
    if summary.deletable_count != 0:
        raise SignalRetentionMaintenanceError("completed retention still has old signals")
    staging = (
        _staging_cleanup_plan(connection)
        if staging_hint is not None
        else None
    )
    if staging is not None and staging.scan_id != staging_hint:
        raise SignalRetentionMaintenanceError(
            "STAGING owner changed during completed retention validation"
        )
    batch = connection.execute(
        "SELECT state, recorded_count, expected_count, first_signal_id, last_signal_id, "
        "manifest_sha256 FROM strategy_signal_batches WHERE scan_id = ?",
        (keep_scan_id,),
    ).fetchone()
    try:
        published = _strategy_signal_published_snapshot(
            connection, keep_scan_id
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        raise SignalRetentionMaintenanceError(
            "completed current publication evidence is invalid"
        ) from exc
    if state[7] == "LEGACY":
        if (
            published[0] > 0
            and published[1] <= state[3] < published[2]
        ):
            raise SignalRetentionMaintenanceError(
                "completed current batch crosses the migration cutoff"
            )
        rows_at_or_below_cutoff = connection.execute(
            "SELECT COUNT(*) FROM strategy_signals WHERE id <= ?",
            (state[3],),
        ).fetchone()
        if (
            rows_at_or_below_cutoff is None
            or type(rows_at_or_below_cutoff[0]) is not int
            or rows_at_or_below_cutoff[0] < 0
        ):
            raise SignalRetentionMaintenanceError(
                "completed migration cutoff population is invalid"
            )
        if (
            published[0] > 0
            and published[1] > state[3]
            and rows_at_or_below_cutoff != (0,)
        ):
            raise SignalRetentionMaintenanceError(
                "sealed legacy source rows reappeared after migration cutoff"
            )
        if (
            published[0] > 0
            and published[2] <= state[3]
            and summary.signal_count > state[4]
        ):
            raise SignalRetentionMaintenanceError(
                "completed source-window count exceeds migration source count"
            )
    elif state[7] != "GENESIS":
        raise SignalRetentionMaintenanceError("completed retention origin is invalid")
    if batch != (
        "CURRENT",
        published[0],
        published[0] if published[0] > 0 else None,
        published[1],
        published[2],
        published[3],
    ):
        raise SignalRetentionMaintenanceError("completed current batch mismatch")
    _validate_permanent_backfill(connection, state)
    return _SourceSummary(
        signal_count=summary.signal_count,
        passed_count=summary.passed_count,
        ledger_count=summary.ledger_count,
        retained_count=summary.retained_count,
        deletable_count=summary.deletable_count,
        cutoff_signal_id=summary.cutoff_signal_id,
        manifest_sha256=summary.manifest_sha256,
        retained_manifest_sha256=published[3],
        retained_first_id=published[1],
        retained_last_id=published[2],
        ledger_sources=summary.ledger_sources,
    )


def inspect_signal_retention(
    database: str,
    keep_scan_id: int,
    keep_signal_count: int,
    keep_manifest_sha256: Optional[str] = None,
    *,
    n16_claim_ledger: str,
    _expected_database_identity: Optional[Tuple[int, int]] = None,
    _database_scope: Optional[_DatabaseFileScope] = None,
    _n16_claim_ledger_scope=None,
) -> SignalRetentionReport:
    """Read and validate a migration plan using SQLite URI ``mode=ro`` only."""

    path = Path(database)
    claim_ledger_path = _required_n16_claim_ledger_path(
        path, n16_claim_ledger
    )
    from .n16_claim_ledger import N16PermanentClaimLedger

    claim_ledger = N16PermanentClaimLedger(
        claim_ledger_path,
        file_scope=_n16_claim_ledger_scope,
    )
    if not path.is_absolute() or not path.is_file():
        raise SignalRetentionMaintenanceError(
            "database must be an existing absolute file"
        )
    with _open_database(
        path,
        read_only=True,
        expected_identity=_expected_database_identity,
        file_scope=_database_scope,
    ) as connection:
        tables = frozenset(_table_names(connection))
        _attest_strategy_lifecycle_boundaries(connection, claim_ledger)
        if not {"scans", "strategy_signals"}.issubset(tables):
            raise SignalRetentionMaintenanceError("required legacy tables are missing")
        foreign_keys, integrity = _database_checks(connection)
        latest = _latest_completed_scan_id(connection)
        protected = _protected_hashes(connection)
        schema_version = _retention_schema_version(connection)
        _validate_retention_dependencies(connection)
        schema_ready = schema_version == "CURRENT"
        migration_state = "SCHEMA_NOT_INSTALLED"
        audit_count = 0
        ledger_count = 0
        summary = None  # type: Optional[_SourceSummary]
        retention_state = None
        if schema_version != "ABSENT":
            if schema_ready:
                _verify_retention_schema(connection)
                retention_state = _retention_state(connection)
            else:
                retention_state = _originless_retention_state(connection)
            migration_state = retention_state[2]
            if migration_state == "COMPLETE":
                if schema_version == "LEGACY_PRECLAIM":
                    raise SignalRetentionMaintenanceError(
                        "pre-claim legacy schema cannot contain COMPLETE retention"
                    )
                summary = _complete_state_summary(
                    connection,
                    keep_scan_id,
                    state_override=(None if schema_ready else retention_state),
                )
            elif migration_state == "BACKFILLED":
                if schema_version == "LEGACY_PRECLAIM":
                    raise SignalRetentionMaintenanceError(
                        "pre-claim legacy schema cannot contain BACKFILLED retention"
                    )
                _validate_permanent_backfill(connection, retention_state)
                summary = _source_summary(connection, keep_scan_id)
            else:
                if schema_version == "LEGACY_PRECLAIM":
                    _validate_preclaim_evidence(connection)
                else:
                    _validate_all_audits(connection)
                    _validate_all_ledgers(connection)
                summary = _source_summary(connection, keep_scan_id)
            audit_count = connection.execute(
                "SELECT COUNT(*) FROM strategy_passed_signal_audits"
            ).fetchone()[0]
            ledger_count = connection.execute(
                "SELECT COUNT(*) FROM strategy_passed_structure_ledger"
            ).fetchone()[0]
        else:
            summary = _source_summary(connection, keep_scan_id)
        if summary is None:
            raise SignalRetentionMaintenanceError(
                "strategy signal maintenance summary is unavailable"
            )
        _validate_keep_runtime_snapshot(
            connection,
            keep_scan_id,
            summary,
            published=(migration_state == "COMPLETE"),
        )
        attestation_verified = _validate_keep_attestation(
            summary, keep_signal_count, keep_manifest_sha256
        )
        n13_hash = (
            _table_full_hash(connection, "n13_rotation_states")
            if "n13_rotation_states" in tables
            else None
        )
        sealed_complete_source = (
            retention_state
            if migration_state == "COMPLETE"
            else None
        )
        staging = (
            _staging_cleanup_plan(connection)
            if migration_state == "COMPLETE" and schema_ready
            else None
        )
        return SignalRetentionReport(
            mode="DRY_RUN",
            database=str(path),
            keep_scan_id=keep_scan_id,
            latest_completed_scan_id=latest,
            migration_state=migration_state,
            source_signal_count=(
                sealed_complete_source[4]
                if sealed_complete_source is not None
                else summary.signal_count
            ),
            source_passed_count=(
                sealed_complete_source[5]
                if sealed_complete_source is not None
                else summary.passed_count
            ),
            source_ledger_count=summary.ledger_count,
            retained_signal_count=summary.retained_count,
            deletable_signal_count=summary.deletable_count,
            deleted_signal_count=0,
            audit_count=audit_count,
            ledger_count=ledger_count,
            source_manifest_sha256=(
                sealed_complete_source[6]
                if sealed_complete_source is not None
                else summary.manifest_sha256
            ),
            retained_manifest_sha256=summary.retained_manifest_sha256,
            keep_attestation_verified=attestation_verified,
            protected_manifest_sha256=_hash_mapping(protected),
            n13_rotation_sha256=n13_hash,
            strategy_signal_sequence=_signal_sequence(connection),
            foreign_key_violations=foreign_keys,
            integrity_check=integrity,
            schema_ready=schema_ready,
            staging_scan_id=(staging.scan_id if staging is not None else None),
            staging_signal_count=(
                staging.recorded_count if staging is not None else 0
            ),
            staging_passed_claim_count=(
                staging.passed_claim_count if staging is not None else 0
            ),
            staging_ledger_claim_count=(
                staging.ledger_claim_count if staging is not None else 0
            ),
            staging_micro_claim_count=(
                staging.micro_claim_count if staging is not None else 0
            ),
            staging_cleanup_required=staging is not None,
        )


def apply_signal_retention_maintenance(
    database: str,
    keep_scan_id: int,
    keep_signal_count: int,
    keep_manifest_sha256: str,
    batch_size: int = 500,
    *,
    n16_claim_ledger: str,
    _expected_database_identity: Optional[Tuple[int, int]] = None,
    _database_scope: Optional[_DatabaseFileScope] = None,
    _n16_claim_ledger_scope=None,
) -> SignalRetentionReport:
    """Backfill permanent evidence, prune old signals, and activate retention.

    The caller must hold the Binance application's :class:`InstanceLock`.
    ``database`` must be absolute.  No VACUUM operation is performed here.
    """

    path = Path(database)
    claim_ledger_path = _required_n16_claim_ledger_path(
        path, n16_claim_ledger
    )
    from .n16_claim_ledger import N16PermanentClaimLedger

    claim_ledger = N16PermanentClaimLedger(
        claim_ledger_path,
        file_scope=_n16_claim_ledger_scope,
    )
    if type(keep_manifest_sha256) is not str:
        raise SignalRetentionMaintenanceError(
            "keep_manifest_sha256 is required for apply"
        )
    if not path.is_absolute() or not path.is_file():
        raise SignalRetentionMaintenanceError(
            "database must be an existing absolute file"
        )
    if type(batch_size) is not int or batch_size <= 0 or batch_size > 10_000:
        raise SignalRetentionMaintenanceError("batch_size must be in 1..10000")
    # The attestation gate is deliberately completed through a read-only URI
    # before schema installation or any other write connection is opened.
    with _open_database(
        path,
        read_only=True,
        expected_identity=_expected_database_identity,
        file_scope=_database_scope,
    ) as preflight:
        _database_checks(preflight)
        _attest_strategy_lifecycle_boundaries(preflight, claim_ledger)
        preflight_tables = frozenset(_table_names(preflight))
        preflight_schema_version = _retention_schema_version(preflight)
        if preflight_schema_version != "ABSENT":
            if preflight_schema_version == "CURRENT":
                _verify_retention_schema(
                    preflight,
                    allow_missing_claim_state_scan_indexes=True,
                )
                preflight_state = _retention_state(preflight)
            else:
                preflight_state = _originless_retention_state(preflight)
                if (
                    preflight_schema_version == "LEGACY_PRECLAIM"
                    and preflight_state[2] != "PENDING"
                ):
                    raise SignalRetentionMaintenanceError(
                        "pre-claim legacy retention must be PENDING"
                    )
            preflight_summary = (
                _complete_state_summary(
                    preflight,
                    keep_scan_id,
                    state_override=(
                        None
                        if preflight_schema_version == "CURRENT"
                        else preflight_state
                    ),
                )
                if preflight_state[2] == "COMPLETE"
                else _source_summary(preflight, keep_scan_id)
            )
        else:
            preflight_state = None
            preflight_summary = _source_summary(preflight, keep_scan_id)
        _validate_keep_runtime_snapshot(
            preflight,
            keep_scan_id,
            preflight_summary,
            published=(
                preflight_state is not None
                and preflight_state[2] == "COMPLETE"
            ),
        )
        _validate_keep_attestation(
            preflight_summary, keep_signal_count, keep_manifest_sha256
        )
        preflight_staging = (
            _staging_cleanup_plan(preflight)
            if preflight_state is not None
            and preflight_state[2] == "COMPLETE"
            and preflight_schema_version == "CURRENT"
            else None
        )
        preinstall_protected = _protected_hashes(preflight)

    deleted = 0
    with _open_database(
        path,
        read_only=False,
        expected_identity=_expected_database_identity,
        file_scope=_database_scope,
        prewrite_validator=lambda connection: _attest_strategy_lifecycle_boundaries(
            connection, claim_ledger
        ),
    ) as connection:
        _attest_strategy_lifecycle_boundaries(connection, claim_ledger)
        _install_retention_schema(connection)
        _advance_protected_generation_after_explicit_maintenance(
            connection, claim_ledger
        )
        _validate_retention_dependencies(connection)
        if _protected_hashes(connection) != preinstall_protected:
            raise SignalRetentionMaintenanceError(
                "minimal retention schema installation changed a protected table"
            )

        staging_cleanup_performed = False

        # Establish the protection baseline after the minimal schema exists.
        # Backfill is one short transaction, each DELETE batch is another, and
        # retention remains inactive until every final check has passed.
        before_protected = _protected_hashes(connection)
        before_n13 = (
            _table_full_hash(connection, "n13_rotation_states")
            if "n13_rotation_states" in _table_names(connection)
            else None
        )
        before_sequence = _signal_sequence(connection)
        latest = _latest_completed_scan_id(connection)
        state = _retention_state(connection)
        if state[2] == "PENDING":
            summary = _source_summary(connection, keep_scan_id)
            _validate_keep_runtime_snapshot(
                connection, keep_scan_id, summary, published=False
            )
            _validate_keep_attestation(
                summary, keep_signal_count, keep_manifest_sha256
            )
            _prepare_backfill(connection, keep_scan_id, summary)
            state = _retention_state(connection)
        elif state[2] == "BACKFILLED":
            summary = _source_summary(connection, keep_scan_id)
            _validate_keep_runtime_snapshot(
                connection, keep_scan_id, summary, published=False
            )
            _validate_keep_attestation(
                summary, keep_signal_count, keep_manifest_sha256
            )
        elif state[2] == "COMPLETE":
            summary = _complete_state_summary(connection, keep_scan_id)
            _validate_keep_runtime_snapshot(
                connection, keep_scan_id, summary, published=True
            )
            _validate_keep_attestation(
                summary, keep_signal_count, keep_manifest_sha256
            )
        else:
            raise SignalRetentionMaintenanceError("unsupported migration state")

        if state[2] == "BACKFILLED":
            _attest_strategy_lifecycle_boundaries(connection, claim_ledger)
            _validate_backfilled_state(connection, keep_scan_id)
            if before_protected != _protected_hashes(connection):
                raise SignalRetentionMaintenanceError(
                    "protected table changed before signal deletion"
                )
            if before_n13 != (
                _table_full_hash(connection, "n13_rotation_states")
                if "n13_rotation_states" in _table_names(connection)
                else None
            ):
                raise SignalRetentionMaintenanceError(
                    "N13 frozen evidence changed before signal deletion"
                )
            if before_sequence != _signal_sequence(connection):
                raise SignalRetentionMaintenanceError(
                    "strategy signal sequence changed before deletion"
                )
            while True:
                removed = _delete_one_batch(
                    connection,
                    keep_scan_id=keep_scan_id,
                    batch_size=batch_size,
                )
                if removed == 0:
                    break
                deleted += removed
                _attest_strategy_lifecycle_boundaries(connection, claim_ledger)
            final_summary = _backfilled_final_summary(
                connection, keep_scan_id
            )
        else:
            final_summary = _complete_state_summary(connection, keep_scan_id)

        _validate_keep_attestation(
            final_summary, keep_signal_count, keep_manifest_sha256
        )
        after_checks = _database_checks(connection)
        after_protected = _protected_hashes(connection)
        after_n13 = (
            _table_full_hash(connection, "n13_rotation_states")
            if "n13_rotation_states" in _table_names(connection)
            else None
        )
        after_sequence = _signal_sequence(connection)
        if before_protected != after_protected:
            raise SignalRetentionMaintenanceError("protected table hash changed")
        if before_n13 != after_n13:
            raise SignalRetentionMaintenanceError(
                "N13 frozen evidence hash changed"
            )
        if before_sequence != after_sequence:
            raise SignalRetentionMaintenanceError(
                "strategy signal sequence changed"
            )
        audit_count = connection.execute(
            "SELECT COUNT(*) FROM strategy_passed_signal_audits"
        ).fetchone()[0]
        ledger_count = connection.execute(
            "SELECT COUNT(*) FROM strategy_passed_structure_ledger"
        ).fetchone()[0]
        marker = _retention_state(connection)
        if marker[2] == "BACKFILLED":
            _attest_strategy_lifecycle_boundaries(connection, claim_ledger)
            (
                final_summary,
                after_protected,
                after_n13,
                after_sequence,
                after_checks,
            ) = _finish_backfill(
                connection,
                keep_scan_id,
                final_summary,
                before_protected,
                before_n13,
                before_sequence,
                after_checks,
            )
        elif marker[2] != "COMPLETE":
            raise SignalRetentionMaintenanceError(
                "invalid retention state before activation"
            )
        _attest_strategy_lifecycle_boundaries(connection, claim_ledger)
        if preflight_staging is not None:
            cleanup_snapshot: Dict[str, Any] = {}

            def validate_staging_cleanup(
                active_connection: sqlite3.Connection,
            ) -> None:
                checks = _database_checks(active_connection)
                if checks != (0, "ok"):
                    raise SignalRetentionMaintenanceError(
                        "STAGING cleanup database checks failed"
                    )
                _attest_strategy_lifecycle_boundaries(
                    active_connection, claim_ledger
                )
                summary_after_cleanup = _complete_state_summary(
                    active_connection, keep_scan_id
                )
                _validate_keep_attestation(
                    summary_after_cleanup,
                    keep_signal_count,
                    keep_manifest_sha256,
                )
                protected_after_cleanup = _protected_hashes(active_connection)
                for table, expected_hash in before_protected.items():
                    if table == "micro_strategy_lifecycle":
                        continue
                    if protected_after_cleanup.get(table) != expected_hash:
                        raise SignalRetentionMaintenanceError(
                            "protected table changed during STAGING cleanup"
                        )
                cleanup_snapshot.update(
                    {
                        "checks": checks,
                        "protected": protected_after_cleanup,
                        "n13": (
                            _table_full_hash(
                                active_connection, "n13_rotation_states"
                            )
                            if "n13_rotation_states"
                            in _table_names(active_connection)
                            else None
                        ),
                        "sequence": _signal_sequence(active_connection),
                        "summary": summary_after_cleanup,
                        "audit_count": active_connection.execute(
                            "SELECT COUNT(*) FROM strategy_passed_signal_audits"
                        ).fetchone()[0],
                        "ledger_count": active_connection.execute(
                            "SELECT COUNT(*) FROM strategy_passed_structure_ledger"
                        ).fetchone()[0],
                    }
                )

            _cleanup_staging_batch_transaction(
                connection,
                preflight_staging,
                postdelete_validator=validate_staging_cleanup,
            )
            staging_cleanup_performed = True
            after_checks = cleanup_snapshot["checks"]
            after_protected = cleanup_snapshot["protected"]
            after_n13 = cleanup_snapshot["n13"]
            after_sequence = cleanup_snapshot["sequence"]
            final_summary = cleanup_snapshot["summary"]
            audit_count = cleanup_snapshot["audit_count"]
            ledger_count = cleanup_snapshot["ledger_count"]
        return SignalRetentionReport(
            mode="APPLY",
            database=str(path),
            keep_scan_id=keep_scan_id,
            latest_completed_scan_id=latest,
            migration_state="COMPLETE",
            source_signal_count=(
                marker[4]
                if type(marker[4]) is int
                else final_summary.signal_count
            ),
            source_passed_count=(
                marker[5]
                if type(marker[5]) is int
                else final_summary.passed_count
            ),
            source_ledger_count=ledger_count,
            retained_signal_count=final_summary.retained_count,
            deletable_signal_count=0,
            deleted_signal_count=(
                deleted
                + (
                    preflight_staging.recorded_count
                    if staging_cleanup_performed
                    and preflight_staging is not None
                    else 0
                )
            ),
            audit_count=audit_count,
            ledger_count=ledger_count,
            source_manifest_sha256=(
                marker[6]
                if type(marker[6]) is str
                else final_summary.manifest_sha256
            ),
            retained_manifest_sha256=final_summary.retained_manifest_sha256,
            keep_attestation_verified=True,
            protected_manifest_sha256=_hash_mapping(after_protected),
            n13_rotation_sha256=after_n13,
            strategy_signal_sequence=after_sequence,
            foreign_key_violations=after_checks[0],
            integrity_check=after_checks[1],
            schema_ready=True,
            staging_scan_id=(
                preflight_staging.scan_id
                if preflight_staging is not None
                else None
            ),
            staging_signal_count=(
                preflight_staging.recorded_count
                if preflight_staging is not None
                else 0
            ),
            staging_passed_claim_count=(
                preflight_staging.passed_claim_count
                if preflight_staging is not None
                else 0
            ),
            staging_ledger_claim_count=(
                preflight_staging.ledger_claim_count
                if preflight_staging is not None
                else 0
            ),
            staging_micro_claim_count=(
                preflight_staging.micro_claim_count
                if preflight_staging is not None
                else 0
            ),
            staging_cleanup_required=preflight_staging is not None,
            staging_cleanup_performed=staging_cleanup_performed,
        )


def _validated_vacuum_source(
    connection: sqlite3.Connection,
) -> Dict[str, Any]:
    _validate_n16_maintenance_boundary(connection)
    _verify_retention_schema(connection)
    _validate_retention_dependencies(connection)
    state = _retention_state(connection)
    if state[1:3] != (1, "COMPLETE"):
        raise SignalRetentionMaintenanceError(
            "VACUUM requires completed signal retention maintenance"
        )
    current_scan_id = state[0]
    if type(current_scan_id) is not int or current_scan_id <= 0:
        raise SignalRetentionMaintenanceError(
            "VACUUM requires one published CURRENT signal batch"
        )
    summary = _complete_state_summary(connection, current_scan_id)
    checks = _database_checks(connection)
    tables = frozenset(_table_names(connection))
    return {
        "state": state,
        "summary": summary,
        "checks": checks,
        "protected": _protected_hashes(connection),
        "n13": (
            _table_full_hash(connection, "n13_rotation_states")
            if "n13_rotation_states" in tables
            else None
        ),
        "tables": _all_table_hashes(connection),
        "schema": _sqlite_schema_identity(connection),
        "signal_count": connection.execute(
            "SELECT COUNT(*) FROM strategy_signals"
        ).fetchone()[0],
        "audit_count": connection.execute(
            "SELECT COUNT(*) FROM strategy_passed_signal_audits"
        ).fetchone()[0],
        "ledger_count": connection.execute(
            "SELECT COUNT(*) FROM strategy_passed_structure_ledger"
        ).fetchone()[0],
        "sequence": _signal_sequence(connection),
    }


def vacuum_signal_database_into(
    database: str,
    destination: str,
    *,
    n16_claim_ledger: str,
    _expected_source_identity: Optional[Tuple[int, int]] = None,
    _source_scope: Optional[_DatabaseFileScope] = None,
    _allowed_roots: Tuple[Path, ...] = (),
    _expected_target_parent_identity: Optional[Tuple[int, int]] = None,
    _n16_claim_ledger_scope=None,
    _precommit_validator: Optional[
        Callable[[_AnchoredVacuumOutput], None]
    ] = None,
) -> SignalRetentionReport:
    """Run the separately authorized physical compaction step.

    This is never called by dry-run or apply.  The destination must be a new,
    absolute file in the Binance application's dedicated maintenance path.
    """

    source = Path(database)
    target = Path(destination)
    claim_ledger_path = _required_n16_claim_ledger_path(
        source, n16_claim_ledger
    )
    from .n16_claim_ledger import N16PermanentClaimLedger

    claim_ledger = N16PermanentClaimLedger(
        claim_ledger_path,
        file_scope=_n16_claim_ledger_scope,
    )
    if not source.is_absolute() or not source.is_file():
        raise SignalRetentionMaintenanceError("database must be an absolute file")
    source_identity = _regular_file_identity(source, "SQLite database")
    source_parent_identity = _directory_identity(
        source.parent, "SQLite database parent directory"
    )
    if (
        _expected_source_identity is not None
        and source_identity != _expected_source_identity
    ):
        raise SignalRetentionMaintenanceError(
            "SQLite database identity changed before VACUUM preflight"
        )
    _validate_database_sidecars(
        source,
        "SQLite database",
        require_absent=True,
    )
    vacuum_source_scope = _source_scope or _DatabaseFileScope(
        path=source,
        main_identity=source_identity,
        parent_identity=source_parent_identity,
        sidecar_identities=(None, None, None),
        roots=(),
    )
    if (
        not target.is_absolute()
        or os.path.lexists(str(target))
        or target == source
    ):
        raise SignalRetentionMaintenanceError(
            "VACUUM destination must be a new absolute file in an existing directory"
        )
    target_parent_identity = _directory_identity(
        target.parent, "VACUUM destination parent directory"
    )
    if target_parent_identity == source_parent_identity:
        raise SignalRetentionMaintenanceError(
            "VACUUM destination must use a different parent directory from source"
        )
    if (
        _expected_target_parent_identity is not None
        and target_parent_identity != _expected_target_parent_identity
    ):
        raise SignalRetentionMaintenanceError(
            "VACUUM destination parent identity changed before write"
        )
    if _allowed_roots and not any(
        _is_within(root, target.resolve()) for root in _allowed_roots
    ):
        raise SignalRetentionMaintenanceError(
            "VACUUM destination escaped the Binance allowlist"
        )
    _validate_database_sidecars(
        target,
        "VACUUM destination",
        require_absent=True,
    )
    # The same immutable read-only source connection performs both validation
    # and VACUUM INTO.  Opening the source read-write would switch its journal
    # mode to WAL in _open_database and violate the zero-write source contract.
    anchored_output = None  # type: Optional[_AnchoredVacuumOutput]
    try:
        with _open_immutable_vacuum_source(
            source,
            expected_identity=_expected_source_identity,
            file_scope=vacuum_source_scope,
        ) as connection:
            source_evidence = _validated_vacuum_source(connection)
            source_n16_summary = _attest_strategy_lifecycle_boundaries(
                connection, claim_ledger
            )
            source_protected_commitment = (
                claim_ledger.protected_generation_highwater()
            )
            if source_protected_commitment is None:
                raise SignalRetentionMaintenanceError(
                    "VACUUM source protected generation commitment is absent"
                )
            state = source_evidence["state"]
            foreign_keys, integrity = source_evidence["checks"]
            protected = source_evidence["protected"]
            n13_hash = source_evidence["n13"]
            all_source_hashes = source_evidence["tables"]
            source_schema_identity = source_evidence["schema"]
            if _regular_file_identity(source, "SQLite database") != source_identity:
                raise SignalRetentionMaintenanceError(
                    "SQLite database identity changed before VACUUM write"
                )
            if _directory_identity(
                source.parent, "SQLite database parent directory"
            ) != source_parent_identity:
                raise SignalRetentionMaintenanceError(
                    "SQLite database parent changed before VACUUM write"
                )
            _validate_database_sidecars(
                source,
                "SQLite database",
                require_absent=True,
            )
            if os.path.lexists(str(target)):
                raise SignalRetentionMaintenanceError(
                    "VACUUM destination appeared before write"
                )
            if (
                _directory_identity(
                    target.parent, "VACUUM destination parent directory"
                )
                != target_parent_identity
            ):
                raise SignalRetentionMaintenanceError(
                    "VACUUM destination parent identity changed before write"
                )
            if _allowed_roots and not any(
                _is_within(root, target.resolve()) for root in _allowed_roots
            ):
                raise SignalRetentionMaintenanceError(
                    "VACUUM destination escaped the Binance allowlist"
                )
            _validate_database_sidecars(
                target,
                "VACUUM destination",
                require_absent=True,
            )
            anchored_output = _execute_anchored_vacuum_into(
                connection,
                target,
                target_parent_identity,
            )
            current_scan_id = state[0]
            source_signal_count = source_evidence["signal_count"]
            source_audit_count = source_evidence["audit_count"]
            source_ledger_count = source_evidence["ledger_count"]
            source_sequence = source_evidence["sequence"]

        if _regular_file_identity(source, "SQLite database") != source_identity:
            raise SignalRetentionMaintenanceError(
                "SQLite database identity changed during VACUUM INTO"
            )
        if _directory_identity(
            source.parent, "SQLite database parent directory"
        ) != source_parent_identity:
            raise SignalRetentionMaintenanceError(
                "SQLite database parent changed during VACUUM INTO"
            )
        _validate_database_sidecars(
            source,
            "SQLite database",
            require_absent=True,
        )
        target_identity = _regular_file_identity(target, "VACUUM output")
        if target_identity != anchored_output.identity:
            raise SignalRetentionMaintenanceError(
                "VACUUM output identity differs from anchored creation"
            )
        _restore_vacuum_output_schema_version(
            target,
            expected_identity=target_identity,
            schema_version=source_protected_commitment[1],
        )
        _validate_database_sidecars(target, "VACUUM output")
        if (
            _directory_identity(target.parent, "VACUUM output parent directory")
            != target_parent_identity
        ):
            raise SignalRetentionMaintenanceError(
                "VACUUM output parent identity changed"
            )
        if _allowed_roots and not any(
            _is_within(root, target.resolve()) for root in _allowed_roots
        ):
            raise SignalRetentionMaintenanceError(
                "VACUUM output escaped the Binance allowlist"
            )
        if target_identity == _regular_file_identity(source, "SQLite database"):
            raise SignalRetentionMaintenanceError(
                "VACUUM output aliases source database"
            )
        target_scope = _DatabaseFileScope(
            path=target,
            main_identity=target_identity,
            parent_identity=target_parent_identity,
            sidecar_identities=_capture_database_sidecars(target, "VACUUM output"),
            roots=_allowed_roots,
        )
        with _open_immutable_database(
            target,
            expected_identity=target_identity,
            file_scope=target_scope,
        ) as compacted:
            compacted_n16_summary = _attest_strategy_lifecycle_boundaries(
                compacted, claim_ledger
            )
            if compacted_n16_summary != source_n16_summary:
                raise SignalRetentionMaintenanceError(
                    "VACUUM output N16 lifecycle summary differs from source"
                )
            _verify_retention_schema(compacted)
            _validate_retention_dependencies(compacted)
            compacted_checks = _database_checks(compacted)
            if compacted_checks != (foreign_keys, integrity):
                raise SignalRetentionMaintenanceError(
                    "VACUUM output database checks differ from source"
                )
            if _all_table_hashes(compacted) != all_source_hashes:
                raise SignalRetentionMaintenanceError(
                    "VACUUM output full-table hash differs from source"
                )
            if _sqlite_schema_identity(compacted) != source_schema_identity:
                raise SignalRetentionMaintenanceError(
                    "VACUUM output SQLite schema identity differs from source"
                )
            compacted_protected = _protected_hashes(compacted)
            if compacted_protected != protected:
                raise SignalRetentionMaintenanceError(
                    "VACUUM output protected-table hash differs from source"
                )
            compacted_n13 = (
                _table_full_hash(compacted, "n13_rotation_states")
                if "n13_rotation_states" in _table_names(compacted)
                else None
            )
            if compacted_n13 != n13_hash:
                raise SignalRetentionMaintenanceError(
                    "VACUUM output N13 frozen evidence differs from source"
                )
            compacted_sequence = _signal_sequence(compacted)
            if compacted_sequence != source_sequence:
                raise SignalRetentionMaintenanceError(
                    "VACUUM output strategy signal sequence differs from source"
                )
            compacted_state = _retention_state(compacted)
            if compacted_state != state:
                raise SignalRetentionMaintenanceError(
                    "VACUUM output retention identity differs from source"
                )
            compacted_summary = _complete_state_summary(
                compacted, current_scan_id
            )
            compacted_audit_count = compacted.execute(
                "SELECT COUNT(*) FROM strategy_passed_signal_audits"
            ).fetchone()[0]
            compacted_ledger_count = compacted.execute(
                "SELECT COUNT(*) FROM strategy_passed_structure_ledger"
            ).fetchone()[0]
            if (
                compacted_summary.signal_count != source_signal_count
                or compacted_audit_count != source_audit_count
                or compacted_ledger_count != source_ledger_count
            ):
                raise SignalRetentionMaintenanceError(
                    "VACUUM output retained evidence counts differ from source"
                )
            report = SignalRetentionReport(
                mode="VACUUM_INTO",
                database=str(source),
                keep_scan_id=compacted_state[0],
                latest_completed_scan_id=_latest_completed_scan_id(compacted),
                migration_state=compacted_state[2],
                source_signal_count=compacted_state[4],
                source_passed_count=compacted_state[5],
                source_ledger_count=compacted_ledger_count,
                retained_signal_count=compacted_summary.signal_count,
                deletable_signal_count=compacted_summary.deletable_count,
                deleted_signal_count=0,
                audit_count=compacted_audit_count,
                ledger_count=compacted_ledger_count,
                source_manifest_sha256=compacted_state[6],
                retained_manifest_sha256=(
                    compacted_summary.retained_manifest_sha256
                ),
                keep_attestation_verified=True,
                protected_manifest_sha256=_hash_mapping(compacted_protected),
                n13_rotation_sha256=compacted_n13,
                strategy_signal_sequence=compacted_sequence,
                foreign_key_violations=compacted_checks[0],
                integrity_check=compacted_checks[1],
                schema_ready=True,
                vacuum_performed=True,
                vacuum_destination=str(target),
            )

        vacuum_source_scope.validate_before_open(source)
        target_scope.validate_before_open(target)
        if _precommit_validator is not None:
            _precommit_validator(anchored_output)
        anchored_output.commit()
        return report
    except BaseException:
        if anchored_output is not None and not anchored_output.closed:
            try:
                anchored_output.discard()
            except BaseException as cleanup_error:
                raise SignalRetentionMaintenanceError(
                    "VACUUM output could not be removed after failed validation"
                ) from cleanup_error
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Offline Binance-only strategy_signals retention maintenance; "
            "never changes global services or another system."
        ),
        epilog=(
            "Scope boundary: only --db, --n16-claim-ledger, --lock-file, and an "
            "explicitly requested --vacuum-into path are touched. Back up and "
            "restore code, Review DB, N16 claim ledger and corresponding state "
            "as one generation; any one-sided restore is NO-GO. "
            "Stop/start only binance-trading-bot.service "
            "outside this tool; other services, ports, directories, journald and "
            "logrotate are out of scope. VACUUM is never implicit."
        ),
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--vacuum-into", metavar="ABSOLUTE_NEW_DB")
    mode.add_argument("--install-n16", action="store_true")
    mode.add_argument("--install-n17", action="store_true")
    mode.add_argument("--install-n19", action="store_true")
    mode.add_argument("--install-n18", action="store_true")
    mode.add_argument("--install-n20", action="store_true")
    mode.add_argument("--install-n21-n25", action="store_true")
    mode.add_argument("--install-history-coverage-epochs", action="store_true")
    mode.add_argument(
        "--inspect-authorized-legacy-v3-witness", action="store_true"
    )
    mode.add_argument(
        "--install-authorized-legacy-v3-witness", action="store_true"
    )
    mode.add_argument(
        "--resolve-authorized-legacy-v3-witness", action="store_true"
    )
    mode.add_argument("--repair-n17-frozen-evidence", action="store_true")
    mode.add_argument("--resolve-n16-publication", action="store_true")
    parser.add_argument("--db", required=True, help="Absolute Binance review DB path")
    parser.add_argument(
        "--n16-claim-ledger",
        required=True,
        help="Explicit absolute Binance state claim ledger path",
    )
    parser.add_argument(
        "--binance-root",
        action="append",
        required=True,
        help=(
            "Existing absolute Binance-only root containing touched paths; "
            "repeat for a separate binance-trading-backups root"
        ),
    )
    parser.add_argument(
        "--lock-file", required=True, help="Absolute Binance instance lock path"
    )
    parser.add_argument(
        "--keep-scan-id",
        type=int,
        help="Explicit independently attested complete scan id",
    )
    parser.add_argument(
        "--keep-signal-count",
        type=int,
        help="Independent exact signal count for --keep-scan-id",
    )
    parser.add_argument(
        "--keep-manifest-sha256",
        help=(
            "Independent 64-character lowercase manifest; optional candidate "
            "output for dry-run, required for apply"
        ),
    )
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--expected-symbol")
    parser.add_argument("--expected-family-id")
    parser.add_argument("--expected-structure-id")
    parser.add_argument("--expected-terminal-evidence-sha256")
    parser.add_argument("--expected-state-row-sha256")
    parser.add_argument("--expected-receipt-count", type=int)
    parser.add_argument("--expected-receipt-set-sha256")
    parser.add_argument("--expected-coverage-graph-sha256")
    parser.add_argument("--expected-coverage-catalog-sha256")
    parser.add_argument("--expected-review-canonical-sha256")
    parser.add_argument("--authorization-sha256")
    parser.add_argument("--expected-review-plan-sha256")
    parser.add_argument(
        "--legacy-witness-resolution",
        choices=("SAFE_ABORT", "SAFE_COMMIT"),
    )
    return parser


def _is_within(root: Path, path: Path) -> bool:
    try:
        path.relative_to(root)
        return path != root
    except ValueError:
        return False


@dataclass(frozen=True)
class _CliScopeProof:
    database: Path
    database_identity: Tuple[int, int]
    database_parent_identity: Tuple[int, int]
    database_sidecar_identities: Tuple[Optional[Tuple[int, int]], ...]
    claim_ledger: Path
    claim_ledger_identity: Optional[Tuple[int, int]]
    claim_ledger_parent_identity: Tuple[int, int]
    claim_ledger_sidecar_identities: Tuple[Optional[Tuple[int, int]], ...]
    lock_file: Path
    lock_identity: Optional[Tuple[int, int]]
    lock_parent_identity: Tuple[int, int]
    roots: Tuple[Path, ...]
    root_identities: Tuple[Tuple[int, int], ...]
    vacuum_destination: Optional[Path]
    vacuum_parent_identity: Optional[Tuple[int, int]]


def _validate_cli_scope(
    database: Path,
    claim_ledger: Path,
    lock_file: Path,
    binance_roots: Sequence[Path],
    vacuum_destination: Optional[Path],
    allow_missing_claim_ledger: bool,
) -> Tuple[Path, Path, Path, Tuple[Path, ...], Optional[Path]]:
    if type(binance_roots) not in (tuple, list) or not binance_roots:
        raise SignalRetentionMaintenanceError(
            "at least one --binance-root is required"
        )
    resolved_roots = []  # type: List[Path]
    for binance_root in binance_roots:
        if not isinstance(binance_root, Path) or not binance_root.is_absolute():
            raise SignalRetentionMaintenanceError("--binance-root must be absolute")
        _directory_identity(binance_root, "--binance-root")
        resolved_root = binance_root.resolve()
        if (
            "binance" not in resolved_root.name.lower()
            and "币安" not in resolved_root.name
        ):
            raise SignalRetentionMaintenanceError(
                "--binance-root must be an existing dedicated Binance directory"
            )
        if resolved_root not in resolved_roots:
            resolved_roots.append(resolved_root)

    def in_dedicated_root(path: Path) -> bool:
        return any(_is_within(root, path) for root in resolved_roots)

    if (
        not database.is_absolute()
        or not claim_ledger.is_absolute()
        or not lock_file.is_absolute()
    ):
        raise SignalRetentionMaintenanceError(
            "--db, --n16-claim-ledger and --lock-file must be absolute"
        )
    _regular_file_identity(database, "--db")
    resolved_database = database.resolve()
    resolved_claim_ledger = claim_ledger.resolve()
    resolved_lock = lock_file.resolve()
    if not resolved_database.is_file() or not in_dedicated_root(resolved_database):
        raise SignalRetentionMaintenanceError(
            "--db must be an existing file inside --binance-root"
        )
    if not in_dedicated_root(resolved_lock):
        raise SignalRetentionMaintenanceError(
            "--lock-file must be inside --binance-root"
        )
    if not in_dedicated_root(resolved_claim_ledger):
        raise SignalRetentionMaintenanceError(
            "--n16-claim-ledger must be inside --binance-root"
        )
    _directory_identity(
        claim_ledger.parent, "--n16-claim-ledger parent directory"
    )
    claim_ledger_exists = os.path.lexists(str(claim_ledger))
    if claim_ledger_exists:
        _regular_file_identity(claim_ledger, "--n16-claim-ledger")
        _validate_database_sidecars(claim_ledger, "--n16-claim-ledger")
    else:
        if not allow_missing_claim_ledger:
            raise SignalRetentionMaintenanceError(
                "--n16-claim-ledger must identify an existing ledger"
            )
        _validate_database_sidecars(
            claim_ledger,
            "--n16-claim-ledger",
            require_absent=True,
        )
    _directory_identity(resolved_lock.parent, "--lock-file parent directory")
    lock_exists = os.path.lexists(str(lock_file))
    if lock_exists:
        _regular_file_identity(lock_file, "--lock-file")
    if resolved_claim_ledger in (resolved_database, resolved_lock):
        raise SignalRetentionMaintenanceError(
            "--n16-claim-ledger must differ from --db and --lock-file"
        )
    if resolved_database == resolved_lock or (
        lock_exists and database.samefile(lock_file)
    ):
        raise SignalRetentionMaintenanceError(
            "--db and --lock-file must identify different files"
        )

    resolved_destination = None  # type: Optional[Path]
    if vacuum_destination is not None:
        if not vacuum_destination.is_absolute():
            raise SignalRetentionMaintenanceError(
                "--vacuum-into must be absolute"
            )
        resolved_destination = vacuum_destination.resolve()
        if not in_dedicated_root(resolved_destination):
            raise SignalRetentionMaintenanceError(
                "--vacuum-into must be inside --binance-root"
            )
        if os.path.lexists(str(vacuum_destination)):
            raise SignalRetentionMaintenanceError(
                "--vacuum-into must identify a new file"
            )
        _validate_database_sidecars(
            resolved_destination,
            "--vacuum-into",
            require_absent=True,
        )
        destination_parent_identity = _directory_identity(
            vacuum_destination.parent, "--vacuum-into parent directory"
        )
        if destination_parent_identity == _directory_identity(
            database.parent,
            "--db parent directory",
        ):
            raise SignalRetentionMaintenanceError(
                "--vacuum-into must use a different parent directory from --db"
            )
        if resolved_destination in (
            resolved_database,
            resolved_claim_ledger,
            resolved_lock,
        ):
            raise SignalRetentionMaintenanceError(
                "--vacuum-into must differ from all maintenance files"
            )
    return (
        resolved_database,
        resolved_claim_ledger,
        resolved_lock,
        tuple(resolved_roots),
        resolved_destination,
    )


def _capture_cli_scope_proof(
    database: Path,
    claim_ledger: Path,
    lock_file: Path,
    roots: Tuple[Path, ...],
    vacuum_destination: Optional[Path],
) -> _CliScopeProof:
    lock_identity = (
        _regular_file_identity(lock_file, "--lock-file")
        if os.path.lexists(str(lock_file))
        else None
    )
    parent_identity = (
        _directory_identity(
            vacuum_destination.parent, "--vacuum-into parent directory"
        )
        if vacuum_destination is not None
        else None
    )
    return _CliScopeProof(
        database=database,
        database_identity=_regular_file_identity(database, "--db"),
        database_parent_identity=_directory_identity(
            database.parent, "--db parent directory"
        ),
        database_sidecar_identities=_capture_database_sidecars(database, "--db"),
        claim_ledger=claim_ledger,
        claim_ledger_identity=(
            _regular_file_identity(claim_ledger, "--n16-claim-ledger")
            if os.path.lexists(str(claim_ledger))
            else None
        ),
        claim_ledger_parent_identity=_directory_identity(
            claim_ledger.parent, "--n16-claim-ledger parent directory"
        ),
        claim_ledger_sidecar_identities=(
            _capture_database_sidecars(claim_ledger, "--n16-claim-ledger")
            if os.path.lexists(str(claim_ledger))
            else (None, None, None)
        ),
        lock_file=lock_file,
        lock_identity=lock_identity,
        lock_parent_identity=_directory_identity(
            lock_file.parent, "--lock-file parent directory"
        ),
        roots=roots,
        root_identities=tuple(
            _directory_identity(root, "--binance-root") for root in roots
        ),
        vacuum_destination=vacuum_destination,
        vacuum_parent_identity=parent_identity,
    )


def _revalidate_cli_scope_after_lock(
    proof: _CliScopeProof,
    vacuum_destination_exists: bool,
    claim_ledger_exists: bool,
    require_initial_database_sidecars: bool = True,
) -> Optional[Tuple[int, int]]:
    if len(proof.roots) != len(proof.root_identities) or any(
        _directory_identity(root, "--binance-root") != expected
        for root, expected in zip(proof.roots, proof.root_identities)
    ):
        raise SignalRetentionMaintenanceError(
            "Binance allowlist root identity changed after lock"
        )
    database_identity = _regular_file_identity(proof.database, "--db")
    if database_identity != proof.database_identity:
        raise SignalRetentionMaintenanceError(
            "database identity changed after lock"
        )
    if (
        _directory_identity(proof.database.parent, "--db parent directory")
        != proof.database_parent_identity
    ):
        raise SignalRetentionMaintenanceError(
            "database parent identity changed after lock"
        )
    if require_initial_database_sidecars:
        _validate_database_sidecars(
            proof.database,
            "--db",
            expected=proof.database_sidecar_identities,
        )
    else:
        _validate_database_sidecars(proof.database, "--db")
    lock_identity = _regular_file_identity(proof.lock_file, "--lock-file")
    if (
        _directory_identity(proof.lock_file.parent, "--lock-file parent directory")
        != proof.lock_parent_identity
    ):
        raise SignalRetentionMaintenanceError(
            "lock parent identity changed during acquire"
        )
    if proof.lock_identity is not None and lock_identity != proof.lock_identity:
        raise SignalRetentionMaintenanceError("lock identity changed during acquire")
    if lock_identity == database_identity:
        raise SignalRetentionMaintenanceError(
            "--db and --lock-file became the same file"
        )
    if (
        _directory_identity(
            proof.claim_ledger.parent,
            "--n16-claim-ledger parent directory",
        )
        != proof.claim_ledger_parent_identity
    ):
        raise SignalRetentionMaintenanceError(
            "N16 claim ledger parent identity changed after lock"
        )
    ledger_is_visible = os.path.lexists(str(proof.claim_ledger))
    if ledger_is_visible != claim_ledger_exists:
        raise SignalRetentionMaintenanceError(
            "N16 claim ledger existence changed unexpectedly"
        )
    ledger_identity = None
    if ledger_is_visible:
        ledger_identity = _regular_file_identity(
            proof.claim_ledger, "--n16-claim-ledger"
        )
        if (
            proof.claim_ledger_identity is not None
            and ledger_identity != proof.claim_ledger_identity
        ):
            raise SignalRetentionMaintenanceError(
                "N16 claim ledger identity changed after lock"
            )
        _validate_database_sidecars(
            proof.claim_ledger,
            "--n16-claim-ledger",
            expected=(
                proof.claim_ledger_sidecar_identities
                if proof.claim_ledger_identity is not None
                else None
            ),
        )
        if ledger_identity in (database_identity, lock_identity):
            raise SignalRetentionMaintenanceError(
                "N16 claim ledger aliases another maintenance file"
            )
    else:
        _validate_database_sidecars(
            proof.claim_ledger,
            "--n16-claim-ledger",
            require_absent=True,
        )
    for path in (proof.database, proof.claim_ledger, proof.lock_file):
        if not any(_is_within(root, path.resolve()) for root in proof.roots):
            raise SignalRetentionMaintenanceError(
                "locked maintenance path escaped the Binance allowlist"
            )

    destination_identity = None  # type: Optional[Tuple[int, int]]
    destination = proof.vacuum_destination
    if destination is not None:
        parent_identity = _directory_identity(
            destination.parent, "--vacuum-into parent directory"
        )
        if parent_identity != proof.vacuum_parent_identity:
            raise SignalRetentionMaintenanceError(
                "VACUUM destination parent identity changed after lock"
            )
        if parent_identity == proof.database_parent_identity:
            raise SignalRetentionMaintenanceError(
                "VACUUM destination parent must differ from database parent"
            )
        if not any(
            _is_within(root, destination.resolve()) for root in proof.roots
        ):
            raise SignalRetentionMaintenanceError(
                "VACUUM destination escaped the Binance allowlist"
            )
        exists = os.path.lexists(str(destination))
        if exists != vacuum_destination_exists:
            raise SignalRetentionMaintenanceError(
                "VACUUM destination existence changed unexpectedly"
            )
        if exists:
            destination_identity = _regular_file_identity(
                destination, "--vacuum-into output"
            )
            _validate_database_sidecars(destination, "--vacuum-into output")
            if destination_identity in (database_identity, lock_identity):
                raise SignalRetentionMaintenanceError(
                    "VACUUM output aliases a protected maintenance file"
                )
        else:
            _validate_database_sidecars(
                destination,
                "--vacuum-into",
                require_absent=True,
            )
    return destination_identity


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    database = Path(args.db)
    claim_ledger = Path(args.n16_claim_ledger)
    lock_file = Path(args.lock_file)
    binance_roots = [Path(value) for value in args.binance_root]
    vacuum_destination = (
        Path(args.vacuum_into) if args.vacuum_into is not None else None
    )
    if (args.dry_run or args.apply) and (
        type(args.keep_scan_id) is not int or args.keep_scan_id <= 0
        or type(args.keep_signal_count) is not int
        or args.keep_signal_count < 0
    ):
        print(
            "--keep-scan-id and --keep-signal-count are required for dry-run/apply",
            file=sys.stderr,
        )
        return 2
    if args.apply and type(args.keep_manifest_sha256) is not str:
        print(
            "--keep-manifest-sha256 is required for apply",
            file=sys.stderr,
        )
        return 2
    expected_legacy_witness = {
        "symbol": args.expected_symbol,
        "family_id": args.expected_family_id,
        "structure_id": args.expected_structure_id,
        "terminal_evidence_sha256": (
            args.expected_terminal_evidence_sha256
        ),
        "state_row_sha256": args.expected_state_row_sha256,
        "receipt_count": args.expected_receipt_count,
        "receipt_set_sha256": args.expected_receipt_set_sha256,
        "coverage_graph_sha256": args.expected_coverage_graph_sha256,
        "coverage_catalog_sha256": (
            args.expected_coverage_catalog_sha256
        ),
        "review_canonical_sha256": (
            args.expected_review_canonical_sha256
        ),
        "authorization_sha256": args.authorization_sha256,
        "review_plan_sha256": args.expected_review_plan_sha256,
    }
    if (
        args.install_authorized_legacy_v3_witness
        or args.resolve_authorized_legacy_v3_witness
    ):
        if any(value is None for value in expected_legacy_witness.values()):
            print(
                "all authorized legacy witness expected values are required",
                file=sys.stderr,
            )
            return 2
        if (
            args.resolve_authorized_legacy_v3_witness
            and args.legacy_witness_resolution is None
        ):
            print(
                "--legacy-witness-resolution is required for resolve",
                file=sys.stderr,
            )
            return 2
    elif args.legacy_witness_resolution is not None:
        print(
            "--legacy-witness-resolution is resolve-only",
            file=sys.stderr,
        )
        return 2
    try:
        official_lock = validate_official_instance_lock(
            lock_file,
            claim_ledger,
        )
        (
            database,
            claim_ledger,
            lock_file,
            _roots,
            vacuum_destination,
        ) = _validate_cli_scope(
            database,
            claim_ledger,
            lock_file,
            binance_roots,
            vacuum_destination,
            allow_missing_claim_ledger=bool(args.install_n16),
        )
        scope_proof = _capture_cli_scope_proof(
            database,
            claim_ledger,
            lock_file,
            _roots,
            vacuum_destination,
        )
        with InstanceLock(
            str(lock_file),
            expected_identity=official_lock.identity,
            expected_parent_identity=official_lock.parent_identity,
            require_single_link=True,
            exclusive_create=False,
        ):
            _revalidate_cli_scope_after_lock(
                scope_proof,
                vacuum_destination_exists=False,
                claim_ledger_exists=(scope_proof.claim_ledger_identity is not None),
            )
            database_scope = _DatabaseFileScope(
                path=scope_proof.database,
                main_identity=scope_proof.database_identity,
                parent_identity=scope_proof.database_parent_identity,
                sidecar_identities=scope_proof.database_sidecar_identities,
                roots=scope_proof.roots,
            )
            from .n16_claim_ledger import N16ClaimLedgerFileScope

            claim_ledger_scope = N16ClaimLedgerFileScope(
                path=scope_proof.claim_ledger,
                main_identity=scope_proof.claim_ledger_identity,
                parent_identity=scope_proof.claim_ledger_parent_identity,
                sidecar_identities=(
                    scope_proof.claim_ledger_sidecar_identities
                ),
                roots=scope_proof.roots,
            )
            if claim_ledger_scope.main_identity is None:
                claim_ledger_scope.validate_absent()
            else:
                claim_ledger_scope.validate_before_open(claim_ledger)
            if args.dry_run:
                report = inspect_signal_retention(
                    str(database),
                    args.keep_scan_id,
                    args.keep_signal_count,
                    args.keep_manifest_sha256,
                    n16_claim_ledger=str(claim_ledger),
                    _expected_database_identity=scope_proof.database_identity,
                    _database_scope=database_scope,
                    _n16_claim_ledger_scope=claim_ledger_scope,
                )
            elif args.apply:
                report = apply_signal_retention_maintenance(
                    str(database),
                    args.keep_scan_id,
                    args.keep_signal_count,
                    args.keep_manifest_sha256,
                    args.batch_size,
                    n16_claim_ledger=str(claim_ledger),
                    _expected_database_identity=scope_proof.database_identity,
                    _database_scope=database_scope,
                    _n16_claim_ledger_scope=claim_ledger_scope,
                )
            elif args.vacuum_into:
                def validate_vacuum_scope_before_commit(
                    output: _AnchoredVacuumOutput,
                ) -> None:
                    claim_ledger_scope.validate_before_open(claim_ledger)
                    destination_identity = _revalidate_cli_scope_after_lock(
                        scope_proof,
                        vacuum_destination_exists=True,
                        claim_ledger_exists=True,
                        require_initial_database_sidecars=True,
                    )
                    if destination_identity != output.identity:
                        raise SignalRetentionMaintenanceError(
                            "VACUUM output identity changed before CLI commit"
                        )

                report = vacuum_signal_database_into(
                    str(database),
                    str(vacuum_destination),
                    n16_claim_ledger=str(claim_ledger),
                    _expected_source_identity=scope_proof.database_identity,
                    _source_scope=database_scope,
                    _allowed_roots=scope_proof.roots,
                    _expected_target_parent_identity=(
                        scope_proof.vacuum_parent_identity
                    ),
                    _n16_claim_ledger_scope=claim_ledger_scope,
                    _precommit_validator=validate_vacuum_scope_before_commit,
                )
            elif args.inspect_authorized_legacy_v3_witness:
                from .n16_claim_ledger import N16PermanentClaimLedger

                plan = inspect_authorized_legacy_v3_witness(
                    database,
                    claim_ledger,
                    expected_database_identity=scope_proof.database_identity,
                    database_scope=database_scope,
                    claim_ledger_scope=claim_ledger_scope,
                )
                report = _authorized_legacy_witness_report(
                    "AUTHORIZED_LEGACY_V3_WITNESS_INSPECT",
                    database,
                    claim_ledger,
                    plan,
                    witness_sha256=None,
                    ledger_phase=(
                        "PRE_MIRROR"
                        if N16PermanentClaimLedger(
                            claim_ledger,
                            file_scope=claim_ledger_scope,
                        ).legacy_witness_mirror()
                        is None
                        else "EMPTY"
                    ),
                    resolution="INSPECTED",
                )
            elif args.install_authorized_legacy_v3_witness:
                report = install_authorized_legacy_v3_witness(
                    database,
                    claim_ledger,
                    expected_legacy_witness,
                    expected_database_identity=scope_proof.database_identity,
                    database_scope=database_scope,
                    claim_ledger_scope=claim_ledger_scope,
                    progress_callback=lambda stage, elapsed: print(
                        "authorized-legacy-witness "
                        "stage=%s elapsed_seconds=%.3f"
                        % (stage, elapsed),
                        file=sys.stderr,
                        flush=True,
                    ),
                )
            elif args.resolve_authorized_legacy_v3_witness:
                report = resolve_authorized_legacy_v3_witness(
                    database,
                    claim_ledger,
                    expected_legacy_witness,
                    args.legacy_witness_resolution,
                    expected_database_identity=scope_proof.database_identity,
                    database_scope=database_scope,
                    claim_ledger_scope=claim_ledger_scope,
                )
            elif args.install_n16:
                summary = _install_n16_claim_boundary(
                    database,
                    claim_ledger,
                    expected_database_identity=scope_proof.database_identity,
                    database_scope=database_scope,
                    claim_ledger_scope=claim_ledger_scope,
                )
                report = N16LifecycleMaintenanceReport(
                    mode="N16_INSTALL",
                    database=str(database),
                    claim_ledger=str(claim_ledger),
                    ledger_phase="READY",
                    confirmed_claim_count=summary.confirmed_claim_count,
                    resolution="INSTALLED",
                )
            elif args.install_n17:
                summary = _install_n17_lifecycle_boundary(
                    database,
                    claim_ledger,
                    expected_database_identity=scope_proof.database_identity,
                    database_scope=database_scope,
                    claim_ledger_scope=claim_ledger_scope,
                )
                report = N16LifecycleMaintenanceReport(
                    mode="N17_INSTALL",
                    database=str(database),
                    claim_ledger=str(claim_ledger),
                    ledger_phase="READY",
                    confirmed_claim_count=summary.confirmed_claim_count,
                    resolution="INSTALLED",
                )
            elif args.install_n19:
                summary = _install_n19_lifecycle_boundary(
                    database,
                    claim_ledger,
                    expected_database_identity=scope_proof.database_identity,
                    database_scope=database_scope,
                    claim_ledger_scope=claim_ledger_scope,
                )
                report = N16LifecycleMaintenanceReport(
                    mode="N19_INSTALL",
                    database=str(database),
                    claim_ledger=str(claim_ledger),
                    ledger_phase="READY",
                    confirmed_claim_count=summary.confirmed_claim_count,
                    resolution="INSTALLED",
                )
            elif args.install_n18:
                summary = _install_n18_lifecycle_boundary(
                    database,
                    claim_ledger,
                    expected_database_identity=scope_proof.database_identity,
                    database_scope=database_scope,
                    claim_ledger_scope=claim_ledger_scope,
                )
                report = N16LifecycleMaintenanceReport(
                    mode="N18_INSTALL",
                    database=str(database),
                    claim_ledger=str(claim_ledger),
                    ledger_phase="READY",
                    confirmed_claim_count=summary.confirmed_claim_count,
                    resolution="INSTALLED",
                )
            elif args.install_n20:
                summary = _install_n20_lifecycle_boundary(
                    database,
                    claim_ledger,
                    expected_database_identity=scope_proof.database_identity,
                    database_scope=database_scope,
                    claim_ledger_scope=claim_ledger_scope,
                )
                report = N16LifecycleMaintenanceReport(
                    mode="N20_INSTALL",
                    database=str(database),
                    claim_ledger=str(claim_ledger),
                    ledger_phase="READY",
                    confirmed_claim_count=summary.confirmed_claim_count,
                    resolution="INSTALLED",
                )
            elif args.install_n21_n25:
                summary = _install_micro_lifecycle_boundary(
                    database,
                    claim_ledger,
                    expected_database_identity=scope_proof.database_identity,
                    database_scope=database_scope,
                    claim_ledger_scope=claim_ledger_scope,
                )
                report = N16LifecycleMaintenanceReport(
                    mode="N21_N25_INSTALL",
                    database=str(database),
                    claim_ledger=str(claim_ledger),
                    ledger_phase="READY",
                    confirmed_claim_count=summary.confirmed_claim_count,
                    resolution="INSTALLED",
                )
            elif args.install_history_coverage_epochs:
                summary = _install_coverage_epoch_boundary(
                    database,
                    claim_ledger,
                    expected_database_identity=scope_proof.database_identity,
                    database_scope=database_scope,
                    claim_ledger_scope=claim_ledger_scope,
                )
                report = N16LifecycleMaintenanceReport(
                    mode="HISTORY_COVERAGE_EPOCH_INSTALL",
                    database=str(database),
                    claim_ledger=str(claim_ledger),
                    ledger_phase="READY",
                    confirmed_claim_count=summary.confirmed_claim_count,
                    resolution="INSTALLED",
                )
            elif args.repair_n17_frozen_evidence:
                report = _repair_n17_frozen_evidence_boundary(
                    database,
                    claim_ledger,
                    expected_database_identity=scope_proof.database_identity,
                    database_scope=database_scope,
                    claim_ledger_scope=claim_ledger_scope,
                )
            else:
                resolution, summary = _resolve_n16_publication_boundary(
                    database,
                    claim_ledger,
                    expected_database_identity=scope_proof.database_identity,
                    database_scope=database_scope,
                    claim_ledger_scope=claim_ledger_scope,
                )
                report = N16LifecycleMaintenanceReport(
                    mode="N16_RESOLVE",
                    database=str(database),
                    claim_ledger=str(claim_ledger),
                    ledger_phase="READY",
                    confirmed_claim_count=summary.confirmed_claim_count,
                    resolution=resolution,
                )
            claim_ledger_scope.validate_before_open(claim_ledger)
            if not args.vacuum_into:
                database_scope.validate_before_open(scope_proof.database)
                _revalidate_cli_scope_after_lock(
                    scope_proof,
                    vacuum_destination_exists=False,
                    claim_ledger_exists=True,
                    require_initial_database_sidecars=False,
                )
        print(json.dumps(asdict(report), ensure_ascii=False, sort_keys=True))
        return 0
    except (
        OSError,
        sqlite3.Error,
        InstanceLockError,
        N16ClaimLedgerError,
        SignalRetentionMaintenanceError,
    ) as exc:
        print("strategy signal maintenance failed: %s" % exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
