from __future__ import annotations

import os
from contextlib import closing
from pathlib import Path
import sqlite3

from trading_bot.recorder import ReviewRecorder, utc_now
from trading_bot.n16_claim_ledger import N16PermanentClaimLedger
from trading_bot.signal_retention import (
    _advance_protected_generation_after_explicit_maintenance,
    _install_n16_claim_boundary,
    _install_n17_lifecycle_boundary,
    _install_n18_lifecycle_boundary,
    _install_n19_lifecycle_boundary,
    _install_n20_lifecycle_boundary,
    _install_coverage_epoch_boundary,
    _install_micro_lifecycle_boundary,
)


def test_claim_ledger_path(db_file) -> Path:
    database = Path(os.path.abspath(os.fspath(db_file)))
    return database.with_name(f".{database.name}.n16-claim-ledger.sqlite3")


def make_test_recorder(
    db_file,
    logger,
    *,
    n16_claim_ledger_file=None,
) -> ReviewRecorder:
    """Explicitly install one disposable Review/ledger pair for tests only."""

    database = Path(os.path.abspath(os.fspath(db_file)))
    ledger = (
        Path(os.path.abspath(os.fspath(n16_claim_ledger_file)))
        if n16_claim_ledger_file is not None
        else test_claim_ledger_path(database)
    )
    database.parent.mkdir(parents=True, exist_ok=True)
    if not database.exists():
        # Production maintenance requires an already attested Review main
        # file.  Disposable tests provision that empty file explicitly before
        # invoking the stopped-service installer.
        with closing(sqlite3.connect(database)) as connection:
            connection.commit()
    if not database.exists() or not ledger.exists():
        _install_n16_claim_boundary(database, ledger)
    _install_n17_lifecycle_boundary(database, ledger)
    _install_n19_lifecycle_boundary(database, ledger)
    _install_n18_lifecycle_boundary(database, ledger)
    _install_n20_lifecycle_boundary(database, ledger)
    _install_micro_lifecycle_boundary(database, ledger)
    _install_coverage_epoch_boundary(database, ledger)
    return ReviewRecorder(
        database,
        logger,
        n16_claim_ledger_file=ledger,
    )


def commit_test_schema_change(
    recorder: ReviewRecorder,
    connection: sqlite3.Connection,
) -> None:
    """Commit an intentional test-only DDL change and seal its new catalog."""

    connection.commit()
    _advance_protected_generation_after_explicit_maintenance(
        connection,
        recorder.n16_claim_ledger,
    )


def seal_test_database_catalog(db_file, ledger_file) -> None:
    """Seal an intentional offline test copy's exact Review catalog."""

    with closing(sqlite3.connect(db_file)) as connection:
        from trading_bot.coverage_family_seal import (
            family_seal_catalog_sha256,
        )

        target = (
            connection.execute(
                "SELECT generation FROM history_coverage_protected_generation "
                "WHERE singleton_id=1"
            ).fetchone()[0],
            connection.execute("PRAGMA schema_version").fetchone()[0],
            family_seal_catalog_sha256(connection),
        )
    # This helper provisions a distinct disposable clone, not a transition of
    # the original pair. Recreate only the clone's independent commitment so
    # it attests the schema cookie assigned by sqlite3.Connection.backup().
    with closing(sqlite3.connect(ledger_file)) as connection:
        for trigger in (
            "trg_n16_protected_generation_install_no_replace",
            "trg_n16_protected_generation_install_no_update",
            "trg_n16_protected_generation_install_no_delete",
            "trg_n16_protected_generation_highwater_no_replace",
            "trg_n16_protected_generation_highwater_transition",
            "trg_n16_protected_generation_highwater_no_delete",
        ):
            connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
        connection.execute(
            "DROP TABLE IF EXISTS n16_protected_generation_highwater"
        )
        connection.execute(
            "DROP TABLE IF EXISTS "
            "n16_protected_generation_highwater_installation"
        )
        connection.commit()
    N16PermanentClaimLedger(ledger_file).install_protected_generation_highwater(
        generation=target[0],
        review_schema_version=target[1],
        family_catalog_sha256=target[2],
        now=utc_now(),
    )
