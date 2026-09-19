"""Generated release gate for the stopped-service legacy witness installer.

This is intentionally not named ``test_*.py``: the normal unit suite stays
small, while release acceptance can reproduce a production-sized Review file
and receipt graph with an explicit command.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import resource
import sqlite3
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from tests.test_authorized_legacy_witness import (
    AuthorizedLegacyV3WitnessTests,
)
from trading_bot.coverage_epoch_schema import (
    coverage_epoch_chain_sha256,
    coverage_publication_receipt_sha256,
)
from trading_bot.signal_retention import (
    inspect_authorized_legacy_v3_witness,
    install_authorized_legacy_v3_witness,
)


def _add_receipts(
    database: Path,
    target_count: int,
) -> None:
    now = "2099-01-01T00:00:00+00:00"
    manifest = hashlib.sha256(b"scale-gate-manifest").hexdigest()
    with closing(sqlite3.connect(database)) as connection:
        connection.create_function(
            "_coverage_epoch_mutation_authorized",
            6,
            lambda *args: 1,
        )
        scan_id = connection.execute(
            "SELECT max(id) FROM scans"
        ).fetchone()[0]
        existing = connection.execute(
            "SELECT count(*) FROM history_coverage_publication_receipts"
        ).fetchone()[0]
        for index in range(target_count - existing):
            symbol = "Q%08dUSDT" % index
            source_start = 2_000_000_000_000
            covered_through = source_start + 900_000
            source_sha = hashlib.sha256(symbol.encode("ascii")).hexdigest()
            chain_sha = coverage_epoch_chain_sha256(
                "N17",
                symbol,
                1,
                source_start,
                covered_through,
                source_sha,
                None,
                None,
                None,
                scan_id,
            )
            receipt_sha = coverage_publication_receipt_sha256(
                scan_id,
                "N17",
                symbol,
                source_start,
                covered_through,
                source_sha,
                1,
                source_start,
                chain_sha,
                1,
                None,
                1,
                manifest,
            )
            connection.execute(
                "INSERT INTO history_coverage_epoch_chain "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "N17", symbol, 1, source_start, covered_through,
                    source_sha, None, None, None, chain_sha, scan_id, now,
                ),
            )
            connection.execute(
                "INSERT INTO history_coverage_publication_receipts "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    scan_id, "N17", symbol, source_start, covered_through,
                    source_sha, 1, source_start, chain_sha, 1, None, 1,
                    manifest, receipt_sha, now,
                ),
            )
            connection.execute(
                "INSERT INTO history_coverage_epoch_heads "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    "N17", symbol, 1, source_start, covered_through,
                    source_sha, chain_sha, 1, receipt_sha, now,
                ),
            )
            connection.execute(
                "INSERT INTO n17_history_coverage VALUES (?,?,?,?,?,?)",
                (
                    "N17", symbol, source_start, covered_through,
                    source_sha, now,
                ),
            )
        connection.commit()


def _inflate_review(database: Path, target_mib: int) -> None:
    payload = json.dumps("x" * (1024 * 1024 - 2))
    with closing(sqlite3.connect(database)) as connection:
        while database.stat().st_size < target_mib * 1024 * 1024:
            connection.execute(
                "INSERT INTO events(occurred_at,event_type,symbol,"
                "payload_json,trade_review_id) VALUES "
                "('2099-01-01T00:00:00+00:00','scale',"
                "'SCALEUSDT',?,NULL)",
                (payload,),
            )
            connection.commit()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-mib", type=int, default=290)
    parser.add_argument("--receipt-count", type=int, default=10_251)
    parser.add_argument("--max-install-seconds", type=float, default=120.0)
    parser.add_argument("--max-rss-mib", type=int, default=512)
    args = parser.parse_args()
    if (
        args.database_mib < 1
        or args.receipt_count < 1
        or args.max_install_seconds <= 0
        or args.max_rss_mib < 64
    ):
        raise SystemExit("scale gate arguments are invalid")

    original_tempdir = tempfile.tempdir
    with tempfile.TemporaryDirectory(
        prefix="authorized-legacy-scale-root-"
    ) as root_name:
        tempfile.tempdir = root_name
        try:
            case = AuthorizedLegacyV3WitnessTests("runTest")
            with tempfile.TemporaryDirectory(
                prefix="review-fixture-"
            ) as fixture_name:
                database, ledger, evidence_sha = case._v3_fixture(
                    fixture_name
                )
                _add_receipts(database, args.receipt_count)
                _inflate_review(database, args.database_mib)
                started = time.monotonic()
                rss_before = resource.getrusage(
                    resource.RUSAGE_SELF
                ).ru_maxrss
                with patch(
                    "trading_bot.n19_analyzer."
                    "decode_n19_state_evidence",
                    return_value=case._mock_record(evidence_sha),
                ):
                    plan = inspect_authorized_legacy_v3_witness(
                        database,
                        ledger,
                    )
                    inspected = time.monotonic()
                    install_authorized_legacy_v3_witness(
                        database,
                        ledger,
                        plan.to_jsonable(),
                    )
                finished = time.monotonic()
                rss_after = resource.getrusage(
                    resource.RUSAGE_SELF
                ).ru_maxrss
                with closing(
                    sqlite3.connect(
                        database.as_uri() + "?mode=ro",
                        uri=True,
                    )
                ) as connection:
                    receipt_count = connection.execute(
                        "SELECT count(*) FROM "
                        "history_coverage_publication_receipts"
                    ).fetchone()[0]
                    quick_check = connection.execute(
                        "PRAGMA quick_check"
                    ).fetchone()[0]
                install_seconds = finished - inspected
                rss_units = max(rss_before, rss_after)
                rss_mib = (
                    rss_units / (1024 * 1024)
                    if sys.platform == "darwin"
                    else rss_units / 1024
                )
                if (
                    receipt_count != args.receipt_count
                    or quick_check != "ok"
                    or install_seconds > args.max_install_seconds
                    or rss_mib > args.max_rss_mib
                ):
                    raise RuntimeError("authorized witness scale gate failed")
                print(
                    json.dumps(
                        {
                            "database_bytes": database.stat().st_size,
                            "receipt_count": receipt_count,
                            "inspect_seconds": inspected - started,
                            "install_seconds": install_seconds,
                            "max_rss_mib": rss_mib,
                            "quick_check": quick_check,
                        },
                        sort_keys=True,
                    )
                )
        finally:
            tempfile.tempdir = original_tempdir
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
