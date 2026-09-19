"""Production-sized release gate for atomic ordinary-signal batching.

This is intentionally outside unittest discovery.  It provisions the same
authorized Review/ledger generation used by production-sized maintenance
tests, retains 10,251 coverage receipts, writes and publishes 2,000 ordinary
signals per round, and proves that fixed database authentication is performed
once per signal batch rather than once per row.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import statistics
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from tests.authorized_legacy_witness_scale_gate import (  # noqa: E402
    _add_receipts,
    _inflate_review,
)
from tests.test_authorized_legacy_witness import (  # noqa: E402
    AuthorizedLegacyV3WitnessTests,
)
from trading_bot.recorder import ReviewRecorder  # noqa: E402
from trading_bot.signal_retention import (  # noqa: E402
    inspect_authorized_legacy_v3_witness,
    install_authorized_legacy_v3_witness,
)


def _records(count: int) -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "strategy_id": "N01",
            "symbol": "Q%04dUSDT" % index,
            "funding_rate": "",
            "matched_patterns": (),
            "trend_slope": "",
            "current_bullish": False,
            "passed": False,
            "decision": "REJECTED",
            "reason": "SIGNAL_BATCH_SCALE_GATE",
            "structure_id": None,
            "detail": {"index": index},
        }
        for index in range(count)
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-mib", type=int, default=290)
    parser.add_argument("--receipt-count", type=int, default=10_251)
    parser.add_argument("--signal-count", type=int, default=2_000)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--max-round-seconds", type=float, default=20.0)
    args = parser.parse_args()
    if (
        args.database_mib < 1
        or args.receipt_count < 1
        or args.signal_count < 1
        or args.rounds < 3
        or args.max_round_seconds <= 0
    ):
        raise SystemExit("signal batch scale gate arguments are invalid")

    case = AuthorizedLegacyV3WitnessTests("runTest")
    logger = logging.getLogger("strategy-signal-batch-scale-gate")
    records = _records(args.signal_count)
    durations: list[float] = []
    authentication_counts: list[dict[str, int]] = []
    with tempfile.TemporaryDirectory(
        prefix="strategy-signal-batch-scale-"
    ) as root:
        database, ledger, evidence_sha = case._v3_fixture(root)
        _add_receipts(database, args.receipt_count)
        _inflate_review(database, args.database_mib)
        mocked_record = case._mock_record(evidence_sha)
        with patch(
            "trading_bot.n19_analyzer.decode_n19_state_evidence",
            return_value=mocked_record,
        ):
            plan = inspect_authorized_legacy_v3_witness(database, ledger)
            install_authorized_legacy_v3_witness(
                database, ledger, plan.to_jsonable()
            )
            recorder = ReviewRecorder(
                database,
                logger,
                n16_claim_ledger_file=ledger,
            )
            for _round in range(args.rounds):
                started = time.monotonic()
                scan_id = recorder.begin_scan(100, [], dry_run=True)
                if type(scan_id) is not int:
                    raise RuntimeError("signal batch scale scan did not begin")
                with patch.object(
                    recorder,
                    "_open_identity_attested_runtime_connection",
                    wraps=recorder._open_identity_attested_runtime_connection,
                ) as opened, patch.object(
                    recorder,
                    "_attest_legacy_witness_pair",
                    wraps=recorder._attest_legacy_witness_pair,
                ) as witness_pair, patch.object(
                    recorder,
                    "_attest_n16_claim_ledger_after_catalog_attestation",
                    wraps=(
                        recorder
                        ._attest_n16_claim_ledger_after_catalog_attestation
                    ),
                ) as paired_ledger, patch.object(
                    recorder.n16_claim_ledger,
                    "attest",
                    wraps=recorder.n16_claim_ledger.attest,
                ) as ledger_attest:
                    result = recorder.record_strategy_signals(
                        scan_id, records
                    )
                counts = {
                    "review_opens": opened.call_count,
                    "legacy_pairs": witness_pair.call_count,
                    "paired_ledger_checks": paired_ledger.call_count,
                    "ledger_attests": ledger_attest.call_count,
                }
                if (
                    not result.complete
                    or len(result.signal_ids) != args.signal_count
                    or counts
                    != {
                        "review_opens": 1,
                        "legacy_pairs": 2,
                        "paired_ledger_checks": 1,
                        "ledger_attests": 2,
                    }
                    or not recorder.publish_strategy_signal_batch(
                        scan_id, args.signal_count
                    )
                ):
                    raise RuntimeError("atomic signal batch scale round failed")
                authentication_counts.append(counts)
                durations.append(time.monotonic() - started)

            ordered = sorted(durations)
            p90 = ordered[
                max(0, math.ceil(len(ordered) * 0.90) - 1)
            ]
            current = recorder.list_current_strategy_signals()
            if (
                len(current) != args.signal_count
                or p90 > args.max_round_seconds
            ):
                raise RuntimeError("atomic signal batch performance gate failed")
            print(json.dumps(
                {
                    "database_bytes": database.stat().st_size,
                    "receipt_count": args.receipt_count,
                    "signal_count": args.signal_count,
                    "rounds": args.rounds,
                    "round_seconds": durations,
                    "median_seconds": statistics.median(durations),
                    "p90_seconds": p90,
                    "authentication_counts": authentication_counts,
                    "current_signal_count": len(current),
                },
                sort_keys=True,
            ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
