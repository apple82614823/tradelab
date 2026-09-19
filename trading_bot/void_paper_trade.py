from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .instance_lock import (
    InstanceLock,
    InstanceLockError,
    validate_official_instance_lock,
)
from .recorder import PAPER_TRADE_VOID_CONFIRMATION, ReviewRecorder


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Void one explicitly identified legacy paper trade without scoring it."
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--n16-claim-ledger", required=True)
    parser.add_argument("--lock-file", required=True)
    parser.add_argument("--trade-id", required=True, type=int)
    parser.add_argument("--strategy-id", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--confirm", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        lock_proof = validate_official_instance_lock(
            Path(args.lock_file),
            Path(args.n16_claim_ledger),
        )
        with InstanceLock(
            str(lock_proof.path),
            expected_identity=lock_proof.identity,
            expected_parent_identity=lock_proof.parent_identity,
            require_single_link=True,
            exclusive_create=False,
        ):
            recorder = ReviewRecorder(
                args.db,
                logging.getLogger("void-paper-trade"),
                n16_claim_ledger_file=args.n16_claim_ledger,
            )
            changed = recorder.void_strategy_paper_trade(
                args.trade_id,
                args.strategy_id,
                args.symbol,
                args.confirm,
            )
    except InstanceLockError as exc:
        print("paper trade VOID failed: %s" % exc, file=sys.stderr)
        return 1
    print("VOIDED" if changed else "ALREADY_VOID")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
