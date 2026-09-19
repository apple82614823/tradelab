#!/usr/bin/env python3
"""Run complete unittest discovery in concurrent, module-preserving shards."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[2]


def _flatten(suite: unittest.TestSuite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from _flatten(item)
        else:
            yield item


def _discover(start_directory: Path, pattern: str):
    start_directory = start_directory.resolve()
    for path in (ROOT, start_directory):
        value = str(path)
        if value not in sys.path:
            sys.path.insert(0, value)
    suite = unittest.defaultTestLoader.discover(
        str(start_directory), pattern=pattern
    )
    return list(_flatten(suite))


def _ids_digest(test_ids):
    payload = "".join(f"{test_id}\n" for test_id in test_ids)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _partition(tests, shard_count: int):
    modules = {}
    for test in tests:
        modules.setdefault(type(test).__module__, []).append(test.id())

    shards = [dict(modules=[], ids=[]) for _ in range(shard_count)]
    for module_name, module_ids in sorted(
        modules.items(), key=lambda item: (-len(item[1]), item[0])
    ):
        shard = min(
            range(shard_count),
            key=lambda index: (len(shards[index]["ids"]), index),
        )
        shards[shard]["modules"].append(module_name)
        shards[shard]["ids"].extend(module_ids)

    discovered = Counter(test.id() for test in tests)
    assigned = Counter(
        test_id for shard in shards for test_id in shard["ids"]
    )
    if assigned != discovered:
        raise RuntimeError("shard assignment does not cover discovery exactly")
    return shards


def _worker(args) -> int:
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if not 0 <= args.worker_index < len(manifest["shards"]):
        raise RuntimeError("worker shard index is outside the manifest")

    tests = _discover(Path(manifest["start_directory"]), manifest["pattern"])
    discovered_ids = [test.id() for test in tests]
    if _ids_digest(discovered_ids) != manifest["discovery_sha256"]:
        raise RuntimeError("worker discovery differs from parent discovery")

    expected_ids = manifest["shards"][args.worker_index]["ids"]
    remaining = Counter(expected_ids)
    selected = []
    for test in tests:
        test_id = test.id()
        if remaining[test_id]:
            selected.append(test)
            remaining[test_id] -= 1
    if any(remaining.values()):
        raise RuntimeError("worker could not select every assigned test")
    selected_ids = [test.id() for test in selected]
    if Counter(selected_ids) != Counter(expected_ids):
        raise RuntimeError("worker selected tests outside its assignment")

    result = unittest.TextTestRunner(
        stream=sys.stdout,
        verbosity=args.verbosity,
    ).run(unittest.TestSuite(selected))
    successful = result.wasSuccessful() and result.testsRun == len(expected_ids)
    report = {
        "errors": len(result.errors),
        "error_ids": [test.id() for test, _ in result.errors],
        "failures": len(result.failures),
        "failure_ids": [test.id() for test, _ in result.failures],
        "selected_ids": selected_ids,
        "skipped": len(result.skipped),
        "skipped_ids": [test.id() for test, _ in result.skipped],
        "successful": successful,
        "tests_run": result.testsRun,
    }
    args.result.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0 if successful else 1


def _terminate(processes) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()
    for process in processes:
        if process.poll() is None:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def _parent(args) -> int:
    start_directory = Path(args.start_directory)
    if not start_directory.is_absolute():
        start_directory = ROOT / start_directory
    tests = _discover(start_directory, args.pattern)
    if not tests:
        raise RuntimeError("unittest discovery returned no tests")
    discovered_ids = [test.id() for test in tests]
    shards = _partition(tests, args.shards)
    manifest = {
        "discovery_sha256": _ids_digest(discovered_ids),
        "pattern": args.pattern,
        "shards": shards,
        "start_directory": str(start_directory.resolve()),
        "test_count": len(discovered_ids),
    }
    print(
        "Full unittest discovery: "
        f"{len(discovered_ids)} tests; module-preserving shard counts: "
        f"{[len(shard['ids']) for shard in shards]}",
        flush=True,
    )

    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="tradelab-unittest-") as directory:
        temporary = Path(directory)
        manifest_path = temporary / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        processes = []
        logs = []
        try:
            for index in range(args.shards):
                log = (temporary / f"shard-{index}.log").open(
                    "w", encoding="utf-8"
                )
                logs.append(log)
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-u",
                        str(Path(__file__).resolve()),
                        "--worker-index",
                        str(index),
                        "--manifest",
                        str(manifest_path),
                        "--result",
                        str(temporary / f"shard-{index}.json"),
                        "--verbosity",
                        str(args.verbosity),
                    ],
                    cwd=str(ROOT),
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                processes.append(process)
            exit_codes = [process.wait() for process in processes]
        except BaseException:
            _terminate(processes)
            raise
        finally:
            for log in logs:
                log.close()

        reports = []
        problems = []
        for index, exit_code in enumerate(exit_codes):
            log_path = temporary / f"shard-{index}.log"
            print(f"\n===== unittest shard {index} =====", flush=True)
            print(log_path.read_text(encoding="utf-8", errors="replace"), end="")
            if exit_code != 0:
                problems.append(f"shard {index} exited with {exit_code}")
            result_path = temporary / f"shard-{index}.json"
            if not result_path.is_file():
                problems.append(f"shard {index} did not write a result")
                continue
            try:
                report = json.loads(result_path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as error:
                problems.append(f"shard {index} result is unreadable: {error}")
                continue
            reports.append(report)
            expected_ids = shards[index]["ids"]
            if Counter(report.get("selected_ids", ())) != Counter(expected_ids):
                problems.append(f"shard {index} reported the wrong test IDs")
            if report.get("tests_run") != len(expected_ids):
                problems.append(f"shard {index} did not run every assigned test")
            if not report.get("successful"):
                problems.append(f"shard {index} reported test failures")

        summary = {
            "discovered": len(discovered_ids),
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "errors": sum(report.get("errors", 0) for report in reports),
            "exit_codes": exit_codes,
            "failures": sum(report.get("failures", 0) for report in reports),
            "problems": problems,
            "shard_counts": [len(shard["ids"]) for shard in shards],
            "skipped": sum(report.get("skipped", 0) for report in reports),
            "tests_run": sum(report.get("tests_run", 0) for report in reports),
        }
        print("\n" + json.dumps(summary, indent=2, sort_keys=True), flush=True)
        return 0 if not problems and summary["tests_run"] == len(tests) else 1


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-directory", default="tests")
    parser.add_argument("--pattern", default="test_*.py")
    parser.add_argument("--shards", type=_positive_int, default=4)
    parser.add_argument("--verbosity", type=int, choices=(1, 2), default=2)
    parser.add_argument("--worker-index", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--manifest", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--result", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker_index is None:
        if args.manifest is not None or args.result is not None:
            parser.error("worker arguments require --worker-index")
        return _parent(args)
    if args.manifest is None or args.result is None:
        parser.error("worker mode requires --manifest and --result")
    return _worker(args)


if __name__ == "__main__":
    raise SystemExit(main())
