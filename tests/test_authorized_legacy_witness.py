from __future__ import annotations

import hashlib
import gc
import logging
import sqlite3
import tempfile
import unittest
import warnings
from dataclasses import replace
from contextlib import closing
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tests.recorder_test_utils import make_test_recorder, test_claim_ledger_path
from trading_bot.coverage_epoch_schema import (
    COVERAGE_EPOCH_RULE_VERSION,
    LEGACY_V3_INSTALLATION_TABLE_SQL,
    LEGACY_V3_PUBLICATION_TABLE_SQL,
    TRIGGER_SQL as COVERAGE_TRIGGER_SQL,
    _LEGACY_V3_OBJECTS,
    _coverage_epoch_catalog_sha256_for_objects,
    coverage_epoch_schema_status,
    validate_coverage_epoch_graph,
)
from trading_bot.coverage_family_seal import (
    AUTHORIZED_LEGACY_DOMAIN,
    AUTHORIZED_FAMILY_ID,
    AUTHORIZED_STRUCTURE_ID,
    AUTHORIZED_SYMBOL,
    PROVED_TERMINAL_DOMAIN,
    INDEX_SQL as FAMILY_INDEX_SQL,
    TRIGGER_SQL as FAMILY_TRIGGER_SQL,
    family_seal_schema_status,
    family_seal_sha256,
    validate_family_seal_graph,
)
from trading_bot.n16_claim_ledger import N16PermanentClaimLedger
from trading_bot.n15_terminal_schema import (
    N15_TERMINAL_INDEX_SQL,
    N15_TERMINAL_TABLES,
    N15_TERMINAL_TRIGGER_SQL,
)
from trading_bot.recorder import HistoryCoverageProposal
from trading_bot.recorder import ReviewRecorder
from trading_bot.strategy_scheduler import StrategyScheduler
from tests.test_n19 import INTERVAL_MS, _row, analyze, market_fixture, n19_fixture
from trading_bot.signal_retention import (
    SignalRetentionMaintenanceError,
    _advance_protected_generation_after_explicit_maintenance,
    _attest_strategy_lifecycle_boundaries,
    _install_coverage_epoch_boundary,
    inspect_authorized_legacy_v3_witness,
    install_authorized_legacy_v3_witness,
    main as retention_main,
    resolve_authorized_legacy_v3_witness,
)


class AuthorizedLegacyV3WitnessTests(unittest.TestCase):
    def _mock_record(self, evidence_sha: str):
        return SimpleNamespace(
            symbol=AUTHORIZED_SYMBOL,
            family_id=AUTHORIZED_FAMILY_ID,
            structure_id=AUTHORIZED_STRUCTURE_ID,
            stage="MISSED",
            reason="N19_HISTORICAL_ENTRY_MISSED",
            evidence_sha256=evidence_sha,
        )

    def _v3_fixture(self, directory: str):
        database = Path(directory) / "review.sqlite3"
        recorder = make_test_recorder(
            database,
            logging.getLogger("authorized-v3-fixture"),
        )
        ledger = test_claim_ledger_path(database)
        proposal = HistoryCoverageProposal(
            "N17",
            "LEGACYV3USDT",
            1_900_000_000_000,
            1_900_108_000_000,
            1_900_108_900_000,
            hashlib.sha256(b"legacy-v3-source").hexdigest(),
        )
        scan_id = recorder.begin_scan(1, [], True)
        self.assertIsInstance(
            recorder.record_strategy_signal(
                scan_id,
                "N17",
                "LEGACYV3USDT",
                "",
                (),
                "",
                False,
                False,
                "REJECTED",
                "N17_NO_SETUP",
            ),
            int,
        )
        self.assertTrue(
            recorder.publish_strategy_signal_batch(scan_id, 1, (proposal,))
        )
        n19_rows = n19_fixture()
        n19_proposal = StrategyScheduler._history_coverage_proposal(
            "N19", "P000USDT", n19_rows, 122
        )
        n19_scan_id = recorder.begin_scan(1, [], True)
        self.assertIsInstance(
            recorder.record_strategy_signal(
                n19_scan_id,
                "N19",
                "P000USDT",
                "",
                (),
                "",
                False,
                False,
                "REJECTED",
                "N19_NO_STAIRCASE_EXHAUSTION",
            ),
            int,
        )
        self.assertTrue(
            recorder.publish_strategy_signal_batch(
                n19_scan_id, 1, (n19_proposal,)
            )
        )
        evidence_sha = "a" * 64
        with recorder._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            # This fixture models a genuine v3 Review database.  Newer test
            # recorders install the independent N15 terminal generation, so
            # remove that later generation before dismantling the coverage
            # epoch tables it references.  SQLite 3.13 correctly rejects an
            # ALTER that would otherwise leave a dangling trigger.
            for name in N15_TERMINAL_TRIGGER_SQL:
                connection.execute('DROP TRIGGER "%s"' % name)
            for name in N15_TERMINAL_INDEX_SQL:
                connection.execute('DROP INDEX "%s"' % name)
            for table in reversed(N15_TERMINAL_TABLES):
                connection.execute('DROP TABLE "%s"' % table)
            connection.execute(
                "ALTER TABLE n15_market_snapshots "
                "DROP COLUMN payload_sha256"
            )
            for name in FAMILY_TRIGGER_SQL:
                connection.execute('DROP TRIGGER "%s"' % name)
            for name in FAMILY_INDEX_SQL:
                connection.execute('DROP INDEX "%s"' % name)
            for table in (
                "history_coverage_protected_generation",
                "history_coverage_n19_terminal_bundles",
                "history_coverage_n19_legacy_reset_successors",
                "history_coverage_v3_snapshot_anchors",
                "history_coverage_authorized_terminal_bindings",
                "history_coverage_n19_legacy_unbound_witnesses",
                "history_coverage_n19_family_seals",
                "history_coverage_family_seal_installation",
            ):
                connection.execute('DROP TABLE "%s"' % table)
            for name in (
                "trg_history_coverage_receipt_insert_authorized",
                "trg_history_coverage_receipt_no_replace",
                "trg_history_coverage_receipt_no_update",
                "trg_history_coverage_receipt_no_delete",
            ):
                connection.execute('DROP TRIGGER "%s"' % name)
            connection.execute("DROP INDEX idx_history_coverage_receipt_owner")
            connection.execute(
                "ALTER TABLE history_coverage_publication_receipts "
                "RENAME TO history_coverage_publication_receipts_v4"
            )
            connection.execute(LEGACY_V3_PUBLICATION_TABLE_SQL)
            connection.execute(
                "INSERT INTO history_coverage_publication_receipts "
                "SELECT source_scan_id,strategy_id,symbol,"
                "source_start_time_ms,covered_through_time_ms,source_sha256,"
                "result_epoch_ordinal,result_epoch_start_time_ms,"
                "result_chain_head_sha256,publication_ordinal,"
                "previous_receipt_sha256,batch_expected_count,"
                "batch_manifest_sha256,receipt_sha256,published_at "
                "FROM history_coverage_publication_receipts_v4"
            )
            connection.execute(
                "DROP TABLE history_coverage_publication_receipts_v4"
            )
            connection.execute(
                "CREATE UNIQUE INDEX idx_history_coverage_receipt_owner "
                "ON history_coverage_publication_receipts("
                "source_scan_id,strategy_id,symbol)"
            )
            for name in (
                "trg_history_coverage_receipt_insert_authorized",
                "trg_history_coverage_receipt_no_replace",
                "trg_history_coverage_receipt_no_update",
                "trg_history_coverage_receipt_no_delete",
            ):
                connection.execute(COVERAGE_TRIGGER_SQL[name])
            for name in (
                "trg_history_coverage_n19_terminal_insert_authorized",
                "trg_history_coverage_n19_terminal_no_replace",
                "trg_history_coverage_n19_terminal_no_update",
                "trg_history_coverage_n19_terminal_no_delete",
            ):
                connection.execute('DROP TRIGGER "%s"' % name)
            connection.execute(
                "DROP INDEX idx_history_coverage_n19_terminal_owner"
            )
            connection.execute(
                "DROP INDEX idx_history_coverage_n19_terminal_identity"
            )
            connection.execute(
                "DROP TABLE history_coverage_n19_terminal_receipts"
            )
            for name in (
                "trg_history_coverage_installation_no_update",
                "trg_history_coverage_installation_no_delete",
                "trg_history_coverage_installation_no_replace",
                "trg_history_coverage_installation_insert_authorized",
            ):
                connection.execute('DROP TRIGGER "%s"' % name)
            connection.execute(
                "DROP TABLE history_coverage_epoch_installation"
            )
            connection.execute(LEGACY_V3_INSTALLATION_TABLE_SQL)
            connection.execute(
                "INSERT INTO history_coverage_epoch_installation "
                "VALUES (1,3,?,?,?,?)",
                (
                    COVERAGE_EPOCH_RULE_VERSION,
                    1,
                    "0" * 64,
                    "2026-07-27T00:00:00+00:00",
                ),
            )
            for name in (
                "trg_history_coverage_installation_no_update",
                "trg_history_coverage_installation_no_delete",
                "trg_history_coverage_installation_no_replace",
                "trg_history_coverage_installation_insert_authorized",
            ):
                connection.execute(COVERAGE_TRIGGER_SQL[name])
            schema_version = connection.execute(
                "PRAGMA schema_version"
            ).fetchone()[0]
            catalog_sha = _coverage_epoch_catalog_sha256_for_objects(
                connection, _LEGACY_V3_OBJECTS
            )
            connection.execute(
                "DROP TRIGGER trg_history_coverage_installation_no_update"
            )
            connection.execute(
                "UPDATE history_coverage_epoch_installation SET "
                "catalog_schema_version=?,catalog_sha256=?",
                (schema_version, catalog_sha),
            )
            connection.execute(
                COVERAGE_TRIGGER_SQL[
                    "trg_history_coverage_installation_no_update"
                ]
            )
            connection.execute(
                "INSERT INTO n19_staircase_states("
                "strategy_id,symbol,family_id,structure_id,stage,reason,"
                "quote_volume_rank,s_open_time_ms,x_open_time_ms,"
                "reset_after_time_ms,evidence_json,evidence_sha256,"
                "created_at,updated_at) VALUES "
                "('N19',?,?,?,?,?,1,?,?,NULL,?,?,?,?)",
                (
                    AUTHORIZED_SYMBOL,
                    AUTHORIZED_FAMILY_ID,
                    AUTHORIZED_STRUCTURE_ID,
                    "MISSED",
                    "N19_HISTORICAL_ENTRY_MISSED",
                    1_900_000_000_000,
                    1_900_010_000_000,
                    "{}",
                    evidence_sha,
                    "2026-07-27T00:00:00+00:00",
                    "2026-07-27T00:00:00+00:00",
                ),
            )
            recorder._refresh_n16_catalog_generation_in_transaction(
                connection
            )
            connection.commit()
        del recorder
        with closing(sqlite3.connect(ledger)) as connection:
            for name in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type='trigger' "
                "AND name LIKE 'trg_n16_protected_generation_%'"
            ).fetchall():
                connection.execute('DROP TRIGGER "%s"' % name[0])
            connection.execute(
                "DROP TABLE n16_protected_generation_highwater"
            )
            connection.execute(
                "DROP TABLE n16_protected_generation_highwater_installation"
            )
            for name in (
                "trg_n16_legacy_witness_install_no_replace",
                "trg_n16_legacy_witness_install_no_update",
                "trg_n16_legacy_witness_install_no_delete",
                "trg_n16_legacy_witness_mirror_no_replace",
                "trg_n16_legacy_witness_mirror_transition",
                "trg_n16_legacy_witness_mirror_no_delete",
            ):
                connection.execute('DROP TRIGGER "%s"' % name)
            connection.execute("DROP TABLE n16_legacy_witness_mirror")
            connection.execute(
                "DROP TABLE n16_legacy_witness_mirror_installation"
            )
            connection.commit()
        return database, ledger, evidence_sha

    def _drop_family_pair_generation(
        self,
        recorder: ReviewRecorder,
        ledger: Path,
    ) -> None:
        with recorder._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for name in FAMILY_TRIGGER_SQL:
                connection.execute('DROP TRIGGER "%s"' % name)
            for name in FAMILY_INDEX_SQL:
                connection.execute('DROP INDEX "%s"' % name)
            for table in (
                "history_coverage_protected_generation",
                "history_coverage_n19_terminal_bundles",
                "history_coverage_n19_legacy_reset_successors",
                "history_coverage_v3_snapshot_anchors",
                "history_coverage_authorized_terminal_bindings",
                "history_coverage_n19_legacy_unbound_witnesses",
                "history_coverage_n19_family_seals",
                "history_coverage_family_seal_installation",
            ):
                connection.execute('DROP TABLE "%s"' % table)
            recorder._refresh_n16_catalog_generation_in_transaction(
                connection
            )
            connection.commit()
        with closing(sqlite3.connect(ledger)) as connection:
            for name in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type='trigger' "
                "AND name LIKE 'trg_n16_protected_generation_%'"
            ).fetchall():
                connection.execute('DROP TRIGGER "%s"' % name[0])
            connection.execute(
                "DROP TABLE n16_protected_generation_highwater"
            )
            connection.execute(
                "DROP TABLE n16_protected_generation_highwater_installation"
            )
            for name in (
                "trg_n16_legacy_witness_install_no_replace",
                "trg_n16_legacy_witness_install_no_update",
                "trg_n16_legacy_witness_install_no_delete",
                "trg_n16_legacy_witness_mirror_no_replace",
                "trg_n16_legacy_witness_mirror_transition",
                "trg_n16_legacy_witness_mirror_no_delete",
            ):
                connection.execute('DROP TRIGGER "%s"' % name)
            connection.execute("DROP TABLE n16_legacy_witness_mirror")
            connection.execute(
                "DROP TABLE n16_legacy_witness_mirror_installation"
            )
            connection.commit()

    def test_explicit_inspect_install_preserves_v3_rows_and_pairs_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            decoder = patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            )
            with decoder:
                with closing(sqlite3.connect(database)) as connection:
                    before = {
                        table: tuple(connection.execute(
                            'SELECT * FROM "%s" ORDER BY rowid' % table
                        ))
                        for table in (
                            "n19_staircase_states",
                            "history_coverage_epoch_chain",
                            "history_coverage_epoch_heads",
                            "history_coverage_publication_receipts",
                        )
                    }
                with self.assertRaises(SignalRetentionMaintenanceError):
                    _install_coverage_epoch_boundary(database, ledger)
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                report = install_authorized_legacy_v3_witness(
                    database,
                    ledger,
                    plan.to_jsonable(),
                )
                self.assertEqual(report.resolution, "INSTALLED")
                with closing(sqlite3.connect(database)) as connection:
                    self.assertEqual(
                        coverage_epoch_schema_status(connection),
                        "AUTHORIZED_LEGACY_V3",
                    )
                    self.assertEqual(
                        family_seal_schema_status(connection), "CURRENT"
                    )
                    after = {
                        table: tuple(connection.execute(
                            'SELECT * FROM "%s" ORDER BY rowid' % table
                        ))
                        for table in before
                    }
                    self.assertEqual(after, before)
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM "
                            "history_coverage_n19_terminal_receipts"
                        ).fetchone(),
                        (0,),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT proof_domain FROM "
                            "history_coverage_n19_family_seals"
                        ).fetchone(),
                        ("AUTHORIZED_LEGACY_V3_UNBOUND",),
                    )
                self.assertEqual(
                    N16PermanentClaimLedger(
                        ledger
                    ).legacy_witness_mirror()[0],
                    "COMMITTED",
                )

    def test_authorized_generation_future_terminal_uses_binding_overlay(self):
        from trading_bot.n19_analyzer import decode_n19_state_evidence

        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)

            def decode(evidence_json, *, expected_symbol):
                if expected_symbol == AUTHORIZED_SYMBOL:
                    return self._mock_record(evidence_sha)
                return decode_n19_state_evidence(
                    evidence_json, expected_symbol=expected_symbol
                )

            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                side_effect=decode,
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                install_authorized_legacy_v3_witness(
                    database, ledger, plan.to_jsonable()
                )
                recorder = ReviewRecorder(
                    database,
                    logging.getLogger("authorized-future-terminal"),
                    n16_claim_ledger_file=ledger,
                )
                rows = n19_fixture()
                symbols, incomplete_market = market_fixture(rows)
                incomplete_market.pop(symbols[-1])
                confirmed = analyze(
                    rows,
                    market_symbols=symbols,
                    market_klines_by_symbol=incomplete_market,
                )
                rolled = [
                    _row(
                        index,
                        ("106.4", "106.8", "106.2", "106.5"),
                        "100",
                        "55",
                    )
                    for index in range(130, 252)
                ]
                terminal = analyze(
                    rolled,
                    checked_at_ms=int(rolled[-1][0]) + 60_000,
                    frozen_evidence=confirmed.state_record.evidence_json,
                )
                terminal_record = terminal.state_record
                proposal = replace(
                    StrategyScheduler._history_coverage_proposal(
                        "N19", "P000USDT", rolled, 122
                    ),
                    n19_terminal_family_id=terminal_record.family_id,
                    n19_terminal_structure_id=terminal_record.structure_id,
                    n19_terminal_evidence_sha256=(
                        terminal_record.evidence_sha256
                    ),
                    n19_terminal_state_record=terminal_record,
                )
                self.assertEqual(
                    recorder.record_n19_state(confirmed.state_record),
                    "INSERTED",
                )
                scan_id = recorder.begin_scan(100, [], dry_run=True)
                self.assertIsInstance(
                    recorder.record_strategy_signal(
                        scan_id,
                        "N19",
                        "P000USDT",
                        "",
                        (),
                        "",
                        False,
                        False,
                        "REJECTED",
                        "N19_HISTORICAL_ENTRY_MISSED",
                        terminal_record.structure_id,
                        terminal.detail_json(),
                    ),
                    int,
                )
                self.assertTrue(
                    recorder.publish_strategy_signal_batch(
                        scan_id, 1, (proposal,)
                    )
                )
                self.assertTrue(
                    recorder.publish_strategy_signal_batch(
                        scan_id, 1, (proposal,)
                    )
                )
                with recorder._read_only_runtime_snapshot() as connection:
                    validate_coverage_epoch_graph(connection)
                    validate_family_seal_graph(connection)
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM "
                            "history_coverage_authorized_terminal_bindings "
                            "WHERE source_scan_id=?",
                            (scan_id,),
                        ).fetchone(),
                        (1,),
                    )
                del recorder
                restarted = ReviewRecorder(
                    database,
                    logging.getLogger("authorized-future-restart"),
                    n16_claim_ledger_file=ledger,
                )
                self.assertTrue(
                    restarted.publish_strategy_signal_batch(
                        scan_id, 1, (proposal,)
                    )
                )
                runtime = ReviewRecorder(
                    database,
                    logging.getLogger("authorized-v3-runtime"),
                    n16_claim_ledger_file=ledger,
                )
                with runtime._read_only_runtime_snapshot() as connection:
                    self.assertEqual(
                        coverage_epoch_schema_status(connection),
                        "AUTHORIZED_LEGACY_V3",
                    )

    def test_authorized_binding_overlay_requires_atomic_bundle_statement(self):
        from trading_bot.n19_analyzer import decode_n19_state_evidence

        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)

            def decode(evidence_json, *, expected_symbol):
                if expected_symbol == AUTHORIZED_SYMBOL:
                    return self._mock_record(evidence_sha)
                return decode_n19_state_evidence(
                    evidence_json, expected_symbol=expected_symbol
                )

            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                side_effect=decode,
            ):
                plan = inspect_authorized_legacy_v3_witness(database, ledger)
                install_authorized_legacy_v3_witness(
                    database, ledger, plan.to_jsonable()
                )
                with closing(sqlite3.connect(database)) as external:
                    external.create_function(
                        "_coverage_epoch_mutation_authorized",
                        6,
                        lambda *_args: 1,
                    )
                    ordinary = external.execute(
                        "SELECT receipt_sha256,source_scan_id,strategy_id,"
                        "symbol,published_at FROM "
                        "history_coverage_publication_receipts "
                        "WHERE strategy_id='N19' "
                        "ORDER BY source_scan_id LIMIT 1"
                    ).fetchone()
                    self.assertIsNotNone(ordinary)
                    before = external.execute(
                        "SELECT "
                        "(SELECT COUNT(*) FROM "
                        "history_coverage_authorized_terminal_bindings),"
                        "(SELECT COUNT(*) FROM "
                        "history_coverage_n19_terminal_bundles),"
                        "(SELECT COUNT(*) FROM "
                        "history_coverage_n19_terminal_receipts)"
                    ).fetchone()
                    external.execute("BEGIN")
                    with self.assertRaisesRegex(
                        sqlite3.IntegrityError,
                        "overlay requires one atomic bundle",
                    ):
                        external.execute(
                            "INSERT INTO "
                            "history_coverage_authorized_terminal_bindings "
                            "VALUES (?,?,?,?,?,?,?)",
                            (
                                ordinary[0],
                                ordinary[1],
                                ordinary[2],
                                ordinary[3],
                                "b" * 64,
                                "c" * 64,
                                ordinary[4],
                            ),
                        )
                    external.commit()
                    self.assertEqual(
                        external.execute(
                            "SELECT "
                            "(SELECT COUNT(*) FROM "
                            "history_coverage_authorized_terminal_bindings),"
                            "(SELECT COUNT(*) FROM "
                            "history_coverage_n19_terminal_bundles),"
                            "(SELECT COUNT(*) FROM "
                            "history_coverage_n19_terminal_receipts)"
                        ).fetchone(),
                        before,
                    )
                restarted = ReviewRecorder(
                    database,
                    logging.getLogger("authorized-overlay-standalone"),
                    n16_claim_ledger_file=ledger,
                )
                restarted.assert_n16_execution_ready()

    def test_authorized_future_terminal_covers_every_coverage_transition(self):
        from trading_bot.n19_analyzer import (
            analyze_n19_staircase_exhaustion_reversal,
            decode_n19_state_evidence,
        )

        for mode in ("NEW", "NO_CHANGE", "CONTIGUOUS", "GAP"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                database, ledger, evidence_sha = self._v3_fixture(directory)

                def decode(evidence_json, *, expected_symbol):
                    if expected_symbol == AUTHORIZED_SYMBOL:
                        return self._mock_record(evidence_sha)
                    return decode_n19_state_evidence(
                        evidence_json, expected_symbol=expected_symbol
                    )

                with patch(
                    "trading_bot.n19_analyzer.decode_n19_state_evidence",
                    side_effect=decode,
                ):
                    plan = inspect_authorized_legacy_v3_witness(
                        database, ledger
                    )
                    install_authorized_legacy_v3_witness(
                        database, ledger, plan.to_jsonable()
                    )
                    recorder = ReviewRecorder(
                        database,
                        logging.getLogger(
                            "authorized-terminal-%s" % mode.lower()
                        ),
                        n16_claim_ledger_file=ledger,
                    )
                    symbol = "P001USDT" if mode == "NEW" else "P000USDT"
                    base_rows = n19_fixture()
                    market_symbols, incomplete_market = market_fixture(
                        base_rows
                    )
                    incomplete_market.pop(market_symbols[-1])
                    confirmed = analyze_n19_staircase_exhaustion_reversal(
                        symbol,
                        base_rows,
                        quote_volume_rank=7,
                        market_symbols=market_symbols,
                        market_klines_by_symbol=incomplete_market,
                        checked_at_ms=(
                            int(base_rows[-1][0]) + 60_000
                        ),
                    )
                    if mode == "GAP" or mode == "NEW":
                        source = [
                            _row(
                                index,
                                ("106.4", "106.8", "106.2", "106.5"),
                                "100",
                                "55",
                            )
                            for index in range(130, 252)
                        ]
                    else:
                        source = list(base_rows) + [
                            _row(
                                122,
                                ("106.4", "106.8", "106.2", "106.5"),
                                "100",
                                "55",
                            )
                        ]
                    terminal = analyze_n19_staircase_exhaustion_reversal(
                        symbol,
                        source,
                        quote_volume_rank=7,
                        checked_at_ms=int(source[-1][0]) + 60_000,
                        frozen_evidence=confirmed.state_record.evidence_json,
                    )
                    self.assertEqual(
                        terminal.reason,
                        "N19_HISTORICAL_ENTRY_MISSED",
                    )
                    base_proposal = (
                        StrategyScheduler._history_coverage_proposal(
                            "N19", symbol, source, 122
                        )
                    )
                    if mode == "NO_CHANGE":
                        seed_scan = recorder.begin_scan(
                            1, [], dry_run=True
                        )
                        self.assertIsInstance(
                            recorder.record_strategy_signal(
                                seed_scan,
                                "N19",
                                symbol,
                                "",
                                (),
                                "",
                                False,
                                False,
                                "REJECTED",
                                "N19_NO_STAIRCASE_EXHAUSTION",
                            ),
                            int,
                        )
                        self.assertTrue(
                            recorder.publish_strategy_signal_batch(
                                seed_scan, 1, (base_proposal,)
                            )
                        )
                    self.assertEqual(
                        recorder.record_n19_state(
                            confirmed.state_record
                        ),
                        "INSERTED",
                    )
                    proposal = replace(
                        base_proposal,
                        n19_terminal_family_id=(
                            terminal.state_record.family_id
                        ),
                        n19_terminal_structure_id=(
                            terminal.state_record.structure_id
                        ),
                        n19_terminal_evidence_sha256=(
                            terminal.state_record.evidence_sha256
                        ),
                        n19_terminal_state_record=terminal.state_record,
                    )
                    self.assertEqual(
                        recorder.prepare_history_coverage_proposal(
                            proposal
                        ),
                        mode,
                    )
                    scan_id = recorder.begin_scan(1, [], dry_run=True)
                    self.assertIsInstance(
                        recorder.record_strategy_signal(
                            scan_id,
                            "N19",
                            symbol,
                            "",
                            (),
                            "",
                            False,
                            False,
                            "REJECTED",
                            terminal.reason,
                            terminal.structure_id,
                            terminal.detail_json(),
                        ),
                        int,
                    )
                    self.assertTrue(
                        recorder.publish_strategy_signal_batch(
                            scan_id, 1, (proposal,)
                        )
                    )
                    self.assertTrue(
                        recorder.publish_strategy_signal_batch(
                            scan_id, 1, (proposal,)
                        )
                    )
                    with recorder._read_only_runtime_snapshot() as connection:
                        validate_coverage_epoch_graph(connection)
                        validate_family_seal_graph(connection)
                        self.assertEqual(
                            connection.execute(
                                "SELECT COUNT(*) FROM "
                                "history_coverage_n19_terminal_receipts "
                                "WHERE source_scan_id=?",
                                (scan_id,),
                            ).fetchone(),
                            (1,),
                        )
                        self.assertEqual(
                            connection.execute(
                                "SELECT COUNT(*) FROM "
                                "history_coverage_authorized_terminal_bindings "
                                "WHERE source_scan_id=?",
                                (scan_id,),
                            ).fetchone(),
                            (1,),
                        )
                    del recorder
                    restarted = ReviewRecorder(
                        database,
                        logging.getLogger(
                            "authorized-terminal-restart-%s"
                            % mode.lower()
                        ),
                        n16_claim_ledger_file=ledger,
                    )
                    self.assertTrue(
                        restarted.publish_strategy_signal_batch(
                            scan_id, 1, (proposal,)
                        )
                    )

    def test_exact_v4_current_family_pair_requires_explicit_upgrade(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            ledger = Path(directory) / "claim.sqlite3"
            recorder = make_test_recorder(
                database,
                logging.getLogger("v4-family-upgrade"),
                n16_claim_ledger_file=ledger,
            )
            self._drop_family_pair_generation(recorder, ledger)
            del recorder
            with self.assertRaises(RuntimeError):
                ReviewRecorder(
                    database,
                    logging.getLogger("pre-family-runtime"),
                    n16_claim_ledger_file=ledger,
                )
            _install_coverage_epoch_boundary(database, ledger)
            runtime = ReviewRecorder(
                database,
                logging.getLogger("post-family-runtime"),
                n16_claim_ledger_file=ledger,
            )
            with runtime._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    coverage_epoch_schema_status(connection), "CURRENT"
                )
                self.assertEqual(
                    family_seal_schema_status(connection), "CURRENT"
                )
            self.assertEqual(
                N16PermanentClaimLedger(
                    ledger
                ).legacy_witness_mirror()[0],
                "EMPTY",
            )

    def test_review_commit_ack_loss_requires_explicit_safe_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                original = (
                    N16PermanentClaimLedger.commit_legacy_witness_mirror
                )
                with patch.object(
                    N16PermanentClaimLedger,
                    "commit_legacy_witness_mirror",
                    side_effect=OSError("ack lost"),
                ):
                    with self.assertRaises(OSError):
                        install_authorized_legacy_v3_witness(
                            database, ledger, plan.to_jsonable()
                        )
                self.assertEqual(
                    N16PermanentClaimLedger(
                        ledger
                    ).legacy_witness_mirror()[0],
                    "PREPARED",
                )
                with patch.object(
                    N16PermanentClaimLedger,
                    "commit_legacy_witness_mirror",
                    original,
                ):
                    report = resolve_authorized_legacy_v3_witness(
                        database,
                        ledger,
                        plan.to_jsonable(),
                        "SAFE_COMMIT",
                    )
                self.assertEqual(report.ledger_phase, "COMMITTED")

    def test_safe_commit_rejects_any_review_or_ledger_drift(self):
        mutations = ("event", "paper", "state", "table", "ledger_uuid")
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                with tempfile.TemporaryDirectory() as directory:
                    database, ledger, evidence_sha = self._v3_fixture(
                        directory
                    )
                    with patch(
                        "trading_bot.n19_analyzer.decode_n19_state_evidence",
                        return_value=self._mock_record(evidence_sha),
                    ):
                        plan = inspect_authorized_legacy_v3_witness(
                            database, ledger
                        )
                        original = (
                            N16PermanentClaimLedger
                            .commit_legacy_witness_mirror
                        )
                        with patch.object(
                            N16PermanentClaimLedger,
                            "commit_legacy_witness_mirror",
                            side_effect=OSError("ack lost"),
                        ):
                            with self.assertRaises(OSError):
                                install_authorized_legacy_v3_witness(
                                    database, ledger, plan.to_jsonable()
                                )
                        if mutation == "ledger_uuid":
                            with closing(sqlite3.connect(ledger)) as connection:
                                connection.execute(
                                    "UPDATE n16_claim_ledger_meta "
                                    "SET ledger_uuid=? WHERE singleton_id=1",
                                    ("f" * 64,),
                                )
                                connection.commit()
                        else:
                            with closing(
                                sqlite3.connect(database)
                            ) as connection:
                                if mutation == "event":
                                    connection.execute(
                                        "INSERT INTO events("
                                        "occurred_at,event_type,symbol,"
                                        "payload_json,trade_review_id"
                                        ") VALUES (?,?,?,?,NULL)",
                                        (
                                            "2099-01-01T00:00:00Z",
                                            "ordinary_event",
                                            "DRIFTUSDT",
                                            "{}",
                                        ),
                                    )
                                elif mutation == "paper":
                                    connection.execute(
                                        "INSERT INTO strategy_paper_trades("
                                        "strategy_id,symbol,opened_at,"
                                        "entry_price,stop_loss_price,"
                                        "take_profit_price,result,"
                                        "funding_rate,orders_json,detail_json"
                                        ") VALUES (?,?,?,?,?,?,'OPEN',?,?,?)",
                                        (
                                            "N19",
                                            AUTHORIZED_SYMBOL,
                                            "2099-01-01T00:00:00Z",
                                            "1",
                                            "0.99",
                                            "1.05",
                                            "",
                                            "{}",
                                            "{}",
                                        ),
                                    )
                                elif mutation == "state":
                                    connection.execute(
                                        "INSERT INTO n19_staircase_states("
                                        "strategy_id,symbol,family_id,"
                                        "structure_id,stage,reason,"
                                        "quote_volume_rank,s_open_time_ms,"
                                        "x_open_time_ms,reset_after_time_ms,"
                                        "evidence_json,evidence_sha256,"
                                        "created_at,updated_at"
                                        ") SELECT strategy_id,'DRIFTUSDT',?,"
                                        "?,stage,'N19_DRIFT_STATE',"
                                        "quote_volume_rank,s_open_time_ms,"
                                        "x_open_time_ms,reset_after_time_ms,"
                                        "evidence_json,evidence_sha256,"
                                        "created_at,updated_at "
                                        "FROM n19_staircase_states "
                                        "WHERE family_id=?",
                                        (
                                            "f" * 24,
                                            "e" * 24,
                                            AUTHORIZED_FAMILY_ID,
                                        ),
                                    )
                                else:
                                    connection.execute(
                                        "CREATE TABLE hostile_review_drift("
                                        "value TEXT)"
                                    )
                                connection.commit()
                        with patch.object(
                            N16PermanentClaimLedger,
                            "commit_legacy_witness_mirror",
                            original,
                        ):
                            with self.assertRaises(
                                SignalRetentionMaintenanceError
                            ):
                                resolve_authorized_legacy_v3_witness(
                                    database,
                                    ledger,
                                    plan.to_jsonable(),
                                    "SAFE_COMMIT",
                                )
                        self.assertEqual(
                            N16PermanentClaimLedger(
                                ledger
                            ).legacy_witness_mirror()[0],
                            "PREPARED",
                        )

    def test_pre_review_failure_requires_explicit_safe_abort(self):
        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                review_before = database.read_bytes()
                with patch(
                    "trading_bot.coverage_epoch_schema."
                    "install_authorized_legacy_v3_terminal_objects",
                    side_effect=OSError("pre-review failure"),
                ):
                    with self.assertRaises(OSError):
                        install_authorized_legacy_v3_witness(
                            database, ledger, plan.to_jsonable()
                        )
                self.assertEqual(database.read_bytes(), review_before)
                self.assertIsNone(
                    N16PermanentClaimLedger(
                        ledger
                    ).legacy_witness_mirror()
                )

    def test_safe_abort_ack_loss_is_exactly_idempotent(self):
        from trading_bot.coverage_family_seal import (
            review_snapshot_digests,
        )

        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            calls = [0]

            def fail_actual_prewrite(connection):
                calls[0] += 1
                if calls[0] == 3:
                    raise OSError("Review open acknowledgement lost")
                return review_snapshot_digests(connection)

            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                with patch(
                    "trading_bot.coverage_family_seal."
                    "review_snapshot_digests",
                    side_effect=fail_actual_prewrite,
                ):
                    with self.assertRaises(OSError):
                        install_authorized_legacy_v3_witness(
                            database, ledger, plan.to_jsonable()
                        )
                self.assertEqual(
                    N16PermanentClaimLedger(
                        ledger
                    ).legacy_witness_mirror()[0],
                    "PREPARED",
                )
                original = N16PermanentClaimLedger.abort_legacy_witness_mirror

                def abort_then_lose_ack(instance, **kwargs):
                    original(instance, **kwargs)
                    raise OSError("abort acknowledgement lost")

                with patch.object(
                    N16PermanentClaimLedger,
                    "abort_legacy_witness_mirror",
                    abort_then_lose_ack,
                ):
                    with self.assertRaises(OSError):
                        resolve_authorized_legacy_v3_witness(
                            database,
                            ledger,
                            plan.to_jsonable(),
                            "SAFE_ABORT",
                        )
                report = resolve_authorized_legacy_v3_witness(
                    database,
                    ledger,
                    plan.to_jsonable(),
                    "SAFE_ABORT",
                )
                self.assertEqual(report.resolution, "ALREADY_ABORTED")
                self.assertEqual(report.ledger_phase, "EMPTY")

    def test_family_seal_domains_are_mutually_exclusive_and_tamper_blocks(self):
        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                install_authorized_legacy_v3_witness(
                    database, ledger, plan.to_jsonable()
                )
                with closing(sqlite3.connect(database)) as connection:
                    connection.create_function(
                        "_coverage_epoch_mutation_authorized",
                        6,
                        lambda *args: 1,
                    )
                    proof_sha = "e" * 64
                    with self.assertRaises(sqlite3.IntegrityError):
                        connection.execute(
                            "INSERT INTO history_coverage_n19_family_seals "
                            "VALUES ('N19',?,?,?,?,?,?,?,'x')",
                            (
                                AUTHORIZED_SYMBOL,
                                AUTHORIZED_FAMILY_ID,
                                AUTHORIZED_STRUCTURE_ID,
                                evidence_sha,
                                PROVED_TERMINAL_DOMAIN,
                                proof_sha,
                                family_seal_sha256(
                                    AUTHORIZED_SYMBOL,
                                    AUTHORIZED_FAMILY_ID,
                                    AUTHORIZED_STRUCTURE_ID,
                                    evidence_sha,
                                    PROVED_TERMINAL_DOMAIN,
                                    proof_sha,
                                ),
                            ),
                        )
                    connection.rollback()
                    connection.execute(
                        "DROP TRIGGER "
                        "trg_history_coverage_n19_legacy_witness_no_update"
                    )
                    connection.execute(
                        "UPDATE history_coverage_n19_legacy_unbound_witnesses "
                        "SET v3_receipt_set_sha256=? WHERE witness_id=1",
                        ("f" * 64,),
                    )
                    connection.commit()
                    with self.assertRaises(RuntimeError):
                        validate_family_seal_graph(connection)
                with self.assertRaises(RuntimeError):
                    ReviewRecorder(
                        database,
                        logging.getLogger("tampered-witness-runtime"),
                        n16_claim_ledger_file=ledger,
                    )

    def test_v3_snapshot_anchor_detects_old_receipt_metadata_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                install_authorized_legacy_v3_witness(
                    database, ledger, plan.to_jsonable()
                )
                with closing(sqlite3.connect(database)) as connection:
                    connection.execute(
                        "DROP TRIGGER "
                        "trg_history_coverage_receipt_no_update"
                    )
                    connection.execute(
                        "UPDATE history_coverage_publication_receipts "
                        "SET published_at='2099-01-01T00:00:00Z' "
                        "WHERE rowid=(SELECT MIN(rowid) FROM "
                        "history_coverage_publication_receipts)"
                    )
                    connection.execute(
                        COVERAGE_TRIGGER_SQL[
                            "trg_history_coverage_receipt_no_update"
                        ]
                    )
                    connection.commit()
                    with self.assertRaisesRegex(
                        RuntimeError, "receipt anchor"
                    ):
                        validate_family_seal_graph(connection)
                with self.assertRaises(RuntimeError):
                    ReviewRecorder(
                        database,
                        logging.getLogger("anchored-receipt-drift"),
                        n16_claim_ledger_file=ledger,
                    )

    def test_v3_anchor_identity_time_and_head_metadata_are_authenticated(self):
        mutations = ("anchor_identity", "anchored_at", "head_updated_at")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                database, ledger, evidence_sha = self._v3_fixture(directory)
                with patch(
                    "trading_bot.n19_analyzer.decode_n19_state_evidence",
                    return_value=self._mock_record(evidence_sha),
                ):
                    plan = inspect_authorized_legacy_v3_witness(
                        database, ledger
                    )
                    install_authorized_legacy_v3_witness(
                        database, ledger, plan.to_jsonable()
                    )
                    with closing(sqlite3.connect(database)) as connection:
                        if mutation in {"anchor_identity", "anchored_at"}:
                            connection.execute(
                                "DROP TRIGGER "
                                "trg_history_coverage_v3_anchor_no_update"
                            )
                            if mutation == "anchor_identity":
                                connection.execute(
                                    "UPDATE history_coverage_v3_snapshot_anchors "
                                    "SET anchor_identity=anchor_identity||'-drift' "
                                    "WHERE rowid=(SELECT MIN(rowid) FROM "
                                    "history_coverage_v3_snapshot_anchors)"
                                )
                            else:
                                connection.execute(
                                    "UPDATE history_coverage_v3_snapshot_anchors "
                                    "SET anchored_at='2099-01-01T00:00:00+00:00' "
                                    "WHERE rowid=(SELECT MIN(rowid) FROM "
                                    "history_coverage_v3_snapshot_anchors)"
                                )
                            connection.execute(
                                FAMILY_TRIGGER_SQL[
                                    "trg_history_coverage_v3_anchor_no_update"
                                ]
                            )
                        else:
                            for name in (
                                "trg_history_coverage_head_update_authorized",
                                "trg_history_coverage_head_transition",
                            ):
                                connection.execute(
                                    'DROP TRIGGER "%s"' % name
                                )
                            connection.execute(
                                "UPDATE history_coverage_epoch_heads "
                                "SET updated_at='2099-01-01T00:00:00+00:00' "
                                "WHERE rowid=(SELECT MIN(rowid) FROM "
                                "history_coverage_epoch_heads)"
                            )
                            for name in (
                                "trg_history_coverage_head_update_authorized",
                                "trg_history_coverage_head_transition",
                            ):
                                connection.execute(
                                    COVERAGE_TRIGGER_SQL[name]
                                )
                        connection.commit()
                        with self.assertRaises(RuntimeError):
                            validate_family_seal_graph(connection)
                    with self.assertRaises(RuntimeError):
                        ReviewRecorder(
                            database,
                            logging.getLogger("v3-anchor-metadata-drift"),
                            n16_claim_ledger_file=ledger,
                        )

    def test_runtime_hot_path_uses_bounded_family_attestation(self):
        from trading_bot.coverage_family_seal import (
            family_seal_schema_status as real_family_seal_schema_status,
            validate_family_seal_graph as real_validate_family_seal_graph,
        )
        from trading_bot.n19_analyzer import decode_n19_state_evidence

        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)

            def decode(evidence_json, *, expected_symbol):
                if expected_symbol == AUTHORIZED_SYMBOL:
                    return self._mock_record(evidence_sha)
                return decode_n19_state_evidence(
                    evidence_json, expected_symbol=expected_symbol
                )

            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                side_effect=decode,
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                install_authorized_legacy_v3_witness(
                    database, ledger, plan.to_jsonable()
                )
                recorder = ReviewRecorder(
                    database,
                    logging.getLogger("bounded-family-runtime"),
                    n16_claim_ledger_file=ledger,
                )
                with patch(
                    "trading_bot.coverage_family_seal."
                    "validate_family_seal_graph",
                    wraps=real_validate_family_seal_graph,
                ) as validate_graph, patch(
                    "trading_bot.coverage_family_seal."
                    "family_seal_schema_status",
                    wraps=real_family_seal_schema_status,
                ) as schema_status:
                    schema_calls = schema_status.call_count
                    scan_id = recorder.begin_scan(1, [], dry_run=True)
                    self.assertEqual(schema_status.call_count, schema_calls)
                    self.assertIsInstance(
                        recorder.record_strategy_signal(
                            scan_id,
                            "N01",
                            "HOTPATHUSDT",
                            "",
                            (),
                            "",
                            False,
                            False,
                            "REJECTED",
                            "NO_MATCH",
                        ),
                        int,
                    )
                    self.assertEqual(schema_status.call_count, schema_calls)
                    self.assertTrue(
                        recorder.publish_strategy_signal_batch(scan_id, 1)
                    )
                    # Publication performs one bounded generation check; it
                    # must not be repeated by ordinary per-signal connections.
                    self.assertEqual(
                        schema_status.call_count, schema_calls + 1
                    )
                    recorder.assert_n16_execution_ready()
                    self.assertEqual(
                        schema_status.call_count, schema_calls + 1
                    )
                    self.assertEqual(validate_graph.call_count, 0)
                    with closing(sqlite3.connect(database)) as external:
                        external.execute(
                            "INSERT INTO events("
                            "occurred_at,event_type,symbol,payload_json,"
                            "trade_review_id) VALUES (?,?,?,?,NULL)",
                            (
                                "2099-01-01T00:00:00+00:00",
                                "ordinary_event",
                                "HOTPATHUSDT",
                                "{}",
                            ),
                        )
                        external.commit()
                    recorder.record_event(
                        "ordinary_event", {}, "HOTPATHUSDT"
                    )
                    self.assertEqual(
                        schema_status.call_count, schema_calls + 1
                    )
                    self.assertEqual(validate_graph.call_count, 0)

    def test_runtime_signature_never_trusts_overlapping_or_failed_graph(self):
        from trading_bot.coverage_family_seal import (
            validate_family_seal_graph as real_validate_family_seal_graph,
        )
        from trading_bot.n19_analyzer import decode_n19_state_evidence

        for snapshot_kind in ("read_write", "read_only"):
            with self.subTest(snapshot_kind=snapshot_kind):
                with tempfile.TemporaryDirectory() as directory:
                    database, ledger, evidence_sha = self._v3_fixture(
                        directory
                    )

                    def decode(evidence_json, *, expected_symbol):
                        if expected_symbol == AUTHORIZED_SYMBOL:
                            return self._mock_record(evidence_sha)
                        return decode_n19_state_evidence(
                            evidence_json,
                            expected_symbol=expected_symbol,
                        )

                    with patch(
                        "trading_bot.n19_analyzer."
                        "decode_n19_state_evidence",
                        side_effect=decode,
                    ):
                        plan = inspect_authorized_legacy_v3_witness(
                            database, ledger
                        )
                        install_authorized_legacy_v3_witness(
                            database, ledger, plan.to_jsonable()
                        )
                        recorder = ReviewRecorder(
                            database,
                            logging.getLogger(
                                f"runtime-overlap-{snapshot_kind}"
                            ),
                            n16_claim_ledger_file=ledger,
                        )
                        proof_sha = "b" * 64
                        evidence_hash = "c" * 64
                        family_id = "1" * 24
                        structure_id = "2" * 24
                        seal_sha = family_seal_sha256(
                            "BROKENUSDT",
                            family_id,
                            structure_id,
                            evidence_hash,
                            PROVED_TERMINAL_DOMAIN,
                            proof_sha,
                        )
                        context = (
                            recorder._connect
                            if snapshot_kind == "read_write"
                            else recorder._read_only_runtime_snapshot
                        )
                        with patch(
                            "trading_bot.coverage_family_seal."
                            "validate_family_seal_graph",
                            wraps=real_validate_family_seal_graph,
                        ) as validate_graph:
                            with context():
                                with closing(
                                    sqlite3.connect(database)
                                ) as external:
                                    external.create_function(
                                        "_coverage_epoch_mutation_authorized",
                                        6,
                                        lambda *_args: 1,
                                    )
                                    external.execute(
                                        "INSERT INTO "
                                        "history_coverage_n19_family_seals "
                                        "VALUES (?,?,?,?,?,?,?,?,?)",
                                        (
                                            "N19",
                                            "BROKENUSDT",
                                            family_id,
                                            structure_id,
                                            evidence_hash,
                                            PROVED_TERMINAL_DOMAIN,
                                            proof_sha,
                                            seal_sha,
                                            "2099-01-01T00:00:00+00:00",
                                        ),
                                    )
                                    external.commit()
                            for _ in range(2):
                                with self.assertRaisesRegex(
                                    RuntimeError,
                                    "above its ledger highwater",
                                ):
                                    recorder.assert_n16_execution_ready()
                            # The independently paired generation is checked
                            # before the more expensive permanent-graph walk.
                            self.assertEqual(validate_graph.call_count, 0)

    def test_read_only_select_does_not_invalidate_runtime_graph_baseline(self):
        from trading_bot.coverage_family_seal import (
            validate_family_seal_graph as real_validate_family_seal_graph,
        )
        from trading_bot.n19_analyzer import decode_n19_state_evidence

        for generation in ("CURRENT", "AUTHORIZED_LEGACY_V3"):
            with self.subTest(generation=generation):
                with tempfile.TemporaryDirectory() as directory:
                    database = Path(directory) / "review.sqlite3"
                    if generation == "CURRENT":
                        recorder = make_test_recorder(
                            database,
                            logging.getLogger("current-read-only-select"),
                        )
                    else:
                        database, ledger, evidence_sha = self._v3_fixture(
                            directory
                        )

                        def decode(evidence_json, *, expected_symbol):
                            if expected_symbol == AUTHORIZED_SYMBOL:
                                return self._mock_record(evidence_sha)
                            return decode_n19_state_evidence(
                                evidence_json,
                                expected_symbol=expected_symbol,
                            )

                        patcher = patch(
                            "trading_bot.n19_analyzer."
                            "decode_n19_state_evidence",
                            side_effect=decode,
                        )
                        patcher.start()
                        self.addCleanup(patcher.stop)
                        plan = inspect_authorized_legacy_v3_witness(
                            database, ledger
                        )
                        install_authorized_legacy_v3_witness(
                            database, ledger, plan.to_jsonable()
                        )
                        recorder = ReviewRecorder(
                            database,
                            logging.getLogger(
                                "authorized-read-only-select"
                            ),
                            n16_claim_ledger_file=ledger,
                        )
                    with closing(
                        sqlite3.connect(
                            database.as_uri() + "?mode=ro",
                            uri=True,
                        )
                    ) as external:
                        self.assertIsNotNone(
                            external.execute(
                                "SELECT current_scan_id FROM "
                                "strategy_signal_current "
                                "WHERE singleton_id=1"
                            ).fetchone()
                        )
                    with patch(
                        "trading_bot.coverage_family_seal."
                        "validate_family_seal_graph",
                        wraps=real_validate_family_seal_graph,
                    ) as validate_graph:
                        scan_id = recorder.begin_scan(
                            1, [], dry_run=True
                        )
                        self.assertIsInstance(
                            recorder.record_strategy_signal(
                                scan_id,
                                "N01",
                                "READONLYUSDT",
                                "",
                                (),
                                "",
                                False,
                                False,
                                "REJECTED",
                                "NO_MATCH",
                            ),
                            int,
                        )
                        self.assertTrue(
                            recorder.publish_strategy_signal_batch(
                                scan_id, 1
                            )
                        )
                        recorder.assert_n16_execution_ready()
                        self.assertEqual(validate_graph.call_count, 0)

    def test_all_runtime_connection_paths_attest_fd_before_first_sql(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database,
                logging.getLogger("runtime-fd-first"),
            )
            wrong_database = Path(directory) / "wrong.sqlite3"
            with closing(sqlite3.connect(wrong_database)) as wrong:
                wrong.execute("CREATE TABLE untouched(value TEXT)")
                wrong.commit()
            before = wrong_database.read_bytes()
            real_connect = sqlite3.connect

            def misdirected_connect(*_args, **_kwargs):
                return real_connect(
                    wrong_database.as_uri() + "?mode=rw",
                    uri=True,
                )

            with patch(
                "trading_bot.recorder.sqlite3.connect",
                side_effect=misdirected_connect,
            ):
                self.assertFalse(
                    recorder.record_event(
                        "ordinary_event", {}, "FDATTESTUSDT"
                    )
                )
                with self.assertRaisesRegex(
                    RuntimeError,
                    "descriptor is not attested",
                ):
                    with recorder._read_only_runtime_snapshot():
                        self.fail("misdirected snapshot was exposed")
            self.assertEqual(wrong_database.read_bytes(), before)
            self.assertFalse(
                Path(str(wrong_database) + "-wal").exists()
            )
            self.assertFalse(
                Path(str(wrong_database) + "-shm").exists()
            )

    def test_validated_generation_never_absorbs_post_validation_commit(self):
        from trading_bot.coverage_family_seal import (
            validate_family_seal_graph as real_validate_family_seal_graph,
        )
        from trading_bot.n19_analyzer import decode_n19_state_evidence

        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)

            def decode(evidence_json, *, expected_symbol):
                if expected_symbol == AUTHORIZED_SYMBOL:
                    return self._mock_record(evidence_sha)
                return decode_n19_state_evidence(
                    evidence_json,
                    expected_symbol=expected_symbol,
                )

            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                side_effect=decode,
            ):
                plan = inspect_authorized_legacy_v3_witness(database, ledger)
                install_authorized_legacy_v3_witness(
                    database, ledger, plan.to_jsonable()
                )
                recorder = ReviewRecorder(
                    database,
                    logging.getLogger("runtime-post-validation-commit"),
                    n16_claim_ledger_file=ledger,
                )
                with closing(sqlite3.connect(database)) as external:
                    external.execute(
                        "UPDATE history_coverage_protected_generation "
                        "SET generation=generation+1 WHERE singleton_id=1"
                    )
                    external.commit()

                def validate_then_inject(connection):
                    real_validate_family_seal_graph(connection)
                    proof_sha = "7" * 64
                    evidence_hash = "8" * 64
                    family_id = "3" * 24
                    structure_id = "4" * 24
                    with closing(sqlite3.connect(database)) as external:
                        external.create_function(
                            "_coverage_epoch_mutation_authorized",
                            6,
                            lambda *_args: 1,
                        )
                        external.execute(
                            "INSERT INTO history_coverage_n19_family_seals "
                            "VALUES (?,?,?,?,?,?,?,?,?)",
                            (
                                "N19",
                                "WINDOWUSDT",
                                family_id,
                                structure_id,
                                evidence_hash,
                                PROVED_TERMINAL_DOMAIN,
                                proof_sha,
                                family_seal_sha256(
                                    "WINDOWUSDT",
                                    family_id,
                                    structure_id,
                                    evidence_hash,
                                    PROVED_TERMINAL_DOMAIN,
                                    proof_sha,
                                ),
                                "2099-01-01T00:00:00+00:00",
                            ),
                        )
                        external.commit()

                with patch(
                    "trading_bot.coverage_family_seal."
                    "validate_family_seal_graph",
                    side_effect=validate_then_inject,
                ):
                    with closing(sqlite3.connect(database)) as external:
                        _advance_protected_generation_after_explicit_maintenance(
                            external,
                            N16PermanentClaimLedger(ledger),
                        )
                self.assertFalse(
                    recorder.record_event(
                        "ordinary_event", {}, "WINDOWUSDT"
                    )
                )
                for _ in range(2):
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "above its ledger highwater",
                    ):
                        recorder.assert_n16_execution_ready()

    def test_protected_generation_rollback_is_rejected_across_restart(self):
        from trading_bot.n19_analyzer import decode_n19_state_evidence

        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)

            def decode(evidence_json, *, expected_symbol):
                if expected_symbol == AUTHORIZED_SYMBOL:
                    return self._mock_record(evidence_sha)
                return decode_n19_state_evidence(
                    evidence_json,
                    expected_symbol=expected_symbol,
                )

            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                side_effect=decode,
            ):
                plan = inspect_authorized_legacy_v3_witness(database, ledger)
                install_authorized_legacy_v3_witness(
                    database, ledger, plan.to_jsonable()
                )
                recorder = ReviewRecorder(
                    database,
                    logging.getLogger("runtime-generation-rollback"),
                    n16_claim_ledger_file=ledger,
                )
                initial_pair = N16PermanentClaimLedger(
                    ledger
                ).protected_generation_highwater()
                self.assertIsNotNone(initial_pair)

                def backup(source, target):
                    Path(target).write_bytes(Path(source).read_bytes())

                def advance_pair():
                    with closing(sqlite3.connect(database)) as connection:
                        connection.execute(
                            "UPDATE history_coverage_protected_generation "
                            "SET generation=generation+1 "
                            "WHERE singleton_id=1"
                        )
                        connection.commit()
                        _advance_protected_generation_after_explicit_maintenance(
                            connection,
                            N16PermanentClaimLedger(ledger),
                        )

                base_review = Path(directory) / "base-review.sqlite3"
                base_ledger = Path(directory) / "base-ledger.sqlite3"
                advanced_review = Path(directory) / "advanced-review.sqlite3"
                advanced_ledger = Path(directory) / "advanced-ledger.sqlite3"
                backup(database, base_review)
                backup(ledger, base_ledger)
                advance_pair()
                advanced_pair = N16PermanentClaimLedger(
                    ledger
                ).protected_generation_highwater()
                self.assertEqual(advanced_pair[0], initial_pair[0] + 1)
                self.assertEqual(advanced_pair[1:], initial_pair[1:])
                recorder.assert_n16_execution_ready()
                backup(database, advanced_review)
                backup(ledger, advanced_ledger)

                with warnings.catch_warnings():
                    warnings.simplefilter("error", ResourceWarning)

                    # Restoring only the independent ledger must never be
                    # interpreted as an acknowledgement that can be repaired
                    # by ordinary startup or an execution gate.
                    backup(base_ledger, ledger)
                    for _ in range(2):
                        with self.assertRaisesRegex(
                            RuntimeError,
                            "above its ledger highwater",
                        ):
                            recorder.assert_n16_execution_ready()
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "above its ledger highwater",
                    ):
                        ReviewRecorder(
                            database,
                            logging.getLogger(
                                "runtime-ledger-only-rollback"
                            ),
                            n16_claim_ledger_file=ledger,
                        )
                    self.assertEqual(
                        N16PermanentClaimLedger(
                            ledger
                        ).protected_generation_highwater(),
                        initial_pair,
                    )

                    # Restoring only Review is the inverse mismatch and is
                    # rejected in the same process and after restart.
                    backup(advanced_ledger, ledger)
                    backup(base_review, database)
                    for _ in range(2):
                        with self.assertRaisesRegex(
                            RuntimeError,
                            "below its ledger highwater|rolled back",
                        ):
                            recorder.assert_n16_execution_ready()
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "below its ledger highwater",
                    ):
                        ReviewRecorder(
                            database,
                            logging.getLogger(
                                "runtime-review-only-rollback"
                            ),
                            n16_claim_ledger_file=ledger,
                        )

                    # An exact paired rollback remains invalid to a process
                    # that has observed the newer generation, but a fresh
                    # process can attest the restored pair and advance it
                    # again only through the explicit stopped-service path.
                    backup(base_ledger, ledger)
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "rolled back in this process",
                    ):
                        recorder.assert_n16_execution_ready()
                    paired = ReviewRecorder(
                        database,
                        logging.getLogger("runtime-paired-rollback"),
                        n16_claim_ledger_file=ledger,
                    )
                    paired.assert_n16_execution_ready()
                    advance_pair()
                    self.assertEqual(
                        N16PermanentClaimLedger(
                            ledger
                        ).protected_generation_highwater(),
                        advanced_pair,
                    )
                    republished = ReviewRecorder(
                        database,
                        logging.getLogger("runtime-paired-republished"),
                        n16_claim_ledger_file=ledger,
                    )
                    republished.assert_n16_execution_ready()

                    # Catalog/schema changes never hitchhike on a generation
                    # acknowledgement.
                    with closing(sqlite3.connect(database)) as external:
                        external.execute(
                            "CREATE TABLE hostile_generation_catalog(x)"
                        )
                        external.commit()
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "catalog changed outside maintenance|schema",
                    ):
                        ReviewRecorder(
                            database,
                            logging.getLogger(
                                "runtime-generation-catalog-mismatch"
                            ),
                            n16_claim_ledger_file=ledger,
                        )
                    del paired, republished
                    gc.collect()

    def test_authorized_generation_cannot_exist_without_witness_and_seal(self):
        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                install_authorized_legacy_v3_witness(
                    database, ledger, plan.to_jsonable()
                )
                with closing(sqlite3.connect(database)) as connection:
                    for trigger in (
                        "trg_history_coverage_n19_family_seal_no_delete",
                        "trg_history_coverage_n19_legacy_witness_no_delete",
                    ):
                        connection.execute(f'DROP TRIGGER "{trigger}"')
                    connection.execute(
                        "DELETE FROM history_coverage_n19_family_seals"
                    )
                    connection.execute(
                        "DELETE FROM "
                        "history_coverage_n19_legacy_unbound_witnesses"
                    )
                    connection.commit()
                    with self.assertRaisesRegex(
                        RuntimeError, "witness generation"
                    ):
                        validate_family_seal_graph(connection)
                with self.assertRaises(RuntimeError):
                    ReviewRecorder(
                        database,
                        logging.getLogger("empty-authorized-generation"),
                        n16_claim_ledger_file=ledger,
                    )

    def test_authorized_null_reset_has_one_sealed_successor(self):
        from trading_bot.n19_analyzer import N19StateRecord, _sha256_json

        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            cutoff = 1_900_010_000_000
            reset_time = cutoff + 900_000
            original_evidence = {
                "strategy_id": "N19",
                "symbol": AUTHORIZED_SYMBOL,
                "family_id": AUTHORIZED_FAMILY_ID,
                "structure_id": AUTHORIZED_STRUCTURE_ID,
                "stage": "MISSED",
                "reason": "N19_HISTORICAL_ENTRY_MISSED",
                "reset_after_time_ms": None,
                "terminal_cutoff_time_ms": cutoff,
                "source": [],
            }
            original_evidence["canonical_sha256"] = _sha256_json(
                original_evidence
            )
            original_record = SimpleNamespace(
                family_id=AUTHORIZED_FAMILY_ID,
                structure_id=AUTHORIZED_STRUCTURE_ID,
                stage="MISSED",
                reason="N19_HISTORICAL_ENTRY_MISSED",
                evidence_sha256=evidence_sha,
                evidence=original_evidence,
                reset_after_time_ms=None,
            )
            successor_evidence = dict(original_evidence)
            successor_evidence.pop("canonical_sha256")
            successor_evidence["reset_after_time_ms"] = reset_time
            successor_evidence["reset_evidence"] = {
                "mode": "DISCONNECTED_WINDOW_V1",
                "reset_open_time_ms": reset_time,
            }
            successor_evidence["canonical_sha256"] = _sha256_json(
                successor_evidence
            )
            successor = N19StateRecord(
                "N19",
                AUTHORIZED_SYMBOL,
                AUTHORIZED_FAMILY_ID,
                AUTHORIZED_STRUCTURE_ID,
                "MISSED",
                "N19_HISTORICAL_ENTRY_MISSED",
                1,
                1_900_000_000_000,
                cutoff,
                reset_time,
                successor_evidence,
            )

            def decode(evidence_json, *, expected_symbol):
                if evidence_json == "{}":
                    return original_record
                if evidence_json == successor.evidence_json:
                    return successor
                raise ValueError("unexpected evidence")

            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                side_effect=decode,
            ), patch(
                "trading_bot.n19_analyzer."
                "_record_with_validated_closed_entry",
                return_value=original_record,
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                install_authorized_legacy_v3_witness(
                    database, ledger, plan.to_jsonable()
                )
                recorder = ReviewRecorder(
                    database,
                    logging.getLogger("authorized-reset-successor"),
                    n16_claim_ledger_file=ledger,
                )
                self.assertEqual(
                    recorder.record_n19_state(successor), "UPDATED"
                )
                self.assertEqual(
                    recorder.record_n19_state(successor), "UNCHANGED"
                )
                with recorder._read_only_runtime_snapshot() as connection:
                    validate_family_seal_graph(connection)
                    self.assertEqual(
                        connection.execute(
                            "SELECT reset_after_time_ms FROM "
                            "n19_staircase_states WHERE family_id=?",
                            (AUTHORIZED_FAMILY_ID,),
                        ).fetchone(),
                        (reset_time,),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM "
                            "history_coverage_n19_legacy_reset_successors"
                        ).fetchone(),
                        (1,),
                    )

    def test_normal_and_legacy_terminal_domains_cross_reject_in_sql(self):
        terminal_values = (
            999,
            "N19",
            AUTHORIZED_SYMBOL,
            AUTHORIZED_FAMILY_ID,
            AUTHORIZED_STRUCTURE_ID,
            "{}",
            "a" * 64,
            1,
            "b" * 64,
            "c" * 64,
            1,
            1,
            "d" * 64,
            "e" * 64,
            "2099-01-01T00:00:00Z",
        )
        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                install_authorized_legacy_v3_witness(
                    database, ledger, plan.to_jsonable()
                )
            with closing(sqlite3.connect(database)) as connection:
                connection.create_function(
                    "_coverage_epoch_mutation_authorized",
                    6,
                    lambda *args: 1,
                )
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError,
                    "legacy proof|atomic bundle",
                ):
                    connection.execute(
                        "INSERT INTO "
                        "history_coverage_n19_terminal_receipts "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        terminal_values,
                    )
                connection.rollback()

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "review.sqlite3"
            recorder = make_test_recorder(
                database, logging.getLogger("cross-domain-normal")
            )
            with recorder._connect() as connection:
                connection.create_function(
                    "_coverage_epoch_mutation_authorized",
                    6,
                    lambda *args: 1,
                )
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "DROP TRIGGER "
                    "trg_history_coverage_n19_terminal_requires_bundle"
                )
                connection.execute(
                    "INSERT INTO history_coverage_n19_terminal_receipts "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    terminal_values,
                )
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError, "normal proof"
                ):
                    connection.execute(
                        "INSERT INTO "
                        "history_coverage_n19_legacy_unbound_witnesses "
                        "VALUES (1,'N19',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            AUTHORIZED_SYMBOL,
                            AUTHORIZED_FAMILY_ID,
                            AUTHORIZED_STRUCTURE_ID,
                            "MISSED",
                            "N19_HISTORICAL_ENTRY_MISSED",
                            "{}",
                            "a" * 64,
                            "b" * 64,
                            1,
                            "c" * 64,
                            "d" * 64,
                            "e" * 64,
                            "f" * 64,
                            "1" * 64,
                            "2dd9bf0e9a19d4464ff53ddb9bba35a850306499d12d88a34257541dcd0b04a1",
                            "V3_TERMINAL_PUBLICATION_BINDING_NOT_PERSISTED",
                            "2" * 64,
                            "2099-01-01T00:00:00Z",
                        ),
                    )
                connection.rollback()

    def test_authorized_generation_is_a_maintenance_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                install_authorized_legacy_v3_witness(
                    database, ledger, plan.to_jsonable()
                )
                claim_ledger = N16PermanentClaimLedger(ledger)
                with closing(
                    sqlite3.connect(
                        "file:%s?mode=ro" % database,
                        uri=True,
                    )
                ) as connection:
                    _attest_strategy_lifecycle_boundaries(
                        connection, claim_ledger
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT proof_domain FROM "
                            "history_coverage_n19_family_seals"
                        ).fetchone(),
                        (AUTHORIZED_LEGACY_DOMAIN,),
                    )

    def test_explicit_cli_inspect_and_install_require_exact_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "binance-only"
            root.mkdir()
            database, ledger, evidence_sha = self._v3_fixture(str(root))
            root = root.resolve()
            lock = root / "trading_bot.lock"
            lock.write_text("\n", encoding="utf-8")
            common = [
                "--db",
                str(database.resolve()),
                "--n16-claim-ledger",
                str(ledger.resolve()),
                "--binance-root",
                str(root),
                "--lock-file",
                str(lock),
            ]
            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                output = io.StringIO()
                with redirect_stdout(output):
                    self.assertEqual(
                        retention_main(
                            [
                                "--inspect-authorized-legacy-v3-witness",
                                *common,
                            ]
                        ),
                        0,
                    )
                inspected = json.loads(output.getvalue())
                plan = inspected
                cli_expected = []
                names = {
                    "symbol": "--expected-symbol",
                    "family_id": "--expected-family-id",
                    "structure_id": "--expected-structure-id",
                    "terminal_evidence_sha256": (
                        "--expected-terminal-evidence-sha256"
                    ),
                    "state_row_sha256": "--expected-state-row-sha256",
                    "receipt_count": "--expected-receipt-count",
                    "receipt_set_sha256": "--expected-receipt-set-sha256",
                    "coverage_graph_sha256": (
                        "--expected-coverage-graph-sha256"
                    ),
                    "coverage_catalog_sha256": (
                        "--expected-coverage-catalog-sha256"
                    ),
                    "review_canonical_sha256": (
                        "--expected-review-canonical-sha256"
                    ),
                    "authorization_sha256": "--authorization-sha256",
                    "review_plan_sha256": "--expected-review-plan-sha256",
                }
                for key, option in names.items():
                    cli_expected.extend((option, str(plan[key])))
                output = io.StringIO()
                with redirect_stdout(output):
                    self.assertEqual(
                        retention_main(
                            [
                                "--install-authorized-legacy-v3-witness",
                                *common,
                                *cli_expected,
                            ]
                        ),
                        0,
                    )
                self.assertEqual(
                    json.loads(output.getvalue())["resolution"],
                    "INSTALLED",
                )

    def test_family_schema_foreign_index_trigger_and_fk_drift_are_rejected(self):
        mutations = {
            "index": (
                "CREATE INDEX hostile_family_index ON "
                "history_coverage_n19_family_seals(structure_id)"
            ),
            "trigger": (
                "CREATE TRIGGER hostile_family_trigger AFTER INSERT ON "
                "history_coverage_n19_family_seals BEGIN SELECT 1; END"
            ),
            "incoming_fk": (
                "CREATE TABLE hostile_family_child("
                "strategy_id TEXT,symbol TEXT,family_id TEXT,"
                "FOREIGN KEY(strategy_id,symbol,family_id) REFERENCES "
                "history_coverage_n19_family_seals("
                "strategy_id,symbol,family_id))"
            ),
        }
        for mode, sql in mutations.items():
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                database, ledger, evidence_sha = self._v3_fixture(directory)
                with patch(
                    "trading_bot.n19_analyzer.decode_n19_state_evidence",
                    return_value=self._mock_record(evidence_sha),
                ):
                    plan = inspect_authorized_legacy_v3_witness(
                        database, ledger
                    )
                    install_authorized_legacy_v3_witness(
                        database, ledger, plan.to_jsonable()
                    )
                with closing(sqlite3.connect(database)) as connection:
                    connection.execute(sql)
                    connection.commit()
                with self.assertRaises(RuntimeError):
                    ReviewRecorder(
                        database,
                        logging.getLogger("family-schema-drift"),
                        n16_claim_ledger_file=ledger,
                    )

    def test_every_operator_expectation_is_prewrite_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                review_before = database.read_bytes()
                ledger_before = ledger.read_bytes()
                for key, original in plan.to_jsonable().items():
                    with self.subTest(key=key):
                        wrong = plan.to_jsonable()
                        wrong[key] = (
                            original + 1
                            if type(original) is int
                            else (
                                ("b" if original[:1] != "b" else "c")
                                + original[1:]
                            )
                        )
                        with self.assertRaises(
                            SignalRetentionMaintenanceError
                        ):
                            install_authorized_legacy_v3_witness(
                                database, ledger, wrong
                            )
                        self.assertEqual(database.read_bytes(), review_before)
                        self.assertEqual(ledger.read_bytes(), ledger_before)

    def test_operator_expectation_rejection_closes_simulation_connection(self):
        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                wrong = plan.to_jsonable()
                wrong["review_plan_sha256"] = "f" * 64
                with warnings.catch_warnings():
                    warnings.simplefilter("error", ResourceWarning)
                    with self.assertRaises(
                        SignalRetentionMaintenanceError
                    ):
                        install_authorized_legacy_v3_witness(
                            database, ledger, wrong
                        )
                    gc.collect()

    def test_open_execution_and_second_candidate_are_rejected(self):
        for mode in ("paper", "second"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                database, ledger, evidence_sha = self._v3_fixture(directory)
                with closing(sqlite3.connect(database)) as connection:
                    if mode == "paper":
                        connection.execute(
                            "INSERT INTO strategy_paper_trades("
                            "strategy_id,symbol,opened_at,last_checked_at,"
                            "closed_at,entry_price,stop_loss_price,"
                            "take_profit_price,result,exit_reason,r_multiple,"
                            "funding_rate,orders_json,detail_json) VALUES "
                            "('N19',?,'x',NULL,NULL,'1','0.9','1.5',"
                            "'OPEN',NULL,NULL,'0','{}','{}')",
                            (AUTHORIZED_SYMBOL,),
                        )
                    else:
                        connection.execute(
                            "INSERT INTO n19_staircase_states("
                            "strategy_id,symbol,family_id,structure_id,stage,"
                            "reason,quote_volume_rank,s_open_time_ms,"
                            "x_open_time_ms,reset_after_time_ms,evidence_json,"
                            "evidence_sha256,created_at,updated_at) VALUES "
                            "('N19','OTHERUSDT',?,?,?,?,2,?,?,NULL,'{}',"
                            "?, 'x','x')",
                            (
                                "b" * 24,
                                "c" * 24,
                                "MISSED",
                                "N19_HISTORICAL_ENTRY_MISSED",
                                1_900_020_000_000,
                                1_900_030_000_000,
                                "d" * 64,
                            ),
                        )
                    connection.commit()
                with patch(
                    "trading_bot.n19_analyzer.decode_n19_state_evidence",
                    return_value=self._mock_record(evidence_sha),
                ):
                    with self.assertRaises(
                        SignalRetentionMaintenanceError
                    ):
                        inspect_authorized_legacy_v3_witness(
                            database, ledger
                        )

    def test_install_uses_three_bounded_snapshots_and_file_simulation(self):
        from trading_bot.coverage_family_seal import (
            review_snapshot_digests,
        )

        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            plan = None
            stages = []
            calls = []

            def counted_snapshot(connection):
                calls.append(connection.execute("PRAGMA query_only").fetchone())
                return review_snapshot_digests(connection)

            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                with patch(
                    "trading_bot.coverage_family_seal."
                    "review_snapshot_digests",
                    side_effect=counted_snapshot,
                ):
                    install_authorized_legacy_v3_witness(
                        database,
                        ledger,
                        plan.to_jsonable(),
                        progress_callback=lambda stage, elapsed: stages.append(
                            (stage, elapsed)
                        ),
                    )
            self.assertEqual(len(calls), 3)
            self.assertEqual(
                [stage for stage, _elapsed in stages],
                [
                    "SOURCE_ATTESTATION_STARTED",
                    "FILE_SIMULATION_BACKUP_STARTED",
                    "FILE_SIMULATION_APPLY_STARTED",
                    "FILE_SIMULATION_COMPLETED",
                    "LEDGER_PREPARED",
                    "REVIEW_BASE_REATTESTATION_STARTED",
                    "REVIEW_APPLY_STARTED",
                    "REVIEW_COMMITTED",
                    "LEDGER_COMMITTED",
                ],
            )
            self.assertEqual(stages, sorted(stages, key=lambda item: item[1]))
            self.assertFalse(
                any(
                    path.name.startswith(
                        ".authorized-legacy-review-simulation-"
                    )
                    for path in Path(tempfile.gettempdir()).iterdir()
                )
            )

    def test_file_simulation_directory_replacement_preserves_external_tree(self):
        import trading_bot.signal_retention as retention

        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            root = Path(directory)
            plan = None
            original_assert = retention._assert_anchored_directory_name
            calls = [0]
            replacement = [None]
            moved = [None]
            original_cwd = os.getcwd()
            descriptors_before = (
                retention._maintenance_regular_fd_snapshot()
            )

            def replace_before_second_attestation(
                parent_descriptor,
                stage_name,
                expected_identity,
            ):
                calls[0] += 1
                if calls[0] == 2:
                    replacement[0] = root / stage_name
                    moved[0] = root / (stage_name + "-moved")
                    os.rename(replacement[0], moved[0])
                    replacement[0].mkdir(mode=0o700)
                    (replacement[0] / "external-marker.txt").write_text(
                        "external",
                        encoding="utf-8",
                    )
                return original_assert(
                    parent_descriptor,
                    stage_name,
                    expected_identity,
                )

            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                with patch.object(
                    retention.tempfile,
                    "gettempdir",
                    return_value=directory,
                ), patch.object(
                    retention,
                    "_assert_anchored_directory_name",
                    side_effect=replace_before_second_attestation,
                ):
                    with self.assertRaisesRegex(
                        SignalRetentionMaintenanceError,
                        "simulation identity changed",
                    ):
                        install_authorized_legacy_v3_witness(
                            database,
                            ledger,
                            plan.to_jsonable(),
                        )
            self.assertEqual(os.getcwd(), original_cwd)
            self.assertEqual(
                retention._maintenance_regular_fd_snapshot(),
                descriptors_before,
            )
            self.assertIsNotNone(replacement[0])
            self.assertTrue(replacement[0].is_dir())
            self.assertEqual(
                (replacement[0] / "external-marker.txt").read_text(
                    encoding="utf-8"
                ),
                "external",
            )
            self.assertFalse(moved[0].exists())
            self.assertFalse(
                any(
                    path.name.startswith(
                        ".authorized-legacy-review-simulation-"
                    )
                    and path != replacement[0]
                    for path in root.iterdir()
                )
            )
            self.assertIsNone(
                N16PermanentClaimLedger(ledger).legacy_witness_mirror()
            )

    def test_file_simulation_cross_parent_move_removes_original_stage(self):
        import trading_bot.signal_retention as retention

        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            root = Path(directory)
            relocated = root / "relocated"
            relocated.mkdir(mode=0o700)
            original_assert = retention._assert_anchored_directory_name
            calls = [0]
            replacement = [None]
            moved = [None]
            original_cwd = os.getcwd()
            descriptors_before = (
                retention._maintenance_regular_fd_snapshot()
            )

            def relocate_before_second_attestation(
                parent_descriptor,
                stage_name,
                expected_identity,
            ):
                calls[0] += 1
                if calls[0] == 2:
                    replacement[0] = root / stage_name
                    moved[0] = relocated / stage_name
                    os.rename(replacement[0], moved[0])
                    replacement[0].mkdir(mode=0o700)
                    (replacement[0] / "external-marker.txt").write_text(
                        "external",
                        encoding="utf-8",
                    )
                return original_assert(
                    parent_descriptor,
                    stage_name,
                    expected_identity,
                )

            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                with patch.object(
                    retention.tempfile,
                    "gettempdir",
                    return_value=directory,
                ), patch.object(
                    retention,
                    "_assert_anchored_directory_name",
                    side_effect=relocate_before_second_attestation,
                ):
                    with self.assertRaisesRegex(
                        SignalRetentionMaintenanceError,
                        "simulation identity changed",
                    ):
                        install_authorized_legacy_v3_witness(
                            database,
                            ledger,
                            plan.to_jsonable(),
                        )
            self.assertEqual(os.getcwd(), original_cwd)
            self.assertEqual(
                retention._maintenance_regular_fd_snapshot(),
                descriptors_before,
            )
            self.assertTrue(replacement[0].is_dir())
            self.assertEqual(
                (replacement[0] / "external-marker.txt").read_text(
                    encoding="utf-8"
                ),
                "external",
            )
            self.assertFalse(moved[0].exists())
            self.assertEqual(list(relocated.iterdir()), [])
            self.assertIsNone(
                N16PermanentClaimLedger(ledger).legacy_witness_mirror()
            )

    def test_file_simulation_cleanup_retries_a_second_cross_parent_move(self):
        import trading_bot.signal_retention as retention

        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            root = Path(directory)
            first_parent = root / "relocated-first"
            second_parent = root / "relocated-second"
            first_parent.mkdir(mode=0o700)
            second_parent.mkdir(mode=0o700)
            original_assert = retention._assert_anchored_directory_name
            original_find = retention._find_anchored_directory_name
            attestations = [0]
            finds = [0]
            stage_name_ref = [None]
            replacement = [None]
            original_cwd = os.getcwd()
            descriptors_before = (
                retention._maintenance_regular_fd_snapshot()
            )

            def relocate_before_second_attestation(
                parent_descriptor,
                stage_name,
                expected_identity,
            ):
                attestations[0] += 1
                if attestations[0] == 2:
                    stage_name_ref[0] = stage_name
                    replacement[0] = root / stage_name
                    os.rename(
                        replacement[0],
                        first_parent / stage_name,
                    )
                    replacement[0].mkdir(mode=0o700)
                    (replacement[0] / "external-marker.txt").write_text(
                        "external",
                        encoding="utf-8",
                    )
                return original_assert(
                    parent_descriptor,
                    stage_name,
                    expected_identity,
                )

            def move_again_during_cleanup(
                parent_descriptor,
                expected_identity,
            ):
                finds[0] += 1
                if finds[0] == 1:
                    os.rename(
                        first_parent / stage_name_ref[0],
                        second_parent / stage_name_ref[0],
                    )
                return original_find(
                    parent_descriptor,
                    expected_identity,
                )

            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                with patch.object(
                    retention.tempfile,
                    "gettempdir",
                    return_value=directory,
                ), patch.object(
                    retention,
                    "_assert_anchored_directory_name",
                    side_effect=relocate_before_second_attestation,
                ), patch.object(
                    retention,
                    "_find_anchored_directory_name",
                    side_effect=move_again_during_cleanup,
                ):
                    with self.assertRaisesRegex(
                        SignalRetentionMaintenanceError,
                        "simulation identity changed",
                    ):
                        install_authorized_legacy_v3_witness(
                            database,
                            ledger,
                            plan.to_jsonable(),
                        )
            self.assertGreaterEqual(finds[0], 2)
            self.assertEqual(os.getcwd(), original_cwd)
            self.assertEqual(
                retention._maintenance_regular_fd_snapshot(),
                descriptors_before,
            )
            self.assertTrue(replacement[0].is_dir())
            self.assertEqual(
                (replacement[0] / "external-marker.txt").read_text(
                    encoding="utf-8"
                ),
                "external",
            )
            self.assertEqual(list(first_parent.iterdir()), [])
            self.assertEqual(list(second_parent.iterdir()), [])
            self.assertIsNone(
                N16PermanentClaimLedger(ledger).legacy_witness_mirror()
            )

    def test_file_simulation_rmdir_ack_loss_is_confirmed_without_residue(self):
        import trading_bot.signal_retention as retention

        with tempfile.TemporaryDirectory() as directory:
            database, ledger, evidence_sha = self._v3_fixture(directory)
            root = Path(directory)
            original_rmdir = retention.os.rmdir
            original_assert = retention._assert_anchored_directory_name
            injected = [False]
            attestations = [0]
            original_cwd = os.getcwd()
            descriptors_before = (
                retention._maintenance_regular_fd_snapshot()
            )

            def fail_second_attestation(
                parent_descriptor,
                stage_name,
                expected_identity,
            ):
                attestations[0] += 1
                original_assert(
                    parent_descriptor,
                    stage_name,
                    expected_identity,
                )
                if attestations[0] == 2:
                    raise SignalRetentionMaintenanceError(
                        "authorized legacy simulation identity changed"
                    )

            def rmdir_with_ack_loss(path, *args, **kwargs):
                if (
                    not injected[0]
                    and type(path) is str
                    and path.startswith(
                        ".authorized-legacy-review-simulation-"
                    )
                ):
                    original_rmdir(path, *args, **kwargs)
                    injected[0] = True
                    raise OSError("simulated rmdir acknowledgement loss")
                return original_rmdir(path, *args, **kwargs)

            with patch(
                "trading_bot.n19_analyzer.decode_n19_state_evidence",
                return_value=self._mock_record(evidence_sha),
            ):
                plan = inspect_authorized_legacy_v3_witness(
                    database, ledger
                )
                with patch.object(
                    retention.tempfile,
                    "gettempdir",
                    return_value=directory,
                ), patch.object(
                    retention,
                    "_assert_anchored_directory_name",
                    side_effect=fail_second_attestation,
                ), patch.object(
                    retention.os,
                    "rmdir",
                    side_effect=rmdir_with_ack_loss,
                ):
                    with self.assertRaisesRegex(
                        SignalRetentionMaintenanceError,
                        "simulation identity changed",
                    ):
                        install_authorized_legacy_v3_witness(
                            database,
                            ledger,
                            plan.to_jsonable(),
                        )
            self.assertTrue(injected[0])
            self.assertEqual(os.getcwd(), original_cwd)
            self.assertEqual(
                retention._maintenance_regular_fd_snapshot(),
                descriptors_before,
            )
            self.assertFalse(
                any(
                    path.name.startswith(
                        ".authorized-legacy-review-simulation-"
                    )
                    for path in root.iterdir()
                )
            )
            self.assertIsNone(
                N16PermanentClaimLedger(ledger).legacy_witness_mirror()
            )

    def test_snapshot_spools_are_anonymous_and_close_on_merge_failure(self):
        import trading_bot.coverage_family_seal as family

        with tempfile.TemporaryDirectory() as directory:
            database, _ledger, _evidence_sha = self._v3_fixture(directory)
            opened = []
            original_temporary_file = family.tempfile.TemporaryFile

            def tracked_temporary_file(*args, **kwargs):
                stream = original_temporary_file(*args, **kwargs)
                opened.append(stream)
                return stream

            with closing(sqlite3.connect(database)) as connection, patch.object(
                family.tempfile,
                "TemporaryDirectory",
                side_effect=AssertionError("path spool is forbidden"),
            ), patch.object(
                family.tempfile,
                "TemporaryFile",
                side_effect=tracked_temporary_file,
            ), patch.object(
                family,
                "_iter_snapshot_digest_chunk",
                side_effect=OSError("merge failed"),
            ):
                with self.assertRaisesRegex(OSError, "merge failed"):
                    family.review_snapshot_digests(connection)
            self.assertTrue(opened)
            self.assertTrue(all(stream.closed for stream in opened))
            self.assertTrue(
                all(type(stream.name) is int for stream in opened)
            )

    def test_streaming_review_snapshot_is_stable_and_type_sensitive(self):
        from trading_bot.coverage_family_seal import (
            review_snapshot_digests,
        )

        with tempfile.TemporaryDirectory() as directory:
            database, _ledger, _evidence_sha = self._v3_fixture(directory)
            with closing(sqlite3.connect(database)) as connection:
                first = review_snapshot_digests(connection)
                second = review_snapshot_digests(connection)
                self.assertEqual(first, second)
                connection.execute(
                    "INSERT INTO events(occurred_at,event_type,symbol,"
                    "payload_json,trade_review_id) VALUES "
                    "('2099-01-01T00:00:00+00:00','snapshot',"
                    "'DIGESTUSDT','{}',NULL)"
                )
                connection.commit()
                changed = review_snapshot_digests(connection)
            self.assertNotEqual(
                first.full_snapshot_sha256,
                changed.full_snapshot_sha256,
            )
            self.assertNotEqual(
                first.canonical_sha256,
                changed.canonical_sha256,
            )


if __name__ == "__main__":
    unittest.main()
