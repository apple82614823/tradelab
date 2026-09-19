from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from datetime import datetime, timedelta, timezone
import json
import logging
import io
import sqlite3
import tempfile
import threading
import unittest
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from tests.recorder_test_utils import make_test_recorder, test_claim_ledger_path
from trading_bot.analyzer import AnalysisResult
from trading_bot.instance_lock import InstanceLock
from trading_bot.monitor import FundingCandidate
from trading_bot.recorder import PAPER_TRADE_VOID_CONFIRMATION, ReviewRecorder
from trading_bot.signal_retention import SignalRetentionMaintenanceError
from trading_bot.state import PositionState
from trading_bot.strategies import load_first_stage_strategies
from trading_bot.void_paper_trade import main as void_paper_trade_main


@contextmanager
def _sqlite_connection(*args, **kwargs):
    connection = sqlite3.connect(*args, **kwargs)
    try:
        with connection:
            yield connection
    finally:
        connection.close()


class RecorderTests(unittest.TestCase):
    def _bare_recorder(self, db_file):
        return make_test_recorder(
            str(db_file),
            logging.getLogger("test_recorder_connection_lifecycle"),
        )

    def _pending_live_finalization_fixture(self, tmpdir):
        recorder = make_test_recorder(
            str(Path(tmpdir) / "review.sqlite3"),
            logging.getLogger("test_atomic_live_finalization"),
        )
        state = PositionState(
            symbol="DODOXUSDT",
            quantity="10",
            entry_price="100",
            stop_loss_price="95",
            take_profit_price="125",
            leverage=10,
            opened_at="2026-07-13T00:00:00+00:00",
            dry_run=False,
            orders={
                "plan": {"planned_entry": "100", "risk": {"amount": "10"}},
                "open_audit": {"market_order_id": "open-1"},
                "strategy": {"strategy_id": "N01"},
            },
        )
        trade_id = recorder.record_trade_open(None, state)
        self.assertIsNotNone(trade_id)
        self.assertIsNotNone(
            recorder.record_strategy_live_open(
                "N01", trade_id, state.symbol, state.opened_at
            )
        )
        self.assertEqual(
            recorder.record_trade_close(
                state,
                "LIVE_RESULT_PENDING",
                "",
                "",
                "",
                "",
                "",
                {"reason": "PROTECTION_ORDER_QUERY_FAILED"},
            ),
            trade_id,
        )
        self.assertTrue(
            recorder.mark_strategy_live_result_pending(
                "N01", "PROTECTION_ORDER_QUERY_FAILED"
            )
        )
        return recorder, state, trade_id

    def _live_open_claim_state(self):
        return PositionState(
            symbol="CLAIMUSDT",
            quantity="10",
            entry_price="100",
            stop_loss_price="95",
            take_profit_price="125",
            leverage=10,
            opened_at="2026-07-13T02:00:00+00:00",
            dry_run=False,
            orders={
                "plan": {
                    "risk_amount": "10",
                    "executed_quantity": "10",
                    "final_protected_quantity": "10",
                },
                "open": {
                    "orderId": 7001,
                    "clientOrderId": "mkt-claim",
                    "symbol": "CLAIMUSDT",
                    "side": "BUY",
                    "type": "MARKET",
                    "status": "FILLED",
                    "executedQty": "10",
                },
                "stop": {
                    "algoId": 7002,
                    "clientAlgoId": "sl-claim",
                    "symbol": "CLAIMUSDT",
                    "algoType": "CONDITIONAL",
                    "side": "SELL",
                    "orderType": "STOP_MARKET",
                    "closePosition": True,
                    "algoStatus": "NEW",
                },
                "take_profit": {
                    "algoId": 7003,
                    "clientAlgoId": "tp-claim",
                    "symbol": "CLAIMUSDT",
                    "algoType": "CONDITIONAL",
                    "side": "SELL",
                    "orderType": "TAKE_PROFIT_MARKET",
                    "closePosition": True,
                    "algoStatus": "NEW",
                },
                "strategy": {"strategy_id": "N01", "structure_id": "claim-1"},
            },
        )

    def _registered_claim_recorder(self, tmpdir, logger_name):
        recorder = make_test_recorder(
            str(Path(tmpdir) / "review.sqlite3"),
            logging.getLogger(logger_name),
        )
        recorder.upsert_strategy_definitions((load_first_stage_strategies()[0],))
        return recorder

    def test_connect_commits_and_closes_on_success(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = Path(tmpdir) / "review.sqlite3"
            original_connect = sqlite3.connect
            connections = []

            class TrackingConnection(sqlite3.Connection):
                def close(self):
                    self.close_called = True
                    return super().close()

            def tracking_connect(path, *args, **kwargs):
                kwargs["factory"] = TrackingConnection
                connection = original_connect(path, *args, **kwargs)
                connection.close_called = False
                connections.append(connection)
                return connection

            recorder = self._bare_recorder(db_file)
            with patch("trading_bot.recorder.sqlite3.connect", side_effect=tracking_connect):
                with recorder._connect() as connection:
                    connection.execute(
                        "INSERT INTO events "
                        "(occurred_at,event_type,symbol,payload_json) "
                        "VALUES ('2026-08-13T00:00:00+00:00',"
                        "'connection_lifecycle',NULL,'{}')"
                    )

            self.assertGreaterEqual(len(connections), 1)
            self.assertTrue(all(connection.close_called for connection in connections))
            with _sqlite_connection(db_file) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT event_type FROM events "
                        "WHERE event_type='connection_lifecycle'"
                    ).fetchall(),
                    [("connection_lifecycle",)],
                )

    def test_connect_serializes_full_connection_lifecycle(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_recorder_serialized_connections"),
            )
            first_entered = threading.Event()
            release_first = threading.Event()
            second_started = threading.Event()
            second_entered = threading.Event()
            errors = []

            def first_worker():
                try:
                    with recorder._connect() as connection:
                        connection.execute("SELECT 1").fetchone()
                        first_entered.set()
                        if not release_first.wait(5):
                            raise RuntimeError("first connection release timed out")
                except Exception as exc:  # pragma: no cover - reported below
                    errors.append(exc)

            def second_worker():
                try:
                    second_started.set()
                    with recorder._connect() as connection:
                        connection.execute("SELECT 1").fetchone()
                        second_entered.set()
                except Exception as exc:  # pragma: no cover - reported below
                    errors.append(exc)

            first = threading.Thread(target=first_worker)
            second = threading.Thread(target=second_worker)
            first.start()
            self.assertTrue(first_entered.wait(5))
            second.start()
            self.assertTrue(second_started.wait(5))
            self.assertFalse(second_entered.wait(0.1))
            release_first.set()
            first.join(5)
            second.join(5)
            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
            self.assertTrue(second_entered.is_set())
            self.assertEqual(errors, [])

    def test_live_open_claim_recovers_both_pre_audit_windows_idempotently(self):
        state = self._live_open_claim_state()
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._registered_claim_recorder(
                tmpdir, "test_live_open_claim_zero_rows"
            )
            first = recorder.claim_strategy_live_open_audit(state, "N01")
            replay = recorder.claim_strategy_live_open_audit(state, "N01")
            self.assertIsNotNone(first)
            self.assertIsNotNone(replay)
            self.assertTrue(first.review_created)
            self.assertTrue(first.link_created)
            self.assertFalse(replay.review_created)
            self.assertFalse(replay.link_created)
            self.assertEqual(replay.trade_review_id, first.trade_review_id)
            with recorder._connect() as connection:
                reviews = connection.execute(
                    "SELECT id, status, orders_json FROM trade_reviews"
                ).fetchall()
                links = connection.execute(
                    "SELECT trade_review_id, closed_at, result FROM strategy_live_links"
                ).fetchall()
            self.assertEqual(len(reviews), 1)
            self.assertEqual(reviews[0][0], first.trade_review_id)
            self.assertEqual(reviews[0][1], "OPENED")
            self.assertEqual(json.loads(reviews[0][2]), {"state_orders": state.orders})
            self.assertEqual(links, [(first.trade_review_id, None, None)])

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._registered_claim_recorder(
                tmpdir, "test_live_open_claim_review_only"
            )
            original_trade_id = recorder.record_trade_open(None, state)
            claim = recorder.claim_strategy_live_open_audit(state, "N01")
            self.assertIsNotNone(claim)
            self.assertEqual(claim.trade_review_id, original_trade_id)
            self.assertFalse(claim.review_created)
            self.assertTrue(claim.link_created)
            with recorder._connect() as connection:
                review_count = connection.execute(
                    "SELECT COUNT(*) FROM trade_reviews"
                ).fetchone()[0]
                links = connection.execute(
                    "SELECT trade_review_id FROM strategy_live_links"
                ).fetchall()
            self.assertEqual(review_count, 1)
            self.assertEqual(links, [(original_trade_id,)])

    def test_live_open_claim_conflicts_and_link_failure_roll_back(self):
        state = self._live_open_claim_state()
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._registered_claim_recorder(
                tmpdir, "test_live_open_claim_rollback"
            )
            with recorder._connect() as connection:
                connection.execute(
                    """
                    CREATE TEMP TRIGGER fail_live_open_claim_link
                    BEFORE INSERT ON strategy_live_links
                    BEGIN
                        SELECT RAISE(ABORT, 'forced live-link failure');
                    END
                    """
                )

                @contextmanager
                def reused_connection():
                    try:
                        yield connection
                    except Exception:
                        connection.rollback()
                        raise

                with patch.object(
                    recorder, "_connect", side_effect=reused_connection
                ):
                    self.assertIsNone(
                        recorder.claim_strategy_live_open_audit(state, "N01")
                    )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM trade_reviews").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM strategy_live_links").fetchone()[0],
                    0,
                )

        for conflict in (
            "wrong_orders",
            "wrong_scalar",
            "terminal_review",
            "null_link",
            "wrong_link",
            "closed_link",
            "multiple_links",
            "multiple_reviews",
        ):
            with self.subTest(conflict=conflict), tempfile.TemporaryDirectory() as tmpdir:
                recorder = self._registered_claim_recorder(
                    tmpdir, "test_live_open_claim_conflict"
                )
                trade_id = recorder.record_trade_open(None, state)
                with recorder._connect() as connection:
                    if conflict == "wrong_orders":
                        connection.execute(
                            "UPDATE trade_reviews SET orders_json = ? WHERE id = ?",
                            (json.dumps({"state_orders": {"open": {"orderId": 9999}}}), trade_id),
                        )
                    elif conflict == "wrong_scalar":
                        connection.execute(
                            "UPDATE trade_reviews SET quantity = '11' WHERE id = ?",
                            (trade_id,),
                        )
                    elif conflict == "terminal_review":
                        connection.execute(
                            """
                            UPDATE trade_reviews
                            SET status = 'CLOSED_STOP_LOSS', closed_at = ?,
                                exit_reason = 'STOP_LOSS', exit_price = '95'
                            WHERE id = ?
                            """,
                            ("2026-07-13T02:01:00+00:00", trade_id),
                        )
                    elif conflict == "null_link":
                        connection.execute(
                            """
                            INSERT INTO strategy_live_links (
                                strategy_id, trade_review_id, symbol, opened_at, created_at
                            ) VALUES ('N01', NULL, ?, ?, ?)
                            """,
                            (state.symbol, state.opened_at, "2026-07-13T02:00:01+00:00"),
                        )
                    elif conflict == "wrong_link":
                        other_review = connection.execute(
                            """
                            INSERT INTO trade_reviews (
                                opened_at, symbol, side, dry_run, status, orders_json
                            ) VALUES ('2026-07-13T01:00:00+00:00', 'OTHERUSDT',
                                      'BUY', 0, 'OPENED', '{}')
                            """
                        )
                        connection.execute(
                            """
                            INSERT INTO strategy_live_links (
                                strategy_id, trade_review_id, symbol, opened_at, created_at
                            ) VALUES ('N01', ?, ?, ?, ?)
                            """,
                            (
                                int(other_review.lastrowid),
                                state.symbol,
                                state.opened_at,
                                "2026-07-13T02:00:01+00:00",
                            ),
                        )
                    elif conflict == "closed_link":
                        connection.execute(
                            """
                            INSERT INTO strategy_live_links (
                                strategy_id, trade_review_id, symbol, opened_at,
                                closed_at, result, created_at
                            ) VALUES ('N01', ?, ?, ?, ?, 'LOSS', ?)
                            """,
                            (
                                trade_id,
                                state.symbol,
                                state.opened_at,
                                "2026-07-13T02:01:00+00:00",
                                "2026-07-13T02:00:01+00:00",
                            ),
                        )
                    elif conflict == "multiple_links":
                        for second in (1, 2):
                            connection.execute(
                                """
                                INSERT INTO strategy_live_links (
                                    strategy_id, trade_review_id, symbol, opened_at, created_at
                                ) VALUES ('N01', ?, ?, ?, ?)
                                """,
                                (
                                    trade_id,
                                    state.symbol,
                                    state.opened_at,
                                    f"2026-07-13T02:00:0{second}+00:00",
                                ),
                            )
                    else:
                        connection.execute(
                            """
                            INSERT INTO trade_reviews (
                                opened_at, symbol, side, dry_run, status, orders_json
                            ) VALUES (?, ?, 'BUY', 0, 'OPENED', '{}')
                            """,
                            (state.opened_at, state.symbol),
                        )
                self.assertIsNone(recorder.claim_strategy_live_open_audit(state, "N01"))
                with recorder._connect() as connection:
                    expected_links = 2 if conflict == "multiple_links" else (
                        1
                        if conflict in {
                            "null_link", "wrong_link", "closed_link"
                        }
                        else 0
                    )
                    self.assertEqual(
                        connection.execute("SELECT COUNT(*) FROM strategy_live_links").fetchone()[0],
                        expected_links,
                    )

    def test_live_open_claim_accepts_trader_recovery_reduction_and_raw_protection_shapes(self):
        base = self._live_open_claim_state()
        valid_variants = []

        recovered_orders = deepcopy(base.orders)
        recovered_orders["open"].pop("orderId")
        recovered_orders["open"]["executionRecoveredFromPosition"] = True
        valid_variants.append(("position_recovered", replace(base, orders=recovered_orders)))

        reduced_orders = deepcopy(base.orders)
        reduced_orders["open"]["executedQty"] = "12"
        reduced_orders["plan"]["executed_quantity"] = "12"
        reduced_orders["plan"]["final_protected_quantity"] = "10"
        valid_variants.append(("post_fill_reduction", replace(base, orders=reduced_orders)))

        raw_protection_orders = deepcopy(base.orders)
        for role in ("stop", "take_profit"):
            raw_protection_orders[role].pop("algoId")
            raw_protection_orders[role].pop("algoStatus")
        valid_variants.append(
            ("raw_protection_without_id_status", replace(base, orders=raw_protection_orders))
        )

        for name, state in valid_variants:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                recorder = self._registered_claim_recorder(
                    tmpdir, f"test_live_open_claim_valid_{name}"
                )
                self.assertIsNotNone(
                    recorder.claim_strategy_live_open_audit(state, "N01")
                )

    def test_live_open_claim_rejects_invalid_state_unregistered_and_global_link_conflict(self):
        base = self._live_open_claim_state()

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_live_open_claim_unregistered"),
            )
            self.assertIsNone(recorder.claim_strategy_live_open_audit(base, "N01"))
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM trade_reviews").fetchone()[0], 0
                )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM strategy_live_links").fetchone()[0], 0
                )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM strategy_states").fetchone()[0], 0
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_live_open_claim_missing_strategy_state"),
            )
            with recorder._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO strategy_definitions (
                        strategy_id, name, enabled, config_json, created_at, updated_at
                    ) VALUES ('N01', 'N01', 1, '{}', ?, ?)
                    """,
                    (
                        "2026-07-13T02:00:00+00:00",
                        "2026-07-13T02:00:00+00:00",
                    ),
                )
            self.assertIsNone(recorder.claim_strategy_live_open_audit(base, "N01"))
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM trade_reviews").fetchone()[0], 0
                )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM strategy_states").fetchone()[0], 0
                )

        invalid_variants = []
        invalid_variants.append(("dry_run", replace(base, dry_run=True)))
        invalid_variants.append(("nan_quantity", replace(base, quantity="NaN")))
        invalid_variants.append(("price_order", replace(base, stop_loss_price="100")))
        invalid_variants.append(
            ("noncanonical_time", replace(base, opened_at="2026-07-13T10:00:00+08:00"))
        )

        missing_recovery_flag = deepcopy(base.orders)
        missing_recovery_flag["open"].pop("orderId")
        invalid_variants.append(
            ("missing_order_id_without_recovery", replace(base, orders=missing_recovery_flag))
        )
        mismatched_quantity = deepcopy(base.orders)
        mismatched_quantity["open"]["executedQty"] = "9"
        invalid_variants.append(
            ("open_quantity_below_state", replace(base, orders=mismatched_quantity))
        )
        wrong_plan_execution = deepcopy(base.orders)
        wrong_plan_execution["open"]["executedQty"] = "12"
        wrong_plan_execution["plan"]["executed_quantity"] = "11"
        invalid_variants.append(
            ("plan_execution_mismatch", replace(base, orders=wrong_plan_execution))
        )
        invalid_algo_id = deepcopy(base.orders)
        invalid_algo_id["stop"]["algoId"] = "00"
        invalid_variants.append(("invalid_algo_id", replace(base, orders=invalid_algo_id)))
        rejected_protection = deepcopy(base.orders)
        rejected_protection["take_profit"]["algoStatus"] = "EXPIRED"
        invalid_variants.append(
            ("rejected_protection", replace(base, orders=rejected_protection))
        )
        invalid_open_status = deepcopy(base.orders)
        invalid_open_status["open"]["status"] = "NEW"
        invalid_variants.append(
            ("invalid_open_status", replace(base, orders=invalid_open_status))
        )
        empty_plan = deepcopy(base.orders)
        empty_plan["plan"] = {}
        invalid_variants.append(("empty_plan", replace(base, orders=empty_plan)))
        pending_orders = deepcopy(base.orders)
        pending_orders["execution_pending"] = {"phase": "UNKNOWN"}
        invalid_variants.append(("execution_pending", replace(base, orders=pending_orders)))
        unicode_client = deepcopy(base.orders)
        unicode_client["open"]["clientOrderId"] = "订单-1"
        invalid_variants.append(("unicode_client", replace(base, orders=unicode_client)))
        overlong_client = deepcopy(base.orders)
        overlong_client["stop"]["clientAlgoId"] = "s" * 37
        invalid_variants.append(("overlong_client", replace(base, orders=overlong_client)))
        whitespace_client = deepcopy(base.orders)
        whitespace_client["take_profit"]["clientAlgoId"] = "tp invalid"
        invalid_variants.append(
            ("whitespace_client", replace(base, orders=whitespace_client))
        )

        class ClientIdSubclass(str):
            pass

        subclass_client = deepcopy(base.orders)
        subclass_client["open"]["clientOrderId"] = ClientIdSubclass("mkt-claim")
        invalid_variants.append(
            ("client_subclass", replace(base, orders=subclass_client))
        )

        class IdentityStringSubclass(str):
            pass

        for name, role, field in (
            ("open_symbol_subclass", "open", "symbol"),
            ("open_side_subclass", "open", "side"),
            ("open_type_subclass", "open", "type"),
            ("open_status_subclass", "open", "status"),
            ("stop_symbol_subclass", "stop", "symbol"),
            ("stop_order_type_subclass", "stop", "orderType"),
            ("stop_algo_type_subclass", "stop", "algoType"),
            ("stop_status_subclass", "stop", "algoStatus"),
        ):
            subclass_identity = deepcopy(base.orders)
            subclass_identity[role][field] = IdentityStringSubclass(
                subclass_identity[role][field]
            )
            invalid_variants.append(
                (name, replace(base, orders=subclass_identity))
            )

        for name, state in invalid_variants:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                recorder = self._registered_claim_recorder(
                    tmpdir, f"test_live_open_claim_invalid_{name}"
                )
                self.assertIsNone(
                    recorder.claim_strategy_live_open_audit(state, "N01")
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute("SELECT COUNT(*) FROM trade_reviews").fetchone()[0],
                        0,
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_live_links"
                        ).fetchone()[0],
                        0,
                    )

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = self._registered_claim_recorder(
                tmpdir, "test_live_open_claim_global_link_conflict"
            )
            trade_id = recorder.record_trade_open(None, base)
            with recorder._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO strategy_live_links (
                        strategy_id, trade_review_id, symbol, opened_at, created_at
                    ) VALUES ('N02', ?, ?, ?, ?)
                    """,
                    (
                        trade_id,
                        base.symbol,
                        base.opened_at,
                        "2026-07-13T02:00:01+00:00",
                    ),
                )
            self.assertIsNone(recorder.claim_strategy_live_open_audit(base, "N01"))
            with recorder._connect() as connection:
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM trade_reviews").fetchone()[0], 1
                )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM strategy_live_links").fetchone()[0],
                    1,
                )

    def test_live_open_claim_rejects_non_json_orders_before_database_access(self):
        base = self._live_open_claim_state()

        class NestedStringSubclass(str):
            pass

        nested_subclass = deepcopy(base.orders)
        nested_subclass["plan"]["risk_amount"] = NestedStringSubclass("10")
        variants = [("nested_str_subclass", nested_subclass)]
        for name, invalid_float in (
            ("nan", float("nan")),
            ("positive_infinity", float("inf")),
            ("negative_infinity", float("-inf")),
        ):
            nonfinite_orders = deepcopy(base.orders)
            nonfinite_orders["plan"]["risk_fraction"] = invalid_float
            variants.append((name, nonfinite_orders))
        tuple_orders = deepcopy(base.orders)
        tuple_orders["plan"]["risk_inputs"] = ("10", "100")
        variants.append(("tuple", tuple_orders))

        for name, orders in variants:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                recorder = self._registered_claim_recorder(
                    tmpdir, f"test_live_open_claim_strict_json_{name}"
                )
                state = replace(base, orders=orders)
                with patch.object(
                    recorder,
                    "_connect",
                    side_effect=AssertionError("claim must reject before DB access"),
                ) as connect_mock:
                    self.assertIsNone(
                        recorder.claim_strategy_live_open_audit(state, "N01")
                    )
                    connect_mock.assert_not_called()
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM trade_reviews"
                        ).fetchone()[0],
                        0,
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_live_links"
                        ).fetchone()[0],
                        0,
                    )

    def test_live_open_claim_and_inspect_use_recursive_type_sensitive_order_identity(self):
        base = self._live_open_claim_state()

        class NestedStringSubclass(str):
            pass

        variants = []
        state_order_id_one = deepcopy(base.orders)
        state_order_id_one["open"]["orderId"] = 1

        bool_review = deepcopy(state_order_id_one)
        bool_review["open"]["orderId"] = True
        variants.append(
            (
                "bool_vs_int",
                replace(base, orders=state_order_id_one),
                bool_review,
            )
        )

        float_review = deepcopy(state_order_id_one)
        float_review["open"]["orderId"] = 1.0
        variants.append(
            (
                "float_vs_int",
                replace(base, orders=state_order_id_one),
                float_review,
            )
        )

        nested_subclass_state = deepcopy(base.orders)
        nested_subclass_state["plan"]["risk_amount"] = NestedStringSubclass("10")
        variants.append(
            (
                "nested_str_subclass",
                replace(base, orders=nested_subclass_state),
                deepcopy(base.orders),
            )
        )

        for name, state, review_orders in variants:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                recorder = self._registered_claim_recorder(
                    tmpdir, f"test_type_sensitive_orders_{name}"
                )
                trade_id = recorder.record_trade_open(None, state)
                self.assertIsNotNone(trade_id)
                with recorder._connect() as connection:
                    connection.execute(
                        "UPDATE trade_reviews SET orders_json = ? WHERE id = ?",
                        (json.dumps(review_orders), trade_id),
                    )
                evidence = recorder.inspect_strategy_live_finalization(state, "N01")
                self.assertEqual(evidence.status, "BLOCKED")
                self.assertIsNone(
                    recorder.claim_strategy_live_open_audit(state, "N01")
                )
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_live_links"
                        ).fetchone()[0],
                        0,
                    )

    def test_connect_rolls_back_and_closes_on_exception(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = Path(tmpdir) / "review.sqlite3"
            with _sqlite_connection(db_file) as connection:
                connection.execute("CREATE TABLE connection_lifecycle (value INTEGER)")

            original_connect = sqlite3.connect
            connections = []

            class TrackingConnection(sqlite3.Connection):
                def close(self):
                    self.close_called = True
                    return super().close()

            def tracking_connect(path, *args, **kwargs):
                kwargs["factory"] = TrackingConnection
                connection = original_connect(path, *args, **kwargs)
                connection.close_called = False
                connections.append(connection)
                return connection

            recorder = self._bare_recorder(db_file)
            with patch("trading_bot.recorder.sqlite3.connect", side_effect=tracking_connect):
                with self.assertRaisesRegex(RuntimeError, "forced transaction failure"):
                    with recorder._connect() as connection:
                        connection.execute("INSERT INTO connection_lifecycle VALUES (1)")
                        raise RuntimeError("forced transaction failure")

            self.assertGreaterEqual(len(connections), 1)
            self.assertTrue(all(connection.close_called for connection in connections))
            with _sqlite_connection(db_file) as connection:
                self.assertEqual(
                    connection.execute("SELECT value FROM connection_lifecycle").fetchall(),
                    [],
                )

    def test_repeated_public_recorder_calls_do_not_accumulate_file_descriptors(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_recorder_fd_lifecycle"),
            )
            fd_directory = "/dev/fd" if Path("/dev/fd").exists() else "/proc/self/fd"
            before = len(list(Path(fd_directory).iterdir()))

            for index in range(150):
                self.assertTrue(recorder.record_event("connection_lifecycle_probe", {"index": index}))

            after = len(list(Path(fd_directory).iterdir()))
            self.assertLessEqual(after, before + 2)

    def test_final_trade_close_is_idempotent_and_conflicts_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_final_close_idempotency"),
            )
            state = PositionState(
                symbol="DODOXUSDT",
                quantity="10",
                entry_price="100",
                stop_loss_price="95",
                take_profit_price="125",
                leverage=10,
                opened_at="2026-07-13T00:00:00+00:00",
                dry_run=False,
                orders={"strategy": {"strategy_id": "N01"}},
            )
            first_id = recorder.record_trade_close(
                state, "STOP_LOSS", "94.9", "", "", "", "", {"source": "stop"}
            )
            repeated_id = recorder.record_trade_close(
                state, "STOP_LOSS", "94.9", "", "", "", "", {"source": "retry"}
            )
            conflicting_id = recorder.record_trade_close(
                state, "TAKE_PROFIT", "125.1", "", "", "", "", {"source": "conflict"}
            )

            self.assertEqual(repeated_id, first_id)
            self.assertIsNone(conflicting_id)
            with recorder._connect() as connection:
                rows = connection.execute(
                    "SELECT id, status, exit_reason, exit_price FROM trade_reviews"
                ).fetchall()
                close_events = connection.execute(
                    "SELECT COUNT(*) FROM events WHERE event_type = 'live_position_closed'"
                ).fetchone()[0]
            self.assertEqual(rows, [(first_id, "CLOSED_STOP_LOSS", "STOP_LOSS", "94.9")])
            self.assertEqual(close_events, 1)

    def test_atomic_live_finalizer_preserves_open_audit_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, state, trade_id = self._pending_live_finalization_fixture(tmpdir)
            cooldown_until = datetime.now(timezone.utc) + timedelta(hours=4)
            finalization = recorder.finalize_strategy_live_result(
                state,
                "N01",
                "LOSS",
                "STOP_LOSS",
                "94.9",
                {"source": "STOP_ALGO_ORDER"},
                cooldown_until,
            )
            self.assertIsNotNone(finalization)
            self.assertFalse(finalization.idempotent)
            with recorder._connect() as connection:
                review = connection.execute(
                    "SELECT status, closed_at, orders_json FROM trade_reviews WHERE id = ?",
                    (trade_id,),
                ).fetchone()
                before_events = connection.execute(
                    "SELECT event_type, COUNT(*) FROM events "
                    "WHERE event_type IN ('live_position_closed', 'symbol_cooldown_set') "
                    "GROUP BY event_type ORDER BY event_type"
                ).fetchall()
                before_cooldown = connection.execute(
                    "SELECT cooldown_until, updated_at, source_trade_id "
                    "FROM symbol_cooldowns WHERE symbol = ?",
                    (state.symbol,),
                ).fetchone()
            self.assertEqual(review[0], "CLOSED_STOP_LOSS")
            self.assertTrue(review[1])
            orders = json.loads(review[2])
            self.assertEqual(orders["plan"], state.orders["plan"])
            self.assertEqual(orders["open_audit"], state.orders["open_audit"])
            self.assertEqual(orders["close"]["exit_reason"], "STOP_LOSS")
            self.assertEqual(before_events, [("live_position_closed", 2), ("symbol_cooldown_set", 1)])

            replay = recorder.finalize_strategy_live_result(
                state,
                "N01",
                "LOSS",
                "STOP_LOSS",
                "94.9",
                {"source": "STOP_ALGO_ORDER"},
                cooldown_until + timedelta(hours=2),
            )
            conflict = recorder.finalize_strategy_live_result(
                state,
                "N01",
                "WIN",
                "TAKE_PROFIT",
                "125",
                {"source": "TP_ALGO_ORDER"},
                cooldown_until,
            )
            self.assertIsNotNone(replay)
            self.assertTrue(replay.idempotent)
            self.assertIsNone(conflict)
            with recorder._connect() as connection:
                after_events = connection.execute(
                    "SELECT event_type, COUNT(*) FROM events "
                    "WHERE event_type IN ('live_position_closed', 'symbol_cooldown_set') "
                    "GROUP BY event_type ORDER BY event_type"
                ).fetchall()
                after_cooldown = connection.execute(
                    "SELECT cooldown_until, updated_at, source_trade_id "
                    "FROM symbol_cooldowns WHERE symbol = ?",
                    (state.symbol,),
                ).fetchone()
            self.assertEqual(after_events, before_events)
            self.assertEqual(after_cooldown, before_cooldown)

    def test_atomic_live_finalizer_rolls_back_intermediate_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, state, trade_id = self._pending_live_finalization_fixture(tmpdir)
            with patch.object(
                recorder,
                "_orders_json_with_close",
                side_effect=RuntimeError("forced finalizer failure"),
            ):
                self.assertIsNone(
                    recorder.finalize_strategy_live_result(
                        state,
                        "N01",
                        "LOSS",
                        "STOP_LOSS",
                        "94.9",
                        {},
                        datetime.now(timezone.utc) + timedelta(hours=4),
                    )
                )
            with recorder._connect() as connection:
                review = connection.execute(
                    "SELECT status, closed_at FROM trade_reviews WHERE id = ?", (trade_id,)
                ).fetchone()
                link = connection.execute(
                    "SELECT closed_at, result FROM strategy_live_links"
                ).fetchone()
                cooldown_count = connection.execute(
                    "SELECT COUNT(*) FROM symbol_cooldowns"
                ).fetchone()[0]
                terminal_event_count = connection.execute(
                    "SELECT COUNT(*) FROM events WHERE event_type = 'symbol_cooldown_set'"
                ).fetchone()[0]
            self.assertEqual(review[0], "CLOSED_LIVE_RESULT_PENDING")
            self.assertTrue(review[1])
            self.assertEqual(link, (None, None))
            self.assertEqual(cooldown_count, 0)
            self.assertEqual(terminal_event_count, 0)

    def test_atomic_live_finalizer_validates_result_reason_and_cooldown_owner(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, state, trade_id = self._pending_live_finalization_fixture(tmpdir)
            with self.assertRaisesRegex(ValueError, "result and exit reason disagree"):
                recorder.finalize_strategy_live_result(
                    state,
                    "N01",
                    "WIN",
                    "STOP_LOSS",
                    "94.9",
                    {},
                    datetime.now(timezone.utc) + timedelta(hours=4),
                )
            self.assertTrue(
                recorder.set_symbol_cooldown(
                    state.symbol,
                    datetime.now(timezone.utc) + timedelta(hours=1),
                    "LIVE_RESULT_PENDING",
                    trade_id,
                )
            )
            self.assertIsNotNone(
                recorder.finalize_strategy_live_result(
                    state,
                    "N01",
                    "LOSS",
                    "STOP_LOSS",
                    "94.9",
                    {},
                    datetime.now(timezone.utc) + timedelta(hours=4),
                )
            )
            with recorder._connect() as connection:
                same_owner = connection.execute(
                    "SELECT reason, source_trade_id FROM symbol_cooldowns WHERE symbol = ?",
                    (state.symbol,),
                ).fetchone()
            self.assertEqual(same_owner, ("STOP_LOSS", trade_id))

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, state, trade_id = self._pending_live_finalization_fixture(tmpdir)
            self.assertTrue(
                recorder.set_symbol_cooldown(
                    state.symbol,
                    datetime(2026, 7, 13, 1, tzinfo=timezone.utc),
                    "OTHER_TRADE",
                    trade_id + 1,
                )
            )
            self.assertIsNone(
                recorder.finalize_strategy_live_result(
                    state,
                    "N01",
                    "LOSS",
                    "STOP_LOSS",
                    "94.9",
                    {},
                    datetime.now(timezone.utc) + timedelta(hours=4),
                )
            )
            with recorder._connect() as connection:
                review = connection.execute(
                    "SELECT status FROM trade_reviews WHERE id = ?", (trade_id,)
                ).fetchone()[0]
                other_owner = connection.execute(
                    "SELECT reason, source_trade_id FROM symbol_cooldowns WHERE symbol = ?",
                    (state.symbol,),
                ).fetchone()
            self.assertEqual(review, "CLOSED_LIVE_RESULT_PENDING")
            self.assertEqual(other_owner, ("OTHER_TRADE", trade_id + 1))

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, state, trade_id = self._pending_live_finalization_fixture(tmpdir)
            self.assertTrue(
                recorder.set_symbol_cooldown(
                    state.symbol,
                    datetime(2026, 7, 12, 23, tzinfo=timezone.utc),
                    "EXPIRED_OTHER_TRADE",
                    trade_id + 1,
                )
            )
            self.assertIsNotNone(
                recorder.finalize_strategy_live_result(
                    state,
                    "N01",
                    "LOSS",
                    "STOP_LOSS",
                    "94.9",
                    {},
                    datetime.now(timezone.utc) + timedelta(hours=4),
                )
            )
            with recorder._connect() as connection:
                expired_owner_replaced = connection.execute(
                    "SELECT reason, source_trade_id FROM symbol_cooldowns WHERE symbol = ?",
                    (state.symbol,),
                ).fetchone()
            self.assertEqual(expired_owner_replaced, ("STOP_LOSS", trade_id))

    def test_atomic_live_finalizer_rejects_invalid_link_identities(self):
        for mutation in ("no_link", "multiple_links", "null_trade_id", "wrong_trade_id"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmpdir:
                recorder, state, trade_id = self._pending_live_finalization_fixture(tmpdir)
                with recorder._connect() as connection:
                    if mutation == "no_link":
                        connection.execute("DELETE FROM strategy_live_links")
                    elif mutation == "multiple_links":
                        connection.execute(
                            "INSERT INTO strategy_live_links "
                            "(strategy_id, trade_review_id, symbol, opened_at, created_at) "
                            "VALUES (?, ?, ?, ?, ?)",
                            ("N01", trade_id, state.symbol, state.opened_at, "2026-07-13T00:00:01+00:00"),
                        )
                    elif mutation == "null_trade_id":
                        connection.execute("UPDATE strategy_live_links SET trade_review_id = NULL")
                    else:
                        duplicate = connection.execute(
                            "INSERT INTO trade_reviews "
                            "(opened_at, symbol, side, dry_run, status, orders_json) "
                            "VALUES (?, ?, 'BUY', 0, 'OPENED', '{}')",
                            (state.opened_at, state.symbol),
                        )
                        connection.execute(
                            "UPDATE strategy_live_links SET trade_review_id = ?",
                            (int(duplicate.lastrowid),),
                        )
                self.assertIsNone(
                    recorder.finalize_strategy_live_result(
                        state,
                        "N01",
                        "LOSS",
                        "STOP_LOSS",
                        "94.9",
                        {},
                        datetime.now(timezone.utc) + timedelta(hours=4),
                    )
                )
                self.assertTrue(recorder.get_strategy_state("N01").live_result_pending)
                with recorder._connect() as connection:
                    self.assertEqual(
                        connection.execute("SELECT COUNT(*) FROM symbol_cooldowns").fetchone()[0],
                        0,
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM events WHERE event_type = 'symbol_cooldown_set'"
                        ).fetchone()[0],
                        0,
                    )

    def test_legacy_live_result_requires_one_exact_link_before_applying_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                str(Path(tmpdir) / "review.sqlite3"),
                logging.getLogger("test_legacy_live_result_identity"),
            )
            with recorder._connect() as connection:
                recorder._ensure_strategy_state(connection, "N01")
                before = connection.execute(
                    "SELECT consecutive_wins, live_eligible, last_trade_result "
                    "FROM strategy_states WHERE strategy_id = 'N01'"
                ).fetchone()
            self.assertFalse(
                recorder.record_strategy_live_result(
                    "N01",
                    "LOSS",
                    "DODOXUSDT",
                    1,
                    "2026-07-13T00:00:00+00:00",
                )
            )
            with recorder._connect() as connection:
                after_missing = connection.execute(
                    "SELECT consecutive_wins, live_eligible, last_trade_result "
                    "FROM strategy_states WHERE strategy_id = 'N01'"
                ).fetchone()
            self.assertEqual(after_missing, before)

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, state, trade_id = self._pending_live_finalization_fixture(tmpdir)
            self.assertEqual(
                recorder.record_trade_close(
                    state, "STOP_LOSS", "94.9", "", "", "", "", {}
                ),
                trade_id,
            )
            self.assertTrue(
                recorder.record_strategy_live_result(
                    "N01", "LOSS", state.symbol, trade_id, state.opened_at
                )
            )
            with recorder._connect() as connection:
                after_first = connection.execute(
                    "SELECT consecutive_wins, live_eligible, live_result_pending, "
                    "last_trade_result, last_trade_closed_at FROM strategy_states "
                    "WHERE strategy_id = 'N01'"
                ).fetchone()
            self.assertTrue(
                recorder.record_strategy_live_result(
                    "N01", "LOSS", state.symbol, trade_id, state.opened_at
                )
            )
            self.assertFalse(
                recorder.record_strategy_live_result(
                    "N01", "WIN", state.symbol, trade_id, state.opened_at
                )
            )
            with recorder._connect() as connection:
                after_replay = connection.execute(
                    "SELECT consecutive_wins, live_eligible, live_result_pending, "
                    "last_trade_result, last_trade_closed_at FROM strategy_states "
                    "WHERE strategy_id = 'N01'"
                ).fetchone()
            self.assertEqual(after_replay, after_first)

    def test_explicit_paper_void_is_idempotent_and_does_not_change_statistics(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = f"{tmpdir}/review.sqlite3"
            recorder = make_test_recorder(db_file, logging.getLogger("test_void_paper"))
            trade_ids = []
            for strategy_id, symbol in (
                ("N06", "SNDKUSDT"),
                ("N07", "CRCLUSDT"),
                ("N08", "TAOUSDT"),
            ):
                trade_ids.append(
                    recorder.open_strategy_paper_trade(
                        strategy_id,
                        symbol,
                        "100",
                        "99",
                        "105",
                        "",
                        {},
                        {"fixture": True},
                    )
                )
            with recorder._connect() as connection:
                connection.execute(
                    """
                    UPDATE strategy_states
                    SET consecutive_wins=2, paper_trade_count=4,
                        win_count=3, loss_count=1, live_eligible=1
                    WHERE strategy_id IN ('N06','N07','N08')
                    """
                )
                before = connection.execute(
                    "SELECT strategy_id, consecutive_wins, paper_trade_count, win_count, "
                    "loss_count, live_eligible FROM strategy_states ORDER BY strategy_id"
                ).fetchall()

            self.assertTrue(
                recorder.void_strategy_paper_trade(
                    trade_ids[0], "N06", "SNDKUSDT", PAPER_TRADE_VOID_CONFIRMATION
                )
            )
            lock_file = Path(recorder.n16_claim_ledger_file).with_name(
                "trading_bot.lock"
            )
            lock_file.write_text("\n", encoding="utf-8")
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(
                    void_paper_trade_main(
                        [
                            "--db", db_file,
                            "--n16-claim-ledger",
                            str(recorder.n16_claim_ledger_file),
                            "--lock-file",
                            str(lock_file),
                            "--trade-id", str(trade_ids[1]),
                            "--strategy-id", "N07",
                            "--symbol", "CRCLUSDT",
                            "--confirm", PAPER_TRADE_VOID_CONFIRMATION,
                        ]
                    ),
                    0,
                )
            self.assertEqual(output.getvalue().strip(), "VOIDED")
            self.assertFalse(
                recorder.void_strategy_paper_trade(
                    trade_ids[0], "N06", "SNDKUSDT", PAPER_TRADE_VOID_CONFIRMATION
                )
            )
            with recorder._connect() as connection:
                results = connection.execute(
                    "SELECT id, result, exit_reason FROM strategy_paper_trades ORDER BY id"
                ).fetchall()
                after = connection.execute(
                    "SELECT strategy_id, consecutive_wins, paper_trade_count, win_count, "
                    "loss_count, live_eligible FROM strategy_states ORDER BY strategy_id"
                ).fetchall()
                events = connection.execute(
                    "SELECT COUNT(*) FROM events WHERE event_type='strategy_paper_trade_voided'"
                ).fetchone()[0]
            self.assertEqual([row[1] for row in results], ["VOID", "VOID", "OPEN"])
            self.assertTrue(all(row[2] == "RULE_VERSION_INVALIDATED" for row in results[:2]))
            self.assertIsNone(results[2][2])
            self.assertEqual(before, after)
            self.assertEqual(events, 2)

    def test_paper_void_requires_the_released_official_instance_lock(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = Path(tmpdir) / "review.sqlite3"
            recorder = make_test_recorder(
                db_file,
                logging.getLogger("test_void_official_lock"),
            )
            trade_id = recorder.open_strategy_paper_trade(
                "N06", "LOCKEDUSDT", "100", "99", "105", "", {}, {}
            )
            ledger_file = Path(recorder.n16_claim_ledger_file)
            lock_file = ledger_file.with_name("trading_bot.lock")
            lock_file.write_text("\n", encoding="utf-8")
            argv = [
                "--db", str(db_file),
                "--n16-claim-ledger", str(ledger_file),
                "--lock-file", str(lock_file),
                "--trade-id", str(trade_id),
                "--strategy-id", "N06",
                "--symbol", "LOCKEDUSDT",
                "--confirm", PAPER_TRADE_VOID_CONFIRMATION,
            ]

            stderr = io.StringIO()
            with InstanceLock(str(lock_file)), redirect_stderr(stderr):
                self.assertEqual(void_paper_trade_main(argv), 1)
            self.assertIn("Another trading bot instance", stderr.getvalue())
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT result FROM strategy_paper_trades WHERE id=?",
                        (trade_id,),
                    ).fetchone(),
                    ("OPEN",),
                )

            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(void_paper_trade_main(argv), 0)
            self.assertEqual(output.getvalue().strip(), "VOIDED")
            with InstanceLock(str(lock_file)) as reacquired:
                self.assertTrue(reacquired.acquired)

    def test_paper_void_rejects_missing_confirmation_mismatch_and_non_open(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                f"{tmpdir}/review.sqlite3", logging.getLogger("test_void_reject")
            )
            trade_id = recorder.open_strategy_paper_trade(
                "N06", "SNDKUSDT", "100", "99", "105", "", {}, {}
            )
            with self.assertRaises(ValueError):
                recorder.void_strategy_paper_trade(trade_id, "N06", "SNDKUSDT", "")
            with self.assertRaises(ValueError):
                recorder.void_strategy_paper_trade(
                    trade_id, "N07", "SNDKUSDT", PAPER_TRADE_VOID_CONFIRMATION
                )
            recorder.close_strategy_paper_trade(trade_id, "WIN", "TP", "105", "5")
            with self.assertRaises(ValueError):
                recorder.void_strategy_paper_trade(
                    trade_id, "N06", "SNDKUSDT", PAPER_TRADE_VOID_CONFIRMATION
                )

    def test_partial_trade_review_table_requires_explicit_safe_migration(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = f"{tmpdir}/review.sqlite3"
            with _sqlite_connection(db_file) as connection:
                connection.execute(
                    """
                    CREATE TABLE trade_reviews (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        opened_at TEXT NOT NULL,
                        symbol TEXT NOT NULL
                    )
                    """
                )

            before = Path(db_file).read_bytes()
            before_names = tuple(Path(tmpdir).iterdir())
            with self.assertRaises(SignalRetentionMaintenanceError):
                make_test_recorder(
                    db_file, logging.getLogger("test_n07_trade_migration")
                )
            self.assertEqual(Path(db_file).read_bytes(), before)
            self.assertEqual(tuple(Path(tmpdir).iterdir()), before_names)
            self.assertFalse(test_claim_ledger_path(db_file).exists())

    def test_existing_strategy_state_table_adds_live_result_pending(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = f"{tmpdir}/review.sqlite3"
            with _sqlite_connection(db_file) as connection:
                connection.execute(
                    """
                    CREATE TABLE strategy_states (
                        strategy_id TEXT PRIMARY KEY,
                        consecutive_wins INTEGER NOT NULL DEFAULT 0,
                        paper_trade_count INTEGER NOT NULL DEFAULT 0,
                        win_count INTEGER NOT NULL DEFAULT 0,
                        loss_count INTEGER NOT NULL DEFAULT 0,
                        win_rate TEXT NOT NULL DEFAULT '0',
                        live_eligible INTEGER NOT NULL DEFAULT 0,
                        last_trade_result TEXT,
                        last_trade_closed_at TEXT,
                        updated_at TEXT NOT NULL
                    )
                    """
                )

            recorder = make_test_recorder(db_file, logging.getLogger("test_strategy_state_migration"))

            with recorder._connect() as connection:
                columns = {row[1] for row in connection.execute("PRAGMA table_info(strategy_states)")}
            self.assertIn("live_result_pending", columns)

    def test_existing_paper_trade_table_adds_last_checked_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = f"{tmpdir}/review.sqlite3"
            with _sqlite_connection(db_file) as connection:
                connection.execute(
                    """
                    CREATE TABLE strategy_paper_trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        strategy_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        opened_at TEXT NOT NULL,
                        closed_at TEXT,
                        entry_price TEXT NOT NULL,
                        stop_loss_price TEXT NOT NULL,
                        take_profit_price TEXT NOT NULL,
                        result TEXT NOT NULL,
                        exit_reason TEXT,
                        r_multiple TEXT,
                        funding_rate TEXT NOT NULL,
                        orders_json TEXT NOT NULL,
                        detail_json TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    "CREATE INDEX idx_strategy_paper_open "
                    "ON strategy_paper_trades(strategy_id, result)"
                )
                connection.execute(
                    "CREATE INDEX idx_strategy_paper_symbol_result "
                    "ON strategy_paper_trades("
                    "strategy_id, symbol, result, closed_at)"
                )

            recorder = make_test_recorder(db_file, logging.getLogger("test_paper_migration"))

            with recorder._connect() as connection:
                columns = {row[1] for row in connection.execute("PRAGMA table_info(strategy_paper_trades)")}
            self.assertIn("last_checked_at", columns)

    def test_existing_strategy_signal_table_is_migrated_before_structure_index(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = f"{tmpdir}/review.sqlite3"
            with _sqlite_connection(db_file) as connection:
                connection.execute(
                    """
                    CREATE TABLE strategy_signals (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        scan_id INTEGER,
                        strategy_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        funding_rate TEXT NOT NULL,
                        matched_patterns TEXT NOT NULL,
                        trend_slope TEXT NOT NULL,
                        current_bullish INTEGER NOT NULL,
                        passed INTEGER NOT NULL,
                        decision TEXT NOT NULL,
                        reason TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    )
                    """
                )

            recorder = make_test_recorder(db_file, logging.getLogger("test_recorder_migration"))

            with recorder._connect() as connection:
                columns = {row[1] for row in connection.execute("PRAGMA table_info(strategy_signals)")}
                indexes = {row[1] for row in connection.execute("PRAGMA index_list(strategy_signals)")}
                coverage_columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(n08_history_coverage)"
                    )
                }
            self.assertIn("structure_id", columns)
            self.assertIn("detail_json", columns)
            self.assertIn("idx_strategy_signals_structure", indexes)
            self.assertIn("continuous_from_open_time", coverage_columns)
            self.assertIn("continuous_until_open_time", coverage_columns)
            self.assertIn("last_gap_to_open_time", coverage_columns)

    def test_n09_structure_state_migration_is_additive_and_idempotent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = f"{tmpdir}/review.sqlite3"
            recorder = make_test_recorder(db_file, logging.getLogger("test_n09_migration"))
            detail = {"structure_id": "n09-structure", "p43": "97.2", "p50": "100"}

            first = recorder.record_n09_structure_consumed(
                "N09",
                "AAAUSDT",
                "n09-structure",
                "2026-07-11T00:00:00+00:00",
                "120",
                "2026-07-11T05:00:00+00:00",
                "80",
                "2026-07-11T06:00:00+00:00",
                "HISTORICAL_P50_TOUCH_MISSED",
                detail,
            )
            second = recorder.record_n09_structure_consumed(
                "N09",
                "AAAUSDT",
                "n09-structure",
                "2026-07-11T00:00:00+00:00",
                "120",
                "2026-07-11T05:00:00+00:00",
                "80",
                "2026-07-11T06:00:00+00:00",
                "HISTORICAL_P50_TOUCH_MISSED",
                detail,
            )

            restarted = make_test_recorder(db_file, logging.getLogger("test_n09_restart"))
            state = restarted.get_n09_structure_state_for_s1(
                "N09", "AAAUSDT", "2026-07-11T00:00:00+00:00"
            )
            with restarted._connect() as connection:
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(n09_structure_states)")
                }
                count = connection.execute(
                    "SELECT COUNT(*) FROM n09_structure_states"
                ).fetchone()[0]

            self.assertTrue(first)
            self.assertTrue(second)
            self.assertEqual(count, 1)
            self.assertIsNotNone(state)
            self.assertEqual(state.status, "CONSUMED")
            self.assertEqual(state.reason, "HISTORICAL_P50_TOUCH_MISSED")
            self.assertTrue(
                {"structure_id", "s1_time", "l_time", "first_touch_time", "detail_json"}
                <= columns
            )

    def test_n10_structure_state_migration_is_additive_and_idempotent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = f"{tmpdir}/review.sqlite3"
            recorder = make_test_recorder(db_file, logging.getLogger("test_n10_migration"))
            args = (
                "N10",
                "AAAUSDT",
                "n10-structure",
                "1000",
                "2000",
                "100",
                "3000",
                "4000",
                "5000",
                "N10_ENTRY_WINDOW_EXPIRED",
                {"support_price": "100"},
            )
            self.assertTrue(recorder.record_n10_structure_consumed(*args))
            self.assertTrue(recorder.record_n10_structure_consumed(*args))

            restarted = make_test_recorder(db_file, logging.getLogger("test_n10_restart"))
            state = restarted.get_n10_structure_state("N10", "n10-structure")
            with restarted._connect() as connection:
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(n10_structure_states)")
                }
                count = connection.execute(
                    "SELECT COUNT(*) FROM n10_structure_states"
                ).fetchone()[0]
            self.assertEqual(count, 1)
            self.assertIsNotNone(state)
            self.assertEqual(state.reason, "N10_ENTRY_WINDOW_EXPIRED")
            self.assertTrue(
                {
                    "structure_id",
                    "support_start_time",
                    "support_end_time",
                    "support_price",
                    "w_time",
                    "c_time",
                    "e_time",
                    "detail_json",
                }
                <= columns
            )

    def test_records_scan_signal_and_trade(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = Path(tmpdir) / "review.sqlite3"
            recorder = make_test_recorder(str(db_file), logging.getLogger("test_recorder"))
            candidate = FundingCandidate("BTCUSDT", Decimal("0.02"), Decimal("60000"))
            scan_id = recorder.begin_scan(300, [candidate], dry_run=True)
            result = AnalysisResult(
                symbol="BTCUSDT",
                passed=True,
                trend_slope=Decimal("1.2"),
                pattern="A_3_BULLISH_CANDLES",
                current_bullish=True,
                detail="ok",
            )
            signal_id = recorder.record_signal(
                scan_id, candidate, result, "PASSED"
            )
            self.assertIsNotNone(signal_id)
            self.assertTrue(
                recorder.publish_strategy_signal_batch(scan_id, 1)
            )
            self.assertEqual(
                recorder.current_strategy_signal_scan_id(), scan_id
            )
            recorder.record_trade_open(
                scan_id,
                PositionState(
                    symbol="BTCUSDT",
                    quantity="0.001",
                    entry_price="60000",
                    stop_loss_price="59000",
                    take_profit_price="65000",
                    leverage=50,
                    opened_at="2026-01-01T00:00:00+00:00",
                    dry_run=True,
                    orders={
                        "plan": {
                            "amplitude_24h_pct": "0.3",
                            "stop_loss_pct": "0.036",
                            "take_profit_pct": "0.18",
                            "risk_amount": "200",
                            "notional_value": "6000",
                            "required_margin": "600",
                            "balance": "1000",
                        },
                        "open": {"dryRun": True},
                    },
                ),
            )
            recorder.complete_scan(scan_id, opened=True)
            trade_id = recorder.record_trade_close(
                PositionState(
                    symbol="BTCUSDT",
                    quantity="0.001",
                    entry_price="60000",
                    stop_loss_price="59000",
                    take_profit_price="65000",
                    leverage=50,
                    opened_at="2026-01-01T00:00:00+00:00",
                    dry_run=True,
                    orders={},
                ),
                "STOP_LOSS",
                "59000",
                "58950",
                "-1",
                "-0.01666666666666666666666666667",
                "999",
            )
            self.assertEqual(trade_id, 1)
            cooldown_until = datetime(2026, 1, 1, 4, tzinfo=timezone.utc)
            recorder.set_symbol_cooldown("BTCUSDT", cooldown_until, "STOP_LOSS", trade_id)

            summary = recorder.latest_summary()
            self.assertEqual(summary["scan_count"], 1)
            self.assertEqual(summary["signal_count"], 1)
            self.assertEqual(summary["trade_count"], 1)

            with recorder._connect() as connection:
                trade = connection.execute(
                    """
                    SELECT status, exit_reason, exit_price, close_mark_price, realized_pnl, balance_after_close
                    FROM trade_reviews
                    """
                ).fetchone()
                self.assertEqual(trade[0], "CLOSED_STOP_LOSS")
                self.assertEqual(trade[1], "STOP_LOSS")
                self.assertEqual(trade[2], "59000")
                self.assertEqual(trade[3], "58950")
                self.assertEqual(trade[4], "-1")
                self.assertEqual(trade[5], "999")
                event_count = connection.execute(
                    "SELECT COUNT(*) FROM events WHERE event_type = 'dry_run_position_closed'"
                ).fetchone()[0]
                self.assertEqual(event_count, 1)
                cooldown_event_count = connection.execute(
                    "SELECT COUNT(*) FROM events WHERE event_type = 'symbol_cooldown_set'"
                ).fetchone()[0]
                self.assertEqual(cooldown_event_count, 1)

            active = recorder.active_symbol_cooldown("BTCUSDT", datetime(2026, 1, 1, 1, tzinfo=timezone.utc))
            self.assertIsNotNone(active)
            self.assertEqual(active.cooldown_until, "2026-01-01T04:00:00+00:00")
            self.assertEqual(active.source_trade_id, 1)

            expired = recorder.active_symbol_cooldown(
                "BTCUSDT",
                datetime(2026, 1, 1, 4, tzinfo=timezone.utc) + timedelta(seconds=1),
            )
            self.assertIsNone(expired)

    def test_records_n07_target_actual_and_margin_capped_risk(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = make_test_recorder(
                f"{tmpdir}/review.sqlite3",
                logging.getLogger("test_n07_trade_record"),
            )
            scan_id = recorder.begin_scan(0, [], dry_run=True)

            trade_id = recorder.record_trade_open(
                scan_id,
                PositionState(
                    symbol="N07USDT",
                    quantity="41.906",
                    entry_price="113.34",
                    stop_loss_price="112",
                    take_profit_price="120.04",
                    leverage=5,
                    opened_at="2026-07-10T00:00:00+00:00",
                    dry_run=True,
                    orders={
                        "plan": {
                            "target_risk_amount": "200",
                            "actual_risk_amount": "56.15404",
                            "risk_capped_by_margin": True,
                            "pretrade_quantity": "41.909",
                            "executed_quantity": "41.909",
                            "final_protected_quantity": "41.850",
                            "post_fill_actual_risk_amount": "62.775",
                            "post_fill_required_margin": "949.995",
                            "reduced_after_fill": True,
                        }
                    },
                ),
            )

            with recorder._connect() as connection:
                row = connection.execute(
                    "SELECT target_risk_amount, actual_risk_amount, risk_capped_by_margin, "
                    "pretrade_quantity, executed_quantity, final_protected_quantity, "
                    "post_fill_actual_risk_amount, post_fill_required_margin, reduced_after_fill "
                    "FROM trade_reviews WHERE id = ?",
                    (trade_id,),
                ).fetchone()
            self.assertEqual(
                tuple(row),
                (
                    "200",
                    "56.15404",
                    1,
                    "41.909",
                    "41.909",
                    "41.850",
                    "62.775",
                    "949.995",
                    1,
                ),
            )


if __name__ == "__main__":
    unittest.main()
