from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import json
import logging
import tempfile
from types import SimpleNamespace
import unittest

from tests.recorder_test_utils import make_test_recorder
from trading_bot.binance_client import BinanceAPIError
from trading_bot.main import TradingBot
from trading_bot.recorder import ReviewRecorder
from trading_bot.state import PositionState, StateStore
from trading_bot.strategies import N14_STRATEGY
from trading_bot.trader import MarketOrderExecutionPendingError, Trader
from tests.test_trader import (
    FakeCloseResolutionClient,
    FakeLiveExecutionClient,
    live_test_config,
)


class SubmitTimeoutClient(FakeLiveExecutionClient):
    def __init__(self, query_response, **kwargs):
        super().__init__({}, **kwargs)
        self.query_response = dict(query_response)

    def place_market_order(
        self, symbol, side, quantity, client_order_id=None
    ):
        self.market_calls.append((symbol, side, quantity))
        self.market_client_order_ids.append(client_order_id)
        self.last_market_client_order_id = client_order_id
        raise BinanceAPIError("market submit response lost")

    def get_order(self, symbol, order_id=None, orig_client_order_id=None):
        if orig_client_order_id is not None:
            return dict(self.query_response)
        return super().get_order(
            symbol,
            order_id=order_id,
            orig_client_order_id=orig_client_order_id,
        )


class HardKillAfterJournalClient(FakeLiveExecutionClient):
    def place_market_order(
        self, symbol, side, quantity, client_order_id=None
    ):
        self.market_calls.append((symbol, side, quantity))
        self.market_client_order_ids.append(client_order_id)
        self.last_market_client_order_id = client_order_id
        raise SystemExit("simulated hard process exit")


class WrongProtectionEchoClient(FakeLiveExecutionClient):
    def __init__(self, wrong_order_type, **kwargs):
        super().__init__({}, **kwargs)
        self.wrong_order_type = wrong_order_type

    def place_close_all_algo_order(
        self,
        symbol,
        order_type,
        trigger_price,
        client_algo_id=None,
    ):
        if order_type != self.wrong_order_type:
            return super().place_close_all_algo_order(
                symbol,
                order_type,
                trigger_price,
                client_algo_id=client_algo_id,
            )
        self.protection_calls.append((order_type, trigger_price))
        self.next_algo_id += 1
        actual = {
            "algoId": self.next_algo_id,
            "clientAlgoId": client_algo_id,
            "symbol": symbol,
            "algoType": "CONDITIONAL",
            "orderType": order_type,
            "side": "SELL",
            "closePosition": True,
            "algoStatus": "NEW",
            "triggerPrice": str(trigger_price),
        }
        self.algo_orders[self.next_algo_id] = dict(actual)
        self.algo_client_index[client_algo_id] = self.next_algo_id
        return {**actual, "clientAlgoId": "alien-protection-id"}


class CancelResponseLostClient(FakeLiveExecutionClient):
    def cancel_algo_order(self, algo_id=None, client_algo_id=None):
        if algo_id is None and client_algo_id is not None:
            algo_id = self.algo_client_index.get(client_algo_id)
        self.cancel_algo_calls.append((algo_id, client_algo_id))
        if algo_id not in self.algo_orders:
            raise BinanceAPIError("algo order unavailable")
        self.algo_orders[algo_id]["algoStatus"] = "CANCELED"
        raise BinanceAPIError("cancel response lost")


class SiblingCancelFailureClient(FakeCloseResolutionClient):
    def cancel_algo_order(self, algo_id=None, client_algo_id=None):
        if algo_id == 22:
            raise BinanceAPIError("sibling cancel unavailable")
        return super().cancel_algo_order(
            algo_id=algo_id,
            client_algo_id=client_algo_id,
        )


class SiblingFinishesWhileCancelingClient(FakeCloseResolutionClient):
    def cancel_algo_order(self, algo_id=None, client_algo_id=None):
        if algo_id == 22:
            self.algo_responses[22].update(
                {
                    "algoStatus": "FINISHED",
                    "actualOrderId": 222,
                }
            )
            return self.get_algo_order(algo_id=22)
        return super().cancel_algo_order(
            algo_id=algo_id,
            client_algo_id=client_algo_id,
        )


class AllKnownCancelFailureClient(FakeCloseResolutionClient):
    def cancel_algo_order(self, algo_id=None, client_algo_id=None):
        if algo_id in {11, 22}:
            raise BinanceAPIError("known protection cancel unavailable")
        return super().cancel_algo_order(
            algo_id=algo_id,
            client_algo_id=client_algo_id,
        )


class ExecutionSafetyTests(unittest.TestCase):
    def _trader_and_plan(self, tmpdir, client):
        state = StateStore(f"{tmpdir}/position.json")
        trader = Trader(
            client,
            live_test_config(f"{tmpdir}/account.json"),
            state,
            logging.getLogger("test_execution_safety"),
        )
        plan = trader.build_sell_pressure_decay_margin_capped_trade_plan(
            "AAAUSDT",
            Decimal("100"),
            Decimal("99"),
            Decimal("5"),
            "n14-execution-safety",
            entry_min_price=Decimal("100"),
            entry_max_price=Decimal("101"),
        )
        return trader, state, plan

    def _create_pending_from_wrong_market_echo(self, tmpdir):
        client = FakeLiveExecutionClient({}, leverage=50)
        trader, state, plan = self._trader_and_plan(tmpdir, client)
        client.open_response = {
            "orderId": 1401,
            "clientOrderId": "wrong-market-id",
            "symbol": plan.symbol,
            "side": "BUY",
            "type": "MARKET",
            "status": "FILLED",
            "executedQty": str(plan.quantity),
            "avgPrice": "100",
        }
        with self.assertRaises(MarketOrderExecutionPendingError):
            trader.open_long_plan_with_protection(plan)
        stored = state.load()
        self.assertIsNotNone(stored)
        return client, trader, state, plan, stored

    def test_stale_emergency_cleanup_is_read_only_for_confirmed_flat(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient({}, leverage=10)
            trader, state_store, _plan = self._trader_and_plan(tmpdir, client)
            state_store.save(PositionState(
                symbol="TUSDT", quantity="79486", entry_price="0.003992",
                stop_loss_price="0.0038", take_profit_price="0.004952",
                leverage=10, opened_at="2026-07-19T21:06:08+00:00",
                dry_run=False,
                orders={
                    "strategy": {
                        "strategy_id": "N25", "signal_id": 7761956,
                        "structure_id": "5bfef983dccc6efdc403ef86",
                    },
                    "emergency_cleanup_pending": {
                        "reason": "EMERGENCY_POSITION_CONFIRMATION_FAILED",
                        "placed_protection_orders": [],
                        "realized_pnl": "-0.079486",
                        "fees": "-0.31726833",
                    },
                },
            ))
            client.get_open_orders = lambda _symbol: []
            client.get_open_algo_orders = lambda _symbol: []
            client.get_long_position_quantity_or_zero = lambda _symbol: Decimal("0")

            result = trader.sync_state_with_exchange()

            self.assertTrue(result.pending_resolved)
            self.assertFalse(result.has_position)
            self.assertEqual(
                result.pending_detail["reason"],
                "EMERGENCY_CLEANUP_CONFIRMED_FLAT_WITHOUT_LIVE_RESULT",
            )
            self.assertEqual(client.close_calls, [])
            self.assertEqual(client.cancel_algo_calls, [])
            self.assertEqual(client.cancel_all_algo_calls, [])
            self.assertEqual(
                result.pending_detail["previous"]["realized_pnl"], "-0.079486"
            )
            self.assertEqual(
                result.pending_detail["previous"]["fees"], "-0.31726833"
            )

    def test_stale_emergency_cleanup_blocks_nonflat_orders_and_query_errors(self):
        cases = (
            ("ordinary", [{"orderId": 1}], [], Decimal("0"), None),
            ("algo", [], [{"algoId": 2}], Decimal("0"), None),
            ("nonflat", [], [], Decimal("1"), None),
            ("query_error", None, [], Decimal("0"), BinanceAPIError("query unavailable")),
        )
        for name, ordinary, algo, quantity, query_error in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                client = FakeLiveExecutionClient({}, leverage=10)
                trader, state_store, _plan = self._trader_and_plan(tmpdir, client)
                state_store.save(PositionState(
                    symbol="TUSDT", quantity="1", entry_price="1",
                    stop_loss_price="0.99", take_profit_price="1.05",
                    leverage=10, opened_at="2026-07-19T21:06:08+00:00",
                    dry_run=False,
                    orders={"emergency_cleanup_pending": {
                        "placed_protection_orders": []
                    }},
                ))
                if query_error is not None:
                    def fail_open_orders(_symbol, error=query_error):
                        raise error
                    client.get_open_orders = fail_open_orders
                else:
                    client.get_open_orders = lambda _symbol, rows=ordinary: rows
                client.get_open_algo_orders = lambda _symbol, rows=algo: rows
                client.get_long_position_quantity_or_zero = (
                    lambda _symbol, value=quantity: value
                )

                result = trader.sync_state_with_exchange()

                self.assertTrue(result.execution_pending)
                self.assertFalse(result.pending_resolved)
                self.assertEqual(client.close_calls, [])
                self.assertEqual(client.cancel_algo_calls, [])
                self.assertEqual(client.cancel_all_algo_calls, [])
                self.assertIsNotNone(state_store.load())

    def test_wrong_market_echo_is_journaled_and_never_places_protection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client, _, _, _, stored = (
                self._create_pending_from_wrong_market_echo(tmpdir)
            )
            pending = stored.orders["execution_pending"]
            expected = client.market_client_order_ids[0]
            self.assertEqual(pending["client_order_id"], expected)
            self.assertNotEqual(expected, "wrong-market-id")
            self.assertEqual(
                pending["phase"], "MARKET_ORDER_RESPONSE_INVALID"
            )
            self.assertEqual(client.protection_calls, [])
            self.assertEqual(client.close_calls, [])

    def test_first_success_state_save_already_owns_n14_before_main_audit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient({}, leverage=50)
            trader, state_store, plan = self._trader_and_plan(
                tmpdir, client
            )
            plan = replace(
                plan,
                structure_context={"strategy_id": "N14"},
            )
            client.open_response = {
                "orderId": 1410,
                "avgPrice": "100",
                "executedQty": str(plan.quantity),
            }
            client.position_quantities = [str(plan.quantity)]

            first_saved_state = trader.open_long_plan_with_protection(plan)

            self.assertEqual(
                first_saved_state.orders["strategy"],
                {
                    "strategy_id": "N14",
                    "structure_id": "n14-execution-safety",
                },
            )
            self.assertEqual(state_store.load(), first_saved_state)

            # Simulate a process exit before main.py can enrich/save the state.
            # On restart the exchange position is already zero, while both
            # known protections are still open.
            client.get_open_positions = lambda: []
            restarted = Trader(
                client,
                live_test_config(f"{tmpdir}/account.json"),
                state_store,
                logging.getLogger("test_n14_restart_before_main_audit"),
            )
            sync = restarted.sync_state_with_exchange()
            self.assertIsNotNone(sync.closed_state)
            self.assertEqual(
                sync.closed_state.orders["strategy"]["strategy_id"],
                "N14",
            )

            recorder = make_test_recorder(
                f"{tmpdir}/review.sqlite3",
                logging.getLogger("test_n14_restart_before_main_audit"),
            )
            recorder.upsert_strategy_definitions((N14_STRATEGY,))
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(symbol_cooldown_hours=4)
            bot.logger = logging.getLogger(
                "test_n14_restart_before_main_audit"
            )
            bot.recorder = recorder
            bot.state = state_store
            bot.trader = restarted

            blocked = bot._handle_closed_strategy_live_state(
                sync.closed_state
            )

            self.assertTrue(blocked)
            self.assertTrue(
                recorder.get_strategy_state("N14").live_result_pending
            )
            self.assertIsNotNone(state_store.load())
            with recorder._connect() as connection:
                closed = connection.execute(
                    "SELECT exit_reason, orders_json FROM trade_reviews "
                    "ORDER BY id DESC LIMIT 1"
                ).fetchone()
                review_count = connection.execute(
                    "SELECT COUNT(*) FROM trade_reviews"
                ).fetchone()[0]
                review_id = connection.execute(
                    "SELECT id FROM trade_reviews"
                ).fetchone()[0]
                live_links = connection.execute(
                    "SELECT strategy_id, trade_review_id, symbol, opened_at, "
                    "closed_at, result FROM strategy_live_links"
                ).fetchall()
            self.assertEqual(closed[0], "MANUAL_OR_EXTERNAL_CLOSE")
            closed_orders = json.loads(closed[1])
            self.assertEqual(
                closed_orders["state_orders"]["strategy"]["strategy_id"],
                "N14",
            )
            self.assertEqual(review_count, 1)
            self.assertEqual(len(live_links), 1)
            self.assertEqual(live_links[0][0], "N14")
            self.assertEqual(live_links[0][1], review_id)
            self.assertEqual(live_links[0][2], sync.closed_state.symbol)
            self.assertEqual(live_links[0][3], sync.closed_state.opened_at)
            self.assertIsNone(live_links[0][4])
            self.assertIsNone(live_links[0][5])

    def test_timeout_query_wrong_identity_stays_pending_across_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = SubmitTimeoutClient(
                {
                    "orderId": 1402,
                    "clientOrderId": "wrong-query-id",
                    "symbol": "AAAUSDT",
                    "side": "BUY",
                    "type": "MARKET",
                    "status": "FILLED",
                    "executedQty": "1",
                    "avgPrice": "100",
                },
                leverage=50,
                position_quantities=["0", "0"],
            )
            trader, state, plan = self._trader_and_plan(tmpdir, client)

            with self.assertRaises(MarketOrderExecutionPendingError):
                trader.open_long_plan_with_protection(plan)
            restarted = trader.sync_state_with_exchange()

            self.assertTrue(restarted.execution_pending)
            self.assertFalse(restarted.pending_resolved)
            self.assertEqual(
                restarted.pending_detail["reason"],
                "MARKET_ORDER_EXECUTION_STILL_UNKNOWN",
            )
            self.assertIsNotNone(state.load())
            self.assertEqual(client.protection_calls, [])
            self.assertEqual(client.close_calls, [])

    def test_pre_submit_journal_survives_hard_exit_and_resolves_unexecuted(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = HardKillAfterJournalClient({}, leverage=50)
            trader, state, plan = self._trader_and_plan(tmpdir, client)

            with self.assertRaises(SystemExit):
                trader.open_long_plan_with_protection(plan)
            stored = state.load()
            self.assertIsNotNone(stored)
            pending = stored.orders["execution_pending"]
            expected = pending["client_order_id"]
            self.assertEqual(pending["phase"], "MARKET_ORDER_SUBMITTING")
            self.assertEqual(
                [item["role"] for item in pending["possible_protection_orders"]],
                ["STOP", "TAKE_PROFIT"],
            )
            client.open_response = {
                "orderId": 1403,
                "clientOrderId": expected,
                "symbol": plan.symbol,
                "side": "BUY",
                "type": "MARKET",
                "status": "CANCELED",
                "executedQty": "0",
                "avgPrice": "0",
            }
            client.position_quantities = ["0"]

            resolved = trader.sync_state_with_exchange()

            self.assertTrue(resolved.pending_resolved)
            self.assertEqual(
                resolved.pending_detail["reason"],
                "MARKET_ORDER_CONFIRMED_NOT_EXECUTED",
            )
            self.assertIsNotNone(state.load())
            self.assertEqual(client.protection_calls, [])

    def test_late_filled_market_order_with_zero_position_resolves_cleanup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client, trader, state, plan, stored = (
                self._create_pending_from_wrong_market_echo(tmpdir)
            )
            expected = stored.orders["execution_pending"]["client_order_id"]
            client.open_response = {
                "orderId": 1404,
                "clientOrderId": expected,
                "symbol": plan.symbol,
                "side": "BUY",
                "type": "MARKET",
                "status": "FILLED",
                "executedQty": str(plan.quantity),
                "avgPrice": "100",
            }
            client.position_quantities = ["0"]

            resolved = trader.sync_state_with_exchange()

            self.assertTrue(resolved.pending_resolved)
            self.assertEqual(
                resolved.pending_detail["reason"],
                "LATE_MARKET_ORDER_EXECUTION_CLEANED",
            )
            self.assertEqual(client.close_calls, [])
            self.assertIsNotNone(state.load())

    def test_late_filled_market_order_with_position_is_closed_and_confirmed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client, trader, state, plan, stored = (
                self._create_pending_from_wrong_market_echo(tmpdir)
            )
            expected = stored.orders["execution_pending"]["client_order_id"]
            client.open_response = {
                "orderId": 1405,
                "clientOrderId": expected,
                "symbol": plan.symbol,
                "side": "BUY",
                "type": "MARKET",
                "status": "FILLED",
                "executedQty": str(plan.quantity),
                "avgPrice": "100",
            }
            client.position_quantities = [str(plan.quantity), "0"]

            resolved = trader.sync_state_with_exchange()

            self.assertTrue(resolved.pending_resolved)
            self.assertEqual(
                client.close_calls,
                [(plan.symbol, plan.quantity)],
            )
            self.assertEqual(resolved.pending_detail["remaining_quantity"], "0")
            self.assertIsNotNone(state.load())

    def test_wrong_stop_or_take_profit_echo_cleans_expected_ids_only(self):
        for wrong_type in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
            with self.subTest(wrong_type=wrong_type), tempfile.TemporaryDirectory() as tmpdir:
                client = WrongProtectionEchoClient(
                    wrong_type,
                    leverage=50,
                    position_quantities=[],
                )
                trader, state, plan = self._trader_and_plan(tmpdir, client)
                client.open_response = {
                    "orderId": 1406,
                    "avgPrice": "100",
                    "executedQty": str(plan.quantity),
                }
                client.position_quantities = [str(plan.quantity), "0"]

                with self.assertRaisesRegex(
                    BinanceAPIError, "post-fill adjustment or protection failed"
                ):
                    trader.open_long_plan_with_protection(plan)

                stored = state.load()
                self.assertIsNotNone(stored)
                resolved = stored.orders["execution_cleanup_resolved"]
                self.assertTrue(
                    resolved["emergency_cleanup"]["confirmed_closed"]
                )
                self.assertTrue(
                    resolved["protection_cleanup"][
                        "confirmed_all_canceled"
                    ]
                )
                attempted_client_ids = {
                    client_id
                    for _, client_id in client.cancel_algo_calls
                    if client_id is not None
                }
                self.assertNotIn("alien-protection-id", attempted_client_ids)
                self.assertTrue(
                    all(
                        order["clientAlgoId"] != "alien-protection-id"
                        for order in resolved["placed_protection_orders"]
                    )
                )

    def test_cancel_response_loss_is_confirmed_by_query_without_bulk_cancel(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = CancelResponseLostClient({}, leverage=50)
            trader, _, _ = self._trader_and_plan(tmpdir, client)
            order = client.place_close_all_algo_order(
                "AAAUSDT",
                "STOP_MARKET",
                Decimal("99"),
                client_algo_id="sl-cancel-response-lost",
            )

            audit = trader._cancel_placed_protection_orders(
                "AAAUSDT", [order]
            )

            self.assertTrue(audit["confirmed_all_canceled"])
            self.assertTrue(audit["attempts"][0]["confirmed_canceled"])
            self.assertIn("cancel response lost", audit["attempts"][0]["cancel_error"])
            self.assertEqual(client.cancel_all_algo_calls, [])

    def test_unrelated_manual_order_prevents_bulk_and_late_expected_is_canceled(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient({}, leverage=50)
            trader, _, _ = self._trader_and_plan(tmpdir, client)
            manual = {
                "algoId": 9001,
                "clientAlgoId": "manual-order",
                "symbol": "AAAUSDT",
                "algoType": "CONDITIONAL",
                "orderType": "STOP_MARKET",
                "side": "SELL",
                "closePosition": True,
                "algoStatus": "NEW",
            }
            client.algo_orders[9001] = dict(manual)
            client.algo_client_index["manual-order"] = 9001
            expected = {
                "role": "STOP",
                "clientAlgoId": "sl-expected-late",
                "submission_attempted": True,
                "absence_confirmations": 0,
            }

            first = trader._cancel_placed_protection_orders(
                "AAAUSDT", [expected]
            )
            self.assertFalse(first["confirmed_all_canceled"])
            self.assertEqual(first["updated_orders"][0]["absence_confirmations"], 1)
            self.assertEqual(client.cancel_all_algo_calls, [])
            self.assertEqual(client.algo_orders[9001]["algoStatus"], "NEW")

            late = {
                "algoId": 9002,
                "clientAlgoId": "sl-expected-late",
                "symbol": "AAAUSDT",
                "algoType": "CONDITIONAL",
                "orderType": "STOP_MARKET",
                "side": "SELL",
                "closePosition": True,
                "algoStatus": "NEW",
            }
            client.algo_orders[9002] = dict(late)
            client.algo_client_index["sl-expected-late"] = 9002
            second = trader._cancel_placed_protection_orders(
                "AAAUSDT", first["updated_orders"]
            )

            self.assertFalse(second["confirmed_all_canceled"])
            self.assertEqual(client.algo_orders[9002]["algoStatus"], "CANCELED")
            self.assertEqual(client.algo_orders[9001]["algoStatus"], "NEW")
            self.assertEqual(
                second["unrelated_open_orders"][0]["clientAlgoId"],
                "manual-order",
            )
            self.assertEqual(client.cancel_all_algo_calls, [])

    def test_filled_stop_cancels_new_take_profit_before_loss(self):
        client = FakeCloseResolutionClient(
            {
                11: {
                    "algoId": 11,
                    "algoStatus": "FINISHED",
                    "actualOrderId": 111,
                },
                22: {
                    "algoId": 22,
                    "algoStatus": "NEW",
                    "actualOrderId": "",
                },
            },
            {
                111: {
                    "orderId": 111,
                    "status": "FILLED",
                    "executedQty": "10",
                    "avgPrice": "94.9",
                }
            },
        )
        trader = Trader(
            client,
            live_test_config(),
            StateStore("/tmp/unused-execution-sibling.json"),
            logging.getLogger("test_execution_sibling"),
        )

        result = trader.resolve_closed_live_position(
            self._closed_live_state()
        )

        self.assertEqual(result.result, "LOSS")
        self.assertEqual(client.algo_responses[22]["algoStatus"], "CANCELED")
        self.assertTrue(result.detail["sibling_cleanup"]["confirmed_all_canceled"])

    def test_filled_take_profit_cancels_new_stop_before_win(self):
        client = FakeCloseResolutionClient(
            {
                11: {
                    "algoId": 11,
                    "algoStatus": "NEW",
                    "actualOrderId": "",
                },
                22: {
                    "algoId": 22,
                    "algoStatus": "FINISHED",
                    "actualOrderId": 222,
                },
            },
            {
                222: {
                    "orderId": 222,
                    "status": "FILLED",
                    "executedQty": "10",
                    "avgPrice": "125.1",
                }
            },
        )
        trader = Trader(
            client,
            live_test_config(),
            StateStore("/tmp/unused-execution-sibling-tp.json"),
            logging.getLogger("test_execution_sibling_tp"),
        )

        result = trader.resolve_closed_live_position(
            self._closed_live_state()
        )

        self.assertEqual(result.result, "WIN")
        self.assertEqual(client.algo_responses[11]["algoStatus"], "CANCELED")
        self.assertTrue(result.detail["sibling_cleanup"]["confirmed_all_canceled"])

    def test_sibling_cancel_failure_with_unrelated_order_stays_pending(self):
        client = SiblingCancelFailureClient(
            {
                11: {
                    "algoId": 11,
                    "algoStatus": "FINISHED",
                    "actualOrderId": 111,
                },
                22: {
                    "algoId": 22,
                    "algoStatus": "NEW",
                    "actualOrderId": "",
                },
                33: {
                    "algoId": 33,
                    "algoStatus": "NEW",
                    "actualOrderId": "",
                },
            },
            {
                111: {
                    "orderId": 111,
                    "status": "FILLED",
                    "executedQty": "10",
                    "avgPrice": "94.9",
                }
            },
        )
        trader = Trader(
            client,
            live_test_config(),
            StateStore("/tmp/unused-execution-sibling-pending.json"),
            logging.getLogger("test_execution_sibling_pending"),
        )

        result = trader.resolve_closed_live_position(
            self._closed_live_state()
        )

        self.assertEqual(result.result, "LIVE_RESULT_PENDING")
        self.assertEqual(
            result.detail["reason"], "SIBLING_PROTECTION_CANCEL_PENDING"
        )
        self.assertEqual(client.algo_responses[22]["algoStatus"], "NEW")
        self.assertEqual(client.algo_responses[33]["algoStatus"], "NEW")

    def test_sibling_finished_and_filled_during_cancel_is_never_loss(self):
        client = SiblingFinishesWhileCancelingClient(
            {
                11: {
                    "algoId": 11,
                    "algoStatus": "FINISHED",
                    "actualOrderId": 111,
                },
                22: {
                    "algoId": 22,
                    "algoStatus": "NEW",
                    "actualOrderId": "",
                },
            },
            {
                111: {
                    "orderId": 111,
                    "status": "FILLED",
                    "executedQty": "10",
                    "avgPrice": "94.9",
                },
                222: {
                    "orderId": 222,
                    "status": "FILLED",
                    "executedQty": "10",
                    "avgPrice": "125.1",
                },
            },
        )
        trader = Trader(
            client,
            live_test_config(),
            StateStore("/tmp/unused-execution-sibling-race.json"),
            logging.getLogger("test_execution_sibling_race"),
        )

        result = trader.resolve_closed_live_position(
            self._closed_live_state()
        )

        self.assertEqual(result.result, "LIVE_RESULT_PENDING")
        self.assertEqual(
            result.detail["reason"],
            "BOTH_PROTECTION_ORDERS_FILLED_DURING_CLEANUP",
        )
        self.assertFalse(
            result.detail["sibling_cleanup"]["confirmed_all_canceled"]
        )

    def test_zero_position_with_two_new_known_protections_cancels_both(self):
        client = FakeCloseResolutionClient(
            {
                11: {
                    "algoId": 11,
                    "algoStatus": "NEW",
                    "actualOrderId": "",
                },
                22: {
                    "algoId": 22,
                    "algoStatus": "NEW",
                    "actualOrderId": "",
                },
            },
            {},
        )
        trader = Trader(
            client,
            live_test_config(),
            StateStore("/tmp/unused-zero-position-protection.json"),
            logging.getLogger("test_zero_position_protection"),
        )

        result = trader.resolve_closed_live_position(
            self._closed_live_state()
        )

        self.assertEqual(result.result, "MANUAL_OR_EXTERNAL_CLOSE")
        self.assertEqual(client.algo_responses[11]["algoStatus"], "CANCELED")
        self.assertEqual(client.algo_responses[22]["algoStatus"], "CANCELED")
        self.assertTrue(
            result.detail["zero_position_protection_cleanup"][
                "confirmed_all_canceled"
            ]
        )

    def test_zero_position_cancel_failure_keeps_result_pending(self):
        client = AllKnownCancelFailureClient(
            {
                11: {
                    "algoId": 11,
                    "algoStatus": "NEW",
                    "actualOrderId": "",
                },
                22: {
                    "algoId": 22,
                    "algoStatus": "NEW",
                    "actualOrderId": "",
                },
                33: {
                    "algoId": 33,
                    "algoStatus": "NEW",
                    "actualOrderId": "",
                },
            },
            {},
        )
        trader = Trader(
            client,
            live_test_config(),
            StateStore("/tmp/unused-zero-position-protection-pending.json"),
            logging.getLogger("test_zero_position_protection_pending"),
        )

        result = trader.resolve_closed_live_position(
            self._closed_live_state()
        )

        self.assertEqual(result.result, "LIVE_RESULT_PENDING")
        self.assertEqual(
            result.detail["reason"],
            "ZERO_POSITION_PROTECTION_CANCEL_PENDING",
        )
        self.assertEqual(client.algo_responses[11]["algoStatus"], "NEW")
        self.assertEqual(client.algo_responses[22]["algoStatus"], "NEW")
        self.assertEqual(client.algo_responses[33]["algoStatus"], "NEW")

    @staticmethod
    def _closed_live_state():
        from trading_bot.state import PositionState

        return PositionState(
            symbol="AAAUSDT",
            quantity="10",
            entry_price="100",
            stop_loss_price="95",
            take_profit_price="125",
            leverage=10,
            opened_at="2026-07-12T00:00:00+00:00",
            dry_run=False,
            orders={
                "stop": {"algoId": 11},
                "take_profit": {"algoId": 22},
                "strategy": {"strategy_id": "N14"},
            },
        )


if __name__ == "__main__":
    unittest.main()
