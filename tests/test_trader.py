from dataclasses import replace
from decimal import Decimal
import json
import logging
import tempfile
import unittest

from tests.recorder_test_utils import make_test_recorder
from trading_bot.binance_client import BinanceAPIError
from trading_bot.config import LIVE_TRADING_CONFIRMATION, Config
from trading_bot.recorder import ReviewRecorder
from trading_bot.state import PositionState, StateStore
from trading_bot.trader import EntryWindowExpiredError, Trader


class FakeClient:
    def __init__(self, leverage=10, mark_price="120"):
        self.leverage = leverage
        self.mark_price = Decimal(mark_price)
        self.has_credentials = False

    def get_max_leverage(self, symbol):
        return self.leverage

    def get_available_usdt(self):
        return Decimal("1000")

    def get_24hr_ticker(self, symbol):
        return {"highPrice": "130", "lowPrice": "100"}

    def get_symbol_info(self, symbol):
        return {
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "100000", "stepSize": "0.001"},
                {"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "maxQty": "100000", "stepSize": "0.001"},
                {"filterType": "MIN_NOTIONAL", "notional": "5"},
            ]
        }

    def get_mark_price(self, symbol):
        return self.mark_price

    def get_open_orders(self, symbol):
        return []


class RuleConstrainedClient(FakeClient):
    def __init__(
        self,
        leverage=10,
        tick_size="0.01",
        min_qty="0.001",
        max_qty="100000",
        step_size="0.001",
        min_notional="5",
    ):
        super().__init__(leverage=leverage)
        self.tick_size = tick_size
        self.min_qty = min_qty
        self.max_qty = max_qty
        self.step_size = step_size
        self.min_notional = min_notional

    def get_symbol_info(self, symbol):
        return {
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": self.tick_size},
                {
                    "filterType": "LOT_SIZE",
                    "minQty": self.min_qty,
                    "maxQty": self.max_qty,
                    "stepSize": self.step_size,
                },
                {
                    "filterType": "MARKET_LOT_SIZE",
                    "minQty": self.min_qty,
                    "maxQty": self.max_qty,
                    "stepSize": self.step_size,
                },
                {"filterType": "MIN_NOTIONAL", "notional": self.min_notional},
            ]
        }


class FakeLiveExecutionClient(FakeClient):
    def __init__(
        self,
        open_response,
        order_response=None,
        position_entry=None,
        leverage=10,
        position_quantities=None,
        close_side_effects=None,
        protection_side_effects=None,
    ):
        super().__init__(leverage=leverage)
        self.has_credentials = True
        self.open_response = open_response
        self.order_response = order_response
        self.position_entry = position_entry
        self.position_quantities = list(position_quantities or [])
        self.close_side_effects = list(close_side_effects or [])
        self.protection_side_effects = list(protection_side_effects or [])
        self.protection_calls = []
        self.close_calls = []
        self.leverage_calls = []
        self.market_calls = []
        self.market_client_order_ids = []
        self.algo_orders = {}
        self.algo_client_index = {}
        self.cancel_algo_calls = []
        self.cancel_all_algo_calls = []
        self.next_algo_id = 1000
        self.last_market_client_order_id = None
        self.last_position_quantity = None

    def set_leverage(self, symbol, leverage):
        self.leverage_calls.append((symbol, leverage))
        return {"symbol": symbol, "leverage": leverage}

    def place_market_order(self, symbol, side, quantity, client_order_id=None):
        self.market_calls.append((symbol, side, quantity))
        self.market_client_order_ids.append(client_order_id)
        self.last_market_client_order_id = client_order_id
        response = dict(self.open_response)
        if client_order_id is not None:
            response.setdefault("clientOrderId", client_order_id)
        response.setdefault("symbol", symbol)
        response.setdefault("side", side)
        response.setdefault("type", "MARKET")
        response.setdefault("executedQty", str(quantity))
        response.setdefault("status", "FILLED")
        return response

    def place_close_all_algo_order(
        self,
        symbol,
        order_type,
        trigger_price,
        client_algo_id=None,
    ):
        self.protection_calls.append((order_type, trigger_price))
        if self.protection_side_effects:
            result = self.protection_side_effects.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        self.next_algo_id += 1
        response = {
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
        self.algo_orders[self.next_algo_id] = dict(response)
        if client_algo_id:
            self.algo_client_index[client_algo_id] = self.next_algo_id
        return dict(response)

    def close_position_market(self, symbol, quantity):
        self.close_calls.append((symbol, quantity))
        if self.close_side_effects:
            result = self.close_side_effects.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        return {"symbol": symbol, "closed": True}

    def get_order(self, symbol, order_id=None, orig_client_order_id=None):
        if orig_client_order_id is not None:
            response = dict(self.open_response)
            if response:
                response.setdefault("clientOrderId", orig_client_order_id)
                response.setdefault("symbol", symbol)
                response.setdefault("side", "BUY")
                response.setdefault("type", "MARKET")
                response.setdefault("status", "FILLED")
                response.setdefault(
                    "executedQty", str(response.get("origQty", "1"))
                )
                return response
            raise BinanceAPIError("client order unavailable")
        if self.order_response is None:
            raise BinanceAPIError("order fill unavailable")
        response = dict(self.order_response)
        response.setdefault("orderId", order_id)
        response.setdefault("clientOrderId", self.last_market_client_order_id)
        response.setdefault("symbol", symbol)
        response.setdefault("side", "BUY")
        response.setdefault("type", "MARKET")
        response.setdefault("status", "FILLED")
        response.setdefault("executedQty", "1")
        return response

    def get_algo_order(self, algo_id=None, client_algo_id=None):
        if algo_id is None and client_algo_id is not None:
            algo_id = self.algo_client_index.get(client_algo_id)
        if algo_id not in self.algo_orders:
            raise BinanceAPIError("algo order unavailable")
        return dict(self.algo_orders[algo_id])

    def cancel_algo_order(self, algo_id=None, client_algo_id=None):
        if algo_id is None and client_algo_id is not None:
            algo_id = self.algo_client_index.get(client_algo_id)
        self.cancel_algo_calls.append((algo_id, client_algo_id))
        if algo_id not in self.algo_orders:
            raise BinanceAPIError("algo order unavailable")
        self.algo_orders[algo_id]["algoStatus"] = "CANCELED"
        return dict(self.algo_orders[algo_id])

    def get_open_algo_orders(self, symbol):
        return [
            dict(order)
            for order in self.algo_orders.values()
            if order.get("symbol") == symbol
            and str(order.get("algoStatus", "")).upper()
            not in {"CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FINISHED"}
        ]

    def cancel_all_algo_open_orders(self, symbol):
        self.cancel_all_algo_calls.append(symbol)
        for order in self.algo_orders.values():
            if order.get("symbol") == symbol:
                order["algoStatus"] = "CANCELED"
        return {"code": 200, "symbol": symbol}

    def get_long_position_entry_price(self, symbol):
        if self.position_entry is None:
            raise BinanceAPIError("position fill unavailable")
        return Decimal(str(self.position_entry))

    def get_long_position_quantity(self, symbol):
        if self.position_quantities:
            result = self.position_quantities.pop(0)
            if isinstance(result, Exception):
                raise result
            self.last_position_quantity = Decimal(str(result))
            return self.last_position_quantity
        if self.last_position_quantity is not None:
            return self.last_position_quantity
        executed_quantity = Decimal(str(self.open_response.get("executedQty", "0")))
        if executed_quantity > 0:
            return executed_quantity
        raise BinanceAPIError("position quantity unavailable")

    def get_long_position_quantity_or_zero(self, symbol):
        try:
            return self.get_long_position_quantity(symbol)
        except BinanceAPIError:
            return Decimal("0")


class FakeCloseResolutionClient(FakeClient):
    def __init__(self, algo_responses, order_responses):
        super().__init__()
        self.algo_responses = algo_responses
        self.order_responses = order_responses

    def get_algo_order(self, algo_id=None, client_algo_id=None):
        response = self.algo_responses[algo_id]
        if isinstance(response, Exception):
            raise response
        response = dict(response)
        response.setdefault("algoId", algo_id)
        response.setdefault("symbol", "AAAUSDT")
        response.setdefault("algoType", "CONDITIONAL")
        response.setdefault(
            "orderType", "STOP_MARKET" if algo_id == 11 else "TAKE_PROFIT_MARKET"
        )
        response.setdefault("side", "SELL")
        response.setdefault("closePosition", True)
        return response

    def get_order(self, symbol, order_id=None, orig_client_order_id=None):
        response = self.order_responses[order_id]
        if isinstance(response, Exception):
            raise response
        response = dict(response)
        response.setdefault("orderId", order_id)
        response.setdefault("symbol", symbol)
        response.setdefault("side", "SELL")
        return response

    def cancel_algo_order(self, algo_id=None, client_algo_id=None):
        if algo_id not in self.algo_responses:
            raise BinanceAPIError("algo order unavailable")
        response = self.algo_responses[algo_id]
        if isinstance(response, Exception):
            raise response
        response["algoStatus"] = "CANCELED"
        return self.get_algo_order(algo_id=algo_id)

    def get_open_algo_orders(self, symbol):
        rows = []
        for algo_id, response in self.algo_responses.items():
            if isinstance(response, Exception):
                continue
            if str(response.get("algoStatus", "")).upper() not in {
                "CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FINISHED"
            }:
                rows.append(self.get_algo_order(algo_id=algo_id))
        return rows

    def cancel_all_algo_open_orders(self, symbol):
        for response in self.algo_responses.values():
            if isinstance(response, dict):
                response["algoStatus"] = "CANCELED"
        return {"code": 200, "symbol": symbol}

    def get_mark_price(self, symbol):
        raise AssertionError("closed live result must not use the current mark price")


def test_config(dry_run_account_file="state/dry_run_account.json"):
    return Config(
        base_url="https://fapi.binance.com",
        api_key="",
        api_secret="",
        dry_run=True,
        live_confirmation="",
        dry_run_balance_usdt=Decimal("1000"),
        poll_interval_seconds=60,
        funding_rate_abs_threshold=Decimal("0.015"),
        kline_interval="15m",
        kline_limit=100,
        trend_window=96,
        dry_run_max_leverage=50,
        dry_run_tradifi_max_leverage=20,
        risk_balance_fraction=Decimal("0.20"),
        take_profit_r_multiple=Decimal("5"),
        stop_loss_amplitude_ratio=Decimal("0.12"),
        min_stop_loss_pct=Decimal("0.01"),
        max_stop_loss_pct=Decimal("0.05"),
        max_margin_balance_fraction=Decimal("0.95"),
        symbol_cooldown_hours=4,
        state_file="state/position.json",
        dry_run_account_file=dry_run_account_file,
        log_file="logs/trading.log",
        review_db_file="data/trading_review.sqlite3",
    )


def live_test_config(dry_run_account_file="state/dry_run_account.json"):
    return Config(
        **{
            **test_config(dry_run_account_file).__dict__,
            "api_key": "key",
            "api_secret": "secret",
            "dry_run": False,
            "live_confirmation": LIVE_TRADING_CONFIRMATION,
        }
    )


class TraderTests(unittest.TestCase):
    def _assert_cleanup_audit_state(
        self,
        state_store,
        trader,
        strategy_id,
        *,
        protection_pending=False,
    ):
        state = state_store.load()
        self.assertIsNotNone(state)
        self.assertEqual(state.orders["strategy"]["strategy_id"], strategy_id)
        if protection_pending:
            pending = state.orders["emergency_cleanup_pending"]
            self.assertTrue(pending["emergency_cleanup"]["confirmed_closed"])
            self.assertFalse(
                pending["protection_cleanup"]["confirmed_all_canceled"]
            )
            self.assertTrue(
                any(
                    order.get("submission_attempted")
                    for order in pending["placed_protection_orders"]
                )
            )
            sync_result = trader.sync_state_with_exchange()
            self.assertTrue(sync_result.pending_resolved)
            self.assertFalse(sync_result.has_position)
            self.assertTrue(
                sync_result.pending_detail["final_protection_cleanup"][
                    "confirmed_all_canceled"
                ]
            )
        else:
            resolved = state.orders["execution_cleanup_resolved"]
            self.assertEqual(state.quantity, "0")
            self.assertTrue(resolved["emergency_cleanup"]["confirmed_closed"])
            self.assertTrue(
                resolved["protection_cleanup"]["confirmed_all_canceled"]
            )
            sync_result = trader.sync_state_with_exchange()
            self.assertTrue(sync_result.pending_resolved)
            self.assertEqual(
                sync_result.pending_detail["reason"],
                "EMERGENCY_CLEANUP_CONFIRMED_FLAT_WITHOUT_LIVE_RESULT",
            )
        self.assertIsNotNone(state_store.load())

    def _closed_live_state(self):
        return PositionState(
            symbol="AAAUSDT",
            quantity="10",
            entry_price="100",
            stop_loss_price="95",
            take_profit_price="125",
            leverage=10,
            opened_at="2026-07-10T10:00:00+00:00",
            dry_run=False,
            orders={
                "stop": {"algoId": 11},
                "take_profit": {"algoId": 22},
                "strategy": {"strategy_id": "N01"},
            },
        )

    def test_closed_live_stop_uses_filled_algo_order_after_price_rebound(self):
        client = FakeCloseResolutionClient(
            {
                11: {"algoId": 11, "algoStatus": "FINISHED", "actualOrderId": 111},
                22: {"algoId": 22, "algoStatus": "EXPIRED", "actualOrderId": ""},
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
            StateStore("/tmp/unused-live-resolution-state.json"),
            logging.getLogger("test_live_resolution"),
        )

        result = trader.resolve_closed_live_position(self._closed_live_state())

        self.assertEqual(result.result, "LOSS")
        self.assertEqual(result.exit_reason, "STOP_LOSS")
        self.assertEqual(result.exit_price, Decimal("94.9"))

    def test_closed_live_stop_accepts_canonical_cross_type_order_ids(self):
        for actual_order_id, response_order_id in (("111", 111), (111, "111")):
            with self.subTest(
                actual_order_id=actual_order_id,
                response_order_id=response_order_id,
            ):
                client = FakeCloseResolutionClient(
                    {
                        11: {
                            "algoId": 11,
                            "algoStatus": "FINISHED",
                            "actualOrderId": actual_order_id,
                        },
                        22: {
                            "algoId": 22,
                            "algoStatus": "EXPIRED",
                            "actualOrderId": "",
                        },
                    },
                    {
                        111: {
                            "orderId": response_order_id,
                            "status": "FILLED",
                            "executedQty": "10",
                            "avgPrice": "94.9",
                        }
                    },
                )
                trader = Trader(
                    client,
                    live_test_config(),
                    StateStore("/tmp/unused-live-resolution-cross-type.json"),
                    logging.getLogger("test_live_resolution_cross_type"),
                )

                result = trader.resolve_closed_live_position(self._closed_live_state())

                self.assertEqual(result.result, "LOSS")
                self.assertEqual(result.exit_reason, "STOP_LOSS")

    def test_closed_live_rejects_invalid_order_ids_and_identity_mismatches(self):
        class StringSubclass(str):
            pass

        trader = Trader(
            FakeCloseResolutionClient({}, {}),
            live_test_config(),
            StateStore("/tmp/unused-live-resolution-invalid-id.json"),
            logging.getLogger("test_live_resolution_invalid_id"),
        )
        invalid_ids = (
            None,
            "",
            False,
            True,
            0,
            "0",
            "00",
            "00123",
            -1,
            "-1",
            1.0,
            "+1",
            " 1",
            "1 ",
            "abc",
            StringSubclass("111"),
        )
        for value in invalid_ids:
            with self.subTest(value=value):
                self.assertIsNone(trader._normalized_exchange_order_id(value))

        for response in (
            {"orderId": "112", "status": "FILLED", "executedQty": "10"},
            {"orderId": "111", "symbol": "OTHERUSDT", "status": "FILLED", "executedQty": "10"},
            {"orderId": "111", "side": "BUY", "status": "FILLED", "executedQty": "10"},
        ):
            with self.subTest(response=response):
                client = FakeCloseResolutionClient(
                    {
                        11: {
                            "algoId": 11,
                            "algoStatus": "FINISHED",
                            "actualOrderId": "111",
                        },
                        22: {
                            "algoId": 22,
                            "algoStatus": "EXPIRED",
                            "actualOrderId": "",
                        },
                    },
                    {111: response},
                )
                rejected = Trader(
                    client,
                    live_test_config(),
                    StateStore("/tmp/unused-live-resolution-mismatch.json"),
                    logging.getLogger("test_live_resolution_mismatch"),
                ).resolve_closed_live_position(self._closed_live_state())
                self.assertEqual(rejected.result, "LIVE_RESULT_PENDING")
                self.assertEqual(
                    rejected.detail["reason"], "PROTECTION_ORDER_QUERY_FAILED"
                )

    def test_closed_live_rejects_hostile_actual_order_id_string_subclass(self):
        class EmptyStringImpersonator(str):
            def __eq__(self, other):
                return other == ""

            def __ne__(self, other):
                return False

        client = FakeCloseResolutionClient(
            {
                11: {
                    "algoId": 11,
                    "algoStatus": "FINISHED",
                    "actualOrderId": EmptyStringImpersonator("111"),
                    # This must not be consulted through the actualQty fallback.
                    "actualQty": "10",
                    "actualPrice": "94.9",
                },
                22: {
                    "algoId": 22,
                    "algoStatus": "EXPIRED",
                    "actualOrderId": "",
                },
            },
            {111: {"orderId": 111, "status": "FILLED", "executedQty": "10"}},
        )

        result = Trader(
            client,
            live_test_config(),
            StateStore("/tmp/unused-live-resolution-hostile-id.json"),
            logging.getLogger("test_live_resolution_hostile_id"),
        ).resolve_closed_live_position(self._closed_live_state())

        self.assertEqual(result.result, "LIVE_RESULT_PENDING")
        self.assertEqual(result.detail["reason"], "PROTECTION_ORDER_QUERY_FAILED")

    def test_closed_live_take_profit_uses_filled_algo_order_after_price_retrace(self):
        client = FakeCloseResolutionClient(
            {
                11: {"algoId": 11, "algoStatus": "EXPIRED", "actualOrderId": ""},
                22: {"algoId": 22, "algoStatus": "FINISHED", "actualOrderId": 222},
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
            StateStore("/tmp/unused-live-resolution-state.json"),
            logging.getLogger("test_live_resolution"),
        )

        result = trader.resolve_closed_live_position(self._closed_live_state())

        self.assertEqual(result.result, "WIN")
        self.assertEqual(result.exit_reason, "TAKE_PROFIT")
        self.assertEqual(result.exit_price, Decimal("125.1"))

    def test_closed_live_query_failure_is_pending(self):
        client = FakeCloseResolutionClient(
            {11: BinanceAPIError("algo query unavailable")},
            {},
        )
        trader = Trader(
            client,
            live_test_config(),
            StateStore("/tmp/unused-live-resolution-state.json"),
            logging.getLogger("test_live_resolution"),
        )

        result = trader.resolve_closed_live_position(self._closed_live_state())

        self.assertEqual(result.result, "LIVE_RESULT_PENDING")
        self.assertEqual(result.detail["reason"], "PROTECTION_ORDER_QUERY_FAILED")

    def test_closed_live_without_filled_protection_is_manual_or_external(self):
        client = FakeCloseResolutionClient(
            {
                11: {"algoId": 11, "algoStatus": "CANCELED", "actualOrderId": ""},
                22: {"algoId": 22, "algoStatus": "EXPIRED", "actualOrderId": ""},
            },
            {},
        )
        trader = Trader(
            client,
            live_test_config(),
            StateStore("/tmp/unused-live-resolution-state.json"),
            logging.getLogger("test_live_resolution"),
        )

        result = trader.resolve_closed_live_position(self._closed_live_state())

        self.assertEqual(result.result, "MANUAL_OR_EXTERNAL_CLOSE")
        self.assertEqual(result.exit_reason, "MANUAL_OR_EXTERNAL_CLOSE")

    def test_conflicting_filled_protection_orders_are_serializable_pending(self):
        client = FakeCloseResolutionClient(
            {
                11: {"algoId": 11, "algoStatus": "FINISHED", "actualOrderId": 111},
                22: {"algoId": 22, "algoStatus": "FINISHED", "actualOrderId": 222},
            },
            {
                111: {"status": "FILLED", "executedQty": "10", "avgPrice": "94.9"},
                222: {"status": "FILLED", "executedQty": "10", "avgPrice": "125.1"},
            },
        )
        trader = Trader(
            client,
            live_test_config(),
            StateStore("/tmp/unused-live-resolution-state.json"),
            logging.getLogger("test_live_resolution"),
        )

        result = trader.resolve_closed_live_position(self._closed_live_state())

        self.assertEqual(result.result, "LIVE_RESULT_PENDING")
        json.dumps(result.detail)

    def test_trade_plan_uses_amplitude_stop_and_risk_sized_quantity(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                FakeClient(leverage=10),
                test_config(f"{tmpdir}/dry_run_account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_trader"),
            )
            plan = trader.build_trade_plan("AAAUSDT", Decimal("120"))

            self.assertEqual(plan.leverage, 10)
            self.assertEqual(plan.amplitude_24h_pct, Decimal("0.3"))
            self.assertEqual(plan.stop_loss_pct, Decimal("0.036"))
            self.assertEqual(plan.take_profit_pct, Decimal("0.180"))
            self.assertEqual(plan.risk_amount, Decimal("200.00"))
            self.assertEqual(plan.quantity, Decimal("46.296"))
            self.assertEqual(plan.stop_loss_price, Decimal("115.68"))
            self.assertEqual(plan.take_profit_price, Decimal("141.60"))

    def test_trade_plan_skips_when_margin_is_not_enough(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                FakeClient(leverage=5),
                test_config(f"{tmpdir}/dry_run_account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_trader"),
            )

            with self.assertRaises(BinanceAPIError):
                trader.build_trade_plan("AAAUSDT", Decimal("120"))

    def test_n06_structure_plan_uses_p1_and_exact_five_r_take_profit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeClient(leverage=10)
            client.get_24hr_ticker = lambda symbol: self.fail("N06 must not read 24h amplitude")
            trader = Trader(
                client,
                test_config(f"{tmpdir}/dry_run_account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_trader"),
            )

            plan = trader.build_structure_trade_plan(
                "AAAUSDT",
                Decimal("120"),
                Decimal("112"),
                Decimal("5"),
                structure_id="structure-1",
            )

            self.assertEqual(plan.stop_mode, "structure_p1")
            self.assertEqual(plan.stop_loss_price, Decimal("112"))
            self.assertEqual(plan.take_profit_price, Decimal("160"))
            self.assertEqual(plan.quantity, Decimal("25"))
            self.assertEqual(plan.amplitude_24h_pct, Decimal("0"))
            self.assertEqual(plan.structure_id, "structure-1")

    def test_structure_plan_rounds_non_tick_take_profit_up(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                FakeClient(leverage=10),
                test_config(f"{tmpdir}/dry_run_account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_trader"),
            )
            requested_r = Decimal("5.0001")

            plan = trader.build_structure_trade_plan(
                "AAAUSDT",
                Decimal("120"),
                Decimal("112"),
                requested_r,
            )

            final_r = (plan.take_profit_price - plan.entry_price) / (
                plan.entry_price - plan.stop_loss_price
            )
            self.assertEqual(plan.take_profit_price, Decimal("160.01"))
            self.assertGreaterEqual(final_r, requested_r)

    def test_n06_structure_plan_rejects_p1_at_or_above_entry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                FakeClient(leverage=10),
                test_config(f"{tmpdir}/dry_run_account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_trader"),
            )

            with self.assertRaisesRegex(BinanceAPIError, "Invalid structure risk distance"):
                trader.build_structure_trade_plan(
                    "AAAUSDT",
                    Decimal("120"),
                    Decimal("120"),
                    Decimal("5"),
                )

    def test_n07_uses_full_target_risk_when_leverage_is_sufficient(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                RuleConstrainedClient(leverage=50),
                test_config(f"{tmpdir}/dry_run_account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_n07_full_risk"),
            )

            plan = trader.build_margin_capped_structure_trade_plan(
                "AAAUSDT",
                Decimal("113.344"),
                Decimal("112"),
                Decimal("5"),
                structure_id="n07-full-risk",
            )

            final_r = (plan.take_profit_price - plan.entry_price) / (
                plan.entry_price - plan.stop_loss_price
            )
            self.assertEqual(plan.stop_mode, "structure_p1_margin_capped")
            self.assertEqual(plan.stop_loss_price, Decimal("112"))
            self.assertEqual(plan.target_risk_amount, Decimal("200.00"))
            self.assertLessEqual(plan.actual_risk_amount, plan.target_risk_amount)
            self.assertLess(plan.target_risk_amount - plan.actual_risk_amount, Decimal("0.01"))
            self.assertFalse(plan.risk_capped_by_margin)
            self.assertGreaterEqual(final_r, Decimal("5"))

    def test_n07_low_leverage_scales_quantity_to_margin_cap(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                RuleConstrainedClient(leverage=5),
                test_config(f"{tmpdir}/dry_run_account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_n07_margin_cap"),
            )

            plan = trader.build_margin_capped_structure_trade_plan(
                "AAAUSDT",
                Decimal("113.344"),
                Decimal("112"),
                Decimal("5"),
                structure_id="n07-margin-capped",
            )

            target_quantity = plan.target_risk_amount / (
                plan.entry_price - plan.stop_loss_price
            )
            self.assertLess(plan.quantity, target_quantity)
            self.assertLessEqual(plan.required_margin, plan.balance * Decimal("0.95"))
            self.assertLessEqual(plan.actual_risk_amount, plan.target_risk_amount)
            self.assertTrue(plan.risk_capped_by_margin)

    def test_n07_margin_capped_plan_rejects_exchange_minimums(self):
        cases = (
            (
                RuleConstrainedClient(leverage=5, min_qty="50"),
                "below minQty",
            ),
            (
                RuleConstrainedClient(leverage=5, min_notional="5000"),
                "below minNotional",
            ),
        )
        for client, error in cases:
            with self.subTest(error=error), tempfile.TemporaryDirectory() as tmpdir:
                trader = Trader(
                    client,
                    test_config(f"{tmpdir}/dry_run_account.json"),
                    StateStore(f"{tmpdir}/state.json"),
                    logging.getLogger("test_n07_exchange_minimum"),
                )

                with self.assertRaisesRegex(BinanceAPIError, error):
                    trader.build_margin_capped_structure_trade_plan(
                        "AAAUSDT",
                        Decimal("113.344"),
                        Decimal("112"),
                        Decimal("5"),
                    )

    def test_n08_amplitude_stop_clamps_to_one_and_five_percent(self):
        cases = (
            ("100.5", "100", Decimal("0.01")),
            ("130", "100", Decimal("0.036")),
            ("200", "100", Decimal("0.05")),
        )
        for high, low, expected_stop_pct in cases:
            with self.subTest(high=high, low=low), tempfile.TemporaryDirectory() as tmpdir:
                client = RuleConstrainedClient(leverage=50)
                client.get_24hr_ticker = lambda symbol, h=high, l=low: {
                    "highPrice": h,
                    "lowPrice": l,
                }
                trader = Trader(
                    client,
                    test_config(f"{tmpdir}/dry_run_account.json"),
                    StateStore(f"{tmpdir}/state.json"),
                    logging.getLogger("test_n08_amplitude_clamp"),
                )

                plan = trader.build_amplitude_margin_capped_trade_plan(
                    "AAAUSDT",
                    Decimal("100"),
                    Decimal("5"),
                )

                self.assertEqual(plan.stop_loss_pct, expected_stop_pct)
                self.assertGreaterEqual(
                    (plan.take_profit_price - plan.entry_price)
                    / (plan.entry_price - plan.stop_loss_price),
                    Decimal("5"),
                )

    def test_n08_sufficient_leverage_uses_nearly_full_target_risk(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = RuleConstrainedClient(leverage=50)
            client.get_24hr_ticker = lambda symbol: {
                "highPrice": "100.5",
                "lowPrice": "100",
            }
            trader = Trader(
                client,
                test_config(f"{tmpdir}/dry_run_account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_n08_full_risk"),
            )

            plan = trader.build_amplitude_margin_capped_trade_plan(
                "AAAUSDT",
                Decimal("100"),
                Decimal("5"),
                structure_id="n08-range",
            )

            self.assertEqual(plan.stop_mode, "amplitude_margin_capped")
            self.assertEqual(plan.stop_loss_price, Decimal("99"))
            self.assertEqual(plan.take_profit_price, Decimal("105"))
            self.assertEqual(plan.quantity, Decimal("200"))
            self.assertEqual(plan.target_risk_amount, Decimal("200.00"))
            self.assertEqual(plan.actual_risk_amount, Decimal("200"))
            self.assertFalse(plan.risk_capped_by_margin)

    def test_n08_low_leverage_scales_to_margin_cap_instead_of_rejecting(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = RuleConstrainedClient(leverage=5)
            client.get_24hr_ticker = lambda symbol: {
                "highPrice": "100.5",
                "lowPrice": "100",
            }
            trader = Trader(
                client,
                test_config(f"{tmpdir}/dry_run_account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_n08_margin_cap"),
            )

            plan = trader.build_amplitude_margin_capped_trade_plan(
                "AAAUSDT",
                Decimal("100"),
                Decimal("5"),
            )

            self.assertEqual(plan.quantity, Decimal("47.5"))
            self.assertTrue(plan.risk_capped_by_margin)
            self.assertLess(plan.actual_risk_amount, plan.target_risk_amount)
            self.assertLessEqual(plan.required_margin, Decimal("950"))

    def test_n08_margin_capped_plan_rejects_exchange_minimums(self):
        cases = (
            (RuleConstrainedClient(leverage=5, min_qty="50"), "below minQty"),
            (RuleConstrainedClient(leverage=5, min_notional="5000"), "below minNotional"),
        )
        for client, expected_error in cases:
            with self.subTest(error=expected_error), tempfile.TemporaryDirectory() as tmpdir:
                client.get_24hr_ticker = lambda symbol: {
                    "highPrice": "100.5",
                    "lowPrice": "100",
                }
                trader = Trader(
                    client,
                    test_config(f"{tmpdir}/dry_run_account.json"),
                    StateStore(f"{tmpdir}/state.json"),
                    logging.getLogger("test_n08_exchange_minimum"),
                )

                with self.assertRaisesRegex(BinanceAPIError, expected_error):
                    trader.build_amplitude_margin_capped_trade_plan(
                        "AAAUSDT",
                        Decimal("100"),
                        Decimal("5"),
                    )

    def test_n09_stop_percent_closed_boundaries_and_outside_rejection(self):
        accepted = (("105", Decimal("0.01")), ("125", Decimal("0.05")))
        for s1, expected_stop_pct in accepted:
            with self.subTest(s1=s1), tempfile.TemporaryDirectory() as tmpdir:
                trader = Trader(
                    RuleConstrainedClient(leverage=50),
                    test_config(f"{tmpdir}/dry_run_account.json"),
                    StateStore(f"{tmpdir}/state.json"),
                    logging.getLogger("test_n09_stop_boundary"),
                )
                plan = trader.build_s1_target_margin_capped_trade_plan(
                    "AAAUSDT", Decimal("100"), Decimal(s1), "n09-boundary"
                )

                self.assertEqual(plan.stop_loss_pct, expected_stop_pct)
                self.assertEqual(plan.take_profit_price, Decimal(s1))
                self.assertGreaterEqual(
                    (plan.take_profit_price - plan.entry_price)
                    / (plan.entry_price - plan.stop_loss_price),
                    Decimal("5"),
                )

        for s1 in ("104.9", "125.1"):
            with self.subTest(rejected_s1=s1), tempfile.TemporaryDirectory() as tmpdir:
                trader = Trader(
                    RuleConstrainedClient(leverage=50),
                    test_config(f"{tmpdir}/dry_run_account.json"),
                    StateStore(f"{tmpdir}/state.json"),
                    logging.getLogger("test_n09_stop_rejected"),
                )
                with self.assertRaisesRegex(BinanceAPIError, "N09_STOP_PCT_OUT_OF_RANGE"):
                    trader.build_s1_target_margin_capped_trade_plan(
                        "AAAUSDT", Decimal("100"), Decimal(s1)
                    )

    def test_n09_sufficient_and_low_leverage_position_sizing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            full = Trader(
                RuleConstrainedClient(leverage=50),
                test_config(f"{tmpdir}/full-account.json"),
                StateStore(f"{tmpdir}/full-state.json"),
                logging.getLogger("test_n09_full_risk"),
            ).build_s1_target_margin_capped_trade_plan(
                "AAAUSDT", Decimal("100"), Decimal("120")
            )
            capped = Trader(
                RuleConstrainedClient(leverage=1),
                test_config(f"{tmpdir}/capped-account.json"),
                StateStore(f"{tmpdir}/capped-state.json"),
                logging.getLogger("test_n09_margin_cap"),
            ).build_s1_target_margin_capped_trade_plan(
                "AAAUSDT", Decimal("100"), Decimal("120")
            )

            self.assertEqual(full.actual_risk_amount, Decimal("200"))
            self.assertFalse(full.risk_capped_by_margin)
            self.assertEqual(capped.quantity, Decimal("9.5"))
            self.assertTrue(capped.risk_capped_by_margin)
            self.assertLess(capped.actual_risk_amount, capped.target_risk_amount)
            self.assertLessEqual(capped.required_margin, Decimal("950"))

    def test_n09_margin_capped_plan_rejects_exchange_minimums(self):
        cases = (
            (RuleConstrainedClient(leverage=1, min_qty="10"), "below minQty"),
            (RuleConstrainedClient(leverage=1, min_notional="1000"), "below minNotional"),
        )
        for client, expected_error in cases:
            with self.subTest(error=expected_error), tempfile.TemporaryDirectory() as tmpdir:
                trader = Trader(
                    client,
                    test_config(f"{tmpdir}/dry_run_account.json"),
                    StateStore(f"{tmpdir}/state.json"),
                    logging.getLogger("test_n09_exchange_minimum"),
                )
                with self.assertRaisesRegex(BinanceAPIError, expected_error):
                    trader.build_s1_target_margin_capped_trade_plan(
                        "AAAUSDT", Decimal("100"), Decimal("120")
                    )

    def test_live_n09_adverse_fill_reduces_and_uses_s1_as_target(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"orderId": 901, "avgPrice": "99", "executedQty": "50"},
                leverage=50,
                position_quantities=["50", "47.619"],
                close_side_effects=[
                    {"status": "FILLED", "executedQty": "2.381", "orderId": 902},
                ],
            )
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                state_store,
                logging.getLogger("test_live_n09_actual_fill"),
            )
            plan = trader.build_s1_target_margin_capped_trade_plan(
                "AAAUSDT", Decimal("100"), Decimal("120"), "n09-live"
            )

            state = trader.open_long_plan_with_protection(plan)

            entry = Decimal(state.entry_price)
            stop = Decimal(state.stop_loss_price)
            take_profit = Decimal(state.take_profit_price)
            self.assertEqual(state.quantity, "47.619")
            self.assertEqual(entry, Decimal("99"))
            self.assertEqual(stop, Decimal("94.8"))
            self.assertEqual(take_profit, Decimal("120"))
            self.assertGreaterEqual((take_profit - entry) / (entry - stop), Decimal("5"))
            self.assertEqual(client.close_calls, [("AAAUSDT", Decimal("2.381"))])
            self.assertLessEqual(
                Decimal(state.orders["plan"]["post_fill_actual_risk_amount"]),
                Decimal("200"),
            )
            self.assertLessEqual(
                Decimal(state.orders["plan"]["post_fill_required_margin"]),
                Decimal("950"),
            )
            self.assertEqual(state.orders["post_fill_adjustment"]["strategy"], "N09")

    def test_live_n09_reduction_confirmation_failure_cleans_full_position(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"orderId": 911, "avgPrice": "99", "executedQty": "50"},
                leverage=50,
                position_quantities=[
                    "50",
                    BinanceAPIError("final position unavailable"),
                    "0",
                ],
                close_side_effects=[
                    {"status": "FILLED", "executedQty": "2.381", "orderId": 912},
                    {"status": "FILLED", "executedQty": "50", "orderId": 913},
                ],
            )
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                state_store,
                logging.getLogger("test_live_n09_cleanup"),
            )
            plan = trader.build_s1_target_margin_capped_trade_plan(
                "AAAUSDT", Decimal("100"), Decimal("120"), "n09-cleanup"
            )

            with self.assertRaisesRegex(BinanceAPIError, "post-fill adjustment or protection failed"):
                trader.open_long_plan_with_protection(plan)

            self.assertEqual(
                client.close_calls,
                [("AAAUSDT", Decimal("2.381")), ("AAAUSDT", Decimal("50"))],
            )
            self.assertEqual(client.protection_calls, [])
            self._assert_cleanup_audit_state(
                state_store, trader, "N09"
            )

    def test_n10_stop_is_one_tick_below_sweep_and_stop_pct_boundaries(self):
        accepted = (
            (Decimal("99.01"), Decimal("99"), Decimal("0.01")),
            (Decimal("95.01"), Decimal("95"), Decimal("0.05")),
        )
        for sweep_low, expected_stop, expected_pct in accepted:
            with self.subTest(sweep_low=sweep_low), tempfile.TemporaryDirectory() as tmpdir:
                trader = Trader(
                    RuleConstrainedClient(leverage=50),
                    test_config(f"{tmpdir}/account.json"),
                    StateStore(f"{tmpdir}/state.json"),
                    logging.getLogger("test_n10_stop_boundary"),
                )
                plan = trader.build_sweep_low_margin_capped_trade_plan(
                    "AAAUSDT",
                    Decimal("100"),
                    sweep_low,
                    Decimal("5"),
                    "n10-boundary",
                    entry_min_price=Decimal("100"),
                    entry_max_price=Decimal("101.5"),
                )
                self.assertEqual(plan.stop_loss_price, expected_stop)
                self.assertEqual(plan.stop_loss_pct, expected_pct)
                self.assertEqual(plan.stop_mode, "sweep_low_tick_margin_capped")
                self.assertGreaterEqual(
                    (plan.take_profit_price - plan.entry_price)
                    / (plan.entry_price - plan.stop_loss_price),
                    Decimal("5"),
                )

        for sweep_low in (Decimal("99.02"), Decimal("95.00")):
            with self.subTest(rejected=sweep_low), tempfile.TemporaryDirectory() as tmpdir:
                trader = Trader(
                    RuleConstrainedClient(leverage=50),
                    test_config(f"{tmpdir}/account.json"),
                    StateStore(f"{tmpdir}/state.json"),
                    logging.getLogger("test_n10_stop_reject"),
                )
                with self.assertRaisesRegex(BinanceAPIError, "N10_STOP_PCT_OUT_OF_RANGE"):
                    trader.build_sweep_low_margin_capped_trade_plan(
                        "AAAUSDT",
                        Decimal("100"),
                        sweep_low,
                        Decimal("5"),
                        entry_min_price=Decimal("100"),
                        entry_max_price=Decimal("101.5"),
                    )

    def test_n10_low_leverage_scales_and_exchange_minimums_reject(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            trader = Trader(
                RuleConstrainedClient(leverage=1),
                test_config(f"{tmpdir}/account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_n10_margin_cap"),
            )
            plan = trader.build_sweep_low_margin_capped_trade_plan(
                "AAAUSDT",
                Decimal("100"),
                Decimal("97"),
                Decimal("5"),
                entry_min_price=Decimal("100"),
                entry_max_price=Decimal("101.5"),
            )
            self.assertEqual(plan.quantity, Decimal("9.5"))
            self.assertTrue(plan.risk_capped_by_margin)
            self.assertLess(plan.actual_risk_amount, plan.target_risk_amount)
            self.assertLessEqual(plan.required_margin, Decimal("950"))

        cases = (
            (RuleConstrainedClient(leverage=1, min_qty="10"), "below minQty"),
            (RuleConstrainedClient(leverage=1, min_notional="1000"), "below minNotional"),
        )
        for client, expected in cases:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as tmpdir:
                trader = Trader(
                    client,
                    test_config(f"{tmpdir}/account.json"),
                    StateStore(f"{tmpdir}/state.json"),
                    logging.getLogger("test_n10_minimum"),
                )
                with self.assertRaisesRegex(BinanceAPIError, expected):
                    trader.build_sweep_low_margin_capped_trade_plan(
                        "AAAUSDT",
                        Decimal("100"),
                        Decimal("97"),
                        Decimal("5"),
                        entry_min_price=Decimal("100"),
                        entry_max_price=Decimal("101.5"),
                    )

    def test_live_n10_adverse_fill_reduces_and_rebuilds_five_r(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"orderId": 1001, "avgPrice": "101", "executedQty": "66.445"},
                leverage=50,
                position_quantities=["66.445", "49.875"],
                close_side_effects=[
                    {"status": "FILLED", "executedQty": "16.570", "orderId": 1002},
                ],
            )
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/account.json"),
                state_store,
                logging.getLogger("test_live_n10_fill"),
            )
            plan = trader.build_sweep_low_margin_capped_trade_plan(
                "AAAUSDT",
                Decimal("100"),
                Decimal("97"),
                Decimal("5"),
                "n10-live",
                entry_min_price=Decimal("100"),
                entry_max_price=Decimal("101.5"),
            )

            state = trader.open_long_plan_with_protection(plan)

            entry = Decimal(state.entry_price)
            stop = Decimal(state.stop_loss_price)
            take_profit = Decimal(state.take_profit_price)
            self.assertEqual(state.quantity, "49.875")
            self.assertEqual(stop, Decimal("96.99"))
            self.assertEqual(take_profit, Decimal("121.05"))
            self.assertGreaterEqual((take_profit - entry) / (entry - stop), Decimal("5"))
            self.assertEqual(client.close_calls, [("AAAUSDT", Decimal("16.57"))])
            self.assertEqual(state.orders["post_fill_adjustment"]["strategy"], "N10")
            self.assertLessEqual(
                Decimal(state.orders["plan"]["post_fill_actual_risk_amount"]),
                Decimal("200"),
            )

    def test_live_n10_actual_fill_closed_entry_boundaries_are_protected(self):
        for actual_entry in (Decimal("100"), Decimal("101.5")):
            with self.subTest(actual_entry=actual_entry), tempfile.TemporaryDirectory() as tmpdir:
                client = FakeLiveExecutionClient({}, leverage=50)
                state_store = StateStore(f"{tmpdir}/state.json")
                trader = Trader(
                    client,
                    live_test_config(f"{tmpdir}/account.json"),
                    state_store,
                    logging.getLogger("test_live_n10_entry_boundary"),
                )
                plan = trader.build_sweep_low_margin_capped_trade_plan(
                    "AAAUSDT",
                    actual_entry,
                    Decimal("97"),
                    Decimal("5"),
                    "n10-entry-boundary",
                    entry_min_price=Decimal("100"),
                    entry_max_price=Decimal("101.5"),
                )
                client.open_response = {
                    "orderId": 1041,
                    "avgPrice": str(actual_entry),
                    "executedQty": str(plan.quantity),
                }
                client.position_quantities = [str(plan.quantity)]

                state = trader.open_long_plan_with_protection(plan)

                self.assertEqual(Decimal(state.entry_price), actual_entry)
                self.assertEqual(len(client.protection_calls), 2)
                self.assertEqual(client.close_calls, [])
                self.assertEqual(state.orders["plan"]["entry_min"], "100")
                self.assertEqual(state.orders["plan"]["entry_max"], "101.5")
                self.assertEqual(state.orders["plan"]["actual_entry"], str(actual_entry))

    def test_live_n10_actual_fill_outside_entry_range_emergency_closes(self):
        for actual_entry in (Decimal("99.499"), Decimal("102.008")):
            with self.subTest(actual_entry=actual_entry), tempfile.TemporaryDirectory() as tmpdir:
                client = FakeLiveExecutionClient({}, leverage=50)
                state_store = StateStore(f"{tmpdir}/state.json")
                trader = Trader(
                    client,
                    live_test_config(f"{tmpdir}/account.json"),
                    state_store,
                    logging.getLogger("test_live_n10_entry_outside"),
                )
                plan = trader.build_sweep_low_margin_capped_trade_plan(
                    "AAAUSDT",
                    Decimal("100"),
                    Decimal("97"),
                    Decimal("5"),
                    "n10-entry-outside",
                    entry_min_price=Decimal("100"),
                    entry_max_price=Decimal("101.5"),
                )
                client.open_response = {
                    "orderId": 1051,
                    "avgPrice": str(actual_entry),
                    "executedQty": str(plan.quantity),
                }
                client.position_quantities = [str(plan.quantity), "0"]
                client.close_side_effects = [
                    {
                        "status": "FILLED",
                        "executedQty": str(plan.quantity),
                        "orderId": 1052,
                    }
                ]

                with self.assertRaisesRegex(
                    BinanceAPIError,
                    "N10_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE",
                ):
                    trader.open_long_plan_with_protection(plan)

                self.assertEqual(client.close_calls, [("AAAUSDT", plan.quantity)])
                self.assertEqual(client.protection_calls, [])
                self._assert_cleanup_audit_state(
                    state_store, trader, "N10"
                )

    def test_live_n15_actual_fill_half_percent_boundaries_are_inclusive(self):
        entry_min = Decimal("0.3394")
        entry_max = Decimal("0.341019389038")
        allowed_min = Decimal("0.337703")
        allowed_max = Decimal("0.342724485983190")
        self.assertEqual(entry_min * Decimal("0.995"), allowed_min)
        self.assertEqual(entry_max * Decimal("1.005"), allowed_max)

        class TinyPriceClient(FakeLiveExecutionClient):
            def get_symbol_info(self, symbol):
                return {
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.000000000000001"},
                        {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "100000", "stepSize": "0.001"},
                        {"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "maxQty": "100000", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ]
                }

        for actual_entry in (
            Decimal("0.3411"),
            allowed_min,
            allowed_max,
        ):
            with self.subTest(actual_entry=actual_entry), tempfile.TemporaryDirectory() as tmpdir:
                client = TinyPriceClient({}, leverage=50)
                state_store = StateStore(f"{tmpdir}/state.json")
                trader = Trader(
                    client,
                    live_test_config(f"{tmpdir}/account.json"),
                    state_store,
                    logging.getLogger("test_live_n15_fill_tolerance"),
                )
                plan = trader.build_breadth_recovery_margin_capped_trade_plan(
                    "GRVTUSDT",
                    Decimal("0.34"),
                    Decimal("0.334"),
                    Decimal("5"),
                    "n15-fill-tolerance",
                    entry_min_price=entry_min,
                    entry_max_price=entry_max,
                )
                safe_quantity = trader._margin_capped_safe_quantity_after_fill(
                    plan,
                    actual_entry,
                    plan.quantity,
                )
                client.open_response = {
                    "orderId": 2051,
                    "avgPrice": str(actual_entry),
                    "executedQty": str(plan.quantity),
                }
                client.position_quantities = [str(plan.quantity)]
                if safe_quantity < plan.quantity:
                    excess_quantity = plan.quantity - safe_quantity
                    client.position_quantities.append(str(safe_quantity))
                    client.close_side_effects = [{
                        "status": "FILLED",
                        "executedQty": str(excess_quantity),
                        "orderId": 2052,
                    }]

                state = trader.open_long_plan_with_protection(plan)

                actual_stop = Decimal(state.stop_loss_price)
                actual_take_profit = Decimal(state.take_profit_price)
                self.assertEqual(Decimal(state.entry_price), actual_entry)
                self.assertEqual(len(client.protection_calls), 2)
                expected_reduction = plan.quantity - safe_quantity
                self.assertEqual(
                    client.close_calls,
                    [] if expected_reduction == 0 else [("GRVTUSDT", expected_reduction)],
                )
                self.assertLessEqual(
                    Decimal(state.orders["plan"]["post_fill_required_margin"]),
                    Decimal("950"),
                )
                self.assertLessEqual(
                    Decimal(state.orders["plan"]["post_fill_actual_risk_amount"]),
                    Decimal("200"),
                )
                self.assertGreaterEqual(
                    (actual_take_profit - actual_entry)
                    / (actual_entry - actual_stop),
                    Decimal("5"),
                )

    def test_live_n15_actual_fill_beyond_half_percent_emergency_closes(self):
        entry_min = Decimal("0.3394")
        entry_max = Decimal("0.341019389038")
        values = (
            entry_min * Decimal("0.995") - Decimal("0.000000000000001"),
            entry_max * Decimal("1.005") + Decimal("0.000000000000001"),
        )

        class TinyPriceClient(FakeLiveExecutionClient):
            def get_symbol_info(self, symbol):
                return {
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.000000000000001"},
                        {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "100000", "stepSize": "0.001"},
                        {"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "maxQty": "100000", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ]
                }

        for actual_entry in values:
            with self.subTest(actual_entry=actual_entry), tempfile.TemporaryDirectory() as tmpdir:
                client = TinyPriceClient({}, leverage=50)
                state_store = StateStore(f"{tmpdir}/state.json")
                trader = Trader(
                    client,
                    live_test_config(f"{tmpdir}/account.json"),
                    state_store,
                    logging.getLogger("test_live_n15_fill_outside_tolerance"),
                )
                plan = trader.build_breadth_recovery_margin_capped_trade_plan(
                    "GRVTUSDT",
                    Decimal("0.34"),
                    Decimal("0.334"),
                    Decimal("5"),
                    "n15-fill-outside-tolerance",
                    entry_min_price=entry_min,
                    entry_max_price=entry_max,
                )
                client.open_response = {
                    "orderId": 2061,
                    "avgPrice": str(actual_entry),
                    "executedQty": str(plan.quantity),
                }
                client.position_quantities = [str(plan.quantity), "0"]
                client.close_side_effects = [{
                    "status": "FILLED",
                    "executedQty": str(plan.quantity),
                    "orderId": 2062,
                }]

                with self.assertRaisesRegex(
                    BinanceAPIError,
                    "N15_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE",
                ):
                    trader.open_long_plan_with_protection(plan)

                self.assertEqual(client.close_calls, [("GRVTUSDT", plan.quantity)])
                self.assertEqual(client.protection_calls, [])
                self._assert_cleanup_audit_state(state_store, trader, "N15")

    def test_n10_pretrade_reference_entry_outside_range_rejects_before_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient({}, leverage=50)
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_n10_pretrade_entry_outside"),
            )

            with self.assertRaisesRegex(BinanceAPIError, "Invalid N10 entry range"):
                trader.build_sweep_low_margin_capped_trade_plan(
                    "AAAUSDT",
                    Decimal("99.99"),
                    Decimal("97"),
                    Decimal("5"),
                    entry_min_price=Decimal("100"),
                    entry_max_price=Decimal("101.5"),
                )

            self.assertEqual(client.market_calls, [])
            self.assertEqual(client.protection_calls, [])

    def test_live_n10_post_fill_stop_pct_failure_emergency_cleans(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"orderId": 1011, "avgPrice": "99.5", "executedQty": "200"},
                leverage=50,
                position_quantities=["200", "0"],
                close_side_effects=[
                    {"status": "FILLED", "executedQty": "200", "orderId": 1012},
                ],
            )
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/account.json"),
                state_store,
                logging.getLogger("test_live_n10_stop_pct_cleanup"),
            )
            plan = trader.build_sweep_low_margin_capped_trade_plan(
                "AAAUSDT",
                Decimal("100"),
                Decimal("99.01"),
                Decimal("5"),
                entry_min_price=Decimal("99"),
                entry_max_price=Decimal("101.5"),
            )

            with self.assertRaisesRegex(BinanceAPIError, "post-fill adjustment or protection failed"):
                trader.open_long_plan_with_protection(plan)

            self.assertEqual(client.close_calls, [("AAAUSDT", Decimal("200"))])
            self.assertEqual(client.protection_calls, [])
            self._assert_cleanup_audit_state(
                state_store, trader, "N10"
            )

    def test_live_n10_deadline_blocks_market_order_after_set_leverage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            deadline_ms = 1_720_000_120_000
            client = FakeLiveExecutionClient(
                {"orderId": 1021, "avgPrice": "100", "executedQty": "66.445"},
                leverage=50,
            )
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_live_n10_deadline"),
                clock_ms=lambda: deadline_ms,
            )
            plan = trader.build_sweep_low_margin_capped_trade_plan(
                "AAAUSDT",
                Decimal("100"),
                Decimal("97"),
                Decimal("5"),
                entry_min_price=Decimal("100"),
                entry_max_price=Decimal("101.5"),
            )
            plan = replace(
                plan,
                entry_candle_open_time_ms=deadline_ms - 120_000,
                entry_deadline_ms=deadline_ms,
            )

            with self.assertRaises(EntryWindowExpiredError):
                trader.open_long_plan_with_protection(plan)

            self.assertEqual(client.leverage_calls, [("AAAUSDT", 50)])
            self.assertEqual(client.market_calls, [])

    def test_live_n10_protection_failure_emergency_cleans_without_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"orderId": 1031, "avgPrice": "100", "executedQty": "66.445"},
                leverage=50,
                position_quantities=["66.445", "0"],
                close_side_effects=[
                    {"status": "FILLED", "executedQty": "66.445", "orderId": 1032},
                ],
                protection_side_effects=[BinanceAPIError("N10 stop unavailable")],
            )
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/account.json"),
                state_store,
                logging.getLogger("test_live_n10_protection_cleanup"),
            )
            plan = trader.build_sweep_low_margin_capped_trade_plan(
                "AAAUSDT",
                Decimal("100"),
                Decimal("97"),
                Decimal("5"),
                entry_min_price=Decimal("100"),
                entry_max_price=Decimal("101.5"),
            )

            with self.assertRaisesRegex(BinanceAPIError, "post-fill adjustment or protection failed"):
                trader.open_long_plan_with_protection(plan)

            self.assertEqual(client.close_calls, [("AAAUSDT", Decimal("66.445"))])
            self._assert_cleanup_audit_state(
                state_store,
                trader,
                "N10",
                protection_pending=True,
            )

    def test_live_n08_adverse_fill_reduces_before_percentage_protection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"orderId": 120, "avgPrice": "101", "executedQty": "200"},
                leverage=50,
                position_quantities=["200", "198.019"],
                close_side_effects=[
                    {"status": "FILLED", "executedQty": "1.981", "orderId": 121},
                ],
            )
            client.get_24hr_ticker = lambda symbol: {
                "highPrice": "100.5",
                "lowPrice": "100",
            }
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                state_store,
                logging.getLogger("test_live_n08_actual_fill"),
            )
            plan = trader.build_amplitude_margin_capped_trade_plan(
                "AAAUSDT",
                Decimal("100"),
                Decimal("5"),
                structure_id="n08-live-range",
            )

            state = trader.open_long_plan_with_protection(plan)

            actual_entry = Decimal(state.entry_price)
            actual_stop = Decimal(state.stop_loss_price)
            actual_take_profit = Decimal(state.take_profit_price)
            self.assertEqual(state.quantity, "198.019")
            self.assertEqual(actual_entry, Decimal("101"))
            self.assertEqual(actual_stop, Decimal("99.99"))
            self.assertEqual(actual_take_profit, Decimal("106.05"))
            self.assertGreaterEqual(
                (actual_take_profit - actual_entry) / (actual_entry - actual_stop),
                Decimal("5"),
            )
            self.assertEqual(client.close_calls, [("AAAUSDT", Decimal("1.981"))])
            self.assertLessEqual(
                Decimal(state.orders["plan"]["post_fill_actual_risk_amount"]),
                Decimal("200"),
            )
            self.assertLessEqual(
                Decimal(state.orders["plan"]["post_fill_required_margin"]),
                Decimal("950"),
            )
            self.assertTrue(state.orders["plan"]["reduced_after_fill"])
            self.assertEqual(state.orders["post_fill_adjustment"]["strategy"], "N08")

            recorder = make_test_recorder(
                f"{tmpdir}/n08-review.sqlite3",
                logging.getLogger("test_live_n08_actual_fill"),
            )
            scan_id = recorder.begin_scan(0, [], dry_run=False)
            recorder.record_trade_open(scan_id, state)
            with recorder._connect() as connection:
                row = connection.execute(
                    "SELECT pretrade_quantity, executed_quantity, final_protected_quantity, "
                    "post_fill_actual_risk_amount, post_fill_required_margin, reduced_after_fill "
                    "FROM trade_reviews"
                ).fetchone()
            self.assertEqual(row[0:3], ("200", "200", "198.019"))
            self.assertEqual(row[5], 1)

    def test_live_n08_expired_after_set_leverage_does_not_submit_market_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            deadline_ms = 1_700_000_120_000
            client = FakeLiveExecutionClient(
                {"orderId": 140, "avgPrice": "100", "executedQty": "200"},
                leverage=50,
            )
            client.get_24hr_ticker = lambda symbol: {
                "highPrice": "100.5",
                "lowPrice": "100",
            }
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                state_store,
                logging.getLogger("test_live_n08_deadline_expired"),
                clock_ms=lambda: deadline_ms,
            )
            plan = replace(
                trader.build_amplitude_margin_capped_trade_plan(
                    "AAAUSDT",
                    Decimal("100"),
                    Decimal("5"),
                ),
                entry_candle_open_time_ms=deadline_ms - 120_000,
                entry_deadline_ms=deadline_ms,
            )

            with self.assertRaises(EntryWindowExpiredError):
                trader.open_long_plan_with_protection(plan)

            self.assertEqual(client.leverage_calls, [("AAAUSDT", 50)])
            self.assertEqual(client.market_calls, [])
            self.assertIsNone(state_store.load())

    def test_live_n08_submits_market_order_while_local_clock_is_before_deadline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            deadline_ms = 1_700_000_120_000
            client = FakeLiveExecutionClient(
                {"orderId": 141, "avgPrice": "100", "executedQty": "200"},
                leverage=50,
                position_quantities=["200"],
            )
            client.get_24hr_ticker = lambda symbol: {
                "highPrice": "100.5",
                "lowPrice": "100",
            }
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_live_n08_deadline_open"),
                clock_ms=lambda: deadline_ms - 1,
            )
            plan = replace(
                trader.build_amplitude_margin_capped_trade_plan(
                    "AAAUSDT",
                    Decimal("100"),
                    Decimal("5"),
                ),
                entry_candle_open_time_ms=deadline_ms - 120_000,
                entry_deadline_ms=deadline_ms,
            )

            state = trader.open_long_plan_with_protection(plan)

            self.assertEqual(client.market_calls, [("AAAUSDT", "BUY", Decimal("200"))])
            self.assertEqual(state.orders["plan"]["entry_deadline_ms"], deadline_ms)

    def test_live_n08_protection_failure_uses_emergency_cleanup_without_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"orderId": 130, "avgPrice": "100", "executedQty": "200"},
                leverage=50,
                position_quantities=["200", "0"],
                protection_side_effects=[BinanceAPIError("stop protection unavailable")],
            )
            client.get_24hr_ticker = lambda symbol: {
                "highPrice": "100.5",
                "lowPrice": "100",
            }
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                state_store,
                logging.getLogger("test_live_n08_protection_failure"),
            )
            plan = trader.build_amplitude_margin_capped_trade_plan(
                "AAAUSDT",
                Decimal("100"),
                Decimal("5"),
            )

            with self.assertRaisesRegex(BinanceAPIError, "stop protection unavailable") as raised:
                trader.open_long_plan_with_protection(plan)

            self.assertIn("'confirmed_closed': True", str(raised.exception))
            self.assertEqual(client.close_calls, [("AAAUSDT", Decimal("200"))])
            self.assertEqual(client.protection_calls, [("STOP_MARKET", Decimal("99"))])
            self._assert_cleanup_audit_state(
                state_store,
                trader,
                "N08",
                protection_pending=True,
            )

    def test_live_n07_adverse_fill_reduces_full_risk_position_before_protection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"orderId": 70, "avgPrice": "113.50", "executedQty": "149.253"},
                leverage=50,
                position_quantities=["149.253", "133.333"],
                close_side_effects=[
                    {"status": "FILLED", "executedQty": "15.920", "orderId": 71},
                ],
            )
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                state_store,
                logging.getLogger("test_live_n07_actual_fill"),
            )
            plan = trader.build_margin_capped_structure_trade_plan(
                "AAAUSDT",
                Decimal("113.344"),
                Decimal("112"),
                Decimal("5"),
                structure_id="n07-actual-fill",
            )

            state = trader.open_long_plan_with_protection(plan)

            actual_entry = Decimal(state.entry_price)
            actual_stop = Decimal(state.stop_loss_price)
            actual_take_profit = Decimal(state.take_profit_price)
            actual_r = (actual_take_profit - actual_entry) / (actual_entry - actual_stop)
            self.assertTrue(Decimal("0.01") <= (actual_entry - Decimal("112")) / Decimal("112") <= Decimal("0.015"))
            self.assertEqual(actual_entry, Decimal("113.50"))
            self.assertEqual(actual_stop, Decimal("112"))
            self.assertEqual(actual_take_profit, Decimal("121.00"))
            self.assertGreaterEqual(actual_r, Decimal("5"))
            self.assertEqual(state.quantity, "133.333")
            self.assertEqual(client.close_calls, [("AAAUSDT", Decimal("15.920"))])
            self.assertEqual(client.protection_calls[0], ("STOP_MARKET", Decimal("112")))
            self.assertEqual(client.protection_calls[1], ("TAKE_PROFIT_MARKET", Decimal("121.00")))
            self.assertEqual(state.orders["pretrade_plan"]["entry_price"], "113.34")
            self.assertEqual(state.orders["plan"]["entry_price"], "113.5")
            self.assertEqual(state.orders["plan"]["target_risk_amount"], "200")
            self.assertEqual(state.orders["plan"]["pretrade_quantity"], "149.253")
            self.assertEqual(state.orders["plan"]["executed_quantity"], "149.253")
            self.assertEqual(state.orders["plan"]["final_protected_quantity"], "133.333")
            self.assertTrue(state.orders["plan"]["reduced_after_fill"])
            self.assertLessEqual(
                Decimal(state.orders["plan"]["post_fill_actual_risk_amount"]),
                Decimal(state.orders["plan"]["target_risk_amount"]),
            )
            self.assertLessEqual(
                Decimal(state.orders["plan"]["post_fill_required_margin"]),
                Decimal("950"),
            )
            self.assertEqual(
                state.orders["post_fill_adjustment"]["reduction_order"]["orderId"],
                71,
            )
            self.assertIsNotNone(state_store.load())

            recorder = make_test_recorder(
                f"{tmpdir}/review.sqlite3",
                logging.getLogger("test_live_n07_actual_fill"),
            )
            scan_id = recorder.begin_scan(0, [], dry_run=False)
            recorder.record_trade_open(scan_id, state)
            with recorder._connect() as connection:
                row = connection.execute(
                    "SELECT pretrade_quantity, executed_quantity, final_protected_quantity, "
                    "post_fill_actual_risk_amount, post_fill_required_margin, reduced_after_fill "
                    "FROM trade_reviews"
                ).fetchone()
            self.assertEqual(row[0:3], ("149.253", "149.253", "133.333"))
            self.assertLessEqual(Decimal(row[3]), Decimal("200"))
            self.assertLessEqual(Decimal(row[4]), Decimal("950"))
            self.assertEqual(row[5], 1)

    def test_live_n07_post_fill_margin_breach_is_reduced(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"orderId": 80, "avgPrice": "113.50", "executedQty": "41.909"},
                leverage=5,
                position_quantities=["41.909", "41.850"],
                close_side_effects=[
                    {"status": "FILLED", "executedQty": "0.059", "orderId": 81},
                ],
            )
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_live_n07_margin_fill"),
            )
            plan = trader.build_margin_capped_structure_trade_plan(
                "AAAUSDT",
                Decimal("113.344"),
                Decimal("112"),
                Decimal("5"),
            )
            unadjusted_margin = Decimal("41.909") * Decimal("113.50") / Decimal("5")

            state = trader.open_long_plan_with_protection(plan)

            self.assertGreater(unadjusted_margin, Decimal("950"))
            self.assertEqual(state.quantity, "41.85")
            self.assertLessEqual(
                Decimal(state.orders["plan"]["post_fill_required_margin"]),
                Decimal("950"),
            )
            self.assertLess(
                Decimal(state.orders["plan"]["post_fill_actual_risk_amount"]),
                Decimal("200"),
            )
            self.assertEqual(client.close_calls, [("AAAUSDT", Decimal("0.059"))])

    def test_live_n07_unconfirmed_reduction_emergency_covers_full_executed_quantity(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"orderId": 90, "avgPrice": "113.50", "executedQty": "149.253"},
                leverage=50,
                position_quantities=["149.253", "149.253", "0"],
                close_side_effects=[
                    {"status": "FILLED", "executedQty": "15.920", "orderId": 91},
                    {"status": "FILLED", "executedQty": "149.253", "orderId": 92},
                ],
            )
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                state_store,
                logging.getLogger("test_live_n07_unconfirmed_reduction"),
            )
            plan = trader.build_margin_capped_structure_trade_plan(
                "AAAUSDT",
                Decimal("113.344"),
                Decimal("112"),
                Decimal("5"),
            )

            with self.assertRaisesRegex(BinanceAPIError, "remaining position exceeds safe quantity"):
                trader.open_long_plan_with_protection(plan)

            self.assertEqual(
                client.close_calls,
                [
                    ("AAAUSDT", Decimal("15.920")),
                    ("AAAUSDT", Decimal("149.253")),
                ],
            )
            self.assertEqual(client.protection_calls, [])
            self._assert_cleanup_audit_state(
                state_store, trader, "N07"
            )

    def test_live_n07_reduction_confirmation_failure_uses_full_cleanup_quantity(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"orderId": 100, "avgPrice": "113.50", "executedQty": "149.253"},
                leverage=50,
                position_quantities=[
                    "149.253",
                    BinanceAPIError("remaining position unavailable"),
                    "133.333",
                    "0",
                ],
                close_side_effects=[
                    {"status": "FILLED", "executedQty": "15.920", "orderId": 101},
                    BinanceAPIError("reduce-only quantity exceeds remaining position"),
                    {"status": "FILLED", "executedQty": "133.333", "orderId": 102},
                ],
            )
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                state_store,
                logging.getLogger("test_live_n07_confirmation_failure"),
            )
            plan = trader.build_margin_capped_structure_trade_plan(
                "AAAUSDT",
                Decimal("113.344"),
                Decimal("112"),
                Decimal("5"),
            )

            with self.assertRaisesRegex(BinanceAPIError, "remaining position unavailable") as raised:
                trader.open_long_plan_with_protection(plan)

            self.assertEqual(
                client.close_calls,
                [
                    ("AAAUSDT", Decimal("15.920")),
                    ("AAAUSDT", Decimal("149.253")),
                    ("AAAUSDT", Decimal("133.333")),
                ],
            )
            self.assertIn("'confirmed_closed': True", str(raised.exception))
            self.assertEqual(client.protection_calls, [])
            self._assert_cleanup_audit_state(
                state_store, trader, "N07"
            )

    def test_live_n07_unfilled_reduction_response_triggers_full_cleanup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"orderId": 105, "avgPrice": "113.50", "executedQty": "149.253"},
                leverage=50,
                position_quantities=["149.253", "0"],
                close_side_effects=[
                    {"status": "NEW", "executedQty": "0", "orderId": 106},
                    {"status": "FILLED", "executedQty": "149.253", "orderId": 107},
                ],
            )
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                state_store,
                logging.getLogger("test_live_n07_unfilled_reduction"),
            )
            plan = trader.build_margin_capped_structure_trade_plan(
                "AAAUSDT",
                Decimal("113.344"),
                Decimal("112"),
                Decimal("5"),
            )

            with self.assertRaisesRegex(BinanceAPIError, "reduction was not fully filled"):
                trader.open_long_plan_with_protection(plan)

            self.assertEqual(
                client.close_calls,
                [
                    ("AAAUSDT", Decimal("15.920")),
                    ("AAAUSDT", Decimal("149.253")),
                ],
            )
            self.assertEqual(client.protection_calls, [])
            self._assert_cleanup_audit_state(
                state_store, trader, "N07"
            )

    def test_live_n07_low_leverage_safe_fill_needs_no_reduction(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"orderId": 110, "avgPrice": "113.34", "executedQty": "83.818"},
                leverage=10,
                position_quantities=["83.818"],
            )
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_live_n07_safe_fill"),
            )
            plan = trader.build_margin_capped_structure_trade_plan(
                "AAAUSDT",
                Decimal("113.344"),
                Decimal("112"),
                Decimal("5"),
            )

            state = trader.open_long_plan_with_protection(plan)

            self.assertEqual(state.quantity, "83.818")
            self.assertEqual(client.close_calls, [])
            self.assertFalse(state.orders["plan"]["reduced_after_fill"])
            self.assertLessEqual(
                Decimal(state.orders["plan"]["post_fill_actual_risk_amount"]),
                Decimal("200"),
            )
            self.assertLessEqual(
                Decimal(state.orders["plan"]["post_fill_required_margin"]),
                Decimal("950"),
            )

    def test_live_n06_protection_uses_actual_fill_and_ceil_preserves_at_least_five_r(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient({"orderId": 7, "avgPrice": "123.033", "executedQty": "25"})
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                state_store,
                logging.getLogger("test_live_actual_fill"),
            )
            plan = trader.build_structure_trade_plan(
                "AAAUSDT",
                Decimal("120"),
                Decimal("112"),
                Decimal("5"),
                structure_id="structure-actual-fill",
            )

            state = trader.open_long_plan_with_protection(plan)

            actual_entry = Decimal(state.entry_price)
            actual_stop = Decimal(state.stop_loss_price)
            actual_take_profit = Decimal(state.take_profit_price)
            actual_r_multiple = (actual_take_profit - actual_entry) / (actual_entry - actual_stop)
            self.assertEqual(actual_entry, Decimal("123.033"))
            self.assertEqual(actual_stop, Decimal("112"))
            self.assertEqual(actual_take_profit, Decimal("178.2"))
            self.assertGreaterEqual(actual_r_multiple, Decimal("5"))
            self.assertEqual(client.protection_calls[0], ("STOP_MARKET", Decimal("112")))
            self.assertEqual(client.protection_calls[1], ("TAKE_PROFIT_MARKET", Decimal("178.20")))
            self.assertEqual(state.orders["entry_price_source"], "MARKET_ORDER_RESPONSE")
            self.assertEqual(state.orders["pretrade_plan"]["entry_price"], "120")
            self.assertEqual(state.orders["plan"]["entry_price"], "123.033")

            recorder = make_test_recorder(f"{tmpdir}/review.sqlite3", logging.getLogger("test_live_actual_fill"))
            scan_id = recorder.begin_scan(0, [], dry_run=False)
            recorder.record_trade_open(scan_id, state)
            with recorder._connect() as connection:
                row = connection.execute(
                    "SELECT entry_price, stop_loss_price, take_profit_price FROM trade_reviews"
                ).fetchone()
            self.assertEqual(tuple(row), ("123.033", "112", "178.2"))

    def test_live_fill_falls_back_to_order_query(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient(
                {"orderId": 8, "avgPrice": "0"},
                order_response={"orderId": 8, "avgPrice": "121.25", "executedQty": "25"},
            )
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                StateStore(f"{tmpdir}/state.json"),
                logging.getLogger("test_live_order_query"),
            )
            plan = trader.build_structure_trade_plan(
                "AAAUSDT",
                Decimal("120"),
                Decimal("112"),
                Decimal("5"),
            )

            state = trader.open_long_plan_with_protection(plan)

            self.assertEqual(state.entry_price, "121.25")
            self.assertEqual(state.orders["entry_price_source"], "ORDER_QUERY")

    def test_live_fill_confirmation_failure_emergency_closes_without_protection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeLiveExecutionClient({"orderId": 9, "avgPrice": "0"})
            state_store = StateStore(f"{tmpdir}/state.json")
            trader = Trader(
                client,
                live_test_config(f"{tmpdir}/dry_run_account.json"),
                state_store,
                logging.getLogger("test_live_missing_fill"),
            )
            plan = trader.build_structure_trade_plan(
                "AAAUSDT",
                Decimal("120"),
                Decimal("112"),
                Decimal("5"),
            )

            with self.assertRaisesRegex(BinanceAPIError, "Unable to confirm actual market entry price"):
                trader.open_long_plan_with_protection(plan)

            self.assertEqual(len(client.close_calls), 1)
            self.assertEqual(client.protection_calls, [])
            self.assertIsNone(state_store.load())

    def test_dry_run_stop_loss_closes_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_store = StateStore(f"{tmpdir}/state.json")
            state_store.save(
                PositionState(
                    symbol="AAAUSDT",
                    quantity="10",
                    entry_price="100",
                    stop_loss_price="95",
                    take_profit_price="120",
                    leverage=50,
                    opened_at="2026-01-01T00:00:00+00:00",
                    dry_run=True,
                    orders={},
                )
            )
            trader = Trader(
                FakeClient(mark_price="94"),
                test_config(f"{tmpdir}/dry_run_account.json"),
                state_store,
                logging.getLogger("test_trader"),
            )

            result = trader.close_dry_run_position_if_triggered()

            self.assertIsNotNone(result)
            self.assertEqual(result.exit_reason, "STOP_LOSS")
            self.assertEqual(result.exit_price, Decimal("95"))
            self.assertEqual(result.mark_price, Decimal("94"))
            self.assertEqual(result.pnl_amount, Decimal("-50"))
            self.assertEqual(result.balance_after, Decimal("950"))
            self.assertIsNone(state_store.load())

    def test_dry_run_open_position_stays_when_trigger_not_touched(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_store = StateStore(f"{tmpdir}/state.json")
            state_store.save(
                PositionState(
                    symbol="AAAUSDT",
                    quantity="10",
                    entry_price="100",
                    stop_loss_price="95",
                    take_profit_price="120",
                    leverage=50,
                    opened_at="2026-01-01T00:00:00+00:00",
                    dry_run=True,
                    orders={},
                )
            )
            trader = Trader(
                FakeClient(mark_price="110"),
                test_config(f"{tmpdir}/dry_run_account.json"),
                state_store,
                logging.getLogger("test_trader"),
            )

            self.assertIsNone(trader.close_dry_run_position_if_triggered())
            self.assertIsNotNone(state_store.load())

    def test_next_dry_run_trade_uses_balance_after_prior_loss(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = test_config(f"{tmpdir}/dry_run_account.json")
            state_store = StateStore(f"{tmpdir}/state.json")
            state_store.save(
                PositionState(
                    symbol="AAAUSDT",
                    quantity="10",
                    entry_price="100",
                    stop_loss_price="95",
                    take_profit_price="120",
                    leverage=50,
                    opened_at="2026-01-01T00:00:00+00:00",
                    dry_run=True,
                    orders={},
                )
            )
            trader = Trader(
                FakeClient(mark_price="94"),
                config,
                state_store,
                logging.getLogger("test_trader"),
            )

            close_result = trader.close_dry_run_position_if_triggered()
            plan = trader.build_trade_plan("AAAUSDT", Decimal("120"))

            self.assertEqual(close_result.balance_after, Decimal("950"))
            self.assertEqual(plan.balance, Decimal("950"))
            self.assertEqual(plan.risk_amount, Decimal("190.00"))

    def test_live_sync_reports_closed_state_when_exchange_position_disappears(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_store = StateStore(f"{tmpdir}/state.json")
            state_store.save(
                PositionState(
                    symbol="AAAUSDT",
                    quantity="10",
                    entry_price="100",
                    stop_loss_price="95",
                    take_profit_price="120",
                    leverage=50,
                    opened_at="2026-01-01T00:00:00+00:00",
                    dry_run=False,
                    orders={},
                )
            )
            client = FakeClient()
            client.get_open_positions = lambda: []
            trader = Trader(
                client,
                test_config(f"{tmpdir}/dry_run_account.json"),
                state_store,
                logging.getLogger("test_trader"),
            )

            result = trader.sync_state_with_exchange()

            self.assertFalse(result.has_position)
            self.assertIsNotNone(result.closed_state)
            self.assertEqual(result.closed_state.symbol, "AAAUSDT")
            self.assertIsNotNone(state_store.load())


if __name__ == "__main__":
    unittest.main()
