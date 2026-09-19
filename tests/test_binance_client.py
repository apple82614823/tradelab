from decimal import Decimal
import logging
import unittest
from unittest.mock import patch

import requests

from trading_bot.binance_client import BinanceAPIError
from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.config import LIVE_TRADING_CONFIRMATION, Config
from trading_bot.monitor import FundingMonitor


def test_config() -> Config:
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
        dry_run_account_file="state/dry_run_account.json",
        log_file="logs/trading.log",
        review_db_file="data/trading_review.sqlite3",
    )


class FakeExchangeInfoClient(BinanceFuturesClient):
    def __init__(self, contract_type: str):
        super().__init__(test_config(), logging.getLogger("test_binance_client"))
        self.contract_type = contract_type

    def get_symbol_info(self, symbol: str) -> dict:
        return {"symbol": symbol, "contractType": self.contract_type}


class FakeResponse:
    status_code = 200
    text = '{"ok": true}'

    def json(self):
        return {"ok": True}


def live_test_config() -> Config:
    return Config(
        **{
            **test_config().__dict__,
            "api_key": "key",
            "api_secret": "secret",
            "dry_run": False,
            "live_confirmation": LIVE_TRADING_CONFIRMATION,
        }
    )


class CapturingOrderClient(BinanceFuturesClient):
    def __init__(self, hedge_mode: bool):
        super().__init__(live_test_config(), logging.getLogger("test_binance_client"))
        self._dual_side_position = hedge_mode
        self.calls = []

    def _request(self, method, path, params=None, signed=False, retry=True):
        self.calls.append(
            {
                "method": method,
                "path": path,
                "params": dict(params or {}),
                "signed": signed,
                "retry": retry,
            }
        )
        return {"ok": True}


class BinanceClientTests(unittest.TestCase):
    def test_long_position_quantity_returns_zero_for_confirmed_empty_symbol(self):
        client = BinanceFuturesClient(live_test_config(), logging.getLogger("test_binance_client"))
        client.get_positions = lambda: [
            {
                "symbol": "AAAUSDT",
                "positionSide": "BOTH",
                "positionAmt": "0",
                "entryPrice": "0",
            }
        ]

        self.assertEqual(client.get_long_position_quantity("AAAUSDT"), Decimal("0"))

    def test_long_position_quantity_prefers_positive_long_over_zero_row(self):
        client = BinanceFuturesClient(live_test_config(), logging.getLogger("test_binance_client"))
        client.get_positions = lambda: [
            {"symbol": "AAAUSDT", "positionSide": "BOTH", "positionAmt": "0"},
            {"symbol": "AAAUSDT", "positionSide": "LONG", "positionAmt": "3.25"},
        ]

        self.assertEqual(client.get_long_position_quantity("AAAUSDT"), Decimal("3.25"))

    def test_long_position_quantity_or_zero_accepts_authenticated_absent_symbol(self):
        client = BinanceFuturesClient(
            live_test_config(), logging.getLogger("test_binance_client")
        )
        client.get_positions = lambda: []

        self.assertEqual(
            client.get_long_position_quantity_or_zero("AAAUSDT"), Decimal("0")
        )

    def test_tradable_universe_includes_perpetual_and_tradifi_perpetual(self):
        client = BinanceFuturesClient(test_config(), logging.getLogger("test_binance_client"))
        client._exchange_info = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                },
                {
                    "symbol": "XAUUSDT",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "TRADIFI_PERPETUAL",
                },
                {
                    "symbol": "BTCUSDT_260925",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "CURRENT_QUARTER",
                },
            ]
        }

        self.assertEqual(
            client.get_tradable_usdt_perpetual_symbols(),
            {"BTCUSDT", "XAUUSDT"},
        )
        client.get_premium_index = lambda: []
        client.get_24hr_tickers = lambda: [
            {"symbol": "BTCUSDT", "quoteVolume": "100", "lastPrice": "60000"},
            {"symbol": "XAUUSDT", "quoteVolume": "90", "lastPrice": "3000"},
            {"symbol": "BTCUSDT_260925", "quoteVolume": "1000", "lastPrice": "61000"},
        ]
        market = FundingMonitor(
            client,
            Decimal("0.015"),
            logging.getLogger("test_binance_client"),
        ).scan_for_strategies(100)
        self.assertEqual(
            [candidate.symbol for candidate in market.volume_candidates],
            ["BTCUSDT", "XAUUSDT"],
        )

    def test_dry_run_caps_tradifi_contracts_to_tradifi_max_leverage(self):
        client = FakeExchangeInfoClient("TRADIFI_PERPETUAL")

        self.assertEqual(client.get_max_leverage("KORUUSDT"), 20)

    def test_dry_run_uses_default_max_leverage_for_crypto_perpetuals(self):
        client = FakeExchangeInfoClient("PERPETUAL")

        self.assertEqual(client.get_max_leverage("BTCUSDT"), 50)

    def test_signed_retries_regenerate_timestamp_and_signature(self):
        config = Config(
            **{
                **test_config().__dict__,
                "api_key": "key",
                "api_secret": "secret",
                "request_retries": 2,
                "network_reconnect_delay_seconds": 0,
            }
        )
        client = BinanceFuturesClient(config, logging.getLogger("test_binance_client"))

        with (
            patch("trading_bot.binance_client.time.time", side_effect=[1000, 1001, 1002, 1003, 1004]),
            patch("trading_bot.binance_client.time.sleep"),
            patch(
                "trading_bot.binance_client.requests.request",
                side_effect=[requests.ConnectionError("reset"), FakeResponse()],
            ) as request_mock,
        ):
            self.assertEqual(
                client._request("GET", "/signed", {"symbol": "BTCUSDT"}, signed=True, retry=True),
                {"ok": True},
            )

        first_params = request_mock.call_args_list[0].kwargs["params"]
        second_params = request_mock.call_args_list[1].kwargs["params"]
        self.assertEqual(first_params["timestamp"], 1000000)
        self.assertGreater(second_params["timestamp"], first_params["timestamp"])
        self.assertNotEqual(first_params["signature"], second_params["signature"])

    def test_missing_credentials_are_reported_before_request(self):
        client = BinanceFuturesClient(test_config(), logging.getLogger("test_binance_client"))

        with self.assertRaises(BinanceAPIError):
            client._request("GET", "/signed", signed=True, retry=True)

    def test_live_market_order_adds_long_position_side_in_hedge_mode(self):
        client = CapturingOrderClient(hedge_mode=True)

        client.place_market_order("ACTUSDT", "BUY", Decimal("10"))

        params = client.calls[-1]["params"]
        self.assertEqual(params["positionSide"], "LONG")
        self.assertEqual(params["side"], "BUY")

    def test_live_market_order_omits_position_side_in_one_way_mode(self):
        client = CapturingOrderClient(hedge_mode=False)

        client.place_market_order("ACTUSDT", "BUY", Decimal("10"))

        self.assertNotIn("positionSide", client.calls[-1]["params"])

    def test_live_protection_order_adds_long_position_side_in_hedge_mode(self):
        client = CapturingOrderClient(hedge_mode=True)

        client.place_close_all_algo_order("ACTUSDT", "STOP_MARKET", Decimal("0.01"))

        params = client.calls[-1]["params"]
        self.assertEqual(params["positionSide"], "LONG")
        self.assertEqual(params["side"], "SELL")
        self.assertEqual(params["closePosition"], "true")

    def test_live_emergency_close_uses_position_side_without_reduce_only_in_hedge_mode(self):
        client = CapturingOrderClient(hedge_mode=True)

        client.close_position_market("ACTUSDT", Decimal("10"))

        params = client.calls[-1]["params"]
        self.assertEqual(params["positionSide"], "LONG")
        self.assertEqual(params["side"], "SELL")
        self.assertNotIn("reduceOnly", params)

    def test_live_emergency_close_keeps_reduce_only_in_one_way_mode(self):
        client = CapturingOrderClient(hedge_mode=False)

        client.close_position_market("ACTUSDT", Decimal("10"))

        params = client.calls[-1]["params"]
        self.assertEqual(params["reduceOnly"], "true")
        self.assertNotIn("positionSide", params)

    def test_query_algo_order_uses_saved_algo_id(self):
        client = CapturingOrderClient(hedge_mode=False)

        client.get_algo_order(algo_id=2146760)

        call = client.calls[-1]
        self.assertEqual(call["method"], "GET")
        self.assertEqual(call["path"], "/fapi/v1/algoOrder")
        self.assertEqual(call["params"], {"algoId": 2146760})
        self.assertTrue(call["signed"])

    def test_cancel_all_algo_orders_is_symbol_scoped_and_not_retried(self):
        client = CapturingOrderClient(hedge_mode=False)

        client.cancel_all_algo_open_orders("ACTUSDT")

        call = client.calls[-1]
        self.assertEqual(call["method"], "DELETE")
        self.assertEqual(call["path"], "/fapi/v1/algoOpenOrders")
        self.assertEqual(call["params"], {"symbol": "ACTUSDT"})
        self.assertTrue(call["signed"])
        self.assertFalse(call["retry"])

    def test_open_algo_order_query_is_symbol_scoped_conditional(self):
        client = BinanceFuturesClient(
            live_test_config(), logging.getLogger("test_binance_client")
        )
        with patch.object(client, "_request", return_value=[]) as request_mock:
            self.assertEqual(client.get_open_algo_orders("ACTUSDT"), [])

        request_mock.assert_called_once_with(
            "GET",
            "/fapi/v1/openAlgoOrders",
            {"symbol": "ACTUSDT", "algoType": "CONDITIONAL"},
            signed=True,
            retry=True,
        )

    def test_aggregate_trade_query_uses_exact_opening_range(self):
        client = CapturingOrderClient(hedge_mode=False)

        with patch.object(client, "_request", return_value=[]) as request_mock:
            rows = client.get_aggregate_trades("AAAUSDT", 1000, 1999)

        self.assertEqual(rows, [])
        request_mock.assert_called_once_with(
            "GET",
            "/fapi/v1/aggTrades",
            {
                "symbol": "AAAUSDT",
                "startTime": 1000,
                "endTime": 1999,
                "limit": 1000,
            },
            retry=True,
        )


if __name__ == "__main__":
    unittest.main()
