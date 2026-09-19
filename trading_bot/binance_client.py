from __future__ import annotations

import hashlib
import hmac
import time
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import requests

from .config import Config
from .precision import decimal_to_api


class BinanceAPIError(RuntimeError):
    pass


class BinanceFuturesClient:
    def __init__(self, config: Config, logger):
        self.config = config
        self.logger = logger
        self._exchange_info: dict[str, Any] | None = None
        self._dual_side_position: bool | None = None

    @property
    def has_credentials(self) -> bool:
        return self.config.has_credentials

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if self.config.api_key:
            headers["X-MBX-APIKEY"] = self.config.api_key
        return headers

    def _signed_params(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self.has_credentials:
            raise BinanceAPIError("Missing Binance API credentials.")
        signed = dict(params or {})
        signed.setdefault("recvWindow", self.config.recv_window)
        signed["timestamp"] = int(time.time() * 1000)
        query = urlencode(signed)
        signature = hmac.new(
            self.config.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        signed["signature"] = signature
        return signed

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        signed: bool = False,
        retry: bool = True,
    ) -> Any:
        attempts = self.config.request_retries if retry else 1
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            request_params = self._signed_params(params) if signed else params
            try:
                response = requests.request(
                    method,
                    f"{self.config.base_url}{path}",
                    params=request_params if method.upper() == "GET" else None,
                    data=request_params if method.upper() != "GET" else None,
                    headers=self._headers(),
                    timeout=self.config.request_timeout_seconds,
                )
                if response.status_code >= 400:
                    raise BinanceAPIError(f"{response.status_code} {response.text}")
                return response.json()
            except requests.ConnectionError as exc:
                last_error = exc
                if attempt < attempts:
                    self.logger.warning(
                        "Network error, reconnecting in %ss: %s",
                        self.config.network_reconnect_delay_seconds,
                        exc,
                    )
                    time.sleep(self.config.network_reconnect_delay_seconds)
            except (requests.Timeout, requests.RequestException, BinanceAPIError) as exc:
                last_error = exc
                if attempt < attempts:
                    self.logger.warning(
                        "API request failed, retrying in %ss (%s/%s): %s",
                        self.config.request_retry_delay_seconds,
                        attempt,
                        attempts,
                        exc,
                    )
                    time.sleep(self.config.request_retry_delay_seconds)

        raise BinanceAPIError(str(last_error))

    def get_premium_index(self) -> list[dict[str, Any]]:
        data = self._request("GET", "/fapi/v1/premiumIndex", retry=True)
        return data if isinstance(data, list) else [data]

    def get_mark_price(self, symbol: str) -> Decimal:
        data = self._request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol}, retry=True)
        row = data[0] if isinstance(data, list) else data
        return Decimal(str(row["markPrice"]))

    def get_klines(self, symbol: str) -> list[list[Any]]:
        return self._request(
            "GET",
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": self.config.kline_interval, "limit": self.config.kline_limit},
            retry=True,
        )

    def get_klines_for_interval(
        self,
        symbol: str,
        interval: str,
        limit: int = 1500,
        start_time_ms: int | None = None,
    ) -> list[list[Any]]:
        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        return self._request("GET", "/fapi/v1/klines", params, retry=True)

    def get_aggregate_trades(
        self,
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
    ) -> list[dict[str, Any]]:
        if end_time_ms < start_time_ms:
            raise ValueError("aggregate trade end time must not precede start time")

        rows: list[dict[str, Any]] = []
        seen_ids: set[int] = set()
        from_id: int | None = None
        while True:
            if from_id is None:
                params = {
                    "symbol": symbol,
                    "startTime": start_time_ms,
                    "endTime": end_time_ms,
                    "limit": 1000,
                }
            else:
                params = {"symbol": symbol, "fromId": from_id, "limit": 1000}

            batch = self._request("GET", "/fapi/v1/aggTrades", params, retry=True)
            if not isinstance(batch, list):
                raise BinanceAPIError(f"Invalid aggregate trades response for {symbol}.")

            for item in batch:
                trade_time = int(item.get("T", -1))
                trade_id = int(item.get("a", -1))
                if start_time_ms <= trade_time <= end_time_ms and trade_id not in seen_ids:
                    rows.append(item)
                    seen_ids.add(trade_id)

            if len(batch) < 1000:
                break
            last_trade_id = int(batch[-1].get("a", -1))
            last_trade_time = int(batch[-1].get("T", -1))
            if last_trade_id < 0 or last_trade_time > end_time_ms:
                break
            next_from_id = last_trade_id + 1
            if from_id is not None and next_from_id <= from_id:
                raise BinanceAPIError(f"Aggregate trade pagination stalled for {symbol}.")
            from_id = next_from_id

        rows.sort(key=lambda item: (int(item.get("T", -1)), int(item.get("a", -1))))
        return rows

    def get_24hr_ticker(self, symbol: str) -> dict[str, Any]:
        return self._request(
            "GET",
            "/fapi/v1/ticker/24hr",
            {"symbol": symbol},
            retry=True,
        )

    def get_24hr_tickers(self) -> list[dict[str, Any]]:
        data = self._request("GET", "/fapi/v1/ticker/24hr", retry=True)
        return data if isinstance(data, list) else [data]

    def get_exchange_info(self) -> dict[str, Any]:
        if self._exchange_info is None:
            self._exchange_info = self._request("GET", "/fapi/v1/exchangeInfo", retry=True)
        return self._exchange_info

    def get_tradable_usdt_perpetual_symbols(self) -> set[str]:
        symbols = set()
        for item in self.get_exchange_info().get("symbols", []):
            if item.get("quoteAsset") != "USDT":
                continue
            if item.get("status") != "TRADING":
                continue
            if item.get("contractType") not in {"PERPETUAL", "TRADIFI_PERPETUAL"}:
                continue
            symbol = item.get("symbol")
            if symbol:
                symbols.add(symbol)
        return symbols

    def get_symbol_info(self, symbol: str) -> dict[str, Any]:
        for item in self.get_exchange_info().get("symbols", []):
            if item.get("symbol") == symbol:
                return item
        raise BinanceAPIError(f"Symbol not found in exchangeInfo: {symbol}")

    def get_available_usdt(self) -> Decimal:
        if self.config.dry_run and not self.has_credentials:
            return self.config.dry_run_balance_usdt

        data = self._request("GET", "/fapi/v2/balance", signed=True, retry=True)
        for asset in data:
            if asset.get("asset") == "USDT":
                return Decimal(str(asset.get("availableBalance", "0")))
        raise BinanceAPIError("USDT balance not found.")

    def get_positions(self) -> list[dict[str, Any]]:
        if self.config.dry_run and not self.has_credentials:
            return []
        return self._request("GET", "/fapi/v3/positionRisk", signed=True, retry=True)

    def get_open_positions(self) -> list[dict[str, Any]]:
        positions = []
        for position in self.get_positions():
            amount = Decimal(str(position.get("positionAmt", "0")))
            if amount != 0:
                positions.append(position)
        return positions

    def get_order(
        self,
        symbol: str,
        order_id: int | str | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        if order_id is None and not orig_client_order_id:
            raise ValueError("order_id or orig_client_order_id is required")
        params: dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        else:
            params["origClientOrderId"] = orig_client_order_id
        return self._request(
            "GET",
            "/fapi/v1/order",
            params,
            signed=True,
            retry=True,
        )

    def get_algo_order(
        self,
        algo_id: int | str | None = None,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        if algo_id is None and not client_algo_id:
            raise ValueError("algo_id or client_algo_id is required")
        params: dict[str, Any] = {}
        if algo_id is not None:
            params["algoId"] = algo_id
        else:
            params["clientAlgoId"] = client_algo_id
        return self._request(
            "GET",
            "/fapi/v1/algoOrder",
            params,
            signed=True,
            retry=True,
        )

    def cancel_algo_order(
        self,
        algo_id: int | str | None = None,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        if algo_id is None and not client_algo_id:
            raise ValueError("algo_id or client_algo_id is required")
        params: dict[str, Any] = {}
        if algo_id is not None:
            params["algoId"] = algo_id
        else:
            params["clientAlgoId"] = client_algo_id
        if self.config.dry_run:
            return {**params, "algoStatus": "CANCELED", "dryRun": True}
        return self._request(
            "DELETE",
            "/fapi/v1/algoOrder",
            params,
            signed=True,
            retry=False,
        )

    def cancel_all_algo_open_orders(self, symbol: str) -> dict[str, Any]:
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("symbol is required")
        if self.config.dry_run:
            return {
                "symbol": symbol,
                "code": 200,
                "msg": "DRY_RUN cancel all algo open orders",
                "dryRun": True,
            }
        return self._request(
            "DELETE",
            "/fapi/v1/algoOpenOrders",
            {"symbol": symbol},
            signed=True,
            retry=False,
        )

    def get_open_algo_orders(self, symbol: str) -> list[dict[str, Any]]:
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("symbol is required")
        if self.config.dry_run:
            return []
        result = self._request(
            "GET",
            "/fapi/v1/openAlgoOrders",
            {"symbol": symbol, "algoType": "CONDITIONAL"},
            signed=True,
            retry=True,
        )
        if not isinstance(result, list) or any(
            not isinstance(item, dict) for item in result
        ):
            raise BinanceAPIError(
                f"Invalid open algo orders response for {symbol}: {result}"
            )
        return result

    def get_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        """Return every ordinary open futures order for one symbol."""

        if not isinstance(symbol, str) or not symbol:
            raise ValueError("symbol is required")
        if self.config.dry_run:
            return []
        result = self._request(
            "GET",
            "/fapi/v1/openOrders",
            {"symbol": symbol},
            signed=True,
            retry=True,
        )
        if not isinstance(result, list) or any(
            not isinstance(item, dict) for item in result
        ):
            raise BinanceAPIError(
                f"Invalid open orders response for {symbol}: {result}"
            )
        return result

    def get_long_position_entry_price(self, symbol: str) -> Decimal:
        for position in self.get_positions():
            if position.get("symbol") != symbol:
                continue
            position_side = str(position.get("positionSide", "BOTH"))
            amount = Decimal(str(position.get("positionAmt", "0")))
            entry_price = Decimal(str(position.get("entryPrice", "0")))
            if position_side in {"BOTH", "LONG", ""} and amount > 0 and entry_price > 0:
                return entry_price
        raise BinanceAPIError(f"Unable to confirm long position entry price for {symbol}.")

    def get_long_position_quantity(self, symbol: str) -> Decimal:
        matched_long_row = False
        for position in self.get_positions():
            if position.get("symbol") != symbol:
                continue
            position_side = str(position.get("positionSide", "BOTH"))
            if position_side not in {"BOTH", "LONG", ""}:
                continue
            matched_long_row = True
            amount = Decimal(str(position.get("positionAmt", "0")))
            if amount > 0:
                return amount
        if matched_long_row:
            return Decimal("0")
        raise BinanceAPIError(f"Unable to confirm long position quantity for {symbol}.")

    def get_long_position_quantity_or_zero(self, symbol: str) -> Decimal:
        for position in self.get_positions():
            if position.get("symbol") != symbol:
                continue
            position_side = str(position.get("positionSide", "BOTH"))
            if position_side not in {"BOTH", "LONG", ""}:
                continue
            amount = Decimal(str(position.get("positionAmt", "0")))
            return amount if amount > 0 else Decimal("0")
        return Decimal("0")

    def is_hedge_position_mode(self) -> bool:
        if self.config.dry_run and not self.has_credentials:
            return False
        if self._dual_side_position is None:
            data = self._request("GET", "/fapi/v1/positionSide/dual", signed=True, retry=True)
            value = data.get("dualSidePosition", False)
            self._dual_side_position = value if isinstance(value, bool) else str(value).lower() == "true"
        return self._dual_side_position

    def _long_position_side_params(self) -> dict[str, str]:
        return {"positionSide": "LONG"} if self.is_hedge_position_mode() else {}

    def get_max_leverage(self, symbol: str) -> int:
        if self.config.dry_run and not self.has_credentials:
            leverage = self.config.dry_run_max_leverage
            try:
                symbol_info = self.get_symbol_info(symbol)
            except BinanceAPIError as exc:
                self.logger.warning(
                    "DRY_RUN | using configured max leverage %sx for %s because symbol info lookup failed: %s",
                    leverage,
                    symbol,
                    exc,
                )
                return leverage

            contract_type = symbol_info.get("contractType")
            if contract_type == "TRADIFI_PERPETUAL":
                leverage = min(leverage, self.config.dry_run_tradifi_max_leverage)

            self.logger.info(
                "DRY_RUN | using simulated max leverage %sx for %s contractType=%s",
                leverage,
                symbol,
                contract_type,
            )
            return leverage

        data = self._request(
            "GET",
            "/fapi/v1/leverageBracket",
            {"symbol": symbol},
            signed=True,
            retry=True,
        )
        items = data if isinstance(data, list) else [data]
        leverages: list[int] = []
        for item in items:
            for bracket in item.get("brackets", []):
                leverages.append(int(bracket.get("initialLeverage", 0)))
        if not leverages:
            raise BinanceAPIError(f"No leverage bracket returned for {symbol}.")
        return max(leverages)

    def set_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        if self.config.dry_run:
            self.logger.info("DRY_RUN | set leverage %s %sx", symbol, leverage)
            return {"symbol": symbol, "leverage": leverage, "dryRun": True}
        return self._request(
            "POST",
            "/fapi/v1/leverage",
            {"symbol": symbol, "leverage": leverage},
            signed=True,
            retry=True,
        )

    def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        params = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": decimal_to_api(quantity),
            "newOrderRespType": "RESULT",
        }
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        if not self.config.dry_run:
            params.update(self._long_position_side_params())
        if self.config.dry_run:
            self.logger.info("DRY_RUN | market order %s", params)
            return {"symbol": symbol, "side": side, "type": "MARKET", "origQty": params["quantity"], "clientOrderId": client_order_id, "dryRun": True}
        return self._request("POST", "/fapi/v1/order", params, signed=True, retry=False)

    def place_close_all_algo_order(
        self,
        symbol: str,
        order_type: str,
        trigger_price: Decimal,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        params = {
            "algoType": "CONDITIONAL",
            "symbol": symbol,
            "side": "SELL",
            "type": order_type,
            "triggerPrice": decimal_to_api(trigger_price),
            "closePosition": "true",
            "workingType": "CONTRACT_PRICE",
        }
        if client_algo_id:
            params["clientAlgoId"] = client_algo_id
        if not self.config.dry_run:
            params.update(self._long_position_side_params())
        if self.config.dry_run:
            self.logger.info("DRY_RUN | algo protection order %s", params)
            return {"symbol": symbol, "type": order_type, "triggerPrice": params["triggerPrice"], "clientAlgoId": client_algo_id, "dryRun": True}
        return self._request("POST", "/fapi/v1/algoOrder", params, signed=True, retry=False)

    def close_position_market(self, symbol: str, quantity: Decimal) -> dict[str, Any]:
        params = {
            "symbol": symbol,
            "side": "SELL",
            "type": "MARKET",
            "quantity": decimal_to_api(quantity),
            "reduceOnly": "true",
            "newOrderRespType": "RESULT",
        }
        if not self.config.dry_run:
            position_side_params = self._long_position_side_params()
            params.update(position_side_params)
            if position_side_params:
                params.pop("reduceOnly", None)
        if self.config.dry_run:
            self.logger.info("DRY_RUN | emergency close %s", params)
            return {"symbol": symbol, "side": "SELL", "type": "MARKET", "origQty": params["quantity"], "dryRun": True}
        return self._request("POST", "/fapi/v1/order", params, signed=True, retry=False)
