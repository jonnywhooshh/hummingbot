import asyncio
import base64
import json
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from aiohttp import WSMsgType, web
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

SYMBOL = "SOL_USDC_PERP"
PRIVATE_STREAMS = {"account.orderUpdate", "account.positionUpdate"}
FEE_RATES = {True: Decimal("0.0002"), False: Decimal("0.0005")}
INSTRUCTIONS = {
    ("GET", "/api/v1/capital/collateral"): "collateralQuery",
    ("GET", "/api/v1/capital"): "balanceQuery",
    ("GET", "/api/v1/account"): "accountQuery",
    ("PATCH", "/api/v1/account"): "accountUpdate",
    ("GET", "/api/v1/position"): "positionQuery",
    ("GET", "/api/v1/order"): "orderQuery",
    ("GET", "/api/v1/orders"): "orderQueryAll",
    ("POST", "/api/v1/order"): "orderExecute",
    ("DELETE", "/api/v1/order"): "orderCancel",
    ("GET", "/wapi/v1/history/fills"): "fillHistoryQueryAll",
    ("GET", "/wapi/v1/history/orders"): "orderHistoryQueryAll",
    ("GET", "/wapi/v1/history/funding"): "fundingHistoryQueryAll",
    ("GET", "/wapi/v1/capital/deposits"): "depositQueryAll",
    ("GET", "/wapi/v1/capital/withdrawals"): "withdrawalQueryAll",
}


def keypair(seed: bytes = bytes(range(32))) -> tuple[str, str]:
    public = ed25519.Ed25519PrivateKey.from_private_bytes(seed).public_key()
    raw = public.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return base64.b64encode(raw).decode(), base64.b64encode(seed).decode()


def signing_string(instruction: str, params: dict, timestamp: str, window: str) -> str:
    body = "&".join(f"{k}={params[k]}" for k in sorted(params)).replace("True", "true").replace("False", "false")
    return "&".join([f"instruction={instruction}"] + ([body] if body else []) +
                    [f"timestamp={timestamp}&window={window}"])


def signature_valid(api_key: str, signature: str, message: str) -> bool:
    try:
        public = ed25519.Ed25519PublicKey.from_public_bytes(base64.b64decode(api_key))
        public.verify(base64.b64decode(signature), message.encode())
        return True
    except (ValueError, InvalidSignature):
        return False


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


class FillPolicy(str, Enum):
    MANUAL = "manual"
    CROSSING = "crossing"
    IMMEDIATE = "immediate"


@dataclass
class RestRecord:
    t_mono_ns: int
    method: str
    path: str
    query: dict
    headers: dict
    body: str
    sig_ok: Optional[bool] = None
    status: Optional[int] = None

    @property
    def t_recv(self) -> float:
        return self.t_mono_ns / 1e9


@dataclass
class Fault:
    status: Optional[int] = None
    body: Any = None
    body_delay: float = 0.0
    drop_after_handling: bool = False
    hold: bool = False


class FakeBackpackVenue:
    def __init__(self, api_key: Optional[str] = None, mid: str = "150.00", state_dir=None,
                 fill_policy: FillPolicy = FillPolicy.MANUAL, symbol: str = SYMBOL):
        self.api_key = api_key or keypair()[0]
        self.symbol = symbol
        self.mid = mid
        self.state_dir = Path(state_dir) if state_dir is not None else None
        self.fill_policy = FillPolicy(fill_policy)
        self.requests: list[RestRecord] = []
        self.ws_inbound: list[tuple[float, int, dict]] = []
        self.ws_outbound: list[tuple[float, int, dict]] = []
        self.subscriptions: dict[web.WebSocketResponse, set[str]] = {}
        self.faults = defaultdict(deque)
        self.orders: dict[str, dict] = {}
        self.fills: list[dict] = []
        self.funding: list[dict] = []
        self.deposits: list[dict] = []
        self.withdrawals: list[dict] = []
        self.positions: list[dict] = []
        self.collateral = {"netEquity": "100", "netEquityAvailable": "100", "collateral": []}
        self.balances = {"USDC": {"available": "100", "locked": "0", "staked": "0"}}
        self.leverage = "5"
        self.update_id = 1000
        self.book = {"bids": [["149.90", "5"], ["149.80", "8"]],
                     "asks": [["150.10", "5"], ["150.20", "8"]]}
        self.port = 0
        self.ws_refused_until = 0.0
        self.ack_delay = 0.0
        self._next_order_id = 111000
        self._next_trade_id = 9000
        self._sub_changed = asyncio.Event()
        self._stopping = asyncio.Event()
        self._runner = None
        self._tasks = set()
        self._rest_log = None
        self._ws_log = None
        self._routes = {
            ("GET", "/api/v1/ping"): lambda r, p: web.Response(text="pong"),
            ("GET", "/api/v1/time"): lambda r, p: web.Response(text=str(now_ms())),
            ("GET", "/api/v1/markets"): lambda r, p: web.json_response([self.market()]),
            ("GET", "/api/v1/depth"): lambda r, p: web.json_response(
                {**self.book, "lastUpdateId": str(self.update_id), "timestamp": now_ms()}),
            ("GET", "/api/v1/markPrices"): lambda r, p: web.json_response([self.mark_price()]),
            ("GET", "/api/v1/ticker"): lambda r, p: web.json_response(
                {"symbol": self.symbol, "lastPrice": self.mid}),
            ("GET", "/api/v1/capital/collateral"): lambda r, p: web.json_response(self.collateral),
            ("GET", "/api/v1/capital"): lambda r, p: web.json_response(self.balances),
            ("GET", "/api/v1/account"): lambda r, p: web.json_response({"leverageLimit": self.leverage}),
            ("PATCH", "/api/v1/account"): self._account_update,
            ("GET", "/api/v1/position"): lambda r, p: web.json_response(self.positions),
            ("GET", "/api/v1/orders"): lambda r, p: web.json_response(
                [o for o in self.orders.values() if o["status"] in ("New", "PartiallyFilled")]),
            ("GET", "/wapi/v1/history/funding"): lambda r, p: self._history(self.funding, p),
            ("GET", "/wapi/v1/history/fills"): lambda r, p: self._history(self.fills, p),
            ("GET", "/wapi/v1/history/orders"): lambda r, p: self._history(list(self.orders.values()), p),
            ("GET", "/wapi/v1/capital/deposits"): lambda r, p: self._history(self.deposits, p),
            ("GET", "/wapi/v1/capital/withdrawals"): lambda r, p: self._history(self.withdrawals, p),
            ("POST", "/api/v1/order"): self._order_execute,
            ("DELETE", "/api/v1/order"): self._order_cancel,
            ("GET", "/api/v1/order"): self._order_query,
        }

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> int:
        if host not in ("127.0.0.1", "::1", "localhost"):
            raise ValueError("The fake venue only binds to loopback")
        if self.state_dir is not None:
            self.state_dir.mkdir(parents=True, exist_ok=True)
            self._rest_log = (self.state_dir / "requests.jsonl").open("a")
            self._ws_log = (self.state_dir / "ws.jsonl").open("a")
            self._schedule(self._flush_logs_forever())
        app = web.Application()
        app.router.add_get("/", self._ws)
        app.router.add_route("*", "/{tail:.+}", self._rest)
        self._runner = web.AppRunner(app, access_log=None, shutdown_timeout=0.1)
        await self._runner.setup()
        site = web.TCPSite(self._runner, host, port)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]
        return self.port

    async def stop(self):
        self._stopping.set()
        for ws in list(self.subscriptions):
            await ws.close()
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._runner is not None:
            await self._runner.cleanup()
        for log in (self._rest_log, self._ws_log):
            if log is not None:
                log.close()

    def _schedule(self, coroutine):
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def market(self) -> dict:
        base, quote, _ = self.symbol.split("_")
        return {"symbol": self.symbol, "baseSymbol": base, "quoteSymbol": quote, "marketType": "PERP",
                "visible": True, "orderBookState": "Open",
                "filters": {"price": {"tickSize": "0.01", "minPrice": "0.01"},
                            "quantity": {"stepSize": "0.01", "minQuantity": "0.01"}}}

    def mark_price(self) -> dict:
        return {"symbol": self.symbol, "markPrice": self.mid, "indexPrice": self.mid,
                "fundingRate": "0.0000125", "nextFundingTimestamp": (now_ms() // 3600000 + 1) * 3600000}

    async def wait_subscribed(self, stream: str, timeout: float = 10.0):
        async def wait():
            while not any(stream in streams for streams in self.subscriptions.values()):
                self._sub_changed.clear()
                await self._sub_changed.wait()
        await asyncio.wait_for(wait(), timeout)

    def flush_logs(self):
        for log in (self._rest_log, self._ws_log):
            if log is not None and not log.closed:
                log.flush()

    async def _flush_logs_forever(self):
        while True:
            await asyncio.sleep(0.1)
            self.flush_logs()

    def _log_ws(self, direction, connection, message_type, payload):
        if self._ws_log is not None:
            self._ws_log.write(json.dumps({"t_mono_ns": time.monotonic_ns(), "direction": direction,
                                          "connection_id": connection, "type": message_type, "payload": payload}) + "\n")

    async def push(self, stream: str, data: dict) -> int:
        frame = json.dumps({"stream": stream, "data": data})
        targets = [ws for ws, streams in self.subscriptions.items() if stream in streams]
        self.ws_outbound.append((time.monotonic(), len(targets), data))
        for ws in targets:
            self._log_ws("out", id(ws), "TEXT", {"stream": stream, "data": data})
            await ws.send_str(frame)
        return len(targets)

    async def push_depth(self, bids: list, asks: list, first_id: Optional[int] = None) -> int:
        first = self.update_id + 1 if first_id is None else first_id
        self.update_id = first
        for side, updates in (("bids", bids), ("asks", asks)):
            levels = {p: q for p, q in self.book[side]}
            for price, quantity in updates:
                if Decimal(quantity) == 0:
                    levels.pop(price, None)
                else:
                    levels[price] = quantity
            self.book[side] = sorted(levels.items(), key=lambda level: Decimal(level[0]), reverse=side == "bids")
        t = now_ms() * 1000
        delivered = await self.push(f"depth.{self.symbol}",
                                    {"e": "depth", "E": t, "T": t, "s": self.symbol,
                                     "U": first, "u": first, "b": bids, "a": asks})
        if self.fill_policy == FillPolicy.CROSSING:
            await self.match_resting()
        return delivered

    async def push_book_ticker(self):
        t = now_ms() * 1000
        return await self.push(f"bookTicker.{self.symbol}",
                               {"e": "bookTicker", "E": t, "T": t, "s": self.symbol, "u": self.update_id,
                                "b": self.book["bids"][0][0], "B": self.book["bids"][0][1],
                                "a": self.book["asks"][0][0], "A": self.book["asks"][0][1]})

    async def push_mark_price(self):
        t = now_ms() * 1000
        return await self.push(f"markPrice.{self.symbol}",
                               {"e": "markPrice", "E": t, "s": self.symbol, "p": self.mid,
                                "i": self.mid, "f": "0.0000125", "n": self.mark_price()["nextFundingTimestamp"] * 1000})

    async def push_trade(self, price: Optional[str] = None, quantity: str = "0.10"):
        t = now_ms() * 1000
        price = price or self.mid
        delivered = await self.push(f"trade.{self.symbol}",
                                    {"e": "trade", "E": t, "T": t, "s": self.symbol, "p": price,
                                     "q": quantity, "m": True, "t": t})
        if self.fill_policy == FillPolicy.CROSSING:
            await self.match_resting(price, quantity)
        return delivered

    async def push_liquidation(self, **values):
        return await self.push(f"liquidation.{self.symbol}",
                               {"e": "liquidation", "E": now_ms() * 1000, "s": self.symbol,
                                "S": "Bid", "p": self.mid, "q": "1", **values})

    async def push_open_interest(self, quantity: str = "1000"):
        return await self.push(f"openInterest.{self.symbol}",
                               {"e": "openInterest", "E": now_ms() * 1000, "s": self.symbol, "o": quantity})

    async def set_position(self, quantity: str, entry_price: Optional[str] = None,
                           unrealized_pnl: str = "0", symbol: Optional[str] = None):
        symbol = symbol or self.symbol
        row = {"symbol": symbol, "netQuantity": quantity, "entryPrice": entry_price or self.mid,
               "pnlUnrealized": unrealized_pnl, "markPrice": self.mid}
        self.positions = [p for p in self.positions if p["symbol"] != symbol]
        if Decimal(quantity) != 0:
            self.positions.append(row)
        await self.push("account.positionUpdate", {"e": "positionUpdate", "E": now_ms() * 1000,
                                                   "s": symbol, "q": quantity, "B": row["entryPrice"],
                                                   "P": unrealized_pnl, "M": self.mid})
        return row

    def refresh_balances(self, net_equity: str = "100", available: Optional[str] = None, collateral=None):
        available = net_equity if available is None else available
        self.collateral = {"netEquity": net_equity, "netEquityAvailable": available,
                           "collateral": [] if collateral is None else collateral}
        self.balances["USDC"] = {"available": available,
                                 "locked": str(Decimal(net_equity) - Decimal(available)), "staked": "0"}

    def add_funding(self, quantity: str, rate: str = "0.0000125", **values) -> dict:
        row = {"symbol": self.symbol, "quantity": quantity, "fundingRate": rate,
               "intervalEndTimestamp": timestamp(), **values}
        self.funding.append(row)
        return row

    def _order_frame(self, event: str, order: dict, **extra) -> dict:
        t = now_ms() * 1000
        return {"e": event, "E": t, "T": t, "s": order["symbol"], "c": order["clientId"],
                "S": order["side"], "o": order["orderType"].upper(), "f": order["timeInForce"],
                "q": order["quantity"], "p": order["price"], "X": order["status"], "i": order["id"],
                "z": order["executedQuantity"], **extra}

    async def fill(self, order_id: str, qty: str, price: str, maker: bool = True,
                   fee: Optional[str] = None, fee_symbol: str = "USDC") -> dict:
        order = self.orders[str(order_id)]
        quantity = Decimal(qty)
        remaining = Decimal(order["quantity"]) - Decimal(order["executedQuantity"])
        if order["status"] not in ("New", "PartiallyFilled") or not 0 < quantity <= remaining:
            raise ValueError("Fill quantity must be positive and no greater than the open quantity")
        executed = Decimal(order["executedQuantity"]) + quantity
        order["executedQuantity"] = str(executed)
        order["executedQuoteQuantity"] = str(Decimal(order["executedQuoteQuantity"]) + quantity * Decimal(price))
        order["status"] = "Filled" if executed == Decimal(order["quantity"]) else "PartiallyFilled"
        fee = str((quantity * Decimal(price) * FEE_RATES[maker]).normalize()) if fee is None else fee
        self._next_trade_id += 1
        frame = self._order_frame("orderFill", order, l=qty, L=price, m=maker, t=self._next_trade_id, n=fee, N=fee_symbol)
        self.fills.append({"tradeId": self._next_trade_id, "orderId": str(order_id), "symbol": order["symbol"],
                           "price": price, "quantity": qty, "fee": fee, "feeSymbol": fee_symbol,
                           "isMaker": maker, "side": order["side"], "timestamp": timestamp()})
        await self.push("account.orderUpdate", frame)
        return frame

    async def match_resting(self, trade_price: Optional[str] = None, quantity: Optional[str] = None):
        available = Decimal(quantity) if quantity is not None else None
        for order in list(self.orders.values()):
            if order["status"] not in ("New", "PartiallyFilled") or order["orderType"] != "Limit":
                continue
            touch = trade_price or self.book["asks" if order["side"] == "Bid" else "bids"][0][0]
            crossed = Decimal(order["price"]) >= Decimal(touch) if order["side"] == "Bid" else (
                Decimal(order["price"]) <= Decimal(touch))
            if self.fill_policy == FillPolicy.IMMEDIATE or crossed:
                amount = Decimal(order["quantity"]) - Decimal(order["executedQuantity"])
                amount = min(amount, available) if available is not None else amount
                if amount > 0:
                    await self.fill(order["id"], str(amount), order["price"])
                    if available is not None:
                        available -= amount

    async def drop_ws(self, private_only: bool = False, refuse_for: float = 0.0):
        self.ws_refused_until = time.monotonic() + refuse_for
        for ws, streams in list(self.subscriptions.items()):
            if not private_only or streams & PRIVATE_STREAMS:
                await ws.close()

    def fail_next(self, method: str, path: str, status: int, body: Any):
        self.faults[(method.upper(), path)].append(Fault(status=status, body=body))

    def delay_body_next(self, method: str, path: str, seconds: float):
        self.faults[(method.upper(), path)].append(Fault(body_delay=seconds))

    def drop_next(self, method: str, path: str):
        self.faults[(method.upper(), path)].append(Fault(drop_after_handling=True))

    def hold_next(self, method: str, path: str):
        self.faults[(method.upper(), path)].append(Fault(hold=True))

    def requests_to(self, method: str, path: str) -> list[RestRecord]:
        return [r for r in self.requests if r.method == method.upper() and r.path == path]

    @staticmethod
    def _history(rows: list, params: dict):
        filtered = [r for r in rows if all(str(r.get(k)) == str(params[k]) for k in ("symbol", "orderId") if k in params)]

        def row_time(row):
            value = row.get("timestamp", row.get("intervalEndTimestamp", row.get("createdAt", 0)))
            if isinstance(value, str) and "T" in value:
                return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
            return int(value)

        if "startTime" in params:
            filtered = [r for r in filtered if row_time(r) >= int(params["startTime"]) * 1000]
        if "endTime" in params:
            filtered = [r for r in filtered if row_time(r) <= int(params["endTime"]) * 1000]
        filtered.sort(key=row_time, reverse=params.get("sortDirection", "Asc") == "Desc")
        offset, limit = int(params.get("offset", 0)), int(params.get("limit", 1000))
        return web.json_response(filtered[offset:offset + limit])

    def _finish(self, record, response):
        record.status = response.status if response is not None else None
        if self._rest_log is not None:
            self._rest_log.write(json.dumps(asdict(record)) + "\n")
            self._rest_log.flush()
        return response

    async def _rest(self, request):
        received = time.monotonic_ns()
        key = (request.method, request.path)
        text = await request.text()
        params = json.loads(text) if text else dict(request.query)
        record = RestRecord(received, request.method, request.path, dict(request.query), dict(request.headers), text)
        self.requests.append(record)
        if key in INSTRUCTIONS:
            record.sig_ok = request.headers.get("X-API-Key") == self.api_key and signature_valid(
                self.api_key, request.headers.get("X-Signature", ""),
                signing_string(INSTRUCTIONS[key], params, request.headers.get("X-Timestamp", ""),
                               request.headers.get("X-Window", "")))
            if not record.sig_ok:
                return self._finish(record, web.json_response(
                    {"code": "INVALID_CLIENT_REQUEST", "message": "Invalid signature"}, status=401))
        fault = self.faults[key].popleft() if self.faults.get(key) else Fault()
        if fault.hold:
            self._finish(record, None)
            await self._stopping.wait()
            raise asyncio.CancelledError()
        if fault.status is not None:
            return self._finish(record, web.json_response(fault.body, status=fault.status))
        handler = self._routes.get(key)
        if handler is None:
            return self._finish(record, web.json_response({"code": "NOT_FOUND", "message": "Not Found"}, status=404))
        response = handler(request, params)
        if asyncio.iscoroutine(response):
            response = await response
        self._finish(record, response)
        if fault.drop_after_handling:
            request.transport.close()
        elif fault.body_delay:
            stream = web.StreamResponse(status=response.status, headers={"Content-Type": response.content_type})
            await stream.prepare(request)
            try:
                await asyncio.sleep(fault.body_delay)
                await stream.write_eof(response.body or b"")
            except (ConnectionResetError, asyncio.CancelledError):
                pass
            return stream
        return response

    def _account_update(self, request, params):
        self.leverage = params.get("leverageLimit", self.leverage)
        return web.Response(status=200)

    def _order_execute(self, request, params):
        self._next_order_id += 1
        order_id = str(self._next_order_id)
        order = {"id": order_id, "clientId": params["clientId"], "symbol": params["symbol"], "side": params["side"],
                 "orderType": params["orderType"], "quantity": params["quantity"], "price": params.get("price"),
                 "postOnly": params.get("postOnly", False), "timeInForce": params.get("timeInForce", "GTC"),
                 "status": "New", "executedQuantity": "0", "executedQuoteQuantity": "0", "createdAt": now_ms()}
        self.orders[order_id] = order
        self._schedule(self._after_accept(order))
        return web.json_response(order)

    async def _after_accept(self, order):
        await asyncio.sleep(self.ack_delay)
        await self.push("account.orderUpdate", self._order_frame("orderAccepted", order))
        if order["orderType"] == "Market":
            touch = self.book["asks" if order["side"] == "Bid" else "bids"][0][0]
            await self.fill(order["id"], order["quantity"], touch, maker=False)
        elif self.fill_policy != FillPolicy.MANUAL:
            await self.match_resting()

    def _find_order(self, params):
        if "orderId" in params:
            return self.orders.get(str(params["orderId"]))
        return next((o for o in self.orders.values() if str(o["clientId"]) == str(params.get("clientId"))), None)

    def _order_cancel(self, request, params):
        order = self._find_order(params)
        if order is None or order["status"] in ("Filled", "Cancelled"):
            return web.json_response({"code": "RESOURCE_NOT_FOUND", "message": "Not Found"}, status=404)
        order["status"] = "Cancelled"
        self._schedule(self.push("account.orderUpdate", self._order_frame("orderCancelled", order)))
        return web.json_response(order)

    def _order_query(self, request, params):
        order = self._find_order(params)
        if order is None:
            return web.json_response({"code": "RESOURCE_NOT_FOUND", "message": "Not Found"}, status=404)
        return web.json_response(order)

    async def _ws(self, request):
        if time.monotonic() < self.ws_refused_until:
            return web.Response(status=503)
        ws = web.WebSocketResponse(autoping=False)
        await ws.prepare(request)
        self.subscriptions[ws] = set()
        try:
            async for msg in ws:
                payload = json.loads(msg.data) if msg.type == WSMsgType.TEXT else (
                    base64.b64encode(msg.data).decode() if isinstance(msg.data, bytes) else str(msg.data))
                self._log_ws("in", id(ws), msg.type.name, payload)
                if msg.type == WSMsgType.PING:
                    self._log_ws("out", id(ws), "PONG", base64.b64encode(msg.data).decode())
                    await ws.pong(msg.data)
                if msg.type != WSMsgType.TEXT:
                    continue
                self.ws_inbound.append((time.monotonic(), id(ws), payload))
                streams = set(payload.get("params", []))
                if payload.get("method") == "SUBSCRIBE":
                    if streams & PRIVATE_STREAMS and not self._ws_signature_ok(payload.get("signature")):
                        error = {"id": None, "error": {"code": 4006, "message": "Invalid signature"}}
                        self._log_ws("out", id(ws), "TEXT", error)
                        await ws.send_json(error)
                        continue
                    self.subscriptions[ws] |= streams
                elif payload.get("method") == "UNSUBSCRIBE":
                    self.subscriptions[ws] -= streams
                self._sub_changed.set()
        finally:
            self._log_ws("in", id(ws), "CLOSE", ws.close_code)
            self.subscriptions.pop(ws, None)
        return ws

    def _ws_signature_ok(self, signature):
        return bool(signature and len(signature) == 4 and signature[0] == self.api_key and signature_valid(
            signature[0], signature[1], signing_string("subscribe", {}, signature[2], signature[3])))
