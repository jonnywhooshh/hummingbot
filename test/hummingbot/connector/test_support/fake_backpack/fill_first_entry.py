import asyncio
from decimal import Decimal


async def drive(venue, ctx):
    async with asyncio.timeout(30):
        while not venue.orders or ctx.ungated_ticks < 5:
            await asyncio.sleep(0.01)
    order = next(order for order in venue.orders.values() if order["status"] == "New" and order["side"] == "Bid")
    await venue.fill(order["id"], order["quantity"], order["price"])
    print(f"FILL entry={order['id']} quantity={order['quantity']} price={order['price']}", flush=True)
    async with asyncio.timeout(30):
        while True:
            ctx.collect_executors()
            executor = next((executor for executor in ctx.executors
                             if executor._open_order and executor._open_order.order_id == str(order["clientId"])), None)
            tracked = executor._take_profit_limit_order if executor else None
            take_profit = next((candidate for candidate in venue.orders.values()
                                if tracked and str(candidate["clientId"]) == tracked.order_id), None)
            if take_profit:
                assert take_profit["side"] == "Ask" and take_profit["postOnly"]
                assert Decimal(take_profit["price"]) > Decimal(order["price"])
                print(f"TAKE_PROFIT entry={order['id']} order={take_profit['id']} executor={executor.config.id}", flush=True)
                break
            await asyncio.sleep(0.01)
