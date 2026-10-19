import asyncio


async def drive(venue, ctx):
    async with asyncio.timeout(30):
        while not venue.orders or ctx.ungated_ticks < 5:
            await asyncio.sleep(0.01)
    order = next(order for order in venue.orders.values() if order["status"] == "New")
    await venue.fill(order["id"], order["quantity"], order["price"])
    print(f"FILL entry={order['id']} quantity={order['quantity']} price={order['price']}", flush=True)
