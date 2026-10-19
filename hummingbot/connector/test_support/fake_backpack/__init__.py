"""Loopback-only Backpack test venue and real-connector drivers.

FakeBackpackVenue keeps orders, fills, positions, and paged history in memory.
REST receipts appear in requests.jsonl. WS frames appear in ws.jsonl. Both logs
use t_mono_ns from time.monotonic_ns() for cross-process joins on Linux.
Fault queues are independent for each method and path. A held request receives
no response and does not execute. A dropped request executes before disconnect.
The default LIMIT fill policy is manual. MARKET orders always fill at the touch.
"""
