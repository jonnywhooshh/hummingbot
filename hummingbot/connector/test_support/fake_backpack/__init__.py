"""Loopback-only Backpack test venue and real-connector drivers.

FakeBackpackVenue keeps orders, fills, positions, and paged history in memory.
REST receipts appear in requests.jsonl. WS frames appear in ws.jsonl. Both logs
use t_mono_ns from time.monotonic_ns() for cross-process joins on Linux.
Fault queues are independent for each method and path. A held request receives
no response and does not execute. A dropped request executes before disconnect.
The default LIMIT fill policy is manual. MARKET orders always fill at the touch.

Commands are available with ``python -m
hummingbot.connector.test_support.fake_backpack``.

``serve --port 0 --state-dir DIR`` binds an ephemeral port and prints exactly
``FAKE_VENUE_READY port=PORT``. SIGINT or SIGTERM closes the venue.
``one-fill --state-dir DIR`` prints an OrderFilled event for trade 9001 and
the signed order POST.
``run-v2 --controller-yaml FILE --ticks 30 --venue-script FILE`` runs
V2WithControllers. Its venue script defines ``async def drive(venue, ctx)``.
The context exposes the application, strategy, connectors, ticks, and executors.
Only ungated controller iterations count toward --ticks. Shutdown uses stop_loop.

All three bot commands accept --pre-start FILE and --post-stop FILE. Each file
defines main(), which may be synchronous or asynchronous. Both callbacks run in
the bot process. The first runs before connector or application startup. The
second runs after shutdown, including when the bot raises.
--analytics-dir DIR adds an enabled analytics block to the temporary
conf_client.yml. Without that flag the block is absent. The package has no
analytics imports and can run against trunk's product code.

``reference-load --minutes 5 --out DIR`` runs four pmm_mister levels and a venue
in a separate process. The controller allocates 50 USDC with portfolio_allocation
1.0, so its amounts do not quantize to zero. The load sends 47 depth frames per
second. It sends bookTicker, markPrice, and trade once per second. Section 5.2
specifies only the depth rate, so the auxiliary rates are explicit fixed inputs,
not claims about a measured venue rate. The bot does not gain subscriptions.
Delivered depth counts exclude frames with no depth subscriber.

metrics.json records loop_lag_p99_ms, decision_to_post_p50_ms,
decision_to_post_p99_ms, cpu_seconds_per_minute, and one
delivered_depth_frames_per_second value per minute, including a partial minute.
CPU time covers only the bot during load, not startup, shutdown, or the venue.
Loop lag samples use a 10 ms schedule. Decision timestamps come from the runtime
ControllerBase.control_task wrapper and join order receipts on Linux monotonic
time. Empty percentile inputs produce null, not zero. Request failures,
controller errors, successful POST counts, and sample counts remain visible.

Public channel and history field names follow https://docs.backpack.exchange/.
The test keys are deterministic ED25519 keys, not credentials for a live account.
"""
