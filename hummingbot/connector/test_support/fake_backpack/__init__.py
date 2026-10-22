"""Loopback-only Backpack test venue and real-connector drivers.

FakeBackpackVenue keeps orders, fills, positions, and paged history in memory.
REST receipts appear in requests.jsonl. WS frames appear in ws.jsonl. Both logs
use t_mono_ns from time.monotonic_ns() for cross-process joins on Linux.
Starting a venue replaces both logs in its state directory. Reusing a directory
does not mix earlier runs or earlier boots into reference-load metrics.
Fault queues are independent for each method and path. A held request receives
no response and does not execute. A dropped request executes before disconnect.
The default LIMIT fill policy is manual. MARKET orders fill at the touch or
expire when the opposite book is empty.
Private fills wait for the orderAccepted delivery attempt. A failed delivery
still releases fills because the venue has accepted the order. Stopping the
venue releases waiters without executing further fills.
A CROSSING trade appears once on the public tape and shares its trade ID with
any resulting private fill.
Crossing post-only limits return INVALID_ORDER. The connector reports an order
failure through its existing LIMIT_MAKER rejection path.

Commands are available with ``python -m
hummingbot.connector.test_support.fake_backpack``.

``serve --port 0 --state-dir DIR`` binds an ephemeral port and prints exactly
``FAKE_VENUE_READY port=PORT``. SIGINT or SIGTERM closes the venue.
``one-fill --state-dir DIR`` waits for the initial REST ticker update before
placing its order. It prints an OrderFilled event for trade 9001 and the signed
order POST.
``run-v2 --controller-yaml FILE --ticks 30 --venue-script FILE`` runs
V2WithControllers. Its venue script defines ``async def drive(venue, ctx)``.
The context exposes the application, strategy, connectors, ticks, and executors.
Only ungated controller iterations count toward --ticks. Shutdown uses stop_loop.
After the requested ticks, run-v2 waits for its drive script to finish. There is
no implicit post-tick timeout. Cancellation still stops the script and strategy.
Strategy discovery uses the checkout's scripts directory, not the caller's
working directory. The harness isolates inherited HTTP factories and gateway
sessions. It closes its own clients before restoring the original singletons.
The harness prevents the gateway monitor from starting. No local Gateway
service participates in a bot run.
The guard checks socket destinations, DNS results, and each aiohttp request
URL, including requests through a loopback proxy.

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
Loop lag samples use a 10 ms schedule. The runtime control_task wrapper associates
CreateExecutorAction IDs with their decision timestamp. The place_order wrapper
consumes that timestamp for only the executor's first order. Take-profit, close,
and retry orders have no controller creation decision and no latency sample.
POST receipts join on client ID, not the closest tick.
Only requests received within the load interval count toward metrics.
Empty percentile inputs produce null, not zero. Request failures, controller
errors, successful POST counts, and sample counts remain visible.
On hosts with at least three CPUs, reference-load pins the venue process to the
last CPU and the bot to the others. The bot's temporary root, which holds its
SQLite database, lives in /dev/shm when that directory is writable. A disk fsync
inside a SQLite commit blocked the event loop for tens of milliseconds and
dominated loop-lag p99 on a host shared with other disk users. The measured
loop lag therefore excludes disk commit latency. Compare runs made with the
same harness only.

Ordinary tests use event-controlled response bodies rather than wall-clock
performance limits. Run the 100 ms unread-body and one-second flood checks with
``mma-slot perf -- env MMA_PERF_TESTS=1 conda run -n hummingbot python -m pytest
test/hummingbot/connector/test_support/fake_backpack/test_fake_backpack.py
-k 'under_100ms or under_one_second' -q``.
The default suite also delivers all 20,000 depth frames and checks the private
fill and final book without a performance threshold.

Public channel and history field names follow https://docs.backpack.exchange/.
History bounds use ``from`` and ``to`` in milliseconds. Naive ISO timestamps are
UTC. History defaults to 100 rows and caps each page at 1000 rows. Fill-history
``from`` is inclusive and ``to`` is exclusive, as documented by Backpack.
Order queries and cancellations accept only resting orders. Filled, cancelled,
and expired orders return 404. Fill history remains available. When the private
stream misses a fill, the stock connector reports it from REST fill history but
does not learn a terminal status from order queries. Repeated 404s exhaust its
not-found limit of three and mark the fully REST-filled order FAILED.
The markPrice ``n`` field is milliseconds. The stock connector converts it to
seconds, as it does for the REST funding timestamp.
Position updates use positionOpened, positionAdjusted, and positionClosed.
Subscribing sends each open position without an event type.
Positions and collateral remain script-controlled through set_position and
refresh_balances. Fills do not run an account ledger. Headless drivers suppress
application notifications through HummingbotLogger.is_testing_mode.
one-fill starts a connector, not a strategy. Its analytics flag writes config
but does not call a strategy-start capture hook. Use pre-start when that hook is
not on the caller's execution path.
The test keys are deterministic ED25519 keys, not credentials for a live account.
"""
