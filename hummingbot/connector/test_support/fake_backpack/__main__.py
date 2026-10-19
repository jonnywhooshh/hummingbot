import argparse
import asyncio
import logging
import signal
import tempfile
from pathlib import Path

from hummingbot.connector.test_support.fake_backpack.venue import FakeBackpackVenue


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def positive_minutes(value):
    number = float(value)
    if not 0 < number < float("inf"):
        raise argparse.ArgumentTypeError("must be a finite positive number")
    return number


def parser():
    root = argparse.ArgumentParser(description="Drive Hummingbot against a loopback-only Backpack venue")
    commands = root.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="Start a standalone fake venue")
    serve.add_argument("--port", type=int, default=0)
    serve.add_argument("--state-dir", type=Path, required=True)
    fill = commands.add_parser("one-fill", help="Buy and fill 0.10 SOL through the real connector")
    v2 = commands.add_parser("run-v2", help="Run V2WithControllers and stop its executors")
    v2.add_argument("--controller-yaml", type=Path, required=True)
    v2.add_argument("--ticks", type=positive_int, default=30)
    v2.add_argument("--venue-script", type=Path)
    load = commands.add_parser("reference-load", help="Run four pmm_mister levels with a separate venue process")
    load.add_argument("--minutes", type=positive_minutes, required=True)
    load.add_argument("--out", type=Path, required=True)
    for command in (fill, v2, load):
        command.add_argument("--pre-start", type=Path, help="Run this file's main() before bot startup")
        command.add_argument("--post-stop", type=Path, help="Run this file's main() after bot shutdown")
        command.add_argument("--analytics-dir", type=Path, help="Enable analytics in the temporary client config")
    for command in (fill, v2):
        command.add_argument("--state-dir", type=Path)
    return root


async def serve(port, state_dir):
    venue = FakeBackpackVenue(state_dir=state_dir)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stopped.set)
    try:
        await venue.start(port=port)
        print(f"FAKE_VENUE_READY port={venue.port}", flush=True)
        await stopped.wait()
    finally:
        await venue.stop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(signum)


async def execute(args):
    if args.command == "serve":
        await serve(args.port, args.state_dir)
        return
    common = {"pre_start": args.pre_start, "post_stop": args.post_stop, "analytics_dir": args.analytics_dir}
    if args.command == "reference-load":
        from hummingbot.connector.test_support.fake_backpack.load import reference_load
        metrics = await reference_load(args.minutes, args.out, **common)
        print(f"METRICS {args.out / 'metrics.json'} posts={metrics['order_posts']}", flush=True)
    else:
        from hummingbot.connector.test_support.fake_backpack.harness import one_fill, run_v2
        state_dir = args.state_dir or Path(tempfile.mkdtemp(prefix="fake-backpack-state-"))
        if args.command == "one-fill":
            await one_fill(state_dir=state_dir, **common)
        else:
            await run_v2(args.controller_yaml, args.ticks, args.venue_script, state_dir=state_dir, **common)
        print(f"STATE_DIR {state_dir}", flush=True)


def main():
    args = parser().parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s %(message)s")
    asyncio.run(execute(args))


if __name__ == "__main__":
    main()
