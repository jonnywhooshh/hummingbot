import json
import os
import shutil
import signal
import subprocess
import sys
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

import yaml

import hummingbot
from hummingbot.connector.test_support.fake_backpack.harness import v2_headless
from hummingbot.connector.test_support.fake_backpack.load import REFERENCE_CONTROLLER


class CliLifecycleTests(unittest.TestCase):
    def wait_until(self, predicate, description, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.05)
        self.fail(f"Timed out waiting for {description}")

    @staticmethod
    def running(pid):
        try:
            return Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()[0] != "Z"
        except FileNotFoundError:
            return False

    def interrupt_cli(self, command, signum):
        checkout = Path(hummingbot.__file__).resolve().parents[1]
        with TemporaryDirectory() as directory:
            work = Path(directory)
            marker = work / "started.json"
            callback = work / "before.py"
            callback.write_text(
                "import json\nimport multiprocessing\nfrom pathlib import Path\n"
                "from hummingbot.client import settings\n"
                "def main():\n"
                "    root = settings.CLIENT_CONFIG_PATH.parents[1]\n"
                f"    Path({str(marker)!r}).write_text(json.dumps(\n"
                "        {'root': str(root), 'children': [p.pid for p in multiprocessing.active_children()]}))\n")
            arguments = [command, "--pre-start", str(callback)]
            if command == "run-v2":
                marker.unlink(missing_ok=True)
                controller = work / "controller.yml"
                controller.write_text(yaml.safe_dump(REFERENCE_CONTROLLER))
                drive = work / "drive.py"
                drive.write_text(
                    "import asyncio\nimport json\nfrom pathlib import Path\n"
                    "async def drive(venue, ctx):\n"
                    "    await ctx.wait_ticks(1)\n"
                    f"    Path({str(marker)!r}).write_text(json.dumps({{'root': str(ctx.root), 'children': []}}))\n"
                    "    await asyncio.Event().wait()\n")
                arguments = [command, "--controller-yaml", str(controller), "--ticks", "1",
                             "--venue-script", str(drive), "--state-dir", str(work / "venue")]
            else:
                arguments += ["--minutes", "5", "--out", str(work / "load")]
            process = subprocess.Popen(
                [sys.executable, "-m", "hummingbot.connector.test_support.fake_backpack", *arguments],
                cwd=work, env={**os.environ, "PYTHONPATH": str(checkout)},
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            root = None
            children = []
            try:
                self.wait_until(marker.exists, "the bot to enter its context")
                started = json.loads(marker.read_text())
                root = Path(started["root"])
                children = started["children"]
                self.assertTrue(root.is_dir())
                if command == "reference-load":
                    self.assertEqual(1, len(children))
                    frames = work / "load/venue/ws.jsonl"
                    self.wait_until(lambda: frames.exists() and '"e": "depth"' in frames.read_text(),
                                    "the venue child to produce load")
                process.send_signal(signum)
                process.wait(timeout=45)
                if signum == signal.SIGTERM:
                    self.assertFalse(root.exists(), f"SIGTERM leaked {root}")
                self.wait_until(lambda: all(not self.running(pid) for pid in children),
                                "the venue child to exit", timeout=10)
                if signum == signal.SIGKILL:
                    self.assertTrue(root.exists(), "SIGKILL must leave a root for the next-start sweep")
                    result = subprocess.run(
                        [sys.executable, "-m", "hummingbot.connector.test_support.fake_backpack", "one-fill",
                         "--state-dir", str(work / "next-venue")],
                        cwd=work, env={**os.environ, "PYTHONPATH": str(checkout)},
                        capture_output=True, text=True, timeout=60)
                    self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                    self.assertFalse(root.exists(), f"The next start did not sweep {root}")
            finally:
                if process.poll() is None:
                    process.kill()
                for pid in children:
                    if self.running(pid):
                        os.kill(pid, signal.SIGKILL)
                output, _ = process.communicate(timeout=15)
                print(f"{command} {signal.Signals(signum).name}\n{output}")
                if root is not None and root.exists():
                    shutil.rmtree(root)

    def test_sigterm_cleans_run_v2_with_a_hung_script(self):
        self.interrupt_cli("run-v2", signal.SIGTERM)

    def test_sigterm_cleans_reference_load_and_its_child(self):
        self.interrupt_cli("reference-load", signal.SIGTERM)

    def test_sigkill_stops_reference_venue_and_next_start_sweeps_root(self):
        self.interrupt_cli("reference-load", signal.SIGKILL)


class RootOwnershipTests(IsolatedAsyncioWrapperTestCase):
    async def test_next_start_sweeps_only_dead_owned_roots(self):
        with TemporaryDirectory() as directory:
            work = Path(directory)
            child = subprocess.Popen([sys.executable, "-c", "pass"])
            child.wait(timeout=10)
            stale = work / f"fake-backpack-v2-{child.pid}-dead"
            live = work / f"fake-backpack-v2-{os.getpid()}-live"
            legacy = work / "fake-backpack-v2-legacy"
            target = work / "keep"
            link = work / f"fake-backpack-v2-{child.pid}-link"
            for root in (stale, live, legacy, target):
                root.mkdir()
                (root / "data").write_text("retain unless owned by a dead PID")
            link.symlink_to(target, target_is_directory=True)
            controller = work / "controller.yml"
            controller.write_text(yaml.safe_dump(REFERENCE_CONTROLLER))
            before = work / "before.py"
            before.write_text("def main():\n    raise RuntimeError('stop before startup')\n")
            with self.assertRaisesRegex(RuntimeError, "stop before startup"):
                async with v2_headless(controller, pre_start=before, temp_dir=work):
                    self.fail("The callback must stop startup")
            self.assertFalse(stale.exists())
            self.assertTrue(live.is_dir())
            self.assertTrue(legacy.is_dir())
            self.assertTrue(link.is_symlink())
            self.assertTrue((target / "data").is_file())
