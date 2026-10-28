"""Run one acceptance mutation without changing product files.

Use `python test/hummingbot/analytics/acceptance/mutation_check.py <mutation>`.
A killed mutation exits zero. A survivor or a test infrastructure error exits one.
Run this command under mma-slot, just like the acceptance suite.
"""

import argparse
import importlib.machinery
import sys
from pathlib import Path
from typing import NamedTuple
from unittest.mock import patch


class Mutation(NamedTuple):
    criterion: str
    changes: tuple[tuple[str, str, str], ...]


MUTATIONS = {
    "M05_drop_gap_records": Mutation("03", ((
        "hummingbot/analytics/recorder/writer.py",
        "                self._append(seq, line, rotate=self.state is not DiskState.FULL, covers=unreported[0])\n",
        "                pass\n",
    ),)),
    "M06_gap_only_at_end_run": Mutation("03", ((
        "hummingbot/analytics/recorder/writer.py",
        "            unreported = self._unreported\n            unreported.extend(self.queue.take_drops())\n",
        "            unreported = self._unreported\n            if self._drain_requested:\n"
        "                unreported.extend(self.queue.take_drops())\n",
    ),)),
    "M16_low_drops_private_ws_in": Mutation("07", ((
        "hummingbot/analytics/recorder/writer.py",
        "                if self._admit(seq, priority):\n",
        "                if self._admit(seq, priority and b'\"kind\":\"ws_in\"' not in line):\n",
    ),)),
    "M17_low_drops_only_first_bulk": Mutation("07", ((
        "hummingbot/analytics/recorder/writer.py",
        "        if state is DiskState.OK or (state is DiskState.LOW and priority):\n",
        "        if state is DiskState.OK or (state is DiskState.LOW and "
        "(priority or DiskState.LOW in self._gaps_written)):\n",
    ),)),
    "M29_skip_disk_low": Mutation("07", ((
        "hummingbot/analytics/recorder/writer.py",
        "            elif self.free_bytes < self.min_free_bytes:\n",
        "            elif False:\n",
    ),)),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mutation", choices=MUTATIONS)
    args = parser.parse_args()
    mutation = MUTATIONS[args.mutation]
    root = Path(__file__).resolve().parents[4]
    sys.path.insert(0, str(root))
    sys.dont_write_bytecode = True
    changes = {str(root / path): (before, after) for path, before, after in mutation.changes}
    applied = set()
    get_code = importlib.machinery.SourceFileLoader.get_code

    def mutated_code(loader, fullname):
        change = changes.get(loader.path)
        if change is None:
            return get_code(loader, fullname)
        source = Path(loader.path).read_text()
        before, after = change
        if source.count(before) != 1:
            raise RuntimeError(f"{args.mutation} anchor must occur once in {loader.path}")
        applied.add(loader.path)
        print(f"MUTATION_APPLIED {args.mutation} {loader.path}", flush=True)
        return compile(source.replace(before, after), loader.path, "exec")

    with patch.object(importlib.machinery.SourceFileLoader, "get_code", mutated_code):
        import pytest
        result = pytest.main([
            str(Path(__file__).with_name("test_phase1.py")), "-v", "-rA", "--tb=short",
            "-p", "no:cacheprovider", "-k", f"test_criterion_{mutation.criterion}",
        ])
    if result == pytest.ExitCode.TESTS_FAILED and applied == set(changes):
        print(f"MUTATION_KILLED {args.mutation}", flush=True)
        return 0
    print(f"MUTATION_NOT_KILLED {args.mutation} pytest_exit={result} applied={sorted(applied)}", flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
