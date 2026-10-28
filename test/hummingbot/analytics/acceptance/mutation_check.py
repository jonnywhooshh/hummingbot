"""Run one acceptance mutation without changing product files.

Use `python test/hummingbot/analytics/acceptance/mutation_check.py <mutation>`.
A killed mutation exits zero. A survivor or a test infrastructure error exits one.
Each mutation requires its unmodified test selection to pass in a separate process.
Run this command under mma-slot, just like the acceptance suite.
Hook-specific mutations select their existing hook subtests. M10 runs every hook.
"""

import argparse
import importlib.machinery
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple
from unittest.mock import patch


class Mutation(NamedTuple):
    criterion: str
    changes: tuple[tuple[str, str, str], ...]
    hooks: tuple[str, ...] = ()


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
    "M10_hook_errors_propagate": Mutation("04", (
        (
            "hummingbot/analytics/recorder/__init__.py",
            "        pending = _run.diagnostics if _run is not None else _diagnostics\n"
            "        pending.append((hook, count, error))\n",
            "        pending = _run.diagnostics if _run is not None else _diagnostics\n"
            "        pending.append((hook, count, error))\n    raise error\n",
        ),
        (
            "hummingbot/analytics/hooks/orders.py",
            "        recorder.hook_error(hook, error)\n    except BaseException:\n        pass\n",
            "        recorder.hook_error(hook, error)\n    except BaseException:\n        raise\n",
        ),
    )),
    "M11_clock_failure_clears_samples": Mutation("04", ((
        "hummingbot/analytics/hooks/orders.py",
        '    except BaseException as error:\n        _hook_error("clock", error)\n',
        '    except BaseException as error:\n        synchronizer._time_offset_ms.clear()\n'
        '        _hook_error("clock", error)\n',
    ),), ("clock",)),
    "M12_log_failure_rewrites_record": Mutation("04", ((
        "hummingbot/analytics/hooks/log.py",
        '            }, t0_ns=t0_ns, hook="log")\n        except Exception as error:\n'
        '            recorder.hook_error("log", error)\n',
        '            }, t0_ns=t0_ns, hook="log")\n        except Exception as error:\n'
        '            record.msg, record.args = "analytics capture failed", ()\n'
        '            recorder.hook_error("log", error)\n',
    ),), ("log",)),
    "M13_rest_reads_body_eagerly": Mutation("04", ((
        "hummingbot/core/web_assistant/connections/rest_connection.py",
        "            resp.capture = transport.rest(sent, request, aiohttp_resp, None)\n",
        "            resp.capture = transport.rest(sent, request, aiohttp_resp, None)\n"
        "            await aiohttp_resp.read()\n",
    ),), ("rest",)),
    "M14_rest_adds_250ms_when_capturing": Mutation("04", ((
        "hummingbot/core/web_assistant/connections/rest_connection.py",
        "        sent = transport.rest_sent() if recorder.ACTIVE else None\n",
        "        sent = transport.rest_sent() if recorder.ACTIVE else None\n"
        "        if sent is not None:\n            import asyncio\n            await asyncio.sleep(0.25)\n",
    ),), ("rest",)),
    "M15a_rest_error_path_replaces_exception": Mutation("04", ((
        "hummingbot/core/web_assistant/connections/rest_connection.py",
        "            if sent is not None:\n                transport.rest(sent, request, None, error)\n            raise\n",
        "            if sent is not None:\n                if transport.rest(sent, request, None, error) is None:\n"
        '                    raise RuntimeError("analytics rest hook failed") from error\n            raise\n',
    ),), ("rest",)),
    "M15b_ws_conn_error_event_reraises": Mutation("04", ((
        "hummingbot/analytics/hooks/transport.py",
        '    except Exception as hook_failure:\n        recorder.hook_error("ws_conn", hook_failure)\n',
        '    except Exception as hook_failure:\n        recorder.hook_error("ws_conn", hook_failure)\n'
        "        if error is not None:\n            raise\n",
    ),), ("ws_conn",)),
    "M15c_order_result_failed_order_reraises": Mutation("04", ((
        "hummingbot/analytics/hooks/orders.py",
        "            client_order_id=order_id, exchange_order_id=exchange_order_id)\n"
        '    except BaseException as error:\n        _hook_error("order_result", error)\n',
        "            client_order_id=order_id, exchange_order_id=exchange_order_id)\n"
        '    except BaseException as error:\n        _hook_error("order_result", error)\n'
        "        if exception is not None:\n            raise\n",
    ),), ("order_result",)),
    "M15d_cancel_result_failed_cancel_reraises": Mutation("04", ((
        "hummingbot/analytics/hooks/orders.py",
        '                _cancel.reset(token)\n    except BaseException as error:\n        _hook_error("cancel_result", error)\n',
        '                _cancel.reset(token)\n    except BaseException as error:\n        _hook_error("cancel_result", error)\n'
        "        if exception is not None:\n            raise\n",
    ),), ("cancel_result",)),
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
    "M20_no_x_signature_rule": Mutation("08", ((
        "hummingbot/analytics/redaction.py",
        '    "backpack_perpetual": Rules(headers=("X-API-Key", "X-Signature"), query=(),\n',
        '    "backpack_perpetual": Rules(headers=("X-API-Key",), query=(),\n',
    ),)),
    "M26_run_start_lists_first_only": Mutation("24", (
        (
            "hummingbot/analytics/hooks/run.py",
            '        "connectors": [_connector_body(name, connector) for name, connector in core.connectors.items()],\n',
            '        "connectors": [_connector_body(name, connector) '
            'for name, connector in list(core.connectors.items())[:1]],\n',
        ),
        (
            "hummingbot/analytics/hooks/run.py",
            "for controller_id, controller in controllers.items()],\n",
            "for controller_id, controller in list(controllers.items())[:1]],\n",
        ),
    )),
    "M27_connector_event_twice": Mutation("24", ((
        "hummingbot/analytics/hooks/run.py",
        "        capture = ConnectorEvents(connector, name)\n"
        "        run.connectors[name] = capture\n        capture.attach()\n",
        "        capture = ConnectorEvents(connector, name)\n"
        "        run.connectors[name] = capture\n        capture.attach()\n"
        "        duplicate = ConnectorEvents(connector, name)\n"
        "        run.connectors[name + '#duplicate'] = duplicate\n        duplicate.attach()\n",
    ),)),
    "M30_tracker_order_result_changes": Mutation("04", ((
        "hummingbot/connector/client_order_tracker.py",
        "        return safe_ensure_future(self._process_order_update(order_update))\n",
        "        task = safe_ensure_future(self._process_order_update(order_update))\n"
        "        return False if recorder.ACTIVE else task\n",
    ),), ("tracker_update",)),
    "M31_tracker_trade_result_changes": Mutation("04", ((
        "hummingbot/connector/client_order_tracker.py",
        "                    exchange_order_id=trade_update.exchange_order_id,\n                )\n",
        "                    exchange_order_id=trade_update.exchange_order_id,\n                )\n"
        "        if recorder.ACTIVE:\n            return False\n",
    ),), ("tracker_update",)),
    "M32_controller_stops_after_first_order": Mutation("07", ((
        "hummingbot/connector/test_support/fake_backpack/harness.py",
        "                async def counted(controller):\n",
        "                async def counted(controller):\n"
        "                    if ctx.decision_by_order:\n                        return\n",
    ),)),
    "M33_ws_out_replaces_send_error": Mutation("04", (
        (
            "hummingbot/analytics/hooks/transport.py",
            '    except Exception as error:\n        recorder.hook_error("ws_out", error)\n',
            '    except Exception as error:\n        recorder.hook_error("ws_out", error)\n        return error\n',
        ),
        (
            "hummingbot/core/web_assistant/connections/ws_connection.py",
            "        if recorder.ACTIVE:\n            transport.ws_out(self._capture, request)\n"
            "        await request.send_with_connection(connection=self)\n",
            "        capture_error = None\n        if recorder.ACTIVE:\n"
            "            capture_error = transport.ws_out(self._capture, request)\n"
            "        try:\n            await request.send_with_connection(connection=self)\n"
            "        except Exception:\n            if capture_error is not None:\n"
            "                raise capture_error\n            raise\n",
        ),
    ), ("ws_out",)),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mutation", choices=MUTATIONS)
    parser.add_argument("--baseline", action="store_true", help="Run the unmodified control only.")
    args = parser.parse_args()
    mutation = MUTATIONS[args.mutation]
    root = Path(__file__).resolve().parents[4]
    sys.path.insert(0, str(root))
    sys.dont_write_bytecode = True
    if not args.baseline:
        baseline = subprocess.run([sys.executable, str(Path(__file__).resolve()), args.mutation, "--baseline"])
        if baseline.returncode != 0:
            print(f"MUTATION_INVALID_BASELINE {args.mutation}", flush=True)
            return 1
    changes = {}
    if not args.baseline:
        for path, before, after in mutation.changes:
            changes.setdefault(str(root / path), []).append((before, after))
    applied = set()
    get_code = importlib.machinery.SourceFileLoader.get_code

    def mutated_code(loader, fullname):
        change = changes.get(loader.path)
        if change is None:
            return get_code(loader, fullname)
        source = Path(loader.path).read_text()
        for before, after in change:
            if source.count(before) != 1:
                raise RuntimeError(f"{args.mutation} anchor must occur once in {loader.path}")
            source = source.replace(before, after)
        applied.add(loader.path)
        print(f"MUTATION_APPLIED {args.mutation} {loader.path}", flush=True)
        return compile(source, loader.path, "exec")

    class FocusHooks:
        def pytest_collection_modifyitems(self, items):
            if mutation.hooks:
                for item in items:
                    item.module.PHASE1_HOOKS = mutation.hooks
                print(f"MUTATION_HOOKS {args.mutation} {mutation.hooks}", flush=True)

    with patch.object(importlib.machinery.SourceFileLoader, "get_code", mutated_code):
        import pytest
        result = pytest.main([
            str(Path(__file__).with_name("test_phase1.py")), "-v", "-rA", "--tb=short",
            "-p", "no:cacheprovider", "-k", f"test_criterion_{mutation.criterion}",
        ], plugins=[FocusHooks()])
    if args.baseline:
        print(f"BASELINE_{'PASSED' if result == pytest.ExitCode.OK else 'FAILED'} {args.mutation}", flush=True)
        return 0 if result == pytest.ExitCode.OK else 1
    if result == pytest.ExitCode.TESTS_FAILED and applied == set(changes):
        print(f"MUTATION_KILLED {args.mutation}", flush=True)
        return 0
    print(f"MUTATION_NOT_KILLED {args.mutation} pytest_exit={result} applied={sorted(applied)}", flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
