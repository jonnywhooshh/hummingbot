from enum import Enum
from typing import Dict, NamedTuple, Optional


class Evidence(Enum):
    VENUE = "venue"
    LOCAL = "local"


class Kind(NamedTuple):
    name: str
    evidence: Evidence
    limit_us: Optional[int]
    per_kib_us: int = 0

    @property
    def priority(self) -> bool:
        # Spec 5.1: every local-evidence kind is priority; venue evidence is priority only when the hook says so.
        return self.evidence is Evidence.LOCAL


VENUE = Evidence.VENUE
LOCAL = Evidence.LOCAL

# limit_us is the section 5.1 p99 limit. None marks records written outside the trading path.
KINDS: Dict[str, Kind] = {kind.name: kind for kind in (
    Kind("run_start", LOCAL, None),
    Kind("run_end", LOCAL, None),
    Kind("ws_in", VENUE, 64, 8),
    Kind("ws_out", VENUE, 64, 8),
    Kind("ws_conn", LOCAL, 64),
    Kind("rest", VENUE, 64, 8),
    Kind("rest_body", VENUE, 64, 8),
    Kind("order_request", LOCAL, 64),
    Kind("order_dispatch", LOCAL, 64),
    Kind("order_result", LOCAL, 64),
    Kind("cancel_request", LOCAL, 64),
    Kind("cancel_result", LOCAL, 64),
    Kind("tracker_update", LOCAL, 64),
    Kind("connector_event", LOCAL, 64),
    Kind("tick", LOCAL, 2048),
    Kind("budget_check", LOCAL, 64),
    Kind("executor_state", LOCAL, 512),
    Kind("executor_order", LOCAL, 64),
    Kind("barrier", LOCAL, 64),
    Kind("config_change", LOCAL, 64),
    Kind("controller_state", LOCAL, 64),
    Kind("log", LOCAL, 64, 8),
    Kind("clock", LOCAL, 64),
    Kind("host", LOCAL, 1024),
    Kind("capture_gap", LOCAL, None),
)}
