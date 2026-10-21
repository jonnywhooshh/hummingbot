"""Venue-time windows retain their clock basis within one run directory.

Server-time samples take precedence over estimated offsets in that run.
Only a run without server-time samples uses offset_estimated windows.
"""

from bisect import bisect_left
from dataclasses import dataclass
from typing import Iterable

from hummingbot.analytics.ledger.model import venue_time


@dataclass(frozen=True)
class Window:
    start: int
    end: int
    basis: str = "server_time"

    def __post_init__(self):
        venue_time(self.start)
        venue_time(self.end)
        if self.end < self.start:
            raise ValueError("window ends before it starts")
        if self.basis not in ("server_time", "offset_estimated", "mixed"):
            raise ValueError("unknown window clock basis")

    @property
    def midpoint(self) -> int:
        return (self.start + self.end) // 2


@dataclass(frozen=True)
class ClockSample:
    run_id: str
    local_midpoint: int
    offset_ns: int
    uncertainty_ns: int
    basis: str = "server_time"

    def __post_init__(self):
        for value in (self.local_midpoint, self.offset_ns, self.uncertainty_ns):
            venue_time(value)
        if not self.run_id or self.uncertainty_ns < 0:
            raise ValueError("clock requires a run directory and nonnegative uncertainty")
        if self.basis not in ("server_time", "offset_estimated"):
            raise ValueError("unknown clock basis")


def clock_sample(run_id: str, sent_ns: int, received_ns: int, server_ns: int) -> ClockSample:
    window = Window(sent_ns, received_ns)
    venue_time(server_ns)
    # Round uncertainty outward when the round trip has an odd nanosecond.
    return ClockSample(run_id, window.midpoint, server_ns - window.midpoint,
                       (received_ns - sent_ns + 1) // 2)


class VenueClock:
    def __init__(self, samples: Iterable[ClockSample]):
        grouped = {}
        for sample in samples:
            grouped.setdefault(sample.run_id, []).append(sample)
        selected = {run: [sample for sample in values if sample.basis == "server_time"]
                    or values for run, values in grouped.items()}
        self._samples = {run: sorted(values, key=lambda sample: sample.local_midpoint)
                         for run, values in selected.items()}
        self._times = {run: [sample.local_midpoint for sample in values]
                       for run, values in self._samples.items()}

    def response_window(self, run_id: str, sent_ns: int, received_ns: int) -> Window:
        local = Window(sent_ns, received_ns)
        samples = self._samples.get(run_id)
        if not samples:
            raise ValueError("no clock sample for this run directory")
        index = bisect_left(self._times[run_id], local.midpoint)
        candidates = samples[max(0, index - 1):index + 1]
        sample = min(candidates, key=lambda value: abs(value.local_midpoint - local.midpoint))
        return Window(sent_ns + sample.offset_ns - sample.uncertainty_ns,
                      received_ns + sample.offset_ns + sample.uncertainty_ns, sample.basis)


def estimated_clock(run_id: str, receipts: Iterable[tuple[int, int]]) -> ClockSample:
    """Estimate an offset from (local receipt ns, private fill venue ns)."""
    samples = []
    for local, venue in receipts:
        venue_time(local)
        venue_time(venue)
        samples.append((local - venue, local))
    if not samples:
        raise ValueError("no private fills to estimate the venue clock")
    delay, local = min(samples)
    return ClockSample(run_id, local, -delay, 0, "offset_estimated")
