"""Select exact-decimal reference mids from one pair's offline depth streams.

``observe`` uses nanoseconds for the fill and horizon. The run end is explicit;
absence of a later depth frame means unknown, not that the run has ended.
Bot and sidecar streams must use the same clock basis. Receipt-clock results
remain separate from engine-clock results.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, Optional, Tuple

from hummingbot.analytics.book.stream import BookStream, ClockBasis

BookSource = Literal["bot", "sidecar"]
ReferenceStatus = Literal["valid", "pending", "invalid"]
DEFAULT_HORIZONS_SECONDS = (1, 5, 15, 60, 300)


@dataclass(frozen=True)
class ReferenceObservation:
    fill_time_ns: int
    horizon_ns: int
    status: ReferenceStatus
    reason: Optional[str]
    m0: Optional[Decimal]
    mh: Optional[Decimal]
    m0_source: Optional[BookSource]
    mh_source: Optional[BookSource]
    m0_tie_uncertain: bool
    clock_basis: ClockBasis

    @property
    def source(self) -> Optional[BookSource]:
        if "sidecar" in (self.m0_source, self.mh_source):
            return "sidecar"
        return self.m0_source or self.mh_source


class BookReference:
    def __init__(self, bot: BookStream, *, run_end_ns: int, sidecar: Optional[BookStream] = None) -> None:
        if sidecar is not None and sidecar.clock_basis != bot.clock_basis:
            raise ValueError("Reference streams must use the same clock basis")
        self.bot = bot
        self.sidecar = sidecar
        self.run_end_ns = run_end_ns
        self.clock_basis = bot.clock_basis

    def _mid(
        self, timestamp_ns: int, *, strictly_before: bool = False,
    ) -> Tuple[Optional[Decimal], Optional[BookSource], bool]:
        for source, stream in (("bot", self.bot), ("sidecar", self.sidecar)):
            if stream is None or not stream.known(timestamp_ns):
                continue
            mid = stream.mid_at(timestamp_ns, strictly_before=strictly_before)
            return mid, source, stream.has_diff_at(timestamp_ns)
        return None, None, False

    def known(self, timestamp_ns: int) -> bool:
        return self.mid_at(timestamp_ns) is not None

    def mid_at(self, timestamp_ns: int) -> Optional[Decimal]:
        return self._mid(timestamp_ns)[0]

    def observe(self, fill_time_ns: int, horizon_ns: int) -> ReferenceObservation:
        if not isinstance(fill_time_ns, int) or not isinstance(horizon_ns, int):
            raise TypeError("Fill times and horizons must be integer nanoseconds")
        if horizon_ns < 0:
            raise ValueError("The horizon must be nonnegative")
        m0, m0_source, tie = self._mid(fill_time_ns, strictly_before=True)
        horizon_time_ns = fill_time_ns + horizon_ns
        if horizon_time_ns > self.run_end_ns:
            status, reason = "pending", None
            mh, mh_source = None, None
        else:
            mh, mh_source, _ = self._mid(horizon_time_ns)
            status = "valid" if m0 is not None and mh is not None else "invalid"
            reason = "book_unknown" if status == "invalid" else None
        return ReferenceObservation(
            fill_time_ns, horizon_ns, status, reason, m0, mh,
            m0_source, mh_source, tie, self.clock_basis,
        )
