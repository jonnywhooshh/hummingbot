"""Select exact-decimal reference mids from one pair's offline depth streams.

``observe`` uses nanoseconds for the fill and horizon on the depth stream's
clock. The run end is explicit. A later continuous depth frame must confirm
silence. Only venue-clock streams can share a sidecar reference. Receipt-clock
results use the bot's own clock and remain separate from venue-clock results.
Phase 3 consumes two invalid reasons. ``book_unknown`` means that the stream
cannot prove the required pre-fill or horizon book, including a fill at the
first covering diff. ``mid_undefined`` means that the required book is known
but a side is empty or the best bid exceeds the best ask. A known bot book
with an undefined mid never falls back to the sidecar. An unknown fill is
invalid even when its horizon is pending.
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
        if sidecar is not None and bot.clock_basis == "receipt":
            raise ValueError("Sidecar references require a shared venue clock")
        self.bot = bot
        self.sidecar = sidecar
        self.run_end_ns = run_end_ns
        self.clock_basis = bot.clock_basis

    def _mid(
        self, timestamp_ns: int, *, strictly_before: bool = False,
    ) -> Tuple[Optional[Decimal], Optional[BookSource], bool]:
        tie = any(stream is not None and stream.has_diff_at(timestamp_ns) for stream in (self.bot, self.sidecar))
        for source, stream in (("bot", self.bot), ("sidecar", self.sidecar)):
            if stream is None or not stream.known(timestamp_ns, strictly_before=strictly_before):
                continue
            mid = stream.mid_at(timestamp_ns, strictly_before=strictly_before)
            return mid, source, tie
        return None, None, tie

    def known(self, timestamp_ns: int) -> bool:
        return self.bot.known(timestamp_ns) or (self.sidecar is not None and self.sidecar.known(timestamp_ns))

    def mid_at(self, timestamp_ns: int) -> Optional[Decimal]:
        return self._mid(timestamp_ns)[0]

    def observe(self, fill_time_ns: int, horizon_ns: int) -> ReferenceObservation:
        if not isinstance(fill_time_ns, int) or not isinstance(horizon_ns, int):
            raise TypeError("Fill times and horizons must be integer nanoseconds")
        if horizon_ns < 0:
            raise ValueError("The horizon must be nonnegative")
        m0, m0_source, tie = self._mid(fill_time_ns, strictly_before=True)
        horizon_time_ns = fill_time_ns + horizon_ns
        pending = horizon_time_ns > self.run_end_ns
        mh, mh_source = (None, None) if pending else self._mid(horizon_time_ns)[:2]
        if m0_source is None or (not pending and mh_source is None):
            status, reason = "invalid", "book_unknown"
        elif m0 is None or (not pending and mh is None):
            status, reason = "invalid", "mid_undefined"
        elif pending:
            status, reason = "pending", None
        else:
            status, reason = "valid", None
        return ReferenceObservation(
            fill_time_ns, horizon_ns, status, reason, m0, mh,
            m0_source, mh_source, tie, self.clock_basis,
        )
