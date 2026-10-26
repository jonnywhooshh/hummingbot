import logging
from time import perf_counter_ns

from hummingbot.analytics import recorder


class CaptureHandler(logging.Handler):
    def handle(self, record: logging.LogRecord) -> bool:
        # Capture is confined to its activation thread, so it needs no handler lock.
        try:
            if self.filter(record):
                self.emit(record)
                return True
        except Exception as error:
            recorder.hook_error("log", error)
        return False

    def emit(self, record: logging.LogRecord) -> None:
        if not recorder.ACTIVE:
            return
        try:
            if not recorder.on_capture_thread():
                recorder.record("log", {}, t0_ns=0, hook="log")
                return
            t0_ns = perf_counter_ns()
            exception = logging.Formatter().formatException(record.exc_info) if record.exc_info else None
            recorder.record("log", {
                "logger": record.name,
                "level": record.levelname,
                "message": recorder.secrets.scrub(record.getMessage()),
                "exception": recorder.secrets.scrub(exception) if exception else None,
            }, t0_ns=t0_ns, hook="log")
        except Exception as error:
            recorder.hook_error("log", error)
