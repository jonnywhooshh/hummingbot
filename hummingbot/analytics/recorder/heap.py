import ctypes
from typing import Any, Optional

M_TRIM_THRESHOLD = -1
M_TOP_PAD = -2
TRIM_THRESHOLD_BYTES = 16 << 20
TOP_PAD_BYTES = 4 << 20


def keep_freed_memory(libc: Optional[Any] = None) -> bool:
    """Stop glibc handing freed heap back to the kernel on every hook.

    Each record allocates a few buffers of the payload's size and the writer frees them in batches. With the default
    128 KiB trim threshold the heap shrinks and regrows continuously, and the page faults land in record() p99.
    Returns False when the allocator has no mallopt or refuses, which only costs the tail latency.
    """
    try:
        libc = libc or ctypes.CDLL(None)
        return all(libc.mallopt(option, value) == 1
                   for option, value in ((M_TRIM_THRESHOLD, TRIM_THRESHOLD_BYTES), (M_TOP_PAD, TOP_PAD_BYTES)))
    except (OSError, AttributeError):
        return False
