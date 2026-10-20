import math
from typing import Dict, List, Optional, Sequence

BUCKETS = 22
_hooks: Dict[str, List[int]] = {}


def observe(hook: str, ns: int) -> None:
    """Count a complete hook duration in log2 microsecond buckets, including an overflow bucket."""
    row = _hooks.get(hook)
    if row is None:
        row = _hooks[hook] = [0] * (BUCKETS + 2)
    row[0] += 1
    row[1] += ns
    us = -(-ns // 1000)
    index = (us - 1).bit_length() if us > 1 else 0
    row[2 + min(index, BUCKETS - 1)] += 1


def drain() -> Dict[str, Dict[str, object]]:
    """Return {hook: {count, total_ns, buckets}} since the previous drain and reset the counts."""
    global _hooks
    hooks, _hooks = _hooks, {}
    return {hook: {"count": row[0], "total_ns": row[1], "buckets": row[2:]} for hook, row in hooks.items()}


def bucket_edge_us(index: int) -> float:
    return math.inf if index >= BUCKETS - 1 else float(2 ** index)


def p99_edge_us(buckets: Sequence[int]) -> Optional[float]:
    total = sum(buckets)
    if total == 0:
        return None
    rank = (99 * total + 99) // 100
    seen = 0
    for index, count in enumerate(buckets):
        seen += count
        if seen >= rank:
            return bucket_edge_us(index)
    return math.inf
