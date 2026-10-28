from typing import AnyStr, Tuple

REDACTED = "<redacted>"
_text: Tuple[str, ...] = ()
_raw: Tuple[bytes, ...] = ()


def add(value: str) -> None:
    """Register a secure config value. Empty and duplicate values have no effect."""
    global _text, _raw
    if not value or value in _text:
        return
    _text = tuple(sorted(_text + (value,), key=len, reverse=True))
    _raw = tuple(secret.encode("utf-8") for secret in _text)


def scrub(data: AnyStr) -> AnyStr:
    """Replace exact secure values, longest first. Unchanged str or bytes retains its identity.

    A memoryview is copied to bytes first, because `in` on a memoryview compares elements, not subsequences.
    """
    if isinstance(data, str):
        values, mark = _text, REDACTED
    else:
        if isinstance(data, memoryview):
            data = data.tobytes()
        values, mark = _raw, REDACTED.encode("utf-8")
    for secret in values:
        if secret in data:
            data = data.replace(secret, mark)
    return data
