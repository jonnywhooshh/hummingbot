"""Outbound redaction tables and the host table that attributes transport records to a connector.

A URL host names its connector because a WSConnection or RESTConnection does not know one. A connector with a table
entry has its credential names removed. Every other connector, and every unregistered host, gets no outbound headers,
query strings, bodies, or WS payloads at all (spec 5.5).
"""
from functools import lru_cache
from typing import Any, Dict, List, Mapping, NamedTuple, Optional, Sequence, Tuple, Union
from urllib.parse import unquote_plus, urlsplit

from hummingbot.analytics.recorder import secrets

REDACTED = secrets.REDACTED
SECURE_VALUE = "secure_config_value"
OUTBOUND_OMITTED = "outbound_omitted"

JsonPath = Tuple[Union[str, int], ...]


class Rules(NamedTuple):
    headers: Tuple[str, ...]
    query: Tuple[str, ...]
    ws_paths: Tuple[JsonPath, ...]
    rest_paths: Tuple[JsonPath, ...]


# Backpack signs with headers on REST and with elements 0 and 1 of the `signature` array on WS subscribe.
# X-Timestamp and X-Window stay because they explain request timing.
TABLE: Dict[str, Rules] = {
    "backpack_perpetual": Rules(headers=("X-API-Key", "X-Signature"), query=(),
                                ws_paths=(("signature", 0), ("signature", 1)), rest_paths=()),
}

# The loopback entry is the fake venue's host (hummingbot.connector.test_support.fake_backpack). Spot Backpack shares
# the production hosts, so its records carry the perpetual connector name until a host can tell them apart.
HOSTS: Dict[str, str] = {
    "api.backpack.exchange": "backpack_perpetual",
    "ws.backpack.exchange": "backpack_perpetual",
    "127.0.0.1": "backpack_perpetual",
}


def has_table(connector: str) -> bool:
    return connector in TABLE


def rules_for(connector: Optional[str]) -> Optional[Rules]:
    return TABLE.get(connector) if connector is not None else None


@lru_cache(maxsize=256)
def connector_of(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    return HOSTS.get(urlsplit(url).hostname or "")


@lru_cache(maxsize=64)
def _folded(names: Tuple[str, ...]) -> Dict[str, str]:
    return {name.casefold(): name for name in names}


@lru_cache(maxsize=512)
def clean_url(connector: Optional[str], url: str) -> Tuple[str, Tuple[str, ...]]:
    """Return url without credentials in its authority, fragment, or (when the connector has no entry) query string.

    With an entry, query values whose names are in the table become "<redacted>" and the names come back as well.
    """
    parts = urlsplit(url)
    rules = rules_for(connector)
    base = f"{parts.scheme}://" if parts.scheme else ""
    base += f"{parts.netloc.rpartition('@')[2]}{parts.path}"
    if rules is None or not parts.query:
        return base, ()
    folded = _folded(rules.query)
    removed: List[str] = []
    pieces = []
    for piece in parts.query.split("&"):
        key = piece.partition("=")[0]
        name = folded.get(unquote_plus(key).casefold())
        if name is None:
            pieces.append(piece)
        else:
            pieces.append(f"{key}={REDACTED}")
            removed.append(name)
    return f"{base}?{'&'.join(pieces)}", tuple(removed)


def redact_items(items: Optional[Mapping[Any, Any]],
                 names: Tuple[str, ...]) -> Tuple[Optional[Dict[Any, Any]], List[str]]:
    """Copy a header or parameter mapping to a plain dict with the named values replaced, matching names case-blind."""
    if items is None:
        return None, []
    folded = _folded(names)
    result = dict(items)
    removed: List[str] = []
    for key in result:
        name = folded.get(str(key).casefold())
        if name is not None:
            result[key] = REDACTED
            removed.append(name)
    return result, removed


def redact_paths(data: Any, paths: Sequence[JsonPath]) -> Tuple[Any, List[str]]:
    """Replace the value at each JSON path, copying only the containers on the way. Missing paths are skipped."""
    removed: List[str] = []
    for path in paths:
        updated = _replace(data, path)
        if updated is not data:
            data = updated
            removed.append(_path_name(path))
    return data, removed


def _replace(node: Any, path: JsonPath) -> Any:
    if not isinstance(node, (Mapping, list, tuple)):
        return node
    key, rest = path[0], path[1:]
    try:
        child = node[key]
    except (KeyError, IndexError, TypeError):
        return node
    replacement = _replace(child, rest) if rest else REDACTED
    if replacement is child:
        return node
    copy: Any = dict(node) if isinstance(node, Mapping) else list(node)
    copy[key] = replacement
    return copy


def _path_name(path: JsonPath) -> str:
    return "".join(f"[{key}]" if isinstance(key, int) else f".{key}" for key in path).lstrip(".")


def scrub_tree(node: Any) -> Tuple[Any, bool]:
    """Pass every string in a dict/list tree through secrets.scrub. The flag says whether any value changed."""
    if isinstance(node, str):
        scrubbed = secrets.scrub(node)
        return scrubbed, scrubbed is not node
    if isinstance(node, Mapping):
        changed = False
        result = {}
        for key, value in node.items():
            result[key], hit = scrub_tree(value)
            changed = changed or hit
        return (result, True) if changed else (node, False)
    if isinstance(node, (list, tuple)):
        changed = False
        items = []
        for value in node:
            scrubbed, hit = scrub_tree(value)
            items.append(scrubbed)
            changed = changed or hit
        return (items, True) if changed else (node, False)
    return node, False
