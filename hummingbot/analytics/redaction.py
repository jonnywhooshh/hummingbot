"""Outbound credential policy for web_assistant transport records.

Host attribution cannot distinguish Backpack spot from perpetual accounts because they share endpoints.
The production hosts and the fake venue's IPv4 loopback host use backpack_perpetual.
Unknown hosts retain only outbound method, URL path, and timing. Inbound payloads stay verbatim.
"""
from functools import lru_cache
from typing import Any, Dict, List, Mapping, NamedTuple, Optional, Sequence, Tuple, Union
from urllib.parse import quote_plus, unquote_plus, urlsplit

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


TABLE: Dict[str, Rules] = {
    "backpack_perpetual": Rules(headers=("X-API-Key", "X-Signature"), query=(),
                                ws_paths=(("signature", 0), ("signature", 1)), rest_paths=()),
}

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


def clean_url(connector: Optional[str], url: str) -> Tuple[str, Tuple[str, ...]]:
    """Remove URL credentials and scrub decoded query values before URL encoding can hide a secure value."""
    parts = urlsplit(url)
    rules = rules_for(connector)
    if rules is None:
        return parts.path or "/", ()
    removed: List[str] = []
    if "@" in parts.netloc:
        removed.append("url.userinfo")
    if parts.fragment:
        removed.append("url.fragment")
    base = f"{parts.scheme}://" if parts.scheme else ""
    base += f"{parts.netloc.rpartition('@')[2]}{parts.path}"
    if not parts.query:
        return base, tuple(removed)
    folded = _folded(rules.query)
    pieces = []
    for piece in parts.query.split("&"):
        key, _, value = piece.partition("=")
        name = folded.get(unquote_plus(key).casefold())
        if name is not None:
            pieces.append(f"{key}={quote_plus(REDACTED)}")
            removed.append(name)
        else:
            decoded = unquote_plus(value)
            scrubbed = secrets.scrub(decoded)
            clean_key = secrets.scrub(unquote_plus(key))
            if scrubbed is not decoded or clean_key != unquote_plus(key):
                pieces.append(f"{quote_plus(clean_key)}={quote_plus(scrubbed)}")
                removed.append(SECURE_VALUE)
            else:
                pieces.append(piece)
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
    if isinstance(node, (str, bytes, bytearray, memoryview)):
        scrubbed = secrets.scrub(node)
        return scrubbed, scrubbed != node
    if isinstance(node, Mapping):
        changed = False
        result = {}
        for key, value in node.items():
            clean_key, key_hit = scrub_tree(key)
            result[clean_key], hit = scrub_tree(value)
            changed = changed or hit or key_hit
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
