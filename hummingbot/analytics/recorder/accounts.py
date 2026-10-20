import hashlib
from typing import Dict, Optional

_ids: Dict[str, str] = {}


def set(connector: str, api_key: str) -> str:
    """Register and return <connector>:<first 16 hex digits of SHA-256 of the API key>."""
    account_id = f"{connector}:{hashlib.sha256(api_key.encode('utf-8')).hexdigest()[:16]}"
    _ids[connector] = account_id
    return account_id


def get(connector: str) -> Optional[str]:
    return _ids.get(connector)
