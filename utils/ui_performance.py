"""Small, framework-agnostic helpers for rerun-safe UI behavior."""
from __future__ import annotations

from typing import Any, Callable, MutableMapping, Optional


def resolve_lazy_value(
    cache: MutableMapping[str, Any],
    *,
    value_key: str,
    fingerprint_key: str,
    fingerprint: Any,
    requested: bool,
    builder: Callable[[], Any],
) -> Optional[Any]:
    """Return a matching cached value, building only after explicit request."""
    if cache.get(fingerprint_key) == fingerprint and value_key in cache:
        return cache[value_key]
    if not requested:
        return None
    value = builder()
    cache[value_key] = value
    cache[fingerprint_key] = fingerprint
    return value
