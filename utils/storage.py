"""
Persistent storage helpers.

Uses st.session_state as the primary store. localStorage writes are fire-and-forget
via hidden HTML/JS snippets (no st_javascript dependency needed).
localStorage reads happen once at startup via query params or are skipped entirely
since .env-based auto-loading makes them unnecessary for connection configs.
"""
import json
from typing import Any, Optional
import streamlit as st
import streamlit.components.v1 as components

from utils.config import STORAGE_PREFIX

# Session-state key prefix for our cached localStorage mirrors
_CACHE_PREFIX = "_ls_cache_"


def _write_to_localstorage(full_key: str, json_value: str):
    """Fire-and-forget write to localStorage via a hidden iframe."""
    escaped = json_value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")
    html = f"""<script>
    try {{ window.parent.localStorage.setItem('{full_key}', '{escaped}'); }}
    catch(e) {{}}
    </script>"""
    components.html(html, height=0, width=0)


def save_to_storage(key: str, value: Any) -> bool:
    """Save a value to session_state and browser localStorage."""
    full_key = key if key.startswith(STORAGE_PREFIX) else f"{STORAGE_PREFIX}{key}"
    cache_key = f"{_CACHE_PREFIX}{full_key}"
    if cache_key in st.session_state and st.session_state[cache_key] == value:
        return True
    st.session_state[cache_key] = value

    try:
        json_value = json.dumps(value)
        _write_to_localstorage(full_key, json_value)
        return True
    except Exception:
        return False


def load_from_storage(key: str, default: Any = None) -> Any:
    """Load a value from session_state cache. Returns default if not cached."""
    full_key = key if key.startswith(STORAGE_PREFIX) else f"{STORAGE_PREFIX}{key}"
    cache_key = f"{_CACHE_PREFIX}{full_key}"

    if cache_key in st.session_state:
        return st.session_state[cache_key]

    return default


def remove_from_storage(key: str) -> bool:
    """Remove a value from session_state and localStorage."""
    full_key = key if key.startswith(STORAGE_PREFIX) else f"{STORAGE_PREFIX}{key}"
    cache_key = f"{_CACHE_PREFIX}{full_key}"
    st.session_state.pop(cache_key, None)

    try:
        html = f"""<script>
        try {{ window.parent.localStorage.removeItem('{full_key}'); }}
        catch(e) {{}}
        </script>"""
        components.html(html, height=0, width=0)
        return True
    except Exception:
        return False


def clear_all_storage() -> bool:
    """Clear all db_migrator_ prefixed keys from localStorage and session_state."""
    keys_to_remove = [k for k in st.session_state if k.startswith(_CACHE_PREFIX)]
    for k in keys_to_remove:
        del st.session_state[k]

    try:
        html = f"""<script>
        try {{
            const keysToRemove = [];
            for (let i = 0; i < window.parent.localStorage.length; i++) {{
                const key = window.parent.localStorage.key(i);
                if (key && key.startsWith('{STORAGE_PREFIX}')) keysToRemove.push(key);
            }}
            keysToRemove.forEach(key => window.parent.localStorage.removeItem(key));
        }} catch(e) {{}}
        </script>"""
        components.html(html, height=0, width=0)
        return True
    except Exception:
        return False


def get_all_storage_keys() -> list:
    """Get all cached storage keys."""
    prefix = f"{_CACHE_PREFIX}{STORAGE_PREFIX}"
    return [k.replace(_CACHE_PREFIX, "") for k in st.session_state if k.startswith(prefix)]


# Convenience functions for common storage operations
def save_connection(connection_type: str, connection_data: dict) -> bool:
    """Save source or target connection details."""
    key = f"{connection_type}_connection"
    safe_data = {k: v for k, v in connection_data.items() if k != "password"}
    return save_to_storage(key, safe_data)


def load_connection(connection_type: str) -> Optional[dict]:
    """Load source or target connection details."""
    key = f"{connection_type}_connection"
    return load_from_storage(key, default=None)


def save_selected_users(user_emails: list) -> bool:
    """Save selected user emails."""
    return save_to_storage("selected_users", user_emails)


def load_selected_users() -> list:
    """Load selected user emails."""
    return load_from_storage("selected_users", default=[])


def save_document_filters(filters: dict) -> bool:
    """Save document filter settings."""
    return save_to_storage("document_filters", filters)


def load_document_filters() -> dict:
    """Load document filter settings."""
    return load_from_storage("document_filters", default={})


def save_mapping_config(config: dict) -> bool:
    """Save column mapping configuration."""
    return save_to_storage("mapping_config", config)


def load_mapping_config() -> Optional[dict]:
    """Load column mapping configuration."""
    return load_from_storage("mapping_config", default=None)
