"""
SQL Guard — validates that user-submitted SQL is read-only.

Internal engine queries (e.g. ``SET default_collation``, ``LOAD excel``)
bypass this guard entirely.  Only strings that arrive from MCP tool
callers are checked.
"""

from __future__ import annotations

import re
from typing import Tuple

# ── Allowed statement prefixes ──────────────────────────────────
_ALLOWED_PREFIXES = ("select", "with", "from", "summarize")

# ── Forbidden keywords (word-boundary matched) ─────────────────
_DANGEROUS_RE = re.compile(
    r"\b("
    r"INSERT|UPDATE|DELETE|MERGE|REPLACE"
    r"|CREATE|ALTER|DROP|TRUNCATE"
    r"|EXEC|EXECUTE"
    r"|GRANT|REVOKE"
    r"|PRAGMA|INSTALL|LOAD"
    r"|ATTACH|DETACH"
    r"|COPY|EXPORT"
    r")\b",
    re.IGNORECASE,
)


def validate(sql: str) -> Tuple[bool, str]:
    """Return ``(True, "")`` if the SQL is safe, else ``(False, reason)``."""
    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        return False, "Empty query."

    # 1. Must start with an allowed keyword
    lower = stripped.lower()
    if not any(lower.startswith(p) for p in _ALLOWED_PREFIXES):
        return False, "Only SELECT / WITH / FROM / SUMMARIZE queries are allowed."

    # 2. No forbidden keywords
    match = _DANGEROUS_RE.search(stripped)
    if match:
        return False, f"Destructive or unsafe keyword '{match.group()}' is not allowed."

    # 3. No unquoted semicolons (multi-statement)
    if _has_unquoted_semicolon(stripped):
        return False, "Multiple statements are not allowed."

    return True, ""


def _has_unquoted_semicolon(sql: str) -> bool:
    """Detect semicolons outside of single- and double-quoted strings."""
    in_single = False
    in_double = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'" and not in_double:
            # Handle escaped quotes ('')
            if in_single and i + 1 < len(sql) and sql[i + 1] == "'":
                i += 2
                continue
            in_single = not in_single
        elif ch == '"' and not in_single:
            if in_double and i + 1 < len(sql) and sql[i + 1] == '"':
                i += 2
                continue
            in_double = not in_double
        elif ch == ";" and not in_single and not in_double:
            return True
        i += 1
    return False
