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

# ── Forbidden statement starts (outside quotes/comments) ─────────────────
_UNSAFE_STATEMENT_RE = re.compile(
    r"(^|[;(])\s*(?P<kw>"
    r"INSERT|UPDATE|DELETE|MERGE"
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
    stripped = _strip_sql_comments(sql).strip().rstrip(";").strip()
    if not stripped:
        return False, "Empty query."

    # 1. Must start with an allowed keyword
    lower = stripped.lower()
    if not any(lower.startswith(p) for p in _ALLOWED_PREFIXES):
        return False, "Only SELECT / WITH / FROM / SUMMARIZE queries are allowed."

    # 2. No unquoted semicolons (multi-statement)
    if _has_unquoted_semicolon(stripped):
        return False, "Multiple statements are not allowed."

    # 3. No forbidden statement keywords outside quoted literals/identifiers
    unsafe_kw = _find_unsafe_statement_keyword(stripped)
    if unsafe_kw:
        return False, f"Destructive or unsafe keyword '{unsafe_kw}' is not allowed."

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


def _find_unsafe_statement_keyword(sql: str) -> str:
    masked = _mask_sql_literals_and_identifiers(sql)
    match = _UNSAFE_STATEMENT_RE.search(masked)
    if not match:
        return ""
    return str(match.group("kw") or "")


def _mask_sql_literals_and_identifiers(sql: str) -> str:
    """Mask quoted text with spaces so keyword scans ignore literals/identifiers."""
    out: list[str] = []
    in_single = False
    in_double = False
    i = 0

    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if in_single:
            # Handle escaped single quote ('')
            if ch == "'" and nxt == "'":
                out.append(" ")
                out.append(" ")
                i += 2
                continue
            if ch == "'":
                in_single = False
            out.append(" ")
            i += 1
            continue

        if in_double:
            # Handle escaped double quote ("")
            if ch == '"' and nxt == '"':
                out.append(" ")
                out.append(" ")
                i += 2
                continue
            if ch == '"':
                in_double = False
            out.append(" ")
            i += 1
            continue

        if ch == "'":
            in_single = True
            out.append(" ")
            i += 1
            continue

        if ch == '"':
            in_double = True
            out.append(" ")
            i += 1
            continue

        out.append(ch)
        i += 1

    return "".join(out)


def _strip_sql_comments(sql: str) -> str:
    """Remove line/block comments while preserving quoted string content."""
    out: list[str] = []
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    i = 0

    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                out.append(ch)
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue

        if not in_single and not in_double:
            if ch == "-" and nxt == "-":
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                in_block_comment = True
                i += 2
                continue

        if ch == "'" and not in_double:
            # Handle escaped single quote ('')
            if in_single and nxt == "'":
                out.append(ch)
                out.append(nxt)
                i += 2
                continue
            in_single = not in_single
            out.append(ch)
            i += 1
            continue

        if ch == '"' and not in_single:
            # Handle escaped double quote ("")
            if in_double and nxt == '"':
                out.append(ch)
                out.append(nxt)
                i += 2
                continue
            in_double = not in_double
            out.append(ch)
            i += 1
            continue

        out.append(ch)
        i += 1

    return "".join(out)

