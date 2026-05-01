"""Multi-line SQL splitting, comment stripping, light token helpers."""
from __future__ import annotations

import re
from typing import List


def strip_comments(sql: str) -> str:
    # remove line comments
    lines = []
    for line in sql.splitlines():
        stripped = re.sub(r"--.*$", "", line)
        lines.append(stripped)
    return "\n".join(lines)


def split_statements(sql: str) -> List[str]:
    sql = strip_comments(sql)
    parts: List[str] = []
    buf: List[str] = []
    in_single = False
    in_double = False
    for ch in sql:
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == ";" and not in_single and not in_double:
            stmt = "".join(buf).strip()
            if stmt:
                parts.append(stmt)
            buf = []
            continue
        buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def tokens(sql: str) -> List[str]:
    """Very small tokenizer: splits on whitespace but keeps quoted strings as single tokens."""
    sql = normalize_ws(sql)
    out: List[str] = []
    i = 0
    n = len(sql)
    buf: List[str] = []
    quote = ""

    def flush() -> None:
        nonlocal buf
        if buf:
            out.append("".join(buf))
            buf = []

    while i < n:
        ch = sql[i]
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = ""
            i += 1
            continue
        if ch in ("'", '"'):
            flush()
            quote = ch
            buf.append(ch)
            i += 1
            continue
        if ch.isspace():
            flush()
            i += 1
            continue
        if ch in "(),*<>=!":
            flush()
            if ch == "!" and i + 1 < n and sql[i + 1] == "=":
                out.append("!=")
                i += 2
                continue
            out.append(ch)
            i += 1
            continue
        buf.append(ch)
        i += 1
    flush()
    return out
