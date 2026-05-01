"""ASCII table rendering for query results."""
from __future__ import annotations

from typing import Any, Dict, List, Sequence


def format_rows(rows: Sequence[Dict[str, Any]]) -> str:
    if not rows:
        return "(0 rows)\n"
    cols = list(rows[0].keys())
    str_rows: List[List[str]] = []
    for r in rows:
        str_rows.append([_fmt(r.get(c)) for c in cols])
    widths = [max(len(c), *(len(sr[i]) for sr in str_rows)) for i, c in enumerate(cols)]

    def line(cells: Sequence[str]) -> str:
        return "| " + " | ".join(c.ljust(widths[i]) for i, c in enumerate(cells)) + " |"

    sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    out_lines = [sep, line(cols), sep]
    for sr in str_rows:
        out_lines.append(line(sr))
    out_lines.append(sep)
    out_lines.append(f"({len(rows)} rows)")
    return "\n".join(out_lines) + "\n"


def _fmt(v: Any) -> str:
    if v is None:
        return "NULL"
    return str(v)
