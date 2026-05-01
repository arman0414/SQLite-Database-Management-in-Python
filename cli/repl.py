"""Interactive SQL shell."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Callable, List

from cli.formatter import format_rows


def start_repl(executor: Callable[[str], object]) -> None:
    print("Mini-SQL REPL. End statements with ; or a blank line. Type EXIT to quit.")
    buffer: List[str] = []
    while True:
        try:
            line = input("sql> " if not buffer else "...> ")
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if line.strip().upper() == "EXIT":
            break
        if not line.strip() and buffer:
            stmt = " ".join(buffer).strip()
            buffer.clear()
            if not stmt:
                continue
            try:
                result = executor(stmt)
                if isinstance(result, list):
                    sys.stdout.write(format_rows(result))
                else:
                    print(result)
            except Exception as exc:  # noqa: BLE001
                print("ERROR:", exc)
            continue
        buffer.append(line)
        if ";" in line:
            stmt = " ".join(buffer)
            buffer.clear()
            stmt = stmt.strip().rstrip(";").strip()
            if not stmt:
                continue
            try:
                result = executor(stmt)
                if isinstance(result, list):
                    sys.stdout.write(format_rows(result))
                else:
                    print(result)
            except Exception as exc:  # noqa: BLE001
                print("ERROR:", exc)
