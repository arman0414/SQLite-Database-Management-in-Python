"""Thin sqlite3 wrapper used in sqlite CLI mode."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable, List, Sequence, Tuple


class SqliteBackend:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row

    def executescript(self, sql: str) -> None:
        self.conn.executescript(sql)
        self.conn.commit()

    def execute(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur

    def fetchall(self, sql: str, params: Sequence[Any] = ()) -> List[sqlite3.Row]:
        cur = self.conn.execute(sql, params)
        return cur.fetchall()

    def close(self) -> None:
        self.conn.close()
