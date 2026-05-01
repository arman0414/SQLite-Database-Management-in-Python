import unittest
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from storage.sqlite_backend import SqliteBackend


class SqliteModeTests(unittest.TestCase):
    def test_fixture_join_aggregate(self):
        fx = Path(__file__).parent / "fixtures" / "init.sql"
        be = SqliteBackend(Path(":memory:"))
        be.executescript(fx.read_text(encoding="utf-8"))
        rows = be.fetchall(
            """
            SELECT students.name, AVG(grades.score) AS avg_score
            FROM students
            INNER JOIN grades ON students.id = grades.student_id
            GROUP BY students.id
            ORDER BY avg_score DESC
            LIMIT 3
            """
        )
        self.assertGreaterEqual(len(rows), 1)
        be.close()


if __name__ == "__main__":
    unittest.main()
