import tempfile
import unittest
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from engine.ddl_engine import execute_ddl_csv
from engine.dml_engine import execute_insert
from engine.query_engine import execute_select_csv
from storage.csv_backend import CsvBackend
from storage.schema_store import SchemaStore


class QueryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.backend = CsvBackend(self.tmp)
        self.schema = SchemaStore(self.tmp / "schema_store.json")
        execute_ddl_csv(
            "CREATE TABLE students (id INT, name TEXT, score REAL)",
            self.backend,
            self.schema,
        )
        for sid, name, score in [
            (1, "a", 80),
            (2, "b", 70),
            (3, "c", 90),
        ]:
            execute_insert(
                f"INSERT INTO students VALUES ({sid},'{name}',{score})",
                self.backend,
                self.schema,
            )

    def test_where_order_limit(self):
        rows = execute_select_csv(
            "SELECT id, score FROM students WHERE score >= 75 ORDER BY score DESC LIMIT 2",
            self.backend,
            self.schema,
        )
        self.assertEqual([r["id"] for r in rows], [3, 1])

    def test_join(self):
        execute_ddl_csv(
            "CREATE TABLE grades (student_id INT, subject TEXT, points REAL)",
            self.backend,
            self.schema,
        )
        execute_insert(
            "INSERT INTO grades VALUES (1,'math',88)", self.backend, self.schema
        )
        rows = execute_select_csv(
            "SELECT students.id, grades.points FROM students INNER JOIN grades ON students.id=grades.student_id",
            self.backend,
            self.schema,
        )
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(float(rows[0]["grades.points"]), 88.0)

    def test_group_by(self):
        rows = execute_select_csv(
            "SELECT name, COUNT(*), AVG(score) FROM students GROUP BY name",
            self.backend,
            self.schema,
        )
        self.assertEqual(len(rows), 3)


if __name__ == "__main__":
    unittest.main()
