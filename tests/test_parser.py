import unittest

from engine.parser import split_statements, strip_comments


class ParserTests(unittest.TestCase):
    def test_split_respects_quotes_and_semicolons(self):
        sql = """
        SELECT * FROM t WHERE name = 'a;b';
        INSERT INTO t VALUES (1);
        """
        parts = split_statements(sql)
        self.assertEqual(len(parts), 2)
        self.assertIn("name = 'a;b'", parts[0])

    def test_strip_comments(self):
        s = "SELECT 1 -- comment\nFROM dual"
        self.assertNotIn("--", strip_comments(s))


if __name__ == "__main__":
    unittest.main()
