import json
import unittest
from unittest.mock import patch

import mcp_server


class _FakeDuckResult:
    def __init__(self, value):
        self._value = value

    def fetchone(self):
        return [self._value]


class _FakeDuck:
    def __init__(self, value):
        self._value = value
        self.executed_sql = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql):
        self.executed_sql.append(sql)
        return _FakeDuckResult(self._value)


class _FakeConn:
    def __init__(self):
        self.executed = []
        self.committed = False
        self.closed = False

    def execute(self, sql, params):
        self.executed.append((sql, params))
        return None

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


class TestMcpRowCountSummary(unittest.TestCase):
    @patch("mcp_server.db.get_connection")
    @patch("mcp_server.connect")
    @patch("mcp_server._tool_get_visible_dataset")
    def test_uses_cached_row_count_when_available(self, mock_get_visible_ds, mock_connect, mock_get_connection):
        mock_get_visible_ds.return_value = (
            {"id": "source_customers", "side": "source", "row_count": 123},
            None,
        )

        parsed = json.loads(mcp_server.row_count_summary("source_customers"))

        self.assertEqual(parsed["dataset"], "source_customers")
        self.assertEqual(parsed["row_count"], 123)
        mock_connect.assert_not_called()
        mock_get_connection.assert_not_called()

    @patch("mcp_server.db.utcnow", return_value="2026-03-14T00:00:00+00:00")
    @patch("mcp_server.db.get_connection")
    @patch("mcp_server.connect")
    @patch("mcp_server._tool_get_visible_dataset")
    def test_computes_and_persists_row_count_when_missing(
        self,
        mock_get_visible_ds,
        mock_connect,
        mock_get_connection,
        _mock_utcnow,
    ):
        ds = {"id": "source_orders", "side": "source", "row_count": None}
        mock_get_visible_ds.return_value = (ds, None)

        fake_duck = _FakeDuck(42)
        fake_conn = _FakeConn()
        mock_connect.return_value = fake_duck
        mock_get_connection.return_value = fake_conn

        parsed = json.loads(mcp_server.row_count_summary("source_orders"))

        self.assertEqual(parsed["dataset"], "source_orders")
        self.assertEqual(parsed["row_count"], 42)
        self.assertEqual(len(fake_duck.executed_sql), 1)
        self.assertIn("SELECT COUNT(*)", fake_duck.executed_sql[0])
        self.assertEqual(len(fake_conn.executed), 1)
        update_sql, update_params = fake_conn.executed[0]
        self.assertIn("UPDATE datasets SET row_count", update_sql)
        self.assertEqual(update_params[0], 42)
        self.assertEqual(update_params[2], "source_orders")
        self.assertTrue(fake_conn.committed)
        self.assertTrue(fake_conn.closed)


if __name__ == "__main__":
    unittest.main()
