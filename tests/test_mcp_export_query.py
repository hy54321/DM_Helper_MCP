import json
import unittest
from unittest.mock import patch

import mcp_server


class _FakeConn:
    def close(self) -> None:
        pass


class _FakeResult:
    description = [("col1", None, None, None, None, None, None)]

    def fetchall(self):
        raise AssertionError("fetchall should not be called in export_query streaming path")


class _FakeDuck:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _sql):
        return _FakeResult()


class TestMcpExportQuery(unittest.TestCase):
    @patch("mcp_server.job_svc.start_export_query_job")
    def test_export_query_async_uses_job_service(self, mock_start):
        mock_start.return_value = {"job_id": "job_123", "state": "queued"}
        result = mcp_server.export_query("SELECT 1")
        parsed = json.loads(result)
        self.assertEqual(parsed["status"], "accepted")
        self.assertEqual(parsed["state"], "queued")
        self.assertEqual(parsed["job_id"], "job_123")
        self.assertIn("Export started in background (job_123)", parsed["message"])
        mock_start.assert_called_once_with(sql="SELECT 1", filename=None)

    @patch("mcp_server.job_svc.start_export_query_job")
    def test_start_export_query_job_tool_returns_accepted_payload(self, mock_start):
        mock_start.return_value = {"job_id": "job_456", "state": "queued"}
        result = mcp_server.start_export_query_job("SELECT 1")
        parsed = json.loads(result)
        self.assertEqual(parsed["status"], "accepted")
        self.assertEqual(parsed["job_id"], "job_456")
        self.assertIn("Export started in background (job_456)", parsed["message"])

    @patch("mcp_server.rpt.export_query_to_xlsx")
    @patch("mcp_server.connect")
    @patch("mcp_server.db.list_datasets")
    @patch("mcp_server.db.get_connection")
    def test_export_query_sync_streams_rows_to_report(
        self,
        mock_get_connection,
        mock_list_datasets,
        mock_connect,
        mock_export,
    ):
        mock_get_connection.return_value = _FakeConn()
        mock_list_datasets.return_value = [{"id": "source_sample"}]
        mock_connect.return_value = _FakeDuck()
        mock_export.return_value = {"report_id": "r1", "row_count": 5}

        result = mcp_server.export_query("SELECT col1 FROM source_sample", async_job=False)
        parsed = json.loads(result)
        self.assertEqual(parsed["report_id"], "r1")
        args, kwargs = mock_export.call_args
        self.assertIsInstance(kwargs["rows"], _FakeResult)


if __name__ == "__main__":
    unittest.main()
