import json
import sys
import types
import unittest
from unittest.mock import patch

# Provide a lightweight FastMCP stub so tests do not require the external mcp package.
if "mcp.server.fastmcp" not in sys.modules:
    fastmcp_mod = types.ModuleType("mcp.server.fastmcp")

    class _FakeFastMCP:
        def __init__(self, *_args, **_kwargs):
            pass

        def tool(self):
            def _decorator(fn):
                return fn

            return _decorator

        def prompt(self):
            def _decorator(fn):
                return fn

            return _decorator

        def resource(self, _uri):
            def _decorator(fn):
                return fn

            return _decorator

        def run(self, *_args, **_kwargs):
            return None

    fastmcp_mod.FastMCP = _FakeFastMCP

    mcp_mod = types.ModuleType("mcp")
    server_mod = types.ModuleType("mcp.server")
    mcp_mod.server = server_mod
    server_mod.fastmcp = fastmcp_mod
    sys.modules["mcp"] = mcp_mod
    sys.modules["mcp.server"] = server_mod
    sys.modules["mcp.server.fastmcp"] = fastmcp_mod

import mcp_server


PAIR = {
    "id": "pair_keys",
    "source_dataset": "source_ds",
    "target_dataset": "target_ds",
    "key_mappings": [
        {"source_field": "ZipCode", "target_field": "ZIPCODE"},
        {"source_field": "COUNTRYREGIONID", "target_field": "COUNTRYREGIONID"},
        {"source_field": "City", "target_field": "CITYID"},
    ],
    "compare_mappings": [
        {"source_field": "ZipCode", "target_field": "ZIPCODE"},
        {"source_field": "COUNTRYREGIONID", "target_field": "COUNTRYREGIONID"},
        {"source_field": "City", "target_field": "CITYID"},
    ],
}


class TestMcpCompareMappingResolution(unittest.TestCase):
    @patch("mcp_server.cat.refresh_catalog")
    def test_refresh_catalog_tool_uses_saved_folders_only(self, mock_refresh):
        mock_refresh.return_value = {"ok": True}

        result = mcp_server.refresh_catalog(include_row_counts=True)

        parsed = json.loads(result)
        self.assertEqual(parsed, {"ok": True})
        mock_refresh.assert_called_once_with(include_row_counts=True)

    def test_refresh_catalog_tool_rejects_folder_arguments(self):
        with self.assertRaises(TypeError):
            mcp_server.refresh_catalog(source_folder="C:/tmp/source")

    @patch("mcp_server.comp.compare_datasets")
    @patch("mcp_server.cat.get_pair_by_datasets")
    def test_compare_tables_resolves_source_key_names(self, mock_get_pair, mock_compare):
        mock_get_pair.return_value = PAIR
        mock_compare.return_value = {"status": "ok"}

        result = mcp_server.compare_tables(
            source_dataset_id="source_ds",
            target_dataset_id="target_ds",
            key_fields="ZipCode,COUNTRYREGIONID,City",
        )

        parsed = json.loads(result)
        self.assertEqual(parsed, {"status": "ok"})
        kwargs = mock_compare.call_args.kwargs
        self.assertEqual(kwargs["key_columns"], ["ZipCode", "COUNTRYREGIONID", "City"])
        self.assertEqual(kwargs["key_mappings"], PAIR["key_mappings"])
        self.assertEqual(kwargs["compare_mappings"], PAIR["compare_mappings"])

    @patch("mcp_server.comp.compare_datasets")
    @patch("mcp_server.cat.get_pair_by_datasets")
    def test_compare_tables_resolves_target_key_names(self, mock_get_pair, mock_compare):
        mock_get_pair.return_value = PAIR
        mock_compare.return_value = {"status": "ok"}

        result = mcp_server.compare_tables(
            source_dataset_id="source_ds",
            target_dataset_id="target_ds",
            key_fields="ZIPCODE,COUNTRYREGIONID,CITYID",
        )

        parsed = json.loads(result)
        self.assertEqual(parsed, {"status": "ok"})
        kwargs = mock_compare.call_args.kwargs
        self.assertEqual(kwargs["key_columns"], ["ZipCode", "COUNTRYREGIONID", "City"])
        self.assertEqual(kwargs["key_mappings"], PAIR["key_mappings"])

    @patch("mcp_server.job_svc.start_comparison_job")
    @patch("mcp_server.cat.get_pair")
    def test_start_comparison_job_uses_pair_id_mapping(self, mock_get_pair, mock_start_job):
        mock_get_pair.return_value = PAIR
        mock_start_job.return_value = {"job_id": "job_1", "state": "queued"}

        result = mcp_server.start_comparison_job(
            source_dataset_id="source_ds",
            target_dataset_id="target_ds",
            key_fields="ZIPCODE,COUNTRYREGIONID,CITYID",
            pair_id="pair_keys",
        )

        parsed = json.loads(result)
        self.assertEqual(parsed, {"job_id": "job_1", "state": "queued"})
        kwargs = mock_start_job.call_args.kwargs
        self.assertEqual(kwargs["key_columns"], ["ZipCode", "COUNTRYREGIONID", "City"])
        self.assertEqual(kwargs["key_mappings"], PAIR["key_mappings"])
        self.assertEqual(kwargs["compare_mappings"], PAIR["compare_mappings"])
        self.assertEqual(kwargs["pair_id"], "pair_keys")

    @patch("mcp_server.comp.compare_datasets")
    @patch("mcp_server.cat.get_pair")
    def test_compare_tables_returns_error_for_unknown_pair_id(self, mock_get_pair, mock_compare):
        mock_get_pair.return_value = None

        result = mcp_server.compare_tables(
            source_dataset_id="source_ds",
            target_dataset_id="target_ds",
            key_fields="ZipCode",
            pair_id="pair_missing",
        )

        parsed = json.loads(result)
        self.assertEqual(parsed["error"], "Pair 'pair_missing' not found.")
        mock_compare.assert_not_called()


if __name__ == "__main__":
    unittest.main()
