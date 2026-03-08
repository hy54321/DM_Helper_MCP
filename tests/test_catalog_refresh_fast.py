import os
import shutil
import unittest
import uuid
from unittest.mock import patch

from server import catalog as cat
from server import db


class TestCatalogRefreshFastMode(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = db.get_connection(":memory:")
        base_tmp = os.path.join(os.getcwd(), "tests_tmp")
        os.makedirs(base_tmp, exist_ok=True)
        self.tmp_dir = os.path.join(base_tmp, f"catalog_fast_{uuid.uuid4().hex[:8]}")
        os.makedirs(self.tmp_dir, exist_ok=True)
        self.source_dir = os.path.join(self.tmp_dir, "source")
        self.target_dir = os.path.join(self.tmp_dir, "target")
        os.makedirs(self.source_dir, exist_ok=True)
        os.makedirs(self.target_dir, exist_ok=True)

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _write_csv(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("id,name\n1,A\n2,B\n")

    def test_refresh_fast_mode_skips_row_count_for_new_files(self) -> None:
        src_csv = os.path.join(self.source_dir, "customers.csv")
        self._write_csv(src_csv)

        summary = cat.refresh_catalog(
            source_folder=self.source_dir,
            target_folder=self.target_dir,
            include_row_counts=False,
            conn=self.conn,
        )

        self.assertFalse(summary["row_counts_included"])
        datasets = db.list_datasets(self.conn)
        self.assertEqual(len(datasets), 1)
        self.assertIsNone(datasets[0]["row_count"])

    def test_refresh_reuses_unchanged_file_metadata_without_recount(self) -> None:
        src_csv = os.path.join(self.source_dir, "customers.csv")
        self._write_csv(src_csv)

        # First pass computes and stores row_count.
        cat.refresh_catalog(
            source_folder=self.source_dir,
            target_folder=self.target_dir,
            include_row_counts=True,
            conn=self.conn,
        )
        first = db.list_datasets(self.conn)
        self.assertEqual(len(first), 1)
        self.assertEqual(first[0]["row_count"], 2)

        # Second pass should reuse unchanged metadata and skip expensive reads.
        with patch("server.catalog.read_csv_headers", side_effect=AssertionError("unexpected re-read")), patch(
            "server.catalog.count_csv_rows", side_effect=AssertionError("unexpected recount")
        ):
            cat.refresh_catalog(
                source_folder=self.source_dir,
                target_folder=self.target_dir,
                include_row_counts=False,
                conn=self.conn,
            )

        second = db.list_datasets(self.conn)
        self.assertEqual(len(second), 1)
        self.assertEqual(second[0]["row_count"], 2)

    def test_refresh_with_counts_on_fills_missing_counts_for_unchanged_files(self) -> None:
        src_csv = os.path.join(self.source_dir, "customers.csv")
        self._write_csv(src_csv)

        # Fast scan first: row_count intentionally omitted.
        cat.refresh_catalog(
            source_folder=self.source_dir,
            target_folder=self.target_dir,
            include_row_counts=False,
            conn=self.conn,
        )
        first = db.list_datasets(self.conn)
        self.assertEqual(len(first), 1)
        self.assertIsNone(first[0]["row_count"])

        # Slow scan should backfill counts even when file is unchanged.
        cat.refresh_catalog(
            source_folder=self.source_dir,
            target_folder=self.target_dir,
            include_row_counts=True,
            conn=self.conn,
        )
        second = db.list_datasets(self.conn)
        self.assertEqual(len(second), 1)
        self.assertEqual(second[0]["row_count"], 2)


if __name__ == "__main__":
    unittest.main()
