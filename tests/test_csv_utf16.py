import os
import tempfile
import unittest

from server import catalog as cat
from server import comparison as comp
from server import db
from server.query_engine import count_csv_rows, detect_text_encoding, read_csv_headers


class TestCsvUtf16Support(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = db.get_connection(":memory:")
        self.tmp = tempfile.TemporaryDirectory()
        self.source_dir = os.path.join(self.tmp.name, "source")
        self.target_dir = os.path.join(self.tmp.name, "target")
        os.makedirs(self.source_dir, exist_ok=True)
        os.makedirs(self.target_dir, exist_ok=True)

    def tearDown(self) -> None:
        self.conn.close()
        self.tmp.cleanup()

    def _write_utf16_csv(self, path: str, content: str) -> None:
        with open(path, "w", encoding="utf-16") as fh:
            fh.write(content)

    def test_read_helpers_detect_utf16(self) -> None:
        path = os.path.join(self.source_dir, "customers.csv")
        self._write_utf16_csv(path, "id,name\n1,Alice\n2,Bob\n")

        py_enc, duck_enc = detect_text_encoding(path)
        self.assertIn(py_enc, ("utf-16", "utf-16le", "utf-16be"))
        self.assertEqual(duck_enc, "utf-16")
        self.assertEqual(read_csv_headers(path), ["id", "name"])
        self.assertEqual(count_csv_rows(path), 2)

    def test_refresh_catalog_populates_headers_counts_and_encoding_for_utf16(self) -> None:
        path = os.path.join(self.source_dir, "customers.csv")
        self._write_utf16_csv(path, "id,name\n1,Alice\n2,Bob\n")

        summary = cat.refresh_catalog(
            source_folder=self.source_dir,
            target_folder=self.target_dir,
            include_row_counts=True,
            conn=self.conn,
        )

        self.assertEqual(summary["source_datasets"], 1)
        ds = db.get_dataset(self.conn, "source_customers")
        self.assertIsNotNone(ds)
        self.assertEqual(ds["raw_columns"], ["id", "name"])
        self.assertEqual(ds["row_count"], 2)
        self.assertEqual(ds["csv_encoding"], "utf-16")

    def test_compare_field_reads_utf16_even_without_persisted_encoding(self) -> None:
        src_path = os.path.join(self.source_dir, "source.csv")
        tgt_path = os.path.join(self.target_dir, "target.csv")
        self._write_utf16_csv(src_path, "id,name\n1,Alice\n2,Bob\n")
        self._write_utf16_csv(tgt_path, "id,name\n1,Alice\n2,Bobby\n")

        db.upsert_dataset(
            self.conn,
            {
                "id": "source_utf16",
                "side": "source",
                "file_name": "source.csv",
                "file_path": src_path,
                "sheet_name": "",
                "ext": ".csv",
                "columns": ["id", "name"],
                "raw_columns": ["id", "name"],
                "column_map": {"id": "id", "name": "name"},
                "row_count": 2,
            },
        )
        db.upsert_dataset(
            self.conn,
            {
                "id": "target_utf16",
                "side": "target",
                "file_name": "target.csv",
                "file_path": tgt_path,
                "sheet_name": "",
                "ext": ".csv",
                "columns": ["id", "name"],
                "raw_columns": ["id", "name"],
                "column_map": {"id": "id", "name": "name"},
                "row_count": 2,
            },
        )

        result = comp.compare_field(
            source_id="source_utf16",
            target_id="target_utf16",
            key_columns=["id"],
            field="name",
            limit=10,
            conn=self.conn,
        )

        self.assertEqual(result["total_differences"], 1)
        self.assertEqual(result["rows"], [{"id": "2", "source_value": "Bob", "target_value": "Bobby"}])


if __name__ == "__main__":
    unittest.main()
