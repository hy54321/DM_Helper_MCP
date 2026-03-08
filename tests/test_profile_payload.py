import os
import tempfile
import unittest

from server import db
from server import profile as prof


class TestDataProfilePayload(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = db.get_connection(":memory:")
        self.tmp = tempfile.TemporaryDirectory()
        self.csv_path = os.path.join(self.tmp.name, "sample.csv")
        with open(self.csv_path, "w", encoding="utf-8") as fh:
            fh.write("id,name\n")
            fh.write("1,Alice\n")
            fh.write("2,\n")
            fh.write(",Bob\n")

        db.upsert_dataset(
            self.conn,
            {
                "id": "source_sample",
                "side": "source",
                "file_name": "sample.csv",
                "file_path": self.csv_path,
                "sheet_name": "",
                "ext": ".csv",
                "columns": ["id", "name"],
                "raw_columns": ["id", "name"],
                "column_map": {"id": "id", "name": "name"},
                "row_count": 3,
            },
        )

    def tearDown(self) -> None:
        self.conn.close()
        self.tmp.cleanup()

    def test_data_profile_omits_redundant_and_null_fields(self) -> None:
        result = prof.data_profile("source_sample", conn=self.conn)
        self.assertEqual(result["dataset"], "source_sample")
        self.assertEqual(result["total_rows"], 3)
        self.assertEqual(len(result["columns"]), 2)

        for col in result["columns"]:
            self.assertNotIn("total_rows", col)
            self.assertNotIn("non_null", col)
            self.assertNotIn("null_count", col)
            self.assertIn("column", col)
            self.assertIn("distinct", col)
            self.assertIn("min", col)
            self.assertIn("max", col)
            self.assertIn("blank_count", col)


if __name__ == "__main__":
    unittest.main()

