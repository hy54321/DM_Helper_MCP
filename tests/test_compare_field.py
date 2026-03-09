import os
import tempfile
import unittest

from server import comparison as comp
from server import db


class TestCompareField(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = db.get_connection(":memory:")
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.conn.close()
        self.tmp.cleanup()

    def _register_csv(self, dataset_id: str, side: str, filename: str, content: str) -> None:
        path = os.path.join(self.tmp.name, filename)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        db.upsert_dataset(
            self.conn,
            {
                "id": dataset_id,
                "side": side,
                "file_name": filename,
                "file_path": path,
                "sheet_name": "",
                "ext": ".csv",
                "columns": ["id", "name"],
                "raw_columns": ["id", "name"],
                "column_map": {"id": "id", "name": "name"},
                "row_count": 4,
            },
        )

    def test_compare_field_returns_total_and_rows(self) -> None:
        self._register_csv(
            "source_cmp",
            "source",
            "source.csv",
            "id,name\n1,Alice\n2,Bob\n3,Cara\n4,Dan\n",
        )
        self._register_csv(
            "target_cmp",
            "target",
            "target.csv",
            "id,name\n1,Alice\n2,Bobby\n3,Cara\n4,Daniel\n",
        )

        result = comp.compare_field(
            source_id="source_cmp",
            target_id="target_cmp",
            key_columns=["id"],
            field="name",
            limit=10,
            conn=self.conn,
        )

        self.assertEqual(result["source"], "source_cmp")
        self.assertEqual(result["target"], "target_cmp")
        self.assertEqual(result["field"], "name")
        self.assertEqual(result["total_differences"], 2)
        self.assertEqual(result["showing"], 2)
        self.assertEqual(len(result["rows"]), 2)
        self.assertEqual(result["rows"][0]["id"], "2")
        self.assertEqual(result["rows"][0]["source_value"], "Bob")
        self.assertEqual(result["rows"][0]["target_value"], "Bobby")

    def test_compare_field_no_differences(self) -> None:
        payload = "id,name\n1,Alice\n2,Bob\n"
        self._register_csv("source_same", "source", "source_same.csv", payload)
        self._register_csv("target_same", "target", "target_same.csv", payload)

        result = comp.compare_field(
            source_id="source_same",
            target_id="target_same",
            key_columns=["id"],
            field="name",
            limit=10,
            conn=self.conn,
        )

        self.assertEqual(result["total_differences"], 0)
        self.assertEqual(result["showing"], 0)
        self.assertEqual(result["rows"], [])


if __name__ == "__main__":
    unittest.main()

