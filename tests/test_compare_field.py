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

    def _register_csv(
        self,
        dataset_id: str,
        side: str,
        filename: str,
        content: str,
        columns: list[str] | None = None,
    ) -> None:
        path = os.path.join(self.tmp.name, filename)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        cols = columns or ["id", "name"]
        db.upsert_dataset(
            self.conn,
            {
                "id": dataset_id,
                "side": side,
                "file_name": filename,
                "file_path": path,
                "sheet_name": "",
                "ext": ".csv",
                "columns": cols,
                "raw_columns": cols,
                "column_map": {c: c for c in cols},
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

    def test_compare_field_with_explicit_mappings(self) -> None:
        self._register_csv(
            "source_map",
            "source",
            "source_map.csv",
            "id_src,name_src\n1,Alice\n2,Bob\n",
            columns=["id_src", "name_src"],
        )
        self._register_csv(
            "target_map",
            "target",
            "target_map.csv",
            "id_tgt,name_tgt\n1,Alice\n2,Bobby\n",
            columns=["id_tgt", "name_tgt"],
        )

        result = comp.compare_field(
            source_id="source_map",
            target_id="target_map",
            key_columns=["id_src"],
            field="name_tgt",
            key_mappings=[{"source_field": "id_src", "target_field": "id_tgt"}],
            field_mapping={"source_field": "name_src", "target_field": "name_tgt"},
            limit=10,
            conn=self.conn,
        )

        self.assertEqual(result["field"], "name_tgt")
        self.assertEqual(result["source_field"], "name_src")
        self.assertEqual(result["target_field"], "name_tgt")
        self.assertEqual(result["field_mapping"], "name_src->name_tgt")
        self.assertEqual(result["total_differences"], 1)
        self.assertEqual(result["rows"], [{"id_src": "2", "source_value": "Bob", "target_value": "Bobby"}])


if __name__ == "__main__":
    unittest.main()
