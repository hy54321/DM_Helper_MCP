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

    def test_column_value_summary_counts_blanks_and_keeps_contract(self) -> None:
        result = prof.column_value_summary("source_sample", top_n=10, conn=self.conn)
        self.assertEqual(result["dataset"], "source_sample")
        self.assertEqual(len(result["summaries"]), 2)

        by_col = {entry["column"]: entry for entry in result["summaries"]}
        self.assertEqual(by_col["id"]["blank_or_null_count"], 1)
        self.assertEqual(by_col["name"]["blank_or_null_count"], 1)

        self.assertTrue(by_col["id"]["top_values"])
        self.assertTrue(by_col["name"]["top_values"])
        self.assertIn("value", by_col["id"]["top_values"][0])
        self.assertIn("count", by_col["id"]["top_values"][0])

    def test_find_duplicates_returns_total_and_top_rows(self) -> None:
        dup_csv = os.path.join(self.tmp.name, "dup_sample.csv")
        with open(dup_csv, "w", encoding="utf-8") as fh:
            fh.write("key,val\n")
            fh.write("A,1\n")
            fh.write("A,2\n")
            fh.write("B,3\n")
            fh.write("C,4\n")
            fh.write("C,5\n")
            fh.write("C,6\n")

        db.upsert_dataset(
            self.conn,
            {
                "id": "source_dup",
                "side": "source",
                "file_name": "dup_sample.csv",
                "file_path": dup_csv,
                "sheet_name": "",
                "ext": ".csv",
                "columns": ["key", "val"],
                "raw_columns": ["key", "val"],
                "column_map": {"key": "key", "val": "val"},
                "row_count": 6,
            },
        )

        result = prof.find_duplicates("source_dup", ["key"], limit=10, conn=self.conn)
        self.assertEqual(result["dataset"], "source_dup")
        self.assertEqual(result["key_columns"], ["key"])
        self.assertEqual(result["total_duplicate_groups"], 2)
        self.assertEqual(result["showing"], 2)
        self.assertEqual(len(result["duplicates"]), 2)
        self.assertEqual(result["duplicates"][0]["key"], "C")
        self.assertEqual(result["duplicates"][0]["duplicate_count"], 3)
        self.assertEqual(result["duplicates"][1]["key"], "A")
        self.assertEqual(result["duplicates"][1]["duplicate_count"], 2)

    def test_suggest_keys_prefers_high_uniqueness_and_overlap(self) -> None:
        src_csv = os.path.join(self.tmp.name, "src_keys.csv")
        tgt_csv = os.path.join(self.tmp.name, "tgt_keys.csv")
        with open(src_csv, "w", encoding="utf-8") as fh:
            fh.write("id,code,name\n")
            fh.write("1,A,alice\n")
            fh.write("2,B,bob\n")
            fh.write("3,C,cara\n")
            fh.write("4,C,dan\n")
        with open(tgt_csv, "w", encoding="utf-8") as fh:
            fh.write("id,code,name\n")
            fh.write("1,A,alice\n")
            fh.write("2,B,bob2\n")
            fh.write("3,D,cara2\n")
            fh.write("4,C,dan2\n")

        db.upsert_dataset(
            self.conn,
            {
                "id": "source_keys",
                "side": "source",
                "file_name": "src_keys.csv",
                "file_path": src_csv,
                "sheet_name": "",
                "ext": ".csv",
                "columns": ["id", "code", "name"],
                "raw_columns": ["id", "code", "name"],
                "column_map": {"id": "id", "code": "code", "name": "name"},
                "row_count": 4,
            },
        )
        db.upsert_dataset(
            self.conn,
            {
                "id": "target_keys",
                "side": "target",
                "file_name": "tgt_keys.csv",
                "file_path": tgt_csv,
                "sheet_name": "",
                "ext": ".csv",
                "columns": ["id", "code", "name"],
                "raw_columns": ["id", "code", "name"],
                "column_map": {"id": "id", "code": "code", "name": "name"},
                "row_count": 4,
            },
        )
        db.upsert_pair(
            self.conn,
            pair_id="pair_keys",
            source_dataset="source_keys",
            target_dataset="target_keys",
            auto_matched=True,
            enabled=True,
        )

        result = prof.suggest_keys("pair_keys", conn=self.conn)
        self.assertEqual(result["pair_id"], "pair_keys")
        self.assertEqual(result["source"], "source_keys")
        self.assertEqual(result["target"], "target_keys")
        self.assertTrue(result["candidates"])
        self.assertEqual(result["candidates"][0]["column"], "id")

    def test_suggest_keys_completeness_uses_non_blank_ratio(self) -> None:
        src_csv = os.path.join(self.tmp.name, "src_blank_keys.csv")
        tgt_csv = os.path.join(self.tmp.name, "tgt_blank_keys.csv")
        with open(src_csv, "w", encoding="utf-8") as fh:
            fh.write("id,maybe_key\n")
            fh.write("1,\n")
            fh.write("2,\n")
            fh.write("3,X\n")
            fh.write("4,\n")
        with open(tgt_csv, "w", encoding="utf-8") as fh:
            fh.write("id,maybe_key\n")
            fh.write("1,\n")
            fh.write("2,Y\n")
            fh.write("3,\n")
            fh.write("4,\n")

        db.upsert_dataset(
            self.conn,
            {
                "id": "source_blank_keys",
                "side": "source",
                "file_name": "src_blank_keys.csv",
                "file_path": src_csv,
                "sheet_name": "",
                "ext": ".csv",
                "columns": ["id", "maybe_key"],
                "raw_columns": ["id", "maybe_key"],
                "column_map": {"id": "id", "maybe_key": "maybe_key"},
                "row_count": 4,
            },
        )
        db.upsert_dataset(
            self.conn,
            {
                "id": "target_blank_keys",
                "side": "target",
                "file_name": "tgt_blank_keys.csv",
                "file_path": tgt_csv,
                "sheet_name": "",
                "ext": ".csv",
                "columns": ["id", "maybe_key"],
                "raw_columns": ["id", "maybe_key"],
                "column_map": {"id": "id", "maybe_key": "maybe_key"},
                "row_count": 4,
            },
        )
        db.upsert_pair(
            self.conn,
            pair_id="pair_blank_keys",
            source_dataset="source_blank_keys",
            target_dataset="target_blank_keys",
            auto_matched=True,
            enabled=True,
        )

        result = prof.suggest_keys("pair_blank_keys", conn=self.conn)
        by_col = {c["column"]: c for c in result["candidates"]}
        self.assertIn("maybe_key", by_col)
        self.assertLess(by_col["maybe_key"]["completeness"], 1.0)


if __name__ == "__main__":
    unittest.main()
