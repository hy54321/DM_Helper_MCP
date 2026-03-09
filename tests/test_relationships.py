import os
import tempfile
import unittest

from server import db
from server import relationships as rel


class TestRelationships(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = db.get_connection(":memory:")
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.conn.close()
        self.tmp.cleanup()

    def _write_csv(self, name: str, content: str) -> str:
        path = os.path.join(self.tmp.name, name)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        return path

    def _register_dataset(self, dataset_id: str, side: str, path: str, columns: list[str]) -> None:
        db.upsert_dataset(
            self.conn,
            {
                "id": dataset_id,
                "side": side,
                "file_name": os.path.basename(path),
                "file_path": path,
                "sheet_name": "",
                "ext": ".csv",
                "columns": columns,
                "raw_columns": columns,
                "column_map": {c: c for c in columns},
                "row_count": 0,
            },
        )

    def test_link_related_tables_suggest_only_does_not_persist(self) -> None:
        p1 = self._write_csv(
            "customers.csv",
            "PARTYNUMBER,CUSTOMERACCOUNT\n1,C1\n2,C2\n3,C3\n",
        )
        p2 = self._write_csv(
            "contacts.csv",
            "PARTY_NUMBER,LOCATOR\n1,a@example.com\n2,\n4,b@example.com\n",
        )
        self._register_dataset("target_customers", "target", p1, ["PARTYNUMBER", "CUSTOMERACCOUNT"])
        self._register_dataset("target_contacts", "target", p2, ["PARTY_NUMBER", "LOCATOR"])

        result = rel.link_related_tables(
            side="target",
            min_confidence=0.8,
            suggest_only=True,
            conn=self.conn,
        )

        self.assertGreaterEqual(result["suggested_count"], 1)
        stored = db.list_relationships(self.conn, side="target", limit=100)
        self.assertEqual(len(stored), 0)

    def test_link_related_tables_persists_high_confidence_links(self) -> None:
        p1 = self._write_csv(
            "customers.csv",
            "PARTYNUMBER,CUSTOMERACCOUNT\n1,C1\n2,C2\n3,C3\n",
        )
        p2 = self._write_csv(
            "contacts.csv",
            "PARTY_NUMBER,LOCATOR\n1,a@example.com\n2,\n4,b@example.com\n",
        )
        self._register_dataset("target_customers", "target", p1, ["PARTYNUMBER", "CUSTOMERACCOUNT"])
        self._register_dataset("target_contacts", "target", p2, ["PARTY_NUMBER", "LOCATOR"])

        result = rel.link_related_tables(
            side="target",
            min_confidence=0.8,
            suggest_only=False,
            conn=self.conn,
        )

        self.assertGreaterEqual(result["applied_count"], 1)
        stored = db.list_relationships(self.conn, side="target", limit=100)
        self.assertGreaterEqual(len(stored), 1)
        sigs = {(r["left_field"], r["right_field"]) for r in stored}
        self.assertTrue(
            ("PARTYNUMBER", "PARTY_NUMBER") in sigs
            or ("PARTY_NUMBER", "PARTYNUMBER") in sigs
        )

    def test_relationship_crud_helpers(self) -> None:
        p = self._write_csv("simple.csv", "A\nx\n")
        self._register_dataset("source_a", "source", p, ["A"])
        self._register_dataset("source_b", "source", p, ["A"])

        row = db.upsert_relationship(
            self.conn,
            side="source",
            left_dataset="source_a",
            left_field="A",
            right_dataset="source_b",
            right_field="A",
            confidence=0.9,
            method="manual",
            active=True,
        )
        self.assertTrue(row["id"])
        rid = row["id"]

        updated = db.update_relationship(
            self.conn,
            relationship_id=rid,
            side="source",
            left_dataset="source_a",
            left_field="A",
            right_dataset="source_b",
            right_field="A",
            confidence=0.95,
            method="manual_edit",
            active=False,
        )
        self.assertIsNotNone(updated)
        self.assertEqual(updated["confidence"], 0.95)
        self.assertFalse(updated["active"])

        listed = db.list_relationships(self.conn, side="source", limit=10)
        self.assertEqual(len(listed), 1)

        ok = db.delete_relationship(self.conn, rid)
        self.assertTrue(ok)
        self.assertEqual(len(db.list_relationships(self.conn, side="source", limit=10)), 0)

    def test_link_related_tables_filters_low_information_and_non_keylike_matches(self) -> None:
        customers = self._write_csv(
            "customers.csv",
            (
                "CUSTOMERACCOUNT,PARTYNUMBER,LEGALENTITY,ISPRIMARY\n"
                "C1,P1,USMF,1\n"
                "C2,P2,USMF,0\n"
                "C3,P3,USMF,1\n"
                "C4,P4,USMF,0\n"
            ),
        )
        postal = self._write_csv(
            "postal.csv",
            (
                "CUSTOMERACCOUNTNUMBER,CUSTOMERLEGALENTITYID,ISPRIMARY\n"
                "C1,USMF,1\n"
                "C1,USMF,0\n"
                "C2,USMF,1\n"
                "C3,USMF,0\n"
            ),
        )
        responsibilities = self._write_csv(
            "responsibilities.csv",
            "ACCOUNTNUM\nC1\nC1\nC2\nC3\n",
        )
        contacts = self._write_csv(
            "contacts.csv",
            "PARTYNUMBER,ISPRIMARY\nP1,1\nP1,0\nP2,0\nP3,1\n",
        )

        self._register_dataset(
            "target_customers",
            "target",
            customers,
            ["CUSTOMERACCOUNT", "PARTYNUMBER", "LEGALENTITY", "ISPRIMARY"],
        )
        self._register_dataset(
            "target_postal",
            "target",
            postal,
            ["CUSTOMERACCOUNTNUMBER", "CUSTOMERLEGALENTITYID", "ISPRIMARY"],
        )
        self._register_dataset(
            "target_responsibilities",
            "target",
            responsibilities,
            ["ACCOUNTNUM"],
        )
        self._register_dataset(
            "target_contacts",
            "target",
            contacts,
            ["PARTYNUMBER", "ISPRIMARY"],
        )

        result = rel.link_related_tables(
            side="target",
            min_confidence=0.9,
            suggest_only=True,
            conn=self.conn,
        )

        found = {
            frozenset(
                (
                    (r["left_dataset"], r["left_field"]),
                    (r["right_dataset"], r["right_field"]),
                )
            )
            for r in result.get("relationships", [])
        }

        self.assertIn(
            frozenset(
                (
                    ("target_customers", "CUSTOMERACCOUNT"),
                    ("target_postal", "CUSTOMERACCOUNTNUMBER"),
                )
            ),
            found,
        )
        self.assertIn(
            frozenset(
                (
                    ("target_customers", "CUSTOMERACCOUNT"),
                    ("target_responsibilities", "ACCOUNTNUM"),
                )
            ),
            found,
        )
        self.assertIn(
            frozenset(
                (
                    ("target_customers", "PARTYNUMBER"),
                    ("target_contacts", "PARTYNUMBER"),
                )
            ),
            found,
        )

        self.assertNotIn(
            frozenset(
                (
                    ("target_customers", "LEGALENTITY"),
                    ("target_postal", "CUSTOMERLEGALENTITYID"),
                )
            ),
            found,
        )
        self.assertNotIn(
            frozenset(
                (
                    ("target_postal", "ISPRIMARY"),
                    ("target_contacts", "ISPRIMARY"),
                )
            ),
            found,
        )
        self.assertNotIn(
            frozenset(
                (
                    ("target_postal", "CUSTOMERACCOUNTNUMBER"),
                    ("target_responsibilities", "ACCOUNTNUM"),
                )
            ),
            found,
        )

    def test_relationship_supports_composite_field_pairs(self) -> None:
        p = self._write_csv("orders.csv", "SALESID,LINENUM\nSO1,1\n")
        self._register_dataset("target_order_lines_a", "target", p, ["SALESID", "LINENUM"])
        self._register_dataset("target_order_lines_b", "target", p, ["SALESID", "LINENUM"])

        row = db.upsert_relationship(
            self.conn,
            side="target",
            left_dataset="target_order_lines_a",
            left_field="SALESID",
            left_fields=["SALESID", "LINENUM"],
            right_dataset="target_order_lines_b",
            right_field="SALESID",
            right_fields=["SALESID", "LINENUM"],
            confidence=0.97,
            method="manual",
            active=True,
        )
        self.assertEqual(row["left_fields"], ["SALESID", "LINENUM"])
        self.assertEqual(row["right_fields"], ["SALESID", "LINENUM"])
        self.assertEqual(len(row["field_pairs"]), 2)
        self.assertEqual(row["left_field"], "SALESID")
        self.assertEqual(row["right_field"], "SALESID")

        listed = db.list_relationships(self.conn, side="target", limit=10)
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0]["left_fields"], ["SALESID", "LINENUM"])
        self.assertEqual(listed[0]["right_fields"], ["SALESID", "LINENUM"])


if __name__ == "__main__":
    unittest.main()
