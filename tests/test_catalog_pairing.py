import unittest

from server import catalog as cat
from server import db


def _make_dataset(
    dataset_id: str,
    side: str,
    file_name: str,
    columns: list[str],
    sheet_name: str = "",
) -> dict:
    return {
        "id": dataset_id,
        "side": side,
        "file_name": file_name,
        "file_path": f"C:/tmp/{file_name}",
        "sheet_name": sheet_name,
        "ext": ".csv",
        "columns": columns,
        "raw_columns": columns,
        "column_map": {c: c for c in columns},
        "row_count": 10,
    }


class TestCatalogAutoPairing(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = db.get_connection(":memory:")

    def tearDown(self) -> None:
        self.conn.close()

    def _register(self, datasets: list[dict]) -> None:
        for ds in datasets:
            db.upsert_dataset(self.conn, ds)

    def test_auto_pair_name_based_match(self) -> None:
        src = _make_dataset(
            "source_customers",
            "source",
            "source_Customers_V3.csv",
            ["customer_id", "name", "city"],
        )
        tgt = _make_dataset(
            "target_customers",
            "target",
            "target_Customers_V3.csv",
            ["customer_id", "name", "city"],
        )
        self._register([src, tgt])

        summary = cat._auto_pair(self.conn, [src], [tgt])  # noqa: SLF001 - intentional unit coverage

        self.assertEqual(summary["created"], 1)
        self.assertEqual(summary["created_by_name"], 1)
        self.assertEqual(summary["created_by_fields"], 0)
        pairs = db.list_pairs(self.conn)
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0]["source_dataset"], "source_customers")
        self.assertEqual(pairs[0]["target_dataset"], "target_customers")

    def test_auto_pair_field_overlap_fallback_creates_compare_mappings(self) -> None:
        src = _make_dataset(
            "source_a",
            "source",
            "legacy_extract_2025.csv",
            ["CustomerID", "Company", "DeliveryTerms", "DiscountGroup", "Region"],
        )
        tgt = _make_dataset(
            "target_b",
            "target",
            "new_erp_snapshot.csv",
            ["customer_id", "company", "delivery_terms", "discount_group", "status"],
        )
        self._register([src, tgt])

        summary = cat._auto_pair(self.conn, [src], [tgt])  # noqa: SLF001 - intentional unit coverage

        self.assertEqual(summary["created"], 1)
        self.assertEqual(summary["created_by_name"], 0)
        self.assertEqual(summary["created_by_fields"], 1)

        pairs = db.list_pairs(self.conn)
        self.assertEqual(len(pairs), 1)
        mappings = pairs[0]["compare_mappings"]
        self.assertGreaterEqual(len(mappings), 4)
        self.assertIn({"source_field": "CustomerID", "target_field": "customer_id"}, mappings)
        self.assertIn({"source_field": "DeliveryTerms", "target_field": "delivery_terms"}, mappings)

    def test_auto_pair_field_overlap_respects_threshold(self) -> None:
        src = _make_dataset(
            "source_orders",
            "source",
            "orders_legacy.csv",
            ["order_id", "customer_id", "amount", "currency", "region", "created_on"],
        )
        tgt = _make_dataset(
            "target_orders",
            "target",
            "sales_feed.csv",
            ["order_id", "customer_id", "amount", "posting_date", "status", "channel"],
        )
        self._register([src, tgt])

        # Shared fields = 3, min(side size) = 6 => overlap ratio = 0.5 (< 0.6 threshold)
        summary = cat._auto_pair(self.conn, [src], [tgt])  # noqa: SLF001 - intentional unit coverage

        self.assertEqual(summary["created"], 0)
        self.assertEqual(summary["created_by_name"], 0)
        self.assertEqual(summary["created_by_fields"], 0)
        self.assertEqual(len(db.list_pairs(self.conn)), 0)

    def test_auto_pair_field_overlap_prefers_matching_sheet_name_on_tie(self) -> None:
        cols = ["ACCOUNTNUM", "HSONEXMARTAPP", "PERSONNELNUMBER", "RESPONSIBILITYID"]
        src_matching_sheet = _make_dataset(
            "source_a_hso",
            "source",
            "source_extract.xlsx",
            cols,
            sheet_name="HSO_customer_responsibilities_",
        )
        src_other_sheet = _make_dataset(
            "source_z_other",
            "source",
            "source_extract.xlsx",
            cols,
            sheet_name="SLS-HSO customer responsibiliti",
        )
        tgt = _make_dataset(
            "target_hso",
            "target",
            "delta_export.xlsx",
            cols,
            sheet_name="HSO_customer_responsibilities_",
        )
        self._register([src_matching_sheet, src_other_sheet, tgt])

        summary = cat._auto_pair(
            self.conn,
            [src_matching_sheet, src_other_sheet],
            [tgt],
        )  # noqa: SLF001 - intentional unit coverage

        self.assertEqual(summary["created"], 1)
        self.assertEqual(summary["created_by_fields"], 1)
        pairs = db.list_pairs(self.conn)
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0]["source_dataset"], "source_a_hso")
        self.assertEqual(pairs[0]["target_dataset"], "target_hso")
        self.assertEqual(len(pairs[0]["compare_mappings"]), 4)


if __name__ == "__main__":
    unittest.main()
