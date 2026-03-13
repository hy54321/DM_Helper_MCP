import unittest

from server import sql_guard


class TestSqlGuard(unittest.TestCase):
    def test_allows_leading_comment_with_with_union_query(self) -> None:
        sql = """-- Count source records excluded by each rule
WITH
valid_items AS (
    SELECT DISTINCT ITEMNUMBER FROM target_SLS_Released_products_V2_Released_products_V2
),
valid_price_groups AS (
    SELECT DISTINCT Price_groups FROM target_price_groups_Sheet1
),
valid_customers AS (
    SELECT DISTINCT CUSTOMERACCOUNT FROM target_SLS_Customers_V3_Customers_V3
)
SELECT
    'Total Source Records' as Category,
    COUNT(*) as Count
FROM source_salesprices_ax

UNION ALL

SELECT
    'Excluded - Item not in Released Products' as Category,
    COUNT(*) as Count
FROM source_salesprices_ax s
WHERE NOT EXISTS (SELECT 1 FROM valid_items v WHERE s.ItemRelation = v.ITEMNUMBER)

UNION ALL

SELECT
    'Excluded - Price Group not in Target' as Category,
    COUNT(*) as Count
FROM source_salesprices_ax s
WHERE s.AccountCode = 'Group'
  AND NOT EXISTS (SELECT 1 FROM valid_price_groups v WHERE s.AccountRelation = v.Price_groups)

UNION ALL

SELECT
    'Excluded - Customer not in Target' as Category,
    COUNT(*) as Count
FROM source_salesprices_ax s
WHERE s.AccountCode = 'Table'
  AND NOT EXISTS (SELECT 1 FROM valid_customers v WHERE s.AccountRelation = v.CUSTOMERACCOUNT)
"""
        ok, err = sql_guard.validate(sql)
        self.assertTrue(ok, err)

    def test_allows_block_comment_prefix(self) -> None:
        ok, err = sql_guard.validate("/* report query */ SELECT 1")
        self.assertTrue(ok, err)

    def test_ignores_semicolon_inside_comment(self) -> None:
        ok, err = sql_guard.validate("-- this ; is in a comment\nSELECT 1")
        self.assertTrue(ok, err)

    def test_rejects_multi_statement(self) -> None:
        ok, err = sql_guard.validate("SELECT 1; SELECT 2")
        self.assertFalse(ok)
        self.assertEqual(err, "Multiple statements are not allowed.")


if __name__ == "__main__":
    unittest.main()
