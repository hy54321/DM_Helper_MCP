import re
import unittest

from server.reports import _normalize_report_filename


class TestReportFilenameNormalization(unittest.TestCase):
    def test_custom_filename_gets_timestamp_suffix(self) -> None:
        name = _normalize_report_filename("my_report.xlsx", "query_export")
        self.assertRegex(name, r"^my_report_\d{8}_\d{6}\.xlsx$")

    def test_filename_without_extension_gets_xlsx_and_timestamp(self) -> None:
        name = _normalize_report_filename("my_report", "query_export")
        self.assertRegex(name, r"^my_report_\d{8}_\d{6}\.xlsx$")

    def test_none_filename_uses_default_stem_with_timestamp(self) -> None:
        name = _normalize_report_filename(None, "query_export")
        self.assertRegex(name, r"^query_export_\d{8}_\d{6}\.xlsx$")

    def test_existing_timestamp_suffix_is_not_duplicated(self) -> None:
        fixed = "query_export_20260307_121314.xlsx"
        name = _normalize_report_filename(fixed, "query_export")
        self.assertEqual(name, fixed)
        self.assertEqual(len(re.findall(r"_\d{8}_\d{6}", name)), 1)


if __name__ == "__main__":
    unittest.main()
