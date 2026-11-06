"""python -m unittest tests.test_montgomery_tax_sale  (synthetic workbook)"""
import io
import sys
import unittest
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots import montgomery_tax_sale_bot as m  # noqa: E402

HEAD = ["Primary Owner", "Map & Parcel", "Parcel Address", "Tax Year/Entity", "Base Tax", "Deed Reference", "Amount Sold For", "Who Purchased"]


def workbook(rows):
    strings, out = [], []
    for ri, row in enumerate(rows, 1):
        cells = []
        for ci, v in enumerate(row):
            ref = f"{chr(65 + ci)}{ri}"
            if v is None:
                continue
            if isinstance(v, (int, float)):
                cells.append(f'<c r="{ref}"><v>{v}</v></c>')
            else:
                strings.append(v)
                cells.append(f'<c r="{ref}" t="s"><v>{len(strings) - 1}</v></c>')
        out.append(f'<row r="{ri}">{"".join(cells)}</row>')
    ns = 'xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("xl/sharedStrings.xml", f'<sst {ns}>' + "".join(f"<si><t>{s.replace('&', '&amp;')}</t></si>" for s in strings) + "</sst>")
        z.writestr("xl/worksheets/sheet1.xml", f'<worksheet {ns}><sheetData>{"".join(out)}</sheetData></worksheet>')
    return buf.getvalue()


class MontgomeryTaxSaleTests(unittest.TestCase):
    def test_rows_and_continuations(self):
        data = workbook([HEAD,
                         ["Example, Pat Etal", "011G B 031.00", "Example Road", "2022 County", 105, "V1, 2", 5100, "Buyer LLC"],
                         [None, None, None, "2023 County", 105],
                         ["Sample, Lee", "011G B 002.00", "528 Example Street", "2022 County   2022 City", "$41.00         $17.00", "V1, 3", 2800, "Someone"]])
        rows = m.parse_rows(m.read_xlsx(data))
        self.assertEqual(len(rows), 2)
        self.assertEqual((rows[0]["tax_years"], rows[0]["base_tax"], rows[0]["sold_for"]), (["2022 County", "2023 County"], 210.0, 5100.0))
        self.assertEqual((rows[1]["base_tax"], rows[1]["address"]), (58.0, "528 Example Street"))

    def test_sale_info_and_deadline(self):
        info = m.sale_info("<p>The 2026 Tax Sale was on September 17, 2026. This is the list</p>")
        self.assertEqual(info, {"sale_date": "2026-09-17", "sold": True})
        self.assertFalse(m.sale_info("The 2027 Tax Sale will be held on Thursday, September 16, 2027.")["sold"])
        self.assertEqual(m.redemption_deadline("2026-09-17"), "2027-09-17")
        self.assertEqual(m.redemption_deadline("2028-02-29"), "2029-02-28")
        self.assertIsNone(m.redemption_deadline(None))


if __name__ == "__main__":
    unittest.main()
