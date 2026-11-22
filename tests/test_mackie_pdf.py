import io
import logging
import unittest
from datetime import date

from reportlab.pdfgen.canvas import Canvas

from src.bots.mackie_wolf_trustee_bot import MackieWolfTrusteeBot


TITLE = "Sale Date File Address Full County Sale Trustee Designation"
# Printed sale, file, street, city, ZIP, county, and designation, Nov 20 report.
MISSED = (
    ("11/24/2026", "23-000240-850", "280 W Lawnwood Dr", "Collierville", "38017", "Shelby", "HUBZU"),
    ("12/10/2026", "15-001673-670", "814 Lake Terrace Dr", "Nashville", "37217-4230", "Davidson", "AUCTION"),
    ("12/21/2026", "26-000315-671", "310 S Tipton Street", "Covington", "38019", "Tipton", "AUCTION"),
    ("1/5/2027", "26-000082-363", "501 N Front Street", "Rockwood", "37854", "Roane", "COVIUS_REALTYBID"),
    ("1/14/2027", "25-000261-505", "1830 Dogwood Dr", "Chattanooga", "37406-2721", "Hamilton", "AUCTION"),
    ("1/19/2027", "24-000344-671", "305 W High Street", "Woodbury", "37190", "Cannon", "HUBZU"),
    ("2/4/2027", "25-000116-363", "501 1st Street", "Lawrenceburg", "38464", "Lawrence", "HUDMARSH"),
    ("2/11/2027", "26-000114-505", "833A E Northfield Blvd", "Murfreesboro", "37130", "Rutherford", "AUCTION"),
)


def parse(*pages):
    stream = io.BytesIO()
    canvas = Canvas(stream)
    for lines in pages:
        text = canvas.beginText(15, 800)
        for line in lines:
            text.textLine(line)
        canvas.drawText(text)
        canvas.showPage()
    canvas.save()
    bot = object.__new__(MackieWolfTrusteeBot)
    bot.logger = logging.getLogger(__name__)
    return bot._parse_pdf(stream.getvalue(), "https://example.org/report.pdf", date(2026, 11, 20))


class MackiePDFTests(unittest.TestCase):
    def test_eight_published_records_keep_printed_identity(self):
        lines = [TITLE]
        for day, file_no, street, city, zip_code, county, platform in MISSED:
            lines.extend([f"{day} {file_no} {street} {county} {platform}", f"{city} TN {zip_code}"])
        leads = parse(lines)
        self.assertEqual(len(leads), len(MISSED))
        for lead, (day, file_no, street, city, zip_code, county, platform) in zip(leads, MISSED):
            month, dom, year = map(int, day.split("/"))
            self.assertEqual(lead.trustee_sale_date, date(year, month, dom).isoformat())
            self.assertEqual(lead.raw_payload["file_no"], file_no)
            self.assertEqual(lead.pipeline_lead_key, MackieWolfTrusteeBot.make_lead_key("mackie_wolf", file_no))
            self.assertEqual(lead.property_address, f"{street}, {city}, TN {zip_code}")
            self.assertEqual((lead.county, lead.raw_payload["sale_platform"]), (county, platform))
            self.assertIsNone(lead.owner_name_records)
            self.assertEqual(lead.raw_payload["borrowers"], [])

    def test_wrapped_unit_and_city_across_pages(self):
        lead, = parse([TITLE, "1/7/2027 25-000487-671 204 Douglas Bend Road Sumner HUBZU"],
                      ["Unit 504", "Gallatin TN 37066"])
        self.assertEqual(lead.property_address, "204 Douglas Bend Road Unit 504, Gallatin, TN 37066")

    def test_ambiguous_fields_do_not_generate_a_property(self):
        for header, rest in (
            ("833A E Northfield Blvd Unknown AUCTION", ["Murfreesboro TN 37130"]),
            ("833A E Northfield Blvd Rutherford UNKNOWN", ["Murfreesboro TN 37130"]),
            ("833A E Northfield Blvd Rutherford AUCTION", ["Other Street", "Murfreesboro TN 37130"]),
            ("833A E Northfield Blvd Rutherford AUCTION", ["Murfreesboro TN 37130", "Smyrna TN 37167"]),
        ):
            with self.subTest(header=header, rest=rest):
                self.assertEqual(parse([TITLE, "2/11/2027 26-000114-505 " + header, *rest]), [])

    def test_older_borrower_column_still_returns_owner(self):
        lead, = parse(["Sale Date File Borrowers Address Full County",
                       "1/7/2027 25-000487-671 Jane Example (Borrower) 204 Douglas Bend Road Sumner",
                       "Gallatin TN 37066"])
        self.assertEqual(lead.owner_name_records, "Jane Example")
        self.assertEqual(lead.property_address, "204 Douglas Bend Road, Gallatin, TN 37066")

    def test_printed_test_row_does_not_supply_a_second_city_to_previous_file(self):
        lead, = parse([TITLE, "11/12/2026 26-000715-671 415 Shady Oaks Drive Van Buren HUBZU",
                       "Spencer TN 38585", "11/12/2026 GL-TEST FILE 2026-TN 123 Any Avenue Sumner MWZM",
                       "Anytown TN 37148"])
        self.assertEqual(lead.county, "Van Buren")
        self.assertEqual(lead.property_address, "415 Shady Oaks Drive, Spencer, TN 38585")
