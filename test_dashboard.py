import tempfile
import unittest
from pathlib import Path

from dashboard import aggregate_by_region_category, aggregate_kpis, build_dashboard_html, load_leads


class DashboardTests(unittest.TestCase):
    def test_load_and_aggregate(self):
        csv_text = """as_of_date,bid_ntce_no,bid_ntce_ord,notice_title,work_type,agency_name,region_guess,deadline_dt,budget_amt,category,match_strength,urgency_score,source_url,contact_policy,notes
2026-01-01,1,1,리더십 교육 운영 용역,용역,서울특별시교육청,서울,202601311800,100,HRD/교육,0.72,80,http://x,a,b
2026-01-01,2,1,조직진단 컨설팅,용역,경기도청,경기,미지정,200,OD/조직,0.84,70,http://y,a,b
"""
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "lead.csv"
            p.write_text(csv_text, encoding="utf-8")
            rows = load_leads(p)

        self.assertEqual(len(rows), 2)
        kpis = aggregate_kpis(rows)
        self.assertEqual(kpis["total_leads"], "2")
        self.assertEqual(kpis["total_budget"], "300")
        grouped = aggregate_by_region_category(rows)
        self.assertEqual(len(grouped), 2)

    def test_html_contains_sections(self):
        rows = []
        kpis = {"total_leads": "0", "total_budget": "0", "known_deadlines": "0", "known_regions": "0"}
        grouped = []
        html_doc = build_dashboard_html(rows, kpis, grouped)
        self.assertIn("지역 × 분류 집계", html_doc)
        self.assertIn("공고 상세 리드 목록", html_doc)


if __name__ == "__main__":
    unittest.main()
