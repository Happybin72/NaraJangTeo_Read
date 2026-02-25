import unittest
from datetime import datetime, timedelta

from g2b_lead_miner import (
    RawNotice,
    build_leads,
    dedupe_notices,
    match_category,
    urgency_score,
)


class MatchCategoryTests(unittest.TestCase):
    def test_match_examples(self):
        self.assertEqual(match_category("리더십 역량강화 교육 운영 용역")[0], "HRD/교육")
        self.assertEqual(match_category("조직진단 및 조직문화 개선 컨설팅 용역")[0], "OD/조직")
        self.assertEqual(match_category("성과평가 제도 고도화 컨설팅")[0], "평가/제도")

    def test_noise(self):
        label, _, _ = match_category("전기설비 안전점검 용역")
        self.assertIn(label, ("비대상", "보류(노이즈 가능)"))


class DedupeTests(unittest.TestCase):
    def test_dedupe(self):
        n1 = RawNotice("123", "1", "교육 운영", "용역", "서울시", "", "", None, "u")
        n2 = RawNotice("123", "1", "교육 운영 (정정)", "용역", "서울시", "", "", None, "u")
        out = dedupe_notices([n1, n2])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].notice_title, "교육 운영 (정정)")


class ScoringTests(unittest.TestCase):
    def test_score_range(self):
        soon = datetime.now() + timedelta(days=1)
        later = datetime.now() + timedelta(days=30)
        high = urgency_score(soon, 500_000_000, repeat=True)
        low = urgency_score(later, 1_000_000, repeat=False)
        self.assertGreaterEqual(high, 0)
        self.assertLessEqual(high, 100)
        self.assertGreater(high, low)


class BuildLeadsTests(unittest.TestCase):
    def test_build_top_n(self):
        notices = [
            RawNotice("1", "1", "리더십 교육 운영 용역", "용역", "서울특별시교육청", "", "202612311800", 300000000, "u1"),
            RawNotice("2", "1", "조직문화 진단 컨설팅", "용역", "경기도청", "", "202612011800", None, "u2"),
            RawNotice("3", "1", "도로 보수 공사", "공사", "강원도", "", "", None, "u3"),
        ]
        leads, stats = build_leads(notices, as_of_date=datetime.now(), top_n=1)
        self.assertEqual(len(leads), 1)
        self.assertEqual(stats.total_notices, 3)
        self.assertGreaterEqual(stats.matched_notices, 2)


if __name__ == "__main__":
    unittest.main()
