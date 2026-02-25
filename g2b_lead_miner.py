from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote


BASE_URL_STD = "http://apis.data.go.kr/1230000/ao/PubDataOpnStdService"
ENDPOINT_STD_BID = "/getDataSetOpnStdBidPblancInfo"

MAX_RETRIES = 4
TIMEOUT_SEC = 20

INCLUDE_PATTERNS = [
    r"HRD",
    r"교육",
    r"연수",
    r"훈련",
    r"강사",
    r"워크숍",
    r"세미나",
    r"코칭",
    r"리더십",
    r"직무역량",
    r"역량",
    r"조직진단",
    r"조직문화",
    r"\bOD\b",
    r"성과평가",
    r"평가제도",
    r"인사제도",
    r"컨설팅",
]

EXCLUDE_PATTERNS = [
    r"시설",
    r"소방",
    r"전기",
    r"청소",
    r"급식",
    r"안전점검",
    r"조경",
    r"도로",
]

INCLUDE_RE = re.compile("|".join(INCLUDE_PATTERNS), re.IGNORECASE)
EXCLUDE_RE = re.compile("|".join(EXCLUDE_PATTERNS), re.IGNORECASE)


@dataclass
class RawNotice:
    bid_ntce_no: str
    bid_ntce_ord: str
    notice_title: str
    work_type: str
    agency_name: str
    notice_date: str
    deadline_dt: str
    budget_amt: Optional[float]
    source_url: str


@dataclass
class LeadRecord:
    as_of_date: str
    bid_ntce_no: str
    bid_ntce_ord: str
    notice_title: str
    work_type: str
    agency_name: str
    region_guess: str
    deadline_dt: str
    budget_amt: str
    category: str
    match_strength: float
    urgency_score: int
    source_url: str
    contact_policy: str
    notes: str


@dataclass
class SummaryStats:
    total_notices: int
    matched_notices: int
    by_category: Dict[str, int]
    by_work_type: Dict[str, int]


def parse_datetime_guess(value: str) -> Optional[datetime]:
    if not value:
        return None
    candidates = ["%Y-%m-%d %H:%M", "%Y%m%d%H%M", "%Y-%m-%d", "%Y%m%d"]
    for fmt in candidates:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def normalize_bid_key(item: Dict[str, Any]) -> Tuple[str, str]:
    bid_no = str(item.get("bidNtceNo") or item.get("bid_ntce_no") or "")
    bid_ord = str(item.get("bidNtceOrd") or item.get("bid_ntce_ord") or "0")
    return bid_no.strip(), bid_ord.strip()


def detect_region(agency_name: str) -> str:
    mapping = {
        "서울": "서울",
        "부산": "부산",
        "대구": "대구",
        "인천": "인천",
        "광주": "광주",
        "대전": "대전",
        "울산": "울산",
        "세종": "세종",
        "경기": "경기",
        "강원": "강원",
        "충북": "충북",
        "충남": "충남",
        "전북": "전북",
        "전남": "전남",
        "경북": "경북",
        "경남": "경남",
        "제주": "제주",
    }
    for needle, region in mapping.items():
        if needle in agency_name:
            return region
    return "미지정"


def match_category(title: str) -> Tuple[str, float, List[str]]:
    txt = (title or "").strip()
    if not txt:
        return "미지정", 0.0, []

    include_hits = [pat for pat in INCLUDE_PATTERNS if re.search(pat, txt, re.IGNORECASE)]
    exclude_hits = [pat for pat in EXCLUDE_PATTERNS if re.search(pat, txt, re.IGNORECASE)]

    if not include_hits:
        return "비대상", 0.0, []
    if exclude_hits and len(include_hits) <= len(exclude_hits):
        return "보류(노이즈 가능)", 0.45, include_hits

    if re.search(r"조직진단|조직문화|\bOD\b|변화관리", txt, re.IGNORECASE):
        return "OD/조직", 0.84, include_hits
    if re.search(r"평가|성과|역량모델|직무체계|인사제도", txt, re.IGNORECASE):
        return "평가/제도", 0.78, include_hits
    return "HRD/교육", 0.72, include_hits


def urgency_score(
    deadline_dt: Optional[datetime],
    budget_amt: Optional[float],
    repeat: bool,
    agency_weight: float = 0.6,
    fit_weight: float = 0.8,
) -> int:
    if deadline_dt is None:
        deadline_score = 0.5
    else:
        days_left = max((deadline_dt - datetime.now()).days, 0)
        deadline_score = 1.0 / (1.0 + (days_left / 7.0))

    if budget_amt and budget_amt > 0:
        budget_score = min(1.0, math.log10(budget_amt + 1.0) / 9.0)
    else:
        budget_score = 0.5

    repeat_score = 1.0 if repeat else 0.0

    score = 100.0 * (
        0.35 * deadline_score
        + 0.25 * budget_score
        + 0.15 * agency_weight
        + 0.15 * repeat_score
        + 0.10 * fit_weight
    )
    return int(round(max(0.0, min(100.0, score))))


def request_json(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            query = urllib.parse.urlencode(params)
            request = urllib.request.Request(f"{url}?{query}", method="GET")
            with urllib.request.urlopen(request, timeout=TIMEOUT_SEC) as response:
                payload = response.read().decode("utf-8")
                return json.loads(payload)
        except urllib.error.HTTPError as exc:
            if exc.code in (401, 403):
                raise RuntimeError(f"인증 오류({exc.code}) - 서비스키/권한 확인 필요") from exc
            if exc.code == 429 or exc.code >= 500:
                last_error = exc
            else:
                raise
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        if attempt == MAX_RETRIES:
            break
        time.sleep(min(2**attempt, 8))
    raise RuntimeError(f"API 호출 실패: {last_error}")


def iter_windows(start: datetime, end: datetime, mode: str) -> Iterable[Tuple[datetime, datetime]]:
    cursor = start
    while cursor <= end:
        if mode == "monthly":
            if cursor.month == 12:
                nxt = datetime(cursor.year + 1, 1, 1)
            else:
                nxt = datetime(cursor.year, cursor.month + 1, 1)
            window_end = min(end, nxt - timedelta(minutes=1))
        else:
            nxt = cursor + timedelta(days=1)
            window_end = min(end, nxt - timedelta(minutes=1))
        yield cursor, window_end
        cursor = nxt


def fetch_notices_std(service_key: str, start: datetime, end: datetime, mode: str = "daily") -> List[RawNotice]:
    notices: List[RawNotice] = []

    for window_start, window_end in iter_windows(start, end, mode=mode):
        page_no = 1
        while True:
            params = {
                "serviceKey": service_key,
                "pageNo": page_no,
                "numOfRows": 100,
                "type": "json",
                "inqryBgnDt": window_start.strftime("%Y%m%d%H%M"),
                "inqryEndDt": window_end.strftime("%Y%m%d%H%M"),
            }
            data = request_json(f"{BASE_URL_STD}{ENDPOINT_STD_BID}", params=params)
            body = data.get("response", {}).get("body", {})
            items = body.get("items", [])
            if isinstance(items, dict):
                items = items.get("item", [])
            if not items:
                break

            for item in items:
                bid_no, bid_ord = normalize_bid_key(item)
                title = str(item.get("bidNtceNm") or item.get("bid_ntce_nm") or "")
                notices.append(
                    RawNotice(
                        bid_ntce_no=bid_no or "미지정",
                        bid_ntce_ord=bid_ord or "0",
                        notice_title=title or "미지정",
                        work_type=str(item.get("bsnsDivNm") or item.get("workType") or "미지정"),
                        agency_name=str(item.get("dminsttNm") or item.get("ntceInsttNm") or "미지정"),
                        notice_date=str(item.get("bidNtceDt") or ""),
                        deadline_dt=str(item.get("bidClseDt") or ""),
                        budget_amt=_parse_budget(item),
                        source_url=build_notice_url(bid_no, bid_ord),
                    )
                )

            total_count = int(body.get("totalCount") or 0)
            if page_no * 100 >= total_count:
                break
            page_no += 1

    return dedupe_notices(notices)


def _parse_budget(item: Dict[str, Any]) -> Optional[float]:
    for key in ("asignBdgtAmt", "presmptPrce", "budgetAmt"):
        raw = item.get(key)
        if raw in (None, "", "-"):
            continue
        try:
            return float(str(raw).replace(",", ""))
        except ValueError:
            continue
    return None


def build_notice_url(bid_no: str, bid_ord: str) -> str:
    if not bid_no:
        return "미지정"
    query = f"bidNtceNo={quote(bid_no)}&bidNtceOrd={quote(bid_ord or '0')}"
    return f"https://www.g2b.go.kr:8101/ep/invitation/publish/bidInfoDtl.do?{query}"


def dedupe_notices(notices: List[RawNotice]) -> List[RawNotice]:
    deduped: Dict[Tuple[str, str], RawNotice] = {}
    for notice in notices:
        deduped[(notice.bid_ntce_no, notice.bid_ntce_ord)] = notice
    return list(deduped.values())


def build_leads(notices: List[RawNotice], as_of_date: datetime, top_n: int = 50) -> Tuple[List[LeadRecord], SummaryStats]:
    repeated_keys = _find_repeated_titles(notices)
    leads: List[LeadRecord] = []
    by_category: Dict[str, int] = {}
    by_work_type: Dict[str, int] = {}

    for notice in notices:
        category, strength, _hits = match_category(notice.notice_title)
        by_work_type[notice.work_type] = by_work_type.get(notice.work_type, 0) + 1
        if category in ("비대상", "보류(노이즈 가능)", "미지정"):
            continue

        deadline = parse_datetime_guess(notice.deadline_dt)
        repeat = (notice.agency_name, _normalize_title(notice.notice_title)) in repeated_keys
        score = urgency_score(deadline, notice.budget_amt, repeat=repeat)

        by_category[category] = by_category.get(category, 0) + 1
        leads.append(
            LeadRecord(
                as_of_date=as_of_date.strftime("%Y-%m-%d"),
                bid_ntce_no=notice.bid_ntce_no,
                bid_ntce_ord=notice.bid_ntce_ord,
                notice_title=notice.notice_title,
                work_type=notice.work_type,
                agency_name=notice.agency_name,
                region_guess=detect_region(notice.agency_name),
                deadline_dt=notice.deadline_dt or "미지정",
                budget_amt=(f"{int(notice.budget_amt):,}" if notice.budget_amt else "미지정"),
                category=category,
                match_strength=round(strength, 2),
                urgency_score=score,
                source_url=notice.source_url,
                contact_policy="개인정보 최소수집/수신거부 및 야간발송 제한 준수",
                notes="자동 산출 리드. 원문 공고에서 세부 요구사항 재확인 필요",
            )
        )

    leads.sort(key=lambda x: x.urgency_score, reverse=True)
    selected = leads[:top_n]
    stats = SummaryStats(
        total_notices=len(notices),
        matched_notices=len(leads),
        by_category=by_category,
        by_work_type=by_work_type,
    )
    return selected, stats


def _normalize_title(title: str) -> str:
    normalized = re.sub(r"20\d{2}", "", title)
    normalized = re.sub(r"\d+차|\d+회", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip().lower()


def _find_repeated_titles(notices: List[RawNotice]) -> set[Tuple[str, str]]:
    counts: Dict[Tuple[str, str], int] = {}
    for n in notices:
        key = (n.agency_name, _normalize_title(n.notice_title))
        counts[key] = counts.get(key, 0) + 1
    return {key for key, count in counts.items() if count >= 2}


def export_csv(path: Path, records: List[LeadRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        with path.open("w", encoding="utf-8") as f:
            f.write("")
        return

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(records[0]).keys()))
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))


def export_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run_pipeline(
    service_key: str,
    start: datetime,
    end: datetime,
    mode: str,
    out_dir: Path,
    top_n: int,
) -> Dict[str, Any]:
    notices = fetch_notices_std(service_key=service_key, start=start, end=end, mode=mode)
    leads, stats = build_leads(notices, as_of_date=datetime.now(), top_n=top_n)

    lead_csv = out_dir / "lead_records.csv"
    stats_json = out_dir / "summary_stats.json"
    raw_json = out_dir / "raw_notices.json"

    export_csv(lead_csv, leads)
    export_json(stats_json, asdict(stats))
    export_json(raw_json, {"count": len(notices), "items": [asdict(n) for n in notices]})

    return {
        "lead_csv": str(lead_csv),
        "stats_json": str(stats_json),
        "raw_json": str(raw_json),
        "total_notices": stats.total_notices,
        "matched_notices": stats.matched_notices,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="나라장터 HRD/OD 리드 마이너 MVP")
    parser.add_argument("--start", required=False, help="조회 시작일시 (YYYY-MM-DD)")
    parser.add_argument("--end", required=False, help="조회 종료일시 (YYYY-MM-DD)")
    parser.add_argument("--mode", choices=["daily", "monthly"], default="daily")
    parser.add_argument("--out-dir", default="output")
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--service-key", default=os.getenv("DATA_GO_KR_SERVICE_KEY", ""))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.service_key:
        raise SystemExit("DATA_GO_KR_SERVICE_KEY(또는 --service-key)가 필요합니다.")

    end = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()
    start = datetime.strptime(args.start, "%Y-%m-%d") if args.start else (end - timedelta(days=7))

    if start > end:
        raise SystemExit("start는 end보다 이전이어야 합니다.")

    result = run_pipeline(
        service_key=args.service_key,
        start=start,
        end=end,
        mode=args.mode,
        out_dir=Path(args.out_dir),
        top_n=args.top_n,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
