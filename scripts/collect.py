#!/usr/bin/env python3
"""keywords.json 의 키워드로 Instagram · Threads 게시물을 Apify 에서 수집한다.

    python3 scripts/collect.py --dry-run      # 계획 · 예상 비용만 보기 (요금 0)
    python3 scripts/collect.py                # enabled 키워드 수집
    python3 scripts/collect.py --only 짠테크 가계부
    python3 scripts/collect.py --all          # enabled=false 포함 전체

- 채널 · 정렬 · 키워드 하나당 Apify 실행 1번, 실행마다 maxTotalChargeUsd(요금 상한)를 건다.
- 원본은 data/raw/<날짜>/<채널>_<정렬>__<키워드>.json 에 받은 그대로 저장한다.
- 같은 날짜에 이미 파일이 있으면 다시 받지 않는다 (비용 0).
- 표준 라이브러리만 쓴다. 토큰은 .env 의 APIFY_TOKEN.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
API = "https://api.apify.com/v2"
KST = ZoneInfo("Asia/Seoul")

IG_ACTOR = "apify~instagram-hashtag-scraper"
TH_ACTOR = "watcher.data~search-threads-by-keywords"

# (채널, 정렬, 액터, 입력 만들기)
JOBS = [
    ("instagram", "posts", IG_ACTOR,
     lambda term, n: {"hashtags": [term], "resultsType": "posts", "resultsLimit": n}),
    ("threads", "popular", TH_ACTOR,   # 반응 분석용
     lambda term, n: {"keywords": [term], "maxItemsPerKeyword": n, "sortByRecent": False}),
    ("threads", "recent", TH_ACTOR,    # 추이 분석용
     lambda term, n: {"keywords": [term], "maxItemsPerKeyword": n, "sortByRecent": True}),
]

CAP_MARGIN = 1.1          # 요금 상한 = 예상액 × 1.1
TERMINAL = {"SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"}


# ---------------------------------------------------------------- Apify API
def load_token() -> str:
    token = os.environ.get("APIFY_TOKEN", "")
    env = ROOT / ".env"
    if not token and env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            if line.startswith("APIFY_TOKEN="):
                token = line.split("=", 1)[1].strip().strip("'\"")
    if not token:
        sys.exit(".env 에 APIFY_TOKEN 이 비어 있습니다.")
    return token


class Apify:
    def __init__(self, token: str):
        self.token = token

    def call(self, method: str, path: str, params: dict | None = None, body=None):
        url = API + path + ("?" + urllib.parse.urlencode(params) if params else "")
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers={
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        })
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            try:
                err = json.loads(e.read().decode("utf-8")).get("error", {})
            except Exception:
                err = {}
            raise ApifyError(e.code, err.get("type", ""), err.get("message", str(e))) from None


class ApifyError(Exception):
    def __init__(self, status, type_, message):
        super().__init__(f"[{status} {type_}] {message}")
        self.status, self.type = status, type_


# ---------------------------------------------------------------- 요금
class Pricing:
    """액터의 현재 Pay-per-event 요금표. 계정 등급(tier)에 맞는 단가를 쓴다."""

    def __init__(self, api: Apify, actor: str, tier: str):
        info = api.call("GET", f"/acts/{actor}")["data"]
        p = (info.get("pricingInfos") or [{}])[-1]
        self.minimum = p.get("minimalMaxTotalChargeUsd") or 0
        self.events: dict[str, float] = {}
        self.per_item = 0.0
        self.per_run = 0.0
        for name, ev in (p.get("pricingPerEvent") or {}).get("actorChargeEvents", {}).items():
            price = ev.get("eventPriceUsd")
            if price is None:
                tiers = ev.get("eventTieredPricingUsd") or {}
                price = (tiers.get(tier) or tiers.get("FREE") or {}).get("tieredEventPriceUsd", 0)
            self.events[name] = price
            if ev.get("isPrimaryEvent"):
                self.per_item = price
            elif ev.get("isOneTimeEvent"):
                self.per_run += price

    def estimate(self, n: int) -> float:
        return self.per_run + self.per_item * n

    def cap(self, n: int) -> float:
        return max(self.minimum, math.ceil(self.estimate(n) * CAP_MARGIN * 1000) / 1000)

    def charged(self, counts: dict | None) -> float:
        return sum(self.events.get(k, 0) * v for k, v in (counts or {}).items())


# ---------------------------------------------------------------- 실행
def run_actor(api: Apify, actor: str, inp: dict, cap: float, limit: int) -> dict:
    params = {"maxTotalChargeUsd": cap, "maxItems": limit, "waitForFinish": 60}
    for attempt in range(20):
        try:
            run = api.call("POST", f"/acts/{actor}/runs", params, inp)["data"]
            break
        except ApifyError as e:
            if e.type == "concurrent-runs-limit-exceeded":   # 무료 플랜 동시 실행 5개
                print("    동시 실행 한도 — 20초 뒤 재시도", flush=True)
                time.sleep(20)
                continue
            raise
    else:
        raise RuntimeError("동시 실행 한도가 계속 차 있어 시작하지 못했습니다.")
    while run["status"] not in TERMINAL:
        run = api.call("GET", f"/actor-runs/{run['id']}", {"waitForFinish": 60})["data"]
    return run


def fetch_items(api: Apify, run_id: str) -> list:
    items, offset = [], 0
    while True:
        url = f"{API}/actor-runs/{run_id}/dataset/items?" + urllib.parse.urlencode(
            {"format": "json", "offset": offset, "limit": 1000})
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {api.token}"})
        with urllib.request.urlopen(req, timeout=120) as r:
            page = json.loads(r.read().decode("utf-8"))
        items += page
        if len(page) < 1000:
            return items
        offset += 1000


def slug(s: str) -> str:
    return re.sub(r"[^\w\-]+", "_", s).strip("_")


# ---------------------------------------------------------------- main
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=30, help="키워드·정렬당 최대 건수 (기본 30)")
    ap.add_argument("--date", default=datetime.now(KST).strftime("%Y-%m-%d"), help="저장 폴더 날짜 (기본 오늘, KST)")
    ap.add_argument("--only", nargs="+", metavar="KEYWORD", help="이 키워드만")
    ap.add_argument("--all", action="store_true", help="enabled=false 키워드도 포함")
    ap.add_argument("--channels", nargs="+", choices=["instagram", "threads"], default=["instagram", "threads"])
    ap.add_argument("--sorts", nargs="+", choices=["posts", "popular", "recent"], default=["posts", "popular", "recent"],
                    help="받을 정렬 (instagram=posts, threads=popular·recent)")
    ap.add_argument("--budget", type=float, default=3.0, help="이번 실행 전체 요금 한도 USD (기본 3.0)")
    ap.add_argument("--dry-run", action="store_true", help="수집하지 않고 계획 · 예상 비용만 출력")
    args = ap.parse_args()

    cfg = json.loads((ROOT / "keywords.json").read_text(encoding="utf-8"))
    kws = [k for k in cfg["keywords"] if args.all or args.only or k.get("enabled")]
    if args.only:
        kws = [k for k in kws if k["keyword"] in args.only]
        missing = set(args.only) - {k["keyword"] for k in kws}
        if missing:
            sys.exit(f"keywords.json 에 없는 키워드: {', '.join(sorted(missing))}")
    if not kws:
        sys.exit("수집할 키워드가 없습니다.")

    api = Apify(load_token())
    me = api.call("GET", "/users/me")["data"]
    tier = (me.get("plan") or {}).get("tier") or "FREE"
    prices = {a: Pricing(api, a, tier) for a in {j[2] for j in JOBS}}

    out_dir = ROOT / "data" / "raw" / args.date
    log_path = out_dir / "_runs.jsonl"

    # 계획 세우기
    plan = []   # (kw, channel, sort, actor, build, path)
    for kw in kws:
        for channel, sort, actor, build in JOBS:
            if channel not in args.channels or sort not in args.sorts or not kw.get(channel):
                continue
            path = out_dir / f"{channel}_{sort}__{slug(kw['keyword'])}.json"
            plan.append((kw, channel, sort, actor, build, path))
    todo = [p for p in plan if not p[5].exists()]
    est = sum(prices[p[3]].estimate(args.limit) for p in todo)

    limits = api.call("GET", "/users/me/limits")["data"]
    used = (limits.get("current") or {}).get("monthlyUsageUsd") or 0
    cap_month = (limits.get("limits") or {}).get("maxMonthlyUsageUsd")

    print(f"날짜 {args.date} · 키워드 {len(kws)}개 · 실행 {len(plan)}개 중 새로 받을 것 {len(todo)}개 "
          f"(이미 받음 {len(plan) - len(todo)}개)")
    print(f"예상 비용 ${est:.3f} · 이번 실행 한도 ${args.budget:.2f} · "
          f"이번 달 사용 ${used:.2f}" + (f" / ${cap_month:.2f}" if cap_month else ""))
    if args.dry_run:
        for kw, channel, sort, actor, build, path in todo:
            print(f"  - {channel:9} {sort:7} {kw['keyword']:<10} 상한 ${prices[actor].cap(args.limit):.3f}")
        return
    if cap_month and used + est > cap_month:
        sys.exit(f"이번 달 한도(${cap_month:.2f})를 넘을 수 있어 중단합니다. --only 로 줄이거나 --limit 을 낮추세요.")

    out_dir.mkdir(parents=True, exist_ok=True)
    spent = 0.0
    results = []   # (channel, sort, keyword, count, cost, state)
    for kw, channel, sort, actor, build, path in plan:
        label = f"{channel}/{sort} · {kw['keyword']}"
        if path.exists():
            n = len(json.loads(path.read_text(encoding="utf-8")))
            results.append((channel, sort, kw["keyword"], n, 0.0, "이미 받음"))
            continue
        price = prices[actor]
        cap = price.cap(args.limit)
        if spent + cap > args.budget:
            print(f"  ! {label}: 이번 실행 한도 ${args.budget:.2f} 초과 예정 — 건너뜀")
            results.append((channel, sort, kw["keyword"], 0, 0.0, "한도로 건너뜀"))
            continue

        inp = build(kw[channel], args.limit)
        print(f"  → {label} (상한 ${cap:.3f}) ...", end=" ", flush=True)
        try:
            run = run_actor(api, actor, inp, cap, args.limit)
            items = fetch_items(api, run["id"])
        except Exception as e:  # 한 건 실패해도 나머지는 계속
            print(f"실패: {e}")
            results.append((channel, sort, kw["keyword"], 0, 0.0, "실패"))
            continue

        cost = price.charged(run.get("chargedEventCounts"))
        spent += cost
        state = "새로 받음" if run["status"] == "SUCCEEDED" else run["status"]
        if items:   # 빈 결과나 실패는 저장하지 않아 다음에 다시 시도
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(items, ensure_ascii=False, indent=1), encoding="utf-8")
            tmp.replace(path)
        elif state == "새로 받음":
            state = "결과 없음"
        with log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps({
                "at": datetime.now(KST).isoformat(timespec="seconds"), "keyword": kw["keyword"],
                "channel": channel, "sort": sort, "actor": actor, "input": inp, "run_id": run["id"],
                "status": run["status"], "items": len(items), "cap_usd": cap, "cost_usd": round(cost, 4),
                "charged_events": run.get("chargedEventCounts"),
            }, ensure_ascii=False) + "\n")
        print(f"{len(items)}건 · ${cost:.3f} ({run['status']})")
        results.append((channel, sort, kw["keyword"], len(items), cost, state))

    # 요약
    print("\n채널/정렬        키워드            건수    비용     상태")
    print("-" * 62)
    for channel, sort, keyword, n, cost, state in results:
        print(f"{channel + '/' + sort:<16} {keyword:<14} {n:>6} {cost:>8.3f}   {state}")
    print("-" * 62)
    by_ch: dict[str, int] = {}
    for channel, sort, _, n, _, _ in results:
        by_ch[f"{channel}/{sort}"] = by_ch.get(f"{channel}/{sort}", 0) + n
    print("  ".join(f"{k} {v}건" for k, v in by_ch.items()))
    limits = api.call("GET", "/users/me/limits")["data"]
    used_after = (limits.get("current") or {}).get("monthlyUsageUsd") or 0
    print(f"이번 실행 청구액 ${spent:.3f} · 이번 달 누적 ${used_after:.2f}"
          + (f" / ${cap_month:.2f}" if cap_month else "") + " (Apify 집계는 몇 분 늦을 수 있음)")
    print(f"저장 위치: {out_dir.relative_to(ROOT)}/")


if __name__ == "__main__":
    main()
