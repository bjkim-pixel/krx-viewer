"""
fetch_kis_data.py  ─  KRX Viewer 수급/신고가 수집
────────────────────────────────────────────────────
Market Radar와 동일한 KIS API 호출 패턴 사용.

실행 방법:
  GitHub Actions → 수동 Run workflow
  또는 로컬: KIS_APP_KEY=xxx KIS_APP_SECRET=xxx python scripts/fetch_kis_data.py

입력:  data/codes.json  (엑셀에서 추출한 종목코드 목록)
출력:  data/enriched.json
"""

import os, json, math, time, requests
from datetime import datetime, timedelta
from pathlib import Path

APP_KEY    = os.environ["KIS_APP_KEY"]
APP_SECRET = os.environ["KIS_APP_SECRET"]
BASE_URL   = "https://openapi.koreainvestment.com:9443"
DATA_DIR   = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

DELAY = 0.08   # Market Radar와 동일


# ── 인증 ─────────────────────────────────────────────────────────
def get_token():
    r = requests.post(f"{BASE_URL}/oauth2/tokenP", json={
        "grant_type": "client_credentials",
        "appkey":     APP_KEY,
        "appsecret":  APP_SECRET,
    }, timeout=10)
    token = r.json()["access_token"]
    print("[token] issued")
    return token


# ── KIS GET 공통 (Market Radar 패턴 동일) ────────────────────────
def kis_get(path, params, tr_id, token, retry=2):
    headers = {
        "Content-Type":  "application/json",
        "authorization": f"Bearer {token}",
        "appkey":        APP_KEY,
        "appsecret":     APP_SECRET,
        "tr_id":         tr_id,
        "custtype":      "P",          # ← Market Radar에서 작동 확인된 필드
    }
    for attempt in range(retry):
        try:
            r = requests.get(f"{BASE_URL}{path}", headers=headers,
                             params=params, timeout=15)
            if not r.text or not r.text.strip():
                return {}
            d = r.json()
            if d.get("rt_cd") == "0":
                return d
            code = d.get("msg_cd", "")
            if code in ("EGW00201", "EGW00202"):   # rate limit
                time.sleep(0.4)
                continue
            print(f"  API error [{tr_id}]: {d.get('msg1','')}")
            return {}
        except Exception as e:
            print(f"  request failed [{tr_id}]: {e}")
            time.sleep(0.5)
    return {}


def safe_float(v):
    try:    return float(str(v).replace(",", ""))
    except: return 0.0

def safe_int(v):
    try:    return int(float(str(v).replace(",", "")))
    except: return 0

def clean_nan(obj):
    if isinstance(obj, dict):  return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):  return [clean_nan(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)): return 0
    return obj


# ── 1. 신고가 여부 (Market Radar: fetch_stock_price 참조) ─────────
def fetch_price_info(token, code):
    d = kis_get(
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
        "FHKST01010100", token
    )
    o = d.get("output", {})
    if not o:
        return {}

    price   = safe_int(o.get("stck_prpr", 0))
    high52  = safe_int(o.get("d250_hgpr", 0))   # 52주 고가 (Market Radar와 동일)
    low52   = safe_int(o.get("d250_lwpr", 0))
    nh_ratio = round(price / high52 * 100, 1) if high52 else 0

    nh_flag = ""
    if nh_ratio >= 100:  nh_flag = "신고가"
    elif nh_ratio >= 99: nh_flag = "99%"
    elif nh_ratio >= 97: nh_flag = "97%+"

    return {
        "is52h":    nh_ratio >= 100,
        "isAllH":   o.get("new_hgpr_lwpr_cls_code") == "1",
        "nh_ratio": nh_ratio,
        "nh_flag":  nh_flag,
        "dept":     o.get("bstp_kor_isnm", "").strip(),
    }


# ── 2. 투자자 매매동향 (Market Radar: fetch_stock_investor_history 동일) ──
def fetch_investor(token, code):
    d = kis_get(
        "/uapi/domestic-stock/v1/quotations/inquire-investor",
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
        "FHKST01010900", token
    )
    rows = d.get("output", [])
    if not rows:
        return None

    history = []
    for r in rows:
        history.append({
            "date":  r.get("stck_bsop_date", ""),
            # 수량 (연속매수 판정용)
            "frgn_qty": safe_int(r.get("frgn_ntby_qty", 0)),
            "inst_qty": safe_int(r.get("orgn_ntby_qty", 0)),
            # 거래대금 (Market Radar: famt = frgn_ntby_tr_pbmn * 1_000_000)
            "frgn": safe_float(r.get("frgn_ntby_tr_pbmn", 0)) * 1_000_000,
            "inst": safe_float(r.get("orgn_ntby_tr_pbmn", 0)) * 1_000_000,
            "priv": safe_float(r.get("pvt_fund_ntby_tr_pbmn", 0)) * 1_000_000,
            "indiv": safe_float(r.get("prsn_ntby_tr_pbmn", 0)) * 1_000_000,
            "close": 0,    # 아래서 ohlcv와 병합
            "rate":  0.0,
            "volume": 0,
        })

    # 연속 매수일 계산 (Market Radar 방식)
    f_consec = 0
    for row in history:
        if row["frgn_qty"] > 0: f_consec += 1
        else: break

    i_consec = 0
    for row in history:
        if row["inst_qty"] > 0: i_consec += 1
        else: break

    # 기간별 합산
    def sum_period(n, key):
        return sum(r.get(key, 0) for r in history[:n])

    today = history[0] if history else {}
    return {
        "today": {
            "frgn": today.get("frgn", 0),
            "inst": today.get("inst", 0),
            "priv": today.get("priv", 0),
        },
        "week": {
            "frgn": sum_period(5,  "frgn"),
            "inst": sum_period(5,  "inst"),
            "priv": sum_period(5,  "priv"),
        },
        "month": {
            "frgn": sum_period(22, "frgn"),
            "inst": sum_period(22, "inst"),
            "priv": sum_period(22, "priv"),
        },
        "f_consec": f_consec,
        "i_consec": i_consec,
        "daily":    history[:15],   # 15일치 daily
    }


# ── 3. 일별 종가+거래량 (Market Radar: fetch_stock_ohlcv 동일) ────
def fetch_ohlcv(token, code):
    kst_now  = datetime.utcnow() + timedelta(hours=9)
    end_dt   = kst_now.strftime("%Y%m%d")
    start_dt = (kst_now - timedelta(days=40)).strftime("%Y%m%d")

    d = kis_get(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD":         code,
            "FID_INPUT_DATE_1":       start_dt,
            "FID_INPUT_DATE_2":       end_dt,
            "FID_PERIOD_DIV_CODE":    "D",
            "FID_ORG_ADJ_PRC":        "1",
        },
        "FHKST03010100", token
    )
    result = {}
    for r in (d.get("output2") or []):
        dt = r.get("stck_bsop_date", "")
        cl = safe_int(r.get("stck_clpr", 0))
        vl = safe_int(r.get("acml_vol", 0))
        rt = safe_float(r.get("prdy_ctrt", 0))
        if dt and cl:
            result[dt] = {"close": cl, "volume": vl, "rate": rt}
    return result


# ── 메인 ─────────────────────────────────────────────────────────
def main():
    codes_path = DATA_DIR / "codes.json"
    if not codes_path.exists():
        print("⚠️  data/codes.json 없음")
        print("   → 로컬에서 실행: python scripts/make_codes.py 엑셀파일.xlsx")
        print("   → 생성된 codes.json을 git push 후 Actions 재실행")
        sys.exit(0)   # exit 0: Actions 실패로 표시 안 함

    with open(codes_path, encoding="utf-8") as f:
        info = json.load(f)

    codes = info.get("codes", [])
    date  = info.get("date", (datetime.utcnow() + timedelta(hours=9)).strftime("%Y%m%d"))
    total = len(codes)

    print(f"\n{'='*55}")
    print(f"  KRX Viewer 수급 수집  [{date}]  {total}개 종목")
    print(f"{'='*55}\n")

    if not codes:
        print("⚠️ 종목 없음")
        return

    token  = get_token()
    result = {}

    for idx, code in enumerate(codes, 1):
        try:
            print(f"[{idx:4d}/{total}] {code}")

            # 신고가
            price_info = fetch_price_info(token, code)
            time.sleep(DELAY)

            # 투자자 수급
            investor = fetch_investor(token, code)
            time.sleep(DELAY)

            # 일별 종가/거래량
            ohlcv = fetch_ohlcv(token, code)
            time.sleep(DELAY)

            # investor daily에 종가/거래량 병합
            if investor and investor.get("daily"):
                for row in investor["daily"]:
                    p = ohlcv.get(row.get("date", ""), {})
                    row["close"]  = p.get("close",  0)
                    row["rate"]   = p.get("rate",   0.0)
                    row["volume"] = p.get("volume", 0)

            # 22일 평균 거래량 계산
            vol_avg_22, vol_ratio = 0, 0.0
            sorted_dates = sorted(ohlcv.keys(), reverse=True)  # 최신순
            if len(sorted_dates) >= 2:
                past_vols = [ohlcv[dt]["volume"] for dt in sorted_dates[1:23] if ohlcv[dt]["volume"] > 0]
                if past_vols:
                    vol_avg_22 = int(sum(past_vols) / len(past_vols))
                    today_vol  = ohlcv.get(sorted_dates[0], {}).get("volume", 0)
                    vol_ratio  = round(today_vol / vol_avg_22 * 100, 1) if vol_avg_22 else 0.0

            result[code] = clean_nan({
                # 신고가
                "is52h":    price_info.get("is52h",  False),
                "isAllH":   price_info.get("isAllH", False),
                "nh_flag":  price_info.get("nh_flag", ""),
                "nh_ratio": price_info.get("nh_ratio", 0),
                "dept":     price_info.get("dept", ""),
                # 수급 요약
                "frgn":  investor["today"]["frgn"] if investor else None,
                "inst":  investor["today"]["inst"] if investor else None,
                "priv":  investor["today"]["priv"] if investor else None,
                "f_consec": investor.get("f_consec", 0) if investor else 0,
                "i_consec": investor.get("i_consec", 0) if investor else 0,
                # 거래량
                "volAvg22": vol_avg_22,
                "volRatio": vol_ratio,
                # 전체 수급 (detail 페이지용)
                "inv": investor,
            })

        except Exception as e:
            print(f"  ❌ [{code}] {e}")
            result[code] = {}

    # 저장
    out = {
        "date":      date,
        "generated": (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M KST"),
        "total":     total,
        "data":      result,
    }
    with open(DATA_DIR / "enriched.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))

    n52h   = sum(1 for v in result.values() if v.get("is52h"))
    nAllH  = sum(1 for v in result.values() if v.get("isAllH"))
    nFrgn  = sum(1 for v in result.values() if (v.get("frgn") or 0) > 0)

    print(f"\n{'='*55}")
    print(f"✅ enriched.json 저장 완료")
    print(f"   총 {total}개 | 52주신고가 {n52h}개 | 역대신고가 {nAllH}개")
    print(f"   외국인 순매수 {nFrgn}개")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
