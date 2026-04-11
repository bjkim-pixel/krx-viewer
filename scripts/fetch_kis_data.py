"""
fetch_kis_data.py
─────────────────────────────────────────────
흐름:
  1. KIS 국내주식 전체 시세 순위 API (FHPST01710000)로 
     KOSPI/KOSDAQ 상승 종목 수집
     → API 제한으로 최대 100건 → 100건 × 2 = 최대 200종목
     
     부족하면 보완용으로 등락률 상위 조회를 여러 구간으로 나눠서 수집
     
  2. 각 종목별:
     - 투자자 매매동향 (외국인/기관/사모펀드)
     - 일별시세 22일치 (거래량 평균)
     - 52주/역대 신고가 여부
     
  3. P = 등락률 × 거래량 / 상장주식수  (API 실수신 데이터)
     Q = 거래대금 / 시가총액
     
출력:  data/today.json, data/meta.json
"""

import os, sys, json, time, requests
from datetime import datetime, timedelta
from pathlib import Path

# ── 설정 ──────────────────────────────────────────────────────────
KIS_BASE         = 'https://openapi.koreainvestment.com:9443'
APP_KEY          = os.environ.get('KIS_APP_KEY', '')
APP_SECRET       = os.environ.get('KIS_APP_SECRET', '')
DATA_DIR         = Path(__file__).parent.parent / 'data'
DATA_DIR.mkdir(exist_ok=True)

TARGET_DATE      = os.environ.get('TARGET_DATE', '').strip()
TODAY            = TARGET_DATE if TARGET_DATE else datetime.now().strftime('%Y%m%d')
RATE_LIMIT_DELAY = 0.13   # 초당 ~7.5 req (KIS 무료 한도 20 req/s)


# ── 인증 ──────────────────────────────────────────────────────────
def get_token():
    if not APP_KEY or not APP_SECRET:
        print("❌ KIS_APP_KEY / KIS_APP_SECRET 환경변수가 없습니다.")
        sys.exit(1)
    res = requests.post(
        f'{KIS_BASE}/oauth2/tokenP',
        headers={'Content-Type': 'application/json'},
        json={'grant_type': 'client_credentials', 'appkey': APP_KEY, 'appsecret': APP_SECRET},
        timeout=10
    )
    data = res.json()
    if 'access_token' not in data:
        print(f"❌ 토큰 발급 실패: {data}")
        sys.exit(1)
    print("✅ KIS 토큰 발급 성공")
    return data['access_token']


def kis_get(token, tr_id, url_path, params):
    """KIS REST GET 공통 호출"""
    return requests.get(
        f'{KIS_BASE}{url_path}',
        params=params,
        headers={
            'authorization': f'Bearer {token}',
            'appkey':        APP_KEY,
            'appsecret':     APP_SECRET,
            'tr_id':         tr_id,
            'Content-Type':  'application/json',
        },
        timeout=10
    ).json()


# ── 1단계: 당일 상승 종목 수집 ────────────────────────────────────
def fetch_rising_stocks(token):
    """
    등락률순위 API (FHPST01710000)를 등락률 구간별로 여러 번 호출해서
    KOSPI + KOSDAQ 상승 종목 전체를 최대한 수집한다.
    
    구간: 0%~5%, 5%~10%, 10%~15%, 15%~20%, 20%~30%, 30%+
    각 구간 × KOSPI/KOSDAQ = 최대 12회 호출
    """
    markets  = [('0001', 'KOSPI'), ('1001', 'KOSDAQ')]
    # 등락률 구간 (rate1, rate2) — 빈 문자열이면 상한 없음
    ranges   = [('0','5'), ('5','10'), ('10','15'), ('15','20'), ('20','30'), ('30','')]
    seen     = set()
    all_stocks = []

    def to_i(v): return int(str(v or '0').replace(',','') or 0)
    def to_f(v): return float(str(v or '0').replace(',','') or 0)

    for mkt_code, mkt_name in markets:
        mkt_count = 0
        for rate1, rate2 in ranges:
            try:
                j = kis_get(token, 'FHPST01710000',
                    '/uapi/domestic-stock/v1/ranking/fluctuation',
                    {
                        'fid_cond_mrkt_div_code': 'J',
                        'fid_cond_scr_div_code':  '20171',
                        'fid_input_iscd':         mkt_code,
                        'fid_rank_sort_cls_code': '0',   # 상승률 높은순
                        'fid_input_cnt_1':        '0',
                        'fid_prc_cls_code':       '1',
                        'fid_input_price_1':      '',
                        'fid_input_price_2':      '',
                        'fid_vol_cnt':            '',
                        'fid_trgt_cls_code':      '0',
                        'fid_trgt_exls_cls_code': '0',
                        'fid_div_cls_code':       '0',
                        'fid_rsfl_rate1':         rate1,
                        'fid_rsfl_rate2':         rate2,
                    }
                )
                if j.get('rt_cd') != '0':
                    time.sleep(RATE_LIMIT_DELAY)
                    continue

                for row in (j.get('output') or []):
                    code = str(row.get('stck_shrn_iscd', '')).zfill(6)
                    rate = to_f(row.get('prdy_ctrt', 0))
                    if rate <= 0 or code in seen:
                        continue
                    seen.add(code)
                    cap_eok = to_i(row.get('hts_avls', 0))
                    all_stocks.append({
                        'code'  : code,
                        'name'  : str(row.get('hts_kor_isnm', '')),
                        'market': mkt_name,
                        'close' : to_i(row.get('stck_prpr', 0)),
                        'diff'  : to_i(row.get('prdy_vrss', 0)),
                        'rate'  : round(rate, 2),
                        'volume': to_i(row.get('acml_vol', 0)),
                        'amount': to_i(row.get('acml_tr_pbmn', 0)),
                        'cap'   : cap_eok * 100_000_000,
                        'shares': to_i(row.get('lstn_stcn', 0)),
                    })
                    mkt_count += 1

            except Exception as e:
                print(f"  ⚠️ {mkt_name} {rate1}~{rate2}% 오류: {e}")
            time.sleep(RATE_LIMIT_DELAY)

        print(f"  {mkt_name}: {mkt_count}개 수집")

    all_stocks.sort(key=lambda x: x['rate'], reverse=True)
    print(f"\n✅ 당일 상승 종목 총 {len(all_stocks)}개")
    return all_stocks


# ── 2단계: 종목별 상세 조회 ───────────────────────────────────────
def fetch_price_detail(token, code):
    """신고가 여부 + 업종명 (FHKST01010100)"""
    try:
        j = kis_get(token, 'FHKST01010100',
            '/uapi/domestic-stock/v1/quotations/inquire-price',
            {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': code}
        )
        if j.get('rt_cd') != '0': return None
        o = j['output']
        close    = int(o.get('stck_prpr', 0) or 0)
        w52_high = int(o.get('w52_hgpr',  0) or 0)
        return {
            'is52h' : close > 0 and w52_high > 0 and close >= w52_high,
            'isAllH': o.get('new_hgpr_lwpr_cls_code') == '1',
            'dept'  : str(o.get('bstp_kor_isnm', '')),
        }
    except Exception as e:
        print(f"  ⚠️ {code} 시세 오류: {e}")
        return None


def fetch_investor(token, code):
    """투자자별 매매동향 (FHKST01010900) — 당일/1주/1달/15일 daily"""
    try:
        j = kis_get(token, 'FHKST01010900',
            '/uapi/domestic-stock/v1/quotations/inquire-investor',
            {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': code}
        )
        if j.get('rt_cd') != '0': return None
        arr = j.get('output', [])
        if not arr: return None

        def ti(v): return int(v or 0)
        t = arr[0]

        def sum_f(days, field):
            return sum(ti(r.get(field)) for r in arr[:days])

        daily = [
            {
                'date': r.get('stck_bsop_date', ''),
                'frgn': ti(r.get('frgn_ntby_tr_pbmn')),
                'inst': ti(r.get('orgn_ntby_tr_pbmn')),
                'priv': ti(r.get('pvt_fund_ntby_tr_pbmn')),
            }
            for r in reversed(arr[:15])
        ]
        return {
            'today': {
                'frgn': ti(t.get('frgn_ntby_tr_pbmn')),
                'inst': ti(t.get('orgn_ntby_tr_pbmn')),
                'priv': ti(t.get('pvt_fund_ntby_tr_pbmn')),
            },
            'week' : {k: sum_f(5,  f) for k, f in [('frgn','frgn_ntby_tr_pbmn'),('inst','orgn_ntby_tr_pbmn'),('priv','pvt_fund_ntby_tr_pbmn')]},
            'month': {k: sum_f(22, f) for k, f in [('frgn','frgn_ntby_tr_pbmn'),('inst','orgn_ntby_tr_pbmn'),('priv','pvt_fund_ntby_tr_pbmn')]},
            'daily': daily,
        }
    except Exception as e:
        print(f"  ⚠️ {code} 투자자 오류: {e}")
        return None


def fetch_daily_price(token, code):
    """기간별 일봉 (FHKST03010100) — 거래량 22일 평균 + daily 종가"""
    try:
        end   = TODAY
        start = (datetime.strptime(TODAY, '%Y%m%d') - timedelta(days=50)).strftime('%Y%m%d')
        j = kis_get(token, 'FHKST03010100',
            '/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice',
            {
                'FID_COND_MRKT_DIV_CODE': 'J',
                'FID_INPUT_ISCD':         code,
                'FID_INPUT_DATE_1':       start,
                'FID_INPUT_DATE_2':       end,
                'FID_PERIOD_DIV_CODE':    'D',
                'FID_ORG_ADJ_PRC':        '0',
            }
        )
        if j.get('rt_cd') != '0': return []
        return [
            {
                'date'  : r.get('stck_bsop_date', ''),
                'close' : int(r.get('stck_clpr', 0) or 0),
                'rate'  : float(r.get('prdy_ctrt', 0) or 0),
                'volume': int(r.get('acml_vol', 0) or 0),
            }
            for r in (j.get('output2') or [])
        ]   # 최신이 [0]
    except Exception as e:
        print(f"  ⚠️ {code} 일별시세 오류: {e}")
        return []


# ── 메인 ──────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*55}")
    print(f"  KIS 데이터 수집  [{TODAY}]")
    print(f"{'='*55}")

    token  = get_token()
    stocks = fetch_rising_stocks(token)
    total  = len(stocks)

    if total == 0:
        print("⚠️ 상승 종목 없음 — 장중 또는 주말/공휴일일 수 있습니다.")
        empty = []
        with open(DATA_DIR / 'today.json', 'w') as f: json.dump(empty, f)
        with open(DATA_DIR / 'meta.json',  'w') as f:
            json.dump({'date': TODAY, 'total': 0,
                       'generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, f)
        return

    print(f"\n{'='*55}")
    print(f"  종목별 상세 조회 ({total}개 × 3 API)")
    print(f"  예상 소요: {total * 3 * RATE_LIMIT_DELAY / 60:.1f}분")
    print(f"{'='*55}\n")

    result = []

    for idx, stock in enumerate(stocks, 1):
        code   = stock['code']
        name   = stock['name']
        rate   = stock['rate']
        volume = stock['volume']
        amount = stock['amount']
        cap    = stock['cap']
        shares = stock['shares']

        print(f"[{idx:3d}/{total}] {code} {name:<14} {rate:+.2f}%")

        detail      = fetch_price_detail(token, code);  time.sleep(RATE_LIMIT_DELAY)
        investor    = fetch_investor(token, code);       time.sleep(RATE_LIMIT_DELAY)
        daily_price = fetch_daily_price(token, code);   time.sleep(RATE_LIMIT_DELAY)

        # ── 22일 평균 거래량 & 비율 ──
        vol_avg_22, vol_ratio = 0, 0.0
        if daily_price:
            past = [r['volume'] for r in daily_price[1:23] if r['volume'] > 0]
            if past:
                vol_avg_22 = int(sum(past) / len(past))
                vol_ratio  = round(volume / vol_avg_22 * 100, 1) if vol_avg_22 else 0.0

            # investor daily 에 종가/거래량 병합
            if investor:
                pm = {r['date']: r for r in daily_price}
                for row in investor['daily']:
                    p = pm.get(row['date'], {})
                    row['close']  = p.get('close',  0)
                    row['rate']   = p.get('rate',   0)
                    row['volume'] = p.get('volume', 0)

        # ── Q / P 계산 ──
        # Q = 거래대금 / 시가총액  (비중, 소수)
        # P = 등락률 × 거래량 / 상장주식수 × 100  (%, 화면 표시용)
        Q = round(amount / cap,                  6) if cap    > 0 else 0.0
        P = round(rate * volume / shares * 100,  4) if shares > 0 else 0.0

        result.append({
            'code'     : code,
            'name'     : name,
            'market'   : stock['market'],
            'dept'     : (detail or {}).get('dept', ''),
            'close'    : stock['close'],
            'diff'     : stock['diff'],
            'rate'     : rate,
            'volume'   : volume,
            'amount'   : amount,
            'cap'      : cap,
            'shares'   : shares,
            'Q'        : Q,
            'P'        : P,          # ×100 적용됨 (% 단위)
            'is52h'    : (detail or {}).get('is52h',  False),
            'isAllH'   : (detail or {}).get('isAllH', False),
            'volAvg22' : vol_avg_22,
            'volRatio' : vol_ratio,
            'inv'      : investor,   # today/week/month/daily
        })

    # ── 저장 ──────────────────────────────────────────────────────
    with open(DATA_DIR / 'today.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, separators=(',', ':'))

    meta = {
        'date'     : TODAY,
        'generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total'    : len(result),
        'up'       : len(result),
        'down'     : 0,
        'new52h'   : sum(1 for r in result if r['is52h']),
        'newAllH'  : sum(1 for r in result if r['isAllH']),
        'volBurst' : sum(1 for r in result if r['volRatio'] >= 200),
    }
    with open(DATA_DIR / 'meta.json', 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*55}")
    print(f"✅ 완료: {len(result)}개 종목 → data/today.json")
    print(f"   52주신고가: {meta['new52h']}  역대신고가: {meta['newAllH']}")
    print(f"   거래량급증(200%↑): {meta['volBurst']}")
    print(f"{'='*55}")


if __name__ == '__main__':
    main()
