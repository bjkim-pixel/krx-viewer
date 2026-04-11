"""
fetch_kis_data.py
─────────────────────────────────────────────
흐름:
  1. KIS 등락률순위 API(FHPST01710000)로 당일 상승 전 종목 수집
     KOSPI / KOSDAQ 각각 조회 → 등락률 0% 초과 종목만
  2. 종목별 신고가 여부 + 투자자 매매동향 + 일별시세(거래량 평균) 조회
  3. data/today.json, data/meta.json 저장

입력:  환경변수 KIS_APP_KEY, KIS_APP_SECRET
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
RATE_LIMIT_DELAY = 0.13   # 초당 ~7.5 req
MAX_STOCKS       = 600    # 상승 종목 최대 처리 수


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


def kis_headers(token, tr_id):
    return {
        'authorization': f'Bearer {token}',
        'appkey':        APP_KEY,
        'appsecret':     APP_SECRET,
        'tr_id':         tr_id,
        'Content-Type':  'application/json',
    }


# ── 1단계: 당일 상승 전 종목 수집 ─────────────────────────────────
def fetch_rising_stocks(token):
    """
    등락률순위 API(FHPST01710000)로 당일 상승 종목 전체 수집.
    KOSPI(0001) + KOSDAQ(1001) 각각 조회.
    """
    markets = [('0001', 'KOSPI'), ('1001', 'KOSDAQ')]
    all_stocks = []

    for mkt_code, mkt_name in markets:
        print(f"\n📊 {mkt_name} 상승 종목 조회 중...")
        try:
            r = requests.get(
                f'{KIS_BASE}/uapi/domestic-stock/v1/ranking/fluctuation',
                params={
                    'fid_cond_mrkt_div_code': 'J',
                    'fid_cond_scr_div_code':  '20171',
                    'fid_input_iscd':         mkt_code,
                    'fid_rank_sort_cls_code': '0',    # 상승률순
                    'fid_input_cnt_1':        '0',
                    'fid_prc_cls_code':       '1',
                    'fid_input_price_1':      '',
                    'fid_input_price_2':      '',
                    'fid_vol_cnt':            '',
                    'fid_trgt_cls_code':      '0',
                    'fid_trgt_exls_cls_code': '0',
                    'fid_div_cls_code':       '0',
                    'fid_rsfl_rate1':         '0',    # 등락률 0% 이상
                    'fid_rsfl_rate2':         '',
                },
                headers=kis_headers(token, 'FHPST01710000'),
                timeout=10
            )
            j = r.json()
            if j.get('rt_cd') != '0':
                print(f"  ⚠️ {mkt_name} 조회 실패: {j.get('msg1', '')}")
                time.sleep(RATE_LIMIT_DELAY)
                continue

            rows = j.get('output', [])

            def to_i(v): return int(str(v or '0').replace(',', '') or 0)
            def to_f(v): return float(str(v or '0').replace(',', '') or 0)

            count = 0
            for row in rows:
                rate = to_f(row.get('prdy_ctrt', 0))
                if rate <= 0:
                    continue
                cap_eok = to_i(row.get('hts_avls', 0))
                all_stocks.append({
                    'code'  : str(row.get('stck_shrn_iscd', '')).zfill(6),
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
                count += 1

            print(f"  {mkt_name} 상승 종목: {count}개")

        except Exception as e:
            print(f"  ⚠️ {mkt_name} 조회 오류: {e}")

        time.sleep(RATE_LIMIT_DELAY)

    # 등락률 내림차순 정렬
    all_stocks.sort(key=lambda x: x['rate'], reverse=True)
    print(f"\n✅ 당일 상승 종목 총 {len(all_stocks)}개 수집")
    return all_stocks[:MAX_STOCKS]


# ── 2단계: 종목별 상세 조회 함수들 ───────────────────────────────
def fetch_price_detail(token, code):
    """신고가 여부 조회 (FHKST01010100)"""
    try:
        r = requests.get(
            f'{KIS_BASE}/uapi/domestic-stock/v1/quotations/inquire-price',
            params={'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': code},
            headers=kis_headers(token, 'FHKST01010100'),
            timeout=8
        )
        j = r.json()
        if j.get('rt_cd') != '0':
            return None
        o = j['output']
        close    = int(o.get('stck_prpr', 0) or 0)
        w52_high = int(o.get('w52_hgpr', 0) or 0)
        return {
            'is52h' : close > 0 and w52_high > 0 and close >= w52_high,
            'isAllH': o.get('new_hgpr_lwpr_cls_code') == '1',
            'dept'  : str(o.get('bstp_kor_isnm', '')),
        }
    except Exception as e:
        print(f"  ⚠️ {code} 시세 오류: {e}")
        return None


def fetch_investor(token, code):
    """투자자별 매매동향 (FHKST01010900)"""
    try:
        r = requests.get(
            f'{KIS_BASE}/uapi/domestic-stock/v1/quotations/inquire-investor',
            params={'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': code},
            headers=kis_headers(token, 'FHKST01010900'),
            timeout=8
        )
        j = r.json()
        if j.get('rt_cd') != '0':
            return None
        arr = j.get('output', [])
        if not arr:
            return None

        def to_int(v): return int(v or 0)

        t = arr[0]
        today_data = {
            'frgn': to_int(t.get('frgn_ntby_tr_pbmn')),
            'inst': to_int(t.get('orgn_ntby_tr_pbmn')),
            'priv': to_int(t.get('pvt_fund_ntby_tr_pbmn')),
        }

        def sum_field(days, field):
            return sum(to_int(row.get(field)) for row in arr[:days])

        week_data  = {k: sum_field(5,  f) for k, f in [('frgn','frgn_ntby_tr_pbmn'),('inst','orgn_ntby_tr_pbmn'),('priv','pvt_fund_ntby_tr_pbmn')]}
        month_data = {k: sum_field(22, f) for k, f in [('frgn','frgn_ntby_tr_pbmn'),('inst','orgn_ntby_tr_pbmn'),('priv','pvt_fund_ntby_tr_pbmn')]}

        daily = []
        for row in reversed(arr[:15]):
            daily.append({
                'date': row.get('stck_bsop_date', ''),
                'frgn': to_int(row.get('frgn_ntby_tr_pbmn')),
                'inst': to_int(row.get('orgn_ntby_tr_pbmn')),
                'priv': to_int(row.get('pvt_fund_ntby_tr_pbmn')),
            })

        return {'today': today_data, 'week': week_data, 'month': month_data, 'daily': daily}
    except Exception as e:
        print(f"  ⚠️ {code} 투자자 오류: {e}")
        return None


def fetch_daily_price(token, code):
    """기간별 일봉 (FHKST03010100) - 22일 평균 거래량 계산용"""
    try:
        end   = TODAY
        start = (datetime.strptime(TODAY, '%Y%m%d') - timedelta(days=50)).strftime('%Y%m%d')
        r = requests.get(
            f'{KIS_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice',
            params={
                'FID_COND_MRKT_DIV_CODE': 'J',
                'FID_INPUT_ISCD':         code,
                'FID_INPUT_DATE_1':       start,
                'FID_INPUT_DATE_2':       end,
                'FID_PERIOD_DIV_CODE':    'D',
                'FID_ORG_ADJ_PRC':        '0',
            },
            headers=kis_headers(token, 'FHKST03010100'),
            timeout=8
        )
        j = r.json()
        if j.get('rt_cd') != '0':
            return []
        rows = j.get('output2', [])
        result = []
        for row in rows:
            result.append({
                'date'  : row.get('stck_bsop_date', ''),
                'close' : int(row.get('stck_clpr', 0) or 0),
                'rate'  : float(row.get('prdy_ctrt', 0) or 0),
                'volume': int(row.get('acml_vol', 0) or 0),
            })
        return result  # 최신이 [0]
    except Exception as e:
        print(f"  ⚠️ {code} 일별시세 오류: {e}")
        return []


# ── 메인 ──────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*50}")
    print(f"  KIS 데이터 수집 시작  [{TODAY}]")
    print(f"{'='*50}")

    token  = get_token()
    stocks = fetch_rising_stocks(token)
    total  = len(stocks)

    if total == 0:
        print("⚠️ 상승 종목이 없습니다. 장 마감 후 재실행하세요.")
        with open(DATA_DIR / 'today.json', 'w') as f:
            json.dump([], f)
        with open(DATA_DIR / 'meta.json', 'w') as f:
            json.dump({'date': TODAY, 'total': 0, 'generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, f)
        return

    print(f"\n{'='*50}")
    print(f"  종목별 상세 조회 ({total}개)")
    print(f"{'='*50}\n")

    result = []

    for idx, stock in enumerate(stocks, 1):
        code   = stock['code']
        name   = stock['name']
        volume = stock['volume']
        amount = stock['amount']
        cap    = stock['cap']
        shares = stock['shares']
        rate   = stock['rate']

        print(f"[{idx:3d}/{total}] {code} {name} ({rate:+.2f}%)")

        # 신고가 조회
        detail = fetch_price_detail(token, code)
        time.sleep(RATE_LIMIT_DELAY)

        # 투자자 조회
        investor = fetch_investor(token, code)
        time.sleep(RATE_LIMIT_DELAY)

        # 일별 시세
        daily_price = fetch_daily_price(token, code)
        time.sleep(RATE_LIMIT_DELAY)

        # 22일 평균 거래량 계산
        vol_avg_22 = 0
        vol_ratio  = 0.0
        if daily_price:
            past_22 = [r['volume'] for r in daily_price[1:23] if r['volume'] > 0]
            if past_22:
                vol_avg_22 = int(sum(past_22) / len(past_22))
                vol_ratio  = round((volume / vol_avg_22) * 100, 1) if vol_avg_22 > 0 else 0.0

            if investor:
                price_map = {r['date']: r for r in daily_price}
                for row in investor['daily']:
                    pm = price_map.get(row['date'], {})
                    row['close']  = pm.get('close', 0)
                    row['rate']   = pm.get('rate', 0)
                    row['volume'] = pm.get('volume', 0)

        # Q, P 계산
        Q = round(amount / cap,              6) if cap    > 0 else 0.0
        P = round(rate * volume / shares,    6) if shares > 0 else 0.0

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
            'P'        : P,
            'is52h'    : (detail or {}).get('is52h', False),
            'isAllH'   : (detail or {}).get('isAllH', False),
            'volAvg22' : vol_avg_22,
            'volRatio' : vol_ratio,
            'inv'      : investor,
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

    print(f"\n{'='*50}")
    print(f"✅ 완료: {len(result)}개 종목 저장")
    print(f"   52주신고가: {meta['new52h']}개  역대신고가: {meta['newAllH']}개")
    print(f"   거래량급증(200%↑): {meta['volBurst']}개")
    print(f"{'='*50}")


if __name__ == '__main__':
    main()
