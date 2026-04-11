"""
fetch_kis_data.py
─────────────────────────────────────────────
GitHub Actions에서 실행되는 KIS API 데이터 수집 스크립트.
Secrets에서 키를 읽어 아래 데이터를 수집하고 data/ 폴더에 JSON으로 저장합니다.

수집 데이터:
  - 주식현재가 시세 (종가, 등락률, 거래량, 52주/역대 신고가)
  - 투자자별 매매동향 (외국인, 기관합계, 사모펀드 - 당일/15일)
  - 일별 종가 (15일)

입력:  환경변수 KIS_APP_KEY, KIS_APP_SECRET
       엑셀 업로드 없이 KIS API만으로 전 종목 데이터를 구성하는 것은
       API 한도 초과 문제로 현실적이지 않아,
       data/uploaded_stocks.json (수동 업로드 또는 별도 스크립트로 생성)에서
       종목 리스트를 읽어 처리합니다.
출력:  data/today.json, data/meta.json
"""

import os, sys, json, time, requests
from datetime import datetime, timedelta
from pathlib import Path

# ── 설정 ────────────────────────────────────────────────────────
KIS_BASE    = 'https://openapi.koreainvestment.com:9443'
APP_KEY     = os.environ.get('KIS_APP_KEY', '')
APP_SECRET  = os.environ.get('KIS_APP_SECRET', '')
DATA_DIR    = Path(__file__).parent.parent / 'data'
DATA_DIR.mkdir(exist_ok=True)

# 수동 실행 시 날짜 지정 가능
TARGET_DATE = os.environ.get('TARGET_DATE', '').strip()
TODAY       = TARGET_DATE if TARGET_DATE else datetime.now().strftime('%Y%m%d')

RATE_LIMIT_DELAY = 0.13   # 초당 최대 ~7.5 req (KIS 권장: 20 req/s)
MAX_STOCKS       = 500    # 처리할 최대 종목 수 (너무 많으면 시간 초과)

# ── 인증 ─────────────────────────────────────────────────────────
def get_token():
    """KIS OAuth2 액세스 토큰 발급"""
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
    print(f"✅ KIS 토큰 발급 성공 (만료: {data.get('access_token_token_expired', '?')})")
    return data['access_token']


def kis_headers(token, tr_id):
    return {
        'authorization': f'Bearer {token}',
        'appkey':        APP_KEY,
        'appsecret':     APP_SECRET,
        'tr_id':         tr_id,
        'Content-Type':  'application/json',
    }


# ── 종목 리스트 로드 ─────────────────────────────────────────────
def load_stock_list():
    """
    data/uploaded_stocks.json 에서 종목 리스트를 읽습니다.
    파일이 없으면 KOSPI200 대표 종목으로 fallback.
    """
    stock_file = DATA_DIR / 'uploaded_stocks.json'
    if stock_file.exists():
        with open(stock_file, encoding='utf-8') as f:
            stocks = json.load(f)
        print(f"📂 uploaded_stocks.json 로드: {len(stocks)}개 종목")
        return stocks[:MAX_STOCKS]

    # fallback: 주요 종목 20개
    print("⚠️ uploaded_stocks.json 없음 → 기본 종목 사용")
    return [
        {'code':'005930','name':'삼성전자','market':'KOSPI','cap':0,'rate':0,'Q':0,'P':0},
        {'code':'000660','name':'SK하이닉스','market':'KOSPI','cap':0,'rate':0,'Q':0,'P':0},
        {'code':'005380','name':'현대차','market':'KOSPI','cap':0,'rate':0,'Q':0,'P':0},
        {'code':'051910','name':'LG화학','market':'KOSPI','cap':0,'rate':0,'Q':0,'P':0},
        {'code':'035420','name':'NAVER','market':'KOSPI','cap':0,'rate':0,'Q':0,'P':0},
        {'code':'035720','name':'카카오','market':'KOSPI','cap':0,'rate':0,'Q':0,'P':0},
        {'code':'068270','name':'셀트리온','market':'KOSDAQ','cap':0,'rate':0,'Q':0,'P':0},
        {'code':'247540','name':'에코프로비엠','market':'KOSDAQ','cap':0,'rate':0,'Q':0,'P':0},
    ]


# ── KIS API 호출 함수들 ──────────────────────────────────────────
def fetch_price(token, code):
    """주식현재가 시세 (FHKST01010100) - 종가, 등락률, 신고가 여부"""
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
            'close'  : close,
            'diff'   : int(o.get('prdy_vrss', 0) or 0),
            'rate'   : float(o.get('prdy_ctrt', 0) or 0),
            'volume' : int(o.get('acml_vol', 0) or 0),
            'amount' : int(o.get('acml_tr_pbmn', 0) or 0),
            'cap'    : int(o.get('hts_avls', 0) or 0) * 100_000_000,  # 억 단위 → 원
            'is52h'  : close > 0 and w52_high > 0 and close >= w52_high,
            'isAllH' : o.get('new_hgpr_lwpr_cls_code') == '1',
        }
    except Exception as e:
        print(f"  ⚠️ {code} 시세 오류: {e}")
        return None


def fetch_investor(token, code):
    """투자자별 매매동향 (FHKST01010900) - 외국인/기관/사모펀드 최근 30일"""
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

        # 당일
        t = arr[0]
        today_data = {
            'frgn': to_int(t.get('frgn_ntby_tr_pbmn')),
            'inst': to_int(t.get('orgn_ntby_tr_pbmn')),
            'priv': to_int(t.get('pvt_fund_ntby_tr_pbmn')),
        }

        # 1주(5일)/1달(22일) 합계
        def sum_field(days, field):
            return sum(to_int(r.get(field)) for r in arr[:days])

        week_data  = {k: sum_field(5,  f) for k, f in [('frgn','frgn_ntby_tr_pbmn'),('inst','orgn_ntby_tr_pbmn'),('priv','pvt_fund_ntby_tr_pbmn')]}
        month_data = {k: sum_field(22, f) for k, f in [('frgn','frgn_ntby_tr_pbmn'),('inst','orgn_ntby_tr_pbmn'),('priv','pvt_fund_ntby_tr_pbmn')]}

        # 15일 일별 내역 (오래된 순)
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
    """기간별 일봉 시세 (FHKST03010100) - 최근 30일 (거래량 평균 계산용)"""
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
        rows = j.get('output2', [])  # 최신순
        result = []
        for row in rows:
            result.append({
                'date'  : row.get('stck_bsop_date', ''),
                'close' : int(row.get('stck_clpr', 0) or 0),
                'rate'  : float(row.get('prdy_ctrt', 0) or 0),
                'volume': int(row.get('acml_vol', 0) or 0),
            })
        return result  # 최신 데이터가 [0]
    except Exception as e:
        print(f"  ⚠️ {code} 일별시세 오류: {e}")
        return []


# ── 메인 ─────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*50}")
    print(f"  KIS 데이터 수집 시작  [{TODAY}]")
    print(f"{'='*50}\n")

    token  = get_token()
    stocks = load_stock_list()
    total  = len(stocks)
    result = []

    for idx, stock in enumerate(stocks, 1):
        code = stock['code']
        name = stock.get('name', code)
        print(f"[{idx:3d}/{total}] {code} {name}")

        # 시세 조회
        price = fetch_price(token, code)
        time.sleep(RATE_LIMIT_DELAY)

        # 투자자 조회
        investor = fetch_investor(token, code)
        time.sleep(RATE_LIMIT_DELAY)

        # 일별 시세 (투자자 daily에 종가 병합용)
        daily_price = fetch_daily_price(token, code)
        time.sleep(RATE_LIMIT_DELAY)

        # 투자자 daily에 종가·거래량 병합, 한달 평균 거래량 계산
        vol_avg_22 = 0
        vol_ratio  = 0.0
        if daily_price:
            # daily_price[0]이 최신(당일), [1]~[22]가 직전 22거래일
            past_22 = [r['volume'] for r in daily_price[1:23] if r['volume'] > 0]
            if past_22:
                vol_avg_22 = int(sum(past_22) / len(past_22))
                today_vol  = daily_price[0]['volume'] if daily_price else volume
                vol_ratio  = round((today_vol / vol_avg_22) * 100, 1) if vol_avg_22 > 0 else 0.0

            # 투자자 daily(오래된 순)에 종가·거래량 병합
            if investor:
                # daily_price는 최신순 → 날짜 맵
                price_map = {r['date']: r for r in daily_price}
                for row in investor['daily']:
                    pm = price_map.get(row['date'], {})
                    row['close']  = pm.get('close', 0)
                    row['rate']   = pm.get('rate', 0)
                    row['volume'] = pm.get('volume', 0)

        # 기본 필드 (업로드된 엑셀 값 우선, 없으면 KIS 값)
        close  = (price or {}).get('close', 0) or stock.get('close', 0)
        rate   = (price or {}).get('rate', 0)  or stock.get('rate', 0)
        cap    = (price or {}).get('cap', 0)   or stock.get('cap', 0)
        volume = (price or {}).get('volume', 0) or stock.get('volume', 0)
        amount = (price or {}).get('amount', 0) or stock.get('amount', 0)
        shares = stock.get('shares', 0)

        # Q, P 재계산 (KIS 값 기반)
        Q = (amount / cap)         if cap    > 0 else stock.get('Q', 0)
        P = (rate * volume / shares) if shares > 0 else stock.get('P', 0)

        entry = {
            'code'     : code,
            'name'     : name,
            'market'   : stock.get('market', ''),
            'dept'     : stock.get('dept', ''),
            'close'    : close,
            'diff'     : (price or {}).get('diff', 0),
            'rate'     : round(rate, 2),
            'volume'   : volume,
            'amount'   : amount,
            'cap'      : cap,
            'shares'   : shares,
            'Q'        : round(Q, 6),
            'P'        : round(P, 6),
            'is52h'    : (price or {}).get('is52h', False),
            'isAllH'   : (price or {}).get('isAllH', False),
            'volAvg22' : vol_avg_22,   # 직전 22거래일 평균 거래량
            'volRatio' : vol_ratio,    # 당일 거래량 / 22일 평균 (%, e.g. 250.0 = 250%)
            'inv'      : investor,     # today/week/month/daily
        }
        result.append(entry)

    # ── 저장 ──────────────────────────────────────────────────────
    out_path = DATA_DIR / 'today.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, separators=(',', ':'))

    meta = {
        'date'       : TODAY,
        'generated'  : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total'      : len(result),
        'up'         : sum(1 for r in result if r['rate'] > 0),
        'down'       : sum(1 for r in result if r['rate'] < 0),
        'new52h'     : sum(1 for r in result if r['is52h']),
        'newAllH'    : sum(1 for r in result if r['isAllH']),
        'volBurst'   : sum(1 for r in result if r['volRatio'] >= 200),
    }
    with open(DATA_DIR / 'meta.json', 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 완료: {len(result)}개 종목 → data/today.json")
    print(f"   52주신고가: {meta['new52h']}개 / 역대신고가: {meta['newAllH']}개")
    print(f"   거래량급증(200%↑): {meta['volBurst']}개")
    print(f"   상승: {meta['up']} / 하락: {meta['down']}")


if __name__ == '__main__':
    main()
