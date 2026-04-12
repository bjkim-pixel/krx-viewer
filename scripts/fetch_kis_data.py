"""
fetch_kis_data.py
─────────────────────────────────────────────
흐름:
  1. KRX 사이트 직접 HTTP 요청으로 KOSPI+KOSDAQ 전종목 당일 시세 수집
     (pykrx가 내부적으로 하는 것과 동일한 방식 — pandas 버전 무관)
     - 등락률 > 0 종목만 필터 (당일 상승 종목)
  2. KRX에서 22거래일 일봉 수집 → 평균 거래량 계산
  3. KIS API로 종목별 투자자 수급 + 신고가 여부 조회

  P = 등락률 × 거래량 / 상장주식수 × 100  (% 단위)
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
RATE_LIMIT_DELAY = 0.13

# KRX 요청 공통 헤더
KRX_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'http://data.krx.co.kr/',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
}


# ── KIS 인증 ──────────────────────────────────────────────────────
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


def kis_get(token, tr_id, path, params):
    return requests.get(
        f'{KIS_BASE}{path}', params=params,
        headers={
            'authorization': f'Bearer {token}',
            'appkey': APP_KEY, 'appsecret': APP_SECRET,
            'tr_id': tr_id, 'Content-Type': 'application/json',
        },
        timeout=10
    ).json()


# ── 1단계: KRX 직접 요청으로 전종목 시세 수집 ────────────────────
def fetch_krx_ohlcv(date, market_id):
    """
    KRX 전종목 시세 API 직접 호출.
    market_id: STK=KOSPI, KSQ=KOSDAQ
    반환: list of dict {code, name, close, diff, rate, volume, amount, cap, shares}
    """
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    params = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
        'locale': 'ko_KR',
        'mktId': market_id,
        'trdDd': date,
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
    }
    try:
        res = requests.post(url, data=params, headers=KRX_HEADERS, timeout=15)
        data = res.json()
        rows = data.get('OutBlock_1', [])
        result = []
        for row in rows:
            try:
                def to_i(k): return int(str(row.get(k, '0') or '0').replace(',', '') or 0)
                def to_f(k): return float(str(row.get(k, '0') or '0').replace(',', '') or 0)

                code   = str(row.get('ISU_SRT_CD', '') or '').zfill(6)
                name   = str(row.get('ISU_ABBRV', '') or '')
                close  = to_i('TDD_CLSPRC')
                diff   = to_i('CMPPREVDD_PRC')
                rate   = to_f('FLUC_RT')
                volume = to_i('ACC_TRDVOL')
                amount = to_i('ACC_TRDVAL')
                cap    = to_i('MKTCAP')         # 억원 단위
                shares = to_i('LIST_SHRS')      # 주 단위

                if not code or close == 0:
                    continue

                result.append({
                    'code'  : code,
                    'name'  : name,
                    'close' : close,
                    'diff'  : diff,
                    'rate'  : round(rate, 2),
                    'volume': volume,
                    'amount': amount,
                    'cap'   : cap * 100_000_000,   # 억 → 원
                    'shares': shares,
                })
            except:
                continue
        return result
    except Exception as e:
        print(f"  ❌ KRX 조회 실패 ({market_id}): {e}")
        return []


def fetch_all_stocks():
    """KOSPI + KOSDAQ 전종목 수집 → 상승 종목만 반환"""
    markets = [('STK', 'KOSPI'), ('KSQ', 'KOSDAQ')]
    all_stocks = []

    for mkt_id, mkt_name in markets:
        print(f"\n📊 {mkt_name} 전종목 시세 수집 중 (KRX)...")
        rows = fetch_krx_ohlcv(TODAY, mkt_id)
        count = 0
        for row in rows:
            if row['rate'] <= 0:
                continue  # 상승 종목만
            cap    = row['cap']
            shares = row['shares']
            volume = row['volume']
            amount = row['amount']
            rate   = row['rate']

            Q = round(amount / cap,                6) if cap    > 0 else 0.0
            P = round(rate * volume / shares * 100, 4) if shares > 0 else 0.0

            all_stocks.append({
                **row,
                'market': mkt_name,
                'dept'  : '',
                'Q'     : Q,
                'P'     : P,
            })
            count += 1
        print(f"  ✅ {mkt_name}: 상승 {count}개 / 전체 {len(rows)}개")
        time.sleep(0.5)  # KRX 서버 부하 방지

    all_stocks.sort(key=lambda x: x['rate'], reverse=True)
    print(f"\n✅ 당일 상승 종목 총 {len(all_stocks)}개")
    return all_stocks


# ── 2단계: KRX에서 22일 평균 거래량 계산 ─────────────────────────
def fetch_vol_avg_krx(code):
    """KRX 일별 시세로 직전 22거래일 평균 거래량 계산"""
    try:
        end   = TODAY
        start = (datetime.strptime(TODAY, '%Y%m%d') - timedelta(days=50)).strftime('%Y%m%d')
        url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
        params = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT01701',
            'locale': 'ko_KR',
            'isuCd': code,
            'strtDd': start,
            'endDd': end,
            'csvxls_isNo': 'false',
        }
        res = requests.post(url, data=params, headers=KRX_HEADERS, timeout=10)
        rows = res.json().get('output', [])

        if not rows:
            # 다른 파라미터 시도
            params2 = {
                'bld': 'dbms/MDC/STAT/standard/MDCSTAT01701',
                'locale': 'ko_KR',
                'isuCd': f'KR7{code}003' if len(code) == 6 else code,
                'strtDd': start,
                'endDd': end,
                'csvxls_isNo': 'false',
            }
            res2 = requests.post(url, data=params2, headers=KRX_HEADERS, timeout=10)
            rows = res2.json().get('output', [])

        if len(rows) < 2:
            return 0, 0.0

        def to_i(v): return int(str(v or '0').replace(',', '') or 0)

        # rows는 오래된 순 — 마지막이 당일
        vols = [to_i(r.get('ACC_TRDVOL', 0)) for r in rows]
        today_vol = vols[-1]
        past_22   = [v for v in vols[:-1][-22:] if v > 0]
        if not past_22:
            return 0, 0.0
        avg   = int(sum(past_22) / len(past_22))
        ratio = round(today_vol / avg * 100, 1) if avg > 0 else 0.0
        return avg, ratio
    except:
        return 0, 0.0


# ── 3단계: KIS API - 신고가 + 투자자 수급 ────────────────────────
def fetch_price_detail(token, code):
    """신고가 여부 + 업종명 (FHKST01010100)"""
    try:
        j = kis_get(token, 'FHKST01010100',
            '/uapi/domestic-stock/v1/quotations/inquire-price',
            {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': code})
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
    """투자자별 매매동향 (FHKST01010900)"""
    try:
        j = kis_get(token, 'FHKST01010900',
            '/uapi/domestic-stock/v1/quotations/inquire-investor',
            {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': code})
        if j.get('rt_cd') != '0': return None
        arr = j.get('output', [])
        if not arr: return None

        def ti(v): return int(v or 0)
        t = arr[0]

        def sum_f(days, field):
            return sum(ti(r.get(field)) for r in arr[:days])

        return {
            'today': {
                'frgn': ti(t.get('frgn_ntby_tr_pbmn')),
                'inst': ti(t.get('orgn_ntby_tr_pbmn')),
                'priv': ti(t.get('pvt_fund_ntby_tr_pbmn')),
            },
            'week' : {k: sum_f(5,  f) for k, f in [('frgn','frgn_ntby_tr_pbmn'),('inst','orgn_ntby_tr_pbmn'),('priv','pvt_fund_ntby_tr_pbmn')]},
            'month': {k: sum_f(22, f) for k, f in [('frgn','frgn_ntby_tr_pbmn'),('inst','orgn_ntby_tr_pbmn'),('priv','pvt_fund_ntby_tr_pbmn')]},
            'daily': [
                {
                    'date': r.get('stck_bsop_date', ''),
                    'frgn': ti(r.get('frgn_ntby_tr_pbmn')),
                    'inst': ti(r.get('orgn_ntby_tr_pbmn')),
                    'priv': ti(r.get('pvt_fund_ntby_tr_pbmn')),
                    'close': 0, 'rate': 0.0, 'volume': 0,
                }
                for r in reversed(arr[:15])
            ],
        }
    except Exception as e:
        print(f"  ⚠️ {code} 투자자 오류: {e}")
        return None


def fetch_daily_price_krx(code):
    """KRX 일별 시세 → investor daily에 종가/거래량 병합용"""
    try:
        end   = TODAY
        start = (datetime.strptime(TODAY, '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')
        url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
        params = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT01701',
            'locale': 'ko_KR',
            'isuCd': code,
            'strtDd': start,
            'endDd': end,
            'csvxls_isNo': 'false',
        }
        res = requests.post(url, data=params, headers=KRX_HEADERS, timeout=10)
        rows = res.json().get('output', [])
        def to_i(v): return int(str(v or '0').replace(',', '') or 0)
        def to_f(v): return float(str(v or '0').replace(',', '') or 0)
        # rows: 오래된 순
        pm = {}
        for r in rows:
            dt = str(r.get('TRD_DD', '') or '').replace('/', '').replace('-', '')
            pm[dt] = {
                'close' : to_i(r.get('TDD_CLSPRC', 0)),
                'rate'  : to_f(r.get('FLUC_RT', 0)),
                'volume': to_i(r.get('ACC_TRDVOL', 0)),
            }
        return pm
    except:
        return {}


# ── 메인 ──────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*55}")
    print(f"  KIS 데이터 수집  [{TODAY}]")
    print(f"{'='*55}")

    token  = get_token()
    stocks = fetch_all_stocks()
    total  = len(stocks)

    if total == 0:
        print("\n⚠️ 상승 종목 없음 — 휴장일이거나 장 마감 전일 수 있습니다.")
        with open(DATA_DIR / 'today.json', 'w') as f: json.dump([], f)
        with open(DATA_DIR / 'meta.json',  'w') as f:
            json.dump({'date': TODAY, 'total': 0,
                       'generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, f)
        return

    print(f"\n{'='*55}")
    print(f"  종목별 상세 조회 ({total}개)")
    print(f"  · KRX: 22일 평균 거래량 + 일별 종가")
    print(f"  · KIS: 신고가 + 투자자 수급")
    print(f"{'='*55}\n")

    result = []

    for idx, stock in enumerate(stocks, 1):
        code = stock['code']
        name = stock['name']
        rate = stock['rate']
        print(f"[{idx:4d}/{total}] {code} {name:<14} {rate:+.2f}%")

        # 22일 평균 거래량 (KRX)
        vol_avg_22, vol_ratio = fetch_vol_avg_krx(code)
        time.sleep(0.1)

        # 신고가 (KIS)
        detail = fetch_price_detail(token, code)
        time.sleep(RATE_LIMIT_DELAY)

        # 투자자 수급 (KIS)
        investor = fetch_investor(token, code)
        time.sleep(RATE_LIMIT_DELAY)

        # investor daily에 KRX 종가/거래량 병합
        if investor and investor.get('daily'):
            price_map = fetch_daily_price_krx(code)
            time.sleep(0.1)
            for row in investor['daily']:
                pm = price_map.get(row.get('date', ''), {})
                row['close']  = pm.get('close',  0)
                row['rate']   = pm.get('rate',   0.0)
                row['volume'] = pm.get('volume', 0)

        result.append({
            'code'     : code,
            'name'     : name,
            'market'   : stock['market'],
            'dept'     : (detail or {}).get('dept', ''),
            'close'    : stock['close'],
            'diff'     : stock['diff'],
            'rate'     : rate,
            'volume'   : stock['volume'],
            'amount'   : stock['amount'],
            'cap'      : stock['cap'],
            'shares'   : stock['shares'],
            'Q'        : stock['Q'],
            'P'        : stock['P'],
            'is52h'    : (detail or {}).get('is52h',  False),
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

    print(f"\n{'='*55}")
    print(f"✅ 완료: {len(result)}개 종목 저장")
    print(f"   52주신고가: {meta['new52h']}  역대신고가: {meta['newAllH']}")
    print(f"   거래량급증(200%↑): {meta['volBurst']}")
    print(f"{'='*55}")


if __name__ == '__main__':
    main()
