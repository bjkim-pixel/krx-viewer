"""
fetch_kis_data.py
─────────────────────────────────────────────
흐름:
  1. pykrx로 KOSPI+KOSDAQ 전종목 당일 시세 수집 (API 2번 호출로 전종목 완료)
     - 등락률 > 0 종목만 필터 (당일 상승 종목)
     - 시가총액, 상장주식수도 같이 수집
  2. pykrx로 22거래일 평균 거래량 계산 (일별시세)
  3. KIS API로 종목별 투자자 수급 + 신고가 여부 조회
     - 투자자: 외국인/기관합계/사모펀드 당일/1주/1달/15일 daily
     - 신고가: 52주 신고가, 역대 신고가

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
RATE_LIMIT_DELAY = 0.13   # KIS API 호출 간격


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


# ── 1단계: pykrx로 전종목 당일 시세 수집 ─────────────────────────
def fetch_all_stocks_pykrx():
    """
    pykrx.stock.get_market_ohlcv(date, market) 로
    KOSPI + KOSDAQ 전종목 시세를 한 번에 수집.
    등락률 > 0 종목만 반환.
    """
    try:
        from pykrx import stock as krx
    except ImportError:
        print("❌ pykrx가 설치되지 않았습니다. pip install pykrx")
        sys.exit(1)

    all_stocks = []

    for market in ['KOSPI', 'KOSDAQ']:
        print(f"\n📊 {market} 전종목 시세 수집 중 (pykrx)...")
        try:
            # 전종목 OHLCV (시가/고가/저가/종가/거래량/거래대금/등락률)
            df_ohlcv = krx.get_market_ohlcv(TODAY, market=market)
            if df_ohlcv is None or df_ohlcv.empty:
                print(f"  ⚠️ {market} 데이터 없음 (휴장일 또는 장 마감 전)")
                continue

            # 시가총액 + 상장주식수
            df_cap = krx.get_market_cap(TODAY, market=market)

            # 종목명
            tickers = df_ohlcv.index.tolist()
            count = 0
            for code in tickers:
                try:
                    row = df_ohlcv.loc[code]
                    rate = float(row.get('등락률', 0) or 0)
                    if rate <= 0:
                        continue  # 상승 종목만

                    close  = int(row.get('종가', 0) or 0)
                    volume = int(row.get('거래량', 0) or 0)
                    amount = int(row.get('거래대금', 0) or 0)
                    open_  = int(row.get('시가', 0) or 0)

                    # diff = 종가 - 시가 근사값 (pykrx는 전일종가를 제공하지 않음)
                    # 등락률과 종가로 역산: 전일종가 = 종가 / (1 + rate/100)
                    prev_close = round(close / (1 + rate / 100)) if rate != -100 else 0
                    diff = close - prev_close

                    # 시가총액/상장주식수
                    cap    = 0
                    shares = 0
                    if df_cap is not None and not df_cap.empty and code in df_cap.index:
                        cap_row = df_cap.loc[code]
                        cap    = int(cap_row.get('시가총액', 0) or 0)
                        shares = int(cap_row.get('상장주식수', 0) or 0)

                    # 종목명
                    try:
                        name = krx.get_market_ticker_name(code)
                    except:
                        name = code

                    # Q, P 계산
                    Q = round(amount / cap,                6) if cap    > 0 else 0.0
                    P = round(rate * volume / shares * 100, 4) if shares > 0 else 0.0

                    all_stocks.append({
                        'code'  : code,
                        'name'  : name,
                        'market': market,
                        'dept'  : '',
                        'close' : close,
                        'diff'  : diff,
                        'rate'  : round(rate, 2),
                        'volume': volume,
                        'amount': amount,
                        'cap'   : cap,
                        'shares': shares,
                        'Q'     : Q,
                        'P'     : P,
                    })
                    count += 1
                except Exception as e:
                    continue

            print(f"  ✅ {market} 상승 종목: {count}개 / 전체: {len(tickers)}개")

        except Exception as e:
            print(f"  ❌ {market} 수집 실패: {e}")
            import traceback; traceback.print_exc()

    # 등락률 내림차순 정렬
    all_stocks.sort(key=lambda x: x['rate'], reverse=True)
    print(f"\n✅ 당일 상승 종목 총 {len(all_stocks)}개")
    return all_stocks


# ── 2단계: 22일 평균 거래량 계산 (pykrx) ─────────────────────────
def fetch_vol_avg_pykrx(code):
    """pykrx로 최근 30거래일 일봉 조회 → 직전 22일 평균 거래량 계산"""
    try:
        from pykrx import stock as krx
        end   = TODAY
        start = (datetime.strptime(TODAY, '%Y%m%d') - timedelta(days=45)).strftime('%Y%m%d')
        df = krx.get_market_ohlcv(start, end, code)
        if df is None or df.empty or len(df) < 2:
            return 0, 0.0
        # 오래된순 정렬, 마지막 행이 당일
        vols = df['거래량'].tolist()
        today_vol = vols[-1]
        past_22   = [v for v in vols[:-1][-22:] if v > 0]
        if not past_22:
            return 0, 0.0
        avg = int(sum(past_22) / len(past_22))
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
                    'close': 0, 'rate': 0, 'volume': 0,  # 아래서 pykrx로 병합
                }
                for r in reversed(arr[:15])
            ],
        }
    except Exception as e:
        print(f"  ⚠️ {code} 투자자 오류: {e}")
        return None


def merge_daily_price(investor, code):
    """investor daily에 pykrx 일별 종가/거래량 병합"""
    if not investor or not investor.get('daily'):
        return
    try:
        from pykrx import stock as krx
        end   = TODAY
        start = (datetime.strptime(TODAY, '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')
        df = krx.get_market_ohlcv(start, end, code)
        if df is None or df.empty:
            return
        # index가 datetime → YYYYMMDD 문자열로 변환
        price_map = {
            idx.strftime('%Y%m%d'): {
                'close' : int(row.get('종가', 0) or 0),
                'rate'  : float(row.get('등락률', 0) or 0),
                'volume': int(row.get('거래량', 0) or 0),
            }
            for idx, row in df.iterrows()
        }
        for row in investor['daily']:
            pm = price_map.get(row.get('date', ''), {})
            row['close']  = pm.get('close',  0)
            row['rate']   = pm.get('rate',   0)
            row['volume'] = pm.get('volume', 0)
    except:
        pass


# ── 메인 ──────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*55}")
    print(f"  KIS 데이터 수집  [{TODAY}]")
    print(f"{'='*55}")

    # KIS 토큰
    token = get_token()

    # 1단계: pykrx로 전종목 당일 상승 종목 수집
    stocks = fetch_all_stocks_pykrx()
    total  = len(stocks)

    if total == 0:
        print("\n⚠️ 상승 종목 없음 — 휴장일이거나 장 마감 전일 수 있습니다.")
        with open(DATA_DIR / 'today.json', 'w') as f: json.dump([], f)
        with open(DATA_DIR / 'meta.json',  'w') as f:
            json.dump({'date': TODAY, 'total': 0,
                       'generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, f)
        return

    # 2+3단계: 종목별 상세 조회
    print(f"\n{'='*55}")
    print(f"  종목별 상세 조회 ({total}개)")
    print(f"  · pykrx: 22일 평균 거래량")
    print(f"  · KIS API: 신고가 + 투자자 수급")
    print(f"{'='*55}\n")

    result = []

    for idx, stock in enumerate(stocks, 1):
        code = stock['code']
        name = stock['name']
        rate = stock['rate']
        print(f"[{idx:4d}/{total}] {code} {name:<14} {rate:+.2f}%")

        # 22일 평균 거래량 (pykrx)
        vol_avg_22, vol_ratio = fetch_vol_avg_pykrx(code)

        # 신고가 여부 (KIS)
        detail = fetch_price_detail(token, code)
        time.sleep(RATE_LIMIT_DELAY)

        # 투자자 수급 (KIS)
        investor = fetch_investor(token, code)
        time.sleep(RATE_LIMIT_DELAY)

        # investor daily에 종가/거래량 병합 (pykrx)
        if investor:
            merge_daily_price(investor, code)

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
