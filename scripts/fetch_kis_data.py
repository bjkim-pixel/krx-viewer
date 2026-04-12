"""
fetch_kis_data.py
─────────────────────────────────────────────
GitHub Actions에서 실행.
data/codes.json 을 읽어서 종목별 KIS API 조회:
  - 52주/역대 신고가 (FHKST01010100)
  - 외국인/기관/사모펀드 수급 (FHKST01010900)
  - 15일 일별 종가+거래량 (FHKST03010100)
결과를 data/enriched.json 으로 저장.

Secrets: KIS_APP_KEY, KIS_APP_SECRET
"""

import os, sys, json, time, requests
from datetime import datetime, timedelta
from pathlib import Path

KIS_BASE    = 'https://openapi.koreainvestment.com:9443'
APP_KEY     = os.environ.get('KIS_APP_KEY', '')
APP_SECRET  = os.environ.get('KIS_APP_SECRET', '')
DATA_DIR    = Path(__file__).parent.parent / 'data'
DATA_DIR.mkdir(exist_ok=True)
DELAY       = 0.14   # KIS rate limit


def get_token():
    if not APP_KEY or not APP_SECRET:
        print("❌ KIS_APP_KEY / KIS_APP_SECRET 없음"); sys.exit(1)
    r = requests.post(f'{KIS_BASE}/oauth2/tokenP',
        headers={'Content-Type':'application/json'},
        json={'grant_type':'client_credentials','appkey':APP_KEY,'appsecret':APP_SECRET},
        timeout=10)
    d = r.json()
    if 'access_token' not in d:
        print(f"❌ 토큰 실패: {d}"); sys.exit(1)
    print("✅ KIS 토큰 발급")
    return d['access_token']


def hdr(token, tr_id):
    return {'authorization':f'Bearer {token}','appkey':APP_KEY,'appsecret':APP_SECRET,
            'tr_id':tr_id,'Content-Type':'application/json'}


def fetch_price(token, code):
    """신고가 여부"""
    try:
        j = requests.get(f'{KIS_BASE}/uapi/domestic-stock/v1/quotations/inquire-price',
            params={'FID_COND_MRKT_DIV_CODE':'J','FID_INPUT_ISCD':code},
            headers=hdr(token,'FHKST01010100'), timeout=8).json()
        if j.get('rt_cd') != '0': return {}
        o = j['output']
        c = int(o.get('stck_prpr',0) or 0)
        w = int(o.get('w52_hgpr',0) or 0)
        return {'is52h': c>0 and w>0 and c>=w, 'isAllH': o.get('new_hgpr_lwpr_cls_code')=='1'}
    except: return {}


def fetch_investor(token, code):
    """외국인/기관/사모 수급"""
    try:
        j = requests.get(f'{KIS_BASE}/uapi/domestic-stock/v1/quotations/inquire-investor',
            params={'FID_COND_MRKT_DIV_CODE':'J','FID_INPUT_ISCD':code},
            headers=hdr(token,'FHKST01010900'), timeout=8).json()
        if j.get('rt_cd') != '0': return {}
        arr = j.get('output', [])
        if not arr: return {}
        ti = lambda v: int(v or 0)
        t  = arr[0]
        sf = lambda days,f: sum(ti(r.get(f)) for r in arr[:days])
        return {
            'today': {'frgn':ti(t.get('frgn_ntby_tr_pbmn')),'inst':ti(t.get('orgn_ntby_tr_pbmn')),'priv':ti(t.get('pvt_fund_ntby_tr_pbmn'))},
            'week' : {k:sf(5,f)  for k,f in [('frgn','frgn_ntby_tr_pbmn'),('inst','orgn_ntby_tr_pbmn'),('priv','pvt_fund_ntby_tr_pbmn')]},
            'month': {k:sf(22,f) for k,f in [('frgn','frgn_ntby_tr_pbmn'),('inst','orgn_ntby_tr_pbmn'),('priv','pvt_fund_ntby_tr_pbmn')]},
            'daily': [{'date':r.get('stck_bsop_date',''),
                       'frgn':ti(r.get('frgn_ntby_tr_pbmn')),
                       'inst':ti(r.get('orgn_ntby_tr_pbmn')),
                       'priv':ti(r.get('pvt_fund_ntby_tr_pbmn')),
                       'close':0,'rate':0.0,'volume':0}
                      for r in reversed(arr[:15])],
        }
    except: return {}


def fetch_daily(token, code, today):
    """15일 종가+거래량 → investor daily에 병합용"""
    try:
        start = (datetime.strptime(today,'%Y%m%d')-timedelta(days=40)).strftime('%Y%m%d')
        j = requests.get(f'{KIS_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice',
            params={'FID_COND_MRKT_DIV_CODE':'J','FID_INPUT_ISCD':code,
                    'FID_INPUT_DATE_1':start,'FID_INPUT_DATE_2':today,
                    'FID_PERIOD_DIV_CODE':'D','FID_ORG_ADJ_PRC':'0'},
            headers=hdr(token,'FHKST03010100'), timeout=8).json()
        if j.get('rt_cd') != '0': return {}, 0, 0.0
        rows = j.get('output2',[])
        pm = {r['stck_bsop_date']:{'close':int(r.get('stck_clpr',0) or 0),
                                    'rate':float(r.get('prdy_ctrt',0) or 0),
                                    'volume':int(r.get('acml_vol',0) or 0)}
              for r in rows}
        # 22일 평균 거래량
        vols = [int(r.get('acml_vol',0) or 0) for r in rows[1:23] if r.get('acml_vol')]
        avg  = int(sum(vols)/len(vols)) if vols else 0
        today_vol = int(rows[0].get('acml_vol',0) or 0) if rows else 0
        ratio = round(today_vol/avg*100, 1) if avg > 0 else 0.0
        return pm, avg, ratio
    except: return {}, 0, 0.0


def main():
    codes_path = DATA_DIR / 'codes.json'
    if not codes_path.exists():
        print("❌ data/codes.json 없음 — 엑셀을 먼저 업로드하세요")
        sys.exit(1)

    with open(codes_path, encoding='utf-8') as f:
        info = json.load(f)

    codes  = info.get('codes', [])
    date   = info.get('date', datetime.now().strftime('%Y%m%d'))
    total  = len(codes)

    print(f"\n{'='*50}")
    print(f"  KIS 수급/신고가 조회  [{date}]  {total}개 종목")
    print(f"{'='*50}\n")

    if total == 0:
        print("⚠️ 종목 없음"); return

    token   = get_token()
    result  = {}

    for idx, code in enumerate(codes, 1):
        print(f"[{idx:4d}/{total}] {code}")

        price    = fetch_price(token, code);    time.sleep(DELAY)
        investor = fetch_investor(token, code); time.sleep(DELAY)
        pm, avg, ratio = fetch_daily(token, code, date); time.sleep(DELAY)

        # daily에 종가/거래량 병합
        if investor and investor.get('daily'):
            for row in investor['daily']:
                p = pm.get(row['date'], {})
                row['close']  = p.get('close', 0)
                row['rate']   = p.get('rate',  0.0)
                row['volume'] = p.get('volume',0)

        result[code] = {
            **price,
            'inv'      : investor or None,
            'volAvg22' : avg,
            'volRatio' : ratio,
            'frgn'     : (investor or {}).get('today', {}).get('frgn'),
            'inst'     : (investor or {}).get('today', {}).get('inst'),
            'priv'     : (investor or {}).get('today', {}).get('priv'),
        }

    out = {
        'date'     : date,
        'generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total'    : total,
        'data'     : result,
    }
    with open(DATA_DIR / 'enriched.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, separators=(',',':'))

    print(f"\n✅ enriched.json 저장 ({total}개)")


if __name__ == '__main__':
    main()
