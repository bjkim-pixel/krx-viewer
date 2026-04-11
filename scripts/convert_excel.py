import sys, json
from pathlib import Path

def to_f(v):
    try:
        return float(str(v).replace(',', ''))
    except:
        return None

def main():
    if len(sys.argv) < 2:
        print("사용법: python scripts/convert_excel.py <엑셀파일.xlsx>")
        sys.exit(1)

    import pandas as pd
    xl_path = Path(sys.argv[1])
    if not xl_path.exists():
        print(f"파일 없음: {xl_path}")
        sys.exit(1)

    print(f"📂 읽는 중: {xl_path}")
    df = pd.read_excel(xl_path, dtype=str)

    result = []
    for _, row in df.iterrows():
        등락률    = to_f(row.get('등락률'))
        거래량    = to_f(row.get('거래량'))
        거래대금  = to_f(row.get('거래대금'))
        시가총액  = to_f(row.get('시가총액'))
        상장주식수 = to_f(row.get('상장주식수'))

        if None in [등락률, 거래량, 거래대금, 시가총액, 상장주식수]:
            continue
        if 시가총액 == 0 or 상장주식수 == 0:
            continue

        Q = 거래대금 / 시가총액
        P = (등락률 * 거래량) / 상장주식수

        result.append({
            'code'   : str(row.get('종목코드', '')).zfill(6),
            'name'   : str(row.get('종목명', '')),
            'market' : str(row.get('시장구분', '')),
            'dept'   : str(row.get('소속부', '')),
            'close'  : to_f(row.get('종가')) or 0,
            'diff'   : to_f(row.get('대비')) or 0,
            'rate'   : round(등락률, 2),
            'volume' : int(거래량),
            'amount' : int(거래대금),
            'cap'    : int(시가총액),
            'shares' : int(상장주식수),
            'Q'      : round(Q, 6),
            'P'      : round(P, 6),
        })

    out = Path(__file__).parent.parent / 'data' / 'uploaded_stocks.json'
    out.parent.mkdir(exist_ok=True)
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, separators=(',', ':'))

    print(f"✅ 변환 완료: {len(result)}개 종목 → {out}")


if __name__ == '__main__':
    main()
