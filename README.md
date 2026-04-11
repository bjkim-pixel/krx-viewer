# 📊 KRX 주식 분석기

KRX 전종목시세 엑셀 + 한국투자증권 KIS API를 연동한 모바일 주식 분석 웹앱.  
GitHub Actions가 매일 자동으로 데이터를 수집하고, GitHub Pages로 서빙합니다.

---

## 🗂 파일 구조

```
/
├── .github/workflows/
│   └── fetch_data.yml        ← Actions 워크플로우 (매일 자동 실행)
├── scripts/
│   ├── fetch_kis_data.py     ← KIS API 데이터 수집 메인 스크립트
│   └── convert_excel.py      ← KRX 엑셀 → JSON 변환 (로컬 1회 실행)
├── data/
│   ├── uploaded_stocks.json  ← 종목 리스트 (convert_excel.py 로 생성)
│   ├── today.json            ← Actions가 매일 생성하는 데이터
│   └── meta.json             ← 메타 정보 (날짜, 종목수 등)
├── index.html                ← 메인 리스트 화면
└── detail.html               ← 종목 상세 화면
```

---

## ⚙️ 초기 설정

### 1단계: GitHub Secrets 등록

**절대로 코드에 직접 키를 입력하지 마세요.**

1. 이 저장소 → **Settings** → **Secrets and variables** → **Actions**
2. **New repository secret** 클릭 후 아래 두 개 등록:

| Name | Value |
|------|-------|
| `KIS_APP_KEY` | 한국투자증권 AppKey |
| `KIS_APP_SECRET` | 한국투자증권 AppSecret |

### 2단계: 종목 리스트 생성 (로컬에서 1회)

KRX에서 다운로드한 전종목시세 엑셀 파일을 JSON으로 변환합니다.

```bash
pip install pandas openpyxl
python scripts/convert_excel.py data_5853_20260410.xlsx
```

생성된 `data/uploaded_stocks.json`을 커밋합니다:

```bash
git add data/uploaded_stocks.json
git commit -m "종목 리스트 업데이트"
git push
```

### 3단계: GitHub Pages 활성화

1. **Settings** → **Pages**
2. **Source**: `Deploy from a branch`
3. **Branch**: `main` / `/ (root)`
4. **Save** 클릭

### 4단계: Actions 첫 실행

1. **Actions** 탭 → **KRX Daily Data Fetch**
2. **Run workflow** 클릭 (수동 첫 실행)
3. 완료 후 Pages URL 접속

---

## 🔄 자동 실행 스케줄

매주 월~금 **오후 4시 30분 KST** (장 마감 후) 자동 실행됩니다.

수동 실행 시 특정 날짜 지정 가능:
- Actions → Run workflow → `target_date` 입력 (YYYYMMDD)

---

## 📱 주요 기능

### 리스트 화면 (index.html)
- **필터**: 시장구분 / 시가총액(1천억·5천억·1조) / 등락률 / 52주·역대 신고가 / 외국인 순매수
- **정렬**: P무게수·Q대금비중·등락률·시가총액·외국인·기관
- **카드**: 신고가 태그 🔥★ + 외국인·기관·사모펀드 당일 순매수 금액

### 상세 화면 (detail.html)
- 기본 지표 (시가총액·Q·P·거래량·거래대금)
- 투자자 요약: 외국인·기관·사모펀드 **당일 / 1주 / 1달** 순매수 합계
- 15일 일별 테이블: 날짜·종가·투자자 거래대금 (미니 바 차트)

---

## 📐 계산식

| 지표 | 계산 |
|------|------|
| **Q (거래대금비중)** | 거래대금 ÷ 시가총액 |
| **P (주식무게수)** | 등락률 × 거래량 ÷ 상장주식수 |

---

## ⚠️ KIS API Rate Limit

- Actions 스크립트는 요청 사이 **0.13초 지연** (초당 ~7.5건)
- KIS 무료 계정: 초당 20건 제한
- 종목 수 많을 경우 `MAX_STOCKS` 값 조정 (scripts/fetch_kis_data.py)
