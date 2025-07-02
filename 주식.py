"""
Stock-Scout KR · Multi-Factor Ranking
────────────────────────────────────
* 시가총액 상위 N → 유동성 필터 → 3-Factor 점수 순 정렬
* 결과를 CSV, Excel 저장 + LLM 프롬프트 텍스트 생성
* LLM 호출 없음 (오프라인 전처리 전용)

필요 패키지:
  pip install pykrx pandas numpy openpyxl python-dotenv
"""

from __future__ import annotations
import os, datetime as dt, time
import numpy as np
import pandas as pd
from pykrx import stock

# ───────────────────────
# 0. 사용자 파라미터
# ───────────────────────
TOP_N_UNIVERSE   = 500          # 시총 상위 500
MIN_MKT_CAP      = 2e11         # 2,000억 이상
MIN_DAILY_VALUE  = 5e8          # 5억 원 이상(1M=백만)
EXPORT_DIR       = "exports"
FILENAME_PREFIX  = "stock_scout"

WEIGHTS = {                     # 팩터 가중치
    "value": 0.4,
    "quality": 0.3,
    "momentum": 0.3,
}

# ───────────────────────
# 1. 날짜 헬퍼
# ───────────────────────
TODAY = dt.date.today()
TODAY_STR = TODAY.strftime("%Y%m%d")

def prev_trading_date(days: int) -> str:
    return (TODAY - dt.timedelta(days=days*1.6)).strftime("%Y%m%d")
    # 1.6 배수 → 주말·휴일 보정 대략

# ───────────────────────
# 2. 데이터 수집
# ───────────────────────
def build_universe(n: int) -> list[str]:
    cap = stock.get_market_cap_by_ticker(TODAY_STR)
    return cap.sort_values("시가총액", ascending=False).head(n).index.tolist()


def fetch_snapshot(tickers: list[str]) -> pd.DataFrame:
    ohlcv = stock.get_market_ohlcv_by_ticker(TODAY_STR)   # 가격·거래량
    cap   = stock.get_market_cap_by_ticker(TODAY_STR)
    fund  = stock.get_market_fundamental_by_ticker(TODAY_STR)

    rows = []
    for code in tickers:
        try:
            price   = ohlcv.loc[code, "종가"]
            volume  = ohlcv.loc[code, "거래량"]
            mkt_val = price * volume            # ← 거래대금 직접 계산

            eps = fund.loc[code, "EPS"]
            bps = fund.loc[code, "BPS"]
            roe = np.nan if bps == 0 else eps / bps * 100

            rows.append({
                "ticker":   code,
                "price":    price,
                "mkt_cap":  cap.loc[code, "시가총액"],
                "per":      fund.loc[code, "PER"],
                "pbr":      fund.loc[code, "PBR"],
                "roe":      roe,
                "daily_val": mkt_val,            # ← 여기 사용
            })
        except KeyError:
            continue
    return pd.DataFrame(rows)

def add_momentum(df: pd.DataFrame, months: int = 6) -> pd.DataFrame:
    if df.empty: return df
    start = prev_trading_date(months*21)     # 약 n개월 거래일
    pct = {}
    for code in df["ticker"]:
        hist = stock.get_market_ohlcv_by_date(start, TODAY_STR, code)
        pct[code] = np.nan if hist.empty else \
            hist["종가"].iloc[-1] / hist["종가"].iloc[0] - 1
        time.sleep(0.01)      # KRX 요청 과속 방지
    df["mom6m"] = df["ticker"].map(pct)
    return df

# ───────────────────────
# 3. 필터 + 점수
# ───────────────────────
def zscore(series: pd.Series) -> pd.Series:
    return (series - series.mean()) / series.std(ddof=0)

def multi_factor_rank(df: pd.DataFrame) -> pd.DataFrame:
    # 결측/비정상값 처리
    df = df.dropna(subset=["per", "pbr", "roe", "mom6m"])
    df = df[(df["per"] > 0) & (df["pbr"] > 0)]

    # Z-score
    df["value_z"]   = - zscore((df["per"] + df["pbr"]) / 2)   # 낮을수록 ↑
    df["quality_z"] =   zscore(df["roe"])
    df["mom_z"]     =   zscore(df["mom6m"])

    df["score"] = (
        WEIGHTS["value"]   * df["value_z"] +
        WEIGHTS["quality"] * df["quality_z"] +
        WEIGHTS["momentum"]* df["mom_z"]
    )
    return df.sort_values("score", ascending=False).reset_index(drop=True)

# ───────────────────────
# 4. 메인 파이프라인
# ───────────────────────
def main() -> None:
    os.makedirs(EXPORT_DIR, exist_ok=True)
    tickers = build_universe(TOP_N_UNIVERSE)

    snap = fetch_snapshot(tickers)
    snap = snap[
        (snap["mkt_cap"] >= MIN_MKT_CAP) &
        (snap["daily_val"] >= MIN_DAILY_VALUE)
    ]

    snap = add_momentum(snap)
    ranked = multi_factor_rank(snap)

    if ranked.empty:
        print("▶ 조건에 맞는 종목이 없습니다.")
        return

    # 상위 50개만 저장 (원하면 변경)
    top = ranked.head(50)

    csv_file  = f"{EXPORT_DIR}/{FILENAME_PREFIX}_{TODAY_STR}.csv"
    xlsx_file = f"{EXPORT_DIR}/{FILENAME_PREFIX}_{TODAY_STR}.xlsx"
    top.to_csv(csv_file,  index=False, encoding="utf-8-sig")
    top.to_excel(xlsx_file, index=False, engine="openpyxl")

    # LLM 프롬프트
    lines = [
        f"- {r.ticker}: 가격 {r.price:,.0f}원, PER {r.per:.1f}, "
        f"PBR {r.pbr:.2f}, ROE {r.roe:.1f}%, 6m {r.mom6m*100:.1f}%"
        for _, r in top.iterrows()
    ]
    prompt = (
        "다음 한국 주식들을 팩터 관점으로 평가하고 JSON 배열로만 답하세요.\n"
        "예: [{\"ticker\":\"000660\",\"score\":80,\"target\":115000,\"reason\":\"...\"}, ...]\n\n"
        + "\n".join(lines)
    )
    txt_file = f"{EXPORT_DIR}/{FILENAME_PREFIX}_{TODAY_STR}_prompt.txt"
    with open(txt_file, "w", encoding="utf-8") as f:
        f.write(prompt)

    print(f"▶ 종합 점수 Top {len(top)} 종목 저장 완료")
    print(f"   • CSV  : {csv_file}")
    print(f"   • XLSX : {xlsx_file}")
    print(f"   • 프롬프트 텍스트 : {txt_file}")

if __name__ == "__main__":
    main()
