"""
포트폴리오 자동 갱신 스크립트 v3
- 시세 + 환율 + 코스피
- 종목별 뉴스 (yfinance + 네이버)
- 시장 수급 (KOSPI/KOSDAQ 외/기/개)
- M7 데일리 (yfinance)
- 종목별 이동평균선 5/20/120일·50주 (신규)
- 21개 업종지수 등락률·거래대금 (신규)
"""
import json
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
import yfinance as yf
import FinanceDataReader as fdr
from bs4 import BeautifulSoup

try:
    from pykrx import stock as pykrx_stock
    PYKRX_OK = True
except Exception as e:
    print(f"[WARN] pykrx import 실패: {e}", file=sys.stderr)
    PYKRX_OK = False

KST = timezone(timedelta(hours=9))
ROOT = Path(__file__).parent
DIST = ROOT / "dist"
DIST.mkdir(exist_ok=True)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


# =============================================================================
# 시세 / 환율 / 코스피
# =============================================================================
def fetch_fx_usd_krw() -> float:
    try:
        t = yf.Ticker("KRW=X")
        h = t.history(period="5d")
        if not h.empty:
            return float(h["Close"].iloc[-1])
    except Exception as e:
        print(f"[FX] {e}", file=sys.stderr)
    return 1461.80


def fetch_kospi():
    try:
        df = fdr.DataReader("KS11", datetime.now(KST) - timedelta(days=10))
        if len(df) >= 2:
            close = float(df["Close"].iloc[-1])
            prev = float(df["Close"].iloc[-2])
            return close, (close - prev) / prev * 100
    except Exception as e:
        print(f"[KOSPI/FDR] {e}", file=sys.stderr)
    try:
        h = yf.Ticker("^KS11").history(period="5d")
        if len(h) >= 2:
            close = float(h["Close"].iloc[-1])
            prev = float(h["Close"].iloc[-2])
            return close, (close - prev) / prev * 100
    except Exception as e:
        print(f"[KOSPI/YF] {e}", file=sys.stderr)
    return None, None


def fetch_history(yahoo_ticker, fdr_code=None, period_days=400):
    """일봉 데이터 — yfinance 우선, FDR 폴백. 컬럼명 'close'로 통일."""
    df = None
    if yahoo_ticker:
        try:
            h = yf.Ticker(yahoo_ticker).history(period="1y")
            if not h.empty:
                df = h.rename(columns={"Close": "close"})
        except Exception as e:
            print(f"[HIST/YF/{yahoo_ticker}] {e}", file=sys.stderr)
    if (df is None or df.empty) and fdr_code:
        try:
            start = datetime.now(KST) - timedelta(days=period_days)
            d = fdr.DataReader(fdr_code, start)
            if not d.empty:
                df = d.rename(columns={c: c.lower() for c in d.columns})
        except Exception as e:
            print(f"[HIST/FDR/{fdr_code}] {e}", file=sys.stderr)
    return df


def fetch_price_yahoo(ticker: str):
    try:
        t = yf.Ticker(ticker)
        h = t.history(period="2d")
        price = float(h["Close"].iloc[-1]) if not h.empty else None
        info = t.info or {}
        fund = {
            "per": info.get("trailingPE"),
            "pbr": info.get("priceToBook"),
            "eps": info.get("trailingEps"),
        }
        fund = {k: (round(v, 2) if isinstance(v, (int, float)) else None) for k, v in fund.items()}
        return price, fund
    except Exception as e:
        print(f"[YF/{ticker}] {e}", file=sys.stderr)
        return None, {}


def fetch_price_fdr(code: str):
    try:
        df = fdr.DataReader(code, datetime.now(KST) - timedelta(days=10))
        if not df.empty:
            return float(df["Close"].iloc[-1])
    except Exception as e:
        print(f"[FDR/{code}] {e}", file=sys.stderr)
    return None


# =============================================================================
# 신규: 이동평균선 5/20/120일 + 50주(=250영업일)
# =============================================================================
def compute_moving_averages(df) -> dict:
    """일봉 DataFrame에서 이평선 + 미니 시계열(최근 60일) 반환."""
    if df is None or df.empty or "close" not in df.columns or len(df) < 5:
        return None

    closes = df["close"]
    current = float(closes.iloc[-1])

    result = {
        "current": round(current, 2),
        "ma5": None, "ma20": None, "ma120": None, "ma50w": None,
        "series": [], "dates": [],
    }
    if len(closes) >= 5:
        result["ma5"] = round(float(closes.rolling(5).mean().iloc[-1]), 2)
    if len(closes) >= 20:
        result["ma20"] = round(float(closes.rolling(20).mean().iloc[-1]), 2)
    if len(closes) >= 120:
        result["ma120"] = round(float(closes.rolling(120).mean().iloc[-1]), 2)
    # 50주선 = 250영업일 평균 (50주 × 5일)
    if len(closes) >= 250:
        result["ma50w"] = round(float(closes.rolling(250).mean().iloc[-1]), 2)

    # 미니 차트용 최근 60일
    recent = closes.tail(60)
    result["series"] = [round(float(v), 2) for v in recent.values]
    result["dates"] = [
        d.strftime("%m.%d") if hasattr(d, "strftime") else str(d)
        for d in recent.index
    ]
    # 이평선 시계열도 같이 (60일분)
    if len(closes) >= 5:
        result["ma5_series"] = [
            round(float(v), 2) if v == v else None
            for v in closes.rolling(5).mean().tail(60).values
        ]
    if len(closes) >= 20:
        result["ma20_series"] = [
            round(float(v), 2) if v == v else None
            for v in closes.rolling(20).mean().tail(60).values
        ]

    # 현재가 vs 이평선 위치 분석
    flags = {}
    for k in ["ma5", "ma20", "ma120", "ma50w"]:
        if result[k] is not None:
            flags[k] = "위" if current >= result[k] else "아래"
    result["position"] = flags

    return result


# =============================================================================
# 뉴스 — 미국
# =============================================================================
def fetch_news_us(ticker: str, limit: int = 5) -> list[dict]:
    try:
        t = yf.Ticker(ticker)
        items = t.news or []
        out = []
        for it in items[:limit]:
            content = it.get("content") or it
            title = content.get("title") or it.get("title")
            url_obj = content.get("clickThroughUrl")
            url = (url_obj.get("url") if isinstance(url_obj, dict) else None) \
                or content.get("link") or it.get("link")
            pub = content.get("pubDate") or it.get("providerPublishTime")
            if isinstance(pub, (int, float)):
                pub = datetime.fromtimestamp(pub, tz=timezone.utc).isoformat()
            if title:
                out.append({"title": title, "url": url, "pub": pub, "source": "Yahoo Finance"})
        return out
    except Exception as e:
        print(f"[NEWS/{ticker}] {e}", file=sys.stderr)
        return []


# =============================================================================
# 뉴스 — 한국 (네이버 금융)
# =============================================================================
def fetch_news_kr(code: str, limit: int = 5) -> list[dict]:
    try:
        url = f"https://finance.naver.com/item/news_news.naver?code={code}&page=1"
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
        r.encoding = "euc-kr"
        soup = BeautifulSoup(r.text, "lxml")

        out = []
        rows = soup.select("table.type5 tbody tr")
        for row in rows:
            a = row.select_one("td.title a")
            info = row.select_one("td.info")
            date_td = row.select_one("td.date")
            if a and a.get("href"):
                href = a["href"]
                if href.startswith("/"):
                    href = "https://finance.naver.com" + href
                out.append({
                    "title": a.get_text(strip=True),
                    "url": href,
                    "pub": date_td.get_text(strip=True) if date_td else "",
                    "source": info.get_text(strip=True) if info else "네이버 금융",
                })
            if len(out) >= limit:
                break
        return out
    except Exception as e:
        print(f"[NAVER/{code}] {e}", file=sys.stderr)
        return []


# =============================================================================
# 시장 수급 (외/기/개) — 기존
# =============================================================================
def fetch_market_flow() -> dict:
    if not PYKRX_OK:
        return {"available": False, "reason": "pykrx 모듈 import 실패"}

    today = datetime.now(KST).date()
    start = (today - timedelta(days=10)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    out = {"available": True, "markets": {}}
    for market_code, market_name in [("KOSPI", "코스피"), ("KOSDAQ", "코스닥")]:
        try:
            df = pykrx_stock.get_market_trading_value_by_date(start, end, market_code)
            if df is None or df.empty:
                continue
            df = df.tail(5)
            recs = []
            for idx, row in df.iterrows():
                recs.append({
                    "date": idx.strftime("%m.%d") if hasattr(idx, "strftime") else str(idx),
                    "foreign": int(row.get("외국인합계", 0) or 0),
                    "institution": int(row.get("기관합계", 0) or 0),
                    "individual": int(row.get("개인", 0) or 0),
                })
            out["markets"][market_name] = recs
        except Exception as e:
            print(f"[FLOW/{market_code}] {e}", file=sys.stderr)
    return out


# =============================================================================
# 신규: 21개 업종지수 등락률·거래대금
# =============================================================================
def fetch_sector_indices() -> dict:
    """KOSPI 업종지수 — 최근 영업일 종가/등락률/거래대금."""
    if not PYKRX_OK:
        return {"available": False, "reason": "pykrx 미설치"}

    today = datetime.now(KST).date()
    # 영업일 가변성 대비, 최근 10일 검색해서 마지막 2개 사용
    start = (today - timedelta(days=15)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    sectors = []
    try:
        # KOSPI 시장의 업종 지수 목록 (1001=KOSPI, 1002~1004=대/중/소형주, 1005~=업종)
        all_indices = pykrx_stock.get_index_ticker_list(market="KOSPI")
        for ticker in all_indices:
            # 업종지수만 (1005부터): 음식료품, 섬유의복, ... 서비스업
            if not ticker.startswith("1") or len(ticker) != 4:
                continue
            try:
                num = int(ticker)
                # 1005 ~ 1099 범위가 업종지수, 그 외 (1001~1004)는 시장 전체/규모별
                if num < 1005 or num > 1099:
                    continue
            except ValueError:
                continue

            try:
                name = pykrx_stock.get_index_ticker_name(ticker)
                df = pykrx_stock.get_index_ohlcv_by_date(start, end, ticker)
                if df is None or len(df) < 2:
                    continue
                close = float(df["종가"].iloc[-1])
                prev = float(df["종가"].iloc[-2])
                # 거래대금 컬럼명은 버전에 따라 '거래대금' 또는 다른 명칭
                value = 0
                for col in ["거래대금", "거래량"]:
                    if col in df.columns:
                        value = float(df[col].iloc[-1])
                        break
                sectors.append({
                    "code": ticker,
                    "name": name,
                    "close": round(close, 2),
                    "change_pct": round((close - prev) / prev * 100, 2),
                    "value": int(value),
                })
            except Exception as e:
                print(f"[SECTOR/{ticker}] {e}", file=sys.stderr)
        # 거래대금 내림차순 정렬
        sectors.sort(key=lambda s: -s.get("value", 0))
    except Exception as e:
        print(f"[SECTOR_LIST] {e}", file=sys.stderr)
        return {"available": False, "reason": str(e)}

    return {"available": True, "sectors": sectors}


# =============================================================================
# 신규: 시장 키워드 (거래대금/등락률 TOP + 네이버 테마)
# =============================================================================
def _last_business_date_str() -> str:
    """최근 영업일 (오늘이 영업일이면 오늘, 아니면 직전 영업일) YYYYMMDD."""
    today = datetime.now(KST).date()
    for delta in range(0, 10):
        d = today - timedelta(days=delta)
        date_str = d.strftime("%Y%m%d")
        try:
            df = pykrx_stock.get_market_ohlcv_by_ticker(date_str, market="KOSPI")
            if df is not None and not df.empty:
                return date_str
        except Exception:
            continue
    return today.strftime("%Y%m%d")


def fetch_top_value_stocks(limit: int = 10) -> list[dict]:
    """KOSPI+KOSDAQ 통합 거래대금 상위 종목."""
    if not PYKRX_OK:
        return []
    try:
        date_str = _last_business_date_str()
        rows = []
        for market in ["KOSPI", "KOSDAQ"]:
            try:
                df = pykrx_stock.get_market_ohlcv_by_ticker(date_str, market=market)
                if df is None or df.empty:
                    continue
                df = df.sort_values("거래대금", ascending=False).head(limit * 2)
                for ticker, row in df.iterrows():
                    try:
                        name = pykrx_stock.get_market_ticker_name(ticker)
                    except Exception:
                        name = ticker
                    rows.append({
                        "ticker": ticker,
                        "name": name,
                        "market": market,
                        "close": float(row.get("종가", 0)),
                        "change_pct": float(row.get("등락률", 0)),
                        "value": int(row.get("거래대금", 0)),
                    })
            except Exception as e:
                print(f"[TOP_VALUE/{market}] {e}", file=sys.stderr)
        rows.sort(key=lambda r: -r["value"])
        return rows[:limit]
    except Exception as e:
        print(f"[TOP_VALUE] {e}", file=sys.stderr)
        return []


def fetch_top_change_stocks(limit: int = 5, ascending: bool = False, min_value: int = 1_000_000_000) -> list[dict]:
    """등락률 상위(하락) 종목. min_value 이상 거래대금만(잡주 제거)."""
    if not PYKRX_OK:
        return []
    try:
        date_str = _last_business_date_str()
        rows = []
        for market in ["KOSPI", "KOSDAQ"]:
            try:
                df = pykrx_stock.get_market_ohlcv_by_ticker(date_str, market=market)
                if df is None or df.empty:
                    continue
                df = df[df["거래대금"] >= min_value]
                df = df.sort_values("등락률", ascending=ascending).head(limit * 2)
                for ticker, row in df.iterrows():
                    try:
                        name = pykrx_stock.get_market_ticker_name(ticker)
                    except Exception:
                        name = ticker
                    rows.append({
                        "ticker": ticker, "name": name, "market": market,
                        "close": float(row.get("종가", 0)),
                        "change_pct": float(row.get("등락률", 0)),
                        "value": int(row.get("거래대금", 0)),
                    })
            except Exception as e:
                print(f"[TOP_CHG/{market}] {e}", file=sys.stderr)
        rows.sort(key=lambda r: r["change_pct"] if ascending else -r["change_pct"])
        return rows[:limit]
    except Exception as e:
        print(f"[TOP_CHG] {e}", file=sys.stderr)
        return []


def fetch_naver_themes(limit: int = 10) -> list[dict]:
    """네이버 금융 테마 상승률 상위."""
    try:
        url = "https://finance.naver.com/sise/theme.naver?&page=1"
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
        r.encoding = "euc-kr"
        soup = BeautifulSoup(r.text, "lxml")

        themes = []
        # 메인 테이블 - 클래스명이 시기마다 다를 수 있음, 여러 패턴 시도
        for table_sel in ["table.type_1.theme", "table.type_1", ".theme_main table"]:
            table = soup.select_one(table_sel)
            if not table:
                continue
            for row in table.select("tbody tr"):
                tds = row.select("td")
                if len(tds) < 4:
                    continue
                name_a = tds[0].select_one("a")
                if not name_a:
                    continue
                theme_name = name_a.get_text(strip=True)
                href = name_a.get("href", "")
                theme_url = ("https://finance.naver.com" + href) if href.startswith("/") else href

                # 등락률 — 보통 2번째 또는 3번째 컬럼
                change_text = None
                for td in tds[1:4]:
                    txt = td.get_text(strip=True).replace(" ", "")
                    if "%" in txt:
                        change_text = txt
                        break

                # 대표 종목 — 마지막 컬럼들에서 a 태그 찾기
                top_stocks = []
                for td in tds[-3:]:
                    for a in td.select("a"):
                        nm = a.get_text(strip=True)
                        if nm and nm not in top_stocks:
                            top_stocks.append(nm)

                if theme_name and change_text:
                    themes.append({
                        "name": theme_name,
                        "url": theme_url,
                        "change_pct_str": change_text,
                        "top_stocks": top_stocks[:3],
                    })
                if len(themes) >= limit:
                    break
            if themes:
                break
        return themes
    except Exception as e:
        print(f"[THEME] {e}", file=sys.stderr)
        return []


# =============================================================================
# 미국 M7
# =============================================================================
M7 = [
    ("AAPL", "Apple"), ("MSFT", "Microsoft"), ("GOOGL", "Alphabet"),
    ("AMZN", "Amazon"), ("META", "Meta"), ("NVDA", "NVIDIA"), ("TSLA", "Tesla"),
]


def fetch_m7_daily() -> list[dict]:
    out = []
    for ticker, name in M7:
        try:
            t = yf.Ticker(ticker)
            h = t.history(period="5d")
            if len(h) < 2:
                continue
            close = float(h["Close"].iloc[-1])
            prev = float(h["Close"].iloc[-2])
            chg_pct = (close - prev) / prev * 100
            info = t.info or {}
            out.append({
                "ticker": ticker, "name": name,
                "price": round(close, 2),
                "change_pct": round(chg_pct, 2),
                "mcap": info.get("marketCap"),
                "headlines": fetch_news_us(ticker, limit=2),
            })
        except Exception as e:
            print(f"[M7/{ticker}] {e}", file=sys.stderr)
        time.sleep(0.2)
    return out


# =============================================================================
# 종합 처리
# =============================================================================
def enrich_holdings(holdings: list[dict]):
    enriched = []
    news_map = {}
    ma_map = {}

    for h in holdings:
        price = None
        fund = {}
        if h.get("yahoo"):
            price, fund = fetch_price_yahoo(h["yahoo"])
        if price is None and h.get("fdr"):
            price = fetch_price_fdr(h["fdr"])

        cost = h.get("cost")
        qty = h.get("qty", 0)
        if price is not None and cost is not None:
            if h["ccy"] == "USD":
                h["price"] = round(price, 4)
                h["plUsd"] = round((price - cost) * qty, 2)
                h["ret"] = round((price - cost) / cost * 100, 2)
            else:
                h["price"] = round(price, 2)
                h["pl"] = round((price - cost) * qty)
                h["ret"] = round((price - cost) / cost * 100, 2)
        else:
            h["price"] = price

        if fund:
            h["per"] = fund.get("per")
            h["pbr"] = fund.get("pbr")
            h["eps"] = fund.get("eps")

        # 이평선 계산 (모든 종목 대상)
        hist = fetch_history(h.get("yahoo"), h.get("fdr"))
        ma = compute_moving_averages(hist)
        if ma:
            ma_map[h["name"]] = ma

        # 뉴스 (개별주만)
        if h.get("category") == "Stock":
            if h["market"] == "US" and h.get("yahoo"):
                news_map[h["name"]] = fetch_news_us(h["yahoo"], limit=5)
            elif h["market"] == "KR" and h.get("fdr"):
                news_map[h["name"]] = fetch_news_kr(h["fdr"], limit=5)
            time.sleep(0.3)

        enriched.append(h)

    return enriched, news_map, ma_map


def build_account_summary(holdings: list[dict], fx: float) -> list[dict]:
    accounts = {}
    for h in holdings:
        rec = accounts.setdefault(h["account"], {
            "id": h["account"], "accNum": h["accNum"], "cost": 0.0, "evalv": 0.0,
        })
        qty = h.get("qty", 0)
        cost = h.get("cost") or 0
        price = h.get("price") or 0
        if h["ccy"] == "USD":
            rec["cost"] += qty * cost * fx
            rec["evalv"] += qty * price * fx
        else:
            rec["cost"] += qty * cost
            rec["evalv"] += qty * price

    result = []
    for acc in accounts.values():
        pl = acc["evalv"] - acc["cost"]
        ret = (pl / acc["cost"] * 100) if acc["cost"] else 0
        acc["cost"] = round(acc["cost"])
        acc["evalv"] = round(acc["evalv"])
        acc["pl"] = round(pl)
        acc["ret"] = round(ret, 2)
        result.append(acc)
    return result


def render_html(data: dict) -> str:
    template = (ROOT / "index_template.html").read_text(encoding="utf-8")
    payload = json.dumps(data, ensure_ascii=False, default=str)
    return template.replace("/*__DATA_JSON__*/null", payload)


def main():
    print(f"=== {datetime.now(KST).isoformat()} 갱신 시작 ===")

    cfg = json.loads((ROOT / "portfolio_data.json").read_text(encoding="utf-8"))

    fx = fetch_fx_usd_krw()
    kospi_close, kospi_chg = fetch_kospi()
    print(f"[FX] {fx:.2f} / [KOSPI] {kospi_close} ({kospi_chg})")

    holdings, news_map, ma_map = enrich_holdings(cfg["holdings"])
    accounts = build_account_summary(holdings, fx)

    print("[M7] 수집...")
    m7 = fetch_m7_daily()

    print("[FLOW] 수집...")
    flow = fetch_market_flow()

    print("[SECTOR] 수집...")
    sectors = fetch_sector_indices()

    print("[KEYWORDS] 수집...")
    keywords = {
        "top_value": fetch_top_value_stocks(limit=10),
        "top_gainers": fetch_top_change_stocks(limit=5, ascending=False),
        "top_losers": fetch_top_change_stocks(limit=5, ascending=True),
        "themes": fetch_naver_themes(limit=10),
    }

    total_cost = sum(a["cost"] for a in accounts)
    total_eval = sum(a["evalv"] for a in accounts)
    total_pl = total_eval - total_cost
    total_ret = (total_pl / total_cost * 100) if total_cost else 0

    payload = {
        "fx": round(fx, 2),
        "kospi": {
            "close": round(kospi_close, 2) if kospi_close else None,
            "change_pct": round(kospi_chg, 2) if kospi_chg is not None else None,
        },
        "holdings": holdings,
        "accounts": accounts,
        "totals": {
            "cost": int(total_cost), "evalv": int(total_eval),
            "pl": int(total_pl), "ret": round(total_ret, 2),
        },
        "news_by_holding": news_map,
        "ma_by_holding": ma_map,
        "m7": m7,
        "market_flow": flow,
        "sector_indices": sectors,
        "market_keywords": keywords,
        "updated_at": datetime.now(KST).isoformat(timespec="seconds"),
    }

    html = render_html(payload)
    (DIST / "index.html").write_text(html, encoding="utf-8")
    (DIST / "data.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8"
    )
    print(f"=== 완료 — 총자산 {total_eval:,.0f}원 ({total_ret:+.2f}%) ===")


if __name__ == "__main__":
    main()
