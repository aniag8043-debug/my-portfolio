"""
포트폴리오 자동 갱신 스크립트 v5
- 시세 + 환율 + 코스피
- 종목별 뉴스 (yfinance + 네이버)
- 시장 수급 (네이버 모바일 API)         ★ v5: KRX 차단 우회
- 업종지수 (네이버 sise_group 크롤링)    ★ v5: KRX 차단 우회
- 시장 키워드 (네이버 거래대금/등락률 크롤링) ★ v5: KRX 차단 우회
- M7 데일리 (yfinance) + 한글 뉴스       ★ v5: 영문 뉴스 → 네이버 한글 뉴스
- 종목별 이동평균선 5/20/120일·50주
"""
import json
import re
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
MOBILE_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
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


def fetch_price_naver(code: str):
    """네이버 금융 종목 페이지에서 현재가 — 한국 종목 최종 폴백 (신규 ETF 대응)"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
        r.encoding = "euc-kr"
        soup = BeautifulSoup(r.text, "lxml")
        # 현재가는 .no_today .blind 또는 #_nowVal 에 있음
        no_today = soup.select_one(".no_today .blind") or soup.select_one("#_nowVal")
        if no_today:
            text = no_today.get_text(strip=True).replace(",", "")
            m = re.search(r"(\d+(?:\.\d+)?)", text)
            if m:
                return float(m.group(1))
        # 보조: dl.blind 안의 "현재가" 다음 dd
        for dl in soup.select("dl.blind"):
            items = dl.get_text(" ", strip=True)
            m = re.search(r"현재가\s*([\d,]+)", items)
            if m:
                return float(m.group(1).replace(",", ""))
    except Exception as e:
        print(f"[NAVER_PRICE/{code}] {e}", file=sys.stderr)
    return None


# =============================================================================
# 이동평균선
# =============================================================================
def compute_moving_averages(df) -> dict:
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
    if len(closes) >= 250:
        result["ma50w"] = round(float(closes.rolling(250).mean().iloc[-1]), 2)

    recent = closes.tail(60)
    result["series"] = [round(float(v), 2) for v in recent.values]
    result["dates"] = [
        d.strftime("%m.%d") if hasattr(d, "strftime") else str(d)
        for d in recent.index
    ]
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

    flags = {}
    for k in ["ma5", "ma20", "ma120", "ma50w"]:
        if result[k] is not None:
            flags[k] = "위" if current >= result[k] else "아래"
    result["position"] = flags

    return result


# =============================================================================
# 뉴스 — 미국 (★ v5: 네이버 한글 뉴스로 교체)
# =============================================================================
# 미국 종목 → 네이버 종목코드 매핑 (필요시 확장)
US_NAVER_SYMBOL_MAP = {
    "AAPL": "AAPL.O",
    "MSFT": "MSFT.O",
    "GOOGL": "GOOGL.O",
    "AMZN": "AMZN.O",
    "META": "META.O",
    "NVDA": "NVDA.O",
    "TSLA": "TSLA.O",
    "PLTR": "PLTR.O",
    "SDGR": "SDGR.O",
    "CCL": "CCL",      # NYSE
    "BOIL": "BOIL",    # NYSE Arca
}


def fetch_news_us(ticker: str, limit: int = 5) -> list[dict]:
    """미국 종목 한글 뉴스 — 네이버 모바일 API"""
    naver_symbol = US_NAVER_SYMBOL_MAP.get(ticker, f"{ticker}.O")
    url = f"https://m.stock.naver.com/api/news/worldstock/{naver_symbol}"
    try:
        r = requests.get(
            url,
            headers={"User-Agent": MOBILE_UA, "Accept": "application/json"},
            params={"pageSize": limit * 2, "page": 1},
            timeout=10,
        )
        if r.status_code != 200:
            print(f"[NEWS_US/{ticker}] HTTP {r.status_code}", file=sys.stderr)
            return _fetch_news_us_fallback(ticker, limit)

        data = r.json()
        # 응답 구조 보호 처리
        items = []
        if isinstance(data, list):
            # 목록이 직접 리스트로 올 수도, 또는 그룹된 형태로 올 수도 있음
            for group in data:
                if isinstance(group, dict):
                    if "items" in group and isinstance(group["items"], list):
                        items.extend(group["items"])
                    else:
                        items.append(group)
        elif isinstance(data, dict):
            items = data.get("items") or data.get("articles") or data.get("list") or []

        out = []
        for it in items[:limit]:
            if not isinstance(it, dict):
                continue
            title = (
                it.get("title") or it.get("articleTitle")
                or it.get("officeName") or ""
            )
            office = it.get("officeName") or it.get("source") or "네이버 금융"
            office_id = it.get("officeId") or ""
            article_id = it.get("articleId") or it.get("aid") or ""
            pub = it.get("datetime") or it.get("pubDate") or it.get("articleDateTime") or ""

            # 네이버 뉴스 URL 조립
            news_url = ""
            if office_id and article_id:
                news_url = f"https://n.news.naver.com/article/{office_id}/{article_id}"
            elif it.get("originalLink"):
                news_url = it["originalLink"]
            elif it.get("link"):
                news_url = it["link"]

            if title:
                out.append({
                    "title": str(title).strip(),
                    "url": news_url,
                    "pub": str(pub).strip(),
                    "source": str(office).strip(),
                })

        if out:
            return out
        # 빈 응답이면 폴백
        return _fetch_news_us_fallback(ticker, limit)
    except Exception as e:
        print(f"[NEWS_US/{ticker}] {e}", file=sys.stderr)
        return _fetch_news_us_fallback(ticker, limit)


def _fetch_news_us_fallback(ticker: str, limit: int = 5) -> list[dict]:
    """폴백 — yfinance 영문 뉴스 (네이버 실패 시)"""
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
                out.append({
                    "title": title, "url": url, "pub": pub,
                    "source": "Yahoo Finance (영문)",
                })
        return out
    except Exception as e:
        print(f"[NEWS_US_FB/{ticker}] {e}", file=sys.stderr)
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
# ★ v5: 시장 수급 — 네이버 모바일 API
# =============================================================================
def fetch_market_flow() -> dict:
    """KOSPI/KOSDAQ 일별 외/기/개 수급 (네이버 모바일 API)"""
    out = {"available": True, "markets": {}}

    for market_code, market_name, api_code in [
        ("KOSPI", "코스피", "KOSPI"),
        ("KOSDAQ", "코스닥", "KOSDAQ"),
    ]:
        url = f"https://m.stock.naver.com/api/index/{api_code}/investors"
        try:
            r = requests.get(
                url,
                headers={"User-Agent": MOBILE_UA, "Accept": "application/json"},
                timeout=10,
            )
            if r.status_code != 200:
                print(f"[FLOW/{market_code}] HTTP {r.status_code}", file=sys.stderr)
                continue
            data = r.json()
            if not isinstance(data, list):
                continue

            recs = []
            for row in data[:5]:
                if not isinstance(row, dict):
                    continue
                # 네이버 API: localTradedAt(YYYYMMDD), foreigner, institution, individual (단위: 백만원으로 추정)
                date_raw = str(row.get("localTradedAt", ""))
                if len(date_raw) == 8:
                    date_str = f"{date_raw[4:6]}.{date_raw[6:8]}"
                else:
                    date_str = date_raw

                recs.append({
                    "date": date_str,
                    "foreign": int(row.get("foreigner", 0) or 0),
                    "institution": int(row.get("institution", 0) or 0),
                    "individual": int(row.get("individual", 0) or 0),
                })
            if recs:
                out["markets"][market_name] = recs
        except Exception as e:
            print(f"[FLOW/{market_code}] {e}", file=sys.stderr)

    if not out["markets"]:
        out["available"] = False
        out["reason"] = "네이버 응답 없음"
    return out


# =============================================================================
# ★ v5: 업종지수 — 네이버 sise_group 크롤링
# =============================================================================
def _parse_pct(text: str):
    """+1.23% 또는 -1.23% 같은 문자열 → float"""
    if not text:
        return 0.0
    m = re.search(r"([+-]?\d+\.?\d*)", text.replace(",", ""))
    if m:
        return float(m.group(1))
    return 0.0


def _parse_int(text: str):
    """천 단위 콤마 있는 숫자 → int"""
    if not text:
        return 0
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else 0


def fetch_sector_indices() -> dict:
    """KOSPI 업종 시세 — 네이버 sise_group?type=upjong"""
    url = "https://finance.naver.com/sise/sise_group.naver?type=upjong"
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
        r.encoding = "euc-kr"
        soup = BeautifulSoup(r.text, "lxml")

        sectors = []
        table = soup.select_one("table.type_1")
        if not table:
            return {"available": False, "reason": "테이블 없음"}

        for row in table.select("tbody tr"):
            tds = row.select("td")
            if len(tds) < 5:
                continue
            name_a = tds[0].select_one("a")
            if not name_a:
                continue

            name = name_a.get_text(strip=True)
            change_text = tds[1].get_text(strip=True)  # 전일대비 (%)
            # 보통: 컬럼 = 업종명 / 전일대비 / 상승종목수 / 보합 / 하락
            sectors.append({
                "code": "",
                "name": name,
                "close": 0,
                "change_pct": _parse_pct(change_text),
                "value": 0,  # 네이버 업종 페이지는 거래대금 미제공 → 별도 처리
            })

        # 등락률 정렬
        sectors.sort(key=lambda s: -s["change_pct"])

        if not sectors:
            return {"available": False, "reason": "업종 데이터 없음"}
        return {"available": True, "sectors": sectors}
    except Exception as e:
        print(f"[SECTOR] {e}", file=sys.stderr)
        return {"available": False, "reason": str(e)}


# =============================================================================
# ★ v5: 시장 키워드 — 네이버 거래대금/등락률 TOP 크롤링
# =============================================================================
def fetch_top_value_stocks(limit: int = 10) -> list[dict]:
    """거래대금 상위 — 네이버 sise_quant.naver (코스피+코스닥 통합)"""
    rows = []
    # sosok=0 (KOSPI), sosok=1 (KOSDAQ)
    for sosok, market_name in [("0", "KOSPI"), ("1", "KOSDAQ")]:
        url = f"https://finance.naver.com/sise/sise_quant.naver?sosok={sosok}&page=1"
        try:
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
            r.encoding = "euc-kr"
            soup = BeautifulSoup(r.text, "lxml")
            table = soup.select_one("table.type_2")
            if not table:
                continue
            for tr in table.select("tbody tr"):
                tds = tr.select("td")
                if len(tds) < 11:
                    continue
                name_a = tds[1].select_one("a")
                if not name_a:
                    continue
                href = name_a.get("href", "")
                m = re.search(r"code=(\d+)", href)
                ticker = m.group(1) if m else ""

                rows.append({
                    "ticker": ticker,
                    "name": name_a.get_text(strip=True),
                    "market": market_name,
                    "close": _parse_int(tds[2].get_text(strip=True)),
                    "change_pct": _parse_pct(tds[4].get_text(strip=True)),
                    "value": _parse_int(tds[7].get_text(strip=True)) * 1_000_000,  # 거래대금(백만원) → 원
                })
        except Exception as e:
            print(f"[TOP_VALUE/{sosok}] {e}", file=sys.stderr)
    rows.sort(key=lambda r: -r["value"])
    return rows[:limit]


def fetch_top_change_stocks(limit: int = 5, ascending: bool = False) -> list[dict]:
    """상승률/하락률 상위 — 네이버 sise_rise/fall (코스피+코스닥 통합)"""
    rows = []
    page_name = "sise_fall.naver" if ascending else "sise_rise.naver"
    for sosok, market_name in [("0", "KOSPI"), ("1", "KOSDAQ")]:
        url = f"https://finance.naver.com/sise/{page_name}?sosok={sosok}&page=1"
        try:
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
            r.encoding = "euc-kr"
            soup = BeautifulSoup(r.text, "lxml")
            table = soup.select_one("table.type_2")
            if not table:
                continue
            for tr in table.select("tbody tr"):
                tds = tr.select("td")
                if len(tds) < 11:
                    continue
                name_a = tds[1].select_one("a")
                if not name_a:
                    continue
                href = name_a.get("href", "")
                m = re.search(r"code=(\d+)", href)
                ticker = m.group(1) if m else ""

                rows.append({
                    "ticker": ticker,
                    "name": name_a.get_text(strip=True),
                    "market": market_name,
                    "close": _parse_int(tds[2].get_text(strip=True)),
                    "change_pct": _parse_pct(tds[4].get_text(strip=True)),
                    "value": _parse_int(tds[7].get_text(strip=True)) * 1_000_000,
                })
        except Exception as e:
            print(f"[TOP_CHG/{sosok}/{page_name}] {e}", file=sys.stderr)
    # 절댓값 큰 순 (상승: 큰 양수가 앞 / 하락: 큰 음수가 앞)
    rows.sort(key=lambda r: r["change_pct"] if ascending else -r["change_pct"])
    return rows[:limit]


def fetch_naver_themes(limit: int = 10) -> list[dict]:
    """네이버 금융 테마 등락률 상위 — 기존 유지"""
    try:
        url = "https://finance.naver.com/sise/theme.naver?&page=1"
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
        r.encoding = "euc-kr"
        soup = BeautifulSoup(r.text, "lxml")

        themes = []
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

                change_text = None
                for td in tds[1:4]:
                    txt = td.get_text(strip=True).replace(" ", "")
                    if "%" in txt:
                        change_text = txt
                        break

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
                "headlines": fetch_news_us(ticker, limit=2),  # ★ 한글 뉴스
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
        # 한국 종목 최종 폴백: 네이버 (신규 ETF/ETN 대응)
        if price is None and h.get("market") == "KR" and h.get("fdr"):
            price = fetch_price_naver(h["fdr"])
            if price is not None:
                print(f"[NAVER_PRICE/{h['fdr']}] {h['name']} = {price}")

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

        hist = fetch_history(h.get("yahoo"), h.get("fdr"))
        ma = compute_moving_averages(hist)
        if ma:
            ma_map[h["name"]] = ma

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
    print(f"=== {datetime.now(KST).isoformat()} 갱신 시작 (v5) ===")

    cfg = json.loads((ROOT / "portfolio_data.json").read_text(encoding="utf-8"))

    fx = fetch_fx_usd_krw()
    kospi_close, kospi_chg = fetch_kospi()
    print(f"[FX] {fx:.2f} / [KOSPI] {kospi_close} ({kospi_chg})")

    holdings, news_map, ma_map = enrich_holdings(cfg["holdings"])
    accounts = build_account_summary(holdings, fx)

    print("[M7] 수집 (한글 뉴스 포함)...")
    m7 = fetch_m7_daily()

    print("[FLOW] 수집 (네이버 모바일 API)...")
    flow = fetch_market_flow()

    print("[SECTOR] 수집 (네이버 sise_group)...")
    sectors = fetch_sector_indices()

    print("[KEYWORDS] 수집 (네이버 sise_quant/rise/fall + theme)...")
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
