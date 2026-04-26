"""
Screener.in fundamentals collector.

Fetches quarterly and annual P&L / balance sheet data by scraping
the Screener.in company page (rate-limited to 1 request per 2 seconds).

Sets usable_from = next trading-day open after the result announcement date.
If the announcement date is unknown, usable_from defaults to the period_end_date
+ 45 days (conservative estimate for when results are typically out).
"""

import time
from datetime import date, datetime, timedelta

import requests
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from config.nse_calendar import next_trading_day
from config.settings import BANKING_STOCKS, SCREENER_BASE_URL, SCREENER_DELAY_SEC
from data.quality.known_time import compute_usable_from
from data.storage.database import Fundamental, get_session

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.screener.in/",
}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=15))
def _fetch_page(symbol: str) -> BeautifulSoup | None:
    """Fetch Screener.in company page. Returns parsed HTML or None."""
    url = f"{SCREENER_BASE_URL}/company/{symbol}/"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        if resp.status_code == 404:
            logger.warning(f"Screener.in: {symbol} not found at {url}")
            return None
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except Exception as exc:
        logger.error(f"Screener.in fetch failed for {symbol}: {exc}")
        raise


def _parse_number(text: str) -> float | None:
    """Parse a financial number string like '12,345.67' or '1,234 Cr'."""
    if not text:
        return None
    clean = text.replace(",", "").replace("Cr", "").replace("cr", "").strip()
    try:
        return float(clean)
    except ValueError:
        return None


def _parse_quarterly_table(soup: BeautifulSoup) -> list[dict]:
    """
    Parse the quarterly results table from Screener.in.
    Returns list of period dicts.
    """
    results = []
    table = soup.find("section", id="quarters")
    if not table:
        return results

    thead = table.find("thead")
    if not thead:
        return results

    # Column headers are quarter-end dates like "Mar 2024", "Dec 2023", etc.
    headers = [th.get_text(strip=True) for th in thead.find_all("th")]
    period_dates = headers[1:]   # first column is the row label

    # Build a map: row_label → [value_per_period]
    row_data: dict[str, list[str]] = {}
    tbody = table.find("tbody")
    if tbody:
        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True)
            values = [c.get_text(strip=True) for c in cells[1:]]
            row_data[label] = values

    for i, period_str in enumerate(period_dates):
        try:
            # Parse "Mar 2024" → date(2024, 3, 31)
            parts = period_str.split()
            if len(parts) != 2:
                continue
            month_map = {
                "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
            }
            month = month_map.get(parts[0])
            year  = int(parts[1])
            if not month:
                continue
            # Last day of the quarter month
            last_day = (date(year, month % 12 + 1, 1) - timedelta(days=1)) if month < 12 else date(year, 12, 31)

            def _get(label_substr: str) -> float | None:
                for k, vals in row_data.items():
                    if label_substr.lower() in k.lower() and i < len(vals):
                        return _parse_number(vals[i])
                return None

            results.append({
                "period_end_date": last_day,
                "period_type":     "Q",
                "revenue":         _get("Sales") or _get("Revenue") or _get("Net Interest Income"),
                "pat":             _get("Net Profit") or _get("PAT"),
                "ebitda":          _get("Operating Profit") or _get("EBITDA"),
                "eps":             _get("EPS"),
            })
        except Exception as exc:
            logger.debug(f"Could not parse period '{period_str}': {exc}")

    return results


def fetch_and_store(symbol: str) -> int:
    """
    Fetch Screener.in data and store quarterly fundamentals.
    Returns number of new rows inserted.
    """
    soup = _fetch_page(symbol)
    if not soup:
        return 0

    time.sleep(SCREENER_DELAY_SEC)   # be respectful to Screener.in

    periods = _parse_quarterly_table(soup)
    if not periods:
        logger.warning(f"Screener.in: no quarterly data parsed for {symbol}")
        return 0

    now      = datetime.utcnow()
    inserted = 0

    with get_session() as session:
        for p in periods:
            exists = session.query(Fundamental).filter_by(
                symbol=symbol,
                period_end_date=p["period_end_date"],
                period_type=p["period_type"],
            ).first()
            if exists:
                continue

            # Conservative usable_from: period end + 45 days (results usually out within 45 days)
            # Will be updated to the actual announcement time when NSE filing is collected
            announced_estimate = datetime.combine(
                p["period_end_date"] + timedelta(days=45), datetime.min.time()
            )
            usable_from = compute_usable_from(announced_estimate)

            session.add(Fundamental(
                symbol=symbol,
                period_end_date=p["period_end_date"],
                period_type=p["period_type"],
                revenue=p.get("revenue"),
                pat=p.get("pat"),
                ebitda=p.get("ebitda"),
                eps=p.get("eps"),
                published_at=now,
                collected_at=now,
                usable_from=usable_from,
            ))
            inserted += 1

        session.commit()

    logger.info(f"Screener.in: inserted {inserted} fundamental rows for {symbol}")
    return inserted


def run_all() -> None:
    """Fetch fundamentals for all tracked stocks."""
    for symbol in BANKING_STOCKS:
        fetch_and_store(symbol)
