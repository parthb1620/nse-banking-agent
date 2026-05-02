"""
NSE corporate announcements collector.

Fetches filings (results, board meetings, shareholding, etc.) from the NSE API.
NSE requires a browser-like session with cookies before API calls will succeed.

Sets usable_from = next trading-day open after published_at.
"""

from datetime import date, datetime

import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import ALL_STOCKS
from data.quality.known_time import compute_usable_from
from data.storage.database import CorporateFiling, get_session

_NSE_HOME    = "https://www.nseindia.com"
_NSE_API_URL = (
    "https://www.nseindia.com/api/corporate-announcements"
    "?index=equities&symbol={symbol}&from_date={from_date}&to_date={to_date}"
)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/",
}

_nse_session: requests.Session | None = None


def _get_nse_session() -> requests.Session:
    global _nse_session
    if _nse_session is None:
        _nse_session = requests.Session()
        _nse_session.headers.update(_HEADERS)
        try:
            _nse_session.get(_NSE_HOME, timeout=15)
            # Second request primes the AJAX cookies NSE requires
            _nse_session.get("https://www.nseindia.com/get-quotes/equity?symbol=HDFCBANK", timeout=10)
        except Exception as exc:
            logger.warning(f"NSE session priming had an error (non-fatal): {exc}")
    return _nse_session


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=3, max=15))
def _fetch_announcements(symbol: str, from_date: date, to_date: date) -> list[dict]:
    """Fetch raw announcement list from NSE API."""
    sess = _get_nse_session()
    url  = _NSE_API_URL.format(
        symbol=symbol,
        from_date=from_date.strftime("%d-%m-%Y"),
        to_date=to_date.strftime("%d-%m-%Y"),
    )
    resp = sess.get(url, timeout=20)
    if resp.status_code == 401:
        # Session expired — reset and retry
        global _nse_session
        _nse_session = None
        raise requests.HTTPError("NSE session expired, resetting")
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else data.get("data", [])


def _parse_filing(item: dict, symbol: str, now: datetime) -> CorporateFiling | None:
    """Convert a raw NSE announcement dict into a CorporateFiling model."""
    subject = (item.get("subject") or item.get("desc") or "")[:500]
    if not subject:
        return None

    # NSE datetime strings are typically "DD-Mon-YYYY HH:MM:SS"
    pub_str = item.get("bm_timestamp") or item.get("date") or item.get("an_dt")
    published_at = None
    if pub_str:
        for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y", "%Y-%m-%dT%H:%M:%S"):
            try:
                published_at = datetime.strptime(pub_str.strip(), fmt)
                break
            except ValueError:
                pass

    event_date_str = item.get("date") or item.get("sm_dt")
    event_date = None
    if event_date_str:
        for fmt in ("%d-%b-%Y", "%Y-%m-%d"):
            try:
                event_date = datetime.strptime(event_date_str.strip(), fmt).date()
                break
            except ValueError:
                pass

    usable_from = compute_usable_from(published_at or datetime.combine(event_date or date.today(), datetime.min.time()))

    return CorporateFiling(
        symbol=symbol,
        event_date=event_date,
        category=item.get("sm_name") or item.get("category"),
        subject=subject,
        content=item.get("attchmntText") or item.get("body") or None,
        published_at=published_at,
        collected_at=now,
        usable_from=usable_from,
    )


def fetch_and_store(symbol: str, from_date: date, to_date: date | None = None) -> int:
    """
    Fetch NSE filings for one symbol over a date range and store new ones.
    Returns number of new filings inserted.
    """
    to_date = to_date or date.today()

    try:
        raw = _fetch_announcements(symbol, from_date, to_date)
    except Exception as exc:
        logger.error(f"NSE filings fetch failed for {symbol}: {exc}")
        return 0

    now      = datetime.utcnow()
    inserted = 0

    with get_session() as session:
        for item in raw:
            filing = _parse_filing(item, symbol, now)
            if not filing:
                continue

            # Deduplicate by subject + published_at
            exists = session.query(CorporateFiling).filter_by(
                symbol=symbol,
                subject=filing.subject,
                published_at=filing.published_at,
            ).first()
            if exists:
                continue

            session.add(filing)
            inserted += 1

        session.commit()

    if inserted:
        logger.info(f"NSE filings: inserted {inserted} new records for {symbol} ({from_date} – {to_date})")
    return inserted


def run_all(days_back: int = 365) -> None:
    """Fetch recent filings for all tracked stocks."""
    from datetime import timedelta
    from_date = date.today() - timedelta(days=days_back)
    for symbol in ALL_STOCKS:
        fetch_and_store(symbol, from_date)
