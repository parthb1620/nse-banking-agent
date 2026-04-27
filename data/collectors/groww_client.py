"""
Groww API client — SECONDARY data source (historical OHLCV + portfolio).

Auth: Bearer JWT in GROWW_API_KEY.
Endpoint paths are based on Groww Developer API v1 documentation.
If any endpoint returns 404, check your Groww API dashboard for the current path.
"""

from datetime import date, datetime

import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import GROWW_API_KEY, GROWW_BASE_URL
from data.storage.database import OHLCVDaily, get_session

# Groww API uses ISIN for historical data lookups; symbol → ISIN map is populated
# on first call via the search endpoint.
_SYMBOL_TO_ISIN: dict[str, str] = {}

_HEADERS = {
    "Authorization": f"Bearer {GROWW_API_KEY}",
    "Content-Type":  "application/json",
    "Accept":        "application/json",
}


def _client() -> httpx.Client:
    return httpx.Client(base_url=GROWW_BASE_URL, headers=_HEADERS, timeout=30)


# ── Symbol → ISIN resolution ───────────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def resolve_isin(symbol: str) -> str | None:
    """Look up ISIN for an NSE symbol via Groww search API."""
    if symbol in _SYMBOL_TO_ISIN:
        return _SYMBOL_TO_ISIN[symbol]
    try:
        with _client() as c:
            resp = c.get("/v1/search", params={"query": symbol, "type": "EQUITY"})
            resp.raise_for_status()
            data = resp.json()
            for item in data.get("data", {}).get("stocks", []):
                if item.get("nse_scrip_code") == symbol or item.get("symbol") == symbol:
                    isin = item.get("isin") or item.get("isinCode")
                    if isin:
                        _SYMBOL_TO_ISIN[symbol] = isin
                        return isin
    except Exception as exc:
        logger.error(f"Groww ISIN lookup failed for {symbol}: {exc}")
    return None


# ── Historical OHLCV ───────────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_historical(symbol: str, start: date, end: date) -> list[dict]:
    """
    Fetch daily OHLCV candles from Groww for one symbol.
    Returns list of dicts with keys: date, open, high, low, close, volume.
    """
    isin = resolve_isin(symbol)
    if not isin:
        logger.warning(f"Groww: cannot resolve ISIN for {symbol}, skipping")
        return []

    try:
        with _client() as c:
            resp = c.get(
                f"/v1/historical-data/candle/{isin}",
                params={
                    "interval": "1d",
                    "from":     start.strftime("%Y-%m-%d"),
                    "to":       end.strftime("%Y-%m-%d"),
                },
            )
            resp.raise_for_status()
            candles = resp.json().get("data", {}).get("candles", [])

        rows = []
        for c in candles:
            # Groww candle format: [timestamp, open, high, low, close, volume]
            if len(c) < 6:
                continue
            ts = c[0]
            rows.append({
                "date":   datetime.fromisoformat(ts).date() if isinstance(ts, str) else date.fromtimestamp(ts / 1000),
                "open":   float(c[1]),
                "high":   float(c[2]),
                "low":    float(c[3]),
                "close":  float(c[4]),
                "volume": int(c[5]),
            })
        return rows

    except Exception as exc:
        logger.error(f"Groww historical fetch failed for {symbol} {start}–{end}: {exc}")
        raise


def store_historical(symbol: str, rows: list[dict]) -> int:
    """
    Upsert Groww candles into ohlcv_daily.
    Does NOT overwrite rows that already have source='nse_bhavcopy'.
    """
    if not rows:
        return 0

    from sqlalchemy.dialects.sqlite import insert

    now = datetime.utcnow()
    records = [{
        "symbol":             symbol,
        "date":               r["date"],
        "open":               r.get("open"),
        "high":               r.get("high"),
        "low":                r.get("low"),
        "close":              r.get("close"),
        "adjusted_close":     r.get("close"),
        "volume":             r.get("volume"),
        "source":             "groww",
        "is_adjusted":        False,
        "needs_verification": False,
        "collected_at":       now,
    } for r in rows]

    with get_session() as session:
        stmt = insert(OHLCVDaily).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol", "date"],
            set_={k: stmt.excluded[k] for k in records[0] if k not in ("symbol", "date")},
            # Only overwrite if not already stored from Bhavcopy (higher priority)
            where=(OHLCVDaily.__table__.c.source != "nse_bhavcopy"),
        )
        session.execute(stmt)
        session.commit()

    logger.info(f"Groww: stored {len(records)} rows for {symbol}")
    return len(records)


# ── Portfolio ──────────────────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_portfolio() -> list[dict]:
    """
    Fetch current portfolio holdings from Groww.
    Used in Phase 6 for paper-trade reconciliation when moving to live.
    Returns list of holding dicts.
    """
    try:
        with _client() as c:
            resp = c.get("/v1/portfolio/holdings")
            resp.raise_for_status()
            return resp.json().get("data", {}).get("holdings", [])
    except Exception as exc:
        logger.error(f"Groww portfolio fetch failed: {exc}")
        raise
