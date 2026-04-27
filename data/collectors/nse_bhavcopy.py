"""
NSE Bhavcopy collector — PRIMARY data source for EOD OHLCV.

Downloads the official NSE end-of-day CSV for a given date.
URL pattern: https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_DDMMYYYY.csv

NSE requires browser-like headers and a primed session cookie. The session is
reused across calls to avoid hitting the site too frequently.
"""

from datetime import date, datetime, timedelta
from io import StringIO

import pandas as pd
import requests
from loguru import logger
from sqlalchemy.dialects.sqlite import insert
from tenacity import retry, stop_after_attempt, wait_exponential

from config.nse_calendar import is_trading_day, trading_days_between
from config.settings import BANKING_STOCKS, DATA_BACKFILL_YEARS, NSE_BHAVCOPY_URL
from data.storage.database import OHLCVDaily, get_session

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
}

_session: requests.Session | None = None


def _get_session() -> requests.Session:
    """Return a primed requests.Session with NSE cookies."""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(_HEADERS)
        try:
            # Prime the session — NSE requires a visit to the main page for cookies
            _session.get("https://www.nseindia.com", timeout=15)
        except Exception as exc:
            logger.warning(f"Could not prime NSE session: {exc}")
    return _session


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def download_bhavcopy(trading_date: date) -> pd.DataFrame | None:
    """
    Download and parse the Bhavcopy CSV for one trading date.
    Returns a DataFrame filtered to BANKING_STOCKS, or None on failure.
    """
    date_str = trading_date.strftime("%d%m%Y")   # DDMMYYYY format required by NSE
    url = NSE_BHAVCOPY_URL.format(date=date_str)

    try:
        resp = _get_session().get(url, timeout=20)
        if resp.status_code == 404:
            logger.warning(f"Bhavcopy not found for {trading_date} (market may have been closed)")
            return None
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error(f"Bhavcopy download failed for {trading_date}: {exc}")
        raise   # tenacity will retry

    try:
        df = pd.read_csv(StringIO(resp.text))
        df.columns = df.columns.str.strip()

        # Keep equity series only; drop F&O, SME, etc.
        if "SERIES" in df.columns:
            df = df[df["SERIES"].str.strip() == "EQ"]

        # Normalize the symbol column name (varies slightly across Bhavcopy versions)
        sym_col = next((c for c in df.columns if c.upper() in ("SYMBOL", "SCRIPCODE")), None)
        if sym_col is None:
            logger.error(f"Bhavcopy for {trading_date}: cannot find SYMBOL column. Columns: {df.columns.tolist()}")
            return None

        df = df[df[sym_col].str.strip().isin(BANKING_STOCKS)].copy()
        if df.empty:
            logger.warning(f"Bhavcopy for {trading_date}: none of the banking stocks found")
            return None

        # Handle both Bhavcopy column formats:
        #   sec_bhavdata_full: OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, TTL_TRD_QNTY
        #   older CM Bhavcopy: OPEN, HIGH, LOW, CLOSE, TOTTRDQTY
        cols_upper = {c.strip().upper(): c.strip() for c in df.columns}
        col_map = {sym_col: "symbol"}
        for target, candidates in {
            "open":   ["OPEN_PRICE",  "OPEN"],
            "high":   ["HIGH_PRICE",  "HIGH"],
            "low":    ["LOW_PRICE",   "LOW"],
            "close":  ["CLOSE_PRICE", "CLOSE"],
            "volume": ["TTL_TRD_QNTY", "TOTTRDQTY"],
        }.items():
            for c in candidates:
                if c in cols_upper:
                    col_map[cols_upper[c]] = target
                    break
        df = df.rename(columns=col_map)
        df["symbol"] = df["symbol"].str.strip()
        df["date"]   = trading_date
        return df[["symbol", "date", "open", "high", "low", "close", "volume"]]

    except Exception as exc:
        logger.error(f"Bhavcopy parse error for {trading_date}: {exc}")
        return None


def store_bhavcopy(df: pd.DataFrame) -> int:
    """Upsert Bhavcopy rows; Bhavcopy always wins over Groww/yfinance."""
    if df is None or df.empty:
        return 0

    now = datetime.utcnow()
    rows = []
    for _, row in df.iterrows():
        rows.append({
            "symbol":             str(row["symbol"]),
            "date":               row["date"],
            "open":               float(row["open"])   if pd.notna(row.get("open"))   else None,
            "high":               float(row["high"])   if pd.notna(row.get("high"))   else None,
            "low":                float(row["low"])    if pd.notna(row.get("low"))    else None,
            "close":              float(row["close"])  if pd.notna(row.get("close"))  else None,
            "adjusted_close":     float(row["close"])  if pd.notna(row.get("close"))  else None,
            "volume":             int(row["volume"])   if pd.notna(row.get("volume")) else None,
            "source":             "nse_bhavcopy",
            "is_adjusted":        False,   # raw price; adjusted_close updated by corporate_actions.py
            "needs_verification": False,
            "collected_at":       now,
        })

    with get_session() as session:
        stmt = insert(OHLCVDaily).values(rows)
        # Bhavcopy always overwrites — it is the source of truth
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol", "date"],
            set_={k: stmt.excluded[k] for k in rows[0] if k not in ("symbol", "date")},
        )
        session.execute(stmt)
        session.commit()

    return len(rows)


def run_daily(trading_date: date | None = None) -> None:
    """Download and store today's (or given date's) Bhavcopy."""
    d = trading_date or date.today()
    if not is_trading_day(d):
        logger.info(f"Bhavcopy: {d} is not a trading day — skipping")
        return
    df = download_bhavcopy(d)
    n  = store_bhavcopy(df)
    logger.info(f"Bhavcopy {d}: stored {n} rows")


def backfill_history(years: int = DATA_BACKFILL_YEARS) -> None:
    """
    Download Bhavcopy for every trading day in the past `years` years.
    Skips dates already present in ohlcv_daily with source='nse_bhavcopy'.
    Safe to run repeatedly — already-stored dates are skipped.
    """
    end   = date.today() - timedelta(days=1)
    start = date(end.year - years, end.month, end.day)
    days  = trading_days_between(start, end)

    # Find already-stored dates to avoid redundant downloads
    with get_session() as session:
        stored = {
            r[0] for r in session.execute(
                __import__("sqlalchemy").text(
                    "SELECT date FROM ohlcv_daily WHERE source='nse_bhavcopy'"
                )
            ).fetchall()
        }

    missing = [d for d in days if d not in stored]
    logger.info(f"Bhavcopy backfill: {len(missing)} dates to fetch over {years} years")

    for i, d in enumerate(missing, 1):
        df = download_bhavcopy(d)
        n  = store_bhavcopy(df)
        if n:
            logger.info(f"  [{i}/{len(missing)}] {d}: {n} rows")
