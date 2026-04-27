"""
yfinance fallback collector — use ONLY when NSE Bhavcopy and Groww both fail.

Every row stored by this module is flagged needs_verification=True because
yfinance retrospective split/dividend adjustments can be wrong. Always
cross-check against Bhavcopy when possible.
"""

from datetime import date, datetime

import pandas as pd
import yfinance as yf
from loguru import logger
from sqlalchemy.dialects.sqlite import insert

from config.settings import BANKING_STOCKS_YF
from data.storage.database import OHLCVDaily, get_session

_YF_TO_SYMBOL = {f"{s}.NS": s for s in [t.replace(".NS", "") for t in BANKING_STOCKS_YF]}


def fetch_yfinance(symbol_ns: str, start: date, end: date) -> pd.DataFrame:
    """
    Download OHLCV for a single .NS symbol from yfinance.
    Returns empty DataFrame on failure.
    """
    try:
        df = yf.download(symbol_ns, start=str(start), end=str(end), auto_adjust=True, progress=False)
        if df.empty:
            logger.warning(f"yfinance: no data for {symbol_ns} {start}–{end}")
            return pd.DataFrame()
        df = df.reset_index()
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
        df = df.rename(columns={
            "Date": "date",
            "Open": "open", "High": "high", "Low": "low", "Close": "close",
            "Volume": "volume",
        })
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["adjusted_close"] = df["close"]   # auto_adjust=True means close IS the adjusted close
        return df[["date", "open", "high", "low", "close", "volume", "adjusted_close"]]
    except Exception as exc:
        logger.error(f"yfinance fetch failed for {symbol_ns}: {exc}")
        return pd.DataFrame()


def store_yfinance(symbol: str, df: pd.DataFrame) -> int:
    """
    Upsert rows into ohlcv_daily with source='yfinance', needs_verification=True.
    Returns number of rows inserted/updated.
    """
    if df.empty:
        return 0

    now = datetime.utcnow()
    rows = []
    for _, row in df.iterrows():
        rows.append({
            "symbol":             symbol,
            "date":               row["date"],
            "open":               float(row["open"])          if pd.notna(row["open"])   else None,
            "high":               float(row["high"])          if pd.notna(row["high"])   else None,
            "low":                float(row["low"])           if pd.notna(row["low"])    else None,
            "close":              float(row["close"])         if pd.notna(row["close"])  else None,
            "volume":             int(row["volume"])          if pd.notna(row["volume"]) else None,
            "adjusted_close":     float(row["adjusted_close"]) if pd.notna(row["adjusted_close"]) else None,
            "source":             "yfinance",
            "is_adjusted":        True,
            "needs_verification": True,
            "collected_at":       now,
        })

    with get_session() as session:
        stmt = insert(OHLCVDaily).values(rows)
        # On conflict: only update if existing source is also yfinance (don't overwrite Bhavcopy/Groww)
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol", "date"],
            set_={k: stmt.excluded[k] for k in rows[0] if k not in ("symbol", "date")},
            where=(OHLCVDaily.__table__.c.source == "yfinance"),
        )
        session.execute(stmt)
        session.commit()

    logger.info(f"yfinance: stored {len(rows)} rows for {symbol}")
    return len(rows)


def backfill(symbol: str, start: date, end: date) -> int:
    """Fetch and store yfinance data for one symbol. Returns rows stored."""
    symbol_ns = f"{symbol}.NS"
    df = fetch_yfinance(symbol_ns, start, end)
    return store_yfinance(symbol, df)
