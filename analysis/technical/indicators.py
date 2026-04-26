"""
Technical indicators — computed on adjusted_close only.

All data loaded with date <= as_of_date so no future data leaks in.
Uses pandas-ta for all indicator maths.
"""

from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd
import pandas_ta as ta
from loguru import logger

from config.settings import (
    ADX_PERIOD, ATR_PERIOD, BB_PERIOD, BB_STD,
    EMA_PERIODS, MACD_FAST, MACD_SIGNAL, MACD_SLOW, RSI_PERIOD,
)
from data.storage.database import OHLCVDaily, get_session

# ── Column finder helper ───────────────────────────────────────────────────────

def _col(df: pd.DataFrame, *prefixes: str) -> Optional[str]:
    """Return the first column name that starts with any of the given prefixes."""
    for p in prefixes:
        matches = [c for c in df.columns if str(c).startswith(p)]
        if matches:
            return matches[0]
    return None


# ── Data loader ────────────────────────────────────────────────────────────────

def load_ohlcv(symbol: str, as_of_date: Optional[date] = None) -> pd.DataFrame:
    """
    Load adjusted OHLCV from DB.
    as_of_date: only include rows with date <= as_of_date (enforces no lookahead).
    Returns DataFrame indexed by date, sorted ascending.
    Returns empty DataFrame if fewer than 30 rows available.
    """
    with get_session() as session:
        q = session.query(OHLCVDaily).filter(OHLCVDaily.symbol == symbol)
        if as_of_date:
            q = q.filter(OHLCVDaily.date <= as_of_date)
        rows = q.order_by(OHLCVDaily.date.asc()).all()

    if len(rows) < 30:
        logger.warning(f"indicators: only {len(rows)} rows for {symbol} — need ≥ 30")
        return pd.DataFrame()

    df = pd.DataFrame([{
        "date":           r.date,
        "open":           r.open,
        "high":           r.high,
        "low":            r.low,
        "close":          r.close,
        "volume":         float(r.volume) if r.volume else 0.0,
        "adjusted_close": r.adjusted_close if r.adjusted_close is not None else r.close,
    } for r in rows])

    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    df = df.dropna(subset=["adjusted_close"])
    return df


# ── Indicator computation ──────────────────────────────────────────────────────

def compute_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add all technical indicators to a OHLCV DataFrame.
    Input must have columns: open, high, low, adjusted_close, volume.
    All indicators use adjusted_close as the price series.
    Returns the DataFrame with new indicator columns appended.
    """
    if df.empty or len(df) < 20:
        return df

    df = df.copy()
    close  = df["adjusted_close"]
    high   = df["high"]
    low    = df["low"]
    volume = df["volume"]

    # EMA 9, 21, 50, 200
    for p in EMA_PERIODS:
        df[f"ema_{p}"] = ta.ema(close, length=p)

    # RSI
    df["rsi"] = ta.rsi(close, length=RSI_PERIOD)

    # MACD — flatten into 3 columns
    macd_df = ta.macd(close, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL)
    if macd_df is not None:
        df["macd"]        = macd_df[_col(macd_df, "MACD_")]
        df["macd_signal"] = macd_df[_col(macd_df, "MACDs_")]
        df["macd_hist"]   = macd_df[_col(macd_df, "MACDh_")]

    # Bollinger Bands
    bb_df = ta.bbands(close, length=BB_PERIOD, std=BB_STD)
    if bb_df is not None:
        df["bb_upper"] = bb_df[_col(bb_df, "BBU_")]
        df["bb_mid"]   = bb_df[_col(bb_df, "BBM_")]
        df["bb_lower"] = bb_df[_col(bb_df, "BBL_")]
        df["bb_pct"]   = bb_df[_col(bb_df, "BBP_")]  # 0=at lower, 1=at upper band

    # ATR
    atr_s = ta.atr(high, low, close, length=ATR_PERIOD)
    if atr_s is not None:
        df["atr"] = atr_s

    # OBV
    df["obv"] = ta.obv(close, volume)

    # ADX
    adx_df = ta.adx(high, low, close, length=ADX_PERIOD)
    if adx_df is not None:
        df["adx"] = adx_df[_col(adx_df, "ADX_")]
        df["dmp"] = adx_df[_col(adx_df, "DMP_")]   # +DI (buyers)
        df["dmn"] = adx_df[_col(adx_df, "DMN_")]   # -DI (sellers)

    # Rolling 20-day VWAP — approximation for daily timeframe
    # Institutional traders use VWAP to benchmark entries; price above = bullish
    typical = (df["high"] + df["low"] + df["adjusted_close"]) / 3
    df["vwap_20"] = (typical * volume).rolling(20).sum() / volume.rolling(20).sum()

    # 10-day OBV slope (positive = accumulation, negative = distribution)
    df["obv_slope"] = df["obv"].diff(10)

    return df


# ── Main entry points ──────────────────────────────────────────────────────────

def get_indicators(symbol: str, as_of_date: Optional[date] = None) -> pd.DataFrame:
    """Load OHLCV and compute all indicators. Returns full DataFrame."""
    df = load_ohlcv(symbol, as_of_date)
    if df.empty:
        return df
    return compute_all(df)


def get_latest_row(symbol: str, as_of_date: Optional[date] = None) -> dict:
    """
    Return the most recent row of indicators as a plain dict.
    Returns empty dict if no data.
    """
    df = get_indicators(symbol, as_of_date)
    if df.empty:
        return {}
    row = df.iloc[-1].to_dict()
    row["date"] = df.index[-1].date()
    return row
