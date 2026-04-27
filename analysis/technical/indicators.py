"""
Technical indicators — computed on adjusted_close only.

All data loaded with date <= as_of_date so no future data leaks in.
Uses pure pandas/numpy only — no pandas-ta, no numba, no LLVM required.
"""

from datetime import date
from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger

from config.settings import (
    ADX_PERIOD, ATR_PERIOD, BB_PERIOD, BB_STD,
    EMA_PERIODS, MACD_FAST, MACD_SIGNAL, MACD_SLOW, RSI_PERIOD,
)
from data.storage.database import OHLCVDaily, get_session


# ── Pure-pandas indicator implementations ─────────────────────────────────────

def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def _rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=length - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=length - 1, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def _macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """Returns (macd_line, signal_line, histogram) as three Series."""
    ema_fast   = _ema(series, fast)
    ema_slow   = _ema(series, slow)
    macd_line  = ema_fast - ema_slow
    signal_line = _ema(macd_line, signal)
    histogram   = macd_line - signal_line
    return macd_line, signal_line, histogram


def _bbands(series: pd.Series, length: int = 20, std: float = 2.0):
    """Returns (upper, mid, lower, pct_b) as four Series."""
    mid   = series.rolling(length).mean()
    sigma = series.rolling(length).std(ddof=0)
    upper = mid + std * sigma
    lower = mid - std * sigma
    pct_b = (series - lower) / (upper - lower).replace(0, np.nan)
    return upper, mid, lower, pct_b


def _true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> pd.Series:
    tr = _true_range(high, low, close)
    return tr.ewm(com=length - 1, adjust=False).mean()


def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff()).fillna(0)
    return (direction * volume).cumsum()


def _adx(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14):
    """Returns (adx, plus_di, minus_di) as three Series."""
    tr = _true_range(high, low, close)

    up_move   = high.diff()
    down_move = -(low.diff())

    plus_dm  = up_move.where((up_move > down_move) & (up_move > 0),   0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    atr_s     = tr.ewm(com=length - 1, adjust=False).mean()
    plus_di   = 100.0 * plus_dm.ewm(com=length - 1,  adjust=False).mean() / atr_s.replace(0, np.nan)
    minus_di  = 100.0 * minus_dm.ewm(com=length - 1, adjust=False).mean() / atr_s.replace(0, np.nan)

    dx  = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(com=length - 1, adjust=False).mean()
    return adx, plus_di, minus_di


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
    Add all technical indicators to an OHLCV DataFrame.
    Input must have columns: open, high, low, adjusted_close, volume.
    All indicators use adjusted_close as the price series.
    Returns the DataFrame with new indicator columns appended.
    """
    if df.empty or len(df) < 20:
        return df

    df    = df.copy()
    close  = df["adjusted_close"]
    high   = df["high"]
    low    = df["low"]
    volume = df["volume"]

    # EMA 9, 21, 50, 200
    for p in EMA_PERIODS:
        df[f"ema_{p}"] = _ema(close, p)

    # RSI
    df["rsi"] = _rsi(close, RSI_PERIOD)

    # MACD
    macd_line, signal_line, histogram = _macd(close, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
    df["macd"]        = macd_line
    df["macd_signal"] = signal_line
    df["macd_hist"]   = histogram

    # Bollinger Bands
    bb_upper, bb_mid, bb_lower, bb_pct = _bbands(close, BB_PERIOD, BB_STD)
    df["bb_upper"] = bb_upper
    df["bb_mid"]   = bb_mid
    df["bb_lower"] = bb_lower
    df["bb_pct"]   = bb_pct

    # ATR
    df["atr"] = _atr(high, low, close, ATR_PERIOD)

    # OBV
    df["obv"] = _obv(close, volume)

    # ADX
    adx_val, plus_di, minus_di = _adx(high, low, close, ADX_PERIOD)
    df["adx"] = adx_val
    df["dmp"] = plus_di
    df["dmn"] = minus_di

    # Rolling 20-day VWAP
    typical    = (high + low + close) / 3.0
    df["vwap_20"] = (typical * volume).rolling(20).sum() / volume.rolling(20).sum()

    # 10-day OBV slope
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
