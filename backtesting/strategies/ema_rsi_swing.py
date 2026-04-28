"""
EMA + RSI + MACD confluence swing strategy.

Entry (all three required):
  price > EMA_200        — bull regime
  RSI in [35, 60]        — entry zone, not overbought or crashed
  MACD histogram > 0     — positive momentum

Exit (either condition):
  RSI > 75               — overbought, take profit
  close < EMA_21         — short-term trend breakdown

Stop-loss:  entry_price − ATR_STOP_MULTIPLIER × ATR  (default 2×ATR)
Target:     entry_price + 2 × risk                   (2:1 reward/risk)

This module only generates signal arrays from a pre-computed indicator
DataFrame. Trade execution is handled by the engine.
"""

import math

import pandas as pd

from config.settings import (
    ADX_TREND_MIN, ATR_STOP_MULTIPLIER,
    MACD_FAST, MACD_SLOW,
    RSI_ENTRY_HIGH, RSI_ENTRY_LOW, RSI_EXIT,
)


def generate_signals(df: pd.DataFrame, params: dict | None = None) -> pd.DataFrame:
    """
    Add entry/exit signal columns and stop/target levels to an indicator DataFrame.

    Input df must already contain indicator columns produced by
    analysis.technical.indicators.compute_all():
      adjusted_close, ema_21, ema_50, ema_200, rsi, macd_hist, atr

    params (optional) — override default settings values:
      rsi_entry_low      (default RSI_ENTRY_LOW=35)
      rsi_entry_high     (default RSI_ENTRY_HIGH=60)
      rsi_exit           (default RSI_EXIT=75)
      atr_stop_multiplier (default ATR_STOP_MULTIPLIER=2.0)
      min_risk_reward    (default 2.0)

    Added columns:
      entry_signal  bool   — True on bars where BUY conditions are met
      exit_signal   bool   — True on bars where EXIT conditions are met
      stop_price    float  — stop level below today's close (ATR-based)
      target_price  float  — target level above today's close

    The engine shifts signals by +1 bar so entry happens at the *next*
    bar's open — this function simply marks the signal bar.
    """
    p = params or {}
    rsi_low  = p.get("rsi_entry_low",       RSI_ENTRY_LOW)
    rsi_high = p.get("rsi_entry_high",      RSI_ENTRY_HIGH)
    rsi_exit = p.get("rsi_exit",            RSI_EXIT)
    atr_mult = p.get("atr_stop_multiplier", ATR_STOP_MULTIPLIER)
    rr       = p.get("min_risk_reward",     2.0)

    df = df.copy()

    close = df.get("adjusted_close", df.get("close"))
    if close is None:
        raise ValueError("DataFrame must have 'adjusted_close' or 'close' column")

    # ── Entry gates ────────────────────────────────────────────────────────────
    regime   = df["ema_200"].notna() & (close > df["ema_200"])
    rsi_ok   = df["rsi"].notna() & (df["rsi"] >= rsi_low) & (df["rsi"] <= rsi_high)
    momentum = df["macd_hist"].notna() & (df["macd_hist"] > 0)

    df["entry_signal"] = regime & rsi_ok & momentum

    # ── Exit gates ─────────────────────────────────────────────────────────────
    rsi_ob    = df["rsi"].notna() & (df["rsi"] > rsi_exit)
    ema21_brk = df["ema_21"].notna() & (close < df["ema_21"])

    df["exit_signal"] = rsi_ob | ema21_brk

    # ── Stop / target levels (computed at signal bar's close + ATR) ────────────
    atr  = df.get("atr", pd.Series(float("nan"), index=df.index))
    risk = atr_mult * atr

    df["stop_price"]   = (close - risk).clip(lower=0)
    df["target_price"] = close + rr * risk

    return df


def describe(df: pd.DataFrame) -> None:
    """Print a summary of the signals in a DataFrame (for quick inspection)."""
    if "entry_signal" not in df.columns:
        df = generate_signals(df)

    n_entry = df["entry_signal"].sum()
    n_exit  = df["exit_signal"].sum()
    n_bars  = len(df)
    date_range = f"{df.index[0].date()} → {df.index[-1].date()}" if len(df) else "empty"

    print(f"  Bars: {n_bars}  |  {date_range}")
    print(f"  Entry signals: {n_entry}  ({n_entry / n_bars * 100:.1f}% of bars)")
    print(f"  Exit  signals: {n_exit}  ({n_exit  / n_bars * 100:.1f}% of bars)")
