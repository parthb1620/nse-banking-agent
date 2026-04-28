"""
EMA + RSI + MACD confluence swing strategy.

Entry (all three required):
  price > EMA_200        — bull regime
  RSI in [35, 60]        — entry zone, not overbought or crashed
  MACD histogram > 0     — positive momentum

Exit (any of):
  RSI > 75                                       — overbought, take profit
  close < EMA_21 for 2 consecutive bars          — sustained breakdown
  close < EMA_21 AND MACD histogram negative     — confirmed momentum loss

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
    MIN_SIGNAL_STRENGTH,
    RSI_ENTRY_HIGH, RSI_ENTRY_LOW, RSI_EXIT,
    SWING_LOW_BUFFER_ATR, VOLUME_CONFIRM_MULTIPLIER,
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
    vol_mult = p.get("volume_confirm_multiplier", VOLUME_CONFIRM_MULTIPLIER)

    df = df.copy()

    close = df.get("adjusted_close", df.get("close"))
    if close is None:
        raise ValueError("DataFrame must have 'adjusted_close' or 'close' column")

    # ── Entry gates (required — all four must be true) ─────────────────────────
    regime   = df["ema_200"].notna() & (close > df["ema_200"])
    rsi_ok   = df["rsi"].notna() & (df["rsi"] >= rsi_low) & (df["rsi"] <= rsi_high)
    momentum = df["macd_hist"].notna() & (df["macd_hist"] > 0)

    # Volume gate — fail-open when 20-day avg unavailable (early bars), else require multiplier.
    if "vol_sma_20" in df.columns and "volume" in df.columns:
        vol_avg = df["vol_sma_20"]
        volume_ok = vol_avg.isna() | (vol_avg == 0) | (df["volume"] >= vol_mult * vol_avg)
    else:
        volume_ok = pd.Series(True, index=df.index)

    base_signal = regime & rsi_ok & momentum & volume_ok

    # ── Bonus conditions — same as live signals._evaluate() ────────────────────
    # Strength = 4 (base gates incl. volume) + 2 per bonus. MIN_SIGNAL_STRENGTH=6 → need ≥1 bonus.
    min_bonus = p.get("min_bonus_conditions", max(0, (MIN_SIGNAL_STRENGTH - 4 + 1) // 2))

    bonus = pd.Series(0, index=df.index)
    if "ema_50"    in df.columns: bonus += (close > df["ema_50"].fillna(0)).astype(int)
    if "ema_21"    in df.columns: bonus += (close > df["ema_21"].fillna(0)).astype(int)
    if "adx"       in df.columns: bonus += (df["adx"].fillna(0) > ADX_TREND_MIN).astype(int)
    if "vwap_20"   in df.columns: bonus += (close > df["vwap_20"].fillna(0)).astype(int)
    if "obv_slope" in df.columns: bonus += (df["obv_slope"].fillna(0) > 0).astype(int)

    df["entry_signal"] = base_signal & (bonus >= min_bonus)

    # ── Exit gates ─────────────────────────────────────────────────────────────
    # Soften EMA-21 exit: a single dip is noise. Trigger only when today closes below
    # EMA_21 AND (yesterday also closed below OR MACD histogram is already negative).
    rsi_ob       = df["rsi"].notna() & (df["rsi"] > rsi_exit)
    close_below  = df["ema_21"].notna() & (close < df["ema_21"])
    prev_below   = close_below.shift(1, fill_value=False)
    macd_neg     = df["macd_hist"].notna() & (df["macd_hist"] < 0)
    ema21_brk    = close_below & (prev_below | macd_neg)

    df["exit_signal"] = rsi_ob | ema21_brk

    # ── Stop / target levels ───────────────────────────────────────────────────
    # Prefer the structural stop (just below recent swing low) but clamp it within
    # [0.5×ATR, atr_mult×ATR] of entry — too tight = pinged on noise, too wide =
    # tiny qty (and protects us from inflated ATR caused by un-adjusted gaps).
    atr        = df.get("atr",          pd.Series(float("nan"), index=df.index))
    swing_low  = df.get("swing_low_10", pd.Series(float("nan"), index=df.index))

    floor_stop   = close - atr_mult * atr               # widest allowed (most risk)
    ceiling_stop = close - 0.5 * atr                    # tightest allowed (least risk)
    raw_struct   = swing_low - SWING_LOW_BUFFER_ATR * atr

    # When swing_low NaN (early bars), fall back to ATR-only stop.
    structure_clamped = raw_struct.where(raw_struct.notna(), other=floor_stop)
    structure_clamped = structure_clamped.clip(lower=floor_stop, upper=ceiling_stop)
    combined_stop = structure_clamped

    risk = (close - combined_stop).clip(lower=0)
    df["stop_price"]   = combined_stop.clip(lower=0)
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
