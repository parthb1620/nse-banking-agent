"""
Signal generation — BUY / SELL / NEUTRAL with strength 1–10 and reason list.

Strategy: EMA + RSI + MACD confluence (ema_rsi_swing)

BUY entry (all three required, bonus conditions add strength):
  REQUIRED  price > EMA_200         — bull regime (regime gate)
  REQUIRED  RSI in [35, 60]         — entry zone, not overbought or crashed
  REQUIRED  MACD histogram > 0      — positive momentum
  +1 bonus  price > EMA_50          — medium-term uptrend
  +1 bonus  price > EMA_21          — short-term uptrend
  +1 bonus  ADX > 25                — trending market (avoid choppy ranges)
  +1 bonus  price > VWAP_20         — above institutional benchmark
  +1 bonus  OBV slope > 0           — accumulation over last 10 days

SELL / exit:
  RSI > 75                          — overbought, take profit
  price < EMA_21                    — short-term breakdown
  MACD histogram turning negative   — momentum lost

Signals are stored in technical_signals table.
"""

import json
from datetime import date, datetime
from typing import Optional
from zoneinfo import ZoneInfo

from loguru import logger

from analysis.technical.indicators import get_indicators, get_latest_row
from config.settings import ADX_TREND_MIN, BANKING_STOCKS, RSI_ENTRY_HIGH, RSI_ENTRY_LOW, RSI_EXIT
from data.storage.database import TechnicalSignal, get_session

_IST = ZoneInfo("Asia/Kolkata")


def _optimized_thresholds(symbol: str) -> tuple[float, float, float]:
    """
    Return (rsi_entry_low, rsi_entry_high, rsi_exit) from walk-forward optimizer
    results if available, otherwise fall back to settings defaults.
    """
    try:
        from backtesting.optimizer import load_best_params
        p = load_best_params(symbol)
        return (
            p.get("rsi_entry_low",  RSI_ENTRY_LOW),
            p.get("rsi_entry_high", RSI_ENTRY_HIGH),
            p.get("rsi_exit",       RSI_EXIT),
        )
    except Exception:
        return RSI_ENTRY_LOW, RSI_ENTRY_HIGH, RSI_EXIT


# ── Core scoring logic ─────────────────────────────────────────────────────────

def _evaluate(row: dict, prev_row: Optional[dict] = None, symbol: str = "") -> tuple[str, int, list[str], float]:
    """
    Evaluate one indicator row and return (signal_type, strength, reasons, tech_score_0_100).
    prev_row is used to detect MACD histogram direction change.
    """
    # Load optimized thresholds if available
    rsi_low, rsi_high, rsi_exit_val = _optimized_thresholds(symbol) if symbol else (RSI_ENTRY_LOW, RSI_ENTRY_HIGH, RSI_EXIT)

    price     = row.get("adjusted_close") or row.get("close")
    rsi       = row.get("rsi")
    macd_hist = row.get("macd_hist")
    ema_21    = row.get("ema_21")
    ema_50    = row.get("ema_50")
    ema_200   = row.get("ema_200")
    adx       = row.get("adx")
    vwap_20   = row.get("vwap_20")
    obv_slope = row.get("obv_slope")

    if price is None:
        return "NEUTRAL", 0, ["insufficient data"], 50.0

    # ── SELL check first (exit signals override entry) ─────────────────────────
    sell_reasons = []
    if rsi is not None and rsi > rsi_exit_val:
        sell_reasons.append(f"RSI {rsi:.1f} > {rsi_exit_val} (overbought)")
    if ema_21 is not None and price < ema_21:
        sell_reasons.append(f"price {price:.2f} < EMA_21 {ema_21:.2f} (trend breakdown)")
    if macd_hist is not None and macd_hist < -0.5:
        sell_reasons.append(f"MACD hist {macd_hist:.2f} strongly negative")

    if len(sell_reasons) >= 2:
        strength = min(10, len(sell_reasons) * 3)
        tech_score = max(0.0, 30.0 - strength * 3)
        return "SELL", strength, sell_reasons, tech_score

    # ── BUY check ──────────────────────────────────────────────────────────────
    # Three required gates
    gate_regime   = ema_200 is not None and price > ema_200
    gate_rsi      = rsi is not None and rsi_low <= rsi <= rsi_high
    gate_momentum = macd_hist is not None and macd_hist > 0

    buy_reasons  = []
    bonus_points = 0

    if gate_regime:
        buy_reasons.append(f"price {price:.2f} > EMA_200 {ema_200:.2f}")
    if gate_rsi:
        buy_reasons.append(f"RSI {rsi:.1f} in entry zone [{rsi_low}–{rsi_high}]")
    if gate_momentum:
        buy_reasons.append(f"MACD hist {macd_hist:.3f} > 0 (positive momentum)")

    if gate_regime and gate_rsi and gate_momentum:
        # Bonus conditions — each adds +1 to strength
        if ema_50 is not None and price > ema_50:
            bonus_points += 1
            buy_reasons.append(f"price > EMA_50 {ema_50:.2f}")
        if ema_21 is not None and price > ema_21:
            bonus_points += 1
            buy_reasons.append(f"price > EMA_21 {ema_21:.2f}")
        if adx is not None and adx > ADX_TREND_MIN:
            bonus_points += 1
            buy_reasons.append(f"ADX {adx:.1f} > {ADX_TREND_MIN} (trending)")
        if vwap_20 is not None and price > vwap_20:
            bonus_points += 1
            buy_reasons.append(f"price > VWAP_20 {vwap_20:.2f}")
        if obv_slope is not None and obv_slope > 0:
            bonus_points += 1
            buy_reasons.append("OBV slope positive (accumulation)")

        strength   = min(10, 3 + bonus_points * 2)   # 3 base for meeting all gates, +2 per bonus
        tech_score = min(100.0, 50.0 + strength * 5)
        return "BUY", strength, buy_reasons, tech_score

    # ── NEUTRAL ────────────────────────────────────────────────────────────────
    neutral_reasons = []
    if not gate_regime:
        ema200_str = f"{ema_200:.2f}" if ema_200 is not None else "n/a"
        neutral_reasons.append(f"price {price:.2f} below EMA_200 {ema200_str} (bear regime)")
    if not gate_rsi:
        rsi_str = f"{rsi:.1f}" if rsi is not None else "n/a"
        neutral_reasons.append(f"RSI {rsi_str} outside entry zone")
    if not gate_momentum:
        macd_str = f"{macd_hist:.3f}" if macd_hist is not None else "n/a"
        neutral_reasons.append(f"MACD hist {macd_str} ≤ 0")

    # Partial bullishness → score above 50 if regime is ok
    partial_score = 50.0
    if gate_regime:
        partial_score += 10
    if gate_rsi:
        partial_score += 5
    if gate_momentum:
        partial_score += 5

    return "NEUTRAL", 0, neutral_reasons, partial_score


# ── Public API ─────────────────────────────────────────────────────────────────

def generate_signal(symbol: str, signal_date: Optional[date] = None) -> Optional[TechnicalSignal]:
    """
    Generate and store a TechnicalSignal for one symbol.
    signal_date defaults to today. Only uses OHLCV data up to signal_date.
    Returns the stored TechnicalSignal, or None if insufficient data.
    """
    signal_date = signal_date or date.today()

    df = get_indicators(symbol, as_of_date=signal_date)
    if df.empty or len(df) < 2:
        logger.warning(f"signals: not enough data for {symbol} as of {signal_date}")
        return None

    row      = df.iloc[-1].to_dict()
    prev_row = df.iloc[-2].to_dict() if len(df) >= 2 else None

    signal_type, strength, reasons, tech_score = _evaluate(row, prev_row, symbol=symbol)

    indicators_snap = {
        k: (None if (v is None or (isinstance(v, float) and __import__("math").isnan(v)))
            else round(float(v), 4))
        for k, v in row.items()
        if k not in ("open", "high", "low", "close", "volume")
    }

    sig = TechnicalSignal(
        symbol=symbol,
        signal_date=signal_date,
        signal_type=signal_type,
        strength=strength,
        reason="; ".join(reasons)[:500],
        indicators_json=json.dumps(indicators_snap),
        generated_at=datetime.now(_IST),
    )

    with get_session() as session:
        # Remove any existing signal for this (symbol, date) before inserting
        session.query(TechnicalSignal).filter_by(
            symbol=symbol, signal_date=signal_date
        ).delete()
        session.add(sig)
        session.commit()
        session.refresh(sig)

    logger.info(f"Signal {symbol} {signal_date}: {signal_type} strength={strength} tech_score={tech_score:.1f}")
    return sig


def score(symbol: str, signal_time: datetime) -> float:
    """
    Return a 0–100 technical score for use in stock_scorer.
    Uses only OHLCV data up to signal_time (no lookahead).
    Returns neutral 50.0 if insufficient data.
    """
    row = get_latest_row(symbol, as_of_date=signal_time.date())
    if not row:
        return 50.0
    _, _, _, tech_score = _evaluate(row, symbol=symbol)
    return tech_score


def generate_all(signal_date: Optional[date] = None) -> list[TechnicalSignal]:
    """Generate and store signals for all tracked stocks."""
    signal_date = signal_date or date.today()
    results = []
    for symbol in BANKING_STOCKS:
        sig = generate_signal(symbol, signal_date)
        if sig:
            results.append(sig)
    return results
