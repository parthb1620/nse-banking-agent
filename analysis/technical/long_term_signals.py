"""
Long-term signal generator — weekly scan, fundamentals-weighted.

Strategy (all four required for BUY):
  REQUIRED  price > EMA_200            — confirmed bull regime
  REQUIRED  RSI in [40, 65]            — not overbought, not in freefall
  REQUIRED  price > EMA_50             — medium-term trend intact
  REQUIRED  revenue growing YoY        — at least one period of positive growth

Bonus conditions (+1 each, max strength 10):
  +1  ADX > 20                         — some directional momentum
  +1  price > EMA_21                   — short-term trend also up
  +1  OBV slope > 0                    — sustained accumulation
  +1  Fundamental score > 60           — strong balance sheet / ratios

SELL signals:
  price < EMA_200                      — bear regime breach
  RSI > 80                             — extreme overbought
  Revenue declining 2+ consecutive periods

Designed for hold periods of 3–12 months. Does not use intraday volume gate
(long-term positions aren't entered on a single candle's volume).
"""

import json
from datetime import date, datetime
from typing import Optional
from zoneinfo import ZoneInfo

from loguru import logger

from analysis.technical.indicators import get_indicators, get_latest_row
from config.settings import LONGTERM_UNIVERSE
from data.storage.database import Fundamental, TechnicalSignal, get_session
from data.quality.known_time import known_time_filter

_IST = ZoneInfo("Asia/Kolkata")

_RSI_ENTRY_LOW  = 40
_RSI_ENTRY_HIGH = 65
_RSI_EXIT       = 80
_ADX_MIN        = 20


def _revenue_trend(symbol: str, signal_date: date) -> tuple[bool, bool]:
    """
    Return (growing, declining_streak) based on recent quarterly revenues.
    growing=True if latest revenue > 1 period ago.
    declining_streak=True if revenue fell for 2+ consecutive periods.
    """
    signal_dt = datetime.combine(signal_date, datetime.min.time())
    try:
        with get_session() as s:
            rows = (
                s.query(Fundamental)
                .filter(
                    Fundamental.symbol == symbol,
                    Fundamental.revenue.isnot(None),
                    Fundamental.period_type == "Q",
                    *known_time_filter(Fundamental, signal_dt),
                )
                .order_by(Fundamental.period_end_date.desc())
                .limit(4)
                .all()
            )
        revenues = [r.revenue for r in rows if r.revenue]
        if len(revenues) < 2:
            return True, False   # insufficient data — give benefit of the doubt
        growing = revenues[0] > revenues[1]
        declining = len(revenues) >= 3 and revenues[0] < revenues[1] < revenues[2]
        return growing, declining
    except Exception:
        return True, False


def _evaluate_longterm(row: dict, symbol: str, signal_date: date) -> tuple[str, int, list[str], float]:
    price     = row.get("adjusted_close") or row.get("close")
    rsi       = row.get("rsi")
    ema_21    = row.get("ema_21")
    ema_50    = row.get("ema_50")
    ema_200   = row.get("ema_200")
    adx       = row.get("adx")
    obv_slope = row.get("obv_slope")

    if price is None:
        return "NEUTRAL", 0, ["no price data"], 50.0

    rev_growing, rev_declining = _revenue_trend(symbol, signal_date)

    # SELL checks
    sell_reasons = []
    if ema_200 is not None and price < ema_200:
        sell_reasons.append(f"price {price:.2f} < EMA_200 {ema_200:.2f} (bear regime)")
    if rsi is not None and rsi > _RSI_EXIT:
        sell_reasons.append(f"RSI {rsi:.1f} > {_RSI_EXIT} (extreme overbought)")
    if rev_declining:
        sell_reasons.append("revenue declining 2+ consecutive quarters")

    if len(sell_reasons) >= 2:
        strength = min(10, len(sell_reasons) * 3)
        return "SELL", strength, sell_reasons, max(0.0, 30.0 - strength * 3)

    # BUY gates
    gate_regime   = ema_200 is not None and price > ema_200
    gate_rsi      = rsi is not None and _RSI_ENTRY_LOW <= rsi <= _RSI_ENTRY_HIGH
    gate_trend    = ema_50 is not None and price > ema_50
    gate_revenue  = rev_growing

    buy_reasons = []
    bonus = 0

    if gate_regime:
        buy_reasons.append(f"price {price:.2f} > EMA_200 (bull regime)")
    if gate_rsi:
        buy_reasons.append(f"RSI {rsi:.1f} in [{_RSI_ENTRY_LOW}–{_RSI_ENTRY_HIGH}]")
    if gate_trend:
        buy_reasons.append(f"price > EMA_50 {ema_50:.2f} (medium-term uptrend)")
    if gate_revenue:
        buy_reasons.append("revenue growing QoQ")

    if gate_regime and gate_rsi and gate_trend and gate_revenue:
        if adx is not None and adx > _ADX_MIN:
            bonus += 1
            buy_reasons.append(f"ADX {adx:.1f} > {_ADX_MIN}")
        if ema_21 is not None and price > ema_21:
            bonus += 1
            buy_reasons.append(f"price > EMA_21 {ema_21:.2f}")
        if obv_slope is not None and obv_slope > 0:
            bonus += 1
            buy_reasons.append("OBV slope positive (accumulation)")

        # Fundamental bonus — import here to avoid circular deps
        try:
            from analysis.fundamental.ratios import score as ratios_score
            sig_dt = datetime.combine(signal_date, datetime.min.time())
            f_score = ratios_score(symbol, sig_dt)
            if f_score > 60:
                bonus += 1
                buy_reasons.append(f"fundamental score {f_score:.0f} > 60")
        except Exception:
            pass

        strength   = min(10, 4 + bonus * 2)
        tech_score = min(100.0, 50.0 + strength * 5)
        return "BUY", strength, buy_reasons, tech_score

    # NEUTRAL
    neutral = []
    if not gate_regime:
        neutral.append(f"price below EMA_200 {ema_200:.2f if ema_200 else 'n/a'}")
    if not gate_rsi:
        neutral.append(f"RSI {rsi:.1f if rsi else 'n/a'} outside [{_RSI_ENTRY_LOW}–{_RSI_ENTRY_HIGH}]")
    if not gate_trend:
        neutral.append(f"price below EMA_50 {ema_50:.2f if ema_50 else 'n/a'}")
    if not gate_revenue:
        neutral.append("revenue not growing")

    partial = 50.0
    if gate_regime:  partial += 10
    if gate_rsi:     partial += 5
    if gate_trend:   partial += 5
    if gate_revenue: partial += 3
    return "NEUTRAL", 0, neutral, partial


def generate_signal(symbol: str, signal_date: Optional[date] = None) -> Optional[TechnicalSignal]:
    """Generate and store a long-term TechnicalSignal for one symbol."""
    signal_date = signal_date or date.today()

    df = get_indicators(symbol, as_of_date=signal_date)
    if df.empty or len(df) < 2:
        logger.warning(f"longterm_signals: not enough data for {symbol} as of {signal_date}")
        return None

    row = df.iloc[-1].to_dict()
    signal_type, strength, reasons, tech_score = _evaluate_longterm(row, symbol, signal_date)

    indicators_snap = {
        k: (None if (v is None or (isinstance(v, float) and __import__("math").isnan(v)))
            else round(float(v), 4))
        for k, v in row.items()
        if k not in ("open", "high", "low", "close", "volume")
    }

    sig = TechnicalSignal(
        symbol          = symbol,
        signal_date     = signal_date,
        signal_type     = signal_type,
        strength        = strength,
        reason          = "; ".join(reasons)[:500],
        indicators_json = json.dumps(indicators_snap),
        generated_at    = datetime.now(_IST),
        engine          = "longterm",
    )

    with get_session() as session:
        session.query(TechnicalSignal).filter_by(
            symbol=symbol, signal_date=signal_date, engine="longterm"
        ).delete()
        session.add(sig)
        session.commit()
        session.refresh(sig)

    logger.info(f"[LONGTERM] {symbol} {signal_date}: {signal_type} str={strength} score={tech_score:.1f}")
    return sig


def generate_all(signal_date: Optional[date] = None) -> list[TechnicalSignal]:
    """Generate long-term signals for all stocks in the long-term universe."""
    signal_date = signal_date or date.today()
    results = []
    for symbol in LONGTERM_UNIVERSE:
        sig = generate_signal(symbol, signal_date)
        if sig:
            results.append(sig)
    return results
