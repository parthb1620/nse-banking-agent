"""
BTST (Buy Today Sell Tomorrow) signal generator — runs at 14:30 IST.

Strategy: identifies stocks likely to gap up or continue next morning.

BUY criteria (all required):
  price > EMA_21                  — short-term trend intact
  RSI in [45, 70]                 — momentum without being overbought
  MACD histogram > 0              — positive momentum
  Today's candle closes in upper 30% of high-low range (bullish close)
  Volume ≥ 1.5× 20-day avg        — unusual institutional interest

Bonus (+1 each):
  +1  price > EMA_50              — medium-term support
  +1  price near day's high (<2% below high)
  +1  Positive recent news sentiment
  +1  Price > VWAP_20             — closed above institutional benchmark

SELL / avoid criteria:
  RSI > 72                        — overbought, likely to pullback overnight
  Candle closes in lower 30% (bearish close — sellers dominated)
  Volume < 0.8× avg (low-conviction day)

Signals tagged engine='btst' in technical_signals table.
Position sizing: 1-day hold only. Stop at today's low. Target = 1.5× stop distance.
"""

import json
from datetime import date, datetime
from typing import Optional
from zoneinfo import ZoneInfo

from loguru import logger

from analysis.technical.indicators import get_indicators
from config.settings import BTST_UNIVERSE
from data.storage.database import TechnicalSignal, get_session

_IST = ZoneInfo("Asia/Kolkata")

_RSI_LOW  = 45
_RSI_HIGH = 70
_RSI_EXIT = 72
_VOL_MULT = 1.5     # volume filter — tighter than shortterm (1.2×)
_CLOSE_UPPER_PCT = 0.70  # close must be in upper 30% of candle range


def _candle_close_position(row: dict) -> float:
    """Returns 0.0–1.0: where close sits within the day's high-low range."""
    high  = row.get("high")
    low   = row.get("low")
    close = row.get("adjusted_close") or row.get("close")
    if None in (high, low, close) or high == low:
        return 0.5
    return (close - low) / (high - low)


def _evaluate_btst(row: dict, symbol: str) -> tuple[str, int, list[str], float]:
    price      = row.get("adjusted_close") or row.get("close")
    rsi        = row.get("rsi")
    macd_hist  = row.get("macd_hist")
    ema_21     = row.get("ema_21")
    ema_50     = row.get("ema_50")
    vwap_20    = row.get("vwap_20")
    volume     = row.get("volume")
    vol_sma_20 = row.get("vol_sma_20")
    high       = row.get("high")

    if price is None:
        return "NEUTRAL", 0, ["no price data"], 50.0

    close_pos = _candle_close_position(row)

    # SELL / avoid
    sell_reasons = []
    if rsi is not None and rsi > _RSI_EXIT:
        sell_reasons.append(f"RSI {rsi:.1f} > {_RSI_EXIT} (overbought overnight risk)")
    if close_pos < 0.30:
        sell_reasons.append(f"bearish close at {close_pos:.0%} of candle range")
    if vol_sma_20 and volume and vol_sma_20 > 0 and volume < 0.8 * vol_sma_20:
        sell_reasons.append(f"weak volume {volume/vol_sma_20:.2f}× avg")

    if len(sell_reasons) >= 2:
        strength = min(10, len(sell_reasons) * 3)
        return "SELL", strength, sell_reasons, max(0.0, 25.0 - strength * 3)

    # BUY gates
    gate_trend    = ema_21 is not None and price > ema_21
    gate_rsi      = rsi is not None and _RSI_LOW <= rsi <= _RSI_HIGH
    gate_momentum = macd_hist is not None and macd_hist > 0
    gate_close    = close_pos >= _CLOSE_UPPER_PCT

    if vol_sma_20 is None or volume is None or vol_sma_20 == 0:
        gate_volume = True
        vol_ratio   = None
    else:
        vol_ratio   = volume / vol_sma_20
        gate_volume = vol_ratio >= _VOL_MULT

    buy_reasons = []
    bonus = 0

    if gate_trend:
        buy_reasons.append(f"price {price:.2f} > EMA_21 {ema_21:.2f}")
    if gate_rsi:
        buy_reasons.append(f"RSI {rsi:.1f} in [{_RSI_LOW}–{_RSI_HIGH}]")
    if gate_momentum:
        buy_reasons.append(f"MACD hist {macd_hist:.3f} > 0")
    if gate_close:
        buy_reasons.append(f"bullish close at {close_pos:.0%} of range")
    if gate_volume and vol_ratio:
        buy_reasons.append(f"volume {vol_ratio:.2f}× avg (≥{_VOL_MULT}×)")

    if gate_trend and gate_rsi and gate_momentum and gate_close and gate_volume:
        if ema_50 is not None and price > ema_50:
            bonus += 1
            buy_reasons.append(f"price > EMA_50 {ema_50:.2f}")
        if high is not None and price >= 0.98 * high:
            bonus += 1
            buy_reasons.append(f"closing near day high (≥98% of {high:.2f})")
        if vwap_20 is not None and price > vwap_20:
            bonus += 1
            buy_reasons.append(f"price > VWAP_20 {vwap_20:.2f}")

        strength   = min(10, 4 + bonus * 2)
        tech_score = min(100.0, 55.0 + strength * 5)
        return "BUY", strength, buy_reasons, tech_score

    # NEUTRAL
    neutral = []
    if not gate_trend:
        neutral.append(f"price below EMA_21 {ema_21:.2f if ema_21 else 'n/a'}")
    if not gate_rsi:
        neutral.append(f"RSI {rsi:.1f if rsi else 'n/a'} outside [{_RSI_LOW}–{_RSI_HIGH}]")
    if not gate_momentum:
        neutral.append("MACD histogram ≤ 0")
    if not gate_close:
        neutral.append(f"weak close at {close_pos:.0%} of range")
    if not gate_volume:
        neutral.append(f"volume {vol_ratio:.2f}× < {_VOL_MULT}× avg" if vol_ratio else "low volume")

    partial = 50.0
    if gate_trend:    partial += 8
    if gate_rsi:      partial += 5
    if gate_momentum: partial += 5
    if gate_close:    partial += 5
    return "NEUTRAL", 0, neutral, partial


def generate_signal(symbol: str, signal_date: Optional[date] = None) -> Optional[TechnicalSignal]:
    """Generate and store a BTST TechnicalSignal for one symbol."""
    signal_date = signal_date or date.today()

    df = get_indicators(symbol, as_of_date=signal_date)
    if df.empty or len(df) < 2:
        logger.warning(f"btst_signals: not enough data for {symbol} as of {signal_date}")
        return None

    row = df.iloc[-1].to_dict()
    signal_type, strength, reasons, tech_score = _evaluate_btst(row, symbol)

    indicators_snap = {
        k: (None if (v is None or (isinstance(v, float) and __import__("math").isnan(v)))
            else round(float(v), 4))
        for k, v in row.items()
        if k not in ("open", "volume")
    }

    sig = TechnicalSignal(
        symbol          = symbol,
        signal_date     = signal_date,
        signal_type     = signal_type,
        strength        = strength,
        reason          = "; ".join(reasons)[:500],
        indicators_json = json.dumps(indicators_snap),
        generated_at    = datetime.now(_IST),
        engine          = "btst",
    )

    with get_session() as session:
        session.query(TechnicalSignal).filter_by(
            symbol=symbol, signal_date=signal_date, engine="btst"
        ).delete()
        session.add(sig)
        session.commit()
        session.refresh(sig)

    logger.info(f"[BTST] {symbol} {signal_date}: {signal_type} str={strength} score={tech_score:.1f}")
    return sig


def generate_all(signal_date: Optional[date] = None) -> list[TechnicalSignal]:
    """Generate BTST signals for all stocks in the BTST universe."""
    signal_date = signal_date or date.today()
    results = []
    for symbol in BTST_UNIVERSE:
        sig = generate_signal(symbol, signal_date)
        if sig:
            results.append(sig)
    return results
