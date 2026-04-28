"""
Paper trading simulator — morning entry logic.

Run after 09:15 IST on trading days:
  python -m paper_trading.simulator

Logic:
  1. Skip non-trading days
  2. Check daily-loss circuit breaker (skip new entries if portfolio dropped > 3% today)
  3. For each BUY signal (highest strength first), if we have room (< MAX_OPEN_POSITIONS)
     and no existing open trade for that symbol:
       - entry_price  = today's open
       - stop_loss    = entry_price - ATR_STOP_MULTIPLIER × ATR_14
       - target       = entry_price + MIN_RISK_REWARD × (entry_price - stop_loss)
       - quantity     = floor(RISK_PER_TRADE × capital / risk_per_share)
     Skip if quantity < 1 or risk_reward < MIN_RISK_REWARD.
  4. Log PaperTrade with status='open'.
"""

import math
from datetime import date, datetime
from zoneinfo import ZoneInfo

from loguru import logger

from config.nse_calendar import is_trading_day
from config.settings import (
    ATR_STOP_MULTIPLIER, BANKING_STOCKS, DAILY_LOSS_LIMIT_PCT,
    FII_SELL_STREAK_DAYS, MAX_OPEN_POSITIONS, MIN_RISK_REWARD,
    MIN_SIGNAL_STRENGTH, PAPER_TRADING_CAPITAL, RISK_PER_TRADE_PCT, STOCK_NAMES,
    SWING_LOW_BUFFER_ATR,
)
from data.storage.database import OHLCVDaily, PaperTrade, TechnicalSignal, get_session

_IST = ZoneInfo("Asia/Kolkata")


def _get_capital() -> float:
    """Current portfolio value = initial capital + sum of all closed P&L."""
    with get_session() as s:
        closed = s.query(PaperTrade).filter(
            PaperTrade.status.in_(["closed_target", "closed_stop", "closed_manual"])
        ).all()
    realised = sum(t.pnl for t in closed if t.pnl is not None)
    return PAPER_TRADING_CAPITAL + realised


def _open_positions() -> list[PaperTrade]:
    with get_session() as s:
        return s.query(PaperTrade).filter(PaperTrade.status == "open").all()


def _today_realised_pnl(today: date) -> float:
    with get_session() as s:
        closed_today = s.query(PaperTrade).filter(
            PaperTrade.exit_date == today,
            PaperTrade.status.in_(["closed_target", "closed_stop"]),
        ).all()
    return sum(t.pnl for t in closed_today if t.pnl is not None)


def _buy_signals_today(today: date) -> list[TechnicalSignal]:
    with get_session() as s:
        sigs = (
            s.query(TechnicalSignal)
            .filter(
                TechnicalSignal.signal_date == today,
                TechnicalSignal.signal_type == "BUY",
            )
            .order_by(TechnicalSignal.strength.desc())
            .all()
        )
    return sigs


def _todays_open(symbol: str, today: date) -> float | None:
    with get_session() as s:
        row = s.query(OHLCVDaily).filter(
            OHLCVDaily.symbol == symbol,
            OHLCVDaily.date == today,
        ).first()
    return row.open if row and row.open else None


def _latest_indicators(symbol: str, today: date) -> dict:
    """Get yesterday's indicator row (pre-open snapshot)."""
    from analysis.technical.indicators import get_latest_row
    from datetime import timedelta
    return get_latest_row(symbol, as_of_date=today - timedelta(days=1)) or {}


def _latest_atr(symbol: str, today: date) -> float | None:
    return _latest_indicators(symbol, today).get("atr")


def run(today: date | None = None) -> list[PaperTrade]:
    """
    Main entry point. Returns list of new PaperTrade rows created.
    Pass today explicitly for backtesting; defaults to date.today().
    """
    today = today or date.today()

    if not is_trading_day(today):
        logger.info(f"simulator: {today} is not a trading day — skipping")
        return []

    capital = _get_capital()

    # Daily-loss circuit breaker
    today_loss = _today_realised_pnl(today)
    if today_loss < -(capital * DAILY_LOSS_LIMIT_PCT):
        logger.warning(
            f"simulator: daily loss limit hit (₹{today_loss:,.0f}) — "
            f"no new entries today"
        )
        return []

    # FII selling streak circuit breaker
    try:
        from data.collectors.fii_dii import is_fii_selling_streak
        if is_fii_selling_streak(days=FII_SELL_STREAK_DAYS):
            logger.warning(
                f"simulator: FII net sellers for {FII_SELL_STREAK_DAYS}+ consecutive days "
                f"— no new entries (institutional headwind)"
            )
            return []
    except Exception as exc:
        logger.debug(f"simulator: FII/DII check skipped — {exc}")

    open_positions = _open_positions()
    open_symbols   = {t.symbol for t in open_positions}
    slots          = MAX_OPEN_POSITIONS - len(open_positions)

    if slots <= 0:
        logger.info(f"simulator: {len(open_positions)} open positions — no slots available")
        return []

    # Only consider high-conviction signals (strength >= MIN_SIGNAL_STRENGTH)
    signals = [s for s in _buy_signals_today(today) if s.strength >= MIN_SIGNAL_STRENGTH]
    if not signals:
        logger.info(f"simulator: no BUY signals with strength >= {MIN_SIGNAL_STRENGTH} today")
        return []

    new_trades: list[PaperTrade] = []

    for sig in signals:
        if slots <= 0:
            break
        if sig.symbol in open_symbols:
            continue  # already holding this stock

        entry_price = _todays_open(sig.symbol, today)
        if entry_price is None:
            logger.warning(f"simulator: no open price for {sig.symbol} on {today} — skipping")
            continue

        ind = _latest_indicators(sig.symbol, today)
        atr = ind.get("atr")
        if atr is None or atr <= 0 or (isinstance(atr, float) and math.isnan(atr)):
            logger.warning(f"simulator: no ATR for {sig.symbol} — skipping")
            continue

        # Stop: structural (just below swing low), clamped to [0.5×ATR, 2×ATR] of entry.
        # Clamping protects from too-tight (noise pings) AND inflated-ATR cases.
        floor_stop    = entry_price - ATR_STOP_MULTIPLIER * atr
        ceiling_stop  = entry_price - 0.5 * atr
        swing_low     = ind.get("swing_low_10")
        if swing_low and not (isinstance(swing_low, float) and math.isnan(swing_low)):
            raw = swing_low - SWING_LOW_BUFFER_ATR * atr
            stop_loss = max(floor_stop, min(raw, ceiling_stop))
        else:
            stop_loss = floor_stop

        risk_per_share = entry_price - stop_loss

        if risk_per_share <= 0:
            continue

        rr = (entry_price * MIN_RISK_REWARD) / risk_per_share  # sanity
        target = round(entry_price + MIN_RISK_REWARD * risk_per_share, 2)
        stop_loss = round(stop_loss, 2)

        # Position size: risk fixed fraction of current capital
        risk_amount = capital * RISK_PER_TRADE_PCT
        quantity    = math.floor(risk_amount / risk_per_share)

        if quantity < 1:
            logger.info(
                f"simulator: {sig.symbol} quantity<1 "
                f"(risk ₹{risk_amount:.0f} / {risk_per_share:.2f}) — skipping"
            )
            continue

        trade = PaperTrade(
            symbol=sig.symbol,
            entry_date=today,
            entry_price=entry_price,
            stop_loss=stop_loss,
            target=target,
            quantity=quantity,
            status="open",
            thesis=(
                f"BUY str={sig.strength} | {sig.reason[:200] if sig.reason else ''}"
            ),
            signal_id=sig.id,
        )

        with get_session() as s:
            s.add(trade)
            s.commit()
            s.refresh(trade)

        logger.info(
            f"simulator: NEW TRADE {sig.symbol}  "
            f"entry={entry_price:.2f}  stop={stop_loss:.2f}  "
            f"target={target:.2f}  qty={quantity}  "
            f"risk=₹{quantity * risk_per_share:,.0f}"
        )

        try:
            from alerts.telegram_bot import send_trade_entry
            send_trade_entry(sig.symbol, entry_price, stop_loss, target, quantity, trade.thesis or "")
        except Exception:
            pass

        new_trades.append(trade)
        open_symbols.add(sig.symbol)
        slots -= 1

    if not new_trades:
        logger.info(f"simulator: no new trades entered on {today}")

    return new_trades


if __name__ == "__main__":
    from data.storage.database import init_db
    init_db()
    trades = run()
    if trades:
        print(f"\n{'─'*55}")
        print(f"  {'Symbol':<12} {'Entry':>8} {'Stop':>8} {'Target':>8} {'Qty':>5}")
        print(f"{'─'*55}")
        for t in trades:
            print(
                f"  {t.symbol:<12} {t.entry_price:>8.2f} "
                f"{t.stop_loss:>8.2f} {t.target:>8.2f} {t.quantity:>5}"
            )
        print(f"{'─'*55}\n")
    else:
        print("No new trades today.")
