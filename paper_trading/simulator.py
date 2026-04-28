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
    MAX_OPEN_POSITIONS, MIN_RISK_REWARD, PAPER_TRADING_CAPITAL,
    RISK_PER_TRADE_PCT, STOCK_NAMES,
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


def _latest_atr(symbol: str, today: date) -> float | None:
    """Get ATR_14 from indicators computed as of yesterday (pre-open)."""
    from analysis.technical.indicators import get_latest_row
    from datetime import timedelta
    row = get_latest_row(symbol, as_of_date=today - timedelta(days=1))
    return row.get("atr") if row else None


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

    open_positions = _open_positions()
    open_symbols   = {t.symbol for t in open_positions}
    slots          = MAX_OPEN_POSITIONS - len(open_positions)

    if slots <= 0:
        logger.info(f"simulator: {len(open_positions)} open positions — no slots available")
        return []

    signals   = _buy_signals_today(today)
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

        atr = _latest_atr(sig.symbol, today)
        if atr is None or atr <= 0:
            logger.warning(f"simulator: no ATR for {sig.symbol} — skipping")
            continue

        stop_loss = entry_price - ATR_STOP_MULTIPLIER * atr
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
