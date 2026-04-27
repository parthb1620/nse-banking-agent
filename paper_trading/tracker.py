"""
Paper trading tracker — EOD close logic + P&L reporting.

Run after 15:30 IST on trading days:
  python -m paper_trading.tracker

Logic (per open trade):
  1. Fetch today's OHLCV (open, high, low, close).
  2. Gap-down protection: if today's open <= stop_loss, exit at open (worse fill).
  3. Stop-first rule: if both stop AND target hit same day, stop wins.
  4. P&L = (exit_price - entry_price) × quantity  minus  0.40% round-trip cost.
  5. Update status: 'closed_stop' | 'closed_target'.

Also exposes:
  report()           — print current open + closed summary to stdout
  get_summary()      — returns dict for dashboard / Telegram
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

from loguru import logger

from config.nse_calendar import is_trading_day
from config.settings import BACKTEST_TRANSACTION_COST_PCT, BANKING_STOCKS, PAPER_TRADING_CAPITAL
from data.storage.database import OHLCVDaily, PaperTrade, get_session

_IST = ZoneInfo("Asia/Kolkata")


# ── helpers ────────────────────────────────────────────────────────────────────

def _fetch_ohlcv(symbol: str, day: date) -> dict | None:
    with get_session() as s:
        row = s.query(OHLCVDaily).filter(
            OHLCVDaily.symbol == symbol,
            OHLCVDaily.date == day,
        ).first()
    if row is None:
        return None
    return {
        "open":  row.open,
        "high":  row.high,
        "low":   row.low,
        "close": row.adjusted_close or row.close,
    }


def _trade_cost(gross_pnl: float, entry_price: float, qty: int) -> float:
    """Deduct 0.40% round-trip transaction cost on notional value."""
    notional = entry_price * qty
    return gross_pnl - notional * BACKTEST_TRANSACTION_COST_PCT


# ── core EOD check ─────────────────────────────────────────────────────────────

def check_open_trades(today: date | None = None) -> dict[str, list]:
    """
    Evaluate all open trades against today's price action.
    Returns {"closed": [PaperTrade, ...], "still_open": [PaperTrade, ...]}.
    """
    today = today or date.today()

    with get_session() as s:
        open_trades = s.query(PaperTrade).filter(PaperTrade.status == "open").all()

    closed:     list[PaperTrade] = []
    still_open: list[PaperTrade] = []

    for trade in open_trades:
        candle = _fetch_ohlcv(trade.symbol, today)
        if candle is None:
            logger.warning(f"tracker: no OHLCV for {trade.symbol} on {today} — skipping")
            still_open.append(trade)
            continue

        day_open  = candle["open"]
        day_high  = candle["high"]
        day_low   = candle["low"]
        day_close = candle["close"]

        stop   = trade.stop_loss
        target = trade.target

        exit_price: float | None = None
        status: str | None = None

        # Gap-down through stop — fill at open, not stop
        if day_open is not None and stop is not None and day_open <= stop:
            exit_price = day_open
            status     = "closed_stop"

        elif day_low is not None and day_high is not None and stop is not None and target is not None:
            stop_hit   = day_low  <= stop
            target_hit = day_high >= target

            if stop_hit and target_hit:
                # Stop-first rule: assume stop triggered before target intraday
                exit_price = stop
                status     = "closed_stop"
            elif stop_hit:
                exit_price = stop
                status     = "closed_stop"
            elif target_hit:
                exit_price = target
                status     = "closed_target"

        if exit_price is not None and status is not None:
            gross_pnl = (exit_price - trade.entry_price) * trade.quantity
            net_pnl   = round(_trade_cost(gross_pnl, trade.entry_price, trade.quantity), 2)

            with get_session() as s:
                t = s.get(PaperTrade, trade.id)
                t.exit_date  = today
                t.exit_price = round(exit_price, 2)
                t.status     = status
                t.pnl        = net_pnl
                s.commit()

            outcome = "TARGET" if status == "closed_target" else "STOP"
            logger.info(
                f"tracker: {trade.symbol} {outcome}  "
                f"entry={trade.entry_price:.2f} exit={exit_price:.2f}  "
                f"qty={trade.quantity}  pnl=₹{net_pnl:,.0f}"
            )
            closed.append(trade)
        else:
            still_open.append(trade)

    return {"closed": closed, "still_open": still_open}


# ── summary / reporting ────────────────────────────────────────────────────────

def get_summary() -> dict:
    """Return a summary dict suitable for Telegram or dashboard."""
    with get_session() as s:
        all_trades  = s.query(PaperTrade).all()
        open_trades = [t for t in all_trades if t.status == "open"]
        closed      = [t for t in all_trades if t.status in ("closed_target", "closed_stop", "closed_manual")]

    total_pnl   = sum(t.pnl for t in closed if t.pnl is not None)
    wins        = [t for t in closed if t.pnl is not None and t.pnl > 0]
    losses      = [t for t in closed if t.pnl is not None and t.pnl <= 0]
    win_rate    = len(wins) / len(closed) * 100 if closed else 0.0
    avg_win     = sum(t.pnl for t in wins)   / len(wins)   if wins   else 0.0
    avg_loss    = sum(t.pnl for t in losses) / len(losses) if losses else 0.0
    capital_now = PAPER_TRADING_CAPITAL + total_pnl

    return {
        "capital_initial": PAPER_TRADING_CAPITAL,
        "capital_current": round(capital_now, 2),
        "total_pnl":       round(total_pnl, 2),
        "return_pct":      round(total_pnl / PAPER_TRADING_CAPITAL * 100, 2),
        "trades_total":    len(closed),
        "trades_open":     len(open_trades),
        "win_rate":        round(win_rate, 1),
        "avg_win":         round(avg_win, 2),
        "avg_loss":        round(avg_loss, 2),
        "open_positions":  [
            {
                "symbol":      t.symbol,
                "entry_date":  str(t.entry_date),
                "entry_price": t.entry_price,
                "stop_loss":   t.stop_loss,
                "target":      t.target,
                "quantity":    t.quantity,
            }
            for t in open_trades
        ],
    }


def report() -> None:
    """Print a formatted P&L report to stdout."""
    s = get_summary()

    print(f"\n{'═'*62}")
    print(f"  Paper Trading Report  —  {datetime.now(_IST).strftime('%d %b %Y %H:%M IST')}")
    print(f"{'═'*62}")
    print(f"  Capital:  ₹{s['capital_initial']:>10,.0f}  →  ₹{s['capital_current']:>10,.0f}")
    print(f"  P&L:      ₹{s['total_pnl']:>+10,.0f}  ({s['return_pct']:+.2f}%)")
    print(f"  Trades:   {s['trades_total']} closed  |  {s['trades_open']} open")
    if s["trades_total"]:
        print(f"  Win rate: {s['win_rate']:.1f}%  |  avg win ₹{s['avg_win']:+,.0f}  avg loss ₹{s['avg_loss']:+,.0f}")

    if s["open_positions"]:
        print(f"\n  {'Symbol':<12} {'Since':<12} {'Entry':>8} {'Stop':>8} {'Target':>8} {'Qty':>5}")
        print(f"  {'─'*58}")
        for p in s["open_positions"]:
            print(
                f"  {p['symbol']:<12} {p['entry_date']:<12} "
                f"{p['entry_price']:>8.2f} {p['stop_loss']:>8.2f} "
                f"{p['target']:>8.2f} {p['quantity']:>5}"
            )

    with get_session() as session:
        closed = (
            session.query(PaperTrade)
            .filter(PaperTrade.status.in_(["closed_target", "closed_stop", "closed_manual"]))
            .order_by(PaperTrade.exit_date.desc())
            .limit(10)
            .all()
        )

    if closed:
        print(f"\n  Last {len(closed)} closed trades:")
        print(f"  {'Symbol':<12} {'Exit date':<12} {'P&L':>10} {'Status'}")
        print(f"  {'─'*50}")
        for t in closed:
            icon = "✓" if t.pnl and t.pnl > 0 else "✗"
            print(
                f"  {t.symbol:<12} {str(t.exit_date):<12} "
                f"₹{t.pnl:>+9,.0f}  {icon} {t.status.replace('closed_','')}"
            )

    print(f"{'═'*62}\n")


# ── entry point ────────────────────────────────────────────────────────────────

def run(today: date | None = None) -> None:
    today = today or date.today()

    if not is_trading_day(today):
        logger.info(f"tracker: {today} is not a trading day — skipping")
        return

    result = check_open_trades(today)
    logger.info(
        f"tracker: {len(result['closed'])} closed  "
        f"{len(result['still_open'])} still open"
    )
    report()


if __name__ == "__main__":
    from data.storage.database import init_db
    init_db()
    run()
