"""
Intraday late-session recovery monitor.

Runs every 5 minutes from 14:45 to 15:25 IST on trading days.

Detects the pattern:
  1. A result / earnings filing was announced in the last 48 hours.
  2. Stock was trading negative (vs prev close) at ~14:45–14:55.
  3. Stock has since recovered to flat or positive — in the last 10-min window.
  4. Volume in the recovery window is elevated (≥ 1.5× the avg 10-min volume for the day).

When all four conditions are true and no alert was sent today for that symbol,
fires a Telegram notification immediately.

Data source: yfinance 1-min bars (.NS suffix). Note: ~15 min delay for NSE.
Prev close: from the local OHLCVDaily table.

Run manually for testing:
  python -m scheduler.jobs.intraday_monitor
"""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from loguru import logger

from config.nse_calendar import is_trading_day
from config.settings import BANKING_STOCKS, STOCK_NAMES

_IST = ZoneInfo("Asia/Kolkata")

# ── Thresholds ─────────────────────────────────────────────────────────────────
_TROUGH_PCT     = -0.50   # stock must have been at least this % below prev close
_RECOVERY_PCT   = -0.20   # stock is "recovered" when within this % of prev close (or above)
_VOLUME_RATIO   = 1.5     # late-window volume must be ≥ this × avg 10-min bucket
_EARLY_WINDOW   = (14, 30, 14, 55)   # (start_h, start_m, end_h, end_m) IST
_LATE_WINDOW    = (15,  0, 15, 25)   # recovery check window

# Filing keywords that indicate an earnings / result event
_RESULT_KEYWORDS = {"result", "financial", "earnings", "profit", "quarterly", "annual", "q1", "q2", "q3", "q4"}

# In-memory deduplication — {symbol: date} — prevents repeat alerts on same day
_alerted: dict[str, date] = {}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _prev_close(symbol: str) -> float | None:
    """Fetch yesterday's adjusted close from the local DB."""
    from data.storage.database import OHLCVDaily, get_session
    today = date.today()
    with get_session() as s:
        row = (
            s.query(OHLCVDaily)
            .filter(
                OHLCVDaily.symbol == symbol,
                OHLCVDaily.date < today,
            )
            .order_by(OHLCVDaily.date.desc())
            .first()
        )
    if row is None:
        return None
    return row.adjusted_close or row.close


def _recent_result_filing(symbol: str, hours: int = 48) -> str | None:
    """
    Return the subject of the most recent earnings-related filing, or None.
    Only looks back `hours` hours to stay relevant.
    """
    from data.storage.database import CorporateFiling, get_session
    cutoff = datetime.now(_IST) - timedelta(hours=hours)
    with get_session() as s:
        filings = (
            s.query(CorporateFiling)
            .filter(
                CorporateFiling.symbol == symbol,
                CorporateFiling.published_at >= cutoff,
            )
            .order_by(CorporateFiling.published_at.desc())
            .all()
        )
    for f in filings:
        subject = (f.subject or f.category or "").lower()
        if any(kw in subject for kw in _RESULT_KEYWORDS):
            return f.subject or f.category or "Recent filing"
    return None


def _intraday_bars(symbol: str) -> "pd.DataFrame | None":
    """Fetch today's 1-min bars from yfinance. Returns None on failure."""
    try:
        import yfinance as yf
        import pandas as pd
        ticker = yf.Ticker(f"{symbol}.NS")
        df = ticker.history(period="1d", interval="1m")
        if df.empty:
            return None
        # Ensure index is tz-aware IST
        if df.index.tzinfo is None:
            df.index = df.index.tz_localize("Asia/Kolkata")
        else:
            df.index = df.index.tz_convert("Asia/Kolkata")
        return df
    except Exception as exc:
        logger.warning(f"intraday_monitor: yfinance fetch failed for {symbol} — {exc}")
        return None


def _window_bars(df: "pd.DataFrame", start_h: int, start_m: int, end_h: int, end_m: int) -> "pd.DataFrame":
    today = date.today()
    start = datetime(today.year, today.month, today.day, start_h, start_m, tzinfo=_IST)
    end   = datetime(today.year, today.month, today.day, end_h,   end_m,   tzinfo=_IST)
    return df[(df.index >= start) & (df.index <= end)]


def _check_symbol(symbol: str) -> bool:
    """
    Evaluate the late-session recovery pattern for one symbol.
    Returns True if an alert was fired.
    """
    today = date.today()

    # Already alerted today?
    if _alerted.get(symbol) == today:
        return False

    # Must have a recent earnings filing
    filing_subject = _recent_result_filing(symbol)
    if not filing_subject:
        return False

    prev_close = _prev_close(symbol)
    if not prev_close or prev_close <= 0:
        logger.warning(f"intraday_monitor: no prev_close for {symbol}")
        return False

    df = _intraday_bars(symbol)
    if df is None or df.empty:
        return False

    # ── Early window: stock was deeply negative ───────────────────────────────
    early = _window_bars(df, *_EARLY_WINDOW)
    if early.empty:
        return False

    # Use the lowest close in the early window (worst point)
    trough_price = early["Close"].min()
    pct_at_trough = (trough_price - prev_close) / prev_close * 100
    if pct_at_trough > _TROUGH_PCT:
        # Never went negative enough — not our setup
        return False

    # ── Late window: stock has recovered ─────────────────────────────────────
    late = _window_bars(df, *_LATE_WINDOW)
    if late.empty:
        # Try the most recent available bars instead (yfinance delay means
        # the "late" window may not have data yet — use last 10 bars)
        late = df.tail(10)

    latest_price = late["Close"].iloc[-1]
    pct_now = (latest_price - prev_close) / prev_close * 100
    if pct_now < _RECOVERY_PCT:
        # Still too negative — no recovery yet
        return False

    # ── Volume spike in late window ───────────────────────────────────────────
    # Compare late-window volume to average 10-min buckets across the full day
    full_vol  = df["Volume"].resample("10min").sum()
    avg_10min = float(full_vol.mean()) if len(full_vol) > 1 else 0
    late_vol  = float(late["Volume"].sum())
    volume_ratio = late_vol / avg_10min if avg_10min > 0 else 0

    if volume_ratio < _VOLUME_RATIO:
        logger.debug(
            f"intraday_monitor: {symbol} recovered but volume ratio {volume_ratio:.1f}× "
            f"< threshold {_VOLUME_RATIO}× — skipping"
        )
        return False

    # ── All conditions met — fire alert ──────────────────────────────────────
    stock_name = STOCK_NAMES.get(symbol, symbol)
    logger.info(
        f"intraday_monitor: RECOVERY DETECTED {symbol}  "
        f"trough={pct_at_trough:+.2f}%  now={pct_now:+.2f}%  vol={volume_ratio:.1f}×"
    )

    try:
        from alerts.telegram_bot import send_late_recovery_alert
        send_late_recovery_alert(
            symbol         = symbol,
            stock_name     = stock_name,
            pct_at_trough  = pct_at_trough,
            pct_now        = pct_now,
            volume_ratio   = volume_ratio,
            filing_subject = filing_subject,
        )
    except Exception as exc:
        logger.error(f"intraday_monitor: Telegram send failed — {exc}")

    _alerted[symbol] = today
    return True


# ── Main entry point ──────────────────────────────────────────────────────────

def run() -> None:
    """
    Scan all banking stocks for late-session recovery.
    Called every 5 minutes by the scheduler between 14:45 and 15:25 IST.
    """
    today = date.today()
    if not is_trading_day(today):
        return

    now_ist = datetime.now(_IST).time()

    # Only run during the monitoring window (14:45 – 15:25)
    from datetime import time as dtime
    if not (dtime(14, 45) <= now_ist <= dtime(15, 25)):
        logger.debug(f"intraday_monitor: outside window ({now_ist}) — skipping")
        return

    logger.info(f"intraday_monitor: scanning {len(BANKING_STOCKS)} stocks at {now_ist}")
    alerts_fired = 0
    for symbol in BANKING_STOCKS:
        try:
            if _check_symbol(symbol):
                alerts_fired += 1
        except Exception as exc:
            logger.error(f"intraday_monitor: {symbol} check failed — {exc}")

    logger.info(f"intraday_monitor: scan complete — {alerts_fired} alert(s) fired")


if __name__ == "__main__":
    # Manual test — bypasses the time-window guard
    import sys
    from data.storage.database import init_db
    init_db()

    symbol = sys.argv[1].upper() if len(sys.argv) > 1 else None
    symbols = [symbol] if symbol else BANKING_STOCKS

    for sym in symbols:
        df = _intraday_bars(sym)
        pc = _prev_close(sym)
        filing = _recent_result_filing(sym)
        print(f"\n{'─'*50}")
        print(f"  {sym}  prev_close={pc}  filing={filing}")
        if df is not None and not df.empty and pc:
            latest = df["Close"].iloc[-1]
            pct = (latest - pc) / pc * 100
            early = _window_bars(df, *_EARLY_WINDOW)
            trough = ((early["Close"].min() - pc) / pc * 100) if not early.empty else None
            print(f"  Latest price: {latest:.2f}  ({pct:+.2f}%)")
            print(f"  Early window trough: {trough:+.2f}%" if trough is not None else "  Early window: no data")
            print(f"  Total bars today: {len(df)}")
        else:
            print("  No intraday data available")
