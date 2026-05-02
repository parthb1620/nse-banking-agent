"""
Intraday breakout / breakdown monitor.

Runs every 5 minutes from 09:20 to 15:25 IST on trading days.

Fires a Telegram alert when ALL of the following are true:
  1. Price has moved ≥ _MOVE_PCT % from the previous day's close
     (positive = breakout, negative = breakdown)
  2. Today's accumulated volume is ≥ _VOLUME_RATIO × the expected volume
     for this time of day (based on 20-session average daily volume).
  3. No alert has been sent for that symbol today in the same direction.

Special case — 52W-high breakout:
  When a stock's current price exceeds its 52-week high (from the local DB),
  the alert header is upgraded to "52W HIGH BREAKOUT" regardless of
  whether the basic _MOVE_PCT threshold is crossed.

Data source: Groww live-data API (real-time, no delay).
  - Prices: batch LTP via Groww at the start of each 5-min scan.
  - Volume: per-symbol live quote from Groww, time-normalized against 20-day avg.
  - Fallback: yfinance if Groww is unavailable.
Prev close / 52W high: from the local OHLCVDaily table.

Run manually for testing:
  python -m scheduler.jobs.breakout_monitor
  python -m scheduler.jobs.breakout_monitor WAAREEENER
"""

from datetime import date, datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

from loguru import logger

from config.nse_calendar import is_trading_day
from config.settings import ALL_STOCKS, ALL_STOCK_NAMES as STOCK_NAMES, SYMBOL_SECTOR

_IST = ZoneInfo("Asia/Kolkata")

# ── Thresholds ─────────────────────────────────────────────────────────────────
_MOVE_PCT      = 2.0   # % move from prev close required to trigger alert
_VOLUME_RATIO  = 1.5   # recent 10-min vol must be ≥ this × avg 10-min bucket
_WINDOW_MINS   = 10    # minutes of recent bars to measure "current" volume

_MARKET_OPEN   = dtime(9, 20)
_MARKET_CLOSE  = dtime(15, 25)

# In-memory deduplication — {symbol: {"up": date, "down": date}}
# prevents repeat alerts on same day for same direction
_alerted: dict[str, dict[str, date]] = {}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _prev_close(symbol: str) -> float | None:
    """Fetch yesterday's adjusted close from the local DB."""
    from data.storage.database import OHLCVDaily, get_session
    today = date.today()
    with get_session() as s:
        row = (
            s.query(OHLCVDaily)
            .filter(OHLCVDaily.symbol == symbol, OHLCVDaily.date < today)
            .order_by(OHLCVDaily.date.desc())
            .first()
        )
    if row is None:
        return None
    return row.adjusted_close or row.close


def _52w_high(symbol: str) -> float | None:
    """Rolling 52-week high from the local DB (252 sessions)."""
    from data.storage.database import OHLCVDaily, get_session
    cutoff = date.today() - timedelta(days=365)
    with get_session() as s:
        rows = (
            s.query(OHLCVDaily.high)
            .filter(OHLCVDaily.symbol == symbol, OHLCVDaily.date >= cutoff)
            .all()
        )
    if not rows:
        return None
    return max(r.high for r in rows if r.high)


def _avg_daily_volume(symbol: str, sessions: int = 20) -> float:
    """Average daily volume from the last `sessions` trading days (from DB)."""
    from data.storage.database import OHLCVDaily, get_session
    with get_session() as s:
        rows = (
            s.query(OHLCVDaily.volume)
            .filter(OHLCVDaily.symbol == symbol, OHLCVDaily.date < date.today())
            .order_by(OHLCVDaily.date.desc())
            .limit(sessions)
            .all()
        )
    vols = [r.volume for r in rows if r.volume]
    return sum(vols) / len(vols) if vols else 0.0


def _time_normalized_volume_ratio(today_vol: int, avg_daily_vol: float) -> float:
    """
    Compare today's accumulated volume to what's expected at this time of day.
    Returns >1.0 when volume rate is above average, <1.0 when below.
    Market session = 09:15 to 15:30 (375 minutes).
    """
    if avg_daily_vol <= 0 or today_vol <= 0:
        return 0.0
    now_ist  = datetime.now(_IST)
    open_min = 9 * 60 + 15
    now_min  = now_ist.hour * 60 + now_ist.minute
    elapsed  = max(now_min - open_min, 1)
    expected = avg_daily_vol * (elapsed / 375)
    return today_vol / expected if expected > 0 else 0.0


def _check_symbol(symbol: str, ltp_cache: dict | None = None) -> bool:
    """
    Evaluate breakout/breakdown conditions for one symbol.
    ltp_cache: pre-fetched {symbol: price} from batch Groww LTP call.
    Returns True if an alert was fired.
    """
    today   = date.today()
    alerted = _alerted.setdefault(symbol, {})

    prev_close = _prev_close(symbol)
    if not prev_close or prev_close <= 0:
        return False

    # ── Live price ────────────────────────────────────────────────────────────
    latest_price: float | None = (ltp_cache or {}).get(symbol)
    if latest_price is None:
        # Per-symbol fallback via Groww quote
        from data.collectors.groww_client import fetch_live_quote
        q = fetch_live_quote(symbol)
        latest_price = q.get("ltp") if q else None
    if not latest_price:
        return False

    pct_change = (latest_price - prev_close) / prev_close * 100
    direction  = "up" if pct_change >= 0 else "down"

    if alerted.get(direction) == today:
        return False

    w52_high = _52w_high(symbol)
    is_52w   = w52_high is not None and latest_price >= w52_high * 0.995

    if direction == "down" and abs(pct_change) < _MOVE_PCT:
        return False
    if direction == "up" and pct_change < _MOVE_PCT and not is_52w:
        return False

    # ── Volume confirmation (time-normalized daily volume) ────────────────────
    from data.collectors.groww_client import fetch_live_quote
    q         = fetch_live_quote(symbol)
    today_vol = q.get("volume", 0) if q else 0
    avg_vol   = _avg_daily_volume(symbol)
    volume_ratio = _time_normalized_volume_ratio(today_vol, avg_vol)

    if today_vol > 0 and volume_ratio < _VOLUME_RATIO:
        logger.debug(
            f"breakout_monitor: {symbol} {pct_change:+.1f}% "
            f"but vol ratio {volume_ratio:.1f}× < {_VOLUME_RATIO}× — skipping"
        )
        return False

    # ── All conditions met — fire alert ──────────────────────────────────────
    stock_name = STOCK_NAMES.get(symbol, symbol)
    sector     = SYMBOL_SECTOR.get(symbol, "")
    label      = "52W HIGH BREAKOUT" if is_52w and direction == "up" else \
                 ("BREAKOUT" if direction == "up" else "BREAKDOWN")

    logger.info(
        f"breakout_monitor: {label} {symbol}  "
        f"pct={pct_change:+.2f}%  vol={volume_ratio:.1f}×"
    )

    try:
        from alerts.telegram_bot import send_breakout_alert
        send_breakout_alert(
            symbol       = symbol,
            stock_name   = stock_name,
            sector       = sector,
            label        = label,
            pct_change   = pct_change,
            latest_price = latest_price,
            prev_close   = prev_close,
            volume_ratio = volume_ratio,
            w52_high     = w52_high if is_52w else None,
        )
    except Exception as exc:
        logger.error(f"breakout_monitor: Telegram send failed — {exc}")

    alerted[direction] = today
    return True


# ── Main entry point ──────────────────────────────────────────────────────────

def run() -> None:
    """
    Scan all watchlist stocks for intraday breakouts/breakdowns.
    Called every 5 minutes by the scheduler between 09:20 and 15:25 IST.
    """
    today = date.today()
    if not is_trading_day(today):
        return

    now_ist = datetime.now(_IST).time()
    if not (_MARKET_OPEN <= now_ist <= _MARKET_CLOSE):
        logger.debug(f"breakout_monitor: outside window ({now_ist}) — skipping")
        return

    logger.info(f"breakout_monitor: scanning {len(ALL_STOCKS)} stocks at {now_ist}")

    # Batch LTP fetch from Groww (real-time, one call for all stocks)
    ltp_cache: dict = {}
    try:
        from data.collectors.groww_client import fetch_ltp
        for i in range(0, len(ALL_STOCKS), 50):
            ltp_cache.update(fetch_ltp(ALL_STOCKS[i:i + 50]))
        logger.debug(f"breakout_monitor: Groww LTP — {len(ltp_cache)} prices fetched")
    except Exception as exc:
        logger.warning(f"breakout_monitor: Groww batch LTP failed — {exc}")

    alerts_fired = 0
    for symbol in ALL_STOCKS:
        try:
            if _check_symbol(symbol, ltp_cache=ltp_cache):
                alerts_fired += 1
        except Exception as exc:
            logger.error(f"breakout_monitor: {symbol} failed — {exc}")

    logger.info(f"breakout_monitor: scan done — {alerts_fired} alert(s) fired")


if __name__ == "__main__":
    import sys
    from data.storage.database import init_db
    init_db()

    symbol = sys.argv[1].upper() if len(sys.argv) > 1 else None
    symbols = [symbol] if symbol else ALL_STOCKS

    for sym in symbols:
        import yfinance as yf
        t = yf.Ticker(f"{sym}.NS")
        h = t.history(period="1d", interval="1m")
        if h.empty:
            print(f"{sym}: no intraday data")
            continue
        if h.index.tzinfo is None:
            h.index = h.index.tz_localize("Asia/Kolkata")
        else:
            h.index = h.index.tz_convert("Asia/Kolkata")
        latest = float(h["Close"].iloc[-1])
        pc = _prev_close(sym)
        w52 = _52w_high(sym)
        pct = (latest - pc) / pc * 100 if pc else None
        print(f"\n{'─'*55}")
        print(f"  {sym}  ({STOCK_NAMES.get(sym, sym)})")
        print(f"  Latest: ₹{latest:.2f}  PrevClose: ₹{pc or '?'}")
        if pct is not None:
            print(f"  Change: {pct:+.2f}%")
        if w52:
            print(f"  52W High: ₹{w52:.2f}  {'← AT 52W HIGH!' if latest >= w52 * 0.995 else ''}")
        print(f"  Total 1-min bars today: {len(h)}")
