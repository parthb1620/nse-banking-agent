"""
APScheduler orchestrator — daily data collection + alert pipeline.

Jobs (all times IST):
  Sun 08:00           — long_term_scan:  weekly fundamentals-first picks (60 stocks)
  Mon–Fri 08:30       — morning_scan:    [SHORT] news + sentiment + top picks
  Mon–Fri 09:20       — paper_entry:     paper trade entries (shortterm engine)
  Mon–Fri 09:20–15:25 — breakout_monitor: full-day breakout/breakdown (every 5 min)
  Mon–Fri 14:30       — btst_scan:       [BTST] overnight pick alert
  Mon–Fri 14:40       — intraday_prefetch: refresh news before intraday
  Mon–Fri 14:45–15:25 — intraday_monitor: late-session recovery detector
  Mon–Fri 15:35       — paper_exit:      paper trade exit check
  Mon–Fri 16:15       — eod_collection:  Bhavcopy + quality + fundamentals
  Mon–Fri 16:17       — eod_report:      [SHORT] EOD scores + signals
  1st of month 07:00  — monthly_optimize: walk-forward RSI/ATR optimiser

Run from project root:
  python -m scheduler.daily_runner             # start scheduler (blocking)
  python -m scheduler.daily_runner once        # run EOD collection once
  python -m scheduler.daily_runner morning     # run morning scan once
  python -m scheduler.daily_runner eod         # run EOD report once
  python -m scheduler.daily_runner btst        # run BTST scan once
  python -m scheduler.daily_runner longterm    # run long-term weekly scan once
  python -m scheduler.daily_runner intraday    # run intraday monitor once
  python -m scheduler.daily_runner breakout    # run breakout monitor once
"""

import sys
from datetime import date

from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger

from config.nse_calendar import is_trading_day
from config.settings import (
    BTST_SCAN_TIME, EOD_REPORT_TIME, LONGTERM_SCAN_TIME,
    MORNING_SCAN_TIME, LOG_LEVEL, LOG_DIR, SCHEDULER_TIMEZONE,
)

# ── Logging setup ──────────────────────────────────────────────────────────────
logger.remove()
logger.add(sys.stderr, level=LOG_LEVEL)
logger.add(
    LOG_DIR / "daily_runner_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="30 days",
    level=LOG_LEVEL,
)


def eod_collection_job() -> None:
    """
    Run after market close (4:00 PM IST).
    1. Download today's Bhavcopy
    2. Fill any gaps from Groww API
    3. Apply corporate action adjustments
    4. Run data quality checks
    5. Update news and filings
    6. Populate banking metrics from latest fundamentals
    """
    today = date.today()

    if not is_trading_day(today):
        logger.info(f"EOD job: {today} is not a trading day — skipping")
        return

    logger.info(f"=== EOD collection job started for {today} ===")

    # Step 1: Primary — NSE Bhavcopy
    try:
        from data.collectors.nse_bhavcopy import run_daily
        run_daily(today)
    except Exception as exc:
        logger.error(f"Bhavcopy step failed: {exc}")

    # Step 2: Secondary — Groww API (fill gaps for last 5 days)
    # Groww publishes EOD data with a delay; fetching a window ensures we catch
    # any days missed by Bhavcopy without failing when today isn't published yet.
    try:
        from datetime import timedelta
        from data.collectors.groww_client import fetch_historical, store_historical
        from config.settings import ALL_STOCKS
        gap_start = today - timedelta(days=5)
        for symbol in ALL_STOCKS:
            rows = fetch_historical(symbol, gap_start, today)
            if rows:
                store_historical(symbol, rows)
                logger.info(f"Groww gap-fill: stored {len(rows)} rows for {symbol}")
            else:
                logger.debug(f"Groww gap-fill: no new rows for {symbol} (data not yet published)")
    except Exception as exc:
        logger.error(f"Groww gap-fill step failed: {exc}")

    # Step 3: Corporate action adjustments
    try:
        from data.quality.corporate_actions import run_all as run_corp_actions
        run_corp_actions()
    except Exception as exc:
        logger.error(f"Corporate action step failed: {exc}")

    # Step 4: Quality checks
    try:
        from data.quality.candle_checks import run_all as run_quality
        scores = run_quality(today)
        bad = {s: sc for s, sc in scores.items() if sc < 0.95}
        if bad:
            logger.warning(f"Quality below 95% for: {bad}")
    except Exception as exc:
        logger.error(f"Quality check step failed: {exc}")

    # Step 5: News and NSE filings
    try:
        from data.collectors.news_collector import run_all as run_news
        from data.collectors.nse_filings import run_all as run_filings
        run_news()
        run_filings(days_back=7)
    except Exception as exc:
        logger.error(f"News/filings step failed: {exc}")

    # Step 5b: FII/DII daily flows
    try:
        from data.collectors.fii_dii import run_daily as run_fii_dii
        run_fii_dii()
    except Exception as exc:
        logger.error(f"FII/DII collection failed: {exc}")

    # Step 6: Banking metrics
    try:
        from analysis.fundamental.banking_metrics import run_all as run_metrics
        run_metrics()
    except Exception as exc:
        logger.error(f"Banking metrics step failed: {exc}")

    logger.info(f"=== EOD collection job completed for {today} ===")


def morning_scan_job() -> None:
    """08:30 IST — news + sentiment + top picks Telegram alert."""
    from scheduler.jobs.morning_scan import run
    run()


def paper_trading_entry_job() -> None:
    """09:20 IST — enter paper trades based on today's BUY signals."""
    from paper_trading.simulator import run
    run()


def paper_trading_exit_job() -> None:
    """15:35 IST — close paper trades that hit stop or target today."""
    from paper_trading.tracker import run
    run()


def intraday_prefetch_job() -> None:
    """14:40 IST — refresh news + filings just before intraday monitor starts."""
    today = date.today()
    if not is_trading_day(today):
        return
    logger.info("=== Intraday pre-fetch: news + filings ===")
    try:
        from data.collectors.news_collector import run_all as run_news
        run_news()
    except Exception as exc:
        logger.warning(f"Intraday pre-fetch: news failed — {exc}")
    try:
        from data.collectors.nse_filings import run_all as run_filings
        run_filings(days_back=1)
    except Exception as exc:
        logger.warning(f"Intraday pre-fetch: filings failed — {exc}")
    logger.info("=== Intraday pre-fetch complete ===")


def breakout_monitor_job() -> None:
    """09:20–15:25 IST (every 5 min) — full-day breakout/breakdown detector."""
    from scheduler.jobs.breakout_monitor import run
    run()


def intraday_monitor_job() -> None:
    """14:45–15:25 IST (every 5 min) — late-session recovery detector."""
    from scheduler.jobs.intraday_monitor import run
    run()


def eod_report_job() -> None:
    """16:17 IST — updated scores + signals Telegram alert."""
    from scheduler.jobs.eod_report import run
    run()


def btst_scan_job() -> None:
    """14:30 IST — BTST overnight pick alert."""
    from scheduler.jobs.btst_scan import run
    run()


def long_term_scan_job() -> None:
    """Sunday 08:00 IST — weekly long-term fundamentals-first scan."""
    from scheduler.jobs.long_term_scan import run
    run()


def monthly_optimize_job() -> None:
    """1st of each month, 07:00 IST — walk-forward optimize all 7 stocks."""
    logger.info("=== Monthly walk-forward optimization started ===")
    try:
        from backtesting.optimizer import optimize_all
        results = optimize_all()
        summary = "  ".join(
            f"{sym}(RSI {p.get('rsi_entry_low')}-{p.get('rsi_entry_high')} ATR×{p.get('atr_stop_multiplier')})"
            for sym, p in results.items()
        )
        logger.info(f"Optimization complete: {summary}")
        try:
            from alerts.telegram_bot import send
            send(f"🔧 <b>Monthly optimizer complete</b>\n\n{summary.replace('  ', chr(10))}")
        except Exception:
            pass
    except Exception as exc:
        logger.error(f"Monthly optimization failed: {exc}")
    logger.info("=== Monthly walk-forward optimization done ===")


def run_once() -> None:
    """Run the EOD collection job immediately."""
    eod_collection_job()


def start_scheduler() -> None:
    """Start the blocking scheduler. Runs until Ctrl+C."""
    from data.storage.database import init_db
    init_db()

    scheduler = BlockingScheduler(timezone=SCHEDULER_TIMEZONE)

    # Long-term weekly scan — every Sunday at 08:00 IST
    lh, lm = LONGTERM_SCAN_TIME.split(":")
    scheduler.add_job(
        long_term_scan_job,
        trigger="cron",
        day_of_week="sun",
        hour=int(lh), minute=int(lm),
        id="long_term_scan",
        name="[LONG] Weekly fundamentals scan",
        misfire_grace_time=3600,
    )

    # Morning scan at 08:30 IST (Mon–Fri)
    mh, mm = MORNING_SCAN_TIME.split(":")
    scheduler.add_job(
        morning_scan_job,
        trigger="cron",
        day_of_week="mon-fri",
        hour=int(mh), minute=int(mm),
        id="morning_scan",
        name="[SHORT] Morning scan + alert",
        misfire_grace_time=300,
    )

    # Paper trading entry at 09:20 IST (5 min after market open)
    scheduler.add_job(
        paper_trading_entry_job,
        trigger="cron",
        hour=9, minute=20,
        id="paper_entry",
        name="Paper trade entries",
        misfire_grace_time=300,
    )

    # Paper trading exit check at 15:35 IST (5 min before close)
    scheduler.add_job(
        paper_trading_exit_job,
        trigger="cron",
        hour=15, minute=35,
        id="paper_exit",
        name="Paper trade exit check",
        misfire_grace_time=300,
    )

    # EOD collection + report at 16:15 IST
    eh, em = EOD_REPORT_TIME.split(":")
    scheduler.add_job(
        eod_collection_job,
        trigger="cron",
        hour=int(eh), minute=int(em),
        id="eod_collection",
        name="EOD data collection",
        misfire_grace_time=300,
    )
    scheduler.add_job(
        eod_report_job,
        trigger="cron",
        hour=int(eh), minute=int(em) + 2,   # 2 min after collection
        id="eod_report",
        name="EOD report + alert",
        misfire_grace_time=300,
    )

    # BTST scan at 14:30 IST (Mon–Fri) — 45 min before close
    bh, bm = BTST_SCAN_TIME.split(":")
    scheduler.add_job(
        btst_scan_job,
        trigger="cron",
        day_of_week="mon-fri",
        hour=int(bh), minute=int(bm),
        id="btst_scan",
        name="[BTST] Overnight pick alert",
        misfire_grace_time=300,
    )

    # Pre-fetch news + filings at 14:40 so intraday monitor has fresh data
    scheduler.add_job(
        intraday_prefetch_job,
        trigger="cron",
        day_of_week="mon-fri",
        hour=14, minute=40,
        id="intraday_prefetch",
        name="Intraday pre-fetch (news + filings)",
        misfire_grace_time=120,
    )

    # Breakout monitor — every 5 min from 09:20 to 15:25 IST (full trading day)
    scheduler.add_job(
        breakout_monitor_job,
        trigger="cron",
        day_of_week="mon-fri",
        hour="9-15",
        minute="20,25,30,35,40,45,50,55,0,5,10,15",
        id="breakout_monitor",
        name="Full-day breakout/breakdown detector (all sectors)",
        misfire_grace_time=60,
    )

    # Intraday monitor — every 5 min from 14:45 to 15:25 IST
    scheduler.add_job(
        intraday_monitor_job,
        trigger="cron",
        day_of_week="mon-fri",
        hour="14-15",
        minute="45,50,55,0,5,10,15,20,25",
        id="intraday_monitor",
        name="Intraday late-session recovery detector",
        misfire_grace_time=60,
    )

    # Walk-forward optimizer on 1st of each month at 07:00 IST (before market open)
    scheduler.add_job(
        monthly_optimize_job,
        trigger="cron",
        day=1, hour=7, minute=0,
        id="monthly_optimize",
        name="Monthly walk-forward optimizer",
        misfire_grace_time=3600,
    )

    logger.info(
        f"Scheduler started — morning={MORNING_SCAN_TIME} "
        f"eod={EOD_REPORT_TIME} ({SCHEDULER_TIMEZONE})"
    )
    logger.info("Press Ctrl+C to stop.")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "once":
        run_once()
    elif cmd == "morning":
        morning_scan_job()
    elif cmd == "eod":
        eod_report_job()
    elif cmd == "paper_entry":
        paper_trading_entry_job()
    elif cmd == "paper_exit":
        paper_trading_exit_job()
    elif cmd == "optimize":
        monthly_optimize_job()
    elif cmd == "btst":
        btst_scan_job()
    elif cmd == "longterm":
        long_term_scan_job()
    elif cmd == "intraday":
        intraday_monitor_job()
    elif cmd == "breakout":
        breakout_monitor_job()
    else:
        start_scheduler()
