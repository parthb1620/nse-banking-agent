"""
APScheduler orchestrator — daily data collection + alert pipeline.

Jobs:
  08:30 IST — morning_scan: news sentiment + top picks Telegram alert
  16:15 IST — eod_collection: Bhavcopy + quality + fundamentals/news
  16:15 IST — eod_report: updated scores + signal Telegram alert

Run from project root:
  python -m scheduler.daily_runner          # start scheduler (blocking)
  python -m scheduler.daily_runner once     # run EOD collection once now
  python -m scheduler.daily_runner morning  # run morning scan once now
  python -m scheduler.daily_runner eod      # run EOD report once now
"""

import sys
from datetime import date

from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger

from config.nse_calendar import is_trading_day
from config.settings import EOD_REPORT_TIME, MORNING_SCAN_TIME, LOG_LEVEL, LOG_DIR, SCHEDULER_TIMEZONE

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

    # Step 2: Secondary — Groww API (fill gaps)
    try:
        from data.collectors.groww_client import fetch_historical, store_historical
        from config.settings import BANKING_STOCKS
        for symbol in BANKING_STOCKS:
            rows = fetch_historical(symbol, today, today)
            if rows:
                store_historical(symbol, rows)
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


def eod_report_job() -> None:
    """16:15 IST — updated scores + signals Telegram alert."""
    from scheduler.jobs.eod_report import run
    run()


def run_once() -> None:
    """Run the EOD collection job immediately."""
    eod_collection_job()


def start_scheduler() -> None:
    """Start the blocking scheduler. Runs until Ctrl+C."""
    from data.storage.database import init_db
    init_db()

    scheduler = BlockingScheduler(timezone=SCHEDULER_TIMEZONE)

    # Morning scan at 08:30 IST
    mh, mm = MORNING_SCAN_TIME.split(":")
    scheduler.add_job(
        morning_scan_job,
        trigger="cron",
        hour=int(mh), minute=int(mm),
        id="morning_scan",
        name="Morning scan + alert",
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
    else:
        start_scheduler()
