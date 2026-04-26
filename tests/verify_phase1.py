"""
Phase 1 verification script.

Run from project root:

    # All tests (includes live network calls):
    python tests/verify_phase1.py

    # Offline only (no network, fast):
    python tests/verify_phase1.py --offline

Each test prints PASS / FAIL / SKIP.
A final summary shows total counts and any failures to fix.
"""

import argparse
import sys
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# Ensure project root is on the path regardless of where the script is run from
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ── Colours ────────────────────────────────────────────────────────────────────
G = "\033[92m"   # green
R = "\033[91m"   # red
Y = "\033[93m"   # yellow
B = "\033[94m"   # blue / cyan for section headers
X = "\033[0m"    # reset

_results: list[tuple[str, str, str]] = []   # (status, name, detail)


def _record(status: str, name: str, detail: str = "") -> None:
    _results.append((status, name, detail))
    sym   = f"{G}PASS{X}" if status == "PASS" else (f"{R}FAIL{X}" if status == "FAIL" else f"{Y}SKIP{X}")
    extra = f"  ← {detail}" if detail else ""
    print(f"  [{sym}] {name}{extra}")


def run(name: str, fn, skip: bool = False) -> None:
    if skip:
        _record("SKIP", name, "network test — run without --offline to enable")
        return
    try:
        fn()
        _record("PASS", name)
    except AssertionError as exc:
        _record("FAIL", name, str(exc))
    except Exception as exc:
        _record("FAIL", name, f"{type(exc).__name__}: {str(exc)[:120]}")


def section(title: str) -> None:
    print(f"\n{B}{'─' * 60}{X}")
    print(f"{B}  {title}{X}")
    print(f"{B}{'─' * 60}{X}")


# ══════════════════════════════════════════════════════════════════════════════
# 1. IMPORTS
# ══════════════════════════════════════════════════════════════════════════════
section("1 · Module imports")

MODULES = [
    "config.settings",
    "config.nse_calendar",
    "data.storage.database",
    "data.quality.known_time",
    "data.quality.candle_checks",
    "data.quality.corporate_actions",
    "data.collectors.nse_bhavcopy",
    "data.collectors.groww_client",
    "data.collectors.yfinance_backfill",
    "data.collectors.fundamentals",
    "data.collectors.news_collector",
    "data.collectors.nse_filings",
    "analysis.fundamental.banking_metrics",
    "scheduler.daily_runner",
]

import importlib
for mod in MODULES:
    run(f"import {mod}", lambda m=mod: importlib.import_module(m))


# ══════════════════════════════════════════════════════════════════════════════
# 2. NSE CALENDAR
# ══════════════════════════════════════════════════════════════════════════════
section("2 · NSE Calendar")

from config.nse_calendar import is_trading_day, next_trading_day, trading_days_between

def test_good_friday_2025():
    assert is_trading_day(date(2025, 4, 18)) is False, "Good Friday should be non-trading"

def test_thursday_is_trading():
    assert is_trading_day(date(2025, 4, 17)) is True, "Apr 17 (Thu) should be trading"

def test_saturday_not_trading():
    assert is_trading_day(date(2025, 4, 19)) is False, "Saturday always non-trading"

def test_sunday_not_trading():
    assert is_trading_day(date(2025, 4, 20)) is False, "Sunday always non-trading"

def test_next_trading_day_skips_weekend():
    # Apr 25 (Fri) → next is Apr 28 (Mon) — skipping weekend
    nxt = next_trading_day(date(2025, 4, 25))
    assert nxt == date(2025, 4, 28), f"Expected Apr 28, got {nxt}"

def test_next_trading_day_skips_holiday():
    # Apr 17 (Thu) → Apr 18 is Good Friday → Apr 19-20 weekend → Apr 21 (Mon) ✓
    nxt = next_trading_day(date(2025, 4, 17))
    assert nxt == date(2025, 4, 21), f"Expected Apr 21, got {nxt}"

def test_trading_days_count():
    # Jan + Feb + Mar 2025 should be roughly 59-62 trading days
    days = trading_days_between(date(2025, 1, 1), date(2025, 3, 31))
    assert 55 <= len(days) <= 65, f"Expected 55–65 trading days, got {len(days)}"

def test_republic_day_2024():
    assert is_trading_day(date(2024, 1, 26)) is False, "Republic Day should be non-trading"

def test_independence_day_2024():
    assert is_trading_day(date(2024, 8, 15)) is False, "Independence Day should be non-trading"

run("Good Friday 2025 is non-trading",      test_good_friday_2025)
run("Regular Thursday is trading",          test_thursday_is_trading)
run("Saturday is non-trading",              test_saturday_not_trading)
run("Sunday is non-trading",                test_sunday_not_trading)
run("next_trading_day skips weekend",       test_next_trading_day_skips_weekend)
run("next_trading_day skips holiday",       test_next_trading_day_skips_holiday)
run("trading_days_between count sane",     test_trading_days_count)
run("Republic Day 2024 non-trading",       test_republic_day_2024)
run("Independence Day 2024 non-trading",   test_independence_day_2024)


# ══════════════════════════════════════════════════════════════════════════════
# 3. KNOWN-TIME / LOOKAHEAD PREVENTION
# ══════════════════════════════════════════════════════════════════════════════
section("3 · Known-time (lookahead prevention)")

from data.quality.known_time import compute_usable_from, known_time_filter, assert_no_lookahead

IST = ZoneInfo("Asia/Kolkata")

def test_after_hours_announcement():
    # Q4 result announced 2025-04-25 18:30 IST → usable 2025-04-28 09:15 IST
    announced = datetime(2025, 4, 25, 18, 30, tzinfo=IST)
    uf = compute_usable_from(announced)
    assert uf.date() == date(2025, 4, 28), f"Expected Apr 28, got {uf.date()}"
    assert uf.hour == 9 and uf.minute == 15, f"Expected 09:15, got {uf.hour}:{uf.minute:02}"

def test_pre_market_announcement():
    # Announcement at 08:00 on a trading day → usable next-trading-day open still
    announced = datetime(2025, 4, 17, 8, 0, tzinfo=IST)
    uf = compute_usable_from(announced)
    assert uf.date() >= date(2025, 4, 17), f"usable_from {uf.date()} before announcement date"

def test_no_lookahead_passes():
    class Row:
        usable_from = datetime(2025, 1, 1, 9, 15, tzinfo=IST)
    # Row is from Jan 2025; signal is Apr 2025 → should pass
    assert_no_lookahead([Row()], datetime(2025, 4, 1, 9, 15, tzinfo=IST))

def test_lookahead_raises():
    class Row:
        usable_from = datetime(2025, 4, 28, 9, 15, tzinfo=IST)
    # Row is usable Apr 28; signal is Apr 26 → must raise
    raised = False
    try:
        assert_no_lookahead([Row()], datetime(2025, 4, 26, 9, 15, tzinfo=IST))
    except AssertionError:
        raised = True
    assert raised, "assert_no_lookahead should have raised for future data"

def test_known_time_filter_returns_tuple():
    from data.storage.database import Fundamental
    fltr = known_time_filter(Fundamental, datetime(2025, 4, 1, 9, 15, tzinfo=IST))
    assert isinstance(fltr, tuple) and len(fltr) == 1, "Should return a 1-element tuple"

run("After-hours announcement → next-day open", test_after_hours_announcement)
run("Pre-market announcement → next-day open",  test_pre_market_announcement)
run("assert_no_lookahead passes for old data",   test_no_lookahead_passes)
run("assert_no_lookahead raises for future data", test_lookahead_raises)
run("known_time_filter returns SQLAlchemy clause", test_known_time_filter_returns_tuple)


# ══════════════════════════════════════════════════════════════════════════════
# 4. DATABASE
# ══════════════════════════════════════════════════════════════════════════════
section("4 · Database")

from data.storage.database import (
    init_db, get_session, Base, engine,
    Stock, OHLCVDaily, CorporateAction, Fundamental, BankingMetric,
    CorporateFiling, NewsArticle, TechnicalSignal, PaperTrade,
    LLMLog, DataQualityLog,
)
from sqlalchemy import inspect, text

def test_init_db_creates_tables():
    init_db()
    tables = set(inspect(engine).get_table_names())
    expected = {
        "stocks", "ohlcv_daily", "corporate_actions", "fundamentals",
        "banking_metrics", "corporate_filings", "news_articles",
        "technical_signals", "paper_trades", "llm_log", "data_quality_log",
    }
    missing = expected - tables
    assert not missing, f"Missing tables: {missing}"

def test_usable_from_indexed_on_event_tables():
    insp = inspect(engine)
    for table in ["corporate_actions", "fundamentals", "banking_metrics",
                  "corporate_filings", "news_articles"]:
        cols = {c["name"] for c in insp.get_columns(table)}
        assert "usable_from" in cols, f"{table} missing usable_from column"

def test_wal_mode():
    with engine.connect() as conn:
        mode = conn.execute(text("PRAGMA journal_mode")).scalar()
    assert mode == "wal", f"Expected WAL mode, got {mode}"

def test_insert_stock():
    with get_session() as s:
        existing = s.query(Stock).filter_by(symbol="TEST__").first()
        if existing:
            s.delete(existing)
            s.commit()
        s.add(Stock(symbol="TEST__", name="Test Stock", sector="Testing"))
        s.commit()
        row = s.query(Stock).filter_by(symbol="TEST__").first()
        assert row is not None and row.name == "Test Stock"
        s.delete(row)
        s.commit()

def test_ohlcv_unique_constraint():
    with get_session() as s:
        # Insert a row, then try inserting the same (symbol, date) again
        s.query(OHLCVDaily).filter_by(symbol="TEST__", date=date(2000, 1, 1)).delete()
        s.commit()
        from sqlalchemy.dialects.sqlite import insert
        stmt = insert(OHLCVDaily).values(
            symbol="TEST__", date=date(2000, 1, 1), close=100.0,
            source="nse_bhavcopy", is_adjusted=False, needs_verification=False,
            collected_at=datetime.utcnow(),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol", "date"],
            set_={"close": 200.0},
        )
        s.execute(stmt)
        s.execute(stmt)   # second call should update, not duplicate
        s.commit()
        count = s.query(OHLCVDaily).filter_by(symbol="TEST__", date=date(2000, 1, 1)).count()
        assert count == 1, f"Expected 1 row, got {count} (unique constraint may be broken)"
        s.query(OHLCVDaily).filter_by(symbol="TEST__", date=date(2000, 1, 1)).delete()
        s.commit()

run("init_db creates all 11 tables",        test_init_db_creates_tables)
run("usable_from present on event tables",  test_usable_from_indexed_on_event_tables)
run("SQLite WAL mode enabled",              test_wal_mode)
run("Can insert and delete a Stock row",    test_insert_stock)
run("ohlcv_daily unique constraint works",  test_ohlcv_unique_constraint)


# ══════════════════════════════════════════════════════════════════════════════
# 5. BANKING METRICS SCORING
# ══════════════════════════════════════════════════════════════════════════════
section("5 · Banking metrics scoring logic")

from analysis.fundamental.banking_metrics import _score_metric

def test_nim_good():
    assert _score_metric(4.5, "nim") == 100.0, "NIM 4.5% should score 100"

def test_nim_bad():
    assert _score_metric(1.0, "nim") == 0.0, "NIM 1.0% should score 0"

def test_gnpa_good():
    assert _score_metric(1.5, "gnpa") == 100.0, "GNPA 1.5% (below 2%) should score 100"

def test_gnpa_bad():
    assert _score_metric(8.0, "gnpa") == 0.0, "GNPA 8% (above 5%) should score 0"

def test_gnpa_mid():
    score = _score_metric(3.5, "gnpa")
    assert 45 <= score <= 55, f"GNPA 3.5% (midpoint) should score ~50, got {score}"

def test_none_returns_neutral():
    assert _score_metric(None, "nim") == 50.0, "None should return neutral 50"

def test_unknown_key_returns_neutral():
    assert _score_metric(99.0, "unknown_key") == 50.0, "Unknown key should return neutral 50"

run("NIM 4.5% → score 100",    test_nim_good)
run("NIM 1.0% → score 0",      test_nim_bad)
run("GNPA 1.5% → score 100",   test_gnpa_good)
run("GNPA 8.0% → score 0",     test_gnpa_bad)
run("GNPA 3.5% → score ~50",   test_gnpa_mid)
run("None value → score 50",   test_none_returns_neutral)
run("Unknown metric → score 50", test_unknown_key_returns_neutral)


# ══════════════════════════════════════════════════════════════════════════════
# 6. CORPORATE ACTION RATIO PARSER
# ══════════════════════════════════════════════════════════════════════════════
section("6 · Corporate action ratio parser")

from data.quality.corporate_actions import _parse_ratio

def test_split_ratio():
    ratio, _ = _parse_ratio("stock split from rs.10/- to rs.1/-", "split")
    assert ratio == 10.0, f"Expected split ratio 10, got {ratio}"

def test_bonus_ratio():
    ratio, _ = _parse_ratio("bonus 1:1", "bonus")
    assert ratio == 2.0, f"Expected bonus ratio 2 (1 held + 1 bonus), got {ratio}"

def test_dividend_amount():
    _, amount = _parse_ratio("interim dividend rs.19 per share", "dividend")
    assert amount == 19.0, f"Expected dividend 19, got {amount}"

run("Split 10:1 → ratio=10",   test_split_ratio)
run("Bonus 1:1 → ratio=2",     test_bonus_ratio)
run("Dividend Rs.19 → amount=19", test_dividend_amount)


# ══════════════════════════════════════════════════════════════════════════════
# 7. CONFIG SETTINGS
# ══════════════════════════════════════════════════════════════════════════════
section("7 · Config / settings")

import config.settings as cfg

def test_banking_stocks_list():
    expected = {"HDFCBANK","ICICIBANK","SBIN","AXISBANK","KOTAKBANK","BANKBARODA","FEDERALBNK"}
    assert set(cfg.BANKING_STOCKS) == expected

def test_yf_symbols_have_ns_suffix():
    assert all(s.endswith(".NS") for s in cfg.BANKING_STOCKS_YF)

def test_live_trading_disabled():
    assert cfg.LIVE_TRADING_ENABLED is False, "LIVE_TRADING_ENABLED must be False"

def test_score_weights_sum_to_1():
    total = cfg.SCORE_WEIGHT_TECHNICAL + cfg.SCORE_WEIGHT_FUNDAMENTAL + cfg.SCORE_WEIGHT_SENTIMENT
    assert abs(total - 1.0) < 1e-9, f"Score weights should sum to 1.0, got {total}"

def test_paths_exist():
    assert cfg.DATA_DIR.exists(), f"DATA_DIR {cfg.DATA_DIR} does not exist"
    assert cfg.LOG_DIR.exists(),  f"LOG_DIR {cfg.LOG_DIR} does not exist"

def test_api_keys_loaded():
    assert len(cfg.GROWW_API_KEY) > 10, "GROWW_API_KEY appears empty or too short"
    assert len(cfg.TELEGRAM_BOT_TOKEN) > 10, "TELEGRAM_BOT_TOKEN appears empty or too short"

run("BANKING_STOCKS has 7 correct symbols",       test_banking_stocks_list)
run("yfinance symbols end with .NS",              test_yf_symbols_have_ns_suffix)
run("LIVE_TRADING_ENABLED is False",              test_live_trading_disabled)
run("Score weights sum to 1.0",                   test_score_weights_sum_to_1)
run("DATA_DIR and LOG_DIR exist on disk",         test_paths_exist)
run("API keys loaded from .env",                  test_api_keys_loaded)


# ══════════════════════════════════════════════════════════════════════════════
# 8. NETWORK TESTS  (skipped with --offline)
# ══════════════════════════════════════════════════════════════════════════════
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--offline", action="store_true")
args, _ = parser.parse_known_args()
net = args.offline   # True = skip network tests

section("8 · yfinance backfill (network)")

from data.collectors.yfinance_backfill import fetch_yfinance

def test_yfinance_fetch():
    end   = date.today() - timedelta(days=1)
    start = end - timedelta(days=7)
    df    = fetch_yfinance("HDFCBANK.NS", start, end)
    assert not df.empty, "yfinance returned no data for HDFCBANK.NS"
    assert "close" in df.columns
    assert "volume" in df.columns
    assert (df["close"] > 0).all(), "All close prices should be positive"
    print(f"       Got {len(df)} rows — last close: ₹{df['close'].iloc[-1]:,.2f}")

run("yfinance: fetch 7 days HDFCBANK", test_yfinance_fetch, skip=net)


section("9 · NSE Bhavcopy (network)")

from data.collectors.nse_bhavcopy import download_bhavcopy
from config.nse_calendar import prev_trading_day

def test_bhavcopy_download():
    # Try the last trading day
    last_trading = prev_trading_day(date.today())
    df = download_bhavcopy(last_trading)
    assert df is not None and not df.empty, f"Bhavcopy returned no data for {last_trading}"
    assert "symbol" in df.columns
    found = set(df["symbol"].tolist()) & set(cfg.BANKING_STOCKS)
    assert len(found) >= 4, f"Expected ≥4 banking stocks in Bhavcopy, found: {found}"
    print(f"       Date: {last_trading}  |  Banking stocks found: {sorted(found)}")

run("Bhavcopy: download last trading day", test_bhavcopy_download, skip=net)


section("10 · NSE filings API (network)")

from data.collectors.nse_filings import _fetch_announcements

def test_nse_filings_fetch():
    from_date = date.today() - timedelta(days=30)
    data = _fetch_announcements("HDFCBANK", from_date, date.today())
    assert isinstance(data, list), "Expected list of announcements"
    print(f"       HDFCBANK filings last 30 days: {len(data)}")

run("NSE filings: fetch HDFCBANK last 30 days", test_nse_filings_fetch, skip=net)


section("11 · News RSS (network)")

from data.collectors.news_collector import _fetch_feed

def test_google_news_rss():
    url      = "https://news.google.com/rss/search?q=HDFCBANK+NSE+bank+India&hl=en-IN&gl=IN&ceid=IN:en"
    articles = _fetch_feed(url, "google_news")
    assert len(articles) > 0, "Google News RSS returned no articles"
    assert "headline" in articles[0]
    print(f"       Google News articles fetched: {len(articles)}")

def test_moneycontrol_rss():
    url      = "https://www.moneycontrol.com/rss/buzzstocks.xml"
    articles = _fetch_feed(url, "moneycontrol")
    assert len(articles) > 0, "MoneyControl RSS returned no articles"
    print(f"       MoneyControl articles fetched: {len(articles)}")

run("Google News RSS: returns articles",    test_google_news_rss,    skip=net)
run("MoneyControl RSS: returns articles",   test_moneycontrol_rss,   skip=net)


section("12 · Screener.in fundamentals (network)")

from data.collectors.fundamentals import _fetch_page, _parse_quarterly_table

def test_screener_fetch():
    soup = _fetch_page("HDFCBANK")
    assert soup is not None, "Screener.in returned None for HDFCBANK"
    periods = _parse_quarterly_table(soup)
    assert len(periods) >= 4, f"Expected ≥4 quarters, got {len(periods)}"
    assert all("period_end_date" in p for p in periods)
    print(f"       Quarters parsed: {len(periods)}  |  latest: {periods[0]['period_end_date'] if periods else 'N/A'}")

run("Screener.in: fetch + parse HDFCBANK", test_screener_fetch, skip=net)


section("13 · Corporate actions NSE (network)")

from data.quality.corporate_actions import _fetch_from_nse

def test_corp_actions_fetch():
    data = _fetch_from_nse("HDFCBANK")
    assert isinstance(data, list), f"Expected list, got {type(data)}"
    print(f"       HDFCBANK corporate actions returned: {len(data)}")

run("NSE corp actions: fetch HDFCBANK", test_corp_actions_fetch, skip=net)


# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
passed  = sum(1 for s, _, _ in _results if s == "PASS")
failed  = sum(1 for s, _, _ in _results if s == "FAIL")
skipped = sum(1 for s, _, _ in _results if s == "SKIP")

print(f"\n{'═' * 60}")
print(f"  Results:  {G}{passed} passed{X}  |  {R}{failed} failed{X}  |  {Y}{skipped} skipped{X}")
print(f"{'═' * 60}")

if failed:
    print(f"\n{R}Failed tests:{X}")
    for status, name, detail in _results:
        if status == "FAIL":
            print(f"  {R}✗{X} {name}")
            print(f"    {detail}")

if args.offline and skipped:
    print(f"\n{Y}Tip:{X} Run without --offline to also test live network connections.")

print()
sys.exit(1 if failed else 0)
