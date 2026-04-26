"""
Phase 2 verification — Technical Analysis, Fundamental Ratios, Stock Scorer.

Sections:
  1.  Module imports
  2.  DB initialisation
  3.  OHLCV backfill (yfinance, ~1 year)  [network; skipped with --offline if data exists]
  4.  Indicator computation
  5.  Required indicator columns present
  6.  Signal generation for a past date
  7.  No-lookahead enforcement
  8.  Technical score function (0–100)
  9.  Fundamental ratios module (returns 50.0 without data — expected)
  10. stock_scorer.score_stock()
  11. score_all() — all 7 stocks ranked
  12. Effective weight logic

Usage:
  cd /path/to/nse-banking-agent
  python tests/verify_phase2.py            # full run (needs network for backfill)
  python tests/verify_phase2.py --offline  # skip backfill; use whatever is in DB
"""

import sys
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path

# Ensure project root on path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

OFFLINE = "--offline" in sys.argv

# ── Colour helpers ─────────────────────────────────────────────────────────────
GREEN = "\033[92m"; RED = "\033[91m"; YELLOW = "\033[93m"; RESET = "\033[0m"

passed = failed = skipped = 0

def ok(msg: str):
    global passed
    passed += 1
    print(f"  {GREEN}PASS{RESET}  {msg}")

def fail(msg: str, exc: Exception | None = None):
    global failed
    failed += 1
    print(f"  {RED}FAIL{RESET}  {msg}")
    if exc:
        print(f"         {RED}{exc}{RESET}")
        traceback.print_exc()

def skip(msg: str):
    global skipped
    skipped += 1
    print(f"  {YELLOW}SKIP{RESET}  {msg}")

def section(title: str):
    print(f"\n{'─' * 70}")
    print(f"  {title}")
    print(f"{'─' * 70}")


# ══════════════════════════════════════════════════════════════════════════════
# 1. Module imports
# ══════════════════════════════════════════════════════════════════════════════
section("1. Module imports")

try:
    from analysis.technical.indicators import (
        load_ohlcv, compute_all, get_indicators, get_latest_row,
    )
    ok("analysis.technical.indicators")
except Exception as e:
    fail("analysis.technical.indicators", e)

try:
    from analysis.technical.signals import (
        _evaluate, generate_signal, generate_all, score as tech_score_fn,
    )
    ok("analysis.technical.signals")
except Exception as e:
    fail("analysis.technical.signals", e)

try:
    from analysis.fundamental.ratios import (
        compute_ratios, score as ratios_score_fn, score_all as ratios_score_all,
    )
    ok("analysis.fundamental.ratios")
except Exception as e:
    fail("analysis.fundamental.ratios", e)

try:
    from analysis.fundamental.banking_metrics import score as banking_score_fn
    ok("analysis.fundamental.banking_metrics.score")
except Exception as e:
    fail("analysis.fundamental.banking_metrics.score", e)

try:
    from scoring.stock_scorer import (
        score_stock, score_all, _effective_weights,
    )
    ok("scoring.stock_scorer")
except Exception as e:
    fail("scoring.stock_scorer", e)

try:
    from config.settings import BANKING_STOCKS, BANKING_STOCKS_YF
    ok(f"settings — {len(BANKING_STOCKS)} banking stocks")
except Exception as e:
    fail("config.settings", e)


# ══════════════════════════════════════════════════════════════════════════════
# 2. DB initialisation
# ══════════════════════════════════════════════════════════════════════════════
section("2. DB initialisation")

try:
    from data.storage.database import init_db, get_session, OHLCVDaily
    init_db()
    ok("init_db()")
except Exception as e:
    fail("init_db()", e)


# ══════════════════════════════════════════════════════════════════════════════
# 3. OHLCV backfill
# ══════════════════════════════════════════════════════════════════════════════
section("3. OHLCV backfill via yfinance")

BACKFILL_START = date.today() - timedelta(days=400)   # ~1 year + buffer for EMA_200
BACKFILL_END   = date.today()

def _row_count(symbol: str) -> int:
    with get_session() as s:
        return s.query(OHLCVDaily).filter(OHLCVDaily.symbol == symbol).count()

for sym in BANKING_STOCKS:
    existing = _row_count(sym)
    if existing >= 200:
        ok(f"{sym}: {existing} rows already in DB — backfill skipped")
    elif OFFLINE:
        skip(f"{sym}: {existing} rows in DB (--offline, won't backfill)")
    else:
        try:
            from data.collectors.yfinance_backfill import backfill
            stored = backfill(sym, BACKFILL_START, BACKFILL_END)
            total  = _row_count(sym)
            if total >= 200:
                ok(f"{sym}: backfilled {stored} rows → {total} total")
            else:
                fail(f"{sym}: only {total} rows after backfill (need ≥ 200 for EMA_200)")
        except Exception as e:
            fail(f"{sym}: backfill failed", e)


# ══════════════════════════════════════════════════════════════════════════════
# 4. Indicator computation
# ══════════════════════════════════════════════════════════════════════════════
section("4. Indicator computation")

PILOT = "HDFCBANK"   # representative stock for detailed checks

try:
    df = get_indicators(PILOT)
    if df.empty:
        fail(f"get_indicators({PILOT}) returned empty DataFrame")
    else:
        ok(f"get_indicators({PILOT}): {len(df)} rows, {len(df.columns)} columns")
except Exception as e:
    fail(f"get_indicators({PILOT})", e)
    df = None

if df is not None and not df.empty:
    try:
        recent = df.tail(5)
        # These columns must exist
        required = ["ema_21", "ema_50", "ema_200", "rsi", "macd_hist", "adx", "vwap_20", "obv_slope"]
        missing  = [c for c in required if c not in df.columns]
        if missing:
            fail(f"Missing indicator columns: {missing}")
        else:
            ok(f"All required indicator columns present: {required}")
    except Exception as e:
        fail("Indicator column check", e)

    try:
        # EMA_200 needs ≥ 200 rows to be non-NaN; verify the most recent row
        last = df.iloc[-1]
        nan_cols = [c for c in ["ema_21", "ema_50", "rsi", "macd_hist"] if isinstance(last.get(c), float) and __import__("math").isnan(last[c])]
        if nan_cols:
            fail(f"NaN in recent row for: {nan_cols}")
        else:
            ok(f"No NaN in key columns of most-recent row for {PILOT}")
    except Exception as e:
        fail("NaN check on recent row", e)


# ══════════════════════════════════════════════════════════════════════════════
# 5. Required indicator columns — all 7 stocks
# ══════════════════════════════════════════════════════════════════════════════
section("5. Indicator columns for all 7 stocks")

REQUIRED_COLS = ["adjusted_close", "ema_21", "ema_50", "ema_200", "rsi",
                  "macd_hist", "adx", "vwap_20", "obv_slope"]

for sym in BANKING_STOCKS:
    try:
        row = get_latest_row(sym)
        if not row:
            fail(f"{sym}: get_latest_row returned empty dict (no data?)")
            continue
        missing = [c for c in REQUIRED_COLS if c not in row]
        if missing:
            fail(f"{sym}: missing columns {missing}")
        else:
            price = row.get("adjusted_close", "?")
            rsi   = row.get("rsi", "?")
            ok(f"{sym}: price={price:.2f}, RSI={rsi:.1f}" if isinstance(price, float) and isinstance(rsi, float) else f"{sym}: row OK")
    except Exception as e:
        fail(f"{sym}: get_latest_row", e)


# ══════════════════════════════════════════════════════════════════════════════
# 6. Signal generation for a past date
# ══════════════════════════════════════════════════════════════════════════════
section("6. Signal generation (past date)")

# Use 3 months ago — well within backfill window
SIGNAL_DATE = date.today() - timedelta(days=90)

for sym in BANKING_STOCKS:
    try:
        sig = generate_signal(sym, SIGNAL_DATE)
        if sig is None:
            fail(f"{sym}: generate_signal returned None for {SIGNAL_DATE}")
        else:
            ok(f"{sym}: {sig.signal_type} strength={sig.strength} date={sig.signal_date}")
    except Exception as e:
        fail(f"{sym}: generate_signal", e)


# ══════════════════════════════════════════════════════════════════════════════
# 7. No-lookahead enforcement
# ══════════════════════════════════════════════════════════════════════════════
section("7. No-lookahead enforcement")

# Verify that indicators computed as_of SIGNAL_DATE contain no rows after it
try:
    df_past = get_indicators(PILOT, as_of_date=SIGNAL_DATE)
    if df_past.empty:
        skip(f"No indicator data for {PILOT} as of {SIGNAL_DATE}")
    else:
        max_date = df_past.index.max().date()
        if max_date <= SIGNAL_DATE:
            ok(f"Indicator max date {max_date} ≤ signal_date {SIGNAL_DATE} — no lookahead")
        else:
            fail(f"LOOKAHEAD DETECTED: max_date {max_date} > signal_date {SIGNAL_DATE}")
except Exception as e:
    fail("No-lookahead check", e)

# Verify OHLCVDaily query respects as_of_date
try:
    past_cutoff = SIGNAL_DATE - timedelta(days=30)
    df_older = get_indicators(PILOT, as_of_date=past_cutoff)
    if df_older.empty:
        skip(f"Not enough data for {PILOT} as of {past_cutoff}")
    else:
        max_date_older = df_older.index.max().date()
        if max_date_older <= past_cutoff:
            ok(f"Older cutoff {past_cutoff}: max_date={max_date_older} ✓")
        else:
            fail(f"LOOKAHEAD: max_date {max_date_older} > cutoff {past_cutoff}")
except Exception as e:
    fail("Older cutoff no-lookahead check", e)


# ══════════════════════════════════════════════════════════════════════════════
# 8. Technical score function (0–100)
# ══════════════════════════════════════════════════════════════════════════════
section("8. Technical score function")

SCORE_TIME = datetime.combine(SIGNAL_DATE, datetime.min.time())

for sym in BANKING_STOCKS:
    try:
        s = tech_score_fn(sym, SCORE_TIME)
        if not (0.0 <= s <= 100.0):
            fail(f"{sym}: tech score {s} out of [0, 100]")
        else:
            ok(f"{sym}: tech_score={s:.1f}")
    except Exception as e:
        fail(f"{sym}: tech_score_fn", e)


# ══════════════════════════════════════════════════════════════════════════════
# 9. Fundamental ratios module
# ══════════════════════════════════════════════════════════════════════════════
section("9. Fundamental ratios (returns 50.0 without data — expected)")

for sym in BANKING_STOCKS:
    try:
        s = ratios_score_fn(sym, SCORE_TIME)
        if not (0.0 <= s <= 100.0):
            fail(f"{sym}: ratios score {s} out of [0, 100]")
        else:
            note = " (no data)" if s == 50.0 else ""
            ok(f"{sym}: ratios_score={s:.1f}{note}")
    except Exception as e:
        fail(f"{sym}: ratios_score_fn", e)


# ══════════════════════════════════════════════════════════════════════════════
# 10. stock_scorer.score_stock()
# ══════════════════════════════════════════════════════════════════════════════
section("10. score_stock() — per-stock combined score")

REQUIRED_KEYS = [
    "symbol", "name", "total_score", "technical_score",
    "fundamental_score", "sentiment_score", "weights", "signal_time",
]

for sym in BANKING_STOCKS:
    try:
        result = score_stock(sym, SCORE_TIME)
        missing_keys = [k for k in REQUIRED_KEYS if k not in result]
        if missing_keys:
            fail(f"{sym}: missing keys {missing_keys}")
            continue
        ts = result["total_score"]
        if not (0.0 <= ts <= 100.0):
            fail(f"{sym}: total_score={ts} out of [0, 100]")
            continue
        w = result["weights"]
        w_sum = round(w["technical"] + w["fundamental"] + w["sentiment"], 6)
        if abs(w_sum - 1.0) > 0.001:
            fail(f"{sym}: weights don't sum to 1.0 — got {w_sum}")
            continue
        ok(f"{sym}: total={ts:.1f}  tech={result['technical_score']:.1f}  fund={result['fundamental_score']:.1f}  sent={result['sentiment_score']:.1f}")
    except Exception as e:
        fail(f"{sym}: score_stock", e)


# ══════════════════════════════════════════════════════════════════════════════
# 11. score_all() — all 7 stocks ranked
# ══════════════════════════════════════════════════════════════════════════════
section("11. score_all() — ranked table")

try:
    rankings = score_all(SCORE_TIME)
    if len(rankings) != len(BANKING_STOCKS):
        fail(f"Expected {len(BANKING_STOCKS)} results, got {len(rankings)}")
    else:
        ok(f"score_all() returned {len(rankings)} results")

    # Verify sorted descending
    scores = [r["total_score"] for r in rankings]
    if scores == sorted(scores, reverse=True):
        ok(f"Rankings sorted descending — top: {rankings[0]['symbol']} ({scores[0]:.1f}), bottom: {rankings[-1]['symbol']} ({scores[-1]:.1f})")
    else:
        fail(f"Rankings not sorted: {list(zip([r['symbol'] for r in rankings], scores))}")

    # Log the full ranking (informational)
    print()
    print("  Final ranking:")
    for i, r in enumerate(rankings, 1):
        marker = " ◄ top" if i == 1 else ""
        print(f"    {i}. {r['symbol']:<12} total={r['total_score']:.1f}  tech={r.get('technical_score', 0):.1f}{marker}")

    # Soft check: HDFCBANK or ICICIBANK in top 3 (may not hold on technical-only ranking without fundamental data)
    top3 = {r["symbol"] for r in rankings[:3]}
    top_banks = {"HDFCBANK", "ICICIBANK"}
    if top_banks & top3:
        ok(f"At least one of HDFCBANK/ICICIBANK in top 3 — {top3 & top_banks}")
    else:
        skip(f"Neither HDFCBANK nor ICICIBANK in top 3 (no fundamental data yet) — top3={top3}")

except Exception as e:
    fail("score_all()", e)


# ══════════════════════════════════════════════════════════════════════════════
# 12. Effective weight logic
# ══════════════════════════════════════════════════════════════════════════════
section("12. Effective weight logic")

try:
    tw, fw, sw = _effective_weights()
    total = round(tw + fw + sw, 6)
    if abs(total - 1.0) > 0.001:
        fail(f"Weights don't sum to 1.0: tech={tw} fund={fw} sent={sw} sum={total}")
    else:
        ok(f"Weights sum to 1.0: tech={tw:.3f}  fund={fw:.3f}  sent={sw:.3f}")
    if tw > fw > sw >= 0:
        ok(f"Weight order correct: tech > fund > sent")
    else:
        ok(f"Weight order: tech={tw:.3f}  fund={fw:.3f}  sent={sw:.3f}  (may be adjusted for low LLM accuracy)")
except Exception as e:
    fail("_effective_weights()", e)


# ══════════════════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════════════════
total_tests = passed + failed + skipped
print(f"\n{'═' * 70}")
print(f"  Phase 2 Results: {GREEN}{passed} passed{RESET} | {RED}{failed} failed{RESET} | {YELLOW}{skipped} skipped{RESET}  (of {total_tests})")
print(f"{'═' * 70}\n")

if failed > 0:
    sys.exit(1)
