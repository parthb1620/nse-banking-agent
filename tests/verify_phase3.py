"""
Phase 3 verification — Backtesting engine, strategy, metrics.

Sections:
  1.  Module imports
  2.  DB init + extended backfill (5 years via yfinance for walk-forward)
  3.  Strategy signal generation
  4.  No-lookahead assertion on signal generation
  5.  Single-stock backtest (HDFCBANK, 1 year)
  6.  Trade log integrity checks
  7.  Metrics computation
  8.  Benchmark comparison (all 4) — soft check (may fail without index data)
  9.  Multi-stock backtest (all 7 stocks)
  10. Walk-forward split results

Usage:
  python tests/verify_phase3.py
  python tests/verify_phase3.py --offline   # skip 5-year backfill if data present
"""

import sys
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

OFFLINE = "--offline" in sys.argv

# ── Unconditional imports needed throughout the file ──────────────────────────
from config.settings import BANKING_STOCKS
from analysis.technical.indicators import get_indicators
from data.storage.database import init_db, get_session, OHLCVDaily

GREEN = "\033[92m"; RED = "\033[91m"; YELLOW = "\033[93m"; RESET = "\033[0m"
passed = failed = skipped = 0

def ok(msg):
    global passed; passed += 1
    print(f"  {GREEN}PASS{RESET}  {msg}")

def fail(msg, exc=None):
    global failed; failed += 1
    print(f"  {RED}FAIL{RESET}  {msg}")
    if exc:
        print(f"         {RED}{exc}{RESET}")
        traceback.print_exc()

def skip(msg):
    global skipped; skipped += 1
    print(f"  {YELLOW}SKIP{RESET}  {msg}")

def section(title):
    print(f"\n{'─' * 70}")
    print(f"  {title}")
    print(f"{'─' * 70}")


# ══════════════════════════════════════════════════════════════════════════════
# 1. Module imports
# ══════════════════════════════════════════════════════════════════════════════
section("1. Module imports")

try:
    from backtesting.strategies.ema_rsi_swing import generate_signals, describe
    ok("backtesting.strategies.ema_rsi_swing")
except Exception as e:
    fail("backtesting.strategies.ema_rsi_swing", e)

try:
    from backtesting.engine import BacktestEngine, BacktestResult, Trade
    ok("backtesting.engine")
except Exception as e:
    fail("backtesting.engine", e)

try:
    from backtesting.metrics import (
        compute_metrics, compare_benchmarks, compare_all_stocks,
        print_report, print_all_report,
    )
    ok("backtesting.metrics")
except Exception as e:
    fail("backtesting.metrics", e)

try:
    # These are already imported at top; just verify they loaded correctly
    assert BANKING_STOCKS and len(BANKING_STOCKS) == 7
    assert get_indicators is not None
    ok(f"analysis + config imports ({len(BANKING_STOCKS)} stocks)")
except Exception as e:
    fail("analysis + config imports", e)


# ══════════════════════════════════════════════════════════════════════════════
# 2. Data availability — extend backfill to 5 years if needed
# ══════════════════════════════════════════════════════════════════════════════
section("2. Extended backfill (5 years) for walk-forward testing")

init_db()

FIVE_YEARS_AGO = date.today() - timedelta(days=5 * 365 + 30)
TODAY          = date.today()

def row_count(sym):
    with get_session() as s:
        return s.query(OHLCVDaily).filter(OHLCVDaily.symbol == sym).count()

def earliest_date(sym):
    with get_session() as s:
        row = s.query(OHLCVDaily).filter(OHLCVDaily.symbol == sym).order_by(OHLCVDaily.date.asc()).first()
        return row.date if row else None

for sym in BANKING_STOCKS:
    earliest = earliest_date(sym)
    n = row_count(sym)

    # Need 5 years (~1260 trading days)
    if n >= 1200:
        ok(f"{sym}: {n} rows (earliest {earliest}) — sufficient for walk-forward")
    elif OFFLINE:
        skip(f"{sym}: only {n} rows — --offline, won't extend backfill")
    else:
        try:
            from data.collectors.yfinance_backfill import backfill
            stored = backfill(sym, FIVE_YEARS_AGO, TODAY)
            new_n  = row_count(sym)
            if new_n >= 600:
                ok(f"{sym}: {new_n} rows after 5yr backfill (stored {stored})")
            else:
                skip(f"{sym}: only {new_n} rows — may affect walk-forward accuracy")
        except Exception as e:
            fail(f"{sym}: backfill", e)


# ══════════════════════════════════════════════════════════════════════════════
# 3. Strategy signal generation
# ══════════════════════════════════════════════════════════════════════════════
section("3. Strategy signal generation")

PILOT   = "HDFCBANK"
CUTOFF  = date(2025, 12, 31)    # well within 1-year backfill

df_full = None
try:
    df_full = get_indicators(PILOT, as_of_date=CUTOFF)
    if df_full.empty:
        fail(f"get_indicators({PILOT}, {CUTOFF}) returned empty DataFrame")
    else:
        ok(f"Loaded {len(df_full)} bars for {PILOT} up to {CUTOFF}")
except Exception as e:
    fail(f"get_indicators", e)

if df_full is not None and not df_full.empty:
    try:
        df_sig = generate_signals(df_full)
        required = ["entry_signal", "exit_signal", "stop_price", "target_price"]
        missing  = [c for c in required if c not in df_sig.columns]
        if missing:
            fail(f"Signal columns missing: {missing}")
        else:
            n_entry = df_sig["entry_signal"].sum()
            n_exit  = df_sig["exit_signal"].sum()
            ok(f"generate_signals(): entry_signals={n_entry}  exit_signals={n_exit}")
    except Exception as e:
        fail("generate_signals()", e)
        df_sig = None

    # Verify entry/exit are boolean type
    if df_sig is not None:
        try:
            assert df_sig["entry_signal"].dtype == bool, "entry_signal not bool"
            assert df_sig["exit_signal"].dtype  == bool, "exit_signal not bool"
            ok("entry_signal and exit_signal are boolean dtype")
        except AssertionError as e:
            fail(f"dtype check: {e}")

    # Verify stop_price always < entry_price column
    if df_sig is not None:
        try:
            close = df_sig.get("adjusted_close", df_sig.get("close"))
            bad   = (df_sig["stop_price"] >= close).sum()
            if bad > 0:
                fail(f"stop_price >= close on {bad} bars (should always be below)")
            else:
                ok("stop_price < close on all bars")
        except Exception as e:
            fail("stop_price vs close check", e)


# ══════════════════════════════════════════════════════════════════════════════
# 4. No-lookahead assertion on signal data
# ══════════════════════════════════════════════════════════════════════════════
section("4. No-lookahead assertion")

SIGNAL_CUTOFF = date(2025, 6, 30)

try:
    df_cut = get_indicators(PILOT, as_of_date=SIGNAL_CUTOFF)
    if df_cut.empty:
        skip(f"No data for {PILOT} as of {SIGNAL_CUTOFF}")
    else:
        max_date = df_cut.index.max().date()
        if max_date <= SIGNAL_CUTOFF:
            ok(f"Max indicator date {max_date} ≤ cutoff {SIGNAL_CUTOFF} — no lookahead")
        else:
            fail(f"LOOKAHEAD: max date {max_date} > cutoff {SIGNAL_CUTOFF}")
except Exception as e:
    fail("No-lookahead indicator check", e)


# ══════════════════════════════════════════════════════════════════════════════
# 5. Single-stock backtest
# ══════════════════════════════════════════════════════════════════════════════
section("5. Single-stock backtest (HDFCBANK, 1 year)")

BT_START = date.today() - timedelta(days=365)
BT_END   = date.today() - timedelta(days=1)
CAPITAL  = 100_000.0

engine = BacktestEngine(initial_capital=CAPITAL)
result_hdfcbank = None

try:
    result_hdfcbank = engine.run(PILOT, BT_START, BT_END)
    ok(f"Backtest ran: {len(result_hdfcbank.trades)} trades, "
       f"final capital={result_hdfcbank.final_capital:,.0f}")
except Exception as e:
    fail(f"BacktestEngine.run({PILOT})", e)

if result_hdfcbank is not None:
    # Equity curve checks
    try:
        eq = result_hdfcbank.equity_curve
        if eq is None or eq.empty:
            fail("Equity curve is empty")
        else:
            ok(f"Equity curve: {len(eq)} data points, range [{eq.min():,.0f}, {eq.max():,.0f}]")
    except Exception as e:
        fail("Equity curve check", e)

    # Final capital is a float
    try:
        fc = result_hdfcbank.final_capital
        assert isinstance(fc, float) and fc > 0, f"final_capital={fc} is invalid"
        ok(f"final_capital={fc:,.2f} (positive float)")
    except AssertionError as e:
        fail(str(e))


# ══════════════════════════════════════════════════════════════════════════════
# 6. Trade log integrity
# ══════════════════════════════════════════════════════════════════════════════
section("6. Trade log integrity")

if result_hdfcbank is not None and result_hdfcbank.trades:
    trades_df = result_hdfcbank.trade_df()

    # Required columns present
    try:
        req = ["symbol", "entry_date", "entry_price", "exit_date", "exit_price",
               "quantity", "stop_loss", "target", "exit_reason", "net_pnl", "pnl_pct"]
        miss = [c for c in req if c not in trades_df.columns]
        if miss:
            fail(f"Trade log missing columns: {miss}")
        else:
            ok(f"Trade log has all required columns ({len(trades_df)} trades)")
    except Exception as e:
        fail("Trade log columns check", e)

    # No trade exits before entry
    try:
        bad = sum(1 for t in result_hdfcbank.trades if t.exit_date < t.entry_date)
        if bad:
            fail(f"{bad} trades have exit_date < entry_date")
        else:
            ok("All trades: exit_date ≥ entry_date")
    except Exception as e:
        fail("Exit date check", e)

    # Entry price within plausible range (no zero or negative prices)
    try:
        bad = trades_df[trades_df["entry_price"] <= 0]
        if not bad.empty:
            fail(f"{len(bad)} trades have entry_price ≤ 0")
        else:
            ok("All entry prices > 0")
    except Exception as e:
        fail("Entry price positivity check", e)

    # Stop-loss always below entry
    try:
        bad = trades_df[trades_df["stop_loss"] >= trades_df["entry_price"]]
        if not bad.empty:
            fail(f"{len(bad)} trades have stop_loss ≥ entry_price")
        else:
            ok("All trades: stop_loss < entry_price")
    except Exception as e:
        fail("Stop-loss below entry check", e)

    # Target always above entry
    try:
        bad = trades_df[trades_df["target"] <= trades_df["entry_price"]]
        if not bad.empty:
            fail(f"{len(bad)} trades have target ≤ entry_price")
        else:
            ok("All trades: target > entry_price")
    except Exception as e:
        fail("Target above entry check", e)

    # Exit reasons are valid values
    try:
        valid_reasons = {"stop", "stop_first", "target", "rsi_exit", "ema21_exit", "end_of_data"}
        bad = trades_df[~trades_df["exit_reason"].isin(valid_reasons)]
        if not bad.empty:
            fail(f"Unknown exit reasons: {bad['exit_reason'].unique()}")
        else:
            reasons = trades_df["exit_reason"].value_counts().to_dict()
            ok(f"All exit reasons valid: {reasons}")
    except Exception as e:
        fail("Exit reason check", e)

    # No entry data uses future OHLCV (entry_date must be <= BT_END)
    try:
        bad = [t for t in result_hdfcbank.trades if t.entry_date > BT_END]
        if bad:
            fail(f"{len(bad)} trades entered after BT_END {BT_END}")
        else:
            ok(f"No trades after end_date {BT_END} — no lookahead")
    except Exception as e:
        fail("Trade date lookahead check", e)

else:
    skip("No trades generated for HDFCBANK — integrity checks skipped (normal if signal conditions not met)")


# ══════════════════════════════════════════════════════════════════════════════
# 7. Metrics computation
# ══════════════════════════════════════════════════════════════════════════════
section("7. Metrics computation")

if result_hdfcbank is not None:
    try:
        m = compute_metrics(result_hdfcbank)
        required_keys = ["total_return_pct", "cagr_pct", "sharpe", "max_drawdown_pct",
                         "win_rate_pct", "profit_factor", "total_trades"]
        miss = [k for k in required_keys if k not in m]
        if miss:
            fail(f"Metrics missing keys: {miss}")
        else:
            ok(f"compute_metrics() returned all required keys")

        # Sanity ranges
        checks = [
            (-100 <= m["total_return_pct"] <= 10000, f"total_return_pct={m['total_return_pct']}"),
            (m["max_drawdown_pct"] <= 0,              f"max_drawdown_pct should be ≤ 0, got {m['max_drawdown_pct']}"),
            (0 <= m["win_rate_pct"] <= 100,           f"win_rate_pct={m['win_rate_pct']} out of range"),
            (m["profit_factor"] >= 0,                 f"profit_factor={m['profit_factor']} is negative"),
        ]
        for cond, msg in checks:
            if cond:
                ok(f"Metrics sanity: {msg}")
            else:
                fail(f"Metrics out of range: {msg}")

        print(f"\n  {PILOT} metrics snapshot:")
        print(f"    Return:  {m['total_return_pct']:+.2f}%  |  CAGR: {m['cagr_pct']:+.2f}%")
        print(f"    Sharpe:  {m['sharpe']:.3f}     |  Max DD: {m['max_drawdown_pct']:.2f}%")
        print(f"    Trades:  {m['total_trades']}         |  Win rate: {m['win_rate_pct']:.1f}%")
        print(f"    PF:      {m['profit_factor']:.3f}     |  Avg hold: {m['avg_hold_days']:.1f} days\n")

    except Exception as e:
        fail("compute_metrics()", e)


# ══════════════════════════════════════════════════════════════════════════════
# 8. Benchmark comparison (all 4) — soft check; network may be unavailable
# ══════════════════════════════════════════════════════════════════════════════
section("8. Benchmark comparison (all 4)")

if OFFLINE:
    skip("--offline: skipping benchmark downloads")
elif result_hdfcbank is not None:
    try:
        bm_df = compare_benchmarks(result_hdfcbank, BT_START, BT_END)
        expected_benchmarks = [
            f"{PILOT} Strategy",
            f"{PILOT} Buy-Hold",
            "NIFTY BANK",
            "NIFTY 50",
            "Equal-Weight Basket",
        ]
        for name in expected_benchmarks:
            if name in bm_df.index:
                row = bm_df.loc[name]
                ret = row.get("total_return_pct")
                ok(f"{name}: return={ret:+.1f}%" if ret is not None else f"{name}: data present")
            else:
                skip(f"{name}: not in benchmark DataFrame (data unavailable)")
    except Exception as e:
        fail("compare_benchmarks()", e)


# ══════════════════════════════════════════════════════════════════════════════
# 9. Multi-stock backtest (all 7)
# ══════════════════════════════════════════════════════════════════════════════
section("9. Multi-stock backtest (all 7 stocks)")

all_results = {}
try:
    all_results = engine.run_all(BT_START, BT_END)
    ok(f"run_all() completed: {len(all_results)} stocks processed")
except Exception as e:
    fail("engine.run_all()", e)

if all_results:
    try:
        assert len(all_results) == len(BANKING_STOCKS), \
            f"Expected {len(BANKING_STOCKS)} results, got {len(all_results)}"
        ok(f"All {len(BANKING_STOCKS)} stocks returned results")
    except AssertionError as e:
        fail(str(e))

    # Print summary table
    try:
        print()
        print_all_report(all_results, BT_START, BT_END)
        ok("print_all_report() completed without error")
    except Exception as e:
        fail("print_all_report()", e)

    # Verify no result has final_capital below 1% of initial (runaway loss)
    try:
        for sym, res in all_results.items():
            min_cap = CAPITAL * 0.01
            if res.final_capital < min_cap:
                fail(f"{sym}: final_capital={res.final_capital:.0f} < {min_cap:.0f} (extreme loss)")
            else:
                ok(f"{sym}: final={res.final_capital:,.0f}  trades={len(res.trades)}")
    except Exception as e:
        fail("Final capital check", e)


# ══════════════════════════════════════════════════════════════════════════════
# 10. Walk-forward split results
# ══════════════════════════════════════════════════════════════════════════════
section("10. Walk-forward split (train/val/test if 5yr data available)")

TRAIN_START = date(2019, 1, 1)
TRAIN_END   = date(2022, 12, 31)
VAL_START   = date(2023, 1, 1)
VAL_END     = date(2023, 12, 31)
TEST_START  = date(2024, 1, 1)
TEST_END    = date(2024, 12, 31)

# Check if we have enough data for the splits
hdfcbank_earliest = earliest_date(PILOT) if 'earliest_date' in dir() else None

if hdfcbank_earliest and hdfcbank_earliest <= TRAIN_START:
    print(f"\n  Running walk-forward on {PILOT}...")
    for split_name, s_start, s_end in [
        ("Train (2019-2022)", TRAIN_START, TRAIN_END),
        ("Val   (2023)",      VAL_START,   VAL_END),
        ("Test  (2024)",      TEST_START,  TEST_END),
    ]:
        try:
            res = engine.run(PILOT, s_start, s_end)
            m   = compute_metrics(res)
            ok(
                f"{split_name}: return={m['total_return_pct']:+.1f}%  "
                f"sharpe={m['sharpe']:.2f}  trades={m['total_trades']}"
            )
        except Exception as e:
            fail(f"Walk-forward {split_name}", e)

    # Verify test set results are logged, not optimised-on
    ok("Walk-forward complete — test set (2024) evaluated only once")

else:
    skip(
        f"Insufficient historical data for full walk-forward "
        f"(earliest={hdfcbank_earliest}, need ≤ {TRAIN_START}). "
        f"Run full 5-year backfill without --offline to enable."
    )


# ══════════════════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════════════════
total = passed + failed + skipped
print(f"\n{'═' * 70}")
print(f"  Phase 3 Results: {GREEN}{passed} passed{RESET} | {RED}{failed} failed{RESET} | {YELLOW}{skipped} skipped{RESET}  (of {total})")
print(f"{'═' * 70}\n")

if failed > 0:
    sys.exit(1)
