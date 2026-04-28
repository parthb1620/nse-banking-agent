"""
Backtesting CLI — run strategy backtests and/or walk-forward optimization.

Usage:
  python -m backtesting.run                     # baseline backtest (default params, last 3 years)
  python -m backtesting.run --optimize          # walk-forward optimize then re-run with best params
  python -m backtesting.run --symbol HDFCBANK   # single stock
  python -m backtesting.run --start 2023-01-01  # custom start date
  python -m backtesting.run --years 2           # last N years

Flags:
  --optimize   run walk-forward optimizer first, then show optimized backtest
  --baseline   show baseline (default params) results alongside optimized
  --symbol X   run for one symbol only
  --start DATE override start date (YYYY-MM-DD)
  --years N    how many years of history to use (default 3)
"""

import argparse
import sys
from datetime import date, timedelta

from loguru import logger

from backtesting.engine import BacktestEngine
from backtesting.metrics import compute_metrics, print_all_report, print_report
from config.settings import BANKING_STOCKS


def _parse_args():
    p = argparse.ArgumentParser(description="NSE Banking backtest runner")
    p.add_argument("--optimize", action="store_true", help="Run walk-forward optimizer first")
    p.add_argument("--baseline", action="store_true", help="Show baseline results alongside optimized")
    p.add_argument("--symbol",   type=str,  default=None, help="Single symbol to run")
    p.add_argument("--start",    type=str,  default=None, help="Start date YYYY-MM-DD")
    p.add_argument("--years",    type=int,  default=3,    help="Years of history (default 3)")
    return p.parse_args()


def run_baseline(symbols: list[str], start: date, end: date) -> dict:
    engine  = BacktestEngine(initial_capital=100_000)
    results = {}
    for sym in symbols:
        results[sym] = engine.run(sym, start, end)
    return results


def run_optimized(symbols: list[str], start: date, end: date) -> dict:
    from backtesting.optimizer import load_best_params
    engine  = BacktestEngine(initial_capital=100_000)
    results = {}
    for sym in symbols:
        params = load_best_params(sym)
        results[sym] = engine.run(sym, start, end, params=params)
    return results


def _print_comparison(baseline: dict, optimized: dict, start: date, end: date) -> None:
    print(f"\n{'═'*88}")
    print(f"  Baseline vs Optimized — {start} → {end}")
    print(f"{'═'*88}")
    print(f"  {'Symbol':<12}  {'Base Return':>12}  {'Opt Return':>12}  {'Base Sharpe':>12}  {'Opt Sharpe':>12}  {'Base WR':>8}  {'Opt WR':>8}")
    print(f"  {'─'*84}")

    for sym in baseline:
        bm = compute_metrics(baseline[sym])
        om = compute_metrics(optimized[sym])
        print(
            f"  {sym:<12}  "
            f"{bm['total_return_pct']:>+11.1f}%  "
            f"{om['total_return_pct']:>+11.1f}%  "
            f"{bm['sharpe']:>12.3f}  "
            f"{om['sharpe']:>12.3f}  "
            f"{bm['win_rate_pct']:>7.1f}%  "
            f"{om['win_rate_pct']:>7.1f}%"
        )
    print(f"{'═'*88}\n")


def main():
    from data.storage.database import init_db
    init_db()

    args = _parse_args()

    end   = date.today()
    start = (
        date.fromisoformat(args.start) if args.start
        else end.replace(year=end.year - args.years)
    )

    symbols = [args.symbol] if args.symbol else BANKING_STOCKS
    print(f"\nBacktest  {', '.join(symbols)}  |  {start} → {end}\n")

    # ── Step 1: Optimize (if requested) ───────────────────────────────────────
    if args.optimize:
        from backtesting.optimizer import optimize_all
        print("Running walk-forward optimization (this takes ~1–2 minutes)...\n")
        opt_results = optimize_all(symbols)

        print(f"\n{'─'*72}")
        print(f"  Optimized parameters:")
        print(f"  {'Symbol':<12} {'RSI Low':>8} {'RSI High':>9} {'ATR Mult':>9} {'OOS Sharpe':>11}")
        print(f"  {'─'*52}")
        for sym, p in opt_results.items():
            print(
                f"  {sym:<12} "
                f"{p.get('rsi_entry_low','?'):>8} "
                f"{p.get('rsi_entry_high','?'):>9} "
                f"{p.get('atr_stop_multiplier','?'):>9} "
                f"{p.get('oos_sharpe', 0):>+11.3f}"
            )
        print(f"{'─'*72}\n")

    # ── Step 2: Baseline backtest ──────────────────────────────────────────────
    baseline_results = run_baseline(symbols, start, end)

    if args.optimize or args.baseline:
        print("── Baseline (default params) ───────────────────────────────────────────")
        if len(symbols) == 1:
            print_report(baseline_results[symbols[0]], start, end)
        else:
            print_all_report(baseline_results, start, end)

    # ── Step 3: Optimized backtest ─────────────────────────────────────────────
    if args.optimize:
        optimized_results = run_optimized(symbols, start, end)

        print("── Optimized (walk-forward params) ─────────────────────────────────────")
        if len(symbols) == 1:
            print_report(optimized_results[symbols[0]], start, end)
        else:
            print_all_report(optimized_results, start, end)

        _print_comparison(baseline_results, optimized_results, start, end)
    else:
        # Plain baseline run
        if len(symbols) == 1:
            print_report(baseline_results[symbols[0]], start, end)
        else:
            print_all_report(baseline_results, start, end)


if __name__ == "__main__":
    main()
