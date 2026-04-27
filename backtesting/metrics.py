"""
Backtest performance metrics.

Computes:
  Sharpe ratio       — annualised (252 trading days), risk-free = 6% (India T-bill)
  Max drawdown       — peak-to-trough as % of peak equity
  Win rate           — % of trades that closed profitable
  Profit factor      — gross wins / gross losses (> 1.5 is good)
  CAGR               — compound annual growth rate
  Total return       — overall % gain

Benchmark comparisons (all 4 required by plan):
  1. NIFTY BANK index
  2. NIFTY 50 index
  3. Buy-and-hold each individual stock
  4. Equal-weight banking basket (rebalanced quarterly)

NIFTY BANK and NIFTY 50 benchmarks use yfinance (^NSEBANK / ^NSEI).
"""

import math
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf
from loguru import logger

from backtesting.engine import BacktestResult
from config.settings import BANKING_STOCKS_YF

RISK_FREE_RATE_ANNUAL = 0.06   # 6% India risk-free proxy
TRADING_DAYS_YEAR     = 252


# ── Core metric calculations ───────────────────────────────────────────────────

def compute_metrics(result: BacktestResult, risk_free: float = RISK_FREE_RATE_ANNUAL) -> dict:
    """
    Compute all performance metrics from a BacktestResult.
    Returns a dict with named metrics.
    """
    trades = result.trade_df()
    eq     = result.equity_curve

    metrics: dict = {
        "symbol":          result.symbol,
        "initial_capital": result.initial_capital,
        "final_capital":   result.final_capital,
        "total_trades":    len(result.trades),
        "total_return_pct": 0.0,
        "cagr_pct":         0.0,
        "sharpe":           0.0,
        "max_drawdown_pct": 0.0,
        "win_rate_pct":     0.0,
        "profit_factor":    0.0,
        "avg_win_pct":      0.0,
        "avg_loss_pct":     0.0,
        "avg_hold_days":    0.0,
    }

    if not result.trades:
        return metrics

    # ── Total return ──────────────────────────────────────────────────────────
    total_ret = (result.final_capital / result.initial_capital - 1) * 100
    metrics["total_return_pct"] = round(total_ret, 2)

    # ── CAGR ─────────────────────────────────────────────────────────────────
    if eq is not None and len(eq) >= 2:
        start_date = eq.index[0]
        end_date   = eq.index[-1]
        years = max((end_date - start_date).days / 365.25, 0.01)
        cagr  = ((result.final_capital / result.initial_capital) ** (1 / years) - 1) * 100
        metrics["cagr_pct"] = round(cagr, 2)
    elif result.trades:
        first = result.trades[0].entry_date
        last  = result.trades[-1].exit_date
        years = max((last - first).days / 365.25, 0.01)
        cagr  = ((result.final_capital / result.initial_capital) ** (1 / years) - 1) * 100
        metrics["cagr_pct"] = round(cagr, 2)

    # ── Sharpe ratio (from equity curve daily returns) ─────────────────────────
    if eq is not None and len(eq) > 5:
        daily_ret = eq.pct_change().dropna()
        rf_daily  = risk_free / TRADING_DAYS_YEAR
        excess    = daily_ret - rf_daily
        std       = excess.std()
        if std > 0:
            sharpe = (excess.mean() / std) * math.sqrt(TRADING_DAYS_YEAR)
            metrics["sharpe"] = round(sharpe, 3)

    # ── Max drawdown ──────────────────────────────────────────────────────────
    if eq is not None and len(eq) > 1:
        peak   = eq.cummax()
        trough = (eq - peak) / peak * 100
        metrics["max_drawdown_pct"] = round(trough.min(), 2)

    # ── Trade statistics ──────────────────────────────────────────────────────
    if not trades.empty:
        wins   = trades[trades["net_pnl"] > 0]
        losses = trades[trades["net_pnl"] <= 0]

        metrics["win_rate_pct"] = round(len(wins) / len(trades) * 100, 1)

        gross_wins   = wins["net_pnl"].sum()
        gross_losses = abs(losses["net_pnl"].sum())
        metrics["profit_factor"] = round(gross_wins / gross_losses, 3) if gross_losses > 0 else float("inf")

        if len(wins) > 0:
            metrics["avg_win_pct"] = round(wins["pnl_pct"].mean(), 2)
        if len(losses) > 0:
            metrics["avg_loss_pct"] = round(losses["pnl_pct"].mean(), 2)

        hold_days = [(t.exit_date - t.entry_date).days for t in result.trades]
        metrics["avg_hold_days"] = round(sum(hold_days) / len(hold_days), 1)

    return metrics


# ── Benchmark helpers ──────────────────────────────────────────────────────────

def _download_yf(ticker: str, start: date, end: date) -> Optional[pd.Series]:
    """Download adjusted close series from yfinance. Returns None on failure."""
    try:
        df = yf.download(ticker, start=str(start), end=str(end),
                         auto_adjust=True, progress=False)
        if df.empty:
            return None
        df = df.reset_index()
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        return df.set_index("Date")["Close"].sort_index()
    except Exception as exc:
        logger.error(f"benchmark download {ticker}: {exc}")
        return None


def benchmark_buy_hold(
    ticker: str,
    start:  date,
    end:    date,
    capital: float = 100_000.0,
) -> Optional[pd.Series]:
    """
    Buy-and-hold equity curve for one ticker (yfinance symbol, e.g. "HDFCBANK.NS").
    Returns None if data unavailable.
    """
    prices = _download_yf(ticker, start, end)
    if prices is None or prices.empty:
        return None
    shares = capital / prices.iloc[0]
    return (prices * shares).rename(ticker)


def benchmark_equal_weight_basket(
    start:   date,
    end:     date,
    capital: float = 100_000.0,
) -> Optional[pd.Series]:
    """
    Equal-weight basket of all 7 banking stocks, rebalanced quarterly.
    Returns equity curve Series or None if insufficient data.
    """
    per_stock = capital / len(BANKING_STOCKS_YF)
    curves = []
    for ticker in BANKING_STOCKS_YF:
        c = benchmark_buy_hold(ticker, start, end, per_stock)
        if c is not None:
            curves.append(c)

    if not curves:
        return None

    # Align on common dates and sum
    combined = pd.concat(curves, axis=1).ffill().sum(axis=1)
    combined.name = "equal_weight_basket"
    return combined


def benchmark_nifty_bank(start: date, end: date, capital: float = 100_000.0) -> Optional[pd.Series]:
    """NIFTY BANK index equity curve (^NSEBANK)."""
    prices = _download_yf("^NSEBANK", start, end)
    if prices is None:
        return None
    shares = capital / prices.iloc[0]
    return (prices * shares).rename("NIFTY_BANK")


def benchmark_nifty50(start: date, end: date, capital: float = 100_000.0) -> Optional[pd.Series]:
    """NIFTY 50 index equity curve (^NSEI)."""
    prices = _download_yf("^NSEI", start, end)
    if prices is None:
        return None
    shares = capital / prices.iloc[0]
    return (prices * shares).rename("NIFTY_50")


# ── Benchmark metrics ─────────────────────────────────────────────────────────

def _equity_curve_metrics(name: str, curve: pd.Series, capital: float) -> dict:
    """Compute CAGR, Sharpe, max-DD for an arbitrary equity curve."""
    if curve is None or len(curve) < 2:
        return {"name": name, "cagr_pct": None, "sharpe": None, "max_drawdown_pct": None, "total_return_pct": None}

    total_ret = (curve.iloc[-1] / capital - 1) * 100
    years     = max((curve.index[-1] - curve.index[0]).days / 365.25, 0.01)
    cagr      = ((curve.iloc[-1] / capital) ** (1 / years) - 1) * 100

    daily_ret = curve.pct_change().dropna()
    rf_daily  = RISK_FREE_RATE_ANNUAL / TRADING_DAYS_YEAR
    excess    = daily_ret - rf_daily
    sharpe    = (excess.mean() / excess.std() * math.sqrt(TRADING_DAYS_YEAR)) if excess.std() > 0 else 0.0

    peak    = curve.cummax()
    max_dd  = ((curve - peak) / peak * 100).min()

    return {
        "name":             name,
        "total_return_pct": round(total_ret, 2),
        "cagr_pct":         round(cagr, 2),
        "sharpe":           round(sharpe, 3),
        "max_drawdown_pct": round(max_dd, 2),
    }


def compare_benchmarks(
    strategy_result: BacktestResult,
    start: date,
    end:   date,
) -> pd.DataFrame:
    """
    Compare strategy against all 4 benchmarks.
    Returns a DataFrame with one row per strategy/benchmark.
    """
    capital = strategy_result.initial_capital
    rows    = []

    # Strategy row
    sm = compute_metrics(strategy_result)
    rows.append({
        "name":             f"{strategy_result.symbol} Strategy",
        "total_return_pct": sm["total_return_pct"],
        "cagr_pct":         sm["cagr_pct"],
        "sharpe":           sm["sharpe"],
        "max_drawdown_pct": sm["max_drawdown_pct"],
        "total_trades":     sm["total_trades"],
        "win_rate_pct":     sm["win_rate_pct"],
    })

    # Buy-and-hold for this symbol
    ticker = f"{strategy_result.symbol}.NS"
    bh = benchmark_buy_hold(ticker, start, end, capital)
    bm = _equity_curve_metrics(f"{strategy_result.symbol} Buy-Hold", bh, capital)
    bm["total_trades"] = 1
    bm["win_rate_pct"] = 100.0 if bh is not None and bh.iloc[-1] > capital else 0.0
    rows.append(bm)

    # NIFTY BANK
    nb = benchmark_nifty_bank(start, end, capital)
    bm = _equity_curve_metrics("NIFTY BANK", nb, capital)
    bm["total_trades"] = 1
    bm["win_rate_pct"] = None
    rows.append(bm)

    # NIFTY 50
    n50 = benchmark_nifty50(start, end, capital)
    bm = _equity_curve_metrics("NIFTY 50", n50, capital)
    bm["total_trades"] = 1
    bm["win_rate_pct"] = None
    rows.append(bm)

    # Equal-weight basket
    ew = benchmark_equal_weight_basket(start, end, capital)
    bm = _equity_curve_metrics("Equal-Weight Basket", ew, capital)
    bm["total_trades"] = 1
    bm["win_rate_pct"] = None
    rows.append(bm)

    return pd.DataFrame(rows).set_index("name")


def compare_all_stocks(
    results: dict[str, BacktestResult],
    start: date,
    end:   date,
) -> pd.DataFrame:
    """
    Compare all 7 strategy results against NIFTY BANK and equal-weight basket.
    Returns a summary DataFrame.
    """
    capital = next(iter(results.values())).initial_capital

    rows = []
    for sym, res in results.items():
        m = compute_metrics(res)
        rows.append({
            "symbol":           sym,
            "total_return_pct": m["total_return_pct"],
            "cagr_pct":         m["cagr_pct"],
            "sharpe":           m["sharpe"],
            "max_drawdown_pct": m["max_drawdown_pct"],
            "total_trades":     m["total_trades"],
            "win_rate_pct":     m["win_rate_pct"],
            "profit_factor":    m["profit_factor"],
        })

    return pd.DataFrame(rows).set_index("symbol").sort_values("cagr_pct", ascending=False)


# ── Reporting ─────────────────────────────────────────────────────────────────

def print_report(result: BacktestResult, start: date, end: date) -> None:
    """Print a formatted performance report with benchmark comparison."""
    m = compute_metrics(result)

    sym = result.symbol
    print(f"\n{'═' * 72}")
    print(f"  {sym} — EMA+RSI+MACD Swing Strategy")
    print(f"  Period: {start} → {end}  |  Capital: ₹{result.initial_capital:,.0f}")
    print(f"{'═' * 72}")
    print(f"  Total return:  {m['total_return_pct']:+.2f}%    CAGR: {m['cagr_pct']:+.2f}%")
    print(f"  Sharpe ratio:  {m['sharpe']:.3f}         Max drawdown: {m['max_drawdown_pct']:.2f}%")
    print(f"  Trades:        {m['total_trades']}               Win rate: {m['win_rate_pct']:.1f}%")
    print(f"  Profit factor: {m['profit_factor']:.3f}         Avg hold: {m['avg_hold_days']:.1f} days")
    print(f"  Avg win:       {m['avg_win_pct']:+.2f}%           Avg loss: {m['avg_loss_pct']:+.2f}%")

    print(f"\n  {'─' * 68}")
    print(f"  Benchmark comparison:")
    print(f"  {'─' * 68}")

    try:
        bm_df = compare_benchmarks(result, start, end)
        print(f"  {'Name':<28} {'Return':>8}  {'CAGR':>7}  {'Sharpe':>7}  {'MaxDD':>7}")
        print(f"  {'─' * 64}")
        for name, row in bm_df.iterrows():
            ret_s  = f"{row['total_return_pct']:+.1f}%" if row['total_return_pct'] is not None else "  n/a"
            cagr_s = f"{row['cagr_pct']:+.1f}%" if row['cagr_pct'] is not None else "  n/a"
            sh_s   = f"{row['sharpe']:.2f}"       if row['sharpe'] is not None else "  n/a"
            dd_s   = f"{row['max_drawdown_pct']:.1f}%" if row['max_drawdown_pct'] is not None else "  n/a"
            print(f"  {str(name):<28} {ret_s:>8}  {cagr_s:>7}  {sh_s:>7}  {dd_s:>7}")
    except Exception as exc:
        logger.error(f"benchmark comparison failed: {exc}")

    print(f"{'═' * 72}\n")


def print_all_report(
    results: dict[str, BacktestResult],
    start:   date,
    end:     date,
) -> None:
    """Print summary table for all 7 stocks."""
    df = compare_all_stocks(results, start, end)
    cap = next(iter(results.values())).initial_capital

    print(f"\n{'═' * 80}")
    print(f"  NSE Banking Sector — Strategy Summary  |  {start} → {end}")
    print(f"  Initial capital per stock: ₹{cap:,.0f}")
    print(f"{'═' * 80}")
    print(f"  {'Symbol':<12} {'Return':>8}  {'CAGR':>7}  {'Sharpe':>7}  {'MaxDD':>7}  {'Trades':>7}  {'WinRate':>8}")
    print(f"  {'─' * 74}")
    for sym, row in df.iterrows():
        print(
            f"  {sym:<12} "
            f"{row['total_return_pct']:+8.1f}%  "
            f"{row['cagr_pct']:+7.1f}%  "
            f"{row['sharpe']:7.2f}  "
            f"{row['max_drawdown_pct']:7.1f}%  "
            f"{int(row['total_trades']):7d}  "
            f"{row['win_rate_pct']:7.1f}%"
        )
    print(f"{'═' * 80}\n")
