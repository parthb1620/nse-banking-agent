"""
Walk-forward parameter optimizer.

Concept (why this is "learning from past data"):
  The strategy has tunable knobs — RSI thresholds, ATR stop size, R:R ratio.
  The default values are guesses. Walk-forward optimization finds the values that
  actually worked on real past price data, without peeking at future prices.

Walk-forward design:
  Training window : 12 months — find the best param combo (in-sample)
  Test window     :  3 months — evaluate those params on unseen data (out-of-sample)
  Step            :  3 months — roll forward, repeat
  Minimum trades  :  3        — skip windows with too few trades (noisy)

  Best params = combo with highest average OOS Sharpe across all test windows.
  If no window has ≥3 trades, fall back to default config values.

Output:
  data_store/best_params.json  — one entry per stock, updated by optimize_all()

Usage:
  python -m backtesting.optimizer              # optimize all 7 stocks
  python -m backtesting.optimizer HDFCBANK     # optimize one stock

Public API:
  optimize_all()               → dict[symbol, best_params]
  optimize_symbol(symbol)      → dict  (best params for one stock)
  load_best_params(symbol)     → dict  (read saved params, or defaults)
"""

import itertools
import json
import math
from datetime import date
from dateutil.relativedelta import relativedelta
from pathlib import Path

from loguru import logger

from analysis.technical.indicators import get_indicators
from backtesting.engine import BacktestEngine
from backtesting.metrics import compute_metrics
from config.settings import (
    ATR_STOP_MULTIPLIER, BANKING_STOCKS,
    RSI_ENTRY_HIGH, RSI_ENTRY_LOW, RSI_EXIT,
)

_PARAMS_FILE = Path(__file__).resolve().parent.parent / "data_store" / "best_params.json"

# ── Parameter search space ─────────────────────────────────────────────────────
# Keep the grid small enough to run in < 2 minutes for all 7 stocks.
PARAM_GRID = {
    "rsi_entry_low":        [25, 30, 35, 40],
    "rsi_entry_high":       [55, 60, 65, 70],
    "atr_stop_multiplier":  [1.5, 2.0, 2.5, 3.0],
}

_DEFAULT_PARAMS = {
    "rsi_entry_low":       RSI_ENTRY_LOW,
    "rsi_entry_high":      RSI_ENTRY_HIGH,
    "rsi_exit":            RSI_EXIT,
    "atr_stop_multiplier": ATR_STOP_MULTIPLIER,
    "min_risk_reward":     2.0,
}

_TRAIN_MONTHS = 12
_TEST_MONTHS  =  3
_MIN_TRADES   =  3   # minimum trades to consider a window valid


# ── Helpers ────────────────────────────────────────────────────────────────────

def _all_param_combos() -> list[dict]:
    keys = list(PARAM_GRID.keys())
    combos = []
    for vals in itertools.product(*PARAM_GRID.values()):
        p = dict(zip(keys, vals))
        # Skip logically invalid combos
        if p["rsi_entry_low"] >= p["rsi_entry_high"]:
            continue
        combos.append(p)
    return combos


def _walk_forward_windows(data_start: date, data_end: date) -> list[tuple[date, date, date, date]]:
    """
    Generate (train_start, train_end, test_start, test_end) tuples.
    Stops when test_end would exceed data_end.
    """
    windows = []
    train_start = data_start
    while True:
        train_end  = train_start + relativedelta(months=_TRAIN_MONTHS)
        test_start = train_end
        test_end   = test_start + relativedelta(months=_TEST_MONTHS)
        if test_end > data_end:
            break
        windows.append((train_start, train_end, test_start, test_end))
        train_start += relativedelta(months=_TEST_MONTHS)   # step forward by test window
    return windows


def _sharpe_for(result) -> float:
    if not result.trades:
        return -999.0
    m = compute_metrics(result)
    s = m.get("sharpe", 0.0)
    return s if (s is not None and not math.isnan(s)) else -999.0


# ── Core optimizer ─────────────────────────────────────────────────────────────

def optimize_symbol(symbol: str) -> dict:
    """
    Run walk-forward optimization for one symbol.
    Returns the best params dict (with metadata) or defaults if optimization failed.
    """
    logger.info(f"optimizer: starting walk-forward for {symbol}")

    # Load full indicator history once — shared across all windows/combos
    full_df = get_indicators(symbol)
    if full_df.empty or len(full_df) < 60:
        logger.warning(f"optimizer: insufficient data for {symbol} — using defaults")
        return _DEFAULT_PARAMS.copy()

    data_start = full_df.index[0].date()
    data_end   = full_df.index[-1].date()
    windows    = _walk_forward_windows(data_start, data_end)

    if not windows:
        logger.warning(f"optimizer: no valid walk-forward windows for {symbol}")
        return _DEFAULT_PARAMS.copy()

    logger.info(f"optimizer: {symbol} — {len(windows)} windows, {len(_all_param_combos())} param combos")

    engine = BacktestEngine(initial_capital=100_000)
    combos = _all_param_combos()

    # oos_scores[combo_idx] = list of OOS Sharpe values from each test window
    oos_scores: dict[int, list[float]] = {i: [] for i in range(len(combos))}

    for win_num, (train_start, train_end, test_start, test_end) in enumerate(windows):
        # Step 1: find best combo on training window
        best_train_sharpe = -999.0
        best_combo_idx    = 0

        for i, params in enumerate(combos):
            result = engine.run(symbol, train_start, train_end, params=params, df=full_df)
            if len(result.trades) < _MIN_TRADES:
                continue
            s = _sharpe_for(result)
            if s > best_train_sharpe:
                best_train_sharpe = s
                best_combo_idx    = i

        # Step 2: evaluate best in-sample combo on out-of-sample test window
        test_result = engine.run(
            symbol, test_start, test_end,
            params=combos[best_combo_idx], df=full_df,
        )
        oos_sharpe = _sharpe_for(test_result)
        oos_scores[best_combo_idx].append(oos_sharpe)

        logger.debug(
            f"  win {win_num+1}: best IS combo={best_combo_idx} "
            f"IS_sharpe={best_train_sharpe:.2f}  OOS_sharpe={oos_sharpe:.2f}"
        )

    # Step 3: pick the combo with the highest average OOS Sharpe
    # Only consider combos that were selected at least once
    best_avg_oos  = -999.0
    best_final_idx = 0

    for idx, scores in oos_scores.items():
        if not scores:
            continue
        avg = sum(scores) / len(scores)
        if avg > best_avg_oos:
            best_avg_oos   = avg
            best_final_idx = idx

    best = combos[best_final_idx].copy()
    best["rsi_exit"]        = _DEFAULT_PARAMS["rsi_exit"]    # not tuned
    best["min_risk_reward"] = _DEFAULT_PARAMS["min_risk_reward"]
    best["oos_sharpe"]      = round(best_avg_oos, 3)
    best["optimized_at"]    = str(date.today())
    best["windows_tested"]  = len(windows)

    logger.info(
        f"optimizer: {symbol} best params = {best}  "
        f"(avg OOS Sharpe={best_avg_oos:.3f})"
    )
    return best


def optimize_all(symbols: list[str] | None = None) -> dict[str, dict]:
    """
    Run walk-forward optimization for all (or specified) symbols.
    Saves results to data_store/best_params.json.
    Returns {symbol: params_dict}.
    """
    symbols = symbols or BANKING_STOCKS
    results = {}

    for sym in symbols:
        try:
            results[sym] = optimize_symbol(sym)
        except Exception as exc:
            logger.error(f"optimizer: {sym} failed — {exc}")
            results[sym] = _DEFAULT_PARAMS.copy()

    _save_params(results)
    return results


# ── Persistence ────────────────────────────────────────────────────────────────

def _save_params(params: dict[str, dict]) -> None:
    _PARAMS_FILE.parent.mkdir(exist_ok=True)
    with open(_PARAMS_FILE, "w") as f:
        json.dump(params, f, indent=2)
    logger.info(f"optimizer: saved best params → {_PARAMS_FILE}")


def load_best_params(symbol: str) -> dict:
    """
    Return saved optimized params for one symbol.
    Falls back to defaults if the file doesn't exist or symbol not found.
    """
    if not _PARAMS_FILE.exists():
        return _DEFAULT_PARAMS.copy()
    try:
        with open(_PARAMS_FILE) as f:
            all_params = json.load(f)
        p = all_params.get(symbol, {})
        if not p:
            return _DEFAULT_PARAMS.copy()
        # Strip metadata keys before returning
        return {k: v for k, v in p.items() if k not in ("oos_sharpe", "optimized_at", "windows_tested")}
    except Exception as exc:
        logger.warning(f"optimizer: could not read best_params.json — {exc}")
        return _DEFAULT_PARAMS.copy()


def load_all_params() -> dict[str, dict]:
    """Return the full best_params.json as a dict, or {} if not found."""
    if not _PARAMS_FILE.exists():
        return {}
    try:
        with open(_PARAMS_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    from data.storage.database import init_db
    init_db()

    symbols = sys.argv[1:] or BANKING_STOCKS
    print(f"\nOptimizing: {', '.join(symbols)}\n")
    results = optimize_all(symbols)

    print(f"\n{'═'*72}")
    print(f"  Walk-Forward Optimization Results")
    print(f"{'═'*72}")
    print(f"  {'Symbol':<12} {'RSI Low':>8} {'RSI High':>9} {'ATR Mult':>9} {'OOS Sharpe':>11}")
    print(f"  {'─'*56}")
    for sym, p in results.items():
        print(
            f"  {sym:<12} "
            f"{p.get('rsi_entry_low', '?'):>8} "
            f"{p.get('rsi_entry_high', '?'):>9} "
            f"{p.get('atr_stop_multiplier', '?'):>9} "
            f"{p.get('oos_sharpe', 0):>+11.3f}"
        )
    print(f"{'═'*72}\n")
    print(f"Saved to: {_PARAMS_FILE}\n")
