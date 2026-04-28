"""
Backtesting engine — strict simulation rules, no lookahead.

Simulation Rules:
  Entry        — next trading day open price after signal
  Stop check   — using that day's low  (if low ≤ stop → stop hit)
  Target check — using that day's high (if high ≥ target → target hit)
  Conflict     — if both stop and target hit on same day → stop first (conservative)
  Gap down     — if open < stop on entry day: exit at open, not stop
  Signal exit  — RSI > 75 or close < EMA_21 → exit at next day's open
  Costs        — 0.40% per round trip (split evenly on entry and exit)
  Position     — risk RISK_PER_TRADE_PCT of equity per trade (ATR-based sizing)
  Max position — capped at 30% of current equity (prevents oversizing)
  Concurrent   — one position per symbol; engine processes symbols independently

Data integrity:
  Loads data with as_of_date = end_date to prevent any lookahead from
  fundamental/news tables. OHLCV is already date-filtered by OHLCVDaily.date.
"""

import math
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

import pandas as pd
from loguru import logger

from analysis.technical.indicators import get_indicators
from backtesting.strategies.ema_rsi_swing import generate_signals
from config.settings import ATR_STOP_MULTIPLIER, BANKING_STOCKS, RISK_PER_TRADE_PCT

ROUND_TRIP_COST = 0.004        # 0.40%
HALF_COST       = ROUND_TRIP_COST / 2
MAX_POSITION_PCT = 0.30        # max 30% of equity in one trade


# ── Result dataclasses ─────────────────────────────────────────────────────────

@dataclass
class Trade:
    symbol:      str
    entry_date:  date
    entry_price: float
    exit_date:   date
    exit_price:  float
    quantity:    int
    stop_loss:   float
    target:      float
    exit_reason: str      # "stop" | "stop_first" | "target" | "rsi_exit" | "ema21_exit" | "end_of_data"
    gross_pnl:   float
    cost:        float
    net_pnl:     float
    pnl_pct:     float    # net P&L as % of invested capital


@dataclass
class BacktestResult:
    symbol:       str
    trades:       list[Trade]          = field(default_factory=list)
    equity_curve: Optional[pd.Series]  = None   # date → portfolio value
    initial_capital: float = 100_000.0
    final_capital:   float = 100_000.0

    def trade_df(self) -> pd.DataFrame:
        if not self.trades:
            return pd.DataFrame()
        return pd.DataFrame([t.__dict__ for t in self.trades])


# ── Core engine ────────────────────────────────────────────────────────────────

class BacktestEngine:
    def __init__(
        self,
        initial_capital: float = 100_000.0,
        risk_per_trade:  float = RISK_PER_TRADE_PCT,
    ):
        self.initial_capital = initial_capital
        self.risk_per_trade  = risk_per_trade

    def run(
        self,
        symbol:     str,
        start_date: date,
        end_date:   date,
        params:     dict | None = None,
        df:         "pd.DataFrame | None" = None,   # pre-loaded indicators (avoids re-query)
    ) -> BacktestResult:
        """
        Run backtest for one symbol over the date range.
        params — optional strategy param overrides (see ema_rsi_swing.generate_signals).
        df     — pass pre-computed indicator DataFrame to skip DB query (used by optimizer).
        Returns a BacktestResult with trade log and daily equity curve.
        """
        if df is None:
            df = get_indicators(symbol, as_of_date=end_date)
        if df.empty or len(df) < 5:
            logger.warning(f"engine: not enough data for {symbol}")
            return BacktestResult(symbol=symbol, initial_capital=self.initial_capital, final_capital=self.initial_capital)

        # Filter to [start_date, end_date]
        df = df[(df.index.date >= start_date) & (df.index.date <= end_date)]
        if len(df) < 5:
            logger.warning(f"engine: only {len(df)} bars for {symbol} in [{start_date}, {end_date}]")
            return BacktestResult(symbol=symbol, initial_capital=self.initial_capital, final_capital=self.initial_capital)

        df = generate_signals(df, params=params)
        return self._simulate(symbol, df)

    def run_all(
        self,
        start_date: date,
        end_date:   date,
        params:     dict | None = None,
    ) -> dict[str, BacktestResult]:
        """Run backtest for all 7 banking stocks independently."""
        results = {}
        for sym in BANKING_STOCKS:
            try:
                results[sym] = self.run(sym, start_date, end_date, params=params)
                trades_n = len(results[sym].trades)
                final = results[sym].final_capital
                ret_pct = (final / self.initial_capital - 1) * 100
                logger.info(f"engine: {sym} — {trades_n} trades, final={final:,.0f} ({ret_pct:+.1f}%)")
            except Exception as exc:
                logger.error(f"engine: {sym} failed — {exc}")
                results[sym] = BacktestResult(symbol=sym, initial_capital=self.initial_capital, final_capital=self.initial_capital)
        return results

    # ── Internal simulation loop ───────────────────────────────────────────────

    def _simulate(self, symbol: str, df: pd.DataFrame) -> BacktestResult:
        capital    = self.initial_capital
        trades     = []
        equity_pts = {}

        in_trade   = False
        entry_price = stop = target = None
        quantity    = 0
        entry_date  = None
        exit_queued = False    # True when EOD exit signal → exit at next open
        exit_reason_q = ""

        for i in range(len(df)):
            bar  = df.iloc[i]
            bdate = df.index[i].date()

            # ── Daily mark-to-market ───────────────────────────────────────────
            close_px = _get(bar, "adjusted_close") or _get(bar, "close") or 0.0
            if in_trade:
                equity_pts[bdate] = capital - entry_price * quantity * (1 + HALF_COST) + quantity * close_px
            else:
                equity_pts[bdate] = capital

            # ── Execute queued EOD exit (from previous bar's signal) ───────────
            if exit_queued and in_trade:
                open_px = _get(bar, "open") or close_px
                trade, capital = self._close_trade(
                    symbol, entry_date, entry_price, bdate, open_px,
                    quantity, stop, target, exit_reason_q, capital,
                )
                trades.append(trade)
                in_trade = exit_queued = False
                equity_pts[bdate] = capital
                continue   # skip signal checking on this bar

            # ── Process current bar ────────────────────────────────────────────
            if not in_trade:
                # Check if prev bar (i-1) had entry signal → enter at today's open
                if i > 0:
                    prev = df.iloc[i - 1]
                    if _get(prev, "entry_signal"):
                        open_px = _get(bar, "open") or close_px
                        atr_val = _get(prev, "atr")
                        if not atr_val or math.isnan(atr_val) or atr_val <= 0:
                            continue   # no ATR data — skip this signal

                        risk_per_share = ATR_STOP_MULTIPLIER * atr_val
                        stop   = open_px - risk_per_share
                        target = open_px + 2.0 * risk_per_share

                        if stop <= 0:
                            continue

                        # Position sizing
                        risk_amt = capital * self.risk_per_trade
                        qty = max(1, int(risk_amt / risk_per_share))
                        qty = min(qty, int(capital * MAX_POSITION_PCT / open_px))
                        if qty < 1:
                            continue

                        entry_price = open_px
                        entry_date  = bdate
                        quantity    = qty
                        in_trade    = True
                        # Update equity after purchase
                        equity_pts[bdate] = capital - entry_price * quantity * HALF_COST + quantity * close_px

            if in_trade:
                low_px  = _get(bar, "low")
                high_px = _get(bar, "high")
                open_px = _get(bar, "open") or close_px

                stop_hit   = low_px  is not None and low_px  <= stop
                target_hit = high_px is not None and high_px >= target

                if stop_hit and target_hit:
                    # Stop-first rule
                    trade, capital = self._close_trade(
                        symbol, entry_date, entry_price, bdate,
                        stop, quantity, stop, target, "stop_first", capital,
                    )
                    trades.append(trade)
                    in_trade = False
                    equity_pts[bdate] = capital

                elif stop_hit:
                    # Gap-down protection: use open if open < stop
                    exit_px = min(open_px, stop)
                    trade, capital = self._close_trade(
                        symbol, entry_date, entry_price, bdate,
                        exit_px, quantity, stop, target, "stop", capital,
                    )
                    trades.append(trade)
                    in_trade = False
                    equity_pts[bdate] = capital

                elif target_hit:
                    trade, capital = self._close_trade(
                        symbol, entry_date, entry_price, bdate,
                        target, quantity, stop, target, "target", capital,
                    )
                    trades.append(trade)
                    in_trade = False
                    equity_pts[bdate] = capital

                else:
                    # Check EOD exit signal
                    rsi   = _get(bar, "rsi")
                    ema21 = _get(bar, "ema_21")

                    if rsi is not None and not math.isnan(rsi) and rsi > 75:
                        exit_queued = True
                        exit_reason_q = "rsi_exit"
                    elif (ema21 is not None and not math.isnan(ema21)
                          and close_px is not None and close_px < ema21):
                        exit_queued = True
                        exit_reason_q = "ema21_exit"

        # ── Force-close any open position at end of data ──────────────────────
        if in_trade:
            last_bar = df.iloc[-1]
            last_date = df.index[-1].date()
            last_close = _get(last_bar, "adjusted_close") or _get(last_bar, "close") or entry_price
            trade, capital = self._close_trade(
                symbol, entry_date, entry_price, last_date,
                last_close, quantity, stop, target, "end_of_data", capital,
            )
            trades.append(trade)
            equity_pts[last_date] = capital

        equity_series = pd.Series(equity_pts).sort_index()
        return BacktestResult(
            symbol=symbol,
            trades=trades,
            equity_curve=equity_series,
            initial_capital=self.initial_capital,
            final_capital=capital,
        )

    @staticmethod
    def _close_trade(
        symbol, entry_date, entry_price, exit_date, exit_price,
        quantity, stop, target, reason, capital,
    ) -> tuple[Trade, float]:
        """Compute P&L, deduct costs, return Trade and updated capital."""
        entry_cost = entry_price * quantity * HALF_COST
        exit_cost  = exit_price  * quantity * HALF_COST
        gross_pnl  = (exit_price - entry_price) * quantity
        net_pnl    = gross_pnl - entry_cost - exit_cost

        trade = Trade(
            symbol=symbol,
            entry_date=entry_date,
            entry_price=round(entry_price, 4),
            exit_date=exit_date,
            exit_price=round(exit_price, 4),
            quantity=quantity,
            stop_loss=round(stop, 4),
            target=round(target, 4),
            exit_reason=reason,
            gross_pnl=round(gross_pnl, 2),
            cost=round(entry_cost + exit_cost, 2),
            net_pnl=round(net_pnl, 2),
            pnl_pct=round(net_pnl / (entry_price * quantity) * 100, 3),
        )
        return trade, capital + net_pnl


# ── Utility ────────────────────────────────────────────────────────────────────

def _get(row, key):
    """Safely get a value from a dict or pandas Series, returning None if missing/NaN."""
    val = row.get(key) if isinstance(row, dict) else getattr(row, key, None)
    if val is None:
        return None
    try:
        if math.isnan(val):
            return None
    except (TypeError, ValueError):
        pass
    return val
