"""
Multi-engine portfolio manager.

Manages four separate paper-trading portfolios, one per engine.
Each portfolio gets a fixed capital slice (from ENGINE_CAPITAL_SPLIT in settings).

Portfolio names (stored in paper_trades.portfolio_name):
  "longterm"   — 40% of PAPER_TRADING_CAPITAL
  "shortterm"  — 30% of PAPER_TRADING_CAPITAL
  "btst"       — 20% of PAPER_TRADING_CAPITAL
  "intraday"   — 10% of PAPER_TRADING_CAPITAL

Usage:
  from paper_trading.portfolio_manager import Portfolio

  port = Portfolio("btst")
  port.enter_trade(symbol, signal, entry_price, atr)
  port.update_open_positions(today)
  summary = port.summary()
"""

from datetime import date, datetime
from typing import Optional
from zoneinfo import ZoneInfo

from loguru import logger

from config.settings import (
    ATR_STOP_MULTIPLIER,
    ENGINE_CAPITAL_SPLIT,
    MAX_OPEN_POSITIONS,
    MIN_RISK_REWARD,
    MIN_SIGNAL_STRENGTH,
    PAPER_TRADING_CAPITAL,
    RISK_PER_TRADE_PCT,
    STOCK_NAMES,
)
from data.storage.database import OHLCVDaily, PaperTrade, TechnicalSignal, get_session

_IST = ZoneInfo("Asia/Kolkata")

# BTST and intraday have tighter position limits
_MAX_POSITIONS: dict[str, int] = {
    "longterm":  5,
    "shortterm": MAX_OPEN_POSITIONS,  # 3 from settings
    "btst":      2,
    "intraday":  2,
}

# BTST/intraday use tighter R:R — quick in, quick out
_MIN_RR: dict[str, float] = {
    "longterm":  2.5,
    "shortterm": MIN_RISK_REWARD,     # 2.0 from settings
    "btst":      1.5,
    "intraday":  1.5,
}


class Portfolio:
    """Single-engine portfolio wrapper."""

    def __init__(self, engine: str) -> None:
        if engine not in ENGINE_CAPITAL_SPLIT:
            raise ValueError(f"Unknown engine '{engine}'. Use: {list(ENGINE_CAPITAL_SPLIT)}")
        self.engine   = engine
        self.name     = engine           # used as portfolio_name in DB
        self.capital  = PAPER_TRADING_CAPITAL * ENGINE_CAPITAL_SPLIT[engine]
        self.max_pos  = _MAX_POSITIONS[engine]
        self.min_rr   = _MIN_RR[engine]

    # ── Queries ────────────────────────────────────────────────────────────────

    def open_positions(self) -> list[PaperTrade]:
        with get_session() as s:
            return (
                s.query(PaperTrade)
                .filter(
                    PaperTrade.portfolio_name == self.name,
                    PaperTrade.status == "open",
                )
                .all()
            )

    def realised_pnl(self) -> float:
        with get_session() as s:
            rows = (
                s.query(PaperTrade)
                .filter(
                    PaperTrade.portfolio_name == self.name,
                    PaperTrade.status.in_(["closed_target", "closed_stop", "closed_manual"]),
                )
                .all()
            )
        return sum(t.pnl for t in rows if t.pnl is not None)

    def current_capital(self) -> float:
        return self.capital + self.realised_pnl()

    def today_pnl(self, today: date) -> float:
        with get_session() as s:
            rows = (
                s.query(PaperTrade)
                .filter(
                    PaperTrade.portfolio_name == self.name,
                    PaperTrade.exit_date == today,
                    PaperTrade.status.in_(["closed_target", "closed_stop"]),
                )
                .all()
            )
        return sum(t.pnl for t in rows if t.pnl is not None)

    # ── Trade entry ────────────────────────────────────────────────────────────

    def enter_trade(
        self,
        symbol: str,
        signal: TechnicalSignal,
        entry_price: float,
        atr: float,
        thesis: str = "",
    ) -> Optional[PaperTrade]:
        """
        Attempt to open a new position for this portfolio.
        Returns the PaperTrade if entered, None if skipped.
        """
        if signal.strength < MIN_SIGNAL_STRENGTH:
            logger.debug(f"[{self.engine}] {symbol}: strength {signal.strength} < {MIN_SIGNAL_STRENGTH} — skip")
            return None

        open_pos = self.open_positions()
        if len(open_pos) >= self.max_pos:
            logger.info(f"[{self.engine}] {symbol}: max positions ({self.max_pos}) reached — skip")
            return None

        if any(p.symbol == symbol for p in open_pos):
            logger.debug(f"[{self.engine}] {symbol}: already have open position — skip")
            return None

        stop_loss = entry_price - ATR_STOP_MULTIPLIER * atr
        risk_per_share = entry_price - stop_loss
        if risk_per_share <= 0:
            return None

        target = entry_price + self.min_rr * risk_per_share
        rr = (target - entry_price) / risk_per_share
        if rr < self.min_rr:
            logger.debug(f"[{self.engine}] {symbol}: R:R {rr:.1f} < {self.min_rr} — skip")
            return None

        capital = self.current_capital()
        risk_amount = capital * RISK_PER_TRADE_PCT
        quantity = int(risk_amount / risk_per_share)
        if quantity < 1:
            return None

        trade = PaperTrade(
            symbol         = symbol,
            entry_date     = date.today(),
            entry_price    = round(entry_price, 2),
            stop_loss      = round(stop_loss, 2),
            target         = round(target, 2),
            quantity       = quantity,
            status         = "open",
            thesis         = thesis[:500] if thesis else "",
            signal_id      = signal.id,
            portfolio_name = self.name,
        )

        with get_session() as s:
            s.add(trade)
            s.commit()
            s.refresh(trade)

        logger.info(
            f"[{self.engine}] ENTERED {symbol}  qty={quantity}  "
            f"entry={entry_price:.2f}  stop={stop_loss:.2f}  target={target:.2f}  R:R={rr:.1f}"
        )
        return trade

    # ── Summary ────────────────────────────────────────────────────────────────

    def summary(self) -> dict:
        with get_session() as s:
            all_trades = (
                s.query(PaperTrade)
                .filter(PaperTrade.portfolio_name == self.name)
                .all()
            )

        closed = [t for t in all_trades if t.status != "open"]
        wins   = [t for t in closed if (t.pnl or 0) > 0]
        open_  = [t for t in all_trades if t.status == "open"]
        total_pnl = sum(t.pnl for t in closed if t.pnl is not None)

        return {
            "engine":          self.engine,
            "portfolio_name":  self.name,
            "initial_capital": round(self.capital, 2),
            "capital_current": round(self.capital + total_pnl, 2),
            "total_pnl":       round(total_pnl, 2),
            "trades_total":    len(closed),
            "trades_open":     len(open_),
            "win_rate":        round(len(wins) / len(closed) * 100, 1) if closed else 0.0,
        }


# ── Convenience helpers ────────────────────────────────────────────────────────

def all_summaries() -> list[dict]:
    """Return summary dicts for all four engine portfolios."""
    return [Portfolio(e).summary() for e in ENGINE_CAPITAL_SPLIT]


def get_portfolio(engine: str) -> Portfolio:
    return Portfolio(engine)
