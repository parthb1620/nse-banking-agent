"""
SQLAlchemy ORM models for the NSE Banking Agent.

Every event-based table carries four mandatory timestamp columns:
  announced_at  — when the event actually happened (e.g. NSE filing timestamp)
  published_at  — when it appeared on the source we fetch from
  collected_at  — when our code fetched and stored it
  usable_from   — next trading-day open after announced_at

The known-time rule (enforced by data/quality/known_time.py):
  No signal may reference any row where usable_from > signal_time.
  All analysis queries must filter: WHERE usable_from <= :signal_time
"""

from datetime import datetime
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

from config.settings import DB_URL


class Base(DeclarativeBase):
    pass


# ── Master list ────────────────────────────────────────────────────────────────

class Stock(Base):
    __tablename__ = "stocks"

    symbol:    Mapped[str]           = mapped_column(sa.String(20), primary_key=True)
    name:      Mapped[str]           = mapped_column(sa.String(100), nullable=False)
    sector:    Mapped[str]           = mapped_column(sa.String(50), default="Banking")
    isin:      Mapped[Optional[str]] = mapped_column(sa.String(12))
    is_active: Mapped[bool]          = mapped_column(sa.Boolean, default=True)


# ── Price data ─────────────────────────────────────────────────────────────────

class OHLCVDaily(Base):
    """
    End-of-day OHLCV prices.
    source: 'nse_bhavcopy' | 'groww' | 'yfinance'
    needs_verification=True when source='yfinance' (retrospective adjustments may be wrong).
    adjusted_close is what indicators and backtesting must always use.
    """
    __tablename__ = "ohlcv_daily"

    id:                 Mapped[int]            = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    symbol:             Mapped[str]            = mapped_column(sa.String(20), sa.ForeignKey("stocks.symbol"), nullable=False)
    date:               Mapped[datetime]       = mapped_column(sa.Date, nullable=False)
    open:               Mapped[Optional[float]] = mapped_column(sa.Float)
    high:               Mapped[Optional[float]] = mapped_column(sa.Float)
    low:                Mapped[Optional[float]] = mapped_column(sa.Float)
    close:              Mapped[Optional[float]] = mapped_column(sa.Float)
    volume:             Mapped[Optional[int]]  = mapped_column(sa.BigInteger)
    adjusted_close:     Mapped[Optional[float]] = mapped_column(sa.Float)
    source:             Mapped[Optional[str]]  = mapped_column(sa.String(20))
    is_adjusted:        Mapped[bool]           = mapped_column(sa.Boolean, default=False)
    needs_verification: Mapped[bool]           = mapped_column(sa.Boolean, default=False)
    collected_at:       Mapped[Optional[datetime]] = mapped_column(sa.DateTime)

    __table_args__ = (
        sa.UniqueConstraint("symbol", "date", name="uq_ohlcv_symbol_date"),
        sa.Index("ix_ohlcv_symbol_date", "symbol", "date"),
    )


# ── Event tables (all carry four timestamp columns) ────────────────────────────

class CorporateAction(Base):
    """
    Splits, bonus issues, dividends from NSE/BSE filings.
    All historical OHLCV before ex_date must be backward-adjusted using ratio/amount.
    action_type: 'split' | 'bonus' | 'dividend'
    """
    __tablename__ = "corporate_actions"

    id:           Mapped[int]            = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    symbol:       Mapped[str]            = mapped_column(sa.String(20), sa.ForeignKey("stocks.symbol"), nullable=False)
    ex_date:      Mapped[datetime]       = mapped_column(sa.Date, nullable=False)
    action_type:  Mapped[str]            = mapped_column(sa.String(20), nullable=False)
    ratio:        Mapped[Optional[float]] = mapped_column(sa.Float)
    amount:       Mapped[Optional[float]] = mapped_column(sa.Float)
    announced_at: Mapped[Optional[datetime]] = mapped_column(sa.DateTime)
    published_at: Mapped[Optional[datetime]] = mapped_column(sa.DateTime)
    collected_at: Mapped[Optional[datetime]] = mapped_column(sa.DateTime)
    usable_from:  Mapped[Optional[datetime]] = mapped_column(sa.DateTime, index=True)

    __table_args__ = (sa.Index("ix_corp_actions_symbol", "symbol"),)


class Fundamental(Base):
    """
    Quarterly and annual P&L / balance sheet data from Screener.in.
    period_type: 'Q' (quarterly) | 'FY' (full year)
    usable_from = next trading-day open after announced_at (result announcement time).
    """
    __tablename__ = "fundamentals"

    id:                   Mapped[int]            = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    symbol:               Mapped[str]            = mapped_column(sa.String(20), sa.ForeignKey("stocks.symbol"), nullable=False)
    period_end_date:      Mapped[datetime]       = mapped_column(sa.Date, nullable=False)
    period_type:          Mapped[str]            = mapped_column(sa.String(2), nullable=False)
    revenue:              Mapped[Optional[float]] = mapped_column(sa.Float)
    pat:                  Mapped[Optional[float]] = mapped_column(sa.Float)
    ebitda:               Mapped[Optional[float]] = mapped_column(sa.Float)
    total_assets:         Mapped[Optional[float]] = mapped_column(sa.Float)
    total_equity:         Mapped[Optional[float]] = mapped_column(sa.Float)
    total_debt:           Mapped[Optional[float]] = mapped_column(sa.Float)
    eps:                  Mapped[Optional[float]] = mapped_column(sa.Float)
    book_value_per_share: Mapped[Optional[float]] = mapped_column(sa.Float)
    announced_at:         Mapped[Optional[datetime]] = mapped_column(sa.DateTime)
    published_at:         Mapped[Optional[datetime]] = mapped_column(sa.DateTime)
    collected_at:         Mapped[Optional[datetime]] = mapped_column(sa.DateTime)
    usable_from:          Mapped[Optional[datetime]] = mapped_column(sa.DateTime, index=True)

    __table_args__ = (
        sa.UniqueConstraint("symbol", "period_end_date", "period_type", name="uq_fundamentals_symbol_period"),
        sa.Index("ix_fundamentals_symbol", "symbol"),
    )


class BankingMetric(Base):
    """
    Banking-sector-specific KPIs parsed from quarterly results.
    All values are percentages unless noted.
    """
    __tablename__ = "banking_metrics"

    id:              Mapped[int]            = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    symbol:          Mapped[str]            = mapped_column(sa.String(20), sa.ForeignKey("stocks.symbol"), nullable=False)
    period_end_date: Mapped[datetime]       = mapped_column(sa.Date, nullable=False)
    nim:             Mapped[Optional[float]] = mapped_column(sa.Float)   # Net Interest Margin %
    gnpa:            Mapped[Optional[float]] = mapped_column(sa.Float)   # Gross NPA %
    nnpa:            Mapped[Optional[float]] = mapped_column(sa.Float)   # Net NPA %
    casa:            Mapped[Optional[float]] = mapped_column(sa.Float)   # CASA ratio %
    pcr:             Mapped[Optional[float]] = mapped_column(sa.Float)   # Provision Coverage Ratio %
    roe:             Mapped[Optional[float]] = mapped_column(sa.Float)   # Return on Equity %
    roa:             Mapped[Optional[float]] = mapped_column(sa.Float)   # Return on Assets %
    car:             Mapped[Optional[float]] = mapped_column(sa.Float)   # Capital Adequacy Ratio %
    credit_growth:   Mapped[Optional[float]] = mapped_column(sa.Float)   # YoY loan book growth %
    slippage_ratio:  Mapped[Optional[float]] = mapped_column(sa.Float)   # Fresh NPA formation %
    announced_at:    Mapped[Optional[datetime]] = mapped_column(sa.DateTime)
    published_at:    Mapped[Optional[datetime]] = mapped_column(sa.DateTime)
    collected_at:    Mapped[Optional[datetime]] = mapped_column(sa.DateTime)
    usable_from:     Mapped[Optional[datetime]] = mapped_column(sa.DateTime, index=True)

    __table_args__ = (
        sa.UniqueConstraint("symbol", "period_end_date", name="uq_banking_metrics_symbol_period"),
        sa.Index("ix_banking_metrics_symbol", "symbol"),
    )


class CorporateFiling(Base):
    """
    NSE corporate announcements: results, board meetings, shareholding patterns, etc.
    published_at = when NSE published it; usable_from = next trading-day open.
    """
    __tablename__ = "corporate_filings"

    id:           Mapped[int]            = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    symbol:       Mapped[str]            = mapped_column(sa.String(20), sa.ForeignKey("stocks.symbol"), nullable=False)
    event_date:   Mapped[Optional[datetime]] = mapped_column(sa.Date)
    category:     Mapped[Optional[str]]  = mapped_column(sa.String(100))
    subject:      Mapped[Optional[str]]  = mapped_column(sa.String(500))
    content:      Mapped[Optional[str]]  = mapped_column(sa.Text)
    published_at: Mapped[Optional[datetime]] = mapped_column(sa.DateTime)
    collected_at: Mapped[Optional[datetime]] = mapped_column(sa.DateTime)
    usable_from:  Mapped[Optional[datetime]] = mapped_column(sa.DateTime, index=True)

    __table_args__ = (sa.Index("ix_filings_symbol", "symbol"),)


class NewsArticle(Base):
    """
    News from Google News RSS, MoneyControl RSS, Economic Times RSS.
    sentiment_score set by LLM analyzer: -1.0 (very bearish) to +1.0 (very bullish).
    usable_from = published_at rounded up to next trading-day open.
    """
    __tablename__ = "news_articles"

    id:              Mapped[int]            = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    symbol:          Mapped[str]            = mapped_column(sa.String(20), sa.ForeignKey("stocks.symbol"), nullable=False)
    source:          Mapped[Optional[str]]  = mapped_column(sa.String(50))
    headline:        Mapped[Optional[str]]  = mapped_column(sa.String(500))
    content:         Mapped[Optional[str]]  = mapped_column(sa.Text)
    published_at:    Mapped[Optional[datetime]] = mapped_column(sa.DateTime)
    collected_at:    Mapped[Optional[datetime]] = mapped_column(sa.DateTime)
    usable_from:     Mapped[Optional[datetime]] = mapped_column(sa.DateTime, index=True)
    sentiment_score: Mapped[Optional[float]] = mapped_column(sa.Float)

    __table_args__ = (sa.Index("ix_news_symbol_published", "symbol", "published_at"),)


# ── Derived / internal tables ──────────────────────────────────────────────────

class TechnicalSignal(Base):
    """
    BUY / SELL / NEUTRAL signals generated after market close using that day's OHLCV.
    signal_type: 'BUY' | 'SELL' | 'NEUTRAL'
    strength: 1 (weak) – 10 (very strong confluence)
    indicators_json: snapshot of indicator values used (for audit and backtesting)
    """
    __tablename__ = "technical_signals"

    id:              Mapped[int]            = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    symbol:          Mapped[str]            = mapped_column(sa.String(20), sa.ForeignKey("stocks.symbol"), nullable=False)
    signal_date:     Mapped[datetime]       = mapped_column(sa.Date, nullable=False)
    signal_type:     Mapped[str]            = mapped_column(sa.String(10), nullable=False)
    strength:        Mapped[Optional[int]]  = mapped_column(sa.Integer)
    reason:          Mapped[Optional[str]]  = mapped_column(sa.String(500))
    indicators_json: Mapped[Optional[str]]  = mapped_column(sa.Text)
    generated_at:    Mapped[Optional[datetime]] = mapped_column(sa.DateTime)

    __table_args__ = (sa.Index("ix_signals_symbol_date", "symbol", "signal_date"),)


class PaperTrade(Base):
    """
    Simulated trades logged by paper_trading/simulator.py.
    Entry always at next-day open. Stop-first rule applies when stop and target both hit same day.
    status: 'open' | 'closed_target' | 'closed_stop' | 'closed_manual'

    Partial-profit fields (populated when half is booked at 1R):
      partial_qty / partial_exit_price / partial_exit_date / partial_pnl
    Final pnl column = partial_pnl + remaining-quantity pnl after costs.
    """
    __tablename__ = "paper_trades"

    id:          Mapped[int]            = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    symbol:      Mapped[str]            = mapped_column(sa.String(20), sa.ForeignKey("stocks.symbol"), nullable=False)
    entry_date:  Mapped[Optional[datetime]] = mapped_column(sa.Date)
    entry_price: Mapped[Optional[float]] = mapped_column(sa.Float)
    stop_loss:   Mapped[Optional[float]] = mapped_column(sa.Float)
    target:      Mapped[Optional[float]] = mapped_column(sa.Float)
    exit_date:   Mapped[Optional[datetime]] = mapped_column(sa.Date)
    exit_price:  Mapped[Optional[float]] = mapped_column(sa.Float)
    quantity:    Mapped[Optional[int]]   = mapped_column(sa.Integer)
    status:      Mapped[str]             = mapped_column(sa.String(20), default="open")
    pnl:         Mapped[Optional[float]] = mapped_column(sa.Float)
    thesis:      Mapped[Optional[str]]   = mapped_column(sa.Text)
    signal_id:   Mapped[Optional[int]]   = mapped_column(sa.Integer, sa.ForeignKey("technical_signals.id"))

    partial_qty:        Mapped[Optional[int]]   = mapped_column(sa.Integer)
    partial_exit_price: Mapped[Optional[float]] = mapped_column(sa.Float)
    partial_exit_date:  Mapped[Optional[datetime]] = mapped_column(sa.Date)
    partial_pnl:        Mapped[Optional[float]] = mapped_column(sa.Float)

    __table_args__ = (sa.Index("ix_paper_trades_symbol", "symbol"),)


class LLMLog(Base):
    """
    Tracks LLM prediction vs actual outcome to measure directional accuracy.
    After 30 days: if accuracy < 55%, reduce sentiment weight in stock_scorer.py.
    prediction / outcome: 'bullish' | 'bearish' | 'neutral'
    accuracy: 1.0 = correct direction, 0.0 = wrong
    """
    __tablename__ = "llm_log"

    id:            Mapped[int]            = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    symbol:        Mapped[str]            = mapped_column(sa.String(20), sa.ForeignKey("stocks.symbol"), nullable=False)
    date:          Mapped[Optional[datetime]] = mapped_column(sa.Date)
    model:         Mapped[Optional[str]]  = mapped_column(sa.String(50))
    prompt_hash:   Mapped[Optional[str]]  = mapped_column(sa.String(64))
    response_json: Mapped[Optional[str]]  = mapped_column(sa.Text)
    prediction:    Mapped[Optional[str]]  = mapped_column(sa.String(10))
    outcome:       Mapped[Optional[str]]  = mapped_column(sa.String(10))
    accuracy:      Mapped[Optional[float]] = mapped_column(sa.Float)

    __table_args__ = (sa.Index("ix_llm_log_symbol_date", "symbol", "date"),)


class DataQualityLog(Base):
    """
    Daily data quality report per stock produced by data/quality/candle_checks.py.
    quality_score = present_candles / expected_candles (0.0–1.0).
    Stocks with quality_score < 0.95 are excluded from analysis until fixed.
    issues_json: JSON list of issue descriptions (missing dates, flagged candles, etc.)
    """
    __tablename__ = "data_quality_log"

    id:               Mapped[int]            = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    symbol:           Mapped[str]            = mapped_column(sa.String(20), sa.ForeignKey("stocks.symbol"), nullable=False)
    check_date:       Mapped[datetime]       = mapped_column(sa.Date, nullable=False)
    expected_candles: Mapped[Optional[int]]  = mapped_column(sa.Integer)
    present_candles:  Mapped[Optional[int]]  = mapped_column(sa.Integer)
    missing_candles:  Mapped[Optional[int]]  = mapped_column(sa.Integer)
    flagged_candles:  Mapped[Optional[int]]  = mapped_column(sa.Integer)
    quality_score:    Mapped[Optional[float]] = mapped_column(sa.Float)
    issues_json:      Mapped[Optional[str]]  = mapped_column(sa.Text)
    checked_at:       Mapped[Optional[datetime]] = mapped_column(sa.DateTime)

    __table_args__ = (
        sa.UniqueConstraint("symbol", "check_date", name="uq_quality_log_symbol_date"),
        sa.Index("ix_quality_log_symbol", "symbol"),
    )


# ── Engine & session helpers ───────────────────────────────────────────────────

engine = create_engine(DB_URL, echo=False)


# WAL mode gives better read concurrency and crash safety for SQLite
@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_conn, _):
    if DB_URL.startswith("sqlite"):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()


def _migrate_paper_trades() -> None:
    """
    SQLite-only ALTER TABLE migration: add partial-profit columns to an existing
    paper_trades table. create_all() does not add new columns, so we patch them in.
    """
    if not DB_URL.startswith("sqlite"):
        return
    new_cols = {
        "partial_qty":        "INTEGER",
        "partial_exit_price": "REAL",
        "partial_exit_date":  "DATE",
        "partial_pnl":        "REAL",
    }
    with engine.begin() as conn:
        existing = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(paper_trades)").fetchall()}
        for col, sql_type in new_cols.items():
            if col not in existing:
                conn.exec_driver_sql(f"ALTER TABLE paper_trades ADD COLUMN {col} {sql_type}")


def init_db() -> None:
    """Create all tables if they don't exist. Safe to call on every startup."""
    Base.metadata.create_all(engine)
    _migrate_paper_trades()


def get_session() -> Session:
    """Return a new SQLAlchemy Session. Caller is responsible for commit/close."""
    return Session(engine)
