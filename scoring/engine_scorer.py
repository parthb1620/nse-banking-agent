"""
Multi-engine scorer — one entry point for all four trading engines.

Engine weight profiles (tech, fund, sentiment):
  longterm   (0.20, 0.60, 0.20) — fundamentals-first; quarterly results, ROE/ROCE
  shortterm  (0.50, 0.30, 0.20) — balanced swing; same as legacy stock_scorer
  btst       (0.70, 0.10, 0.20) — technicals-first; overnight momentum + news
  intraday   (1.00, 0.00, 0.00) — pure price action; VWAP/ORB/volume

Long-term engine uses a fundamentals-enhanced scorer that also evaluates
revenue growth trend and return metrics in addition to the standard ratios.
"""

from datetime import datetime
from typing import Literal, Optional

from loguru import logger

from analysis.fundamental.ratios import score as ratios_score
from analysis.technical.signals import score as technical_score
from config.settings import (
    BANKING_STOCKS,
    ENGINE_WEIGHTS,
    LONGTERM_UNIVERSE,
    SHORTTERM_UNIVERSE,
    BTST_UNIVERSE,
    INTRADAY_UNIVERSE,
    LLM_ACCURACY_LOW_WEIGHT,
    LLM_ACCURACY_THRESHOLD,
    STOCK_NAMES,
)
from data.storage.database import LLMLog, NewsArticle, get_session
from data.quality.known_time import known_time_filter

Engine = Literal["longterm", "shortterm", "btst", "intraday"]

_UNIVERSE: dict[str, list[str]] = {
    "longterm":  LONGTERM_UNIVERSE,
    "shortterm": SHORTTERM_UNIVERSE,
    "btst":      BTST_UNIVERSE,
    "intraday":  INTRADAY_UNIVERSE,
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _sentiment_score(symbol: str, signal_time: datetime) -> float:
    with get_session() as session:
        recent = (
            session.query(NewsArticle)
            .filter(
                NewsArticle.symbol == symbol,
                NewsArticle.sentiment_score.isnot(None),
                *known_time_filter(NewsArticle, signal_time),
            )
            .order_by(NewsArticle.published_at.desc())
            .limit(10)
            .all()
        )
    if not recent:
        return 50.0
    avg = sum(r.sentiment_score for r in recent) / len(recent)
    return round((avg + 1.0) / 2.0 * 100.0, 2)


def _llm_accuracy_ok() -> bool:
    """True if LLM directional accuracy is above threshold (last 30 days)."""
    from datetime import timedelta
    with get_session() as session:
        cutoff = (datetime.utcnow() - timedelta(days=30)).date()
        rows = (
            session.query(LLMLog)
            .filter(LLMLog.accuracy.isnot(None), LLMLog.date >= cutoff)
            .all()
        )
    if len(rows) < 10:
        return True
    return (sum(r.accuracy for r in rows) / len(rows)) >= LLM_ACCURACY_THRESHOLD


def _effective_sentiment_weight(base_sent: float) -> float:
    return base_sent if _llm_accuracy_ok() else LLM_ACCURACY_LOW_WEIGHT


def _longterm_fundamental_score(symbol: str, signal_time: datetime) -> float:
    """
    Enhanced fundamental score for long-term engine.
    Combines ratio score with a revenue-growth trend bonus.
    """
    base = ratios_score(symbol, signal_time)

    # Add banking KPI layer for banking stocks
    if symbol in BANKING_STOCKS:
        from analysis.fundamental.banking_metrics import score as banking_score
        bscore = banking_score(symbol, signal_time)
        base = round((base + bscore) / 2.0, 2)

    # Revenue growth bonus (up to +10 pts) — rewards consistent growers
    try:
        from data.storage.database import Fundamental, get_session
        from data.quality.known_time import known_time_filter as ktf
        with get_session() as s:
            rows = (
                s.query(Fundamental)
                .filter(
                    Fundamental.symbol == symbol,
                    Fundamental.revenue.isnot(None),
                    *ktf(Fundamental, signal_time),
                )
                .order_by(Fundamental.period_end_date.desc())
                .limit(5)
                .all()
            )
        if len(rows) >= 3:
            revenues = [r.revenue for r in rows if r.revenue]
            if len(revenues) >= 3 and revenues[-1] > 0:
                growth = (revenues[0] - revenues[-1]) / revenues[-1]
                bonus = min(10.0, max(-10.0, growth * 50))
                base = min(100.0, base + bonus)
    except Exception:
        pass

    return round(base, 2)


# ── Core scorer ────────────────────────────────────────────────────────────────

def score_stock(symbol: str, engine: Engine, signal_time: Optional[datetime] = None) -> dict:
    """Score one stock for the given engine. Returns a breakdown dict."""
    signal_time = signal_time or datetime.now()

    raw_tech, raw_fund, raw_sent = ENGINE_WEIGHTS[engine]
    sent_w = _effective_sentiment_weight(raw_sent)
    total_w = raw_tech + raw_fund + sent_w
    tech_w = raw_tech / total_w
    fund_w = raw_fund / total_w
    sent_w = sent_w / total_w

    t_score = technical_score(symbol, signal_time)
    s_score = _sentiment_score(symbol, signal_time) if raw_sent > 0 else 50.0

    if engine == "longterm":
        f_score = _longterm_fundamental_score(symbol, signal_time)
    elif engine == "intraday":
        f_score = 50.0   # not used; weight is 0
    else:
        if symbol in BANKING_STOCKS:
            from analysis.fundamental.banking_metrics import score as banking_score
            b = banking_score(symbol, signal_time)
            f_score = round((b + ratios_score(symbol, signal_time)) / 2.0, 2)
        else:
            f_score = round(ratios_score(symbol, signal_time), 2)

    total = round(t_score * tech_w + f_score * fund_w + s_score * sent_w, 2)

    return {
        "symbol":            symbol,
        "name":              STOCK_NAMES.get(symbol, symbol),
        "engine":            engine,
        "total_score":       total,
        "technical_score":   round(t_score, 2),
        "fundamental_score": round(f_score, 2),
        "sentiment_score":   round(s_score, 2),
        "weights":           {"technical": round(tech_w, 3), "fundamental": round(fund_w, 3), "sentiment": round(sent_w, 3)},
        "signal_time":       signal_time.isoformat(),
    }


def score_all(engine: Engine, signal_time: Optional[datetime] = None) -> list[dict]:
    """Score every stock in the engine's universe, sorted by total_score desc."""
    signal_time = signal_time or datetime.now()
    universe = _UNIVERSE[engine]
    results = []

    for symbol in universe:
        try:
            results.append(score_stock(symbol, engine, signal_time))
        except Exception as exc:
            logger.error(f"engine_scorer [{engine}]: {symbol} failed — {exc}")
            results.append({
                "symbol": symbol, "name": STOCK_NAMES.get(symbol, symbol),
                "engine": engine, "total_score": 50.0,
                "technical_score": 50.0, "fundamental_score": 50.0,
                "sentiment_score": 50.0, "error": str(exc),
            })

    results.sort(key=lambda r: r["total_score"], reverse=True)
    _print_table(results, engine, signal_time)
    return results


def _print_table(results: list[dict], engine: str, signal_time: datetime) -> None:
    print(f"\n{'═' * 78}")
    print(f"  [{engine.upper()}] Engine Scores  |  {signal_time.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'═' * 78}")
    print(f"  {'#':<3} {'Symbol':<13} {'Name':<26} {'Total':>6}  {'Tech':>5}  {'Fund':>5}  {'Sent':>5}")
    print(f"  {'─' * 72}")
    for i, r in enumerate(results, 1):
        print(
            f"  {i:<3} {r['symbol']:<13} {r.get('name',''):<26} "
            f"{r['total_score']:>6.1f}  "
            f"{r.get('technical_score', 0):>5.1f}  "
            f"{r.get('fundamental_score', 0):>5.1f}  "
            f"{r.get('sentiment_score', 0):>5.1f}"
        )
    print(f"{'═' * 78}\n")
