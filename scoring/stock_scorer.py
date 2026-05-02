"""
Stock scorer — combines Technical, Fundamental, and Sentiment scores.

Weights (from settings.py):
  Technical   50%  — indicator confluence + trend regime
  Fundamental 30%  — banking KPIs (NIM/GNPA/CASA/PCR) + ratios (ROE/ROA/P/B)
  Sentiment   20%  — news sentiment (0 until Phase 4 populates llm_log)

Sentiment weight auto-reduces to 10% if LLM directional accuracy < 55%
(checked against the last 30 days of llm_log). Remaining weight is
redistributed proportionally between technical and fundamental.
"""

from datetime import datetime, timedelta
from typing import Optional

from loguru import logger

from analysis.fundamental.banking_metrics import score as banking_score
from analysis.fundamental.ratios import score as ratios_score
from analysis.technical.signals import score as technical_score
from config.settings import (
    ALL_STOCKS, BANKING_STOCKS, LLM_ACCURACY_LOW_WEIGHT, LLM_ACCURACY_THRESHOLD,
    SCORE_WEIGHT_FUNDAMENTAL, SCORE_WEIGHT_SENTIMENT, SCORE_WEIGHT_TECHNICAL,
    STOCK_NAMES,
)
from data.storage.database import LLMLog, NewsArticle, get_session
from data.quality.known_time import known_time_filter


# ── Sentiment helpers ──────────────────────────────────────────────────────────

def _sentiment_score(symbol: str, signal_time: datetime) -> float:
    """
    Average of the 10 most recent sentiment_score values (scaled -1..+1 → 0..100).
    Returns neutral 50.0 until Phase 4 populates sentiment_score in news_articles.
    """
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
    return round((avg + 1.0) / 2.0 * 100.0, 2)   # -1..+1 → 0..100


def _effective_weights() -> tuple[float, float, float]:
    """
    Return (tech_w, fund_w, sent_w) normalised to sum to 1.0.
    If LLM accuracy over last 30 days < threshold, reduce sentiment weight.
    """
    with get_session() as session:
        cutoff = (datetime.utcnow() - timedelta(days=30)).date()
        rows = (
            session.query(LLMLog)
            .filter(LLMLog.accuracy.isnot(None), LLMLog.date >= cutoff)
            .all()
        )

    if len(rows) >= 10:
        accuracy = sum(r.accuracy for r in rows) / len(rows)
        if accuracy < LLM_ACCURACY_THRESHOLD:
            logger.warning(
                f"LLM accuracy {accuracy:.1%} < {LLM_ACCURACY_THRESHOLD:.0%} "
                f"— reducing sentiment weight from {SCORE_WEIGHT_SENTIMENT:.0%} "
                f"to {LLM_ACCURACY_LOW_WEIGHT:.0%}"
            )
            sent_w = LLM_ACCURACY_LOW_WEIGHT
        else:
            sent_w = SCORE_WEIGHT_SENTIMENT
    else:
        sent_w = SCORE_WEIGHT_SENTIMENT   # not enough data to override

    raw_tech = SCORE_WEIGHT_TECHNICAL
    raw_fund = SCORE_WEIGHT_FUNDAMENTAL
    total    = raw_tech + raw_fund + sent_w

    return raw_tech / total, raw_fund / total, sent_w / total


# ── Per-stock scorer ───────────────────────────────────────────────────────────

def score_stock(symbol: str, signal_time: Optional[datetime] = None) -> dict:
    """
    Compute combined score (0–100) for one stock.
    Returns a dict with total_score and full component breakdown.
    """
    signal_time = signal_time or datetime.now()

    tech_w, fund_w, sent_w = _effective_weights()

    t_score = technical_score(symbol, signal_time)
    r_score = ratios_score(symbol, signal_time)
    s_score = _sentiment_score(symbol, signal_time)

    if symbol in BANKING_STOCKS:
        b_score = banking_score(symbol, signal_time)
        f_score = round((b_score + r_score) / 2.0, 2)
    else:
        # Non-banking stocks have no NIM/GNPA/CASA data — use ratios only
        b_score = None
        f_score = round(r_score, 2)

    total = round(t_score * tech_w + f_score * fund_w + s_score * sent_w, 2)

    return {
        "symbol":              symbol,
        "name":                STOCK_NAMES.get(symbol, symbol),
        "total_score":         total,
        "technical_score":     round(t_score, 2),
        "fundamental_score":   round(f_score, 2),
        "banking_kpi_score":   round(b_score, 2) if b_score is not None else None,
        "ratios_score":        round(r_score, 2),
        "sentiment_score":     round(s_score, 2),
        "weights": {
            "technical":   round(tech_w, 3),
            "fundamental": round(fund_w, 3),
            "sentiment":   round(sent_w, 3),
        },
        "signal_time": signal_time.isoformat(),
    }


# ── Rank all stocks ────────────────────────────────────────────────────────────

def score_all(signal_time: Optional[datetime] = None) -> list[dict]:
    """
    Score all watchlist stocks and return sorted by total_score descending.
    Prints a formatted table to stdout.
    """
    signal_time = signal_time or datetime.now()
    results = []

    for symbol in ALL_STOCKS:
        try:
            results.append(score_stock(symbol, signal_time))
        except Exception as exc:
            logger.error(f"Scoring failed for {symbol}: {exc}")
            results.append({
                "symbol": symbol, "name": STOCK_NAMES.get(symbol, symbol),
                "total_score": 50.0, "technical_score": 50.0,
                "fundamental_score": 50.0, "sentiment_score": 50.0,
                "error": str(exc),
            })

    results.sort(key=lambda r: r["total_score"], reverse=True)
    _print_table(results, signal_time)
    return results


def _print_table(results: list[dict], signal_time: datetime) -> None:
    """Print a formatted ranking table to stdout."""
    print(f"\n{'═' * 78}")
    print(f"  NSE Banking Sector — Stock Scores  |  {signal_time.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'═' * 78}")
    print(f"  {'#':<3} {'Symbol':<12} {'Name':<26} {'Total':>6}  {'Tech':>5}  {'Fund':>5}  {'Sent':>5}")
    print(f"  {'─' * 72}")
    for i, r in enumerate(results, 1):
        print(
            f"  {i:<3} {r['symbol']:<12} {r.get('name',''):<26} "
            f"{r['total_score']:>6.1f}  "
            f"{r.get('technical_score', 0):>5.1f}  "
            f"{r.get('fundamental_score', 0):>5.1f}  "
            f"{r.get('sentiment_score', 0):>5.1f}"
        )
    print(f"{'═' * 78}\n")
