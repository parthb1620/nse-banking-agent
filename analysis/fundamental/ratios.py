"""
Fundamental ratios computed from stored Fundamental data.

All queries respect the known-time rule: usable_from <= signal_time.

Ratios computed:
  ROE   = PAT / Total Equity × 100   (good > 15%)
  ROA   = PAT / Total Assets × 100   (good > 1.5% for banks)
  P/B   = Market Price / Book Value per Share  (good < 3 for banks)
  D/E   = Total Debt / Total Equity   (skip for banks — leverage is structural)

Note: For banking stocks, NIM/GNPA/PCR/CASA in banking_metrics.py are more
      meaningful than generic corporate ratios. Ratios here are supplementary.
"""

from datetime import datetime
from typing import Optional

from loguru import logger

from config.settings import ALL_STOCKS
from data.quality.known_time import known_time_filter
from data.storage.database import Fundamental, OHLCVDaily, get_session


def compute_ratios(symbol: str, signal_time: datetime) -> dict:
    """
    Return fundamental ratios for one stock.
    Only uses data with usable_from <= signal_time (no lookahead).
    Returns empty dict if no fundamental data available.
    """
    with get_session() as session:
        fund = (
            session.query(Fundamental)
            .filter(
                Fundamental.symbol == symbol,
                *known_time_filter(Fundamental, signal_time),
            )
            .order_by(Fundamental.period_end_date.desc())
            .first()
        )
        if not fund:
            return {}

        ratios: dict = {"period_end_date": str(fund.period_end_date)}

        annualized_pat = (fund.pat * 4) if (fund.pat and fund.period_type == "Q") else fund.pat

        if annualized_pat and fund.total_equity and fund.total_equity > 0:
            ratios["roe"] = round(annualized_pat / fund.total_equity * 100, 2)

        if annualized_pat and fund.total_assets and fund.total_assets > 0:
            ratios["roa"] = round(annualized_pat / fund.total_assets * 100, 2)

        if fund.total_debt is not None and fund.total_equity and fund.total_equity > 0:
            ratios["debt_equity"] = round(fund.total_debt / fund.total_equity, 2)

        if fund.book_value_per_share and fund.book_value_per_share > 0:
            # Get latest price for P/B calculation
            price_row = (
                session.query(OHLCVDaily)
                .filter(
                    OHLCVDaily.symbol == symbol,
                    OHLCVDaily.date <= signal_time.date(),
                )
                .order_by(OHLCVDaily.date.desc())
                .first()
            )
            if price_row and price_row.adjusted_close:
                ratios["pb_ratio"] = round(price_row.adjusted_close / fund.book_value_per_share, 2)

        if fund.eps:
            ratios["eps"] = fund.eps

    return ratios


def score(symbol: str, signal_time: datetime) -> float:
    """
    Return a 0–100 ratios score for use in stock_scorer.
    Based on ROE, ROA, P/B — higher ROE/ROA is better, moderate P/B is better.
    Returns neutral 50.0 if no data.
    """
    ratios = compute_ratios(symbol, signal_time)
    if not ratios:
        return 50.0

    component_scores = []

    # ROE: good ≥ 15%, bad ≤ 8%
    if "roe" in ratios:
        roe = ratios["roe"]
        s = min(100.0, max(0.0, (roe - 8.0) / (15.0 - 8.0) * 100.0))
        component_scores.append(s)

    # ROA: good ≥ 1.5%, bad ≤ 0.5%
    if "roa" in ratios:
        roa = ratios["roa"]
        s = min(100.0, max(0.0, (roa - 0.5) / (1.5 - 0.5) * 100.0))
        component_scores.append(s)

    # P/B: good ≤ 1.5, bad ≥ 5.0 (for banks — lower is cheaper relative to book)
    if "pb_ratio" in ratios:
        pb = ratios["pb_ratio"]
        if pb <= 1.5:
            s = 100.0
        elif pb >= 5.0:
            s = 0.0
        else:
            s = (5.0 - pb) / (5.0 - 1.5) * 100.0
        component_scores.append(s)

    if not component_scores:
        return 50.0

    return round(sum(component_scores) / len(component_scores), 2)


def score_all(signal_time: Optional[datetime] = None) -> dict[str, float]:
    """Return {symbol: score} for all tracked stocks."""
    signal_time = signal_time or datetime.now()
    return {s: score(s, signal_time) for s in ALL_STOCKS}
