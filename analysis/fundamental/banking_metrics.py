"""
Banking-sector KPI extractor and scorer.

Phase 1 role: reads stored fundamentals + filings and populates the banking_metrics table
              (NIM, GNPA, CASA, PCR, ROE, ROA, CAR, credit_growth, slippage_ratio).

Phase 2 role: score() method returns a 0–100 fundamental score for use in stock_scorer.

Good levels (private banks):
  NIM > 3.5%  |  GNPA < 2%  |  CASA > 40%  |  PCR > 70%
  ROA > 1.5%  |  ROE > 15%  |  CAR > 11.5% |  Credit growth YoY > 15%
"""

from datetime import datetime

from loguru import logger

from config.settings import BANKING_STOCKS
from data.storage.database import BankingMetric, Fundamental, get_session
from data.quality.known_time import compute_usable_from


# ── Scoring thresholds ──────────────────────────────────────────────────────────
_THRESHOLDS = {
    "nim":           {"good": 3.5,  "bad": 2.5,  "higher_is_better": True},
    "gnpa":          {"good": 2.0,  "bad": 5.0,  "higher_is_better": False},
    "casa":          {"good": 40.0, "bad": 25.0, "higher_is_better": True},
    "pcr":           {"good": 70.0, "bad": 50.0, "higher_is_better": True},
    "roa":           {"good": 1.5,  "bad": 0.5,  "higher_is_better": True},
    "roe":           {"good": 15.0, "bad": 8.0,  "higher_is_better": True},
    "car":           {"good": 16.0, "bad": 11.5, "higher_is_better": True},
    "credit_growth": {"good": 15.0, "bad": 5.0,  "higher_is_better": True},
}


def _score_metric(value: float | None, key: str) -> float:
    """Scale a single metric to 0–100 based on thresholds."""
    if value is None:
        return 50.0   # neutral when data is missing

    t = _THRESHOLDS.get(key)
    if not t:
        return 50.0

    good, bad, higher = t["good"], t["bad"], t["higher_is_better"]

    if higher:
        if value >= good:
            return 100.0
        if value <= bad:
            return 0.0
        return (value - bad) / (good - bad) * 100.0
    else:   # lower is better (e.g. GNPA)
        if value <= good:
            return 100.0
        if value >= bad:
            return 0.0
        return (bad - value) / (bad - good) * 100.0


def score(symbol: str, signal_time: datetime) -> float:
    """
    Return a 0–100 fundamental score for one stock.
    Uses only data with usable_from <= signal_time (known-time rule).
    """
    from data.quality.known_time import known_time_filter

    with get_session() as session:
        metric = (
            session.query(BankingMetric)
            .filter(
                BankingMetric.symbol == symbol,
                *known_time_filter(BankingMetric, signal_time),
            )
            .order_by(BankingMetric.period_end_date.desc())
            .first()
        )

    if not metric:
        logger.warning(f"No banking metrics found for {symbol} at {signal_time}")
        return 50.0

    component_scores = []
    for k in _THRESHOLDS:
        val = getattr(metric, k)
        if val is not None:
            component_scores.append(_score_metric(val, k))

    if not component_scores:
        return 50.0

    return round(sum(component_scores) / len(component_scores), 2)


def populate_from_fundamentals(symbol: str) -> int:
    """
    Read stored Fundamental rows and attempt to extract / store BankingMetric rows.
    Banking-specific numbers (NIM, GNPA, CASA, PCR) are sourced from NSE filings
    and populated separately. This function handles derivable metrics (ROE, ROA).

    Returns number of rows inserted.
    """
    now      = datetime.utcnow()
    inserted = 0

    with get_session() as session:
        fundamentals = (
            session.query(Fundamental)
            .filter(Fundamental.symbol == symbol)
            .order_by(Fundamental.period_end_date.desc())
            .all()
        )

        for f in fundamentals:
            exists = session.query(BankingMetric).filter_by(
                symbol=symbol, period_end_date=f.period_end_date
            ).first()

            if exists:
                # Update derivable fields if not already set
                changed = False
                annualized_pat = (f.pat * 4) if (f.pat and f.period_type == "Q") else f.pat
                if exists.roe is None and annualized_pat and f.total_equity and f.total_equity > 0:
                    exists.roe = round(annualized_pat / f.total_equity * 100, 2)
                    changed = True
                if exists.roa is None and annualized_pat and f.total_assets and f.total_assets > 0:
                    exists.roa = round(annualized_pat / f.total_assets * 100, 2)
                    changed = True
                if changed:
                    session.commit()
                continue

            # Annualize quarterly PAT (×4) before dividing by annual balance sheet totals
            annualized_pat = (f.pat * 4) if (f.pat and f.period_type == "Q") else f.pat
            roe = round(annualized_pat / f.total_equity * 100, 2) if (annualized_pat and f.total_equity and f.total_equity > 0) else None
            roa = round(annualized_pat / f.total_assets * 100, 2) if (annualized_pat and f.total_assets and f.total_assets > 0) else None

            usable_from = f.usable_from or compute_usable_from(
                f.announced_at or datetime.combine(f.period_end_date, datetime.min.time())
            )

            session.add(BankingMetric(
                symbol=symbol,
                period_end_date=f.period_end_date,
                roe=roe,
                roa=roa,
                announced_at=f.announced_at,
                published_at=f.published_at,
                collected_at=now,
                usable_from=usable_from,
            ))
            inserted += 1

        session.commit()

    if inserted:
        logger.info(f"Banking metrics: inserted {inserted} rows for {symbol}")
    return inserted


def run_all() -> None:
    """Populate banking metrics for all tracked stocks from stored fundamentals."""
    for symbol in BANKING_STOCKS:
        populate_from_fundamentals(symbol)
