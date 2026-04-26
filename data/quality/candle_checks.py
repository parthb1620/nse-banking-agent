"""
Data quality checks — run after every collection job.

Checks per stock:
  1. Missing candles  — trading days with no OHLCV row
  2. Duplicate candles — (symbol, date) pairs with more than one row
  3. Volume sanity    — zero volume, extreme spikes, OHLC rule violations
  4. Price integrity  — low > high, open outside high/low, >20% overnight gap

Results are stored in data_quality_log. Stocks with quality_score < 0.95 are
flagged for exclusion from analysis until the issue is resolved.
"""

import json
from datetime import date, datetime

from loguru import logger
from sqlalchemy import func, text

from config.nse_calendar import trading_days_between
from config.settings import BANKING_STOCKS
from data.storage.database import DataQualityLog, OHLCVDaily, get_session

_VOLUME_SPIKE_MULTIPLIER = 10   # flag if volume > 10× 20-day avg
_MAX_PRICE_CHANGE_PCT    = 0.20  # flag if overnight change > 20%
_MIN_QUALITY_SCORE       = 0.95  # stocks below this are excluded from analysis


def check_missing_candles(symbol: str, start: date, end: date, session) -> list[str]:
    """Return list of missing trading day strings for this symbol."""
    expected = set(trading_days_between(start, end))
    present  = {
        r[0] for r in session.query(OHLCVDaily.date)
        .filter(OHLCVDaily.symbol == symbol)
        .all()
    }
    missing = sorted(expected - present)
    return [str(d) for d in missing]


def check_duplicates(symbol: str, session) -> int:
    """
    Find and resolve duplicate (symbol, date) rows.
    Resolution: keep nse_bhavcopy > groww > yfinance. Delete the rest.
    Returns count of duplicates removed.
    """
    source_priority = {"nse_bhavcopy": 0, "groww": 1, "yfinance": 2}

    dupes = (
        session.query(OHLCVDaily.date, func.count(OHLCVDaily.id).label("cnt"))
        .filter(OHLCVDaily.symbol == symbol)
        .group_by(OHLCVDaily.date)
        .having(func.count(OHLCVDaily.id) > 1)
        .all()
    )
    removed = 0
    for dupe_date, _ in dupes:
        rows = (
            session.query(OHLCVDaily)
            .filter(OHLCVDaily.symbol == symbol, OHLCVDaily.date == dupe_date)
            .all()
        )
        rows.sort(key=lambda r: source_priority.get(r.source or "yfinance", 99))
        for row in rows[1:]:   # keep the first (highest-priority source), delete the rest
            session.delete(row)
            removed += 1
    session.commit()
    return removed


def check_volume_sanity(symbol: str, session) -> list[str]:
    """
    Return list of issue descriptions for volume and OHLC integrity violations.
    Does NOT delete anything — just flags for review.
    """
    issues = []
    rows = (
        session.query(OHLCVDaily)
        .filter(OHLCVDaily.symbol == symbol)
        .order_by(OHLCVDaily.date)
        .all()
    )
    if not rows:
        return issues

    # Build 20-day rolling volume average for spike detection
    volumes = [r.volume or 0 for r in rows]
    prev_close = None

    for i, row in enumerate(rows):
        d = str(row.date)
        o, h, l, c, v = row.open, row.high, row.low, row.close, row.volume

        # OHLC integrity
        if None not in (h, l) and l > h:
            issues.append(f"{d}: low ({l}) > high ({h})")
        if None not in (o, h) and o > h:
            issues.append(f"{d}: open ({o}) > high ({h})")
        if None not in (o, l) and o < l:
            issues.append(f"{d}: open ({o}) < low ({l})")
        if None not in (c, h) and c > h:
            issues.append(f"{d}: close ({c}) > high ({h})")
        if None not in (c, l) and c < l:
            issues.append(f"{d}: close ({c}) < low ({l})")

        # Volume checks
        if v == 0:
            issues.append(f"{d}: volume = 0 (suspicious if not suspended)")
        elif v is not None and i >= 20:
            avg_20 = sum(volumes[i - 20:i]) / 20
            if avg_20 > 0 and v > _VOLUME_SPIKE_MULTIPLIER * avg_20:
                issues.append(f"{d}: volume spike {v:,} > {_VOLUME_SPIKE_MULTIPLIER}× 20-day avg {avg_20:,.0f}")

        # Overnight price gap
        if prev_close is not None and c is not None and prev_close > 0:
            change = abs(c - prev_close) / prev_close
            if change > _MAX_PRICE_CHANGE_PCT:
                issues.append(f"{d}: price change {change:.1%} > {_MAX_PRICE_CHANGE_PCT:.0%} (check for corporate action)")

        if c is not None:
            prev_close = c

    return issues


def run_quality_checks(symbol: str, check_date: date | None = None) -> float:
    """
    Run all quality checks for one stock, store results in data_quality_log.
    Returns quality_score (0.0–1.0). Score < 0.95 means stock needs attention.
    """
    check_date = check_date or date.today()

    from datetime import timedelta
    from config.settings import DATA_BACKFILL_YEARS

    end   = check_date
    start = date(end.year - DATA_BACKFILL_YEARS, end.month, end.day)

    with get_session() as session:
        dupes_removed = check_duplicates(symbol, session)

        present_count = session.query(func.count(OHLCVDaily.id)).filter(
            OHLCVDaily.symbol == symbol
        ).scalar() or 0

        missing_dates  = check_missing_candles(symbol, start, end, session)
        expected_count = present_count + len(missing_dates)
        vol_issues     = check_volume_sanity(symbol, session)

        quality_score = present_count / expected_count if expected_count > 0 else 0.0
        flagged_count = len(vol_issues)

        all_issues = (
            [f"missing: {d}" for d in missing_dates] +
            vol_issues +
            ([f"removed {dupes_removed} duplicates"] if dupes_removed else [])
        )

        # Upsert quality log
        existing = session.query(DataQualityLog).filter_by(
            symbol=symbol, check_date=check_date
        ).first()

        if existing:
            existing.expected_candles = expected_count
            existing.present_candles  = present_count
            existing.missing_candles  = len(missing_dates)
            existing.flagged_candles  = flagged_count
            existing.quality_score    = quality_score
            existing.issues_json      = json.dumps(all_issues[:50])   # cap at 50 items
            existing.checked_at       = datetime.utcnow()
        else:
            session.add(DataQualityLog(
                symbol=symbol,
                check_date=check_date,
                expected_candles=expected_count,
                present_candles=present_count,
                missing_candles=len(missing_dates),
                flagged_candles=flagged_count,
                quality_score=quality_score,
                issues_json=json.dumps(all_issues[:50]),
                checked_at=datetime.utcnow(),
            ))

        session.commit()

    level = "info" if quality_score >= _MIN_QUALITY_SCORE else "warning"
    getattr(logger, level)(
        f"Quality check {symbol}: score={quality_score:.1%}  "
        f"present={present_count}  missing={len(missing_dates)}  flagged={flagged_count}"
    )
    return quality_score


def run_all(check_date: date | None = None) -> dict[str, float]:
    """Run quality checks for all tracked stocks. Returns {symbol: score}."""
    return {s: run_quality_checks(s, check_date) for s in BANKING_STOCKS}
