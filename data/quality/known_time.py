"""
Known-time enforcement — prevents lookahead bias in all analysis queries.

Rule: no signal may reference any data row where usable_from > signal_time.

Usage in every query that touches event tables:

    from data.quality.known_time import known_time_filter
    rows = session.query(Fundamental).filter(
        Fundamental.symbol == symbol,
        *known_time_filter(Fundamental, signal_time),
    ).all()
"""

from datetime import datetime, time
from zoneinfo import ZoneInfo

from config.nse_calendar import next_trading_day

_IST = ZoneInfo("Asia/Kolkata")
_MARKET_OPEN = time(9, 15)


def compute_usable_from(announced_at: datetime) -> datetime:
    """
    Given when an event was announced, return the datetime from which it is safe
    to use in signals: 09:15 IST on the next trading day after announcement.

    Example:
        Q4 result announced at 18:30 IST on 2025-04-25
        → usable_from = 2025-04-28 09:15:00 IST  (next trading day open)
    """
    if announced_at.tzinfo is None:
        announced_at = announced_at.replace(tzinfo=_IST)
    next_day = next_trading_day(announced_at.date())
    return datetime(next_day.year, next_day.month, next_day.day, 9, 15, 0, tzinfo=_IST)


def known_time_filter(model_class, signal_time: datetime):
    """
    Return a tuple of SQLAlchemy filter expressions that enforce the known-time rule.
    Rows where usable_from is NULL are treated as always usable (e.g. legacy data
    loaded without a known announcement time).

    Usage:
        session.query(Model).filter(*known_time_filter(Model, signal_time))
    """
    col = model_class.usable_from
    return (
        (col == None) | (col <= signal_time),   # noqa: E711  (SQLAlchemy requires == None)
    )


def assert_no_lookahead(rows: list, signal_time: datetime, label: str = "") -> None:
    """
    Raise AssertionError if any row has usable_from > signal_time.
    Call this in tests to verify no lookahead bias crept in.
    """
    for row in rows:
        uf = getattr(row, "usable_from", None)
        if uf is not None and uf > signal_time:
            raise AssertionError(
                f"Lookahead bias detected{' in ' + label if label else ''}: "
                f"row usable_from={uf} > signal_time={signal_time}"
            )
