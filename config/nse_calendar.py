"""
NSE trading calendar — holiday list and trading-day utilities.

The holiday list is hard-coded from official NSE circulars.
2026 dates are approximate (NSE publishes the annual list in Nov of the prior year).
Override by placing a JSON file at data_store/nse_holidays_override.json:
  {"extra": ["2026-03-21"], "remove": ["2026-03-20"]}
"""

import json
from datetime import date, datetime, timedelta
from pathlib import Path

# fmt: off
_NSE_HOLIDAYS: set[date] = {
    # ── 2020 ──────────────────────────────────────────────────────────────────
    date(2020, 2, 21),   # Mahashivratri
    date(2020, 3, 10),   # Holi
    date(2020, 4, 2),    # Ram Navami
    date(2020, 4, 6),    # Mahavir Jayanti
    date(2020, 4, 10),   # Good Friday
    date(2020, 4, 14),   # Dr. Ambedkar Jayanti
    date(2020, 5, 1),    # Maharashtra Day
    date(2020, 10, 2),   # Gandhi Jayanti
    date(2020, 11, 16),  # Diwali Laxmi Puja
    date(2020, 11, 30),  # Gurunanak Jayanti
    date(2020, 12, 25),  # Christmas
    # ── 2021 ──────────────────────────────────────────────────────────────────
    date(2021, 1, 26),   # Republic Day
    date(2021, 3, 11),   # Mahashivratri
    date(2021, 3, 29),   # Holi
    date(2021, 4, 2),    # Good Friday
    date(2021, 4, 14),   # Dr. Ambedkar Jayanti
    date(2021, 4, 21),   # Ram Navami
    date(2021, 5, 13),   # Eid ul Fitr
    date(2021, 7, 21),   # Bakri Eid
    date(2021, 9, 10),   # Ganesh Chaturthi
    date(2021, 10, 15),  # Dussehra
    date(2021, 11, 4),   # Diwali Laxmi Puja
    date(2021, 11, 5),   # Diwali Balipratipada
    date(2021, 11, 19),  # Gurunanak Jayanti
    date(2021, 12, 25),  # Christmas
    # ── 2022 ──────────────────────────────────────────────────────────────────
    date(2022, 1, 26),   # Republic Day
    date(2022, 3, 1),    # Mahashivratri
    date(2022, 3, 18),   # Holi
    date(2022, 4, 14),   # Dr. Ambedkar Jayanti
    date(2022, 4, 15),   # Good Friday
    date(2022, 5, 3),    # Eid ul Fitr
    date(2022, 8, 9),    # Muharram
    date(2022, 8, 15),   # Independence Day
    date(2022, 8, 31),   # Ganesh Chaturthi
    date(2022, 10, 2),   # Gandhi Jayanti
    date(2022, 10, 5),   # Dussehra
    date(2022, 10, 24),  # Diwali Laxmi Puja
    date(2022, 10, 26),  # Diwali Balipratipada
    date(2022, 11, 8),   # Gurunanak Jayanti
    date(2022, 12, 25),  # Christmas
    # ── 2023 ──────────────────────────────────────────────────────────────────
    date(2023, 1, 26),   # Republic Day
    date(2023, 3, 7),    # Holi
    date(2023, 3, 30),   # Ram Navami
    date(2023, 4, 4),    # Mahavir Jayanti
    date(2023, 4, 7),    # Good Friday
    date(2023, 4, 14),   # Dr. Ambedkar Jayanti
    date(2023, 5, 1),    # Maharashtra Day
    date(2023, 6, 28),   # Bakri Eid
    date(2023, 8, 15),   # Independence Day
    date(2023, 9, 19),   # Ganesh Chaturthi
    date(2023, 10, 2),   # Gandhi Jayanti
    date(2023, 10, 24),  # Dussehra
    date(2023, 11, 13),  # Diwali Laxmi Puja
    date(2023, 11, 14),  # Diwali Balipratipada
    date(2023, 11, 27),  # Gurunanak Jayanti
    date(2023, 12, 25),  # Christmas
    # ── 2024 ──────────────────────────────────────────────────────────────────
    date(2024, 1, 22),   # Ram Mandir Consecration (special NSE circular)
    date(2024, 1, 26),   # Republic Day
    date(2024, 3, 25),   # Holi
    date(2024, 3, 29),   # Good Friday
    date(2024, 4, 11),   # Eid ul Fitr
    date(2024, 4, 14),   # Dr. Ambedkar Jayanti
    date(2024, 4, 17),   # Ram Navami
    date(2024, 4, 21),   # Mahavir Jayanti
    date(2024, 5, 1),    # Maharashtra Day
    date(2024, 5, 23),   # Buddha Pournima
    date(2024, 6, 17),   # Eid ul Adha (Bakri Eid)
    date(2024, 7, 17),   # Muharram
    date(2024, 8, 15),   # Independence Day
    date(2024, 10, 2),   # Gandhi Jayanti
    date(2024, 11, 1),   # Diwali Laxmi Puja
    date(2024, 11, 15),  # Gurunanak Jayanti
    date(2024, 12, 25),  # Christmas
    # ── 2025 ──────────────────────────────────────────────────────────────────
    date(2025, 2, 26),   # Mahashivratri
    date(2025, 3, 14),   # Holi
    date(2025, 3, 31),   # Eid ul Fitr
    date(2025, 4, 10),   # Ram Navami
    date(2025, 4, 14),   # Dr. Ambedkar Jayanti
    date(2025, 4, 18),   # Good Friday
    date(2025, 5, 1),    # Maharashtra Day
    date(2025, 8, 15),   # Independence Day
    date(2025, 8, 27),   # Ganesh Chaturthi
    date(2025, 10, 2),   # Gandhi Jayanti
    date(2025, 10, 20),  # Diwali Laxmi Puja
    date(2025, 10, 21),  # Diwali Balipratipada
    date(2025, 11, 5),   # Gurunanak Jayanti
    date(2025, 12, 25),  # Christmas
    # ── 2026 (approximate — verify against NSE circular when published) ───────
    date(2026, 1, 26),   # Republic Day
    date(2026, 3, 20),   # Holi (approx)
    date(2026, 4, 3),    # Good Friday (approx)
    date(2026, 4, 14),   # Dr. Ambedkar Jayanti
    date(2026, 5, 1),    # Maharashtra Day
    date(2026, 8, 15),   # Independence Day
    date(2026, 10, 2),   # Gandhi Jayanti
    date(2026, 12, 25),  # Christmas
}
# fmt: on

# Apply JSON overrides if the user has placed one in data_store/
_OVERRIDE_FILE = Path(__file__).resolve().parent.parent / "data_store" / "nse_holidays_override.json"
if _OVERRIDE_FILE.exists():
    _overrides = json.loads(_OVERRIDE_FILE.read_text())
    for s in _overrides.get("extra", []):
        _NSE_HOLIDAYS.add(date.fromisoformat(s))
    for s in _overrides.get("remove", []):
        _NSE_HOLIDAYS.discard(date.fromisoformat(s))


def is_trading_day(d: date | datetime) -> bool:
    if isinstance(d, datetime):
        d = d.date()
    return d.weekday() < 5 and d not in _NSE_HOLIDAYS


def next_trading_day(d: date | datetime) -> date:
    """First trading day strictly after d."""
    if isinstance(d, datetime):
        d = d.date()
    nxt = d + timedelta(days=1)
    while not is_trading_day(nxt):
        nxt += timedelta(days=1)
    return nxt


def prev_trading_day(d: date | datetime) -> date:
    """First trading day strictly before d."""
    if isinstance(d, datetime):
        d = d.date()
    prv = d - timedelta(days=1)
    while not is_trading_day(prv):
        prv -= timedelta(days=1)
    return prv


def trading_days_between(start: date, end: date) -> list[date]:
    """All trading days in [start, end] inclusive."""
    days, d = [], start
    while d <= end:
        if is_trading_day(d):
            days.append(d)
        d += timedelta(days=1)
    return days
