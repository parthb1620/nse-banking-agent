"""
FII/DII daily equity flow collector — NSE India.

Source: NSE API  https://www.nseindia.com/api/fiidiiTradeReact
Data:   Equity cash market net buy/sell for FII and DII.

Storage: data_store/fii_dii.csv  (date, fii_net_cr, dii_net_cr)
  fii_net_cr > 0 → FII net buyers (bullish)
  fii_net_cr < 0 → FII net sellers (bearish)

Public API:
  run_daily()                     → fetch today's data and append to CSV
  is_fii_selling_streak(days=3)  → True if FII net < 0 for last N trading days
  load_recent(days=10)           → DataFrame with last N entries
  get_status()                   → dict for morning Telegram alert
"""

import csv
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from loguru import logger

_DATA_FILE = Path(__file__).resolve().parent.parent.parent / "data_store" / "fii_dii.csv"
_CSV_HEADERS = ["date", "fii_buy_cr", "fii_sell_cr", "fii_net_cr", "dii_buy_cr", "dii_sell_cr", "dii_net_cr"]

_NSE_FII_URL   = "https://www.nseindia.com/api/fiidiiTradeReact"
_NSE_HOME_URL  = "https://www.nseindia.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/",
}

_session: requests.Session | None = None


# ── Session ────────────────────────────────────────────────────────────────────

def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(_HEADERS)
        try:
            _session.get(_NSE_HOME_URL, timeout=15)
            time.sleep(1)   # NSE detects bots that skip the wait
        except Exception as exc:
            logger.warning(f"FII/DII: could not prime NSE session — {exc}")
    return _session


# ── Fetch ──────────────────────────────────────────────────────────────────────

def fetch_today() -> dict | None:
    """
    Fetch today's FII/DII equity flow from NSE.
    Returns {"date", "fii_net_cr", "dii_net_cr", ...} or None on failure.
    """
    try:
        sess = _get_session()
        resp = sess.get(_NSE_FII_URL, timeout=15)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as exc:
        logger.error(f"FII/DII: fetch failed — {exc}")
        return None

    # NSE returns a list; find FII and DII rows
    fii = next((r for r in raw if "fii" in r.get("category", "").lower()), None)
    dii = next((r for r in raw if "dii" in r.get("category", "").lower()), None)

    if not fii:
        logger.warning(f"FII/DII: unexpected response format — {str(raw)[:200]}")
        return None

    def _parse(val) -> float:
        try:
            return float(str(val).replace(",", ""))
        except Exception:
            return 0.0

    today_str = fii.get("date", str(date.today()))
    try:
        parsed_date = datetime.strptime(today_str, "%d-%b-%Y").date()
    except ValueError:
        parsed_date = date.today()

    return {
        "date":        parsed_date,
        "fii_buy_cr":  _parse(fii.get("buyValue",  0)),
        "fii_sell_cr": _parse(fii.get("sellValue", 0)),
        "fii_net_cr":  _parse(fii.get("netValue",  0)),
        "dii_buy_cr":  _parse(dii.get("buyValue",  0)) if dii else 0.0,
        "dii_sell_cr": _parse(dii.get("sellValue", 0)) if dii else 0.0,
        "dii_net_cr":  _parse(dii.get("netValue",  0)) if dii else 0.0,
    }


# ── Storage ────────────────────────────────────────────────────────────────────

def _load_csv() -> list[dict]:
    if not _DATA_FILE.exists():
        return []
    with open(_DATA_FILE, newline="") as f:
        return list(csv.DictReader(f))


def _save_csv(rows: list[dict]) -> None:
    _DATA_FILE.parent.mkdir(exist_ok=True)
    with open(_DATA_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def store(data: dict) -> bool:
    """Append or update today's FII/DII row in the CSV."""
    rows = _load_csv()
    date_str = str(data["date"])

    # Replace existing row for this date, or append
    rows = [r for r in rows if r["date"] != date_str]
    row = {k: data.get(k, 0) for k in _CSV_HEADERS}
    row["date"] = date_str   # ensure it's a string, not datetime.date
    rows.append(row)
    rows.sort(key=lambda r: str(r["date"]))

    _save_csv(rows)
    logger.info(
        f"FII/DII stored: date={date_str}  "
        f"FII net={data['fii_net_cr']:+,.0f} Cr  "
        f"DII net={data['dii_net_cr']:+,.0f} Cr"
    )
    return True


# ── Query helpers ──────────────────────────────────────────────────────────────

def load_recent(days: int = 10) -> list[dict]:
    """Return up to `days` most recent FII/DII rows, newest first."""
    rows = _load_csv()
    rows.sort(key=lambda r: r["date"], reverse=True)
    return rows[:days]


def is_fii_selling_streak(days: int = 3) -> bool:
    """
    Return True if FII has been a net SELLER for the last `days` trading days.
    Returns False if insufficient data (gives benefit of the doubt).
    """
    recent = load_recent(days)
    if len(recent) < days:
        return False   # not enough data — don't block trading
    try:
        return all(float(r["fii_net_cr"]) < 0 for r in recent[:days])
    except Exception:
        return False


def get_status() -> dict:
    """Return FII/DII status dict for Telegram / dashboard."""
    recent = load_recent(5)
    if not recent:
        return {"available": False}

    latest = recent[0]
    streak = sum(1 for r in recent if float(r.get("fii_net_cr", 0)) < 0)
    consecutive_sell = is_fii_selling_streak(3)

    return {
        "available":        True,
        "date":             latest["date"],
        "fii_net_cr":       float(latest.get("fii_net_cr", 0)),
        "dii_net_cr":       float(latest.get("dii_net_cr", 0)),
        "sell_streak_days": streak,
        "blocking_entries": consecutive_sell,
    }


# ── Entry point ────────────────────────────────────────────────────────────────

def run_daily() -> bool:
    """Fetch today's FII/DII data and store it. Returns True on success."""
    data = fetch_today()
    if data is None:
        logger.warning("FII/DII: no data fetched — skipping storage")
        return False
    return store(data)


if __name__ == "__main__":
    run_daily()
    status = get_status()
    print(f"\nFII net: {status.get('fii_net_cr', 'n/a'):+,.0f} Cr")
    print(f"DII net: {status.get('dii_net_cr', 'n/a'):+,.0f} Cr")
    print(f"FII selling streak: {status.get('sell_streak_days', 0)} days")
    print(f"Blocking new entries: {status.get('blocking_entries', False)}")
