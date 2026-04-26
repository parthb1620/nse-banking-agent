"""
Corporate action fetcher and backward price adjuster.

Fetches splits, bonus issues, and dividends from NSE.
Adjusts all historical OHLCV rows before ex_date using backward adjustment factors.

Adjustment rules:
  Split 1:N  → multiply historical prices by 1/N, multiply volume by N
  Bonus 1:N  → multiply historical prices by N/(N+1), multiply volume by (N+1)/N
  Dividend D → subtract D from all historical closes before ex_date (cash adjustment)

Adjusted close is stored in ohlcv_daily.adjusted_close.
Raw close is preserved in ohlcv_daily.close.
"""

from datetime import date, datetime

import requests
from loguru import logger
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import BANKING_STOCKS
from data.quality.known_time import compute_usable_from
from data.storage.database import CorporateAction, get_session

_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nseindia.com/",
    "Accept":  "application/json",
}

_NSE_CORP_ACTIONS_URL = (
    "https://www.nseindia.com/api/corporatecAnnouncement-latest"
    "?index=equities&symbol={symbol}"
)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _fetch_from_nse(symbol: str) -> list[dict]:
    """Fetch corporate actions JSON from NSE API."""
    session = requests.Session()
    session.headers.update(_NSE_HEADERS)
    # Prime cookies
    session.get("https://www.nseindia.com", timeout=15)

    url = _NSE_CORP_ACTIONS_URL.format(symbol=symbol)
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json() if isinstance(resp.json(), list) else resp.json().get("data", [])


def fetch_and_store_actions(symbol: str) -> int:
    """
    Fetch corporate actions from NSE and store new ones in corporate_actions table.
    Returns number of new rows inserted.
    """
    try:
        raw = _fetch_from_nse(symbol)
    except Exception as exc:
        logger.error(f"Corporate actions fetch failed for {symbol}: {exc}")
        return 0

    now      = datetime.utcnow()
    inserted = 0

    with get_session() as session:
        for item in raw:
            purpose = (item.get("purpose") or item.get("subject") or "").lower()

            # Classify action type
            if "split" in purpose:
                action_type = "split"
            elif "bonus" in purpose:
                action_type = "bonus"
            elif "dividend" in purpose or "div" in purpose:
                action_type = "dividend"
            else:
                continue   # ignore AGM, rights, etc. for now

            ex_date_str = item.get("exDate") or item.get("ex_date") or item.get("exdate")
            if not ex_date_str:
                continue

            try:
                ex_date = datetime.strptime(ex_date_str.strip(), "%d-%b-%Y").date()
            except ValueError:
                try:
                    ex_date = date.fromisoformat(ex_date_str.strip())
                except ValueError:
                    logger.warning(f"Cannot parse ex_date '{ex_date_str}' for {symbol}")
                    continue

            # Parse ratio/amount from purpose string (best-effort)
            ratio, amount = _parse_ratio(purpose, action_type)

            announced_str = item.get("broadcastDateTime") or item.get("bcstDateTime")
            announced_at  = None
            if announced_str:
                try:
                    announced_at = datetime.fromisoformat(announced_str.replace("Z", "+00:00"))
                except ValueError:
                    pass

            usable_from = compute_usable_from(announced_at or datetime.combine(ex_date, datetime.min.time()))

            exists = session.query(CorporateAction).filter_by(
                symbol=symbol, ex_date=ex_date, action_type=action_type
            ).first()
            if exists:
                continue

            session.add(CorporateAction(
                symbol=symbol,
                ex_date=ex_date,
                action_type=action_type,
                ratio=ratio,
                amount=amount,
                announced_at=announced_at,
                published_at=announced_at,
                collected_at=now,
                usable_from=usable_from,
            ))
            inserted += 1

        session.commit()

    if inserted:
        logger.info(f"Corporate actions: inserted {inserted} new records for {symbol}")
    return inserted


def _parse_ratio(purpose: str, action_type: str) -> tuple[float | None, float | None]:
    """
    Best-effort ratio/amount extraction from free-text purpose string.
    Examples: "Bonus 1:1", "Stock Split From Rs.10/- To Rs.1/-", "Dividend Rs.19"
    Returns (ratio, amount).
    """
    import re

    if action_type == "split":
        # Format: "10:1"  or "1:10" (old face : new face)
        m = re.search(r"(\d+)\s*:\s*(\d+)", purpose)
        if m:
            old, new = int(m.group(1)), int(m.group(2))
            if new > 0:
                return old / new, None
        # Format: "Rs.10/- to Rs.1/-" (face value change)
        m = re.search(r"rs\.?\s*(\d+).*?rs\.?\s*(\d+)", purpose, re.IGNORECASE | re.DOTALL)
        if m:
            old_fv, new_fv = int(m.group(1)), int(m.group(2))
            if new_fv > 0:
                return old_fv / new_fv, None
        # Format: plain "10/1"
        m = re.search(r"(\d+)\s*/\s*(\d+)", purpose)
        if m:
            old, new = int(m.group(1)), int(m.group(2))
            if new > 0:
                return old / new, None

    elif action_type == "bonus":
        m = re.search(r"(\d+)\s*:\s*(\d+)", purpose)
        if m:
            bonus_shares, held = int(m.group(1)), int(m.group(2))
            ratio = (held + bonus_shares) / held if held > 0 else None
            return ratio, None

    elif action_type == "dividend":
        m = re.search(r"rs\.?\s*([\d.]+)", purpose, re.IGNORECASE)
        if m:
            return None, float(m.group(1))

    return None, None


def apply_adjustments(symbol: str) -> None:
    """
    Recompute adjusted_close for all historical OHLCV rows of a symbol
    by applying each corporate action in reverse chronological order.

    Stores result in ohlcv_daily.adjusted_close.
    Sets ohlcv_daily.is_adjusted = True.
    """
    with get_session() as session:
        actions = (
            session.query(CorporateAction)
            .filter_by(symbol=symbol)
            .order_by(CorporateAction.ex_date.desc())
            .all()
        )
        if not actions:
            return

        for action in actions:
            factor, vol_factor = _compute_factors(action)
            if factor is None and action.action_type == "dividend":
                # Cash dividend: subtract from adjusted_close before ex_date
                session.execute(text("""
                    UPDATE ohlcv_daily
                    SET adjusted_close = adjusted_close - :amount,
                        is_adjusted    = 1
                    WHERE symbol = :sym AND date < :ex_date AND adjusted_close IS NOT NULL
                """), {"amount": action.amount or 0, "sym": symbol, "ex_date": action.ex_date})
            elif factor is not None:
                session.execute(text("""
                    UPDATE ohlcv_daily
                    SET adjusted_close = adjusted_close * :factor,
                        volume         = CAST(volume * :vfactor AS INTEGER),
                        is_adjusted    = 1
                    WHERE symbol = :sym AND date < :ex_date AND adjusted_close IS NOT NULL
                """), {"factor": factor, "vfactor": vol_factor, "sym": symbol, "ex_date": action.ex_date})

        session.commit()
    logger.info(f"Corporate action adjustment applied for {symbol} ({len(actions)} action(s))")


def _compute_factors(action: CorporateAction) -> tuple[float | None, float]:
    """Return (price_factor, volume_factor) for price backward-adjustment."""
    if action.action_type == "split" and action.ratio:
        # ratio = old_face / new_face (e.g. 10/1 = 10 means stock split into 10 parts)
        # price goes down by factor ratio, volume goes up by ratio
        return 1.0 / action.ratio, action.ratio
    elif action.action_type == "bonus" and action.ratio:
        # ratio = (held + bonus) / held (e.g. 2/1 bonus → ratio=2)
        # price halves (ratio=2), volume doubles
        return 1.0 / action.ratio, action.ratio
    return None, 1.0


def run_all() -> None:
    """Fetch and apply corporate actions for every tracked stock."""
    for symbol in BANKING_STOCKS:
        fetch_and_store_actions(symbol)
        apply_adjustments(symbol)
