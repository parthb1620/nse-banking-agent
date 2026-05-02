"""
Corporate action fetcher and backward price adjuster.

Source: yfinance (.splits and .dividends) — reliable historical data for all NSE stocks.
yfinance split ratio = new_shares / old_shares  (e.g. 5.0 for a 1→5 split).
Adjustment rules:
  Split/Bonus → price factor = 1/ratio, volume factor = ratio
  Dividend    → subtract amount from all adjusted_close before ex_date

Adjusted close is stored in ohlcv_daily.adjusted_close.
Raw close is preserved in ohlcv_daily.close.
"""

from datetime import datetime

import yfinance as yf
from loguru import logger
from sqlalchemy import text

from config.settings import ALL_STOCKS
from data.quality.known_time import compute_usable_from
from data.storage.database import CorporateAction, get_session


def fetch_corporate_actions(symbol: str) -> list[dict]:
    """
    Fetch splits and dividends for one NSE symbol via yfinance.
    Returns list of action dicts ready for storage.
    yfinance split ratio = new_shares/old_shares (matches our CorporateAction.ratio field).
    """
    ticker = yf.Ticker(f"{symbol}.NS")
    actions = []

    try:
        for ts, ratio in ticker.splits.items():
            if ratio <= 0:
                continue
            actions.append({
                "ex_date":     ts.date(),
                "action_type": "split",
                "ratio":       float(ratio),
                "amount":      None,
            })
    except Exception as exc:
        logger.warning(f"yfinance splits fetch failed for {symbol}: {exc}")

    try:
        for ts, amount in ticker.dividends.items():
            if amount <= 0:
                continue
            actions.append({
                "ex_date":     ts.date(),
                "action_type": "dividend",
                "ratio":       None,
                "amount":      float(amount),
            })
    except Exception as exc:
        logger.warning(f"yfinance dividends fetch failed for {symbol}: {exc}")

    return actions


def fetch_and_store_actions(symbol: str) -> int:
    """
    Fetch corporate actions via yfinance and store new ones in corporate_actions table.
    Returns number of new rows inserted.
    """
    raw = fetch_corporate_actions(symbol)
    if not raw:
        logger.warning(f"No corporate actions found for {symbol}")
        return 0

    now      = datetime.utcnow()
    inserted = 0

    with get_session() as session:
        for item in raw:
            ex_date     = item["ex_date"]
            action_type = item["action_type"]
            ratio       = item["ratio"]
            amount      = item["amount"]

            usable_from = compute_usable_from(datetime.combine(ex_date, datetime.min.time()))

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
                announced_at=None,
                published_at=None,
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
    Parse ratio/amount from a free-text NSE corporate action purpose string.
    Used when processing raw NSE announcement text (e.g. from filings scraped later).
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
    for symbol in ALL_STOCKS:
        fetch_and_store_actions(symbol)
        apply_adjustments(symbol)
