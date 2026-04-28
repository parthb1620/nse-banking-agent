"""
Telegram alert sender.

Sends plain-text messages to a configured chat using the Bot API.
Used by morning_scan.py and eod_report.py.

No dependencies beyond requests — deliberately lightweight.
"""

import requests
from loguru import logger

from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

_SEND_URL = "https://api.telegram.org/bot{token}/sendMessage"


def send(message: str, parse_mode: str = "HTML") -> bool:
    """
    Send a message to the configured Telegram chat.
    Returns True on success, False on failure (never raises).
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram: BOT_TOKEN or CHAT_ID not set — message skipped")
        return False

    url  = _SEND_URL.format(token=TELEGRAM_BOT_TOKEN)
    data = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": parse_mode,
    }

    try:
        resp = requests.post(url, data=data, timeout=10)
        if resp.status_code == 200:
            logger.info("Telegram: message sent")
            return True
        else:
            logger.error(f"Telegram: HTTP {resp.status_code} — {resp.text[:200]}")
            return False
    except Exception as exc:
        logger.error(f"Telegram: send failed — {exc}")
        return False


def send_morning_alert(content: str) -> bool:
    return send(f"🌅 <b>NSE Banking — Morning Scan</b>\n\n{content}")


def send_eod_alert(content: str) -> bool:
    return send(f"📊 <b>NSE Banking — EOD Report</b>\n\n{content}")


def send_trade_entry(symbol: str, entry: float, stop: float, target: float, qty: int, thesis: str) -> bool:
    rr = round((target - entry) / (entry - stop), 1) if entry != stop else 0
    msg = (
        f"📥 <b>Paper Trade ENTERED</b>\n\n"
        f"<b>{symbol}</b>  qty={qty}\n"
        f"Entry  ₹{entry:.2f}\n"
        f"Stop   ₹{stop:.2f}  ({(stop - entry) / entry * 100:+.1f}%)\n"
        f"Target ₹{target:.2f}  ({(target - entry) / entry * 100:+.1f}%)  R:R {rr}\n"
        f"\n{thesis[:120]}"
    )
    return send(msg)


def send_trade_exit(symbol: str, status: str, entry: float, exit_price: float, qty: int, pnl: float) -> bool:
    icon   = "🎯" if "target" in status else "🛑"
    result = "TARGET HIT" if "target" in status else "STOP HIT"
    pct    = (exit_price - entry) / entry * 100
    msg = (
        f"{icon} <b>Paper Trade CLOSED — {result}</b>\n\n"
        f"<b>{symbol}</b>  qty={qty}\n"
        f"Entry ₹{entry:.2f} → Exit ₹{exit_price:.2f}  ({pct:+.1f}%)\n"
        f"P&L <b>₹{pnl:+,.0f}</b>"
    )
    return send(msg)


def send_paper_pnl_summary(capital: float, total_pnl: float, win_rate: float, trades: int, open_pos: int) -> bool:
    ret_pct = total_pnl / (capital - total_pnl) * 100 if capital != total_pnl else 0
    msg = (
        f"💼 <b>Paper Trading Summary</b>\n\n"
        f"Capital  ₹{capital:,.0f}  ({ret_pct:+.2f}%)\n"
        f"Total P&L  <b>₹{total_pnl:+,.0f}</b>\n"
        f"Win rate  {win_rate:.0f}%  ({trades} closed)\n"
        f"Open positions  {open_pos}"
    )
    return send(msg)
