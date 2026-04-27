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
