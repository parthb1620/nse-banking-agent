"""
Telegram alert sender.

Sends plain-text messages to a configured chat using the Bot API.
Used by morning_scan.py, eod_report.py, intraday_monitor.py, and paper_trading/.

No dependencies beyond requests — deliberately lightweight.
"""

import requests
from loguru import logger

from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

_SEND_URL       = "https://api.telegram.org/bot{token}/sendMessage"
_TELEGRAM_LIMIT = 4096   # Telegram hard cap per message


def _split_message(text: str) -> list[str]:
    """Split at newline boundaries so each chunk stays under the Telegram limit."""
    if len(text) <= _TELEGRAM_LIMIT:
        return [text]

    chunks: list[str] = []
    current = ""
    for line in text.split("\n"):
        candidate = (current + "\n" + line) if current else line
        if len(candidate) > _TELEGRAM_LIMIT:
            if current:
                chunks.append(current)
            while len(line) > _TELEGRAM_LIMIT:
                chunks.append(line[:_TELEGRAM_LIMIT])
                line = line[_TELEGRAM_LIMIT:]
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def send(message: str, parse_mode: str = "HTML") -> bool:
    """
    Send a message to the configured Telegram chat.
    Automatically splits messages that exceed Telegram's 4096-char limit.
    Returns True if all chunks sent successfully, False on any failure (never raises).
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram: BOT_TOKEN or CHAT_ID not set — message skipped")
        return False

    url    = _SEND_URL.format(token=TELEGRAM_BOT_TOKEN)
    chunks = _split_message(message)
    all_ok = True

    for i, chunk in enumerate(chunks, 1):
        data = {
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       chunk,
            "parse_mode": parse_mode,
        }
        try:
            resp = requests.post(url, data=data, timeout=10)
            if resp.status_code == 200:
                if len(chunks) > 1:
                    logger.info(f"Telegram: chunk {i}/{len(chunks)} sent")
                else:
                    logger.info("Telegram: message sent")
            else:
                logger.error(f"Telegram: HTTP {resp.status_code} — {resp.text[:200]}")
                all_ok = False
        except Exception as exc:
            logger.error(f"Telegram: send failed — {exc}")
            all_ok = False

    return all_ok


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
        f"\n{thesis}"
    )
    return send(msg)


def send_trade_partial(symbol: str, entry: float, exit_price: float, qty: int, total_qty: int, pnl: float) -> bool:
    pct = (exit_price - entry) / entry * 100
    msg = (
        f"💰 <b>Paper Trade PARTIAL BOOKED (1R)</b>\n\n"
        f"<b>{symbol}</b>  booked qty={qty}/{total_qty}  (rest runs with stop at breakeven)\n"
        f"Entry ₹{entry:.2f} → Partial exit ₹{exit_price:.2f}  ({pct:+.1f}%)\n"
        f"Booked P&L <b>₹{pnl:+,.0f}</b>"
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


def send_late_recovery_alert(
    symbol: str,
    stock_name: str,
    pct_at_trough: float,
    pct_now: float,
    volume_ratio: float,
    filing_subject: str,
) -> bool:
    msg = (
        f"⚡ <b>LATE SESSION RECOVERY — {symbol}</b>\n\n"
        f"<b>{stock_name}</b>\n"
        f"Was  {pct_at_trough:+.2f}%  at ~14:50\n"
        f"Now  <b>{pct_now:+.2f}%</b>  (recovered)\n"
        f"Volume  <b>{volume_ratio:.1f}×</b> avg last 10-min\n\n"
        f"📋 Recent filing: {filing_subject[:200]}\n\n"
        f"⚠️ yfinance has ~15 min delay — price is approximate.\n"
        f"Possible institutional accumulation post-result."
    )
    return send(msg)
