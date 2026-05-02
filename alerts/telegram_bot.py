"""
Telegram alert sender.

Sends plain-text messages to a configured chat using the Bot API.
Used by morning_scan.py, eod_report.py, intraday_monitor.py, and paper_trading/.

No dependencies beyond requests — deliberately lightweight.
"""

import requests
from loguru import logger

from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_EXTRA_CHAT_IDS

_SEND_URL       = "https://api.telegram.org/bot{token}/sendMessage"
_TELEGRAM_LIMIT = 4096   # Telegram hard cap per message


def _esc(text: str) -> str:
    """Escape HTML special chars for Telegram HTML parse mode."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


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


def _send_to(chat_id: str, message: str, parse_mode: str) -> bool:
    """Send one message (with chunking) to a single chat_id. Returns True on full success."""
    url    = _SEND_URL.format(token=TELEGRAM_BOT_TOKEN)
    chunks = _split_message(message)
    ok     = True
    for i, chunk in enumerate(chunks, 1):
        try:
            resp = requests.post(url, data={"chat_id": chat_id, "text": chunk, "parse_mode": parse_mode}, timeout=10)
            if resp.status_code == 200:
                if len(chunks) > 1:
                    logger.info(f"Telegram [{chat_id}]: chunk {i}/{len(chunks)} sent")
                else:
                    logger.info(f"Telegram [{chat_id}]: message sent")
            else:
                logger.error(f"Telegram [{chat_id}]: HTTP {resp.status_code} — {resp.text[:200]}")
                ok = False
        except Exception as exc:
            logger.error(f"Telegram [{chat_id}]: send failed — {exc}")
            ok = False
    return ok


def send(message: str, parse_mode: str = "HTML") -> bool:
    """
    Send a message to all configured Telegram chats (primary + extras).
    Automatically splits messages that exceed Telegram's 4096-char limit.
    Returns True if all chunks sent to all chats successfully.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram: BOT_TOKEN or CHAT_ID not set — message skipped")
        return False

    all_chat_ids = [TELEGRAM_CHAT_ID] + TELEGRAM_EXTRA_CHAT_IDS
    return all(_send_to(cid, message, parse_mode) for cid in all_chat_ids)


def send_morning_alert(content: str) -> bool:
    return send(f"🌅 <b>[SHORT] Morning Scan</b>\n\n{content}")


def send_eod_alert(content: str) -> bool:
    return send(f"📊 <b>[SHORT] EOD Report</b>\n\n{content}")


def send_btst_alert(content: str) -> bool:
    return send(f"🌙 <b>[BTST] Overnight Picks</b>\n\n{content}")


def send_longterm_alert(content: str) -> bool:
    return send(f"📅 <b>[LONG] Weekly Picks</b>\n\n{content}")


def send_intraday_alert(content: str) -> bool:
    return send(f"⚡ <b>[INTRA] Intraday Alert</b>\n\n{content}")


def send_engine_pnl(engine: str, summary: dict) -> bool:
    engine_icons = {"longterm": "📅", "shortterm": "📊", "btst": "🌙", "intraday": "⚡"}
    icon = engine_icons.get(engine, "💼")
    ret_pct = summary["total_pnl"] / summary["initial_capital"] * 100 if summary["initial_capital"] else 0
    msg = (
        f"{icon} <b>[{engine.upper()}] Portfolio</b>\n\n"
        f"Capital  ₹{summary['capital_current']:,.0f}  ({ret_pct:+.2f}%)\n"
        f"Total P&amp;L  <b>₹{summary['total_pnl']:+,.0f}</b>\n"
        f"Win rate  {summary['win_rate']:.0f}%  ({summary['trades_total']} closed)\n"
        f"Open positions  {summary['trades_open']}"
    )
    return send(msg)


def send_trade_entry(symbol: str, entry: float, stop: float, target: float, qty: int, thesis: str) -> bool:
    rr = round((target - entry) / (entry - stop), 1) if entry != stop else 0
    msg = (
        f"📥 <b>Paper Trade ENTERED</b>\n\n"
        f"<b>{_esc(symbol)}</b>  qty={qty}\n"
        f"Entry  ₹{entry:.2f}\n"
        f"Stop   ₹{stop:.2f}  ({(stop - entry) / entry * 100:+.1f}%)\n"
        f"Target ₹{target:.2f}  ({(target - entry) / entry * 100:+.1f}%)  R:R {rr}\n"
        f"\n{_esc(thesis)}"
    )
    return send(msg)


def send_trade_partial(symbol: str, entry: float, exit_price: float, qty: int, total_qty: int, pnl: float) -> bool:
    pct = (exit_price - entry) / entry * 100
    msg = (
        f"💰 <b>Paper Trade PARTIAL BOOKED (1R)</b>\n\n"
        f"<b>{_esc(symbol)}</b>  booked qty={qty}/{total_qty}  (rest runs with stop at breakeven)\n"
        f"Entry ₹{entry:.2f} → Partial exit ₹{exit_price:.2f}  ({pct:+.1f}%)\n"
        f"Booked P&amp;L <b>₹{pnl:+,.0f}</b>"
    )
    return send(msg)


def send_trade_exit(symbol: str, status: str, entry: float, exit_price: float, qty: int, pnl: float) -> bool:
    icon   = "🎯" if "target" in status else "🛑"
    result = "TARGET HIT" if "target" in status else "STOP HIT"
    pct    = (exit_price - entry) / entry * 100
    msg = (
        f"{icon} <b>Paper Trade CLOSED — {result}</b>\n\n"
        f"<b>{_esc(symbol)}</b>  qty={qty}\n"
        f"Entry ₹{entry:.2f} → Exit ₹{exit_price:.2f}  ({pct:+.1f}%)\n"
        f"P&amp;L <b>₹{pnl:+,.0f}</b>"
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


def send_breakout_alert(
    symbol: str,
    stock_name: str,
    sector: str,
    label: str,
    pct_change: float,
    latest_price: float,
    prev_close: float,
    volume_ratio: float,
    w52_high: float | None = None,
) -> bool:
    icon = "🚀" if "BREAKOUT" in label else "📉"
    sector_line = f"Sector  <b>{_esc(sector)}</b>\n" if sector else ""
    w52_line = f"52W High  <b>₹{w52_high:.2f}</b>  ← BROKEN OUT\n" if w52_high else ""
    msg = (
        f"{icon} <b>{_esc(label)} — {_esc(symbol)}</b>\n\n"
        f"<b>{_esc(stock_name)}</b>\n"
        f"{sector_line}"
        f"Price     <b>₹{latest_price:.2f}</b>  ({pct_change:+.2f}% vs prev close ₹{prev_close:.2f})\n"
        f"{w52_line}"
        f"Volume    <b>{volume_ratio:.1f}×</b> avg daily rate"
    )
    return send(msg)


def send_late_recovery_alert(
    symbol: str,
    stock_name: str,
    pct_at_trough: float,
    pct_now: float,
    volume_ratio: float,
    filing_subject: str,
    sector: str = "",
) -> bool:
    sector_line = f"Sector  <b>{_esc(sector)}</b>\n" if sector else ""
    msg = (
        f"⚡ <b>LATE SESSION RECOVERY — {_esc(symbol)}</b>\n\n"
        f"<b>{_esc(stock_name)}</b>\n"
        f"{sector_line}"
        f"Was  {pct_at_trough:+.2f}%  at ~14:50\n"
        f"Now  <b>{pct_now:+.2f}%</b>  (recovered)\n"
        f"Volume  <b>{volume_ratio:.1f}×</b> avg last 10-min\n\n"
        f"📋 Recent filing: {_esc(filing_subject[:200])}\n\n"
        f"Possible institutional accumulation post-result."
    )
    return send(msg)
