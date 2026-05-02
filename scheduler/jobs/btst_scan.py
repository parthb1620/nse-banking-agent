"""
BTST scan — runs at 14:30 IST on trading days.

Picks stocks likely to gap up or continue the next morning.
Signal logic: see analysis/technical/btst_signals.py

Steps:
  1. Generate BTST signals for all 45 stocks
  2. Score using BTST engine weights (Tech 70%, Fund 10%, Sent 20%)
  3. Collect recent news sentiment for top picks
  4. Send Telegram alert with top BTST candidates

Output format (Telegram):
  Top BTST picks: AXISBANK (82) RELIANCE (76)
  ─────────────────────────────
  AXISBANK  str=8  BUY  — [reason]  Vol 2.1× avg
  ...
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

from loguru import logger

from config.nse_calendar import is_trading_day
from config.settings import BTST_UNIVERSE, STOCK_NAMES
from alerts.telegram_bot import _esc, send

_IST = ZoneInfo("Asia/Kolkata")


def _generate_btst_signals() -> dict[str, dict]:
    from analysis.technical.btst_signals import generate_signal
    signals = {}
    today = date.today()
    for sym in BTST_UNIVERSE:
        try:
            sig = generate_signal(sym, today)
            if sig:
                signals[sym] = {
                    "type":     sig.signal_type,
                    "strength": sig.strength,
                    "reason":   sig.reason or "",
                }
        except Exception as exc:
            logger.warning(f"btst_scan: signal failed for {sym} — {exc}")
    return signals


def build_message(scores: list[dict], signals: dict[str, dict]) -> str:
    today = datetime.now(_IST).strftime("%d %b %Y")
    lines = [f"🌙 <b>BTST Scan — {today}</b>"]

    buy_signals = {s: d for s, d in signals.items() if d["type"] == "BUY"}

    if not buy_signals:
        lines.append("\n⚪ No BTST setups today — market close not constructive.")
        return "\n".join(lines)

    # Rank by score among BUY stocks only
    buy_scores = [r for r in scores if r["symbol"] in buy_signals]
    buy_scores.sort(key=lambda r: r["total_score"], reverse=True)

    picks = "  ".join(f"{_esc(r['symbol'])} ({r['total_score']:.0f})" for r in buy_scores[:3])
    lines.append(f"\n🏆 <b>Top BTST picks:</b> {picks}")
    lines.append("─" * 32)

    for r in buy_scores[:4]:
        sym = r["symbol"]
        sig = signals[sym]
        lines.append(
            f"<b>{_esc(sym)}</b>  str={sig['strength']}  "
            f"Tech:{r.get('technical_score',0):.0f}  "
            f"Sent:{r.get('sentiment_score',0):.0f}"
        )
        if sig.get("reason"):
            lines.append(f"  → {_esc(sig['reason'][:100])}")

    lines.append("\n⚠️ <i>BTST: buy near close, sell next morning at open. Stop = today's low.</i>")
    return "\n".join(lines)


def run() -> None:
    today = date.today()
    if not is_trading_day(today):
        logger.info(f"btst_scan: {today} is not a trading day — skipping")
        return

    logger.info("=== BTST scan started ===")

    # 1. Generate signals
    signals = _generate_btst_signals()
    buy_count = sum(1 for d in signals.values() if d["type"] == "BUY")
    logger.info(f"btst_scan: {buy_count} BUY signals out of {len(signals)}")

    # 2. Score with BTST engine weights
    from scoring.engine_scorer import score_all
    scores = score_all("btst")

    # 3. Build and send
    msg = build_message(scores, signals)
    sent = send(msg)
    logger.info(f"=== BTST scan complete — Telegram {'sent' if sent else 'FAILED'} ===")
