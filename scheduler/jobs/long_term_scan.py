"""
Long-term scan — runs every Sunday at 08:00 IST.

Weekly fundamentals-first review of the 60-stock long-term universe
(45 large-caps + 15 quality midcaps).

Steps:
  1. Generate long-term signals for the full universe
  2. Score with long-term weights (Tech 20%, Fund 60%, Sent 20%)
  3. Identify new BUY signals (strong fundamentals + above EMA_200)
  4. Send weekly Telegram digest

Output format (Telegram):
  [LONG] Weekly picks: TCS (78) SUNPHARMA (74) PERSISTENT (71)
  ─────────────────────────────────────────
  TCS  Fund:85 Tech:62 Sent:55  str=8 — [reason]
  ...
  Midcap alert: KPIT (68) — new BUY
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

from loguru import logger

from config.settings import LONGTERM_UNIVERSE, STOCK_NAMES, MIDCAP_STOCKS
from alerts.telegram_bot import _esc, send

_IST = ZoneInfo("Asia/Kolkata")


def _generate_longterm_signals() -> dict[str, dict]:
    from analysis.technical.long_term_signals import generate_signal
    signals = {}
    today = date.today()
    for sym in LONGTERM_UNIVERSE:
        try:
            sig = generate_signal(sym, today)
            if sig:
                signals[sym] = {
                    "type":     sig.signal_type,
                    "strength": sig.strength,
                    "reason":   sig.reason or "",
                }
        except Exception as exc:
            logger.warning(f"long_term_scan: signal failed for {sym} — {exc}")
    return signals


def build_message(scores: list[dict], signals: dict[str, dict]) -> str:
    today = datetime.now(_IST).strftime("%d %b %Y")
    lines = [f"📅 <b>[LONG] Weekly Scan — {today}</b>"]

    buy_scores = [r for r in scores if signals.get(r["symbol"], {}).get("type") == "BUY"]
    buy_scores.sort(key=lambda r: r["total_score"], reverse=True)

    # Separate large-cap and midcap picks
    largecap_buys = [r for r in buy_scores if r["symbol"] not in MIDCAP_STOCKS]
    midcap_buys   = [r for r in buy_scores if r["symbol"] in MIDCAP_STOCKS]

    if not buy_scores:
        lines.append("\n⚪ No long-term BUY setups this week. Market or fundamentals not supportive.")
        return "\n".join(lines)

    # Top picks header
    top3 = buy_scores[:3]
    picks = "  ".join(f"{_esc(r['symbol'])} ({r['total_score']:.0f})" for r in top3)
    lines.append(f"\n🏆 <b>Top picks:</b> {picks}")
    lines.append("─" * 36)

    # Large-cap BUYs
    if largecap_buys:
        lines.append("\n🔵 <b>Large-cap BUYs:</b>")
        for r in largecap_buys[:5]:
            sym = r["symbol"]
            sig = signals.get(sym, {})
            lines.append(
                f"  <b>{_esc(sym)}</b>  Fund:{r.get('fundamental_score',0):.0f}  "
                f"Tech:{r.get('technical_score',0):.0f}  "
                f"str={sig.get('strength',0)}"
            )
            if sig.get("reason"):
                lines.append(f"    → {_esc(sig['reason'][:120])}")

    # Midcap BUYs
    if midcap_buys:
        lines.append("\n🟡 <b>Midcap opportunities:</b>")
        for r in midcap_buys[:4]:
            sym = r["symbol"]
            sig = signals.get(sym, {})
            lines.append(
                f"  <b>{_esc(sym)}</b> ({_esc(STOCK_NAMES.get(sym, sym))})  "
                f"Fund:{r.get('fundamental_score',0):.0f}  "
                f"Tech:{r.get('technical_score',0):.0f}  "
                f"str={sig.get('strength',0)}"
            )

    # Full ranking (top 10 regardless of signal)
    lines.append("\n📋 <b>Full ranking (top 10):</b>")
    for i, r in enumerate(scores[:10], 1):
        sym = r["symbol"]
        sig_type = signals.get(sym, {}).get("type", "?")
        icon = "🟢" if sig_type == "BUY" else "🔴" if sig_type == "SELL" else "⚪"
        tag = " [MID]" if sym in MIDCAP_STOCKS else ""
        lines.append(f"  {i}. {icon} {_esc(sym):<12}{tag}  {r['total_score']:.1f}")

    lines.append("\n<i>Long-term engine: hold 3–12 months. Review weekly.</i>")
    return "\n".join(lines)


def run() -> None:
    logger.info("=== Long-term weekly scan started ===")

    # 1. Generate long-term signals for all 60 stocks
    signals = _generate_longterm_signals()
    buy_count = sum(1 for d in signals.values() if d["type"] == "BUY")
    logger.info(f"long_term_scan: {buy_count}/{len(signals)} BUY signals")

    # 2. Score with long-term engine
    from scoring.engine_scorer import score_all
    scores = score_all("longterm")

    # 3. Build and send
    msg = build_message(scores, signals)
    sent = send(msg)
    logger.info(f"=== Long-term scan complete — Telegram {'sent' if sent else 'FAILED'} ===")
