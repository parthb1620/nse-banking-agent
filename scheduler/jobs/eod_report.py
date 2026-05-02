"""
EOD report job — runs at 16:15 IST on trading days.

Steps:
  1. Generate technical signals for today
  2. Score all 7 stocks with today's data
  3. Identify new BUY signals
  4. Compare scores to yesterday (if available)
  5. Send Telegram EOD summary

Output format (Telegram):
  Scores: AXISBANK 75 (+5) SBIN 57 (=) ...
  ──────────────────────────
  🟢 NEW BUY: AXISBANK str=8 — [reason]
  🔴 SELL: HDFCBANK str=6 — [reason]
"""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from loguru import logger

from config.nse_calendar import is_trading_day, prev_trading_day
from config.settings import ALL_STOCKS as BANKING_STOCKS, ALL_STOCK_NAMES as STOCK_NAMES
from alerts.telegram_bot import _esc

_IST = ZoneInfo("Asia/Kolkata")


def _generate_todays_signals() -> dict[str, dict]:
    """Generate and store signals for today. Returns {symbol: signal_dict}."""
    from analysis.technical.signals import generate_signal
    signals = {}
    today = date.today()
    for sym in BANKING_STOCKS:
        try:
            sig = generate_signal(sym, today)
            if sig:
                signals[sym] = {
                    "type":     sig.signal_type,
                    "strength": sig.strength,
                    "reason":   sig.reason,
                }
        except Exception as exc:
            logger.error(f"eod_report: signal generation failed for {sym}: {exc}")
    return signals


def _score_change_str(today_score: float, yesterday_score: float | None) -> str:
    if yesterday_score is None:
        return ""
    diff = today_score - yesterday_score
    if abs(diff) < 0.5:
        return "(=)"
    return f"({diff:+.0f})"


def build_message(
    scores: list[dict],
    signals: dict[str, dict],
    prev_scores: dict[str, float],
) -> str:
    today = datetime.now(_IST).strftime("%d %b %Y")
    lines = [f"<b>Date:</b> {today}"]

    # Score summary line
    score_parts = []
    for r in scores[:5]:
        sym = r["symbol"]
        chg = _score_change_str(r["total_score"], prev_scores.get(sym))
        score_parts.append(f"{_esc(sym)} {r['total_score']:.0f} {chg}".strip())
    lines.append("\n📊 <b>Scores:</b> " + "  |  ".join(score_parts))
    lines.append("─" * 32)

    # Signal highlights
    buy_signals  = [(s, d) for s, d in signals.items() if d["type"] == "BUY"]
    sell_signals = [(s, d) for s, d in signals.items() if d["type"] == "SELL"]

    if buy_signals:
        lines.append("\n🟢 <b>BUY signals:</b>")
        for sym, sig in buy_signals:
            lines.append(f"  {_esc(sym)}  str={sig['strength']}  — {_esc(sig['reason'][:80])}")

    if sell_signals:
        lines.append("\n🔴 <b>SELL / exit signals:</b>")
        for sym, sig in sell_signals:
            lines.append(f"  {_esc(sym)}  str={sig['strength']}  — {_esc(sig['reason'][:80])}")

    if not buy_signals and not sell_signals:
        lines.append("\n⚪ No actionable signals today — all NEUTRAL")

    # Full ranking
    lines.append("\n📋 <b>Full ranking:</b>")
    for i, r in enumerate(scores, 1):
        sym = r["symbol"]
        chg = _score_change_str(r["total_score"], prev_scores.get(sym))
        lines.append(f"  {i}. {_esc(sym):<12} {r['total_score']:.1f} {chg}")

    return "\n".join(lines)


def run() -> None:
    """Execute the EOD report and send Telegram alert."""
    today = date.today()
    if not is_trading_day(today):
        logger.info(f"eod_report: {today} is not a trading day — skipping")
        return

    logger.info("=== EOD report started ===")

    # 1. Generate today's signals
    signals = _generate_todays_signals()
    sig_summary = ", ".join(f"{s}:{d['type']}" for s, d in signals.items())
    logger.info(f"eod_report: signals — {sig_summary}")

    # 2. Score all stocks with today's data
    from scoring.stock_scorer import score_all
    scores = score_all()

    # 3. Previous day scores for comparison
    prev_day = prev_trading_day(today)
    prev_scores: dict[str, float] = {}
    try:
        prev_dt = datetime.combine(prev_day, datetime.min.time())
        prev_results = score_all(prev_dt)
        prev_scores = {r["symbol"]: r["total_score"] for r in prev_results}
    except Exception:
        pass

    # 4. Build and send scores/signals
    msg = build_message(scores, signals, prev_scores)

    from alerts.telegram_bot import send_eod_alert, send_paper_pnl_summary
    sent = send_eod_alert(msg)

    # 5. Paper trading P&L summary
    try:
        from paper_trading.tracker import get_summary
        s = get_summary()
        send_paper_pnl_summary(
            capital=s["capital_current"],
            total_pnl=s["total_pnl"],
            win_rate=s["win_rate"],
            trades=s["trades_total"],
            open_pos=s["trades_open"],
        )
    except Exception as exc:
        logger.warning(f"eod_report: paper trading summary failed — {exc}")

    logger.info(f"=== EOD report complete — Telegram {'sent' if sent else 'FAILED'} ===")
