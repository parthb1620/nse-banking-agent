"""
Morning scan job — runs at 08:30 IST on trading days.

Steps:
  1. Collect overnight news (RSS feeds)
  2. Run LLM sentiment on new articles
  3. Score all 7 stocks
  4. Pick top 3 by total_score
  5. Summarise any recent NSE filings
  6. Send Telegram alert before market open (09:15)

Output format (Telegram):
  Top picks: AXISBANK (75.0) FEDERALBNK (67.5) SBIN (57.5)
  ──────────────────────────
  AXISBANK  Tech:100 Fund:50 Sent:50 → BUY str=10
  ...
  Recent news: [headline snippets]
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

from loguru import logger

from config.nse_calendar import is_trading_day
from config.settings import BANKING_STOCKS, STOCK_NAMES

_IST = ZoneInfo("Asia/Kolkata")


def _collect_news() -> int:
    """Collect overnight news and return count of new articles."""
    try:
        from data.collectors.news_collector import run_all
        return run_all() or 0
    except Exception as exc:
        logger.error(f"morning_scan: news collection failed — {exc}")
        return 0


def _run_sentiment(max_per_symbol: int = 5) -> int:
    """Run LLM sentiment on unscored articles. Returns count processed."""
    try:
        from llm.analyzers.news_sentiment import process_all_pending
        results = process_all_pending(max_per_symbol=max_per_symbol)
        return sum(results.values())
    except Exception as exc:
        logger.error(f"morning_scan: sentiment run failed — {exc}")
        return 0


def _get_signals() -> dict[str, dict]:
    """Return latest technical signal per stock."""
    from data.storage.database import TechnicalSignal, get_session
    signals = {}
    with get_session() as session:
        for sym in BANKING_STOCKS:
            sig = (
                session.query(TechnicalSignal)
                .filter(TechnicalSignal.symbol == sym)
                .order_by(TechnicalSignal.signal_date.desc())
                .first()
            )
            if sig:
                signals[sym] = {
                    "type":     sig.signal_type,
                    "strength": sig.strength,
                    "date":     str(sig.signal_date),
                }
    return signals


def _recent_headlines(n: int = 5) -> list[str]:
    """Return the n most recent news headlines across all stocks."""
    from data.storage.database import NewsArticle, get_session
    with get_session() as session:
        arts = (
            session.query(NewsArticle)
            .order_by(NewsArticle.published_at.desc())
            .limit(n)
            .all()
        )
        return [f"[{a.symbol}] {a.headline}" for a in arts if a.headline]


def build_message(scores: list[dict], signals: dict, headlines: list[str], fii_status: dict | None = None) -> str:
    """Build the Telegram message body."""
    today = datetime.now(_IST).strftime("%d %b %Y")
    lines = [f"<b>Date:</b> {today}"]

    # Top 3 picks
    top3 = scores[:3]
    picks = "  ".join(f"{r['symbol']} ({r['total_score']:.0f})" for r in top3)
    lines.append(f"\n🏆 <b>Top picks:</b> {picks}")
    lines.append("─" * 32)

    for r in top3:
        sym = r["symbol"]
        sig = signals.get(sym, {})
        sig_str = f"{sig.get('type','?')} str={sig.get('strength',0)}" if sig else "no signal"
        lines.append(
            f"<b>{sym}</b>  Tech:{r.get('technical_score',0):.0f}  "
            f"Fund:{r.get('fundamental_score',0):.0f}  "
            f"Sent:{r.get('sentiment_score',0):.0f}  → {sig_str}"
        )

    # All stocks ranked
    lines.append("\n📋 <b>Full ranking:</b>")
    for i, r in enumerate(scores, 1):
        lines.append(f"  {i}. {r['symbol']:<12} {r['total_score']:.1f}")

    # FII/DII institutional flow
    if fii_status and fii_status.get("available"):
        fii_net  = fii_status.get("fii_net_cr", 0)
        dii_net  = fii_status.get("dii_net_cr", 0)
        blocking = fii_status.get("blocking_entries", False)
        fii_icon = "🟢" if fii_net >= 0 else "🔴"
        dii_icon = "🟢" if dii_net >= 0 else "🔴"
        block_str = "  ⛔ <b>NEW ENTRIES BLOCKED</b> (FII selling streak)" if blocking else ""
        lines.append(
            f"\n🏦 <b>Institutional flows:</b>\n"
            f"  {fii_icon} FII {fii_net:+,.0f} Cr  |  {dii_icon} DII {dii_net:+,.0f} Cr"
            f"{block_str}"
        )

    # Recent news
    if headlines:
        lines.append("\n📰 <b>Recent news:</b>")
        for h in headlines[:3]:
            lines.append(f"  • {h[:80]}")

    return "\n".join(lines)


def run() -> None:
    """Execute the morning scan and send Telegram alert."""
    today = date.today()
    if not is_trading_day(today):
        logger.info(f"morning_scan: {today} is not a trading day — skipping")
        return

    logger.info("=== Morning scan started ===")

    # 1. Collect news
    new_articles = _collect_news()
    logger.info(f"morning_scan: {new_articles} new articles collected")

    # 2. LLM sentiment
    scored = _run_sentiment()
    logger.info(f"morning_scan: {scored} articles sentiment-scored")

    # 3. Score all stocks
    from scoring.stock_scorer import score_all
    scores = score_all()

    # 4. Latest signals
    signals = _get_signals()

    # 5. FII/DII flows
    fii_status: dict = {}
    try:
        from data.collectors.fii_dii import run_daily, get_status
        run_daily()
        fii_status = get_status()
    except Exception as exc:
        logger.warning(f"morning_scan: FII/DII fetch failed — {exc}")

    # 6. Recent headlines
    headlines = _recent_headlines()

    # 7. Build and send alert
    msg = build_message(scores, signals, headlines, fii_status)

    from alerts.telegram_bot import send_morning_alert
    sent = send_morning_alert(msg)

    logger.info(f"=== Morning scan complete — Telegram {'sent' if sent else 'FAILED'} ===")
