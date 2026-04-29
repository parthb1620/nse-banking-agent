"""
Morning scan job — runs at 08:30 IST on trading days.

Steps:
  1. Collect overnight news (RSS feeds)
  2. Embed new articles/filings with nomic-embed-text
  3. Run Gemma4:e4b sentiment on new articles
  4. Score all 7 stocks
  5. For top BUY signals: Gemma4 writes trade thesis, DeepSeek R1 stress-tests it
  6. Send Telegram alert before market open (09:15)

Output format (Telegram):
  Top picks: AXISBANK (75.0) FEDERALBNK (67.5) SBIN (57.5)
  ──────────────────────────
  AXISBANK  Tech:100 Fund:50 Sent:50 → BUY str=10
  Thesis: <3-5 sentence synthesis>
  Risk: MEDIUM — CAUTION | <concern>
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


def _embed_documents() -> int:
    """Embed pending news + filings for all stocks with nomic-embed-text."""
    total = 0
    try:
        from llm.embeddings.store import embed_pending
        for sym in BANKING_STOCKS:
            try:
                total += embed_pending(sym)
            except Exception as exc:
                logger.warning(f"morning_scan: embed failed for {sym} — {exc}")
    except Exception as exc:
        logger.warning(f"morning_scan: embedding step skipped — {exc}")
    return total


def _run_sentiment(max_per_symbol: int = 5) -> int:
    """Run Gemma4 sentiment on unscored articles. Returns count processed."""
    try:
        from llm.analyzers.news_sentiment import process_all_pending
        results = process_all_pending(max_per_symbol=max_per_symbol)
        return sum(results.values())
    except Exception as exc:
        logger.error(f"morning_scan: sentiment run failed — {exc}")
        return 0


def _recent_sentiment_summary(symbol: str) -> str:
    """Return a short text summary of recent sentiment scores for a symbol."""
    try:
        from data.storage.database import NewsArticle, get_session
        from datetime import timedelta
        cutoff = datetime.now(_IST) - timedelta(days=3)
        with get_session() as s:
            arts = (
                s.query(NewsArticle)
                .filter(
                    NewsArticle.symbol == symbol,
                    NewsArticle.published_at >= cutoff,
                    NewsArticle.sentiment_score.isnot(None),
                )
                .order_by(NewsArticle.published_at.desc())
                .limit(5)
                .all()
            )
        if not arts:
            return "(no recent scored articles)"
        parts = [f"{a.headline[:60] if a.headline else '?'}: {a.sentiment_score:+.2f}" for a in arts]
        return "\n".join(parts)
    except Exception:
        return "(unavailable)"


def _latest_signal_for(symbol: str) -> dict:
    """Return the latest TechnicalSignal row for a symbol as a dict."""
    from data.storage.database import TechnicalSignal, get_session
    with get_session() as s:
        sig = (
            s.query(TechnicalSignal)
            .filter(TechnicalSignal.symbol == symbol)
            .order_by(TechnicalSignal.signal_date.desc())
            .first()
        )
        if sig:
            return {
                "type":     sig.signal_type,
                "strength": sig.strength or 0,
                "reason":   sig.reason or "",
                "date":     str(sig.signal_date),
            }
    return {}


def _build_thesis_and_risk(symbol: str, score_row: dict, stock_name: str) -> tuple[str, str]:
    """
    Run the Gemma4 thesis writer then the DeepSeek R1 risk manager.
    Returns (thesis_text, risk_text) — both may be empty strings on failure.
    """
    from llm.analyzers.thesis_writer import write_thesis
    from llm.analyzers.risk_manager import assess
    from llm.embeddings.store import search

    sig = _latest_signal_for(symbol)
    if sig.get("type") != "BUY":
        return "", ""

    technical_summary = (
        f"Signal: {sig['type']} strength={sig['strength']}/10  "
        f"Tech score={score_row.get('technical_score', 0):.0f}  "
        f"Reason: {sig['reason'][:150]}"
    )
    sentiment_summary = _recent_sentiment_summary(symbol)

    # Semantic retrieval — find relevant news/filing snippets
    context = search(symbol, f"{symbol} banking outlook trading opportunity")

    thesis = write_thesis(
        symbol            = symbol,
        stock_name        = stock_name,
        today             = datetime.now(_IST).strftime("%Y-%m-%d"),
        technical_summary = technical_summary,
        sentiment_summary = sentiment_summary,
        context_snippets  = context,
    )

    risk_text = ""
    if thesis:
        # Estimate stop/target pct from scores (rough proxy — real values in simulator)
        stop_pct   = -3.0   # ~2×ATR rough default
        target_pct = +6.0   # 2:1 R:R
        ra = assess(
            symbol          = symbol,
            thesis          = thesis,
            signal_strength = sig.get("strength", 0),
            stop_pct        = stop_pct,
            target_pct      = target_pct,
        )
        if ra:
            concerns_str = "; ".join(ra.concerns[:2]) if ra.concerns else ""
            risk_text = (
                f"{ra.risk_level} — {ra.recommendation}"
                + (f" | {concerns_str}" if concerns_str else "")
            )

    return thesis, risk_text


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


def build_message(
    scores: list[dict],
    signals: dict,
    headlines: list[str],
    fii_status: dict | None = None,
    llm_insights: dict | None = None,   # {symbol: {"thesis": str, "risk": str}}
) -> str:
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
        # Attach LLM thesis + risk if available
        if llm_insights and sym in llm_insights:
            insight = llm_insights[sym]
            if insight.get("thesis"):
                lines.append(f"  📝 {insight['thesis'][:280]}")
            if insight.get("risk"):
                lines.append(f"  ⚠️ Risk: {insight['risk']}")

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

    # 2. Embed new articles/filings (nomic-embed-text)
    embedded = _embed_documents()
    logger.info(f"morning_scan: {embedded} documents embedded")

    # 3. Gemma4 sentiment on unscored articles
    scored = _run_sentiment()
    logger.info(f"morning_scan: {scored} articles sentiment-scored")

    # 4. Score all stocks
    from scoring.stock_scorer import score_all
    scores = score_all()

    # 5. Latest signals
    signals = _get_signals()

    # 6. FII/DII flows
    fii_status: dict = {}
    try:
        from data.collectors.fii_dii import run_daily, get_status
        run_daily()
        fii_status = get_status()
    except Exception as exc:
        logger.warning(f"morning_scan: FII/DII fetch failed — {exc}")

    # 7. Recent headlines
    headlines = _recent_headlines()

    # 8. Gemma4 thesis + DeepSeek R1 risk assessment for top BUY signals
    llm_insights: dict = {}
    top_buy_symbols = [
        r["symbol"] for r in scores[:3]
        if signals.get(r["symbol"], {}).get("type") == "BUY"
    ]
    for sym in top_buy_symbols:
        try:
            score_row  = next((r for r in scores if r["symbol"] == sym), {})
            stock_name = STOCK_NAMES.get(sym, sym)
            thesis, risk = _build_thesis_and_risk(sym, score_row, stock_name)
            if thesis or risk:
                llm_insights[sym] = {"thesis": thesis, "risk": risk}
                logger.info(f"morning_scan: {sym} thesis+risk generated")
        except Exception as exc:
            logger.warning(f"morning_scan: LLM insights failed for {sym} — {exc}")

    # 9. Build and send alert
    msg = build_message(scores, signals, headlines, fii_status, llm_insights)

    from alerts.telegram_bot import send_morning_alert
    sent = send_morning_alert(msg)

    logger.info(f"=== Morning scan complete — Telegram {'sent' if sent else 'FAILED'} ===")
