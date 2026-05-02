"""
News RSS collector — Google News, MoneyControl, Economic Times.

Stores articles in news_articles table with usable_from = published_at
rounded up to the next trading-day open (news published after market close
is only tradeable the next morning).

sentiment_score is populated later by llm/analyzers/news_sentiment.py.
"""

from datetime import datetime
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

import feedparser
import requests
from loguru import logger

_RSS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

from config.nse_calendar import is_trading_day, next_trading_day
from config.settings import ALL_STOCKS
from data.quality.known_time import compute_usable_from
from data.storage.database import NewsArticle, get_session

_IST = ZoneInfo("Asia/Kolkata")
_MARKET_CLOSE_HOUR = 15   # 3:30 PM — news after this goes to next-day usable_from

# RSS feed URL templates
_RSS_FEEDS = {
    "google_news": (
        "https://news.google.com/rss/search"
        "?q={symbol}+NSE+bank+India&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    "moneycontrol": "https://www.moneycontrol.com/rss/MCtopnews.xml",
    "economic_times": (
        "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms"
    ),
}


def _compute_usable_from_for_news(published_at: datetime) -> datetime:
    """
    News published during trading hours (before 3:30 PM IST) → usable same day open.
    News published after market close or on non-trading days → next trading-day open.
    """
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=_IST)
    ist_time = published_at.astimezone(_IST)

    pub_date = ist_time.date()
    if is_trading_day(pub_date) and ist_time.hour < _MARKET_CLOSE_HOUR:
        # Use same-day open (but still after we fetch it — conservative)
        return compute_usable_from(published_at)
    else:
        return compute_usable_from(published_at)   # next trading-day open


def _fetch_feed(url: str, source: str) -> list[dict]:
    """Parse a single RSS feed URL. Returns list of article dicts."""
    try:
        # feedparser's default User-Agent is blocked by Google News and some feeds;
        # fetch with requests first and hand the bytes to feedparser
        resp = requests.get(url, headers=_RSS_HEADERS, timeout=15)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        articles = []
        for entry in feed.entries:
            pub_str = entry.get("published") or entry.get("updated")
            published_at = None
            if pub_str:
                try:
                    published_at = parsedate_to_datetime(pub_str)
                except Exception:
                    pass

            articles.append({
                "source":       source,
                "headline":     entry.get("title", "")[:500],
                "content":      entry.get("summary", "")[:2000],
                "published_at": published_at,
            })
        return articles
    except Exception as exc:
        logger.error(f"RSS fetch failed for {source} ({url}): {exc}")
        return []


def _is_relevant(article: dict, symbol: str) -> bool:
    """Rough filter: article must mention the symbol or stock name in headline/content."""
    from config.settings import STOCK_NAMES
    name = STOCK_NAMES.get(symbol, symbol).lower()
    text = (article.get("headline", "") + " " + article.get("content", "")).lower()
    return symbol.lower() in text or name in text


def fetch_and_store(symbol: str) -> int:
    """
    Fetch news for one symbol from all RSS sources and store new articles.
    Returns number of new articles inserted.
    """
    now      = datetime.utcnow().replace(tzinfo=_IST)
    inserted = 0

    # Symbol-specific Google News feed
    google_url = _RSS_FEEDS["google_news"].format(symbol=symbol)
    all_articles = _fetch_feed(google_url, "google_news")

    # General market feeds — filter by relevance
    for source, url in _RSS_FEEDS.items():
        if source == "google_news":
            continue
        for art in _fetch_feed(url, source):
            if _is_relevant(art, symbol):
                all_articles.append(art)

    with get_session() as session:
        for art in all_articles:
            pub = art["published_at"]
            if pub is None:
                pub = now

            usable_from = _compute_usable_from_for_news(pub)

            # Deduplicate by headline + source
            exists = session.query(NewsArticle).filter_by(
                symbol=symbol,
                headline=art["headline"],
                source=art["source"],
            ).first()
            if exists:
                continue

            session.add(NewsArticle(
                symbol=symbol,
                source=art["source"],
                headline=art["headline"],
                content=art["content"],
                published_at=pub,
                collected_at=now,
                usable_from=usable_from,
            ))
            inserted += 1

        session.commit()

    if inserted:
        logger.info(f"News: inserted {inserted} new articles for {symbol}")
    return inserted


def run_all() -> None:
    """Fetch news for all tracked stocks."""
    for symbol in ALL_STOCKS:
        fetch_and_store(symbol)


collect_all = run_all   # alias
