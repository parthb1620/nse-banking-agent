"""
News sentiment analyser — uses Gemma4:e4b via Ollama.

For each news article:
  1. Send headline + content snippet to Gemma3
  2. Extract a structured JSON response (validated via Pydantic)
  3. Store sentiment_score (-1.0 to +1.0) back into news_articles table
  4. Log the LLM call in llm_log for accuracy tracking

Sentiment scale:
  -1.0  Very negative (fraud, crash, major NPA spike, rating downgrade)
  -0.5  Mildly negative
   0.0  Neutral / mixed
  +0.5  Mildly positive
  +1.0  Very positive (record profit, rating upgrade, major loan growth)

Direction prediction (for accuracy tracking):
  "UP"   — article suggests price likely to rise next session
  "DOWN" — article suggests price likely to fall next session
  "FLAT" — article has no clear directional implication
"""

import hashlib
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from loguru import logger
from pydantic import BaseModel, Field, field_validator

from config.settings import ALL_STOCKS, OLLAMA_MODEL_PARSER
from data.storage.database import LLMLog, NewsArticle, get_session
from llm.ollama_client import generate_validated

_IST = ZoneInfo("Asia/Kolkata")


# ── Pydantic response model ────────────────────────────────────────────────────

class SentimentResponse(BaseModel):
    sentiment_score: float = Field(
        description="Sentiment score from -1.0 (very negative) to +1.0 (very positive)"
    )
    direction: str = Field(
        description="Price direction prediction: UP, DOWN, or FLAT"
    )
    confidence: float = Field(
        description="Confidence in direction prediction, 0.0 to 1.0"
    )
    reason: str = Field(
        description="One-sentence explanation of the sentiment"
    )

    @field_validator("sentiment_score")
    @classmethod
    def clamp_score(cls, v):
        return max(-1.0, min(1.0, float(v)))

    @field_validator("direction")
    @classmethod
    def normalise_direction(cls, v):
        v = v.upper().strip()
        if v not in ("UP", "DOWN", "FLAT"):
            return "FLAT"
        return v

    @field_validator("confidence")
    @classmethod
    def clamp_confidence(cls, v):
        return max(0.0, min(1.0, float(v)))


# ── Prompt builder ─────────────────────────────────────────────────────────────

_PROMPT_TEMPLATE = """\
You are a financial analyst specialising in Indian banking stocks.

Analyse the following news article about {symbol} ({stock_name}) and assess its \
sentiment for the stock price.

HEADLINE: {headline}

CONTENT: {content}

Today's date: {today}

Respond with a JSON object with these exact keys:
{{
  "sentiment_score": <float from -1.0 to +1.0>,
  "direction": "<UP | DOWN | FLAT>",
  "confidence": <float from 0.0 to 1.0>,
  "reason": "<one sentence explanation>"
}}

Rules:
- sentiment_score: -1.0 = very negative (NPA spike, fraud, downgrade), +1.0 = very positive (record profit, upgrade, strong growth), 0.0 = neutral
- direction: your prediction for the stock's next-session price move
- confidence: how confident you are in the direction (0.3 = uncertain, 0.8 = confident)
- reason: one sentence, factual, no speculation about exact price levels
"""


def _build_prompt(article: NewsArticle, stock_name: str) -> str:
    content_snippet = (article.content or article.headline or "")[:800]
    return _PROMPT_TEMPLATE.format(
        symbol=article.symbol,
        stock_name=stock_name,
        headline=article.headline or "(no headline)",
        content=content_snippet,
        today=datetime.now(_IST).strftime("%Y-%m-%d"),
    )


# ── Core analyser ──────────────────────────────────────────────────────────────

def analyse_article(article: NewsArticle, stock_name: str = "") -> Optional[SentimentResponse]:
    """
    Run sentiment analysis on one article.
    Returns a validated SentimentResponse or None on failure.
    Does NOT write to DB — caller decides what to persist.
    """
    if not article.headline and not article.content:
        logger.warning(f"news_sentiment: skipping article id={article.id} — no text")
        return None

    prompt = _build_prompt(article, stock_name)
    result = generate_validated(prompt, SentimentResponse, model=OLLAMA_MODEL_PARSER)
    return result


def _prompt_hash(prompt: str) -> str:
    return hashlib.sha256(prompt.encode()).hexdigest()[:16]


def process_article(article: NewsArticle, stock_name: str = "") -> bool:
    """
    Analyse one article and persist:
      - sentiment_score back to news_articles
      - LLMLog entry for accuracy tracking

    Returns True on success.
    """
    prompt   = _build_prompt(article, stock_name)
    p_hash   = _prompt_hash(prompt)
    result   = generate_validated(prompt, SentimentResponse, model=OLLAMA_MODEL_PARSER)

    if result is None:
        logger.warning(f"news_sentiment: analysis failed for article id={article.id}")
        return False

    now = datetime.now(_IST)

    with get_session() as session:
        # Update sentiment_score on the article
        db_article = session.get(NewsArticle, article.id)
        if db_article:
            db_article.sentiment_score = result.sentiment_score
            session.add(db_article)

        # Log the LLM call
        log_entry = LLMLog(
            symbol      = article.symbol,
            date        = now.date(),
            model       = OLLAMA_MODEL_PARSER,
            prompt_hash = p_hash,
            response_json = str({
                "sentiment_score": result.sentiment_score,
                "direction":       result.direction,
                "confidence":      result.confidence,
                "reason":          result.reason,
            }),
            prediction  = result.direction,  # UP / DOWN / FLAT — compared to next-day price later
            outcome     = None,              # filled in by accuracy_tracker after next session
            accuracy    = None,
        )
        session.add(log_entry)
        session.commit()

    logger.info(
        f"news_sentiment: {article.symbol} score={result.sentiment_score:+.2f} "
        f"dir={result.direction} conf={result.confidence:.2f} | {result.reason[:80]}"
    )
    return True


def process_all_pending(max_per_symbol: int = 10) -> dict[str, int]:
    """
    Process the most recent unscored articles for all tracked stocks.
    Returns {symbol: count_processed}.
    """
    from config.settings import STOCK_NAMES

    results: dict[str, int] = {}

    with get_session() as session:
        for symbol in ALL_STOCKS:
            articles = (
                session.query(NewsArticle)
                .filter(
                    NewsArticle.symbol == symbol,
                    NewsArticle.sentiment_score.is_(None),
                )
                .order_by(NewsArticle.published_at.desc())
                .limit(max_per_symbol)
                .all()
            )

            count = 0
            stock_name = STOCK_NAMES.get(symbol, symbol)
            for art in articles:
                try:
                    if process_article(art, stock_name):
                        count += 1
                except Exception as exc:
                    logger.error(f"news_sentiment: {symbol} article {art.id} failed: {exc}")

            results[symbol] = count
            if count:
                logger.info(f"news_sentiment: processed {count} articles for {symbol}")

    return results


# ── Accuracy tracker ───────────────────────────────────────────────────────────

def update_accuracy(symbol: str, log_date, actual_direction: str) -> int:
    """
    After a trading session closes, compare LLMLog predictions to actual price move.
    actual_direction: "UP" if close > prev_close, "DOWN" if close < prev_close, else "FLAT"

    Returns number of rows updated.
    """
    updated = 0
    with get_session() as session:
        rows = (
            session.query(LLMLog)
            .filter(
                LLMLog.symbol == symbol,
                LLMLog.date   == log_date,
                LLMLog.outcome.is_(None),
                LLMLog.prediction.isnot(None),
            )
            .all()
        )
        for row in rows:
            row.outcome  = actual_direction
            row.accuracy = 1.0 if row.prediction == actual_direction else 0.0
            session.add(row)
            updated += 1
        session.commit()

    return updated
