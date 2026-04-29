"""
Semantic embedding store backed by SQLite.

Embeds news articles and corporate filings using nomic-embed-text via Ollama,
then supports cosine-similarity search to retrieve the most relevant context
for a given query (e.g. "Why is AXISBANK bullish today?").

Usage:
    from llm.embeddings.store import embed_pending, search

    embed_pending("AXISBANK")                       # embed any un-embedded articles
    hits = search("AXISBANK", "loan growth outlook") # top-3 relevant snippets
"""

import hashlib
import json
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import numpy as np
from loguru import logger

from config.settings import OLLAMA_MODEL_EMBED
from data.storage.database import (
    CorporateFiling, NewsArticle, TextEmbedding, get_session,
)
from llm.ollama_client import embed

_IST = ZoneInfo("Asia/Kolkata")
_TOP_K = 3


def _text_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _article_text(article: NewsArticle) -> str:
    parts = [article.headline or "", article.content or ""]
    return " ".join(p for p in parts if p).strip()[:1000]


def _filing_text(filing: CorporateFiling) -> str:
    parts = [filing.subject or "", filing.content or ""]
    return " ".join(p for p in parts if p).strip()[:1000]


def _cosine(a: list[float], b: list[float]) -> float:
    va, vb = np.array(a, dtype=np.float32), np.array(b, dtype=np.float32)
    denom = np.linalg.norm(va) * np.linalg.norm(vb)
    return float(np.dot(va, vb) / denom) if denom > 0 else 0.0


# ── Embed pending articles/filings ────────────────────────────────────────────

def embed_pending_news(symbol: str, days_back: int = 7) -> int:
    """Embed recent news articles that don't yet have an embedding. Returns count."""
    cutoff = datetime.now(_IST) - timedelta(days=days_back)
    count = 0

    with get_session() as s:
        articles = (
            s.query(NewsArticle)
            .filter(
                NewsArticle.symbol == symbol,
                NewsArticle.published_at >= cutoff,
            )
            .all()
        )

        existing_ids = {
            row.source_id
            for row in s.query(TextEmbedding).filter(
                TextEmbedding.symbol == symbol,
                TextEmbedding.source_type == "news",
            ).all()
        }

    for article in articles:
        if article.id in existing_ids:
            continue
        text = _article_text(article)
        if not text:
            continue
        try:
            vector = embed(text)
            with get_session() as s:
                s.add(TextEmbedding(
                    symbol      = symbol,
                    source_type = "news",
                    source_id   = article.id,
                    text_hash   = _text_hash(text),
                    vector_json = json.dumps(vector),
                    model       = OLLAMA_MODEL_EMBED,
                    created_at  = datetime.now(_IST),
                ))
                s.commit()
            count += 1
        except Exception as exc:
            logger.warning(f"embed_store: failed to embed news id={article.id}: {exc}")

    return count


def embed_pending_filings(symbol: str, limit: int = 10) -> int:
    """Embed recent filings that don't yet have an embedding. Returns count."""
    count = 0

    with get_session() as s:
        filings = (
            s.query(CorporateFiling)
            .filter(CorporateFiling.symbol == symbol)
            .order_by(CorporateFiling.published_at.desc())
            .limit(limit)
            .all()
        )

        existing_ids = {
            row.source_id
            for row in s.query(TextEmbedding).filter(
                TextEmbedding.symbol == symbol,
                TextEmbedding.source_type == "filing",
            ).all()
        }

    for filing in filings:
        if filing.id in existing_ids:
            continue
        text = _filing_text(filing)
        if not text:
            continue
        try:
            vector = embed(text)
            with get_session() as s:
                s.add(TextEmbedding(
                    symbol      = symbol,
                    source_type = "filing",
                    source_id   = filing.id,
                    text_hash   = _text_hash(text),
                    vector_json = json.dumps(vector),
                    model       = OLLAMA_MODEL_EMBED,
                    created_at  = datetime.now(_IST),
                ))
                s.commit()
            count += 1
        except Exception as exc:
            logger.warning(f"embed_store: failed to embed filing id={filing.id}: {exc}")

    return count


def embed_pending(symbol: str) -> int:
    """Embed all pending news + filings for a symbol. Returns total count."""
    n = embed_pending_news(symbol) + embed_pending_filings(symbol)
    if n:
        logger.info(f"embed_store: {symbol} embedded {n} new documents")
    return n


# ── Semantic search ────────────────────────────────────────────────────────────

def search(symbol: str, query: str, top_k: int = _TOP_K) -> list[str]:
    """
    Return the top-k most relevant text snippets for a symbol + query.
    Falls back to empty list if embeddings unavailable or Ollama down.
    """
    try:
        q_vec = embed(query)
    except Exception as exc:
        logger.warning(f"embed_store: query embed failed — {exc}")
        return []

    with get_session() as s:
        rows = (
            s.query(TextEmbedding)
            .filter(TextEmbedding.symbol == symbol)
            .all()
        )

        if not rows:
            return []

        # Load source texts for all embeddings
        news_ids    = [r.source_id for r in rows if r.source_type == "news"]
        filing_ids  = [r.source_id for r in rows if r.source_type == "filing"]

        news_map: dict[int, str] = {}
        if news_ids:
            articles = s.query(NewsArticle).filter(NewsArticle.id.in_(news_ids)).all()
            news_map = {a.id: _article_text(a) for a in articles}

        filing_map: dict[int, str] = {}
        if filing_ids:
            filings = s.query(CorporateFiling).filter(CorporateFiling.id.in_(filing_ids)).all()
            filing_map = {f.id: _filing_text(f) for f in filings}

    scored: list[tuple[float, str]] = []
    for row in rows:
        try:
            vec = json.loads(row.vector_json)
        except Exception:
            continue
        sim = _cosine(q_vec, vec)
        if row.source_type == "news":
            text = news_map.get(row.source_id, "")
        else:
            text = filing_map.get(row.source_id, "")
        if text:
            scored.append((sim, text))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [text for _, text in scored[:top_k]]
