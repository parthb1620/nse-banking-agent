"""
Trade thesis writer — uses Gemma4:e4b to synthesise a 3-5 sentence trade thesis
from technical signals, recent sentiment scores, and semantically retrieved context.

Called by morning_scan for each top BUY signal before the risk manager runs.
"""

from loguru import logger

from config.settings import OLLAMA_MODEL_SYNTHESIZER
from llm.ollama_client import generate

_THESIS_PROMPT = """\
You are a concise trading desk analyst writing a morning briefing for Indian banking stocks.

STOCK: {symbol} ({stock_name})
DATE: {today}

TECHNICAL PICTURE:
{technical_summary}

RECENT SENTIMENT (scored -1 to +1):
{sentiment_summary}

RELEVANT CONTEXT (from filings and news):
{context_snippets}

Write a focused 3-5 sentence trade thesis for a long swing trade entry today.
Cover: (1) why technicals set up, (2) what the news/sentiment says, (3) key risk to watch.
Be direct. No greetings. No recommendations for buy/sell — only describe the picture.
"""


def write_thesis(
    symbol: str,
    stock_name: str,
    today: str,
    technical_summary: str,
    sentiment_summary: str,
    context_snippets: list[str],
) -> str:
    """
    Returns a plain-text thesis paragraph, or an empty string on failure.
    """
    snippets_text = "\n".join(f"- {s[:200]}" for s in context_snippets) if context_snippets else "(none available)"

    prompt = _THESIS_PROMPT.format(
        symbol            = symbol,
        stock_name        = stock_name,
        today             = today,
        technical_summary = technical_summary or "(no data)",
        sentiment_summary = sentiment_summary or "(no recent sentiment)",
        context_snippets  = snippets_text,
    )

    try:
        text = generate(prompt, model=OLLAMA_MODEL_SYNTHESIZER, num_predict=400)
        return text.strip()
    except Exception as exc:
        logger.warning(f"thesis_writer: {symbol} failed — {exc}")
        return ""
