"""
NSE filing analyser — uses Gemma3 via Ollama.

Analyses corporate filings (earnings results, board decisions, SEBI orders, etc.)
and returns a structured summary:
  - classification: EARNINGS_BEAT | EARNINGS_MISS | EARNINGS_INLINE | CORPORATE_ACTION |
                    REGULATORY | MANAGEMENT_CHANGE | OTHER
  - impact: POSITIVE | NEGATIVE | NEUTRAL
  - bullets: up to 5 key points from the filing
  - key_metric: the single most important number mentioned (e.g. "PAT ₹4,200 Cr")

Earnings beat/miss logic (for quarterly results):
  - BEAT   → PAT / revenue grew meaningfully YoY AND above analyst estimates (if mentioned)
  - MISS   → PAT / revenue declined YoY OR significantly missed estimates
  - INLINE → within ±5% of prior year, no strong positive/negative signals
"""

import hashlib
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from loguru import logger
from pydantic import BaseModel, Field, field_validator

from config.settings import OLLAMA_MODEL
from data.storage.database import CorporateFiling, LLMLog, get_session
from llm.ollama_client import generate_validated

_IST = ZoneInfo("Asia/Kolkata")

_VALID_CLASSIFICATIONS = {
    "EARNINGS_BEAT", "EARNINGS_MISS", "EARNINGS_INLINE",
    "CORPORATE_ACTION", "REGULATORY", "MANAGEMENT_CHANGE", "OTHER",
}
_VALID_IMPACTS = {"POSITIVE", "NEGATIVE", "NEUTRAL"}


# ── Pydantic response model ────────────────────────────────────────────────────

class FilingResponse(BaseModel):
    classification: str = Field(
        description="Type of filing: EARNINGS_BEAT | EARNINGS_MISS | EARNINGS_INLINE | "
                    "CORPORATE_ACTION | REGULATORY | MANAGEMENT_CHANGE | OTHER"
    )
    impact: str = Field(
        description="Expected price impact: POSITIVE | NEGATIVE | NEUTRAL"
    )
    bullets: list[str] = Field(
        description="Up to 5 key points from the filing (concise, factual)"
    )
    key_metric: str = Field(
        description="Single most important number or fact from this filing"
    )
    confidence: float = Field(
        description="Confidence in the classification, 0.0 to 1.0"
    )

    @field_validator("classification")
    @classmethod
    def validate_classification(cls, v):
        v = v.upper().strip()
        return v if v in _VALID_CLASSIFICATIONS else "OTHER"

    @field_validator("impact")
    @classmethod
    def validate_impact(cls, v):
        v = v.upper().strip()
        return v if v in _VALID_IMPACTS else "NEUTRAL"

    @field_validator("bullets")
    @classmethod
    def limit_bullets(cls, v):
        return [str(b)[:200] for b in (v or [])[:5]]

    @field_validator("confidence")
    @classmethod
    def clamp_confidence(cls, v):
        return max(0.0, min(1.0, float(v)))


# ── Prompt builder ─────────────────────────────────────────────────────────────

_PROMPT_TEMPLATE = """\
You are a financial analyst specialising in Indian banking stocks.

Analyse the following NSE corporate filing for {symbol} and extract structured information.

FILING CATEGORY: {category}
FILING DATE: {filing_date}
SUBJECT: {subject}

CONTENT:
{content}

Respond with a JSON object with these exact keys:
{{
  "classification": "<EARNINGS_BEAT | EARNINGS_MISS | EARNINGS_INLINE | CORPORATE_ACTION | REGULATORY | MANAGEMENT_CHANGE | OTHER>",
  "impact": "<POSITIVE | NEGATIVE | NEUTRAL>",
  "bullets": ["<key point 1>", "<key point 2>", "...up to 5 points"],
  "key_metric": "<single most important number or fact>",
  "confidence": <float 0.0 to 1.0>
}}

Guidelines:
- EARNINGS_BEAT: PAT/Net profit grew > 10% YoY or beat estimates
- EARNINGS_MISS: PAT/Net profit declined YoY or missed estimates
- EARNINGS_INLINE: PAT/Net profit within ±10% YoY, no strong signals
- CORPORATE_ACTION: dividend, split, bonus, buyback
- REGULATORY: RBI order, SEBI action, penalty, licence matter
- MANAGEMENT_CHANGE: CEO/MD/CXO appointment or resignation
- impact: how this news is likely to affect the stock price next session
- bullets: concise, factual, include numbers where present
- key_metric: e.g. "Net profit ₹4,200 Cr (+18% YoY)" or "RBI penalty ₹2 Cr"
"""


def _build_prompt(filing: CorporateFiling) -> str:
    content_snippet = (filing.content or filing.subject or "")[:1000]
    return _PROMPT_TEMPLATE.format(
        symbol      = filing.symbol,
        category    = filing.category or "GENERAL",
        filing_date = str(filing.event_date or filing.published_at),
        subject     = filing.subject or "(no subject)",
        content     = content_snippet,
    )


def _prompt_hash(prompt: str) -> str:
    return hashlib.sha256(prompt.encode()).hexdigest()[:16]


# ── Core analyser ──────────────────────────────────────────────────────────────

def analyse_filing(filing: CorporateFiling) -> Optional[FilingResponse]:
    """
    Run analysis on one filing. Returns FilingResponse or None on failure.
    Does NOT write to DB.
    """
    if not filing.subject and not filing.content:
        logger.warning(f"filing_analyzer: skipping filing id={filing.id} — no text")
        return None

    prompt = _build_prompt(filing)
    return generate_validated(prompt, FilingResponse)


def process_filing(filing: CorporateFiling) -> bool:
    """
    Analyse one filing and log the result to llm_log.
    Returns True on success.
    """
    prompt  = _build_prompt(filing)
    p_hash  = _prompt_hash(prompt)
    result  = generate_validated(prompt, FilingResponse)

    if result is None:
        logger.warning(f"filing_analyzer: failed for filing id={filing.id}")
        return False

    now = datetime.now(_IST)

    with get_session() as session:
        log_entry = LLMLog(
            symbol        = filing.symbol,
            date          = now.date(),
            model         = OLLAMA_MODEL,
            prompt_hash   = p_hash,
            response_json = str({
                "classification": result.classification,
                "impact":         result.impact,
                "key_metric":     result.key_metric,
                "confidence":     result.confidence,
                "bullets":        result.bullets,
            }),
            prediction    = result.impact,   # POSITIVE/NEGATIVE/NEUTRAL as direction proxy
            outcome       = None,
            accuracy      = None,
        )
        session.add(log_entry)
        session.commit()

    logger.info(
        f"filing_analyzer: {filing.symbol} [{result.classification}] "
        f"impact={result.impact} conf={result.confidence:.2f} | {result.key_metric}"
    )
    return True


def process_recent_filings(symbol: str, limit: int = 5) -> list[FilingResponse]:
    """
    Analyse the most recent unprocessed filings for one symbol.
    Returns a list of FilingResponse objects.
    """
    results = []

    with get_session() as session:
        filings = (
            session.query(CorporateFiling)
            .filter(CorporateFiling.symbol == symbol)
            .order_by(CorporateFiling.published_at.desc())
            .limit(limit)
            .all()
        )

        for filing in filings:
            try:
                r = analyse_filing(filing)
                if r:
                    results.append(r)
                    logger.info(
                        f"filing_analyzer: {symbol} '{filing.subject[:60]}' "
                        f"→ {r.classification} ({r.impact})"
                    )
            except Exception as exc:
                logger.error(f"filing_analyzer: {symbol} filing {filing.id} failed: {exc}")

    return results


def summarise_for_morning_scan(symbol: str) -> Optional[str]:
    """
    Return a plain-text summary of the most impactful recent filing for morning alerts.
    Returns None if no recent filings.
    """
    with get_session() as session:
        filing = (
            session.query(CorporateFiling)
            .filter(CorporateFiling.symbol == symbol)
            .order_by(CorporateFiling.published_at.desc())
            .first()
        )
        if not filing:
            return None

        result = analyse_filing(filing)
        if not result:
            return None

    lines = [f"[{result.classification}] {result.key_metric}"]
    lines += [f"  • {b}" for b in result.bullets[:3]]
    return "\n".join(lines)
