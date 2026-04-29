"""
Risk manager — uses deepseek-r1:8b to stress-test a trade thesis.

DeepSeek R1 reasons internally with chain-of-thought (<think> tags) before
producing its answer. We strip the thinking block and only surface the final
assessment. The model is deliberately given a contrarian persona to surface
risks the primary model may have missed.

Returns a RiskAssessment with:
  risk_level:     LOW | MEDIUM | HIGH
  recommendation: PROCEED | CAUTION | SKIP
  concerns:       list of up to 3 specific risk factors
  summary:        one-sentence risk verdict
"""

import re

from loguru import logger
from pydantic import BaseModel, Field, field_validator

from config.settings import LLM_RISK_NUM_PREDICT, OLLAMA_MODEL_RISK
from llm.ollama_client import generate_validated

_VALID_RISK_LEVELS     = {"LOW", "MEDIUM", "HIGH"}
_VALID_RECOMMENDATIONS = {"PROCEED", "CAUTION", "SKIP"}

_RISK_PROMPT = """\
You are a skeptical risk manager at an Indian equity fund. Your job is to find
reasons why the trade below could FAIL. Be contrarian — assume the bullish thesis
is wrong until proven otherwise.

TRADE SETUP:
{thesis}

SIGNAL STRENGTH: {strength}/10
STOP DISTANCE: {stop_pct:.1f}% below entry
TARGET DISTANCE: {target_pct:.1f}% above entry

Assess the risks. Reply with ONLY a JSON object — no explanation outside the JSON:
{{
  "risk_level": "<LOW | MEDIUM | HIGH>",
  "recommendation": "<PROCEED | CAUTION | SKIP>",
  "concerns": ["<risk 1>", "<risk 2>", "<risk 3 (optional)>"],
  "summary": "<one sentence risk verdict>"
}}

Guidelines:
- LOW / PROCEED: clear setup, strong signal, tight stop, no macro headwinds
- MEDIUM / CAUTION: mixed signals or macro uncertainty — smaller size or wait for confirmation
- HIGH / SKIP: conflicting signals, wide stop, sector headwinds, or news risk
"""


class RiskAssessment(BaseModel):
    risk_level:     str        = Field(description="LOW | MEDIUM | HIGH")
    recommendation: str        = Field(description="PROCEED | CAUTION | SKIP")
    concerns:       list[str]  = Field(description="Up to 3 specific risk factors")
    summary:        str        = Field(description="One-sentence risk verdict")

    @field_validator("risk_level")
    @classmethod
    def validate_risk(cls, v):
        v = v.upper().strip()
        return v if v in _VALID_RISK_LEVELS else "MEDIUM"

    @field_validator("recommendation")
    @classmethod
    def validate_rec(cls, v):
        v = v.upper().strip()
        return v if v in _VALID_RECOMMENDATIONS else "CAUTION"

    @field_validator("concerns")
    @classmethod
    def limit_concerns(cls, v):
        return [str(c)[:200] for c in (v or [])[:3]]

    @field_validator("summary")
    @classmethod
    def limit_summary(cls, v):
        return str(v)[:300]


def _strip_thinking(text: str) -> str:
    """Remove DeepSeek R1 <think>...</think> chain-of-thought block before JSON parsing."""
    return re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()


def assess(
    symbol: str,
    thesis: str,
    signal_strength: int,
    stop_pct: float,
    target_pct: float,
) -> RiskAssessment | None:
    """
    Run the risk-manager model against a trade thesis.
    Returns a RiskAssessment or None on failure.
    """
    if not thesis:
        return None

    prompt = _RISK_PROMPT.format(
        thesis      = thesis[:600],
        strength    = signal_strength,
        stop_pct    = abs(stop_pct),
        target_pct  = abs(target_pct),
    )

    result = generate_validated(
        prompt,
        RiskAssessment,
        model       = OLLAMA_MODEL_RISK,
        num_predict = LLM_RISK_NUM_PREDICT,
    )

    if result is None:
        logger.warning(f"risk_manager: {symbol} — no assessment returned")
        return None

    logger.info(
        f"risk_manager: {symbol} [{result.risk_level}] "
        f"{result.recommendation} | {result.summary[:80]}"
    )
    return result
