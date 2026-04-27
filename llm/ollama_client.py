"""
Ollama REST API client.

Wraps the /api/generate endpoint with:
  - Timeout (30 s default)
  - Exponential-backoff retry (3 attempts)
  - Strict JSON extraction from response text
  - Temperature 0.1 / top_p 0.9 for deterministic finance outputs

Usage:
    from llm.ollama_client import generate, generate_json

    raw  = generate("Summarise this news article in 3 bullets: ...")
    data = generate_json("Return JSON with key sentiment: ...", SomePydanticModel)
"""

import json
import re
import time
from typing import Optional, Type, TypeVar

import requests
from loguru import logger
from pydantic import BaseModel, ValidationError

from config.settings import (
    LLM_NUM_PREDICT, LLM_TEMPERATURE, LLM_TOP_P, OLLAMA_BASE_URL, OLLAMA_MODEL,
)

_GENERATE_URL = f"{OLLAMA_BASE_URL}/api/generate"
_TIMEOUT      = 60          # seconds per request
_MAX_RETRIES  = 3
_RETRY_DELAY  = 2           # seconds, doubled each attempt

T = TypeVar("T", bound=BaseModel)


# ── Core generate ──────────────────────────────────────────────────────────────

def generate(prompt: str, model: str = OLLAMA_MODEL) -> str:
    """
    Send a prompt to Ollama and return the full response text.
    Retries up to _MAX_RETRIES times on transient errors.
    Raises RuntimeError if all attempts fail.
    """
    payload = {
        "model":  model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature":  LLM_TEMPERATURE,
            "top_p":        LLM_TOP_P,
            "num_predict":  LLM_NUM_PREDICT,
        },
    }

    last_exc: Optional[Exception] = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = requests.post(_GENERATE_URL, json=payload, timeout=_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            text = data.get("response", "").strip()
            if not text:
                raise ValueError("Empty response from Ollama")
            return text
        except Exception as exc:
            last_exc = exc
            logger.warning(f"Ollama attempt {attempt}/{_MAX_RETRIES} failed: {exc}")
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_DELAY * (2 ** (attempt - 1)))

    raise RuntimeError(f"Ollama unavailable after {_MAX_RETRIES} attempts: {last_exc}")


def generate_json(prompt: str, model: str = OLLAMA_MODEL) -> dict:
    """
    Call Ollama and extract a JSON object from the response.
    Tries json_mode first; falls back to regex extraction from free text.
    Returns empty dict if no valid JSON found.
    """
    # Instruct the model explicitly to return only JSON
    json_prompt = (
        f"{prompt}\n\n"
        "IMPORTANT: Respond with ONLY a valid JSON object. "
        "No explanation, no markdown, no code fences. Just the raw JSON."
    )

    text = generate(json_prompt, model)

    # Attempt 1: entire response is JSON
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Attempt 2: extract first {...} block
    match = re.search(r"\{.*?\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    # Attempt 3: extract largest {...} block (handles nested keys)
    matches = re.findall(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)?\}", text, re.DOTALL)
    for m in sorted(matches, key=len, reverse=True):
        try:
            return json.loads(m)
        except json.JSONDecodeError:
            continue

    logger.warning(f"Could not extract JSON from Ollama response: {text[:200]}")
    return {}


def generate_validated(
    prompt:       str,
    model_class:  Type[T],
    model:        str = OLLAMA_MODEL,
) -> Optional[T]:
    """
    Call Ollama, extract JSON, validate against a Pydantic model.
    Returns a validated Pydantic instance or None on failure.
    """
    raw = generate_json(prompt, model)
    if not raw:
        return None
    try:
        return model_class(**raw)
    except (ValidationError, TypeError) as exc:
        logger.warning(f"Pydantic validation failed for {model_class.__name__}: {exc} | raw={raw}")
        return None


def is_available() -> bool:
    """Quick health-check — returns True if Ollama is reachable."""
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        return resp.status_code == 200
    except Exception:
        return False
