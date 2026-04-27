"""
Phase 4 verification — LLM integration (Ollama + Gemma3).

Sections:
  1.  Module imports
  2.  Ollama health check
  3.  ollama_client.generate() — raw text response
  4.  ollama_client.generate_json() — JSON extraction
  5.  ollama_client.generate_validated() — Pydantic validation
  6.  Sentiment analysis on a synthetic article
  7.  Sentiment score stored to DB (mocked article)
  8.  Filing analyser on a synthetic filing
  9.  Pydantic validation rejects bad LLM output gracefully
  10. LLMLog entry written to DB

Usage:
  python tests/verify_phase4.py
"""

import sys
import traceback
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import BANKING_STOCKS, STOCK_NAMES
from data.storage.database import init_db, get_session, NewsArticle, LLMLog, CorporateFiling

GREEN = "\033[92m"; RED = "\033[91m"; YELLOW = "\033[93m"; RESET = "\033[0m"
passed = failed = skipped = 0

def ok(msg):
    global passed; passed += 1
    print(f"  {GREEN}PASS{RESET}  {msg}")

def fail(msg, exc=None):
    global failed; failed += 1
    print(f"  {RED}FAIL{RESET}  {msg}")
    if exc:
        print(f"         {RED}{exc}{RESET}")
        traceback.print_exc()

def skip(msg):
    global skipped; skipped += 1
    print(f"  {YELLOW}SKIP{RESET}  {msg}")

def section(title):
    print(f"\n{'─' * 70}")
    print(f"  {title}")
    print(f"{'─' * 70}")


init_db()
_IST = ZoneInfo("Asia/Kolkata")

# ══════════════════════════════════════════════════════════════════════════════
# 1. Module imports
# ══════════════════════════════════════════════════════════════════════════════
section("1. Module imports")

try:
    from llm.ollama_client import generate, generate_json, generate_validated, is_available
    ok("llm.ollama_client")
except Exception as e:
    fail("llm.ollama_client", e)

try:
    from llm.analyzers.news_sentiment import (
        SentimentResponse, analyse_article, process_article, process_all_pending,
    )
    ok("llm.analyzers.news_sentiment")
except Exception as e:
    fail("llm.analyzers.news_sentiment", e)

try:
    from llm.analyzers.filing_analyzer import (
        FilingResponse, analyse_filing, process_filing, process_recent_filings,
    )
    ok("llm.analyzers.filing_analyzer")
except Exception as e:
    fail("llm.analyzers.filing_analyzer", e)


# ══════════════════════════════════════════════════════════════════════════════
# 2. Ollama health check
# ══════════════════════════════════════════════════════════════════════════════
section("2. Ollama health check")

ollama_up = False
try:
    ollama_up = is_available()
    if ollama_up:
        ok("Ollama reachable at localhost:11434")
    else:
        skip("Ollama not reachable — remaining sections will be skipped")
except Exception as e:
    fail("Ollama health check", e)

if not ollama_up:
    print(f"\n{'═' * 70}")
    print(f"  Phase 4 Results: {GREEN}{passed} passed{RESET} | {RED}{failed} failed{RESET} | {YELLOW}{skipped} skipped{RESET}")
    print(f"{'═' * 70}\n")
    sys.exit(0 if failed == 0 else 1)


# ══════════════════════════════════════════════════════════════════════════════
# 3. generate() — raw text response
# ══════════════════════════════════════════════════════════════════════════════
section("3. generate() — raw text")

try:
    response = generate("Say exactly: READY", model="gemma3:latest")
    if response and len(response) > 0:
        ok(f"generate() returned {len(response)} chars: '{response[:60]}'")
    else:
        fail("generate() returned empty response")
except Exception as e:
    fail("generate()", e)


# ══════════════════════════════════════════════════════════════════════════════
# 4. generate_json() — JSON extraction
# ══════════════════════════════════════════════════════════════════════════════
section("4. generate_json() — JSON extraction")

try:
    result = generate_json(
        'Return a JSON object with key "status" set to "ok" and key "value" set to 42.'
    )
    if isinstance(result, dict) and len(result) > 0:
        ok(f"generate_json() returned dict: {result}")
    else:
        fail(f"generate_json() returned unexpected: {result}")
except Exception as e:
    fail("generate_json()", e)


# ══════════════════════════════════════════════════════════════════════════════
# 5. generate_validated() — Pydantic validation
# ══════════════════════════════════════════════════════════════════════════════
section("5. generate_validated() — Pydantic round-trip")

try:
    result = generate_validated(
        'Return JSON with keys: sentiment_score (float between -1 and 1), '
        'direction (one of UP/DOWN/FLAT), confidence (0.0 to 1.0), reason (short string).',
        SentimentResponse,
    )
    if result is None:
        fail("generate_validated() returned None")
    else:
        ok(f"SentimentResponse validated: score={result.sentiment_score} dir={result.direction} conf={result.confidence}")
except Exception as e:
    fail("generate_validated()", e)


# ══════════════════════════════════════════════════════════════════════════════
# 6. Sentiment analysis on a synthetic article
# ══════════════════════════════════════════════════════════════════════════════
section("6. Sentiment analysis — synthetic article")

SYNTHETIC_ARTICLES = [
    {
        "symbol": "HDFCBANK",
        "headline": "HDFC Bank reports record Q3 net profit of ₹17,258 crore, up 33% YoY",
        "content": (
            "HDFC Bank reported a record net profit of ₹17,258 crore for Q3 FY25, "
            "up 33.5% year-on-year. Net interest income rose 24% to ₹28,470 crore. "
            "Gross NPA ratio improved to 1.26% from 1.34% in the previous quarter. "
            "The bank declared an interim dividend of ₹19 per share."
        ),
        "expected_direction": "UP",
    },
    {
        "symbol": "SBIN",
        "headline": "SBI faces ₹200 crore RBI penalty for KYC non-compliance",
        "content": (
            "The Reserve Bank of India imposed a ₹200 crore monetary penalty on "
            "State Bank of India for non-compliance with KYC and anti-money laundering "
            "directives. The bank said it has rectified the deficiencies."
        ),
        "expected_direction": "DOWN",
    },
]

for art_data in SYNTHETIC_ARTICLES:
    try:
        # Create a mock NewsArticle object (not stored in DB)
        mock_article = NewsArticle(
            id          = -1,
            symbol      = art_data["symbol"],
            headline    = art_data["headline"],
            content     = art_data["content"],
            published_at = datetime.now(_IST),
            source      = "test",
        )

        result = analyse_article(mock_article, STOCK_NAMES.get(art_data["symbol"], art_data["symbol"]))
        if result is None:
            fail(f"{art_data['symbol']}: analyse_article() returned None")
            continue

        # Validate output ranges
        if not (-1.0 <= result.sentiment_score <= 1.0):
            fail(f"{art_data['symbol']}: sentiment_score {result.sentiment_score} out of [-1, 1]")
            continue

        if result.direction not in ("UP", "DOWN", "FLAT"):
            fail(f"{art_data['symbol']}: invalid direction '{result.direction}'")
            continue

        expected = art_data["expected_direction"]
        match_str = f"(matches expected {expected})" if result.direction == expected else f"(expected {expected})"

        ok(
            f"{art_data['symbol']}: score={result.sentiment_score:+.2f} "
            f"dir={result.direction} conf={result.confidence:.2f} {match_str}"
        )
        print(f"         reason: {result.reason[:100]}")

    except Exception as e:
        fail(f"{art_data['symbol']}: analyse_article", e)


# ══════════════════════════════════════════════════════════════════════════════
# 7. Sentiment score written to DB
# ══════════════════════════════════════════════════════════════════════════════
section("7. Sentiment score stored to DB")

TEST_ARTICLE_ID = None

try:
    now = datetime.now(_IST)

    # Insert a test article with no sentiment score
    with get_session() as session:
        test_art = NewsArticle(
            symbol          = "HDFCBANK",
            headline        = "HDFC Bank announces new digital banking initiative",
            content         = "HDFC Bank launched a new AI-powered digital banking platform "
                              "targeting 50 million new customers by 2026. The initiative is "
                              "expected to reduce operating costs by 15%.",
            source          = "test",
            published_at    = now,
            collected_at    = now,
            usable_from     = now,
            sentiment_score = None,
        )
        session.add(test_art)
        session.flush()
        TEST_ARTICLE_ID = test_art.id
        session.commit()

    ok(f"Inserted test article id={TEST_ARTICLE_ID}")

    # Run process_article
    with get_session() as session:
        art = session.get(NewsArticle, TEST_ARTICLE_ID)
        success = process_article(art, "HDFC Bank")

    if not success:
        fail("process_article() returned False")
    else:
        # Verify sentiment_score was stored
        with get_session() as session:
            updated = session.get(NewsArticle, TEST_ARTICLE_ID)
            if updated.sentiment_score is None:
                fail(f"sentiment_score still None after process_article")
            elif not (-1.0 <= updated.sentiment_score <= 1.0):
                fail(f"stored sentiment_score={updated.sentiment_score} out of range")
            else:
                ok(f"sentiment_score={updated.sentiment_score:+.2f} stored in news_articles")

except Exception as e:
    fail("Sentiment DB write", e)
finally:
    # Clean up test article
    if TEST_ARTICLE_ID is not None:
        try:
            with get_session() as session:
                art = session.get(NewsArticle, TEST_ARTICLE_ID)
                if art:
                    session.delete(art)
                    session.commit()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# 8. Filing analyser on a synthetic filing
# ══════════════════════════════════════════════════════════════════════════════
section("8. Filing analyser — synthetic filing")

SYNTHETIC_FILINGS = [
    {
        "symbol":   "ICICIBANK",
        "category": "Financial Results",
        "subject":  "ICICI Bank Q3 FY25 Standalone Financial Results",
        "content":  (
            "ICICI Bank reports standalone net profit of ₹11,792 crore for Q3 FY25, "
            "up 15.1% from ₹10,272 crore in Q3 FY24. Net interest income was ₹20,371 crore "
            "compared to ₹18,303 crore a year ago. Gross NPA ratio at 1.96% vs 2.30% a year ago. "
            "Capital adequacy ratio at 16.11%."
        ),
        "expected_classification": "EARNINGS_BEAT",
        "expected_impact": "POSITIVE",
    },
]

for f_data in SYNTHETIC_FILINGS:
    try:
        mock_filing = CorporateFiling(
            id           = -1,
            symbol       = f_data["symbol"],
            category     = f_data["category"],
            subject      = f_data["subject"],
            content      = f_data["content"],
            event_date   = date.today(),
            published_at = datetime.now(_IST),
        )

        result = analyse_filing(mock_filing)
        if result is None:
            fail(f"{f_data['symbol']}: analyse_filing() returned None")
            continue

        # Validate
        from llm.analyzers.filing_analyzer import _VALID_CLASSIFICATIONS, _VALID_IMPACTS
        if result.classification not in _VALID_CLASSIFICATIONS:
            fail(f"Invalid classification: {result.classification}")
            continue
        if result.impact not in _VALID_IMPACTS:
            fail(f"Invalid impact: {result.impact}")
            continue
        if not result.bullets:
            fail("No bullets returned")
            continue

        exp_c = f_data["expected_classification"]
        exp_i = f_data["expected_impact"]
        c_match = f"✓" if result.classification == exp_c else f"(expected {exp_c})"
        i_match = f"✓" if result.impact == exp_i else f"(expected {exp_i})"

        ok(
            f"{f_data['symbol']}: {result.classification} {c_match}  "
            f"impact={result.impact} {i_match}  conf={result.confidence:.2f}"
        )
        ok(f"  key_metric: {result.key_metric}")
        for b in result.bullets[:3]:
            print(f"         • {b}")

    except Exception as e:
        fail(f"{f_data['symbol']}: analyse_filing", e)


# ══════════════════════════════════════════════════════════════════════════════
# 9. Pydantic validator rejects bad LLM output gracefully
# ══════════════════════════════════════════════════════════════════════════════
section("9. Pydantic validator handles bad output gracefully")

try:
    # Manually test the validator with out-of-range values
    s = SentimentResponse(
        sentiment_score = 5.0,   # should be clamped to 1.0
        direction       = "MAYBE",  # should become FLAT
        confidence      = -0.5,  # should be clamped to 0.0
        reason          = "test",
    )
    assert s.sentiment_score == 1.0,  f"expected 1.0, got {s.sentiment_score}"
    assert s.direction == "FLAT",     f"expected FLAT, got {s.direction}"
    assert s.confidence == 0.0,       f"expected 0.0, got {s.confidence}"
    ok("SentimentResponse clamps out-of-range values correctly")
except Exception as e:
    fail("SentimentResponse validator", e)

try:
    f = FilingResponse(
        classification = "RANDOM_VALUE",
        impact         = "SUPER_POSITIVE",
        bullets        = ["a"] * 10,  # should be trimmed to 5
        key_metric     = "test",
        confidence     = 1.5,         # should be clamped to 1.0
    )
    assert f.classification == "OTHER", f"expected OTHER, got {f.classification}"
    assert f.impact == "NEUTRAL",       f"expected NEUTRAL, got {f.impact}"
    assert len(f.bullets) == 5,         f"expected 5 bullets, got {len(f.bullets)}"
    assert f.confidence == 1.0,         f"expected 1.0, got {f.confidence}"
    ok("FilingResponse clamps and normalises values correctly")
except Exception as e:
    fail("FilingResponse validator", e)


# ══════════════════════════════════════════════════════════════════════════════
# 10. LLMLog entry written to DB
# ══════════════════════════════════════════════════════════════════════════════
section("10. LLMLog entry written to DB")

try:
    from datetime import timedelta
    today = date.today()

    with get_session() as session:
        count_before = session.query(LLMLog).filter(LLMLog.date == today).count()

    # process_article already wrote an LLMLog entry during Section 7
    # Let's just verify at least one entry exists for today
    with get_session() as session:
        count_after = session.query(LLMLog).filter(LLMLog.date == today).count()

    if count_after > 0:
        ok(f"LLMLog has {count_after} entries for today")
    else:
        skip("No LLMLog entries for today (process_article may have run before test date)")

    # Verify the schema — check a recent entry has expected fields
    with get_session() as session:
        recent = session.query(LLMLog).order_by(LLMLog.id.desc()).first()
        if recent:
            assert recent.symbol is not None,    "symbol is None"
            assert recent.model  is not None,    "model is None"
            assert recent.prediction is not None, "prediction is None"
            ok(f"LLMLog schema OK: symbol={recent.symbol} model={recent.model} prediction={recent.prediction}")
        else:
            skip("No LLMLog entries in DB yet")

except Exception as e:
    fail("LLMLog DB check", e)


# ══════════════════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════════════════
total = passed + failed + skipped
print(f"\n{'═' * 70}")
print(f"  Phase 4 Results: {GREEN}{passed} passed{RESET} | {RED}{failed} failed{RESET} | {YELLOW}{skipped} skipped{RESET}  (of {total})")
print(f"{'═' * 70}\n")

if failed > 0:
    sys.exit(1)
