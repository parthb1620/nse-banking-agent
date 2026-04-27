"""
Groww Trade API client — dual-mode (SDK or REST).

Mode is selected by GROWW_USE_SDK in .env:
  GROWW_USE_SDK=true   → uses `growwapi` Python SDK  (pip install growwapi pyotp)
  GROWW_USE_SDK=false  → uses raw requests (default, no extra install)

Both modes share identical public functions:
  get_access_token()                    → str | None
  fetch_historical(symbol, start, end)  → list[dict]
  store_historical(symbol, rows)        → int
  fetch_ltp(symbols)                    → dict[str, float]
  fetch_holdings()                      → list[dict]
  backfill(symbol, years)               → int

Auth:
  GROWW_API_KEY    = your API key (from groww.in/trade-api/api-keys)
  GROWW_API_SECRET = your API secret
  Token is generated via SHA256(secret + epoch) and cached until 06:00 IST.

Historical data note:
  The /v1/historical/candles endpoint requires a backtesting subscription.
  If you get 403 on historical data, Groww falls back to yfinance automatically
  in the EOD collection job — existing OHLCV data is unaffected.
"""

import hashlib
import os
import time
from datetime import date, datetime, timedelta, timezone

import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import GROWW_API_KEY, GROWW_API_SECRET, GROWW_BASE_URL
from data.storage.database import OHLCVDaily, get_session

# ── Mode selection ─────────────────────────────────────────────────────────────

_USE_SDK = os.getenv("GROWW_USE_SDK", "false").lower() == "true"

_GrowwAPI = None  # populated below when SDK mode is active
if _USE_SDK:
    try:
        from growwapi import GrowwAPI as _GrowwAPI  # type: ignore[assignment]
        logger.info("Groww: using growwapi SDK mode")
    except ImportError:
        logger.warning("Groww: growwapi not installed — falling back to REST. Run: pip install growwapi")
        _USE_SDK = False

_INSTRUMENTS_CSV_URL = "https://growwapi-assets.groww.in/instruments/instrument.csv"
_TOKEN_ENDPOINT      = "/v1/token/api/access"
_HISTORY_ENDPOINT    = "/v1/historical/candle/range"
_LTP_ENDPOINT        = "/v1/live-data/ltp"
_HOLDINGS_ENDPOINT   = "/v1/holdings/user"

_MAX_DAYS_PER_REQUEST = 180

# ── Shared token cache ─────────────────────────────────────────────────────────

_cached_token:  str | None = None
_token_expiry:  datetime | None = None
_sdk_instance = None   # cached GrowwAPI object (SDK mode)


# ══════════════════════════════════════════════════════════════════════════════
# Auth
# ══════════════════════════════════════════════════════════════════════════════

def get_access_token() -> str | None:
    """
    Return a valid Bearer access token.
    Cached until 06:00 IST daily. Auto-refreshes on expiry.
    Returns None if credentials are missing or auth fails.
    """
    global _cached_token, _token_expiry

    if not GROWW_API_KEY:
        logger.error("Groww: GROWW_API_KEY not set in .env")
        return None

    # Without secret — use API key directly as Bearer token (Method 1)
    if not GROWW_API_SECRET:
        return GROWW_API_KEY

    # Check cache
    now = datetime.now(timezone.utc)
    if _cached_token and _token_expiry and now < _token_expiry:
        return _cached_token

    if _USE_SDK:
        return _sdk_get_token()
    return _rest_get_token()


def _rest_get_token() -> str | None:
    global _cached_token, _token_expiry

    timestamp = str(int(time.time()))
    checksum  = _sha256(GROWW_API_SECRET + timestamp)

    try:
        resp = requests.post(
            f"{GROWW_BASE_URL}{_TOKEN_ENDPOINT}",
            headers={
                "Authorization": f"Bearer {GROWW_API_KEY}",
                "Content-Type":  "application/json",
                "Accept":        "application/json",
                "X-API-VERSION": "1.0",
            },
            json={"key_type": "approval", "checksum": checksum, "timestamp": timestamp},
            timeout=15,
        )
        resp.raise_for_status()
        body  = resp.json()
        token = (body.get("payload") or body).get("token")
        if not token:
            logger.error(f"Groww: token not in response — {resp.text[:200]}")
            return None
        _cache_token(token)
        return token
    except requests.HTTPError as exc:
        logger.error(f"Groww: token fetch {exc.response.status_code}: {exc.response.text[:200]}")
    except Exception as exc:
        logger.error(f"Groww: token fetch error: {exc}")
    return None


def _sdk_get_token() -> str | None:
    global _cached_token, _token_expiry, _sdk_instance

    try:
        token = _GrowwAPI.get_access_token(api_key=GROWW_API_KEY, secret=GROWW_API_SECRET)
        _sdk_instance = _GrowwAPI(token)
        _cache_token(token)
        return token
    except Exception as exc:
        logger.error(f"Groww SDK: token fetch error: {exc}")
        return None


def _cache_token(token: str) -> None:
    global _cached_token, _token_expiry
    from zoneinfo import ZoneInfo
    ist        = ZoneInfo("Asia/Kolkata")
    now_ist    = datetime.now(ist)
    expiry_ist = now_ist.replace(hour=6, minute=0, second=0, microsecond=0)
    if now_ist >= expiry_ist:
        expiry_ist += timedelta(days=1)
    _cached_token = token
    _token_expiry = expiry_ist.astimezone(timezone.utc)
    logger.info(f"Groww: access token cached (expires {expiry_ist.strftime('%H:%M IST')})")


def _sha256(text: str) -> str:
    h = hashlib.sha256()
    h.update(text.encode("utf-8"))
    return h.hexdigest()


def _rest_headers() -> dict:
    token = get_access_token()
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}", "Accept": "application/json", "X-API-VERSION": "1.0"}


def _sdk():
    """Return an initialised GrowwAPI instance, refreshing token if needed."""
    global _sdk_instance
    token = get_access_token()
    if not token:
        return None
    if _sdk_instance is None:
        _sdk_instance = _GrowwAPI(token)
    return _sdk_instance




# ══════════════════════════════════════════════════════════════════════════════
# Historical OHLCV
# ══════════════════════════════════════════════════════════════════════════════

def fetch_historical(symbol: str, start: date, end: date) -> list[dict]:
    """
    Fetch daily OHLCV candles for one NSE symbol.
    Returns [] and logs a warning (not error) on 403 subscription errors.
    """
    if _USE_SDK:
        return _sdk_fetch_historical(symbol, start, end)
    return _rest_fetch_historical(symbol, start, end)


# ── SDK path ───────────────────────────────────────────────────────────────────

def _sdk_fetch_historical(symbol: str, start: date, end: date) -> list[dict]:
    client = _sdk()
    if not client:
        return []

    all_rows: list[dict] = []
    chunk_start = start

    while chunk_start <= end:
        chunk_end = min(end, chunk_start + timedelta(days=_MAX_DAYS_PER_REQUEST - 1))
        try:
            # Omit interval_in_minutes — 1440 returns 0 candles for same-day queries
            # (trading day is ~6.25 hrs, not 24 hrs). Get 1-min candles and aggregate.
            resp = client.get_historical_candle_data(
                trading_symbol=symbol,
                exchange="NSE",
                segment="CASH",
                start_time=f"{chunk_start} 09:15:00",
                end_time=f"{chunk_end} 15:15:00",
            )
            candles = (resp or {}).get("candles", [])
            all_rows.extend(_aggregate_to_daily(_parse_candles(candles)))
        except Exception as exc:
            err = str(exc)
            if "403" in err or "forbidden" in err.lower():
                logger.warning(f"Groww SDK: historical data forbidden for {symbol}: {err[:100]}")
                return []
            logger.error(f"Groww SDK: historical fetch failed for {symbol}: {exc}")
            return []

        chunk_start = chunk_end + timedelta(days=1)

    logger.info(f"Groww SDK: fetched {len(all_rows)} candles for {symbol} ({start}–{end})")
    return all_rows


# ── REST path ──────────────────────────────────────────────────────────────────

def _rest_fetch_historical(symbol: str, start: date, end: date) -> list[dict]:
    headers = _rest_headers()
    if not headers:
        return []

    all_rows: list[dict] = []
    chunk_start = start

    while chunk_start <= end:
        chunk_end = min(end, chunk_start + timedelta(days=_MAX_DAYS_PER_REQUEST - 1))
        rows = _rest_fetch_chunk(headers, symbol, chunk_start, chunk_end)
        if rows is None:
            return []
        all_rows.extend(rows)
        chunk_start = chunk_end + timedelta(days=1)

    logger.info(f"Groww REST: fetched {len(all_rows)} candles for {symbol} ({start}–{end})")
    return all_rows


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _rest_fetch_chunk(headers: dict, symbol: str, start: date, end: date) -> list[dict] | None:
    # Omit interval_in_minutes — 1440 returns 0 candles for same-day queries.
    # Get 1-min candles and aggregate to daily.
    params = {
        "exchange":       "NSE",
        "segment":        "CASH",
        "trading_symbol": symbol,
        "start_time":     f"{start} 09:15:00",
        "end_time":       f"{end} 15:15:00",
    }
    resp = requests.get(f"{GROWW_BASE_URL}{_HISTORY_ENDPOINT}", headers=headers, params=params, timeout=30)

    if resp.status_code in (401, 403):
        logger.warning(f"Groww REST: {resp.status_code} on historical fetch for {symbol} — {resp.text[:200]}")
        return None

    resp.raise_for_status()
    body = resp.json()
    data = body.get("payload") or body.get("data") or body
    return _aggregate_to_daily(_parse_candles(data.get("candles", [])))


# ── Daily aggregator (for 1-minute candle responses) ──────────────────────────

def _aggregate_to_daily(candles: list[dict]) -> list[dict]:
    """
    Group intraday (1-minute) candles into daily OHLCV rows.
    Input candles must have keys: date, open, high, low, close, volume.
    """
    from collections import defaultdict
    by_date: dict = defaultdict(list)
    for c in candles:
        by_date[c["date"]].append(c)
    result = []
    for d, rows in sorted(by_date.items()):
        result.append({
            "date":   d,
            "open":   rows[0]["open"],
            "high":   max(r["high"] for r in rows),
            "low":    min(r["low"]  for r in rows),
            "close":  rows[-1]["close"],
            "volume": sum(r["volume"] for r in rows),
        })
    return result


# ── Shared candle parser ───────────────────────────────────────────────────────

def _parse_candles(candles: list) -> list[dict]:
    rows = []
    for c in candles:
        if len(c) < 6:
            continue
        ts = c[0]
        if isinstance(ts, (int, float)):
            # > 1e10 → epoch milliseconds; otherwise epoch seconds
            ts_sec = ts / 1000 if ts > 1e10 else ts
            candle_date = datetime.fromtimestamp(ts_sec, tz=timezone.utc).date()
        else:
            candle_date = datetime.fromisoformat(str(ts)).date()
        rows.append({
            "date":   candle_date,
            "open":   float(c[1]),
            "high":   float(c[2]),
            "low":    float(c[3]),
            "close":  float(c[4]),
            "volume": int(c[5]),
        })
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# Store
# ══════════════════════════════════════════════════════════════════════════════

def store_historical(symbol: str, rows: list[dict]) -> int:
    """Upsert candles into ohlcv_daily. Never overwrites nse_bhavcopy rows."""
    if not rows:
        return 0

    from sqlalchemy.dialects.sqlite import insert

    now = datetime.now(timezone.utc)
    records = [{
        "symbol":             symbol,
        "date":               r["date"],
        "open":               r.get("open"),
        "high":               r.get("high"),
        "low":                r.get("low"),
        "close":              r.get("close"),
        "adjusted_close":     r.get("close"),
        "volume":             r.get("volume"),
        "source":             "groww",
        "is_adjusted":        False,
        "needs_verification": False,
        "collected_at":       now,
    } for r in rows]

    with get_session() as session:
        stmt = insert(OHLCVDaily).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol", "date"],
            set_={k: stmt.excluded[k] for k in records[0] if k not in ("symbol", "date")},
            where=(OHLCVDaily.__table__.c.source != "nse_bhavcopy"),
        )
        session.execute(stmt)
        session.commit()

    logger.info(f"Groww: stored {len(records)} rows for {symbol}")
    return len(records)


# ══════════════════════════════════════════════════════════════════════════════
# Live prices (LTP)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_ltp(symbols: list[str]) -> dict[str, float]:
    """Fetch last traded price for up to 50 NSE symbols. Returns {symbol: ltp}."""
    if _USE_SDK:
        return _sdk_fetch_ltp(symbols)
    return _rest_fetch_ltp(symbols)


def _sdk_fetch_ltp(symbols: list[str]) -> dict[str, float]:
    client = _sdk()
    if not client:
        return {}
    try:
        # SDK expects ("NSE_SYMBOL1", "NSE_SYMBOL2", ...) tuple
        exchange_symbols = tuple(f"NSE_{s}" for s in symbols[:50])
        resp = client.get_ltp(exchange_trading_symbols=exchange_symbols, segment="CASH")
        result = {}
        items = resp if isinstance(resp, list) else (resp or {}).get("ltpData", [])
        for item in items:
            raw_sym = item.get("trading_symbol") or item.get("tradingSymbol") or ""
            sym = raw_sym.replace("NSE_", "")   # strip exchange prefix if present
            ltp = item.get("ltp") or item.get("last_traded_price")
            if sym and ltp is not None:
                result[sym] = float(ltp)
        return result
    except Exception as exc:
        logger.error(f"Groww SDK LTP failed: {exc}")
        return {}


def _rest_fetch_ltp(symbols: list[str]) -> dict[str, float]:
    headers = _rest_headers()
    if not headers:
        return {}
    params = [("exchange", "NSE"), ("segment", "CASH")] + [("trading_symbol", s) for s in symbols[:50]]
    try:
        resp = requests.get(f"{GROWW_BASE_URL}{_LTP_ENDPOINT}", headers=headers, params=params, timeout=15)
        if resp.status_code in (401, 403):
            logger.error(f"Groww LTP: auth error {resp.status_code}")
            return {}
        resp.raise_for_status()
        data = resp.json().get("payload") or resp.json().get("data") or {}
        result = {}
        for item in (data if isinstance(data, list) else data.get("ltpData", [])):
            sym = item.get("trading_symbol") or item.get("tradingSymbol")
            ltp = item.get("ltp") or item.get("last_traded_price")
            if sym and ltp is not None:
                result[sym] = float(ltp)
        return result
    except Exception as exc:
        logger.error(f"Groww LTP failed: {exc}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# Portfolio / holdings
# ══════════════════════════════════════════════════════════════════════════════

def fetch_holdings() -> list[dict]:
    """Fetch DEMAT holdings from Groww. Returns [] on failure."""
    if _USE_SDK:
        return _sdk_fetch_holdings()
    return _rest_fetch_holdings()


def _sdk_fetch_holdings() -> list[dict]:
    client = _sdk()
    if not client:
        return []
    try:
        resp = client.get_holdings_for_user()
        if isinstance(resp, list):
            return resp
        # SDK wraps in payload/holdings key depending on version
        return (resp or {}).get("holdings") or (resp or {}).get("payload", [])
    except Exception as exc:
        logger.error(f"Groww SDK holdings failed: {exc}")
        return []


def _rest_fetch_holdings() -> list[dict]:
    headers = _rest_headers()
    if not headers:
        return []
    try:
        resp = requests.get(f"{GROWW_BASE_URL}{_HOLDINGS_ENDPOINT}", headers=headers, timeout=15)
        if resp.status_code in (401, 403):
            logger.error(f"Groww holdings: auth error {resp.status_code}")
            return []
        resp.raise_for_status()
        data = resp.json().get("payload") or resp.json().get("data") or {}
        return data if isinstance(data, list) else data.get("holdings", [])
    except Exception as exc:
        logger.error(f"Groww holdings failed: {exc}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# Convenience
# ══════════════════════════════════════════════════════════════════════════════

def backfill(symbol: str, years: int = 3) -> int:
    """Fetch last `years` of daily data and store it."""
    end   = date.today()
    start = end.replace(year=end.year - years)
    rows  = fetch_historical(symbol, start, end)
    return store_historical(symbol, rows)
