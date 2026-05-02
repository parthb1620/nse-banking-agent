import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
LOG_DIR  = BASE_DIR / "logs"

DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# ── Database ───────────────────────────────────────────────────────────────────
DB_URL = os.getenv("DB_URL", f"sqlite:///{DATA_DIR}/nse_agent.db")

# ── Groww API ──────────────────────────────────────────────────────────────────
GROWW_API_KEY    = os.getenv("GROWW_API_KEY", "")
GROWW_API_SECRET = os.getenv("GROWW_API_SECRET", "")
GROWW_BASE_URL   = "https://api.groww.in"

# ── Ollama / LLM ───────────────────────────────────────────────────────────────
OLLAMA_BASE_URL  = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL     = os.getenv("OLLAMA_MODEL", "gemma3")   # legacy fallback

# Multi-model pipeline (all served by local Ollama)
OLLAMA_MODEL_PARSER      = os.getenv("OLLAMA_MODEL_PARSER",      "gemma4:e4b")       # fast JSON parsing + synthesis
OLLAMA_MODEL_SYNTHESIZER = os.getenv("OLLAMA_MODEL_SYNTHESIZER", "gemma4:e4b")       # trade thesis writing
OLLAMA_MODEL_RISK        = os.getenv("OLLAMA_MODEL_RISK",        "deepseek-r1:8b")   # contrarian risk manager
OLLAMA_MODEL_EMBED       = os.getenv("OLLAMA_MODEL_EMBED",       "nomic-embed-text") # semantic embeddings

LLM_TEMPERATURE      = 0.1
LLM_TOP_P            = 0.9
LLM_NUM_PREDICT      = 800    # bumped from 500 — Gemma4 is more verbose than Gemma3
LLM_RISK_NUM_PREDICT = 1200   # deepseek-r1 needs more tokens for chain-of-thought

# ── Telegram ───────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
# Additional chat IDs to broadcast to (comma-separated in env, e.g. "111,222")
_extra_raw = os.getenv("TELEGRAM_EXTRA_CHAT_IDS", "1515854594")
TELEGRAM_EXTRA_CHAT_IDS: list[str] = [c.strip() for c in _extra_raw.split(",") if c.strip()]

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ── Sector Stock Lists ─────────────────────────────────────────────────────────
# NSE symbols (no suffix) — used for Bhavcopy and all NSE APIs

BANKING_STOCKS = [
    "HDFCBANK",
    "ICICIBANK",
    "SBIN",
    "AXISBANK",
    "KOTAKBANK",
    "BANKBARODA",
    "FEDERALBNK",
]

IT_STOCKS = [
    "TCS",
    "INFY",
    "WIPRO",
    "HCLTECH",
    "TECHM",
    "LTM",       # LTIMindtree (renamed from LTIM)
    "MPHASIS",
    "COFORGE",
]

FMCG_STOCKS = [
    "HINDUNILVR",
    "ITC",
    "NESTLEIND",
    "BRITANNIA",
    "MARICO",
    "DABUR",
    "GODREJCP",
]

PHARMA_STOCKS = [
    "SUNPHARMA",
    "DRREDDY",
    "CIPLA",
    "DIVISLAB",
    "LUPIN",
    "AUROPHARMA",
]

AUTO_STOCKS = [
    "MARUTI",
    "TMPV",      # Tata Motors Passenger Vehicles (demerged from TATAMOTORS, listed Oct 2025)
    # "TMLCV",   # Tata Motors Commercial Vehicles — not yet listed on NSE; add when trading begins
    "M&M",
    "BAJAJ-AUTO",
    "HEROMOTOCO",
    "EICHERMOT",
]

ENERGY_STOCKS = [
    "RELIANCE",
    "ONGC",
    "NTPC",
    "POWERGRID",
    "COALINDIA",
    "WAAREEENER",   # Waaree Energies — solar/green energy
]

METALS_STOCKS = [
    "TATASTEEL",
    "HINDALCO",
    "JSWSTEEL",
    "VEDL",
    "NMDC",
]

# Sector → symbol-list mapping (used for sector-aware reporting)
WATCHLIST: dict[str, list[str]] = {
    "Banking": BANKING_STOCKS,
    "IT":      IT_STOCKS,
    "FMCG":   FMCG_STOCKS,
    "Pharma":  PHARMA_STOCKS,
    "Auto":    AUTO_STOCKS,
    "Energy":  ENERGY_STOCKS,
    "Metals":  METALS_STOCKS,
}

# Flat list of all symbols across every sector
ALL_STOCKS: list[str] = [s for stocks in WATCHLIST.values() for s in stocks]

# yfinance symbols — used ONLY for fallback / backfill (Phase 1 fallback path)
BANKING_STOCKS_YF = [f"{s}.NS" for s in BANKING_STOCKS]
ALL_STOCKS_YF     = [f"{s}.NS" for s in ALL_STOCKS]

STOCK_NAMES = {
    # Banking
    "HDFCBANK":    "HDFC Bank",
    "ICICIBANK":   "ICICI Bank",
    "SBIN":        "State Bank of India",
    "AXISBANK":    "Axis Bank",
    "KOTAKBANK":   "Kotak Mahindra Bank",
    "BANKBARODA":  "Bank of Baroda",
    "FEDERALBNK":  "Federal Bank",
    # IT
    "TCS":         "Tata Consultancy Services",
    "INFY":        "Infosys",
    "WIPRO":       "Wipro",
    "HCLTECH":     "HCL Technologies",
    "TECHM":       "Tech Mahindra",
    "LTM":         "LTIMindtree",
    "MPHASIS":     "Mphasis",
    "COFORGE":     "Coforge",
    # FMCG
    "HINDUNILVR":  "Hindustan Unilever",
    "ITC":         "ITC",
    "NESTLEIND":   "Nestle India",
    "BRITANNIA":   "Britannia Industries",
    "MARICO":      "Marico",
    "DABUR":       "Dabur India",
    "GODREJCP":    "Godrej Consumer Products",
    # Pharma
    "SUNPHARMA":   "Sun Pharmaceutical",
    "DRREDDY":     "Dr. Reddy's Laboratories",
    "CIPLA":       "Cipla",
    "DIVISLAB":    "Divi's Laboratories",
    "LUPIN":       "Lupin",
    "AUROPHARMA":  "Aurobindo Pharma",
    # Auto
    "MARUTI":      "Maruti Suzuki",
    "TMPV":        "Tata Motors Passenger Vehicles",
    # "TMLCV":    "Tata Motors Commercial Vehicles",  # add when listed on NSE
    "M&M":         "Mahindra & Mahindra",
    "BAJAJ-AUTO":  "Bajaj Auto",
    "HEROMOTOCO":  "Hero MotoCorp",
    "EICHERMOT":   "Eicher Motors",
    # Energy
    "RELIANCE":    "Reliance Industries",
    "ONGC":        "Oil & Natural Gas Corporation",
    "NTPC":        "NTPC",
    "POWERGRID":   "Power Grid Corporation",
    "COALINDIA":   "Coal India",
    "WAAREEENER":  "Waaree Energies",
    # Metals
    "TATASTEEL":   "Tata Steel",
    "HINDALCO":    "Hindalco Industries",
    "JSWSTEEL":    "JSW Steel",
    "VEDL":        "Vedanta",
    "NMDC":        "NMDC",
}

# Reverse lookup: symbol → sector name
SYMBOL_SECTOR: dict[str, str] = {
    sym: sector
    for sector, syms in WATCHLIST.items()
    for sym in syms
}

# ALL_STOCK_NAMES is an alias kept for convenience in multi-sector modules
ALL_STOCK_NAMES = STOCK_NAMES

# ── Midcap / Smallcap additions ───────────────────────────────────────────────
# These trade alongside large-caps in long-term and short-term engines.
# All are F&O-eligible or top-200 by market cap — liquid enough for paper trading.

MIDCAP_STOCKS = [
    # IT midcap
    "LTTS",        # L&T Technology Services
    "PERSISTENT",  # Persistent Systems
    "KPIT",        # KPIT Technologies
    "TATAELXSI",   # Tata Elxsi
    # Pharma midcap
    "ALKEM",       # Alkem Laboratories
    "TORNTPHARM",  # Torrent Pharmaceuticals
    # Banking / NBFC midcap
    "BANDHANBNK",  # Bandhan Bank
    "IDFCFIRSTB",  # IDFC First Bank
    "CANBK",       # Canara Bank
    # Auto midcap
    "TVSMOTOR",    # TVS Motor Company
    "BALKRISIND",  # Balkrishna Industries
    # FMCG midcap
    "VARUNBEV",    # Varun Beverages
    "COLPAL",      # Colgate-Palmolive India
    # Metals midcap
    "APLAPOLLO",   # APL Apollo Tubes
    "JINDALSTEL",  # Jindal Steel & Power
]

MIDCAP_STOCK_NAMES: dict[str, str] = {
    "LTTS":        "L&T Technology Services",
    "PERSISTENT":  "Persistent Systems",
    "KPIT":        "KPIT Technologies",
    "TATAELXSI":   "Tata Elxsi",
    "ALKEM":       "Alkem Laboratories",
    "TORNTPHARM":  "Torrent Pharmaceuticals",
    "BANDHANBNK":  "Bandhan Bank",
    "IDFCFIRSTB":  "IDFC First Bank",
    "CANBK":       "Canara Bank",
    "TVSMOTOR":    "TVS Motor Company",
    "BALKRISIND":  "Balkrishna Industries",
    "VARUNBEV":    "Varun Beverages",
    "COLPAL":      "Colgate-Palmolive India",
    "APLAPOLLO":   "APL Apollo Tubes",
    "JINDALSTEL":  "Jindal Steel & Power",
}

# Merge into master name lookup
STOCK_NAMES.update(MIDCAP_STOCK_NAMES)
ALL_STOCK_NAMES = STOCK_NAMES

# ── Per-engine stock universes ────────────────────────────────────────────────
# Each engine operates on a specific universe; smaller = faster cycle time.

LONGTERM_UNIVERSE  = ALL_STOCKS + MIDCAP_STOCKS          # 60 stocks — weekly scan
SHORTTERM_UNIVERSE = ALL_STOCKS                           # 45 stocks — daily EOD
BTST_UNIVERSE      = ALL_STOCKS                           # 45 stocks — 14:30 IST scan
INTRADAY_UNIVERSE  = [                                    # 20 most liquid — real-time
    # Banking (highest F&O OI)
    "HDFCBANK", "ICICIBANK", "SBIN", "AXISBANK", "KOTAKBANK",
    # IT
    "TCS", "INFY", "WIPRO", "HCLTECH",
    # Diversified large-cap
    "RELIANCE", "MARUTI", "TATASTEEL", "HINDALCO", "JSWSTEEL",
    # FMCG / Pharma
    "HINDUNILVR", "ITC", "SUNPHARMA",
    # Auto
    "M&M", "BAJAJ-AUTO", "EICHERMOT",
]

# ── Engine capital allocation (fraction of PAPER_TRADING_CAPITAL) ─────────────
ENGINE_CAPITAL_SPLIT = {
    "longterm":  0.40,
    "shortterm": 0.30,
    "btst":      0.20,
    "intraday":  0.10,
}

# ── Per-engine score weights: (technical, fundamental, sentiment) ─────────────
ENGINE_WEIGHTS = {
    "longterm":  (0.20, 0.60, 0.20),
    "shortterm": (0.50, 0.30, 0.20),   # same as current defaults
    "btst":      (0.70, 0.10, 0.20),
    "intraday":  (1.00, 0.00, 0.00),   # pure price action
}

# ── Engine scheduler times (IST) ─────────────────────────────────────────────
BTST_SCAN_TIME      = "14:30"   # BTST signal scan — 45 min before close
LONGTERM_SCAN_TIME  = "08:00"   # Sunday only — weekly long-term scan

# ── Technical Indicator Parameters ────────────────────────────────────────────
EMA_PERIODS  = [9, 21, 50, 200]
RSI_PERIOD   = 14
MACD_FAST    = 12
MACD_SLOW    = 26
MACD_SIGNAL  = 9
BB_PERIOD    = 20
BB_STD       = 2
ATR_PERIOD   = 14
ADX_PERIOD   = 14

# ── Signal Thresholds ──────────────────────────────────────────────────────────
RSI_ENTRY_LOW  = 35   # only enter long when RSI is above this (not deeply oversold)
RSI_ENTRY_HIGH = 60   # only enter long when RSI is below this (not overbought)
RSI_EXIT       = 75   # exit long when RSI crosses above this
ADX_TREND_MIN  = 25   # below this = no clear trend; skip momentum signals

# ── Risk Management ────────────────────────────────────────────────────────────
PAPER_TRADING_CAPITAL  = float(os.getenv("PAPER_TRADING_CAPITAL", "500000"))
RISK_PER_TRADE_PCT     = 0.02   # risk 2% of portfolio per trade
MAX_OPEN_POSITIONS     = 3      # max simultaneous positions (one sector concentration)
MIN_RISK_REWARD        = 2.0    # minimum 1:2 R:R before entering a trade
ATR_STOP_MULTIPLIER    = 2.0    # stop_loss = entry - (ATR_STOP_MULTIPLIER × ATR_14)
DAILY_LOSS_LIMIT_PCT   = 0.03   # pause new trades if portfolio drops 3% in one day
MIN_SIGNAL_STRENGTH    = 6      # only trade BUY signals with strength >= this (4 required gates + ≥1 bonus)
FII_SELL_STREAK_DAYS   = 3      # skip new entries if FII net sellers for this many consecutive days
TRAILING_BREAKEVEN_RR  = 1.0    # move stop to breakeven when trade reaches 1:1 R
TRAILING_EMA_RR        = 1.5    # trail stop at EMA_21 when trade reaches 1.5:1 R

VOLUME_CONFIRM_MULTIPLIER = 1.2  # entry day volume must be ≥ this × 20-day average (filters weak-hand reversals)
SWING_LOW_LOOKBACK     = 10     # bars to look back for structural swing low (stop placement)
SWING_LOW_BUFFER_ATR   = 0.3    # extra buffer below swing low, expressed in ATR units

PARTIAL_PROFIT_RR      = 1.0    # book PARTIAL_PROFIT_PCT of position when price hits this R multiple
PARTIAL_PROFIT_PCT     = 0.5    # fraction of qty to book at PARTIAL_PROFIT_RR (0.5 = half)

# ── Backtesting ────────────────────────────────────────────────────────────────
# 0.40% total round-trip cost: brokerage 0.06% + STT 0.20% + exchange 0.0067% + GST ~0.012% + slippage 0.10%
BACKTEST_TRANSACTION_COST_PCT = 0.0040
INDIA_RISK_FREE_RATE          = 0.065   # 6.5% annualised for Sharpe ratio

BACKTEST_TRAIN_START = "2019-01-01"
BACKTEST_TRAIN_END   = "2022-12-31"
BACKTEST_VAL_START   = "2023-01-01"
BACKTEST_VAL_END     = "2023-12-31"
BACKTEST_TEST_START  = "2024-01-01"
BACKTEST_TEST_END    = "2024-12-31"

# ── Scoring Weights ────────────────────────────────────────────────────────────
SCORE_WEIGHT_TECHNICAL   = 0.50
SCORE_WEIGHT_FUNDAMENTAL = 0.30
SCORE_WEIGHT_SENTIMENT   = 0.20

# If LLM directional accuracy falls below this threshold, sentiment weight drops to LOW value
LLM_ACCURACY_THRESHOLD  = 0.55
LLM_ACCURACY_LOW_WEIGHT = 0.10

# ── Data Collection ────────────────────────────────────────────────────────────
# NSE Bhavcopy URL — date must be formatted as DDMMYYYY before substituting
NSE_BHAVCOPY_URL    = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv"
SCREENER_BASE_URL   = "https://www.screener.in"
SCREENER_DELAY_SEC  = 2     # 1 request per 2 seconds — Screener.in rate limit
DATA_BACKFILL_YEARS = 5

# ── Scheduler ──────────────────────────────────────────────────────────────────
SCHEDULER_TIMEZONE = "Asia/Kolkata"
MORNING_SCAN_TIME  = "08:30"   # IST — overnight news + top picks before market open
EOD_REPORT_TIME    = "16:15"   # IST — updated scores + new signals after market close

# ── Live Trading Gate ──────────────────────────────────────────────────────────
# This must be explicitly set to "true" in .env to enable live order placement.
# Keep false until 3+ months of paper trading shows positive results.
LIVE_TRADING_ENABLED = os.getenv("LIVE_TRADING_ENABLED", "false").lower() == "true"
