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

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ── Banking Stocks ─────────────────────────────────────────────────────────────
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

# yfinance symbols — used ONLY for fallback / backfill (Phase 1 fallback path)
BANKING_STOCKS_YF = [f"{s}.NS" for s in BANKING_STOCKS]

STOCK_NAMES = {
    "HDFCBANK":   "HDFC Bank",
    "ICICIBANK":  "ICICI Bank",
    "SBIN":       "State Bank of India",
    "AXISBANK":   "Axis Bank",
    "KOTAKBANK":  "Kotak Mahindra Bank",
    "BANKBARODA": "Bank of Baroda",
    "FEDERALBNK": "Federal Bank",
}

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
