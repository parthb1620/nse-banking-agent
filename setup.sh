#!/usr/bin/env bash
# NSE Banking Agent — one-shot setup script
# Usage:
#   chmod +x setup.sh && ./setup.sh          # full setup
#   ./setup.sh --skip-data                   # skip initial data collection
#
# What it does:
#   1. Checks Python 3.11+
#   2. Creates .venv and installs requirements.txt
#   3. Copies .env.example → .env (if .env absent)
#   4. Creates data_store/ and logs/ directories
#   5. Initialises the SQLite database (creates tables)
#   6. (Optional) runs a first EOD data collection
#   7. Prints the full command reference

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

SKIP_DATA=false
for arg in "$@"; do
  [[ "$arg" == "--skip-data" ]] && SKIP_DATA=true
done

# ── colours ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "${GREEN}✔ $*${NC}"; }
warn() { echo -e "${YELLOW}⚠ $*${NC}"; }
fail() { echo -e "${RED}✘ $*${NC}"; exit 1; }
step() { echo -e "\n${YELLOW}▶ $*${NC}"; }

# ── Step 1: Python version ─────────────────────────────────────────────────────
step "Checking Python version"

PYTHON=""
for cmd in python3.13 python3.12 python3.11 python3; do
  if command -v "$cmd" &>/dev/null; then
    ver=$("$cmd" -c "import sys; print(sys.version_info[:2])")
    if "$cmd" -c "import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)" 2>/dev/null; then
      PYTHON="$cmd"
      break
    fi
  fi
done

[[ -z "$PYTHON" ]] && fail "Python 3.11+ required. Install from https://python.org or via pyenv."
ok "Using $($PYTHON --version)"

# ── Step 2: Virtual environment ────────────────────────────────────────────────
step "Setting up virtual environment"

if [[ ! -d ".venv" ]]; then
  "$PYTHON" -m venv .venv
  ok "Created .venv"
else
  ok ".venv already exists — skipping creation"
fi

# Activate for the rest of this script
# shellcheck disable=SC1091
source .venv/bin/activate
ok "Activated .venv ($(.venv/bin/python --version))"

# ── Step 3: Install dependencies ───────────────────────────────────────────────
step "Installing dependencies"

pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
ok "All packages installed"

# ── Step 4: .env file ──────────────────────────────────────────────────────────
step "Checking .env"

if [[ ! -f ".env" ]]; then
  cp .env.example .env
  warn ".env created from .env.example — edit it and add your GROWW_API_KEY before running."
else
  ok ".env already present"
fi

# ── Step 5: Directories ────────────────────────────────────────────────────────
step "Creating required directories"

mkdir -p data_store logs
ok "data_store/ and logs/ ready"

# ── Step 6: Initialise database ────────────────────────────────────────────────
step "Initialising database"

.venv/bin/python - <<'EOF'
from data.storage.database import init_db
init_db()
print("Database tables created.")
EOF
ok "Database ready"

# ── Step 7: First data collection (optional) ───────────────────────────────────
if [[ "$SKIP_DATA" == false ]]; then
  step "Running first data collection (use --skip-data to skip)"

  warn "Fetching fundamentals from Screener.in (~2 min, rate-limited)..."
  .venv/bin/python -c "from data.collectors.fundamentals import run_all; run_all()" || warn "Fundamentals fetch failed — continue manually"

  warn "Running EOD collection (Bhavcopy + Groww gap-fill + quality check)..."
  .venv/bin/python -m scheduler.daily_runner once || warn "EOD collection failed — check .env credentials"

  warn "Computing banking metrics..."
  .venv/bin/python -c "from analysis.fundamental.banking_metrics import run_all; run_all()" || warn "Banking metrics failed"

  ok "Initial data collection done"
else
  warn "Skipped data collection (--skip-data). Run manually when ready."
fi

# ── Done: command reference ────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Setup complete. Activate the venv before running commands:${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
cat <<'COMMANDS'

  source .venv/bin/activate    # activate once per terminal session

  # ── Scheduler (blocking, runs 08:30 + 16:15 IST daily) ──
  python -m scheduler.daily_runner

  # ── Manual one-offs ──────────────────────────────────────
  python -m scheduler.daily_runner morning    # morning scan + Telegram alert
  python -m scheduler.daily_runner eod        # EOD report + Telegram alert
  python -m scheduler.daily_runner once       # EOD data collection right now

  # ── Dashboard ────────────────────────────────────────────
  streamlit run dashboard/app.py              # open http://localhost:8501

  # ── Data collection ──────────────────────────────────────
  python -c "from data.collectors.fundamentals import run_all; run_all()"
  python -c "from data.collectors.news_collector import run_all; run_all()"
  python -c "from llm.analyzers.news_sentiment import process_all_pending; process_all_pending()"
  python -c "from analysis.fundamental.banking_metrics import run_all; run_all()"

  # ── Manual paper trading ─────────────────────────────────
  python -m scheduler.daily_runner once                                    # 1. get prices
  python -c "from analysis.technical.signals import generate_all; generate_all()"  # 2. signals
  python -m paper_trading.simulator                                        # 3. enter trades
  python -m paper_trading.tracker                                          # 4. check exits + P&L

COMMANDS

echo -e "${YELLOW}  Edit .env to add GROWW_API_KEY, TELEGRAM_BOT_TOKEN, etc.${NC}"
echo ""
