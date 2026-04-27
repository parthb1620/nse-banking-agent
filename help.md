# Dashboard (open http://localhost:8501)

.venv/bin/streamlit run dashboard/app.py

# Scheduler (runs 08:30 + 16:15 daily, blocking)

.venv/bin/python -m scheduler.daily_runner

# Manual morning scan now

.venv/bin/python -m scheduler.daily_runner morning

# Manual EOD report now

.venv/bin/python -m scheduler.daily_runner eod

## Run commands for fetching data

# Fundamentals (Screener.in — ~2 min, rate-limited)

.venv/bin/python -c "from data.collectors.fundamentals import run_all; run_all()"

# News (RSS feeds)

.venv/bin/python -c "from data.collectors.news_collector import run_all; run_all()"

# LLM sentiment on collected news

.venv/bin/python -c "from llm.analyzers.news_sentiment import process_all_pending; process_all_pending()"

.venv/bin/python -m data.collectors.fundamentals # re-fetches + updates equity/asset fields
.venv/bin/python -c "from analysis.fundamental.banking_metrics import run_all; run_all()"

# manual paper trading steps

# 1. Download today's prices first

.venv/bin/python -m scheduler.daily_runner once

# 2. Generate today's signals

.venv/bin/python -c "from analysis.technical.signals import generate_all; generate_all()"

# 3. Enter trades

.venv/bin/python -m paper_trading.simulator

# 4. Check stops/targets + see P&L report

.venv/bin/python -m paper_trading.tracker
