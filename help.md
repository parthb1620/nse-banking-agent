# NSE Multi-Sector Agent — Run Commands

---

## Dashboard

```bash
.venv/bin/streamlit run dashboard/app.py
```
Opens the web UI at `http://localhost:8501`. Shows live scores for all 38 stocks, price charts with EMAs/RSI, signals, news, paper trade P&L, and backtest runner. Leave this open while working.

---

## Scheduler (automated daily operations)

```bash
.venv/bin/python -m scheduler.daily_runner
```
**The main process.** Runs forever in the background. Fires all jobs automatically at the right times — don't run this alongside manual commands or you'll get duplicates.

Jobs fired automatically:
- `08:30 IST` — morning scan (news + top picks Telegram alert)
- `09:20–15:25 IST` — breakout monitor every 5 min (all 38 stocks)
- `14:45–15:25 IST` — late-session recovery monitor every 5 min
- `16:15 IST` — EOD data collection + report Telegram alert
- `1st of each month 07:00 IST` — walk-forward optimizer

---

## Manual one-shot jobs (run when scheduler is NOT running)

```bash
.venv/bin/python -m scheduler.daily_runner morning
```
Runs the 08:30 IST morning scan right now — collects overnight news, scores all stocks, sends the Telegram top-picks alert. Use this to test the morning alert or trigger it manually.

```bash
.venv/bin/python -m scheduler.daily_runner eod
```
Runs the 16:15 IST EOD report right now — generates signals, scores all stocks, sends the Telegram EOD summary.

```bash
.venv/bin/python -m scheduler.daily_runner once
```
Runs the full EOD data collection pipeline — downloads today's Bhavcopy, fills gaps from Groww, applies corporate actions, runs quality checks, fetches news and filings. **Run this first every day if the scheduler isn't running.**

```bash
.venv/bin/python -m scheduler.daily_runner breakout
```
Runs one breakout scan right now across all 38 stocks. Fires Telegram alert if any stock has moved ≥2% with volume confirmation or hit its 52W high. Use this to manually test the breakout alert (e.g. WAAREEENER).

```bash
.venv/bin/python -m scheduler.daily_runner intraday
```
Runs one late-session recovery scan (14:45–15:25 pattern detector). Only useful between 14:45 and 15:25 IST.

```bash
.venv/bin/python -m scheduler.daily_runner optimize
```
Runs the monthly walk-forward optimizer across all banking stocks and sends results to Telegram. Takes 5–10 minutes.

---

## Data collection (refresh specific data manually)

```bash
.venv/bin/python -c "from data.collectors.fundamentals import run_all; run_all()"
```
Fetches quarterly fundamentals from Screener.in for all 38 stocks. Rate-limited to 1 req/2 sec — takes ~2 minutes. Run after a quarterly results season.

```bash
.venv/bin/python -c "from data.collectors.news_collector import run_all; run_all()"
```
Fetches latest news from RSS feeds (Google News + MoneyControl) for all 38 stocks. Fast — ~30 seconds.

```bash
.venv/bin/python -c "from data.collectors.nse_filings import run_all; run_all()"
```
Fetches corporate announcements from NSE for all 38 stocks (last 365 days). Run this when you hear about a result or board meeting that hasn't appeared yet.

```bash
.venv/bin/python -c "from llm.analyzers.news_sentiment import process_all_pending; process_all_pending()"
```
Runs LLM sentiment analysis on news articles that don't have a sentiment score yet. Requires Ollama running locally. Updates sentiment scores used in stock scoring.

```bash
.venv/bin/python -c "from analysis.fundamental.banking_metrics import run_all; run_all()"
```
Recomputes NIM, GNPA, CASA, PCR for all banking stocks from stored fundamentals. Run after fundamentals collection to refresh banking KPI scores.

```bash
.venv/bin/python -m data.collectors.fundamentals
```
Re-fetches and updates equity/asset fields from Screener.in.

---

## Analysis

```bash
.venv/bin/python -c "from analysis.technical.signals import generate_all; generate_all()"
```
Generates today's BUY/SELL/NEUTRAL signals for all 38 stocks and stores them in the DB. Run after EOD data collection.

```bash
.venv/bin/python -c "from scoring.stock_scorer import score_all; score_all()"
```
Prints a ranked score table for all 38 stocks (Technical 50% + Fundamental 30% + Sentiment 20%). Run any time to see current rankings.

---

## Paper trading (step-by-step manual flow)

```bash
# Step 1 — get today's prices
.venv/bin/python -m scheduler.daily_runner once

# Step 2 — generate today's signals
.venv/bin/python -c "from analysis.technical.signals import generate_all; generate_all()"

# Step 3 — enter trades based on BUY signals
.venv/bin/python -m paper_trading.simulator

# Step 4 — check stops/targets and see P&L
.venv/bin/python -m paper_trading.tracker
```
Run in this order after market close each day if stepping through manually instead of using the scheduler.

---

## Backtesting

```bash
python -m backtesting.run --years 3
```
Runs the EMA+RSI swing strategy on the last 3 years of data for all banking stocks. Prints Sharpe, CAGR, max drawdown, win rate vs NIFTY BANK benchmark.

```bash
python -m backtesting.run --optimize --years 3
```
Same but first runs walk-forward optimization to find best RSI/ATR parameters, then backtests with those. Takes longer but gives better results.

```bash
python -m backtesting.run --optimize --symbol HDFCBANK
```
Optimize and backtest a single stock. Useful when checking if the strategy works on a specific stock before trading it.

---

## Intraday monitor (test individual stocks)

```bash
.venv/bin/python -m scheduler.jobs.breakout_monitor WAAREEENER
```
Prints current price, prev close, % change, and 52W high for WAAREEENER without sending any Telegram alert.

```bash
.venv/bin/python -m scheduler.jobs.intraday_monitor AXISBANK
```
Prints intraday bar data and late-session recovery check for AXISBANK without sending any Telegram alert.

---

## Typical daily routine (manual, no scheduler)

| Time | Command | Purpose |
|------|---------|---------|
| After 16:00 IST | `daily_runner once` | Download prices + quality checks |
| After 16:00 IST | `daily_runner eod` | Scores + Telegram EOD alert |
| Before 09:15 IST | `daily_runner morning` | News + Telegram top picks |
| 09:20–15:25 IST | `daily_runner breakout` | Manual breakout check (or let scheduler run it) |
