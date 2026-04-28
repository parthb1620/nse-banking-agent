"""
NSE Banking Sector — Streamlit Dashboard.

Sections:
  1. Sector overview  — stock scores, ranked table
  2. Technical signals — current signal per stock + strength
  3. Price chart       — adjusted close + EMA lines + signal markers
  4. Sentiment         — recent news with LLM sentiment scores
  5. Data quality      — row counts, last update, source breakdown

Run:
  cd /path/to/nse-banking-agent
  streamlit run dashboard/app.py
"""

import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# Ensure project root is on path when running via streamlit
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import BANKING_STOCKS, PAPER_TRADING_CAPITAL, STOCK_NAMES
from data.storage.database import (
    NewsArticle, OHLCVDaily, PaperTrade, TechnicalSignal, get_session, init_db,
)

_IST = ZoneInfo("Asia/Kolkata")

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NSE Banking Agent",
    page_icon="📈",
    layout="wide",
)

init_db()


# ── Cached data loaders ────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_scores() -> list[dict]:
    from scoring.stock_scorer import score_all
    return score_all()


@st.cache_data(ttl=300)
def load_signals() -> pd.DataFrame:
    rows = []
    with get_session() as session:
        for sym in BANKING_STOCKS:
            sig = (
                session.query(TechnicalSignal)
                .filter(TechnicalSignal.symbol == sym)
                .order_by(TechnicalSignal.signal_date.desc())
                .first()
            )
            if sig:
                rows.append({
                    "Symbol":   sym,
                    "Date":     sig.signal_date,
                    "Signal":   sig.signal_type,
                    "Strength": sig.strength,
                    "Reason":   sig.reason,
                })
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_ohlcv(symbol: str, days: int = 180) -> pd.DataFrame:
    cutoff = date.today() - timedelta(days=days)
    with get_session() as session:
        rows = (
            session.query(OHLCVDaily)
            .filter(OHLCVDaily.symbol == symbol, OHLCVDaily.date >= cutoff)
            .order_by(OHLCVDaily.date.asc())
            .all()
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([{
        "date":          r.date,
        "open":          r.open,
        "high":          r.high,
        "low":           r.low,
        "close":         r.close,
        "adjusted_close": r.adjusted_close or r.close,
        "volume":        r.volume or 0,
    } for r in rows])
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date")


@st.cache_data(ttl=300)
def load_news(symbol: str, limit: int = 10) -> pd.DataFrame:
    with get_session() as session:
        arts = (
            session.query(NewsArticle)
            .filter(NewsArticle.symbol == symbol)
            .order_by(NewsArticle.published_at.desc())
            .limit(limit)
            .all()
        )
    if not arts:
        return pd.DataFrame()
    return pd.DataFrame([{
        "Date":      a.published_at.strftime("%Y-%m-%d") if a.published_at else "",
        "Headline":  a.headline or "",
        "Source":    a.source or "",
        "Sentiment": round(a.sentiment_score, 2) if a.sentiment_score is not None else None,
    } for a in arts])


@st.cache_data(ttl=300)
def load_data_quality() -> pd.DataFrame:
    rows = []
    with get_session() as session:
        for sym in BANKING_STOCKS:
            count = session.query(OHLCVDaily).filter(OHLCVDaily.symbol == sym).count()
            latest = (
                session.query(OHLCVDaily)
                .filter(OHLCVDaily.symbol == sym)
                .order_by(OHLCVDaily.date.desc())
                .first()
            )
            rows.append({
                "Symbol":      sym,
                "OHLCV Rows":  count,
                "Latest Date": str(latest.date) if latest else "—",
                "Source":      latest.source if latest else "—",
            })
    return pd.DataFrame(rows)


# ── Colour helpers ─────────────────────────────────────────────────────────────

def score_colour(score: float) -> str:
    if score >= 70:
        return "🟢"
    if score >= 50:
        return "🟡"
    return "🔴"


def signal_colour(signal_type: str) -> str:
    return {"BUY": "🟢", "SELL": "🔴", "NEUTRAL": "⚪"}.get(signal_type, "⚪")


def sentiment_colour(score) -> str:
    if score is None:
        return "—"
    if score >= 0.3:
        return f"🟢 {score:+.2f}"
    if score <= -0.3:
        return f"🔴 {score:+.2f}"
    return f"🟡 {score:+.2f}"


# ══════════════════════════════════════════════════════════════════════════════
# Sidebar
# ══════════════════════════════════════════════════════════════════════════════

st.sidebar.title("NSE Banking Agent")
st.sidebar.caption(f"As of {datetime.now(_IST).strftime('%d %b %Y %H:%M')} IST")

selected_symbol = st.sidebar.selectbox(
    "Select stock for detail view",
    BANKING_STOCKS,
    format_func=lambda s: f"{s} — {STOCK_NAMES.get(s, s)}",
)

chart_days = st.sidebar.slider("Price chart window (days)", 30, 365, 180)

if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# Section 1 — Sector overview
# ══════════════════════════════════════════════════════════════════════════════

st.header("📊 Sector Overview")

try:
    scores = load_scores()
    score_df = pd.DataFrame(scores)

    cols = st.columns(len(BANKING_STOCKS))
    for col, row in zip(cols, scores):
        with col:
            icon = score_colour(row["total_score"])
            st.metric(
                label=row["symbol"],
                value=f"{row['total_score']:.1f}",
                delta=None,
            )
            st.caption(icon)

    st.subheader("Rankings")
    display_df = score_df[[
        "symbol", "name", "total_score", "technical_score",
        "fundamental_score", "sentiment_score",
    ]].copy()
    display_df.columns = ["Symbol", "Name", "Total", "Technical", "Fundamental", "Sentiment"]
    display_df = display_df.set_index("Symbol")
    st.dataframe(display_df.style.format("{:.1f}", subset=["Total","Technical","Fundamental","Sentiment"]), use_container_width=True)

except Exception as e:
    st.error(f"Could not load scores: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Section 2 — Technical signals
# ══════════════════════════════════════════════════════════════════════════════

st.header("🔔 Technical Signals")

try:
    signals_df = load_signals()
    if signals_df.empty:
        st.info("No signals generated yet. Run `python -m scheduler.daily_runner once` to generate.")
    else:
        for _, row in signals_df.iterrows():
            icon = signal_colour(row["Signal"])
            st.markdown(
                f"{icon} **{row['Symbol']}** &nbsp; `{row['Signal']}` "
                f"str={row['Strength']} &nbsp; *{row['Date']}* &nbsp; — {row['Reason'][:100]}"
            )
except Exception as e:
    st.error(f"Could not load signals: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Section 3 — Price chart
# ══════════════════════════════════════════════════════════════════════════════

st.header(f"📈 {selected_symbol} — {STOCK_NAMES.get(selected_symbol, '')}")

try:
    ohlcv = load_ohlcv(selected_symbol, chart_days)
    if ohlcv.empty:
        st.info("No price data available.")
    else:
        from analysis.technical.indicators import compute_all
        ind = compute_all(ohlcv)

        fig = go.Figure()

        # Candlestick
        fig.add_trace(go.Candlestick(
            x=ind.index,
            open=ind["open"], high=ind["high"],
            low=ind["low"],   close=ind["adjusted_close"],
            name="Price", increasing_line_color="#26a69a",
            decreasing_line_color="#ef5350",
        ))

        # EMA lines
        for period, colour in [(21, "#ffb300"), (50, "#42a5f5"), (200, "#ab47bc")]:
            col = f"ema_{period}"
            if col in ind.columns:
                fig.add_trace(go.Scatter(
                    x=ind.index, y=ind[col],
                    name=f"EMA {period}",
                    line=dict(color=colour, width=1),
                    opacity=0.8,
                ))

        fig.update_layout(
            xaxis_rangeslider_visible=False,
            height=450,
            margin=dict(l=0, r=0, t=20, b=0),
            legend=dict(orientation="h", y=1.02),
        )
        st.plotly_chart(fig, use_container_width=True)

        # RSI sub-chart
        if "rsi" in ind.columns:
            fig_rsi = go.Figure()
            fig_rsi.add_trace(go.Scatter(
                x=ind.index, y=ind["rsi"], name="RSI",
                line=dict(color="#ff7043", width=1.5),
            ))
            fig_rsi.add_hline(y=60, line_dash="dot", line_color="green",  annotation_text="60")
            fig_rsi.add_hline(y=35, line_dash="dot", line_color="orange", annotation_text="35")
            fig_rsi.add_hline(y=75, line_dash="dot", line_color="red",    annotation_text="75 (exit)")
            fig_rsi.update_layout(height=180, margin=dict(l=0, r=0, t=10, b=0), showlegend=False)
            st.plotly_chart(fig_rsi, use_container_width=True)

except Exception as e:
    st.error(f"Chart error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Section 4 — News & Sentiment
# ══════════════════════════════════════════════════════════════════════════════

st.header(f"📰 News — {selected_symbol}")

try:
    news_df = load_news(selected_symbol)
    if news_df.empty:
        st.info("No news articles yet. News collection runs at 08:30 IST daily.")
    else:
        news_df["Sentiment"] = news_df["Sentiment"].apply(sentiment_colour)
        st.dataframe(news_df, use_container_width=True, hide_index=True)
except Exception as e:
    st.error(f"Could not load news: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Section 5 — Paper Trading P&L
# ══════════════════════════════════════════════════════════════════════════════

st.header("💼 Paper Trading")

try:
    with get_session() as _s:
        _all_trades  = _s.query(PaperTrade).all()
        _open        = [t for t in _all_trades if t.status == "open"]
        _closed      = [t for t in _all_trades if t.status in ("closed_target", "closed_stop", "closed_manual")]

    _total_pnl  = sum(t.pnl for t in _closed if t.pnl is not None)
    _wins       = [t for t in _closed if t.pnl is not None and t.pnl > 0]
    _win_rate   = len(_wins) / len(_closed) * 100 if _closed else 0.0
    _capital    = PAPER_TRADING_CAPITAL + _total_pnl

    _c1, _c2, _c3, _c4 = st.columns(4)
    _c1.metric("Capital", f"₹{_capital:,.0f}", delta=f"₹{_total_pnl:+,.0f}")
    _c2.metric("Return", f"{_total_pnl / PAPER_TRADING_CAPITAL * 100:+.2f}%")
    _c3.metric("Win rate", f"{_win_rate:.0f}%", delta=f"{len(_closed)} trades")
    _c4.metric("Open positions", len(_open))

    if _open:
        st.subheader("Open Positions")
        _open_df = pd.DataFrame([{
            "Symbol":     t.symbol,
            "Entry date": str(t.entry_date),
            "Entry ₹":    t.entry_price,
            "Stop ₹":     t.stop_loss,
            "Target ₹":   t.target,
            "Qty":        t.quantity,
            "Thesis":     (t.thesis or "")[:80],
        } for t in _open])
        st.dataframe(_open_df.set_index("Symbol"), use_container_width=True)

    if _closed:
        st.subheader("Trade History (last 20)")
        _hist_df = pd.DataFrame([{
            "Symbol":     t.symbol,
            "Entry":      str(t.entry_date),
            "Exit":       str(t.exit_date),
            "Entry ₹":    t.entry_price,
            "Exit ₹":     t.exit_price,
            "Qty":        t.quantity,
            "P&L ₹":      t.pnl,
            "Result":     t.status.replace("closed_", ""),
        } for t in sorted(_closed, key=lambda x: x.exit_date or date.min, reverse=True)[:20]])
        st.dataframe(_hist_df.set_index("Symbol"), use_container_width=True)
    elif not _open:
        st.info("No trades yet. Trades are entered at 09:20 IST when BUY signals exist.")

except Exception as e:
    st.error(f"Could not load paper trades: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Section 6 — Data quality
# ══════════════════════════════════════════════════════════════════════════════

st.header("🔍 Data Quality")

try:
    quality_df = load_data_quality()
    st.dataframe(quality_df.set_index("Symbol"), use_container_width=True)
    st.caption("Source = yfinance means data is unverified. NSE Bhavcopy is the authoritative source.")
except Exception as e:
    st.error(f"Could not load quality data: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Section 7 — Backtesting
# ══════════════════════════════════════════════════════════════════════════════

st.header("🧪 Backtesting")

_bt_years = st.slider("Backtest window (years)", 1, 5, 3, key="bt_years")
_bt_use_opt = st.checkbox("Use walk-forward optimized params", value=False, key="bt_opt")

if st.button("Run backtest", key="run_bt"):
    _bt_end   = date.today()
    _bt_start = _bt_end.replace(year=_bt_end.year - _bt_years)

    with st.spinner("Running backtest..."):
        try:
            from backtesting.engine import BacktestEngine
            from backtesting.metrics import compute_metrics

            _engine = BacktestEngine(initial_capital=100_000)

            if _bt_use_opt:
                from backtesting.optimizer import load_best_params
                _all_results = {
                    sym: _engine.run(sym, _bt_start, _bt_end, params=load_best_params(sym))
                    for sym in BANKING_STOCKS
                }
            else:
                _all_results = {
                    sym: _engine.run(sym, _bt_start, _bt_end)
                    for sym in BANKING_STOCKS
                }

            # ── Summary table ─────────────────────────────────────────────────
            _rows = []
            for sym, res in _all_results.items():
                m = compute_metrics(res)
                _rows.append({
                    "Symbol":       sym,
                    "Return %":     m["total_return_pct"],
                    "CAGR %":       m["cagr_pct"],
                    "Sharpe":       m["sharpe"],
                    "Max DD %":     m["max_drawdown_pct"],
                    "Trades":       m["total_trades"],
                    "Win Rate %":   m["win_rate_pct"],
                    "Profit Factor": m["profit_factor"],
                    "Avg Hold Days": m["avg_hold_days"],
                })
            _summary_df = pd.DataFrame(_rows).set_index("Symbol")
            st.subheader("Strategy Summary")
            st.dataframe(
                _summary_df.style.format({
                    "Return %": "{:+.1f}", "CAGR %": "{:+.1f}",
                    "Sharpe": "{:.2f}", "Max DD %": "{:.1f}",
                    "Win Rate %": "{:.1f}", "Profit Factor": "{:.2f}",
                    "Avg Hold Days": "{:.1f}",
                }),
                use_container_width=True,
            )

            # ── Equity curves ─────────────────────────────────────────────────
            st.subheader("Equity Curves")
            _fig_eq = go.Figure()
            for sym, res in _all_results.items():
                if res.equity_curve is not None and not res.equity_curve.empty:
                    _fig_eq.add_trace(go.Scatter(
                        x=res.equity_curve.index,
                        y=res.equity_curve.values,
                        name=sym,
                        mode="lines",
                    ))
            _fig_eq.add_hline(y=100_000, line_dash="dot", line_color="grey", annotation_text="Start ₹1L")
            _fig_eq.update_layout(
                height=400,
                margin=dict(l=0, r=0, t=20, b=0),
                yaxis_title="Portfolio value (₹)",
                legend=dict(orientation="h", y=1.02),
            )
            st.plotly_chart(_fig_eq, use_container_width=True)

            # ── Trade log for selected stock ──────────────────────────────────
            st.subheader(f"Trade log — {selected_symbol}")
            _sel_res = _all_results.get(selected_symbol)
            if _sel_res and _sel_res.trades:
                _tdf = _sel_res.trade_df()
                _tdf = _tdf[["entry_date","exit_date","entry_price","exit_price","quantity","stop_loss","target","exit_reason","net_pnl","pnl_pct"]]
                _tdf.columns = ["Entry","Exit","Entry ₹","Exit ₹","Qty","Stop","Target","Reason","Net P&L ₹","P&L %"]
                st.dataframe(
                    _tdf.style.format({
                        "Entry ₹": "{:.2f}", "Exit ₹": "{:.2f}",
                        "Stop": "{:.2f}", "Target": "{:.2f}",
                        "Net P&L ₹": "{:+,.0f}", "P&L %": "{:+.2f}",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("No trades for selected symbol in this period.")

        except Exception as _e:
            st.error(f"Backtest error: {_e}")

# ── Optimized params view ──────────────────────────────────────────────────────
with st.expander("Current optimized parameters (from last optimizer run)"):
    try:
        from backtesting.optimizer import load_all_params
        _opt_params = load_all_params()
        if _opt_params:
            _opt_df = pd.DataFrame(_opt_params).T
            st.dataframe(_opt_df, use_container_width=True)
            st.caption("Run `python -m backtesting.run --optimize` to refresh these.")
        else:
            st.info("No optimized params yet. Run: python -m backtesting.run --optimize"
                    " (or check 'Run optimizer monthly' in scheduler)")
    except Exception as _e:
        st.warning(f"Could not load params: {_e}")
