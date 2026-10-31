import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


####### Load Data #######
## Loads Gold-layer ADA data, converts the timestamp column to datetime format, and sorts the dataset
df = pd.read_csv("gold_ada.csv")
df["trade_ts"] = pd.to_datetime(df["trade_ts"])
df = df.sort_values("trade_ts").drop_duplicates(subset="trade_ts", keep="last").reset_index(drop=True)

## Columns added in the latest SilverToGold run — may be absent in older CSV exports
HAS_BB    = {"bb_upper", "bb_middle", "bb_lower"}.issubset(df.columns)
HAS_STOCH = {"stoch_k", "stoch_d"}.issubset(df.columns)
HAS_ATR   = "atr_14" in df.columns

print(f"Bollinger Bands: {'✓' if HAS_BB else '✗ — export a new gold_ada.csv from Athena'}")
print(f"Stochastic:      {'✓' if HAS_STOCH else '✗ — export a new gold_ada.csv from Athena'}")
print(f"ATR 14:          {'✓' if HAS_ATR else '✗ — export a new gold_ada.csv from Athena'}")


####### Create Dashboard Layout #######
## 6-row layout: Price+BB / Volume / RSI / Stochastic / MACD / ATR
fig = make_subplots(
    rows=6, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.03,
    row_heights=[0.35, 0.09, 0.14, 0.14, 0.14, 0.14],
    subplot_titles=("", "", "", "", "", "")
)


####### Price Panel — Candlestick + SMA 200 + EMA 50 + Bollinger Bands #######
## Candlestick with long-term (SMA200) and medium-term (EMA50) trend lines.
## Bollinger Bands (20-period, ±2σ) show volatility envelope around price.
fig.add_trace(go.Candlestick(
    x=df["trade_ts"],
    open=df["open"],
    high=df["high"],
    low=df["low"],
    close=df["close"],
    name="ADA/USD",
    increasing_line_color="#00cc96",
    decreasing_line_color="#ef553b"
), row=1, col=1)

## Bollinger Bands — only if present in the CSV
if HAS_BB:
    fig.add_trace(go.Scatter(
        x=df["trade_ts"], y=df["bb_upper"],
        line=dict(color="rgba(180,180,255,0.0)"),
        showlegend=False, hoverinfo="skip"
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df["trade_ts"], y=df["bb_lower"],
        name="Bollinger Bands",
        fill="tonexty", fillcolor="rgba(180,180,255,0.07)",
        line=dict(color="rgba(180,180,255,0.0)"),
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df["trade_ts"], y=df["bb_upper"],
        line=dict(color="#b4b4ff", width=0.8, dash="dot"), showlegend=False
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df["trade_ts"], y=df["bb_lower"],
        line=dict(color="#b4b4ff", width=0.8, dash="dot"), showlegend=False
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df["trade_ts"], y=df["bb_middle"],
        name="BB Middle (SMA20)", line=dict(color="#b4b4ff", width=1.0, dash="dash")
    ), row=1, col=1)

fig.add_trace(go.Scatter(
    x=df["trade_ts"],
    y=df["sma_200"],
    name="SMA 200",
    line=dict(color="#ffa500", width=1.5)
), row=1, col=1)

fig.add_trace(go.Scatter(
    x=df["trade_ts"],
    y=df["ema_50"],
    name="EMA 50",
    line=dict(color="#00bfff", width=1.5)
), row=1, col=1)


####### Volume Panel #######
## Bars colored green/red depending on candle direction
colors_vol = [
    "#00cc96" if df["close"].iloc[i] >= df["open"].iloc[i]
    else "#ef553b"
    for i in range(len(df))
]

fig.add_trace(go.Bar(
    x=df["trade_ts"],
    y=df["volume"],
    name="Volume",
    marker_color=colors_vol,
    opacity=0.6,
    showlegend=False
), row=2, col=1)


####### RSI Panel #######
## 14-period RSI with overbought (70) and oversold (30) zones
fig.add_trace(go.Scatter(
    x=df["trade_ts"],
    y=df["rsi"],
    name="RSI (14)",
    line=dict(color="#e377c2", width=1.5)
), row=3, col=1)

fig.add_hline(y=70, line_dash="dash", line_color="red",
              line_width=0.8, row=3, col=1)
fig.add_hline(y=30, line_dash="dash", line_color="green",
              line_width=0.8, row=3, col=1)
fig.add_hrect(y0=70, y1=100, fillcolor="red",   opacity=0.05, row=3, col=1)
fig.add_hrect(y0=0,  y1=30,  fillcolor="green", opacity=0.05, row=3, col=1)


####### Stochastic Panel (NEW) #######
## %K (fast) and %D (slow = SMA3 of %K) with overbought (80) and oversold (20) zones.
## Crossovers between %K and %D are classic entry/exit signals.
if HAS_STOCH:
    fig.add_trace(go.Scatter(
        x=df["trade_ts"], y=df["stoch_k"],
        name="Stoch %K", line=dict(color="#00e5ff", width=1.5)
    ), row=4, col=1)
    fig.add_trace(go.Scatter(
        x=df["trade_ts"], y=df["stoch_d"],
        name="Stoch %D", line=dict(color="#ff9800", width=1.5)
    ), row=4, col=1)
    fig.add_hline(y=80, line_dash="dash", line_color="red",   line_width=0.8, row=4, col=1)
    fig.add_hline(y=20, line_dash="dash", line_color="green", line_width=0.8, row=4, col=1)
    fig.add_hrect(y0=80, y1=100, fillcolor="red",   opacity=0.05, row=4, col=1)
    fig.add_hrect(y0=0,  y1=20,  fillcolor="green", opacity=0.05, row=4, col=1)


####### MACD Panel #######
## MACD histogram, MACD line and Signal line
colors_macd = [
    "#00cc96" if val >= 0 else "#ef553b"
    for val in df["macd_histogram"].fillna(0)
]

fig.add_trace(go.Bar(
    x=df["trade_ts"],
    y=df["macd_histogram"],
    name="MACD Histogram",
    marker_color=colors_macd,
    opacity=0.6
), row=5, col=1)

fig.add_trace(go.Scatter(
    x=df["trade_ts"],
    y=df["macd_line"],
    name="MACD Line",
    line=dict(color="#00bfff", width=1.5)
), row=5, col=1)

fig.add_trace(go.Scatter(
    x=df["trade_ts"],
    y=df["macd_signal"],
    name="Signal Line",
    line=dict(color="#ffa500", width=1.5)
), row=5, col=1)

fig.add_hline(y=0, line_dash="solid", line_color="gray",
              line_width=0.5, row=5, col=1)


####### ATR Panel (NEW) #######
## Average True Range (14-period): measures daily volatility in price units.
## Spikes indicate high-volatility events (crashes, rallies).
if HAS_ATR:
    colors_atr = [
        "#ef553b" if df["atr_14"].iloc[i] > df["atr_14"].median()
        else "#26c6da"
        for i in range(len(df))
    ]
    fig.add_trace(go.Bar(
        x=df["trade_ts"], y=df["atr_14"],
        name="ATR (14)", marker_color=colors_atr,
        opacity=0.7, showlegend=False
    ), row=6, col=1)
    fig.add_hline(
        y=df["atr_14"].median(),
        line_dash="dash", line_color="#555555", line_width=0.8,
        row=6, col=1
    )


####### Apply Dark Financial Theme #######
## Bloomberg-style dark layout — same as original
fig.update_layout(
    title=dict(
        text="<b>ADA/USD - Technical Analysis</b>",
        font=dict(size=22, color="#ffffff"),
        x=0.5
    ),
    template="plotly_dark",
    paper_bgcolor="#0a0a0a",
    plot_bgcolor="#0a0a0a",
    font=dict(family="Courier New", size=12, color="#cccccc"),
    legend=dict(
        orientation="h",
        yanchor="top", y=-0.03,
        xanchor="center", x=0.5,
        font=dict(size=11)
    ),
    height=1450,
    margin=dict(l=60, r=30, t=60, b=100),
    xaxis_rangeslider_visible=False,
    hovermode="x unified"
)

fig.update_yaxes(title_text="Price (USD)", row=1, col=1, gridcolor="#1a1a2e")
fig.update_yaxes(title_text="Volume",      row=2, col=1, gridcolor="#1a1a2e")
fig.update_yaxes(title_text="RSI",         row=3, col=1, range=[0, 100], gridcolor="#1a1a2e")
fig.update_yaxes(title_text="Stoch",       row=4, col=1, range=[0, 100], gridcolor="#1a1a2e")
fig.update_yaxes(title_text="MACD",        row=5, col=1, gridcolor="#1a1a2e")
fig.update_yaxes(title_text="ATR",         row=6, col=1, gridcolor="#1a1a2e")

fig.update_xaxes(gridcolor="#1a1a2e")
fig.update_xaxes(title_text="Date", row=6, col=1)


####### Export Dashboard #######
## Saves the interactive dashboard to HTML to render locally
fig.write_html("dashboard_ada.html")
fig.show()

print("Dashboard saved as dashboard_ada.html")
