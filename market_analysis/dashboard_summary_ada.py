import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# =============================================================================
# LOAD & PREPARE
# =============================================================================
df = pd.read_csv("summary_ada.csv")

df["period"] = pd.to_datetime(
    df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2) + "-01"
)
df = df.sort_values("period").reset_index(drop=True)

df["monthly_return_pct"] = df["avg_close"].pct_change() * 100
df["stddev_upper"]       = df["avg_close"] + df["stddev_close"]
df["stddev_lower"]       = df["avg_close"] - df["stddev_close"]
# Coefficient of variation as volatility proxy when ATR is unavailable
df["volatility_pct"]     = (df["stddev_close"] / df["avg_close"] * 100).fillna(0)

# Key events
best_month  = df.loc[df["monthly_return_pct"].idxmax()]
worst_month = df.loc[df["monthly_return_pct"].idxmin()]
most_vol    = df.loc[df["volatility_pct"].idxmax()]
gc_months   = df[df["golden_crosses"] > 0]
dc_months   = df[df["death_crosses"]  > 0]

ret_colors = ["#00cc96" if v >= 0 else "#ef553b"
              for v in df["monthly_return_pct"].fillna(0)]
vol_median = df["volatility_pct"].median()
vol_colors = ["#ef553b" if v > vol_median else "#636efa"
              for v in df["volatility_pct"]]


# =============================================================================
# LAYOUT
# =============================================================================
fig = make_subplots(
    rows=4, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.08,
    row_heights=[0.36, 0.20, 0.24, 0.20],
    subplot_titles=(
        "① Price Band — Avg Close ± StdDev  &  Monthly High/Low Range  |  Cross Signals",
        "② Monthly Return %",
        "③ Market Regime — Days Above vs Below SMA 200  |  RSI Extreme Net",
        "④ Monthly Volatility — StdDev / Avg Close %  (red = above median)",
    )
)


# =============================================================================
# ROW 1 — PRICE BAND
# =============================================================================
# Outer band: max/min
fig.add_trace(go.Scatter(
    x=df["period"], y=df["max_close"],
    line=dict(color="rgba(0,0,0,0)"), showlegend=False, hoverinfo="skip"
), row=1, col=1)
fig.add_trace(go.Scatter(
    x=df["period"], y=df["min_close"],
    fill="tonexty", fillcolor="rgba(99,110,250,0.07)",
    line=dict(color="rgba(0,0,0,0)"),
    name="High/Low Range", legendgroup="g1"
), row=1, col=1)

# Inner band: ±stddev
fig.add_trace(go.Scatter(
    x=df["period"], y=df["stddev_upper"],
    line=dict(color="rgba(0,0,0,0)"), showlegend=False, hoverinfo="skip"
), row=1, col=1)
fig.add_trace(go.Scatter(
    x=df["period"], y=df["stddev_lower"],
    fill="tonexty", fillcolor="rgba(99,110,250,0.18)",
    line=dict(color="rgba(0,0,0,0)"),
    name="±1 StdDev", legendgroup="g1"
), row=1, col=1)

# Avg close
fig.add_trace(go.Scatter(
    x=df["period"], y=df["avg_close"],
    name="Avg Close",
    line=dict(color="#818cf8", width=2.5),
    hovertemplate="<b>%{x|%b %Y}</b><br>Avg: $%{y:.4f}<extra></extra>"
), row=1, col=1)

# Golden Cross
if not gc_months.empty:
    fig.add_trace(go.Scatter(
        x=gc_months["period"], y=gc_months["max_close"] * 1.05,
        mode="markers+text",
        name="Golden Cross",
        marker=dict(symbol="triangle-up", color="#fbbf24", size=13),
        text=["GC"] * len(gc_months),
        textposition="top center",
        textfont=dict(color="#fbbf24", size=9),
        hovertemplate="<b>Golden Cross</b>  %{x|%b %Y}<extra></extra>"
    ), row=1, col=1)

# Death Cross
if not dc_months.empty:
    fig.add_trace(go.Scatter(
        x=dc_months["period"], y=dc_months["min_close"] * 0.95,
        mode="markers+text",
        name="Death Cross",
        marker=dict(symbol="triangle-down", color="#f87171", size=13),
        text=["DC"] * len(dc_months),
        textposition="bottom center",
        textfont=dict(color="#f87171", size=9),
        hovertemplate="<b>Death Cross</b>  %{x|%b %Y}<extra></extra>"
    ), row=1, col=1)


# =============================================================================
# ROW 2 — MONTHLY RETURN
# =============================================================================
fig.add_trace(go.Bar(
    x=df["period"], y=df["monthly_return_pct"],
    name="Monthly Return %",
    marker_color=ret_colors, opacity=0.85,
    hovertemplate="<b>%{x|%b %Y}</b><br>%{y:.1f}%<extra></extra>"
), row=2, col=1)

fig.add_hline(y=0, line_color="#374151", line_width=1, row=2, col=1)

fig.add_annotation(
    x=best_month["period"], y=best_month["monthly_return_pct"],
    text=f"<b>+{best_month['monthly_return_pct']:.1f}%</b>",
    showarrow=True, arrowhead=2, arrowcolor="#00cc96", arrowwidth=1.5,
    font=dict(color="#00cc96", size=10), ax=0, ay=-30, row=2, col=1
)
fig.add_annotation(
    x=worst_month["period"], y=worst_month["monthly_return_pct"],
    text=f"<b>{worst_month['monthly_return_pct']:.1f}%</b>",
    showarrow=True, arrowhead=2, arrowcolor="#ef553b", arrowwidth=1.5,
    font=dict(color="#ef553b", size=10), ax=0, ay=30, row=2, col=1
)


# =============================================================================
# ROW 3 — MARKET REGIME
# =============================================================================
fig.add_trace(go.Bar(
    x=df["period"], y=df["days_above_sma200"],
    name="Above SMA200 (Bull)",
    marker_color="#34d399", opacity=0.85,
    hovertemplate="<b>%{x|%b %Y}</b><br>Bullish: %{y}d<extra></extra>"
), row=3, col=1)

fig.add_trace(go.Bar(
    x=df["period"], y=-df["days_below_sma200"],
    name="Below SMA200 (Bear)",
    marker_color="#fb923c", opacity=0.85,
    customdata=df["days_below_sma200"],
    hovertemplate="<b>%{x|%b %Y}</b><br>Bearish: %{customdata}d<extra></extra>"
), row=3, col=1)

fig.add_trace(go.Scatter(
    x=df["period"],
    y=df["days_overbought"] - df["days_oversold"],
    name="RSI Net (OB − OS days)",
    line=dict(color="#e879f9", width=1.5, dash="dot"),
    hovertemplate="<b>%{x|%b %Y}</b><br>RSI Net: %{y}d<extra></extra>"
), row=3, col=1)

fig.add_hline(y=0, line_color="#374151", line_width=1, row=3, col=1)


# =============================================================================
# ROW 4 — VOLATILITY (stddev/avg as %)
# =============================================================================
fig.add_trace(go.Bar(
    x=df["period"], y=df["volatility_pct"],
    name="Volatility %",
    marker_color=vol_colors, opacity=0.85,
    showlegend=False,
    hovertemplate="<b>%{x|%b %Y}</b><br>Volatility: %{y:.1f}%<extra></extra>"
), row=4, col=1)

fig.add_hline(y=vol_median, line_dash="dash", line_color="#6b7280",
              line_width=1, row=4, col=1)

fig.add_annotation(
    x=most_vol["period"], y=most_vol["volatility_pct"],
    text=f"<b>Peak<br>{most_vol['period'].strftime('%b %Y')}</b>",
    showarrow=True, arrowhead=2, arrowcolor="#ef553b", arrowwidth=1.5,
    font=dict(color="#ef553b", size=9), ax=0, ay=-32, row=4, col=1
)


# =============================================================================
# THEME
# =============================================================================
fig.update_layout(
    title=dict(
        text="<b>ADA / USD — Monthly Intelligence Dashboard</b>",
        font=dict(size=22, color="#f1f5f9", family="Courier New"),
        x=0.5, y=0.995
    ),
    template="plotly_dark",
    paper_bgcolor="#060818",
    plot_bgcolor="#060818",
    font=dict(family="Courier New", size=11, color="#94a3b8"),
    barmode="relative",
    legend=dict(
        orientation="h",
        yanchor="top", y=-0.03,
        xanchor="center", x=0.5,
        font=dict(size=10),
        bgcolor="rgba(0,0,0,0)",
        tracegroupgap=5
    ),
    height=1150,
    margin=dict(l=70, r=40, t=60, b=80),
    hovermode="x unified",
)

grid = "#0f172a"
fig.update_yaxes(title_text="Price (USD)", row=1, col=1, gridcolor=grid)
fig.update_yaxes(title_text="Return %",   row=2, col=1, gridcolor=grid, zerolinecolor="#374151")
fig.update_yaxes(title_text="Days",       row=3, col=1, gridcolor=grid, zerolinecolor="#374151")
fig.update_yaxes(title_text="Vol %",      row=4, col=1, gridcolor=grid)
fig.update_xaxes(gridcolor=grid)
fig.update_xaxes(title_text="Month", row=4, col=1)

# Style subplot titles
for ann in fig.layout.annotations:
    ann.font.size   = 11
    ann.font.color  = "#64748b"
    ann.font.family = "Courier New"
    ann.x           = 0.0
    ann.xanchor     = "left"
    ann.xref        = "paper"


# =============================================================================
# EXPORT
# =============================================================================
fig.write_html("dashboard_summary_ada.html")
print("Saved → dashboard_summary_ada.html")
