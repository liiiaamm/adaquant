// The streaming lane of ADAQuant, ported from Python so it can run in a browser:
//   producerMessage  <- kafka/binance_kafka_producer.py  (closed 1-minute candles only, same JSON)
//   vwapUpdates      <- jobs_aws/timestream_job.py        (5-minute windows sliding by 1 minute, update mode)
//   indicators       <- kafka/sns_alerts.py               (SMA200, EMA50 seeded with an SMA, Wilder RSI14)
//   evaluateAlerts   <- kafka/sns_alerts.py               (oversold, overbought, death cross, golden cross)
(function (root) {
  "use strict";
  const MIN = 60000;

  const iso = (ms) => new Date(ms).toISOString().replace(/\.\d{3}Z$/, "Z");
  const isoMs = (ms) => new Date(ms).toISOString();

  // binance_kafka_producer.handle_kline: only closed candles (k.x) become messages
  function producerMessage(k) {
    if (!k || !k.x) return null;
    const symbol = k.s;
    return {
      key: symbol.replace("USDT", "").replace("USD", ""),
      timestamp_ms: k.T,
      value: { symbol, "@timestamp": "@" + iso(k.T), close: parseFloat(k.c), volume: parseFloat(k.v) },
    };
  }

  // Spark window("event_time", "5 minutes", "1 minute") with event_time = Kafka timestamp = candle close time.
  // A new candle lands in five overlapping windows; in update mode each of them is re-emitted.
  function vwapUpdates(messages, newest) {
    const t = newest.timestamp_ms, out = [];
    const lastStart = Math.floor(t / MIN) * MIN;
    for (let s = lastStart - 4 * MIN; s <= lastStart; s += MIN) {
      const e = s + 5 * MIN;
      let pv = 0, v = 0;
      for (const m of messages) if (m.timestamp_ms >= s && m.timestamp_ms < e) { pv += m.value.close * m.value.volume; v += m.value.volume; }
      if (v > 0) out.push({ key: newest.value.symbol, value: { window_start: isoMs(s), window_end: isoMs(e), symbol: newest.value.symbol, vwap: Math.round((pv / v) * 1e6) / 1e6 }, complete: e <= t + 1 });
    }
    return out;
  }

  // complete 5-minute windows over a whole series, for the chart
  function vwapSeries(messages) {
    const out = [];
    if (!messages.length) return out;
    const first = Math.floor(messages[0].timestamp_ms / MIN) * MIN, last = messages[messages.length - 1].timestamp_ms;
    for (let s = first; s + 5 * MIN <= last + 1; s += MIN) {
      let pv = 0, v = 0;
      for (const m of messages) if (m.timestamp_ms >= s && m.timestamp_ms < s + 5 * MIN) { pv += m.value.close * m.value.volume; v += m.value.volume; }
      if (v > 0) out.push({ t: s + 5 * MIN, vwap: pv / v });
    }
    return out;
  }

  // sns_alerts._compute_indicators
  function indicators(close) {
    const n = close.length, sma200 = new Array(n).fill(NaN), ema50 = new Array(n).fill(NaN), rsi = new Array(n).fill(NaN);
    let acc = 0;
    for (let i = 0; i < n; i++) { acc += close[i]; if (i >= 200) acc -= close[i - 200]; if (i >= 199) sma200[i] = acc / 200; }
    const k50 = 2 / (50 + 1);
    if (n >= 50) {
      ema50[49] = close.slice(0, 50).reduce((a, b) => a + b, 0) / 50;
      for (let i = 50; i < n; i++) ema50[i] = close[i] * k50 + ema50[i - 1] * (1 - k50);
    }
    const P = 14;
    if (n > P) {
      const g = [], l = [];
      for (let i = 1; i < n; i++) { const d = close[i] - close[i - 1]; g.push(d > 0 ? d : 0); l.push(d < 0 ? -d : 0); }
      let ag = g.slice(0, P).reduce((a, b) => a + b, 0) / P, al = l.slice(0, P).reduce((a, b) => a + b, 0) / P;
      rsi[P] = al === 0 ? 100 : 100 - 100 / (1 + ag / al);
      for (let i = P; i < n - 1; i++) {
        ag = (ag * (P - 1) + g[i]) / P; al = (al * (P - 1) + l[i]) / P;
        rsi[i + 1] = al === 0 ? 100 : 100 - 100 / (1 + ag / al);
      }
    }
    return { sma200, ema50, rsi };
  }

  // sns_alerts.evaluate_and_alert, run over the last 250 closes
  function evaluateAlerts(closes, times, symbol = "ADAUSDT") {
    const { sma200, ema50, rsi } = indicators(closes);
    const n = closes.length, i = n - 1, ok = (x) => Number.isFinite(x);
    const price = closes[i], time = new Date(times[i]).toISOString().replace("T", " ").replace(/\.\d+Z$/, "+00:00");
    const r = rsi[i], e = ema50[i], s = sma200[i], pe = ema50[i - 1], ps = sma200[i - 1];
    const rules = [
      { id: "OVERSOLD", label: "RSI below 30", value: r, fired: ok(r) && r < 30,
        subject: "[ADAQuant] OVERSOLD SIGNAL: ADA RSI below 30",
        body: `OVERSOLD SIGNAL: ADA RSI has dropped below 30 (current value: ${ok(r) ? r.toFixed(2) : "N/A"})\n\nSymbol : ${symbol}\nTime   : ${time}\nPrice  : ${price.toFixed(6)}\nRSI    : ${ok(r) ? r.toFixed(2) : "N/A"}` },
      { id: "OVERBOUGHT", label: "RSI above 70", value: r, fired: ok(r) && r > 70,
        subject: "[ADAQuant] OVERBOUGHT SIGNAL: ADA RSI above 70",
        body: `OVERBOUGHT SIGNAL: ADA RSI has exceeded 70 (current value: ${ok(r) ? r.toFixed(2) : "N/A"})\n\nSymbol : ${symbol}\nTime   : ${time}\nPrice  : ${price.toFixed(6)}\nRSI    : ${ok(r) ? r.toFixed(2) : "N/A"}` },
      { id: "DEATH_CROSS", label: "EMA50 crosses below SMA200", value: ok(e) && ok(s) ? e - s : NaN,
        fired: ok(e) && ok(s) && ok(pe) && ok(ps) && e < s && pe >= ps,
        subject: "[ADAQuant] DEATH CROSS DETECTED: EMA50 crossed below SMA200",
        body: `DEATH CROSS DETECTED: EMA50 has crossed below SMA200\n\nSymbol  : ${symbol}\nTime    : ${time}\nPrice   : ${price.toFixed(6)}\nEMA50   : ${ok(e) ? e.toFixed(6) : "N/A"}\nSMA200  : ${ok(s) ? s.toFixed(6) : "N/A"}` },
      { id: "GOLDEN_CROSS", label: "EMA50 crosses above SMA200", value: ok(e) && ok(s) ? e - s : NaN,
        fired: ok(e) && ok(s) && ok(pe) && ok(ps) && e > s && pe <= ps,
        subject: "[ADAQuant] GOLDEN CROSS DETECTED: EMA50 crossed above SMA200",
        body: `GOLDEN CROSS DETECTED: EMA50 has crossed above SMA200\n\nSymbol  : ${symbol}\nTime    : ${time}\nPrice   : ${price.toFixed(6)}\nEMA50   : ${ok(e) ? e.toFixed(6) : "N/A"}\nSMA200  : ${ok(s) ? s.toFixed(6) : "N/A"}` },
    ];
    return { rules, rsi: r, ema50: e, sma200: s, price, series: { sma200, ema50, rsi } };
  }

  const api = { producerMessage, vwapUpdates, vwapSeries, indicators, evaluateAlerts, MIN };
  if (typeof module !== "undefined" && module.exports) module.exports = api; else root.Lane = api;
})(typeof window !== "undefined" ? window : globalThis);
