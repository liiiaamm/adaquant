// ADAQuant demo: the streaming lane on Binance's live feed, and the batch lane's monthly summary.
(() => {
  "use strict";
  const $ = (id) => document.getElementById(id);
  const NS = "http://www.w3.org/2000/svg";
  const el = (tag, attrs = {}, parent) => { const e = document.createElementNS(NS, tag); for (const k in attrs) e.setAttribute(k, attrs[k]); if (parent) parent.appendChild(e); return e; };
  const reduce = matchMedia("(prefers-reduced-motion: reduce)").matches;
  const C = { mist: "#f3f4f4", sea: "#5f9598", ocean: "#1d546d", hot: "#ffd27a", red: "#ff7b7b", green: "#7be0b4", dim: "#8fb3b6" };
  const fmt = (x, d = 4) => (Number.isFinite(x) ? x.toFixed(d) : "—");
  const hhmm = (ms) => new Date(ms).toISOString().slice(11, 16);

  // ------------------------------------------------------------------ star field (the brand is a rhino made of stars)
  (function sky() {
    const c = $("sky"), x = c.getContext("2d");
    const draw = () => {
      const d = Math.min(devicePixelRatio || 1, 2); c.width = innerWidth * d; c.height = innerHeight * d;
      let s = 7; const r = () => ((s = (s * 16807) % 2147483647) / 2147483647);
      for (let i = 0; i < 220; i++) { x.fillStyle = `rgba(243,244,244,${0.15 + r() * 0.55})`; const z = (r() * 1.4 + 0.3) * d; x.fillRect(r() * c.width, r() * c.height, z, z); }
    };
    draw(); addEventListener("resize", draw);
  })();

  // ------------------------------------------------------------------ pipeline strip
  const pipe = $("pipe");
  const nodes = [
    ["binance", 20, "Binance", "kline_1m websocket"],
    ["producer", 180, "producer", "closed candles only"],
    ["raw", 340, "imat3a-ADA", "Kafka topic", true],
    ["glue", 500, "Glue Streaming", "VWAP · 5 min / 1 min"],
    ["vwap", 660, "imat3a-ADA-VWAP", "Kafka topic", true],
    ["ts", 840, "Timestream", "quotes · vwap"],
    ["sns", 1010, "sns_alerts.py", "RSI · EMA50 · SMA200"],
  ];
  const W = 150, Y = 55, H = 64, pos = {}, counters = {};
  el("defs", {}, pipe).innerHTML = '<marker id="ar" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0 0L10 5L0 10z" fill="#1d546d"/></marker>';
  nodes.forEach(([id, x], i) => { if (i) { const px = nodes[i - 1][1] + W; el("path", { class: "pedge", d: `M${px} ${Y + H / 2} L${x - 2} ${Y + H / 2}`, "marker-end": "url(#ar)" }, pipe); } });
  el("path", { class: "pedge", d: `M${340 + W / 2} ${Y + H} C ${340 + W / 2} 150, ${840 + W / 2} 150, ${840 + W / 2} ${Y + H + 2}`, "marker-end": "url(#ar)", "stroke-dasharray": "3 4" }, pipe);
  nodes.forEach(([id, x, t1, t2, topic]) => {
    const g = el("g", { class: "pnode" + (topic ? " topic" : "") }, pipe);
    el("rect", { x, y: Y, width: W, height: H }, g);
    el("text", { x: x + 12, y: Y + 22, class: "t1" }, g).textContent = t1;
    el("text", { x: x + 12, y: Y + 39, class: "t2" }, g).textContent = t2;
    counters[id] = el("text", { x: x + 12, y: Y + 55, class: "n" }, g); counters[id].textContent = "";
    pos[id] = [x, x + W];
  });
  const cnt = { binance: 0, producer: 0, raw: 0, glue: 0, vwap: 0, ts: 0, sns: 0 };
  const bump = (id, n = 1, label) => { cnt[id] += n; counters[id].textContent = label ? label(cnt[id]) : `${cnt[id]} msgs`; };
  function packet(from, to, drop) {
    if (reduce || document.hidden) return;
    const x0 = pos[from][1] - 10, x1 = drop ? pos[from][1] + 30 : pos[to][0] + 10, c = el("circle", { r: drop ? 3 : 4.5, cy: Y + H / 2, cx: x0, class: "packet" + (drop ? " drop" : "") }, pipe);
    const t0 = performance.now(), dur = drop ? 600 : 700;
    (function step(now) { const f = Math.min(1, (now - t0) / dur); c.setAttribute("cx", x0 + (x1 - x0) * f); if (drop) c.setAttribute("opacity", 1 - f); if (f < 1) requestAnimationFrame(step); else c.remove(); })(t0);
  }
  const chain = (hops, gap = 650) => hops.forEach((h, i) => setTimeout(() => packet(h[0], h[1]), i * gap));

  // ------------------------------------------------------------------ state
  const MAX = 250;            // sns_alerts.TIMESTREAM_LIMIT
  let messages = [];          // imat3a-ADA, producer format
  let vwapLine = [];          // complete windows for the chart
  let forming = null, offset1 = 0, offset2 = 0, mode = "connecting", lastDrop = 0;

  const status = (m, text) => { mode = m; $("status").className = "status " + m; $("statusText").textContent = text; };

  function tail(id, html, partial) {
    const box = $(id), d = document.createElement("div");
    d.className = "msg" + (partial ? " partial" : ""); d.innerHTML = html; box.prepend(d);
    while (box.children.length > 8) box.lastChild.remove();
  }
  const esc = (s) => s.replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));

  function publish(msg, live) {
    messages.push(msg); if (messages.length > 400) messages.shift();
    const off = offset1++;
    if (live) {
      tail("tail1", `<span class="meta">partition 0 · offset ${off} · key ${msg.key} · ts ${msg.timestamp_ms}</span>${esc(JSON.stringify(msg.value))}`);
      bump("binance"); bump("producer", 1, (n) => `${n} sent`); bump("raw");
      chain([["binance", "producer"], ["producer", "raw"], ["raw", "glue"]]);
      // Glue Streaming, update mode: every window containing the new candle is emitted again
      const ups = Lane.vwapUpdates(messages, msg);
      setTimeout(() => {
        ups.forEach((u) => { const o = offset2++; tail("tail2", `<span class="meta">offset ${o} · window ${u.value.window_start.slice(11, 16)}–${u.value.window_end.slice(11, 16)}${u.complete ? " · closed" : " · still open"}</span>${esc(JSON.stringify(u.value))}`, !u.complete); });
        bump("glue", 1, (n) => `${n} triggers`); bump("vwap", ups.length);
        chain([["glue", "vwap"], ["vwap", "ts"]]);
        setTimeout(() => packet("raw", "ts"), 300);
        bump("ts", 1 + ups.length, (n) => `${n} records`);
        $("off2").textContent = `offset ${offset2}`;
      }, 1300);
      setTimeout(() => { evaluate(true); packet("ts", "sns"); bump("sns", 1, (n) => `${n} evaluations`); }, 2600);
    }
    $("off1").textContent = `offset ${offset1}`;
  }

  // ------------------------------------------------------------------ alerts
  let lastEval = null;
  function evaluate(live) {
    const last = messages.slice(-MAX);
    if (last.length < 20) return;
    const res = Lane.evaluateAlerts(last.map((m) => m.value.close), last.map((m) => m.timestamp_ms));
    lastEval = res;
    const box = $("rules"); box.innerHTML = "";
    res.rules.forEach((r) => {
      const d = document.createElement("div"); d.className = "rule" + (r.fired ? " fired" : "");
      const v = r.id.includes("CROSS") ? (Number.isFinite(r.value) ? (r.value >= 0 ? "EMA50 above" : "EMA50 below") : "warming up") : fmt(r.value, 1);
      const state = r.fired ? "FIRED · email goes out" : r.id.includes("CROSS") ? (Number.isFinite(r.value) ? `EMA50 − SMA200 = ${r.value >= 0 ? "+" : ""}${r.value.toFixed(5)}` : "needs 200 closes") : "armed, not triggered";
      d.innerHTML = `<div class="name">${r.id}</div><div class="what">${r.label}</div><div class="val">${v}</div><div class="state">${state}</div>`;
      box.appendChild(d);
    });
    const fired = res.rules.filter((r) => r.fired), mail = $("mail");
    if (fired.length) { mail.style.display = "block"; mail.innerHTML = fired.map((r) => `<b>${esc(r.subject)}</b>${esc(r.body)}`).join("\n\n"); }
    else mail.style.display = "none";
    const t = new Date(last[last.length - 1].timestamp_ms).toISOString().slice(11, 19);
    $("quiet").textContent = fired.length ? `Last evaluation at ${t} UTC over ${last.length} closes: ${fired.length} alert${fired.length > 1 ? "s" : ""} would go out by SNS.` : `Last evaluation at ${t} UTC over ${last.length} closes: no alert conditions triggered.`;
  }

  // ------------------------------------------------------------------ chart
  const cv = $("chart"), cx = cv.getContext("2d");
  let chartQueued = false;
  const drawChart = () => { if (!chartQueued) { chartQueued = true; requestAnimationFrame(() => { chartQueued = false; renderChart(); }); } };
  function renderChart() {
    const d = Math.min(devicePixelRatio || 1, 2), w = cv.clientWidth, h = cv.clientHeight;
    cv.width = w * d; cv.height = h * d; cx.setTransform(d, 0, 0, d, 0, 0); cx.clearRect(0, 0, w, h);
    const data = messages.slice(-MAX);
    if (data.length < 2) return;
    const closes = data.map((m) => m.value.close), times = data.map((m) => m.timestamp_ms);
    const ind = lastEval && lastEval.series.rsi.length === data.length ? lastEval.series : Lane.indicators(closes);
    const vw = vwapLine.filter((p) => p.t >= times[0]);
    const all = closes.concat(vw.map((p) => p.vwap), ind.ema50.filter(Number.isFinite), ind.sma200.filter(Number.isFinite), forming ? [forming.c] : []);
    let lo = Math.min(...all), hi = Math.max(...all); const pad = (hi - lo) * 0.08 || 0.001; lo -= pad; hi += pad;
    const R = 58, L = 6, top = 10, pH = h * 0.66, rTop = pH + 26, rH = h - rTop - 22;
    const t0 = times[0], t1 = times[times.length - 1] + 60000;
    const X = (t) => L + ((t - t0) / (t1 - t0)) * (w - L - R), Yp = (v) => top + (1 - (v - lo) / (hi - lo)) * (pH - top), Yr = (v) => rTop + (1 - v / 100) * rH;
    // grid and labels
    cx.font = "10.5px JetBrains Mono, monospace"; cx.fillStyle = C.dim; cx.strokeStyle = "rgba(95,149,152,0.18)"; cx.lineWidth = 1;
    for (let k = 0; k <= 4; k++) { const v = lo + ((hi - lo) * k) / 4, y = Yp(v); cx.beginPath(); cx.moveTo(L, y); cx.lineTo(w - R, y); cx.stroke(); cx.fillText(v.toFixed(4), w - R + 6, y + 3); }
    const step = w < 520 ? 3600000 : 1800000;
    for (let t = Math.ceil(t0 / step) * step; t < t1; t += step) { const x = X(t); cx.beginPath(); cx.moveTo(x, top); cx.lineTo(x, h - 20); cx.stroke(); cx.fillText(hhmm(t), x - 15, h - 6); }
    const line = (pts, color, width) => { cx.beginPath(); let on = false; pts.forEach(([x, y]) => { if (!Number.isFinite(y)) { on = false; return; } on ? cx.lineTo(x, y) : cx.moveTo(x, y); on = true; }); cx.strokeStyle = color; cx.lineWidth = width; cx.stroke(); };
    line(times.map((t, i) => [X(t), Yp(ind.sma200[i])]), C.sea, 1.6);
    line(times.map((t, i) => [X(t), Yp(ind.ema50[i])]), C.green, 1.4);
    line(vw.map((p) => [X(p.t - 1), Yp(p.vwap)]), C.hot, 1.4);
    line(times.map((t, i) => [X(t), Yp(closes[i])]), C.mist, 1.6);
    if (forming) { const x = X(forming.T), y = Yp(forming.c); cx.setLineDash([3, 3]); line([[X(times[times.length - 1]), Yp(closes[closes.length - 1])], [x, y]], C.mist, 1); cx.setLineDash([]); cx.beginPath(); cx.arc(x, y, 3.5, 0, 7); cx.fillStyle = C.hot; cx.fill(); }
    // RSI panel
    cx.fillStyle = C.dim; cx.fillText("RSI 14", L, rTop - 8);
    [30, 70].forEach((v) => { cx.setLineDash([4, 4]); cx.strokeStyle = "rgba(255,123,123,0.45)"; cx.beginPath(); cx.moveTo(L, Yr(v)); cx.lineTo(w - R, Yr(v)); cx.stroke(); cx.setLineDash([]); cx.fillStyle = C.dim; cx.fillText(String(v), w - R + 6, Yr(v) + 3); });
    for (let i = 1; i < data.length; i++) {
      const a = ind.rsi[i - 1], b = ind.rsi[i]; if (!Number.isFinite(a) || !Number.isFinite(b)) continue;
      cx.beginPath(); cx.moveTo(X(times[i - 1]), Yr(a)); cx.lineTo(X(times[i]), Yr(b));
      cx.strokeStyle = b > 70 || b < 30 ? C.red : C.sea; cx.lineWidth = 1.4; cx.stroke();
    }
    $("clock").textContent = `${hhmm(t0)}–${hhmm(t1)} UTC`;
  }
  addEventListener("resize", drawChart);

  function hero() {
    const data = messages.slice(-MAX); if (!data.length) return;
    const p = forming ? forming.c : data[data.length - 1].value.close, first = data[0].value.close, ch = ((p - first) / first) * 100;
    $("price").innerHTML = `${p.toFixed(4)}<small>USDT</small>`;
    const hrs = Math.round((Date.now() - data[0].timestamp_ms) / 3600000 * 10) / 10;
    $("change").innerHTML = `<span class="${ch >= 0 ? "up" : "down"}">${ch >= 0 ? "+" : ""}${ch.toFixed(2)}%</span> over the last ${hrs} h · ${mode === "live" ? "live from Binance" : "simulated"}`;
  }

  // ------------------------------------------------------------------ data sources
  const REST = ["https://data-api.binance.vision", "https://api.binance.com", "https://api1.binance.com"];
  const WSS = ["wss://data-stream.binance.vision/ws/adausdt@kline_1m", "wss://stream.binance.com:9443/ws/adausdt@kline_1m"];
  const kFromRest = (r) => ({ s: "ADAUSDT", t: r[0], T: r[6], o: r[1], h: r[2], l: r[3], c: r[4], v: r[5], x: true });

  async function seed() {
    for (const host of REST) {
      try {
        const ctl = new AbortController(), timer = setTimeout(() => ctl.abort(), 6000);
        const res = await fetch(`${host}/api/v3/klines?symbol=ADAUSDT&interval=1m&limit=${MAX + 1}`, { signal: ctl.signal });
        clearTimeout(timer);
        if (!res.ok) continue;
        const rows = await res.json(); if (!Array.isArray(rows) || rows.length < 30) continue;
        const now = Date.now();
        rows.forEach((r) => { if (r[6] < now) publish(Lane.producerMessage(kFromRest(r)), false); else forming = { T: r[6], c: parseFloat(r[4]) }; });
        messages.slice(-3).forEach((m, k, a) => tail("tail1", `<span class="meta">partition 0 · offset ${offset1 - a.length + k} · key ${m.key} · ts ${m.timestamp_ms} · from history</span>${esc(JSON.stringify(m.value))}`));
        return true;
      } catch (_) { /* next host */ }
    }
    return false;
  }

  function handleKline(k) {
    forming = { T: k.T, c: parseFloat(k.c) };
    if (k.x) {
      const last = messages[messages.length - 1];
      if (last && last.timestamp_ms >= k.T) return;
      publish(Lane.producerMessage(k), true);
      vwapLine = Lane.vwapSeries(messages);
      forming = null;
    } else if (performance.now() - lastDrop > 2500) {
      lastDrop = performance.now(); packet("binance", "producer", true);  // x=false: the producer ignores open candles
    }
    hero(); drawChart();
  }

  function connect(i = 0) {
    if (i >= WSS.length) { if (mode !== "live") simulate("Binance stream unreachable from this network"); return; }
    let opened = false;
    const ws = new WebSocket(WSS[i]);
    const giveUp = setTimeout(() => { if (!opened) { ws.close(); connect(i + 1); } }, 7000);
    ws.onopen = () => { opened = true; clearTimeout(giveUp); status("live", "LIVE · Binance ADAUSDT kline_1m"); };
    ws.onmessage = (e) => { try { const m = JSON.parse(e.data); if (m.k) handleKline(m.k); } catch (_) {} };
    ws.onerror = () => {};
    ws.onclose = () => { clearTimeout(giveUp); if (opened) { status("connecting", "reconnecting…"); setTimeout(() => connect(0), 3000); } else connect(i + 1); };
  }

  // offline fallback: a labelled random walk, one "minute" every 4 seconds
  let simTimer = null;
  function simulate(why) {
    if (simTimer) return;
    status("sim", innerWidth < 600 ? "SIMULATED · Binance unreachable" : `SIMULATED · ${why}`);
    $("realness").textContent = "Binance is not reachable from your network, so the price below is a labelled simulation.";
    let s = 42; const rnd = () => ((s = (s * 16807) % 2147483647) / 2147483647);
    const gauss = () => { let u = 0; for (let k = 0; k < 6; k++) u += rnd(); return u - 3; };
    let p = messages.length ? messages[messages.length - 1].value.close : 0.62, t = messages.length ? messages[messages.length - 1].timestamp_ms : Math.floor(Date.now() / 60000) * 60000 - 1 - MAX * 60000;
    const candle = () => { p *= Math.exp(gauss() * 0.0011); t += 60000; return { s: "ADAUSDT", T: t, c: p.toFixed(4), v: (40000 + rnd() * 160000).toFixed(1), x: true }; };
    if (messages.length < 30) for (let i = 0; i < MAX; i++) publish(Lane.producerMessage(candle()), false);
    vwapLine = Lane.vwapSeries(messages); evaluate(false); hero(); drawChart();
    let tick = 0;
    simTimer = setInterval(() => {
      tick++;
      if (tick % 8 === 0) handleKline(candle());
      else handleKline({ s: "ADAUSDT", T: t + 60000, c: (p * Math.exp(gauss() * 0.0004)).toFixed(4), v: "0", x: false });
    }, 500);
  }

  (async function start() {
    const ok = await seed();
    vwapLine = Lane.vwapSeries(messages); evaluate(false); hero(); drawChart();
    if (ok) { status("connecting", "history loaded · opening stream…"); connect(0); }
    else simulate("Binance API unreachable from this network");
  })();

  // ------------------------------------------------------------------ batch lane: the monthly summary table
  fetch("market_analysis/summary_ada.csv").then((r) => r.text()).then((txt) => {
    const lines = txt.trim().split(/\r?\n/), head = lines[0].split(",").map((h) => h.replace(/"/g, ""));
    const rows = lines.slice(1).map((l) => { const v = l.split(",").map((x) => x.replace(/"/g, "")); const o = {}; head.forEach((h, i) => (o[h] = h === "symbol" ? v[i] : +v[i])); return o; })
      .sort((a, b) => a.year - b.year || a.month - b.month);
    const svg = $("months"), Wd = 760, x0 = 46, x1 = Wd - 12, top = 18, bottom = 236, n = rows.length, bw = (x1 - x0) / n;
    const lo = Math.min(...rows.map((r) => r.min_close)) * 0.95, hi = Math.max(...rows.map((r) => r.max_close)) * 1.03;
    const X = (i) => x0 + (i + 0.5) * bw, Yv = (v) => bottom - ((v - lo) / (hi - lo)) * (bottom - top);
    for (let k = 0; k <= 4; k++) { const v = lo + ((hi - lo) * k) / 4; el("line", { x1: x0, x2: x1, y1: Yv(v), y2: Yv(v), stroke: "rgba(95,149,152,0.18)" }, svg); el("text", { x: 4, y: Yv(v) + 3, fill: C.dim, "font-size": 10 }, svg).textContent = v.toFixed(2); }
    el("path", { d: "M" + rows.map((r, i) => `${X(i)} ${Yv(r.max_close)}`).join(" L") + " L" + rows.map((r, i) => `${X(i)} ${Yv(r.min_close)}`).reverse().join(" L") + "Z", fill: "rgba(95,149,152,0.25)" }, svg);
    el("path", { d: "M" + rows.map((r, i) => `${X(i)} ${Yv(r.avg_close)}`).join(" L"), fill: "none", stroke: C.mist, "stroke-width": 1.8 }, svg);
    // overbought and oversold days
    const base = 300, sc = 2.2;
    el("line", { x1: x0, x2: x1, y1: base, y2: base, stroke: "rgba(95,149,152,0.4)" }, svg);
    el("text", { x: 4, y: base - 18, fill: C.dim, "font-size": 9.5 }, svg).textContent = "RSI>70";
    el("text", { x: 4, y: base + 26, fill: C.dim, "font-size": 9.5 }, svg).textContent = "RSI<30";
    const groups = rows.map((r, i) => {
      const g = el("g", { class: "m" }, svg);
      el("rect", { class: "hit", x: X(i) - bw / 2, y: top - 10, width: bw, height: 350 - top, fill: "transparent" }, g);
      if (r.days_overbought) el("rect", { x: X(i) - bw * 0.32, y: base - r.days_overbought * sc, width: bw * 0.64, height: r.days_overbought * sc, fill: C.hot }, g);
      if (r.days_oversold) el("rect", { x: X(i) - bw * 0.32, y: base, width: bw * 0.64, height: r.days_oversold * sc, fill: C.red }, g);
      if (r.golden_crosses) el("text", { x: X(i), y: top + 4, fill: C.green, "font-size": 12, "text-anchor": "middle" }, g).textContent = "▲";
      if (r.death_crosses) el("text", { x: X(i), y: top + 4, fill: C.red, "font-size": 12, "text-anchor": "middle" }, g).textContent = "▼";
      if (r.month === 1) el("text", { x: X(i) - bw / 2, y: 356, fill: C.dim, "font-size": 10.5 }, svg).textContent = r.year;
      g.addEventListener("click", () => select(i));
      return g;
    });
    function select(i) {
      groups.forEach((g, k) => g.classList.toggle("sel", k === i));
      const r = rows[i];
      $("rowPath").textContent = `s3://adaquant-data-bucket/summary/ada/year=${r.year}/month=${r.month}/`;
      $("rowJson").textContent = JSON.stringify(r, null, 2);
    }
    // open on the month with the most overbought days, the most eventful row
    let best = 0; rows.forEach((r, i) => { if (r.days_overbought > rows[best].days_overbought) best = i; });
    select(best);
  });
})();
