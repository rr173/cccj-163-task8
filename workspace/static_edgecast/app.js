/* EdgeCast 网页控制台：SSE 实时驱动 + REST 操作。 */
"use strict";

const $ = (s, el = document) => el.querySelector(s);
const $$ = (s, el = document) => [...el.querySelectorAll(s)];
const api = async (path, opts = {}) => {
  const res = await fetch("/api/edgecast" + path, {
    headers: { "Content-Type": "application/json" }, ...opts });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw Object.assign(new Error(body?.error?.message || res.status),
    { code: body?.error?.code, body });
  return body;
};
const post = (path, body) => api(path, { method: "POST", body: JSON.stringify(body || {}) });
const esc = s => String(s ?? "").replace(/[&<>"]/g, c =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const CN = { queued: "排队中", active: "投放中", paused: "已暂停",
  completed: "已完成", revoked: "已撤销" };
const DCN = { pending: "排队", inflight: "投放中", awaiting_retry: "待重试",
  succeeded: "成功", failed: "失败", canceled: "已撤销" };

let SNAP = null, SELECTED = null;

// --------------------------------------------------------------- tabs ----
$$(".tabs button").forEach(btn => btn.addEventListener("click", () => {
  $$(".tabs button").forEach(b => b.classList.remove("active"));
  $$(".tab").forEach(t => t.classList.remove("active"));
  btn.classList.add("active");
  $("#tab-" + btn.dataset.tab).classList.add("active");
}));

// -------------------------------------------------------------- snapshot --
async function refresh() {
  SNAP = await api("/snapshot");
  $("#now").textContent = "服务时钟 " + SNAP.now;
  renderBatches();
  renderRejects();
  renderEvents(SNAP);
  if (SELECTED) renderDetail(SELECTED);
}

function bar(counts, total) {
  const seg = (cls, n) => n > 0 ? `<i class="${cls}" style="width:${n / total * 100}%"></i>` : "";
  return `<div class="bar">${seg("s", counts.succeeded)}${seg("f", counts.failed)}` +
    `${seg("c", counts.canceled)}${seg("i", counts.dispatched - counts.retried
      - counts.succeeded - counts.failed > 0 ? 0 : 0)}` +
    `${seg("p", 0)}${seg("r", counts.retried)}</div>`;
}

function renderBatches() {
  const box = $("#batch-cards");
  if (!SNAP.batches.length) {
    box.innerHTML = `<div class="panel">暂无批次，去「创建批次」新建一个。</div>`;
    return;
  }
  box.innerHTML = SNAP.batches.map(b => {
    const total = b.groups.reduce((n, g) => n + g.pending.length + g.in_flight_now
      + g.retry.length + g.succeeded.length + g.failed.length + g.canceled.length, 0);
    const c = b.counts;
    const inflight = b.groups.reduce((n, g) => n + g.in_flight_now, 0);
    const retry = b.groups.reduce((n, g) => n + g.retry.length, 0);
    const pending = b.groups.reduce((n, g) => n + g.pending.length, 0);
    return `<div class="card ${SELECTED === b.batch_id ? "selected" : ""}" data-id="${b.batch_id}">
      <div class="top"><span class="bid">${b.batch_id}</span>
        <span class="badge ${b.status}">${CN[b.status] || b.status}</span></div>
      <div class="pkg">${esc(b.package_id)} @ <b>${esc(b.package_version)}</b>
        · ${esc(b.note || "")}</div>
      <div class="bar">
        <i class="s" style="width:${c.succeeded / total * 100}%"></i>
        <i class="f" style="width:${c.failed / total * 100}%"></i>
        <i class="c" style="width:${c.canceled / total * 100}%"></i>
        <i class="i" style="width:${inflight / total * 100}%"></i>
        <i class="r" style="width:${retry / total * 100}%"></i>
        <i class="p" style="width:${pending / total * 100}%"></i>
      </div>
      <div class="metrics">
        <span>✅ 成功 <b>${c.succeeded}</b></span>
        <span>❌ 失败 <b>${c.failed}</b></span>
        <span>🚫 撤销 <b>${c.canceled}</b></span>
        <span>📡 在飞 <b>${inflight}</b></span>
        <span>⏳ 待重试 <b>${retry}</b></span>
        <span>🕘 排队 <b>${pending}</b></span>
      </div>
      <div class="metrics" style="margin-top:6px">
        <span>投放次数 <b>${c.dispatched}</b>（重投 ${c.retried}）</span>
        <span>阈值 <b>${b.threshold}${b.threshold_mode === "ratio" ? "（比例）" : ""}</b></span>
        ${b.verdict ? `<span>判定 <b>${b.verdict.verdict === "success" ? "达标" : "未达标"}</b></span>` : ""}
      </div>
    </div>`;
  }).join("");
  $$(".card", box).forEach(card => card.addEventListener("click",
    () => { SELECTED = card.dataset.id; renderBatches(); renderDetail(SELECTED); }));
}

function chips(devices, cls) {
  return `<span class="chip ${cls}">${esc(devices)}</span>`;
}

async function renderDetail(bid) {
  let b;
  try { b = await api(`/batches/${bid}`); } catch { return; }
  const tl = await api(`/batches/${bid}/timeline`);
  const el = $("#batch-detail");
  el.classList.remove("hidden");
  const total = Object.keys(b.devices).length;
  const groups = b.groups.map(g => `
    <div class="group-block">
      <div class="group-head"><b>分组 ${esc(g.name)}</b>
        <span class="kv">并发上限 <b>${g.cap}</b> · 当前在飞 <b>${g.in_flight_now}</b></span></div>
      <div class="chips">
        ${g.pending.map(d => chips(d, "pending")).join("")}
        ${g.inflight.map(d => chips(`${d.device_id} · a${d.attempt} · 至${d.deadline.slice(11, 19)}`, "inflight")).join("")}
        ${g.retry.map(d => chips(`${d.device} · 待 a${d.attempt + 1} · ${d.next_attempt_at.slice(11, 19)}`, "awaiting_retry")).join("")}
        ${g.succeeded.map(d => chips(d, "succeeded")).join("")}
        ${g.failed.map(d => chips(`${d.device_id} · ${d.code}`, "failed")).join("")}
        ${g.canceled.map(d => chips(d, "canceled")).join("")}
      </div>
    </div>`).join("");

  const devRows = Object.values(b.devices).map(d => {
    const cs = d.current_state;
    const extra = d.final
      ? `终态 ${d.final.status}@a${d.final.attempt}`
      : (d.failure_code || `期望序号 ${cs ? cs.expected_seq : "-"}`);
    return `<tr><td>${esc(d.device_id)}</td><td>${esc(d.group)}</td>
      <td><span class="chip ${d.status}">${DCN[d.status] || d.status}</span></td>
      <td>${d.attempt}</td><td>${esc(extra)}</td>
      <td>${cs ? "暂存[" + cs.buffered_seqs.join(",") + "] 已处理[" + cs.processed_seqs.join(",") + "]" : "-"}</td>
      <td>${d.votes}</td></tr>`;
  }).join("");

  el.innerHTML = `
    <div style="display:flex;justify-content:space-between;align-items:center">
      <h2>${b.batch_id} <span class="badge ${b.status}">${CN[b.status] || b.status}</span>
        <small>${esc(b.package_id)} @ ${esc(b.package_version)} · ${esc(b.note)}</small></h2>
      <a class="dl" href="/api/edgecast/batches/${b.batch_id}/summary?format=text" target="_blank">⬇ 下载摘要 TXT</a>
    </div>
    <div class="kv">时间窗 <b>${b.window_start}</b> ~ <b>${b.window_end}</b> ·
      单设备超时 <b>${b.timeout_sec}s</b> · 最多重试 <b>${b.max_retries}</b> ·
      退避 <b>${b.retry_delay_sec}s</b> · 阈值 <b>${b.threshold}${b.threshold_mode === "ratio" ? "（比例）" : ""}</b></div>
    <div class="dtl-actions">
      <button class="warn" data-act="pause">⏸ 暂停</button>
      <button data-act="resume">▶ 恢复</button>
      <button class="danger" data-act="revoke">⛔ 撤销</button>
    </div>
    ${groups}
    <h3>设备明细（${total}）</h3>
    <table><thead><tr><th>设备</th><th>分组</th><th>状态</th><th>当前代次</th><th>终态/原因</th><th>序号空间</th><th>票数</th></tr></thead>
    <tbody>${devRows}</tbody></table>
    <h3>完整时间线（可按分组/设备过滤）</h3>
    <div class="form-row">
      <label>分组过滤<input id="tl-group" placeholder="如 north"></label>
      <label>设备过滤<input id="tl-device" placeholder="如 D1"></label>
      <label>&nbsp;<button id="tl-filter">应用过滤</button></label>
    </div>
    <div class="timeline" id="tl-box">${tl.events.map(fmtEvent).join("\n")}</div>`;

  $$("#batch-detail [data-act]").forEach(btn => btn.addEventListener("click", async () => {
    const reason = btn.dataset.act === "pause"
      ? prompt("暂停原因（可留空）", "") : btn.dataset.act === "revoke"
      ? prompt("撤销原因", "操作员紧急撤销") : "";
    if (reason === null) return;
    try {
      await post(`/batches/${bid}/${btn.dataset.act}`, { reason });
      await refresh();
    } catch (e) { alert(e.message); }
  }));
  $("#tl-filter").addEventListener("click", async () => {
    const g = $("#tl-group").value.trim(), d = $("#tl-device").value.trim();
    const q = new URLSearchParams();
    if (g) q.set("group", g); if (d) q.set("device", d);
    const t2 = await api(`/batches/${bid}/timeline?${q}`);
    $("#tl-box").innerHTML = t2.events.map(fmtEvent).join("\n");
  });
}

function fmtEvent(ev) {
  const d = ev.data;
  const did = d.device_id || d.device || "";
  const slim = Object.fromEntries(Object.entries(d).filter(([k]) => k !== "payload"));
  return `<span class="eid">#${ev.id}</span> <span class="et">${ev.type}</span> ` +
    `<span>${esc(did)}</span> <span class="em">${esc(JSON.stringify(slim))}</span>`;
}

// --------------------------------------------------------------- create --
function addGroup(name = "north", devices = ["D1", "D2"], cap = 2) {
  const div = document.createElement("div");
  div.className = "group-block";
  div.innerHTML = `<div class="group-head">
      分组名 <input name="gname" value="${esc(name)}" style="width:120px">
      并发上限 <input gcap type="number" value="${cap}" style="width:80px">
      设备（逗号分隔） <input gdevs value="${esc(devices.join(","))}" style="flex:1">
      <button class="mini danger grp-del">删除</button></div>`;
  $(".grp-del", div).addEventListener("click", () => div.remove());
  $("#groups-editor").appendChild(div);
}
$("#btn-add-group").addEventListener("click",
  () => addGroup("group-" + ($$("#groups-editor .group-block").length + 1),
    ["DX"], 1));
addGroup(); addGroup("south", ["D3"], 1);

$("#btn-create").addEventListener("click", async () => {
  const groups = $$("#groups-editor .group-block").map(g => ({
    name: $("[name=gname]", g).value.trim(),
    devices: $("[gdevs]", g).value.split(",").map(s => s.trim()).filter(Boolean),
    cap: parseInt($("[gcap]", g).value, 10),
  }));
  const body = {
    package_id: $("#f-pkg").value.trim(),
    package_version: $("#f-ver").value.trim(),
    window_sec: parseFloat($("#f-window").value),
    dispatch_timeout_sec: parseFloat($("#f-timeout").value),
    max_retries: parseInt($("#f-retries").value, 10),
    retry_delay_sec: parseFloat($("#f-retry-delay").value),
    threshold_mode: $("#f-thr-mode").value,
    ack_threshold: $("#f-thr-mode").value === "ratio"
      ? parseFloat($("#f-threshold").value) : parseInt($("#f-threshold").value, 10),
    note: $("#f-note").value, groups,
  };
  try {
    const r = await post("/batches", body);
    $("#create-result").textContent = "已创建 " + r.batch.batch_id
      + "\n" + JSON.stringify(r, null, 2);
    SELECTED = r.batch.batch_id;
    await refresh();
  } catch (e) { $("#create-result").textContent = "[" + e.code + "] " + e.message; }
});

// -------------------------------------------------------------- receipt --
$("#btn-send-receipt").addEventListener("click", async () => {
  const payload = {
    device_id: $("#r-device").value.trim(),
    package_id: $("#r-pkg").value.trim(),
    package_version: $("#r-old").checked ? "0.9.0" : $("#r-ver").value.trim(),
    attempt: parseInt($("#r-attempt").value, 10),
    seq: parseInt($("#r-seq").value, 10),
    status: $("#r-status").value,
    detail: $("#r-detail").value,
  };
  // 正常摘要在浏览器按与服务端一致的规范化算法本地计算；篡改时发送错误 checksum
  const body = { ...payload };
  body.checksum = $("#r-bad").checked ? "sha256:0000000000000000"
    : await localChecksum(payload);
  try {
    const r = await post("/receipts", body);
    $("#receipt-result").textContent = JSON.stringify(r, null, 2);
    await refresh();
  } catch (e) { $("#receipt-result").textContent = "[" + e.code + "] " + e.message; }
});

// 与 edgecast.receipt_hash 完全一致：
// sha256( JSON 按 key 排序 + 紧凑分隔 ensure_ascii )
async function localChecksum(p) {
  const body = {
    device_id: p.device_id, package_id: p.package_id,
    package_version: p.package_version, attempt: p.attempt, seq: p.seq,
    status: p.status, detail: p.detail ?? "",
  };
  const canonical = JSON.stringify(stableSortKeys(body))
    .replace(/[":,]\s*/g, m => m.trim());
  const hash = await crypto.subtle.digest("SHA-256",
    new TextEncoder().encode(canonical));
  return "sha256:" + [...new Uint8Array(hash)]
    .map(b => b.toString(16).padStart(2, "0")).join("");
}
function stableSortKeys(o) {
  if (Array.isArray(o)) return o.map(stableSortKeys);
  if (o && typeof o === "object")
    return Object.keys(o).sort().reduce((a, k) =>
      (a[k] = stableSortKeys(o[k]), a), {});
  return o;
}

// -------------------------------------------------------------- rejects --
function renderRejects() {
  const tb = $("#rejects-table tbody");
  if (!tb) return;
  tb.innerHTML = SNAP.recent_rejects.map(r => `<tr>
    <td>${r.ts}</td><td>${r.kind}</td><td><b>${r.code}</b></td>
    <td>${esc(r.device_id || "")}</td>
    <td>a${r.attempt ?? "-"}#s${r.seq ?? "-"}</td>
    <td>${esc(r.message)}</td></tr>`).join("");
}

// --------------------------------------------------------------- events --
function renderEvents() {
  const box = $("#events-log");
  if (!box) return;
  api("/events?limit=300").then(evs => {
    box.innerHTML = evs.events.slice().reverse().map(fmtEvent)
      .map(s => `<div class="ev">${s}</div>`).join("");
  });
}

// ------------------------------------------------------------------ SSE --
function connect() {
  const es = new EventSource("/api/edgecast/events/stream");
  const dot = $("#conn-dot"), txt = $("#conn-text");
  es.onopen = () => { dot.className = "dot on"; txt.textContent = "实时连接"; };
  es.onerror = () => { dot.className = "dot off"; txt.textContent = "已断开，重连中…"; };
  // 任意事件都触发轻量刷新（服务端事件名即类型）
  es.addEventListener("dispatch_started", refresh);
  es.addEventListener("ack_recorded", refresh);
  es.addEventListener("ack_buffered", refresh);
  es.addEventListener("dispatch_timeout", refresh);
  es.addEventListener("batch_completed", refresh);
  es.addEventListener("batch_revoked", refresh);
  es.addEventListener("batch_paused", refresh);
  es.addEventListener("batch_resumed", refresh);
  es.addEventListener("window_closed", refresh);
  es.addEventListener("ack_rejected", refresh);
  es.addEventListener("ack_conflict", refresh);
  es.addEventListener("ack_stale", refresh);
}

refresh().then(connect).catch(err => {
  $("#conn-text").textContent = "加载失败：" + err.message;
});
setInterval(() => SNAP && ($("#now").textContent = "服务时钟 " + SNAP.now), 1000);
