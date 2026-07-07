// Arkivo web UI: WebAuthn ceremonies + JSON API glue. No framework.

// ---- base64url <-> ArrayBuffer (WebAuthn wire format) ----------------------

function b64uToBuf(s) {
  const pad = "=".repeat((4 - (s.length % 4)) % 4);
  const b64 = (s + pad).replace(/-/g, "+").replace(/_/g, "/");
  return Uint8Array.from(atob(b64), (c) => c.charCodeAt(0)).buffer;
}

function bufToB64u(buf) {
  return btoa(String.fromCharCode(...new Uint8Array(buf)))
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function api(path, body, method = "POST") {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["content-type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch { /* plain-text error */ }
  if (!res.ok) throw new Error(json?.message || text || `HTTP ${res.status}`);
  return json;
}

function msg(id, text, ok) {
  const el = document.getElementById(id);
  if (el) { el.textContent = text; el.className = "feedback " + (ok ? "ok" : "error"); }
}

// ---- ceremony plumbing ------------------------------------------------------

function decodeCreationOptions(ccr) {
  const pk = ccr.publicKey;
  pk.challenge = b64uToBuf(pk.challenge);
  pk.user.id = b64uToBuf(pk.user.id);
  (pk.excludeCredentials || []).forEach((c) => (c.id = b64uToBuf(c.id)));
  return pk;
}

function encodeAttestation(cred) {
  return {
    id: cred.id,
    rawId: bufToB64u(cred.rawId),
    type: cred.type,
    extensions: cred.getClientExtensionResults(),
    response: {
      attestationObject: bufToB64u(cred.response.attestationObject),
      clientDataJSON: bufToB64u(cred.response.clientDataJSON),
    },
  };
}

function decodeRequestOptions(rcr) {
  const pk = rcr.publicKey;
  pk.challenge = b64uToBuf(pk.challenge);
  (pk.allowCredentials || []).forEach((c) => (c.id = b64uToBuf(c.id)));
  return pk;
}

function encodeAssertion(cred) {
  return {
    id: cred.id,
    rawId: bufToB64u(cred.rawId),
    type: cred.type,
    extensions: cred.getClientExtensionResults(),
    response: {
      authenticatorData: bufToB64u(cred.response.authenticatorData),
      clientDataJSON: bufToB64u(cred.response.clientDataJSON),
      signature: bufToB64u(cred.response.signature),
      userHandle: cred.response.userHandle ? bufToB64u(cred.response.userHandle) : null,
    },
  };
}

async function registrationCeremony(startPath, startBody, finishPath) {
  const ccr = await api(startPath, startBody);
  const cred = await navigator.credentials.create({ publicKey: decodeCreationOptions(ccr) });
  return api(finishPath, encodeAttestation(cred));
}

// ---- login page -------------------------------------------------------------

async function initLoginPage() {
  try {
    const r = await api("/auth/setup-needed", undefined, "GET");
    if (r.needed) {
      document.getElementById("setup-card").style.display = "";
      document.querySelectorAll(".auth-card").forEach((el) => (el.style.display = "none"));
    }
  } catch (e) { console.error(e); }
}

async function doSetup() {
  try {
    await registrationCeremony(
      "/auth/setup/start",
      { handle: document.getElementById("setup-handle").value },
      "/auth/setup/finish",
    );
    location.href = "/";
  } catch (e) { msg("setup-msg", e.message, false); }
}

async function doLogin() {
  try {
    const handle = document.getElementById("login-handle").value;
    const rcr = await api("/auth/login/start", { handle });
    const cred = await navigator.credentials.get({ publicKey: decodeRequestOptions(rcr) });
    await api("/auth/login/finish", encodeAssertion(cred));
    location.href = "/";
  } catch (e) { msg("login-msg", e.message, false); }
}

async function doRegister() {
  try {
    await registrationCeremony(
      "/auth/register/start",
      {
        invite_code: document.getElementById("reg-invite").value,
        handle: document.getElementById("reg-handle").value,
      },
      "/auth/register/finish",
    );
    location.href = "/";
  } catch (e) { msg("reg-msg", e.message, false); }
}

async function doRecover() {
  try {
    await registrationCeremony(
      "/auth/recover/start",
      {
        handle: document.getElementById("rec-handle").value,
        recovery_code: document.getElementById("rec-code").value,
      },
      "/auth/recover/finish",
    );
    location.href = "/";
  } catch (e) { msg("rec-msg", e.message, false); }
}

async function doLogout() {
  await api("/auth/logout", {});
  location.href = "/login";
}

// ---- dashboard --------------------------------------------------------------

function table(rows, headers) {
  const head = headers.map((h) => `<th>${h}</th>`).join("");
  const body = rows
    .map((r) => `<tr>${r.map((c) => `<td>${c ?? ""}</td>`).join("")}</tr>`)
    .join("");
  return `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

async function loadDashboard(isAdmin) {
  await Promise.all([
    refreshStatus(),
    refreshTokens(),
    refreshPasskeys(),
    refreshAudit(),
    isAdmin ? refreshAdmin() : Promise.resolve(),
  ]).catch((e) => console.error(e));
}

let refreshTimer = null;

function jobProgress(job) {
  const s = job.stats || {};
  if (job.kind === "backfill") {
    const total = s.total ? ` / ~${s.total}` : "";
    return `Importing \u2014 ${s.fetched ?? 0}${total} messages (page ${s.pages ?? 0})`;
  }
  if (job.kind === "promote" || job.kind === "reindex") {
    const verb = job.kind === "reindex" ? "Rebuilding search index" : "Indexing for search";
    return `${verb} \u2014 ${s.promoted ?? 0} indexed, ${s.failed ?? 0} failed`;
  }
  if (job.kind === "poll") return "Checking for new mail\u2026";
  return `${job.kind} running\u2026`;
}

// A short, human identity for an account: the JMAP host, not the full URL.
function accountLabel(url) {
  try { return esc(new URL(url).host); } catch { return esc(url); }
}

// Readable count breakdown: always show searchable; add the rest only when
// non-zero so a healthy account reads as one clean line.
function messageSummary(c) {
  const parts = [`${c.indexed} searchable`];
  if (c.staged) parts.push(`${c.staged} awaiting indexing`);
  if (c.unfetched) parts.push(`${c.unfetched} downloading`);
  if (c.quarantined) parts.push(`${c.quarantined} quarantined`);
  if (c.failed) parts.push(`${c.failed} failed`);
  return `<strong>${c.total}</strong> messages <span class="muted">·</span> ${parts.join(' <span class="muted">·</span> ')}`;
}

// Surface a failure where the decision is made. An incomplete import wins
// (it's the actionable state); otherwise flag the most recent failed job.
function statusBanner(a, lastByKind, latest) {
  if (!a.backfill_done) {
    const failed = lastByKind.backfill && lastByKind.backfill.status === "failed";
    const detail = failed
      ? `Last attempt failed: ${esc(lastByKind.backfill.error || "unknown error")}`
      : `${a.counts.total} messages imported so far. Resuming continues where it left off.`;
    return `<div class="banner ${failed ? "banner-danger" : "banner-warn"}">
      <div class="banner-body">
        <p class="banner-title">Mail import is incomplete</p>
        <p class="banner-detail">${detail}</p>
      </div>
      <button class="btn btn-primary btn-sm" onclick="startBackfill(${a.id})">Resume import</button>
    </div>`;
  }
  if (latest && latest.status === "failed") {
    const retry = latest.kind === "backfill"
      ? `startBackfill(${a.id})`
      : `startJob(${a.id}, '${latest.kind}')`;
    return `<div class="banner banner-danger">
      <div class="banner-body">
        <p class="banner-title">Last ${esc(latest.kind)} failed</p>
        <p class="banner-detail">${esc(latest.error || "unknown error")}</p>
      </div>
      <button class="btn btn-sm" onclick="${retry}">Retry</button>
    </div>`;
  }
  return "";
}

function accountPanel(a, running) {
  const jobs = a.recent_jobs || [];
  const latest = jobs[0];
  const lastByKind = {};
  jobs.forEach((j) => { if (!lastByKind[j.kind]) lastByKind[j.kind] = j; });

  const statusPill = a.backfill_done
    ? `<span class="pill pill-ok">import complete</span>`
    : `<span class="pill pill-warn">import incomplete</span>`;

  const header = `<div class="actions">
    <strong>${accountLabel(a.jmap_session_url)}</strong>
    ${statusPill}
    <span class="push muted mono">#${a.id} · ${a.recency_cutoff_days}-day cutoff · ${esc(a.deletion_policy)}</span>
  </div>`;

  let body = `<div>${messageSummary(a.counts)}</div>`;

  if (a.counts.staged > 0 && !running.length) {
    body += `<p class="hint">${a.counts.staged} imported but not searchable yet — <strong>Index for search</strong> ` +
      `indexes mail older than ${a.recency_cutoff_days} days. Newer mail is indexed automatically as it ages.</p>`;
  }

  if (running.length) {
    body += running.map((j) =>
      `<div class="actions"><span>${jobProgress(j)}</span>
        <button class="btn btn-sm" onclick="cancelJob(${j.job_id})">Cancel</button></div>`).join("");
  } else {
    body += statusBanner(a, lastByKind, latest);
    const primary = a.backfill_done
      ? `<button class="btn btn-primary" onclick="startJob(${a.id}, 'poll')">Check for new mail</button>`
      : "";
    body += `<div class="actions">
      ${primary}
      <button class="btn" onclick="startJob(${a.id}, 'promote')">Index for search</button>
      <button class="btn btn-ghost" onclick="startBackfill(${a.id}, 'test')">Test import…</button>
      <button class="btn btn-ghost" onclick="changeCutoff(${a.id}, ${a.recency_cutoff_days})">Change cutoff</button>
      <button class="btn btn-ghost" onclick="startJob(${a.id}, 'reindex')">Rebuild index</button>
      <button class="btn btn-danger push" onclick="removeAccount(${a.id})">Remove</button>
    </div>`;
  }

  return `<div class="card card--primary">${header}${body}</div>`;
}

async function refreshStatus() {
  const status = await api("/api/status", undefined, "GET");
  const runningByAccount = {};
  (status.running_jobs || []).forEach((j) => {
    (runningByAccount[j.account_id] ||= []).push(j);
  });

  const panels = status.accounts
    .map((a) => accountPanel(a, runningByAccount[a.id] || []))
    .join('<div style="height: var(--space-md)"></div>');
  document.getElementById("accounts").innerHTML =
    status.accounts.length ? panels
      : `<p class="empty">No accounts yet. Add a Fastmail account below to begin.</p>`;

  // Auto-refresh while anything is running; stop when idle.
  const busy = (status.running_jobs || []).length > 0;
  if (busy && !refreshTimer) {
    refreshTimer = setInterval(refreshStatus, 5000);
  } else if (!busy && refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

// Full import (mode omitted) runs directly — no dialog. `mode === 'test'`
// asks for a small message count for a quick trial and validates it.
async function startBackfill(id, mode) {
  let body = {};
  if (mode === "test") {
    const answer = prompt("Import how many of the oldest messages? (quick test)", "500");
    if (answer === null) return;
    const n = parseInt((answer || "").trim(), 10);
    if (!Number.isInteger(n) || n <= 0) {
      msg("acct-msg", "Enter a whole number greater than 0.", false);
      return;
    }
    body = { limit: n };
  }
  try {
    await api(`/api/accounts/${id}/backfill`, body);
    msg("acct-msg", mode === "test" ? "Test import started." : "Import started.", true);
  } catch (e) { msg("acct-msg", e.message, false); }
  refreshStatus();
}

const JOB_NAMES = {
  poll: "check for new mail",
  promote: "index for search",
  reindex: "rebuild search index",
};

async function startJob(id, kind) {
  if (kind === "reindex" && !confirm(
    "Rebuild the search index? It is cleared and recreated from the archive on disk. " +
    "Your mail is untouched, but search is unavailable until the rebuild finishes.")) return;
  try {
    await api(`/api/accounts/${id}/${kind}`, {});
    msg("acct-msg", `${JOB_NAMES[kind] || kind} started`, true);
  } catch (e) { msg("acct-msg", e.message, false); }
  refreshStatus();
}

async function changeCutoff(id, current) {
  const answer = prompt(
    "Recency cutoff in days — mail becomes searchable only once it is this " +
    "old. Lower values surface mail sooner but shrink the window that keeps " +
    "recent (e.g. password-reset) mail out of agent reach:", String(current));
  if (answer === null) return;
  const days = parseInt(answer.trim(), 10);
  if (!Number.isInteger(days) || days < 0 || days > 365) {
    msg("acct-msg", "cutoff must be a whole number of days between 0 and 365", false);
    return;
  }
  try {
    await api(`/api/accounts/${id}`, { recency_cutoff_days: days }, "PATCH");
    msg("acct-msg",
      `cutoff set to ${days} day(s) — click “Index for search” to index newly eligible mail`, true);
  } catch (e) { msg("acct-msg", e.message, false); }
  refreshStatus();
}

async function cancelJob(jobId) {
  try {
    await api(`/api/jobs/${jobId}/cancel`, {});
    msg("acct-msg", "cancel requested \u2014 the job stops at the next page boundary", true);
  } catch (e) { msg("acct-msg", e.message, false); }
  refreshStatus();
}

async function addAccount() {
  try {
    await api("/api/accounts", {
      jmap_session_url: document.getElementById("acct-url").value,
      token: document.getElementById("acct-token").value,
    });
    document.getElementById("acct-token").value = "";
    msg("acct-msg", "account added — click “Import mail” to download it, then “Index for search” to make it searchable", true);
    refreshStatus();
  } catch (e) { msg("acct-msg", e.message, false); }
}

async function removeAccount(id) {
  if (!confirm(`Remove account ${id}? The Maildir on disk is kept.`)) return;
  await api(`/api/accounts/${id}`, undefined, "DELETE");
  refreshStatus();
}

async function refreshTokens() {
  const data = await api("/api/tokens", undefined, "GET");
  const rows = data.tokens.map((t) => [
    t.id, esc(t.label),
    t.revoked_at ? "revoked" : "active",
    t.last_used_at ?? "never used",
    t.revoked_at ? "" : `<button onclick="revokeToken(${t.id})">revoke</button>`,
  ]);
  document.getElementById("tokens").innerHTML =
    rows.length ? table(rows, ["id", "label", "state", "last used", ""])
      : `<p class="empty">No tokens yet — mint one for an agent to search with.</p>`;
}

async function mintToken() {
  try {
    const r = await api("/api/tokens", { label: document.getElementById("token-label").value });
    document.getElementById("token-msg").innerHTML =
      `Copy it now — shown once: <code>${esc(r.token)}</code>`;
    refreshTokens();
  } catch (e) { msg("token-msg", e.message, false); }
}

async function revokeToken(id) {
  await api(`/api/tokens/${id}`, undefined, "DELETE");
  refreshTokens();
}

async function refreshPasskeys() {
  const data = await api("/api/passkeys", undefined, "GET");
  const rows = data.passkeys.map((p) => [
    p.id, esc(p.label || "(unnamed)"), p.created_at, p.last_used_at ?? "",
    `<button onclick="removePasskey(${p.id})">remove</button>`,
  ]);
  document.getElementById("passkeys").innerHTML =
    table(rows, ["id", "label", "created", "last used", ""]);
}

async function addPasskey() {
  try {
    await registrationCeremony("/auth/add-passkey/start", {}, "/auth/add-passkey/finish");
    msg("passkey-msg", "passkey added", true);
    refreshPasskeys();
  } catch (e) { msg("passkey-msg", e.message, false); }
}

async function removePasskey(id) {
  try {
    await api(`/api/passkeys/${id}`, undefined, "DELETE");
    refreshPasskeys();
  } catch (e) { msg("passkey-msg", e.message, false); }
}

async function refreshAudit() {
  const data = await api("/api/audit", undefined, "GET");
  const rows = data.entries.slice(0, 50).map((e) => [
    e.at, esc(e.actor), esc(e.action), esc(e.message_ref ?? ""),
  ]);
  document.getElementById("audit").innerHTML =
    rows.length ? table(rows, ["at", "actor", "action", "message"])
      : `<p class="empty">No access recorded yet.</p>`;
}

// ---- admin ------------------------------------------------------------------

async function refreshAdmin() {
  const data = await api("/api/admin/users", undefined, "GET");
  const rows = data.users.map((u) => [
    u.id, esc(u.handle), esc(u.role),
    u.disabled_at ? "disabled" : "active",
    `<button onclick="setDisabled(${u.id}, ${u.disabled_at ? "false" : "true"})">
       ${u.disabled_at ? "enable" : "disable"}</button>
     <button onclick="issueRecovery(${u.id})">recovery code</button>`,
  ]);
  document.getElementById("admin-users").innerHTML =
    table(rows, ["id", "handle", "role", "state", ""]);

  const health = await api("/api/admin/health", undefined, "GET");
  document.getElementById("admin-health").innerHTML = table(
    [["postgres", health.postgres], ["opensearch", health.opensearch], ["embedding", health.embedding]]
      .map(([name, ok]) => [name, ok
        ? `<span class="pill pill-ok">up</span>`
        : `<span class="pill pill-danger">down</span>`]),
    ["component", "status"],
  );
}

async function createInvite() {
  const r = await api("/api/admin/invites", { role: document.getElementById("invite-role").value });
  document.getElementById("invite-msg").innerHTML =
    `Invite code (7-day expiry, shown once): <code>${esc(r.code)}</code>`;
}

async function setDisabled(id, disabled) {
  await api(`/api/admin/users/${id}/disabled`, { disabled });
  refreshAdmin();
}

async function issueRecovery(id) {
  const r = await api(`/api/admin/users/${id}/recovery`, {});
  document.getElementById("invite-msg").innerHTML =
    `Recovery code for user ${id} (3-day expiry, shown once): <code>${esc(r.code)}</code>`;
}
