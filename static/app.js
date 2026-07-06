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
  if (el) { el.textContent = text; el.className = ok ? "ok" : "error"; }
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
  return `<table><tr>${head}</tr>${body}</table>`;
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

async function refreshStatus() {
  const status = await api("/api/status", undefined, "GET");
  const runningByAccount = {};
  (status.running_jobs || []).forEach((j) => {
    (runningByAccount[j.account_id] ||= []).push(j);
  });

  const rows = status.accounts.map((a) => {
    const running = runningByAccount[a.id] || [];
    const progress = running
      .map((j) => `${jobProgress(j)} <button onclick="cancelJob(${j.job_id})">cancel</button>`)
      .join("<br>");
    const actions = running.length
      ? progress
      : `<button onclick="startBackfill(${a.id})">${a.backfill_done ? "Re-import mail" : "Import mail"}</button>
         <button onclick="startJob(${a.id}, 'poll')">Check for new mail</button>
         <button onclick="startJob(${a.id}, 'promote')">Index for search</button>
         <button onclick="startJob(${a.id}, 'reindex')">Rebuild search index</button>
         <button onclick="removeAccount(${a.id})">Remove</button>`;

    // Readable message breakdown: always show searchable; add the rest
    // only when non-zero to keep it uncluttered.
    const c = a.counts;
    const parts = [`${c.indexed} searchable`];
    if (c.staged) parts.push(`${c.staged} awaiting indexing`);
    if (c.unfetched) parts.push(`${c.unfetched} downloading`);
    if (c.quarantined) parts.push(`${c.quarantined} quarantined`);
    if (c.failed) parts.push(`${c.failed} failed`);
    let messages = `${c.total} messages — ${parts.join(" · ")}`;
    if (c.staged > 0 && !running.length) {
      messages += `<div class="hint">${c.staged} imported but not searchable yet — click ` +
        `<strong>Index for search</strong> to index messages older than ${a.recency_cutoff_days} ` +
        `days (newer mail is indexed automatically as it ages; the hourly worker also does this).</div>`;
    }

    return [
      a.id,
      esc(a.jmap_session_url),
      messages,
      a.backfill_done ? "complete" : "incomplete",
      `${a.recency_cutoff_days}-day cutoff · ${esc(a.deletion_policy)}`,
      actions,
    ];
  });
  document.getElementById("accounts").innerHTML =
    rows.length ? table(rows, ["id", "account", "messages", "import", "config", "actions"])
                : "<p>No accounts yet.</p>";

  // Auto-refresh while anything is running; stop when idle.
  const busy = (status.running_jobs || []).length > 0;
  if (busy && !refreshTimer) {
    refreshTimer = setInterval(refreshStatus, 5000);
  } else if (!busy && refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

async function startBackfill(id) {
  const answer = prompt(
    "Import all mail? Leave empty for a full import (can run for hours; " +
    "cancellable and resumable). Or enter a message count for a quick test:", "");
  if (answer === null) return;
  const body = answer.trim() ? { limit: parseInt(answer, 10) } : {};
  try {
    await api(`/api/accounts/${id}/backfill`, body);
    msg("acct-msg", "import started", true);
  } catch (e) { msg("acct-msg", e.message, false); }
  refreshStatus();
}

const JOB_NAMES = {
  poll: "check for new mail",
  promote: "index for search",
  reindex: "rebuild search index",
};

async function startJob(id, kind) {
  try {
    await api(`/api/accounts/${id}/${kind}`, {});
    msg("acct-msg", `${JOB_NAMES[kind] || kind} started`, true);
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
    rows.length ? table(rows, ["id", "label", "state", "last used", ""]) : "<p>No tokens.</p>";
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
    rows.length ? table(rows, ["at", "actor", "action", "message"]) : "<p>No activity.</p>";
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
      .map(([name, ok]) => [name, ok ? "✅ up" : "❌ down"]),
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
