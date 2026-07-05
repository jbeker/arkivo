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

async function refreshStatus() {
  const status = await api("/api/status", undefined, "GET");
  const rows = status.accounts.map((a) => [
    a.id,
    esc(a.jmap_session_url),
    `${a.counts.indexed}/${a.counts.total} indexed, ${a.counts.staged} staged, ` +
      `${a.counts.quarantined} quarantined, ${a.counts.failed} failed, ${a.counts.unfetched} unfetched`,
    a.backfill_done ? "backfill done" : "backfill pending",
    `cutoff ${a.recency_cutoff_days}d / ${esc(a.deletion_policy)}`,
    `<button onclick="reindexAccount(${a.id})">reindex</button>
     <button onclick="removeAccount(${a.id})">remove</button>`,
  ]);
  document.getElementById("accounts").innerHTML =
    rows.length ? table(rows, ["id", "session URL", "messages", "backfill", "config", ""])
                : "<p>No accounts yet.</p>";
}

async function addAccount() {
  try {
    await api("/api/accounts", {
      jmap_session_url: document.getElementById("acct-url").value,
      token: document.getElementById("acct-token").value,
    });
    document.getElementById("acct-token").value = "";
    msg("acct-msg", "account added — run backfill from the CLI to seed it", true);
    refreshStatus();
  } catch (e) { msg("acct-msg", e.message, false); }
}

async function removeAccount(id) {
  if (!confirm(`Remove account ${id}? The Maildir on disk is kept.`)) return;
  await api(`/api/accounts/${id}`, undefined, "DELETE");
  refreshStatus();
}

async function reindexAccount(id) {
  const r = await api(`/api/accounts/${id}/reindex`, {});
  msg("acct-msg", `${r.restaged} messages restaged; next promote run rebuilds the index`, true);
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
