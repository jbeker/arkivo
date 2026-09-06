# Arkivo

Email archive with recency-gated, agent-safe search. Arkivo ingests
mail — a Fastmail account over JMAP, a Gmail account via the Gmail API
with OAuth, a Microsoft 365 mailbox via Graph, or any mailbox over IMAP
— into a canonical Maildir store,
promotes messages past
a configurable **recency cutoff** through sanitization into OpenSearch
(BM25 + semantic vectors), and exposes read-only hybrid search to AI
agents via an MCP server.

The security model (see `email-archive-spec-0.1.md`): a mailbox is a
recovery root of trust, so agents never touch the live account. A
message becomes searchable only after it ages past the cutoff (default
7 days), which means an attacker-triggered password-reset email is never
visible to an agent. Reset links and one-time codes are additionally
redacted at promotion, and the MCP surface is read-only, per-user
scoped, and fully audit-logged.

## Components

One binary, several subcommands:

Everything is operable from the web interface: first-run admin setup,
adding mail accounts (credentials are validated live, then sealed),
starting/cancelling the initial backfill with progress, manual
poll/promote/reindex, MCP tokens, and status. The CLI subcommands exist
for automation (the worker container's cron uses them) and as a fallback:

| Subcommand   | Role |
|--------------|------|
| `serve-web`  | Web administration UI (WebAuthn passkeys only) |
| `serve-mcp`  | Read-only MCP search server (bearer tokens) |
| `poll`       | Incremental sync, JMAP, Gmail, Microsoft 365, or IMAP per account (`--account N` or `--all`) — cron entry point |
| `promote`    | Move aged messages through sanitization into the index — cron entry point |
| `backfill`   | Seed the archive from the mail source (resumable; `--limit`, `--since`) |
| `reindex`    | Rebuild the search index from the Maildir (`--recreate` for mapping changes) |
| `migrate`    | Apply database migrations (also runs at serve-web startup) |
| `bootstrap`  | Mint an admin invite from the CLI (fallback; the web UI offers first-run setup) |

## Development

```sh
docker compose -f docker/compose.dev.yaml up -d   # postgres + opensearch
cp arkivo.toml.example arkivo.toml                # edit as needed
head -c 32 /dev/urandom | xxd -p -c 64 > data/master.key
cargo run -- migrate
cargo test                                        # unit + Postgres tests
ARKIVO_TEST_OPENSEARCH_URL=http://localhost:9200 cargo test  # + search/MCP/E2E
```

Configuration comes from `arkivo.toml` overlaid with `ARKIVO_*`
environment variables (`__` separates nesting: `ARKIVO_OPENSEARCH__URL`).

## Deployment

```sh
cd docker
mkdir -p secrets && head -c 32 /dev/urandom | xxd -p -c 64 > secrets/master.key
cat > .env <<EOF
POSTGRES_PASSWORD=...
OLLAMA_URL=http://your-ollama-host:11434
RP_ID=arkivo.example.com
RP_ORIGIN=https://arkivo.example.com
EOF
docker compose up -d
```

Put a TLS-terminating reverse proxy in front of ports 8080 (web) and
8081 (MCP). WebAuthn requires a secure context: `RP_ID`/`RP_ORIGIN`
must match the public domain exactly.

Everything else happens in the browser:

1. Open the web UI — with no users yet, it offers **first-run setup**:
   create the first admin with a passkey (no invite needed; the flow
   closes permanently once a user exists).
2. Add your mail account: a Fastmail read-only API token, **Connect
   Gmail** with read-only access approved at Google, **Connect
   Microsoft 365** with read-only access approved at Microsoft, or an
   IMAP host/username/password. Every credential is validated live,
   then sealed and stored.
3. Press **start backfill** — optionally with a message limit as a
   smoke test first. Progress is shown live; the job is cancellable and
   resumable (multi-hour for large mailboxes; interruptions are safe).

### Gmail

Gmail ingestion needs a Google Cloud OAuth client (for a Workspace
account, an **Internal** app — no verification): enable the Gmail API,
add the `gmail.readonly` scope to the consent screen, and register the
redirect URI `<rp_origin>/oauth/google/callback`. Then configure:

```toml
[google]
client_id = "...apps.googleusercontent.com"
client_secret = "..."   # or ARKIVO_GOOGLE__CLIENT_SECRET
```

The refresh token is sealed like the Fastmail token. Spam and Trash are
never archived; deletions upstream follow the account's deletion policy.

### Microsoft 365

Office 365 and personal Microsoft mailboxes are archived over the Graph
API; Exchange Online no longer accepts IMAP passwords. Register an app
in the Entra admin center: Web platform, redirect URI
`<rp_origin>/oauth/microsoft/callback`, a client secret (note its
expiry), and the delegated permissions `Mail.Read`, `User.Read`, and
`offline_access`. Choose "any organizational directory and personal
accounts" for the default `tenant = "common"`, or single-tenant and set
`tenant` to the tenant id. Then configure:

```toml
[microsoft]
client_id = "..."
client_secret = "..."   # or ARKIVO_MICROSOFT__CLIENT_SECRET
tenant = "common"
```

The refresh token is sealed like the other credentials; Microsoft
rotates it on every use, so jobs rewrite the sealed value. Messages are
identified by their Graph immutable id and filed under the folder's
display path ("Inbox", "Archive/2024"). Deleted Items and Junk Email
are never archived, so moving a message there counts as a deletion
under the account's deletion policy, unlike Gmail where Trash is a
label. A backfill with a `since` date bakes that floor into the sync
state: newer mail is always picked up, but an older message later moved
into a folder is not; run a backfill without `since` to widen.

### IMAP

Any other mailbox is archived over plain IMAP: add it in the dashboard
with host, username, and password (use an app password where your
provider offers one). The login is validated live, then sealed like the
other credentials. The connection defaults to implicit TLS on port 993;
the form also offers STARTTLS on 143. No config-file changes are
needed, and the CLI subcommands work unchanged.

IMAP has no stable message identity — UIDs are per-folder and a move
renumbers the message — so Arkivo identifies each message by the
SHA-256 of its raw bytes and tracks folder placements separately. A
moved message is matched by Message-ID and re-homed, never downloaded
twice; a message copied into several folders is one archived message
with several placements, and the deletion policy applies only when the
last placement disappears. A folder whose UIDVALIDITY, UIDNEXT, and
message count are all unchanged is skipped entirely during a poll; the
tradeoff is that flag-only changes (read, flagged) in a quiet folder
wait for the folder's next addition or deletion. As with the other
providers, Trash and Junk are never archived; other folders, including
ones created later, are picked up automatically.

For Gmail accounts, prefer **Connect Gmail**: over IMAP every label
becomes a folder.

The worker container polls and promotes on a schedule (`docker/crontab`);
the same jobs can be triggered manually per account from the dashboard.
Mint an MCP token in the dashboard and point your agent at
`https://arkivo.example.com:.../mcp` with `Authorization: Bearer <token>`.

## Operations

- **Backups**: the Maildir volume and `pg_dump` of Postgres are the
  backup targets. The OpenSearch index is derived and rebuildable with
  `reindex` (hours of embedding time for large corpora; OpenSearch
  snapshots are an optional shortcut).
- **Monitoring**: `GET /metrics` on the web service returns JSON
  counters (messages fetched/staged/indexed/quarantined/failed, last
  job status/age/duration per account) for Zabbix HTTP-agent items.
  `/healthz` and `/readyz` on both services.
- **Audit**: every MCP access — including failed probes — is written to
  `audit_log` and visible in the dashboard.
- **Key rotation**: sealed credentials carry a `seal_key_id`; introduce
  a new key file, re-add accounts (tokens re-seal on save).

## License

AGPL-3.0-or-later — see [LICENSE](LICENSE).
