# Arkivo

Email archive with recency-gated, agent-safe search. Arkivo ingests
mail — a Fastmail account over JMAP, or a Gmail account via the Gmail
API with OAuth — into a canonical Maildir store, promotes messages past
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
adding a Fastmail account (the token is validated live, then sealed),
starting/cancelling the initial backfill with progress, manual
poll/promote/reindex, MCP tokens, and status. The CLI subcommands exist
for automation (the worker container's cron uses them) and as a fallback:

| Subcommand   | Role |
|--------------|------|
| `serve-web`  | Web administration UI (WebAuthn passkeys only) |
| `serve-mcp`  | Read-only MCP search server (bearer tokens) |
| `poll`       | Incremental sync, JMAP or Gmail per account (`--account N` or `--all`) — cron entry point |
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
2. Add your Fastmail account (read-only API token), or press **Connect
   Gmail** and approve read-only access at Google. Either credential is
   validated live before it is sealed and stored.
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
