# Arkivo

Email archive with recency-gated, agent-safe search. Arkivo ingests a
Fastmail account over JMAP into a canonical Maildir store, promotes
messages past a configurable **recency cutoff** through sanitization
into OpenSearch (BM25 + semantic vectors), and exposes read-only hybrid
search to AI agents via an MCP server.

The security model (see `email-archive-spec-0.1.md`): a mailbox is a
recovery root of trust, so agents never touch the live account. A
message becomes searchable only after it ages past the cutoff (default
7 days), which means an attacker-triggered password-reset email is never
visible to an agent. Reset links and one-time codes are additionally
redacted at promotion, and the MCP surface is read-only, per-user
scoped, and fully audit-logged.

## Components

One binary, several subcommands:

| Subcommand   | Role |
|--------------|------|
| `bootstrap`  | Mint the first admin invite (passkey-only auth bootstrap) |
| `migrate`    | Apply database migrations |
| `backfill`   | Seed the archive from Fastmail (resumable; `--limit`, `--since`) |
| `poll`       | Incremental JMAP sync (`--account N` or `--all`) |
| `promote`    | Move aged messages through sanitization into the index |
| `reindex`    | Rebuild the search index from the Maildir (`--recreate` for mapping changes) |
| `serve-web`  | Web administration UI (WebAuthn passkeys only) |
| `serve-mcp`  | Read-only MCP search server (bearer tokens) |

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
docker compose run --rm arkivo-web bootstrap   # prints the admin invite
```

Put a TLS-terminating reverse proxy in front of ports 8080 (web) and
8081 (MCP). WebAuthn requires a secure context: `RP_ID`/`RP_ORIGIN`
must match the public domain exactly.

First archive: register at `/login` with the invite, add your Fastmail
account (read-only API token) in the dashboard, then run the initial
backfill (multi-hour for large mailboxes; resumable — safe to interrupt):

```sh
docker compose run --rm arkivo-web backfill --account 1 --limit 500  # smoke test
docker compose run --rm arkivo-web backfill --account 1             # full run
```

The worker container polls and promotes on a schedule (`docker/crontab`).
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
