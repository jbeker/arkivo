# Email Archive System: Implementation Specification

Working name: **Arkivo** (placeholder, following the established Esperanto naming convention; *arkivo* = archive). Replaceable.

Version: draft v0.1
Date: 2026-07-05
Status: draft for review

---

## 1. Overview

### 1.1 Purpose

The system ingests a personal email corpus from a single Fastmail account, maintains a searchable archive derived from that corpus, and exposes read-only search to AI agents through a Model Context Protocol (MCP) server. The design treats historical email as a static source rather than granting an agent live mailbox access, which removes the account-takeover risk inherent in exposing a live recovery mailbox to an agent.

### 1.2 Goals

- Maintain a continuously updated, searchable archive of a Fastmail account holding roughly 50GB spanning approximately 30 years.
- Provide keyword, semantic, and hybrid search over message text.
- Expose search to AI agents through a read-only, capability-separated MCP interface.
- Support multiple users, each with an isolated archive and search corpus.
- Use passkeys as the sole authentication mechanism for all interactive human access, including administration.
- Deploy through Docker with data held on standard, tool-agnostic storage.

### 1.3 Non-goals

- No live mailbox access from any agent-facing component.
- No message composition, sending, or any outbound action capability.
- No mutation of the source Fastmail account.
- No password-based or federated interactive login.

---

## 2. Security Model

The security model is foundational and constrains the architecture. It derives from a single observation: a mailbox is a recovery root of trust, so read access to it is partial access to every account that uses that address for recovery.

### 2.1 Threat

An attacker who influences an agent with mailbox read access, most plausibly through prompt injection carried in an inbound message, can trigger a password reset at a third-party service and then read the resulting reset email to complete an account takeover. The attack requires two chained capabilities: reading the reset message, and acting on it. Breaking either link defeats the chain.

### 2.2 Controls

The controls attack the root-of-trust relationship and the capability chain directly, rather than attempting to sanitize adversary-controlled message content as a primary defense.

1. **Historical-source boundary.** The agent never touches the live account. An ingestion service holds the Fastmail credential and writes to a canonical store. Agent-facing components read only the derived archive.

2. **Recency cutoff (primary control).** A message becomes searchable only after it ages past a configured threshold. An attacker-triggered reset email is too recent to have entered the searchable index, so the takeover chain has no readable reset message. This converts the temporal structure of the attack into a defense.

3. **Ingestion-time sanitization (defense in depth).** At promotion into the index, reset-link and one-time-code patterns are redacted, and configurable sender categories are quarantined. This layer is heuristic and secondary to the recency cutoff.

4. **Capability separation.** The reader has no send capability, no outbound web or browser capability, and no co-resident action tools. A malicious message cannot be converted into an outbound action.

5. **Read-only, least-privilege credentials.** The Fastmail credential is a read-only API token. Agent-facing MCP access is read-only and scoped to a single user's corpus.

---

## 3. System Architecture

### 3.1 Components

| Component | Responsibility | Process |
|-----------|----------------|---------|
| Ingestion worker | JMAP poll, fetch, write to canonical store, advance state | Rust CLI, cron-invoked |
| Promotion worker | Move aged messages through sanitization into the index | Rust CLI, cron-invoked |
| Indexer | Extract text, dedup, strip quotes, chunk, embed, write to search backend | Rust, invoked by promotion worker |
| Canonical store | Durable, tool-agnostic message store per user | Maildir on disk |
| Search backend | Keyword and vector search | OpenSearch |
| Metadata store | Users, credentials, JMAP state, job state, audit log | PostgreSQL |
| Web administration service | Management interface and control API | Rust, long-running |
| MCP server | Read-only search tools for agents | Rust, long-running |
| Embedding provider | Text embeddings | Ollama-compatible endpoint |

### 3.2 Data flow

1. The ingestion worker polls Fastmail over JMAP, writes new and changed messages to the per-user Maildir, and advances the stored JMAP state.
2. The promotion worker selects messages whose `receivedAt` is older than the recency cutoff and not yet indexed, applies sanitization, and hands them to the indexer.
3. The indexer extracts text, removes quoted history, chunks long bodies, computes embeddings, and writes message metadata and chunk vectors to the user's OpenSearch indices.
4. The MCP server answers agent queries by running hybrid search against the user's indices and returning sanitized results.
5. The web service manages users, credentials, configuration, and monitoring, authenticated by passkeys.

The Maildir canonical store is the source of truth. The OpenSearch index is a derived artifact and is rebuildable from the canonical store without re-fetching from Fastmail.

---

## 4. Technology Choices

The core engine is implemented in Rust as a single binary exposing subcommands (`backfill`, `poll`, `promote`, `reindex`, `serve-web`, `serve-mcp`). Cron invokes the batch subcommands; the web and MCP services run as long-lived processes.

| Concern | Choice | Notes |
|---------|--------|-------|
| Async runtime | `tokio` | |
| Web framework | `axum` with `tower-http` | |
| Database access | `sqlx` against PostgreSQL | compile-time checked queries |
| Sessions | `tower-sessions` | server-side session store in PostgreSQL |
| WebAuthn | `webauthn-rs` | passkey registration and assertion |
| MIME parsing | `mail-parser` | robust handling of legacy encodings |
| Maildir access | `maildir` | canonical store read and write |
| JMAP client | `jmap-client`, or direct HTTP via `reqwest` | see 7.1 |
| Search client | `opensearch` crate, or direct REST via `reqwest` | |
| HTML to text | `html2text` or equivalent | body normalization |
| Embeddings | `reqwest` to an Ollama-compatible endpoint | model configurable |
| Encryption at rest | `chacha20poly1305` or the `age` crate | credential sealing |
| Logging and tracing | `tracing`, `tracing-subscriber` | structured output |

Attachment text extraction for formats such as PDF and office documents is not reliably available in pure Rust. Attachment extraction is therefore an optional, pluggable stage that may shell out to external tools (for example `pdftotext` from poppler). The default configuration indexes message body text only.

---

## 5. Data Stores

- **Canonical message store.** One Maildir tree per user, on a persistent volume. Attachments remain in place inside their messages and are never copied into the index.
- **Search backend.** OpenSearch, with per-user indices for isolation (see Section 10). Two indices per user: a message index for metadata and full body text (BM25), and a chunk index for semantic vectors (kNN).
- **Metadata store.** PostgreSQL holds users, passkey credentials, sealed Fastmail tokens, JMAP state tokens, job and lock state, per-user configuration, MCP tokens, and the access audit log.
- **Object handling.** Attachments are not stored separately; they stay within the canonical Maildir. Optional extracted attachment text, when enabled, is treated as additional indexable text and is not retained as a blob.

---

## 6. Data Model

### 6.1 PostgreSQL (system metadata)

Outline, not final DDL:

- `users`: `id`, `handle`, `role` (`admin` or `user`), `created_at`, `disabled_at`.
- `passkeys`: `id`, `user_id`, `credential_id`, `public_key`, `sign_count`, `label`, `created_at`. Multiple rows per user for multiple devices.
- `invites`: `id`, `code_hash`, `created_by`, `expires_at`, `consumed_at`.
- `recovery_codes`: `id`, `user_id`, `code_hash`, `expires_at`, `consumed_at`.
- `mail_accounts`: `id`, `user_id`, `jmap_session_url`, `sealed_token`, `read_only` (always true), `created_at`.
- `jmap_state`: `mail_account_id`, `email_state`, `updated_at`. Advanced only after a durable write (see 7.3).
- `jobs`: `id`, `kind`, `mail_account_id`, `status`, `started_at`, `finished_at`, `error`.
- `locks`: `name`, `holder`, `acquired_at`. Single-instance guard for batch subcommands.
- `mcp_tokens`: `id`, `user_id`, `token_hash`, `label`, `created_at`, `revoked_at`.
- `config`: per-user and system settings (recency cutoff, sanitization policy, deletion policy, poll interval).
- `audit_log`: `id`, `user_id`, `actor` (MCP token or session), `action`, `message_ref`, `at`.

### 6.2 OpenSearch (per user)

Message index (`mail-{user_id}-msg`):

- `message_id` (keyword), `thread_id` (keyword), `mailbox_ids` (keyword), `from`, `to`, `cc` (keyword and text), `subject` (text), `received_at` (date), `size` (long), `body_text` (text, BM25), `has_attachments` (boolean), `sanitized` (boolean).

Chunk index (`mail-{user_id}-chunk`):

- `message_id` (keyword), `chunk_index` (integer), `chunk_text` (text), `embedding` (knn_vector, dimension set by the embedding model).

Hybrid retrieval runs BM25 over the message index and kNN over the chunk index, then fuses results keyed on `message_id`, using an OpenSearch search pipeline with score normalization or reciprocal rank fusion.

---

## 7. Ingestion Pipeline

### 7.1 JMAP poll loop

The ingestion worker uses the JMAP state model:

1. Load the stored `email_state` for the account.
2. Call `Email/changes` with that state to obtain created, updated, and destroyed IDs and a new state.
3. Call `Email/get` for created and updated IDs, requesting only index-relevant properties: headers, text and HTML `bodyValues`, `keywords`, `mailboxIds`, `receivedAt`, and `size`. Attachment blobs stay out of this loop and are fetched through `Blob/get` only when attachment extraction is enabled.
4. Write fetched messages to the canonical Maildir.
5. Apply destroyed IDs per the deletion policy (7.6).
6. Persist the new state per the crash-safety rule (7.3).

`Email/changes` and the dependent `Email/get` are chained in one JMAP request using result references, holding the loop to one round trip per cycle under normal conditions.

The JMAP client may be the `jmap-client` crate or a direct `reqwest` implementation of the small set of methods required (`Email/query`, `Email/changes`, `Email/get`, `Blob/get`).

### 7.2 Backfill

The initial seed records the account state at the start, then pages through all message IDs with `Email/query` using position or anchor pagination, fetching bodies and metadata with `Email/get` in batches. After the backfill completes, the first incremental poll continues from the recorded state.

### 7.3 State persistence and crash safety

The stored JMAP state is advanced only after the fetched batch is durably written to the canonical store. A run that fails partway retries from the last good state on the next interval, re-fetching rather than skipping messages. The loop is therefore idempotent under interruption.

If a stored state is too old for the server to compute a delta, `Email/changes` returns `cannotCalculateChanges`, and the worker falls back to a full `Email/query` resync. This fallback is implemented from the outset.

### 7.4 Scheduling and single-instance guard

The poll model is used; there is no persistent push connection. Cron invokes the `poll` subcommand on an interval. Because the recency cutoff governs when a message becomes searchable, poll frequency does not drive search freshness; an interval of roughly 15 minutes to one hour suffices, and a lower frequency is acceptable when the cutoff is measured in days.

A single-instance guard, implemented as a row in the `locks` table or a lockfile, prevents a slow run from overlapping the next scheduled start and racing on the state record.

### 7.5 Staging, recency cutoff, and promotion

Fetched messages land in the canonical store immediately but are not indexed until they age past the recency cutoff. The `promote` subcommand, scheduled independently, selects messages older than the cutoff that are not yet indexed, applies sanitization, and passes them to the indexer. Its cadence is set by the cutoff granularity, independent of the poll interval.

### 7.6 Deletion policy

`Email/changes` reports destroyed IDs. The policy is configurable:

- **retain** (default): destroyed IDs are recorded in the audit log but the corresponding messages remain in the canonical store and index. An archive retains history the live account later removes.
- **mirror**: destroyed IDs are removed from the canonical store and index.

---

## 8. Indexing Pipeline

For each promoted message the indexer:

1. Parses the message with `mail-parser`, selecting the text body, or converting the HTML body to text when no plain-text part exists.
2. Strips quoted reply history so repeated passages across a thread are not embedded multiple times.
3. Applies Message-ID deduplication, which has a minor role given the single-account Fastmail source but covers messages filed into more than one folder.
4. Chunks long bodies into embedding-sized segments.
5. Computes embeddings for each chunk through the Ollama-compatible endpoint.
6. Writes message metadata and body text to the message index and chunk text with vectors to the chunk index.

Estimated volume after chunking is on the order of 0.5 million to 3 million vectors for the stated corpus, which sits within OpenSearch kNN operating range.

---

## 9. Search and MCP Interface

The MCP server exposes read-only tools and holds no capability beyond search:

- `search`: hybrid keyword and semantic query, returning ranked message references with snippets.
- `get_message`: return sanitized metadata and body text for a message reference.
- Optional `list_facets`: return sender, date-range, or mailbox facets for query refinement.

The MCP server has no send capability, no outbound web or browser capability, and no write path to any store. Each request is authenticated by a per-user MCP token and scoped to that user's indices only. Every message access is written to the audit log. Results are drawn from the index, which contains only post-cutoff, sanitized content.

---

## 10. Multi-User and Tenancy Model

- **Roles.** `admin` manages the system, users, and invites. `user` owns one or more mail accounts and the corresponding archive.
- **Isolation.** Each user has a dedicated Maildir tree, dedicated OpenSearch indices (`mail-{user_id}-msg` and `mail-{user_id}-chunk`), a dedicated sealed Fastmail token, and dedicated MCP tokens. No query path crosses user boundaries.
- **Onboarding.** An admin issues an invite. The invitee registers, enrolls a passkey, and adds a Fastmail account by supplying a read-only API token, which is sealed before storage.
- **Per-user configuration.** Recency cutoff, sanitization policy, deletion policy, and poll interval are configurable per user, with system defaults.

---

## 11. Authentication and Authorization

### 11.1 Interactive authentication: passkeys only

Passkeys (WebAuthn) are the sole interactive authentication mechanism for the web administration interface and all human access. There is no password, no email-link, and no federated login.

- **Registration.** An invite code authorizes a WebAuthn registration ceremony (`navigator.credentials.create`). The resulting credential public key is stored in `passkeys`. Multiple passkeys per user are supported for multiple devices.
- **Login.** A WebAuthn assertion (`navigator.credentials.get`) is verified by `webauthn-rs`, establishing a server-side session held in PostgreSQL through `tower-sessions`.
- **Recovery.** A lost passkey is recovered through an admin-issued, single-use recovery code that authorizes enrollment of a new passkey. This mirrors the recovery pattern used elsewhere in the operator's infrastructure.

### 11.2 Programmatic authentication: MCP tokens

MCP clients are non-interactive and cannot perform a WebAuthn ceremony. Agent access therefore uses per-user bearer tokens, minted within a passkey-authenticated web session and revocable from it. These tokens grant only read-only search scoped to a single user's corpus. They are not administrative credentials and do not authenticate to the administration interface, so passkeys remain the sole authentication mechanism for administration. Tokens are stored as hashes and presented over TLS.

### 11.3 Authorization

Session and token checks run as `axum` middleware. Administrative routes require the `admin` role. User routes require ownership of the referenced resource. MCP queries are constrained to the token's user indices before reaching OpenSearch.

---

## 12. Web Administration Interface

Features by role.

**User:**
- Enroll and manage passkeys.
- Add or remove a Fastmail account and its read-only token.
- View ingestion status: last poll, backfill progress, staging backlog, index counts.
- Configure recency cutoff, sanitization policy, deletion policy, and poll interval.
- Mint and revoke MCP tokens.
- Trigger a manual reindex from the canonical store.
- View the personal access audit log.

**Admin:**
- Manage users: create invites, disable or enable users, issue recovery codes.
- View system health across workers, OpenSearch, PostgreSQL, and the embedding endpoint.
- Set system defaults.

The interface is server-rendered or a small single-page client served by the `axum` service. No frontend framework is mandated by this specification.

---

## 13. Configuration

Configuration is supplied through environment variables and a configuration file, resolved at process start. Key settings:

- Fastmail JMAP session URL and per-account sealed token reference.
- Recency cutoff duration.
- Poll interval and promotion interval (as cron schedules).
- Sanitization policy: redaction patterns and quarantined sender categories.
- Deletion policy: `retain` or `mirror`.
- Embedding endpoint URL and model name.
- OpenSearch endpoint and credentials.
- PostgreSQL connection string.
- Master key reference for credential sealing.

---

## 14. Deployment

Docker Compose services:

- `postgres`: metadata store, persistent volume.
- `opensearch`: search backend, persistent volume.
- `arkivo-web`: long-running web administration service.
- `arkivo-mcp`: long-running read-only MCP server.
- `arkivo-worker`: optional container running cron for `poll`, `promote`, and periodic maintenance, or these subcommands are scheduled by the host cron already in use on the homelab.
- Embedding provider: an existing Ollama-compatible endpoint, referenced rather than bundled.

Volumes: the per-user Maildir root, the OpenSearch data directory, and the PostgreSQL data directory. The engine is a single image invoked with different subcommands across the service and worker containers.

---

## 15. Observability and Operations

- **Logging.** Structured logs through `tracing`, with per-job correlation identifiers.
- **Health.** Liveness and readiness endpoints on the web and MCP services. Job status recorded in the `jobs` table and surfaced in the interface.
- **Metrics.** Counters and gauges for messages fetched, staged, promoted, indexed, vectors written, and poll and promotion durations, exposed for the existing Zabbix monitoring.
- **Audit.** Every MCP message access recorded in `audit_log`. Anomaly review, for example a reset-pattern access shortly after agent activity, is supported by this log.
- **Backup.** The canonical Maildir and the PostgreSQL database are the backup targets. The OpenSearch index is reconstructible and need not be backed up, though snapshots may be taken to avoid full reindex time.

---

## 16. Security Considerations

- Fastmail tokens are sealed with an authenticated cipher before storage and unsealed only in the ingestion worker.
- The MCP server and the ingestion worker are separate processes with separate credentials; the MCP server never holds a Fastmail token.
- The agent-facing surface is read-only and capability-separated, with no send or outbound path.
- TLS terminates in front of the web and MCP services.
- The recency cutoff and sanitization run before any content is reachable by an agent.
- MCP tokens and passkey credentials are stored only as hashes or public keys, never as recoverable secrets.

---

## 17. Open Decisions

- **Deletion policy default.** Set to `retain` in this draft. Confirm whether the archive should preserve messages deleted in Fastmail or mirror deletions.
- **Attachment text extraction.** Disabled by default. Decide whether to enable extraction for document attachments and, if so, which external tools are acceptable in the container image.
- **Recency cutoff duration.** A concrete default, for example seven days, is to be set against the operator's tolerance for search staleness versus reset-window safety.
- **Chunking parameters and embedding model.** Chunk size, overlap, and the specific Ollama embedding model and vector dimension are to be fixed during implementation.
- **Hybrid fusion method.** Score normalization versus reciprocal rank fusion in the OpenSearch search pipeline.

---

## 18. Phased Implementation Plan

1. **Foundation.** Single-binary skeleton, configuration loading, PostgreSQL schema and migrations, sealed-credential handling.
2. **Ingestion.** JMAP client, backfill, poll loop, state persistence, crash-safety, locking, canonical Maildir writes.
3. **Indexing.** Text extraction, quote stripping, dedup, chunking, embeddings, OpenSearch mappings and writes.
4. **Promotion and security controls.** Recency cutoff, staging selection, sanitization, deletion policy.
5. **Search and MCP.** Hybrid query, MCP tools, per-user scoping, audit logging, capability separation.
6. **Authentication and web interface.** WebAuthn registration and login, sessions, invites and recovery, user and admin interfaces, MCP token management.
7. **Multi-user hardening.** Per-user index isolation, authorization middleware, cross-user access tests.
8. **Operations.** Metrics for Zabbix, health endpoints, backup and reindex procedures, Docker Compose packaging.

