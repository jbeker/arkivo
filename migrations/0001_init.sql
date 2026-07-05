-- Arkivo initial schema (spec §6.1, with the messages ledger driving
-- promotion, backfill resumability, and crash safety).

create table users (
    id          bigint generated always as identity primary key,
    handle      text not null unique,
    role        text not null check (role in ('admin', 'user')),
    created_at  timestamptz not null default now(),
    disabled_at timestamptz
);

create table passkeys (
    id            bigint generated always as identity primary key,
    user_id       bigint not null references users (id) on delete cascade,
    credential_id bytea not null unique,
    -- Serialized webauthn-rs Passkey (public key, sign count, policies).
    passkey       jsonb not null,
    label         text not null default '',
    created_at    timestamptz not null default now(),
    last_used_at  timestamptz
);

create table invites (
    id          bigint generated always as identity primary key,
    code_hash   bytea not null unique,
    created_by  bigint references users (id) on delete set null,
    role        text not null default 'user' check (role in ('admin', 'user')),
    expires_at  timestamptz not null,
    consumed_at timestamptz,
    consumed_by bigint references users (id) on delete set null
);

create table recovery_codes (
    id          bigint generated always as identity primary key,
    user_id     bigint not null references users (id) on delete cascade,
    code_hash   bytea not null unique,
    expires_at  timestamptz not null,
    consumed_at timestamptz
);

create table mail_accounts (
    id                  bigint generated always as identity primary key,
    user_id             bigint not null references users (id) on delete cascade,
    jmap_session_url    text not null,
    -- JMAP accountId, learned from the first session fetch.
    account_id          text,
    sealed_token        bytea not null,
    seal_key_id         text not null,
    recency_cutoff_days int not null default 7,
    deletion_policy     text not null default 'retain'
                        check (deletion_policy in ('retain', 'mirror')),
    poll_interval_secs  int,
    sanitize_policy     jsonb,
    created_at          timestamptz not null default now()
);

create table jmap_state (
    mail_account_id bigint primary key references mail_accounts (id) on delete cascade,
    email_state     text,
    backfill_done   boolean not null default false,
    -- Resumable Email/query cursor for an in-progress backfill.
    backfill_anchor jsonb,
    updated_at      timestamptz not null default now()
);

create table messages (
    id               bigint generated always as identity primary key,
    mail_account_id  bigint not null references mail_accounts (id) on delete cascade,
    jmap_email_id    text not null,
    blob_id          text not null,
    message_id_hdr   text,
    thread_id        text,
    received_at      timestamptz not null,
    size             bigint not null default 0,
    has_attachments  boolean not null default false,
    from_addr        text,
    subject          text,
    mailbox_ids      jsonb not null default '[]',
    keywords         jsonb not null default '{}',
    -- Relative path inside the user's Maildir; null until the blob is
    -- durably written. Doubles as the backfill refetch list.
    maildir_path     text,
    fetched_at       timestamptz,
    indexed_at       timestamptz,
    index_status     text not null default 'staged'
                     check (index_status in ('staged', 'indexed', 'quarantined', 'failed')),
    pipeline_version int,
    error            text,
    deleted_at       timestamptz,
    unique (mail_account_id, jmap_email_id)
);

-- The promotion scan is exactly this partial index.
create index idx_messages_promotable
    on messages (mail_account_id, received_at)
    where indexed_at is null and deleted_at is null and maildir_path is not null;

create index idx_messages_msgid on messages (mail_account_id, message_id_hdr);

create table mcp_tokens (
    id           bigint generated always as identity primary key,
    user_id      bigint not null references users (id) on delete cascade,
    token_hash   bytea not null unique,
    label        text not null default '',
    created_at   timestamptz not null default now(),
    last_used_at timestamptz,
    revoked_at   timestamptz
);

create table jobs (
    id              bigint generated always as identity primary key,
    kind            text not null,
    mail_account_id bigint references mail_accounts (id) on delete cascade,
    status          text not null default 'running'
                    check (status in ('running', 'succeeded', 'failed')),
    started_at      timestamptz not null default now(),
    finished_at     timestamptz,
    stats           jsonb,
    error           text
);

create index idx_jobs_account on jobs (mail_account_id, started_at desc);

create table system_config (
    key   text primary key,
    value jsonb not null
);

create table audit_log (
    id          bigint generated always as identity primary key,
    user_id     bigint references users (id) on delete set null,
    -- 'mcp:<token_id>' or 'session:<user_id>'.
    actor       text not null,
    action      text not null,
    message_ref text,
    detail      jsonb,
    at          timestamptz not null default now()
);

create index idx_audit_user_at on audit_log (user_id, at desc);
