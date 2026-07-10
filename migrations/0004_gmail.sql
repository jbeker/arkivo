-- Gmail ingestion: provider discriminator on mail_accounts, per-provider
-- sync state, and OAuth handshake state.

alter table mail_accounts
    add column provider text not null default 'jmap'
        check (provider in ('jmap', 'gmail'));

-- Gmail accounts have no JMAP session URL. Naming debt, accepted: for
-- provider='gmail' rows, mail_accounts.account_id holds the granted
-- Gmail address, and messages.jmap_email_id / messages.blob_id hold the
-- Gmail message id (it is both the identity and the format=raw fetch key).
alter table mail_accounts
    alter column jmap_session_url drop not null;

create table gmail_state (
    mail_account_id bigint primary key
                    references mail_accounts (id) on delete cascade,
    -- Last durably-applied history id; null until backfill records one.
    history_id      text,
    backfill_done   boolean not null default false,
    -- Resumable backfill cursor:
    -- {"page_token": "...", "oldest_internal_date": <epoch-millis>}
    backfill_cursor jsonb,
    updated_at      timestamptz not null default now()
);

-- OAuth handshake state. The session cookie is SameSite=Strict, so it is
-- not sent on the cross-site redirect back from accounts.google.com; the
-- callback authenticates via this single-use, expiring row instead.
create table oauth_states (
    id            bigint generated always as identity primary key,
    user_id       bigint not null references users (id) on delete cascade,
    state_hash    bytea not null unique,
    pkce_verifier text not null,
    expires_at    timestamptz not null,
    consumed_at   timestamptz
);
