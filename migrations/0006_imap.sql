-- IMAP ingestion: third provider, connection parameters, and per-folder
-- UID state. Naming debt continues (see 0004): for provider='imap' rows,
-- mail_accounts.account_id holds the login username and sealed_token the
-- sealed password. Message identity is content-based ("imap-" + sha256 of
-- the raw RFC822 bytes) so moves and copies never re-key a messages row;
-- which (folder, uid) currently holds a message lives in imap_uid_map.

alter table mail_accounts drop constraint mail_accounts_provider_check;
alter table mail_accounts add constraint mail_accounts_provider_check
    check (provider in ('jmap', 'gmail', 'imap'));

alter table mail_accounts
    add column imap_host text,
    add column imap_port int,
    add column imap_tls  text check (imap_tls in ('implicit', 'starttls', 'none'));

create table imap_state (
    mail_account_id bigint primary key
                    references mail_accounts (id) on delete cascade,
    backfill_done   boolean not null default false,
    -- Resumable backfill cursor:
    -- {"folders": [...], "folder_index": 0, "uidvalidity": N, "last_uid": N}
    backfill_cursor jsonb,
    updated_at      timestamptz not null default now()
);

-- Per-folder sync cursor. last_seen_uidnext is advisory (poll diffs the
-- full server UID list against imap_uid_map); a uidvalidity mismatch at
-- EXAMINE time forces a per-folder resync.
create table imap_folders (
    mail_account_id   bigint not null references mail_accounts (id) on delete cascade,
    folder            text not null,
    uidvalidity       bigint not null,
    last_seen_uidnext bigint not null default 1,
    updated_at        timestamptz not null default now(),
    primary key (mail_account_id, folder)
);

-- Placement map: which (folder, uid) currently holds which ledger row. A
-- message copied into two folders has two rows here and one messages row;
-- the row is destroyed (per deletion policy) only when its last placement
-- vanishes. Cascade from messages auto-cleans placements on mirror delete.
create table imap_uid_map (
    mail_account_id bigint not null references mail_accounts (id) on delete cascade,
    folder          text not null,
    uidvalidity     bigint not null,
    uid             bigint not null,
    message_id      bigint not null references messages (id) on delete cascade,
    -- IMAP flags last seen on this placement (["\\Seen", ...]); keywords on
    -- the messages row are recomputed as the union across placements.
    flags           jsonb not null default '[]',
    primary key (mail_account_id, folder, uid)
);
create index idx_imap_uid_map_msg on imap_uid_map (message_id);
