-- Office 365 ingestion over Microsoft Graph: fourth provider, per-folder
-- delta state. Naming debt continues (see 0004): for provider='o365'
-- rows, mail_accounts.account_id holds the user principal name (or the
-- mailbox address) and sealed_token the sealed OAuth refresh token —
-- which Microsoft rotates on every redemption, so jobs rewrite that
-- column. messages.jmap_email_id / blob_id hold the Graph *immutable*
-- message id (Prefer: IdType="ImmutableId"), which survives folder
-- moves. messages.mailbox_ids is a one-element array holding the
-- folder's display path ("Inbox", "Archive/2024"): an Exchange message
-- lives in exactly one folder, so no placement table is needed.

alter table mail_accounts drop constraint mail_accounts_provider_check;
alter table mail_accounts add constraint mail_accounts_provider_check
    check (provider in ('jmap', 'gmail', 'imap', 'o365'));

create table o365_state (
    mail_account_id bigint primary key
                    references mail_accounts (id) on delete cascade,
    backfill_done   boolean not null default false,
    -- receivedDateTime floor the folders were walked with; null =
    -- unbounded. A delta link built with a filter keeps that filter, so
    -- a backfill asking for an earlier (or no) floor re-walks every
    -- folder.
    backfill_since  timestamptz,
    updated_at      timestamptz not null default now()
);

-- One row per archivable folder seen. Cursor lifecycle:
--   next_link   set while the folder's initial delta walk is in
--               progress (@odata.nextLink, persisted per durable page);
--   delta_link  set once the walk finished (@odata.deltaLink); poll
--               continues from it and advances it only after the
--               folder's adds AND removes are durably applied.
-- Neither set = never walked. A folder absent from the live tree (or
-- fallen into a skipped subtree) is swept and its row deleted.
create table o365_folders (
    mail_account_id  bigint not null references mail_accounts (id) on delete cascade,
    folder_id        text not null,
    display_path     text not null,
    -- inbox, sentitems, drafts, archive, ... when resolvable; null for
    -- user folders.
    well_known_name  text,
    delta_link       text,
    next_link        text,
    -- Set when an expired delta token (HTTP 410) forced a re-walk: rows
    -- still placed here but not seen in the walk are then verified and
    -- destroyed per policy. Cleared when the walk's delta link lands.
    resync_pending   boolean not null default false,
    -- Server-reported totalItemCount at last listing; sums into the
    -- backfill progress estimate.
    total_item_count bigint,
    updated_at       timestamptz not null default now(),
    primary key (mail_account_id, folder_id)
);
