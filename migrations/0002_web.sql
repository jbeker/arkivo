-- Web milestone: WebAuthn user handles and server-side sessions.

-- webauthn-rs identifies users by UUID; generated here, never reused.
alter table users
    add column webauthn_uuid uuid not null unique default gen_random_uuid();

-- tower-sessions backing store (custom, on the app's own sqlx pool).
create table sessions (
    id          text primary key,
    data        jsonb not null,
    expiry_unix bigint not null
);

create index idx_sessions_expiry on sessions (expiry_unix);
