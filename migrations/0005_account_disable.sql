-- Pause switch: a disabled account is skipped by scheduled polling
-- (cron poll --all). Manual jobs and scheduled promotion still run.
alter table mail_accounts
    add column disabled_at timestamptz;
