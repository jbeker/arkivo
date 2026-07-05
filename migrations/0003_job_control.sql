-- Web-triggered jobs: cooperative cancellation and a fast running-jobs
-- lookup for the dashboard.

alter table jobs add column cancel_requested_at timestamptz;

alter table jobs drop constraint jobs_status_check;
alter table jobs add constraint jobs_status_check
    check (status in ('running', 'succeeded', 'failed', 'cancelled'));

create index idx_jobs_running on jobs (status) where status = 'running';
