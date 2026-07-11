//! Counters and gauges for the existing Zabbix monitoring (spec §15),
//! served as JSON for HTTP-agent items.

use anyhow::Result;
use serde_json::{Value, json};
use sqlx::PgPool;

pub async fn gather(pool: &PgPool) -> Result<Value> {
    let accounts = sqlx::query!(
        r#"select a.id, a.user_id, a.disabled_at,
                  count(m.id) as "total!",
                  count(m.id) filter (where m.index_status = 'staged') as "staged!",
                  count(m.id) filter (where m.index_status = 'indexed') as "indexed!",
                  count(m.id) filter (where m.index_status = 'quarantined') as "quarantined!",
                  count(m.id) filter (where m.index_status = 'failed') as "failed!",
                  count(m.id) filter (where m.maildir_path is null) as "unfetched!"
           from mail_accounts a
           left join messages m on m.mail_account_id = a.id and m.deleted_at is null
           group by a.id, a.user_id, a.disabled_at
           order by a.id"#
    )
    .fetch_all(pool)
    .await?;

    let jobs = sqlx::query!(
        r#"select distinct on (mail_account_id, kind)
                  mail_account_id, kind, status,
                  extract(epoch from now() - started_at)::bigint as "age_secs!",
                  extract(epoch from coalesce(finished_at, now()) - started_at)::bigint
                      as "duration_secs!"
           from jobs
           where mail_account_id is not null
           order by mail_account_id, kind, started_at desc"#
    )
    .fetch_all(pool)
    .await?;

    let account_metrics: Vec<Value> = accounts
        .iter()
        .map(|a| {
            let account_jobs: Vec<Value> = jobs
                .iter()
                .filter(|j| j.mail_account_id == Some(a.id))
                .map(|j| {
                    json!({
                        "kind": j.kind,
                        "status": j.status,
                        "age_secs": j.age_secs,
                        "duration_secs": j.duration_secs,
                    })
                })
                .collect();
            json!({
                "account_id": a.id,
                "user_id": a.user_id,
                // Paused accounts stop polling; exclude them from
                // poll-age alerting on the Zabbix side.
                "disabled": a.disabled_at.is_some(),
                "messages": {
                    "total": a.total, "staged": a.staged, "indexed": a.indexed,
                    "quarantined": a.quarantined, "failed": a.failed,
                    "unfetched": a.unfetched,
                },
                "last_jobs": account_jobs,
            })
        })
        .collect();

    Ok(json!({
        "accounts": account_metrics,
        "totals": {
            "accounts": accounts.len(),
            "messages": accounts.iter().map(|a| a.total).sum::<i64>(),
            "indexed": accounts.iter().map(|a| a.indexed).sum::<i64>(),
            "staged": accounts.iter().map(|a| a.staged).sum::<i64>(),
            "failed": accounts.iter().map(|a| a.failed).sum::<i64>(),
        }
    }))
}
