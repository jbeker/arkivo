use anyhow::Result;
use clap::Args as ClapArgs;

use super::context::AccountContext;
use crate::config::AppConfig;
use crate::db::{jobs, locks::AdvisoryLock};
use crate::jmap::backfill::{BackfillOptions, backfill_account, fetch_missing_blobs};

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Mail account to backfill.
    #[arg(long)]
    pub account: i64,
    /// Stop after this many messages (staged rollout / smoke testing).
    #[arg(long)]
    pub limit: Option<u64>,
    /// Only fetch messages received at or after this RFC3339 timestamp.
    #[arg(long)]
    pub since: Option<chrono::DateTime<chrono::Utc>>,
}

pub async fn run(config: AppConfig, args: Args) -> Result<()> {
    let Some(lock) = AdvisoryLock::try_acquire(&config.database_url, "sync", args.account).await?
    else {
        tracing::info!(account = args.account, "another sync is running; skipping");
        return Ok(());
    };

    let ctx = AccountContext::open(&config, args.account).await?;
    let job_id = jobs::start(&ctx.pool, "backfill", Some(ctx.account.id)).await?;

    let options = BackfillOptions {
        limit: args.limit,
        since: args.since,
        ..Default::default()
    };
    let result = async {
        let stats =
            backfill_account(&ctx.pool, &ctx.client, &ctx.maildir, &ctx.account, &options).await?;
        // Sweep up any blobs that failed mid-run before declaring success.
        let retried =
            fetch_missing_blobs(&ctx.pool, &ctx.client, &ctx.maildir, ctx.account.id).await?;
        Ok::<_, anyhow::Error>((stats, retried))
    }
    .await;

    match &result {
        Ok((stats, retried)) => {
            let mut value = serde_json::to_value(stats)?;
            value["retried_blobs"] = serde_json::json!(retried);
            jobs::succeed(&ctx.pool, job_id, &value).await?;
            tracing::info!(
                account = ctx.account.id,
                fetched = stats.fetched,
                pages = stats.pages,
                complete = stats.complete,
                retried_blobs = retried,
                "backfill run complete"
            );
        }
        Err(e) => {
            jobs::fail(&ctx.pool, job_id, &format!("{e:#}")).await?;
        }
    }
    lock.release().await?;
    result.map(|_| ())
}
