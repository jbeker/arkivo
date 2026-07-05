use anyhow::Result;
use clap::Args as ClapArgs;

use super::context::AccountContext;
use crate::config::AppConfig;
use crate::db::{jobs, locks::AdvisoryLock};
use crate::jmap::sync::poll_account;

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Mail account to poll.
    #[arg(long)]
    pub account: i64,
}

pub async fn run(config: AppConfig, args: Args) -> Result<()> {
    let Some(lock) = AdvisoryLock::try_acquire(&config.database_url, "poll", args.account).await?
    else {
        tracing::info!(account = args.account, "another poll is running; skipping");
        return Ok(());
    };

    let ctx = AccountContext::open(&config, args.account).await?;
    let job_id = jobs::start(&ctx.pool, "poll", Some(ctx.account.id)).await?;

    let result = poll_account(
        &ctx.pool,
        &ctx.client,
        &ctx.maildir,
        &ctx.account,
        ctx.deletion_policy(),
    )
    .await;

    match &result {
        Ok(stats) => {
            jobs::succeed(&ctx.pool, job_id, &serde_json::to_value(stats)?).await?;
            tracing::info!(
                account = ctx.account.id,
                fetched = stats.fetched,
                updated = stats.updated,
                destroyed = stats.destroyed,
                resynced = stats.resynced,
                "poll complete"
            );
        }
        Err(e) => {
            jobs::fail(&ctx.pool, job_id, &format!("{e:#}")).await?;
        }
    }
    lock.release().await?;
    result.map(|_| ())
}
