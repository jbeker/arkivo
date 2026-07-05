use anyhow::Result;
use clap::Args as ClapArgs;

use super::context::AccountContext;
use crate::config::AppConfig;
use crate::db::{jobs, locks::AdvisoryLock};
use crate::jmap::sync::poll_account;

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Mail account to poll.
    #[arg(long, conflicts_with = "all", required_unless_present = "all")]
    pub account: Option<i64>,
    /// Poll every mail account (cron entry point).
    #[arg(long)]
    pub all: bool,
}

pub async fn run(config: AppConfig, args: Args) -> Result<()> {
    let account_ids = super::resolve_account_ids(&config, args.account, args.all).await?;
    let mut failures = 0u32;
    for account_id in account_ids {
        if let Err(e) = poll_one(&config, account_id).await {
            tracing::error!(account = account_id, error = %format!("{e:#}"), "poll failed");
            failures += 1;
        }
    }
    anyhow::ensure!(failures == 0, "{failures} account poll(s) failed");
    Ok(())
}

async fn poll_one(config: &AppConfig, account_id: i64) -> Result<()> {
    let Some(lock) = AdvisoryLock::try_acquire(&config.database_url, "poll", account_id).await?
    else {
        tracing::info!(account = account_id, "another poll is running; skipping");
        return Ok(());
    };

    let ctx = AccountContext::open(config, account_id).await?;
    let search = crate::search::SearchClient::new(&config.opensearch)?;
    let job_id = jobs::start(&ctx.pool, "poll", Some(ctx.account.id)).await?;

    let result = poll_account(
        &ctx.pool,
        &ctx.client,
        &ctx.maildir,
        Some(&search),
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
