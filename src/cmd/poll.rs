use anyhow::Result;
use clap::Args as ClapArgs;

use crate::config::AppConfig;
use crate::ops::{JobKind, run_blocking};

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
    let pool = crate::db::connect(&config.database_url).await?;
    // active_only: paused accounts are excluded from scheduled polling.
    let account_ids = super::resolve_account_ids(&pool, args.account, args.all, true).await?;
    let mut failures = 0u32;
    for account_id in account_ids {
        if let Err(e) = run_blocking(&config, &pool, account_id, JobKind::Poll).await {
            tracing::error!(account = account_id, error = %format!("{e:#}"), "poll failed");
            failures += 1;
        }
    }
    anyhow::ensure!(failures == 0, "{failures} account poll(s) failed");
    Ok(())
}
