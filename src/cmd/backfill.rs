use anyhow::Result;
use clap::Args as ClapArgs;

use crate::config::AppConfig;
use crate::ops::{JobKind, run_blocking};

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
    let pool = crate::db::connect(&config.database_url).await?;
    run_blocking(
        &config,
        &pool,
        args.account,
        JobKind::Backfill {
            limit: args.limit,
            since: args.since,
        },
    )
    .await
}
