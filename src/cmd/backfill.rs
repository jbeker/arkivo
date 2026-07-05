use anyhow::Result;
use clap::Args as ClapArgs;

use crate::config::AppConfig;

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

pub async fn run(_config: AppConfig, _args: Args) -> Result<()> {
    anyhow::bail!("not yet implemented")
}
