use anyhow::Result;
use clap::Args as ClapArgs;

use crate::config::AppConfig;
use crate::ops::{JobKind, run_blocking};

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Mail account to reindex.
    #[arg(long)]
    pub account: i64,
    /// Drop and recreate the OpenSearch indices instead of upserting
    /// over the existing ones (needed for mapping changes).
    #[arg(long)]
    pub recreate: bool,
}

/// Rebuild the search indices from the canonical Maildir (spec §3.2:
/// the index is a derived artifact). Restages every stored message and
/// runs a promotion pass; nothing is re-fetched from Fastmail.
pub async fn run(config: AppConfig, args: Args) -> Result<()> {
    let pool = crate::db::connect(&config.database_url).await?;
    run_blocking(
        &config,
        &pool,
        args.account,
        JobKind::Reindex {
            recreate: args.recreate,
        },
    )
    .await
}
