use anyhow::{Context, Result};
use clap::Args as ClapArgs;

use crate::clock::SystemClock;
use crate::config::AppConfig;
use crate::db::{accounts, jobs, locks::AdvisoryLock};
use crate::embed::OllamaEmbedder;
use crate::maildir::Maildir;
use crate::promote::promote_account;
use crate::search::SearchClient;

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Mail account to promote.
    #[arg(long, conflicts_with = "all", required_unless_present = "all")]
    pub account: Option<i64>,
    /// Promote every mail account (cron entry point).
    #[arg(long)]
    pub all: bool,
}

/// Promotion touches the DB, Maildir, OpenSearch, and the embedder —
/// but never Fastmail; no JMAP credential is unsealed here.
pub async fn run(config: AppConfig, args: Args) -> Result<()> {
    let account_ids = super::resolve_account_ids(&config, args.account, args.all).await?;
    let mut failures = 0u32;
    for account_id in account_ids {
        if let Err(e) = promote_one(&config, account_id).await {
            tracing::error!(account = account_id, error = %format!("{e:#}"), "promote failed");
            failures += 1;
        }
    }
    anyhow::ensure!(failures == 0, "{failures} account promotion(s) failed");
    Ok(())
}

async fn promote_one(config: &AppConfig, account_id: i64) -> Result<()> {
    let Some(lock) = AdvisoryLock::try_acquire(&config.database_url, "promote", account_id).await?
    else {
        tracing::info!(account = account_id, "another promote is running; skipping");
        return Ok(());
    };

    let pool = crate::db::connect(&config.database_url).await?;
    let account = accounts::get(&pool, account_id)
        .await?
        .with_context(|| format!("mail account {account_id} not found"))?;
    let maildir = Maildir::open_or_create(
        config
            .maildir_root
            .join(format!("user-{}", account.user_id))
            .join(format!("account-{}", account.id)),
    )?;
    let search = SearchClient::new(&config.opensearch)?;
    let embedder = OllamaEmbedder::new(&config.embedding)?;

    let job_id = jobs::start(&pool, "promote", Some(account.id)).await?;
    let result = promote_account(&pool, &maildir, &search, &embedder, &SystemClock, &account).await;

    match &result {
        Ok(stats) => {
            jobs::succeed(&pool, job_id, &serde_json::to_value(stats)?).await?;
            tracing::info!(
                account = account.id,
                promoted = stats.promoted,
                quarantined = stats.quarantined,
                deduped = stats.deduped,
                failed = stats.failed,
                chunks = stats.chunks,
                "promotion complete"
            );
        }
        Err(e) => {
            jobs::fail(&pool, job_id, &format!("{e:#}")).await?;
        }
    }
    lock.release().await?;
    result.map(|_| ())
}
