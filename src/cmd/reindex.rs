use anyhow::{Context, Result};
use clap::Args as ClapArgs;

use crate::clock::SystemClock;
use crate::config::AppConfig;
use crate::db::{accounts, jobs, locks::AdvisoryLock, messages};
use crate::embed::OllamaEmbedder;
use crate::maildir::Maildir;
use crate::promote::promote_account;
use crate::search::SearchClient;

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
    let Some(lock) =
        AdvisoryLock::try_acquire(&config.database_url, "promote", args.account).await?
    else {
        tracing::info!(account = args.account, "promotion busy; try again later");
        return Ok(());
    };

    let pool = crate::db::connect(&config.database_url).await?;
    let account = accounts::get(&pool, args.account)
        .await?
        .with_context(|| format!("mail account {} not found", args.account))?;
    let maildir = Maildir::open_or_create(
        config
            .maildir_root
            .join(format!("user-{}", account.user_id))
            .join(format!("account-{}", account.id)),
    )?;
    let search = SearchClient::new(&config.opensearch)?;
    let embedder = OllamaEmbedder::new(&config.embedding)?;

    let job_id = jobs::start(&pool, "reindex", Some(account.id)).await?;
    let result = async {
        if args.recreate {
            search.delete_user_indices(account.user_id).await?;
        }
        let restaged = messages::reset_index_state(&pool, account.id).await?;
        tracing::info!(account = account.id, restaged, "restaged for reindex");
        promote_account(&pool, &maildir, &search, &embedder, &SystemClock, &account).await
    }
    .await;

    match &result {
        Ok(stats) => {
            jobs::succeed(&pool, job_id, &serde_json::to_value(stats)?).await?;
            tracing::info!(
                account = account.id,
                promoted = stats.promoted,
                "reindex complete"
            );
        }
        Err(e) => {
            jobs::fail(&pool, job_id, &format!("{e:#}")).await?;
        }
    }
    lock.release().await?;
    result.map(|_| ())
}
