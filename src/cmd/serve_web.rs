use std::sync::Arc;

use anyhow::Result;
use clap::Args as ClapArgs;

use crate::config::AppConfig;
use crate::crypto::Sealer;
use crate::search::SearchClient;
use crate::web::{WebState, app, build_webauthn};

#[derive(Debug, ClapArgs)]
pub struct Args {}

pub async fn run(config: AppConfig, _args: Args) -> Result<()> {
    let pool = crate::db::connect(&config.database_url).await?;
    crate::db::migrate(&pool).await?;

    // A prior process may have died mid-job, leaving `running` rows that
    // no one owns. Retire them now so the UI doesn't show a phantom import
    // that can't be cancelled.
    match crate::ops::reconcile_orphaned_jobs(&config, &pool).await {
        Ok(0) => {}
        Ok(n) => tracing::warn!(count = n, "retired orphaned running jobs at startup"),
        Err(e) => tracing::error!(error = %format!("{e:#}"), "orphaned-job reconciliation failed"),
    }

    let state = WebState {
        pool: pool.clone(),
        webauthn: Arc::new(build_webauthn(&config)?),
        sealer: Arc::new(Sealer::from_key_file(
            &config.master_key_path,
            super::context::SEAL_KEY_ID,
        )?),
        search: Arc::new(SearchClient::new(&config.opensearch)?),
        embedding_url: config.embedding.url.clone(),
        defaults: config.defaults.clone(),
        config: config.clone(),
    };

    let listener = tokio::net::TcpListener::bind(&config.web.bind).await?;
    tracing::info!(bind = %config.web.bind, rp_id = %config.web.rp_id, "web service listening");
    axum::serve(listener, app(state)).await?;
    Ok(())
}
