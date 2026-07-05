use std::sync::Arc;

use anyhow::Result;
use clap::Args as ClapArgs;

use crate::config::AppConfig;
use crate::embed::OllamaEmbedder;
use crate::search::SearchClient;

#[derive(Debug, ClapArgs)]
pub struct Args {}

pub async fn run(config: AppConfig, _args: Args) -> Result<()> {
    let pool = crate::db::connect(&config.database_url).await?;
    let search = Arc::new(SearchClient::new(&config.opensearch)?);
    let embedder = Arc::new(OllamaEmbedder::new(&config.embedding)?);

    let app = crate::mcp::app(pool, search, embedder, config.mcp.allowed_hosts.clone());
    let listener = tokio::net::TcpListener::bind(&config.mcp.bind).await?;
    tracing::info!(bind = %config.mcp.bind, "MCP server listening");
    axum::serve(listener, app).await?;
    Ok(())
}
