use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::config::AppConfig;

mod backfill;
mod bootstrap;
pub mod context;
mod migrate;
mod poll;
mod promote;
mod reindex;
mod serve_mcp;
mod serve_web;

/// Arkivo: email archive with recency-gated, agent-safe search.
#[derive(Debug, Parser)]
#[command(name = "arkivo", version, about)]
pub struct Cli {
    /// Path to the TOML configuration file (default: ./arkivo.toml,
    /// overridable field-by-field via ARKIVO_* environment variables).
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Emit structured JSON logs instead of human-readable output.
    #[arg(long, global = true)]
    pub log_json: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Seed the archive: page through the full mailbox and download every message.
    Backfill(backfill::Args),
    /// Incremental sync: apply Email/changes since the stored JMAP state.
    Poll(poll::Args),
    /// Promote aged messages through sanitization into the search index.
    Promote(promote::Args),
    /// Rebuild search indices from the canonical Maildir store.
    Reindex(reindex::Args),
    /// Run the web administration service.
    ServeWeb(serve_web::Args),
    /// Run the read-only MCP search server.
    ServeMcp(serve_mcp::Args),
    /// Apply pending database migrations.
    Migrate(migrate::Args),
    /// Mint the first admin invite on a fresh install.
    Bootstrap(bootstrap::Args),
}

/// Resolve `--account N` / `--all` into a concrete id list for the
/// batch subcommands. With `active_only`, `--all` skips paused accounts
/// (poll passes true; promote passes false so already-fetched mail still
/// ages into the index). An explicit `--account N` always resolves —
/// manual invocation is intentional.
pub async fn resolve_account_ids(
    pool: &sqlx::PgPool,
    account: Option<i64>,
    all: bool,
    active_only: bool,
) -> Result<Vec<i64>> {
    if let Some(id) = account {
        return Ok(vec![id]);
    }
    anyhow::ensure!(all, "pass --account <id> or --all");
    let ids = sqlx::query_scalar!(
        r#"select a.id from mail_accounts a
           join users u on u.id = a.user_id
           where u.disabled_at is null
             and (not $1 or a.disabled_at is null)
           order by a.id"#,
        active_only,
    )
    .fetch_all(pool)
    .await?;
    Ok(ids)
}

pub async fn run(cli: Cli) -> Result<()> {
    let config = AppConfig::load(cli.config.as_deref())?;
    match cli.command {
        Command::Backfill(args) => backfill::run(config, args).await,
        Command::Poll(args) => poll::run(config, args).await,
        Command::Promote(args) => promote::run(config, args).await,
        Command::Reindex(args) => reindex::run(config, args).await,
        Command::ServeWeb(args) => serve_web::run(config, args).await,
        Command::ServeMcp(args) => serve_mcp::run(config, args).await,
        Command::Migrate(args) => migrate::run(config, args).await,
        Command::Bootstrap(args) => bootstrap::run(config, args).await,
    }
}
