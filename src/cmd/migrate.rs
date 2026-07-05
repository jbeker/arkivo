use anyhow::Result;
use clap::Args as ClapArgs;

use crate::config::AppConfig;

#[derive(Debug, ClapArgs)]
pub struct Args {}

pub async fn run(config: AppConfig, _args: Args) -> Result<()> {
    let pool = crate::db::connect(&config.database_url).await?;
    crate::db::migrate(&pool).await?;
    tracing::info!("migrations applied");
    Ok(())
}
