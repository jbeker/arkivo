use anyhow::Result;
use clap::Args as ClapArgs;

use crate::config::AppConfig;

#[derive(Debug, ClapArgs)]
pub struct Args {}

pub async fn run(_config: AppConfig, _args: Args) -> Result<()> {
    anyhow::bail!("not yet implemented")
}
