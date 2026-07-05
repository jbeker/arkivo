use anyhow::Result;
use clap::Parser;

use arkivo::cmd::{Cli, run};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    arkivo::telemetry::init(cli.log_json);
    run(cli).await
}
