use anyhow::Result;
use clap::Args as ClapArgs;

use crate::config::AppConfig;
use crate::crypto::generate_token;
use crate::db::auth;

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Invite validity in days.
    #[arg(long, default_value = "7")]
    pub ttl_days: i64,
}

/// Passkey-only auth has a first-admin bootstrap problem: no one can
/// sign in to create the first invite. This mints an admin invite from
/// the CLI; register with it at /login.
pub async fn run(config: AppConfig, args: Args) -> Result<()> {
    let pool = crate::db::connect(&config.database_url).await?;
    crate::db::migrate(&pool).await?;

    let generated = generate_token("inv");
    auth::create_invite(&pool, &generated.hash, None, "admin", args.ttl_days).await?;
    println!(
        "Admin invite code (valid {} days, shown once):",
        args.ttl_days
    );
    println!("{}", generated.token);
    Ok(())
}
