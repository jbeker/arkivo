//! Shared setup for the batch subcommands: account, unsealed
//! credential, source client (JMAP or Gmail, per the account's
//! provider), and the per-account Maildir. The caller's pool is
//! borrowed, never duplicated — web-spawned jobs would exhaust
//! Postgres connections otherwise.

use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::config::{AppConfig, DeletionPolicy};
use crate::crypto::Sealer;
use crate::db::accounts::MailAccount;
use crate::gmail::GmailClient;
use crate::jmap::{JmapClient, RetryPolicy};
use crate::maildir::Maildir;

pub const SEAL_KEY_ID: &str = "primary";

/// The account's mail source. Enum, not a trait: the two sync models are
/// structurally different (opaque state strings + separate blob download
/// vs. monotonic history ids + format=raw), so job execution matches on
/// the variant and calls the matching engine.
pub enum SourceClient {
    Jmap(JmapClient),
    Gmail(GmailClient),
}

pub struct AccountContext {
    pub account: MailAccount,
    pub client: SourceClient,
    pub maildir: Maildir,
}

impl AccountContext {
    pub async fn open(config: &AppConfig, pool: &PgPool, account_id: i64) -> Result<Self> {
        let account = crate::db::accounts::get(pool, account_id)
            .await?
            .with_context(|| format!("mail account {account_id} not found"))?;

        let sealer = Sealer::from_key_file(&config.master_key_path, SEAL_KEY_ID)?;
        if account.seal_key_id != sealer.key_id() {
            anyhow::bail!(
                "account {} sealed with key '{}' but configured key is '{}'",
                account.id,
                account.seal_key_id,
                sealer.key_id()
            );
        }
        // JMAP bearer token or Gmail refresh token, per provider.
        let token = String::from_utf8(sealer.unseal(&account.sealed_token)?)
            .context("unsealed token is not valid UTF-8")?;

        let client = match account.provider.as_str() {
            "gmail" => {
                let google = config.google.as_ref().with_context(|| {
                    format!(
                        "account {} is a gmail account but [google] is not configured",
                        account.id
                    )
                })?;
                let client = GmailClient::new(google, token, RetryPolicy::default())?;
                // Fail fast on a bad grant, like the JMAP session fetch does.
                client.access_token().await?;
                SourceClient::Gmail(client)
            }
            _ => {
                let session_url = account
                    .jmap_session_url
                    .as_deref()
                    .with_context(|| format!("jmap account {} has no session url", account.id))?;
                SourceClient::Jmap(
                    JmapClient::connect(session_url, &token, RetryPolicy::default()).await?,
                )
            }
        };

        let maildir = Maildir::open_or_create(
            config
                .maildir_root
                .join(format!("user-{}", account.user_id))
                .join(format!("account-{}", account.id)),
        )?;

        Ok(Self {
            account,
            client,
            maildir,
        })
    }

    pub fn deletion_policy(&self) -> DeletionPolicy {
        match self.account.deletion_policy.as_str() {
            "mirror" => DeletionPolicy::Mirror,
            _ => DeletionPolicy::Retain,
        }
    }
}
