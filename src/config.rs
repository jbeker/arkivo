use std::path::{Path, PathBuf};

use figment::Figment;
use figment::providers::{Env, Format, Toml};
use serde::{Deserialize, Serialize};

/// Full application configuration, resolved at process start from a TOML
/// file overlaid with `ARKIVO_`-prefixed environment variables
/// (nested keys separated by `__`, e.g. `ARKIVO_OPENSEARCH__URL`).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub database_url: String,
    /// Root directory holding one Maildir tree per user.
    pub maildir_root: PathBuf,
    /// Path to a file containing the 32-byte master key (hex) for
    /// credential sealing. Referenced, never inlined, per spec §13.
    pub master_key_path: PathBuf,
    pub opensearch: OpenSearchConfig,
    pub embedding: EmbeddingConfig,
    #[serde(default)]
    pub web: WebConfig,
    #[serde(default)]
    pub mcp: McpConfig,
    #[serde(default)]
    pub defaults: UserDefaults,
    /// Google OAuth client for Gmail ingestion. Absent disables the
    /// "Connect Gmail" flow; existing gmail accounts fail jobs with a
    /// clear error until it is restored.
    #[serde(default)]
    pub google: Option<GoogleConfig>,
    /// Microsoft identity platform OAuth client for Office 365 ingestion
    /// over Graph. Absent disables the "Connect Microsoft 365" flow;
    /// existing o365 accounts fail jobs with a clear error until it is
    /// restored.
    #[serde(default)]
    pub microsoft: Option<MicrosoftConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GoogleConfig {
    pub client_id: String,
    /// Prefer supplying via ARKIVO_GOOGLE__CLIENT_SECRET.
    pub client_secret: String,
    /// Endpoint overrides for tests; unset means real Google.
    #[serde(default)]
    pub auth_url: Option<String>,
    #[serde(default)]
    pub token_url: Option<String>,
    #[serde(default)]
    pub api_base: Option<String>,
}

impl GoogleConfig {
    /// The registered OAuth redirect URI, derived from the web origin.
    pub fn redirect_uri(rp_origin: &str) -> String {
        format!("{}/oauth/google/callback", rp_origin.trim_end_matches('/'))
    }

    pub fn auth_url(&self) -> &str {
        self.auth_url
            .as_deref()
            .unwrap_or("https://accounts.google.com/o/oauth2/v2/auth")
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MicrosoftConfig {
    pub client_id: String,
    /// Prefer supplying via ARKIVO_MICROSOFT__CLIENT_SECRET.
    pub client_secret: String,
    /// Entra tenant the consent flow authorizes against: "common" (any
    /// work/school or personal account), "organizations", "consumers",
    /// or a tenant id / verified domain to lock it to one directory.
    #[serde(default = "default_tenant")]
    pub tenant: String,
    /// Endpoint overrides for tests; unset means real Microsoft.
    #[serde(default)]
    pub auth_url: Option<String>,
    #[serde(default)]
    pub token_url: Option<String>,
    #[serde(default)]
    pub api_base: Option<String>,
}

impl MicrosoftConfig {
    /// The registered OAuth redirect URI, derived from the web origin.
    pub fn redirect_uri(rp_origin: &str) -> String {
        format!(
            "{}/oauth/microsoft/callback",
            rp_origin.trim_end_matches('/')
        )
    }

    pub fn auth_url(&self) -> String {
        self.auth_url.clone().unwrap_or_else(|| {
            format!(
                "https://login.microsoftonline.com/{}/oauth2/v2.0/authorize",
                self.tenant
            )
        })
    }

    pub fn token_url(&self) -> String {
        self.token_url.clone().unwrap_or_else(|| {
            format!(
                "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
                self.tenant
            )
        })
    }

    pub fn api_base(&self) -> String {
        self.api_base
            .clone()
            .unwrap_or_else(|| "https://graph.microsoft.com/v1.0".to_string())
    }
}

fn default_tenant() -> String {
    "common".into()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenSearchConfig {
    pub url: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EmbeddingConfig {
    /// Ollama-compatible endpoint base URL.
    pub url: String,
    #[serde(default = "default_embedding_model")]
    pub model: String,
    /// Vector dimension; must match the model and the chunk index mapping.
    #[serde(default = "default_embedding_dimension")]
    pub dimension: usize,
    /// Explicit context window sent with every request: Ollama's default
    /// (2048) silently truncates longer inputs.
    #[serde(default = "default_num_ctx")]
    pub num_ctx: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebConfig {
    pub bind: String,
    /// WebAuthn relying-party ID: must equal the effective domain.
    pub rp_id: String,
    /// Origin the browser reports during ceremonies, e.g. "https://localhost:8443".
    pub rp_origin: String,
    /// Absolute session lifetime: sessions are rejected this long after
    /// login regardless of activity (the idle timeout stays separate).
    #[serde(default = "default_session_max_age_days")]
    pub session_max_age_days: u32,
    /// When set, `/metrics` requires `Authorization: Bearer <token>`.
    /// Unset keeps it open (loopback/proxy-guarded deployments).
    #[serde(default)]
    pub metrics_token: Option<String>,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8080".into(),
            rp_id: "localhost".into(),
            rp_origin: "http://localhost:8080".into(),
            session_max_age_days: default_session_max_age_days(),
            metrics_token: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct McpConfig {
    pub bind: String,
    /// Host/authority allowlist for inbound requests (DNS-rebinding
    /// guard). Empty disables the check — appropriate behind the TLS
    /// terminator the spec places in front of this service.
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8081".into(),
            allowed_hosts: Vec::new(),
        }
    }
}

/// System-wide defaults for per-user settings (spec §10).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UserDefaults {
    #[serde(default = "default_recency_cutoff_days")]
    pub recency_cutoff_days: u32,
    #[serde(default)]
    pub deletion_policy: DeletionPolicy,
}

impl Default for UserDefaults {
    fn default() -> Self {
        Self {
            recency_cutoff_days: default_recency_cutoff_days(),
            deletion_policy: DeletionPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DeletionPolicy {
    /// Destroyed messages stay in the archive; the deletion is audit-logged.
    #[default]
    Retain,
    /// Destroyed messages are removed from the canonical store and index.
    Mirror,
}

fn default_embedding_model() -> String {
    "nomic-embed-text".into()
}

fn default_embedding_dimension() -> usize {
    768
}

fn default_num_ctx() -> usize {
    // nomic-embed-text's full context. Larger than any chunk we produce,
    // so token-dense chunks still fit.
    8192
}

fn default_recency_cutoff_days() -> u32 {
    7
}

fn default_session_max_age_days() -> u32 {
    7
}

impl AppConfig {
    /// Load from `path` (optional TOML file) overlaid with environment.
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let mut figment = Figment::new();
        if let Some(path) = path {
            figment = figment.merge(Toml::file_exact(path));
        } else {
            figment = figment.merge(Toml::file("arkivo.toml"));
        }
        let mut config: Self = figment
            .merge(Env::prefixed("ARKIVO_").split("__"))
            .extract()?;
        // Compose files pass ARKIVO_GOOGLE__* through unconditionally, so
        // an unset deployment arrives as empty strings; treat that as
        // "not configured" rather than a broken OAuth client.
        if config
            .google
            .as_ref()
            .is_some_and(|g| g.client_id.is_empty() || g.client_secret.is_empty())
        {
            config.google = None;
        }
        if config
            .microsoft
            .as_ref()
            .is_some_and(|m| m.client_id.is_empty() || m.client_secret.is_empty())
        {
            config.microsoft = None;
        }
        Ok(config)
    }
}

#[cfg(test)]
#[allow(clippy::result_large_err)] // figment::Jail closures return figment::Error by contract
mod tests {
    use super::*;

    fn base_toml() -> &'static str {
        r#"
            database_url = "postgres://arkivo@localhost/arkivo"
            maildir_root = "/var/lib/arkivo/mail"
            master_key_path = "/etc/arkivo/master.key"

            [opensearch]
            url = "http://localhost:9200"

            [embedding]
            url = "http://localhost:11434"
        "#
    }

    #[test]
    fn file_values_load_with_defaults_applied() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("arkivo.toml", base_toml())?;
            let cfg = AppConfig::load(None).expect("config should load");
            assert_eq!(cfg.embedding.model, "nomic-embed-text");
            assert_eq!(cfg.embedding.dimension, 768);
            assert_eq!(cfg.embedding.num_ctx, 8192);
            assert_eq!(cfg.defaults.recency_cutoff_days, 7);
            assert_eq!(cfg.defaults.deletion_policy, DeletionPolicy::Retain);
            Ok(())
        });
    }

    #[test]
    fn env_overrides_file() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("arkivo.toml", base_toml())?;
            jail.set_env("ARKIVO_OPENSEARCH__URL", "http://search:9200");
            jail.set_env("ARKIVO_DEFAULTS__RECENCY_CUTOFF_DAYS", "14");
            let cfg = AppConfig::load(None).expect("config should load");
            assert_eq!(cfg.opensearch.url, "http://search:9200");
            assert_eq!(cfg.defaults.recency_cutoff_days, 14);
            Ok(())
        });
    }

    #[test]
    fn empty_google_env_means_not_configured() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("arkivo.toml", base_toml())?;
            jail.set_env("ARKIVO_GOOGLE__CLIENT_ID", "");
            jail.set_env("ARKIVO_GOOGLE__CLIENT_SECRET", "");
            let cfg = AppConfig::load(None).expect("config should load");
            assert!(cfg.google.is_none());

            jail.set_env("ARKIVO_GOOGLE__CLIENT_ID", "id.apps.googleusercontent.com");
            jail.set_env("ARKIVO_GOOGLE__CLIENT_SECRET", "secret");
            let cfg = AppConfig::load(None).expect("config should load");
            assert_eq!(
                cfg.google.as_ref().map(|g| g.client_id.as_str()),
                Some("id.apps.googleusercontent.com")
            );
            Ok(())
        });
    }

    #[test]
    fn empty_microsoft_env_means_not_configured() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("arkivo.toml", base_toml())?;
            jail.set_env("ARKIVO_MICROSOFT__CLIENT_ID", "");
            jail.set_env("ARKIVO_MICROSOFT__CLIENT_SECRET", "");
            let cfg = AppConfig::load(None).expect("config should load");
            assert!(cfg.microsoft.is_none());

            jail.set_env("ARKIVO_MICROSOFT__CLIENT_ID", "app-id");
            jail.set_env("ARKIVO_MICROSOFT__CLIENT_SECRET", "secret");
            let cfg = AppConfig::load(None).expect("config should load");
            let ms = cfg.microsoft.as_ref().expect("configured");
            assert_eq!(ms.client_id, "app-id");
            assert_eq!(ms.tenant, "common");
            assert_eq!(
                ms.auth_url(),
                "https://login.microsoftonline.com/common/oauth2/v2.0/authorize"
            );

            jail.set_env("ARKIVO_MICROSOFT__TENANT", "contoso.onmicrosoft.com");
            let cfg = AppConfig::load(None).expect("config should load");
            assert_eq!(
                cfg.microsoft.as_ref().unwrap().token_url(),
                "https://login.microsoftonline.com/contoso.onmicrosoft.com/oauth2/v2.0/token"
            );
            Ok(())
        });
    }

    #[test]
    fn missing_required_field_is_an_error() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("arkivo.toml", "database_url = \"postgres://x\"\n")?;
            assert!(AppConfig::load(None).is_err());
            Ok(())
        });
    }
}
