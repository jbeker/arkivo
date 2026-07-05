pub mod clock;
pub mod cmd;
pub mod config;
pub mod crypto;
pub mod db;
pub mod embed;
pub mod extract;
pub mod jmap;
pub mod maildir;
pub mod mcp;
pub mod metrics;
pub mod promote;
pub mod sanitize;
pub mod search;
pub mod telemetry;
pub mod web;

pub use config::AppConfig;
