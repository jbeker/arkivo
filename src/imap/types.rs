use std::str::FromStr;

use chrono::{DateTime, Utc};

/// Connection security for an IMAP account, stored as
/// `mail_accounts.imap_tls`. `None` is plain TCP — the test fixture and
/// homelab-dev posture, same stance as the plain-http JMAP allowance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode {
    Implicit,
    StartTls,
    None,
}

impl TlsMode {
    pub fn as_str(self) -> &'static str {
        match self {
            TlsMode::Implicit => "implicit",
            TlsMode::StartTls => "starttls",
            TlsMode::None => "none",
        }
    }

    pub fn default_port(self) -> u16 {
        match self {
            TlsMode::Implicit => 993,
            TlsMode::StartTls | TlsMode::None => 143,
        }
    }
}

impl FromStr for TlsMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "implicit" => Ok(TlsMode::Implicit),
            "starttls" => Ok(TlsMode::StartTls),
            "none" => Ok(TlsMode::None),
            other => anyhow::bail!("unknown imap tls mode '{other}'"),
        }
    }
}

/// RFC 6154 SPECIAL-USE roles we recognize from LIST attributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecialUse {
    All,
    Archive,
    Drafts,
    Flagged,
    Junk,
    Sent,
    Trash,
}

#[derive(Debug, Clone)]
pub struct FolderInfo {
    pub name: String,
    pub selectable: bool,
    pub special_use: Option<SpecialUse>,
}

/// EXAMINE result, the per-folder sync anchor.
#[derive(Debug, Clone, Copy)]
pub struct FolderStatus {
    pub uidvalidity: u32,
    pub uidnext: u32,
    pub exists: u32,
}

/// One entry from a full-folder `UID FETCH 1:* (UID FLAGS)` sweep.
#[derive(Debug, Clone)]
pub struct UidEntry {
    pub uid: u32,
    pub flags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RawMessage {
    pub raw: Vec<u8>,
    pub internal_date: Option<DateTime<Utc>>,
    pub flags: Vec<String>,
}
