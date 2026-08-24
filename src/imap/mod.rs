pub mod backfill;
pub mod client;
pub mod sync;
pub mod types;

pub use client::{ImapClient, ImapError};
pub use types::{FolderInfo, FolderStatus, RawMessage, SpecialUse, TlsMode, UidEntry};
