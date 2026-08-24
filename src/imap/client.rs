//! Thin IMAP client over async-imap: one authenticated session behind a
//! Mutex (IMAP is a stateful single-command protocol — commands must not
//! interleave). Connection setup handles the three TLS modes behind one
//! boxed stream type; everything else is the small command set the sync
//! engine needs.

use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_imap::Session;
use async_imap::types::{Fetch, Flag, NameAttribute};
use chrono::{DateTime, NaiveDate, Utc};
use futures::TryStreamExt;
use mail_parser::MessageParser;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;

use crate::imap::types::{FolderInfo, FolderStatus, RawMessage, SpecialUse, TlsMode, UidEntry};

#[derive(Debug, thiserror::Error)]
pub enum ImapError {
    /// The server rejected the credentials.
    #[error("imap auth failed: {0}")]
    Auth(String),
    #[error("imap connect failed: {0}")]
    Connect(String),
    #[error("imap protocol error: {0}")]
    Imap(#[from] async_imap::error::Error),
    #[error("imap io error: {0}")]
    Io(#[from] std::io::Error),
}

trait StreamInner: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> StreamInner for T {}

/// Type-erased connection so implicit-TLS, STARTTLS, and plain-TCP
/// sessions share one `Session<T>` instantiation.
struct BoxedStream(Box<dyn StreamInner>);

impl fmt::Debug for BoxedStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BoxedStream")
    }
}

impl AsyncRead for BoxedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for BoxedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut *self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.0).poll_shutdown(cx)
    }
}

pub struct ImapClient {
    session: Mutex<Session<BoxedStream>>,
}

impl fmt::Debug for ImapClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ImapClient")
    }
}

fn tls_connector() -> Result<TlsConnector, ImapError> {
    use rustls_platform_verifier::BuilderVerifierExt;
    // Explicit provider: the dependency tree enables both ring (sqlx) and
    // aws-lc-rs (reqwest), so the no-argument builder would panic.
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let config = tokio_rustls::rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| ImapError::Connect(format!("tls config: {e}")))?
        .with_platform_verifier()
        .map_err(|e| ImapError::Connect(format!("tls config: {e}")))?
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}

fn flag_to_string(flag: &Flag<'_>) -> String {
    match flag {
        Flag::Seen => "\\Seen".into(),
        Flag::Answered => "\\Answered".into(),
        Flag::Flagged => "\\Flagged".into(),
        Flag::Deleted => "\\Deleted".into(),
        Flag::Draft => "\\Draft".into(),
        Flag::Recent => "\\Recent".into(),
        Flag::MayCreate => "\\*".into(),
        Flag::Custom(s) => s.to_string(),
    }
}

fn fetch_flags(fetch: &Fetch) -> Vec<String> {
    fetch.flags().map(|f| flag_to_string(&f)).collect()
}

/// Normalize the peeked `HEADER.FIELDS (MESSAGE-ID)` section to the
/// angle-bracketed form the ledger stores (matches `meta_from_raw` in the
/// Gmail/JMAP paths, which both go through mail-parser).
fn parse_message_id_header(header: &[u8]) -> Option<String> {
    MessageParser::default()
        .parse_headers(header)
        .as_ref()
        .and_then(|m| m.message_id())
        .map(|id| format!("<{id}>"))
}

impl ImapClient {
    /// Connect and LOGIN. This is the fail-fast credential check: a bad
    /// host fails as [`ImapError::Connect`], a bad password as
    /// [`ImapError::Auth`].
    pub async fn connect(
        host: &str,
        port: u16,
        tls: TlsMode,
        username: &str,
        password: &str,
    ) -> Result<Self, ImapError> {
        let tcp = TcpStream::connect((host, port))
            .await
            .map_err(|e| ImapError::Connect(format!("{host}:{port}: {e}")))?;

        let stream: BoxedStream = match tls {
            TlsMode::Implicit => {
                let server_name = ServerName::try_from(host.to_string())
                    .map_err(|e| ImapError::Connect(format!("invalid host name: {e}")))?;
                let tls_stream = tls_connector()?
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| ImapError::Connect(format!("tls handshake: {e}")))?;
                BoxedStream(Box::new(tls_stream))
            }
            TlsMode::StartTls => {
                // Consume the greeting, upgrade, and hand the TLS stream to
                // a fresh Client (no second greeting arrives post-upgrade).
                let mut client = async_imap::Client::new(tcp);
                client
                    .read_response()
                    .await?
                    .ok_or_else(|| ImapError::Connect("no server greeting".into()))?;
                client.run_command_and_check_ok("STARTTLS", None).await?;
                let tcp = client.into_inner();
                let server_name = ServerName::try_from(host.to_string())
                    .map_err(|e| ImapError::Connect(format!("invalid host name: {e}")))?;
                let tls_stream = tls_connector()?
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| ImapError::Connect(format!("tls handshake: {e}")))?;
                let client = async_imap::Client::new(BoxedStream(Box::new(tls_stream)));
                return Self::login(client, username, password, false).await;
            }
            TlsMode::None => BoxedStream(Box::new(tcp)),
        };

        let client = async_imap::Client::new(stream);
        Self::login(client, username, password, true).await
    }

    async fn login(
        mut client: async_imap::Client<BoxedStream>,
        username: &str,
        password: &str,
        expect_greeting: bool,
    ) -> Result<Self, ImapError> {
        if expect_greeting {
            client
                .read_response()
                .await?
                .ok_or_else(|| ImapError::Connect("no server greeting".into()))?;
        }
        let session = client
            .login(username, password)
            .await
            .map_err(|(e, _client)| match e {
                async_imap::error::Error::No(msg) => ImapError::Auth(msg),
                other => ImapError::Imap(other),
            })?;
        Ok(Self {
            session: Mutex::new(session),
        })
    }

    /// LIST "" "*" with SPECIAL-USE attributes where the server provides
    /// them (Dovecot and friends include them in plain LIST replies).
    pub async fn list_folders(&self) -> Result<Vec<FolderInfo>, ImapError> {
        let mut session = self.session.lock().await;
        let names: Vec<_> = session.list(None, Some("*")).await?.try_collect().await?;
        Ok(names
            .iter()
            .map(|name| {
                let mut selectable = true;
                let mut special_use = None;
                for attr in name.attributes() {
                    match attr {
                        NameAttribute::NoSelect => selectable = false,
                        NameAttribute::All => special_use = Some(SpecialUse::All),
                        NameAttribute::Archive => special_use = Some(SpecialUse::Archive),
                        NameAttribute::Drafts => special_use = Some(SpecialUse::Drafts),
                        NameAttribute::Flagged => special_use = Some(SpecialUse::Flagged),
                        NameAttribute::Junk => special_use = Some(SpecialUse::Junk),
                        NameAttribute::Sent => special_use = Some(SpecialUse::Sent),
                        NameAttribute::Trash => special_use = Some(SpecialUse::Trash),
                        _ => {}
                    }
                }
                FolderInfo {
                    name: name.name().to_string(),
                    selectable,
                    special_use,
                }
            })
            .collect())
    }

    /// EXAMINE (read-only select). Servers that omit UIDVALIDITY are
    /// rejected — without it the per-folder cursor is meaningless.
    pub async fn examine(&self, folder: &str) -> Result<FolderStatus, ImapError> {
        let mut session = self.session.lock().await;
        // async-imap quotes the mailbox name itself (validate_str).
        let mailbox = session.examine(folder).await?;
        Ok(FolderStatus {
            uidvalidity: mailbox.uid_validity.ok_or_else(|| {
                ImapError::Imap(async_imap::error::Error::Bad(format!(
                    "server reported no UIDVALIDITY for {folder}"
                )))
            })?,
            uidnext: mailbox.uid_next.unwrap_or(1),
            exists: mailbox.exists,
        })
    }

    /// Full UID+FLAGS sweep of the currently-examined folder. `exists` is
    /// the EXAMINE count — passing 0 skips the round trip (a `UID FETCH
    /// 1:*` on an empty mailbox is a protocol error on some servers).
    pub async fn uid_list(&self, exists: u32) -> Result<Vec<UidEntry>, ImapError> {
        if exists == 0 {
            return Ok(Vec::new());
        }
        let mut session = self.session.lock().await;
        let fetches: Vec<_> = session
            .uid_fetch("1:*", "(UID FLAGS)")
            .await?
            .try_collect()
            .await?;
        Ok(fetches
            .iter()
            .filter_map(|f| {
                f.uid.map(|uid| UidEntry {
                    uid,
                    flags: fetch_flags(f),
                })
            })
            .collect())
    }

    /// Batched Message-ID peek for the re-home check: uid → normalized
    /// `<...>` header value (None when the message has no Message-ID).
    /// UIDs absent from the reply vanished mid-poll; callers treat their
    /// absence from the map like a missing header (download path, which
    /// then notices the message is gone).
    pub async fn fetch_message_id_headers(
        &self,
        uids: &[u32],
    ) -> Result<HashMap<u32, Option<String>>, ImapError> {
        if uids.is_empty() {
            return Ok(HashMap::new());
        }
        let set = uid_set(uids);
        let mut session = self.session.lock().await;
        let fetches: Vec<_> = session
            .uid_fetch(&set, "(UID BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])")
            .await?
            .try_collect()
            .await?;
        Ok(fetches
            .iter()
            .filter_map(|f| {
                f.uid
                    .map(|uid| (uid, f.header().and_then(parse_message_id_header)))
            })
            .collect())
    }

    /// Download one full message. `None` means the UID vanished between
    /// the listing and the fetch.
    pub async fn fetch_raw(&self, uid: u32) -> Result<Option<RawMessage>, ImapError> {
        let mut session = self.session.lock().await;
        let fetches: Vec<_> = session
            .uid_fetch(uid.to_string(), "(UID BODY.PEEK[] INTERNALDATE FLAGS)")
            .await?
            .try_collect()
            .await?;
        Ok(fetches
            .iter()
            .find(|f| f.uid == Some(uid))
            .and_then(|f| {
                f.body().map(|body| RawMessage {
                    raw: body.to_vec(),
                    internal_date: f.internal_date().map(|d| d.with_timezone(&Utc)),
                    flags: fetch_flags(f),
                })
            }))
    }

    /// `UID SEARCH SINCE <date>` in the currently-examined folder, for
    /// bounded backfills. SINCE compares INTERNALDATE, date-granular.
    pub async fn uid_search_since(&self, date: DateTime<Utc>) -> Result<Vec<u32>, ImapError> {
        let mut session = self.session.lock().await;
        let query = format!("SINCE {}", imap_date(date.date_naive()));
        let uids = session.uid_search(&query).await?;
        let mut uids: Vec<u32> = uids.into_iter().collect();
        uids.sort_unstable();
        Ok(uids)
    }

    pub async fn logout(&self) -> Result<(), ImapError> {
        let mut session = self.session.lock().await;
        session.logout().await?;
        Ok(())
    }
}

/// Compress a sorted-or-not UID list into an IMAP sequence-set string
/// ("1:5,9,12:13") to keep batched FETCH commands short.
fn uid_set(uids: &[u32]) -> String {
    let mut sorted = uids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut parts: Vec<String> = Vec::new();
    let mut i = 0;
    while i < sorted.len() {
        let start = sorted[i];
        let mut end = start;
        while i + 1 < sorted.len() && sorted[i + 1] == end + 1 {
            i += 1;
            end = sorted[i];
        }
        parts.push(if start == end {
            start.to_string()
        } else {
            format!("{start}:{end}")
        });
        i += 1;
    }
    parts.join(",")
}

fn imap_date(date: NaiveDate) -> String {
    // IMAP date format: 24-Aug-2026, always English month names.
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    use chrono::Datelike;
    format!(
        "{}-{}-{}",
        date.day(),
        MONTHS[date.month0() as usize],
        date.year()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uid_set_compresses_ranges() {
        assert_eq!(uid_set(&[1, 2, 3, 5, 9, 10]), "1:3,5,9:10");
        assert_eq!(uid_set(&[7]), "7");
        assert_eq!(uid_set(&[3, 1, 2, 2]), "1:3");
    }

    #[test]
    fn message_id_header_normalized() {
        let hdr = b"Message-ID: <abc@example.com>\r\n\r\n";
        assert_eq!(
            parse_message_id_header(hdr).as_deref(),
            Some("<abc@example.com>")
        );
        assert_eq!(parse_message_id_header(b"Subject: x\r\n\r\n"), None);
    }

    #[test]
    fn imap_date_format() {
        let d = NaiveDate::from_ymd_opt(2026, 8, 24).unwrap();
        assert_eq!(imap_date(d), "24-Aug-2026");
    }
}
