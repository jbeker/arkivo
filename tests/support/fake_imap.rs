//! In-process fake IMAP server: a raw TCP, line-based fixture exercising
//! the real `ImapClient` over `TlsMode::None`. Implements only the
//! command subset the sync engine uses (LOGIN, CAPABILITY, LIST,
//! SELECT/EXAMINE, UID FETCH, UID SEARCH, NOOP, LOGOUT), with literal
//! syntax on responses. Mutators simulate server-side changes between
//! polls; `body_fetch_count` proves the no-re-download contract.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

pub const USERNAME: &str = "arkivo@example.com";
pub const PASSWORD: &str = "app-password";

#[derive(Clone)]
pub struct FakeMessage {
    pub raw: Vec<u8>,
    pub flags: Vec<String>,
    pub internal_date: DateTime<Utc>,
}

struct Folder {
    special_use: Option<&'static str>,
    uidvalidity: u32,
    uidnext: u32,
    messages: BTreeMap<u32, FakeMessage>,
}

impl Folder {
    fn new(uidvalidity: u32) -> Self {
        Self {
            special_use: None,
            uidvalidity,
            uidnext: 1,
            messages: BTreeMap::new(),
        }
    }
}

struct Inner {
    folders: BTreeMap<String, Folder>,
    /// Count of full-body (BODY.PEEK[]) fetches — the "did we
    /// re-download" probe.
    body_fetches: u64,
    /// Count of full-folder membership sweeps (`UID FETCH 1:*` for
    /// UID/FLAGS only) — the "did we skip the unchanged folder" probe.
    uid_sweeps: u64,
    msgid_seq: u64,
}

pub struct FakeImap {
    inner: Arc<Mutex<Inner>>,
    addr: SocketAddr,
}

impl FakeImap {
    pub async fn start() -> Self {
        let mut folders = BTreeMap::new();
        folders.insert("INBOX".to_string(), Folder::new(1000));
        let inner = Arc::new(Mutex::new(Inner {
            folders,
            body_fetches: 0,
            uid_sweeps: 0,
            msgid_seq: 0,
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept_inner = inner.clone();
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let conn_inner = accept_inner.clone();
                tokio::spawn(async move {
                    let _ = serve_connection(socket, conn_inner).await;
                });
            }
        });
        Self { inner, addr }
    }

    pub fn host(&self) -> String {
        self.addr.ip().to_string()
    }

    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    // ---- mutators ------------------------------------------------------

    pub fn add_folder(&self, name: &str, special_use: Option<&'static str>) {
        let mut inner = self.inner.lock().unwrap();
        let uidvalidity = 1000 + inner.folders.len() as u32;
        let mut folder = Folder::new(uidvalidity);
        folder.special_use = special_use;
        inner.folders.insert(name.to_string(), folder);
    }

    /// Add a message with an auto-generated Message-ID. Returns its UID.
    pub fn add_message(
        &self,
        folder: &str,
        subject: &str,
        from: &str,
        date: DateTime<Utc>,
        flags: &[&str],
    ) -> u32 {
        let msgid = {
            let mut inner = self.inner.lock().unwrap();
            inner.msgid_seq += 1;
            format!("<msg-{}@fake.example.com>", inner.msgid_seq)
        };
        self.add_message_with_msgid(folder, subject, from, date, flags, Some(&msgid))
    }

    /// Add a message with no Message-ID header (or an explicit one).
    pub fn add_message_with_msgid(
        &self,
        folder: &str,
        subject: &str,
        from: &str,
        date: DateTime<Utc>,
        flags: &[&str],
        msgid: Option<&str>,
    ) -> u32 {
        let msgid_line = msgid
            .map(|m| format!("Message-ID: {m}\r\n"))
            .unwrap_or_default();
        let raw = format!(
            "{msgid_line}From: {from}\r\nSubject: {subject}\r\nDate: {}\r\n\r\nBody of {subject}\r\n",
            date.to_rfc2822(),
        )
        .into_bytes();
        self.add_raw(
            folder,
            FakeMessage {
                raw,
                flags: flags.iter().map(|f| f.to_string()).collect(),
                internal_date: date,
            },
        )
    }

    fn add_raw(&self, folder: &str, msg: FakeMessage) -> u32 {
        let mut inner = self.inner.lock().unwrap();
        let folder = inner.folders.get_mut(folder).expect("folder exists");
        let uid = folder.uidnext;
        folder.uidnext += 1;
        folder.messages.insert(uid, msg);
        uid
    }

    /// Move a message: same bytes, new folder, new UID (like a real
    /// server-side MOVE). Returns the new UID.
    pub fn move_message(&self, from: &str, uid: u32, to: &str) -> u32 {
        let msg = {
            let mut inner = self.inner.lock().unwrap();
            inner
                .folders
                .get_mut(from)
                .expect("source folder")
                .messages
                .remove(&uid)
                .expect("message exists")
        };
        self.add_raw(to, msg)
    }

    /// Copy a message into a second folder (same bytes, new UID there).
    pub fn copy_message(&self, from: &str, uid: u32, to: &str) -> u32 {
        let msg = {
            let inner = self.inner.lock().unwrap();
            inner
                .folders
                .get(from)
                .expect("source folder")
                .messages
                .get(&uid)
                .expect("message exists")
                .clone()
        };
        self.add_raw(to, msg)
    }

    pub fn delete_message(&self, folder: &str, uid: u32) {
        let mut inner = self.inner.lock().unwrap();
        inner
            .folders
            .get_mut(folder)
            .expect("folder exists")
            .messages
            .remove(&uid)
            .expect("message exists");
    }

    pub fn set_flags(&self, folder: &str, uid: u32, flags: &[&str]) {
        let mut inner = self.inner.lock().unwrap();
        let msg = inner
            .folders
            .get_mut(folder)
            .expect("folder exists")
            .messages
            .get_mut(&uid)
            .expect("message exists");
        msg.flags = flags.iter().map(|f| f.to_string()).collect();
    }

    /// Simulate a mailbox rebuild: new UIDVALIDITY, all UIDs renumbered.
    pub fn bump_uidvalidity(&self, folder: &str) {
        let mut inner = self.inner.lock().unwrap();
        let folder = inner.folders.get_mut(folder).expect("folder exists");
        folder.uidvalidity += 1;
        let old = std::mem::take(&mut folder.messages);
        folder.uidnext = 1;
        for (_, msg) in old {
            folder.messages.insert(folder.uidnext, msg);
            folder.uidnext += 1;
        }
    }

    pub fn body_fetch_count(&self) -> u64 {
        self.inner.lock().unwrap().body_fetches
    }

    pub fn uid_sweep_count(&self) -> u64 {
        self.inner.lock().unwrap().uid_sweeps
    }

    pub fn uids(&self, folder: &str) -> Vec<u32> {
        let inner = self.inner.lock().unwrap();
        inner.folders[folder].messages.keys().copied().collect()
    }
}

// ---- protocol ----------------------------------------------------------

async fn serve_connection(socket: TcpStream, inner: Arc<Mutex<Inner>>) -> std::io::Result<()> {
    let (read_half, mut write) = socket.into_split();
    let mut lines = BufReader::new(read_half).lines();
    write.write_all(b"* OK fake-imap ready\r\n").await?;

    let mut selected: Option<String> = None;
    while let Some(line) = lines.next_line().await? {
        let (tag, rest) = match line.split_once(' ') {
            Some(parts) => parts,
            None => continue,
        };
        let mut tokens = tokenize(rest);
        if tokens.is_empty() {
            continue;
        }
        let command = tokens.remove(0).to_uppercase();
        // `UID FETCH` / `UID SEARCH` prefix.
        let (command, tokens) = if command == "UID" && !tokens.is_empty() {
            let sub = tokens[0].to_uppercase();
            (format!("UID {sub}"), tokens[1..].to_vec())
        } else {
            (command, tokens)
        };

        let mut out = Vec::new();
        match command.as_str() {
            "CAPABILITY" => {
                out.extend_from_slice(b"* CAPABILITY IMAP4rev1 SPECIAL-USE\r\n");
                ok(&mut out, tag, "CAPABILITY");
            }
            "LOGIN" => {
                if tokens.len() == 2 && tokens[0] == USERNAME && tokens[1] == PASSWORD {
                    ok(&mut out, tag, "LOGIN");
                } else {
                    out.extend_from_slice(
                        format!("{tag} NO [AUTHENTICATIONFAILED] bad credentials\r\n").as_bytes(),
                    );
                }
            }
            "LIST" => {
                let inner = inner.lock().unwrap();
                for (name, folder) in &inner.folders {
                    let mut attrs: Vec<&str> = Vec::new();
                    if let Some(su) = folder.special_use {
                        attrs.push(su);
                    }
                    out.extend_from_slice(
                        format!("* LIST ({}) \"/\" {}\r\n", attrs.join(" "), quote(name))
                            .as_bytes(),
                    );
                }
                drop(inner);
                ok(&mut out, tag, "LIST");
            }
            "SELECT" | "EXAMINE" => {
                let name = tokens.first().cloned().unwrap_or_default();
                let inner = inner.lock().unwrap();
                match inner.folders.get(&name) {
                    Some(folder) => {
                        out.extend_from_slice(
                            format!(
                                "* {} EXISTS\r\n* OK [UIDVALIDITY {}] UIDs valid\r\n* OK [UIDNEXT {}] next\r\n",
                                folder.messages.len(),
                                folder.uidvalidity,
                                folder.uidnext
                            )
                            .as_bytes(),
                        );
                        drop(inner);
                        selected = Some(name);
                        out.extend_from_slice(
                            format!("{tag} OK [READ-ONLY] {command} done\r\n").as_bytes(),
                        );
                    }
                    None => {
                        drop(inner);
                        out.extend_from_slice(format!("{tag} NO no such mailbox\r\n").as_bytes());
                    }
                }
            }
            "UID FETCH" => match &selected {
                Some(folder_name) if tokens.len() >= 2 => {
                    let items = tokens[1..].join(" ").to_uppercase();
                    let mut inner = inner.lock().unwrap();
                    let folder = inner.folders.get(folder_name).expect("selected exists");
                    let uids = parse_uid_set(&tokens[0], folder);
                    let mut responses = Vec::new();
                    for (seq, uid) in uids.iter().enumerate() {
                        let Some(msg) = folder.messages.get(uid) else {
                            continue;
                        };
                        responses.push(fetch_response(seq + 1, *uid, msg, &items));
                    }
                    let body_fetch = items.contains("BODY.PEEK[]");
                    if body_fetch {
                        inner.body_fetches += responses.len() as u64;
                    }
                    if tokens[0] == "1:*" && items.contains("FLAGS") && !items.contains("BODY") {
                        inner.uid_sweeps += 1;
                    }
                    drop(inner);
                    for r in responses {
                        out.extend_from_slice(&r);
                    }
                    ok(&mut out, tag, "UID FETCH");
                }
                _ => bad(&mut out, tag, "UID FETCH without folder"),
            },
            "UID SEARCH" => match &selected {
                Some(folder_name) => {
                    let inner = inner.lock().unwrap();
                    let folder = inner.folders.get(folder_name).expect("selected exists");
                    let query = tokens.join(" ").to_uppercase();
                    let since = query
                        .strip_prefix("SINCE ")
                        .and_then(|d| chrono::NaiveDate::parse_from_str(d.trim(), "%d-%b-%Y").ok());
                    let hits: Vec<String> = folder
                        .messages
                        .iter()
                        .filter(|(_, m)| {
                            since
                                .map(|s| m.internal_date.date_naive() >= s)
                                .unwrap_or(true)
                        })
                        .map(|(uid, _)| uid.to_string())
                        .collect();
                    drop(inner);
                    out.extend_from_slice(format!("* SEARCH {}\r\n", hits.join(" ")).as_bytes());
                    ok(&mut out, tag, "UID SEARCH");
                }
                None => bad(&mut out, tag, "UID SEARCH without folder"),
            },
            "NOOP" => ok(&mut out, tag, "NOOP"),
            "LOGOUT" => {
                out.extend_from_slice(b"* BYE fake-imap closing\r\n");
                ok(&mut out, tag, "LOGOUT");
                write.write_all(&out).await?;
                break;
            }
            _ => bad(&mut out, tag, "unimplemented"),
        }
        write.write_all(&out).await?;
    }
    Ok(())
}

fn ok(out: &mut Vec<u8>, tag: &str, what: &str) {
    out.extend_from_slice(format!("{tag} OK {what} completed\r\n").as_bytes());
}

fn bad(out: &mut Vec<u8>, tag: &str, why: &str) {
    out.extend_from_slice(format!("{tag} BAD {why}\r\n").as_bytes());
}

fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

/// Split a command tail into tokens, unquoting quoted strings and
/// keeping bracketed/parenthesized groups (`BODY.PEEK[HEADER.FIELDS
/// (MESSAGE-ID)]`, `(UID FLAGS)`) intact enough for our matching.
fn tokenize(rest: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = rest.chars().peekable();
    while let Some(&c) = chars.peek() {
        match c {
            ' ' => {
                chars.next();
            }
            '"' => {
                chars.next();
                let mut tok = String::new();
                while let Some(c) = chars.next() {
                    match c {
                        '\\' => {
                            if let Some(esc) = chars.next() {
                                tok.push(esc);
                            }
                        }
                        '"' => break,
                        other => tok.push(other),
                    }
                }
                tokens.push(tok);
            }
            _ => {
                let mut tok = String::new();
                let mut depth = 0i32;
                while let Some(&c) = chars.peek() {
                    match c {
                        '(' | '[' => depth += 1,
                        ')' | ']' => depth -= 1,
                        ' ' if depth <= 0 => break,
                        _ => {}
                    }
                    tok.push(c);
                    chars.next();
                }
                tokens.push(tok);
            }
        }
    }
    tokens
}

/// "1:*", "5", "1:3,7" against the folder's live UIDs.
fn parse_uid_set(set: &str, folder: &Folder) -> Vec<u32> {
    let max = folder.messages.keys().max().copied().unwrap_or(0);
    let resolve = |s: &str| -> u32 {
        if s == "*" {
            max
        } else {
            s.parse().unwrap_or(0)
        }
    };
    let mut uids = Vec::new();
    for part in set.split(',') {
        match part.split_once(':') {
            Some((a, b)) => {
                let (a, b) = (resolve(a), resolve(b));
                let (lo, hi) = (a.min(b), a.max(b));
                uids.extend(folder.messages.keys().filter(|u| (lo..=hi).contains(u)));
            }
            None => {
                let v = resolve(part);
                if folder.messages.contains_key(&v) {
                    uids.push(v);
                }
            }
        }
    }
    uids.sort_unstable();
    uids.dedup();
    uids
}

/// The Message-ID header section for a HEADER.FIELDS peek: the matching
/// header line if present, then the mandatory blank line.
fn message_id_section(raw: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(raw);
    let headers = text.split("\r\n\r\n").next().unwrap_or("");
    let mut section = String::new();
    for line in headers.lines() {
        if line.to_ascii_lowercase().starts_with("message-id:") {
            section.push_str(line);
            section.push_str("\r\n");
        }
    }
    section.push_str("\r\n");
    section.into_bytes()
}

fn fetch_response(seq: usize, uid: u32, msg: &FakeMessage, items: &str) -> Vec<u8> {
    let mut attrs: Vec<Vec<u8>> = vec![format!("UID {uid}").into_bytes()];
    if items.contains("FLAGS") {
        attrs.push(format!("FLAGS ({})", msg.flags.join(" ")).into_bytes());
    }
    if items.contains("INTERNALDATE") {
        attrs.push(
            format!(
                "INTERNALDATE \"{}\"",
                msg.internal_date.format("%d-%b-%Y %H:%M:%S +0000")
            )
            .into_bytes(),
        );
    }
    if items.contains("BODY.PEEK[HEADER.FIELDS") {
        let section = message_id_section(&msg.raw);
        let mut attr =
            format!("BODY[HEADER.FIELDS (MESSAGE-ID)] {{{}}}\r\n", section.len()).into_bytes();
        attr.extend_from_slice(&section);
        attrs.push(attr);
    }
    if items.contains("BODY.PEEK[]") {
        let mut attr = format!("BODY[] {{{}}}\r\n", msg.raw.len()).into_bytes();
        attr.extend_from_slice(&msg.raw);
        attrs.push(attr);
    }
    let mut out = format!("* {seq} FETCH (").into_bytes();
    for (i, attr) in attrs.iter().enumerate() {
        if i > 0 {
            out.push(b' ');
        }
        out.extend_from_slice(attr);
    }
    out.extend_from_slice(b")\r\n");
    out
}
