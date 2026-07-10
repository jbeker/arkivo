//! Serde types for the slice of the Gmail REST API the connector uses.
//!
//! Google is inconsistent about numeric fields: `historyId` arrives as a
//! string from `messages.get` but as a number from `history.list`, and
//! `internalDate` is an epoch-millis string. The deserializer helpers
//! below accept either representation.

use base64::Engine;
use serde::{Deserialize, Deserializer};

/// users.getProfile — the granted address and the mailbox's current
/// history id, used to bracket a backfill sweep before it starts.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Profile {
    pub email_address: String,
    #[serde(deserialize_with = "de_string_or_number")]
    pub history_id: String,
}

/// One entry of users.messages.list: ids only.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageRef {
    pub id: String,
    #[serde(default)]
    pub thread_id: Option<String>,
}

/// users.messages.list response. `messages` is absent entirely when the
/// listing is empty.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageListPage {
    #[serde(default)]
    pub messages: Vec<MessageRef>,
    #[serde(default)]
    pub next_page_token: Option<String>,
    #[serde(default)]
    pub result_size_estimate: Option<u64>,
}

/// users.messages.get. With `format=raw` the `raw` field carries the full
/// RFC822 message, base64url-encoded; with `format=minimal` it is absent
/// and only the metadata fields are populated.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailMessage {
    pub id: String,
    #[serde(default)]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub label_ids: Vec<String>,
    #[serde(default, deserialize_with = "de_opt_millis")]
    pub internal_date: Option<i64>,
    #[serde(default)]
    pub size_estimate: Option<i64>,
    #[serde(default)]
    pub raw: Option<String>,
}

impl GmailMessage {
    /// Decode the base64url `raw` body. Google emits unpadded URL-safe
    /// base64; stripping any padding first accepts both variants.
    pub fn raw_bytes(&self) -> Result<Vec<u8>, String> {
        let raw = self
            .raw
            .as_deref()
            .ok_or("message has no raw body (fetched without format=raw)")?;
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(raw.trim_end_matches('='))
            .map_err(|e| format!("invalid base64url body: {e}"))
    }
}

/// users.history.list response. The top-level `historyId` is the
/// mailbox's *current* id — adopt it only after every page is durable.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryPage {
    #[serde(default)]
    pub history: Vec<HistoryRecord>,
    #[serde(default)]
    pub next_page_token: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string_or_number")]
    pub history_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryRecord {
    #[serde(deserialize_with = "de_string_or_number")]
    pub id: String,
    #[serde(default)]
    pub messages_added: Vec<HistoryMessage>,
    #[serde(default)]
    pub messages_deleted: Vec<HistoryMessage>,
    #[serde(default)]
    pub labels_added: Vec<HistoryLabelChange>,
    #[serde(default)]
    pub labels_removed: Vec<HistoryLabelChange>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryMessage {
    pub message: MessageStub,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryLabelChange {
    pub message: MessageStub,
    #[serde(default)]
    pub label_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageStub {
    pub id: String,
    #[serde(default)]
    pub label_ids: Vec<String>,
}

fn de_string_or_number<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum V {
        S(String),
        N(u64),
    }
    Ok(match V::deserialize(deserializer)? {
        V::S(s) => s,
        V::N(n) => n.to_string(),
    })
}

fn de_opt_string_or_number<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    de_string_or_number(deserializer).map(Some)
}

fn de_opt_millis<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum V {
        S(String),
        N(i64),
    }
    match V::deserialize(deserializer)? {
        V::N(n) => Ok(Some(n)),
        V::S(s) => s
            .parse::<i64>()
            .map(Some)
            .map_err(|e| serde::de::Error::custom(format!("bad internalDate {s:?}: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_id_accepts_number_and_string() {
        let p: Profile =
            serde_json::from_str(r#"{"emailAddress": "a@b.c", "historyId": 42}"#).unwrap();
        assert_eq!(p.history_id, "42");
        let p: Profile =
            serde_json::from_str(r#"{"emailAddress": "a@b.c", "historyId": "42"}"#).unwrap();
        assert_eq!(p.history_id, "42");
    }

    #[test]
    fn raw_bytes_decodes_padded_and_unpadded() {
        let mut m: GmailMessage = serde_json::from_str(
            r#"{"id": "m1", "raw": "aGVsbG8gd29ybGQ", "internalDate": "1577880000000"}"#,
        )
        .unwrap();
        assert_eq!(m.raw_bytes().unwrap(), b"hello world");
        assert_eq!(m.internal_date, Some(1_577_880_000_000));
        m.raw = Some("aGVsbG8gd29ybGQ=".into());
        assert_eq!(m.raw_bytes().unwrap(), b"hello world");
    }

    #[test]
    fn empty_list_page_deserializes() {
        let page: MessageListPage = serde_json::from_str(r#"{"resultSizeEstimate": 0}"#).unwrap();
        assert!(page.messages.is_empty());
        assert!(page.next_page_token.is_none());
    }
}
