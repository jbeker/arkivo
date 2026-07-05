//! Serde types for the small JMAP surface Arkivo uses (RFC 8620/8621):
//! session, Email/changes, Email/get, Email/query, blob download.

use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::HashMap;

pub const CAP_CORE: &str = "urn:ietf:params:jmap:core";
pub const CAP_MAIL: &str = "urn:ietf:params:jmap:mail";

/// Email/get properties Arkivo requests: index-relevant metadata only.
/// Bodies stay out of the sync loop; raw blobs are downloaded separately.
pub const EMAIL_PROPERTIES: &[&str] = &[
    "id",
    "blobId",
    "threadId",
    "messageId",
    "mailboxIds",
    "keywords",
    "receivedAt",
    "size",
    "hasAttachment",
    "from",
    "subject",
];

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Session {
    pub capabilities: HashMap<String, serde_json::Value>,
    pub primary_accounts: HashMap<String, String>,
    pub api_url: String,
    /// URI template with {accountId}, {blobId}, {name}, {type} placeholders.
    pub download_url: String,
    #[serde(default)]
    pub state: Option<String>,
}

impl Session {
    pub fn mail_account_id(&self) -> Option<&str> {
        self.primary_accounts.get(CAP_MAIL).map(String::as_str)
    }

    fn core_capability(&self, key: &str) -> Option<u64> {
        self.capabilities.get(CAP_CORE)?.get(key)?.as_u64()
    }

    /// Server-advertised batch ceiling for */get calls (RFC 8620 §2).
    pub fn max_objects_in_get(&self) -> u64 {
        self.core_capability("maxObjectsInGet").unwrap_or(500)
    }

    pub fn max_calls_in_request(&self) -> u64 {
        self.core_capability("maxCallsInRequest").unwrap_or(16)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailChangesResponse {
    pub account_id: String,
    pub old_state: String,
    pub new_state: String,
    pub has_more_changes: bool,
    #[serde(default)]
    pub created: Vec<String>,
    #[serde(default)]
    pub updated: Vec<String>,
    #[serde(default)]
    pub destroyed: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailGetResponse {
    pub account_id: String,
    pub state: String,
    #[serde(default)]
    pub list: Vec<Email>,
    #[serde(default)]
    pub not_found: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailQueryResponse {
    pub account_id: String,
    pub query_state: String,
    #[serde(default)]
    pub ids: Vec<String>,
    #[serde(default)]
    pub position: i64,
    #[serde(default)]
    pub total: Option<u64>,
}

/// The metadata subset of a JMAP Email object (see [`EMAIL_PROPERTIES`]).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Email {
    pub id: String,
    pub blob_id: String,
    #[serde(default)]
    pub thread_id: Option<String>,
    /// RFC 5322 Message-ID values (JMAP models this as an array).
    #[serde(default)]
    pub message_id: Option<Vec<String>>,
    #[serde(default)]
    pub mailbox_ids: HashMap<String, bool>,
    #[serde(default)]
    pub keywords: HashMap<String, bool>,
    pub received_at: DateTime<Utc>,
    #[serde(default)]
    pub size: i64,
    #[serde(default)]
    pub has_attachment: bool,
    #[serde(default)]
    pub from: Option<Vec<EmailAddress>>,
    #[serde(default)]
    pub subject: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmailAddress {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
}

impl Email {
    /// First Message-ID header value, angle-bracketed for storage.
    pub fn message_id_hdr(&self) -> Option<String> {
        self.message_id
            .as_ref()
            .and_then(|ids| ids.first())
            .map(|id| format!("<{id}>"))
    }

    pub fn from_addr(&self) -> Option<String> {
        self.from
            .as_ref()
            .and_then(|addrs| addrs.first())
            .and_then(|a| a.email.clone())
    }

    pub fn mailbox_id_list(&self) -> Vec<&str> {
        let mut ids: Vec<&str> = self
            .mailbox_ids
            .iter()
            .filter(|(_, present)| **present)
            .map(|(id, _)| id.as_str())
            .collect();
        ids.sort();
        ids
    }
}

/// JMAP method-level error (the `["error", {...}, id]` form).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MethodError {
    #[serde(rename = "type")]
    pub error_type: String,
    #[serde(default)]
    pub description: Option<String>,
}
