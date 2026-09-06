//! Serde types for the slice of Microsoft Graph the connector uses:
//! `/me`, the mail folder tree, message delta pages, and single-message
//! metadata. Delta items and metadata fetches share one shape because
//! the same `$select` is used for both.

use chrono::{DateTime, Utc};
use serde::Deserialize;

/// `GET /me?$select=userPrincipalName,mail`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Me {
    #[serde(default)]
    pub user_principal_name: Option<String>,
    #[serde(default)]
    pub mail: Option<String>,
}

impl Me {
    /// The mailbox address: `mail` when the directory populates it,
    /// otherwise the user principal name (always present).
    pub fn address(&self) -> Option<&str> {
        self.mail
            .as_deref()
            .filter(|m| !m.is_empty())
            .or(self.user_principal_name.as_deref())
    }
}

/// One `mailFolder` resource. Hidden folders are never returned unless
/// asked for, so a missing `isHidden` means visible.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FolderNode {
    pub id: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub parent_folder_id: Option<String>,
    #[serde(default)]
    pub child_folder_count: i64,
    #[serde(default)]
    pub total_item_count: Option<i64>,
    #[serde(default)]
    pub is_hidden: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
pub struct FolderPage {
    #[serde(default)]
    pub value: Vec<FolderNode>,
    #[serde(default, rename = "@odata.nextLink")]
    pub next_link: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Removed {
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowupFlag {
    #[serde(default)]
    pub flag_status: Option<String>,
}

/// A message as it appears in a delta page or a `$select`ed metadata
/// fetch. `removed` is set only on delta tombstones, whose other fields
/// are absent.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaItem {
    pub id: String,
    #[serde(default, rename = "@removed")]
    pub removed: Option<Removed>,
    #[serde(default)]
    pub is_read: Option<bool>,
    #[serde(default)]
    pub flag: Option<FollowupFlag>,
    #[serde(default)]
    pub is_draft: Option<bool>,
    #[serde(default)]
    pub parent_folder_id: Option<String>,
    #[serde(default)]
    pub received_date_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub internet_message_id: Option<String>,
}

impl DeltaItem {
    pub fn is_removed(&self) -> bool {
        self.removed.is_some()
    }

    pub fn is_flagged(&self) -> bool {
        self.flag
            .as_ref()
            .and_then(|f| f.flag_status.as_deref())
            .is_some_and(|s| s.eq_ignore_ascii_case("flagged"))
    }
}

/// One delta page: `next_link` while more pages follow, `delta_link` on
/// the last page of a cycle. Exactly one of the two is set.
#[derive(Debug, Default, Deserialize)]
pub struct DeltaPage {
    #[serde(default)]
    pub value: Vec<DeltaItem>,
    #[serde(default, rename = "@odata.nextLink")]
    pub next_link: Option<String>,
    #[serde(default, rename = "@odata.deltaLink")]
    pub delta_link: Option<String>,
}

/// Token endpoint response. Microsoft returns a fresh `refresh_token` on
/// every refresh-grant redemption (rotation); the client must adopt it.
#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub expires_in: u64,
    #[serde(default)]
    pub refresh_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GraphErrorBody {
    pub error: GraphErrorInner,
}

#[derive(Debug, Deserialize)]
pub struct GraphErrorInner {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_page_parses_items_and_tombstones() {
        let page: DeltaPage = serde_json::from_str(
            r#"{
              "value": [
                {"id": "m1", "isRead": false, "flag": {"flagStatus": "flagged"},
                 "parentFolderId": "f1", "receivedDateTime": "2020-01-03T12:00:00Z",
                 "internetMessageId": "<m1@x>"},
                {"id": "m2", "@removed": {"reason": "deleted"}}
              ],
              "@odata.deltaLink": "https://graph/delta?$deltatoken=abc"
            }"#,
        )
        .unwrap();
        assert_eq!(page.value.len(), 2);
        assert!(!page.value[0].is_removed());
        assert!(page.value[0].is_flagged());
        assert_eq!(
            page.value[0].received_date_time.unwrap().timestamp(),
            1_578_052_800
        );
        assert!(page.value[1].is_removed());
        assert!(page.next_link.is_none());
        assert!(page.delta_link.is_some());
    }

    #[test]
    fn me_prefers_mail_over_upn() {
        let me: Me = serde_json::from_str(
            r#"{"userPrincipalName": "u@contoso.onmicrosoft.com", "mail": "u@contoso.com"}"#,
        )
        .unwrap();
        assert_eq!(me.address(), Some("u@contoso.com"));
        let me: Me = serde_json::from_str(
            r#"{"userPrincipalName": "u@contoso.onmicrosoft.com", "mail": null}"#,
        )
        .unwrap();
        assert_eq!(me.address(), Some("u@contoso.onmicrosoft.com"));
    }
}
