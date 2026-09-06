//! Office 365 ingestion over Microsoft Graph. Message identity is the
//! Graph immutable id; placement is the single folder display path in
//! `messages.mailbox_ids`; sync state is a per-folder delta link (see
//! `migrations/0007_o365.sql`).

pub mod backfill;
pub mod client;
pub mod sync;
pub mod types;

use std::collections::HashMap;

use anyhow::Result;
use sqlx::PgPool;

pub use client::{O365Client, O365Error, SCOPE};
pub use types::{DeltaItem, DeltaPage, FolderNode, Me};

use crate::db::accounts;

/// Concurrent `$value` downloads. Outlook enforces a small per-mailbox
/// concurrency ceiling (four) before it starts returning 429s.
pub const DOWNLOAD_CONCURRENCY: usize = 4;

/// Well-known folders that are never archived, subtrees included.
/// Deleted Items and Junk match the other providers' Trash/Spam policy;
/// the rest are transient or system folders.
pub const SKIP_WELL_KNOWN: [&str; 6] = [
    "deleteditems",
    "junkemail",
    "outbox",
    "syncissues",
    "conversationhistory",
    "recoverableitemsdeletions",
];

/// Well-known folders resolved purely so their rows carry a label.
pub const LABEL_WELL_KNOWN: [&str; 4] = ["inbox", "sentitems", "drafts", "archive"];

/// A folder the archive covers, with its computed display path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivableFolder {
    pub id: String,
    pub path: String,
    pub well_known_name: Option<String>,
    pub total_item_count: Option<i64>,
}

/// Compute display paths for the flattened folder tree and drop the
/// skipped subtrees. `well_known` maps folder id → well-known name for
/// every resolvable well-known folder. Top level = parent not in the
/// listed set (the root itself is never listed).
pub fn archivable_folders(
    nodes: &[FolderNode],
    well_known: &HashMap<String, String>,
) -> Vec<ArchivableFolder> {
    let by_id: HashMap<&str, &FolderNode> = nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    let is_skipped = |id: &str| -> bool {
        well_known
            .get(id)
            .is_some_and(|name| SKIP_WELL_KNOWN.contains(&name.as_str()))
    };

    let mut out = Vec::new();
    'nodes: for node in nodes {
        if node.is_hidden == Some(true) || is_skipped(&node.id) {
            continue;
        }
        // Walk up: build the path and check every ancestor. A cycle or a
        // runaway depth is treated as top-level after a generous bound.
        let mut segments = vec![node.display_name.as_str()];
        let mut cursor = node.parent_folder_id.as_deref();
        let mut depth = 0;
        while let Some(pid) = cursor {
            let Some(parent) = by_id.get(pid) else { break };
            if is_skipped(pid) || parent.is_hidden == Some(true) {
                continue 'nodes;
            }
            segments.push(parent.display_name.as_str());
            cursor = parent.parent_folder_id.as_deref();
            depth += 1;
            if depth > 64 {
                break;
            }
        }
        segments.reverse();
        out.push(ArchivableFolder {
            id: node.id.clone(),
            path: segments.join("/"),
            well_known_name: well_known.get(&node.id).cloned(),
            total_item_count: node.total_item_count,
        });
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out
}

/// List the mailbox's archivable folders: the flattened tree plus the
/// well-known resolutions that drive the skip set and labels.
pub async fn list_archivable(client: &O365Client) -> Result<Vec<ArchivableFolder>> {
    let nodes = client.list_folders().await?;
    let mut well_known: HashMap<String, String> = HashMap::new();
    for name in SKIP_WELL_KNOWN.iter().chain(LABEL_WELL_KNOWN.iter()) {
        if let Some(id) = client.resolve_well_known(name).await? {
            well_known.insert(id, (*name).to_string());
        }
    }
    Ok(archivable_folders(&nodes, &well_known))
}

/// Persist a rotated refresh token, if the client received one since
/// the last checkpoint. Cheap when nothing rotated; called at every
/// cursor checkpoint and at job end so the unpersisted window stays
/// one page wide.
pub async fn checkpoint_token(pool: &PgPool, account_id: i64, client: &O365Client) -> Result<()> {
    if let Some(sealed) = client.take_rotated_sealed_token()? {
        accounts::set_sealed_token(pool, account_id, &sealed).await?;
        tracing::debug!(account = account_id, "persisted rotated refresh token");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, name: &str, parent: Option<&str>) -> FolderNode {
        FolderNode {
            id: id.into(),
            display_name: name.into(),
            parent_folder_id: parent.map(String::from),
            child_folder_count: 0,
            total_item_count: Some(1),
            is_hidden: None,
        }
    }

    #[test]
    fn paths_nest_and_skipped_subtrees_vanish() {
        let nodes = vec![
            node("root-inbox", "Inbox", Some("root")),
            node("root-archive", "Archive", Some("root")),
            node("arch-2024", "2024", Some("root-archive")),
            node("arch-2024-q1", "Q1", Some("arch-2024")),
            node("root-deleted", "Deleted Items", Some("root")),
            node("deleted-sub", "Old stuff", Some("root-deleted")),
            node("root-junk", "Junk Email", Some("root")),
            {
                let mut n = node("hidden", "Hidden", Some("root"));
                n.is_hidden = Some(true);
                n
            },
        ];
        let well_known: HashMap<String, String> = [
            ("root-inbox", "inbox"),
            ("root-deleted", "deleteditems"),
            ("root-junk", "junkemail"),
        ]
        .into_iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect();
        let folders = archivable_folders(&nodes, &well_known);
        let paths: Vec<&str> = folders.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["Archive", "Archive/2024", "Archive/2024/Q1", "Inbox"]
        );
        assert_eq!(folders[3].well_known_name.as_deref(), Some("inbox"));
    }
}
