//! Hand-rolled Maildir writer. The crash-safety contract of spec §7.3
//! lives here: a message is "durably written" only after the file content
//! is fsync'd, the rename into new/ has happened, and the directory entry
//! itself is fsync'd. Only then may the JMAP state advance.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Abstraction over the canonical message store so tests can inject
/// write failures mid-batch.
pub trait MessageStore: Send + Sync {
    /// Durably write a message; returns the path relative to the store root.
    fn write(&self, name_hint: &str, contents: &[u8]) -> Result<String>;
    fn read(&self, rel_path: &str) -> Result<Vec<u8>>;
    fn remove(&self, rel_path: &str) -> Result<()>;
}

pub struct Maildir {
    root: PathBuf,
}

impl Maildir {
    /// Open (creating if needed) a Maildir tree at `root`.
    pub fn open_or_create(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        for sub in ["tmp", "new", "cur"] {
            fs::create_dir_all(root.join(sub))
                .with_context(|| format!("creating {}/{sub}", root.display()))?;
        }
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn unique_name(name_hint: &str) -> String {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        // Maildir forbids ':' and '/' in names; keep the hint conservative.
        let hint: String = name_hint
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .take(64)
            .collect();
        format!("{secs}.P{pid}Q{seq}.arkivo,{hint}")
    }

    fn fsync_dir(path: &Path) -> Result<()> {
        File::open(path)
            .and_then(|d| d.sync_all())
            .with_context(|| format!("fsync dir {}", path.display()))
    }
}

impl MessageStore for Maildir {
    fn write(&self, name_hint: &str, contents: &[u8]) -> Result<String> {
        let name = Self::unique_name(name_hint);
        let tmp_path = self.root.join("tmp").join(&name);
        let new_rel = format!("new/{name}");
        let new_path = self.root.join(&new_rel);

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .with_context(|| format!("creating {}", tmp_path.display()))?;
        file.write_all(contents)?;
        file.sync_all()?;
        drop(file);

        fs::rename(&tmp_path, &new_path)?;
        Self::fsync_dir(&self.root.join("new"))?;
        Ok(new_rel)
    }

    fn read(&self, rel_path: &str) -> Result<Vec<u8>> {
        let path = self.root.join(rel_path);
        fs::read(&path).with_context(|| format!("reading {}", path.display()))
    }

    fn remove(&self, rel_path: &str) -> Result<()> {
        let path = self.root.join(rel_path);
        fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        Self::fsync_dir(path.parent().unwrap_or(&self.root))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_lands_in_new_and_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let maildir = Maildir::open_or_create(dir.path()).unwrap();
        let rel = maildir.write("msg-1", b"raw message").unwrap();
        assert!(rel.starts_with("new/"));
        assert_eq!(maildir.read(&rel).unwrap(), b"raw message");
        assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
    }

    #[test]
    fn names_are_unique_across_rapid_writes() {
        let dir = tempfile::tempdir().unwrap();
        let maildir = Maildir::open_or_create(dir.path()).unwrap();
        let a = maildir.write("same-hint", b"a").unwrap();
        let b = maildir.write("same-hint", b"b").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn hint_is_sanitized() {
        let dir = tempfile::tempdir().unwrap();
        let maildir = Maildir::open_or_create(dir.path()).unwrap();
        let rel = maildir.write("a/b:c d", b"x").unwrap();
        assert!(!rel[4..].contains('/') && !rel.contains(':'));
    }

    #[test]
    fn remove_deletes_file() {
        let dir = tempfile::tempdir().unwrap();
        let maildir = Maildir::open_or_create(dir.path()).unwrap();
        let rel = maildir.write("m", b"x").unwrap();
        maildir.remove(&rel).unwrap();
        assert!(maildir.read(&rel).is_err());
    }
}
