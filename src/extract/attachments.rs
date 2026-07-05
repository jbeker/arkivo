//! Attachment text extraction is deferred (user decision at planning):
//! only the pluggable seam exists. An implementation would shell out to
//! external tools (e.g. pdftotext) and return additional indexable text;
//! extracted text is never retained as a blob (spec §5).

use anyhow::Result;

pub struct AttachmentPart<'a> {
    pub content_type: &'a str,
    pub file_name: Option<&'a str>,
    pub data: &'a [u8],
}

pub trait AttachmentExtractor: Send + Sync {
    /// Return indexable text for the attachment, or None to skip it.
    fn extract(&self, part: &AttachmentPart<'_>) -> Result<Option<String>>;
}

/// Default: index message bodies only.
pub struct NoopExtractor;

impl AttachmentExtractor for NoopExtractor {
    fn extract(&self, _part: &AttachmentPart<'_>) -> Result<Option<String>> {
        Ok(None)
    }
}
