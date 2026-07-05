//! Indexing-time text extraction (spec §8): parse the raw RFC822 blob,
//! select the best body, normalize HTML, and expose address headers for
//! the message index. Quote stripping and chunking live in their own
//! modules and apply only to the embedding path.

pub mod attachments;
pub mod chunk;
pub mod html;
pub mod quotes;

use mail_parser::{Address, MessageParser};

/// Everything the indexer needs from one raw message.
#[derive(Debug, Clone, Default)]
pub struct ExtractedEmail {
    pub subject: Option<String>,
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub body_text: String,
}

fn collect_addresses(addr: Option<&Address<'_>>) -> Vec<String> {
    addr.map(|a| {
        a.iter()
            .filter_map(|item| item.address().map(str::to_string))
            .collect()
    })
    .unwrap_or_default()
}

/// Parse a raw message and extract indexable text. Never fails hard: a
/// message that defeats the parser yields an empty body rather than
/// wedging the pipeline (the caller records index_status='failed' only
/// on infrastructure errors, not parse quality).
pub fn extract(raw: &[u8]) -> ExtractedEmail {
    let Some(message) = MessageParser::default().parse(raw) else {
        return ExtractedEmail::default();
    };

    // Prefer the first genuine text/plain part (mail-parser lists an
    // HTML part among text_bodies for HTML-only messages, so filter);
    // fall back to HTML through our hardened converter.
    let body_text = message
        .text_bodies()
        .find(|part| !part.is_text_html())
        .and_then(|part| part.text_contents())
        .map(str::to_string)
        .or_else(|| {
            message
                .html_bodies()
                .next()
                .and_then(|part| part.text_contents())
                .map(html::html_to_text)
        })
        .unwrap_or_default();

    ExtractedEmail {
        subject: message.subject().map(str::to_string),
        from: collect_addresses(message.from()),
        to: collect_addresses(message.to()),
        cc: collect_addresses(message.cc()),
        body_text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(headers_and_body: &str) -> Vec<u8> {
        headers_and_body.replace('\n', "\r\n").into_bytes()
    }

    #[test]
    fn plain_text_body_is_selected() {
        let email = extract(&raw(
            "From: Alice <alice@example.com>\nTo: bob@example.com\nCc: carol@example.com\n\
             Subject: hi\nContent-Type: text/plain\n\nHello there.\n",
        ));
        assert_eq!(email.subject.as_deref(), Some("hi"));
        assert_eq!(email.from, vec!["alice@example.com"]);
        assert_eq!(email.to, vec!["bob@example.com"]);
        assert_eq!(email.cc, vec!["carol@example.com"]);
        assert_eq!(email.body_text.trim(), "Hello there.");
    }

    #[test]
    fn html_only_body_is_converted() {
        let email = extract(&raw(
            "From: a@example.com\nSubject: html\nContent-Type: text/html\n\n\
             <html><body><p>Paragraph <b>one</b>.</p><p>Two.</p></body></html>\n",
        ));
        assert!(email.body_text.contains("Paragraph one."));
        assert!(email.body_text.contains("Two."));
        assert!(!email.body_text.contains('<'));
    }

    #[test]
    fn multipart_prefers_plain_text() {
        let email = extract(&raw("From: a@example.com\nSubject: mp\n\
             Content-Type: multipart/alternative; boundary=B\n\n\
             --B\nContent-Type: text/plain\n\nplain wins\n\
             --B\nContent-Type: text/html\n\n<p>html loses</p>\n--B--\n"));
        assert_eq!(email.body_text.trim(), "plain wins");
    }

    #[test]
    fn garbage_input_yields_empty_not_panic() {
        let email = extract(&[0xff, 0xfe, 0x00, 0x01]);
        assert!(email.body_text.is_empty());
    }
}
