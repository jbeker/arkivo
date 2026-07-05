//! Quote and signature stripping for the embedding path ONLY. The BM25
//! message index keeps the full body text, so an over-eager heuristic
//! here costs semantic-search recall, never keyword recall. Heuristics
//! are deliberately conservative and line-based.

/// Strip quoted reply history, forwarded blocks, and signatures.
pub fn strip_quotes(body: &str) -> String {
    let mut kept: Vec<&str> = Vec::new();

    for line in body.lines() {
        let trimmed = line.trim_end();

        // Everything after these markers is history/noise; stop entirely.
        if is_block_terminator(trimmed) {
            break;
        }
        // Quoted lines drop individually.
        if trimmed.starts_with('>') {
            continue;
        }
        // Attribution line immediately preceding a quote block
        // ("On <date>, <someone> wrote:").
        if is_attribution(trimmed) {
            continue;
        }
        kept.push(line);
    }

    // Trim trailing blank lines left behind by stripped content.
    while kept.last().is_some_and(|l| l.trim().is_empty()) {
        kept.pop();
    }
    kept.join("\n")
}

fn is_block_terminator(line: &str) -> bool {
    let t = line.trim();
    t == "-- "
        || t == "--"
        || t.starts_with("-----Original Message-----")
        || t.starts_with("________________________________")
        || t.starts_with("---------- Forwarded message")
        || t.starts_with("Begin forwarded message:")
}

fn is_attribution(line: &str) -> bool {
    let t = line.trim();
    (t.starts_with("On ") && t.ends_with("wrote:") && t.len() < 120)
        || (t.starts_with("Le ") && t.ends_with("écrit :") && t.len() < 120)
        || (t.starts_with("Am ") && t.ends_with("schrieb:") && t.len() < 120)
}

#[cfg(test)]
mod tests {
    use super::strip_quotes;

    #[test]
    fn drops_angle_quoted_lines() {
        let body = "Thanks, sounds good.\n\n> Earlier message\n> more of it\n";
        assert_eq!(strip_quotes(body), "Thanks, sounds good.");
    }

    #[test]
    fn drops_attribution_line() {
        let body = "Agreed.\n\nOn Tue, Jan 2, 2024 at 9:15 AM Alice <a@example.com> wrote:\n> hi\n";
        assert_eq!(strip_quotes(body), "Agreed.");
    }

    #[test]
    fn stops_at_outlook_original_message() {
        let body = "My reply here.\n\n-----Original Message-----\nFrom: someone\nfull history";
        assert_eq!(strip_quotes(body), "My reply here.");
    }

    #[test]
    fn stops_at_forwarded_block() {
        let body = "FYI see below\n\n---------- Forwarded message ---------\nFrom: x";
        assert_eq!(strip_quotes(body), "FYI see below");
    }

    #[test]
    fn stops_at_signature_delimiter() {
        let body = "Content here.\n\n-- \nJane Doe\nSome Corp\n";
        assert_eq!(strip_quotes(body), "Content here.");
    }

    #[test]
    fn keeps_normal_prose_untouched() {
        let body = "Line one.\nLine two mentions On something but not a quote.\nLine three.";
        assert_eq!(strip_quotes(body), body);
    }

    #[test]
    fn interleaved_reply_keeps_new_content() {
        let body = "> old point one\nMy response to one.\n> old point two\nMy response to two.";
        assert_eq!(
            strip_quotes(body),
            "My response to one.\nMy response to two."
        );
    }
}
