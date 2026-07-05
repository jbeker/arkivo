/// HTML-to-text with a hardened fallback: html2text handles real-world
/// markup well, but a 30-year corpus will contain something pathological,
/// and one bad message must never wedge ingestion.
pub fn html_to_text(html: &str) -> String {
    // TrivialDecorator: no ** emphasis markers, no link footnotes —
    // search text, not terminal rendering.
    match html2text::config::with_decorator(html2text::render::TrivialDecorator::new())
        .string_from_read(html.as_bytes(), 200)
    {
        Ok(text) => text,
        Err(_) => strip_tags(html),
    }
}

/// Dumb tag stripper — last-resort fallback only.
fn strip_tags(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    for c in html.chars() {
        match c {
            '<' => in_tag = true,
            '>' => {
                in_tag = false;
                out.push(' ');
            }
            c if !in_tag => out.push(c),
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_paragraphs_and_lists() {
        let text = html_to_text("<p>Hello</p><ul><li>one</li><li>two</li></ul>");
        assert!(text.contains("Hello"));
        assert!(text.contains("one"));
        assert!(text.contains("two"));
    }

    #[test]
    fn tag_stripper_removes_markup() {
        assert_eq!(strip_tags("<b>bold</b> text").trim(), "bold  text".trim());
    }
}
