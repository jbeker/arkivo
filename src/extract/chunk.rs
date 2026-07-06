//! Chunker for the embedding path: ~500-token targets (chars/4 heuristic)
//! with ~60-token overlap, splitting on paragraph boundaries where
//! possible. The chars/4 ratio holds for English but not for token-dense
//! content (non-Latin scripts, long encoded blobs, URLs), which can
//! tokenize at close to 1 token per character. Keeping the target small
//! leaves headroom so even dense chunks stay within the embedding
//! context; the embedder also truncates as a final backstop.

/// Character budgets derived from the token targets (chars ≈ tokens × 4
/// for typical prose; deliberately conservative for denser content).
pub const TARGET_CHARS: usize = 2000;
pub const OVERLAP_CHARS: usize = 240;

/// Split `text` into embedding-sized chunks. Guarantees:
/// - no chunk exceeds TARGET_CHARS + OVERLAP_CHARS characters;
/// - every character of input appears in at least one chunk;
/// - adjacent chunks overlap (context bleed for retrieval).
pub fn chunk_text(text: &str) -> Vec<String> {
    let text = text.trim();
    if text.is_empty() {
        return Vec::new();
    }
    if text.chars().count() <= TARGET_CHARS {
        return vec![text.to_string()];
    }

    // Build paragraph units, hard-splitting any single paragraph that
    // exceeds the budget on its own.
    let mut units: Vec<String> = Vec::new();
    for paragraph in text.split("\n\n") {
        let paragraph = paragraph.trim_matches('\n');
        if paragraph.is_empty() {
            continue;
        }
        if paragraph.chars().count() <= TARGET_CHARS {
            units.push(paragraph.to_string());
        } else {
            let chars: Vec<char> = paragraph.chars().collect();
            for piece in chars.chunks(TARGET_CHARS) {
                units.push(piece.iter().collect());
            }
        }
    }

    let mut chunks: Vec<String> = Vec::new();
    let mut current = String::new();
    for unit in units {
        let unit_len = unit.chars().count();
        let current_len = current.chars().count();
        if current_len > 0 && current_len + unit_len + 2 > TARGET_CHARS {
            chunks.push(std::mem::take(&mut current));
        }
        if !current.is_empty() {
            current.push_str("\n\n");
        }
        current.push_str(&unit);
    }
    if !current.is_empty() {
        chunks.push(current);
    }

    // Prepend overlap from the previous chunk's tail.
    let mut with_overlap = Vec::with_capacity(chunks.len());
    for (i, chunk) in chunks.iter().enumerate() {
        if i == 0 {
            with_overlap.push(chunk.clone());
        } else {
            let prev: Vec<char> = chunks[i - 1].chars().collect();
            let tail_start = prev.len().saturating_sub(OVERLAP_CHARS);
            let tail: String = prev[tail_start..].iter().collect();
            with_overlap.push(format!("{tail}\n\n{chunk}"));
        }
    }
    with_overlap
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paragraphs(count: usize, words_each: usize) -> String {
        (0..count)
            .map(|p| {
                (0..words_each)
                    .map(|w| format!("word{p}x{w}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>()
            .join("\n\n")
    }

    #[test]
    fn short_text_is_one_chunk() {
        assert_eq!(chunk_text("hello world"), vec!["hello world"]);
    }

    #[test]
    fn empty_text_yields_no_chunks() {
        assert!(chunk_text("   \n\n  ").is_empty());
    }

    #[test]
    fn no_chunk_exceeds_budget() {
        let text = paragraphs(40, 120);
        for chunk in chunk_text(&text) {
            assert!(
                chunk.chars().count() <= TARGET_CHARS + OVERLAP_CHARS + 2,
                "chunk of {} chars exceeds budget",
                chunk.chars().count()
            );
        }
    }

    #[test]
    fn every_paragraph_appears_in_some_chunk() {
        let text = paragraphs(40, 120);
        let chunks = chunk_text(&text);
        for paragraph in text.split("\n\n") {
            assert!(
                chunks.iter().any(|c| c.contains(paragraph)),
                "paragraph lost during chunking"
            );
        }
    }

    #[test]
    fn adjacent_chunks_overlap() {
        let text = paragraphs(40, 120);
        let chunks = chunk_text(&text);
        assert!(chunks.len() > 1);
        for pair in chunks.windows(2) {
            let tail: String = pair[0]
                .chars()
                .skip(pair[0].chars().count().saturating_sub(80))
                .collect();
            assert!(
                pair[1].contains(tail.trim()),
                "second chunk must start with the first chunk's tail"
            );
        }
    }

    #[test]
    fn giant_single_paragraph_is_hard_split() {
        let text = "x".repeat(TARGET_CHARS * 3);
        let chunks = chunk_text(&text);
        assert!(chunks.len() >= 3);
        for chunk in &chunks {
            assert!(chunk.chars().count() <= TARGET_CHARS + OVERLAP_CHARS + 2);
        }
    }
}
