//! Ingestion-time sanitization (spec §2.2 control 3 — defense in depth
//! behind the recency cutoff). At promotion, reset-link URLs and one-time
//! codes are redacted and configurable sender categories are quarantined.
//! Heuristic by design; the cutoff is the primary control.

use regex::Regex;
use serde::Deserialize;
use std::sync::LazyLock;

pub const REDACTED_LINK: &str = "[REDACTED-LINK]";
pub const REDACTED_CODE: &str = "[REDACTED-CODE]";

/// URLs whose path/query smells like account recovery or verification.
static RESET_URL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)https?://[^\s<>\x22]*(reset|verify|confirm|passwordless|magic[-_]?link|recover|activate|one[-_]?time|token=|otp=|code=)[^\s<>\x22]*",
    )
    .unwrap()
});

/// A 6-8 digit number close to an OTP-ish keyword. The keyword gate is
/// what keeps invoice numbers and zip codes out of the redactor.
static OTP_KEYWORD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(code|verification|one[- ]?time|otp|passcode|2fa|security code|pin)\b")
        .unwrap()
});
static OTP_DIGITS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b\d{6,8}\b").unwrap());

/// How near (in characters) a digit run must be to a keyword to count.
const OTP_PROXIMITY: usize = 120;

/// Per-account policy, deserialized from mail_accounts.sanitize_policy
/// jsonb; None falls back to defaults.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SanitizePolicy {
    /// Extra regex patterns to redact (on top of the built-ins).
    #[serde(default)]
    pub extra_redact_patterns: Vec<String>,
    /// Sender substrings to quarantine entirely (e.g. "@bank.example").
    #[serde(default)]
    pub quarantine_senders: Vec<String>,
}

impl SanitizePolicy {
    pub fn from_value(value: Option<&serde_json::Value>) -> Self {
        value
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default()
    }
}

#[derive(Debug, PartialEq)]
pub enum SanitizeOutcome {
    /// Body safe to index; `redacted` reports whether anything was cut.
    Clean { body: String, redacted: bool },
    /// Sender matches a quarantined category: do not index at all.
    Quarantined { reason: String },
}

pub fn sanitize(policy: &SanitizePolicy, from_addr: Option<&str>, body: &str) -> SanitizeOutcome {
    if let Some(from) = from_addr {
        let from_lower = from.to_lowercase();
        for needle in &policy.quarantine_senders {
            if !needle.is_empty() && from_lower.contains(&needle.to_lowercase()) {
                return SanitizeOutcome::Quarantined {
                    reason: format!("sender matches quarantine pattern {needle:?}"),
                };
            }
        }
    }

    let mut redacted = false;
    let mut out = RESET_URL
        .replace_all(body, |_: &regex::Captures| {
            redacted = true;
            REDACTED_LINK.to_string()
        })
        .into_owned();

    out = redact_otp_codes(&out, &mut redacted);

    for pattern in &policy.extra_redact_patterns {
        if let Ok(re) = Regex::new(pattern) {
            out = re
                .replace_all(&out, |_: &regex::Captures| {
                    redacted = true;
                    REDACTED_CODE.to_string()
                })
                .into_owned();
        }
    }

    SanitizeOutcome::Clean {
        body: out,
        redacted,
    }
}

/// Redact 6-8 digit runs that sit within OTP_PROXIMITY characters of an
/// OTP-ish keyword.
fn redact_otp_codes(body: &str, redacted: &mut bool) -> String {
    let keyword_spans: Vec<(usize, usize)> = OTP_KEYWORD
        .find_iter(body)
        .map(|m| (m.start(), m.end()))
        .collect();
    if keyword_spans.is_empty() {
        return body.to_string();
    }
    let mut out = String::with_capacity(body.len());
    let mut last = 0;
    for m in OTP_DIGITS.find_iter(body) {
        let near_keyword = keyword_spans.iter().any(|(ks, ke)| {
            // Gap between the keyword span and the digit span, whichever
            // side the keyword is on. saturating_sub alone would read
            // "keyword anywhere before the digits" as distance zero.
            let gap = if *ke <= m.start() {
                m.start() - ke
            } else if m.end() <= *ks {
                ks - m.end()
            } else {
                0 // overlapping
            };
            gap < OTP_PROXIMITY
        });
        if near_keyword {
            out.push_str(&body[last..m.start()]);
            out.push_str(REDACTED_CODE);
            last = m.end();
            *redacted = true;
        }
    }
    out.push_str(&body[last..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clean_body(policy: &SanitizePolicy, from: Option<&str>, body: &str) -> (String, bool) {
        match sanitize(policy, from, body) {
            SanitizeOutcome::Clean { body, redacted } => (body, redacted),
            other => panic!("expected Clean, got {other:?}"),
        }
    }

    #[test]
    fn reset_links_are_redacted() {
        let cases = [
            "Click https://example.com/account/reset?token=abc123 to continue.",
            "Verify at https://id.example.org/verify/xyz now",
            "https://example.com/magic-link/aaaa",
            "Recover: https://example.com/recover?u=1",
        ];
        for body in cases {
            let (out, redacted) = clean_body(&SanitizePolicy::default(), None, body);
            assert!(redacted, "should redact: {body}");
            assert!(out.contains(REDACTED_LINK), "missing marker in: {out}");
            assert!(!out.contains("token=abc123"));
        }
    }

    #[test]
    fn ordinary_links_survive() {
        let body = "Read the article at https://example.com/blog/2024/rust-tips.html";
        let (out, redacted) = clean_body(&SanitizePolicy::default(), None, body);
        assert!(!redacted);
        assert_eq!(out, body);
    }

    #[test]
    fn otp_near_keyword_is_redacted() {
        let body = "Your verification code is 482913. It expires in 10 minutes.";
        let (out, redacted) = clean_body(&SanitizePolicy::default(), None, body);
        assert!(redacted);
        assert!(out.contains(REDACTED_CODE));
        assert!(!out.contains("482913"));
    }

    #[test]
    fn digits_without_keyword_survive() {
        // Invoice numbers, order ids, zips: no OTP keyword nearby.
        let body = "Invoice 483920 for order 1234567 shipped to 94110.";
        let (out, redacted) = clean_body(&SanitizePolicy::default(), None, body);
        assert!(!redacted, "no keyword context: must not redact");
        assert_eq!(out, body);
    }

    #[test]
    fn prose_about_codes_without_digits_is_untouched() {
        let body = "The verification code arrives by SMS and is six digits long.";
        let (out, redacted) = clean_body(&SanitizePolicy::default(), None, body);
        assert!(!redacted);
        assert_eq!(out, body);
    }

    #[test]
    fn distant_digits_survive_keyword_elsewhere() {
        let filler = "x".repeat(400);
        let body = format!("Enter your code above.\n{filler}\nRef 555123 is your parcel number.");
        let (out, _) = clean_body(&SanitizePolicy::default(), None, &body);
        assert!(
            out.contains("555123"),
            "digits far from keyword must survive"
        );
    }

    #[test]
    fn quarantined_sender_is_flagged() {
        let policy = SanitizePolicy {
            quarantine_senders: vec!["@bank.example".into()],
            ..Default::default()
        };
        let outcome = sanitize(&policy, Some("alerts@bank.example"), "anything");
        assert!(matches!(outcome, SanitizeOutcome::Quarantined { .. }));

        // Other senders unaffected.
        let outcome = sanitize(&policy, Some("friend@gmail.com"), "anything");
        assert!(matches!(outcome, SanitizeOutcome::Clean { .. }));
    }

    #[test]
    fn extra_patterns_apply() {
        let policy = SanitizePolicy {
            extra_redact_patterns: vec![r"SECRET-\d+".into()],
            ..Default::default()
        };
        let (out, redacted) = clean_body(&policy, None, "ref SECRET-99 attached");
        assert!(redacted);
        assert!(!out.contains("SECRET-99"));
    }

    #[test]
    fn realistic_reset_email_fully_sanitized() {
        let body = "\
Someone requested a password reset for your account.\n\n\
Reset your password: https://accounts.example.com/reset?token=eyJhbGciOi\n\n\
Or enter this one-time code: 71042953\n\n\
If you didn't request this, ignore this email.";
        let (out, redacted) = clean_body(&SanitizePolicy::default(), None, body);
        assert!(redacted);
        assert!(!out.contains("eyJhbGciOi"));
        assert!(!out.contains("71042953"));
        assert!(out.contains("Someone requested a password reset"));
    }

    #[test]
    fn policy_parses_from_account_jsonb() {
        let value = serde_json::json!({
            "quarantine_senders": ["@paypal.example"],
            "extra_redact_patterns": ["ACCT-\\d{4}"],
        });
        let policy = SanitizePolicy::from_value(Some(&value));
        assert_eq!(policy.quarantine_senders, vec!["@paypal.example"]);
        assert_eq!(policy.extra_redact_patterns.len(), 1);

        assert!(
            SanitizePolicy::from_value(None)
                .quarantine_senders
                .is_empty()
        );
    }
}
