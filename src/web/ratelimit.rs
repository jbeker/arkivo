//! Minimal per-IP token bucket for the unauthenticated auth endpoints.
//! Invite/recovery/MCP tokens carry 256 bits of entropy, so this guards
//! cost (DB round-trips, session writes), not guessability.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Sustained allowance: 10 requests/minute, with the same burst.
const REFILL_PER_SEC: f64 = 10.0 / 60.0;
const BURST: f64 = 10.0;
/// Buckets idle this long are swept once the map grows large.
const SWEEP_IDLE_SECS: u64 = 600;
const SWEEP_THRESHOLD: usize = 10_000;

#[derive(Default)]
pub struct RateLimiter {
    buckets: Mutex<HashMap<IpAddr, Bucket>>,
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// True when a request from `ip` is within budget.
    fn allow(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let mut buckets = self.buckets.lock().expect("rate limiter lock poisoned");
        if buckets.len() > SWEEP_THRESHOLD {
            buckets.retain(|_, b| now.duration_since(b.last).as_secs() < SWEEP_IDLE_SECS);
        }
        let bucket = buckets.entry(ip).or_insert(Bucket {
            tokens: BURST,
            last: now,
        });
        let elapsed = now.duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * REFILL_PER_SEC).min(BURST);
        bucket.last = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Client address for limiting. The last `X-Forwarded-For` entry is the
/// hop our own TLS terminator recorded, correct under both overwrite and
/// append proxy semantics; earlier entries are client-controlled. The
/// service port is loopback/compose-bound (spec §14), so requests that
/// bypass the proxy can't reach us with a forged header. Falls back to
/// the socket peer for direct (dev) access.
fn client_ip(request: &Request) -> Option<IpAddr> {
    let forwarded = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.rsplit(',').next())
        .and_then(|v| v.trim().parse().ok());
    forwarded.or_else(|| {
        request
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ci| ci.0.ip())
    })
}

pub async fn limit(
    State(limiter): State<Arc<RateLimiter>>,
    request: Request,
    next: Next,
) -> Response {
    match client_ip(&request) {
        Some(ip) if !limiter.allow(ip) => {
            tracing::warn!(%ip, path = %request.uri().path(), "rate limited");
            (StatusCode::TOO_MANY_REQUESTS, "too many requests").into_response()
        }
        _ => next.run(request).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burst_then_deny() {
        let limiter = RateLimiter::new();
        let ip: IpAddr = "203.0.113.7".parse().unwrap();
        for _ in 0..BURST as usize {
            assert!(limiter.allow(ip));
        }
        assert!(!limiter.allow(ip), "budget exhausted");
    }

    #[test]
    fn ips_are_independent() {
        let limiter = RateLimiter::new();
        let a: IpAddr = "203.0.113.7".parse().unwrap();
        let b: IpAddr = "203.0.113.8".parse().unwrap();
        for _ in 0..BURST as usize {
            assert!(limiter.allow(a));
        }
        assert!(!limiter.allow(a));
        assert!(limiter.allow(b), "other clients unaffected");
    }
}
