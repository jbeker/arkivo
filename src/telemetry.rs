use tracing_subscriber::{EnvFilter, fmt};

/// Initialize tracing. `json` switches to structured JSON output for
/// production; the default is human-readable for interactive use.
pub fn init(json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = fmt().with_env_filter(filter);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}
