use crate::config::LoggingConfig;
use tracing_subscriber::EnvFilter;

pub fn init(config: &LoggingConfig) {
    let filter = EnvFilter::try_new(config.level.clone()).unwrap_or_else(|_| {
        EnvFilter::new("info")
    });
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true)
        .init();
}
