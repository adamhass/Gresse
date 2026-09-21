use env_logger::{Builder, Env};
use log::LevelFilter;
use std::sync::Once;

static LOGGER_INIT: Once = Once::new();

pub fn init() {
    init_internal(false);
}

pub fn init_for_tests() {
    init_internal(true);
}

fn init_internal(is_test: bool) {
    LOGGER_INIT.call_once(|| {
        let mut builder = Builder::from_env(Env::new().filter("GRESSE_LOG"));
        if std::env::var_os("GRESSE_LOG").is_none() {
            builder.filter_level(default_level());
        }
        builder.format_timestamp_millis();
        if is_test {
            builder.is_test(true);
        }
        let _ = builder.try_init();
    });
}

fn default_level() -> LevelFilter {
    if cfg!(feature = "log-level-trace") {
        LevelFilter::Trace
    } else if cfg!(feature = "log-level-debug") {
        LevelFilter::Debug
    } else if cfg!(feature = "log-level-info") {
        LevelFilter::Info
    } else if cfg!(feature = "log-level-warn") {
        LevelFilter::Warn
    } else if cfg!(feature = "log-level-error") {
        LevelFilter::Error
    } else {
        LevelFilter::Off
    }
}
