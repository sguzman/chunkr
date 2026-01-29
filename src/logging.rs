use crate::config::LoggingConfig;
use chrono::Utc;
use std::fmt;
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

pub fn init(config: &LoggingConfig) {
    let filter = EnvFilter::try_new(config.level.clone()).unwrap_or_else(|_| {
        EnvFilter::new("info")
    });
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .with_level(true)
        .with_target(true)
        .event_format(ColorPrefixFormat);
    Registry::default().with(filter).with(fmt_layer).init();
}

struct ColorPrefixFormat;

impl<S, N> FormatEvent<S, N> for ColorPrefixFormat
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> tracing_subscriber::fmt::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        write!(writer, "{now} ")?;
        write!(writer, "{} ", meta.level())?;
        write!(writer, "{}: ", meta.target())?;

        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);

        if let Some(prefix) = visitor.color_prefix.take() {
            write!(writer, "{prefix}")?;
        }
        if let Some(message) = visitor.message.take() {
            write!(writer, "{message}")?;
        }

        for (k, v) in visitor.fields {
            write!(writer, " {k}={v}")?;
        }
        writeln!(writer)
    }
}

#[derive(Default)]
struct FieldVisitor {
    color_prefix: Option<String>,
    message: Option<String>,
    fields: Vec<(String, String)>,
}

impl tracing::field::Visit for FieldVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "color_prefix" => self.color_prefix = Some(value.to_string()),
            "message" => self.message = Some(value.to_string()),
            name => self.fields.push((name.to_string(), format!("{value:?}"))),
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        match field.name() {
            "color_prefix" => self.color_prefix = Some(format!("{value:?}").trim_matches('"').to_string()),
            "message" => self.message = Some(format!("{value:?}").trim_matches('"').to_string()),
            name => self.fields.push((name.to_string(), format!("{value:?}"))),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum LogOp {
    Ollama,
    Qdrant,
    Quickwit,
}

pub fn color_prefix(file_key: &str, chunk_key: Option<&str>, op: Option<LogOp>) -> String {
    let mut out = String::new();
    out.push_str(&color_block(color_for_key(file_key)));
    out.push(' ');
    if let Some(chunk_key) = chunk_key {
        out.push_str(&color_block(color_for_key(chunk_key)));
        out.push(' ');
    }
    if let Some(op) = op {
        out.push_str(&color_block(op_color(op)));
        out.push(' ');
    }
    out
}

fn op_color(op: LogOp) -> u8 {
    match op {
        LogOp::Ollama => 39,
        LogOp::Qdrant => 82,
        LogOp::Quickwit => 220,
    }
}

fn color_for_key(key: &str) -> u8 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in key.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let palette = 216u64;
    let idx = (hash % palette) as u8;
    16 + idx
}

fn color_block(color: u8) -> String {
    format!("\x1b[38;5;{}m█\x1b[0m", color)
}
