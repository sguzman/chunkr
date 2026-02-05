# Preferred Libraries

This guide names the “go-to” libraries for common tasks when bootstrapping
services from this Rust template. When a new component is added, start with the
listed dependency for the corresponding concern and only consider alternatives
when that ecosystem cannot meet a specific requirement.

## Runtime: `tokio`

- **Motivation**: `tokio` is the most widely adopted async runtime in Rust,
  ships with a mature ecosystem of helpers (net, sync, time), and integrates
  cleanly with `reqwest`, `sqlx`, and `tracing`. It should be the first choice
  whenever you need asynchronous scheduling, timers, or blocking offloading.

## Logging: `tracing`

- **Motivation**: `tracing` provides structured, contextual logging and
  observability that scale beyond simple `println!`. It integrates with `tokio`
  and `sqlx`, supports spans across async boundaries, and has adapters for
  emitting logs to layered subscribers.

## HTTP Client: `reqwest`

- **Motivation**: Built on `tokio`, `reqwest` offers a high-level, ergonomic
  HTTP client with async/await support, JSON helpers, and TLS options suitable
  for service-to-service calls. Prefer it before pulling in lower-level crates
  unless you need an embedded/non-async client.

## SQL Clients: `sqlx`

- **Motivation**: `sqlx` (with `tokio` runtime support) has compile-time checked
  queries, connection pooling, and comprehensive feature support for
  PostgreSQL/MySQL/SQLite. Use it for relational database access before
  considering rimd, diesel, or other ORMs.

## Serialization: `serde`

- **Motivation**: `serde` is the de-facto standard for (de)serialization in
  Rust. It supports JSON, TOML, YAML, and custom formats via derive macros.
  Always author structs with `serde` derives before experimenting with
  alternative serializers.

## TOML Parsing: `toml`

- **Motivation**: The `toml` crate is the canonical implementation used by Cargo
  itself. It offers serde integration and is the lightest-weight way to consume
  config files or manifest-like documents.

## JSON Schema Validation: `jsonschema`

- **Motivation**: `jsonschema` provides a unifying validator that works with
  `serde_json::Value` and adheres to the JSON Schema specification. Use it for
  schema enforcement before exploring more niche validators.

## Error Handling: `thiserror`

- **Motivation**: `thiserror` keeps error enums concise with derive macros and
  integrates nicely with `anyhow` or `eyre` if you need error chaining. Favor it
  to keep error definitions readable and consistent.

## Date/time Handling: `chrono`

- **Motivation**: `chrono` offers a rich API for time zones, durations, and
  formatting. Combine it with `tokio` timers and `serde` for consistent
  timestamp handling before reaching for smaller/time-only crates.

## Randomness: `rand`

- **Motivation**: `rand` supports secure random numbers, distributions, and
  generators; it is maintained by the Rust team and covers most needs from
  simple sampling to cryptographic RNGs. Use it as the first option for
  randomness before niche randomness crates.
