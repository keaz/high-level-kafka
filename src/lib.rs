//! # High-level kafka client for rust.
//!
//! This crate provides a high-level kafka client for rust. It is built on top of the [rdkafka](https://docs.rs/rdkafka/0.23.0/rdkafka/) crate.
//!
//! ## Features
//!
//! * [x] Consumer
//! * [x] Producer
//! * [ ] Admin
//! * [ ] Streams
//!

pub mod consumer;
pub mod publisher;

extern crate rdkafka;
extern crate serde_json;
extern crate tokio;

/// Error type for the crate
///
#[derive(Debug)]
pub enum SimpleKafkaError {
    /// Wrapper for rd_kafka errors
    KafkaError(rdkafka::error::KafkaError),
    /// Wrapper for serde_json errors
    SerdeError(serde_json::Error),
}
