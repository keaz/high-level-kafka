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

use std::collections::HashMap;

use thiserror::Error;

pub mod consumer;
pub mod publisher;

extern crate rdkafka;
extern crate tokio;

///
/// Metadata for a consumed message
///
#[derive(Debug)]
pub struct Metadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub headers: HashMap<String, String>,
}

/// Error type for the crate
///
#[derive(Debug, Error)]
pub enum KafkaError {
    /// Wrapper for rd_kafka errors
    #[error("Kafka error: {0}")]
    Kafka(rdkafka::error::KafkaError),
    /// Wrapper for serde_json errors
    #[error("Serde error: {0}")]
    Serde(serde_json::Error),
}

/// Result type for the crate
pub enum KafkaResult<T> {
    Ok(Option<(T, Metadata)>),
    Err(KafkaError),
}
