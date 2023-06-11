pub mod consumer;
pub mod publisher;

extern crate rdkafka;
extern crate serde_json;
extern crate tokio;

pub enum SimpleKafkaError {
    KafkaError(rdkafka::error::KafkaError),
    SerdeError(serde_json::Error),
}
