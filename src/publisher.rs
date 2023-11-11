use std::{collections::HashMap, fmt::Debug, time::Duration};

use log::error;
use rdkafka::{
    message::{Header as KafkaHeader, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};

use crate::SimpleKafkaError;

#[derive(Debug)]
pub struct Message<T: serde::Serialize + Debug> {
    topic: String,
    headers: HashMap<String, String>,
    data: T,
    key: String,
}

impl<T: serde::Serialize + Debug> Message<T> {
    ///
    /// Creates a new Message struct
    /// # Arguments
    /// * `topic` - A topic that the message should be published to
    /// * `headers` - A HashMap that holds the headers that should be published with the message
    /// * `data` - A generic type that holds the data that should be published, data should be serializable
    /// * `key` - A key that should be used to publish the message
    ///
    pub fn new(topic: String, headers: HashMap<String, String>, data: T, key: String) -> Self {
        Message {
            topic,
            headers,
            data,
            key,
        }
    }
}

///
/// A Producer that can be use to publish messages to kafka
///
///
pub struct KafkaProducer {
    producer: FutureProducer,
    duration_secs: Duration,
}

impl KafkaProducer {
    ///
    /// Creates a KakfkaProducer from a bootstrap_servers string
    ///
    /// # Arguments
    /// * `bootstrap_servers` - Comma separated bootstrap servers
    ///
    /// # Returns
    /// * `KafkaProducer` - A KafkaProducer that can be used to publish messages to kafka
    ///
    /// # Example
    ///
    /// ```
    /// use simple_kafka::KafkaProducer;
    ///
    /// let producer = KafkaProducer::from("localhost:9092").unwrap();
    /// ```
    pub fn from(bootstrap_servers: &str) -> Result<Self, SimpleKafkaError> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create::<FutureProducer>();

        if let Err(error) = producer {
            return Err(SimpleKafkaError::KafkaError(error));
        }

        let producer = producer.unwrap();

        Ok(KafkaProducer {
            producer,
            duration_secs: Duration::from_secs(10),
        })
    }

    ///
    /// Publishes a message to a topic
    ///
    /// # Arguments
    /// * `message` - A Message struct that holds the topic, headers, data and key
    ///
    /// # Example
    ///
    /// ```
    /// use simple_kafka::{KafkaProducer, Message};
    /// #[derive(Serialize, Deserialize, Debug)]
    ///  struct Data {
    ///     attra_one: String,
    ///     attra_two: i8,
    /// }
    ///
    /// let producer = KafkaProducer::from("localhost:9092").unwrap();
    /// let data  = Data {
    ///     attra_one: "123".to_string(),
    ///     attra_two: 12,
    /// };  
    /// let data = Message::new("topic".to_string(), HashMap::new(), data, "key".to_string());
    /// let result = producer.produce(data).await;
    /// ```
    pub async fn produce<T: serde::Serialize + Debug>(
        &self,
        message: Message<T>,
    ) -> Result<(), SimpleKafkaError> {
        let mut builder = FutureRecord::to(&message.topic).key(message.key.as_str());
        let mut kafka_headers = OwnedHeaders::new();
        for (header, value) in message.headers.iter() {
            kafka_headers = kafka_headers.insert(KafkaHeader {
                key: header.as_str(),
                value: Some(value.as_str()),
            });
        }

        builder = builder.headers(kafka_headers);

        let restult = serde_json::to_string(&message.data);
        if let Err(error) = restult {
            return Err(SimpleKafkaError::SerdeError(error));
        }

        let serialized = restult.unwrap();

        let publish_result = self
            .producer
            .send(builder.payload(&serialized), self.duration_secs)
            .await;

        if let Err((error, _)) = publish_result {
            error!("Unable to send message {:?}, error {}", message, error);
            return Err(SimpleKafkaError::KafkaError(error));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[tokio::test]
    async fn publish_message_test() {
        let publisher = KafkaProducer::from("localhost:9092").unwrap();
        let data = Data {
            attra_one: "123".to_string(),
            attra_two: 12,
        };

        let mut headers = HashMap::new();
        headers.insert("header_one".to_string(), "value_one".to_string());
        headers.insert("header_two".to_string(), "value_two".to_string());

        let data = Message::new("topic".to_string(), headers, data, "key".to_string());
        let result = publisher.produce(data).await;
        assert!(result.is_ok());
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Data {
        attra_one: String,
        attra_two: i8,
    }
}
