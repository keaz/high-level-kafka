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
/// Configuration options for the producer
///
pub struct ProducerOptiopns<'a> {
    bootstrap_servers: String,
    message_timeout_ms: String,
    queue_timeout_secs: u64 ,
    other_options: HashMap<&'a str, &'a str>,
}

impl<'a> ProducerOptiopns<'a> {
    ///
    /// Creates a new ConsumerOptiopns
    /// # Arguments
    /// * `bootstrap_servers` - Comma separated bootstrap servers
    /// * `message_timeout_ms` - Message timeout in milliseconds
    /// * `queue_timeout_secs` - Queue timeout in seconds
    /// * `other_options` - A HashMap that holds other options that should be used to create the consumer
    ///
    /// # Example
    /// ```
    /// use simple_kafka::ProducerOptiopns;
    ///
    /// let consumer_options = ProducerOptiopns::from("localhost:9092".to_string(), "5000".to_string(),5 HashMap::new());
    /// ```
    pub fn from(
        bootstrap_servers: String,
        message_timeout_ms: String,
        queue_timeout_secs: u64 ,
        other_options: HashMap<&'a str, &'a str>,
    ) -> Self {
        ProducerOptiopns {
            bootstrap_servers,
            message_timeout_ms,
            queue_timeout_secs,
            other_options,
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
    /// * `queue_timeout_secs` - Queue timeout in seconds
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
    pub fn from(bootstrap_servers: &str, queue_timeout_secs: u64) -> Result<Self, SimpleKafkaError> {
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
            duration_secs: Duration::from_secs(queue_timeout_secs),
        })
    }

    pub fn with_options(
        producer_options: ProducerOptiopns,
    ) -> Result<Self, SimpleKafkaError> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", producer_options.bootstrap_servers.as_str());
        config.set(
            "message.timeout.ms",
            producer_options.message_timeout_ms.as_str(),
        );

        producer_options.other_options.iter().for_each(|(key, value)| {
            config.set(*key, *value);
        });

        let producer = config.create::<FutureProducer>();

        if let Err(error) = producer {
            return Err(SimpleKafkaError::KafkaError(error));
        }

        let producer = producer.unwrap();

        Ok(KafkaProducer {
            producer,
            duration_secs: Duration::from_secs(producer_options.queue_timeout_secs),
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
        let publisher = KafkaProducer::from("localhost:9092",10).unwrap();
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
