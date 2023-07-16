use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};

use futures::Future;
use log::{debug, error};
use rdkafka::{
    consumer::{Consumer as KafkaConsumer, StreamConsumer},
    error::KafkaError,
    message::{Headers, OwnedMessage},
    ClientConfig, Message as KafkaMessage,
};
use tokio::sync::Mutex;

///
/// A Consumer that can be used to consume messages from kafka and has the ability to pause and resume
///
pub struct PausableConsumer<T, F>
where
    T: for<'b> serde::Deserialize<'b>,
    F: Future<Output = ()> + Send + Sync + 'static,
{
    consumer: rdkafka::consumer::StreamConsumer,
    topics_map: HashMap<String, Box<dyn Fn(T, Metadata) -> F>>,
    is_runnig: Arc<Mutex<bool>>,
}

///
/// A Consumer that can be used to consume messages from kafka
///
pub struct Consumer<T, F>
where
    T: for<'b> serde::Deserialize<'b>,
    F: Future<Output = ()> + Send + Sync + 'static,
{
    consumer: rdkafka::consumer::StreamConsumer,
    topics_map: HashMap<String, Box<dyn Fn(T, Metadata) -> F>>,
}

///
/// Configuration options for Consumers
///
pub struct ConsumerOptiopns {
    bootstrap_servers: String,
    group_id: String,
    session_timeout_ms: String,
    enable_auto_commit: bool,
    enable_partition_eof: bool,
}

impl ConsumerOptiopns {
    ///
    /// Creates a new ConsumerOptiopns
    /// # Arguments
    /// * `bootstrap_servers` - Comma separated bootstrap servers
    /// * `group_id` - The group_id of the consumer
    /// * `session_timeout_ms` - The session timeout in milliseconds
    /// * `enable_auto_commit` - Enable auto commit
    /// * `enable_partition_eof` - Enable partition eof
    ///
    /// # Example
    /// ```
    /// use simple_kafka::ConsumerOptiopns;
    ///
    /// let consumer_options = ConsumerOptiopns::from("localhost:9092".to_string(), "group_id".to_string(), "5000".to_string(), true, true);
    /// ```
    pub fn from(
        bootstrap_servers: String,
        group_id: String,
        session_timeout_ms: String,
        enable_auto_commit: bool,
        enable_partition_eof: bool,
    ) -> Self {
        ConsumerOptiopns {
            bootstrap_servers,
            group_id,
            session_timeout_ms,
            enable_auto_commit,
            enable_partition_eof,
        }
    }
}

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

impl<T, F> PausableConsumer<T, F>
where
    T: for<'a> serde::Deserialize<'a>,
    F: Future<Output = ()> + Send + Sync + 'static,
{
    ///
    /// Creates a new PausableConsumer from a group_id and bootstrap_servers
    /// # Arguments
    /// * `group_id` - The group_id of the consumer
    /// * `bootstrap_servers` - The comma separated bootstrap servers
    ///
    /// # Example
    /// ```
    /// use simple_kafka::{PausableConsumer};
    ///
    /// let consumer = PausableConsumer::from("group_id", "localhost:9092");
    /// ```
    pub fn from(group_id: &str, bootstrap_servers: &str) -> Self {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .create()
            .expect("Consumer creation failed");
        PausableConsumer {
            consumer,
            topics_map: HashMap::new(),
            is_runnig: Arc::new(Mutex::new(true)),
        }
    }

    ///
    /// Creates a new PausedConsumer from consumer options
    /// # Arguments
    /// * `options` - A ConsumerOptions struct that holds the consumer options
    ///
    /// # Example
    /// ```
    /// use simple_kafka::{PausableConsumer, ConsumerOptiopns};
    ///
    /// let options = ConsumerOptiopns {
    ///     bootstrap_servers: "localhost:9092".to_string(),
    ///     group_id: "group_id".to_string(),
    ///     session_timeout_ms: "6000".to_string(),
    ///     enable_auto_commit: true,
    ///     enable_partition_eof: false,
    /// };
    /// let consumer = PausableConsumer::with_options(options);
    /// ```
    pub fn with_options(options: ConsumerOptiopns) -> Self {
        let enable_partition_eof = match options.enable_partition_eof {
            true => "true",
            false => "false",
        };

        let enable_auto_commit = match options.enable_auto_commit {
            true => "true",
            false => "false",
        };
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", options.group_id.as_str())
            .set("bootstrap.servers", options.bootstrap_servers.as_str())
            .set("enable.partition.eof", enable_partition_eof)
            .set("session.timeout.ms", options.session_timeout_ms.as_str())
            .set("enable.auto.commit", enable_auto_commit)
            .create()
            .expect("Consumer creation failed");
        PausableConsumer {
            consumer,
            topics_map: HashMap::new(),
            is_runnig: Arc::new(Mutex::new(true)),
        }
    }

    /// FIXME: Not ready yet
    /// Add message handles to the cosnumer
    /// Currently only support one handler.
    ///
    /// # Arguments
    /// * `topic` - The topic to subscribe to
    /// * `handler` - The handler function that will be called for each message for the give `topic`
    ///
    /// # Example
    /// ```
    /// use simple_kafka::{PausableConsumer};
    /// #[derive(Serialize, Deserialize, Debug)]
    /// struct Data {
    ///    attra_one: String,
    ///   attra_two: i8,
    /// }
    ///
    /// let consumer = PausableConsumer::from("group_id", "localhost:9092");
    /// let handler_1 = Box::new(| data: Data, metadata: Metadata| async move {
    ///    println!("Handler One ::: data: {:?}, metadata: {:?}", data, metadata);
    /// });
    /// consumer.add("topic_1".to_string(), handler_1);
    /// consumer.subscribe().await;
    /// ```
    fn add(&mut self, topic: String, handler: Box<dyn Fn(T, Metadata) -> F>) {
        self.topics_map.insert(topic, handler);
    }

    /// FIXME: Not ready yet
    /// Subcribes to given set of topics and calls the given function for each message
    ///
    /// # Example
    /// ```
    /// use simple_kafka::{PausableConsumer};
    /// #[derive(Serialize, Deserialize, Debug)]
    ///  struct Data {
    ///     attra_one: String,
    ///     attra_two: i8,
    /// }
    ///
    /// let consumer = PausableConsumer::from("group_id", "localhost:9092");
    /// let handler_1 = Box::new(| data: Data, metadata: Metadata| async move {
    ///     println!("Handler One ::: data: {:?}, metadata: {:?}", data, metadata);
    /// });
    /// consumer.add("topic_1".to_string(), handler_1);
    /// consumer.subscribe().await;
    /// ```
    async fn subscribe(&self) {
        let topics = self
            .topics_map
            .keys()
            .map(|key| key.as_str())
            .collect::<Vec<&str>>();

        self.consumer
            .subscribe(&topics)
            .expect("Can't subscribe to specified topic");
        loop {
            let is_runnig = self.is_runnig.lock().await;
            debug!("Subscriber is running: {:?}", *is_runnig);
            if !(*is_runnig) {
                debug!("Subscriber is stopped");
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }

            match self.consumer.recv().await {
                Ok(message) => {
                    let owned_message = message.detach();
                    handle_message(owned_message, &self.topics_map).await;
                }
                Err(error) => handle_error(error).await,
            };
        }
    }

    ///
    /// Subscribe to a given topic and calls the given function for each message
    ///
    /// # Arguments
    /// * `topic` - The topic to subscribe to
    /// * `handler` - The handler function that will be called for each message for the give `topic`
    ///
    /// # Example
    /// ```
    /// use simple_kafka::{Consumer};
    /// #[derive(Serialize, Deserialize, Debug)]
    ///  struct Data {
    ///     attra_one: String,
    ///     attra_two: i8,
    /// }
    ///
    /// let consumer = Consumer::from("group_id", "localhost:9092");
    /// let handler_1 = | data: Data, metadata: Metadata| async move {
    ///     println!("Handler One ::: data: {:?}, metadata: {:?}", data, metadata);
    /// };
    /// consumer.add("topic_1".to_string(), handler_1);
    /// consumer.subscribe().await;
    /// ```
    pub async fn subscribe_to_topic<H>(&mut self, topic: String, handler: H)
    where
        H: Fn(T, Metadata) -> F,
    {
        self.consumer
            .subscribe(&[topic.as_str()])
            .expect("Can't subscribe to specified topic");

        loop {
            let is_runnig = self.is_runnig.lock().await;
            debug!("Subscriber is running: {:?}", *is_runnig);
            if !(*is_runnig) {
                debug!("Subscriber is stopped");
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }

            match self.consumer.recv().await {
                Ok(message) => {
                    let owned_message = message.detach();
                    single_handle_message(owned_message, &handler).await;
                }
                Err(error) => handle_error(error).await,
            };
        }
    }

    ///
    /// Pause the consumer. Consumer will not be disconnect, will not request new messages
    ///
    pub async fn pause(&self) {
        let mut is_runnig = self.is_runnig.lock().await;
        *is_runnig = false;
    }

    ///
    /// Resume the consumer
    ///
    pub async fn resume(&self) {
        let mut is_runnig = self.is_runnig.lock().await;
        *is_runnig = true;
    }
}

impl<T, F> Consumer<T, F>
where
    T: for<'a> serde::Deserialize<'a>,
    F: Future<Output = ()> + Send + Sync + 'static,
{
    ///
    /// Creates a new Consumer from group id and bootstrap servers
    /// # Arguments
    /// * `group_id` - The group_id of the consumer
    /// * `bootstrap_servers` - The comma separated bootstrap servers
    ///
    /// # Example
    /// ```
    /// use simple_kafka::{Consumer};
    /// let consumer = Consumer::from("group_id", "localhost:9092");
    /// ```
    pub fn from(group_id: &str, bootstrap_servers: &str) -> Self {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .create()
            .expect("Consumer creation failed");
        Consumer {
            consumer,
            topics_map: HashMap::new(),
        }
    }

    ///
    /// Creates a new Consumer from consumer options
    /// # Arguments
    /// * `options` - A ConsumerOptions struct that holds the consumer options
    ///
    /// # Example
    /// ```
    /// use simple_kafka::{Consumer, ConsumerOptiopns};
    ///
    /// let options = ConsumerOptiopns {
    ///     bootstrap_servers: "localhost:9092".to_string(),
    ///     group_id: "group_id".to_string(),
    ///     session_timeout_ms: "6000".to_string(),
    ///     enable_auto_commit: true,
    ///     enable_partition_eof: false,
    /// };
    /// let consumer = Consumer::with_options(options);
    /// ```
    pub fn with_options(options: ConsumerOptiopns) -> Self {
        let enable_partition_eof = match options.enable_partition_eof {
            true => "true",
            false => "false",
        };

        let enable_auto_commit = match options.enable_auto_commit {
            true => "true",
            false => "false",
        };
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", options.group_id.as_str())
            .set("bootstrap.servers", options.bootstrap_servers.as_str())
            .set("enable.partition.eof", enable_partition_eof)
            .set("session.timeout.ms", options.session_timeout_ms.as_str())
            .set("enable.auto.commit", enable_auto_commit)
            .create()
            .expect("Consumer creation failed");
        Consumer {
            consumer,
            topics_map: HashMap::new(),
        }
    }

    /// FIXME: Not ready yet
    /// Add message handles to the cosnumer
    /// Currently only support one handler.
    ///
    /// # Arguments
    /// * `topic` - The topic to subscribe to
    /// * `handler` - The handler function that will be called for each message for the give `topic`
    ///
    /// # Example
    /// ```
    /// use simple_kafka::{Consumer};
    /// #[derive(Serialize, Deserialize, Debug)]
    /// struct Data {
    ///    attra_one: String,
    ///   attra_two: i8,
    /// }
    ///
    /// let consumer = Consumer::from("group_id", "localhost:9092");
    /// let handler_1 = Box::new(| data: Data, metadata: Metadata| async move {
    ///    println!("Handler One ::: data: {:?}, metadata: {:?}", data, metadata);
    /// });
    /// consumer.add("topic_1".to_string(), handler_1);
    /// consumer.subscribe().await;
    /// ```
    fn add(&mut self, topic: String, handler: Box<dyn Fn(T, Metadata) -> F>) {
        self.topics_map.insert(topic, handler);
    }

    /// FIXME: Not ready yet
    /// Subcribes to given set of topics and calls the given function for each message
    ///
    /// # Example
    /// ```
    /// use simple_kafka::{Consumer};
    /// #[derive(Serialize, Deserialize, Debug)]
    ///  struct Data {
    ///     attra_one: String,
    ///     attra_two: i8,
    /// }
    ///
    /// let consumer = Consumer::from("group_id", "localhost:9092");
    /// let handler_1 = Box::new(| data: Data, metadata: Metadata| async move {
    ///     println!("Handler One ::: data: {:?}, metadata: {:?}", data, metadata);
    /// });
    /// consumer.add("topic_1".to_string(), handler_1);
    /// consumer.subscribe().await;
    /// ```
    async fn subscribe(&self) {
        let topics = self
            .topics_map
            .keys()
            .map(|key| key.as_str())
            .collect::<Vec<&str>>();

        self.consumer
            .subscribe(&topics)
            .expect("Can't subscribe to specified topic");
        loop {
            match self.consumer.recv().await {
                Ok(message) => {
                    let owned_message = message.detach();
                    handle_message(owned_message, &self.topics_map).await;
                }
                Err(error) => handle_error(error).await,
            };
        }
    }

    ///
    /// Subscribe to a given topic and calls the given function for each message
    ///
    /// # Arguments
    /// * `topic` - The topic to subscribe to
    /// * `handler` - The handler function that will be called for each message for the give `topic`
    ///
    /// # Example
    /// ```
    /// use simple_kafka::{Consumer};
    /// #[derive(Serialize, Deserialize, Debug)]
    ///  struct Data {
    ///     attra_one: String,
    ///     attra_two: i8,
    /// }
    ///
    /// let mut consumer = Consumer::from("group_id", "localhost:9092");
    /// let handler = consumer.subscribe_to_topic("topic".to_string(), |data: Data, medatad: Metadata| async move {
    ///    info!("data: {:?}, metadata: {:?}", data, medatad);
    /// });
    /// handler.await;
    /// ```
    pub async fn subscribe_to_topic<H>(&mut self, topic: String, handler: H)
    where
        H: Fn(T, Metadata) -> F,
    {
        self.consumer
            .subscribe(&[topic.as_str()])
            .expect("Can't subscribe to specified topic");

        loop {
            match self.consumer.recv().await {
                Ok(message) => {
                    let owned_message = message.detach();
                    single_handle_message(owned_message, &handler).await;
                }
                Err(error) => handle_error(error).await,
            };
        }
    }
}

async fn handle_message<F, T>(
    owned_message: OwnedMessage,
    topics_map: &HashMap<String, impl Fn(T, Metadata) -> F>,
) where
    F: Future<Output = ()> + Send + Sync + 'static,
    T: for<'a> serde::Deserialize<'a>,
{
    let payload = owned_message.payload().unwrap();
    let topic = owned_message.topic().to_string();
    let handler = topics_map.get(topic.as_str()).unwrap();
    let partition = owned_message.partition();
    let offset = owned_message.offset();
    let headers = extract_headers(&owned_message);

    let metadata = Metadata {
        topic,
        partition,
        offset,
        headers,
    };
    let message = String::from_utf8_lossy(payload);
    let message: T = serde_json::from_str(&message).unwrap();
    handler(message, metadata).await;
}

async fn single_handle_message<F, T>(
    owned_message: OwnedMessage,
    handler: &impl Fn(T, Metadata) -> F,
) where
    F: Future<Output = ()> + Send + Sync + 'static,
    T: for<'a> serde::Deserialize<'a>,
{
    let payload = owned_message.payload().unwrap();
    let topic = owned_message.topic().to_string();
    let partition = owned_message.partition();
    let offset = owned_message.offset();
    let headers = extract_headers(&owned_message);

    let metadata = Metadata {
        topic,
        partition,
        offset,
        headers,
    };
    let message = String::from_utf8_lossy(payload);
    let message: T = serde_json::from_str(&message).unwrap();
    handler(message, metadata).await;
}

async fn handle_error(error: KafkaError) {
    error!("Error while receiving message: {:?}", error);
    match error {
        rdkafka::error::KafkaError::Global(code) => match code {
            rdkafka::types::RDKafkaErrorCode::BrokerTransportFailure => {
                error!("Broker transport failure");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
            _ => {
                error!("Error while receiving message: {:?}", error)
            }
        },
        _ => {
            error!("Error while receiving message: {:?}", error);
        }
    }
}

fn extract_headers(owned_message: &rdkafka::message::OwnedMessage) -> HashMap<String, String> {
    let headers = match owned_message.headers() {
        Some(headers) => {
            let mut map = HashMap::new();
            for header in headers.iter() {
                if let Some(value) = header.value {
                    let key = String::from(header.key);
                    if let Ok(value) = String::from_utf8(value.to_vec()) {
                        map.insert(key, value);
                    }
                }
            }
            map
        }
        None => HashMap::new(),
    };
    headers
}

#[cfg(test)]
mod tests {
    use log::info;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[tokio::test]
    async fn create_consumer_test() {
        let mut consumer = Consumer::from("group_id", "localhost:9092");
        let handler = consumer.subscribe_to_topic(
            "topic".to_string(),
            |data: Data, medatad: Metadata| async move {
                info!("data: {:?}, metadata: {:?}", data, medatad);
            },
        );
        handler.await;
    }

    #[tokio::test]
    async fn create_pausable_consumer_test() {
        let mut consumer = PausableConsumer::from("group_id", "localhost:9092");
        let handler_1 = Box::new(|data: Data, metadata: Metadata| async move {
            println!("Handler One ::: data: {:?}, metadata: {:?}", data, metadata);
        });

        consumer.add("topic_1".to_string(), handler_1);
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Data {
        attra_one: String,
        attra_two: i8,
    }
}
