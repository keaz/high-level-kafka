use high_level_kafka::publisher;
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let publisher = publisher::KafkaProducer::from("localhost:9092");
    let data = Data {
        attra_one: "one".to_string(),
        attra_two: 2,
    };
    let message = publisher::Message::new(
        "topic".to_string(),
        HashMap::new(),
        data,
        "some_key".to_string(),
    );
    let _result = publisher.produce(message).await;
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Data {
    attra_one: String,
    attra_two: i8,
}
