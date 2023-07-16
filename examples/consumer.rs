use std::sync::Arc;

use high_level_kafka::consumer::{Consumer, Metadata};
use log::info;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    let consumer = Consumer::from("group_id", "localhost:9092");
    let mut_consumer = Arc::new(Mutex::new(consumer));
    let mut con = mut_consumer.clone().lock_owned().await;
    con.subscribe_to_topic(
        "topic".to_string(),
        |data: Data, medatad: Metadata| async move {
            info!("data: {:?}, metadata: {:?}", data, medatad);
        },
    )
    .await;
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Data {
    attra_one: String,
    attra_two: i8,
}
