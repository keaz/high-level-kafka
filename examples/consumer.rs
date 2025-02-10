use high_level_kafka::{consumer::Consumer, KafkaResult};
use log::info;

#[tokio::main]
async fn main() {
    let mut con = Consumer::from("group_id", "localhost:9092").unwrap();
    con.subscribe_to_topic("topic", |result: KafkaResult<Data>| async move {
        if let KafkaResult::Ok(Some((data, metadata))) = result {
            info!("data: {:?}, metadata: {:?}", data, metadata);
        }
    })
    .await;
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Data {
    attra_one: String,
    attra_two: i8,
}
