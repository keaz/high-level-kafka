use high_level_kafka::consumer::{Consumer, Metadata};
use log::info;

#[tokio::main]
async fn main() {
    let mut con = Consumer::from("group_id", "localhost:9092").unwrap();
    con.subscribe_to_topic("topic", |data: Data, medatad: Metadata| async move {
        info!("data: {:?}, metadata: {:?}", data, medatad);
    })
    .await;
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Data {
    attra_one: String,
    attra_two: i8,
}
