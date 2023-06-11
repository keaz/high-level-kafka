# Simple Kafka client library for Rust

A ldap client library that wraps [rdkafka](https://github.com/fede1024/rust-rdkafka) to make it easy to use. 
Message parsing is done with [serde](https://github.com/serde-rs/serde).

### Status of the project
Currently this is in early beta stage. Library only support use asynchronously. 


## Usage
```
cargo add simple-kafka
```


## Examples

### Producer
```rust
#[tokio::main]
async fn main() -> Result<()>{
    let publisher = KafkaProducer::from("localhost:9092");
    let data  = Data {
        attra_one: "123".to_string(),
        attra_two: 12,
    };

    let mut headers = HashMap::new();
    headers.insert("header_one".to_string(), "value_one".to_string());
    headers.insert("header_two".to_string(), "value_two".to_string());

    let data = Message::new("topic".to_string(), headers, data, "key".to_string());
    let result = publisher.produce(data).await;

    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct Data {
    attra_one: String,
    attra_two: i8,
}
```

### Consumer
```rust
#[tokio::main]
async fn main() -> Result<()>{

    let mut consumer = Consumer::from("group_id", "localhost:9092");
    let handler_1 = Box::new(| data: Data, metadata: Metadata| async move {
        println!("Handler One ::: data: {:?}, metadata: {:?}", data, metadata);
    });
    
    consumer.add("topic_1".to_string(), handler_1);
    consumer.subscribe().await;
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct Data {
    attra_one: String,
    attra_two: i8,
}
```

### PausableConsumer
```rust
#[tokio::main]
async fn main() -> Result<()>{

    let mut consumer = PausableConsumer::from("group_id", "localhost:9092");
    let handler_1 = Box::new(| data: Data, metadata: Metadata| async move {
        println!("Handler One ::: data: {:?}, metadata: {:?}", data, metadata);
    });
    
    consumer.add("topic_1".to_string(), handler_1);

    
    consumer.subscribe().await;
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct Data {
    attra_one: String,
    attra_two: i8,
}
```