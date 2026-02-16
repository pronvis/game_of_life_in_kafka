use std::time::Duration;

use kafka_client::KafkaClientOpt;
use kafkang::{
    client::{FetchOffset, GroupOffsetStorage, RequiredAcks},
    consumer::Consumer,
    error::Error as KafkaError,
    producer::{Producer, Record},
};
use log::{debug, info};
use structopt::StructOpt;

fn main() {
    let opt = KafkaClientOpt::from_args();
    let opt_clone = opt.clone();
    std::env::set_var("RUST_LOG", opt.rust_log);
    env_logger::init();

    debug!("start kafka_client with config: {:#?}", opt_clone);

    //     let broker_0 = "localhost:9092".to_owned();
    //     let broker_1 = "localhost:9093".to_owned();
    //     let broker_2 = "localhost:9094".to_owned();
    //     let brokers = vec![broker_0, broker_1, broker_2];
    //     let topic = "test".to_owned();
    //     let group = "g4".to_owned();

    //     if let Err(e) = consume_messages(group, topic, brokers) {
    //         println!("Failed consuming messages: {}", e);
    //     }

    // for i in 0..100 {
    //     let message = format!("hello, kafka {}", i);
    //     if let Err(e) = produce_messages(topic.as_str(), brokers.clone(), message.as_bytes()) {
    //         println!("Failed producing messages: {}", e);
    //     }
    // }
}

fn consume_messages(group: String, topic: String, brokers: Vec<String>) -> Result<(), KafkaError> {
    let mut con = Consumer::from_hosts(brokers)
        .with_topic(topic)
        .with_group(group)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(Some(GroupOffsetStorage::Kafka))
        .with_fetch_max_wait_time(Duration::from_secs(1))
        .with_fetch_min_bytes(1)
        .with_fetch_max_bytes_per_partition(100_000)
        .with_retry_max_bytes_limit(1_000_000)
        .create()?;

    println!("consumer connection created");

    loop {
        let mss = con.poll()?;
        if mss.is_empty() {
            println!("No messages available right now.");
            return Ok(());
        }

        for ms in mss.iter() {
            for m in ms.messages() {
                println!(
                    "{}:{}@{}: {:?}",
                    ms.topic(),
                    ms.partition(),
                    m.offset,
                    str::from_utf8(m.value).unwrap()
                );
            }
            let _ = con.consume_messageset(&ms);
        }
        con.commit_consumed()?;
    }
}

fn produce_messages(topic: &str, brokers: Vec<String>, message: &[u8]) -> Result<(), KafkaError> {
    let mut producer = Producer::from_hosts(brokers)
        // ~ give the brokers one second time to ack the message
        .with_ack_timeout(Duration::from_secs(1))
        // ~ require only one broker to ack the message
        .with_required_acks(RequiredAcks::One)
        // ~ build the producer with the above settings
        .create()?;

    // ~ now send a single message.  this is a synchronous/blocking
    // operation.

    // ~ we're sending 'data' as a 'value'. there will be no key
    // associated with the sent message.

    // ~ we leave the partition "unspecified" - this is a negative
    // partition - which causes the producer to find out one on its
    // own using its underlying partitioner.

    producer.send(&Record::from_value(&topic, message))?;
    Ok(())
}
