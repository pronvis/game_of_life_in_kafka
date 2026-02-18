use std::{collections::HashMap, time::Duration, vec};

use anyhow::anyhow;
use game_of_life_in_kafka::{game::ToTopic, Result, SendStartOpt};
use kafkang::{
    client::RequiredAcks,
    producer::{Producer, Record},
};
use log::{debug, error, info};
use structopt::StructOpt;

#[tokio::main]
async fn main() -> Result<()> {
    let opt = SendStartOpt::from_args();
    let opt_clone = opt.clone();
    std::env::set_var("RUST_LOG", opt.rust_log.clone());
    env_logger::init();

    let topics: Vec<String> = (0..opt.game_size.x)
        .flat_map(move |x| (0..opt.game_size.y).map(move |y| (x, y).to_topic()))
        .collect();
    info!("start send_start with config: {:#?}", opt_clone);

    let mut topic_with_msg: HashMap<String, Vec<u8>> =
        topics.into_iter().map(|t| (t, vec![0])).collect();

    topic_with_msg.insert("3-6".to_string(), vec![1]);
    topic_with_msg.insert("4-6".to_string(), vec![1]);
    topic_with_msg.insert("4-5".to_string(), vec![1]);
    topic_with_msg.insert("4-4".to_string(), vec![1]);
    topic_with_msg.insert("5-4".to_string(), vec![1]);
    topic_with_msg.insert("5-3".to_string(), vec![1]);
    topic_with_msg.insert("6-3".to_string(), vec![1]);

    let mut producer =
        Producer::from_hosts(opt.kafka_brokers.iter().map(|a| a.to_owned()).collect())
            .with_ack_timeout(Duration::from_millis(200))
            .with_required_acks(RequiredAcks::One)
            .create()?;

    let send_res = send_initial_state(topic_with_msg, &mut producer);
    let errors: Vec<_> = send_res.into_iter().filter_map(|h| h.err()).collect();
    errors.iter().for_each(|e| error!("{}", e));
    if errors.len() > 0 {
        return Err(anyhow!("some cell initial state has not been sent. stopping").into());
    }

    info!("Initial state has been successfully sent.");
    Ok(())
}

fn send_initial_state(
    topic_with_msg: HashMap<String, Vec<u8>>,
    producer: &mut Producer,
) -> Vec<Result<()>> {
    topic_with_msg
        .into_iter()
        .map(|(topic, data)| {
            let send_res = producer
                .send(&Record::from_value(topic.as_str(), data.as_slice()))
                .map_err(|e| {
                    anyhow!("fail to send init message to topic {}, err: {:#}", topic, e).into()
                });
            info!("send res: {:?}, data: {:?}", send_res, data);
            send_res
        })
        .collect()
}
