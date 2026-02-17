use std::time::Duration;

use game_of_life_in_kafka::game::life_cell::LifeCellProcessor;
use game_of_life_in_kafka::game::LifeCell;
use game_of_life_in_kafka::GameOfLifeInKafkaOpt;
use kafkang::{
    client::{FetchOffset, GroupOffsetStorage, RequiredAcks},
    consumer::Consumer,
    error::Error as KafkaError,
    producer::{Producer, Record},
};
use log::{debug, error, info};
use structopt::StructOpt;

#[tokio::main]
async fn main() {
    let opt = GameOfLifeInKafkaOpt::from_args();
    let opt_clone = opt.clone();
    std::env::set_var("RUST_LOG", opt.rust_log.clone());
    env_logger::init();

    debug!("start kafka_client with config: {:#?}", opt_clone);

    let x = tokio::task::spawn(async move {
        for i in 0..10 {
            let opt_clone = opt.clone();
            let x = i % 3;
            let y = i / 3;
            let lfp = LifeCellProcessor::new(LifeCell::new(x, y), opt_clone);
            if let Err(err) = lfp {
                error!("fail to crate LifeCellProcessor, reason: {:#}", err);
                return;
            }
            debug!("[{}:{}]: {:?}", x, y, lfp.unwrap().life_cells_to_read);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    x.await.unwrap();
}
