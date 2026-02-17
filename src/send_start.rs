use game_of_life_in_kafka::GameOfLifeInKafkaOpt;
use log::{debug, info};
use structopt::StructOpt;

#[tokio::main]
async fn main() {
    let opt = GameOfLifeInKafkaOpt::from_args();
    let opt_clone = opt.clone();
    std::env::set_var("RUST_LOG", opt.rust_log.clone());
    env_logger::init();

    debug!("start send_start with config: {:#?}", opt_clone);
}
