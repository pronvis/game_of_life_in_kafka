use anyhow::anyhow;
use game_of_life_in_kafka::game::life_cell::LifeCellProcessor;
use game_of_life_in_kafka::game::LifeCell;
use game_of_life_in_kafka::GameOfLifeInKafkaOpt;
use game_of_life_in_kafka::Result;

use futures::future;
use log::{debug, error, info};
use structopt::StructOpt;
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() -> Result<()> {
    let opt = GameOfLifeInKafkaOpt::from_args();
    let opt_clone = opt.clone();
    std::env::set_var("RUST_LOG", opt.rust_log.clone());
    env_logger::init();

    debug!("start life_cell with config: {:#?}", opt_clone);

    let handlers = create_cell_processors(opt);

    let (handlers, errors): (Vec<_>, Vec<_>) = handlers.into_iter().partition(|h| h.is_ok());
    let errors: Vec<_> = errors.into_iter().filter_map(|h| h.err()).collect();
    errors.iter().for_each(|e| error!("{}", e));
    if errors.len() > 0 {
        return Err(anyhow!("some life cell was not created. stopping").into());
    }

    let handlers: Vec<_> = handlers.into_iter().filter_map(|h| h.ok()).collect();

    let _ = future::try_join_all(handlers).await?;
    Ok(())
}

fn create_cell_processors(opt: GameOfLifeInKafkaOpt) -> Vec<Result<JoinHandle<()>>> {
    (opt.cells.from..opt.cells.to)
        .map(|i| {
            let opt_clone = opt.clone();
            let x = i % opt.game_size.x;
            let y = i % opt.game_size.y;
            let lfp = LifeCellProcessor::new(LifeCell::new(x, y), opt_clone);
            match lfp {
                Err(err) => Err(anyhow!(
                    "fail to crate LifeCellProcessor[{}, {}], reason: {:#}",
                    x,
                    y,
                    err
                )
                .into()),
                Ok(lfp) => {
                    debug!("[{}:{}]: {:?}", x, y, lfp.life_cells_to_read);
                    Ok(lfp.start())
                }
            }
        })
        .collect()
}
