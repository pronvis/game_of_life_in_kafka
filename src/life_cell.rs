use anyhow::anyhow;
use game_of_life_in_kafka::game::life_cell::LifeCellProcessor;
use game_of_life_in_kafka::game::LifeCell;
use game_of_life_in_kafka::CellsRange;
use game_of_life_in_kafka::GameOfLifeInKafkaOpt;
use game_of_life_in_kafka::GameSize;
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

    info!("start life_cell with config: {:#?}", opt_clone);

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
    get_list_of_coordinates(&opt.cells, &opt.game_size)
        .into_iter()
        .map(|(x, y)| {
            let opt_clone = opt.clone();
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

fn get_list_of_coordinates(cells_range: &CellsRange, game_size: &GameSize) -> Vec<(u16, u16)> {
    (cells_range.from..cells_range.to)
        .map(|i| {
            let x = i % game_size.x;
            let y = i / game_size.y;
            (x, y)
        })
        .collect()
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn list_of_cells() {
        let cells_range = CellsRange { from: 0, to: 4 };
        let game_size = GameSize { x: 10, y: 10 };
        let res = get_list_of_coordinates(&cells_range, &game_size);
        assert_eq!(res, vec![(0, 0), (1, 0), (2, 0), (3, 0),]);
        println!("{:?}", res);

        let cells_range = CellsRange { from: 10, to: 14 };
        let res = get_list_of_coordinates(&cells_range, &game_size);
        assert_eq!(res, vec![(0, 1), (1, 1), (2, 1), (3, 1),]);
        println!("{:?}", res);

        let cells_range = CellsRange { from: 58, to: 62 };
        let res = get_list_of_coordinates(&cells_range, &game_size);
        assert_eq!(res, vec![(8, 5), (9, 5), (0, 6), (1, 6),]);
        println!("{:?}", res);

        let cells_range = CellsRange { from: 98, to: 100 };
        let res = get_list_of_coordinates(&cells_range, &game_size);
        assert_eq!(res, vec![(8, 9), (9, 9)]);
        println!("{:?}", res);
    }
}
