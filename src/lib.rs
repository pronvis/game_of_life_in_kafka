pub mod errors;
pub mod game;

use anyhow::anyhow;
use errors::GameOfLifeInKafkaError;
use structopt::StructOpt;

pub type Result<T> = std::result::Result<T, GameOfLifeInKafkaError>;
pub type StdResult<T, E> = std::result::Result<T, E>;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "GameOfLifeInKafkaConfig")]
pub struct GameOfLifeInKafkaOpt {
    #[structopt(long, env, default_value = "life_cell=debug")]
    pub rust_log: String,

    #[structopt(long, env)]
    pub kafka_brokers: Addrs,

    #[structopt(long, env, parse(try_from_str = parse_cells_range))]
    pub cells: CellsRange,

    #[structopt(long, env, parse(try_from_str = parse_game_size))]
    pub game_size: GameSize,
}

#[derive(Debug, Clone)]
pub struct GameSize {
    pub x: u16,
    pub y: u16,
}

#[derive(Debug, Clone)]
pub struct CellsRange {
    pub from: u16,
    pub to: u16,
}

fn parse_game_size(src: &str) -> Result<GameSize> {
    let mut splitted = src.split('x');
    let x = splitted
        .next()
        .ok_or(anyhow!("fail to parse game size: {}", src))?
        .parse()?;
    let y = splitted
        .next()
        .ok_or(anyhow!("fail to parse game size: {}", src))?
        .parse()?;

    Ok(GameSize { x, y })
}

fn parse_cells_range(src: &str) -> Result<CellsRange> {
    let mut splitted = src.split('-');
    let from = splitted
        .next()
        .ok_or(anyhow!("fail to parse cells range: {}", src))?
        .parse()?;
    let to = splitted
        .next()
        .ok_or(anyhow!("fail to parse cells range: {}", src))?
        .parse()?;

    Ok(CellsRange { from, to })
}

#[derive(Debug, Clone)]
pub struct Addrs(Vec<String>);

impl std::str::FromStr for Addrs {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        Ok(Addrs(
            s.split(',').map(|x| x.to_owned()).collect::<Vec<_>>(),
        ))
    }
}

impl std::ops::Deref for Addrs {
    type Target = Vec<String>;

    fn deref(&self) -> &Vec<String> {
        &self.0
    }
}

impl std::fmt::Display for Addrs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.join(", "))
    }
}
