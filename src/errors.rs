use std::{
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    num::ParseIntError,
};

pub struct GameOfLifeInKafkaError {
    err: anyhow::Error,
}

impl Debug for GameOfLifeInKafkaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let anyhow_str = format!("{:?}", self.err).replace("\n", " ");
        f.debug_tuple("").field(&anyhow_str).finish()
    }
}

impl Display for GameOfLifeInKafkaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "({:#})", self.err)
    }
}

impl std::convert::From<kafkang::error::Error> for GameOfLifeInKafkaError {
    fn from(err: kafkang::error::Error) -> GameOfLifeInKafkaError {
        let msg = format!("kafkang error: '{:#}'", err);
        let anyhow_err = anyhow::Error::msg(msg);
        GameOfLifeInKafkaError { err: anyhow_err }
    }
}

impl From<anyhow::Error> for GameOfLifeInKafkaError {
    fn from(err: anyhow::Error) -> GameOfLifeInKafkaError {
        GameOfLifeInKafkaError { err }
    }
}

impl std::convert::From<ParseIntError> for GameOfLifeInKafkaError {
    fn from(err: ParseIntError) -> GameOfLifeInKafkaError {
        let msg = format!("parse int err: '{:#}'", err);
        let anyhow_err = anyhow::Error::msg(msg);
        GameOfLifeInKafkaError { err: anyhow_err }
    }
}

impl std::convert::From<tokio::task::JoinError> for GameOfLifeInKafkaError {
    fn from(err: tokio::task::JoinError) -> GameOfLifeInKafkaError {
        let msg = format!("tokio task join error: {}", err);
        let anyhow_err = anyhow::Error::msg(msg);
        GameOfLifeInKafkaError { err: anyhow_err }
    }
}
