use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

pub struct KafkaClientError {
    err: anyhow::Error,
}

impl Debug for KafkaClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let anyhow_str = format!("{:?}", self.err).replace("\n", " ");
        f.debug_tuple("").field(&anyhow_str).finish()
    }
}

impl Display for KafkaClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "({:#})", self.err)
    }
}

impl std::convert::From<kafkang::error::Error> for KafkaClientError {
    fn from(err: kafkang::error::Error) -> KafkaClientError {
        let msg = format!("kafkang error: '{:#}'", err);
        let anyhow_err = anyhow::Error::msg(msg);
        KafkaClientError { err: anyhow_err }
    }
}
