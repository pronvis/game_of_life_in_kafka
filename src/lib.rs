pub mod errors;

use errors::KafkaClientError;
use structopt::StructOpt;

pub type Result<T> = std::result::Result<T, KafkaClientError>;
pub type StdResult<T, E> = std::result::Result<T, E>;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "KafkaClientConfig")]
pub struct KafkaClientOpt {
    #[structopt(long, env, default_value = "kafka_client=debug")]
    pub rust_log: String,

    #[structopt(long, env)]
    pub kafka_brokers: Addrs,

    #[structopt(long, env)]
    pub kafka_topic: String,

    #[structopt(long, env)]
    pub kafka_consumer_group: String,

    /// Consumer || Producer
    #[structopt(long, env)]
    pub kafka_client_role: String,
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
