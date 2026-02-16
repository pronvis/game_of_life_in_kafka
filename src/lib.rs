pub mod errors;
pub mod game;

use errors::KafkaClientError;
use structopt::{clap::arg_enum, StructOpt};

pub type Result<T> = std::result::Result<T, KafkaClientError>;
pub type StdResult<T, E> = std::result::Result<T, E>;

arg_enum! {
    #[derive(Debug, Clone)]
    enum ClientRole {
        Consumer,
        Producer
    }
}

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

    #[structopt(long, env, possible_values = &ClientRole::variants(), case_insensitive = true)]
    pub kafka_client_role: ClientRole,
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
