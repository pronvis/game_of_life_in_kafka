use std::{time::Duration, vec};

use kafkang::{
    client::{FetchOffset, GroupOffsetStorage},
    consumer::Consumer,
    producer::{DefaultPartitioner, Producer},
};

use crate::{game::LifeCell, KafkaClientOpt};
use crate::{game::ToTopic, Result};

pub struct LifeCellProcessor {
    consumers: Vec<Consumer>,
    // producer: Producer<DefaultPartitioner>,
    pub life_cells_to_read: Vec<String>,
}

impl LifeCellProcessor {
    pub fn new(life_cell: LifeCell, opt: KafkaClientOpt) -> Result<Self> {
        let topics_to_read = (0..9)
            .filter(|&z| z != 4)
            .map(|z| {
                let x = 1 - z % 3;
                let y = 1 - z / 3;
                (life_cell.x as i32 + x, life_cell.y as i32 + y)
            })
            .filter(|&(x, y)| x >= 0 && y >= 0)
            .map(|(x, y)| (x, y).to_topic())
            .collect::<Vec<String>>();

        let consumers = topics_to_read
            .iter()
            .map(|topic| {
                Consumer::from_hosts(opt.kafka_brokers.0.clone())
                    .with_topic(topic.clone())
                    .with_group(life_cell.to_topic())
                    .with_fallback_offset(FetchOffset::Earliest)
                    .with_offset_storage(Some(GroupOffsetStorage::Kafka))
                    .with_fetch_max_wait_time(Duration::from_secs(1))
                    .with_fetch_min_bytes(1)
                    .with_fetch_max_bytes_per_partition(100_000)
                    .with_retry_max_bytes_limit(1_000_000)
                    .create()
                    .map_err(|e| e.into())
            })
            .collect::<Result<Vec<Consumer>>>()?;

        return Ok(Self {
            life_cells_to_read: topics_to_read,
            consumers,
        });
    }
}
