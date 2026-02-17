use std::{time::Duration, u16};

use kafkang::{
    client::{FetchOffset, GroupOffsetStorage, RequiredAcks},
    consumer::Consumer,
    producer::{DefaultPartitioner, Producer, Record},
};
use tokio::task::JoinHandle;

use crate::{game::LifeCell, GameOfLifeInKafkaOpt};
use crate::{game::ToTopic, Result};
use log::{debug, error, warn};

pub struct LifeCellProcessor {
    consumers: Vec<Consumer>,
    producer: Producer<DefaultPartitioner>,
    topic: String,
    pub life_cells_to_read: Vec<String>,
    pub state: bool,
}

impl LifeCellProcessor {
    pub fn new(life_cell: LifeCell, opt: GameOfLifeInKafkaOpt) -> Result<Self> {
        let topics_to_read = (0..9)
            .filter(|&z| z != 4)
            .map(|z| {
                let x = 1 - z % 3;
                let y = 1 - z / 3;
                (life_cell.x as i32 + x, life_cell.y as i32 + y)
            })
            .filter(|&(x, y)| x >= 0 && y >= 0)
            .map(|(x, y)| (x as u16, y as u16))
            .filter(|&(x, y)| x < opt.game_size.x && y < opt.game_size.y)
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

        let producer = Producer::from_hosts(opt.kafka_brokers.0)
            .with_ack_timeout(Duration::from_millis(200))
            .with_required_acks(RequiredAcks::One)
            .create()?;

        return Ok(Self {
            life_cells_to_read: topics_to_read,
            consumers,
            producer,
            topic: life_cell.to_topic(),
            state: false,
        });
    }

    pub fn start(self) -> JoinHandle<()> {
        tokio::task::spawn(async move { self.start_changing_state().await })
    }

    async fn start_changing_state(mut self) {
        loop {
            let messages: Vec<MessageWithMeta> = self
                .consumers
                .iter_mut()
                .flat_map(Self::next_message)
                .flatten()
                .collect();

            if messages.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            if messages.len() != self.consumers.len() {
                warn!(
                    "{} consumed lesser then needed messages: {}/{}",
                    self.topic,
                    messages.len(),
                    self.consumers.len()
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            } else {
                let first_offset = messages[0].offset;
                let offsets: Vec<i64> = messages.iter().map(|m| m.offset).collect();
                if !offsets.iter().all(|&offset| offset == first_offset) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    warn!(
                        "{} consumed messages have different offsets: {:?}",
                        self.topic, offsets
                    );
                    continue;
                }

                let neighbors_alive = messages.iter().filter(|m| m.value).count();
                let commit_result_count: usize = self
                    .consumers
                    .iter_mut()
                    .zip(messages)
                    .flat_map(|(consumer, msg)| Self::consume_message(consumer, &msg))
                    .count();
                if commit_result_count != self.consumers.len() {
                    error!("{} kafka: commit only part of consumers. moved to inconsistent state. stopping", self.topic);
                    return;
                }

                let new_state = self.calc_new_state(neighbors_alive);
                self.state = new_state;

                let flag: u8 = if self.state { 1 } else { 0 };
                let mut table = [0u8; 1];
                table[0] = flag;

                let produce_res = self
                    .producer
                    .send(&Record::from_value(self.topic.as_str(), table.as_slice()));

                if let Err(err) = produce_res {
                    error!(
                        "kafka: fail to send record about cell #{} new state. error: {:#}. stopping",
                        self.topic, err
                    );
                    return;
                }
                debug!("Successfully sent new state to topic: {}", self.topic);

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    fn next_message(consumer: &mut Consumer) -> Result<Option<MessageWithMeta>> {
        let mss = consumer.poll()?;
        let Some(next_messages) = mss.into_iter().next() else {
            return Ok(None);
        };
        let Some(next_message) = next_messages.messages().iter().next() else {
            return Ok(None);
        };

        let topic = next_messages.topic().to_owned();
        let partition = next_messages.partition();

        //1 - cell alive; 0 - cell dead
        let cell_vlaue = next_message.value[0] == 1;

        return Ok(Some(MessageWithMeta::new(
            cell_vlaue,
            topic,
            partition,
            next_message.offset,
        )));
    }

    fn consume_message(consumer: &mut Consumer, msg: &MessageWithMeta) -> Result<()> {
        consumer.consume_message(msg.topic.as_str(), msg.partition, msg.offset)?;
        consumer.commit_consumed().map_err(|e| e.into())
    }

    fn calc_new_state(&self, neighbors_alize: usize) -> bool {
        if !self.state && neighbors_alize == 3 {
            return true;
        } else if self.state && (neighbors_alize == 3 || neighbors_alize == 2) {
            return true;
        }
        return false;
    }
}

struct MessageWithMeta {
    pub value: bool,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

impl MessageWithMeta {
    pub fn new(value: bool, topic: String, partition: i32, offset: i64) -> Self {
        Self {
            value,
            topic,
            partition,
            offset,
        }
    }
}
// if cell is dead & have 3 alive cells around -> it become alive
// if cell alive & have 2 || 3 alive cells around -> it continue live, otherwise become dead
