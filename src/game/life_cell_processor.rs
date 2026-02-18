use std::collections::HashMap;
use std::sync::Arc;
use std::{time::Duration, u16};

use kafkang::consumer;
use kafkang::{
    client::{FetchOffset, GroupOffsetStorage, RequiredAcks},
    consumer::Consumer,
    producer::{DefaultPartitioner, Producer, Record},
};
use tokio::task::JoinHandle;

use crate::{game::LifeCell, GameOfLifeInKafkaOpt};
use crate::{game::ToTopic, Result};
use log::{debug, error, info, warn};

pub struct LifeCellProcessor {
    consumers: HashMap<String, Consumer>,
    producer: Producer<DefaultPartitioner>,
    topic: String,
    msg_buff: HashMap<String, Vec<MessageWithMeta>>,
    pub state: bool,
}

impl LifeCellProcessor {
    pub fn topics(&self) -> Vec<&String> {
        self.consumers.iter().map(|(topic, _)| topic).collect()
    }

    pub fn new(life_cell: LifeCell, opt: Arc<GameOfLifeInKafkaOpt>) -> Result<Self> {
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
            .into_iter()
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
                    .map(|c| (topic, c))
                    .map_err(|e| e.into())
            })
            .collect::<Result<HashMap<String, Consumer>>>()?;

        let producer = Producer::from_hosts(opt.kafka_brokers.0.clone())
            .with_ack_timeout(Duration::from_millis(200))
            .with_required_acks(RequiredAcks::One)
            .create()?;

        return Ok(Self {
            consumers,
            producer,
            msg_buff: HashMap::default(),
            topic: life_cell.to_topic(),
            state: false,
        });
    }

    pub fn start(self) -> JoinHandle<()> {
        tokio::task::spawn(async move { self.start_changing_state().await })
    }

    async fn start_changing_state(mut self) {
        loop {
            let messages_map: HashMap<String, Vec<MessageWithMeta>> = self
                .consumers
                .iter_mut()
                .flat_map(|(_, consumer)| Self::next_message(consumer))
                .flatten()
                .collect();

            for (key, msgs) in messages_map.into_iter() {
                if let Some(buffered_msgs) = self.msg_buff.get_mut(&key) {
                    buffered_msgs.extend(msgs);
                } else {
                    self.msg_buff.insert(key, msgs);
                }
            }

            let messages_to_process: HashMap<&String, &MessageWithMeta> = self
                .msg_buff
                .iter()
                .map(|(topic, msgs)| (topic, msgs.iter().min_by_key(|m| m.offset)))
                .filter(|(_, msg)| msg.is_some())
                .map(|(topic, msg)| (topic, msg.unwrap()))
                .collect();

            if messages_to_process.is_empty() {
                let msgs_in_buff = self
                    .msg_buff
                    .iter()
                    .map(|(topic, msgs)| format!("{}: {} messages", topic, msgs.len()))
                    .collect::<Vec<_>>();
                debug!("empty messages. buff_state: {:?}", msgs_in_buff);
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            if messages_to_process.len() != self.consumers.len() {
                warn!(
                    "{} consumed lesser then needed messages: {}/{}",
                    self.topic,
                    messages_to_process.len(),
                    self.consumers.len()
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            } else {
                let first_offset = messages_to_process
                    .iter()
                    .map(|(_, msg)| msg.offset)
                    .next()
                    .unwrap_or(-1);
                let all_same_offset: bool = messages_to_process
                    .iter()
                    .all(|(_, msg)| msg.offset == first_offset);

                if !all_same_offset {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    warn!(
                        "{} consumed messages have different offsets: {:?}",
                        self.topic,
                        messages_to_process
                            .iter()
                            .map(|(_, msg)| msg.offset)
                            .collect::<Vec<_>>()
                    );
                    continue;
                }

                let neighbors_alive = messages_to_process
                    .iter()
                    .filter(|(_, msg)| msg.value)
                    .count();
                let commit_result_count: usize = self
                    .consumers
                    .iter_mut()
                    .flat_map(|(topic, consumer)| {
                        let msg = messages_to_process.get(topic).unwrap();
                        Self::consume_message(consumer, topic, *msg)
                    })
                    .count();
                if commit_result_count != self.consumers.len() {
                    error!("{} kafka: commit only part of consumers. moved to inconsistent state. stopping", self.topic);
                    return;
                }
                self.msg_buff.iter_mut().for_each(|(_, msgs)| {
                    if let Some((index, _)) =
                        msgs.iter().enumerate().min_by_key(|(_, msg)| msg.offset)
                    {
                        msgs.remove(index);
                    }
                });

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

    fn next_message(consumer: &mut Consumer) -> Result<Option<(String, Vec<MessageWithMeta>)>> {
        let mss = consumer.poll()?;
        let Some(next_messages) = mss.into_iter().next() else {
            return Ok(None);
        };
        debug!("next_messages len: {}", next_messages.messages().len());
        let topic = next_messages.topic().to_owned();
        let partition = next_messages.partition();
        let parsed_messages: Vec<_> = next_messages
            .messages()
            .iter()
            .map(|msg| {
                //1 - cell alive; 0 - cell dead
                MessageWithMeta::new(msg.value[0] == 1, partition, msg.offset)
            })
            .collect();
        return Ok(Some((topic, parsed_messages)));
    }

    fn consume_message(
        consumer: &mut Consumer,
        topic: &String,
        msg: &MessageWithMeta,
    ) -> Result<()> {
        consumer.consume_message(topic, msg.partition, msg.offset)?;
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
    pub partition: i32,
    pub offset: i64,
}

impl MessageWithMeta {
    pub fn new(value: bool, partition: i32, offset: i64) -> Self {
        Self {
            value,
            partition,
            offset,
        }
    }
}
// if cell is dead & have 3 alive cells around -> it become alive
// if cell alive & have 2 || 3 alive cells around -> it continue live, otherwise become dead
