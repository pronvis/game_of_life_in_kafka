use std::{collections::HashMap, mem::offset_of, str::FromStr, time::Duration, usize};

use anyhow::anyhow;
use game_of_life_in_kafka::{
    game::{self, ToTopic},
    GameSize, Result, SendStartOpt,
};
use kafkang::{
    client::{FetchOffset, GroupOffsetStorage},
    consumer::Consumer,
    producer::Record,
};
use log::{debug, error, info, trace, warn};
use structopt::StructOpt;
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() -> Result<()> {
    let opt = SendStartOpt::from_args();
    let opt_clone = opt.clone();
    std::env::set_var("RUST_LOG", opt.rust_log.clone());
    env_logger::init();

    info!("start frontend with config: {:#?}", opt_clone);

    let topics: Vec<(String, Coord)> = (0..opt.game_size.y)
        .flat_map(move |y| {
            (0..opt.game_size.x).map(move |x| {
                (
                    (y, x).to_topic(),
                    Coord {
                        x: x as usize,
                        y: y as usize,
                    },
                )
            })
        })
        .collect();

    let consumers = topics
        .into_iter()
        .map(|(topic, coord)| {
            Consumer::from_hosts(opt.kafka_brokers.iter().map(|a| a.to_owned()).collect())
                .with_topic(topic.clone())
                .with_group("frontend".to_owned())
                .with_fallback_offset(FetchOffset::Earliest)
                .with_offset_storage(Some(GroupOffsetStorage::Kafka))
                .with_fetch_max_wait_time(Duration::from_secs(1))
                .with_fetch_min_bytes(1)
                .with_fetch_max_bytes_per_partition(100_000)
                .with_retry_max_bytes_limit(1_000_000)
                .create()
                .map(|c| (coord, c))
                .map_err(|e| e.into())
        })
        .collect::<Result<HashMap<Coord, Consumer>>>()?;

    let frontend = GameOfLifeFrontend::new(consumers, &opt);

    let _ = frontend.start().await?;

    Ok(())
}

struct MessageWithMeta {
    pub value: bool,
    pub offset: i64,
}

impl MessageWithMeta {
    pub fn new(value: bool, offset: i64) -> Self {
        Self { value, offset }
    }
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
struct Coord {
    x: usize,
    y: usize,
}

impl<T> From<T> for Coord
where
    T: Into<String>,
{
    fn from(value: T) -> Self {
        let splitted = value
            .into()
            .split('-')
            .map(|s| s.parse().unwrap())
            .collect::<Vec<usize>>();
        let y = splitted[0];
        let x = splitted[1];
        Coord { x, y }
    }
}

struct GameOfLifeFrontend {
    msg_buff: HashMap<Coord, Vec<MessageWithMeta>>,
    consumers: HashMap<Coord, Consumer>,
    game_size: GameSize,
}

impl GameOfLifeFrontend {
    pub fn new(consumers: HashMap<Coord, Consumer>, opt: &SendStartOpt) -> Self {
        Self {
            msg_buff: HashMap::default(),
            consumers,
            game_size: opt.game_size.clone(),
        }
    }

    pub fn start(self) -> JoinHandle<()> {
        tokio::task::spawn(async move { self.start_changing_state().await })
    }

    fn need_to_read_from_kafka(&mut self) -> Option<HashMap<&Coord, &mut Consumer>> {
        let map_with_min_offset: HashMap<_, _> = self
            .msg_buff
            .iter()
            .flat_map(|(coord, msgs)| {
                let min_offset = msgs.iter().min_by_key(|msg| msg.offset);
                min_offset.map(|mo| (coord, mo.offset))
            })
            .collect();
        let min_offset = map_with_min_offset.iter().next().map(|(_, offset)| *offset);

        if let None = min_offset {
            return Some(self.consumers.iter_mut().collect());
        }
        let min_offset = min_offset?;

        let need_msgs = self.game_size.x * self.game_size.y;
        let have_msgs: HashMap<_, _> = map_with_min_offset
            .iter()
            .filter(|(_, offset)| **offset == min_offset)
            .collect();
        if (have_msgs.len() as u16) < need_msgs {
            let need_to_read: HashMap<_, _> = self
                .consumers
                .iter_mut()
                .filter(|(coord, _)| !have_msgs.contains_key(coord))
                .collect();

            return Some(need_to_read);
        }
        return None;
    }

    async fn start_changing_state(mut self) {
        loop {
            let messages_map: Option<HashMap<Coord, Vec<MessageWithMeta>>> =
                self.need_to_read_from_kafka().map(|mut consumers| {
                    consumers
                        .iter_mut()
                        .flat_map(|(_, consumer)| Self::next_message(consumer))
                        .flatten()
                        .collect()
                });
            if let Some(messages_map) = messages_map {
                for (key, msgs) in messages_map.into_iter() {
                    if let Some(buffered_msgs) = self.msg_buff.get_mut(&key) {
                        buffered_msgs.extend(msgs);
                    } else {
                        self.msg_buff.insert(key, msgs);
                    }
                }
            }

            let messages_to_process: HashMap<&Coord, &MessageWithMeta> = self
                .msg_buff
                .iter()
                .map(|(coord, msgs)| (coord, msgs.iter().min_by_key(|m| m.offset)))
                .filter(|(_, msg)| msg.is_some())
                .map(|(coord, msg)| (coord, msg.unwrap()))
                .collect();

            if messages_to_process.is_empty() {
                let msgs_in_buff = self
                    .msg_buff
                    .iter()
                    .map(|(coord, msgs)| format!("{:?}: {} messages", coord, msgs.len()))
                    .collect::<Vec<_>>();
                debug!("empty messages. buff_state: {:?}", msgs_in_buff);
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            if messages_to_process.len() != self.consumers.len() {
                warn!(
                    "consumed lesser then needed messages: {}/{}",
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
                        "consumed messages have different offsets: {:?}",
                        messages_to_process
                            .iter()
                            .map(|(_, msg)| msg.offset)
                            .collect::<Vec<_>>()
                    );
                    continue;
                }

                Self::output_game_state(messages_to_process, &self.game_size, first_offset);

                self.msg_buff.iter_mut().for_each(|(_, msgs)| {
                    if let Some((index, _)) =
                        msgs.iter().enumerate().min_by_key(|(_, msg)| msg.offset)
                    {
                        msgs.remove(index);
                    }
                });

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    fn output_game_state(
        messages: HashMap<&Coord, &MessageWithMeta>,
        game_size: &GameSize,
        offset: i64,
    ) {
        println!("-------- {} --------", offset);
        let mut str_builder = String::with_capacity((game_size.x * game_size.y * 2) as usize);
        let mut game_state = vec![vec![false; game_size.x as usize]; game_size.y as usize];
        messages.iter().for_each(|(coord, msg)| {
            game_state[coord.y][coord.x] = msg.value;
        });
        for y in game_state.iter() {
            for x in y.iter() {
                let ch = if *x { '*' } else { '.' };
                str_builder.push(ch);
            }
            str_builder.push_str("\r\n");
        }
        println!("{}", str_builder);
    }

    fn next_message(consumer: &mut Consumer) -> Result<Option<(Coord, Vec<MessageWithMeta>)>> {
        let mss = consumer.poll()?;
        let Some(next_messages) = mss.into_iter().next() else {
            return Ok(None);
        };
        trace!("next_messages len: {}", next_messages.messages().len());
        let topic = next_messages.topic().to_owned();
        let parsed_messages: Vec<_> = next_messages
            .messages()
            .iter()
            .map(|msg| {
                //1 - cell alive; 0 - cell dead
                MessageWithMeta::new(msg.value[0] == 1, msg.offset)
            })
            .collect();
        return Ok(Some((topic.into(), parsed_messages)));
    }
}
