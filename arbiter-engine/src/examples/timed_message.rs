#[cfg(test)]

const AGENT_ID: &str = "agent";

use std::{pin::Pin, time::Duration};

use ethers::types::BigEndianHash;
use futures_util::Stream;
use serde::*;
use tokio::time::timeout;

use self::machine::MachineHalt;
use super::*;
use crate::{
    agent::Agent,
    machine::{Behavior, Engine, State, StateMachine},
    messager::To,
    world::World,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TimedMessage {
    delay: u64,
    receive_data: String,
    send_data: String,
    messager: Option<Messager>,
    #[serde(default)]
    count: u64,
    max_count: Option<u64>,
    startup_message: Option<String>,
}

impl TimedMessage {
    pub fn new(
        delay: u64,
        receive_data: String,
        send_data: String,
        max_count: Option<u64>,
        startup_message: Option<String>,
    ) -> Self {
        Self {
            delay,
            receive_data,
            send_data,
            messager: None,
            count: 0,
            max_count,
            startup_message,
        }
    }
}

#[async_trait::async_trait]
impl Behavior<Message> for TimedMessage {
    async fn startup(
        &mut self,
        _client: Arc<RevmMiddleware>,
        messager: Messager,
    ) -> Pin<Box<dyn Stream<Item = Message> + Send + Sync>> {
        trace!("Starting up `TimedMessage`.");
        self.messager = Some(messager.clone());
        tokio::time::sleep(std::time::Duration::from_secs(self.delay)).await;
        if let Some(startup_message) = &self.startup_message {
            messager
                .clone()
                .send(Message {
                    from: messager.id.clone().unwrap(),
                    to: To::All,
                    data: startup_message.clone(),
                })
                .await;
        }
        trace!("Started `TimedMessage`.");
        return Box::pin(messager.stream());
    }

    async fn process(&mut self, event: Message) -> Option<MachineHalt> {
        trace!("Processing event.");
        let messager = self.messager.as_ref().unwrap();
        if event.data == self.receive_data {
            trace!("Event matches message. Sending a new message.");
            let message = Message {
                from: messager.id.clone().unwrap(),
                to: To::All,
                data: self.send_data.clone(),
            };
            messager.send(message).await;
            self.count += 1;
        }
        if self.count == self.max_count.unwrap_or(u64::MAX) {
            warn!("Reached max count. Halting behavior.");
            return Some(MachineHalt);
        }

        tokio::time::sleep(std::time::Duration::from_secs(self.delay)).await;
        trace!("Processed event.");
        None
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn echoer() {
    let mut world = World::new("world");

    let agent = Agent::builder(AGENT_ID);
    let behavior = TimedMessage::new(
        1,
        "Hello, world!".to_owned(),
        "Hello, world!".to_owned(),
        Some(2),
        Some("Hello, world!".to_owned()),
    );
    world.add_agent(agent.with_behavior(behavior));
    let messager = world.messager.for_agent("outside_world");

    world.run().await;

    let mut stream = Box::pin(messager.stream());
    let mut idx = 0;

    loop {
        match timeout(Duration::from_secs(1), stream.next()).await {
            Ok(Some(event)) => {
                println!("Event received in outside world: {:?}", event);
                idx += 1;
                if idx == 2 {
                    break;
                }
            }
            _ => {
                panic!("Timeout reached. Test failed.");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ping_pong() {
    let mut world = World::new("world");

    let agent = Agent::builder(AGENT_ID);
    let behavior_ping = TimedMessage::new(
        1,
        "pong".to_owned(),
        "ping".to_owned(),
        Some(2),
        Some("ping".to_owned()),
    );
    let behavior_pong = TimedMessage::new(1, "ping".to_owned(), "pong".to_owned(), Some(2), None);
    world.add_agent(
        agent
            .with_behavior(behavior_ping)
            .with_behavior(behavior_pong),
    );

    let messager = world.messager.for_agent("outside_world");
    world.run().await;

    let mut stream = Box::pin(messager.stream());
    let mut idx = 0;

    loop {
        match timeout(Duration::from_secs(1), stream.next()).await {
            Ok(Some(event)) => {
                println!("Event received in outside world: {:?}", event);
                idx += 1;
                if idx == 4 {
                    break;
                }
            }
            _ => {
                panic!("Timeout reached. Test failed.");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ping_pong_two_agent() {
    let mut world = World::new("world");

    let agent_ping = Agent::builder("agent_ping");
    let agent_pong = Agent::builder("agent_pong");

    let behavior_ping = TimedMessage::new(
        1,
        "pong".to_owned(),
        "ping".to_owned(),
        Some(2),
        Some("ping".to_owned()),
    );
    let behavior_pong = TimedMessage::new(1, "ping".to_owned(), "pong".to_owned(), Some(2), None);

    world.add_agent(agent_ping.with_behavior(behavior_ping));
    world.add_agent(agent_pong.with_behavior(behavior_pong));

    let messager = world.messager.for_agent("outside_world");
    world.run().await;

    let mut stream = Box::pin(messager.stream());
    let mut idx = 0;

    loop {
        match timeout(Duration::from_secs(1), stream.next()).await {
            Ok(Some(event)) => {
                println!("Event received in outside world: {:?}", event);
                idx += 1;
                if idx == 5 {
                    break;
                }
            }
            _ => {
                panic!("Timeout reached. Test failed.");
            }
        }
    }
}
