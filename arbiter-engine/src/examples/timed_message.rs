#[cfg(test)]

const AGENT_ID: &str = "agent";

use std::time::Duration;

use tokio::time::timeout;

use self::machine::MachineHalt;
use super::*;
use crate::{
    agent::Agent,
    machine::{Behavior, Engine, State, StateMachine},
    messager::To,
    world::World,
};

struct TimedMessage {
    delay: u64,
    receive_data: String,
    send_data: String,
    messager: Option<Messager>,
    count: u64,
    max_count: Option<u64>,
}

impl TimedMessage {
    pub fn new(
        delay: u64,
        receive_data: String,
        send_data: String,
        max_count: Option<u64>,
    ) -> Self {
        Self {
            delay,
            receive_data,
            send_data,
            messager: None,
            count: 0,
            max_count,
        }
    }
}

#[async_trait::async_trait]
impl Behavior<Message> for TimedMessage {
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

    async fn sync(&mut self, messager: Messager, _client: Arc<RevmMiddleware>) {
        trace!("Syncing state for `TimedMessage`.");
        self.messager = Some(messager);
        tokio::time::sleep(std::time::Duration::from_secs(self.delay)).await;
        trace!("Synced state for `TimedMessage`.");
    }

    async fn startup(&mut self) {
        trace!("Starting up `TimedMessage`.");
        tokio::time::sleep(std::time::Duration::from_secs(self.delay)).await;
        trace!("Started up `TimedMessage`.");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn echoer() {
    let mut world = World::new("world");

    let agent = Agent::new(AGENT_ID, &world);
    let behavior = TimedMessage::new(
        1,
        "Hello, world!".to_owned(),
        "Hello, world!".to_owned(),
        Some(2),
    );
    world.add_agent(agent.with_behavior(behavior));

    let messager = world.messager.join_with_id(Some("god".to_owned()));
    let task = world.run();

    let message = Message {
        from: "god".to_owned(),
        to: To::Agent("agent".to_owned()),
        data: "Hello, world!".to_owned(),
    };
    messager.send(message).await;
    task.await;

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

    let agent = Agent::new(AGENT_ID, &world);
    let behavior_ping = TimedMessage::new(1, "pong".to_owned(), "ping".to_owned(), Some(2));
    let behavior_pong = TimedMessage::new(1, "ping".to_owned(), "pong".to_owned(), Some(2));
    world.add_agent(
        agent
            .with_behavior(behavior_ping)
            .with_behavior(behavior_pong),
    );

    let messager = world.messager.join_with_id(Some("god".to_owned()));
    let task = world.run();

    let init_message = Message {
        from: "god".to_owned(),
        to: To::Agent("agent".to_owned()),
        data: "ping".to_owned(),
    };
    messager.send(init_message).await;

    task.await;

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

    let agent_ping = Agent::new("agent_ping", &world);
    let behavior_ping = TimedMessage::new(1, "pong".to_owned(), "ping".to_owned(), Some(2));

    let agent_pong = Agent::new("agent_pong", &world);
    let behavior_pong = TimedMessage::new(1, "ping".to_owned(), "pong".to_owned(), Some(2));

    world.add_agent(agent_ping.with_behavior(behavior_ping));
    world.add_agent(agent_pong.with_behavior(behavior_pong));

    let messager = world.messager.join_with_id(Some("god".to_owned()));
    let task = world.run();

    let init_message = Message {
        from: "god".to_owned(),
        to: To::All,
        data: "ping".to_owned(),
    };

    messager.send(init_message).await;

    task.await;

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
