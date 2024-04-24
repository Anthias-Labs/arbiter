//! The [`StateMachine`] trait, [`Behavior`] trait, and the [`Engine`] that runs
//! [`Behavior`]s.

use std::pin::Pin;

use anyhow::Result;
use arbiter_core::middleware::ArbiterMiddleware;
use futures_util::{Stream, StreamExt};
use tokio::task::JoinHandle;
use tracing::error;

use super::*;

/// A type alias for a pinned, boxed stream of events.
///
/// This stream is capable of handling items of any type that implements the
/// `Stream` trait, and it is both sendable across threads and synchronizable
/// between threads.
///
/// # Type Parameters
///
/// * `E`: The type of the items in the stream.
pub type EventStream<E> = Pin<Box<dyn Stream<Item = E> + Send + Sync>>;

/// The instructions that can be sent to a [`StateMachine`].
#[derive(Clone, Debug)]
pub enum MachineInstruction {
    /// Used to make a [`StateMachine`] start up.
    Start(Arc<ArbiterMiddleware>, Messager),

    /// Used to make a [`StateMachine`] process events.
    /// This will offload the process into a task that can be halted by sending
    /// a [`ControlFlow::Halt`] message from the [`Messager`]. For our purposes,
    /// the [`crate::world::World`] will handle this.
    Process,
}

/// The message that is used in a [`StateMachine`] to continue or halt its
/// processing.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ControlFlow {
    /// Used to halt the processing of a [`StateMachine`].
    Halt,

    /// Used to continue on the processing of a [`StateMachine`].
    Continue,
}

/// The [`State`] trait can be used on a [`Behavior`] to allow for the same
/// underlying struct to exist in both a `startup` and `processing` state.
pub trait State {
    /// The type of data that the state will hold.
    type Data;
}

// NOTE: `async_trait::async_trait` is used throughout to make the trait object
// safe even though rust >=1.75 has async trait stabilized

/// The [`Behavior`] trait is the lowest level functionality that will be used
/// by a [`StateMachine`]. This constitutes what each state transition will do.
#[async_trait::async_trait]
pub trait Behavior<E>
where
    E: Send + 'static,
{
    /// The processor that will be used to process events.
    type Processor: Processor<E> + Send;

    /// Starts up the behavior and returns a processor optionally.
    async fn startup(
        self,
        client: Arc<ArbiterMiddleware>,
        messager: Messager,
    ) -> Result<Self::Processor>;
}

/// A [`Processor`] is a type that a [`Behavior`] can spawn upon completion of
/// their startup in order to continuously process events.
#[async_trait::async_trait]
pub trait Processor<E: Send + 'static> {
    /// Returns a stream of events that the processor will process.
    async fn get_stream(&mut self) -> Result<Option<EventStream<E>>>;

    /// Processes an event and returns a [`ControlFlow`] to determine if the
    /// processor should continue or halt.
    async fn process(&mut self, event: E) -> Result<ControlFlow>;
}

#[async_trait::async_trait]
impl Processor<()> for () {
    async fn get_stream(&mut self) -> Result<Option<EventStream<()>>> {
        Ok(None)
    }
    async fn process(&mut self, _event: ()) -> Result<ControlFlow> {
        Ok(ControlFlow::Halt)
    }
}

// #[async_trait::async_trait]
// pub trait ConfigureAndStart<E>: DeserializeOwned {
//     async fn startup(
//         &mut self,
//         client: Arc<ArbiterMiddleware>,
//         messager: Messager,
//     ) -> Result<Option<(impl Processor<E>, EventStream<E>)>>
//     where
//         E: Send + Sync + 'static;
// }

/// A trait for creating a state machine.
///
/// This trait is intended to be implemented by types that can be converted into
/// a state machine. A state machine, in this context, is an entity capable of
/// executing a set of instructions or operations based on its current state and
/// inputs it receives.
///
/// Implementers of this trait should provide the logic to initialize and return
/// a new instance of a state machine, encapsulated within a `Box<dyn
/// StateMachine>`. This allows for dynamic dispatch to the state machine's
/// methods, enabling polymorphism where different types of state machines can
/// be used interchangeably at runtime.
///
/// # Returns
///
/// - `Box<dyn StateMachine>`: A boxed state machine object that can be
///   dynamically dispatched.
pub trait CreateStateMachine {
    /// Creates and returns a new state machine instance.
    ///
    /// This method consumes the implementer and returns a new instance of a
    /// state machine encapsulated within a `Box<dyn StateMachine>`. The
    /// specific type of the state machine returned can vary, allowing for
    /// flexibility and reuse of the state machine logic across
    /// different contexts.
    fn create_state_machine(self) -> Box<dyn StateMachine>;
}
#[async_trait::async_trait]
/// A trait defining the capabilities of a state machine within the system.
///
/// This trait is designed to be implemented by entities that can execute
/// instructions based on their current state and inputs they receive. The
/// execution of these instructions is asynchronous, allowing for non-blocking
/// operations within the state machine's logic.
///
/// Implementers of this trait must be able to be sent across threads and shared
/// among threads safely, hence the `Send`, `Sync`, and `'static` bounds. They
/// should also support debugging through the `Debug` trait.
pub trait StateMachine: Send + 'static {
    /// Executes a given instruction asynchronously.
    ///
    /// This method takes a mutable reference to self, allowing the state
    /// machine to modify its state in response to the instruction. The
    /// instruction to be executed is passed as an argument, encapsulating the
    /// action to be performed by the state machine.
    ///
    /// # Parameters
    ///
    /// - `instruction`: The instruction that the state machine is to execute.
    ///
    /// # Returns
    ///
    /// This method does not return a value, but it may result in state changes
    /// within the implementing type or the generation of further instructions
    /// or events.
    async fn execute(&mut self, _instruction: MachineInstruction) -> Result<()>;
}

/// The `Engine` struct represents the core logic unit of a state machine-based
/// entity, such as an agent. It encapsulates a behavior and manages the flow
/// of events to and from this behavior, effectively driving the entity's
/// response to external stimuli.
///
/// The `Engine` is generic over a behavior type `B` and an event type `E`,
/// allowing it to be used with a wide variety of behaviors and event sources.
/// It is itself a state machine, capable of executing instructions that
/// manipulate its behavior or react to events.
///
/// # Fields
///
/// - `behavior`: An optional behavior that the engine is currently managing.
///   This is where the engine's logic is primarily executed in response to
///   events.
pub struct Engine<B, E>
where
    B: Behavior<E>,
    E: Send + 'static,
{
    /// The agent ID that owns the engine.
    agent_id: Option<String>,

    /// The behavior the `Engine` runs.
    behavior: Option<B>,

    processor: Option<<B as Behavior<E>>::Processor>,

    /// The receiver of events that the [`Engine`] will process.
    /// The [`State::Processing`] stage will attempt a decode of the [`String`]s
    /// into the event type `<E>`.
    event_stream: Option<EventStream<E>>,
}

impl<B, E> Engine<B, E>
where
    B: Behavior<E>,
    E: Send + Debug,
{
    /// Creates a new [`Engine`] with the given [`Behavior`] and [`Receiver`].
    pub fn new(behavior: B) -> Self {
        Self {
            agent_id: None,
            behavior: Some(behavior),
            processor: None,
            event_stream: None,
        }
    }
}

#[async_trait::async_trait]
impl<B, E> StateMachine for Engine<B, E>
where
    B: Behavior<E> + Send + 'static,
    E: Send + Debug,
{
    async fn execute(&mut self, instruction: MachineInstruction) -> Result<()> {
        // NOTE: The unwraps here are safe because the `Behavior` in an engine is only
        // accessed here and it is private.
        match instruction {
            MachineInstruction::Start(client, messager) => {
                self.agent_id = messager.id.clone();
                let id_clone = self.agent_id.clone();
                let behavior = self.behavior.take().unwrap();
                let behavior_task: JoinHandle<Result<<B as Behavior<E>>::Processor>> =
                    tokio::spawn(async move {
                        let processor = match behavior.startup(client, messager).await {
                            Ok(processor) => processor,
                            Err(e) => {
                                error!(
                                    "Startup failed for behavior {:#?}: \n reason: {:#?}",
                                    id_clone, e
                                );
                                // Throw a panic as we cannot recover from this for now.
                                panic!();
                            }
                        };
                        debug!(
                            "Startup complete for behavior of agent: {:#?}.",
                            id_clone.unwrap_or("No ID".to_string())
                        );
                        Ok(processor)
                    });
                let mut processor = behavior_task.await??;
                match processor.get_stream().await? {
                    None => {
                        warn!(
                            "No stream found for behavior of agent: {:#?} \nBreaking!",
                            self.agent_id.clone().unwrap_or("No ID".to_string())
                        );
                        return Ok(());
                    }
                    Some(stream) => {
                        self.processor = Some(processor);
                        self.event_stream = Some(stream);
                    }
                }

                match self.execute(MachineInstruction::Process).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!(
                            "Process failed for behavior of agent: {:#?}: \n reason: {:#?}",
                            self.agent_id.clone(),
                            e
                        );
                    }
                }

                Ok(())
            }
            MachineInstruction::Process => {
                debug!(
                    "A behavior is now processing for agent: {:#?}.",
                    self.agent_id.clone().unwrap_or("No ID".to_string())
                );
                let mut processor = self.processor.take().unwrap();
                let mut stream = self.event_stream.take().unwrap();
                let processor_task: JoinHandle<Result<<B as Behavior<E>>::Processor>> =
                    tokio::spawn(async move {
                        // debug!("About to start watching events in the task.");
                        while let Some(event) = stream.next().await {
                            debug!("Received event: {:#?}", event);
                            match processor.process(event).await? {
                                ControlFlow::Halt => {
                                    break;
                                }
                                ControlFlow::Continue => {}
                            }
                        }
                        Ok(processor)
                    });
                self.processor = Some(processor_task.await??);
                Ok(())
            }
        }
    }
}
