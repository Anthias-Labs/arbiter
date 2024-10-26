//! The [`environment`] module provides abstractions and functionality for
//! handling the Ethereum execution environment. This includes managing its
//! state, interfacing with the EVM, and broadcasting events to subscribers.
//! Other features include the ability to control block rate and gas settings
//! and execute other database modifications from external agents.
//!
//! The key integration for the environment is the Rust EVM [`revm`](https://github.com/bluealloy/revm).
//! This is an implementation of the EVM in Rust that we utilize for processing
//! raw smart contract bytecode.
//!
//! Core structures:
//! - [`Environment`]: Represents the Ethereum execution environment, allowing
//!   for its management (e.g., starting, stopping) and interfacing with agents.
//! - [`EnvironmentParameters`]: Parameters necessary for creating or modifying
//!   an [`Environment`].
//! - [`Instruction`]: Enum indicating the type of instruction that is being
//!   sent to the EVM.

use std::thread::{self, JoinHandle};

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use ethers::{abi::AbiDecode, types::ValueOrArray};
use revm::{
    db::AccountState,
    inspector_handle_register,
    primitives::{Env, HashMap, B256},
};
use tokio::sync::broadcast::channel;

use super::*;
#[cfg_attr(doc, doc(hidden))]
#[cfg_attr(doc, allow(unused_imports))]
#[cfg(doc)]
use crate::middleware::ArbiterMiddleware;
use crate::{
    console::abi::HardhatConsoleCalls, database::inspector::ArbiterInspector,
    middleware::connection::revm_logs_to_ethers_logs,
};

pub mod instruction;
use instruction::*;

/// Alias for the sender of the channel for transmitting transactions.
pub(crate) type InstructionSender = Sender<Instruction>;

/// Alias for the receiver of the channel for transmitting transactions.
pub(crate) type InstructionReceiver = Receiver<Instruction>;

/// Alias for the sender of the channel for transmitting [`RevmResult`] emitted
/// from transactions.
pub(crate) type OutcomeSender = Sender<Result<Outcome, ArbiterCoreError>>;

/// Alias for the receiver of the channel for transmitting [`RevmResult`]
/// emitted from transactions.
pub(crate) type OutcomeReceiver = Receiver<Result<Outcome, ArbiterCoreError>>;

/// Represents a sandboxed EVM environment.
///
/// ## Features
/// * [`revm::Evm`] and its connections to the "outside world" (agents) via the
///   [`Socket`] provide the [`Environment`] a means to route and execute
///   transactions.
/// * [`ArbiterDB`] is the database structure used that allows for read-only
///   sharing of execution and write-only via the main thread. This can also be
///   a database read in from disk storage via [`database::fork::Fork`].
/// * [`ArbiterInspector`] is an that allows for the EVM to be able to display
///   logs and properly handle gas payments.
/// * [`EnvironmentParameters`] are used to set the gas limit, contract size
///   limit, and label for the [`Environment`].
#[derive(Debug)]
pub struct Environment {
    /// The label used to define the [`Environment`].
    pub parameters: EnvironmentParameters,

    /// The [`EVM`] that is used as an execution environment and database for
    /// calls and transactions.
    pub(crate) db: ArbiterDB,

    inspector: Option<ArbiterInspector>,

    /// This gives a means of letting the "outside world" connect to the
    /// [`Environment`] so that users (or agents) may send and receive data from
    /// the [`EVM`].
    pub(crate) socket: Socket,

    /// [`JoinHandle`] for the thread in which the [`EVM`] is running.
    /// Used for assuring that the environment is stopped properly or for
    /// performing any blocking action the end user needs.
    pub(crate) handle: Option<JoinHandle<Result<(), ArbiterCoreError>>>,
}

/// Parameters to create [`Environment`]s with different settings.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct EnvironmentParameters {
    /// The label used to define the [`Environment`].
    pub label: Option<String>,

    /// The gas limit for the blocks in the [`Environment`].
    pub gas_limit: Option<U256>,

    /// The contract size limit for the [`Environment`].
    pub contract_size_limit: Option<usize>,

    /// Enables inner contract logs to be printed to the console.
    pub console_logs: bool,

    /// Allows for turning off any gas payments for transactions so no inspector
    /// is needed.
    pub pay_gas: bool,
}

/// A builder for creating an [`Environment`].
///
/// This builder allows for the configuration of an [`Environment`] before it is
/// instantiated. It provides methods for setting the label, gas limit, contract
/// size limit, and a database for the [`Environment`].
pub struct EnvironmentBuilder {
    parameters: EnvironmentParameters,
    db: ArbiterDB,
}

impl EnvironmentBuilder {
    /// Builds and runs an [`Environment`] with the parameters set in the
    /// [`EnvironmentBuilder`].
    pub fn build(self) -> Environment {
        Environment::create(self.parameters, self.db).run()
    }

    /// Sets the label for the [`Environment`].
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.parameters.label = Some(label.into());
        self
    }

    /// Sets the gas limit for the [`Environment`].
    pub fn with_gas_limit(mut self, gas_limit: U256) -> Self {
        self.parameters.gas_limit = Some(gas_limit);
        self
    }

    /// Sets the contract size limit for the [`Environment`].
    pub fn with_contract_size_limit(mut self, contract_size_limit: usize) -> Self {
        self.parameters.contract_size_limit = Some(contract_size_limit);
        self
    }

    /// Sets the state for the [`Environment`]. This can come from a saved state
    /// of a simulation or a [`database::fork::Fork`].
    pub fn with_state(mut self, state: impl Into<CacheDB<EmptyDB>>) -> Self {
        self.db.state = Arc::new(RwLock::new(state.into()));
        self
    }

    /// Sets the logs for the [`Environment`]. This can come from a saved state
    /// of a simulation and can be useful for doing analysis.
    pub fn with_logs(
        mut self,
        logs: impl Into<std::collections::HashMap<U256, Vec<eLog>>>,
    ) -> Self {
        self.db.logs = Arc::new(RwLock::new(logs.into()));
        self
    }

    /// Sets the entire database for the [`Environment`] including both the
    /// state and logs. This can come from the saved state of a simulation and
    /// can be useful for doing analysis.
    pub fn with_arbiter_db(mut self, db: ArbiterDB) -> Self {
        self.db = db;
        self
    }

    /// Enables inner contract logs to be printed to the console as `trace`
    /// level logs prepended with "Console logs: ".
    pub fn with_console_logs(mut self) -> Self {
        self.parameters.console_logs = true;
        self
    }

    /// Turns on gas payments for transactions so that the [`EVM`] will
    /// automatically pay for gas and revert if balance is not met by sender.
    pub fn with_pay_gas(mut self) -> Self {
        self.parameters.pay_gas = true;
        self
    }
}

impl Environment {
    /// Creates a new [`EnvironmentBuilder`] with default parameters that can be
    /// used to build an [`Environment`].
    pub fn builder() -> EnvironmentBuilder {
        EnvironmentBuilder {
            parameters: EnvironmentParameters::default(),
            db: ArbiterDB::default(),
        }
    }

    fn create(parameters: EnvironmentParameters, db: ArbiterDB) -> Self {
        let (instruction_sender, instruction_receiver) = unbounded();
        let (event_broadcaster, _) = channel(512);
        let socket = Socket {
            instruction_sender: Arc::new(instruction_sender),
            instruction_receiver,
            event_broadcaster,
        };

        let inspector = if parameters.console_logs || parameters.pay_gas {
            Some(ArbiterInspector::new(
                parameters.console_logs,
                parameters.pay_gas,
            ))
        } else {
            Some(ArbiterInspector::new(false, false))
        };

        Self {
            socket,
            inspector,
            parameters,
            db,
            handle: None,
        }
    }

    /// This starts the [`Environment`] thread to process any [`Instruction`]s
    /// coming through the [`Socket`].
    fn run(mut self) -> Self {
        // Bring in parameters for the `Environment`.
        let label = self.parameters.label.clone();

        // Bring in the EVM db and log storage by cloning the interior Arc
        // (lightweight).
        let db = self.db.clone();

        // Bring in the EVM ENV
        let mut env = Env::default();
        env.cfg.limit_contract_code_size = self.parameters.contract_size_limit;
        env.block.gas_limit = self.parameters.gas_limit.unwrap_or(U256::MAX);
        // Bring in the inspector
        let inspector = self.inspector.take().unwrap();

        // Pull communication clones to move into a new thread.
        let instruction_receiver = self.socket.instruction_receiver.clone();
        let event_broadcaster = self.socket.event_broadcaster.clone();

        // Move the EVM and its socket to a new thread and retrieve this handle
        let handle = thread::spawn(move || {
            // Create a new EVM builder
            let mut evm = Evm::builder()
                .with_db(db.clone())
                .with_env(Box::new(env))
                .with_external_context(inspector)
                .append_handler_register(inspector_handle_register)
                .build();

            // Initialize counters that are returned on some receipts.
            let mut transaction_index = U64::from(0_u64);
            let mut cumulative_gas_per_block = eU256::from(0);

            // Loop over the instructions sent through the socket.
            while let Ok(instruction) = instruction_receiver.recv() {
                trace!(
                    "Instruction {:?} received by environment labeled: {:?}",
                    instruction,
                    label
                );
                match instruction {
                    Instruction::AddAccount {
                        address,
                        outcome_sender,
                    } => {
                        let recast_address = Address::from(address.as_fixed_bytes());
                        let account = revm::db::DbAccount {
                            info: AccountInfo::default(),
                            account_state: AccountState::None,
                            storage: HashMap::new(),
                        };
                        match db.state.write()?.accounts.insert(recast_address, account) {
                            None => outcome_sender.send(Ok(Outcome::AddAccountCompleted))?,
                            Some(_) => {
                                outcome_sender.send(Err(ArbiterCoreError::AccountCreationError))?;
                            }
                        }
                    }
                    Instruction::BlockUpdate {
                        block_number,
                        block_timestamp,
                        outcome_sender,
                    } => {
                        // Return the old block data in a `ReceiptData`
                        let old_block_number = evm.block().number;
                        let receipt_data = ReceiptData {
                            block_number: convert_uint_to_u64(old_block_number)?,
                            transaction_index,
                            cumulative_gas_per_block,
                        };

                        // Update the block number and timestamp
                        evm.block_mut().number = U256::from_limbs(block_number.0);
                        evm.block_mut().timestamp = U256::from_limbs(block_timestamp.0);

                        // Reset the counters.
                        transaction_index = U64::from(0);
                        cumulative_gas_per_block = eU256::from(0);

                        // Return the old block data in a `ReceiptData` after the block update.
                        outcome_sender.send(Ok(Outcome::BlockUpdateCompleted(receipt_data)))?;
                    }
                    Instruction::Cheatcode {
                        cheatcode,
                        outcome_sender,
                    } => match cheatcode {
                        Cheatcodes::Load {
                            account,
                            key,
                            block: _,
                        } => {
                            let recast_address = Address::from(account.as_fixed_bytes());
                            let recast_key = B256::from(key.as_fixed_bytes()).into();

                            // Get the account storage value at the key in the db.
                            match db.state.write()?.accounts.get_mut(&recast_address) {
                                Some(account) => {
                                    // Returns zero if the account is missing.
                                    let value: U256 = match account.storage.get::<U256>(&recast_key)
                                    {
                                        Some(value) => *value,
                                        None => U256::ZERO,
                                    };
                                    outcome_sender.send(Ok(Outcome::CheatcodeReturn(
                                        CheatcodesReturn::Load { value },
                                    )))?;
                                }
                                None => {
                                    outcome_sender
                                        .send(Err(ArbiterCoreError::AccountDoesNotExistError))?;
                                }
                            };
                        }
                        Cheatcodes::Store {
                            account,
                            key,
                            value,
                        } => {
                            let recast_address = Address::from(account.as_fixed_bytes());
                            let recast_key = B256::from(key.as_fixed_bytes());
                            let recast_value = B256::from(value.as_fixed_bytes());

                            // Mutate the db by inserting the new key-value pair into the account's
                            // storage and send the successful CheatcodeCompleted outcome.
                            match db.state.write()?.accounts.get_mut(&recast_address) {
                                Some(account) => {
                                    account
                                        .storage
                                        .insert(recast_key.into(), recast_value.into());

                                    outcome_sender.send(Ok(Outcome::CheatcodeReturn(
                                        CheatcodesReturn::Store,
                                    )))?;
                                }
                                None => {
                                    outcome_sender
                                        .send(Err(ArbiterCoreError::AccountDoesNotExistError))?;
                                }
                            };
                        }
                        Cheatcodes::Deal { address, amount } => {
                            let recast_address = Address::from(address.as_fixed_bytes());
                            match db.state.write()?.accounts.get_mut(&recast_address) {
                                Some(account) => {
                                    account.info.balance += U256::from_limbs(amount.0);
                                    outcome_sender.send(Ok(Outcome::CheatcodeReturn(
                                        CheatcodesReturn::Deal,
                                    )))?;
                                }
                                None => {
                                    outcome_sender
                                        .send(Err(ArbiterCoreError::AccountDoesNotExistError))?;
                                }
                            };
                        }
                        Cheatcodes::Access { address } => {
                            let recast_address = Address::from(address.as_fixed_bytes());
                            match db.state.write()?.accounts.get(&recast_address) {
                                Some(account) => {
                                    let account_state = match account.account_state {
                                        AccountState::None => AccountStateSerializable::None,
                                        AccountState::Touched => AccountStateSerializable::Touched,
                                        AccountState::StorageCleared => {
                                            AccountStateSerializable::StorageCleared
                                        }
                                        AccountState::NotExisting => {
                                            AccountStateSerializable::NotExisting
                                        }
                                    };

                                    let account = CheatcodesReturn::Access {
                                        account_state,
                                        info: account.info.clone(),
                                        storage: account.storage.clone(),
                                    };

                                    outcome_sender.send(Ok(Outcome::CheatcodeReturn(account)))?;
                                }
                                None => {
                                    outcome_sender
                                        .send(Err(ArbiterCoreError::AccountDoesNotExistError))?;
                                }
                            }
                        }
                    },
                    // A `Call` is not state changing and will not create events but will create
                    // console logs.
                    Instruction::Call {
                        tx_env,
                        outcome_sender,
                    } => {
                        // Set the tx_env and prepare to process it
                        *evm.tx_mut() = tx_env;

                        let result = evm.transact()?.result;

                        if let Some(console_log) = &mut evm.context.external.console_log {
                            console_log.0.drain(..).for_each(|log| {
                                // This unwrap is safe because the logs are guaranteed to be
                                // `HardhatConsoleCalls` by the `ArbiterInspector`.
                                trace!(
                                    "Console logs: {:?}",
                                    HardhatConsoleCalls::decode(log).unwrap().to_string()
                                )
                            });
                        };

                        outcome_sender.send(Ok(Outcome::CallCompleted(result)))?;
                    }
                    Instruction::SetGasPrice {
                        gas_price,
                        outcome_sender,
                    } => {
                        evm.tx_mut().gas_price = U256::from_limbs(gas_price.0);
                        outcome_sender.send(Ok(Outcome::SetGasPriceCompleted))?;
                    }

                    // A `Transaction` is state changing and will create events.
                    Instruction::Transaction {
                        tx_env,
                        outcome_sender,
                    } => {
                        // Set the tx_env and prepare to process it
                        *evm.tx_mut() = tx_env;

                        let execution_result = match evm.transact_commit() {
                            Ok(result) => {
                                if let Some(console_log) = &mut evm.context.external.console_log {
                                    console_log.0.drain(..).for_each(|log| {
                                        // This unwrap is safe because the logs are guaranteed to be
                                        // `HardhatConsoleCalls` by the `ArbiterInspector`.
                                        trace!(
                                            "Console logs: {:?}",
                                            HardhatConsoleCalls::decode(log).unwrap().to_string()
                                        )
                                    });
                                };
                                result
                            }
                            Err(e) => {
                                outcome_sender.send(Err(ArbiterCoreError::EVMError(e)))?;
                                continue;
                            }
                        };
                        cumulative_gas_per_block += eU256::from(execution_result.gas_used());
                        let block_number = convert_uint_to_u64(evm.block().number)?;
                        let receipt_data = ReceiptData {
                            block_number,
                            transaction_index,
                            cumulative_gas_per_block,
                        };

                        let mut logs = db.logs.write()?;
                        match logs.get_mut(&evm.block().number) {
                            Some(log_vec) => {
                                log_vec.extend(revm_logs_to_ethers_logs(
                                    execution_result.logs().to_vec(),
                                    &receipt_data,
                                ));
                            }
                            None => {
                                logs.insert(
                                    evm.block().number,
                                    revm_logs_to_ethers_logs(
                                        execution_result.logs().to_vec(),
                                        &receipt_data,
                                    ),
                                );
                            }
                        }

                        match event_broadcaster.send(Broadcast::Event(
                            execution_result.logs().to_vec(),
                            receipt_data.clone(),
                        )) {
                            Ok(_) => {}
                            Err(_) => {
                                warn!(
                                    "Event was not sent to any listeners. Are there any listeners?"
                                )
                            }
                        }
                        outcome_sender.send(Ok(Outcome::TransactionCompleted(
                            execution_result,
                            receipt_data,
                        )))?;

                        transaction_index += U64::from(1);
                    }
                    Instruction::Query {
                        environment_data,
                        outcome_sender,
                    } => {
                        let outcome = match environment_data {
                            EnvironmentData::BlockNumber => {
                                Ok(Outcome::QueryReturn(evm.block().number.to_string()))
                            }
                            EnvironmentData::BlockTimestamp => {
                                Ok(Outcome::QueryReturn(evm.block().timestamp.to_string()))
                            }
                            EnvironmentData::GasPrice => {
                                Ok(Outcome::QueryReturn(evm.tx().gas_price.to_string()))
                            }
                            EnvironmentData::Balance(address) => {
                                match db
                                    .state
                                    .read()
                                    .unwrap()
                                    .accounts
                                    .get::<Address>(&address.as_fixed_bytes().into())
                                {
                                    Some(account) => {
                                        Ok(Outcome::QueryReturn(account.info.balance.to_string()))
                                    }
                                    None => Err(ArbiterCoreError::AccountDoesNotExistError),
                                }
                            }
                            EnvironmentData::TransactionCount(address) => {
                                match db
                                    .state
                                    .read()
                                    .unwrap()
                                    .accounts
                                    .get::<Address>(&address.as_fixed_bytes().into())
                                {
                                    Some(account) => {
                                        Ok(Outcome::QueryReturn(account.info.nonce.to_string()))
                                    }
                                    None => Err(ArbiterCoreError::AccountDoesNotExistError),
                                }
                            }
                            EnvironmentData::Logs { filter } => {
                                let logs = db.logs.read().unwrap();
                                let from_block = U256::from(
                                    filter
                                        .block_option
                                        .get_from_block()
                                        .ok_or(ArbiterCoreError::MissingDataError)?
                                        .as_number()
                                        .ok_or(ArbiterCoreError::MissingDataError)?
                                        .0[0],
                                );
                                let to_block = U256::from(
                                    filter
                                        .block_option
                                        .get_to_block()
                                        .ok_or(ArbiterCoreError::MissingDataError)?
                                        .as_number()
                                        .ok_or(ArbiterCoreError::MissingDataError)?
                                        .0[0],
                                );
                                let mut return_logs = Vec::new();
                                logs.keys().for_each(|blocknum| {
                                    if blocknum >= &from_block && blocknum <= &to_block {
                                        return_logs.extend(logs.get(blocknum).cloned().unwrap());
                                    }
                                });
                                return_logs.retain(|log| {
                                    filter.topics.iter().any(|topic_option| match topic_option {
                                        Some(topic_val_or_array) => match topic_val_or_array {
                                            ValueOrArray::Value(topic) => match topic {
                                                Some(topic) => log.topics.contains(topic),
                                                None => true,
                                            },
                                            ValueOrArray::Array(topics) => {
                                                topics.iter().any(|topic| match topic {
                                                    Some(topic) => log.topics.contains(topic),
                                                    None => true,
                                                })
                                            }
                                        },
                                        None => true,
                                    })
                                });
                                return_logs.retain(|log| {
                                    filter.address.iter().any(|address_value_or_array| {
                                        match address_value_or_array {
                                            ValueOrArray::Value(address) => &log.address == address,

                                            ValueOrArray::Array(addresses) => {
                                                addresses.iter().any(|addr| &log.address == addr)
                                            }
                                        }
                                    })
                                });
                                Ok(Outcome::QueryReturn(
                                    serde_json::to_string(&return_logs).unwrap(),
                                ))
                            }
                        };
                        outcome_sender.send(outcome)?;
                    }
                    Instruction::Stop(outcome_sender) => {
                        match event_broadcaster.send(Broadcast::StopSignal) {
                            Ok(_) => {}
                            Err(_) => {
                                warn!("Stop signal was not sent to any listeners. Are there any listeners?")
                            }
                        }
                        outcome_sender.send(Ok(Outcome::StopCompleted(db)))?;
                        break;
                    }
                }
            }
            Ok(())
        });
        self.handle = Some(handle);
        self
    }

    /// Obtains the current underlying [`CacheDB`] instance
    /// after a read lock was acquired to inspect the current execution state
    pub fn db(&self) -> Result<CacheDB<EmptyDB>, ArbiterCoreError> {
        match self.db.state.read() {
            Ok(cache_db) => Ok(cache_db.clone()),
            Err(err) => Err(ArbiterCoreError::RwLockError(err.to_string())),
        }
    }

    /// Stops the execution of the environment and returns the [`ArbiterDB`] in
    /// its final state.
    pub fn stop(mut self) -> Result<ArbiterDB, ArbiterCoreError> {
        let (outcome_sender, outcome_receiver) = bounded(1);
        self.socket
            .instruction_sender
            .send(Instruction::Stop(outcome_sender))?;
        let outcome = outcome_receiver.recv()??;

        let db = match outcome {
            Outcome::StopCompleted(stopped_db) => stopped_db,
            _ => unreachable!(),
        };

        if let Some(label) = &self.parameters.label {
            warn!("Stopped environment with label: {}", label);
        } else {
            warn!("Stopped environment with no label.");
        }
        drop(self.socket.instruction_sender);
        self.handle
            .take()
            .unwrap()
            .join()
            .map_err(|_| ArbiterCoreError::JoinError)??;
        Ok(db)
    }
}

/// Provides channels for communication between the EVM and external entities.
///
/// The socket contains senders and receivers for transactions, as well as an
/// event broadcaster to broadcast logs from the EVM to subscribers.
#[derive(Debug, Clone)]
pub(crate) struct Socket {
    pub(crate) instruction_sender: Arc<InstructionSender>,
    pub(crate) instruction_receiver: InstructionReceiver,
    pub(crate) event_broadcaster: BroadcastSender<Broadcast>,
}

/// Enum representing the types of broadcasts that can be sent.
///
/// This enum is used to differentiate between different types of broadcasts
/// that can be sent from the environment to external entities.
///
/// Variants:
/// * `StopSignal`: Represents a signal to stop the event logger process.
/// * `Event(Vec<Log>)`: Represents a broadcast of a vector of Ethereum logs.
#[derive(Clone, Debug)]
pub enum Broadcast {
    /// Represents a signal to stop the event logger process.
    StopSignal,
    /// Represents a broadcast of a vector of Ethereum logs.
    Event(Vec<Log>, ReceiptData),
}

/// Convert a U256 to a U64, discarding the higher bits if the number is larger
/// than 2^64 # Arguments
/// * `input` - The U256 to convert.
/// # Returns
/// * `Ok(U64)` - The converted U64. Used for block number which is a U64.
#[inline]
fn convert_uint_to_u64(input: U256) -> Result<U64, ArbiterCoreError> {
    let as_str = input.to_string();
    match as_str.parse::<u64>() {
        Ok(val) => Ok(val.into()),
        Err(e) => Err(e)?,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) const TEST_ENV_LABEL: &str = "test";
    const TEST_CONTRACT_SIZE_LIMIT: usize = 42069;
    const TEST_GAS_LIMIT: u64 = 1_333_333_333_337;

    #[test]
    fn new_with_parameters() {
        let environment = Environment::builder()
            .with_label(TEST_ENV_LABEL)
            .with_contract_size_limit(TEST_CONTRACT_SIZE_LIMIT)
            .with_gas_limit(U256::from(TEST_GAS_LIMIT));
        assert_eq!(environment.parameters.label, Some(TEST_ENV_LABEL.into()));
        assert_eq!(
            environment.parameters.contract_size_limit.unwrap(),
            TEST_CONTRACT_SIZE_LIMIT
        );
        assert_eq!(
            environment.parameters.gas_limit.unwrap(),
            U256::from(TEST_GAS_LIMIT)
        );
    }

    #[test]
    fn conversion() {
        // Test with a value that fits in u64.
        let input = U256::from(10000);
        assert_eq!(convert_uint_to_u64(input).unwrap(), U64::from(10000));

        // Test with a value that is exactly at the limit of u64.
        let input = U256::from(u64::MAX);
        assert_eq!(convert_uint_to_u64(input).unwrap(), U64::from(u64::MAX));

        // Test with a value that exceeds the limit of u64.
        let input = U256::from(u64::MAX) + U256::from(1);
        assert!(convert_uint_to_u64(input).is_err());
    }
}
