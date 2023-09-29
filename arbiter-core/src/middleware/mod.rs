//! The `middleware` module provides functionality to interact with
//! Ethereum-like virtual machines. It achieves this by offering a middleware
//! implementation for sending and reading transactions, as well as watching
//! for events.
//!
//! Main components:
//! - [`RevmMiddleware`]: The core middleware implementation.
//! - [`RevmMiddlewareError`]: Error type for the middleware.
//! - [`Connection`]: Handles communication with the Ethereum VM.
//! - `FilterReceiver`: Facilitates event watching based on certain filters.

#![warn(missing_docs)]

use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use ethers::{
    abi::ethereum_types::BloomInput,
    prelude::{
        k256::{
            ecdsa::SigningKey,
            sha2::{Digest, Sha256},
        },
        ProviderError,
    },
    providers::{
        FilterKind, FilterWatcher, JsonRpcClient, Middleware, MiddlewareError, PendingTransaction,
        Provider,
    },
    signers::{Signer, Wallet},
    types::{
        transaction::eip2718::TypedTransaction, Address, BlockId, Bloom, Bytes, Filter, Log,
        NameOrAddress, Transaction, TransactionReceipt, U64,
    },
};
use futures_timer::Delay;
use rand::{rngs::StdRng, SeedableRng};
use revm::primitives::{CreateScheme, Output, TransactTo, TxEnv, B160, U256};

use crate::environment::{cheatcodes::*, instruction::*, Environment};

pub mod errors;
use errors::*;

pub mod transactions;
use transactions::*;

pub mod connections;
use connections::*;

pub mod events;
use events::*;

pub mod cast;
use cast::*;

/// A middleware structure that integrates with `revm`.
///
/// [`RevmMiddleware`] serves as a bridge between the application and `revm`'s
/// execution environment, allowing for transaction sending, call execution, and
/// other core functions. It uses a custom connection and error system tailored
/// to Revm's specific needs.
///
/// This allows for `revm` and the [`Environment`] built around it to be treated
/// in much the same way as a live EVM blockchain can be addressed.
///
/// # Examples
///
/// Basic usage:
/// ```
/// // Get the necessary dependencies
/// // Import `Arc` if you need to create a client instance
/// use std::sync::Arc;
///
/// use arbiter_core::{environment::builder::EnvironmentBuilder, middleware::RevmMiddleware};
///
/// // Create a new environment and run it
/// let mut environment = EnvironmentBuilder::new().build();
/// environment.run();
///
/// // Retrieve the environment to create a new middleware instance
/// let middleware = RevmMiddleware::new(&environment, Some("test_label"));
/// let client = Arc::new(&middleware);
/// ```
/// The client can now be used for transactions with the environment.
/// Use a seed like `Some("test_label")` for maintaining a
/// consistent address across simulations and client labeling. Seeding is be
/// useful for debugging and post-processing.
#[derive(Debug)]
pub struct RevmMiddleware {
    provider: Provider<Connection>,
    wallet: Wallet<SigningKey>,
}

impl RevmMiddleware {
    /// Creates a new instance of `RevmMiddleware` with procedurally generated
    /// signer/address if provided a seed/label and otherwise a random
    /// signer if not.
    ///
    /// # Examples
    /// ```
    /// // Get the necessary dependencies
    /// // Import `Arc` if you need to create a client instance
    /// use std::sync::Arc;
    ///
    /// use arbiter_core::{environment::builder::EnvironmentBuilder, middleware::RevmMiddleware};
    ///
    /// // Create a new environment and run it
    /// let mut environment = EnvironmentBuilder::new().build();
    /// environment.run();
    ///
    /// // Retrieve the environment to create a new middleware instance
    /// let middleware = RevmMiddleware::new(&environment, Some("test_label"));
    /// let client = Arc::new(&middleware);
    ///
    /// // We can create a middleware instance without a seed by doing the following
    /// let no_seed_middleware = RevmMiddleware::new(&environment, None);
    /// ```
    /// Use a seed if you want to have a constant address across simulations as
    /// well as a label for a client. This can be useful for debugging.
    pub fn new(
        environment: &Environment,
        seed_and_label: Option<&str>,
    ) -> Result<Self, RevmMiddlewareError> {
        let instruction_sender = &Arc::clone(&environment.socket.instruction_sender);
        let (outcome_sender, outcome_receiver) = crossbeam_channel::unbounded();
        let wallet = if let Some(seed) = seed_and_label {
            let mut hasher = Sha256::new();
            hasher.update(seed);
            let hashed = hasher.finalize();
            let mut rng: StdRng = SeedableRng::from_seed(hashed.into());
            Wallet::new(&mut rng)
        } else {
            let mut rng = rand::thread_rng();
            Wallet::new(&mut rng)
        };
        instruction_sender
            .send(Instruction::AddAccount {
                address: wallet.address(),
                outcome_sender: outcome_sender.clone(),
            })
            .map_err(|e| RevmMiddlewareError::Send(e.to_string()))?;
        outcome_receiver.recv()??;

        let connection = Connection {
            instruction_sender: Arc::downgrade(instruction_sender),
            outcome_sender,
            outcome_receiver: outcome_receiver.clone(),
            event_broadcaster: Arc::clone(&environment.socket.event_broadcaster),
            filter_receivers: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        };
        let provider = Provider::new(connection);
        Ok(Self { wallet, provider })
    }

    /// Allows the user to update the block number and timestamp of the
    /// [`Environment`] to whatever they may choose at any time.
    /// This can only be done when the [`Environment`] has
    /// [`EnvironmentParameters`] `block_settings` field set to
    /// [`BlockSettings::UserControlled`].
    pub fn update_block(
        &self,
        block_number: impl Into<ethers::types::U256>,
        block_timestamp: impl Into<ethers::types::U256>,
    ) -> Result<ReceiptData, RevmMiddlewareError> {
        let block_number: ethers::types::U256 = block_number.into();
        let block_timestamp: ethers::types::U256 = block_timestamp.into();
        let provider = self.provider().as_ref();
        if let Some(instruction_sender) = provider.instruction_sender.upgrade() {
            instruction_sender
                .send(Instruction::BlockUpdate {
                    block_number: block_number.into(),
                    block_timestamp: block_timestamp.into(),
                    outcome_sender: provider.outcome_sender.clone(),
                })
                .map_err(|e| RevmMiddlewareError::Send(e.to_string()))?;
            match provider.outcome_receiver.recv() {
                Ok(Ok(Outcome::BlockUpdateCompleted(receipt_data))) => Ok(receipt_data),
                _ => Err(RevmMiddlewareError::MissingData(
                    "Block did not update Successfully".to_string(),
                )),
            }
        } else {
            Err(RevmMiddlewareError::Send(
                "Environment is offline!".to_string(),
            ))
        }
    }

    /// Returns the timestamp of the current block.
    pub async fn get_block_timestamp(&self) -> Result<ethers::types::U256, RevmMiddlewareError> {
        if let Some(instruction_sender) = self.provider().as_ref().instruction_sender.upgrade() {
            instruction_sender
                .send(Instruction::Query {
                    environment_data: EnvironmentData::BlockTimestamp,
                    outcome_sender: self.provider().as_ref().outcome_sender.clone(),
                })
                .map_err(|e| RevmMiddlewareError::Send(e.to_string()))?;
            match self.provider().as_ref().outcome_receiver.recv()?? {
                Outcome::QueryReturn(outcome) => {
                    ethers::types::U256::from_str_radix(outcome.as_ref(), 10)
                        .map_err(|e| RevmMiddlewareError::Conversion(e.to_string()))
                }
                _ => Err(RevmMiddlewareError::MissingData(
                    "Wrong variant returned via query!".to_string(),
                )),
            }
        } else {
            Err(RevmMiddlewareError::Send(
                "Environment is offline!".to_string(),
            ))
        }
    }

    /// Sends a cheatcode instruction to the environment.
    pub async fn apply_cheatcode(
        &self,
        cheatcode: Cheatcodes,
    ) -> Result<CheatcodesReturn, RevmMiddlewareError> {
        if let Some(instruction_sender) = self.provider.as_ref().instruction_sender.upgrade() {
            instruction_sender
                .send(Instruction::Cheatcode {
                    cheatcode,
                    outcome_sender: self.provider().as_ref().outcome_sender.clone(),
                })
                .map_err(|e| RevmMiddlewareError::Send(e.to_string()))?;

            match self.provider().as_ref().outcome_receiver.recv()?? {
                Outcome::CheatcodeReturn(outcome) => Ok(outcome),
                _ => Err(RevmMiddlewareError::MissingData(
                    "Wrong variant returned via instruction outcome!".to_string(),
                )),
            }
        } else {
            Err(RevmMiddlewareError::Send(
                "Environment is offline!".to_string(),
            ))
        }
    }

    /// Returns the address of the wallet/signer given to a client.
    pub fn address(&self) -> Address {
        self.wallet.address()
    }

    /// Allows a client to set a gas price for transactions.
    /// This can only be done if the [`Environment`] has
    /// [`EnvironmentParameters`] `gas_settings` field set to
    /// [`GasSettings::UserControlled`].
    pub async fn set_gas_price(
        &self,
        gas_price: ethers::types::U256,
    ) -> Result<(), RevmMiddlewareError> {
        if let Some(instruction_sender) = self.provider().as_ref().instruction_sender.upgrade() {
            instruction_sender
                .send(Instruction::SetGasPrice {
                    gas_price,
                    outcome_sender: self.provider().as_ref().outcome_sender.clone(),
                })
                .map_err(|e| RevmMiddlewareError::Send(e.to_string()))?;
            match self.provider().as_ref().outcome_receiver.recv()?? {
                Outcome::SetGasPriceCompleted => Ok(()),
                _ => Err(RevmMiddlewareError::MissingData(
                    "Wrong variant returned via instruction outcome!".to_string(),
                )),
            }
        } else {
            Err(RevmMiddlewareError::Send(
                "Environment is offline!".to_string(),
            ))
        }
    }
}

#[async_trait::async_trait]
impl Middleware for RevmMiddleware {
    type Provider = Connection;
    type Error = RevmMiddlewareError;
    type Inner = Provider<Connection>;

    /// Returns a reference to the inner middleware of which there is none when
    /// using [`RevmMiddleware`] so we relink to `Self`
    fn inner(&self) -> &Self::Inner {
        &self.provider
    }

    /// Provides access to the associated Ethereum provider which is given by
    /// the [`Provider<Connection>`] for [`RevmMiddleware`].
    fn provider(&self) -> &Provider<Self::Provider> {
        &self.provider
    }

    /// Provides the default sender address for transactions, i.e., the address
    /// of the wallet/signer given to a client of the [`Environment`].
    fn default_sender(&self) -> Option<Address> {
        Some(self.wallet.address())
    }

    /// Sends a transaction to the [`Environment`] which acts as a simulated
    /// Ethereum network.
    ///
    /// The method checks if the transaction is either a call to an existing
    /// contract or a deploy of a new one, and constructs the necessary
    /// transaction environment used for `revm`-based transactions.
    /// It then sends this transaction for execution and returns the
    /// corresponding pending transaction.
    async fn send_transaction<T: Into<TypedTransaction> + Send + Sync>(
        &self,
        tx: T,
        _block: Option<BlockId>,
    ) -> Result<PendingTransaction<'_, Self::Provider>, Self::Error> {
        let tx: TypedTransaction = tx.into();

        // Check the `to` field of the transaction to determine if it is a call or a
        // deploy. If there is no `to` field, then it is a `Deploy` else it is a
        // `Call`.
        let transact_to = match tx.to_addr() {
            Some(to) => TransactTo::Call(B160::from(*to)),
            None => TransactTo::Create(CreateScheme::Create),
        };
        let tx_env = TxEnv {
            caller: B160::from(self.wallet.address()),
            gas_limit: u64::MAX,
            gas_price: revm::primitives::U256::from_limbs(self.get_gas_price().await?.0),
            gas_priority_fee: None,
            transact_to,
            value: U256::ZERO,
            data: bytes::Bytes::from(
                tx.data()
                    .ok_or(RevmMiddlewareError::MissingData(
                        "Data missing in transaction!".to_string(),
                    ))?
                    .to_vec(),
            ),
            chain_id: None,
            nonce: None,
            access_list: Vec::new(),
        };
        let instruction = Instruction::Transaction {
            tx_env: tx_env.clone(),
            outcome_sender: self.provider.as_ref().outcome_sender.clone(),
        };

        if let Some(instruction_sender) = self.provider().as_ref().instruction_sender.upgrade() {
            instruction_sender
                .send(instruction)
                .map_err(|e| RevmMiddlewareError::Send(e.to_string()))?;
        } else {
            return Err(RevmMiddlewareError::Send(
                "Environment is offline!".to_string(),
            ));
        }

        let outcome = self.provider().as_ref().outcome_receiver.recv()??;

        if let Outcome::TransactionCompleted(execution_result, receipt_data) = outcome {
            let Success {
                _reason: _,
                _gas_used: gas_used,
                _gas_refunded: _,
                logs,
                output,
            } = unpack_execution_result(execution_result)?;

            let to: Option<ethers::types::H160> = match tx_env.transact_to {
                TransactTo::Call(address) => Some(address.into()),
                TransactTo::Create(_) => None,
            };

            // Note that this is technically not the correct construction on the tx hash
            // but until we increment the nonce correctly this will do
            let sender = self.wallet.address();
            let data = tx_env.clone().data;
            let mut hasher = Sha256::new();
            hasher.update(sender.as_bytes());
            hasher.update(data.as_ref());
            let hash = hasher.finalize();

            let mut block_hasher = Sha256::new();
            block_hasher.update(receipt_data.block_number.to_string().as_bytes());
            let block_hash = block_hasher.finalize();
            let block_hash = Some(ethers::types::H256::from_slice(&block_hash));

            match output {
                Output::Create(_, address) => {
                    let tx_receipt = TransactionReceipt {
                        block_hash,
                        block_number: Some(receipt_data.block_number),
                        contract_address: Some(recast_address(address.unwrap())),
                        logs: logs.clone(),
                        from: sender,
                        gas_used: Some(gas_used.into()),
                        effective_gas_price: Some(tx_env.clone().gas_price.into()),
                        transaction_hash: ethers::types::TxHash::from_slice(&hash),
                        to,
                        cumulative_gas_used: receipt_data.cumulative_gas_per_block.into(),
                        status: Some(1.into()),
                        root: None,
                        logs_bloom: {
                            let mut bloom = Bloom::default();
                            for log in &logs {
                                bloom.accrue(BloomInput::Raw(&log.address.0));
                                for topic in log.topics.iter() {
                                    bloom.accrue(BloomInput::Raw(topic.as_bytes()));
                                }
                            }
                            bloom
                        },
                        transaction_type: match tx {
                            TypedTransaction::Eip2930(_) => Some(1.into()),
                            _ => None,
                        },
                        transaction_index: receipt_data.transaction_index,
                        ..Default::default()
                    };

                    // TODO: I'm not sure we need to set the confirmations.
                    let mut pending_tx =
                        PendingTransaction::new(ethers::types::H256::zero(), self.provider())
                            .interval(Duration::ZERO)
                            .confirmations(0);

                    let state_ptr: *mut PendingTxState =
                        &mut pending_tx as *mut _ as *mut PendingTxState;

                    // Modify the value (this assumes you have access to the enum variants)
                    unsafe {
                        *state_ptr = PendingTxState::CheckingReceipt(Some(tx_receipt));
                    }

                    Ok(pending_tx)
                }
                Output::Call(_) => {
                    let tx_receipt = TransactionReceipt {
                        block_hash,
                        block_number: Some(receipt_data.block_number),
                        contract_address: None,
                        logs: logs.clone(),
                        from: sender,
                        gas_used: Some(gas_used.into()),
                        effective_gas_price: Some(tx_env.clone().gas_price.into()),
                        transaction_hash: ethers::types::TxHash::from_slice(&hash),
                        to,
                        cumulative_gas_used: receipt_data.cumulative_gas_per_block.into(),
                        status: Some(1.into()),
                        root: None,
                        logs_bloom: {
                            let mut bloom = Bloom::default();
                            for log in &logs {
                                bloom.accrue(BloomInput::Raw(&log.address.0));
                                for topic in log.topics.iter() {
                                    bloom.accrue(BloomInput::Raw(topic.as_bytes()));
                                }
                            }
                            bloom
                        },
                        transaction_type: match tx {
                            TypedTransaction::Eip2930(_) => Some(1.into()),
                            _ => None,
                        },
                        transaction_index: receipt_data.transaction_index,
                        ..Default::default()
                    };

                    // TODO: Create the actual tx_hash
                    // TODO: I'm not sure we need to set the confirmations.
                    let mut pending_tx =
                        PendingTransaction::new(ethers::types::H256::zero(), self.provider())
                            .interval(Duration::ZERO)
                            .confirmations(0);

                    let state_ptr: *mut PendingTxState =
                        &mut pending_tx as *mut _ as *mut PendingTxState;

                    // Modify the value (this assumes you have access to the enum variants)
                    unsafe {
                        *state_ptr = PendingTxState::CheckingReceipt(Some(tx_receipt));
                    }

                    Ok(pending_tx)
                }
            }
        } else {
            panic!("This should never happen!")
        }
    }

    /// Calls a contract method without creating a worldstate-changing
    /// transaction on the [`Environment`] (again, simulating the Ethereum
    /// network).
    ///
    /// Similar to `send_transaction`, this method checks if the call is
    /// targeting an existing contract or deploying a new one. After
    /// executing the call, it returns the output, but no worldstate change will
    /// be documented in the `revm` DB.
    async fn call(
        &self,
        tx: &TypedTransaction,
        _block: Option<BlockId>,
    ) -> Result<Bytes, Self::Error> {
        let tx = tx.clone();

        // Check the `to` field of the transaction to determine if it is a call or a
        // deploy. If there is no `to` field, then it is a `Deploy` else it is a
        // `Call`.
        let transact_to = match tx.to_addr() {
            Some(to) => TransactTo::Call(B160::from(*to)),
            None => TransactTo::Create(CreateScheme::Create),
        };
        let tx_env = TxEnv {
            caller: B160::from(self.wallet.address()),
            gas_limit: u64::MAX,
            gas_price: U256::ZERO,
            gas_priority_fee: None,
            transact_to,
            value: U256::ZERO,
            data: bytes::Bytes::from(
                tx.data()
                    .ok_or(RevmMiddlewareError::MissingData(
                        "Data missing in transaction!".to_string(),
                    ))?
                    .to_vec(),
            ),
            chain_id: None,
            nonce: None,
            access_list: Vec::new(),
        };
        let instruction = Instruction::Call {
            tx_env,
            outcome_sender: self.provider().as_ref().outcome_sender.clone(),
        };
        if let Some(instruction_sender) = self.provider().as_ref().instruction_sender.upgrade() {
            instruction_sender
                .send(instruction)
                .map_err(|e| RevmMiddlewareError::Send(e.to_string()))?;
        } else {
            return Err(RevmMiddlewareError::Send(
                "Environment is offline!".to_string(),
            ));
        }
        let outcome = self.provider().as_ref().outcome_receiver.recv()??;

        if let Outcome::CallCompleted(execution_result) = outcome {
            let output = unpack_execution_result(execution_result)?.output;
            match output {
                Output::Create(bytes, ..) => {
                    return Ok(Bytes::from(bytes.to_vec()));
                }
                Output::Call(bytes) => {
                    return Ok(Bytes::from(bytes.to_vec()));
                }
            }
        } else {
            panic!("This should never happen!")
        }
    }

    /// Creates a new filter for incoming Ethereum logs based on certain
    /// criteria.
    ///
    /// Currently, this method supports log filters. Other filters like
    /// `NewBlocks` and `PendingTransactions` are not yet implemented.
    async fn new_filter(&self, filter: FilterKind<'_>) -> Result<ethers::types::U256, Self::Error> {
        let (_method, args) = match filter {
            FilterKind::NewBlocks => unimplemented!(
                "Filtering via new `FilterKind::NewBlocks` has not been implemented yet!"
            ),
            FilterKind::PendingTransactions => {
                unimplemented!("Filtering via `FilterKind::PendingTransactions` has not been implemented yet! 
                At the current development stage of Arbiter, transactions do not actually sit in a pending state
                 -- they are executed immediately.")
            }
            FilterKind::Logs(filter) => ("eth_newFilter", filter),
        };
        let filter = args.clone();
        let mut hasher = Sha256::new();
        hasher.update(serde_json::to_string(&args).map_err(RevmMiddlewareError::Json)?);
        let hash = hasher.finalize();
        let id = ethers::types::U256::from(ethers::types::H256::from_slice(&hash).as_bytes());
        let (event_sender, event_receiver) =
            crossbeam_channel::unbounded::<Vec<revm::primitives::Log>>();
        let filter_receiver = FilterReceiver {
            filter,
            receiver: event_receiver,
        };
        self.provider()
            .as_ref()
            .event_broadcaster
            .lock()
            .map_err(|e| {
                RevmMiddlewareError::EventBroadcaster(format!(
                    "Failed to gain lock on the `Connection`'s `event_broadcaster` due to {:?} ",
                    e
                ))
            })?
            .add_sender(event_sender);
        self.provider()
            .as_ref()
            .filter_receivers
            .lock()
            .await
            .insert(id, filter_receiver);
        Ok(id)
    }

    /// Starts watching for logs that match a specific filter.
    ///
    /// This method creates a filter watcher that continuously checks for new
    /// logs matching the criteria in a separate thread.
    async fn watch<'b>(
        &'b self,
        filter: &Filter,
    ) -> Result<FilterWatcher<'b, Self::Provider, Log>, Self::Error> {
        let id = self.new_filter(FilterKind::Logs(filter)).await?;
        Ok(FilterWatcher::new(id, self.provider()).interval(Duration::ZERO))
    }

    async fn get_gas_price(&self) -> Result<ethers::types::U256, Self::Error> {
        if let Some(instruction_sender) = self.provider().as_ref().instruction_sender.upgrade() {
            instruction_sender
                .send(Instruction::Query {
                    environment_data: EnvironmentData::GasPrice,
                    outcome_sender: self.provider().as_ref().outcome_sender.clone(),
                })
                .map_err(|e| RevmMiddlewareError::Send(e.to_string()))?;
            match self.provider().as_ref().outcome_receiver.recv()?? {
                Outcome::QueryReturn(outcome) => {
                    ethers::types::U256::from_str_radix(outcome.as_ref(), 10)
                        .map_err(|e| RevmMiddlewareError::Conversion(e.to_string()))
                }
                _ => Err(RevmMiddlewareError::MissingData(
                    "Wrong variant returned via query!".to_string(),
                )),
            }
        } else {
            Err(RevmMiddlewareError::Send(
                "Environment is offline!".to_string(),
            ))
        }
    }

    async fn get_block_number(&self) -> Result<U64, Self::Error> {
        if let Some(instruction_sender) = self.provider().as_ref().instruction_sender.upgrade() {
            instruction_sender
                .send(Instruction::Query {
                    environment_data: EnvironmentData::BlockNumber,
                    outcome_sender: self.provider().as_ref().outcome_sender.clone(),
                })
                .map_err(|e| RevmMiddlewareError::Send(e.to_string()))?;
            match self.provider().as_ref().outcome_receiver.recv()?? {
                Outcome::QueryReturn(outcome) => {
                    ethers::types::U64::from_str_radix(outcome.as_ref(), 10)
                        .map_err(|e| RevmMiddlewareError::Conversion(e.to_string()))
                }
                _ => Err(RevmMiddlewareError::MissingData(
                    "Wrong variant returned via query!".to_string(),
                )),
            }
        } else {
            Err(RevmMiddlewareError::Send(
                "Environment is offline!".to_string(),
            ))
        }
    }

    async fn get_balance<T: Into<NameOrAddress> + Send + Sync>(
        &self,
        from: T,
        block: Option<BlockId>,
    ) -> Result<ethers::types::U256, Self::Error> {
        if block.is_some() {
            return Err(RevmMiddlewareError::MissingData(
                "Querying balance at a specific block is not supported!".to_string(),
            ));
        }
        let address: NameOrAddress = from.into();
        let address = match address {
            NameOrAddress::Name(_) => {
                return Err(RevmMiddlewareError::MissingData(
                    "Querying balance via name is not supported!".to_string(),
                ))
            }
            NameOrAddress::Address(address) => address,
        };

        if let Some(instruction_sender) = self.provider().as_ref().instruction_sender.upgrade() {
            instruction_sender
                .send(Instruction::Query {
                    environment_data: EnvironmentData::Balance(ethers::types::Address::from(
                        address,
                    )),
                    outcome_sender: self.provider().as_ref().outcome_sender.clone(),
                })
                .map_err(|e| RevmMiddlewareError::Send(e.to_string()))?;
            match self.provider().as_ref().outcome_receiver.recv()?? {
                Outcome::QueryReturn(outcome) => {
                    ethers::types::U256::from_str_radix(outcome.as_ref(), 10)
                        .map_err(|e| RevmMiddlewareError::Conversion(e.to_string()))
                }
                _ => Err(RevmMiddlewareError::MissingData(
                    "Wrong variant returned via query!".to_string(),
                )),
            }
        } else {
            Err(RevmMiddlewareError::Send(
                "Environment is offline!".to_string(),
            ))
        }
    }

    /// Fetches the value stored at the storage slot `key` for an account at `address`.
    /// todo: implement the storage at a specific block feature.
    async fn get_storage_at<T: Into<NameOrAddress> + Send + Sync>(
        &self,
        account: T,
        key: ethers::types::H256,
        block: Option<BlockId>,
    ) -> Result<ethers::types::H256, RevmMiddlewareError> {
        let address: NameOrAddress = account.into();
        let address = match address {
            NameOrAddress::Name(_) => {
                return Err(RevmMiddlewareError::MissingData(
                    "Querying storage via name is not supported!".to_string(),
                ))
            }
            NameOrAddress::Address(address) => address,
        };

        let result = self
            .apply_cheatcode(Cheatcodes::Load {
                account: address.into(),
                key: key.into(),
                block: block.map(|b| b.into()),
            })
            .await
            .unwrap();

        match result {
            CheatcodesReturn::Load { value } => {
                // Convert the revm ruint type into big endian bytes, then convert into ethers H256.
                let value: ethers::types::H256 = ethers::types::H256::from(value.to_be_bytes());
                Ok(value)
            }
            _ => Err(RevmMiddlewareError::MissingData(
                "Wrong variant returned via cheatcode!".to_string(),
            )),
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) type PinBoxFut<'a, T> = Pin<Box<dyn Future<Output = Result<T, ProviderError>> + 'a>>;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type PinBoxFut<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ProviderError>> + Send + 'a>>;

// Because this is the exact same struct it will have the exact same memory
// aliment allowing us to bypass the fact that ethers-rs doesn't export this
// enum normally We box the TransactionReceipts to keep the enum small.
#[allow(unused, missing_docs)]
pub enum PendingTxState<'a> {
    /// Initial delay to ensure the GettingTx loop doesn't immediately fail
    InitialDelay(Pin<Box<Delay>>),

    /// Waiting for interval to elapse before calling API again
    PausedGettingTx,

    /// Polling The blockchain to see if the Tx has confirmed or dropped
    GettingTx(PinBoxFut<'a, Option<Transaction>>),

    /// Waiting for interval to elapse before calling API again
    PausedGettingReceipt,

    /// Polling the blockchain for the receipt
    GettingReceipt(PinBoxFut<'a, Option<TransactionReceipt>>),

    /// If the pending tx required only 1 conf, it will return early. Otherwise
    /// it will proceed to the next state which will poll the block number
    /// until there have been enough confirmations
    CheckingReceipt(Option<TransactionReceipt>),

    /// Waiting for interval to elapse before calling API again
    PausedGettingBlockNumber(Option<TransactionReceipt>),

    /// Polling the blockchain for the current block number
    GettingBlockNumber(PinBoxFut<'a, U64>, Option<TransactionReceipt>),

    /// Future has completed and should panic if polled again
    Completed,
}
