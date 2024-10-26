//! The [`middleware`] module provides functionality to interact with
//! Ethereum-like virtual machines. It achieves this by offering a middleware
//! implementation for sending and reading transactions, as well as watching
//! for events.
//!
//! Main components:
//! - [`ArbiterMiddleware`]: The core middleware implementation.
//! - [`Connection`]: Handles communication with the Ethereum VM.
//! - [`FilterReceiver`]: Facilitates event watching based on certain filters.

#![warn(missing_docs)]
use std::{future::Future, pin::Pin, sync::Mutex, time::Duration};

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
        FilterKind, FilterWatcher, JsonRpcClient, Middleware, PendingTransaction, Provider,
        PubsubClient, SubscriptionStream,
    },
    signers::{Signer, Wallet},
    types::{
        transaction::{eip2718::TypedTransaction, eip712::Eip712},
        Address as eAddress, BlockId, Bloom, Bytes as eBytes, FilteredParams, NameOrAddress,
        Signature, Transaction, TransactionReceipt,
    },
};
use futures_timer::Delay;
use futures_util::Stream;
use rand::{rngs::StdRng, SeedableRng};
use revm::primitives::{CreateScheme, Output, TransactTo};
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;

use super::*;
use crate::environment::{instruction::*, Broadcast, Environment};

pub mod connection;
use connection::*;

pub mod nonce_middleware;
/// A middleware structure that integrates with `revm`.
///
/// [`ArbiterMiddleware`] serves as a bridge between the application and
/// [`revm`]'s execution environment, allowing for transaction sending, call
/// execution, and other core functions. It uses a custom connection and error
/// system tailored to Revm's specific needs.
///
/// This allows for [`revm`] and the [`Environment`] built around it to be
/// treated in much the same way as a live EVM blockchain can be addressed.
///
/// # Examples
///
/// Basic usage:
/// ```
/// use arbiter_core::{environment::Environment, middleware::ArbiterMiddleware};
///
/// // Create a new environment and run it
/// let mut environment = Environment::builder().build();
///
/// // Retrieve the environment to create a new middleware instance
/// let middleware = ArbiterMiddleware::new(&environment, Some("test_label"));
/// ```
/// The client can now be used for transactions with the environment.
/// Use a seed like `Some("test_label")` for maintaining a
/// consistent address across simulations and client labeling. Seeding is be
/// useful for debugging and post-processing.
#[derive(Debug)]
pub struct ArbiterMiddleware {
    provider: Provider<Connection>,
    wallet: EOA,
    /// An optional label for the middleware instance
    #[allow(unused)]
    pub label: Option<String>,
}

#[async_trait]
impl Signer for ArbiterMiddleware {
    type Error = ArbiterCoreError;

    async fn sign_message<S: Send + Sync + AsRef<[u8]>>(
        &self,
        message: S,
    ) -> Result<Signature, Self::Error> {
        match self.wallet {
            EOA::Forked(_) => Err(ArbiterCoreError::ForkedEOASignError),
            EOA::Wallet(ref wallet) => {
                let message = message.as_ref();
                let message_hash = ethers::utils::hash_message(message);
                let signature = wallet.sign_message(message_hash).await?;
                Ok(signature)
            }
        }
    }

    /// Signs the transaction
    async fn sign_transaction(&self, message: &TypedTransaction) -> Result<Signature, Self::Error> {
        match self.wallet {
            EOA::Forked(_) => Err(ArbiterCoreError::ForkedEOASignError),
            EOA::Wallet(ref wallet) => {
                let signature = wallet.sign_transaction(message).await?;
                Ok(signature)
            }
        }
    }

    /// Encodes and signs the typed data according EIP-712.
    /// Payload must implement Eip712 trait.
    async fn sign_typed_data<T: Eip712 + Send + Sync>(
        &self,
        payload: &T,
    ) -> Result<Signature, Self::Error> {
        match self.wallet {
            EOA::Forked(_) => Err(ArbiterCoreError::ForkedEOASignError),
            EOA::Wallet(ref wallet) => {
                let signature = wallet.sign_typed_data(payload).await?;
                Ok(signature)
            }
        }
    }

    /// Returns the signer's Ethereum Address
    fn address(&self) -> eAddress {
        match &self.wallet {
            EOA::Forked(address) => *address,
            EOA::Wallet(wallet) => wallet.address(),
        }
    }

    /// Returns the signer's chain id
    fn chain_id(&self) -> u64 {
        0 // TODO: THIS MIGHT BE STUPID
    }

    /// Sets the signer's chain id
    #[must_use]
    fn with_chain_id<T: Into<u64>>(self, chain_id: T) -> Self {
        match self.wallet {
            EOA::Forked(_) => self,
            EOA::Wallet(wallet) => Self {
                wallet: EOA::Wallet(wallet.with_chain_id(chain_id)),
                ..self
            },
        }
    }
}

#[async_trait::async_trait]
impl JsonRpcClient for ArbiterMiddleware {
    type Error = ProviderError;
    async fn request<T: Serialize + Send + Sync + Debug, R: DeserializeOwned + Send>(
        &self,
        method: &str,
        params: T,
    ) -> Result<R, ProviderError> {
        self.provider().as_ref().request(method, params).await
    }
}

#[async_trait::async_trait]
impl PubsubClient for ArbiterMiddleware {
    type NotificationStream = Pin<Box<dyn Stream<Item = Box<RawValue>> + Send>>;

    fn subscribe<T: Into<ethers::types::U256>>(
        &self,
        id: T,
    ) -> Result<Self::NotificationStream, Self::Error> {
        self.provider().as_ref().subscribe(id)
    }

    fn unsubscribe<T: Into<ethers::types::U256>>(&self, id: T) -> Result<(), Self::Error> {
        self.provider.as_ref().unsubscribe(id)
    }
}

/// A wrapper enum for the two types of accounts that can be used with the
/// middleware.
#[derive(Debug, Clone)]
pub enum EOA {
    /// The [`Forked`] variant is used for the forked EOA,
    /// allowing us to treat them as mock accounts that we can still authorize
    /// transactions with that we would be unable to do on mainnet.
    Forked(eAddress),
    /// The [`Wallet`] variant "real" in the sense that is has a valid private
    /// key from the provided seed
    Wallet(Wallet<SigningKey>),
}

impl ArbiterMiddleware {
    /// Creates a new instance of `ArbiterMiddleware` with procedurally
    /// generated signer/address if provided a seed/label and otherwise a
    /// random signer if not.
    ///
    /// # Examples
    /// ```
    /// use arbiter_core::{environment::Environment, middleware::ArbiterMiddleware};
    ///
    /// // Create a new environment and run it
    /// let mut environment = Environment::builder().build();
    ///
    /// // Retrieve the environment to create a new middleware instance
    /// let client = ArbiterMiddleware::new(&environment, Some("test_label"));
    ///
    /// // We can create a middleware instance without a seed by doing the following
    /// let no_seed_middleware = ArbiterMiddleware::new(&environment, None);
    /// ```
    /// Use a seed if you want to have a constant address across simulations as
    /// well as a label for a client. This can be useful for debugging.
    pub fn new(
        environment: &Environment,
        seed_and_label: Option<&str>,
    ) -> Result<Arc<Self>, ArbiterCoreError> {
        let connection = Connection::from(environment);
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
        connection
            .instruction_sender
            .upgrade()
            .ok_or(ArbiterCoreError::UpgradeSenderError)?
            .send(Instruction::AddAccount {
                address: wallet.address(),
                outcome_sender: connection.outcome_sender.clone(),
            })?;
        connection.outcome_receiver.recv()??;

        let provider = Provider::new(connection);
        info!(
            "Created new `ArbiterMiddleware` instance attached to environment labeled:
        {:?}",
            environment.parameters.label
        );
        Ok(Arc::new(Self {
            wallet: EOA::Wallet(wallet),
            provider,
            label: seed_and_label.map(|s| s.to_string()),
        }))
    }

    // TODO: This needs to have the label retrieved from the fork config.
    /// Creates a new instance of `ArbiterMiddleware` from a forked EOA.
    pub fn new_from_forked_eoa(
        environment: &Environment,
        forked_eoa: eAddress,
    ) -> Result<Arc<Self>, ArbiterCoreError> {
        let instruction_sender = &Arc::clone(&environment.socket.instruction_sender);
        let (outcome_sender, outcome_receiver) = crossbeam_channel::unbounded();

        let connection = Connection {
            instruction_sender: Arc::downgrade(instruction_sender),
            outcome_sender,
            outcome_receiver: outcome_receiver.clone(),
            event_sender: environment.socket.event_broadcaster.clone(),
            filter_receivers: Arc::new(Mutex::new(HashMap::new())),
        };
        let provider = Provider::new(connection);
        info!(
            "Created new `ArbiterMiddleware` instance from a fork -- attached to environment labeled: {:?}",
            environment.parameters.label
        );
        Ok(Arc::new(Self {
            wallet: EOA::Forked(forked_eoa),
            provider,
            label: None,
        }))
    }

    /// Allows the user to update the block number and timestamp of the
    /// [`Environment`] to whatever they may choose at any time.
    pub fn update_block(
        &self,
        block_number: impl Into<eU256>,
        block_timestamp: impl Into<eU256>,
    ) -> Result<ReceiptData, ArbiterCoreError> {
        let provider = self.provider().as_ref();
        provider
            .instruction_sender
            .upgrade()
            .ok_or(ArbiterCoreError::UpgradeSenderError)?
            .send(Instruction::BlockUpdate {
                block_number: block_number.into(),
                block_timestamp: block_timestamp.into(),
                outcome_sender: provider.outcome_sender.clone(),
            })?;

        match provider.outcome_receiver.recv()?? {
            Outcome::BlockUpdateCompleted(receipt_data) => Ok(receipt_data),
            _ => unreachable!(),
        }
    }

    /// Returns the timestamp of the current block.
    pub async fn get_block_timestamp(&self) -> Result<ethers::types::U256, ArbiterCoreError> {
        let provider = self.provider().as_ref();
        provider
            .instruction_sender
            .upgrade()
            .ok_or(ArbiterCoreError::UpgradeSenderError)?
            .send(Instruction::Query {
                environment_data: EnvironmentData::BlockTimestamp,
                outcome_sender: provider.outcome_sender.clone(),
            })?;

        match provider.outcome_receiver.recv()?? {
            Outcome::QueryReturn(outcome) => {
                Ok(ethers::types::U256::from_str_radix(outcome.as_ref(), 10)?)
            }
            _ => unreachable!(),
        }
    }

    /// Sends a cheatcode instruction to the environment.
    pub async fn apply_cheatcode(
        &self,
        cheatcode: Cheatcodes,
    ) -> Result<CheatcodesReturn, ArbiterCoreError> {
        let provider = self.provider.as_ref();
        provider
            .instruction_sender
            .upgrade()
            .ok_or(ArbiterCoreError::UpgradeSenderError)?
            .send(Instruction::Cheatcode {
                cheatcode,
                outcome_sender: provider.outcome_sender.clone(),
            })?;

        match provider.outcome_receiver.recv()?? {
            Outcome::CheatcodeReturn(outcome) => Ok(outcome),
            _ => unreachable!(),
        }
    }

    /// Returns the address of the wallet/signer given to a client.
    /// Matches on the [`EOA`] variant of the [`ArbiterMiddleware`] struct.
    pub fn address(&self) -> eAddress {
        match &self.wallet {
            EOA::Forked(address) => *address,
            EOA::Wallet(wallet) => wallet.address(),
        }
    }

    /// Allows a client to set a gas price for transactions.
    /// This can only be done if the [`Environment`] has
    /// [`EnvironmentParameters`] `gas_settings` field set to
    /// [`GasSettings::UserControlled`].
    pub async fn set_gas_price(
        &self,
        gas_price: ethers::types::U256,
    ) -> Result<(), ArbiterCoreError> {
        let provider = self.provider.as_ref();
        provider
            .instruction_sender
            .upgrade()
            .ok_or(ArbiterCoreError::UpgradeSenderError)?
            .send(Instruction::SetGasPrice {
                gas_price,
                outcome_sender: provider.outcome_sender.clone(),
            })?;
        match provider.outcome_receiver.recv()?? {
            Outcome::SetGasPriceCompleted => {
                debug!("Gas price set");
                Ok(())
            }
            _ => unreachable!(),
        }
    }
}

#[async_trait::async_trait]
impl Middleware for ArbiterMiddleware {
    type Provider = Connection;
    type Error = ArbiterCoreError;
    type Inner = Provider<Connection>;

    /// Returns a reference to the inner middleware of which there is none when
    /// using [`ArbiterMiddleware`] so we relink to `Self`
    fn inner(&self) -> &Self::Inner {
        &self.provider
    }

    /// Provides access to the associated Ethereum provider which is given by
    /// the [`Provider<Connection>`] for [`ArbiterMiddleware`].
    fn provider(&self) -> &Provider<Self::Provider> {
        &self.provider
    }

    /// Provides the default sender address for transactions, i.e., the address
    /// of the wallet/signer given to a client of the [`Environment`].
    fn default_sender(&self) -> Option<eAddress> {
        Some(self.address())
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
        trace!("Building transaction");
        let tx: TypedTransaction = tx.into();

        // Check the `to` field of the transaction to determine if it is a call or a
        // deploy. If there is no `to` field, then it is a `Deploy` else it is a
        // `Call`.
        let transact_to = match tx.to_addr() {
            Some(&to) => TransactTo::Call(to.to_fixed_bytes().into()),
            None => TransactTo::Create(CreateScheme::Create),
        };

        // Extract `from` field if it exists
        let caller = match tx.from() {
            Some(&from) => from.to_fixed_bytes().into(),
            None => self.address().to_fixed_bytes().into(),
        };

        let tx_env = TxEnv {
            caller,
            gas_limit: u64::MAX,
            gas_price: revm::primitives::U256::from_limbs(self.get_gas_price().await?.0),
            gas_priority_fee: None,
            transact_to,
            value: U256::ZERO,
            data: revm_primitives::Bytes(bytes::Bytes::from(
                tx.data()
                    .ok_or(ArbiterCoreError::MissingDataError)?
                    .to_vec(),
            )),
            chain_id: None,
            nonce: None,
            access_list: Vec::new(),
            blob_hashes: Vec::new(),
            max_fee_per_blob_gas: None,
        };
        let instruction = Instruction::Transaction {
            tx_env: tx_env.clone(),
            outcome_sender: self.provider.as_ref().outcome_sender.clone(),
        };

        let provider = self.provider.as_ref();
        provider
            .instruction_sender
            .upgrade()
            .ok_or(ArbiterCoreError::UpgradeSenderError)?
            .send(instruction)?;

        let outcome = provider.outcome_receiver.recv()??;

        if let Outcome::TransactionCompleted(execution_result, receipt_data) = outcome {
            match execution_result {
                ExecutionResult::Revert { gas_used, output } => {
                    return Err(ArbiterCoreError::ExecutionRevert {
                        gas_used,
                        output: output.to_vec(),
                    });
                }
                ExecutionResult::Halt { reason, gas_used } => {
                    return Err(ArbiterCoreError::ExecutionHalt { reason, gas_used });
                }
                ExecutionResult::Success {
                    output,
                    gas_used,
                    logs,
                    ..
                } => {
                    // TODO: This is why we need the signer middleware
                    // Note that this is technically not the correct construction on the tx hash
                    // but until we increment the nonce correctly this will do
                    let sender = self.address();

                    let logs = revm_logs_to_ethers_logs(logs, &receipt_data);
                    let to: Option<eAddress> = match tx_env.transact_to {
                        TransactTo::Call(address) => Some(address.into_array().into()),
                        TransactTo::Create(_) => None,
                    };

                    match output {
                        Output::Create(_, address) => {
                            let tx_receipt = TransactionReceipt {
                                block_hash: None,
                                block_number: Some(receipt_data.block_number),
                                contract_address: Some(recast_address(address.unwrap())),
                                logs: logs.clone(),
                                from: sender,
                                gas_used: Some(gas_used.into()),
                                effective_gas_price: Some(
                                    tx_env.clone().gas_price.to_be_bytes().into(),
                                ),
                                transaction_hash: H256::default(),
                                to,
                                cumulative_gas_used: receipt_data.cumulative_gas_per_block,
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
                            let mut pending_tx = PendingTransaction::new(
                                ethers::types::H256::zero(),
                                self.provider(),
                            )
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
                                block_hash: None,
                                block_number: Some(receipt_data.block_number),
                                contract_address: None,
                                logs: logs.clone(),
                                from: sender,
                                gas_used: Some(gas_used.into()),
                                effective_gas_price: Some(
                                    tx_env.clone().gas_price.to_be_bytes().into(),
                                ),
                                transaction_hash: H256::default(),
                                to,
                                cumulative_gas_used: receipt_data.cumulative_gas_per_block,
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

                            let mut pending_tx = PendingTransaction::new(
                                ethers::types::H256::zero(),
                                self.provider(),
                            )
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
                }
            }
        } else {
            unreachable!()
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
    ) -> Result<eBytes, Self::Error> {
        trace!("Building call");
        let tx = tx.clone();

        // Check the `to` field of the transaction to determine if it is a call or a
        // deploy. If there is no `to` field, then it is a `Deploy` else it is a
        // `Call`.
        let transact_to = match tx.to_addr() {
            Some(&to) => TransactTo::Call(to.to_fixed_bytes().into()),
            None => TransactTo::Create(CreateScheme::Create),
        };
        let tx_env = TxEnv {
            caller: self.address().to_fixed_bytes().into(),
            gas_limit: u64::MAX,
            gas_price: U256::ZERO,
            gas_priority_fee: None,
            transact_to,
            value: U256::ZERO,
            data: revm_primitives::Bytes(bytes::Bytes::from(
                tx.data()
                    .ok_or(ArbiterCoreError::MissingDataError)?
                    .to_vec(),
            )),
            chain_id: None,
            nonce: None,
            access_list: Vec::new(),
            blob_hashes: Vec::new(),
            max_fee_per_blob_gas: None,
        };
        let instruction = Instruction::Call {
            tx_env,
            outcome_sender: self.provider().as_ref().outcome_sender.clone(),
        };
        self.provider()
            .as_ref()
            .instruction_sender
            .upgrade()
            .ok_or(ArbiterCoreError::UpgradeSenderError)?
            .send(instruction)?;

        let outcome = self.provider().as_ref().outcome_receiver.recv()??;

        if let Outcome::CallCompleted(execution_result) = outcome {
            match execution_result {
                ExecutionResult::Revert { gas_used, output } => {
                    return Err(ArbiterCoreError::ExecutionRevert {
                        gas_used,
                        output: output.to_vec(),
                    });
                }
                ExecutionResult::Halt { reason, gas_used } => {
                    return Err(ArbiterCoreError::ExecutionHalt { reason, gas_used });
                }
                ExecutionResult::Success { output, .. } => {
                    return Ok(eBytes::from(output.data().to_vec()));
                }
            }
        } else {
            unreachable!()
        }
    }

    /// Creates a new filter for incoming Ethereum logs based on certain
    /// criteria.
    ///
    /// Currently, this method supports log filters. Other filters like
    /// `NewBlocks` and `PendingTransactions` are not yet implemented.
    async fn new_filter(&self, filter: FilterKind<'_>) -> Result<eU256, Self::Error> {
        let provider = self.provider.as_ref();
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
        hasher.update(serde_json::to_string(&args)?);
        let hash = hasher.finalize();
        let id = ethers::types::U256::from(ethers::types::H256::from_slice(&hash).as_bytes());
        let event_receiver = provider.event_sender.subscribe();
        let filter_receiver = FilterReceiver {
            filter,
            receiver: Some(event_receiver),
        };
        provider
            .filter_receivers
            .lock()
            .unwrap()
            .insert(id, filter_receiver);
        debug!("Filter created with ID: {:?}", id);
        Ok(id)
    }

    async fn get_logs(&self, filter: &Filter) -> Result<Vec<eLog>, Self::Error> {
        let provider = self.provider.as_ref();
        provider
            .instruction_sender
            .upgrade()
            .ok_or(ArbiterCoreError::UpgradeSenderError)?
            .send(Instruction::Query {
                environment_data: EnvironmentData::Logs {
                    filter: filter.clone(),
                },
                outcome_sender: provider.outcome_sender.clone(),
            })?;
        let outcome = provider.outcome_receiver.recv()??;
        match outcome {
            Outcome::QueryReturn(outcome) => {
                let logs: Vec<eLog> = serde_json::from_str(outcome.as_ref())?;
                Ok(logs)
            }
            _ => unreachable!(),
        }
    }

    /// Starts watching for logs that match a specific filter.
    ///
    /// This method creates a filter watcher that continuously checks for new
    /// logs matching the criteria in a separate thread.
    async fn watch<'b>(
        &'b self,
        filter: &Filter,
    ) -> Result<FilterWatcher<'b, Self::Provider, eLog>, Self::Error> {
        let id = self.new_filter(FilterKind::Logs(filter)).await?;
        Ok(FilterWatcher::new(id, self.provider()).interval(Duration::ZERO))
    }

    async fn get_gas_price(&self) -> Result<ethers::types::U256, Self::Error> {
        let provider = self.provider.as_ref();
        provider
            .instruction_sender
            .upgrade()
            .ok_or(ArbiterCoreError::UpgradeSenderError)?
            .send(Instruction::Query {
                environment_data: EnvironmentData::GasPrice,
                outcome_sender: provider.outcome_sender.clone(),
            })?;

        match provider.outcome_receiver.recv()?? {
            Outcome::QueryReturn(outcome) => {
                Ok(ethers::types::U256::from_str_radix(outcome.as_ref(), 10)?)
            }
            _ => unreachable!(),
        }
    }

    async fn get_block_number(&self) -> Result<U64, Self::Error> {
        let provider = self.provider().as_ref();
        provider
            .instruction_sender
            .upgrade()
            .ok_or(ArbiterCoreError::UpgradeSenderError)?
            .send(Instruction::Query {
                environment_data: EnvironmentData::BlockNumber,
                outcome_sender: provider.outcome_sender.clone(),
            })?;
        match provider.outcome_receiver.recv()?? {
            Outcome::QueryReturn(outcome) => {
                Ok(ethers::types::U64::from_str_radix(outcome.as_ref(), 10)?)
            }
            _ => unreachable!(),
        }
    }

    async fn get_balance<T: Into<NameOrAddress> + Send + Sync>(
        &self,
        from: T,
        block: Option<BlockId>,
    ) -> Result<ethers::types::U256, Self::Error> {
        if block.is_some() {
            return Err(ArbiterCoreError::InvalidQueryError);
        }
        let address: NameOrAddress = from.into();
        let address = match address {
            NameOrAddress::Name(_) => return Err(ArbiterCoreError::InvalidQueryError),
            NameOrAddress::Address(address) => address,
        };

        let provider = self.provider.as_ref();
        provider
            .instruction_sender
            .upgrade()
            .ok_or(ArbiterCoreError::UpgradeSenderError)?
            .send(Instruction::Query {
                environment_data: EnvironmentData::Balance(ethers::types::Address::from(address)),
                outcome_sender: provider.outcome_sender.clone(),
            })?;

        match provider.outcome_receiver.recv()?? {
            Outcome::QueryReturn(outcome) => {
                Ok(ethers::types::U256::from_str_radix(outcome.as_ref(), 10)?)
            }
            _ => unreachable!(),
        }
    }

    /// Returns the nonce of the address
    async fn get_transaction_count<T: Into<NameOrAddress> + Send + Sync>(
        &self,
        from: T,
        _block: Option<BlockId>,
    ) -> Result<eU256, Self::Error> {
        let address: NameOrAddress = from.into();
        let address = match address {
            NameOrAddress::Name(_) => return Err(ArbiterCoreError::MissingDataError),
            NameOrAddress::Address(address) => address,
        };
        let provider = self.provider.as_ref();
        provider
            .instruction_sender
            .upgrade()
            .ok_or(ArbiterCoreError::UpgradeSenderError)?
            .send(Instruction::Query {
                environment_data: EnvironmentData::TransactionCount(address),
                outcome_sender: provider.outcome_sender.clone(),
            })?;

        match provider.outcome_receiver.recv()?? {
            Outcome::QueryReturn(outcome) => {
                Ok(ethers::types::U256::from_str_radix(outcome.as_ref(), 10)?)
            }
            _ => unreachable!(),
        }
    }

    /// Fill necessary details of a transaction for dispatch
    ///
    /// This function is defined on providers to behave as follows:
    /// 1. populate the `from` field with the client address
    /// 2. Estimate gas usage
    ///
    /// It does NOT set the nonce by default.

    async fn fill_transaction(
        &self,
        tx: &mut TypedTransaction,
        _block: Option<BlockId>,
    ) -> Result<(), Self::Error> {
        // Set the `from` field of the transaction to the client address
        if tx.from().is_none() {
            tx.set_from(self.address());
        }

        // get the gas usage price
        if tx.gas_price().is_none() {
            let gas_price = self.get_gas_price().await?;
            tx.set_gas_price(gas_price);
        }

        Ok(())
    }
    /// Fetches the value stored at the storage slot `key` for an account at
    /// `address`. todo: implement the storage at a specific block feature.
    async fn get_storage_at<T: Into<NameOrAddress> + Send + Sync>(
        &self,
        account: T,
        key: ethers::types::H256,
        block: Option<BlockId>,
    ) -> Result<ethers::types::H256, ArbiterCoreError> {
        let address: NameOrAddress = account.into();
        let address = match address {
            NameOrAddress::Name(_) => return Err(ArbiterCoreError::InvalidQueryError),
            NameOrAddress::Address(address) => address,
        };

        let result = self
            .apply_cheatcode(Cheatcodes::Load {
                account: address,
                key,
                block,
            })
            .await
            .unwrap();

        match result {
            CheatcodesReturn::Load { value } => {
                // Convert the revm ruint type into big endian bytes, then convert into ethers
                // H256.
                let value: ethers::types::H256 = ethers::types::H256::from(value.to_be_bytes());
                Ok(value)
            }
            _ => unreachable!(),
        }
    }

    async fn subscribe_logs<'a>(
        &'a self,
        filter: &Filter,
    ) -> Result<SubscriptionStream<'a, Self::Provider, eLog>, Self::Error>
    where
        <Self as Middleware>::Provider: PubsubClient,
    {
        let watcher = self.watch(filter).await?;
        let id = watcher.id;
        Ok(SubscriptionStream::new(id, self.provider())?)
    }

    async fn subscribe<T, R>(
        &self,
        _params: T,
    ) -> Result<SubscriptionStream<'_, Self::Provider, R>, Self::Error>
    where
        T: Debug + Serialize + Send + Sync,
        R: DeserializeOwned + Send + Sync,
        <Self as Middleware>::Provider: PubsubClient,
    {
        todo!("This is not implemented yet, but `subscribe_logs` is.")
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

// Certainly will go away with alloy-types
/// Recast a B160 into an Address type
/// # Arguments
/// * `address` - B160 to recast. (B160)
/// # Returns
/// * `Address` - Recasted Address.
#[inline]
pub fn recast_address(address: Address) -> eAddress {
    eAddress::from(address.into_array())
}
