//! This module contains the `Instruction` and `Outcome` enums that are used to
//! communicate instructions and their outcomes between the
//! [`middleware::ArbiterMiddleware`] and the [`Environment`].

use super::*;

/// [`Instruction`]s that can be sent to the [`Environment`] via the
/// [`Socket`].
/// These instructions can be:
/// - [`Instruction::AddAccount`],
/// - [`Instruction::BlockUpdate`],
/// - [`Instruction::Call`],
/// - [`Instruction::Cheatcode`],
/// - [`Instruction::Query`].
/// - [`Instruction::SetGasPrice`],
/// - [`Instruction::Stop`],
/// - [`Instruction::Transaction`],
///
/// The [`Instruction`]s are sent to the [`Environment`] via the
///   [`Socket::instruction_sender`] and the results are received via the
///   [`crate::middleware::Connection::outcome_receiver`].
#[derive(Debug, Clone)]
pub(crate) enum Instruction {
    /// An `AddAccount` is used to add a default/unfunded account to the
    /// [`Environment`].
    AddAccount {
        /// The address of the account to add to the [`EVM`].
        address: eAddress,

        /// The sender used to to send the outcome of the account addition back
        /// to.
        outcome_sender: OutcomeSender,
    },

    /// A `BlockUpdate` is used to update the block number and timestamp of the
    /// [`Environment`].
    BlockUpdate {
        /// The block number to update the [`EVM`] to.
        block_number: eU256,

        /// The block timestamp to update the [`EVM`] to.
        block_timestamp: eU256,

        /// The sender used to to send the outcome of the block update back to.
        outcome_sender: OutcomeSender,
    },

    /// A `Call` is processed by the [`EVM`] but will not be state changing and
    /// will not create events.
    Call {
        /// The transaction environment for the call.
        tx_env: TxEnv,

        /// The sender used to to send the outcome of the call back to.
        outcome_sender: OutcomeSender,
    },

    /// A `cheatcode` enables direct access to the underlying [`EVM`].
    Cheatcode {
        /// The [`Cheatcode`] to use to access the underlying [`EVM`].
        cheatcode: Cheatcodes,

        /// The sender used to to send the outcome of the cheatcode back to.
        outcome_sender: OutcomeSender,
    },

    /// A `Query` is used to query the [`EVM`] for some data, the choice of
    /// which data is specified by the inner `EnvironmentData` enum.
    Query {
        /// The data to query the [`EVM`] for.
        environment_data: EnvironmentData,

        /// The sender used to to send the outcome of the query back to.
        outcome_sender: OutcomeSender,
    },

    /// A `SetGasPrice` is used to set the gas price of the [`EVM`].
    SetGasPrice {
        /// The gas price to set the [`EVM`] to.
        gas_price: eU256,

        /// The sender used to to send the outcome of the gas price setting back
        /// to.
        outcome_sender: OutcomeSender,
    },

    /// A `Stop` is used to stop the [`Environment`].
    Stop(OutcomeSender),

    /// A `Transaction` is processed by the [`EVM`] and will be state changing
    /// and will create events.
    Transaction {
        /// The transaction environment for the transaction.
        tx_env: TxEnv,

        /// The sender used to to send the outcome of the transaction back to.
        outcome_sender: OutcomeSender,
    },
}

/// [`Outcome`]s that can be sent back to the the client via the
/// [`Socket`].
/// These outcomes can be from `Call`, `Transaction`, or `BlockUpdate`
/// instructions sent to the [`Environment`]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum Outcome {
    /// The outcome of an [`Instruction::AddAccount`] instruction that is used
    /// to signify that the account was added successfully.
    AddAccountCompleted,

    /// The outcome of a `BlockUpdate` instruction that is used to provide a
    /// non-error output of updating the block number and timestamp of the
    /// [`EVM`] to the client.
    BlockUpdateCompleted(ReceiptData),

    /// Return value from a cheatcode instruction.
    /// todo: make a decision on how to handle cheatcode returns.
    CheatcodeReturn(CheatcodesReturn),

    /// The outcome of a `Call` instruction that is used to provide the output
    /// of some [`EVM`] computation to the client.
    CallCompleted(ExecutionResult),

    /// The outcome of a [`Instruction::SetGasPrice`] instruction that is used
    /// to signify that the gas price was set successfully.
    SetGasPriceCompleted,

    /// The outcome of a `Transaction` instruction that is first unpacked to see
    /// if the result is successful, then it can be used to build a
    /// `TransactionReceipt` in the `Middleware`.
    TransactionCompleted(ExecutionResult, ReceiptData),

    /// The outcome of a `Query` instruction that carries a `String`
    /// representation of the data. Currently this may carry the block
    /// number, block timestamp, gas price, or balance of an account.
    QueryReturn(String),

    /// The outcome of a `Stop` instruction that is used to signify that the
    /// [`Environment`] was stopped successfully.
    StopCompleted(ArbiterDB),
}

/// [`EnvironmentData`] is an enum used inside of the [`Instruction::Query`] to
/// specify what data should be returned to the user.
/// Currently this may be the block number, block timestamp, gas price, or
/// balance of an account.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum EnvironmentData {
    /// The query is for the block number of the [`EVM`].
    BlockNumber,

    /// The query is for the block timestamp of the [`EVM`].
    BlockTimestamp,

    /// The query is for the gas price of the [`EVM`].
    GasPrice,

    /// The query is for the balance of an account given by the inner `Address`.
    Balance(eAddress),

    // TODO: Rename this to `Nonce`?
    /// The query is for the nonce of an account given by the inner `Address`.
    TransactionCount(eAddress),

    /// Query for logs in a range of blocks.
    Logs {
        /// The filter to use to query for logs
        filter: Filter,
    },
}

/// [`ReceiptData`] is a structure that holds the block number, transaction
/// index, and cumulative gas used per block for a transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReceiptData {
    /// `block_number` is the number of the block in which the transaction was
    /// included.
    pub block_number: U64,
    /// `transaction_index` is the index position of the transaction in the
    /// block.
    pub transaction_index: U64,
    /// `cumulative_gas_per_block` is the total amount of gas used in the
    /// block up until and including the transaction.
    pub cumulative_gas_per_block: eU256,
}

/// Cheatcodes are a direct way to access the underlying [`EVM`] environment and
/// database.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Cheatcodes {
    /// A `Deal` is used to increase the balance of an account in the [`EVM`].
    Deal {
        /// The address of the account to increase the balance of.
        address: eAddress,

        /// The amount to increase the balance of the account by.
        amount: eU256,
    },
    /// Fetches the value of a storage slot of an account.
    Load {
        /// The address of the account to fetch the storage slot from.
        account: eAddress,
        /// The storage slot to fetch.
        key: H256,
        /// The block to fetch the storage slot from.
        /// todo: implement storage slots at blocks.
        block: Option<ethers::types::BlockId>,
    },
    /// Overwrites a storage slot of an account.
    /// TODO: for more complicated data types, like structs, there's more work
    /// to do.
    Store {
        /// The address of the account to overwrite the storage slot of.
        account: ethers::types::Address,
        /// The storage slot to overwrite.
        key: ethers::types::H256,
        /// The value to overwrite the storage slot with.
        value: ethers::types::H256,
    },
    /// Fetches the `DbAccount` account at the given address.
    Access {
        /// The address of the account to fetch.
        address: ethers::types::Address,
    },
    /// An `AddAccount` is used to add a default/unfunded account.
    /// If the account exists, the value is updated.
    AddAccount {
        /// The address of the account to be added.
        address: eAddress,
    },
}

/// Wrapper around [`AccountState`] that can be serialized and deserialized.
#[derive(Debug, Clone, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AccountStateSerializable {
    /// Before Spurious Dragon hardfork there was a difference between empty and
    /// not existing. And we are flagging it here.
    NotExisting,
    /// EVM touched this account. For newer hardfork this means it can be
    /// cleared/removed from state.
    Touched,
    /// EVM cleared storage of this account, mostly by selfdestruct, we don't
    /// ask database for storage slots and assume they are U256::ZERO
    StorageCleared,
    /// EVM didn't interacted with this account
    #[default]
    None,
}

/// Return values of applying cheatcodes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CheatcodesReturn {
    /// A `Load` returns the value of a storage slot of an account.
    Load {
        /// The value of the storage slot.
        value: U256,
    },
    /// A `Store` returns nothing.
    Store,
    /// A `Deal` returns nothing.
    Deal,
    /// Gets the DbAccount associated with an address.
    Access {
        /// Basic account information like nonce, balance, code hash, bytcode.
        info: AccountInfo,
        /// todo: revm must be updated with serde deserialize, then `DbAccount`
        /// can be used.
        account_state: AccountStateSerializable,
        /// Storage slots of the account.
        storage: HashMap<U256, U256>,
    },

    /// A `AddAccount` returns nothing.
    AddAccount {
        /// Basic account information of inserted account like nonce, balance,
        /// code hash, bytcode.
        info: AccountInfo,
    },
}
