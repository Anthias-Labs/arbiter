//! `arbiter-core` is designed to facilitate agent-based simulations of Ethereum
//! smart contracts in a local environment.
//!
//! With a primary emphasis on ease of use and performance, it employs the [`revm`](https://crates.io/crates/revm) (Rust EVM) to provide a local execution environment that closely simulates the Ethereum blockchain but without associated overheads like networking latency.
//!
//! Key Features:
//! - **Manager Interface**: The main user entry-point that offers management of
//!   different environments and agents.
//! - **Environment Handling**: Detailed setup and control mechanisms for
//!   running the Ethereum-like blockchain environment.
//! - **Middleware Implementation**: Customized middleware to reduce overhead
//!   and provide optimal performance.
//!
//! For a detailed guide on getting started and best practices, check out [link
//! to your guide or further documentation]. // TODO: Add in a link.
//!
//! For specific module-level information and examples, navigate to the
//! respective module documentation below.

#![warn(missing_docs, unsafe_code)]

pub mod agent;
pub mod bindings; // TODO: Add better documentation here and some kind of overwrite protection.
pub mod environment;
pub mod manager;
pub mod math;
pub mod middleware;
#[cfg(test)]
pub mod tests;
