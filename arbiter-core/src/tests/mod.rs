#![allow(missing_docs)]
#![cfg(feature = "contracts")]

// mod interaction;
mod clients;
mod contracts;
mod data_output;
mod derives;
mod environment_control;
mod middleware_instructions;

use std::{str::FromStr, sync::Arc};

use anyhow::Result;
use ethers::{
    prelude::{
        k256::sha2::{Digest, Sha256},
        EthLogDecode, Middleware,
    },
    providers::ProviderError,
    types::{Address, Filter, ValueOrArray, U256},
};
use futures::StreamExt;

use crate::{
    bindings::{arbiter_math::*, arbiter_token::*, liquid_exchange::LiquidExchange},
    environment::{cheatcodes::*, *},
    math::*,
    middleware::*,
};

pub const TEST_BLOCK_RATE: f64 = 2.0;
pub const TEST_BLOCK_TIME: u32 = 12;
pub const TEST_ENV_SEED: u64 = 1;
pub const TEST_GAS_PRICE: u128 = 100;
pub const TEST_GAS_MULTIPLIER: f64 = 2.0;

pub const TEST_ARG_NAME: &str = "ArbiterToken";
pub const TEST_ARG_SYMBOL: &str = "ARBT";
pub const TEST_ARG_DECIMALS: u8 = 18;

pub const TEST_MINT_AMOUNT: u128 = 69;
pub const TEST_MINT_TO: &str = "0xf7e93cc543d97af6632c9b8864417379dba4bf15";

pub const TEST_APPROVAL_AMOUNT: u128 = 420;

pub const TEST_SIGNER_SEED_AND_LABEL: &str = "test_seed_and_label";

pub const ARBITER_TOKEN_X_NAME: &str = "Arbiter Token X";
pub const ARBITER_TOKEN_X_SYMBOL: &str = "ARBX";
pub const ARBITER_TOKEN_X_DECIMALS: u8 = 18;

pub const ARBITER_TOKEN_Y_NAME: &str = "Arbiter Token Y";
pub const ARBITER_TOKEN_Y_SYMBOL: &str = "ARBY";
pub const ARBITER_TOKEN_Y_DECIMALS: u8 = 18;

pub const LIQUID_EXCHANGE_PRICE: f64 = 420.69;

fn startup_randomly_sampled() -> Result<(Environment, Arc<RevmMiddleware>)> {
    let env = builder::EnvironmentBuilder::new()
        .block_settings(builder::BlockSettings::RandomlySampled {
            block_rate: TEST_BLOCK_RATE,
            block_time: TEST_BLOCK_TIME,
            seed: TEST_ENV_SEED,
        })
        .gas_settings(builder::GasSettings::RandomlySampled {
            multiplier: TEST_GAS_MULTIPLIER,
        })
        .build();
    let client = RevmMiddleware::new(&env, Some(TEST_SIGNER_SEED_AND_LABEL))?;
    Ok((env, client))
}

fn startup_user_controlled() -> Result<(Environment, Arc<RevmMiddleware>)> {
    let env = builder::EnvironmentBuilder::new().build();
    let client = RevmMiddleware::new(&env, Some(TEST_SIGNER_SEED_AND_LABEL))?;
    Ok((env, client))
}

fn startup_constant_gas() -> Result<(Environment, Arc<RevmMiddleware>)> {
    let env = builder::EnvironmentBuilder::new()
        .gas_settings(builder::GasSettings::Constant(TEST_GAS_PRICE))
        .build();
    let client = RevmMiddleware::new(&env, Some(TEST_SIGNER_SEED_AND_LABEL))?;
    Ok((env, client))
}

async fn deploy_arbx(client: Arc<RevmMiddleware>) -> Result<ArbiterToken<RevmMiddleware>> {
    Ok(ArbiterToken::deploy(
        client,
        (
            ARBITER_TOKEN_X_NAME.to_string(),
            ARBITER_TOKEN_X_SYMBOL.to_string(),
            ARBITER_TOKEN_X_DECIMALS,
        ),
    )?
    .send()
    .await?)
}

async fn deploy_arby(client: Arc<RevmMiddleware>) -> Result<ArbiterToken<RevmMiddleware>> {
    Ok(ArbiterToken::deploy(
        client,
        (
            ARBITER_TOKEN_Y_NAME.to_string(),
            ARBITER_TOKEN_Y_SYMBOL.to_string(),
            ARBITER_TOKEN_Y_DECIMALS,
        ),
    )?
    .send()
    .await?)
}

async fn deploy_liquid_exchange(
    client: Arc<RevmMiddleware>,
) -> Result<(
    ArbiterToken<RevmMiddleware>,
    ArbiterToken<RevmMiddleware>,
    LiquidExchange<RevmMiddleware>,
)> {
    let arbx = deploy_arbx(client.clone()).await?;
    let arby = deploy_arby(client.clone()).await?;
    let price = float_to_wad(LIQUID_EXCHANGE_PRICE);
    let liquid_exchange = LiquidExchange::deploy(client, (arbx.address(), arby.address(), price))?
        .send()
        .await?;
    Ok((arbx, arby, liquid_exchange))
}

async fn deploy_arbiter_math(client: Arc<RevmMiddleware>) -> Result<ArbiterMath<RevmMiddleware>> {
    Ok(ArbiterMath::deploy(client, ())?.send().await?)
}
