// TODO: Hit all the contract bindings.

use super::*;
use crate::bindings::liquid_exchange::LiquidExchange;

pub const ARBITER_TOKEN_X_NAME: &str = "Arbiter Token X";
pub const ARBITER_TOKEN_X_SYMBOL: &str = "ARBX";
pub const ARBITER_TOKEN_X_DECIMALS: u8 = 18;

pub const ARBITER_TOKEN_Y_NAME: &str = "Arbiter Token Y";
pub const ARBITER_TOKEN_Y_SYMBOL: &str = "ARBY";
pub const ARBITER_TOKEN_Y_DECIMALS: u8 = 18;

pub const LIQUID_EXCHANGE_PRICE: f64 = 420.69;

fn startup() -> Result<(Manager, Arc<RevmMiddleware>)> {
    let manager = Manager::new();
    manager.add_environment(TEST_ENV_LABEL, TEST_BLOCK_RATE, TEST_ENV_SEED)?;
    let client = Arc::new(RevmMiddleware::new(
        manager.environments.borrow().get(TEST_ENV_LABEL).unwrap(),
        Some(TEST_SIGNER_SEED_AND_LABEL.to_string()),
    ));
    manager.start_environment(TEST_ENV_LABEL)?;
    Ok((manager, client))
}

async fn deploy_arbiter_math(client: Arc<RevmMiddleware>) -> Result<ArbiterMath<RevmMiddleware>> {
    Ok(ArbiterMath::deploy(client, ())?.send().await?)
}

#[tokio::test]
async fn arbiter_math() -> Result<()> {
    let (_manager, client) = startup()?;
    let arbiter_math = deploy_arbiter_math(client).await?;

    // Test the cdf function
    let cdf_output = arbiter_math
        .cdf(ethers::types::I256::from(1))
        .call()
        .await?;
    println!("cdf(1) = {}", cdf_output);
    assert_eq!(cdf_output, ethers::types::I256::from(500000000000000000u64));

    // Test the pdf function
    let pdf_output = arbiter_math
        .pdf(ethers::types::I256::from(1))
        .call()
        .await?;
    println!("pdf(1) = {}", pdf_output);
    assert_eq!(pdf_output, ethers::types::I256::from(398942280401432678u64));

    // Test the ppf function.
    let ppf_output = arbiter_math
        .ppf(ethers::types::I256::from(1))
        .call()
        .await?;
    println!("ppf(1) = {}", ppf_output);
    assert_eq!(
        ppf_output,
        ethers::types::I256::from(-8710427241990476442_i128)
    );

    // Test the mulWadDown function.
    let mulwaddown_output = arbiter_math
        .mul_wad_down(
            ethers::types::U256::from(1_000_000_000_000_000_000_u128),
            ethers::types::U256::from(2),
        )
        .call()
        .await?;
    println!("mulWadDown(1, 2) = {}", mulwaddown_output);
    assert_eq!(mulwaddown_output, ethers::types::U256::from(2));

    // Test the mulWadUp function.
    let mulwadup_output = arbiter_math
        .mul_wad_up(
            ethers::types::U256::from(1_000_000_000_000_000_000_u128),
            ethers::types::U256::from(2),
        )
        .call()
        .await?;
    println!("mulWadUp(1, 2) = {}", mulwadup_output);
    assert_eq!(mulwadup_output, ethers::types::U256::from(2));

    // Test the divWadDown function.
    let divwaddown_output = arbiter_math
        .div_wad_down(
            ethers::types::U256::from(1_000_000_000_000_000_000_u128),
            ethers::types::U256::from(2),
        )
        .call()
        .await?;
    println!("divWadDown(1, 2) = {}", divwaddown_output);
    assert_eq!(
        divwaddown_output,
        ethers::types::U256::from(500000000000000000000000000000000000_u128)
    );

    // Test the divWadUp function.
    let divwadup_output = arbiter_math
        .div_wad_up(
            ethers::types::U256::from(1_000_000_000_000_000_000_u128),
            ethers::types::U256::from(2),
        )
        .call()
        .await?;
    println!("divWadUp(1, 2) = {}", divwadup_output);
    assert_eq!(
        divwadup_output,
        ethers::types::U256::from(500000000000000000000000000000000000_u128)
    );

    // Test the lnWad function.
    let lnwad_output = arbiter_math
        .log(ethers::types::I256::from(1_000_000_000_000_000_000_u128))
        .call()
        .await?;
    println!("ln(1) = {}", lnwad_output);
    assert_eq!(lnwad_output, ethers::types::I256::from(0));

    // Test the sqrt function
    let sqrt_output = arbiter_math
        .sqrt(ethers::types::U256::from(1_000_000_000_000_000_000_u128))
        .call()
        .await?;
    println!("sqrt(1) = {}", sqrt_output);
    assert_eq!(sqrt_output, ethers::types::U256::from(1_000_000_000));
    Ok(())
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

// TODO: It would be good to change this to `token_functions` and test all
// relevant ERC20 functions (e.g., transfer, approve, etc.).
#[tokio::test]
async fn token_mint_and_balance() -> Result<()> {
    let (_manager, client) = startup()?;
    let arbx = deploy_arbx(client.clone()).await?;

    // Mint some tokens to the client.
    arbx.mint(
        client.default_sender().unwrap(),
        ethers::types::U256::from(TEST_MINT_AMOUNT),
    )
    .send()
    .await?
    .await?;

    // Fetch the balance of the client.
    let balance = arbx
        .balance_of(client.default_sender().unwrap())
        .call()
        .await?;

    // Check that the balance is correct.
    assert_eq!(balance, ethers::types::U256::from(TEST_MINT_AMOUNT));

    Ok(())
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

#[tokio::test]
async fn liquid_exchange_swap() -> Result<()> {
    let (_manager, client) = startup()?;
    let (arbx, arby, liquid_exchange) = deploy_liquid_exchange(client.clone()).await?;

    // Mint tokens to the client then check balances.
    arbx.mint(
        client.default_sender().unwrap(),
        ethers::types::U256::from(TEST_MINT_AMOUNT),
    )
    .send()
    .await?
    .await?;
    arby.mint(
        client.default_sender().unwrap(),
        ethers::types::U256::from(TEST_MINT_AMOUNT),
    )
    .send()
    .await?
    .await?;
    let arbx_balance = arbx
        .balance_of(client.default_sender().unwrap())
        .call()
        .await?;
    let arby_balance = arby
        .balance_of(client.default_sender().unwrap())
        .call()
        .await?;
    println!("arbx_balance prior to swap = {}", arbx_balance);
    println!("arby_balance prior to swap = {}", arby_balance);
    assert_eq!(arbx_balance, ethers::types::U256::from(TEST_MINT_AMOUNT));
    assert_eq!(arby_balance, ethers::types::U256::from(TEST_MINT_AMOUNT));

    // Get the price at the liquid exchange
    let price = liquid_exchange.price().call().await?;
    println!("price in 18 decimal WAD: {}", price);

    // Mint tokens to the liquid exchange.
    let exchange_mint_amount = ethers::types::U256::MAX / 2;
    arbx.mint(liquid_exchange.address(), exchange_mint_amount)
        .send()
        .await?
        .await?;
    arby.mint(liquid_exchange.address(), exchange_mint_amount)
        .send()
        .await?
        .await?;

    // Approve the liquid exchange to spend the client's tokens.
    arbx.approve(liquid_exchange.address(), ethers::types::U256::MAX)
        .send()
        .await?
        .await?;
    arby.approve(liquid_exchange.address(), ethers::types::U256::MAX)
        .send()
        .await?
        .await?;

    // Swap some X for Y on the liquid exchange.
    let swap_amount_x = ethers::types::U256::from(TEST_MINT_AMOUNT) / 2;
    liquid_exchange
        .swap(arbx.address(), swap_amount_x)
        .send()
        .await?
        .await?
        .unwrap();

    // Check the client's balances are correct.
    let arbx_balance_after_swap_x = arbx
        .balance_of(client.default_sender().unwrap())
        .call()
        .await?;
    let arby_balance_after_swap_x = arby
        .balance_of(client.default_sender().unwrap())
        .call()
        .await?;
    println!("arbx_balance after swap = {}", arbx_balance_after_swap_x);
    println!("arby_balance after swap = {}", arby_balance_after_swap_x);
    assert_eq!(
        arbx_balance_after_swap_x,
        ethers::types::U256::from(TEST_MINT_AMOUNT) - swap_amount_x
    );
    let additional_y = swap_amount_x * price / ethers::types::U256::from(10_u64.pow(18));
    assert_eq!(
        arby_balance_after_swap_x,
        ethers::types::U256::from(TEST_MINT_AMOUNT) + additional_y
    );

    // Swap some Y for X on the liquid exchange.
    let swap_amount_y = additional_y;
    liquid_exchange
        .swap(arby.address(), swap_amount_y)
        .send()
        .await?
        .await?;

    // Check the client's balances are correct.
    let arbx_balance_after_swap_y = arbx
        .balance_of(client.default_sender().unwrap())
        .call()
        .await?;
    let arby_balance_after_swap_y = arby
        .balance_of(client.default_sender().unwrap())
        .call()
        .await?;
    println!("arbx_balance after swap = {}", arbx_balance_after_swap_y);
    println!("arby_balance after swap = {}", arby_balance_after_swap_y);

    // The balance here is off by one due to rounding and the extremely small
    // balances we are using.
    assert_eq!(
        arbx_balance_after_swap_y,
        ethers::types::U256::from(TEST_MINT_AMOUNT) - 1
    );
    assert_eq!(
        arby_balance_after_swap_y,
        ethers::types::U256::from(TEST_MINT_AMOUNT)
    );

    Ok(())
}

#[tokio::test]
async fn price_simulation_oracle() -> Result<()> {
    let (_manager, client) = startup()?;
    let (.., liquid_exchange) = deploy_liquid_exchange(client.clone()).await?;

    let price_path = vec![
        1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 6000.0, 7000.0, 8000.0,
    ];

    // Get the initial price of the liquid exchange.
    let initial_price = liquid_exchange.price().call().await?;
    assert_eq!(initial_price, float_to_wad(LIQUID_EXCHANGE_PRICE));

    for price in price_path {
        let wad_price = float_to_wad(price);
        liquid_exchange.set_price(wad_price).send().await?.await?;
        let new_price = liquid_exchange.price().call().await?;
        assert_eq!(new_price, wad_price);
    }

    Ok(())
}
