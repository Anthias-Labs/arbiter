#![warn(missing_docs)]
use std::error::Error;

use ethers::types::U256;
use eyre::Result;
use revm::primitives::ExecutionResult;
use ruint::Uint;
use simulate::{
    agent::{simple_arbitrageur::NextTx, Agent, AgentType},
    contract::{IsDeployed, SimulationContract},
    environment::SimulationEnvironment,
    manager::SimulationManager,
    stochastic::price_process::{PriceProcess, PriceProcessType, OU},
};

pub mod arbitrage;
pub mod startup;

/// Run a simulation.
pub fn run() -> Result<(), Box<dyn Error>> {
    // Create a `SimulationManager` that runs simulations in their `SimulationEnvironment`.
    let mut manager = SimulationManager::new();

    // Run the startup script
    let (contracts, _pool_data, pool_id) = startup::run(&mut manager)?;

    // Start the arbitrageur
    let arbitrageur = manager.agents.get("arbitrageur").unwrap();

    // Intialize the arbitrageur with the prices from the two exchanges.
    let arbitrageur = match arbitrageur {
        AgentType::SimpleArbitrageur(base_arbitrageur) => base_arbitrageur,
        _ => panic!(),
    };
    let liquid_exchange_xy_price = arbitrageur.call_contract(
        &mut manager.environment,
        &contracts.liquid_exchange_xy,
        contracts.liquid_exchange_xy.encode_function("price", ())?,
        Uint::ZERO,
    );
    let liquid_exchange_xy_price = manager.unpack_execution(liquid_exchange_xy_price)?;
    let liquid_exchange_xy_price: U256 = contracts
        .liquid_exchange_xy
        .decode_output("price", liquid_exchange_xy_price)?;
    let portfolio_price = arbitrageur.call_contract(
        &mut manager.environment,
        &contracts.portfolio,
        contracts
            .portfolio
            .encode_function("getSpotPrice", pool_id)?,
        Uint::ZERO,
    );
    let portfolio_price = manager.unpack_execution(portfolio_price)?;
    let portfolio_price: U256 = contracts
        .portfolio
        .decode_output("getSpotPrice", portfolio_price)?;
    let mut prices = arbitrageur.prices.lock().unwrap();
    prices[0] = liquid_exchange_xy_price.into();
    prices[1] = portfolio_price.into();
    drop(prices);

    println!("Initial prices for Arbitrageur: {:#?}", arbitrageur.prices);

    let (_handle, rx) = arbitrageur.detect_arbitrage();

    // Get prices
    let ou = OU::new(0.001, 50.0, 1.0);
    let price_process = PriceProcess::new(
        PriceProcessType::OU(ou),
        0.01,
        "trade".to_string(),
        5,
        1.0,
        1,
    );
    let prices = price_process.generate_price_path().1;

    // Run the simulation
    // Update the first price
    let liquid_exchange = &contracts.liquid_exchange_xy;
    let price = prices[0];
    update_price(
        manager.agents.get("admin").unwrap(),
        &mut manager.environment,
        liquid_exchange,
        price,
    )?;
    let mut index: usize = 1;
    while let Ok((next_tx, sell_asset)) = rx.recv() {
        println!("Entered Main's `while let` with index: {}", index);
        if index >= prices.len() {
            println!("Reached end of price path\n");
            break;
        }
        let price = prices[index];

        match next_tx {
            NextTx::Swap => {
                arbitrage::swap(
                    manager.agents.get("arbitrageur").unwrap(),
                    &mut manager.environment,
                    &contracts.portfolio,
                    pool_id,
                    10_u128.pow(15),
                    sell_asset.unwrap(),
                )?;
                update_price(
                    manager.agents.get("admin").unwrap(),
                    &mut manager.environment,
                    liquid_exchange,
                    price,
                )?;

                // Get new portfolio price
                println!("Getting new portfolio price...");
                let portfolio_price = arbitrageur.call_contract(
                    &mut manager.environment,
                    &contracts.portfolio,
                    contracts
                        .portfolio
                        .encode_function("getSpotPrice", pool_id)?,
                    Uint::ZERO,
                );
                let portfolio_price = match portfolio_price {
                    ExecutionResult::Success { output, .. } => output.into_data(),
                    _ => {
                        println!("Error getting portfolio price.");
                        break;
                    }
                };
                let portfolio_price: U256 = contracts
                    .portfolio
                    .decode_output("getSpotPrice", portfolio_price)?;
                println!("New portfolio price: {}\n", portfolio_price);
                let mut prices = arbitrageur.prices.lock().unwrap();
                prices[1] = portfolio_price.into();
                drop(prices);
                println!("Updated prices for Arbitrageur: {:#?}", arbitrageur.prices);
                index += 1;
                continue;
            }
            NextTx::UpdatePrice => {
                update_price(
                    manager.agents.get("admin").unwrap(),
                    &mut manager.environment,
                    liquid_exchange,
                    price,
                )?;
                index += 1;
                continue;
            }
            NextTx::None => {
                println!("Can't update prices\n");
                continue;
            }
        }
    }

    // handle.join().unwrap();

    println!("=======================================");
    println!("🎉 Simulation Completed 🎉");
    println!("=======================================");

    Ok(())
}

/// Update prices on the liquid exchange.
fn update_price(
    admin: &dyn Agent,
    environment: &mut SimulationEnvironment,
    liquid_exchange: &SimulationContract<IsDeployed>,
    price: f64,
) -> Result<(), Box<dyn Error>> {
    // let admin = manager.agents.get("admin").unwrap();
    println!("Updating price...");
    println!("Price from price path: {}\n", price);
    let wad_price = simulate::utils::float_to_wad(price);
    // println!("WAD price: {}", wad_price);
    let call_data = liquid_exchange.encode_function("setPrice", wad_price)?;
    admin.call_contract(environment, liquid_exchange, call_data, Uint::from(0));
    Ok(())
}
