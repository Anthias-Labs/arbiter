#![warn(missing_docs)]
use std::{error::Error};

use bindings::{
    arbiter_token, liquid_exchange, rmm01_portfolio, simple_registry, weth9, shared_types::Order, i_portfolio_actions, portfolio_virtual
};
use bytes::Bytes;
use ethers::{prelude::{U256, BaseContract}, types::H160, abi::{Token, Tokenize, ParamType, Function}};
use eyre::Result;
use revm::primitives::{ruint::Uint, B160};
use simulate::{
    agent::{user::User, Agent, AgentType},
    contract::{IsDeployed, SimulationContract},
    manager::SimulationManager,
    utils::recast_address,
};

struct SimulationContracts(
    SimulationContract<IsDeployed>,
    SimulationContract<IsDeployed>,
    SimulationContract<IsDeployed>,
    SimulationContract<IsDeployed>,
);

/// Run a simulation.
pub fn portfolio_sim() -> Result<(), Box<dyn Error>> {
    // define the wad constant
    let decimals = 18_u8;
    let wad: U256 = U256::from(10_i64.pow(decimals as u32));
    // Create a `SimulationManager` that runs simulations in their `SimulationEnvironment`.
    let mut manager = SimulationManager::new();

    let user_name = "arbitrageur";
    let user_address = B160::from_low_u64_be(2);
    let arbitrageur = User::new(user_name, None);

    manager.activate_agent(AgentType::User(arbitrageur), user_address)?;
    let _arbitrageur = manager.agents.get(user_name).unwrap();
    println!("Arbitrageur created at: {}", user_address);
    let _admin = manager.agents.get("admin").unwrap();

    // Deploying Contracts
    let contracts = deploy_portfolio_sim_contracts(&mut manager, wad)?;

    portfolio_sim_intitalization_calls(&mut manager, contracts)?;

    Ok(())
}

/// Deploy the contracts to the simulation environment.
/// # Arguments
/// * `manager` - Simulation manager to deploy contracts to. (SimulationManager)
/// * `wad` - Wad constant to use for the simulation. (U256)
/// # Returns
/// * `SimulationContracts` - Contracts deployed to the simulation environment. (SimulationContracts)
fn deploy_portfolio_sim_contracts(
    manager: &mut SimulationManager,
    wad: U256,
) -> Result<SimulationContracts, Box<dyn Error>> {
    let decimals = 18_u8;
    let admin = manager.agents.get("admin").unwrap();
    // Deploy Weth
    let weth = SimulationContract::new(weth9::WETH9_ABI.clone(), weth9::WETH9_BYTECODE.clone());
    let weth = weth.deploy(&mut manager.environment, admin, ());
    println!("WETH deployed at: {}", weth.address);

    // Deploy the registry contract.
    let registry = SimulationContract::new(
        simple_registry::SIMPLEREGISTRY_ABI.clone(),
        simple_registry::SIMPLEREGISTRY_BYTECODE.clone(),
    );
    let registry = registry.deploy(&mut manager.environment, admin, ());
    println!("Simple registry deployed at: {}", registry.address);

    // Deploy the portfolio contract.
    let portfolio = SimulationContract::new(
        rmm01_portfolio::RMM01PORTFOLIO_ABI.clone(),
        rmm01_portfolio::RMM01PORTFOLIO_BYTECODE.clone(),
    );

    let portfolio_args = (
        recast_address(weth.address),
        recast_address(registry.address),
    );
    let portfolio = portfolio.deploy(&mut manager.environment, admin, portfolio_args);
    println!("Portfolio deployed at: {}", portfolio.address);

    let arbiter_token = SimulationContract::new(
        arbiter_token::ARBITERTOKEN_ABI.clone(),
        arbiter_token::ARBITERTOKEN_BYTECODE.clone(),
    );

    // Choose name and symbol and combine into the constructor args required by ERC-20 contracts.
    let name = "ArbiterToken";
    let symbol = "ARBX";
    let args = (name.to_string(), symbol.to_string(), decimals);

    // Call the contract deployer and receive a IsDeployed version of SimulationContract that now has an address.
    let arbiter_token_x = arbiter_token.deploy(&mut manager.environment, admin, args);
    println!("Arbiter Token x deployed at: {}", arbiter_token_x.address);

    let name = "ArbiterTokenY";
    let symbol = "ARBY";
    let args = (name.to_string(), symbol.to_string(), decimals);

    // Call the contract deployer and receive a IsDeployed version of SimulationContract that now has an address.
    let arbiter_token_y = arbiter_token.deploy(&mut manager.environment, admin, args);
    println!("Arbiter Token Y deployed at: {}", arbiter_token_y.address);

    // Deploy LiquidExchange
    let price_to_check = 1000;
    let initial_price = wad.checked_mul(U256::from(price_to_check)).unwrap();
    let liquid_exchange = SimulationContract::new(
        liquid_exchange::LIQUIDEXCHANGE_ABI.clone(),
        liquid_exchange::LIQUIDEXCHANGE_BYTECODE.clone(),
    );
    let args = (
        recast_address(arbiter_token_x.address),
        recast_address(arbiter_token_y.address),
        initial_price,
    );
    let liquid_exchange_xy = liquid_exchange.deploy(&mut manager.environment, admin, args);

    Ok(SimulationContracts(
        arbiter_token_x,
        arbiter_token_y,
        portfolio,
        liquid_exchange_xy,
    ))
}

/// Calls the initialization functions of each contract.
/// # Arguments
/// * `manager` - Simulation manager to deploy contracts to. (SimulationManager)
/// * `contracts` - Contracts deployed to the simulation environment. (SimulationContracts)
/// * `decimals` - Decimals to use for the simulation. (u8)
fn portfolio_sim_intitalization_calls(
    manager: &mut SimulationManager,
    contracts: SimulationContracts,
) -> Result<(), Box<dyn Error>> {
    let admin = manager.agents.get("admin").unwrap();
    // Get all the necessary users.
    let SimulationContracts(arbiter_token_x, arbiter_token_y, portfolio, liquid_exchange_xy) =
        contracts;

    let arbitrageur = manager.agents.get("arbitrageur").unwrap();

    // Allocating new tokens to user by calling Arbiter Token's ERC20 'mint' instance.
    let mint_amount = u128::MAX;
    let input_arguments = (recast_address(arbitrageur.address()), mint_amount);
    let call_data = arbiter_token_x.encode_function("mint", input_arguments)?;

    // Call the 'mint' function to the arber. for token x
    let result_mint_x_for_arber = admin.call_contract(
        &mut manager.environment,
        &arbiter_token_x,
        call_data.clone(),
        Uint::from(0),
    ); // TODO: SOME KIND OF ERROR HANDLING IS NECESSARY FOR THESE TYPES OF CALLS
    println!(
        "Minted token_x to arber {:#?}",
        result_mint_x_for_arber.is_success()
    );
    // Call the `mint` function to the arber for token y.
    let result_mint_y_for_arber = admin.call_contract(
        &mut manager.environment,
        &arbiter_token_y,
        call_data,
        Uint::from(0),
    );
    println!(
        "Minted token_y to arber: {:#?}",
        result_mint_y_for_arber.is_success()
    );

    // Call the `mint` function for the admin for token x.
    let mint_token_x_admin_arguments = (recast_address(admin.address()), mint_amount);
    let call_data = arbiter_token_x.encode_function("mint", mint_token_x_admin_arguments)?;
    let execution_result = admin.call_contract(
        &mut manager.environment,
        &arbiter_token_x,
        call_data,
        Uint::from(0),
    );
    assert!(execution_result.is_success());
    // Call the `mint` function for the admin for token y.
    let mint_token_y_admin_arguments = (recast_address(admin.address()), mint_amount);
    let call_data = arbiter_token_y.encode_function("mint", mint_token_y_admin_arguments)?;
    let execution_result = admin.call_contract(
        &mut manager.environment,
        &arbiter_token_y,
        call_data,
        Uint::from(0),
    );
    assert!(execution_result.is_success());

    // Mint max token_y to the liquid_exchange contract.
    let args = (recast_address(liquid_exchange_xy.address), u128::MAX);
    let call_data = arbiter_token_y.encode_function("mint", args)?;
    let mint_result_y_liquid_exchange = admin.call_contract(
        &mut manager.environment,
        &arbiter_token_y,
        call_data,
        Uint::from(0),
    );
    println!(
        "Minted token_y to liquid_excahnge: {:#?}",
        mint_result_y_liquid_exchange.is_success()
    );

    // APROVALS
    // --------------------------------------------------------------------------------------------
    //
    // aprove the liquid_exchange to spend the arbitrageur's token_x
    let approve_liquid_excahnge_args = (recast_address(liquid_exchange_xy.address), U256::MAX);
    let call_data = arbiter_token_x.encode_function("approve", approve_liquid_excahnge_args)?;

    let result = arbitrageur.call_contract(
        &mut manager.environment,
        &arbiter_token_x,
        call_data,
        Uint::from(0),
    );
    println!(
        "Aproved token_x to liquid_excahnge for arber: {:#?}",
        result.is_success()
    );

    // aprove the liquid_exchange to spend the arbitrageur's token_y
    let approval_call_liquid_exchange =
        arbiter_token_y.encode_function("approve", approve_liquid_excahnge_args)?;
    let approval_call_result = arbitrageur.call_contract(
        &mut manager.environment,
        &arbiter_token_y,
        approval_call_liquid_exchange,
        Uint::from(0),
    );
    println!(
        "Aproved token_y to liquid_excahnge for arber: {:#?}",
        approval_call_result.is_success()
    );

    // aprove tokens on portfolio for arbitrageur
    let approve_portfolio_args = (recast_address(portfolio.address), U256::MAX);
    // Approve token_y
    let aprove_token_y_call_data =
        arbiter_token_y.encode_function("approve", approve_portfolio_args)?;
    let approve_token_y_result_arbitrageur = arbitrageur.call_contract(
        &mut manager.environment,
        &arbiter_token_y,
        aprove_token_y_call_data.clone(),
        Uint::from(0),
    );

    let approve_token_y_result_admin = admin.call_contract(
        &mut manager.environment,
        &arbiter_token_y,
        aprove_token_y_call_data.clone(),
        Uint::from(0),
    );
    println!(
        "Aproved token_y to portfolio for arber: {:#?}",
        approve_token_y_result_arbitrageur.is_success()
    );
    println!(
        "Aproved token_y to portfolio for admin: {:#?}",
        approve_token_y_result_admin.is_success()
    );
    // approve token_x
    let approve_token_x_call_data =
        arbiter_token_x.encode_function("approve", approve_portfolio_args)?;
    let approve_token_x_call_result_arbitrageur = arbitrageur.call_contract(
        &mut manager.environment,
        &arbiter_token_x,
        approve_token_x_call_data.clone(),
        Uint::from(0),
    );
    let approve_token_x_call_result_admin = admin.call_contract(
        &mut manager.environment,
        &arbiter_token_x,
        approve_token_x_call_data.clone(),
        Uint::from(0),
    );

    println!(
        "Aproved token_x to portfolio for arber: {:#?}",
        approve_token_x_call_result_arbitrageur.is_success()
    );
    println!(
        "Aproved token_x to portfolio for admin: {:#?}",
        approve_token_x_call_result_admin.is_success()
    );

    let create_pair_args = (
        recast_address(arbiter_token_x.address),
        recast_address(arbiter_token_y.address),
    );
    let create_pair_call_data = portfolio.encode_function("createPair", create_pair_args)?;
    let create_pair_result = admin.call_contract(
        &mut manager.environment,
        &portfolio,
        create_pair_call_data,
        Uint::from(0),
    );
    assert!(create_pair_result.is_success());

    let create_pair_unpack = manager.unpack_execution(create_pair_result)?;
    let pair_id: u32 = portfolio.decode_output("createPair", create_pair_unpack)?;
    println!("Created portfolio pair with Pair id: {:#?}", pair_id);

    let create_pool_builder = (
        pair_id,                         // pub pair_id: u32
        recast_address(admin.address()), // pub controller: ::ethers::core::types::Address
        100_u16,                         // pub priority_fee: u16,
        100_u16,                         // pub fee: u16,
        100_u16,                         // pub vol: u16,
        65535_u16,                       // pub dur: u16,
        0_u16,                           // pub jit: u16,
        10000000000000000000u128,        // pub max_price: u128,
        10000000000000000000u128,        // pub price: u128,
    );

    let create_pool_call = portfolio.encode_function("createPool", create_pool_builder)?;
    let create_pool_result = admin.call_contract(
        &mut manager.environment,
        &portfolio,
        create_pool_call,
        Uint::from(0),
    );
    assert!(create_pool_result.is_success());

    let create_pool_unpack = manager.unpack_execution(create_pool_result)?;
    let pool_id: u64 = portfolio.decode_output("createPool", create_pool_unpack)?;
    println!("created portfolio pool with pool ID: {:#?}", pool_id);

    let get_liquidity_args = (
        pool_id,         // pool_id: u64,
        1000000000_i128, // delta_liquidity: i128,
    );
    let get_liquidity_call = portfolio.encode_function("getLiquidityDeltas", get_liquidity_args)?;
    let get_liquidity_result = admin.call_contract(
        &mut manager.environment,
        &portfolio,
        get_liquidity_call,
        Uint::from(0),
    );
    assert!(get_liquidity_result.is_success());

    let get_liquidity_unpack = manager.unpack_execution(get_liquidity_result)?;
    let liquidity_deltas: (u128, u128) =
        portfolio.decode_output("getLiquidityDeltas", get_liquidity_unpack)?;

    let allocate_builder = (
        false,              // use_max: bool, // Usually set to false?
        pool_id,            // pool_id: u64,
        1000000000_u128,    // delta_liquidity: u128,
        liquidity_deltas.0, // max_delta_asset: u128,
        liquidity_deltas.1, // max_delta_quote: u128,
    );

    let allocate_call = portfolio.encode_function("allocate", allocate_builder)?;
    let allocate_result = admin.call_contract(
        &mut manager.environment,
        &portfolio,
        allocate_call,
        Uint::from(0),
    );
    assert!(allocate_result.is_success());

    let unpacked_allocate = manager.unpack_execution(allocate_result)?;
    let deltas: (u128, u128) = portfolio.decode_output("allocate", unpacked_allocate)?;
    println!("allocate result: {:#?}", deltas);

    // 498005301
    // 4980053002
    let get_amount_out_args: (u64, bool, U256, H160) = (
        pool_id,                        // pool_id: u64,
        true,                           // sell_asset: bool,
        U256::from(100000000),          // amount_in: ::ethers::core::types::U256,
        arbitrageur.address().into()           // swapper: ::ethers::core::types::Address,
    );

    let get_amount_out_call_data = portfolio.encode_function("getAmountOut", get_amount_out_args)?;
    println!("getAmountOut call data: {:#?}", hex::encode(get_amount_out_call_data.clone()));
    let get_amount_out_result = admin.call_contract(&mut manager.environment,
        &portfolio,
        get_amount_out_call_data,
        Uint::from(0),
    );
    assert!(get_amount_out_result.is_success());
    let unpacked_get_amount_out = manager.unpack_execution(get_amount_out_result)?;
    println!("getAmountOut result: {:#?}", hex::encode(unpacked_get_amount_out.clone()));
    let decoded_amount_out: u128 = portfolio.decode_output("getAmountOut", unpacked_get_amount_out)?;
    println!("getAmountOut result: {:#?}", decoded_amount_out);

    // for somereason we want this type
    let swap_args_order = (Order {
        use_max: false,                  // pub use_max: bool,
        pool_id,                         // pub pool_id: u64,
        input: 1000000000_u128,          // pub input: u128, 
        output: decoded_amount_out,      // pub output: u128,
        sell_asset: false,               // pub sell_asset: bool,
    });

    let swap_args = {(
        false,                  // pub use_max: bool,
        pool_id,                         // pub pool_id: u64,
        1000000000_u128,          // pub input: u128, 
        decoded_amount_out,      // pub output: u128,
        false,               // pub sell_asset: bool,
    )};

    // from cast
    // let magic_hexstring: Bytes = hex::decode("64f14ef20000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000001010000000100000000000000000000000000000000000000000000000000000000009896800000000000000000000000000000000000000000000000000000000005e66f6f0000000000000000000000000000000000000000000000000000000000000001".as_bytes())?.into_iter().collect();
    // let bytes_from_chisle: Bytes = "64f14ef2000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010100000001000000000000000000000000000000000000000000000000000000003b9aca00000000000000000000000000000000000000000000000000000000003aef626e00000000000000000000000000000000000000000000000000000000".as_bytes().to_owned().into_iter().collect();
    // // let byte_array:Bytes = hex::decode(bytes_from_chisle).expect("Decoding failed").into_iter().collect();
    // // println!("byte_array: {:#?}", byte_array);

    // selector: 64f14ef2
    // 0x0000000000000000000000000000000000000000000000000000000000000020
    // 0x00000000000000000000000000000000000000000000000000000000000000a0
    // 0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010100000001000000000000000000000000000000000000000000000000000000003b9aca00000000000000000000000000000000000000000000000000000000003aef626e000000000000000000000000000000000000000000000000000000000
    
    println!("swap args: {:#?}", swap_args);

    let portfolio_virtual = BaseContract::from(portfolio_virtual::PORTFOLIOVIRTUAL_ABI.clone());
    //let sig = 0x64f14ef2
    //let function_selector = portfolio_virtual.encode_with_selector(signature, args)
    // getting error on encoding


    //let thing = portfolio_virtual.encode(name, args)
    // let func = portfolio_virtual.abi().function("swap").unwrap();
    // let tokens: Vec<Token> = swap_args_order.into_tokens();
    // let len_tokens = tokens.len();
    // println!("Tokens: {:#?}", len_tokens);
    // let short_sig = hex::encode(func.short_signature());
    // let params: Vec<ParamType> = func.inputs.iter().map(|p| p.kind.clone()).collect();
    // let param_len = params.len();
    // assert_eq!(len_tokens, param_len);
    // println!("Short sig: {:#?}", short_sig);
    // println!("Tokens: {:#?}", tokens);
    // let thing = func.encode_input(&tokens)?;
    // println!("Thing: {:#?}", thing);
    let call_date: Bytes = portfolio_virtual.encode("swap", swap_args)?.into_iter().collect();
    // let swap_call_data = portfolio_virtual.encode("swap", swap_args_order)?;
    // println!("Thing1");
    let swap_result = admin.call_contract(&mut manager.environment,
        &portfolio,
        call_date.clone(),
        Uint::from(0),
    );
    println!("Bytes From Chisle {:#?}", call_date);
    println!("{:#?}", swap_result);
    // gas is different 
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use std::str::FromStr;

    use compiler::{assembler::Expression, codegen::Codegen, opcode::Opcode};
    use ethers::{abi::Address, prelude::BaseContract, types::H160, utils::parse_ether};
    use tokio::sync::mpsc::error;

    use super::*;

    #[test]
    fn test_create_pair_call() -> Result<(), Box<dyn std::error::Error>> {
        let decimals = 18_u8;
        let wad: U256 = U256::from(10_i64.pow(decimals as u32));
        // Create a `SimulationManager` that runs simulations in their `SimulationEnvironment`.
        let mut manager = SimulationManager::new();
        // Deploy the contracts
        let SimulationContracts(arbiter_token_x, arbiter_token_y, portfolio, _liquid_exchange_xy) =
            deploy_portfolio_sim_contracts(&mut manager, wad)?;

        let admin = manager.agents.get("admin").unwrap();

        let create_pair_args = (
            recast_address(arbiter_token_x.address),
            recast_address(arbiter_token_y.address),
        );
        let create_pair_call_data = portfolio.encode_function("createPair", create_pair_args)?;
        let create_pair_result = admin.call_contract(
            &mut manager.environment,
            &portfolio,
            create_pair_call_data,
            Uint::from(0),
        );
        assert_eq!(create_pair_result.is_success(), true);
        let create_pair_unpack = manager.unpack_execution(create_pair_result)?;
        let pair_id: u32 = portfolio.decode_output("createPair", create_pair_unpack)?;
        println!("Created portfolio pair with Pair id: {:#?}", pair_id);

        // Check the pair was created
        let encoded_pair = portfolio.encode_function("pairs", pair_id)?;
        let pairs = admin.call_contract(
            &mut manager.environment,
            &portfolio,
            encoded_pair,
            Uint::from(0),
        );
        let unpacked_pairs = manager.unpack_execution(pairs)?;
        let decoded_pairs_response: (H160, u8, H160, u8) =
            portfolio.decode_output("pairs", unpacked_pairs)?;

        assert!(decoded_pairs_response.0 == arbiter_token_x.address.into());
        assert!(decoded_pairs_response.2 == arbiter_token_y.address.into());
        assert!(decoded_pairs_response.1 == decimals);
        assert!(decoded_pairs_response.3 == decimals);

        Ok(())
    }

    #[test]
    fn test_create_pool_call() -> Result<(), Box<dyn std::error::Error>> {
        let decimals = 18_u8;
        let wad: U256 = U256::from(10_i64.pow(decimals as u32));
        // Create a `SimulationManager` that runs simulations in their `SimulationEnvironment`.
        let mut manager = SimulationManager::new();
        // Deploy the contracts
        let SimulationContracts(arbiter_token_x, arbiter_token_y, portfolio, _liquid_exchange_xy) =
            deploy_portfolio_sim_contracts(&mut manager, wad)?;

        let admin = manager.agents.get("admin").unwrap();

        let create_pair_args = (
            recast_address(arbiter_token_x.address),
            recast_address(arbiter_token_y.address),
        );
        let create_pair_call_data = portfolio.encode_function("createPair", create_pair_args)?;
        let create_pair_result = admin.call_contract(
            &mut manager.environment,
            &portfolio,
            create_pair_call_data,
            Uint::from(0),
        );
        assert_eq!(create_pair_result.is_success(), true);

        let create_pair_unpack = manager.unpack_execution(create_pair_result)?;
        let pair_id: u32 = portfolio.decode_output("createPair", create_pair_unpack)?;
        println!("Created portfolio pair with Pair id: {:#?}", pair_id);

        // let pair_id: u32 = pair_id.into_iter().collect().to_string().parse::<u32>().unwrap();

        let create_pool_builder = (
            pair_id,                         // pub pair_id: u32
            recast_address(admin.address()), // pub controller: ::ethers::core::types::Address
            100_u16,                         // pub priority_fee: u16,
            100_u16,                         // pub fee: u16,
            100_u16,                         // pub vol: u16,
            65535_u16,                       // pub dur: u16,
            0_u16,                           // pub jit: u16,
            u128::MAX,                       // pub max_price: u128,
            1_u128,                          // pub price: u128,
        );

        let create_pool_call = portfolio.encode_function("createPool", create_pool_builder)?;
        let create_pool_result = admin.call_contract(
            &mut manager.environment,
            &portfolio,
            create_pool_call,
            Uint::from(0),
        );
        assert!(create_pool_result.is_success());


        Ok(())
    }

    #[test]
    fn allocate_test() -> Result<(), Box<dyn std::error::Error>> {
        let decimals = 18_u8;
        let wad: U256 = U256::from(10_i64.pow(decimals as u32));
        // Create a `SimulationManager` that runs simulations in their `SimulationEnvironment`.
        let mut manager = SimulationManager::new();
        // Deploy the contracts
        let SimulationContracts(arbiter_token_x, arbiter_token_y, portfolio, _liquid_exchange_xy) =
            deploy_portfolio_sim_contracts(&mut manager, wad)?;

        let admin = manager.agents.get("admin").unwrap();

        let create_pair_args = (
            recast_address(arbiter_token_x.address),
            recast_address(arbiter_token_y.address),
        );
        let create_pair_call_data = portfolio.encode_function("createPair", create_pair_args)?;
        let create_pair_result = admin.call_contract(
            &mut manager.environment,
            &portfolio,
            create_pair_call_data,
            Uint::from(0),
        );

        let create_pair_unpack = manager.unpack_execution(create_pair_result)?;
        let pair_id: u32 = portfolio.decode_output("createPair", create_pair_unpack)?;
        println!("Created portfolio pair with Pair id: {:#?}", pair_id);

        let create_pool_builder = (
            pair_id,                         // pub pair_id: u32
            recast_address(admin.address()), // pub controller: ::ethers::core::types::Address
            100_u16,                         // pub priority_fee: u16,
            100_u16,                         // pub fee: u16,
            100_u16,                         // pub vol: u16,
            65535_u16,                       // pub dur: u16,
            0_u16,                           // pub jit: u16,
            u128::MAX,                       // pub max_price: u128,
            1_u128,                          // pub price: u128,
        );

        let create_pool_call = portfolio.encode_function("createPool", create_pool_builder)?;
        let create_pool_result = admin.call_contract(
            &mut manager.environment,
            &portfolio,
            create_pool_call,
            Uint::from(0),
        );
        assert!(create_pool_result.is_success());
        let create_pool_unpack = manager.unpack_execution(create_pool_result)?;
        let pool_id: u64 = portfolio.decode_output("createPool", create_pool_unpack)?;
        println!("created portfolio pool with pool ID: {:#?}", pool_id);

        let allocate_builder = (
            true,      // use_max: bool,
            pool_id,   // pool_id: u64,
            100_u64,   // delta_liquidity: u128,
            1000_u128, // max_delta_asset: u128,
            1000_u128, // max_delta_quote: u128,
        );

        let allocate_call = portfolio.encode_function("allocate", allocate_builder)?;
        let allocate_result = admin.call_contract(
            &mut manager.environment,
            &portfolio,
            allocate_call,
            Uint::from(0),
        );
        println!("allocate result: {:#?}", allocate_result.is_success());
        Ok(())
    }
}