mod benchmark_client;
mod coordinator;

use crate::benchmark_client::Client;
use crate::coordinator::Coordinator;

use anyhow::{Context, Result};
use tokio::sync::mpsc::channel;
use smallbank::SmallBankTransactionHandler;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use log::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Set up logging
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    // Parse command line arguments
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Narwhal and Tusk.")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--n_users=<INT> 'Number of users in small-bank'")
        .args_from_usage("--skew_factor=<FLOAT> 'Skew factor for users in small-bank'")
        .args_from_usage("--prob_choose_mtx=<FLOAT> 'Probability of choosing modifying transactions in small-bank'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--num_shards=<INT> 'Number of shards to use'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    // Parse arguments
    let size = matches.value_of("size").unwrap().parse::<usize>()?;
    let n_users = matches.value_of("n_users").unwrap().parse::<u64>()?;
    let skew_factor = matches.value_of("skew_factor").unwrap().parse::<f64>()?;
    let prob_choose_mtx = matches.value_of("prob_choose_mtx").unwrap().parse::<f64>()?;
    let rate = matches.value_of("rate").unwrap().parse::<u64>()?;
    let num_shards = matches.value_of("num_shards").unwrap_or("4").parse::<u32>()?;

    // Create channel for communication
    let (tx_transaction, rx_transaction) = channel(1000);

    // Initialize SmallBankTransactionHandler
    let sb_handler = SmallBankTransactionHandler::new(size, n_users, skew_factor, prob_choose_mtx);

    // Create and spawn coordinator
    let mut coordinator = Coordinator::new(
        rx_transaction,
        num_shards,
        size,
        n_users,
        skew_factor,
        prob_choose_mtx,
    );

    let coordinator_handle = tokio::spawn(async move {
        coordinator.run().await
    });

    // Create and run client
    let client = Client::new(size, sb_handler, rate);
    let client_handle = tokio::spawn(async move {
        client.send(tx_transaction).await
    });

    // Wait for both tasks to complete
    tokio::try_join!(coordinator_handle, client_handle)?;

    Ok(())
}