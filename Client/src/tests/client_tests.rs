use std::fs::{File, read_to_string};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tempfile::NamedTempFile;
use anyhow::Result;
use log::info;
use smallbank::SmallBankTransactionHandler;
use crate::Client;

#[tokio::test]
async fn test_client_transaction_generation() -> Result<()> {
    // Temporary file for testing
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let temp_file_path = temp_file.path().to_path_buf();

    // Set up the client
    let size = 64; // Example size in bytes
    let n_users = 10; // Example number of users
    let skew_factor = 0.5; // Example skew factor
    let prob_choose_mtx = 0.8; // Example probability
    let rate = 20; // Example rate (txs/s)

    let sb_handler = SmallBankTransactionHandler::new(size, n_users, skew_factor, prob_choose_mtx);
    let client = Client {
        size,
        sb_handler,
        rate,
    };

    // Run the client's `send` method
    let send_result = {
        let temp_file_path_clone = temp_file_path.clone();
        tokio::spawn(async move {
            let mut file = File::create(&temp_file_path_clone)?;
            client.send_to_file(&mut file).await
        })
        .await
    };

    assert!(send_result.is_ok(), "Client send returned an error: {:?}", send_result);

    // Read the transactions from the temporary file
    let transactions = read_to_string(temp_file_path)
        .expect("Failed to read temp file")
        .lines()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    // Check basic properties of the transactions
    assert!(!transactions.is_empty(), "No transactions were written to the file");

    for (i, tx) in transactions.iter().enumerate() {
        assert!(
            tx.len() >= size,
            "Transaction {} is smaller than expected size ({} bytes)",
            i,
            size
        );
    }

    Ok(())
}
