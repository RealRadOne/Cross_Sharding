use tokio::sync::mpsc::Sender;
use log::{info, warn};
use rand::Rng;
use tokio::time::{interval, Duration, Instant};
use smallbank::SmallBankTransactionHandler;
use anyhow::Result;
use crate::coordinator::Transaction;

pub struct Client {
    size: usize,
    sb_handler: SmallBankTransactionHandler,
    rate: u64,
}

impl Client {
    pub fn new(size: usize, sb_handler: SmallBankTransactionHandler, rate: u64) -> Self {
        Client {
            size,
            sb_handler,
            rate,
        }
    }

    pub async fn send(&self, tx_coordinator: Sender<Transaction>) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        if self.size < 9 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 9 bytes",
            ));
        }

        let burst = self.rate / PRECISION;
        let mut counter = 0;
        let mut r = rand::thread_rng().gen();
        let interval = interval(Duration::from_millis(BURST_DURATION));
        const MAX_TRANSACTIONS: u64 = 1000;
        let mut total_sent = 0;
        
        tokio::pin!(interval);

        info!("Start sending transactions");

        'main: loop {
            if total_sent >= MAX_TRANSACTIONS {
                break;
            }
            interval.as_mut().tick().await;
            let now = Instant::now();
            
            let mut x : u64 = 0;
            while x <= burst {
                let tx_uid;

                if x == counter % burst {
                    tx_uid = counter;
                    //info!("Sending sample transaction {}", tx_uid);
                } else {
                    r += 1;
                    tx_uid = r;
                }
                let bytes = self.sb_handler.get_next_transaction(x == counter % burst, tx_uid);
                
                if let Err(e) = tx_coordinator.send(bytes.to_vec()).await {
                    warn!("Failed to send transaction to coordinator: {}", e);
                    break 'main;
                }
                //info!("Sent transaction {} to coordinator", tx_uid);

                x += 1;
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                warn!("Transaction rate too high for this client");
            }
            counter += 1;
            total_sent += 1;
        }
        Ok(())
    }
}