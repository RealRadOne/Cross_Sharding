use anyhow::{Context, Result};
use log::{info, warn, error};
use smallbank::SmallBankTransactionHandler;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use std::fs::OpenOptions;
use std::io::Write;
use std::net::SocketAddr;
use tokio::net::TcpStream;


pub type Transaction = Vec<u8>;
type UserId = u64;
type ShardId = u32;

pub struct Coordinator {
    rx_transaction: Receiver<Transaction>,
    nodes: Vec<SocketAddr>,
    locks: Arc<Mutex<HashMap<UserId, bool>>>,
    num_shards: u32,
    sb_handler: SmallBankTransactionHandler,
}

impl Coordinator {
    pub fn new(
        rx_transaction: Receiver<Transaction>,
        nodes: Vec<SocketAddr>,
        num_shards: u32,
        size: usize,
        n_users: u64,
        skew_factor: f64,
        prob_choose_mtx: f64,
    ) -> Self {
        let sb_handler = SmallBankTransactionHandler::new(size, n_users, skew_factor, prob_choose_mtx);

        Coordinator {
            rx_transaction,
            nodes,
            locks: Arc::new(Mutex::new(HashMap::new())),
            num_shards,
            sb_handler,
        }
    }

    async fn ping_nodes(&self) -> Result<()> {
        for (i, node) in self.nodes.iter().enumerate() {
            match TcpStream::connect(node).await {
                Ok(_) => {
                    info!("Node {} at address {} is available", i, node);
                }
                Err(e) => {
                    error!("Failed to connect to node {} at address {}: {}", i, node, e);
                }
            }
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Coordinator started");

        self.ping_nodes().await?;
        
        let mut shard_logs = HashMap::new();
        for shard_id in 0..self.num_shards {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("shard_{}_transactions.log", shard_id))
                .context(format!("Failed to open log file for shard {}", shard_id))?;
            shard_logs.insert(shard_id, file);
        }

        while let Some(transaction) = self.rx_transaction.recv().await {
            let user_id = self.extract_user_id(&transaction);
            let shard_id = self.get_shard_id(user_id);

            if self.acquire_locks(&transaction).await {
                if let Some(file) = shard_logs.get_mut(&shard_id) {
                    if let Err(e) = writeln!(file, "{}", String::from_utf8_lossy(&transaction)) {
                        error!("Failed to write transaction to shard {} log: {}", shard_id, e);
                    } else {
                        //info!("Transaction written to shard {} log", shard_id);
                    }
                }
                self.release_locks(&transaction).await;
            } else {
                warn!("Failed to acquire locks for transaction");
            }
        }
        Ok(())
    }

    fn extract_user_id(&self, transaction: &Transaction) -> UserId {
        self.sb_handler.get_transaction_uid(transaction.clone().into())
    }

    fn get_shard_id(&self, user_id: UserId) -> ShardId {
        user_id as u32 % self.num_shards
    }

    async fn acquire_locks(&self, transaction: &Transaction) -> bool {
        let mut locks = self.locks.lock().unwrap();
        let user_id = self.extract_user_id(transaction);
        
        if *locks.get(&user_id).unwrap_or(&false){
            return false;
        }
        locks.insert(user_id, true);
        true
    }

    async fn release_locks(&self, transaction: &Transaction) {
        let mut locks = self.locks.lock().unwrap();
        let user_id = self.extract_user_id(transaction);
        locks.insert(user_id, false);
    }
}