// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use futures::StreamExt;
use log::{info, warn};
use rand::Rng;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use smallbank::SmallBankTransactionHandler;
use std::collections::{HashMap, HashSet};
use bytes::Bytes;


#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Narwhal and Tusk.")
        .args_from_usage("<ADDR> 'The network address of the node where to send txs'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--n_users=<INT> 'Number of users in small-bank'")
        .args_from_usage("--shards=[STRING]... 'list of Shard data'")
        .args_from_usage("--skew_factor=<FLOAT> 'Skew factor for users in small-bank'")
        .args_from_usage("--prob_choose_mtx=<FLOAT> 'Probability of choosing modifying transactions in small-bank'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let target = matches
        .value_of("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid socket address format")?;
    let size = matches
        .value_of("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let n_users = matches
        .value_of("n_users")
        .unwrap()
        .parse::<u64>()
        .context("Number of users in small-bank must be a non-negative integer")?;
    let shards = matches
        .values_of("shards")
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.parse::<String>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid shard assignment format")?;
    let skew_factor = matches
        .value_of("skew_factor")
        .unwrap()
        .parse::<f64>()
        .context("Skew factor for users in small-bank must be a non-negative integer")?;
    let prob_choose_mtx = matches
        .value_of("prob_choose_mtx")
        .unwrap()
        .parse::<f64>()
        .context("Probability of choosing modifying transactions in small-bank must be a non-negative integer")?;
    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let nodes = matches
        .values_of("nodes")
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid socket address format")?;


    // Shard format: eg: 2 workers, 3 parties => 2 shards
    // [#workers, lower-range-of-shard-1, lower-range-of-shard-2, 
    // addr-of-party-1-worker-1-assigned-to-shard-1, addr-of-party-1-worker-2-assigned-to-shard-2, 
    // addr-of-party-2-worker-1-assigned-to-shard-1, addr-of-party-2-worker-2-assigned-to-shard-2, 
    // addr-of-party-3-worker-1-assigned-to-shard-1, addr-of-party-3-worker-2-assigned-to-shard-2]
    let mut shard_lower_range: Vec<u32> = Vec::new();
    let mut shard_assignment: HashMap<u32, Vec<SocketAddr>> = HashMap::new();
    let n_workers = shards[0].parse::<usize>().unwrap();
    
    for i in 1..1+n_workers{
        shard_lower_range.push(shards[i].parse::<u32>().unwrap());
        shard_assignment.insert(shard_lower_range[i-1], Vec::<SocketAddr>::new(),);
    }
    for i in 1+n_workers..shards.len(){
        let idx = i-(1+n_workers);
        shard_assignment.entry(shard_lower_range[idx%n_workers]).or_insert_with(Vec::new).push(shards[i].parse::<SocketAddr>().unwrap());
    }

    info!("Node address: {}", target);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("# users: {}", n_users);

    // NOTE: This log entry is used to compute performance.
    info!("shard_lower_range = {:?}", shard_lower_range);
    info!("shard_assignment = {:?}", shard_assignment);

    // NOTE: This log entry is used to compute performance.
    info!("Skew Factor: {}", skew_factor);

    // NOTE: This log entry is used to compute performance.
    info!("Probability of choosing modifying transactions : {}", prob_choose_mtx);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);

    let sb_handler = SmallBankTransactionHandler::new(size, n_users, skew_factor, prob_choose_mtx);

    let client = Client {
        target,
        size,
        sb_handler,
        rate,
        nodes,
        shard_lower_range,
        shard_assignment,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    target: SocketAddr,
    size: usize,
    sb_handler: SmallBankTransactionHandler,
    rate: u64,
    nodes: Vec<SocketAddr>,
    shard_lower_range: Vec<u32>,
    shard_assignment: HashMap<u32, Vec<SocketAddr>>,
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // The transaction size must be at least 16 bytes to ensure all txs are different.
        if self.size < 9 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 9 bytes",
            ));
        }

        // // connect to mempool
        let mut writers_readers = HashMap::new();
        for worker_addr_vec in self.shard_assignment.values(){
            for worker_address in worker_addr_vec{
                info!("worker_address = {:?}", worker_address);
                let stream = TcpStream::connect(worker_address)
                    .await
                    .context(format!("failed to connect to {}", worker_address))?; 
                let transport = Framed::new(stream, LengthDelimitedCodec::new());
                let (mut writer, mut reader) = transport.split();
                writers_readers.insert(worker_address, (writer, reader));
            }
        }

        // Connect to the mempool.
        // let stream = TcpStream::connect(self.target)
        //     .await
        //     .context(format!("failed to connect to {}", self.target))?;

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        let mut counter = 0;
        let mut r = rand::thread_rng().gen();
        // let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // Create a pool of pending transaction responses
        let mut waiting: HashSet<u64> = HashSet::new();

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            let mut x : u64 = 0;
            while x <= burst {
                let tx_uid;
                if x == counter % burst{
                    tx_uid = counter;
                    info!("Sending sample transaction {}", tx_uid);
                }
                else{
                    r += 1;
                    tx_uid = r;
                }
                let bytes = self.sb_handler.get_next_transaction(x == counter % burst, tx_uid);
                waiting.insert(tx_uid);
                info!("for fairness Sending tx {}", tx_uid);
                
                // get the target address besed on dependency
                let dependency: (char, Vec<u32>) = self.sb_handler.get_transaction_dependency(bytes.clone());
                let mut target_addr: HashSet<SocketAddr> = HashSet::new();
                for dep in &dependency.1{
                    let mut idx: usize = 0;
                    for lower_range in &self.shard_lower_range{
                        if dep < lower_range{ break;}
                        idx += 1;
                    }
                    for addr in &self.shard_assignment[&self.shard_lower_range[idx-1]]{
                        target_addr.insert(*addr);
                    }
                }
                // info!("target_addr = {:?}", target_addr);

                for addr in target_addr{
                    let (writer, reader) = writers_readers.get_mut(&addr).unwrap();

                    // if let Err(e) = (*writer).send(bytes.clone()).await {
                    //     warn!("Failed to send transaction: {}", e);
                    //     break 'main;
                    // }

                    tokio::select! {
                        // Sending a transaction
                        request = (*writer).send(bytes.clone()) => {
                            x += 1;
                            match request {
                                Ok(()) => {
                                    // TODO: Add this even in the Log file
                                }
                                Err(e) =>{
                                    warn!("Failed to send transaction: {}", e);
                                    break 'main;
                                }
                            }
                        }

                        // receiving ack
                        response = reader.next() => {
                            match response {
                                Some(Ok(bytes)) => {
                                    // remove awaited transaction uid from the waiting pool
                                    let tx_uid = u64::from_be_bytes(bytes[..8].try_into().unwrap());
                                    if waiting.contains(&tx_uid) {
                                        info!("for fairness Receiving tx ack {}", tx_uid);
                                        waiting.remove(&tx_uid);
                                        // TODO: Add this in the log file
                                    }
                                },
                                _ => {
                                    // TODO: Something has gone wrong (either the channel dropped or we failed to read from it).                            
                                }
                            }
                        }
                    }
                }
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
            counter += 1;
        }
        Ok(())
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }
}
