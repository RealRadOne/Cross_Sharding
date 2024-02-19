// Copyright(C) Heena Nagda.
use crate::global_order_quorum_waiter::GlobalOrderQuorumWaiterMessage;
use crate::worker::{Round, SerializedBatchDigestMessage, WorkerMessage};
use config::{WorkerId, Committee};
use crypto::Digest;
use crypto::PublicKey;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use std::net::SocketAddr;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use log::{info};
use graph::{LocalOrderGraph, GlobalOrderGraph};
use petgraph::prelude::DiGraphMap;
use network::ReliableSender;
use bytes::Bytes;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;
pub type Transaction = Vec<u8>;
pub type GlobalOrder = Vec<Transaction>;

#[derive(Debug)]
pub struct GlobalOrderMakerMessage {
    /// A serialized `WorkerMessage::Batch` message.
    pub batch: SerializedBatchMessage,
    /// Whether we are processing our own batches or the batches of other nodes.
    pub own_digest: bool,
}

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct GlobalOrderMaker{
    /// The committee information.
    committee: Committee,
    /// Our worker's id.
    id: WorkerId,
    /// The persistent storage.
    store: Store,
    /// Current round.
    current_round: Round,
    /// Local orders
    local_order_dags: Vec<DiGraphMap<u16, u8>>,
    /// Input channel to receive updated current round.
    rx_round: Receiver<Round>,
    /// Input channel to receive batches.
    rx_batch: Receiver<GlobalOrderMakerMessage>,
    // /// Output channel to send out Global Ordered batches' digests.
    // tx_digest: Sender<SerializedBatchDigestMessage>,
    /// Output channel to deliver sealed Global Order to the `GlobalOrderQuorumWaiter`.
    tx_message: Sender<GlobalOrderQuorumWaiterMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,
}

impl GlobalOrderMaker {
    /// Spawn a new GlobalOrderMaker.
    pub fn spawn(
        committee: Committee,
        id: WorkerId,
        mut store: Store,
        mut rx_round: Receiver<Round>,
        mut rx_batch: Receiver<GlobalOrderMakerMessage>,
        // tx_digest: Sender<SerializedBatchDigestMessage>,
        tx_message: Sender<GlobalOrderQuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                id,
                store,
                current_round: 1,
                local_order_dags: Vec::new(),
                rx_round,
                rx_batch,
                // tx_digest,
                tx_message,
                workers_addresses,
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Main loop.
    async fn run(&mut self) {
        while let Some(GlobalOrderMakerMessage { batch, own_digest }) = self.rx_batch.recv().await {
            // Get the new round number if advanced (non blocking)
            match self.rx_round.try_recv(){
                Ok(round) => {
                    info!("Update round received : {}", round);
                    self.current_round = round;
                    self.local_order_dags.clear();
                },
                _ => (),
            }

            info!("current_round = {:?}", self.current_round);

            let mut send_order: bool = false;
            // creating a Global Order
            if (self.local_order_dags.len() as u32) < self.committee.quorum_threshold(){
                info!("global_order_maker-1");
                match bincode::deserialize(&batch).unwrap() {
                    WorkerMessage::Batch(mut batch) => {
                        info!("global_order_maker-2");
                        match batch.pop() {
                            Some(batch_round_vec) => {
                                info!("global_order_maker-3");
                                let batch_round_arr = batch_round_vec.try_into().unwrap_or_else(|batch_round_vec: Vec<u8>| panic!("Expected a Vec of length {} but it was {}", 8, batch_round_vec.len()));
                                let batch_round = u64::from_le_bytes(batch_round_arr);
                                // 
                                if batch_round == self.current_round {
                                    info!("global_order_maker-4");
                                    self.local_order_dags.push(LocalOrderGraph::get_dag_deserialized(batch));
                                    if (self.local_order_dags.len() as u32) >= self.committee.quorum_threshold(){
                                        info!("global_order_maker-5");
                                        send_order = true;
                                    }
                                }
                            }
                            _ => panic!("Unexpected batch round found"),
                        }
                    },
                    _ => panic!("Unexpected message"),
                }
            }

            if send_order{
                /// TODO: Pending and fixed transaction threshold
                // create a Global Order based on n-f received local orders 
                let global_order_graph_obj: GlobalOrderGraph = GlobalOrderGraph::new(self.local_order_dags.clone(), 3.0, 2.5);
                let global_order_graph = global_order_graph_obj.get_dag_serialized();
                
                
                let message = WorkerMessage::GlobalOrder(global_order_graph);
                let serialized = bincode::serialize(&message).expect("Failed to serialize global order graph");

                // Broadcast the batch through the network.
                let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
                let bytes = Bytes::from(serialized.clone());
                let handlers = self.network.broadcast(addresses, bytes).await;
                
                // Send the batch through the deliver channel for further processing.
                self.tx_message
                .send(GlobalOrderQuorumWaiterMessage {
                    global_order: serialized,
                    handlers: names.into_iter().zip(handlers.into_iter()).collect(),
                })
                .await
                .expect("Failed to deliver global order");
                
                
                
                
                
                // ///////////////////////old//////////////////////
                // let message = WorkerMessage::Batch(global_order_graph);
                // let serialized = bincode::serialize(&message).expect("Failed to serialize global order graph");

                // // Hash the batch.
                // let digest = Digest(Sha512::digest(&serialized).as_slice()[..32].try_into().unwrap());

                // // Store the batch.
                // self.store.write(digest.to_vec(), serialized).await;

                // // Deliver the batch's digest.
                // let message = WorkerPrimaryMessage::OurBatch(digest, self.id);
                // // let message = match own_digest {
                // //     true => WorkerPrimaryMessage::OurBatch(digest, self.id),
                // //     false => WorkerPrimaryMessage::OthersBatch(digest, self.id),
                // // };
                // info!("global_order_maker- Sending message to primary connector");
                // let message = bincode::serialize(&message)
                //     .expect("Failed to serialize our own worker-primary message");
                // self.tx_digest
                //     .send(message)
                //     .await
                //     .expect("Failed to send digest");
            }
        }
    }
}
