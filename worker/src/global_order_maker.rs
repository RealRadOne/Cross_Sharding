// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{Round, SerializedBatchDigestMessage, WorkerMessage};
use config::{WorkerId, Committee};
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use log::{info};
use graph::{LocalOrderGraph, GlobalOrderGraph};

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

#[derive(Debug)]
pub struct GlobalOrderMakerMessage {
    /// A serialized `WorkerMessage::Batch` message.
    pub batch: SerializedBatchMessage,
    /// Whether we are processing our own batches or the batches of other nodes.
    pub own_digest: bool,
    /// Round number in which this batch was created
    pub round: Round,
}

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct GlobalOrderMaker{
    /// The committee information.
    committee: Committee,
    /// Our worker's id.
    id: WorkerId,
    /// The persistent storage.
    mut store: Store,
    /// Current round.
    mut current_round: Round,
    /// Local orders
    mut local_order_dags: Vec<DiGraphMap<u16, u8>>,
    /// Input channel to receive updated current round.
    mut rx_round: Receiver<Round>,
    /// Input channel to receive batches.
    mut rx_batch: Receiver<GlobalOrderMakerMessage>,
    /// Output channel to send out Global Ordered batches' digests.
    tx_digest: Sender<SerializedBatchDigestMessage>,
}

impl GlobalOrderMaker {
    /// Spawn a new GlobalOrderMaker.
    pub fn spawn(
        committee: Committee,
        id: WorkerId,
        mut store: Store,
        mut current_round: Round,
        mut rx_round: Receiver<Round>,
        mut rx_batch: Receiver<GlobalOrderMakerMessage>,
        tx_digest: Sender<SerializedBatchDigestMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                id,
                store,
                1,
                rx_round,
                rx_batch,
                tx_digest,
            }
            .run()
            .await;
        });
    }

    /// Main loop.
    async fn run(&mut self) {
        while let Some(GlobalOrderMakerMessage { batch, own_digest, batch_round }) = self.rx_batch.recv().await {
            match self.rx_round.poll_recv(&mut cx) {
                Poll::Ready(round) => {
                    info!("Update round received : {}", round.unwrap());
                    self.current_round = round.unwrap;
                    self.local_order_dags.clear();
                },
                _ => (),
            };

            let send_order: bool = false;
            // creating a Global Order
            if batch_round == self.current_round && self.local_order_dags.len() < self.committee.quorum_threshold(){
                match bincode::deserialize(&batch).unwrap() {
                    WorkerMessage::Batch(batch) => {
                        self.local_order_dags.push(LocalOrderGraph::get_dag_deserialized(batch));
                        if self.local_order_dags.len() >= self.committee.quorum_threshold(){
                            send_order = true;
                        }
                    },
                    _ => panic!("Unexpected message"),
                }
            }

            if send_order{
                /// TODO: Pending and fixed transaction threshold
                // create a Global Order based on n-f received local orders 
                let global_order_graph_obj: GlobalOrderGraph = GlobalOrderGraph::new(local_order_dags, 3.0, 2.5);
                let global_order_dag_serialized = global_order_graph_obj.get_dag_serialized();

                // Hash the batch.
                let digest = Digest(Sha512::digest(&global_order_dag_serialized).as_slice()[..32].try_into().unwrap());

                // Store the batch.
                store.write(digest.to_vec(), global_order_dag_serialized).await;

                // Deliver the batch's digest.
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id),
                };
                let message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");
                tx_digest
                    .send(message)
                    .await
                    .expect("Failed to send digest");
            }
        }
    }
}
