// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{Round, SerializedBatchDigestMessage};
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug)]
pub struct GlobalOrderMakerMessage {
    /// A serialized `WorkerMessage::Batch` message.
    pub batch: SerializedBatchMessage,
    /// Whether we are processing our own batches or the batches of other nodes.
    pub own_digest: bool,
}

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct GlobalOrderMaker{
    /// Our worker's id.
    id: WorkerId,
    /// The persistent storage.
    mut store: Store,
    /// Current round.
    mut current_round: Round,
    /// Input channel to receive updated current round.
    mut rx_round: Receiver<Round>,
    /// Input channel to receive batches.
    mut rx_batch: Receiver<SerializedBatchMessage>,
    /// Output channel to send out Global Ordered batches' digests.
    tx_digest: Sender<SerializedBatchDigestMessage>,
}

impl GlobalOrderMaker {
    /// Spawn a new GlobalOrderMaker.
    pub fn spawn(
        id: WorkerId,
        mut store: Store,
        mut current_round: Round,
        mut rx_round: Receiver<Round>,
        mut rx_batch: Receiver<SerializedBatchMessage>,
        tx_digest: Sender<SerializedBatchDigestMessage>,
    ) {
        tokio::spawn(async move {
            Self {
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
        while let Some(batch) = rx_batch.recv().await {
            // Hash the batch.
            let digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());

            // Store the batch.
            store.write(digest.to_vec(), batch).await;

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