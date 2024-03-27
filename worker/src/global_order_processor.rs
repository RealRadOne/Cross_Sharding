// Copyright(C) Heena Nagda.
use crate::worker::SerializedBatchDigestMessage;
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use log::info;

/// Indicates a serialized `WorkerMessage::GlobalOrder` message.
pub type SerializedGlobalOrderMessage = Vec<u8>;

#[derive(Debug)]
/// Hashes and stores batches, it then outputs the batch's digest.
pub struct GlobalOrderProcessor;

impl GlobalOrderProcessor {
    pub fn spawn(
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_global_order: Receiver<SerializedGlobalOrderMessage>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<SerializedBatchDigestMessage>,
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
    ) {
        tokio::spawn(async move {
            while let Some(global_order) = rx_global_order.recv().await {
                info!("Received Global order to process further. own_digest = {:?}", own_digest);
                // Hash the batch.
                let digest = Digest(Sha512::digest(&global_order).as_slice()[..32].try_into().unwrap());

                // Store the batch.
                store.write(digest.to_vec(), global_order).await;

                // Deliver the batch's digest.
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id),
                };
                info!("Sending digest to primary connector. own_digest = {:?}", own_digest);
                let message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");
                tx_digest
                    .send(message)
                    .await
                    .expect("Failed to send digest");
            }
        });
    }
}
