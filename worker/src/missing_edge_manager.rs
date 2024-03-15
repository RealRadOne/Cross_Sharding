// Copyright(C) Heena Nagda.
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use serde::{Deserialize, Serialize};
use crypto::Digest;
use store::Store;
use config::Committee;

#[derive(Debug, Serialize, Deserialize)]
enum EdgeManagerFormat {
    MissingEdgeFormat(Vec<Digest>),
}

#[derive(Clone)]
pub struct MissingEdgeManager {
    store: Store,
    committee: Committee,  // if (self.local_order_dags.len() as u32) < self.committee.quorum_threshold(){
}

impl MissingEdgeManager {
    pub fn new(store: Store, committee: Committee) -> MissingEdgeManager {
        MissingEdgeManager{
            store,
            committee,
        }
    }

    pub async fn add_missing_edge(v1: Digest, v2: Digest) {
        let edge_pair_fwd: Vec<Digest> = vec![v1, v2];
        let edge_pair_rev: Vec<Digest> = vec![v2, v1];

        let message_fwd = EdgeManagerFormat::MissingEdgeFormat(edge_pair_fwd);
        let message_rev = EdgeManagerFormat::MissingEdgeFormat(edge_pair_rev);

        let serialized_fwd = bincode::serialize(&message_fwd).expect("Failed to serialize missing edge pair (fwd) while adding to the store");
        let serialized_rev = bincode::serialize(&message_rev).expect("Failed to serialize missing edge pair (rev) while adding to the store");

        // check if this edge is already exists
    }

    pub async fn add_updated_edge(to: Digest, from: Digest) {

    }

    pub async fn is_missing_edge_updated(to: Digest, from: Digest) -> bool {

        return true;
    }
}