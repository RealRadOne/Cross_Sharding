// Copyright(C) Heena Nagda.
use serde::{Deserialize, Serialize};
use store::Store;
use config::Committee;
use log::error;

#[derive(Clone, Debug, Serialize, Deserialize)]
enum EdgeManagerFormat {
    MissingEdgeFormat(Vec<u16>),
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
    // self.round.to_le_bytes()
    // let batch_round = u64::from_le_bytes(batch_round_arr);
    pub async fn add_missing_edge(&mut self, v1: u16, v2: u16) {

        let message_fwd = EdgeManagerFormat::MissingEdgeFormat(vec![v1, v2]);
        let message_rev = EdgeManagerFormat::MissingEdgeFormat(vec![v2, v1]);

        let serialized_fwd = bincode::serialize(&message_fwd).expect("Failed to serialize missing edge (fwd) while adding to the store");
        let serialized_rev = bincode::serialize(&message_rev).expect("Failed to serialize missing edge (rev) while adding to the store");


        // check if this edge is already exists
        match self.store.read(serialized_fwd.to_vec()).await {
            Ok(Some(_count_arr)) => (),
            Ok(None) => {
                let count: u64 = 0;
                self.store.write(serialized_fwd, count.to_le_bytes().to_vec()).await;
                self.store.write(serialized_rev, count.to_le_bytes().to_vec()).await;
            },
            Err(e) => error!("Error while storing missing edge for the first time = {}", e),
        }
    }

    pub async fn is_missing_edge(&mut self, from: u16, to: u16) -> bool {
        let message = EdgeManagerFormat::MissingEdgeFormat(vec![from, to]);
        let serialized = bincode::serialize(&message).expect("Failed to serialize missing edge while checking into the store");
        
        match self.store.read(serialized).await {
            Ok(Some(_count_arr)) => return true,
            Ok(None) => return false,
            Err(e) => error!("Error while checking if there is a missing edge = {}", e),
        }
        return false;
    }

    pub async fn add_updated_edge(&mut self, from: u16, to: u16, new_count: u16) -> bool{
        let message = EdgeManagerFormat::MissingEdgeFormat(vec![from, to]);
        let serialized = bincode::serialize(&message).expect("Failed to serialize updated edge while adding into the store");

        match self.store.read(serialized.clone()).await {
            Ok(Some(count_vec)) => {
                let mut count_arr: [u8; 8] = [Default::default(); 8];
                count_arr[..8].copy_from_slice(&count_vec);
                let mut count = u64::from_le_bytes(count_arr);
                count += new_count as u64;
                self.store.write(serialized, count.to_le_bytes().to_vec()).await;
                return count >= self.committee.quorum_threshold() as u64;
            },
            Ok(None) => (),
            Err(e) => error!("Error while checking if there is a missing edge = {}", e),
        }
        return false;
    }

    // pub async fn is_missing_edge_updated(self, from: u16, to: u16) -> bool {
    //     return true;
    // }

    async fn u64_to_vec8(self, num: u64) -> Vec<u8>{
        return num.to_le_bytes().to_vec().clone();
    }

    async fn vec8_to_u64(self, num_vec: Vec<u8>) -> u64{
        let mut num_arr: [u8; 8] = [Default::default(); 8];
        num_arr[..8].copy_from_slice(&num_vec);
        let num = u64::from_le_bytes(num_arr);
        return num;
    }
}