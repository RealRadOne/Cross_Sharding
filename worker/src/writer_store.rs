use network::Writer;
use std::collections::HashMap;
use futures::sink::SinkExt as _;
use std::sync::{Arc};
use futures::lock::Mutex;
// use log::{info, error};


#[derive(Clone)]
pub struct WriterStore {
    store: HashMap<u64, Arc<Mutex<Writer>>>,
}

impl WriterStore {
    pub fn new() -> WriterStore {
        WriterStore{
            store: HashMap::new(),
        }
    }

    pub fn add_writer(&mut self, tx_uid: u64, writer: Arc<Mutex<Writer>>){
        // info!("add_writer: tx_uid = {:?}, self.store.len() = {:?}", tx_uid, self.store.len());
        self.store.insert(tx_uid, writer);
    }

    pub fn writer_exists(&mut self, tx_uid: u64) -> bool{
        // info!("writer_exists: tx_uid = {:?}, self.store.len() = {:?}", tx_uid, self.store.len());
        return self.store.contains_key(&tx_uid);
    }

    pub fn get_writer(&mut self, tx_uid: u64) -> Arc<Mutex<Writer>>{
        // info!("get_writer: tx_uid = {:?}", tx_uid);
        return self.store.get(&tx_uid).unwrap().clone();
    }
}
