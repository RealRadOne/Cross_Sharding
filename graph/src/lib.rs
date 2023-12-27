use log::{info};
use petgraph::graphmap::DiGraphMap;
use bytes::BufMut as _;
use bytes::BytesMut;
use bytes::Bytes;
use crypto::Digest;
use smallbank::SmallBankTransactionHandler;
use std::collections::HashMap;
use debugtimer::DebugTimer;


type Transaction = Vec<u8>;

#[derive(Clone)]
pub struct LocalOrderGraph{
    // local order
    local_order: Vec<(u16, Transaction)>,
    sb_handler: SmallBankTransactionHandler,
    dependencies: HashMap<u16, (char, Vec<u32>)>,
}

impl LocalOrderGraph{
    pub fn new(local_order: Vec<(u16, Transaction)>, sb_handler: SmallBankTransactionHandler) -> Self {
        // info!("local order is received = {:?}", local_order);
        let mut dependencies: HashMap<u16, (char, Vec<u32>)> = HashMap::new();
        for order in &local_order{
            let id  = (*order).0;
            let dep = sb_handler.get_transaction_dependency((*order).1.clone().into());
            dependencies.insert(id, dep);
        }
        // info!("size of input local order = {:?}", local_order.len());

        LocalOrderGraph{
            local_order: local_order,
            sb_handler: sb_handler,
            dependencies: dependencies,
        }
    }

    pub fn get_dag(&self) -> DiGraphMap<u16, u8>{
        // info!("local order DAG creation start");

        // (1) Create an empty graph G=(V,E)
        let mut dag: DiGraphMap<u16, u8> = DiGraphMap::new();

        // (2) Add all the nodes into the empty graph, as per local order received
        for order in &self.local_order{
            let node = dag.add_node((*order).0);
            // info!("node added {:?} :: {:?}", (*order).0, node);
        }

        // // Better time complexity with some space
        let mut seen_deps: HashMap<u32, Vec<(u16, char)>> = HashMap::new();
        for (curr_digest, tx) in &self.local_order{
            let curr_deps: &(char, Vec<u32>) = &self.dependencies[curr_digest];
            for curr_dep in &curr_deps.1{
                seen_deps.entry(*curr_dep).or_insert_with(Vec::new);
                for prev_tx_info in &seen_deps[&curr_dep]{
                    if curr_deps.0=='r' && prev_tx_info.1=='r'{
                        continue;
                    }
                    dag.add_edge(prev_tx_info.0, *curr_digest, 1);
                }
                seen_deps.entry(*curr_dep).or_insert_with(Vec::new).push((*curr_digest,curr_deps.0));
            }
        }
        return dag;
    }

    pub fn get_dag_serialized(&self) -> Vec<Vec<u8>>{
        let dag: DiGraphMap<u16, u8> = self.get_dag();
        let mut dag_vec: Vec<Vec<u8>> = Vec::new();

        for node in dag.nodes(){
            let mut node_vec: Vec<u16> = Vec::new();
            node_vec.push(node);
            for neighbor in dag.neighbors(node){
                node_vec.push(neighbor);
            }
            dag_vec.push(bincode::serialize(&node_vec).expect("Failed to serialize local order dag"));
        }

        return dag_vec;
    }
}


pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
