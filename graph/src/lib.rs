use log::{info};
use petgraph::graphmap::DiGraphMap;
use bytes::BufMut as _;
use bytes::BytesMut;
use bytes::Bytes;
use crypto::Digest;
use smallbank::SmallBankTransactionHandler;
use std::collections::HashMap;


type Transaction = Vec<u8>;

#[derive(Clone)]
pub struct LocalOrderGraph{
    // local order
    local_order: Vec<(Digest, Transaction)>,
    sb_handler: SmallBankTransactionHandler,
    dependencies: HashMap<Digest, (char, Vec<u32>)>,
}

impl LocalOrderGraph{
    pub fn new(local_order: Vec<(Digest, Transaction)>, sb_handler: SmallBankTransactionHandler) -> Self {
        // info!("local order is received = {:?}", local_order);
        let mut dependencies: HashMap<Digest, (char, Vec<u32>)> = HashMap::new();
        for order in &local_order{
            let digest  = (*order).0;
            let dep = sb_handler.get_transaction_dependency((*order).1.clone().into());
            dependencies.insert(digest, dep);
        }

        LocalOrderGraph{
            local_order: local_order,
            sb_handler: sb_handler,
            dependencies: dependencies,
        }
    }

    pub fn get_dag(&self) -> DiGraphMap<Digest, u32>{
        // info!("local order DAG creation start");

        // (1) Create an empty graph G=(V,E)
        let mut dag: DiGraphMap<Digest, u32> = DiGraphMap::new();

        // (2) Add all the nodes into the empty graph, as per local order received
        for order in &self.local_order{
            let node = dag.add_node((*order).0);
            // info!("node added {:?} :: {:?}", (*order).0, node);
        }

        // (3) Update edges based on dependency

        // // Brute Force with no space
        // let node_count = self.local_order.len();
        // for curr_idx in 0..node_count{
        //     let curr_dep: &(char, Vec<u32>) = &self.dependencies[&self.local_order[curr_idx].0];
        //     for prev_idx in 0..curr_idx{
        //         let prev_dep: &(char, Vec<u32>) = &self.dependencies[&self.local_order[prev_idx].0];
        //         if curr_dep.0 == 'r' && prev_dep.0 == 'r'{
        //             continue;
        //         }
        //         'outer: for curr_dep_obj in &curr_dep.1{
        //             for prev_dep_obj in &prev_dep.1{
        //                 if curr_dep_obj==prev_dep_obj{
        //                     // add edge 
        //                     dag.add_edge(self.local_order[prev_idx].0, self.local_order[curr_idx].0, 1);
        //                     // info!("Edge added {:?} -> {:?}, with weight {:?}", self.local_order[prev_idx].0, self.local_order[curr_idx].0, edge_weight);
        //                     break 'outer;
        //                 }
        //             }
        //         }
        //     }
        // }

        // // Better time complexity with some space
        let mut seen_deps: HashMap<u32, Vec<(Digest, char)>> = HashMap::new();
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

    fn _usize_to_vec(&self, mut number: usize) -> Vec<u8> {
        let mut size_vec: Vec<u8> = Vec::new();
        while number>0{
            size_vec.push((number%10) as u8);
            number = number/10;
        }
        return size_vec;
    }

    fn _usize_to_digest(&self, mut number: usize) -> Digest {
        let mut arr: [u8; 32] = [0; 32];
        let mut idx:usize = 0;
        while number>0{
            arr[idx+1] = (number%10) as u8;
            number = number/10;
            idx +=1;
        }
        arr[0] = idx as u8;
        return Digest(arr);
    }

    fn _digest_to_usize(&self, digest: Digest) -> usize {
        let digest_vec: Vec<u8> = digest.to_vec();
        let mut number: usize = 0;
        let mut multiplier: usize = 1;
        for idx in 1..=digest_vec[0]{
            let e = digest_vec[idx as usize];
            number = (e as usize * multiplier) + number;
            multiplier *= 10;
        }
        return number;
    }

    // pub fn get_dag_serialized(&self) -> Vec<u8>{
    //     let dag: DiGraphMap<Digest, u32> = self.get_dag();
    //     let mut dag_vec: Vec<Digest> = Vec::new();

    //     for node in dag.nodes(){
    //         let mut neighbor_vec: Vec<Digest> = Vec::new();
    //         for neighbor in dag.neighbors(node){
    //             neighbor_vec.push(neighbor);
    //         }
    //         dag_vec.push(node);
    //         dag_vec.push(self._usize_to_digest(dag_vec.len()));
    //         dag_vec.append(&mut neighbor_vec);
    //     }
    //     return bincode::serialize(&dag_vec).expect("Failed to serialize local order dag");
    // }

    pub fn get_dag_serialized(&self) -> Vec<Vec<u8>>{
        let dag: DiGraphMap<Digest, u32> = self.get_dag();
        let mut dag_vec: Vec<Vec<u8>> = Vec::new();

        for node in dag.nodes(){
            let mut node_vec: Vec<Digest> = Vec::new();
            node_vec.push(node);
            for neighbor in dag.neighbors(node){
                node_vec.push(neighbor);
            }
            dag_vec.push(bincode::serialize(&node_vec).expect("Failed to serialize local order dag"));
        }
        return dag_vec;
    }

    // pub fn get_dag_serialized(&self) -> Vec<Vec<u8>>{
    //     let dag: DiGraphMap<Digest, u32> = self.get_dag();
    //     let mut dag_vec: Vec<Vec<u8>> = Vec::new();

    //     for node in dag.nodes(){
    //         let mut neighbor_vec: Vec<Vec<u8>> = Vec::new();
    //         for neighbor in dag.neighbors(node){
    //             neighbor_vec.push(neighbor.to_vec());
    //         }
    //         dag_vec.push(node.to_vec());
    //         dag_vec.push(self._usize_to_vec(neighbor_vec.len()));
    //         dag_vec.append(&mut neighbor_vec);
    //     }
    //     return dag_vec;
    // }
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
