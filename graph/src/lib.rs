use log::{info};
use petgraph::graphmap::DiGraphMap;
use petgraph::algo::{condensation, kosaraju_scc};
use bytes::BufMut as _;
use bytes::BytesMut;
use bytes::Bytes;
use crypto::Digest;
use smallbank::SmallBankTransactionHandler;
use std::collections::{HashMap, HashSet};
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

#[derive(Clone)]
pub struct GlobalDependencyGraph{
    dag: DiGraphMap<u16, u8>,
    fixed_transactions: HashSet<u16>,
}

impl GlobalDependencyGraph{
    pub fn new(local_order_graphs: Vec<DiGraphMap<u16, u8>>, fixed_tx_threshold: f32, pending_tx_threshold: f32) -> Self {
        info!("local order graphs are received = {:?}", local_order_graphs);
        
        // (1) Create an empty graph G=(V,E)
        let mut dag: DiGraphMap<u16, u8> = DiGraphMap::new();

        // (2) Find transactions' counts
        let mut transaction_counts: HashMap<u16, u16> = HashMap::new();
        let mut edge_counts: HashMap<(u16,u16), u16> = HashMap::new();
        for local_order_graph in &local_order_graphs{
            for node in local_order_graph.nodes(){
                let count = *transaction_counts.entry(node).or_insert(0);
                transaction_counts.insert(node, count+1);
            }
            for (from, to, weight) in local_order_graph.all_edges(){
                edge_counts.entry((from, to)).or_insert(0);
                edge_counts.entry((to, from)).or_insert(0);
                edge_counts.insert((from, to), edge_counts[&(from, to)]+1);
            }
        }

        // (3) Find fixed and pending transactions and add them into the graph
        let mut fixed_transactions: HashSet<u16> = HashSet::new();
        for (&tx, &count) in &transaction_counts{
            if count as f32 >= fixed_tx_threshold || count as f32 >= pending_tx_threshold{
                dag.add_node(tx);
            }
            if count as f32 >= pending_tx_threshold{
                fixed_transactions.insert(tx);
            }
        }

        // (4) Find edges to add into the graph
        for (&(from, to), &count) in &edge_counts{
            if (count as f32 >= fixed_tx_threshold || count as f32 >= pending_tx_threshold) && count > edge_counts[&(to, from)]{
                dag.add_edge(from, to, 1);
            }
        }

        GlobalDependencyGraph{
            dag: dag,
            fixed_transactions: fixed_transactions,
        }
    }

    pub fn get_dag(&self) -> &DiGraphMap<u16, u8>{
        return &self.dag;
    }

    pub fn get_fixed_transactions(&self) -> &HashSet<u16>{
        return &self.fixed_transactions;
    }
}

#[derive(Clone)]
pub struct PrunedGraph{
    pruned_graph:  DiGraphMap<u16, u8>,
}

impl PrunedGraph{
    pub fn new(global_dependency_graph: &DiGraphMap<u16, u8>, fixed_transactions: &HashSet<u16>, sb_handler: SmallBankTransactionHandler) -> Self {
        let strongely_connected_components = kosaraju_scc(global_dependency_graph);
        let mut pruned_graph:  DiGraphMap<u16, u8> = global_dependency_graph.clone();
        let mut idx: usize = strongely_connected_components.len();
        
        while idx>=0{
            let mut is_fixed: bool = false;
            for node in &strongely_connected_components[idx]{
                if fixed_transactions.contains(node){
                    is_fixed = true;
                    break;
                }
            }
            if is_fixed{
                break;
            }
            // All pending transactions are found in this scc : remove these
            for &node in &strongely_connected_components[idx]{
                pruned_graph.remove_node(node);
            }
            idx -= 1;
        } 
        

        PrunedGraph{
            pruned_graph: pruned_graph,
        }
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
