
use crate::worker::WorkerMessage;
use crate::missing_edge_manager::MissingEdgeManager;
use petgraph::graphmap::DiGraphMap;
use threadpool::ThreadPool;
use std::collections::{LinkedList, HashSet, HashMap};
use crypto::Digest;
use store::Store;
use smallbank::SmallBankTransactionHandler;
use graph::GlobalOrderGraph;
use log::{error, info};

const MAX_THREADS: usize = 10;

#[derive(Clone)]
struct QueueElement{
    global_order_digest: Digest,
    missed_pairs: HashSet<(u16, u16)>,
    updated_edges: Vec<(u16, u16)>,
}

#[derive(Clone)]
pub struct ExecutionQueue {
    queue: LinkedList<QueueElement>,
    store: Store,
    sb_handler: SmallBankTransactionHandler,
    missed_edge_manager: MissingEdgeManager,
}

impl ExecutionQueue {
    pub fn new(store: Store, sb_handler: SmallBankTransactionHandler, missed_edge_manager: MissingEdgeManager) -> ExecutionQueue {
        ExecutionQueue{
            queue: LinkedList::new(),
            store: store,
            sb_handler: sb_handler,
            missed_edge_manager: missed_edge_manager,
        }
    }

    async fn add_to_queue(&mut self, digest: Digest) {
        match self.store.read(digest.to_vec()).await {
            Ok(Some(global_order_info)) => {
                match bincode::deserialize(&global_order_info).unwrap() {
                    WorkerMessage::GlobalOrderInfo(global_order_graph, missed) => {
                        self.queue.push_back(QueueElement{ global_order_digest: digest, missed_pairs: missed, updated_edges: Vec::new()});
                    },
                    _ => panic!("PrimaryWorkerMessage::Execute : Unexpected batch"),
                }
            }
            Ok(None) => error!("Could not find a digest in the store while adding to the execution queue"),
            Err(e) => error!("error while adding a digest to the execution queue = {}", e),
        }        
    }

    pub async fn execute(&mut self, digest: Digest){
        // add new element in the queue associated with this new digest
        self.add_to_queue(digest);

        // traverse the queue from front and update missing pairs if any
        for element in self.queue.iter_mut() {
            // check if missed edges are found for this digest
            if element.missed_pairs.is_empty(){
                continue;
            }

            let mut updated_pairs: Vec<(u16, u16)> = Vec::new();
            let mut updated_edges: Vec<(u16, u16)> = Vec::new();
            for missed_pair in &element.missed_pairs{
                if self.missed_edge_manager.is_missing_edge_updated(missed_pair.0, missed_pair.1).await{
                    updated_pairs.push((missed_pair.0, missed_pair.1));
                    updated_edges.push((missed_pair.0, missed_pair.1));
                }
                else if self.missed_edge_manager.is_missing_edge_updated(missed_pair.1, missed_pair.0).await{
                    updated_pairs.push((missed_pair.0, missed_pair.1));
                    updated_edges.push((missed_pair.1, missed_pair.0));
                }
            }

            // remove from missed pairs set
            for pair in &updated_pairs{
                element.missed_pairs.remove(pair);
            }
            // add to the updated edges
            for edge in &updated_edges{
                element.updated_edges.push(*edge);
            }
        }

        // Execute global order if missed edges are found
        let mut n_elements_to_execute = 0;
        for element in self.queue.iter_mut() {
            // check if there are no missed edges for this digest
            if element.missed_pairs.is_empty(){
                // TODO: Update the graph based on the "updated_edges"
                n_elements_to_execute += 1;
            }
            else{
                // execution can only be done in sequence
                break;
            }
        }

        // remove queue elements and Execute global order if no more missed edges
        for _ in 0..n_elements_to_execute{
            let queue_element: QueueElement = self.queue.pop_front().unwrap();

            // execute the global order graph
            match self.store.read(queue_element.global_order_digest.to_vec()).await {
                Ok(Some(global_order_info)) => {
                    match bincode::deserialize(&global_order_info).unwrap() {
                        WorkerMessage::GlobalOrderInfo(global_order_graph_serialized, missed) => {
                            // deserialize received serialized glbal order graph
                            let dag: DiGraphMap<u16, u8> = GlobalOrderGraph::get_dag_deserialized(global_order_graph_serialized);
                            // for tx in batch{
                            //     self.sb_handler.execute_transaction(Bytes::from(tx));
                            // }              
                        },
                        _ => panic!("PrimaryWorkerMessage::Execute : Unexpected global order graph at execution"),
                    }
                }
                Ok(None) => (),
                Err(e) => error!("{}", e),
            } 
        }
    }
}



#[derive(Clone)]
pub struct ParallelExecution {
    thread_pool: ThreadPool,
    global_order_graph: DiGraphMap<u16, u8>,
    store: Store,
    sb_handler: SmallBankTransactionHandler,
}

impl ParallelExecution {
    pub fn new(global_order_graph: DiGraphMap<u16, u8>, store: Store, sb_handler: SmallBankTransactionHandler) -> ParallelExecution {
        ParallelExecution{
            thread_pool: ThreadPool::new(MAX_THREADS),
            global_order_graph: global_order_graph,
            store: store,
            sb_handler: sb_handler,
        }
    }

    pub async fn execute(&mut self){
        // find incoming edge count for each node in the graph
        let mut incoming_count: HashMap<u16, usize> = HashMap::new();
        for node in self.global_order_graph.nodes(){
            let mut count: usize = 0;
            for neighbor in self.global_order_graph.neighbors(node){
                incoming_count.entry(neighbor).or_insert(0);
                incoming_count.insert(neighbor, incoming_count[&neighbor]+1);
            }                   
        }

        // find root nodes of the graph
        let mut roots: Vec<u16> = Vec::new();
        for (node, count) in &incoming_count{
            if *count==0{
                roots.push(*node);
            }
        }

        // create a shared queue
        // https://stackoverflow.com/questions/72879440/how-to-use-vecdeque-in-multi-threaded-app
        // initialize the shared queue with root nodes

        // Traverse the graph and execute the nodes
        // execute using threadpool

    }
}