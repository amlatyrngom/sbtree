mod block;
mod btree_bench;
mod btree_test;
mod local_ownership;
mod manager;
mod recovery;
mod structure;

use manager::BTreeManager;
use obelisk::{ActorInstance, PersistentLog};
use recovery::Recovery;
use serde::{Deserialize, Serialize};
use std::sync::{atomic, Arc};
use structure::{BTreeStructure, LookupResult};

/// Request metadata.
#[derive(Serialize, Deserialize, Debug)]
pub enum BTreeReqMeta {
    Get { key: String },
    Put { key: String },
    Delete { key: String },
    Load,
    Unload,
    Manage,
    Rescale { op: RescalingOp },
    ForceRescale { op: Option<RescalingOp> },
    Cleanup { worker: Option<String> },
    BulkLoad { okeys: Vec<String> },
}

/// Response metadata.
#[derive(Serialize, Deserialize, Debug)]
pub enum BTreeRespMeta {
    NotFound,
    Ok,
    BadOwner,
    NetworkIssue,
    LockConflict,
    InvariantIssue,
}

/// Type of rescaling.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RescalingOp {
    /// Scale out transfers half of elements.
    ScaleOut {
        from: String,
        to: String,
        uid: String,
    },
    /// Scale in transfers all elements and removes self.
    ScaleIn {
        from: String,
        to: String,
        uid: String,
    },
}

/// BTreeLogEntry
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum BTreeLogEntry {
    /// Running indicates that the worker has been fully started at least once and does not need initialization.
    Running {
        next_block_id: usize,
        last_rescaling: String,
        is_active: bool,
    },
    /// Insertion.
    Put {
        ownership_key: String,
        key: String,
        value: Vec<u8>,
        leaf_id: String,
        leaf_version: usize,
    },
    /// Splitting leaf.
    SplitLeaf {
        ownership_key: String,
        root_id: String,
        leaf_id: String,
        new_leaf_id_counter: usize,
        leaf_version: usize,
    },
    /// Deletion
    Delete {
        ownership_key: String,
        key: String,
        leaf_id: String,
        leaf_version: usize,
    },
    /// Merging leaves.
    MergeLeaf {
        ownership_key: String,
        root_id: String,
        leaf_id: String,
        from_leaf_id: String,
        from_leaf_key: Vec<u8>,
    },
    /// Indicates that log entry at the specified lsn can be removed.
    Completion { lsn: usize },
    /// Splitting a root.
    SplitRoot {
        ownership_key: String,
        new_ownership_key: String,
        root_version: usize,
    },
    /// Merging a root
    MergeRoot {
        to_ownership_key: String,
        from_ownership_key: String,
        to_version: usize,
    },
    /// Rescale.
    Rescaling {
        rescaling_uid: String,
        to_owner_id: String,
        to_transfer: Vec<String>,
        remove_self: bool,
    },
    /// Delete all.
    DeleteAll,
    /// Bulk load
    BulkLoad { okeys: Vec<String> },
}

/// BTree Actor.
#[derive(Clone)]
pub struct BTreeActor {
    owner_id: String,
    tree_structure: Arc<BTreeStructure>,
    manager: Option<Arc<BTreeManager>>,
    terminating: Arc<atomic::AtomicBool>,
}

#[async_trait::async_trait]
impl ActorInstance for BTreeActor {
    /// Receive message.
    async fn message(&self, msg: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        let req_meta: BTreeReqMeta = serde_json::from_str(&msg).unwrap();
        let (resp, payload) = match req_meta {
            BTreeReqMeta::Get { key } => self.get(&key).await,
            BTreeReqMeta::Put { key } => self.put(&key, payload).await,
            BTreeReqMeta::Delete { key } => self.delete(&key).await,
            BTreeReqMeta::Rescale { op } => self.rescale(op).await,
            BTreeReqMeta::ForceRescale { op } => self.force_rescale(op).await,
            BTreeReqMeta::Manage => {
                let manager = self.manager.clone().unwrap();
                manager.manage().await;
                (BTreeRespMeta::Ok, vec![])
            }
            BTreeReqMeta::Cleanup { worker } => self.cleanup(worker).await,
            BTreeReqMeta::BulkLoad { okeys } => self.bulk_load(okeys).await,
            BTreeReqMeta::Load => {
                self.tree_structure.block_cache.reset_stats().await;
                let kvs: Vec<(String, Vec<u8>)> =
                    tokio::task::block_in_place(move || bincode::deserialize(&payload).unwrap());
                println!("Loading {} kv pairs.", kvs.len());
                let mut not_owned = Vec::new();
                for (key, value) in kvs {
                    let (resp, _) = self.load_kv(&key, value.clone()).await;
                    match resp {
                        BTreeRespMeta::Ok => continue,
                        resp => {
                            println!("Load resp: {resp:?}");
                            not_owned.push((key, value));
                        }
                    }
                }
                println!("Resending {}.", not_owned.len());
                let not_owned =
                    tokio::task::block_in_place(move || bincode::serialize(&not_owned).unwrap());
                self.tree_structure.block_cache.reset_stats().await;
                (BTreeRespMeta::Ok, not_owned)
            }
            BTreeReqMeta::Unload => {
                self.tree_structure.block_cache.reset_stats().await;
                let keys: Vec<String> =
                    tokio::task::block_in_place(move || bincode::deserialize(&payload).unwrap());
                let mut not_owned = Vec::new();
                for key in keys {
                    let (resp, _) = self.unload_kv(&key).await;
                    match resp {
                        BTreeRespMeta::Ok => continue,
                        _ => {
                            not_owned.push(key);
                        }
                    }
                }
                let not_owned =
                    tokio::task::block_in_place(move || bincode::serialize(&not_owned).unwrap());
                self.tree_structure.block_cache.reset_stats().await;
                (BTreeRespMeta::Ok, not_owned)
            }
        };
        let resp = serde_json::to_string(&resp).unwrap();
        (resp, payload)
    }

    /// Checkpointing.
    async fn checkpoint(&self, terminating: bool) {
        println!("Checkpointing: termination={terminating}.");
        let tree_structure = self.tree_structure.clone();
        if terminating {
            tree_structure.plog.terminate().await;
            self.terminating.store(true, atomic::Ordering::Release);
        }
    }
}

impl BTreeActor {
    /// Make actor.
    pub async fn new(name: &str, plog: Arc<PersistentLog>) -> Self {
        // Attempt recovery.
        println!("BTreeActor::new(). Recovering.");
        let recovery = Recovery::new(name, plog.clone()).await;
        let (tree_structure, already_running) = recovery.recover().await;
        println!("BTreeActor::new(). Done recovering.");
        if !already_running {
            println!("BTreeActor::new(). Initializing.");
            tree_structure.initialize().await;
            println!("BTreeActor::new(). Done initializing.");
        }
        // Mark as running and truncate all previous logs.
        println!("BTreeActor::new(). Checkpointing and logging!");
        let old_flush_lsn = tree_structure.plog.get_flush_lsn().await;
        {
            let mut running_state = tree_structure.running_state.write().await;
            running_state.checkpoint().await;
        }
        tree_structure.plog.truncate(old_flush_lsn).await.unwrap();
        println!("BTreeActor::new(). Done checkpointing and logging!");
        let tree_structure = Arc::new(tree_structure);
        let manager = if name == "manager" {
            let manager = BTreeManager::new(tree_structure.global_ownership.clone()).await;
            Some(Arc::new(manager))
        } else {
            None
        };
        let terminating = Arc::new(atomic::AtomicBool::new(false));
        {
            let tree_structure = tree_structure.clone();
            let terminating = terminating.clone();
            tokio::spawn(async move {
                Self::bookkeeping(tree_structure, terminating).await;
            });
        }
        // Now make worker
        println!("BTreeActor::new(). Finished making object.");
        BTreeActor {
            owner_id: name.into(),
            tree_structure,
            manager,
            terminating,
        }
    }

    async fn bookkeeping(
        tree_structure: Arc<BTreeStructure>,
        terminating: Arc<atomic::AtomicBool>,
    ) {
        // Duration to sleep every second.
        let sleep_duration = std::time::Duration::from_millis(100);
        // Truncate log every few seconds.
        let mut truncation_duration = std::time::Duration::from_secs(2);
        let mut last_truncation = std::time::Instant::now();
        // Retrieve and update load.
        let load_duration = std::time::Duration::from_secs(30);
        let mut last_load = std::time::Instant::now();
        loop {
            // Sleep a short duration.
            tokio::time::sleep(sleep_duration).await;
            // Flush.
            let plog = tree_structure.plog.clone();
            // let start_time = std::time::Instant::now();
            plog.flush().await;
            // let end_time = std::time::Instant::now();
            // let duration = end_time.duration_since(start_time);
            // println!("Bookkeeping flush duration: {duration:?}");
            // Check if should truncate.
            if terminating.load(atomic::Ordering::Acquire) {
                // Decrease truncation time when scaling down.
                truncation_duration = std::time::Duration::from_millis(500);
            }
            let now = std::time::Instant::now();
            if now.duration_since(last_truncation) > truncation_duration {
                last_truncation = now;
                let tree_structure = tree_structure.clone();
                tokio::spawn(async move {
                    Recovery::safe_truncate(tree_structure).await;
                });
            }
            // Check if should retrieve and update load.
            if now.duration_since(last_load) > load_duration {
                last_load = now;
                let tree_structure = tree_structure.clone();
                tokio::spawn(async move {
                    let load = tree_structure.block_cache.retrieve_stats().await;
                    println!("Retrieved Load: {load:?}");
                    // Maintain lock to prevent concurrent change.
                    let running_state = tree_structure.running_state.read().await;
                    if running_state.is_active {
                        tree_structure.global_ownership.update_load(load).await;
                    } else {
                        tree_structure.global_ownership.remove_load().await;
                    }
                });
            }
        }
    }

    /// Rescale.
    pub async fn rescale(&self, op: RescalingOp) -> (BTreeRespMeta, Vec<u8>) {
        println!("Worker {}. Received rescaling: {op:?}", self.owner_id);
        let (rescaling_uid, from, to, scaling_in) = match op {
            RescalingOp::ScaleIn { from, to, uid } => (uid, from, to, true),
            RescalingOp::ScaleOut { from, to, uid } => (uid, from, to, false),
        };
        let remove_self = self.owner_id == from && scaling_in;
        println!("Should remove self: {remove_self}");
        self.tree_structure
            .perform_rescaling(&rescaling_uid, &to, remove_self, None)
            .await;
        (BTreeRespMeta::Ok, vec![])
    }

    /// Forcibly rescale. Should only be sent to manager.
    pub async fn force_rescale(&self, op: Option<RescalingOp>) -> (BTreeRespMeta, Vec<u8>) {
        match op {
            None => {
                self.tree_structure
                    .global_ownership
                    .allow_regular_rescaling()
                    .await;
                (BTreeRespMeta::Ok, vec![])
            }
            Some(op) => {
                println!("Received force rescaling request!");
                if !obelisk::common::has_external_access() {
                    return (BTreeRespMeta::NetworkIssue, vec![]);
                }
                let manager = self.manager.clone().unwrap();
                // Complete any ongoing rescaling.
                println!("Completing ongoing management.");
                manager.manage().await;
                // Lock manager and prevent rescaling.
                println!("Locking manager.");
                let _locked = loop {
                    let locked = manager.lock_manager().await;
                    if !locked.is_none() {
                        break locked;
                    }
                    println!("Manager already locked!");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                };
                self.tree_structure
                    .global_ownership
                    .prevent_regular_rescaling()
                    .await;
                println!("Performing forcible rescale: {op:?}.");
                manager.manage_rescaling(op, false).await;
                (BTreeRespMeta::Ok, vec![])
            }
        }
    }

    /// Will bypass logging. Used only to load data for tests.
    pub async fn load_kv(&self, key: &str, value: Vec<u8>) -> (BTreeRespMeta, Vec<u8>) {
        // Lookup ownership, root, and leaf information.
        let lookup_res = match self.tree_structure.lookup_root_and_leaf(key).await {
            Ok(x) => x,
            Err(resp) => return (resp, vec![]),
        };
        let LookupResult {
            ownership_key,
            root: _,
            leaf,
            root_id,
            leaf_id,
            split_key,
            is_last_leaf: _,
            shared_lock,
        } = lookup_res;
        let should_try_split = self
            .tree_structure
            .perform_load_kv(&ownership_key, key, value, leaf, shared_lock)
            .await;
        if should_try_split {
            self.tree_structure
                .split_or_merge_leaf(&ownership_key, &split_key)
                .await;
        }
        // Unpin.
        self.tree_structure.block_cache.unpin(&root_id).await;
        self.tree_structure.block_cache.unpin(&leaf_id).await;
        (BTreeRespMeta::Ok, vec![])
    }

    /// Will bypass logging. Used only to delete data for tests.
    pub async fn unload_kv(&self, key: &str) -> (BTreeRespMeta, Vec<u8>) {
        // Lookup ownership, root, and leaf information.
        let lookup_res = match self.tree_structure.lookup_root_and_leaf(key).await {
            Ok(x) => x,
            Err(resp) => return (resp, vec![]),
        };
        let LookupResult {
            ownership_key,
            root: _,
            leaf,
            root_id,
            leaf_id,
            split_key,
            is_last_leaf,
            shared_lock,
        } = lookup_res;
        let should_try_split = self
            .tree_structure
            .perform_unload_kv(&ownership_key, key, leaf, is_last_leaf, shared_lock)
            .await;
        if should_try_split {
            self.tree_structure
                .split_or_merge_leaf(&ownership_key, &split_key)
                .await;
        }
        // Unpin.
        self.tree_structure.block_cache.unpin(&root_id).await;
        self.tree_structure.block_cache.unpin(&leaf_id).await;
        (BTreeRespMeta::Ok, vec![])
    }

    /// Put.
    pub async fn put(&self, key: &str, value: Vec<u8>) -> (BTreeRespMeta, Vec<u8>) {
        // Lookup ownership, root, and leaf information.
        let lookup_res = match self.tree_structure.lookup_root_and_leaf(key).await {
            Ok(x) => x,
            Err(resp) => return (resp, vec![]),
        };
        let LookupResult {
            ownership_key,
            root: _,
            leaf,
            root_id,
            leaf_id,
            split_key,
            is_last_leaf: _,
            shared_lock,
        } = lookup_res;
        let should_try_split = self
            .tree_structure
            .perform_put(&ownership_key, key, value, leaf, shared_lock, None)
            .await;
        if should_try_split {
            self.tree_structure
                .split_or_merge_leaf(&ownership_key, &split_key)
                .await;
        }
        // Unpin.
        self.tree_structure.block_cache.unpin(&root_id).await;
        self.tree_structure.block_cache.unpin(&leaf_id).await;
        (BTreeRespMeta::Ok, vec![])
    }

    /// Delete.
    pub async fn delete(&self, key: &str) -> (BTreeRespMeta, Vec<u8>) {
        // Lookup ownership, root, and leaf information.
        let lookup_res = match self.tree_structure.lookup_root_and_leaf(key).await {
            Ok(x) => x,
            Err(resp) => return (resp, vec![]),
        };
        let LookupResult {
            ownership_key,
            root: _,
            leaf,
            root_id,
            leaf_id,
            split_key,
            is_last_leaf,
            shared_lock,
        } = lookup_res;
        let should_try_merge = self
            .tree_structure
            .perform_delete(&ownership_key, key, leaf, is_last_leaf, shared_lock, None)
            .await;
        if should_try_merge {
            self.tree_structure
                .split_or_merge_leaf(&ownership_key, &split_key)
                .await;
        }
        // Unpin.
        self.tree_structure.block_cache.unpin(&root_id).await;
        self.tree_structure.block_cache.unpin(&leaf_id).await;
        (BTreeRespMeta::Ok, vec![])
    }

    pub async fn get(&self, key: &str) -> (BTreeRespMeta, Vec<u8>) {
        // Lookup ownership, root, and leaf information.
        let lookup_res = match self.tree_structure.lookup_root_and_leaf(key).await {
            Ok(x) => x,
            Err(resp) => return (resp, vec![]),
        };

        // Read value.
        let ret = {
            let leaf = lookup_res.leaf.read().await;
            let ret = leaf.find_exact(key.as_bytes());
            ret
        };
        // Unpin.
        self.tree_structure
            .block_cache
            .unpin(&lookup_res.root_id)
            .await;
        self.tree_structure
            .block_cache
            .unpin(&lookup_res.leaf_id)
            .await;
        // Return value if found.
        match ret {
            Some(value) => (BTreeRespMeta::Ok, value),
            None => (BTreeRespMeta::NotFound, vec![]),
        }
    }

    pub async fn bulk_load(&self, okeys: Vec<String>) -> (BTreeRespMeta, Vec<u8>) {
        self.tree_structure.bulk_load(okeys, None).await;
        (BTreeRespMeta::Ok, vec![])
    }

    pub async fn cleanup(&self, worker_id: Option<String>) -> (BTreeRespMeta, Vec<u8>) {
        match worker_id {
            Some(worker_id) => {
                if worker_id != self.owner_id && !obelisk::common::has_external_access() {
                    return (BTreeRespMeta::NetworkIssue, vec![]);
                }
                println!("Cleanup {worker_id}.");
                let manager = self.manager.clone().unwrap();
                // Complete any ongoing rescaling.
                println!("Completing ongoing management.");
                manager.manage().await;
                // Lock manager and prevent rescaling.
                println!("Locking manager.");
                let _locked = loop {
                    let locked = manager.lock_manager().await;
                    if locked.is_some() {
                        break locked;
                    }
                    println!("Manager already locked!");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                };
                self.tree_structure
                    .global_ownership
                    .prevent_regular_rescaling()
                    .await;
                // If non manager, scale in.
                if worker_id != self.owner_id {
                    println!("Performing scale in to manager: {worker_id}.");
                    let rescaling_op = RescalingOp::ScaleIn {
                        from: worker_id.clone(),
                        to: self.owner_id.clone(),
                        uid: uuid::Uuid::new_v4().to_string(),
                    };
                    manager.manage_rescaling(rescaling_op, false).await;
                }
                (BTreeRespMeta::Ok, vec![])
            }
            None => {
                // Cleanup everything in manager, which should be the only worker by this step.
                println!("Manager. Deleting all for cleanup");
                self.tree_structure.delete_all(None).await;
                (BTreeRespMeta::Ok, vec![])
            }
        }
    }
}
