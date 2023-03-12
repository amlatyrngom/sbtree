use super::local_ownership::LocalOwnership;
use super::structure::BTreeStructure;
use super::BTreeLogEntry;
use obelisk::PersistentLog;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{oneshot, OwnedRwLockReadGuard, RwLock};

pub struct RunningState {
    plog: Arc<PersistentLog>,
    pub is_active: bool,
    pub next_block_id: usize,
    pub last_rescaling_uid: String,
}

impl RunningState {
    /// Initialize a new running state.
    pub fn new(
        plog: Arc<PersistentLog>,
        is_active: bool,
        next_block_id: usize,
        last_rescaling_uid: &str,
    ) -> Self {
        RunningState {
            plog,
            is_active,
            next_block_id,
            last_rescaling_uid: last_rescaling_uid.into(),
        }
    }

    /// Get next block id for a leaf split.
    /// No need to log this since the split operation will be logged.
    pub fn get_next_block_id(&mut self) -> usize {
        let next_id = self.next_block_id;
        self.next_block_id += 1;
        next_id
    }

    /// Checkpoint current running state.
    pub async fn checkpoint(&mut self) {
        let log_entry = BTreeLogEntry::Running {
            next_block_id: self.next_block_id,
            last_rescaling: self.last_rescaling_uid.clone(),
            is_active: self.is_active,
        };
        let log_entry = bincode::serialize(&log_entry).unwrap();
        self.plog.enqueue(log_entry).await;
        self.plog.flush().await;
    }
}

pub struct Recovery {
    recovery_lock: Arc<RwLock<()>>,
    tree_structure: BTreeStructure,
}

impl Recovery {
    /// Make recovery object.
    pub async fn new(owner_id: &str, plog: Arc<PersistentLog>) -> Self {
        let tree_structure = BTreeStructure::new(owner_id, plog).await;
        Recovery {
            recovery_lock: Arc::new(RwLock::new(())),
            tree_structure,
        }
    }

    /// Allow flushing while recovery is ongoing.
    async fn recovery_flusher(
        plog: Arc<PersistentLog>,
        recovery_lock: Arc<RwLock<()>>,
        mut done_rx: oneshot::Receiver<()>,
    ) {
        let mut done = false;
        println!("Recovery::recovery_flusher(). Start.");
        let mut i = 0;
        loop {
            if !done {
                done = done_rx.try_recv().is_ok();
            }
            // Also Wait until all async writes are over.
            if done {
                let no_ongoing_write = recovery_lock.try_write().is_ok();
                if no_ongoing_write {
                    println!("Recovery::recovery_flusher(). Done waiting for signal.");
                    return;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            plog.flush().await;
            i += 1;
            if i % 100 == 0 {
                let flush_lsn = plog.get_flush_lsn().await;
                println!("Recovery::recovery_flusher(i={i}). flush_lsn={flush_lsn}.");
            }
        }
    }

    /// Safely truncate log.
    /// TODO: Might have to vacuum. But I think the free space will just be reused.
    pub async fn safe_truncate(tree_structure: Arc<BTreeStructure>) {
        let mut min_lsn_before = 0;
        let mut curr_start_lsn = 0;
        let old_flush_lsn = tree_structure.plog.get_flush_lsn().await;
        let mut to_replay: BTreeMap<usize, BTreeLogEntry> = BTreeMap::new();
        loop {
            let entries = tree_structure.plog.replay(curr_start_lsn).await;
            let entries = match entries {
                Ok(entries) => entries,
                Err(x) => {
                    println!("Safe Truncate: {x:?}");
                    return;
                }
            };
            if entries.is_empty() {
                break;
            }
            for (lsn, entry) in entries {
                curr_start_lsn = lsn;
                if min_lsn_before == 0 {
                    min_lsn_before = lsn;
                }
                let entry: BTreeLogEntry = bincode::deserialize(&entry).unwrap();
                match entry {
                    BTreeLogEntry::Completion { lsn } => {
                        to_replay.remove(&lsn);
                    }
                    BTreeLogEntry::Running {
                        next_block_id: _,
                        last_rescaling: _,
                        is_active: _,
                    } => {
                        // Do nothing.
                    }
                    _ => {
                        to_replay.insert(lsn, entry);
                    }
                }
            }
        }
        let min_lsn_after = to_replay
            .first_key_value()
            .map(|(lsn, _)| *lsn)
            .unwrap_or(old_flush_lsn);
        if min_lsn_after > min_lsn_before {
            {
                let mut running_state = tree_structure.running_state.write().await;
                running_state.checkpoint().await;
            }
            let _ = tree_structure.plog.truncate(min_lsn_after - 1).await;
        }
    }

    pub async fn recover(mut self) -> (BTreeStructure, bool) {
        let mut curr_start_lsn = 0;
        let mut already_running = false;
        let mut to_replay: BTreeMap<usize, BTreeLogEntry> = BTreeMap::new();
        println!("Recovery::recover(). Starting recovery.");
        // Spawn recovery flusher
        let done_tx = {
            let (done_tx, done_rx) = oneshot::channel();
            let plog = self.tree_structure.plog.clone();
            let recovery_lock = self.recovery_lock.clone();
            tokio::spawn(async move {
                Self::recovery_flusher(plog, recovery_lock, done_rx).await;
            });
            done_tx
        };
        loop {
            let entries = self.tree_structure.plog.replay(curr_start_lsn).await;
            let entries = match entries {
                Ok(entries) => entries,
                Err(x) => {
                    println!("Safe Truncate: {x:?}");
                    // Must own db at this point.
                    std::process::exit(1);
                }
            };
            if entries.is_empty() {
                break;
            }
            for (lsn, entry) in entries {
                curr_start_lsn = lsn;
                let entry: BTreeLogEntry = bincode::deserialize(&entry).unwrap();
                println!("Recovery::recover(). Found entry ({lsn}): {entry:?}.");
                match entry {
                    BTreeLogEntry::Completion { lsn } => {
                        to_replay.remove(&lsn);
                    }
                    entry => {
                        to_replay.insert(lsn, entry);
                    }
                }
            }
        }
        for (lsn, entry) in to_replay {
            println!("Recovery::recover(). Replaying ({lsn}): {entry:?}.");
            match entry {
                BTreeLogEntry::Running {
                    next_block_id,
                    last_rescaling,
                    is_active,
                } => {
                    let mut running_state = self.tree_structure.running_state.write().await;
                    if next_block_id > running_state.next_block_id {
                        running_state.next_block_id = next_block_id;
                    }
                    running_state.last_rescaling_uid = last_rescaling;
                    running_state.is_active = is_active;
                    already_running = true;
                }
                BTreeLogEntry::Put {
                    ownership_key,
                    key,
                    value,
                    leaf_id,
                    leaf_version,
                } => {
                    self.recover_put(lsn, &ownership_key, &key, value, &leaf_id, leaf_version)
                        .await;
                }
                BTreeLogEntry::Delete {
                    ownership_key,
                    key,
                    leaf_id,
                    leaf_version,
                } => {
                    self.recover_delete(lsn, &ownership_key, &key, &leaf_id, leaf_version)
                        .await;
                }
                BTreeLogEntry::SplitLeaf {
                    ownership_key,
                    root_id,
                    leaf_id,
                    new_leaf_id_counter,
                    leaf_version,
                } => {
                    {
                        let mut running_state = self.tree_structure.running_state.write().await;
                        if new_leaf_id_counter >= running_state.next_block_id {
                            running_state.next_block_id = new_leaf_id_counter + 1;
                        }
                    }
                    self.recover_split_leaf(
                        lsn,
                        &ownership_key,
                        &root_id,
                        &leaf_id,
                        new_leaf_id_counter,
                        leaf_version,
                    )
                    .await;
                }
                BTreeLogEntry::MergeLeaf {
                    ownership_key,
                    root_id,
                    leaf_id,
                    from_leaf_id,
                    from_leaf_key,
                } => {
                    self.recover_merge_leaf(
                        lsn,
                        &ownership_key,
                        &root_id,
                        &leaf_id,
                        &from_leaf_id,
                        &from_leaf_key,
                    )
                    .await;
                }
                BTreeLogEntry::Completion { lsn: _ } => {}
                BTreeLogEntry::SplitRoot {
                    ownership_key,
                    new_ownership_key,
                    root_version,
                } => {
                    self.recover_split_root(lsn, &ownership_key, &new_ownership_key, root_version)
                        .await;
                }
                BTreeLogEntry::MergeRoot {
                    to_ownership_key,
                    from_ownership_key,
                    to_version,
                } => {
                    self.recover_merge_root(
                        lsn,
                        &to_ownership_key,
                        &from_ownership_key,
                        to_version,
                    )
                    .await;
                }
                BTreeLogEntry::Rescaling {
                    rescaling_uid,
                    to_owner_id,
                    to_transfer,
                    remove_self,
                } => {
                    self.recover_rescaling(
                        lsn,
                        &rescaling_uid,
                        &to_owner_id,
                        to_transfer,
                        remove_self,
                    )
                    .await;
                }
                BTreeLogEntry::DeleteAll => {
                    self.recover_delete_all(lsn).await;
                }
                BTreeLogEntry::BulkLoad { okeys } => {
                    self.recover_bulk_load(lsn, okeys).await;
                }
            }
        }
        done_tx.send(()).unwrap();
        (self.tree_structure, already_running)
    }

    async fn recover_bulk_load(&mut self, lsn: usize, okeys: Vec<String>) {
        let _recovery_exclusive = self.recovery_lock.clone().write_owned().await;
        self.tree_structure.bulk_load(okeys, Some(lsn)).await;
    }

    async fn recover_delete_all(&mut self, lsn: usize) {
        let _recovery_exclusive = self.recovery_lock.clone().write_owned().await;
        self.tree_structure.delete_all(Some(lsn)).await;
    }

    /// Recover rescaling.
    async fn recover_rescaling(
        &mut self,
        lsn: usize,
        rescaling_uid: &str,
        to_owner: &str,
        to_transfer: Vec<String>,
        remove_self: bool,
    ) {
        let _recovery_exclusive = self.recovery_lock.clone().write_owned().await;
        self.tree_structure
            .perform_rescaling(
                rescaling_uid,
                to_owner,
                remove_self,
                Some((lsn, to_transfer)),
            )
            .await;
    }

    /// Recover merge.
    async fn recover_merge_root(
        &mut self,
        lsn: usize,
        to_ownership_key: &str,
        from_ownership_key: &str,
        to_version: usize,
    ) {
        let _recovery_exclusive = self.recovery_lock.clone().write_owned().await;
        let to_root_id = LocalOwnership::block_id_from_key(to_ownership_key);
        let to_root = self
            .tree_structure
            .block_cache
            .pin(to_ownership_key, &to_root_id)
            .await
            .unwrap();
        let from_root_id = LocalOwnership::block_id_from_key(from_ownership_key);
        let from_root = self
            .tree_structure
            .block_cache
            .pin(from_ownership_key, &from_root_id)
            .await;
        // Recover
        self.tree_structure
            .perform_root_merge(
                from_root.clone(),
                from_ownership_key,
                to_root,
                to_ownership_key,
                Some((lsn, to_version)),
            )
            .await;
        // Unpin.
        self.tree_structure.block_cache.unpin(&to_root_id).await;
        if from_root.is_some() {
            self.tree_structure.block_cache.unpin(&from_root_id).await;
        }
    }

    async fn recover_split_root(
        &mut self,
        lsn: usize,
        ownership_key: &str,
        new_ownership_key: &str,
        root_version: usize,
    ) {
        let _recovery_exclusive = self.recovery_lock.clone().write_owned().await;
        let root_id = LocalOwnership::block_id_from_key(ownership_key);
        let root = self
            .tree_structure
            .block_cache
            .pin(ownership_key, &root_id)
            .await
            .unwrap();
        // Recover.
        self.tree_structure
            .perform_root_split(
                ownership_key,
                root,
                Some((lsn, root_version, new_ownership_key.into())),
            )
            .await;
        // Unpin.
        self.tree_structure.block_cache.unpin(&root_id).await;
    }

    async fn recover_merge_leaf(
        &self,
        lsn: usize,
        ownership_key: &str,
        root_id: &str,
        leaf_id: &str,
        from_leaf_id: &str,
        from_leaf_key: &[u8],
    ) {
        let _recovery_exclusive = self.recovery_lock.clone().write_owned().await;
        let leaf = self
            .tree_structure
            .block_cache
            .pin(ownership_key, leaf_id)
            .await
            .unwrap();
        let root = self
            .tree_structure
            .block_cache
            .pin(ownership_key, root_id)
            .await
            .unwrap();
        let from_leaf = self
            .tree_structure
            .block_cache
            .pin(ownership_key, from_leaf_id)
            .await;
        // Recover.
        self.tree_structure
            .perform_leaf_merge(
                ownership_key,
                root,
                leaf,
                from_leaf.clone(),
                from_leaf_key,
                Some(lsn),
            )
            .await;
        // Unpin.
        self.tree_structure.block_cache.unpin(leaf_id).await;
        self.tree_structure.block_cache.unpin(root_id).await;
        if from_leaf.is_some() {
            self.tree_structure.block_cache.unpin(from_leaf_id).await;
        }
    }

    async fn recover_split_leaf(
        &self,
        lsn: usize,
        ownership_key: &str,
        root_id: &str,
        leaf_id: &str,
        new_leaf_id_counter: usize,
        leaf_version: usize,
    ) {
        let _recovery_exclusive = self.recovery_lock.clone().write_owned().await;
        let root = self
            .tree_structure
            .block_cache
            .pin(ownership_key, root_id)
            .await
            .unwrap();
        let leaf = self
            .tree_structure
            .block_cache
            .pin(ownership_key, leaf_id)
            .await
            .unwrap();
        // Recover.
        self.tree_structure
            .perform_leaf_split(
                ownership_key,
                root,
                leaf,
                Some((lsn, leaf_version, new_leaf_id_counter)),
            )
            .await;
        // Unpin.
        self.tree_structure.block_cache.unpin(leaf_id).await;
        self.tree_structure.block_cache.unpin(root_id).await;
    }

    async fn recover_delete(
        &self,
        lsn: usize,
        ownership_key: &str,
        key: &str,
        leaf_id: &str,
        leaf_version: usize,
    ) {
        let recovery_shared = self.recovery_lock.clone().read_owned().await;
        // Check leaf existence.
        let leaf = self
            .tree_structure
            .block_cache
            .pin(ownership_key, leaf_id)
            .await;
        if leaf.is_none() {
            return;
        }
        let leaf = leaf.unwrap();
        // Do recovery.
        self.tree_structure
            .perform_delete(
                ownership_key,
                key,
                leaf,
                false,
                recovery_shared,
                Some((lsn, leaf_version)),
            )
            .await;
        // Unpin.
        self.tree_structure.block_cache.unpin(leaf_id).await;
    }

    async fn recover_put(
        &self,
        lsn: usize,
        ownership_key: &str,
        key: &str,
        value: Vec<u8>,
        leaf_id: &str,
        leaf_version: usize,
    ) {
        let recovery_shared = self.recovery_lock.clone().read_owned().await;
        // Check leaf existence.
        let leaf = self
            .tree_structure
            .block_cache
            .pin(ownership_key, leaf_id)
            .await;
        if leaf.is_none() {
            return;
        }
        let leaf = leaf.unwrap();
        // Do recovery.
        self.tree_structure
            .perform_put(
                ownership_key,
                key,
                value,
                leaf,
                recovery_shared,
                Some((lsn, leaf_version)),
            )
            .await;
        // Unpin.
        self.tree_structure.block_cache.unpin(leaf_id).await;
    }

    /// Write log entry and wait for it to be flushed.
    pub async fn write_log_entry(plog: Arc<PersistentLog>, log_entry: BTreeLogEntry) -> usize {
        let log_entry = bincode::serialize(&log_entry).unwrap();
        let lsn = plog.enqueue(log_entry).await;
        // let start_time = std::time::Instant::now();
        plog.flush_at(Some(lsn)).await;
        // let end_time = std::time::Instant::now();
        // let duration = end_time.duration_since(start_time);
        // println!("Recovery::write_log_entry() flush duration: {duration:?}");
        lsn
    }

    /// Wait for a set of writes to complete.
    /// When a shared lock is passed in, the writes can be async, but the lock will be held.
    pub async fn wait_for_writes(
        plog: Arc<PersistentLog>,
        lsn: usize,
        write_chs: Vec<oneshot::Receiver<()>>,
        shared_lock: Option<OwnedRwLockReadGuard<()>>,
    ) {
        let in_sync = shared_lock.is_none();
        let t = tokio::spawn(async move {
            let _shared_lock = shared_lock;
            for write_ch in write_chs {
                write_ch.await.unwrap();
            }
            if lsn > 0 {
                let completion_entry = BTreeLogEntry::Completion { lsn };
                Self::write_log_entry(plog, completion_entry).await;
            }
        });
        if in_sync {
            t.await.unwrap();
        }
    }
}
