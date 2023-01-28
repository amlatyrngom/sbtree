use super::block::{BlockCache, TreeBlock, TreeBlockRef};
use super::local_ownership::LocalOwnership;
use super::recovery::{Recovery, RunningState};
use super::{BTreeLogEntry, BTreeRespMeta};
use crate::global_ownership::GlobalOwnership;
use obelisk::PersistentLog;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

type WorkerOwnership = Arc<RwLock<BTreeMap<String, Arc<tokio::sync::RwLock<()>>>>>;

pub struct LookupResult {
    pub ownership_key: String,
    pub root: TreeBlockRef,
    pub leaf: TreeBlockRef,
    pub root_id: String,
    pub leaf_id: String,
    pub split_key: Vec<u8>,
    pub is_last_leaf: bool,
    pub shared_lock: OwnedRwLockReadGuard<()>,
}

struct RootAdjacencyResult {
    from_root: TreeBlockRef,
    from_ownership_key: String,
    to_root: TreeBlockRef,
    to_ownership_key: String,
    other_exclusive_lock: OwnedRwLockWriteGuard<()>,
}

#[derive(Clone)]
pub struct BTreeStructure {
    pub owner_id: String,
    pub block_cache: Arc<BlockCache>,
    pub plog: Arc<PersistentLog>,
    pub global_ownership: Arc<GlobalOwnership>,
    pub ownership: WorkerOwnership,
    pub running_state: Arc<RwLock<RunningState>>, // RwLock only because of async.
    pub ownership_change_lock: Arc<RwLock<()>>,   // RwLock only because of async.
}

impl BTreeStructure {
    /// Create a new btree structure.
    pub async fn new(owner_id: &str, plog: Arc<PersistentLog>) -> Self {
        println!("Creating global ownership!");
        let global_ownership = Arc::new(GlobalOwnership::new(Some(owner_id.into())).await);
        let block_cache = Arc::new(BlockCache::new().await);
        let ownership = global_ownership.fetch_ownership_keys().await;
        let ownership: WorkerOwnership = Arc::new(RwLock::new(
            ownership
                .into_iter()
                .map(|ownership_key| (ownership_key, Arc::new(RwLock::new(()))))
                .collect(),
        ));
        let running_state = Arc::new(RwLock::new(RunningState::new(plog.clone(), true, 1, "")));
        BTreeStructure {
            owner_id: owner_id.into(),
            block_cache,
            global_ownership,
            plog,
            ownership,
            running_state,
            ownership_change_lock: Arc::new(RwLock::new(())),
        }
    }

    /// Initialize structure.
    pub async fn initialize(&self) {
        if self.owner_id != "manager" {
            // Nothing to do.
            return;
        }
        // Make default ownership.
        // There should a root and a leaf.
        // The empty key should always be associated with the empty value.
        let empty_key = "";
        let empty_val: &[u8] = &[];
        let ownership_key = format!("/okey/{empty_key}");
        let root_id = LocalOwnership::block_id_from_key(&ownership_key);
        let mut root = TreeBlock::new(&root_id, 1);
        let leaf_id = 0;
        let leaf_id = format!("{}block{leaf_id}", self.owner_id);
        let mut leaf = TreeBlock::new(&leaf_id, 0);
        root.insert(empty_key.into(), leaf_id.as_bytes().into());
        leaf.insert(empty_key.into(), empty_val.into());
        let root_ch = self.block_cache.create(&ownership_key, root).await;
        let leaf_ch = self.block_cache.create(&ownership_key, leaf).await;
        root_ch.await.unwrap();
        leaf_ch.await.unwrap();
        self.global_ownership
            .add_ownership_key(&self.owner_id, &ownership_key)
            .await;
        let mut ownership = self.ownership.write().await;
        ownership.insert(ownership_key, Arc::new(RwLock::new(())));
    }

    /// Logic for rescaling.
    pub async fn perform_rescaling(
        &self,
        rescaling_uid: &str,
        to_owner: &str,
        remove_self: bool,
        recovery: Option<(usize, Vec<String>)>,
    ) {
        let _ownership_change_lock = self.ownership_change_lock.write().await;
        // If not recovering, avoid redo.
        if recovery.is_none() {
            // Do not repeat already performed operation.
            let running_state = self.running_state.read().await;
            if running_state.last_rescaling_uid == rescaling_uid {
                return;
            }
        }
        // If target of ownership change, mark self as active.
        if to_owner == self.owner_id {
            self.block_cache.reset_load().await;
            self.global_ownership.update_load(1.0).await;
            let this = self.clone();
            tokio::spawn(async move {
                // Must do this async to avoid deadlock.
                this.fetch_new_ownership().await;
            });
            let mut running_state = self.running_state.write().await;
            running_state.mark_active(true).await;
            return;
        }
        // If source to ownership change, must transfer nodes after guarantees that all ongoing queries are finished.
        let (to_transfer, _locks) = match &recovery {
            Some((_, to_transfer)) => (to_transfer.to_vec(), vec![]),
            None => {
                let mut to_transfer = Vec::new();
                let mut locks = Vec::new();
                let mut ownership = self.ownership.write().await;
                for (i, (ownership_key, ownership_lock)) in ownership.iter().enumerate() {
                    if remove_self {
                        locks.push(ownership_lock.clone().write_owned().await);
                        to_transfer.push(ownership_key.clone());
                    } else {
                        // DO NOT COMPARE WITH 0.
                        // This is because the lowest key should not be moved. It belongs to the manager.
                        if i % 2 == 1 {
                            locks.push(ownership_lock.clone().write_owned().await);
                            to_transfer.push(ownership_key.clone());
                        }
                    }
                }
                for k in &to_transfer {
                    ownership.remove(k);
                }
                (to_transfer, locks)
            }
        };
        // Log operation if not recovering.
        let lsn = match recovery {
            Some((lsn, _)) => lsn,
            None => {
                let log_entry = BTreeLogEntry::Rescaling {
                    rescaling_uid: rescaling_uid.into(),
                    to_owner_id: to_owner.into(),
                    to_transfer: to_transfer.clone(),
                    remove_self,
                };
                let lsn = Recovery::write_log_entry(self.plog.clone(), log_entry).await;
                lsn
            }
        };
        // Do transfer.
        for ownership_key in &to_transfer {
            self.block_cache
                .remove_ownership_key(ownership_key, false)
                .await;
            let mut ownership = self.ownership.write().await;
            ownership.remove(ownership_key);
        }
        self.global_ownership
            .perform_ownership_transfer(to_owner, to_transfer)
            .await;
        if remove_self {
            self.global_ownership.remove_load().await;
        } else {
            self.block_cache.reset_load().await;
            self.global_ownership.update_load(1.0).await;
        }
        // Update running state.
        {
            let mut running_state = self.running_state.write().await;
            running_state.mark_rescaling(rescaling_uid).await;
        }
        Recovery::wait_for_writes(self.plog.clone(), lsn, vec![], None).await;
    }

    /// Handle put logic common to normal operations and recovery.
    pub async fn perform_put(
        &self,
        ownership_key: &str,
        key: &str,
        value: Vec<u8>,
        leaf: TreeBlockRef,
        shared_lock: OwnedRwLockReadGuard<()>,
        recovery: Option<(usize, usize)>,
    ) -> bool {
        let mut leaf = leaf.write().await;
        let lsn = match &recovery {
            Some((lsn, leaf_version)) => {
                if leaf.version > *leaf_version {
                    Some(*lsn)
                } else {
                    None
                }
            }
            None => None,
        };
        let mut write_chs = Vec::new();
        let (lsn, should_split_leaf) = if let Some(lsn) = lsn {
            (lsn, false)
        } else {
            let leaf_version = leaf.version;
            leaf.insert(key.as_bytes().to_vec(), value.clone());
            // println!("Leaf size: {}KB. IsFull={}", (leaf.total_size as f64) / 1024.0, leaf.is_full());
            // Log if not recovering.
            let lsn = match recovery {
                Some((lsn, _)) => lsn,
                None => {
                    let log_entry = BTreeLogEntry::Put {
                        ownership_key: ownership_key.into(),
                        key: key.into(),
                        value,
                        leaf_id: leaf.block_id.clone(),
                        leaf_version,
                    };
                    let lsn = Recovery::write_log_entry(self.plog.clone(), log_entry).await;
                    lsn
                }
            };
            let write_ch = self
                .block_cache
                .write_back(ownership_key, &mut leaf)
                .await
                .unwrap();
            write_chs.push(write_ch);
            (lsn, leaf.is_full())
        };
        // Write asynchronously.
        Recovery::wait_for_writes(self.plog.clone(), lsn, write_chs, Some(shared_lock)).await;
        should_split_leaf
    }

    /// Handle delete logic common to recovery and regular operation.
    pub async fn perform_delete(
        &self,
        ownership_key: &str,
        key: &str,
        leaf: TreeBlockRef,
        is_last_leaf: bool,
        shared_lock: OwnedRwLockReadGuard<()>,
        recovery: Option<(usize, usize)>,
    ) -> bool {
        let mut leaf = leaf.write().await;
        let lsn = match &recovery {
            Some((lsn, leaf_version)) => {
                if leaf.version > *leaf_version {
                    Some(*lsn)
                } else {
                    None
                }
            }
            None => None,
        };
        let mut write_chs = Vec::new();
        let (lsn, should_merge_leaf) = if let Some(lsn) = lsn {
            (lsn, false)
        } else {
            let leaf_version = leaf.version;
            leaf.delete(key.as_bytes());
            // Log if not recovering.
            let lsn = match recovery {
                Some((lsn, _)) => lsn,
                None => {
                    let log_entry = BTreeLogEntry::Delete {
                        ownership_key: ownership_key.into(),
                        key: key.into(),
                        leaf_id: leaf.block_id.clone(),
                        leaf_version,
                    };
                    let lsn = Recovery::write_log_entry(self.plog.clone(), log_entry).await;
                    lsn
                }
            };
            let write_ch = self
                .block_cache
                .write_back(ownership_key, &mut leaf)
                .await
                .unwrap();
            write_chs.push(write_ch);
            (lsn, leaf.is_half_empty())
        };
        // Write asynchronously.
        Recovery::wait_for_writes(self.plog.clone(), lsn, write_chs, Some(shared_lock)).await;
        should_merge_leaf && !is_last_leaf
    }

    /// Handle load kv logic common to normal operations and recovery.
    pub async fn perform_load_kv(
        &self,
        ownership_key: &str,
        key: &str,
        value: Vec<u8>,
        leaf: TreeBlockRef,
        shared_lock: OwnedRwLockReadGuard<()>,
    ) -> bool {
        let mut leaf = leaf.write().await;
        let mut write_chs = Vec::new();
        // Insert without logging.
        leaf.insert(key.as_bytes().to_vec(), value.clone());
        let write_ch = self
            .block_cache
            .write_back(ownership_key, &mut leaf)
            .await
            .unwrap();
        write_chs.push(write_ch);
        // Write asynchronously.
        Recovery::wait_for_writes(self.plog.clone(), 0, write_chs, Some(shared_lock)).await;
        leaf.is_full()
    }

    /// Handle load kv logic common to normal operations and recovery.
    pub async fn perform_unload_kv(
        &self,
        ownership_key: &str,
        key: &str,
        leaf: TreeBlockRef,
        is_last_leaf: bool,
        shared_lock: OwnedRwLockReadGuard<()>,
    ) -> bool {
        let mut leaf = leaf.write().await;
        let mut write_chs = Vec::new();
        // Insert without logging.
        leaf.delete(key.as_bytes());
        let write_ch = self
            .block_cache
            .write_back(ownership_key, &mut leaf)
            .await
            .unwrap();
        write_chs.push(write_ch);
        // Write asynchronously.
        Recovery::wait_for_writes(self.plog.clone(), 0, write_chs, Some(shared_lock)).await;
        leaf.is_half_empty() && !is_last_leaf
    }

    /// Lock ownership shared.
    async fn lock_owner_shared(
        &self,
        key: &str,
    ) -> Result<(String, OwnedRwLockReadGuard<()>, usize), BTreeRespMeta> {
        let ownership = self.global_ownership.find_owner(key, true).await;
        // println!("Ownership key: {ownership:?}");
        let ownership_key = match ownership {
            None => return Err(BTreeRespMeta::InvariantIssue), // There should always be an owner.
            Some((ownership_key, owner_id)) => {
                if self.owner_id != owner_id {
                    let this = self.clone();
                    tokio::spawn(async move {
                        this.fetch_new_ownership().await;
                    });
                    return Err(BTreeRespMeta::BadOwner);
                } else {
                    ownership_key
                }
            }
        };
        // Block ownership changes while this query is processing.
        let ownership = self.ownership.read().await;
        if !ownership.contains_key(&ownership_key) {
            let this = self.clone();
            tokio::spawn(async move {
                this.fetch_new_ownership().await;
            });
            return Err(BTreeRespMeta::LockConflict);
        }
        let lock = ownership.get(&ownership_key).cloned().unwrap();
        let shared_lock = match lock.try_read_owned() {
            Ok(lock) => lock,
            Err(_) => {
                // There is an ongoing ownership change.
                return Err(BTreeRespMeta::LockConflict);
            }
        };
        Ok((ownership_key, shared_lock, ownership.len()))
    }

    pub async fn split_or_merge_leaf(&self, ownership_key: &str, leaf_key: &[u8]) {
        let this = self.clone();
        let ownership_key = ownership_key.to_string();
        let leaf_key = leaf_key.to_vec();
        tokio::spawn(async move {
            let ownership_lock = {
                let ownership = this.ownership.read().await;
                let lock = ownership.get(&ownership_key).cloned();
                if let Some(lock) = lock {
                    lock
                } else {
                    return;
                }
            };
            // println!(
            //     "BTreeStructure::split_or_merge_leaf(). Taking exclusive lock on {ownership_key}!"
            // );
            let _exclusive_lock = ownership_lock.write_owned().await;
            let root_id = LocalOwnership::block_id_from_key(&ownership_key);
            let root = this.block_cache.pin(&ownership_key, &root_id).await;
            if root.is_none() {
                return;
            }
            let root = root.unwrap();
            let leaf = {
                let root = root.read().await;
                if root.data.contains_key(&leaf_key) {
                    let leaf_id = root.data.get(&leaf_key).unwrap().to_vec();
                    let leaf_id = String::from_utf8(leaf_id).unwrap();
                    let leaf = this
                        .block_cache
                        .pin(&ownership_key, &leaf_id)
                        .await
                        .unwrap();
                    let (_, last_leaf_id) = root.data.last_key_value().unwrap();
                    let is_last_leaf = leaf_id.as_bytes() == last_leaf_id;
                    let (should_split, should_merge) = {
                        let leaf = leaf.read().await;
                        (leaf.is_full(), leaf.is_half_empty() && !is_last_leaf)
                    };
                    Some((leaf, leaf_id, should_split, should_merge))
                } else {
                    None
                }
            };
            if leaf.is_none() {
                this.block_cache.unpin(&root_id).await;
                return;
            }
            let (leaf, leaf_id, should_split, should_merge) = leaf.unwrap();
            if should_split {
                this.perform_leaf_split(&ownership_key, root, leaf, None)
                    .await;
            } else if should_merge {
                let (from_leaf, from_leaf_id, from_leaf_key) = {
                    let root = root.read().await;
                    let (from_leaf_key, from_leaf_id) = root.find_next(&leaf_key).unwrap();
                    let from_leaf_id = String::from_utf8(from_leaf_id).unwrap();
                    let from_leaf = this
                        .block_cache
                        .pin(&ownership_key, &from_leaf_id)
                        .await
                        .unwrap();
                    (from_leaf, from_leaf_id, from_leaf_key)
                };
                this.perform_leaf_merge(
                    &ownership_key,
                    root,
                    leaf,
                    Some(from_leaf),
                    &from_leaf_key,
                    None,
                )
                .await;
                // Unpin merged in node.
                this.block_cache.unpin(&from_leaf_id).await;
            }
            // Unpin.
            this.block_cache.unpin(&leaf_id).await;
            this.block_cache.unpin(&root_id).await;
        });
    }

    /// Lookup the root and leaf of a key.
    pub async fn lookup_root_and_leaf(&self, key: &str) -> Result<LookupResult, BTreeRespMeta> {
        // Lock ownership until end of query.
        // When inner node is full, will launch an async thread with exclusive lock.
        let (ownership_key, shared_lock, num_ownerships) = loop {
            match self.lock_owner_shared(key).await {
                Ok(x) => break x,
                Err(resp) => match resp {
                    BTreeRespMeta::LockConflict => {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                        continue;
                    }
                    _ => {
                        println!("No Ownership.");
                        return Err(resp);
                    }
                },
            };
        };

        // Find root.
        let root_id = LocalOwnership::block_id_from_key(&ownership_key);
        let root = self.block_cache.pin(&ownership_key, &root_id).await;
        if root.is_none() {
            // This should never occur. Since we create a default root.
            self.block_cache.unpin(&root_id).await;
            return Err(BTreeRespMeta::InvariantIssue);
        }
        let root = root.unwrap();
        // Find leaf.
        let (leaf_id, split_key, is_last_leaf, split_or_merge_root) = {
            // If the root is full and half empty, asynchronously split it.
            let root = root.read().await;
            let leaf_id = root.find_nearest(key.as_bytes());
            let (leaf_id, split_key, is_last) = if let Some((split_key, leaf_id)) = leaf_id {
                if let Some((_, last_leaf_id)) = root.data.last_key_value() {
                    let is_last = last_leaf_id == &leaf_id;
                    (String::from_utf8(leaf_id).unwrap(), split_key, is_last)
                } else {
                    // This should not happen since there is always a leaf.
                    std::mem::drop(root);
                    self.block_cache.unpin(&root_id).await;
                    return Err(BTreeRespMeta::InvariantIssue);
                }
            } else {
                // This should not happen since there is always a leaf.
                std::mem::drop(root);
                self.block_cache.unpin(&root_id).await;
                return Err(BTreeRespMeta::InvariantIssue);
            };
            (
                leaf_id,
                split_key,
                is_last,
                (root.is_half_empty() && num_ownerships > 1) || root.is_full(),
            )
        };
        let leaf = self.block_cache.pin(&ownership_key, &leaf_id).await;
        if leaf.is_none() {
            // This should never happen. The leaf should always exist if it has a reference in the root.
            return Err(BTreeRespMeta::InvariantIssue);
        }
        let leaf = leaf.unwrap();
        // When root needs to be split or merged, do it asynchronously.
        if split_or_merge_root {
            self.split_or_merge_ownership(&ownership_key).await;
        }

        Ok(LookupResult {
            ownership_key,
            root,
            root_id,
            leaf,
            leaf_id,
            split_key,
            is_last_leaf,
            shared_lock,
        })
    }

    /// Refresh ownership to acquire new ownership keys.
    /// Old ownership keys should be removed in a different way.
    async fn fetch_new_ownership(&self) {
        let _ownership_change_lock = self.ownership_change_lock.write().await;
        let new_ownership: HashSet<String> = self
            .global_ownership
            .fetch_ownership_keys()
            .await
            .into_iter()
            .collect();
        let old_ownership: HashSet<String> = {
            let ownership = self.ownership.read().await;
            ownership.iter().map(|(k, _)| k.clone()).collect()
        };
        if old_ownership != new_ownership {
            let mut ownership = self.ownership.write().await;
            for new_key in new_ownership {
                if !old_ownership.contains(&new_key) {
                    ownership.insert(new_key, Arc::new(tokio::sync::RwLock::new(())));
                }
            }
        }
    }

    /// When this is called.
    async fn split_or_merge_ownership(&self, ownership_key: &str) {
        // Copy objects for async block.
        let this = self.clone();
        let ownership_key = ownership_key.to_string();
        tokio::spawn(async move {
            // Only allow one ownership change at a time.
            let _ownership_change_lock = this.ownership_change_lock.clone().write_owned().await;
            // Grab an exclusive lock on this root's ownership.
            // This guarantees that all ongoing queries below this root have completed.
            // Also return the number of owned roots to see if merging is possible.
            let (ownership_lock, num_ownerships) = {
                let ownership = this.ownership.read().await;
                let num_ownerships = ownership.len();
                let lock = ownership.get(&ownership_key).cloned();
                if let Some(lock) = lock {
                    (lock, num_ownerships)
                } else {
                    return;
                }
            };
            let _ownership_lock = ownership_lock.write_owned().await;
            // Check if should merge or split.
            let root_id = LocalOwnership::block_id_from_key(&ownership_key);
            let root = this.block_cache.pin(&ownership_key, &root_id).await;
            if root.is_none() {
                return;
            }
            let root = root.unwrap();
            let (full, empty) = {
                let root = root.read().await;
                (root.is_full(), root.is_half_empty())
            };
            if full {
                // Do splitting.
                this.perform_root_split(&ownership_key, root.clone(), None)
                    .await;
            } else if empty {
                // Do merging if possible.
                if num_ownerships > 1 {
                    let RootAdjacencyResult {
                        from_root,
                        from_ownership_key,
                        to_root,
                        to_ownership_key,
                        other_exclusive_lock,
                    } = this.find_adjacent_root(root.clone(), &ownership_key).await;
                    let _other_exclusive_lock = other_exclusive_lock;
                    println!("Merging {from_ownership_key} into {to_ownership_key}");
                    this.perform_root_merge(
                        Some(from_root),
                        &from_ownership_key,
                        to_root,
                        &to_ownership_key,
                        None,
                    )
                    .await;
                    // Unpin other block.
                    let other_ownership_key = if from_ownership_key != ownership_key {
                        from_ownership_key
                    } else {
                        to_ownership_key
                    };
                    this.block_cache
                        .unpin(&LocalOwnership::block_id_from_key(&other_ownership_key))
                        .await;
                }
            }
            // Unpin.
            this.block_cache.unpin(&root_id).await;
        });
    }

    /// Perform a leaf split.
    pub async fn perform_leaf_split(
        &self,
        ownership_key: &str,
        root: TreeBlockRef,
        leaf: TreeBlockRef,
        recovery: Option<(usize, usize, usize)>,
    ) {
        let mut root = root.write().await;
        let mut leaf = leaf.write().await;
        let split_result = if let Some((lsn, leaf_version, _)) = &recovery {
            if leaf.version > *leaf_version {
                Some(*lsn)
            } else {
                None
            }
        } else {
            None
        };
        // If no valid recovery state, perfom the operation.
        let lsn = if let Some(lsn) = split_result {
            lsn
        } else {
            // Read apriori leaf version.
            let leaf_version = leaf.version;
            // Get new id from counter if not recovering.
            let new_leaf_id_counter = match &recovery {
                Some((_, _, counter)) => *counter,
                None => {
                    let mut running_state = self.running_state.write().await;
                    running_state.get_next_block_id()
                }
            };
            let new_leaf_id = format!("{}block{new_leaf_id_counter}", self.owner_id);
            let (split_key, new_leaf) = leaf.split(new_leaf_id.clone());
            // Write insert+split log entry if not recovering.
            let lsn = match recovery {
                Some((lsn, _, _)) => lsn,
                None => {
                    let log_entry = BTreeLogEntry::SplitLeaf {
                        ownership_key: ownership_key.into(),
                        root_id: root.block_id.clone(),
                        leaf_id: leaf.block_id.clone(),
                        new_leaf_id_counter,
                        leaf_version,
                    };
                    let lsn = Recovery::write_log_entry(self.plog.clone(), log_entry).await;
                    lsn
                }
            };
            // Must write split leaf last for recovery.
            // First create new leaf.
            let create_ch = self.block_cache.create(ownership_key, new_leaf).await;
            create_ch.await.unwrap();
            // Then update root.
            root.insert(split_key, new_leaf_id.as_bytes().to_vec());
            let write_ch = self
                .block_cache
                .write_back(ownership_key, &mut root)
                .await
                .unwrap();
            write_ch.await.unwrap();
            // Finally, update split leaf.
            let write_ch = self
                .block_cache
                .write_back(ownership_key, &mut leaf)
                .await
                .unwrap();
            write_ch.await.unwrap();
            lsn
        };
        // Write completion record.
        Recovery::wait_for_writes(self.plog.clone(), lsn, vec![], None).await;
    }

    /// Perform a leaf split.
    pub async fn perform_leaf_merge(
        &self,
        ownership_key: &str,
        root: TreeBlockRef,
        leaf: TreeBlockRef,
        from_leaf: Option<TreeBlockRef>,
        from_leaf_key: &[u8],
        recovery: Option<usize>,
    ) {
        let merge_result = if let Some(lsn) = &recovery {
            if from_leaf.is_none() {
                // Already completed.
                Some(*lsn)
            } else {
                // Not yet completed. Can be redone.
                None
            }
        } else {
            // Not recovering.
            None
        };
        // Perform op if no valid recovery.
        let lsn = if let Some(lsn) = merge_result {
            // Already merged.
            lsn
        } else {
            let mut root = root.write().await;
            let mut leaf = leaf.write().await;
            let from_leaf = from_leaf.unwrap();
            let mut from_leaf = from_leaf.write().await;
            // Do merge.
            leaf.merge(&mut from_leaf);
            root.delete(from_leaf_key);
            // Log if not recovering.
            let lsn = match recovery {
                Some(lsn) => lsn,
                None => {
                    let log_entry = BTreeLogEntry::MergeLeaf {
                        ownership_key: ownership_key.into(),
                        root_id: root.block_id.clone(),
                        leaf_id: leaf.block_id.clone(),
                        from_leaf_id: from_leaf.block_id.clone(),
                        from_leaf_key: from_leaf_key.to_vec(),
                    };
                    let lsn = Recovery::write_log_entry(self.plog.clone(), log_entry).await;
                    lsn
                }
            };
            // The merged-in keys must be synchronously written, or they will be lost.
            let write_ch1 = self
                .block_cache
                .write_back(ownership_key, &mut leaf)
                .await
                .unwrap();
            let write_ch2 = self
                .block_cache
                .write_back(ownership_key, &mut root)
                .await
                .unwrap();
            write_ch1.await.unwrap();
            write_ch2.await.unwrap();
            // The next leaf can now be deleted.
            let delete_ch = self
                .block_cache
                .write_back(ownership_key, &mut from_leaf)
                .await
                .unwrap();
            delete_ch.await.unwrap();
            lsn
        };
        // Synchronously write completion record.
        Recovery::wait_for_writes(self.plog.clone(), lsn, vec![], None).await;
    }

    /// Common logic used by normal operation and by recovery.
    pub async fn perform_root_split(
        &self,
        ownership_key: &str,
        root: TreeBlockRef,
        recovery: Option<(usize, usize, String)>,
    ) {
        let mut root = root.write().await;
        // Check recovery state if any.
        let split_result = if let Some((lsn, root_version, new_ownership_key)) = &recovery {
            if root.version > *root_version {
                // Writes already performed.
                Some((*lsn, new_ownership_key.clone()))
            } else {
                // Writes not yet performed. Must redo.
                None
            }
        } else {
            // No recovery.
            None
        };
        // If no valid recovery state, perfom the operation.
        let (lsn, new_ownership_key) = if let Some((lsn, new_ownership_key)) = split_result {
            (lsn, new_ownership_key)
        } else {
            // Split block in memory.
            // Read pre-split version first.
            let root_version = root.version;
            let (split_key, mut new_root) = root.split("".into());
            let split_key = String::from_utf8(split_key).unwrap();
            let new_ownership_key = format!("/okey/{split_key}");
            let new_root_id = LocalOwnership::block_id_from_key(&new_ownership_key);
            new_root.block_id = new_root_id;
            // Persist log entry if not recovering.
            let lsn = match recovery {
                Some((lsn, _, _)) => lsn,
                None => {
                    let log_entry = BTreeLogEntry::SplitRoot {
                        ownership_key: ownership_key.into(),
                        new_ownership_key: new_ownership_key.clone(),
                        root_version,
                    };
                    let lsn = Recovery::write_log_entry(self.plog.clone(), log_entry).await;
                    lsn
                }
            };
            // Transfer ownership of blocks to new root.
            let to_transfer: Vec<String> = new_root
                .data
                .values()
                .map(|block_id| String::from_utf8(block_id.clone()).unwrap())
                .collect();
            self.block_cache
                .move_ownership(ownership_key, &new_ownership_key, to_transfer)
                .await;
            // Write new root, then old root.
            let create_ch = self.block_cache.create(&new_ownership_key, new_root).await;
            create_ch.await.unwrap();
            let write_ch = self
                .block_cache
                .write_back(ownership_key, &mut root)
                .await
                .unwrap();
            write_ch.await.unwrap();
            (lsn, new_ownership_key)
        };
        // Add to ownership.
        self.global_ownership
            .add_ownership_key(&self.owner_id, &new_ownership_key)
            .await;
        {
            let mut ownership = self.ownership.write().await;
            ownership.insert(new_ownership_key.clone(), Arc::new(RwLock::new(())));
        }
        // Synchronously write completion record.
        Recovery::wait_for_writes(self.plog.clone(), lsn, vec![], None).await;
    }

    /// Perform a root merge.
    pub async fn perform_root_merge(
        &self,
        from_root: Option<TreeBlockRef>,
        from_ownership_key: &str,
        to_root: TreeBlockRef,
        to_ownership_key: &str,
        recovery: Option<(usize, usize)>,
    ) {
        let mut to_root = to_root.write().await;
        let merge_res = if let Some((lsn, to_version)) = &recovery {
            if to_root.version > *to_version {
                Some(*lsn)
            } else {
                None
            }
        } else {
            None
        };
        let lsn = if let Some(lsn) = merge_res {
            lsn
        } else {
            let from_root = from_root.clone().unwrap();
            let mut from_root = from_root.write().await;
            // Read pre-merge version first.
            let to_version = to_root.version;
            // Write log entry if not recovering.
            let lsn = match &recovery {
                Some((lsn, _)) => *lsn,
                None => {
                    let log_entry = BTreeLogEntry::MergeRoot {
                        to_ownership_key: to_ownership_key.into(),
                        from_ownership_key: from_ownership_key.into(),
                        to_version,
                    };
                    let lsn = Recovery::write_log_entry(self.plog.clone(), log_entry).await;
                    lsn
                }
            };
            // Transfer ownership of blocks.
            let to_transfer: Vec<String> = from_root
                .data
                .values()
                .map(|block_id| String::from_utf8(block_id.clone()).unwrap())
                .collect();
            self.block_cache
                .move_ownership(from_ownership_key, to_ownership_key, to_transfer)
                .await;
            // Merge and synchrously write.
            to_root.merge(&mut from_root);
            let write_ch = self
                .block_cache
                .write_back(to_ownership_key, &mut to_root)
                .await
                .unwrap();
            write_ch.await.unwrap();
            lsn
        };
        // Mark from root as deleted it it exists.
        if let Some(from_root) = from_root {
            let mut from_root = from_root.write().await;
            from_root.deleted = true;
            from_root.version += 1;
            let delete_ch = self
                .block_cache
                .write_back(from_ownership_key, &mut from_root)
                .await
                .unwrap();
            delete_ch.await.unwrap();
        }
        // Delete ownership.
        {
            let mut ownership = self.ownership.write().await;
            ownership.remove(from_ownership_key);
        }
        println!("Delete root ownership after merge: {from_ownership_key:?}");
        self.block_cache
            .remove_ownership_key(from_ownership_key, true)
            .await;
        self.global_ownership
            .remove_ownership_key(&self.owner_id, from_ownership_key)
            .await;

        // Synchronously write completion record now that all operations are done.
        Recovery::wait_for_writes(self.plog.clone(), lsn, vec![], None).await;
    }

    /// Find an adjancent root.
    async fn find_adjacent_root(
        &self,
        root: TreeBlockRef,
        ownership_key: &str,
    ) -> RootAdjacencyResult {
        let next_root = {
            let ownership = self.ownership.read().await;
            let ownership_key = ownership_key.to_string();
            let mut range = ownership.range(ownership_key..);
            range.next();
            let next_root = range.next();
            next_root.map(|(x, y)| (x.clone(), y.clone()))
        };
        let (other_ownership_key, other_root_lock, is_after) = match next_root {
            Some((next_ownership_key, next_root_lock)) => {
                (next_ownership_key, next_root_lock, true)
            }
            None => {
                println!("Reverse merging!!!!!!!");
                let ownership = self.ownership.read().await;
                let ownership_key = ownership_key.to_string();
                let mut range = ownership.range(..ownership_key);
                // Range ..key is exclusive of key, so one call to next_back suffices.
                let (other_ownership_key, other_root_lock) = match range.next_back() {
                    Some(x) => x,
                    None => {
                        // TODO: Just replace with next_back().unwrap() when done debugging.
                        println!("Bad ownerships: {:?}", ownership.keys());
                        std::process::exit(1);
                    }
                };
                (other_ownership_key.clone(), other_root_lock.clone(), false)
            }
        };

        let other_root_lock = other_root_lock.write_owned().await;
        let other_root_id = LocalOwnership::block_id_from_key(&other_ownership_key);
        let other_root = self
            .block_cache
            .pin(&other_ownership_key, &other_root_id)
            .await
            .unwrap();
        if !is_after {
            RootAdjacencyResult {
                from_root: root,
                from_ownership_key: ownership_key.into(),
                to_root: other_root,
                to_ownership_key: other_ownership_key,
                other_exclusive_lock: other_root_lock,
            }
        } else {
            RootAdjacencyResult {
                from_root: other_root,
                from_ownership_key: other_ownership_key,
                to_root: root,
                to_ownership_key: ownership_key.into(),
                other_exclusive_lock: other_root_lock,
            }
        }
    }
}
