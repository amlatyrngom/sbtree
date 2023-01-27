use super::local_ownership::LocalOwnership;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, RwLock};
use std::ops::Bound;


/// Leaves should be around ~256KB.
pub const TARGET_LEAF_SIZE: usize = 1 << 18;
/// Inner nodes have around 1000 children.
pub const TARGET_INNER_LENGTH: usize = 1000;
/// Reference to a block
pub type TreeBlockRef = Arc<RwLock<TreeBlock>>;

/// Tree block.
/// This is somewhat inefficient since it uses serialization/deserialization.
/// Ideally it should directly be byte-encoded, but that's too incovenient.
/// I doubt serialization/deserialization will be an issue.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TreeBlock {
    pub block_id: String,
    pub version: usize,
    pub dirty: bool,
    pub deleted: bool,
    pub height: usize,
    pub total_size: usize,
    pub data: BTreeMap<Vec<u8>, Vec<u8>>,
}

/// Caches blocks shared storage.
/// This cache is not optimized for performance at all, but it at least avoids holding locks during IO.
/// For now, that's enough. Other things will likely be the bottleneck.
/// There are unhandled edges (like simultaneously pinnning a large number of blocks that exceed available memory).
pub struct BlockCache {
    transfer: Arc<Mutex<Transfer>>,
    inner: Arc<Mutex<BlockCacheInner>>,
    total_mem: usize,
    accessed_blocks: dashmap::DashSet<String>,
    stats: Arc<Mutex<BlockCacheStats>>,
    local_ownership: Arc<RwLock<HashMap<String, LocalOwnership>>>,
}

/// Modifyable elements.
struct BlockCacheInner {
    pinned: HashMap<String, Arc<RwLock<TreeBlock>>>,
    pincounts: HashMap<String, usize>,
    unpinned: HashMap<String, Arc<RwLock<TreeBlock>>>,
    unpinned_lru: lru::LruCache<String, ()>,
    avail_space: usize,
}

/// Statistics about block cache.
struct BlockCacheStats {
    load: f64,
    time: std::time::Instant,
}

/// Marks ongoing transfers.
struct Transfer {
    ongoing_reads: HashSet<String>,
    ongoing_writes: HashMap<String, VecDeque<usize>>,
}

impl BlockCache {
    /// Create a new block cache.
    pub async fn new() -> Self {
        // Transfer.
        let transfer = Arc::new(Mutex::new(Transfer {
            ongoing_reads: HashSet::new(),
            ongoing_writes: HashMap::new(),
        }));
        let total_mem = std::env::var("MEMORY").unwrap_or_else(|_| "1".into());
        let total_mem: usize = total_mem.parse().unwrap();
        // 80% of available memory. Rest should be for pinned stuff and for other data structures.
        let total_mem = (total_mem * (1024 * 1024) * 80) / 100;
        // Inner.
        let inner = Arc::new(Mutex::new(BlockCacheInner {
            avail_space: total_mem,
            unpinned: HashMap::new(),
            pinned: HashMap::new(),
            pincounts: HashMap::new(),
            unpinned_lru: lru::LruCache::unbounded(),
        }));
        // Stats.
        let stats = Arc::new(Mutex::new(BlockCacheStats {
            load: 1.0,
            time: std::time::Instant::now(),
        }));
        // Make object.
        BlockCache {
            local_ownership: Arc::new(RwLock::new(HashMap::new())),
            transfer,
            inner,
            stats,
            total_mem,
            accessed_blocks: dashmap::DashSet::new(),
        }
    }

    /// For testing only.
    #[cfg(test)]
    pub async fn check_all_unpinned(&self, exact_unpinned_count: Option<usize>, min_unpinned_count: Option<usize>, max_unpinned_count: Option<usize>) {
        let inner = self.inner.lock().await;
        assert!(inner.pincounts.is_empty());
        assert!(inner.pinned.is_empty());
        if let Some(c) = exact_unpinned_count {
            // println!("Checking {}=={c}", inner.unpinned.len());
            assert!(inner.unpinned.len() == c);
            assert!(inner.unpinned_lru.len() == inner.unpinned.len());
        }
        if let Some(c) = min_unpinned_count {
            // println!("Checking {}>={c}", inner.unpinned.len());
            assert!(inner.unpinned.len() >= c);
            // println!("Comparing unpins: {} and {}", inner.unpinned_lru.len(), inner.unpinned.len());
            assert!(inner.unpinned_lru.len() == inner.unpinned.len());
        }
        if let Some(c) = max_unpinned_count {
            // println!("Checking {}<={c}", inner.unpinned.len());
            assert!(inner.unpinned.len() <= c);
            // println!("Comparing unpins: {} and {}", inner.unpinned_lru.len(), inner.unpinned.len());
            assert!(inner.unpinned_lru.len() == inner.unpinned.len());
        }
    }

    /// Get or make the db associated with this ownership.
    async fn get_or_make_db(&self, ownership_key: &str) -> LocalOwnership {
        let db = {
            let dbs = self.local_ownership.read().await;
            dbs.get(ownership_key).cloned()
        };
        match db {
            Some(db) => db,
            None => {
                let mut dbs = self.local_ownership.write().await;
                let db = LocalOwnership::new(ownership_key).await;
                dbs.insert(ownership_key.into(), db.clone());
                db
            }
        }
    }

    /// Removes ownership.
    /// When moving object to another worker, delete should be set to false.
    pub async fn remove_ownership_key(&self, ownership_key: &str, delete: bool) {
        let db = {
            let mut local_ownership = self.local_ownership.write().await;
            local_ownership.remove(ownership_key)
        };
        if delete {
            if let Some(db) = db {
                db.delete().await;
            }
        }
    }

    /// Move ownership.
    /// Operation must be logged for recovery before hand.
    pub async fn move_ownership(&self, from: &str, to: &str, block_ids: Vec<String>) {
        let from = self.get_or_make_db(from).await;
        let to = self.get_or_make_db(to).await;
        from.move_blocks(&to, block_ids.clone()).await;
    }

    /// Reset load.
    pub async fn reset_load(&self) {
        let mut stats = self.stats.lock().await;
        self.accessed_blocks.clear();
        stats.time = std::time::Instant::now();
        stats.load = 1.0;
    }

    /// Retrieve load.
    /// This measure indicates that if the working set is stable, it should be fully cached.
    pub async fn retrieve_load(&self) -> f64 {
        // Compute new load every 1 minute.
        let mut stats = self.stats.lock().await;
        let curr_time = std::time::Instant::now();
        let since = curr_time.duration_since(stats.time);
        if since < std::time::Duration::from_secs(60) {
            return stats.load;
        }
        // Estimate accessed size and reset set of accessed blocks.
        let access_size = (self.accessed_blocks.len() * TARGET_LEAF_SIZE) as f64;
        self.accessed_blocks.clear();
        // Compute load.
        let total_mem = self.total_mem as f64;
        stats.load = access_size / total_mem;
        if !obelisk::common::has_external_access() {
            // Should never scale out while the actor is a lambda.
            // Likewise, should only scale in under very low activity.
            // This is because low load is likely due to the high messaging latency.
            if stats.load > 1.0 || stats.load < 0.1 {
                stats.load = 1.0;
            }
        }
        stats.time = curr_time;
        stats.load
    }

    /// Record access to a block of data.
    fn record_access(&self, block_id: &str) {
        self.accessed_blocks.insert(block_id.into());
    }

    /// Get or insert a block from the block cache.
    async fn get_or_insert(
        &self,
        block_id: &str,
        tree_block: Option<TreeBlock>,
    ) -> Option<Arc<RwLock<TreeBlock>>> {
        let mut inner = self.inner.lock().await;
        match inner.pinned.get(block_id).cloned() {
            None => match inner.unpinned.remove(block_id) {
                None => match tree_block {
                    None => None,
                    Some(tree_block) => {
                        let block_ref = Arc::new(RwLock::new(tree_block));
                        inner.pinned.insert(block_id.into(), block_ref.clone());
                        inner.pincounts.insert(block_id.into(), 1);
                        Some(block_ref)
                    }
                },
                Some(block_ref) => {
                    inner.unpinned_lru.pop(block_id); // Also remove from lru.
                    let block_ref = match tree_block {
                        None => block_ref,
                        Some(tree_block) => Arc::new(RwLock::new(tree_block)),
                    };
                    inner.avail_space += block_ref.read().await.total_size;
                    inner.pinned.insert(block_id.into(), block_ref.clone());
                    inner.pincounts.insert(block_id.into(), 1);
                    Some(block_ref.clone())
                }
            },
            Some(block_ref) => {
                let block_ref = match tree_block {
                    None => block_ref,
                    Some(tree_block) => Arc::new(RwLock::new(tree_block)),
                };
                inner.pinned.insert(block_id.into(), block_ref.clone());
                let pincount = inner.pincounts.get_mut(block_id).unwrap();
                *pincount += 1;
                Some(block_ref)
            }
        }
    }

    /// Returns (block, retry).
    /// Will first attempt to check cache.
    /// If uncached, it will initiate transfer if no other transfer is ongoing.
    /// If other transfer is ongoing (should be rare), will set retry=true for caller to retry.
    /// TODO: Come with better way of preventing parallel reads and writes from S3.
    async fn pin_aux(
        &self,
        ownership_key: &str,
        block_id: &str,
        force_fetch: bool,
    ) -> (Option<Arc<RwLock<TreeBlock>>>, bool) {
        // Return if already cached.
        self.record_access(block_id);
        if !force_fetch {
            let block = self.get_or_insert(block_id, None).await;
            if block.is_some() {
                return (block, false);
            }
        }
        // Initiate transfer if none ongoing.
        {
            let mut transfer = self.transfer.lock().await;
            if transfer.ongoing_writes.contains_key(block_id) {
                return (None, true);
            }
            if transfer.ongoing_reads.contains(block_id) {
                return (None, true);
            } else {
                transfer.ongoing_reads.insert(block_id.into());
            }
        }
        // Fetch block from local ownership.
        let db = self.get_or_make_db(ownership_key).await;
        let block = match db.fetch_raw_block(block_id).await {
            None => None,
            Some(block) => tokio::task::block_in_place(move || {
                let block = bincode::deserialize(&block).unwrap();
                Some(block)
            }),
        };

        // Insert in cache.
        let block = self.get_or_insert(block_id, block).await;
        // Mark transfer as finished.
        {
            let mut transfer = self.transfer.lock().await;
            transfer.ongoing_reads.remove(block_id);
        }
        // Return
        (block, false)
    }

    /// Pin block in memory.
    pub async fn pin(&self, ownership_key: &str, block_id: &str) -> Option<Arc<RwLock<TreeBlock>>> {
        // Try pinning until success.
        loop {
            let (block, retry) = self.pin_aux(ownership_key, block_id, false).await;
            if retry {
                // Hacky. Should use condition variables.
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            } else {
                break block;
            }
        }
    }

    /// Create a new block.
    /// Return a channel indicating completion when data has been written to S3.
    pub async fn create(&self, ownership_key: &str, block: TreeBlock) -> oneshot::Receiver<()> {
        // Fake pin.
        let block_id = block.block_id.clone();
        let tree_block = {
            let mut inner = self.inner.lock().await;
            let tree_block = Arc::new(RwLock::new(block));
            inner.pincounts.insert(block_id.clone(), 1);
            inner.pinned.insert(block_id.clone(), tree_block.clone());
            tree_block
        };
        // Write.
        let write_ch = {
            let mut block = tree_block.write().await;
            let write_ch = self.write_back(ownership_key, &mut block).await.unwrap();
            write_ch
        };
        // Unpin
        self.unpin(&block_id).await;
        // Return
        write_ch
    }

    /// Push new version to back of the write queue.
    /// This is to guarantee in-order writes.
    async fn initiate_write(transfer: Arc<Mutex<Transfer>>, tree_block: &mut TreeBlock) -> usize {
        let mut transfer = transfer.lock().await;
        if !transfer.ongoing_writes.contains_key(&tree_block.block_id) {
            transfer
                .ongoing_writes
                .insert(tree_block.block_id.clone(), VecDeque::new());
        }
        transfer
            .ongoing_writes
            .get_mut(&tree_block.block_id)
            .unwrap()
            .push_back(tree_block.version);
        tree_block.version
    }

    /// Wait for new version to be at the front of the write queue.
    async fn wait_for_write_turn(
        transfer: Arc<Mutex<Transfer>>,
        block_id: &str,
        write_version: usize,
    ) {
        loop {
            {
                let transfer = transfer.lock().await;
                if transfer.ongoing_writes.get(block_id).unwrap()[0] == write_version {
                    break;
                }
            }
            // Hacky. Should use condition variables.
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    /// Remove new version from front of queue and delete queue if empty (to signal that reads can proceed).
    async fn finish_write_back(transfer: Arc<Mutex<Transfer>>, block_id: &str) {
        let mut transfer = transfer.lock().await;
        let writes = transfer.ongoing_writes.get_mut(block_id).unwrap();
        writes.pop_front();
        if writes.is_empty() {
            transfer.ongoing_writes.remove(block_id);
        }
    }

    /// Perform a write back.
    /// Return a channel to indicate completion.
    pub async fn write_back(
        &self,
        ownership_key: &str,
        tree_block: &mut TreeBlock,
    ) -> Option<oneshot::Receiver<()>> {
        // If block has not changed, no need to do anything.
        if !tree_block.deleted && !tree_block.dirty {
            return None;
        }
        // Completion channels.
        let (write_tx, write_rx) = oneshot::channel();
        // Copy elements for async block.
        let db = self.get_or_make_db(ownership_key).await;
        let block_id = tree_block.block_id.clone();
        let transfer = self.transfer.clone();
        let write_version = Self::initiate_write(transfer.clone(), tree_block).await;
        // Mark as clean.
        tree_block.dirty = false;
        // Check if deleted or just dirty.

        if tree_block.deleted {
            tokio::spawn(async move {
                // Wait for version to come in front of queue.
                Self::wait_for_write_turn(transfer.clone(), &block_id, write_version).await;
                // Delete.
                db.delete_block(&block_id).await;
                // Delete version from queue.
                Self::finish_write_back(transfer, &block_id).await;
                write_tx.send(()).unwrap();
            });
        } else {
            let tree_block = tree_block.clone();
            tokio::spawn(async move {
                // Serialize.
                let data =
                    tokio::task::block_in_place(move || bincode::serialize(&tree_block).unwrap());
                // Wait for write turn.
                Self::wait_for_write_turn(transfer.clone(), &block_id, write_version).await;
                // Write.
                db.write_block(&block_id, data).await;
                // Delete version from queue.
                Self::finish_write_back(transfer, &block_id).await;
                write_tx.send(()).unwrap();
            });
        }
        Some(write_rx)
    }

    /// Unpin. Unpinning thread should not hold a lock on the block.
    /// write_back must have already been called.
    pub async fn unpin(&self, block_id: &str) {
        let mut inner = self.inner.lock().await;
        let block = match inner.pinned.get(block_id).cloned() {
            Some(block) => block,
            None => {
                println!("Unpin wrong block: {block_id}");
                std::process::exit(1);
            }
        };
        let pincount = inner.pincounts.get_mut(block_id).unwrap();
        *pincount -= 1;
        if *pincount == 0 {
            inner.pinned.remove(block_id);
            inner.pincounts.remove(block_id);
            let tree_block = block.read().await;
            if !tree_block.deleted {
                while inner.avail_space < tree_block.total_size {
                    let (evicted_block_id, _): (String, _) = inner.unpinned_lru.pop_lru().unwrap();
                    let evicted_block = inner.unpinned.remove(&evicted_block_id).unwrap();
                    let evicted_tree_block = evicted_block.read().await;
                    inner.avail_space += evicted_tree_block.total_size;
                }
                inner.avail_space -= tree_block.total_size;
                inner.unpinned.insert(block_id.into(), block.clone());
                inner.unpinned_lru.put(block_id.into(), ());
            }
        }
    }
}

impl TreeBlock {
    /// Create a new block.
    pub fn new(id: &str, height: usize) -> TreeBlock {
        TreeBlock {
            block_id: id.to_string(),
            height,
            version: 1,
            dirty: true,
            deleted: false,
            total_size: 0,
            data: BTreeMap::new(),
        }
    }

    /// Check if block is full.
    pub fn is_full(&self) -> bool {
        if self.height == 0 {
            self.total_size >= TARGET_LEAF_SIZE
        } else {
            self.data.len() >= TARGET_INNER_LENGTH
        }
    }

    /// Check if block is half empty.
    pub fn is_half_empty(&self) -> bool {
        if self.height == 0 {
            self.total_size < TARGET_LEAF_SIZE / 2
        } else {
            self.data.len() < TARGET_INNER_LENGTH / 2
        }
    }

    /// Find nearest key.
    pub fn find_nearest(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        let key = key.to_vec();
        let kv = self
            .data
            .range((Bound::Unbounded, Bound::Included(key)))
            .last()
            .map(|(k, v)| (k.clone(), v.clone()));
        if kv.is_some() {
            kv
        } else {
            // Just return first kv pair.
            self.data
                .first_key_value()
                .map(|(k, v)| (k.clone(), v.clone()))
        }
    }

    pub fn find_next(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        let key = key.to_vec();
        let mut range = self.data.range(key..);
        range.next();
        range.next().map(|(x, y)| (x.clone(), y.clone()))
    }

    /// Find exact value for key.
    pub fn find_exact(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.get(key).cloned()
    }

    /// Delete kv pair.
    pub fn delete(&mut self, key: &[u8]) {
        // Advance version.
        self.version += 1;
        self.dirty = true;
        // Delete item and compute new size.
        let old_value = self.data.remove(key);
        if let Some(old_value) = old_value {
            self.total_size -= key.len() + old_value.len();
        }
    }

    /// Insert kv pair.
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Advance version.
        self.version += 1;
        self.dirty = true;
        // Compute new size.
        let key_len = key.len();
        self.total_size += key_len + value.len();
        // Insert.
        let old_value = self.data.insert(key, value);
        // Subtract old entry.
        if let Some(old_value) = old_value {
            self.total_size -= key_len + old_value.len();
        }
    }

    /// Merge two blocks.
    pub fn merge(&mut self, old_block: &mut TreeBlock) {
        // Advance version.
        self.version += 1;
        self.dirty = true;
        // Advance old block's version and mark as deleted.
        old_block.version += 1;
        old_block.dirty = true;
        old_block.deleted = true;
        // Compute new size and merge.
        for (key, value) in old_block.data.iter() {
            let key_len = key.len();
            let val_len = value.len();
            self.total_size += key_len + val_len;
            let old_val = self.data.insert(key.clone(), value.clone());
            // Double merge can happen during recovery.
            if let Some(old_value) = old_val {
                self.total_size -= key_len + old_value.len();
            }
        }
    }

    /// Split a block in half.
    pub fn split(&mut self, new_block_id: String) -> (Vec<u8>, TreeBlock) {
        // Advance version.
        self.version += 1;
        self.dirty = true;
        // Create and fill new block.
        let mut new_block = TreeBlock::new(&new_block_id, self.height);
        while new_block.is_half_empty() {
            let kv = self.data.pop_last();
            if let Some((key, value)) = kv {
                self.total_size -= key.len() + value.len();
                new_block.total_size += key.len() + value.len();
                new_block.data.insert(key.clone(), value.clone());
            } else {
                break;
            }
        }
        let (split_key, _) = new_block.data.first_key_value().unwrap();
        (split_key.clone(), new_block)
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockCache, TreeBlock};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn basic_test() {
        run_basic_test().await;
    }

    async fn run_basic_test() {
        // Create a few blocks.
        let bc = BlockCache::new().await;
        println!("Creating Blocks");
        {
            let mut root = TreeBlock::new("root", 1);
            let child1 = TreeBlock::new("child1", 0);
            let child2 = TreeBlock::new("child2", 0);
            root.insert(b"child1".to_vec(), b"owner1".to_vec());
            root.insert(b"child2".to_vec(), b"owner1".to_vec());
            let create_ch = bc.create("foo", child1).await;
            create_ch.await.unwrap();
            let create_ch = bc.create("foo", child2).await;
            create_ch.await.unwrap();
            let create_ch = bc.create("foo", root).await;
            create_ch.await.unwrap();
        }
        println!("Reading root!");
        let root_ref = bc.pin("foo", "root".into()).await.unwrap();
        {
            let root = root_ref.read().await;
            assert_eq!(root.data.len(), 2);
            assert!(root.version > 0);
            println!("Reading child1!");
        }
        bc.unpin("root".into()).await;
        println!("Reading children!");
        let child1_ref = bc.pin("foo", "child1".into()).await.unwrap();
        let child2_ref = bc.pin("foo", "child2".into()).await.unwrap();
        let (write_ch, delete_ch) = {
            let mut child1 = child1_ref.write().await;
            let mut child2 = child2_ref.write().await;
            child1.insert(b"Amadou1".to_vec(), b"Ngom1".to_vec());
            child2.insert(b"Amadou2".to_vec(), b"Ngom1".to_vec());
            child1.merge(&mut child2);
            assert!(child1.dirty);
            assert!(child2.dirty);
            assert!(child2.deleted);
            println!("Writing children!");
            let write_ch = bc.write_back("foo", &mut child1).await.unwrap();
            let delete_ch = bc.write_back("foo", &mut child2).await.unwrap();
            assert!(!child1.dirty);
            (write_ch, delete_ch)
        };
        write_ch.await.unwrap();
        delete_ch.await.unwrap();
        bc.unpin("child1".into()).await;
        bc.unpin("child2".into()).await;
        // Simulate complete eviction.
        println!("Making new block cache!");
        let bc = BlockCache::new().await;
        println!("Trying to rereading child2!");
        let child2_ref = bc.pin("foo", "child2".into()).await;
        assert!(matches!(child2_ref, None));
        println!("Rereading child1!");
        let child1_ref = bc.pin("foo", "child1".into()).await.unwrap();
        let (write_ch, create_ch) = {
            let mut child1 = child1_ref.write().await;
            assert!(child1.data.contains_key(&b"Amadou1".to_vec()));
            assert!(child1.data.contains_key(&b"Amadou2".to_vec()));
            let (_, child3) = child1.split("child3".into());
            let write_ch = bc.write_back("foo", &mut child1).await.unwrap();
            let create_ch = bc.create("foo", child3).await;
            (write_ch, create_ch)
        };
        write_ch.await.unwrap();
        create_ch.await.unwrap();
        bc.unpin("child1".into()).await;
        // Simulate complete eviction.
        println!("Making new block cache!");
        let bc = BlockCache::new().await;
        println!("Trying to rereading child2!");
        let child2_ref = bc.pin("foo", "child2".into()).await;
        assert!(matches!(child2_ref, None));
        println!("Rereading children!");
        let child1_ref = bc.pin("foo", "child1".into()).await.unwrap();
        let child3_ref = bc.pin("foo", "child3".into()).await.unwrap();
        {
            let child1 = child1_ref.read().await;
            let child3 = child3_ref.read().await;
            assert!(!child1.data.contains_key(&b"Amadou1".to_vec()));
            assert!(child1.data.contains_key(&b"Amadou2".to_vec()));
            assert!(child3.data.contains_key(&b"Amadou1".to_vec()));
            assert!(!child3.data.contains_key(&b"Amadou2".to_vec()));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn ownership_test() {
        run_ownership_test().await;
    }

    async fn run_ownership_test() {
        // Create a few blocks.
        let bc = BlockCache::new().await;
        println!("Creating 1000 blocks for 'foo'");
        // First make 1000 blocks for foo. Then transfer [0, 500) to bar.
        let mut to_transfer = Vec::new();
        for i in 0..1000 {
            let block_id = format!("block{i}");
            if i < 500 {
                to_transfer.push(block_id.clone());
            }
            let mut block = TreeBlock::new(&block_id, 0);
            block.insert(block_id.as_bytes().to_vec(), block_id.as_bytes().to_vec());
            let create_ch = bc.create("foo", block).await;
            create_ch.await.unwrap();
        }
        println!("Moving [0, 500) from foo to bar");
        bc.move_ownership("foo", "bar", to_transfer).await;
        // Reinit block cache.
        let bc = BlockCache::new().await;
        // Now transfer [250, 500) back to foo.
        // Also check that previous transfer was correct.
        let mut to_transfer = Vec::new();
        for i in 0..1000 {
            let block_id = format!("block{i}");
            // Range [500, 750] does not belong to bar, but the code should still work.
            // This handles the recovery case in which blocks were partially moved already.
            if i >= 250 && i < 750 {
                to_transfer.push(block_id.clone());
            }
            // Check ownership.
            let (owner, not_owner) = if i < 500 {
                ("bar", "foo")
            } else {
                ("foo", "bar")
            };
            let block = bc.pin(not_owner, &block_id).await;
            assert!(block.is_none());
            let block = bc.pin(owner, &block_id).await;
            assert!(block.is_some());
            bc.unpin(&block_id).await;
        }
        println!("Moving [250, 500) from bar to foo");
        bc.move_ownership("bar", "foo", to_transfer).await;
        // Reinit block cache.
        let bc = BlockCache::new().await;
        // Now check [0, 250) belongs to bar, and [250, 1000) belongs to foo.
        // Also verify block content.
        for i in 0..1000 {
            let block_id = format!("block{i}");
            // Check ownership.
            let (owner, not_owner) = if i < 250 {
                ("bar", "foo")
            } else {
                ("foo", "bar")
            };
            let block = bc.pin(not_owner, &block_id).await;
            assert!(block.is_none());
            let block = bc.pin(owner, &block_id).await;
            assert!(block.is_some());
            let block = block.unwrap();
            {
                let block = block.read().await;
                assert!(block.block_id == block_id);
                assert!(block.data.contains_key(block_id.as_bytes()));
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn bench_test() {
        run_bench_test().await;
    }

    async fn run_bench_test() {
        let bc = Arc::new(BlockCache::new().await);
        println!("Creating Blocks");
        {
            let mut root = TreeBlock::new("root", 0);
            root.insert(b"child1".to_vec(), b"owner1".to_vec());
            root.insert(b"child2".to_vec(), b"owner1".to_vec());
            let create_ch = bc.create("foo", root).await;
            create_ch.await.unwrap();
        }
        let mut ts = Vec::new();
        let num_tries = 10000;
        let num_threads = 16;
        let start_time = std::time::Instant::now();
        for i in 0..num_threads {
            let bc = bc.clone();
            ts.push(tokio::spawn(async move {
                let start_time = std::time::Instant::now();
                let mut count = 0; // Prevent optimization.
                for _ in 0..num_tries {
                    let root_ref = bc.pin("foo", "root".into()).await.unwrap();
                    {
                        let root = root_ref.read().await;
                        count += root.total_size;
                    }
                    bc.unpin("root".into()).await;
                }
                count /= num_tries;
                let end_time = std::time::Instant::now();
                let duration = end_time.duration_since(start_time);
                println!("Thread {i} count: {count:?}");
                println!("Thread {i} duration: {duration:?}");
            }));
        }
        for t in ts {
            t.await.unwrap();
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Total Duration: {duration:?}");
    }

    #[test]
    fn bincode_bench() {
        let mut data: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        // Entry is a little over 1KB (128B keys, 1KB values).
        // Data is ~1MB.
        let num_entries = super::TARGET_LEAF_SIZE / (1024 + 128);
        println!("Num entries: {num_entries}");
        for i in 0..num_entries {
            let k0 = (i & 0xff) as u8;
            let k1 = ((i >> 8) & 0xff) as u8;
            let k2 = ((i >> 16) & 0xff) as u8;
            let k3 = ((i >> 24) & 0xff) as u8;
            let ks = vec![k0, k1, k2, k3];
            let k: Vec<u8> = (0..128).map(|k| ks[k % 4]).collect();
            let v: Vec<u8> = (0..1024).map(|_| 0).collect();
            data.insert(k, v);
        }
        let mut total_ser_duration = 0.0;
        let mut total_deser_duration = 0.0;
        let mut num_tries = 0.0;
        for _ in 0..5 {
            let start_time = std::time::Instant::now();
            let sdata = bincode::serialize(&data).unwrap();
            println!("Serialized data Len: {}KB", sdata.len() / 1024);
            let end_time = std::time::Instant::now();
            total_ser_duration += end_time.duration_since(start_time).as_secs_f64();
            let start_time = std::time::Instant::now();
            let data: BTreeMap<Vec<u8>, Vec<u8>> = bincode::deserialize(&sdata).unwrap();
            println!("Deserialized data Len: {}", data.len());
            let end_time = std::time::Instant::now();
            total_deser_duration += end_time.duration_since(start_time).as_secs_f64();
            num_tries += 1.0;
        }

        println!(
            "Serialize Duration: {}ms",
            (total_ser_duration / num_tries) * 1000.0
        );
        println!(
            "Deserialize Duration: {}ms",
            (total_deser_duration / num_tries) * 1000.0
        );
    }
}
