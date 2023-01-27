#[cfg(test)]
mod tests {
    use crate::btree::BTreeRespMeta;
    use obelisk::{PersistentLog, ActorInstance};
    use std::sync::Arc;

    use crate::BTreeActor;

    const LEAF_SIZE_KB: usize = 256;

    /// Reset test.
    async fn reset_for_test() {
        std::env::set_var("EXECUTION_MODE", "local");
        std::env::set_var("MEMORY", "1");
        let storage_dir = obelisk::common::shared_storage_prefix();
        if let Err(e) = std::fs::remove_dir_all(&storage_dir) {
            if e.kind() != std::io::ErrorKind::NotFound {
                panic!("Test directory could not be reset!");
            }
        }
    }


    /// Test simple get, insert, delete.
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_test() {
        reset_for_test().await;
        run_simple_test().await;
    }

    async fn run_simple_test() {
        // Make btree actor.
        let name = "manager";
        let plog = PersistentLog::new("sbtree", name).await;
        let btree_actor = BTreeActor::new(name, Arc::new(plog)).await;
        // The empty key always maps to the empty value.
        let (resp_meta, value) = btree_actor.get("").await;
        assert!(matches!(resp_meta, BTreeRespMeta::Ok));
        assert!(value.is_empty());
        // Try reading a non-existent key.
        let (resp_meta, _) = btree_actor.get("Amadou").await;
        println!("Resp Meta: {resp_meta:?}");
        assert!(matches!(resp_meta, BTreeRespMeta::NotFound));
        // Insert a key, then look it up.
        let (resp_meta, _) = btree_actor.put("Amadou", b"Ngom".to_vec()).await;
        assert!(matches!(resp_meta, BTreeRespMeta::Ok));
        let (resp_meta, value) = btree_actor.get("Amadou").await;
        assert!(matches!(resp_meta, BTreeRespMeta::Ok));
        println!("Resp: {resp_meta:?}; {value:?}");
        assert!(value == b"Ngom");
        // Delete a key, then look it up.
        let (resp_meta, _) = btree_actor.delete("Amadou").await;
        assert!(matches!(resp_meta, BTreeRespMeta::Ok));
        let (resp_meta, _) = btree_actor.get("Amadou").await;
        println!("Resp Meta: {resp_meta:?}");
        assert!(matches!(resp_meta, BTreeRespMeta::NotFound));
        // Check that everything is unpinned.
        btree_actor
            .tree_structure
            .block_cache
            .check_all_unpinned(Some(2), None, None)
            .await;
    }

    /// Test leaf split.
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn leaf_split_merge_test() {
        println!("Split Leaf 256");
        reset_for_test().await;
        run_leaf_split_merge_test(256, true).await;
        println!("Split Leaf 1000");
        reset_for_test().await;
        run_leaf_split_merge_test(1000, true).await;
    }

    async fn run_leaf_split_merge_test(insert_size_kb: usize, force_checkpoint: bool) {
        // Make btree actor.
        let name = "manager";
        let plog = PersistentLog::new("sbtree", name).await;
        let btree_actor = BTreeActor::new(name, Arc::new(plog)).await;        
        assert!(insert_size_kb <= 10000); // Higher could lead to root split.
        // assert!(insert_size_kb > 64); // Lower does not lead to leaf splits.
        // Insert ~1KB kv pairs.
        let mut data: Vec<(usize, Vec<u8>)> = Vec::new();
        for i in 0..insert_size_kb {
            let key: usize = i;
            let v1 = (key & 0xFF) as u8;
            let v2 = ((key >> 8) & 0xFF) as u8;
            let v3 = ((key >> 16) & 0xFF) as u8;
            let v4 = ((key >> 24) & 0xFF) as u8;
            let vs = [v1, v2, v3, v4];
            let value: Vec<u8> = (0..1024).map(|i| vs[i % 4]).collect();
            data.push((key, value));
        }
        // Insert all of them.
        let start_time = std::time::Instant::now();
        for (key, value) in data.iter() {
            let (resp_meta, _) = btree_actor.put(&key.to_string(), value.clone()).await;
            assert!(matches!(resp_meta, BTreeRespMeta::Ok));
        }
        let end_time = std::time::Instant::now();
        let insert_duration = end_time.duration_since(start_time);
        // Check that there is a sufficient number of blocks.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let min_num_blocks = insert_size_kb / LEAF_SIZE_KB + 2;
        let max_num_blocks = (insert_size_kb / LEAF_SIZE_KB) * 2 + 2;
        btree_actor
        .tree_structure
        .block_cache
        .check_all_unpinned(None, Some(min_num_blocks), Some(max_num_blocks))
        .await;
        // Update all of them.
        let start_time = std::time::Instant::now();
        for (key, value) in data.iter() {
            let mut value = value.clone();
            value[0] = 0;
            let (resp_meta, _) = btree_actor.put(&key.to_string(), value.clone()).await;
            assert!(matches!(resp_meta, BTreeRespMeta::Ok));
        }
        let end_time = std::time::Instant::now();
        let update_duration = end_time.duration_since(start_time);
        // Check that there is a sufficient number of blocks.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let min_num_blocks = insert_size_kb / LEAF_SIZE_KB + 2;
        let max_num_blocks = (insert_size_kb / LEAF_SIZE_KB) * 2 + 2;
        btree_actor
        .tree_structure
        .block_cache
        .check_all_unpinned(None, Some(min_num_blocks), Some(max_num_blocks))
        .await;
        // Verify content.
        let start_time = std::time::Instant::now();
        for (key, value) in data.iter() {
            let mut value = value.clone();
            value[0] = 0;
            let (resp_meta, found_value) = btree_actor.get(&key.to_string()).await;
            assert!(matches!(resp_meta, BTreeRespMeta::Ok));
            assert_eq!(value, found_value);
        }
        let end_time = std::time::Instant::now();
        let get_duration = end_time.duration_since(start_time);
        // Delete ordered half of values to forcibly trigger merges.
        let start_time = std::time::Instant::now();
        for (key, _) in data.iter() {
            if *key <= insert_size_kb / 2 {
                let (resp_meta, _) = btree_actor.delete(&key.to_string()).await;
                assert!(matches!(resp_meta, BTreeRespMeta::Ok));
            }
        }
        let end_time = std::time::Instant::now();
        let delete_duration = end_time.duration_since(start_time);
        // Check that there is a sufficient number of blocks.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let min_num_blocks = (insert_size_kb / LEAF_SIZE_KB) / 2 + 2;
        let max_num_blocks = insert_size_kb / LEAF_SIZE_KB + 2;
        btree_actor
            .tree_structure
            .block_cache
            .check_all_unpinned(None, Some(min_num_blocks), Some(max_num_blocks))
            .await;
        // Verify content.
        for (key, value) in data.iter() {
            let (resp_meta, found_value) = btree_actor.get(&key.to_string()).await;
            if *key <= insert_size_kb / 2 {
                assert!(matches!(resp_meta, BTreeRespMeta::NotFound));
            } else {
                let mut value = value.clone();
                value[0] = 0;    
                assert!(matches!(resp_meta, BTreeRespMeta::Ok));
                assert_eq!(value, found_value);
            }
        }
        // Try a checkpoint.
        // Wait for ongoing ops.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let old_flush_lsn = btree_actor.tree_structure.plog.get_flush_lsn().await;
        println!("Old Flush Lsn: {old_flush_lsn}.");
        if force_checkpoint {
            // Make a checkpoint.
            btree_actor.checkpoint(false).await;
        } else {
            // Wait for checkpointing loop iteration.
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
        let new_start_lsn = btree_actor.tree_structure.plog.get_start_lsn().await;
        println!("New Start Lsn: {new_start_lsn}.");
        assert!(new_start_lsn >= old_flush_lsn);
        // Show durations
        println!("Insert Duration: {insert_duration:?}");
        println!("Update Duration: {update_duration:?}");
        println!("Get Duration: {get_duration:?}");
        println!("Delete Duration: {delete_duration:?}");
    }


    /// Test root split.
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn root_split_merge_test() {
        reset_for_test().await;
        run_root_split_merge_test().await;
    }

    async fn run_root_split_merge_test() {
        
    }

}
