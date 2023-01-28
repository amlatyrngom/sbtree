#[cfg(test)]
mod tests {
    use crate::btree::BTreeRespMeta;
    use obelisk::{ActorInstance, PersistentLog};
    use std::sync::Arc;

    use crate::BTreeActor;

    const LEAF_SIZE_KB: usize = crate::btree::block::TARGET_LEAF_SIZE / 1024;
    const _OWNERSHIP_SIZE_KB: usize = crate::btree::block::TARGET_INNER_LENGTH * LEAF_SIZE_KB;

    /// Reset test.
    async fn reset_for_test() {
        std::env::set_var("EXECUTION_MODE", "local");
        std::env::set_var("MEMORY", "1024");
        let storage_dir = obelisk::common::shared_storage_prefix();
        if let Err(e) = std::fs::remove_dir_all(&storage_dir) {
            if e.kind() != std::io::ErrorKind::NotFound {
                panic!("Test directory could not be reset!");
            }
        }
        // Read from dynamodb.
        let shared_config = aws_config::load_from_env().await;
        let dynamo_client = aws_sdk_dynamodb::Client::new(&shared_config);
        let resp = dynamo_client
            .execute_statement()
            .statement("SELECT owner_id, ownership_key FROM sbtree_ownership")
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        let resp: Vec<(String, String)> = resp
            .items()
            .unwrap()
            .iter()
            .map(|item| {
                let owner = item.get("owner_id").unwrap().as_s().cloned().unwrap();
                let ownership_key = item.get("ownership_key").unwrap().as_s().cloned().unwrap();
                (owner, ownership_key)
            })
            .collect();
        for (owner_id, ownership_key) in resp {
            dynamo_client
                .delete_item()
                .table_name("sbtree_ownership")
                .key(
                    "owner_id",
                    aws_sdk_dynamodb::model::AttributeValue::S(owner_id),
                )
                .key(
                    "ownership_key",
                    aws_sdk_dynamodb::model::AttributeValue::S(ownership_key),
                )
                .send()
                .await
                .unwrap();
        }
    }

    fn make_value(key: usize, size_kb: usize) -> Vec<u8> {
        let v1 = (key & 0xFF) as u8;
        let v2 = ((key >> 8) & 0xFF) as u8;
        let v3 = ((key >> 16) & 0xFF) as u8;
        let v4 = ((key >> 24) & 0xFF) as u8;
        let vs = [v1, v2, v3, v4];
        (0..(size_kb * 1024)).map(|i| vs[i % 4]).collect()
    }

    /// Test simple get, insert, delete.
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_test() {
        reset_for_test().await;
        run_simple_test().await;
        // Give time to write backs.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
        // Load a key, then lookup it up.
        println!("Testing load!");
        let (resp_meta, _) = btree_actor.load_kv("Amadou", b"Ngom".to_vec()).await;
        assert!(matches!(resp_meta, BTreeRespMeta::Ok));
        let (resp_meta, value) = btree_actor.get("Amadou").await;
        assert!(matches!(resp_meta, BTreeRespMeta::Ok));
        println!("Resp: {resp_meta:?}; {value:?}");
        assert!(value == b"Ngom");
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
        // println!("Leaf Test 1");
        // reset_for_test().await;
        // run_leaf_split_merge_test(1, true).await;
        println!("Leaf Test 1024");
        reset_for_test().await;
        run_leaf_split_merge_test(1024, true).await;
        // Give time to write backs.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    async fn run_leaf_split_merge_test(insert_size_kb: usize, force_checkpoint: bool) {
        // Make btree actor.
        let name = "manager";
        let plog = PersistentLog::new("sbtree", name).await;
        let btree_actor = BTreeActor::new(name, Arc::new(plog)).await;
        // Insert values.
        assert!(insert_size_kb <= 10000); // Higher could lead to root split.
        let mut data: Vec<(usize, Vec<u8>)> = Vec::new();
        for i in 0..insert_size_kb {
            let key: usize = i;
            let value = make_value(key, 1);
            data.push((key, value));
        }
        // Load all of them.
        let start_time = std::time::Instant::now();
        for (key, value) in data.iter() {
            let (resp_meta, _) = btree_actor.load_kv(&key.to_string(), value.clone()).await;
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
            let (resp_meta, found_value) = btree_actor.get(&key.to_string()).await;
            assert!(matches!(resp_meta, BTreeRespMeta::Ok));
            assert_eq!(value, &found_value);
        }
        let end_time = std::time::Instant::now();
        let get_duration = end_time.duration_since(start_time);
        // Delete ordered half of values to forcibly trigger merges.
        let start_time = std::time::Instant::now();
        for (key, _) in data.iter() {
            if *key <= insert_size_kb / 2 {
                let (resp_meta, _) = btree_actor.unload_kv(&key.to_string()).await;
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
                assert!(matches!(resp_meta, BTreeRespMeta::Ok));
                assert_eq!(value, &found_value);
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
        println!("Get Duration: {get_duration:?}");
        println!("Delete Duration: {delete_duration:?}");
    }

    /// Test root split.
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn root_split_merge_test() {
        println!("Root Test 1024");
        reset_for_test().await;
        run_root_split_merge_test(1024, 32, 16).await;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await; // Write backs.
        reset_for_test().await;
    }

    async fn run_root_split_merge_test(
        insert_size_mb: usize,
        val_size_kb: usize,
        parallelism: usize,
    ) {
        // Make btree actor.
        let name = "manager";
        let plog = PersistentLog::new("sbtree", name).await;
        let btree_actor = BTreeActor::new(name, Arc::new(plog)).await;
        let num_inserts = (1024 * insert_size_mb) / val_size_kb; // Enough inserts to fill mbs.
        let start_time = std::time::Instant::now();
        // First do sequential inserts to create enough parallelism.
        for i in 0..(num_inserts / parallelism) {
            let key: usize = i * parallelism;
            let value = make_value(key, val_size_kb);
            let (resp_meta, _) = btree_actor.load_kv(&key.to_string(), value).await;
            assert!(matches!(resp_meta, BTreeRespMeta::Ok));
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Sequential Insert Duration: {duration:?}");
        // Now go parallel.
        let start_time = std::time::Instant::now();
        let mut ts = Vec::new();
        for t in 0..parallelism {
            let lo = t * (num_inserts / parallelism);
            let hi = lo + (num_inserts / parallelism);
            let btree_actor = btree_actor.clone();
            ts.push(tokio::spawn(async move {
                for i in lo..hi {
                    let key = i;
                    if key % parallelism == 0 {
                        continue;
                    }
                    let value = make_value(key, val_size_kb);
                    let (resp_meta, _) = btree_actor.load_kv(&key.to_string(), value).await;
                    assert!(matches!(resp_meta, BTreeRespMeta::Ok));
                }
            }));
        }
        for t in ts {
            t.await.unwrap();
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Parallel Insert Duration: {duration:?}");

        // Verify content.
        println!("Verifying content");
        let start_time = std::time::Instant::now();
        for i in 0..num_inserts {
            let key = i;
            let value = make_value(key, val_size_kb);
            let (resp_meta, found_value) = btree_actor.get(&key.to_string()).await;
            assert!(matches!(resp_meta, BTreeRespMeta::Ok));
            assert_eq!(value, found_value);
        }

        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Read Duration: {duration:?}");

        // Now delete half.
        let start_time = std::time::Instant::now();
        let mut ts = Vec::new();
        let num_deletes = num_inserts / 2;
        for t in 0..parallelism {
            let lo = t * (num_deletes / parallelism);
            let hi = lo + (num_deletes / parallelism);
            let btree_actor = btree_actor.clone();
            ts.push(tokio::task::spawn(async move {
                for i in lo..hi {
                    let key = i;
                    let (resp_meta, _) = btree_actor.unload_kv(&key.to_string()).await;
                    assert!(matches!(resp_meta, BTreeRespMeta::Ok));
                }
            }));
        }
        for t in ts {
            t.await.unwrap();
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Delete Duration: {duration:?}");
        for i in 0..num_inserts {
            let key = i;
            let value = make_value(key, val_size_kb);
            let (resp_meta, found_value) = btree_actor.get(&key.to_string()).await;
            if i < num_deletes {
                match &resp_meta {
                    BTreeRespMeta::NotFound => continue,
                    x => {
                        println!("Key {i}. Bad Resp({x:?}). Value={found_value:?}");
                        assert!(false);
                    }
                }
                assert!(matches!(resp_meta, BTreeRespMeta::NotFound));
            } else {
                assert!(matches!(resp_meta, BTreeRespMeta::Ok));
                assert_eq!(value, found_value);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_actor_test() {
        reset_for_test().await;
        run_simple_actor_test().await;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await; // Write backs.
    }

    async fn run_simple_actor_test() {
        
    }

}
