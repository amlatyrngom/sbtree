#[cfg(test)]
mod tests {
    use crate::{
        btree::{local_ownership::LocalOwnership, BTreeRespMeta, RescalingOp},
        BTreeClient,
    };
    use obelisk::{ActorInstance, FunctionalClient, MessagingClient, PersistentLog};
    use std::{collections::BTreeMap, sync::Arc};

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

    /// Run a simple test.
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

    async fn setup_node(
        btree_client: &BTreeClient,
        node_name: &str,
        spinup: bool,
    ) -> Arc<MessagingClient> {
        println!("Setting up: {node_name}!");
        let mc = btree_client
            .global_ownership
            .get_or_make_client(node_name)
            .await;
        if spinup {
            mc.spin_up().await;
        }
        if node_name == "manager" {
            btree_client.cleanup().await;
        }
        mc
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_actor_test() {
        run_simple_actor_test().await;
    }

    async fn run_simple_actor_test() {
        // Cleanup all data.
        let btree_client = BTreeClient::new().await;
        let _manager_mc = setup_node(&btree_client, "manager", false).await;
        // The empty key always maps to the empty value.
        let value = btree_client.get("").await;
        assert!(matches!(value, Some(v) if v.is_empty()));
        // Lookup non-existent key.
        let resp = btree_client.get("Amadou").await;
        assert!(matches!(resp, None));
        // Insert a key, then look it up.
        btree_client.put("Amadou", b"Ngom".to_vec()).await;
        let resp = btree_client.get("Amadou").await;
        assert!(matches!(resp, Some(v) if v == b"Ngom"));
        // Delete a key, then look it up.
        btree_client.delete("Amadou").await;
        let resp = btree_client.get("Amadou").await;
        assert!(matches!(resp, None));
        // Insert, cleanup, lookup.
        btree_client.put("Amadou", b"Ngom".to_vec()).await;
        btree_client.cleanup().await;
        let resp = btree_client.get("Amadou").await;
        assert!(matches!(resp, None));
    }

    /// Test actor load, unload.
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_actor_load_test() {
        run_simple_actor_load_test().await;
    }

    async fn run_simple_actor_load_test() {
        // Cleanup all data.
        let btree_client = BTreeClient::new().await;
        btree_client.cleanup().await;
        // Load data.
        let num_inserts: usize = 256;
        let kvs = (0..num_inserts)
            .map(|key: usize| (key.to_string(), make_value(key, 1)))
            .collect();
        btree_client.load(kvs).await;
        // Check content.
        for key in 0..num_inserts {
            if key % 32 == 0 {
                // Only check a few values, since messaging is slow on lambda.
                let expected_val = make_value(key, 1);
                let actual_val = btree_client.get(&key.to_string()).await;
                assert!(matches!(actual_val, Some(actual_val) if expected_val == actual_val));
            }
        }
        // Delete half of values
        let keys = (0..(num_inserts / 2))
            .map(|key: usize| key.to_string())
            .collect();
        btree_client.unload(keys).await;
        // Check content.
        for key in 0..num_inserts {
            if key % 32 == 0 {
                // Only check a few values, since messaging is slow on lambda.
                let expected_val = make_value(key, 1);
                let actual_val = btree_client.get(&key.to_string()).await;
                if key < num_inserts / 2 {
                    assert!(matches!(actual_val, None));
                } else {
                    assert!(matches!(actual_val, Some(actual_val) if expected_val == actual_val));
                }
            }
        }
        // Cleanup and check emptiness.
        btree_client.cleanup().await;
        for key in 0..256 {
            if key % 32 == 0 {
                // Only check a few values, since messaging is slow on lambda.
                let actual_val = btree_client.get(&key.to_string()).await;
                assert!(matches!(actual_val, None));
            }
        }
    }

    /// Simple rescaling test.
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_rescale_test() {
        run_simple_rescale_test().await;
    }

    async fn run_simple_rescale_test() {
        let btree_client = BTreeClient::new().await;
        // Spin up to speed up test.
        println!("Spinning up manager and worker!");
        let (manager_mc, worker_mc) = {
            let manager_mc = btree_client
                .global_ownership
                .get_or_make_client("manager")
                .await;
            let btree_client = btree_client.clone();
            let worker_mc = tokio::spawn(async move {
                let worker_mc = btree_client
                    .global_ownership
                    .get_or_make_client("worker1")
                    .await;
                worker_mc.spin_up().await;
                worker_mc
            });
            let (_, worker_mc) = tokio::join!(manager_mc.spin_up(), worker_mc);
            let worker_mc = worker_mc.unwrap();
            (manager_mc, worker_mc)
        };

        println!("Done spinning up");
        // Cleanup all data.
        btree_client.cleanup().await;
        // Sanity check: The empty key always maps to the empty value.
        let value = btree_client.get("").await;
        assert!(matches!(value, Some(v) if v.is_empty()));
        // // Load data.
        let insert_size_mb: usize = 64; // Enough to split roots.
        let load_size_kb: usize = 1024 * 4;
        let val_size_kb: usize = 32;
        let num_inserts = (insert_size_mb * 1024) / val_size_kb;
        let inserts_per_load = load_size_kb / val_size_kb;
        let num_loads = num_inserts / inserts_per_load;
        for i in 0..num_loads {
            let start_key = i * inserts_per_load;
            let end_key = start_key + inserts_per_load;
            let kvs = (start_key..end_key)
                .map(|key: usize| (key.to_string(), make_value(key, val_size_kb)))
                .collect();
            btree_client.load(kvs).await;
            // Keep actors up.
            manager_mc.spin_up().await;
            worker_mc.spin_up().await;
        }

        // Check content.
        let checking_interval_mb = 4; // Interval fully fit in a leaf
        let checking_interval = (checking_interval_mb * 1024) / val_size_kb;
        for key in 0..num_inserts {
            if key % checking_interval == 0 {
                // Only check a few values, since messaging is slow on lambda.
                println!("Checking key {key}.");
                let expected_val = make_value(key, val_size_kb);
                let actual_val = btree_client.get(&key.to_string()).await;
                assert!(matches!(actual_val, Some(actual_val) if expected_val == actual_val));
            }
        }
        // Now rescale.
        manager_mc.spin_up().await;
        worker_mc.spin_up().await;
        let op = RescalingOp::ScaleOut {
            from: "manager".into(),
            to: "worker1".into(),
            uid: uuid::Uuid::new_v4().to_string(),
        };
        btree_client.force_rescale(Some(op)).await;
        // Check content again (different owners should be printed).
        for key in 0..num_inserts {
            if key % checking_interval == 0 {
                // Only check a few values, since messaging is slow on lambda.
                println!("Checking key {key}.");
                let expected_val = make_value(key, val_size_kb);
                let actual_val = btree_client.get(&key.to_string()).await;
                assert!(matches!(actual_val, Some(actual_val) if expected_val == actual_val));
            }
        }
        // Scale in.
        let op = RescalingOp::ScaleIn {
            from: "worker1".into(),
            to: "manager".into(),
            uid: uuid::Uuid::new_v4().to_string(),
        };
        btree_client.force_rescale(Some(op)).await;
        for key in 0..num_inserts {
            if key % checking_interval == 0 {
                // Only check a few values, since messaging is slow on lambda.
                println!("Checking key {key}.");
                let expected_val = make_value(key, val_size_kb);
                let actual_val = btree_client.get(&key.to_string()).await;
                assert!(matches!(actual_val, Some(actual_val) if expected_val == actual_val));
            }
        }

        btree_client.cleanup().await;
    }

    async fn write_bench_output(points: Vec<(u64, f64, f64, f64)>, expt_name: &str) {
        let expt_dir = "results/bench";
        std::fs::create_dir_all(expt_dir).unwrap();
        let mut writer = csv::WriterBuilder::new()
            .from_path(format!("{expt_dir}/{expt_name}.csv"))
            .unwrap();
        for (since, get_duration, put_duration, active_duration) in points {
            writer
                .write_record(&[
                    since.to_string(),
                    get_duration.to_string(),
                    put_duration.to_string(),
                    active_duration.to_string(),
                ])
                .unwrap();
        }
        writer.flush().unwrap();
    }

    /// Benchmarking regime.
    #[derive(Debug)]
    enum RequestRate {
        Low,    // 5%.
        Medium, // 50%.
        High,   // 100%.
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_bench_test() {
        // run_simple_bench_test(true, false, "simple").await;
        run_simple_bench_test("simple", RequestRate::High).await;
    }

    /// Takes ~35 minutes to run to completion.
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn variable_bench_test() {
        run_simple_bench_test("pre", RequestRate::Low).await;
        run_simple_bench_test("pre", RequestRate::Medium).await;
        run_simple_bench_test("mid", RequestRate::High).await;
        run_simple_bench_test("post", RequestRate::Medium).await;
        run_simple_bench_test("post", RequestRate::Low).await;
    }

    async fn run_simple_bench_test(prefix: &str, rate: RequestRate) {
        // Cleanup all data.
        let btree_client = BTreeClient::new().await;
        btree_client.cleanup().await;
        // Put on value
        btree_client.put("Amadou", b"Ngom".to_vec()).await;
        // Run bench.
        let fc = FunctionalClient::new("sbtree").await;
        let (activity, num_workers) = match rate {
            RequestRate::Low => (0.05, 1),
            RequestRate::Medium => (0.5, 1),
            RequestRate::High => (1.0, 1),
        };
        let test_duration = std::time::Duration::from_secs(10 * 60);
        let mut workers = Vec::new();
        for n in 0..num_workers {
            let fc = fc.clone();
            let test_duration = test_duration.clone();
            workers.push(tokio::spawn(async move {
                let mut results: Vec<(u64, f64, f64, f64)> = Vec::new();
                let start_time = std::time::Instant::now();
                loop {
                    let curr_time = std::time::Instant::now();
                    let since = curr_time.duration_since(start_time);
                    if since > test_duration {
                        break;
                    }
                    let since = since.as_millis() as u64;
                    let mut ops = Vec::new();
                    for i in 0..5 {
                        if i == 2 {
                            // ops.push(crate::bench_fn::BenchOp::Put {
                            //     key: "Amadou".into(),
                            //     value: b"Ngom".to_vec(),
                            //     dynamo: false,
                            // });
                            ops.push(crate::bench_fn::BenchOp::Get {
                                key: "Amadou".into(),
                                dynamo: false,
                            });
                        } else {
                            ops.push(crate::bench_fn::BenchOp::Get {
                                key: "Amadou".into(),
                                dynamo: false,
                            });
                        }
                    }
                    let ops = serde_json::to_vec(&ops).unwrap();
                    let start_time = std::time::Instant::now();
                    let resp = fc.invoke(&ops).await;
                    if resp.is_err() {
                        println!("Err: {resp:?}");
                        continue;
                    }
                    let resp = resp.unwrap();
                    let (get_duration, put_duration): (std::time::Duration, std::time::Duration) =
                        serde_json::from_value(resp).unwrap();
                    let end_time = std::time::Instant::now();
                    let active_time = end_time.duration_since(start_time).as_secs_f64() * 1000.0;
                    println!("Worker {n} Get Duration: {get_duration:?}");
                    println!("Worker {n} Put Duration: {put_duration:?}");
                    println!("Worker {n} Since: {since:?}");
                    let get_duration = get_duration.as_secs_f64() * 1000.0;
                    let put_duration = put_duration.as_secs_f64() * 1000.0;
                    results.push((since, get_duration, put_duration, active_time));
                    // Time to wait to get desired activity.
                    let wait_time = active_time / activity - active_time;
                    if wait_time > 1e-4 {
                        let wait_time = std::time::Duration::from_millis(wait_time.ceil() as u64);
                        tokio::time::sleep(wait_time).await;
                    }
                }
                results
            }));
        }
        let mut results: Vec<(u64, f64, f64, f64)> = Vec::new();
        for w in workers {
            let mut r = w.await.unwrap();
            results.append(&mut r);
        }
        write_bench_output(results, &format!("{prefix}_{rate:?}")).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn test_gen_data() {
        // run_simple_bench_test(true, false, "simple").await;
        reset_for_test().await;
        let okeys = gen_data(64, 32, 8).await;
        std::env::set_var("EXECUTION_MODE", "");
        reset_for_test().await;
        bulk_load_data(okeys).await;
        let btree_client = BTreeClient::new().await;
        let val = btree_client.get("37").await.unwrap();
        println!("Val Len: {}", val.len());
    }

    async fn bulk_load_data(okeys: BTreeMap<String, String>) {
        // Cleanup all data.
        let btree_client = BTreeClient::new().await;
        println!("Spinning up manager!");
        let manager_mc = btree_client
            .global_ownership
            .get_or_make_client("manager")
            .await;
        println!("Spun up manager!");
        btree_client.cleanup().await;
        let okeys: Vec<String> = okeys.keys().cloned().collect();
        let chunks: Vec<Vec<String>> = okeys.chunks(8).map(|c| c.to_vec()).collect();
        for chunk in chunks {
            println!("Bulk loading {okeys:?}");
            let msg = crate::btree::BTreeReqMeta::BulkLoad { okeys: chunk };
            let msg = serde_json::to_string(&msg).unwrap();
            manager_mc.send_message(&msg, &[]).await;
            println!("Bulk loaded {okeys:?}");
        }
    }

    async fn gen_data(
        insert_size_mb: usize,
        val_size_kb: usize,
        parallelism: usize,
    ) -> BTreeMap<String, String> {
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
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let all_ownership_keys = btree_actor
            .tree_structure
            .global_ownership
            .all_ownership_keys()
            .await;
        let chunks: Vec<(String, String)> = all_ownership_keys
            .iter()
            .map(|(x, y)| (x.clone(), y.clone()))
            .collect();
        let chunk_size = if chunks.len() <= 16 {
            1
        } else {
            chunks.len() / 16
        };
        let chunks: Vec<Vec<(String, String)>> =
            chunks.chunks(chunk_size).map(|c| c.to_vec()).collect();
        // Drop to close db.
        drop(btree_actor);
        let mut ts = Vec::new();
        let shared_config = aws_config::load_from_env().await;
        for chunk in chunks {
            let s3_client = aws_sdk_s3::Client::new(&shared_config);
            ts.push(tokio::spawn(async move {
                let load_dir = format!("/sbtree/loading");
                println!("Chunk: {chunk:?}");
                for (okey, _owner_id) in &chunk {
                    let block_id = LocalOwnership::block_id_from_key(okey);
                    let data_file = LocalOwnership::data_file_from_key(okey);
                    let body = std::fs::read(&data_file).unwrap();
                    println!("Writing to S3: {block_id}");
                    let body = aws_sdk_s3::types::ByteStream::from(body);
                    let s3_key = format!("{load_dir}/{block_id}");
                    let _resp = s3_client
                        .put_object()
                        .bucket(&obelisk::common::bucket_name())
                        .key(&s3_key)
                        .body(body)
                        .send()
                        .await
                        .unwrap();
                }
            }));
        }
        for t in ts {
            t.await.unwrap();
        }
        all_ownership_keys
    }
}
