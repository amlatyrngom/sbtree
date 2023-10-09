use serde::{Deserialize, Serialize};

pub const START_KEY: usize = 1_000_000_000; // Ensures same key length. Must have 10 characters.
pub const KEY_SIZE_B: usize = 100; // Keep small
pub const VAL_SIZE_B: usize = 9900; // Sum of val + key should be multiple of 1000
pub const DATA_SIZE_MB: usize = 20 * 1000; // 20GB
pub const DATA_SIZE_KB: usize = DATA_SIZE_MB * 1000;
pub const NUM_KEYS: usize = DATA_SIZE_KB / ((VAL_SIZE_B + KEY_SIZE_B) / 1000);

#[cfg(test)]
mod tests {
    use crate::btree::btree_bench::{DATA_SIZE_MB, KEY_SIZE_B, NUM_KEYS, START_KEY, VAL_SIZE_B};
    use crate::btree::local_ownership::LocalOwnership;
    use crate::btree::BTreeRespMeta;
    use obelisk::{FunctionalClient, HandlerKit, InstanceInfo, PersistentLog, ServerlessStorage};
    use rand::distributions::Distribution;
    use std::collections::{BTreeMap, HashSet};
    use std::sync::Arc;

    use crate::{BTreeActor, BTreeClient};

    // Test actor kit.
    async fn make_test_actor_kit(reset: bool) -> HandlerKit {
        if reset {
            let _ = std::fs::remove_dir_all(obelisk::common::shared_storage_prefix());
        }
        std::env::set_var("OBK_EXECUTION_MODE", "local_lambda");
        std::env::set_var("OBK_EXTERNAL_ACCESS", false.to_string());
        let instance_info = Arc::new(InstanceInfo {
            peer_id: "111-222-333".into(),
            az: Some("af-sn-1".into()),
            mem: 8192,
            cpus: 4096,
            public_url: Some("chezmoi.com".into()),
            private_url: Some("chezmoi.com".into()),
            service_name: None,
            handler_name: Some("sbactor".into()),
            subsystem: "functional".into(),
            namespace: "sbtree".into(),
            identifier: "sbactor0".into(),
            unique: true,
            persistent: true,
        });

        let serverless_storage = ServerlessStorage::new_from_info(instance_info.clone())
            .await
            .unwrap();

        HandlerKit {
            instance_info,
            serverless_storage, // Does not matter.
        }
    }

    pub fn make_key_val(key: usize, key_size_b: usize, val_size_b: usize) -> (String, Vec<u8>) {
        let key = key.to_string();
        let key = key.repeat(key_size_b / key.len());
        let val = key.repeat(val_size_b / key.len());
        (key, val.as_bytes().into())
    }

    /// Reset test.
    async fn reset_for_test() {
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
                    aws_sdk_dynamodb::types::AttributeValue::S(owner_id.clone()),
                )
                .key(
                    "ownership_key",
                    aws_sdk_dynamodb::types::AttributeValue::S(ownership_key),
                )
                .send()
                .await
                .unwrap();
            dynamo_client
                .delete_item()
                .table_name("sbtree_rescaling")
                .key(
                    "entry_type",
                    aws_sdk_dynamodb::types::AttributeValue::S("load".into()),
                )
                .key(
                    "owner_id",
                    aws_sdk_dynamodb::types::AttributeValue::S(owner_id),
                )
                .send()
                .await
                .unwrap();
        }
    }

    /// Write bench output.
    async fn write_bench_output(points: Vec<(u64, String, f64)>, expt_name: &str) {
        let expt_dir = "results/bench";
        std::fs::create_dir_all(expt_dir).unwrap();
        let mut writer = csv::WriterBuilder::new()
            .from_path(format!("{expt_dir}/{expt_name}.csv"))
            .unwrap();
        for (since, op_type, duration) in points {
            writer
                .write_record(&[since.to_string(), op_type, duration.to_string()])
                .unwrap();
        }
        writer.flush().unwrap();
    }

    /// Benchmarking regime.
    #[derive(Debug)]
    enum RequestRate {
        Low,
        Medium,
        High(usize),
    }

    async fn gen_local_data(btree_actor: &BTreeActor, parallelism: usize) {
        let num_inserts = NUM_KEYS;
        let start_time = std::time::Instant::now();
        // First do sequential inserts to create enough parallelism.
        for i in 0..(num_inserts / parallelism) {
            let key: usize = START_KEY + i * parallelism;
            let (key, value) = make_key_val(key, KEY_SIZE_B, VAL_SIZE_B);
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
                    let key = START_KEY + i;
                    if key % parallelism == 0 {
                        continue;
                    }
                    let (key, value) = make_key_val(key, KEY_SIZE_B, VAL_SIZE_B);
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
    }

    async fn upload_okeys(btree_actor: Arc<BTreeActor>) {
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
                    let body = aws_sdk_s3::primitives::ByteStream::from(body);
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
    }

    // async fn init_bench_test(bc: Arc<BTreeClient>) {
    //     println!("Generating data");
    //     reset_for_test().await;
    //     let okeys = gen_data(8, 1, 8).await;
    //     std::env::set_var("EXECUTION_MODE", "");
    //     reset_for_test().await;
    //     println!("Bulk Loading data");
    //     bulk_load_data(bc.clone(), okeys).await;
    //     let val = bc.get("37").await.unwrap();
    //     println!("Val Len: {}", val.len());
    // }

    async fn init_large_bench_test(step: &str) {
        let reset = step == "gen" || step == "load";
        if reset {
            println!("Resetting");
            reset_for_test().await;
        }
        // Make btree actor.
        let memory_mb: usize = 8192;
        std::env::set_var("OBK_MEMORY", memory_mb.to_string());
        if step == "gen" {
            let test_kit = make_test_actor_kit(true).await;
            let btree_actor = Arc::new(BTreeActor::new(test_kit).await);
            gen_local_data(&btree_actor, 8).await;
            let sleep_time = 5 + DATA_SIZE_MB / 20; // Sleep for long enough to allow splits.
            for _ in 0..sleep_time {
                println!("Waiting for splits...");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            println!("There should be no ongoing splits after this");
            btree_actor.tree_structure.block_cache.clear().await; // Close dbs.
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            std::process::exit(0);
        }
        if step == "restore" {
            // TODO: Restore generated stuff.
        }
        if step == "upload" {
            let test_kit = make_test_actor_kit(false).await;
            let btree_actor = Arc::new(BTreeActor::new(test_kit).await);
            let okeys = btree_actor
                .tree_structure
                .global_ownership
                .all_ownership_keys()
                .await;
            println!("Num keys: {}", okeys.len());
            let okeys = serde_json::to_string(&okeys).unwrap();
            std::fs::write("loading_okeys.json", okeys).unwrap();
            upload_okeys(btree_actor.clone()).await;
            std::process::exit(0);
        }
        if step == "load" {
            // std::env::set_var("EXECUTION_MODE", "");
            let bc = Arc::new(BTreeClient::new().await);
            let okeys = std::fs::read_to_string("loading_okeys.json").unwrap();
            let okeys: BTreeMap<String, String> = serde_json::from_str(&okeys).unwrap();
            bulk_load_data(bc.clone(), okeys).await;
            let (key, _) = make_key_val(START_KEY + 37, KEY_SIZE_B, VAL_SIZE_B);
            let val = bc.get(&key).await.unwrap();
            println!("Val Len: {}", val.len());
            std::process::exit(0);
        }
        if step == "check" {
            // std::env::set_var("EXECUTION_MODE", "");
            let bc = Arc::new(BTreeClient::new().await);
            for key in [37, 7337, 2023, 666, 999999, 1000000] {
                let key = START_KEY + key;
                let (key, expected) = make_key_val(key, KEY_SIZE_B, VAL_SIZE_B);
                let val = bc.get(&key.to_string()).await.unwrap();
                println!("Val Len: {}", val.len());
                assert_eq!(val, expected);
            }
        }
        if step == "check_scale" {
            // std::env::set_var("EXECUTION_MODE", "");
            let bc = Arc::new(BTreeClient::new().await);
            for _ in 0..10000 {
                let key = START_KEY + 37;
                let (key, expected) = make_key_val(key, KEY_SIZE_B, VAL_SIZE_B);
                let val = bc.get(&key.to_string()).await.unwrap();
                println!("Val Len: {}", val.len());
                assert_eq!(val, expected);
            }
        }
    }

    // #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    // async fn test_gen_data() {
    //     let bc = Arc::new(BTreeClient::new().await);
    //     init_bench_test(bc.clone()).await;
    // }

    async fn bulk_load_data(bc: Arc<BTreeClient>, okeys: BTreeMap<String, String>) {
        // Cleanup all data.
        println!("Bulk Loading: {okeys:?}");
        bc.cleanup().await;
        let manager_mc = bc.fc.clone();
        let okeys: Vec<String> = okeys.keys().cloned().collect();
        let chunks: Vec<Vec<String>> = okeys.chunks(4).map(|c| c.to_vec()).collect();
        for (_, chunk) in chunks.into_iter().enumerate() {
            println!("Bulk loading {chunk:?}");
            let msg = crate::btree::BTreeReqMeta::BulkLoad {
                okeys: chunk.clone(),
            };
            let msg = serde_json::to_string(&msg).unwrap();
            let mut loaded = false;
            loop {
                let resp = manager_mc.invoke(&msg, &[]).await;
                loaded = resp.is_ok();
                if loaded {
                    break;
                } else {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
            if loaded {
                println!("Bulk loaded {chunk:?}.");
            } else {
                println!("Could not bulk load: {chunk:?}.");
                std::process::exit(1);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn btree_bench() {
        let step = "check_scale";
        init_large_bench_test(step).await;
        if step != "run" {
            return;
        }
        // let fc = Arc::new(FunctionalClient::new("sbtree").await);
        // // run_benchmark(
        // //     fc.clone(),
        // //     "pre",
        // //     RequestRate::Low,
        // //     RequestDistribution::Zipf,
        // //     num_keys,
        // //     16,
        // // )
        // // .await;
        // run_benchmark(
        //     fc.clone(),
        //     "pre",
        //     RequestRate::Medium,
        //     RequestDistribution::Zipf,
        //     num_keys,
        //     16,
        // )
        // .await;
        // // run_benchmark(
        // //     fc.clone(),
        // //     "pre",
        // //     RequestRate::High(8),
        // //     RequestDistribution::Zipf,
        // //     num_keys,
        // //     16,
        // // )
        // // .await;
        // // run_benchmark(
        // //     fc.clone(),
        // //     "post",
        // //     RequestRate::Low,
        // //     RequestDistribution::Zipf,
        // //     num_keys,
        // //     16,
        // // )
        // // .await;

        // run_benchmark(
        //     fc.clone(),
        //     "dynamo",
        //     RequestRate::High(8),
        //     RequestDistribution::Cached,
        //     num_keys,
        //     16,
        // )
        // .await;
    }
}
