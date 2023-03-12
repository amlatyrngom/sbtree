#[cfg(test)]
mod tests {
    use crate::btree::local_ownership::LocalOwnership;
    use crate::btree::BTreeRespMeta;
    use obelisk::{FunctionalClient, PersistentLog};
    use rand::distributions::Distribution;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use crate::{BTreeActor, BTreeClient};

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
                    aws_sdk_dynamodb::model::AttributeValue::S(owner_id.clone()),
                )
                .key(
                    "ownership_key",
                    aws_sdk_dynamodb::model::AttributeValue::S(ownership_key),
                )
                .send()
                .await
                .unwrap();
            dynamo_client
                .delete_item()
                .table_name("sbtree_rescaling")
                .key(
                    "entry_type",
                    aws_sdk_dynamodb::model::AttributeValue::S("load".into()),
                )
                .key(
                    "owner_id",
                    aws_sdk_dynamodb::model::AttributeValue::S(owner_id),
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn local_bench_test() {
        println!("Local Bench Test");
        reset_for_test().await;
        run_local_bench_test().await;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await; // Write backs.
        reset_for_test().await;
    }

    async fn load_local_data(ba: Arc<BTreeActor>, total_size_mb: usize, val_size_kb: usize) {
        let num_inserts = (1024 * total_size_mb) / val_size_kb; // Enough inserts to fill mbs.
        let start_time = std::time::Instant::now();
        for i in 0..num_inserts {
            let key: usize = i;
            let value = make_value(key, val_size_kb);
            let (resp_meta, _) = ba.load_kv(&key.to_string(), value).await;
            assert!(matches!(resp_meta, BTreeRespMeta::Ok));
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Load Duration: {duration:?}");
    }

    async fn local_bench(ba: Arc<BTreeActor>, num_workers: usize) {
        let mut ts = Vec::new();
        for n in 0..num_workers {
            let ba = ba.clone();
            ts.push(tokio::spawn(async move {
                let key = n + 1;
                let mut total_duration = std::time::Duration::from_secs(0);
                let num_tries = 3;
                for _ in 0..num_tries {
                    let start_time = std::time::Instant::now();
                    ba.put(&key.to_string(), make_value(key, 1)).await;
                    let end_time = std::time::Instant::now();
                    total_duration += end_time.duration_since(start_time);
                }
                let avg_duration = total_duration.div_f64(num_tries as f64);
                println!("Worker {n}. Avg Duration: {avg_duration:?}");
            }))
        }
        for t in ts {
            t.await.unwrap();
        }
    }

    async fn run_local_bench_test() {
        let name = "manager";
        let plog = PersistentLog::new("sbtree", name).await.unwrap();
        let ba = Arc::new(BTreeActor::new(name, Arc::new(plog)).await);
        load_local_data(ba.clone(), 8, 1).await;
        local_bench(ba.clone(), 1).await;
        local_bench(ba.clone(), 8).await;
    }

    /// Write bench output.
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
        Low,
        Medium,
        High(usize),
    }

    // /// Simple test.
    // #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    // async fn simple_bench_test() {
    //     // run_simple_bench_test(true, false, "simple").await;
    //     let btree_client = Arc::new(BTreeClient::new().await);
    //     init_bench_test(btree_client.clone()).await;
    //     run_simple_bench_test(btree_client.clone(), "simple", RequestRate::High(1)).await;
    //     run_simple_bench_test(btree_client.clone(), "simple", RequestRate::High(8)).await;
    //     run_simple_bench_test(btree_client.clone(), "simple", RequestRate::Low).await;
    // }

    // /// Takes long to run to completion.
    // #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    // async fn variable_bench_test() {
    //     let btree_client = Arc::new(BTreeClient::new().await);
    //     run_simple_bench_test(btree_client.clone(), "pre", RequestRate::Low).await;
    //     run_simple_bench_test(btree_client.clone(), "pre", RequestRate::Medium).await;
    //     run_simple_bench_test(btree_client.clone(), "mid", RequestRate::High(8)).await;
    //     run_simple_bench_test(btree_client.clone(), "post", RequestRate::Low).await;
    // }

    // async fn run_simple_bench_test(_bc: Arc<BTreeClient>, prefix: &str, rate: RequestRate) {
    //     // Run bench.
    //     let fc = FunctionalClient::new("sbtree").await;
    //     let (activity, num_workers) = match rate {
    //         RequestRate::Low => (0.1, 1),
    //         RequestRate::Medium => (1.0, 1),
    //         RequestRate::High(num_workers) => (1.0, num_workers),
    //     };
    //     let test_duration = std::time::Duration::from_secs(60 * 10); // 10 minutes.
    //     let mut workers = Vec::new();
    //     for n in 0..num_workers {
    //         let fc = fc.clone();
    //         let test_duration = test_duration.clone();
    //         workers.push(tokio::spawn(async move {
    //             let mut results: Vec<(u64, f64, f64, f64)> = Vec::new();
    //             let start_time = std::time::Instant::now();
    //             loop {
    //                 let curr_time = std::time::Instant::now();
    //                 let since = curr_time.duration_since(start_time);
    //                 if since > test_duration {
    //                     break;
    //                 }
    //                 let since = since.as_millis() as u64;
    //                 let mut ops = Vec::new();
    //                 let key = (n + 1) as usize;
    //                 for i in 0..2 {
    //                     if i == 1 {
    //                         ops.push(crate::bench_fn::BenchOp::Put {
    //                             key: key.to_string(),
    //                             value: make_value(key, 1),
    //                             dynamo: false,
    //                         });
    //                         // ops.push(crate::bench_fn::BenchOp::Get {
    //                         //     key: "Amadou".into(),
    //                         //     dynamo: false,
    //                         // });
    //                     } else {
    //                         ops.push(crate::bench_fn::BenchOp::Get {
    //                             key: key.to_string(),
    //                             dynamo: false,
    //                         });
    //                     }
    //                 }
    //                 let ops = serde_json::to_vec(&ops).unwrap();
    //                 let start_time = std::time::Instant::now();
    //                 let resp = fc.invoke(&ops).await;
    //                 if resp.is_err() {
    //                     println!("Err: {resp:?}");
    //                     continue;
    //                 }
    //                 let resp = resp.unwrap();
    //                 let (get_duration, put_duration): (std::time::Duration, std::time::Duration) =
    //                     serde_json::from_value(resp).unwrap();
    //                 let end_time = std::time::Instant::now();
    //                 let mut active_time =
    //                     end_time.duration_since(start_time).as_secs_f64() * 1000.0;
    //                 if n < 3 {
    //                     // Avoid too many prints.
    //                     println!("Worker {n} Get Duration: {get_duration:?}");
    //                     println!("Worker {n} Put Duration: {put_duration:?}");
    //                     println!("Worker {n} Since: {since:?}");
    //                 }
    //                 let get_duration = get_duration.as_secs_f64() * 1000.0;
    //                 let put_duration = put_duration.as_secs_f64() * 1000.0;
    //                 results.push((since, get_duration, put_duration, active_time));
    //                 // Time to wait to get desired activity.
    //                 if active_time < 25.0 {
    //                     // Treat like a lambda because of activity metrics.
    //                     active_time = 25.0;
    //                 }
    //                 let wait_time = active_time / activity - active_time;
    //                 if wait_time > 1e-4 {
    //                     let wait_time = std::time::Duration::from_millis(wait_time.ceil() as u64);
    //                     tokio::time::sleep(wait_time).await;
    //                 }
    //             }
    //             results
    //         }));
    //     }
    //     let mut results: Vec<(u64, f64, f64, f64)> = Vec::new();
    //     for w in workers {
    //         let mut r = w.await.unwrap();
    //         results.append(&mut r);
    //     }
    //     write_bench_output(results, &format!("{prefix}_{rate:?}")).await;
    // }

    async fn gen_local_data(
        btree_actor: &BTreeActor,
        insert_size_mb: usize,
        val_size_kb: usize,
        parallelism: usize,
    ) {
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

    async fn init_large_bench_test(step: &str) -> usize {
        if step == "gen" || step == "load" {
            println!("Resetting for test!");
            reset_for_test().await;
        }
        let insert_size_mb = 16000;
        let val_size_kb = 16;
        let parallism = 4;

        // Make btree actor.
        let memory_mb: usize = 4096;
        std::env::set_var("MEMORY", memory_mb.to_string());
        std::env::set_var("EXECUTION_MODE", "local");
        let name = "manager";
        if step == "gen" {
            let plog = PersistentLog::new("sbtree", name).await.unwrap();
            let btree_actor = Arc::new(BTreeActor::new(name, Arc::new(plog)).await);
            gen_local_data(&btree_actor, insert_size_mb, val_size_kb, parallism).await;
            let sleep_time = 5 + insert_size_mb / 60; // Sleep for long enough to allow splits.
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
            let plog = PersistentLog::new("sbtree", name).await.unwrap();
            let btree_actor = Arc::new(BTreeActor::new(name, Arc::new(plog)).await);
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
            std::env::set_var("EXECUTION_MODE", "");
            let bc = Arc::new(BTreeClient::new().await);
            let okeys = std::fs::read_to_string("loading_okeys.json").unwrap();
            let okeys: BTreeMap<String, String> = serde_json::from_str(&okeys).unwrap();
            bulk_load_data(bc.clone(), okeys).await;
            let val = bc.get("37").await.unwrap();
            println!("Val Len: {}", val.len());
            std::process::exit(0);
        }
        if step == "check" {
            std::env::set_var("EXECUTION_MODE", "");
            let bc = Arc::new(BTreeClient::new().await);
            for key in [37, 7337, 2023, 666, 999999, 1000000] {
                let expected = make_value(key, val_size_kb);
                let val = bc.get(&key.to_string()).await.unwrap();
                println!("Val Len: {}", val.len());
                assert_eq!(val, expected);
            }
        }

        (insert_size_mb * 1024) / val_size_kb
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
        let manager_mc = bc.global_ownership.get_or_make_client("manager").await;
        let okeys: Vec<String> = okeys.keys().cloned().collect();
        let chunks: Vec<Vec<String>> = okeys.chunks(4).map(|c| c.to_vec()).collect();
        for (_, chunk) in chunks.into_iter().enumerate() {
            println!("Bulk loading {chunk:?}");
            let msg = crate::btree::BTreeReqMeta::BulkLoad {
                okeys: chunk.clone(),
            };
            let msg = serde_json::to_string(&msg).unwrap();
            let mut loaded = false;
            for _ in 0..5 {
                let resp = manager_mc.send_message(&msg, &[]).await;
                loaded = resp.is_some();
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
    async fn run_scaling_bench() {
        let step = "run";
        let num_keys = init_large_bench_test(step).await;
        if step != "run" {
            return;
        }
        std::env::set_var("EXECUTION_MODE", "");
        let test_duration = std::time::Duration::from_secs_f64(60.0 * 3.0);
        let start_time = std::time::Instant::now();
        let mut rng = rand::thread_rng();
        let zipf = zipf::ZipfDistribution::new(num_keys, 1.0).unwrap();
        let fc = FunctionalClient::new("sbtree").await;
        loop {
            let curr_time = std::time::Instant::now();
            let since = curr_time.duration_since(start_time);
            if since > test_duration {
                break;
            }
            let mut ops = Vec::new();
            for _ in 0..10 {
                let key: usize = zipf.sample(&mut rng);
                ops.push(crate::bench_fn::BenchOp::Get {
                    key: key.to_string(),
                    dynamo: false,
                });
            }
            let ops = serde_json::to_vec(&ops).unwrap();
            // let start_time = std::time::Instant::now();
            let resp = fc.invoke(&ops).await;
            if resp.is_err() {
                println!("Err: {resp:?}");
                continue;
            }
            let resp = resp.unwrap();
            let (get_duration, put_duration): (Vec<std::time::Duration>, Vec<std::time::Duration>) =
                serde_json::from_value(resp).unwrap();
            // let end_time = std::time::Instant::now();
            // let mut active_time = end_time.duration_since(start_time).as_secs_f64() * 1000.0;
            println!("Get Durations: {get_duration:?}");
            println!("Put Durations: {put_duration:?}");
        }

        
    }
}
