use crate::BTreeClient;
use aws_sdk_dynamodb::types::AttributeValue;
use obelisk::{HandlerKit, ScalingState, ServerlessHandler};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

pub const START_KEY: usize = 1_000_000_000; // Ensures same key length. Must have 10 characters.
pub const KEY_SIZE_B: usize = 100; // Keep small
pub const VAL_SIZE_B: usize = 9900; // Sum of val + key should be multiple of 1000
pub const DATA_SIZE_MB: usize = 20 * 1000; // 20GB
pub const DATA_SIZE_KB: usize = DATA_SIZE_MB * 1000;
pub const NUM_KEYS: usize = DATA_SIZE_KB / ((VAL_SIZE_B + KEY_SIZE_B) / 1000);

/// Benchmark Function.
/// TODO: Add a dynamodb option.
pub struct BenchFn {
    btree_client: Arc<BTreeClient>,
    dynamo_client: aws_sdk_dynamodb::Client,
}

/// A benchmark option.
#[derive(Serialize, Deserialize)]
pub struct BenchOps {
    reads: Vec<usize>,
    writes: Vec<usize>,
    is_sbtree: bool,
}

#[async_trait::async_trait]
impl ServerlessHandler for BenchFn {
    async fn handle(&self, meta: String, _payload: Vec<u8>) -> (String, Vec<u8>) {
        let ops: BenchOps = serde_json::from_str(&meta).unwrap();
        self.do_ops(ops).await
    }

    /// Do checkpointing.
    async fn checkpoint(&self, _scaling_state: &ScalingState, _terminating: bool) {}
}

impl BenchFn {
    /// Create.
    pub async fn new(_kit: HandlerKit) -> Self {
        let shared_config = aws_config::load_from_env().await;
        let dynamo_client = aws_sdk_dynamodb::Client::new(&shared_config);
        BenchFn {
            btree_client: Arc::new(BTreeClient::new().await),
            dynamo_client,
        }
    }

    pub async fn do_ops(&self, ops: BenchOps) -> (String, Vec<u8>) {
        let mut durations = Vec::new();
        // Do reads
        for key in ops.reads {
            let (key, _value) = make_key_val(key, KEY_SIZE_B, VAL_SIZE_B, false);
            let start_time = std::time::Instant::now();
            let is_ok = if !ops.is_sbtree {
                let resp = self
                    .dynamo_client
                    .get_item()
                    .table_name("dynamo_bench")
                    .consistent_read(true)
                    .key("key", AttributeValue::S(key))
                    .send()
                    .await;
                resp.is_ok()
            } else {
                let _resp = self.btree_client.get(&key).await;
                _resp.is_some()
            };
            if is_ok {
                let end_time = std::time::Instant::now();
                let duration = end_time.duration_since(start_time);
                durations.push((duration, String::from("Read")));
            }
        }
        for key in ops.writes {
            let (key, value) = make_key_val(key, KEY_SIZE_B, VAL_SIZE_B, true);
            let start_time = std::time::Instant::now();
            let is_ok = if !ops.is_sbtree {
                let resp = self
                    .dynamo_client
                    .put_item()
                    .table_name("dynamo_bench")
                    .item("key", AttributeValue::S(key))
                    .item(
                        "value",
                        AttributeValue::S(String::from_utf8(value).unwrap()),
                    )
                    .send()
                    .await;
                resp.is_ok()
            } else {
                let _resp = self.btree_client.put(&key, value).await;
                true
            };
            if is_ok {
                let end_time = std::time::Instant::now();
                let duration = end_time.duration_since(start_time);
                durations.push((duration, String::from("Write")));
            }
        }
        (serde_json::to_string(&durations).unwrap(), vec![])
    }
}

pub fn make_key_val(
    key: usize,
    key_size_b: usize,
    val_size_b: usize,
    random: bool,
) -> (String, Vec<u8>) {
    let key = key.to_string();
    let key = key.repeat(key_size_b / key.len());
    let base = if random {
        let base = uuid::Uuid::new_v4().to_string();
        let base = &base.as_bytes()[0..10];
        let base = String::from_utf8(base.into()).unwrap();
        base.repeat(key_size_b / base.len())
    } else {
        key.clone()
    };
    let val = base.repeat(val_size_b / base.len());
    assert!(val.len() == val_size_b);
    (key, val.as_bytes().into())
}

#[cfg(test)]
mod tests {
    use super::make_key_val;
    use super::{BenchOps, DATA_SIZE_MB, KEY_SIZE_B, NUM_KEYS, START_KEY, VAL_SIZE_B};
    use crate::btree::local_ownership::LocalOwnership;
    use crate::btree::BTreeRespMeta;
    use aws_sdk_dynamodb::types::AttributeValue;
    use obelisk::{FunctionalClient, HandlerKit, InstanceInfo, PersistentLog, ServerlessStorage};
    use rand::distributions::Distribution;
    use std::collections::{BTreeMap, HashSet};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

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

    #[derive(Debug, Clone)]
    enum RequestPattern {
        Zipf(bool),
        Small(bool),
    }

    fn make_req(
        pattern: &RequestPattern,
        per_request_count: usize,
        is_sbtree: bool,
    ) -> (String, Vec<u8>) {
        let (keys, read_only) = match pattern {
            RequestPattern::Small(read_only) => {
                let keys: Vec<usize> = (START_KEY..(START_KEY + per_request_count)).collect();
                (keys, *read_only)
            }
            RequestPattern::Zipf(read_only) => {
                let mut rng = rand::thread_rng();
                let zipf = zipf::ZipfDistribution::new(NUM_KEYS, 0.5).unwrap();
                let keys = (0..per_request_count)
                    .map(|i| {
                        START_KEY + zipf.sample(&mut rng) - 1 // Starts from 1.
                    })
                    .collect();
                (keys, *read_only)
            }
        };
        let (rkeys, wkeys) = if read_only {
            (keys, vec![])
        } else {
            let mid = (per_request_count + 1) / 2;
            let rkeys = &keys[..mid];
            let wkeys = &keys[mid..];
            (rkeys.into(), wkeys.into())
        };
        let req = BenchOps {
            reads: rkeys,
            writes: wkeys,
            is_sbtree,
        };
        let meta = serde_json::to_string(&req).unwrap();
        (meta, vec![])
    }

    struct RequestSender {
        is_sbtree: bool,
        pattern: RequestPattern,
        curr_avg_latency: f64,
        desired_requests_per_second: f64,
        fc: Arc<FunctionalClient>,
    }

    impl RequestSender {
        // Next 5 seconds of requests
        async fn send_request_window(&mut self) -> Vec<(Duration, String)> {
            // Window duration.
            let window_duration = 5.0;
            let num_needed_threads =
                (self.desired_requests_per_second * self.curr_avg_latency).ceil();
            let total_num_requests = window_duration * self.desired_requests_per_second;
            let requests_per_thread = (total_num_requests / num_needed_threads).ceil();
            let actual_total_num_requests = requests_per_thread * num_needed_threads;
            let num_needed_threads = num_needed_threads as u64;
            println!("NT={num_needed_threads}; RPT={requests_per_thread};");
            let mut ts = Vec::new();
            let overall_start_time = Instant::now();
            for _ in 0..num_needed_threads {
                let requests_per_thread = requests_per_thread as u64;
                let fc = self.fc.clone();
                let is_sbtree = self.is_sbtree;
                let pattern = self.pattern.clone();
                let t = tokio::spawn(async move {
                    let start_time = std::time::Instant::now();
                    let mut responses = Vec::new();
                    let mut curr_idx = 0;
                    while curr_idx < requests_per_thread {
                        // Find number of calls to make and update curr idx.
                        let batch_size = 10;
                        let call_count = if requests_per_thread - curr_idx < batch_size {
                            requests_per_thread - curr_idx
                        } else {
                            batch_size
                        };
                        curr_idx += call_count;
                        // Now send requests.
                        let (meta, payload) = make_req(&pattern, call_count as usize, is_sbtree);
                        let resp = fc.invoke(&meta, &payload).await;
                        if resp.is_err() {
                            println!("Err: {resp:?}");
                            continue;
                        }
                        let (resp, _) = resp.unwrap();
                        let mut resp: Vec<(Duration, String)> =
                            serde_json::from_str(&resp).unwrap();
                        responses.append(&mut resp);
                    }
                    let end_time = std::time::Instant::now();
                    let duration = end_time.duration_since(start_time);

                    (duration, responses)
                });
                ts.push(t);
            }
            let mut sum_duration = Duration::from_millis(0);
            let mut all_responses = Vec::new();
            for t in ts {
                let (duration, mut responses) = t.await.unwrap();
                sum_duration = sum_duration.checked_add(duration).unwrap();
                all_responses.append(&mut responses);
            }
            let avg_duration = sum_duration.as_secs_f64() / (actual_total_num_requests);
            self.curr_avg_latency = 0.9 * self.curr_avg_latency + 0.1 * avg_duration;
            let mut avg_req_latency =
                all_responses.iter().fold(Duration::from_secs(0), |acc, x| {
                    acc.checked_add(x.0.clone()).unwrap()
                });
            if all_responses.len() > 0 {
                avg_req_latency = avg_req_latency.div_f64(all_responses.len() as f64);
            }
            println!(
                "AVG_LATENCY={avg_duration}; CURR_AVG_LATENCY={}; REQ_LATENCY={avg_req_latency:?}",
                self.curr_avg_latency
            );
            let overall_end_time = Instant::now();
            let overall_duration = overall_end_time.duration_since(overall_start_time);
            if overall_duration.as_secs_f64() < window_duration {
                let sleep_duration =
                    Duration::from_secs_f64(window_duration - overall_duration.as_secs_f64());
                println!("Window sleeping for: {:?}.", sleep_duration);
                tokio::time::sleep(sleep_duration).await;
            }
            all_responses
        }
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
            let (key, value) = make_key_val(key, KEY_SIZE_B, VAL_SIZE_B, false);
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
                    let (key, value) = make_key_val(key, KEY_SIZE_B, VAL_SIZE_B, false);
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

    async fn vaccuum_okeys(btree_actor: &BTreeActor) {
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
        for chunk in chunks {
            ts.push(tokio::spawn(async move {
                println!("Chunk: {chunk:?}");
                for (okey, _owner_id) in &chunk {
                    println!("Vaccuuming: {okey}");
                    let lo = LocalOwnership::new(okey).await;
                    lo.vacuum().await;
                }
            }));
        }
        for t in ts {
            t.await.unwrap();
        }
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
            gen_local_data(&btree_actor, 10).await;
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
        if step == "vaccuum" {
            let test_kit = make_test_actor_kit(false).await;
            let btree_actor = Arc::new(BTreeActor::new(test_kit).await);
            vaccuum_okeys(&btree_actor).await;
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
            let (key, _) = make_key_val(START_KEY + 37, KEY_SIZE_B, VAL_SIZE_B, false);
            let val = bc.get(&key).await.unwrap();
            println!("Val Len: {}", val.len());
            std::process::exit(0);
        }
        if step == "check" {
            // std::env::set_var("EXECUTION_MODE", "");
            let bc = Arc::new(BTreeClient::new().await);
            for key in [37, 7337, 2023, 666, 999999, 1000000] {
                let key = START_KEY + key;
                let (key, expected) = make_key_val(key, KEY_SIZE_B, VAL_SIZE_B, false);
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
                let (key, expected) = make_key_val(key, KEY_SIZE_B, VAL_SIZE_B, false);
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

    async fn run_bench(resquest_sender: &mut RequestSender, name: &str, test_duration: Duration) {
        let mut results: Vec<(u64, String, f64)> = Vec::new();
        let start_time = std::time::Instant::now();
        loop {
            // Pick an image at random.
            // TODO: Find a better way to select images.
            let curr_time = std::time::Instant::now();
            let since = curr_time.duration_since(start_time);
            if since > test_duration {
                break;
            }
            let since = since.as_millis() as u64;
            let resp = resquest_sender.send_request_window().await;
            for (duration, op_type) in resp {
                results.push((since, op_type, duration.as_secs_f64()));
            }
        }
        write_bench_output(results, &format!("{name}")).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn btree_bench() {
        let step = "load";
        init_large_bench_test(step).await;
        if step != "run" {
            return;
        }
        let fc = Arc::new(FunctionalClient::new("sbtree", "sbbench", None, Some(512)).await);
        let duration_mins = 1.0;
        let low_req_per_secs = 1.0;
        let medium_req_per_secs = 10.0;
        let high_req_per_secs = 100.0;
        let mut request_sender = RequestSender {
            is_sbtree: true,
            curr_avg_latency: 10.0,
            desired_requests_per_second: 0.0,
            pattern: RequestPattern::Zipf(true),
            fc: fc.clone(),
        };
        // Low
        request_sender.desired_requests_per_second = low_req_per_secs;
        run_bench(
            &mut request_sender,
            "pre_low",
            Duration::from_secs_f64(60.0 * duration_mins),
        )
        .await;
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

    pub async fn gen_dynamo_data() {
        let reset = true;
        let start_key = START_KEY;
        let num_keys = 10000; // NUM_KEYS;
        let shared_config = aws_config::load_from_env().await;
        let dynamo_client = aws_sdk_dynamodb::Client::new(&shared_config);
        if reset {
            let start_time = std::time::Instant::now();
            let mut i: usize = start_key;
            let batch_size = 25;
            let end_key = start_key + num_keys;
            while i < end_key {
                let batch_start = i;
                let batch_end = batch_start + batch_size;
                let mut request = dynamo_client.batch_write_item();
                let mut items = Vec::new();
                println!("Batch: ({batch_start}, {batch_end}).");
                if batch_start % 1000 == 0 {
                    let end_time = std::time::Instant::now();
                    let duration = end_time.duration_since(start_time);
                    println!("Duration so far: {duration:?}");
                }
                for key in batch_start..batch_end {
                    let (key, value) = make_key_val(key, KEY_SIZE_B, VAL_SIZE_B, false);
                    let value = String::from_utf8(value).unwrap();
                    let item = aws_sdk_dynamodb::types::WriteRequest::builder()
                        .put_request(
                            aws_sdk_dynamodb::types::PutRequest::builder()
                                .item("key", AttributeValue::S(key))
                                .item("value", AttributeValue::S(value))
                                .build(),
                        )
                        .build();
                    items.push(item);
                }
                request = request.request_items("dynamo_bench", items);
                loop {
                    let resp = request.clone().send().await;
                    if resp.is_err() {
                        println!("Dynamo Err: {resp:?}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    } else {
                        break;
                    }
                }
                i += batch_size;
            }
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Gen Duration: {duration:?}");
        }
        let (key, _) = make_key_val(start_key + 37, KEY_SIZE_B, VAL_SIZE_B, false);
        let resp = dynamo_client
            .get_item()
            .table_name("dynamo_bench")
            .consistent_read(true)
            .key("key", AttributeValue::S(key))
            .send()
            .await
            .unwrap();
        println!("Read Resp: {resp:?}");
    }


    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn dynamo_bench() {
        let step = "load";
        if step == "load" {
            gen_dynamo_data().await;
            return;
        }
    }
}
