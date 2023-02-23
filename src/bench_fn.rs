use crate::BTreeClient;
use aws_sdk_dynamodb::model::AttributeValue;
use obelisk::FunctionInstance;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// Benchmark Function.
/// TODO: Add a dynamodb option.
pub struct BenchFn {
    btree_client: Arc<BTreeClient>,
    dynamo_client: aws_sdk_dynamodb::Client,
}

/// A benchmark option.
#[derive(Serialize, Deserialize)]
pub enum BenchOp {
    Get {
        key: String,
        dynamo: bool,
    },
    Put {
        key: String,
        value: Vec<u8>,
        dynamo: bool,
    },
}

#[async_trait::async_trait]
impl FunctionInstance for BenchFn {
    async fn invoke(&self, arg: Value) -> Value {
        let ops: Vec<BenchOp> = serde_json::from_value(arg).unwrap();
        self.do_ops(ops).await
    }
}

impl BenchFn {
    /// Create.
    pub async fn new() -> Self {
        let shared_config = aws_config::load_from_env().await;
        let dynamo_client = aws_sdk_dynamodb::Client::new(&shared_config);
        BenchFn {
            btree_client: Arc::new(BTreeClient::new().await),
            dynamo_client,
        }
    }

    pub async fn do_ops(&self, ops: Vec<BenchOp>) -> Value {
        let mut num_gets = 0.0;
        let mut num_puts = 0.0;
        let mut get_duration = std::time::Duration::from_secs(0);
        let mut put_duration = std::time::Duration::from_secs(0);
        for op in ops {
            match op {
                BenchOp::Get { key, dynamo } => {
                    let start_time = std::time::Instant::now();
                    if dynamo {
                        let _resp = self
                            .dynamo_client
                            .get_item()
                            .table_name("sbtree_rescaling")
                            .consistent_read(true)
                            .key("entry_type", AttributeValue::S("test".into()))
                            .key("owner_id", AttributeValue::S(key))
                            .send()
                            .await
                            .unwrap();
                        println!("Resp: {_resp:?}");
                    } else {
                        let _resp = self.btree_client.get(&key).await;
                    }
                    let end_time = std::time::Instant::now();
                    let duration = end_time.duration_since(start_time);
                    get_duration += duration;
                    num_gets += 1.0;
                }
                BenchOp::Put { key, value, dynamo } => {
                    let start_time = std::time::Instant::now();
                    if dynamo {
                        let _resp = self
                            .dynamo_client
                            .put_item()
                            .table_name("sbtree_rescaling")
                            .item("entry_type", AttributeValue::S("test".into()))
                            .item("owner_id", AttributeValue::S(key))
                            .item(
                                "entry_data",
                                AttributeValue::S(String::from_utf8(value).unwrap()),
                            )
                            .send()
                            .await
                            .unwrap();
                    } else {
                        let _resp = self.btree_client.put(&key, value).await;
                    }
                    let end_time = std::time::Instant::now();
                    let duration = end_time.duration_since(start_time);
                    put_duration += duration;
                    num_puts += 1.0;
                }
            }
        }
        get_duration = if num_gets > 0.0 {
            get_duration.div_f64(num_gets)
        } else {
            get_duration
        };
        put_duration = if num_puts > 0.0 {
            put_duration.div_f64(num_puts)
        } else {
            put_duration
        };
        let resp = (get_duration, put_duration);
        serde_json::to_value(resp).unwrap()
    }
}
