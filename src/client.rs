use obelisk::FunctionalClient;

use crate::btree::{BTreeReqMeta, BTreeRespMeta};
use std::sync::{atomic, Arc};

/// SBTree Client.
/// It should be relatively sticky (e.g. do not create a new client for every new request).
#[derive(Clone)]
pub struct BTreeClient {
    pub fc: Arc<FunctionalClient>,
    // num_reqs: Arc<atomic::AtomicUsize>,
    // pub global_ownership: Arc<GlobalOwnership>,
}

impl BTreeClient {
    /// Create new client.
    pub async fn new() -> Self {
        let num_reqs = Arc::new(atomic::AtomicUsize::new(0));
        // let global_ownership = Arc::new(GlobalOwnership::new(None).await);
        let fc = FunctionalClient::new("sbtree", "sbactor", Some(0), Some(512)).await;
        fc.set_indirect_lambda_retry(false);
        BTreeClient {
            fc: Arc::new(fc),
            // num_reqs,
            // global_ownership,
        }
    }

    /// Send request to owner.
    async fn send_to_owner(
        &self,
        key: &str,
        req_meta: BTreeReqMeta,
        req_payload: Vec<u8>,
    ) -> (BTreeRespMeta, Vec<u8>) {
        let req_meta = serde_json::to_string(&req_meta).unwrap();
        let mut num_tries = 0;
        loop {
            // Send message until response.
            let (resp_meta, resp_payload) = loop {
                let resp = self.fc.invoke(&req_meta, &req_payload).await;
                if let Ok((resp_meta, resp_payload)) = resp {
                    let resp_meta: BTreeRespMeta = serde_json::from_str(&resp_meta).unwrap();
                    break (resp_meta, resp_payload);
                } else {
                    // Change later.
                    num_tries += 1;
                    if num_tries > 10 {
                        println!("Unable to send messaging. Exiting for test!");
                        std::process::exit(1);
                    }
                }
            };
            // If bad owner, retry without cache.
            match &resp_meta {
                BTreeRespMeta::InvariantIssue => {
                    panic!("BTree Invariant issues!");
                }
                _ => {
                    println!("Got response: {resp_meta:?}");
                    break (resp_meta, resp_payload);
                }
            }
        }
    }

    /// Delete all kv pairs.
    pub async fn cleanup(&self) {
        // Tell manager to stop accepting scale request.
        let manager_mc = self.fc.clone();
        let req_meta = BTreeReqMeta::Cleanup;
        let req_meta = serde_json::to_string(&req_meta).unwrap();
        println!("Initiating cleanup on manager!");
        let (resp_meta, _) = manager_mc.invoke(&req_meta, &[]).await.unwrap();
        let resp_meta: BTreeRespMeta = serde_json::from_str(&resp_meta).unwrap();
        match resp_meta {
            BTreeRespMeta::Ok => return,
            _ => {
                panic!("Should be ok");
            }
        }
    }

    /// Put key/value pair.
    pub async fn put(&self, key: &str, value: Vec<u8>) {
        let req_meta = BTreeReqMeta::Put { key: key.into() };
        let (resp_meta, _resp_payload) = self.send_to_owner(key, req_meta, value).await;
        match &resp_meta {
            BTreeRespMeta::Ok => {}
            _ => {
                panic!("Unknown response: {resp_meta:?}");
            }
        }
    }

    /// Get key.
    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let req_meta = BTreeReqMeta::Get { key: key.into() };
        let (resp_meta, resp_payload) = self.send_to_owner(key, req_meta, vec![]).await;
        match &resp_meta {
            BTreeRespMeta::Ok => Some(resp_payload),
            BTreeRespMeta::NotFound => None,
            _ => {
                println!("Unknown response: {resp_meta:?}");
                None
            }
        }
    }

    /// Remove key/value pair.
    pub async fn delete(&self, key: &str) {
        let req_meta = BTreeReqMeta::Delete { key: key.into() };
        let (resp_meta, _resp_payload) = self.send_to_owner(key, req_meta, vec![]).await;
        match &resp_meta {
            BTreeRespMeta::Ok => {}
            BTreeRespMeta::NotFound => {}
            _ => {
                panic!("Unknown response: {resp_meta:?}");
            }
        }
    }

    /// Load key value pairs.
    pub async fn load(&self, kvs: Vec<(String, Vec<u8>)>) {
        let mc = self.fc.clone();
        let payload = tokio::task::block_in_place(move || bincode::serialize(&kvs).unwrap());
        let req_meta = BTreeReqMeta::Load;
        let req_meta = serde_json::to_string(&req_meta).unwrap();
        for _ in 0..10 {
            let resp = mc.invoke(&req_meta, &payload).await;
            match resp {
                Err(e) => {
                    println!("Load. Could not send message: {e}!");
                }
                Ok(_) => return,
            }
        }
        std::process::exit(1);
    }

    pub async fn unload(&self, keys: Vec<String>) {
        let mc = self.fc.clone();
        let payload = tokio::task::block_in_place(move || bincode::serialize(&keys).unwrap());
        let req_meta = BTreeReqMeta::Unload;
        let req_meta = serde_json::to_string(&req_meta).unwrap();
        let req_meta = serde_json::to_string(&req_meta).unwrap();
        for _ in 0..10 {
            let resp = mc.invoke(&req_meta, &payload).await;
            match resp {
                Err(e) => {
                    println!("Load. Could not send message: {e}!");
                }
                Ok(_) => return,
            }
        }
        std::process::exit(1);
    }
}
