use crate::btree::{BTreeReqMeta, BTreeRespMeta};
use crate::global_ownership::GlobalOwnership;
use std::collections::HashMap;
use std::sync::{atomic, Arc};

/// SBTree Client.
/// It should be relatively sticky (e.g. do not create a new client for every new request).
#[derive(Clone)]
pub struct BTreeClient {
    num_reqs: Arc<atomic::AtomicUsize>,
    global_ownership: Arc<GlobalOwnership>,
}

impl BTreeClient {
    /// Create new client.
    pub async fn new() -> Self {
        let num_reqs = Arc::new(atomic::AtomicUsize::new(0));
        let global_ownership = Arc::new(GlobalOwnership::new(None).await);
        BTreeClient {
            num_reqs,
            global_ownership,
        }
    }

    /// Send request to owner.
    async fn send_to_owner(
        &self,
        key: &str,
        req_meta: BTreeReqMeta,
        req_payload: Vec<u8>,
    ) -> (BTreeRespMeta, Vec<u8>) {
        let mut try_cache = true;
        let req_meta = serde_json::to_string(&req_meta).unwrap();
        let curr_num_reqs = self.num_reqs.fetch_add(1, atomic::Ordering::AcqRel);
        // Send a manage request every 10 requests.
        if curr_num_reqs % 10 == 0 {
            // Reset to 1 to prevent perpetual reset to 0 (because 0%10 == 0).
            self.num_reqs.store(1, atomic::Ordering::Release);
            let global_ownership = self.global_ownership.clone();
            tokio::spawn(async move {
                let mc = global_ownership.get_or_make_client("manager").await;
                let req_meta = BTreeReqMeta::Manage;
                let req_meta = serde_json::to_string(&req_meta).unwrap();
                loop {
                    let resp = mc.send_message(&req_meta, &[]).await;
                    if resp.is_some() {
                        break;
                    }
                }
            });
        }
        loop {
            // Get current owner.
            let (_ownership_key, owner_id) =
                match self.global_ownership.find_owner(key, try_cache).await {
                    Some(x) => x,
                    None => {
                        if !try_cache {
                            panic!("There should always be an owner.");
                        }
                        try_cache = false;
                        continue;
                    }
                };
            let owner = self.global_ownership.get_or_make_client(&owner_id).await;
            // Send message until response.
            let (resp_meta, resp_payload) = loop {
                let resp = owner.send_message(&req_meta, &req_payload).await;
                if let Some((resp_meta, resp_payload)) = resp {
                    let resp_meta: BTreeRespMeta = serde_json::from_str(&resp_meta).unwrap();
                    break (resp_meta, resp_payload);
                }
            };
            // If bad owner, retry without cache.
            match &resp_meta {
                BTreeRespMeta::BadOwner => {
                    try_cache = false;
                    continue;
                }
                BTreeRespMeta::InvariantIssue => {
                    panic!("BTree Invariant issues!");
                }
                _ => break (resp_meta, resp_payload),
            }
        }
    }

    /// Delete all kv pairs.
    pub async fn cleanup(&self) {
        // Tell manager to stop accepting scale request.
        let manager_mc = self.global_ownership.get_or_make_client("manager").await;
        let req_meta = BTreeReqMeta::Cleanup {
            worker: Some("manager".into()),
        };
        let req_meta = serde_json::to_string(&req_meta).unwrap();
        println!("Initiating cleanup on manager!");
        let (resp_meta, _) = manager_mc.send_message(&req_meta, &[]).await.unwrap();
        let resp_meta: BTreeRespMeta = serde_json::from_str(&resp_meta).unwrap();
        match resp_meta {
            BTreeRespMeta::Ok => {}
            _ => {
                panic!("Should be ok");
            }
        }
        println!("Done initiating cleanup!");
        // Scale in all non-manager workers.
        println!("Scaling in for cleanup!");
        let actors: Vec<String> = self
            .global_ownership
            .read_loads()
            .await
            .keys()
            .cloned()
            .collect();
        for owner_id in actors {
            if owner_id == "manager" {
                continue;
            }
            println!("Scaling in {owner_id:?}");
            manager_mc.spin_up().await;
            let req_meta = BTreeReqMeta::Cleanup {
                worker: Some(owner_id),
            };
            let req_meta = serde_json::to_string(&req_meta).unwrap();
            loop {
                let (resp_meta, _) = manager_mc.send_message(&req_meta, &[]).await.unwrap();
                let resp_meta: BTreeRespMeta = serde_json::from_str(&resp_meta).unwrap();
                match resp_meta {
                    BTreeRespMeta::Ok => break,
                    _ => {
                        manager_mc.spin_up().await;
                        continue;
                    }
                }
            }
        }
        println!("Done scaling in for cleanup!");
        // Delete all in manager.
        let req_meta = BTreeReqMeta::Cleanup { worker: None };
        let req_meta = serde_json::to_string(&req_meta).unwrap();
        println!("Completing cleanup on manager!");
        loop {
            let resp = manager_mc.send_message(&req_meta, &[]).await;
            match resp {
                None => continue,
                Some((resp_meta, _)) => {
                    let resp_meta: BTreeRespMeta = serde_json::from_str(&resp_meta).unwrap();
                    match resp_meta {
                        BTreeRespMeta::Ok => break,
                        _ => {
                            panic!("Should be ok");
                        }
                    }
                }
            }
        }
        println!("Done completing cleaning up");
    }

    /// Put key/value pair.
    pub async fn put(&self, key: &str, value: Vec<u8>) {
        let req_meta = BTreeReqMeta::Put { key: key.into() };
        let (resp_meta, _resp_payload) = self.send_to_owner(key, req_meta, value).await;
        match &resp_meta {
            BTreeRespMeta::Ok => {},
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
            BTreeRespMeta::Ok => {},
            BTreeRespMeta::NotFound => {},
            _ => {
                panic!("Unknown response: {resp_meta:?}");
            }
        }
    }

    /// Load key value pairs.
    pub async fn load(&self, mut kvs: Vec<(String, Vec<u8>)>) {
        while !kvs.is_empty() {
            println!("Client loading: {}", kvs.len());
            // Find owner of each kv pair.
            let mut msgs: HashMap<String, Vec<(String, Vec<u8>)>> = HashMap::new();
            for (key, value) in kvs.drain(..) {
                let (_ownership_key, owner_id) =
                    match self.global_ownership.find_owner(&key, true).await {
                        Some(x) => x,
                        None => {
                            self.global_ownership.find_owner(&key, false).await.unwrap()
                        }
                    };
                if !msgs.contains_key(&owner_id) {
                    msgs.insert(owner_id.clone(), Vec::new());
                }
                msgs.get_mut(&owner_id).unwrap().push((key, value));
            }
            // Send messages to each owner in different threads.
            let mut ts = Vec::new();
            for (owner_id, msgs) in msgs {
                let global_ownership = self.global_ownership.clone();
                ts.push(tokio::spawn(async move {
                    let mc = global_ownership.get_or_make_client(&owner_id).await;
                    let payload = tokio::task::block_in_place(move || {
                        bincode::serialize(&msgs).unwrap()
                    });
                    let req_meta = BTreeReqMeta::Load;
                    let req_meta = serde_json::to_string(&req_meta).unwrap();
                    let not_owned = loop {
                        let resp = mc.send_message(&req_meta, &payload).await;
                        match resp {
                            None => {
                                println!("Load. Could not send message!");
                                std::process::exit(1);
                            },
                            Some((_, payload)) => {
                                let not_owned: Vec<(String, Vec<u8>)> = tokio::task::block_in_place(move || {
                                    bincode::deserialize(&payload).unwrap()
                                });
                                break not_owned;
                            },
                            
                        }
                    };
                    not_owned
                }));
            }
            // Wait for threads.
            // Reload remaining messages for next iteration.
            for t in ts {
                let not_owned = t.await.unwrap();
                for (k, v) in not_owned {
                    kvs.push((k, v));
                }
            }
            println!("Client loading. Num remaining keys: {}", kvs.len());
            // Just for testing.
            if !kvs.is_empty() {
                println!("Exiting for test. Remove this block of code later!!!");
                std::process::exit(1);
            }
        } 
    }

    pub async fn unload(&self, mut keys: Vec<String>) {
        while !keys.is_empty() {
            println!("Client unloading: {}", keys.len());
            // Find owner of each kv pair.
            let mut msgs: HashMap<String, Vec<String>> = HashMap::new();
            for key in keys.drain(..) {
                let (_ownership_key, owner_id) =
                    match self.global_ownership.find_owner(&key, true).await {
                        Some(x) => x,
                        None => {
                            self.global_ownership.find_owner(&key, false).await.unwrap()
                        }
                    };
                if !msgs.contains_key(&owner_id) {
                    msgs.insert(owner_id.clone(), Vec::new());
                }
                msgs.get_mut(&owner_id).unwrap().push(key);
            }
            // Send messages to each owner in different threads.
            let mut ts = Vec::new();
            for (owner_id, msgs) in msgs {
                let global_ownership = self.global_ownership.clone();
                ts.push(tokio::spawn(async move {
                    let mc = global_ownership.get_or_make_client(&owner_id).await;
                    let payload = tokio::task::block_in_place(move || {
                        bincode::serialize(&msgs).unwrap()
                    });
                    let req_meta = BTreeReqMeta::Load;
                    let req_meta = serde_json::to_string(&req_meta).unwrap();
                    let not_owned = loop {
                        let resp = mc.send_message(&req_meta, &payload).await;
                        match resp {
                            None => {
                                println!("Load. Could not send message!");
                                std::process::exit(1);
                            },
                            Some((_, payload)) => {
                                let not_owned: Vec<String> = tokio::task::block_in_place(move || {
                                    bincode::deserialize(&payload).unwrap()
                                });
                                break not_owned;
                            },
                            
                        }
                    };
                    not_owned
                }));
            }
            // Wait for threads.
            // Reload remaining messages for next iteration.
            for t in ts {
                let not_owned = t.await.unwrap();
                for k in not_owned {
                    keys.push(k);
                }
            }
            println!("Client unloading. Num remaining keys: {}", keys.len());
            // Just for testing.
            if !keys.is_empty() {
                println!("Exiting for test. Remove this block of code later!!!");
                std::process::exit(1);
            }
        }
    }
}
