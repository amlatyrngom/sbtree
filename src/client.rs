use crate::btree::{BTreeReqMeta, BTreeRespMeta};
use crate::global_ownership::GlobalOwnership;
use std::sync::{atomic, Arc};

/// SBTree Client.
/// It should be relatively sticky (e.g. do not create a new client for every new request).
pub struct BTreeClient {
    num_reqs: atomic::AtomicUsize,
    global_ownership: Arc<GlobalOwnership>,
}

impl BTreeClient {
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

    /// Put key/value pair.
    pub async fn put(&self, key: &str, value: Vec<u8>) -> bool {
        let req_meta = BTreeReqMeta::Put { key: key.into() };
        let (resp_meta, _resp_payload) = self.send_to_owner(key, req_meta, value).await;
        match &resp_meta {
            BTreeRespMeta::Ok => true,
            _ => {
                println!("Unknown response: {resp_meta:?}");
                false
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
    pub async fn delete(&self, key: &str) -> bool {
        let req_meta = BTreeReqMeta::Delete { key: key.into() };
        let (resp_meta, _resp_payload) = self.send_to_owner(key, req_meta, vec![]).await;
        match &resp_meta {
            BTreeRespMeta::Ok => true,
            _ => {
                println!("Unknown response: {resp_meta:?}");
                false
            }
        }
    }
}
