use crate::btree::RescalingOp;
use aws_sdk_dynamodb::model::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use obelisk::MessagingClient;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::{Arc, RwLock};

/// Manages global ownership by workers though dynamodb.
/// Global ownership keys are level-2 keys in the btree.
/// level-1 and level-0 keys are managed by the `LocalOwnership` object.
#[derive(Clone)]
pub struct GlobalOwnership {
    owner_id: Option<String>,
    dynamo_client: aws_sdk_dynamodb::Client,
    inner: Arc<RwLock<OwnershipInner>>,
}

/// Modifyable elements.
struct OwnershipInner {
    ownership_keys: BTreeMap<String, String>,
    clients: HashMap<String, Arc<MessagingClient>>,
    free_owners: BTreeSet<String>,
    next_worker_id: usize, // Allow allocation of next free worker.
    rescaling_op: Option<RescalingOp>,
}

impl GlobalOwnership {
    /// Create.
    pub async fn new(owner_id: Option<String>) -> Self {
        println!("Globalownership::new()");
        let shared_config = aws_config::load_from_env().await;
        let dynamo_client = aws_sdk_dynamodb::Client::new(&shared_config);
        let inner = Arc::new(RwLock::new(OwnershipInner {
            ownership_keys: BTreeMap::new(),
            clients: HashMap::new(),
            free_owners: BTreeSet::new(),
            next_worker_id: 0,
            rescaling_op: None,
        }));
        let ownership = GlobalOwnership {
            owner_id: owner_id.clone(),
            dynamo_client,
            inner,
        };
        if let Some(owner_id) = owner_id {
            if owner_id == "manager" {
                ownership.mark_owner_state("manager", false).await;
                ownership.recover_worker_states().await;
                ownership.recover_rescaling().await;
            }
        }
        ownership.refresh_ownership_keys().await;
        ownership
    }

    /// Get a free worker.
    /// When `wait_for_spin_up` is set, this call will wait for the actor
    /// to spin up from a lambda to a dedicated instance.
    pub async fn get_free_worker(&self, wait_for_spin_up: bool) -> String {
        let (free_owner, is_new) = {
            let mut inner = self.inner.write().unwrap();
            if inner.free_owners.is_empty() {
                let next_owner_id = format!("worker{}", inner.next_worker_id);
                inner.next_worker_id += 1;
                inner.free_owners.insert(next_owner_id.clone());
                (next_owner_id, true)
            } else {
                (inner.free_owners.first().cloned().unwrap(), false)
            }
        };
        if is_new {
            self.mark_owner_state(&free_owner, true).await;
        }
        // Cache and possibly spin up new owner.
        let mc = self.get_or_make_client(&free_owner).await;
        if wait_for_spin_up {
            mc.spin_up().await;
        }
        free_owner
    }

    /// Recover worker states.
    async fn recover_worker_states(&self) {
        loop {
            let resp = self
                .dynamo_client
                .execute_statement()
                .statement(
                    "SELECT owner_id, entry_data FROM sbtree_rescaling WHERE entry_type='freelist'",
                )
                .consistent_read(true)
                .send()
                .await;
            match resp {
                Err(x) => {
                    let x = format!("{x:?}");
                    if Self::is_normal_dynamo_error(&x) {
                        // Sleep to avoid high request rate on dynamo.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        panic!("Bad dynamo error {x}");
                    }
                    continue;
                }
                Ok(resp) => {
                    let items = match resp.items() {
                        None => return,
                        Some(items) => items,
                    };
                    let resp: HashMap<String, bool> = items
                        .iter()
                        .map(|item| {
                            // Read data.
                            let owner_id = item.get("owner_id").unwrap().as_s().cloned().unwrap();
                            let is_free = item.get("entry_data").unwrap().as_s().cloned().unwrap();
                            let is_free: bool = is_free.parse().unwrap();
                            (owner_id, is_free)
                        })
                        .collect();
                    let mut inner = self.inner.write().unwrap();
                    inner.next_worker_id = resp.len();
                    for (worker_id, is_free) in resp {
                        if is_free {
                            inner.free_owners.insert(worker_id.clone());
                        }
                        if obelisk::common::has_external_access() {
                            let this = self.clone();
                            tokio::spawn(async move {
                                let _mc = this.get_or_make_client(&worker_id).await;
                            });
                        }
                    }
                    return;
                }
            }
        }
    }

    /// Marks the state of this owner.
    async fn mark_owner_state(&self, owner_id: &str, free: bool) {
        // Manager should never be marked as free.
        assert!(!(owner_id == "manager" && free));
        loop {
            let resp = self
                .dynamo_client
                .put_item()
                .table_name("sbtree_rescaling")
                .item("entry_type", AttributeValue::S("freelist".into()))
                .item("owner_id", AttributeValue::S(owner_id.into()))
                .item("entry_data", AttributeValue::S(free.to_string()))
                .send()
                .await;
            match resp {
                Err(x) => {
                    let x = format!("{x:?}");
                    if Self::is_normal_dynamo_error(&x) {
                        // Sleep to avoid high request rate on dynamo.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        panic!("Bad dynamo error {x}");
                    }
                    continue;
                }
                Ok(_resp) => break,
            }
        }
        let mut inner = self.inner.write().unwrap();
        if free {
            inner.free_owners.insert(owner_id.into());
        } else {
            inner.free_owners.remove(owner_id);
        }
    }

    /// Prevent regular rescaling.
    pub async fn prevent_regular_rescaling(&self) {
        loop {
            let resp = self
                .dynamo_client
                .put_item()
                .table_name("sbtree_rescaling")
                .item("entry_type", AttributeValue::S("cleanup".into()))
                .item("owner_id", AttributeValue::S("manager".into()))
                .item("entry_data", AttributeValue::S(true.to_string()))
                .send()
                .await;
            match resp {
                Err(x) => {
                    println!("{x:?}");
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Ok(_resp) => break,
            }
        }
    }

    /// Allow regular scaling.
    pub async fn allow_regular_rescaling(&self) {
        loop {
            let resp = self
                .dynamo_client
                .delete_item()
                .table_name("sbtree_rescaling")
                .key("entry_type", AttributeValue::S("cleanup".into()))
                .key("owner_id", AttributeValue::S("manager".into()))
                .send()
                .await;
            match resp {
                Err(x) => {
                    println!("{x:?}");
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Ok(_resp) => break,
            }
        }
    }

    /// Allow regular scaling.
    pub async fn can_do_regular_rescaling(&self) -> bool {
        loop {
            let resp = self
                .dynamo_client
                .get_item()
                .table_name("sbtree_rescaling")
                .key("entry_type", AttributeValue::S("cleanup".into()))
                .key("owner_id", AttributeValue::S("manager".into()))
                .send()
                .await;
            match resp {
                Err(x) => {
                    println!("{x:?}");
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Ok(resp) => {
                    return resp.item().is_none();
                }
            }
        }
    }

    /// Start rescaling.
    /// Only one rescaling can happen at a time.
    pub async fn start_rescaling(&self, rescaling_op: RescalingOp, recovering: bool) -> bool {
        if recovering {
            loop {
                let rescaling_op = serde_json::to_string(&rescaling_op).unwrap();
                let resp = self
                    .dynamo_client
                    .put_item()
                    .table_name("sbtree_rescaling")
                    .item("entry_type", AttributeValue::S("rescaling".into()))
                    .item("owner_id", AttributeValue::S("manager".into()))
                    .item("entry_data", AttributeValue::S(rescaling_op.clone()))
                    .condition_expression("attribute_not_exists(#entry_type)") // Allow one rescaling at a time.
                    .expression_attribute_names("#entry_type", "entry_type")
                    .send()
                    .await;
                match resp {
                    Err(x) => {
                        let x = format!("{x:?}");
                        if x.contains("ConditionalCheckFailedException") {
                            return false;
                        }
                        continue;
                    }
                    Ok(_resp) => break,
                }
            }
        }

        {
            let mut inner = self.inner.write().unwrap();
            inner.rescaling_op = Some(rescaling_op.clone());
        }
        match rescaling_op {
            RescalingOp::ScaleIn { from, to, uid: _ } => {
                // Mark `from` as free, and `to` as busy.
                self.mark_owner_state(&from, true).await;
                self.mark_owner_state(&to, false).await;
            }
            RescalingOp::ScaleOut { from, to, uid: _ } => {
                // Mark `from` and `to` as busy.
                self.mark_owner_state(&from, false).await;
                self.mark_owner_state(&to, false).await;
            }
        }
        true
    }

    /// Get the ongoing rescaling operation.
    pub fn get_ongoing_rescaling(&self) -> Option<RescalingOp> {
        let inner = self.inner.read().unwrap();
        inner.rescaling_op.clone()
    }

    /// Recover ongoing rescaling.
    async fn recover_rescaling(&self) {
        let rescaling_op: Option<RescalingOp> = loop {
            let resp = self
                .dynamo_client
                .get_item()
                .table_name("sbtree_rescaling")
                .key("entry_type", AttributeValue::S("rescaling".into()))
                .key("owner_id", AttributeValue::S("manager".into()))
                .send()
                .await;
            match resp {
                Err(x) => {
                    let x = format!("{x:?}");
                    if Self::is_normal_dynamo_error(&x) {
                        // Sleep to avoid high request rate on dynamo.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        panic!("Bad dynamo error {x}");
                    }
                    continue;
                }
                Ok(resp) => match resp.item() {
                    None => break None,
                    Some(item) => {
                        let rescaling_op = item.get("entry_data").unwrap().as_s().unwrap();
                        break Some(serde_json::from_str(rescaling_op).unwrap());
                    }
                },
            }
        };
        if let Some(rescaling_op) = &rescaling_op {
            self.start_rescaling(rescaling_op.clone(), true).await;
        }
    }

    /// Finish rescaling.
    pub async fn finish_rescaling(&self) {
        loop {
            let resp = self
                .dynamo_client
                .delete_item()
                .table_name("sbtree_rescaling")
                .key("entry_type", AttributeValue::S("rescaling".into()))
                .key("owner_id", AttributeValue::S("manager".into()))
                .send()
                .await;
            match resp {
                Err(x) => {
                    let x = format!("{x:?}");
                    if Self::is_normal_dynamo_error(&x) {
                        // Sleep to avoid high request rate on dynamo.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        panic!("Bad dynamo error {x}");
                    }
                    continue;
                }
                Ok(_resp) => break,
            }
        }
        {
            let mut inner = self.inner.write().unwrap();
            inner.rescaling_op = None;
        }
    }

    /// Update load.
    pub async fn update_load(&self, load: String) {
        // First multiply by a 1000 to prevent floating point issues.
        let owner_id = self.owner_id.clone().unwrap();
        loop {
            let resp = self
                .dynamo_client
                .put_item()
                .table_name("sbtree_rescaling")
                .item("entry_type", AttributeValue::S("load".into()))
                .item("owner_id", AttributeValue::S(owner_id.clone()))
                .item("entry_data", AttributeValue::S(load.clone()))
                .send()
                .await;
            match resp {
                Err(x) => {
                    let x = format!("{x:?}");
                    if Self::is_normal_dynamo_error(&x) {
                        // Sleep to avoid high request rate on dynamo.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        panic!("Bad dynamo error {x}");
                    }
                    continue;
                }
                Ok(_resp) => return,
            }
        }
    }

    /// Remove load.
    pub async fn remove_load(&self) {
        let owner_id = self.owner_id.clone().unwrap();
        println!("Deleting load: {owner_id}");
        loop {
            let resp = self
                .dynamo_client
                .delete_item()
                .table_name("sbtree_rescaling")
                .key("entry_type", AttributeValue::S("load".into()))
                .key("owner_id", AttributeValue::S(owner_id.clone()))
                .send()
                .await;
            println!("Delete load resp: {resp:?}");
            match resp {
                Err(x) => {
                    let x = format!("{x:?}");
                    if Self::is_normal_dynamo_error(&x) {
                        // Sleep to avoid high request rate on dynamo.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        panic!("Bad dynamo error {x}");
                    }
                    continue;
                }
                Ok(_resp) => return,
            }
        }
    }

    /// Read loads from persistent store.
    pub async fn read_loads(&self) -> HashMap<String, (f64, f64)> {
        loop {
            let resp = self
                .dynamo_client
                .execute_statement()
                .statement(
                    "SELECT owner_id, entry_data FROM sbtree_rescaling WHERE entry_type='load'",
                )
                .consistent_read(true)
                .send()
                .await;
            match resp {
                Err(x) => {
                    let x = format!("{x:?}");
                    if Self::is_normal_dynamo_error(&x) {
                        // Sleep to avoid high request rate on dynamo.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        panic!("Bad dynamo error {x}");
                    }
                    continue;
                }
                Ok(resp) => {
                    let items = match resp.items() {
                        None => break HashMap::new(),
                        Some(items) => items,
                    };
                    let resp: HashMap<String, (f64, f64)> = items
                        .iter()
                        .map(|item| {
                            // Read data.
                            let owner_id = item.get("owner_id").unwrap().as_s().cloned().unwrap();
                            let load = item.get("entry_data").unwrap().as_s().cloned().unwrap();
                            // Parse and divide by 1000 to recover original value.
                            let load: (f64, f64) = serde_json::from_str(&load).unwrap();
                            (owner_id, load)
                        })
                        .collect();
                    break resp;
                }
            }
        }
    }

    /// Perform ownership transfer.
    /// Should only be called by the `from` owner.
    pub async fn perform_ownership_transfer(&self, to: &str, ownership_keys: Vec<String>) {
        let from = self.owner_id.clone().unwrap();
        for ownership_key in ownership_keys {
            self.add_ownership_key(to, &ownership_key).await;
            self.remove_ownership_key(&from, &ownership_key, false)
                .await;
        }
    }

    /// Add an ownership key for this owner.
    pub async fn add_ownership_key(&self, owner_id: &str, ownership_key: &str) {
        loop {
            let resp = self
                .dynamo_client
                .put_item()
                .table_name("sbtree_ownership")
                .item("owner_id", AttributeValue::S(owner_id.into()))
                .item("ownership_key", AttributeValue::S(ownership_key.into()))
                .send()
                .await;
            match resp {
                Err(x) => {
                    let x = format!("{x:?}");
                    if Self::is_normal_dynamo_error(&x) {
                        // Sleep to avoid high request rate on dynamo.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        panic!("Bad dynamo error {x}");
                    }
                    continue;
                }
                Ok(_resp) => break,
            }
        }
        let mut inner = self.inner.write().unwrap();
        inner
            .ownership_keys
            .insert(ownership_key.into(), owner_id.into());
    }

    /// Remove an ownership key.
    pub async fn remove_ownership_key(&self, owner_id: &str, ownership_key: &str, delete: bool) {
        loop {
            let resp = self
                .dynamo_client
                .delete_item()
                .table_name("sbtree_ownership")
                .key("owner_id", AttributeValue::S(owner_id.into()))
                .key("ownership_key", AttributeValue::S(ownership_key.into()))
                .send()
                .await;
            match resp {
                Err(x) => {
                    let x = format!("{x:?}");
                    if Self::is_normal_dynamo_error(&x) {
                        // Sleep to avoid high request rate on dynamo.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        panic!("Bad dynamo error {x}");
                    }
                    continue;
                }
                Ok(_resp) => break,
            }
        }

        if delete {
            let mut inner = self.inner.write().unwrap();
            inner.ownership_keys.remove(ownership_key);
        }
    }

    /// Fetch the keys owned by this owner.
    pub async fn fetch_ownership_keys(&self) -> Vec<String> {
        let owner_id = self.owner_id.clone().unwrap();
        self.refresh_ownership_keys().await;
        let ownership = self.inner.read().unwrap();
        ownership
            .ownership_keys
            .iter()
            .filter(|(_okey, oid)| **oid == owner_id)
            .map(|(okey, _)| okey.clone())
            .collect()
    }

    /// Returns the owner of a key.
    pub async fn find_owner(&self, key: &str, use_cache: bool) -> Option<(String, String)> {
        if !use_cache {
            self.refresh_ownership_keys().await;
        }
        let inner = self.inner.read().unwrap();
        let key = format!("/okey/{key}");
        let owner_id = inner
            .ownership_keys
            .range((Bound::Unbounded, Bound::Included(key)))
            .last()
            .map(|(k, v)| (k.clone(), v.clone()));
        if owner_id.is_some() {
            owner_id
        } else {
            // This should probably never happen since the default ownership key is the smallest possible key (empty key).
            // But I am too lazy to put an unwrap() there.
            inner
                .ownership_keys
                .first_key_value()
                .map(|(k, v)| (k.clone(), v.clone()))
        }
    }

    /// Refresh ownership keys.
    pub async fn refresh_ownership_keys(&self) {
        loop {
            // Read from dynamodb.
            let resp = self
                .dynamo_client
                .execute_statement()
                .statement("SELECT owner_id, ownership_key FROM sbtree_ownership")
                .consistent_read(true)
                .send()
                .await;
            match resp {
                Err(x) => {
                    let x = format!("{x:?}");
                    if Self::is_normal_dynamo_error(&x) {
                        // Sleep to avoid high request rate on dynamo.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        panic!("Bad dynamo error {x}");
                    }
                    println!("{x:?}");
                    continue;
                }
                Ok(resp) => {
                    let resp: Vec<(String, String)> = resp
                        .items()
                        .unwrap()
                        .iter()
                        .map(|item| {
                            let owner = item.get("owner_id").unwrap().as_s().cloned().unwrap();
                            let ownership_key =
                                item.get("ownership_key").unwrap().as_s().cloned().unwrap();
                            (owner, ownership_key)
                        })
                        .collect();
                    let mut ownership_keys = BTreeMap::new();
                    for (owner_id, ownership_key) in resp {
                        ownership_keys.insert(ownership_key, owner_id);
                    }
                    let mut inner = self.inner.write().unwrap();
                    inner.ownership_keys = ownership_keys;
                    return;
                }
            }
        }
    }

    /// Return all ownership keys.
    pub async fn all_ownership_keys(&self) -> BTreeMap<String, String> {
        let inner = self.inner.write().unwrap();
        inner.ownership_keys.clone()
    }

    /// Get or make messaging client.
    pub async fn get_or_make_client(&self, client_id: &str) -> Arc<MessagingClient> {
        // Try getting already built client.
        // If no client was built, make new client.
        let owner = {
            let inner = self.inner.read().unwrap();
            inner.clients.get(client_id).cloned()
        };
        let owner = match owner {
            Some(owner) => owner,
            None => {
                let mc = Arc::new(MessagingClient::new("sbtree", client_id).await);
                let mut inner = self.inner.write().unwrap();
                inner.clients.insert(client_id.into(), mc.clone());
                mc
            }
        };
        owner
    }

    async fn make_table_if_not_exist(table_name: &str, pkey: &str, skey: &str) {
        let shared_config = aws_config::load_from_env().await;
        let dynamo_client = aws_sdk_dynamodb::Client::new(&shared_config);
        // key-value table.
        let resp = dynamo_client
            .create_table()
            .table_name(table_name)
            .billing_mode(BillingMode::PayPerRequest)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(pkey)
                    .key_type(KeyType::Hash)
                    .build(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(skey)
                    .key_type(KeyType::Range)
                    .build(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(pkey)
                    .attribute_type(ScalarAttributeType::S)
                    .build(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(skey)
                    .attribute_type(ScalarAttributeType::S)
                    .build(),
            )
            .send()
            .await;
        if resp.is_err() {
            let err = format!("{resp:?}");
            if !err.contains("ResourceInUseException") {
                eprintln!("Dynamodb request error: {resp:?}");
                std::process::exit(1);
            }
        } else {
            println!("Creating {table_name} table...");
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    }

    pub fn is_normal_dynamo_error(x: &str) -> bool {
        x.contains("InternalServerError")
            || x.contains(" ConditionalCheckFailedException ")
            || x.contains("ResourceNotFoundException")
            || x.contains("TransactionConflictException")
            || x.contains("ConditionalCheckFailedException")
    }

    pub async fn make_tables_if_not_exist() {
        let create1 =
            Self::make_table_if_not_exist("sbtree_ownership", "owner_id", "ownership_key");
        let create2 = Self::make_table_if_not_exist("sbtree_rescaling", "entry_type", "owner_id");
        tokio::join!(create1, create2);
    }
}
