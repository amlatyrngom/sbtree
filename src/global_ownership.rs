// use crate::btree::RescalingOp;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
// use obelisk::MessagingClient;
use std::collections::BTreeMap;
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
}

impl GlobalOwnership {
    /// Create.
    pub async fn new(owner_id: Option<String>) -> Self {
        println!("Globalownership::new()");
        let shared_config = aws_config::load_from_env().await;
        let dynamo_client = aws_sdk_dynamodb::Client::new(&shared_config);
        let inner = Arc::new(RwLock::new(OwnershipInner {
            ownership_keys: BTreeMap::new(),
        }));
        let ownership = GlobalOwnership {
            owner_id: owner_id.clone(),
            dynamo_client,
            inner,
        };
        ownership.refresh_ownership_keys().await;
        ownership
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
                    println!("Rescaling: {x:?}");
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
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
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

    /// Return all ownership keys.
    pub async fn all_ownership_keys(&self) -> BTreeMap<String, String> {
        let inner = self.inner.write().unwrap();
        inner.ownership_keys.clone()
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
