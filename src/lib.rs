mod btree;
mod client;
mod global_ownership;

pub use btree::BTreeActor;
pub use client::BTreeClient;
use obelisk::{ActorInstance, FunctionInstance, PersistentLog};
use serde_json::Value;
use std::sync::Arc;

pub struct EssaiFn {}

impl EssaiFn {
    pub async fn new() -> Self {
        EssaiFn {}
    }
}

#[async_trait::async_trait]
impl FunctionInstance for EssaiFn {
    async fn invoke(&self, arg: Value) -> Value {
        println!("Essai called: {arg}");
        if let Some(duration_ms) = arg.as_u64() {
            let duration = std::time::Duration::from_millis(duration_ms);
            tokio::time::sleep(duration).await;
        }
        arg
    }
}

pub struct EssaiActor {}

impl EssaiActor {
    pub async fn new(name: &str, _plog: Arc<PersistentLog>) -> Self {
        println!("Creating actor with name: {name}");
        EssaiActor {}
    }
}

#[async_trait::async_trait]
impl ActorInstance for EssaiActor {
    async fn message(&self, msg: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        println!("Essai. msg={msg}; payload={payload:?}");
        (msg, payload)
    }

    async fn checkpoint(&self, terminating: bool) {
        println!("Checkpointing: termination={terminating}")
    }
}

#[cfg(test)]
mod tests {
    use obelisk::FunctionalClient;
    use obelisk::MessagingClient;

    async fn run_basic_test() {
        let fc = FunctionalClient::new("essai").await;
        let arg = String::from("Amadou Ngom");
        let arg = serde_json::to_vec(&arg).unwrap();
        let resp = fc.invoke(&arg).await;
        println!("Resp: {resp:?}");
        let mc = MessagingClient::new("essai", "essai1").await;
        let resp = mc.send_message("Amadou Ngom", &vec![1, 2, 3]).await;
        println!("Resp: {resp:?}");
    }

    #[tokio::test]
    pub async fn basic_test() {
        run_basic_test().await;
    }
}
