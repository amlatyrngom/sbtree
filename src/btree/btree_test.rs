#[cfg(test)]
mod tests {
    use obelisk::PersistentLog;
    use std::sync::Arc;

    use crate::BTreeActor;

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_test() {
        run_simple_test().await;
    }

    async fn run_simple_test() {
        let name = "manager";
        println!("Making log");
        let plog = PersistentLog::new("sbtree", name).await;
        println!("Making actor");
        let btree_actor = BTreeActor::new(name, Arc::new(plog)).await;
        let resp = btree_actor.get("").await;
        println!("Resp: {resp:?}");
    }
}
