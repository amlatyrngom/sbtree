#[cfg(test)]
mod tests {
    use crate::btree::BTreeRespMeta;
    use obelisk::PersistentLog;
    use std::sync::Arc;

    use crate::BTreeActor;

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_test() {
        run_simple_test().await;
    }

    async fn run_simple_test() {
        // TODO: Delete data directory to reset test.
        let name = "manager";
        std::env::set_var("EXECUTION_MODE", "local");
        let plog = PersistentLog::new("sbtree", name).await;
        let btree_actor = BTreeActor::new(name, Arc::new(plog)).await;
        // The empty key always maps to the empty value.
        let (resp_meta, value) = btree_actor.get("").await;
        assert!(matches!(resp_meta, BTreeRespMeta::Ok));
        assert!(value.is_empty());
        // Try reading a non-existent key.
        let (resp_meta, _) = btree_actor.get("NotExists").await;
        println!("Resp Meta: {resp_meta:?}");
        assert!(matches!(resp_meta, BTreeRespMeta::NotFound));
        // Check that everything is unpinned.
        btree_actor
            .tree_structure
            .block_cache
            .check_all_unpinned(Some(2))
            .await;
    }
}
