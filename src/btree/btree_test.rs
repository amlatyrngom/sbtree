#[cfg(test)]
mod tests {
    use crate::btree::BTreeRespMeta;
    use obelisk::PersistentLog;
    use std::sync::Arc;

    use crate::BTreeActor;

    async fn reset_for_test() {
        std::env::set_var("EXECUTION_MODE", "local");
        let storage_dir = obelisk::common::shared_storage_prefix();
        if let Err(e) = std::fs::remove_dir_all(&storage_dir) {
            if e.kind() != std::io::ErrorKind::NotFound {
                panic!("Test directory could not be reset!");
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_test() {
        reset_for_test().await;
        run_simple_test().await;
    }

    async fn run_simple_test() {
        // Make btree actor.
        let name = "manager";
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
