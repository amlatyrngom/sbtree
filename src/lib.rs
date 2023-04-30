mod bench_fn;
mod btree;
mod client;
mod global_ownership;
// pub mod sqlite;

pub use bench_fn::BenchFn;
pub use btree::BTreeActor;
pub use client::BTreeClient;

pub async fn make_deployment() {
    let op1 = global_ownership::GlobalOwnership::make_tables_if_not_exist();
    let op2 = obelisk::MessagingClient::new("sbtree", "manager");
    let op3 = obelisk::MessagingClient::new("sbtree", "worker1");
    tokio::join!(op1, op2, op3);
}

pub async fn make_test_deployment() {
    let op1 = global_ownership::GlobalOwnership::make_tables_if_not_exist();
    op1.await;
}
