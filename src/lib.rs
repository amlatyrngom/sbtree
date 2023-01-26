mod btree;
mod client;
mod global_ownership;

pub use btree::BTreeActor;
pub use client::BTreeClient;

pub async fn make_deployment() {
    let op1 = global_ownership::GlobalOwnership::make_tables_if_not_exist();
    let op2 = obelisk::MessagingClient::new("sbtree", "manager");
    tokio::join!(op1, op2);
}

pub async fn make_test_deployment() {
    let op1 = global_ownership::GlobalOwnership::make_tables_if_not_exist();
    op1.await;
}
