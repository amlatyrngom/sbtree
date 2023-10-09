#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Replace with "deployment.toml" when done testing.
    let main_deployment = include_str!("deployment.toml");
    let deployments: Vec<String> = vec![main_deployment.into()];
    obelisk_deployment::build_user_deployment("sbtree", &deployments).await;
    // Create global tables and manager.
    sbtree::make_deployment().await;
    Ok(())
}
