#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Replace with "deployment.toml" when done testing.
    let main_deployment = include_str!("test_deployment.toml");
    let deployments: Vec<String> = vec![main_deployment.into()];
    obelisk_deployment::build_user_deployment(
        "sbtree",
        "public.ecr.aws/c6l0e8f4/obk-img-system",
        &deployments,
    )
    .await;
    // Create global tables and manager.
    sbtree::make_deployment().await;
    Ok(())
}
