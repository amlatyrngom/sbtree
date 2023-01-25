#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if let Some(arg) = args.get(1) {
        // AWS charges for each active endpoints.
        // This is needed to avoid extra costs.
        if arg == "teardown" {
            obelisk_deployment::teardown_deployment().await;
            return Ok(());
        }
    }
    let main_deployment = include_str!("deployment.toml");
    let deployments: Vec<String> = vec![main_deployment.into()];
    obelisk_deployment::build_user_deployment(
        "sbtree",
        "public.ecr.aws/c6l0e8f4/obk-img-system",
        &deployments,
    )
    .await;
    // Create manager.
    let _mc = obelisk::MessagingClient::new("sbtree", "manager").await;
    Ok(())
}
