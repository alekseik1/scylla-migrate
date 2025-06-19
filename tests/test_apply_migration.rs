use scylla::client::session_builder::SessionBuilder;
use scylla_migrate::Migrator;
use std::path::PathBuf;

#[tokio::test]
async fn test_apply_migration_does_not_apply_twice() {
    let builder = SessionBuilder::new().known_node("localhost:9042");
    let session = builder.build().await.unwrap();

    let current_file = std::file!();
    let absolute_path = PathBuf::from(current_file)
        .canonicalize()
        .expect("Failed to get absolute path");
    let migrations_path = absolute_path.parent().unwrap().join("fixtures");

    // Migrate the scylla database
    let runner = Migrator::new(&session, migrations_path.to_str().unwrap());
    runner.run().await.unwrap();
}
