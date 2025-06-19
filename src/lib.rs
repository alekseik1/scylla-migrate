//! ScyllaDB migration runner library
//!
//! This library provides functionality for managing database migrations in ScyllaDB.
//! It supports reading .cql files from a specified directory and executing them in order,
//! while tracking which migrations have been applied.
//!
//! # Example
//! ```no_run
//! use scylla_migrate::Migrator;
//! use scylla::client::session_builder::SessionBuilder;
//!
//! async fn migrate() -> anyhow::Result<()> {
//!     let session = SessionBuilder::new()
//!         .known_node("localhost:9042")
//!         .build()
//!         .await?;
//!
//!     let runner = Migrator::new(&session, "migrations");
//!     runner.run().await?;
//!     Ok(())
//! }
//! ```

mod migration;

use crate::migration::{AppliedMigration, Migration};
use anyhow::{anyhow, Context, Result};
use scylla::client::session::Session;
use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use time::OffsetDateTime;
use tokio::fs;

fn check_conflicting_files(migrations_path: &PathBuf, name_to_add: &str) -> Result<()> {
    let version = name_to_add.split("_").next();
    if version.is_none() {
        return Ok(());
    }
    let version = version.unwrap();
    for one_entry in std::fs::read_dir(migrations_path)? {
        let one_entry = one_entry.expect("Failed to read migrations directory entry");
        let does_contain = one_entry
            .file_name()
            .to_str()
            .and_then(|name| Some(name.contains(version)));
        if does_contain.unwrap_or(false) {
            return Err(anyhow!("Conflicting migration file names found"));
        }
    }
    Ok(())
}

pub fn create_migration(migrations_path: &PathBuf, name: &str) -> Result<()> {
    std::fs::create_dir_all(migrations_path).context("Unable to create migrations directory")?;

    let dt = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)?
        .replace([':', '-', '.', 'Z', 'T'], "")[..17]
        .to_string();

    let filename = format!("{}_{}.cql", dt, name);
    check_conflicting_files(migrations_path, &filename)?;
    let filepath = migrations_path.join(filename);

    let content = format!(
        "-- Migration: {}\n-- Timestamp: {}\n\n-- Add your CQL queries here\n",
        name, dt
    );

    std::fs::write(&filepath, content)?;
    println!("Created migration: {:?}", filepath);

    Ok(())
}

/// Main runner for executing database migrations
#[derive(Debug)]
pub struct Migrator<'a> {
    session: &'a Session,
    migrations_src: &'a str,
}

impl<'a> Migrator<'a> {
    /// Creates a new Migrator instance
    pub fn new(session: &'a Session, migrations_src: &'a str) -> Self {
        Self {
            session,
            migrations_src,
        }
    }

    async fn create_public_keyspace(&self) -> Result<()> {
        self.session
            .query_unpaged(
                r#"
                CREATE KEYSPACE IF NOT EXISTS public
                WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}
                "#,
                &[],
            )
            .await?;
        self.session.await_schema_agreement().await?;
        Ok(())
    }

    async fn create_migration_table(&self) -> Result<()> {
        self.session
            .query_unpaged(
                r#"CREATE TABLE IF NOT EXISTS public.migrations (
                    version bigint,
                    checksum blob,
                    description text,
                    applied_at timestamp,
                    PRIMARY KEY (version, checksum)
                )"#,
                &[],
            )
            .await?;
        self.session.await_schema_agreement().await?;
        Ok(())
    }

    async fn record_migration(&self, migration: &Migration) -> Result<()> {
        self.session
            .query_unpaged(
                r#"
                    INSERT INTO public.migrations
                        (version, description, checksum, applied_at)
                        VALUES (?, ?, ?, ?)
                "#,
                (
                    migration.version,
                    migration.description.as_ref(),
                    migration.checksum.as_ref(),
                    OffsetDateTime::now_utc(),
                ),
            )
            .await?;
        Ok(())
    }

    async fn get_applied_migrations(&self) -> Result<HashMap<i64, AppliedMigration>> {
        let query_rows = self
            .session
            .query_unpaged("SELECT version, checksum FROM public.migrations", ())
            .await?
            .into_rows_result()
            .context("Failed to get rows from migrations table")?;

        let mut map = HashMap::new();

        for row in query_rows.rows()? {
            let (v, c): (i64, Vec<u8>) = row?;
            map.insert(
                v,
                AppliedMigration {
                    checksum: Cow::Owned(c),
                },
            );
        }

        Ok(map)
    }

    async fn load_migrations(migrations_src: &str) -> Result<Vec<Migration>> {
        let mut entries = fs::read_dir(migrations_src)
            .await
            .context("Could not find migrations directory")?;

        let mut migrations = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            if let Ok(meta) = entry.metadata().await {
                if !meta.is_file() {
                    continue;
                }

                let path = entry.path();
                if path.extension().and_then(|ext| ext.to_str()) != Some("cql") {
                    continue;
                }

                let filename = entry.file_name().to_string_lossy().into_owned();

                let version = filename
                    .split('_')
                    .next()
                    .and_then(|v| v.parse::<i64>().ok())
                    .ok_or_else(|| {
                        anyhow::anyhow!("Invalid migration filename format: {}", filename)
                    })?;

                let cql = fs::read_to_string(path).await?;

                migrations.push(Migration::new(
                    version,
                    Cow::Owned(entry.file_name().to_string_lossy().to_string()),
                    Cow::Owned(cql),
                ));
            }
        }

        // Sort migrations by version
        migrations.sort_by(|a, b| a.version.cmp(&b.version));
        Ok(migrations)
    }

    /// Runs all pending migrations
    ///
    /// This will:
    /// 1. Create the public keyspace and migrations table if they don't exist
    /// 2. Load all migrations from the migrations directory
    /// 3. Check each migration and execute it if it hasn't been applied
    pub async fn run(&self) -> Result<()> {
        self.create_public_keyspace().await?;
        self.create_migration_table().await?;

        let migrations = Migrator::load_migrations(self.migrations_src).await?;
        let applied_migrations = self.get_applied_migrations().await?;
        for migration in migrations {
            if let Some(applied) = applied_migrations.get(&migration.version) {
                if applied.checksum.as_ref() == migration.checksum.as_ref() {
                    println!("Migration {} already applied", migration.description);
                    continue;
                } else {
                    // Checksum different - run the migration again as it might have new statements
                    println!(
                        "Migration {} has changes, applying updates",
                        migration.description
                    );
                }
            }

            // Either migration hasn't been applied or has changes
            migration.up(self.session).await?;
            self.record_migration(&migration).await?;
            println!(
                "Applied {}/migrate {}",
                migration.version, migration.description
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_migration_in_same_day_would_give_different_versions() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let migrations_path = temp_dir.path().to_path_buf();

        create_migration(&migrations_path, "test1").expect("Failed to create migration");
        // minimal resolution - 1 ms
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        
        create_migration(&migrations_path, "test2").expect("Failed to create migration");
        let paths: Vec<String> = std::fs::read_dir(&migrations_path)
            .expect("Failed to read migrations directory")
            .map(|entry| {
                entry
                    .expect("Failed to read migrations directory entry")
                    .path()
                    .file_name()
                    .expect("Failed to get file name")
                    .to_owned()
                    .into_string()
                    .expect("Failed to convert file name to string")
            })
            .collect();
        assert_eq!(paths.len(), 2);
        let migrations = Migrator::load_migrations(migrations_path.to_str().unwrap())
            .await
            .unwrap();
        let unique_versions: HashSet<_> = migrations.iter().map(|m| m.version).collect();
        assert_eq!(unique_versions.len(), 2);
    }
}
