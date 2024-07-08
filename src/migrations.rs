use rusqlite_migration::Migrations;
use include_dir::{Dir, include_dir};
use rusqlite::{params, Connection};

static MIGRATION_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/migrations");


pub fn run_migrations(conn: &mut Connection) {
    let migrations = Migrations::from_directory(&MIGRATION_DIR).unwrap();

    // Apply some PRAGMA, often better to do it outside of migrations
    conn.pragma_update_and_check(None, "journal_mode", &"WAL", |_| Ok(())).unwrap();

    // 2️⃣ Update the database schema, atomically
    let r = migrations.to_latest(conn);
    r.unwrap_or_else(|_| panic!("Failed to apply migrations"));
}