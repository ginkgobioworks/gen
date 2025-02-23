use crate::models::block_group::BlockGroup;
use crate::models::collection::Collection;
use crate::models::sample::Sample;
use crate::models::traits::Query;
use rusqlite::{params, Connection};
use std::collections::{HashMap, HashSet};

/// Normalize a hierarchical collection name by removing trailing delimiters
/// (except if the entire collection name is "/"). For example:
/// "/foo/bar///" -> "/foo/bar", but "/" stays "/".
fn normalize_collection_name(mut full_collection: &str) -> &str {
    if full_collection == "/" {
        return "/";
    }
    full_collection = full_collection.trim_end_matches('/');
    if full_collection.is_empty() {
        // If it was all delimiters (e.g. "////"), treat it as "/"
        "/"
    } else {
        full_collection
    }
}

/// Return the final segment of a hierarchical collection name. For example,
/// given "/foo/bar", the final segment is "bar". Special case: "/" is root.
fn collection_basename(full_collection: &str) -> &str {
    let normalized = normalize_collection_name(full_collection);
    if normalized == "/" {
        return "/";
    }
    if let Some(idx) = normalized.rfind('/') {
        &normalized[idx + 1..]
    } else {
        normalized
    }
}

/// Return the parent portion of a hierarchical collection name. For example:
///   parent_collection("/foo/bar")   -> "/foo"
///   parent_collection("/foo/bar/")  -> "/foo"
///   parent_collection("/foo")       -> "/"
///   parent_collection("/")          -> "/"
///   parent_collection("bar")        -> "."
///
/// Note: If there's no slash in `full_collection`, we return "." to indicate
/// the "current directory" (matching typical Unix `dirname` behavior).
fn parent_collection(full_collection: &str) -> String {
    let normalized = normalize_collection_name(full_collection);
    if normalized == "/" {
        // Root has no parent
        return "/".to_string();
    }
    if let Some(idx) = normalized.rfind('/') {
        if idx == 0 {
            // "/foo"; parent is "/"
            "/".to_string()
        } else {
            normalized[..idx].to_string()
        }
    } else {
        // If there's no slash, treat it as a single component => parent is "."
        ".".to_string()
    }
}

#[derive(Debug)]
pub struct CollectionExplorerData {
    /// The final segment of the current collection name. For example,
    /// if the full collection is "/foo/bar", this would be "bar".
    pub current_collection: String,
    /// The block groups in the *entire* collection that have sample_name = NULL
    pub reference_block_groups: Vec<(i64, String)>,
    /// The samples in the entire collection
    pub collection_samples: Vec<String>,
    /// The block groups for each sample
    pub sample_block_groups: HashMap<String, Vec<(i64, String)>>,
    /// Immediate sub-collections ("direct children") one level deeper
    pub nested_collections: Vec<String>,
}

/// Gathers information about a hierarchical collection, enumerating direct
/// block groups, sampled block groups, and immediate sub-collections.
pub fn gather_collection_explorer_data(
    conn: &Connection,
    full_collection_name: &str,
) -> CollectionExplorerData {
    let current_collection = collection_basename(full_collection_name).to_string();
    let parent = parent_collection(full_collection_name);

    // 2) Query block groups that have sample_name = NULL for the entire collection
    let base_bgs = BlockGroup::query(
        conn,
        "SELECT * FROM block_groups
         WHERE collection_name = ?1
           AND sample_name IS NULL",
        params![full_collection_name],
    );
    let reference_block_groups: Vec<(i64, String)> =
        base_bgs.iter().map(|bg| (bg.id, bg.name.clone())).collect();

    // 3) Gather all samples associated with the entire collection
    let all_blocks = Collection::get_block_groups(conn, full_collection_name);
    let mut sample_names: HashSet<String> = all_blocks
        .iter()
        .filter_map(|bg| bg.sample_name.clone())
        .collect();
    let mut collection_samples: Vec<String> = sample_names.drain().collect();
    collection_samples.sort();

    // 4) For each sample, retrieve block groups
    let mut sample_block_groups = HashMap::new();
    for sample in &collection_samples {
        let bgs = Sample::get_block_groups(conn, full_collection_name, Some(sample));
        let pairs = bgs
            .iter()
            .map(|bg| (bg.id, bg.name.clone()))
            .collect::<Vec<(i64, String)>>();
        sample_block_groups.insert(sample.clone(), pairs);
    }

    // 5) Direct "nested" collections: must start with "full_collection_name + /" but no further delimiter
    let direct_prefix = format!("{}{}", full_collection_name, "/");

    let sibling_candidates = Collection::query(
        conn,
        "SELECT * FROM collections
         WHERE name GLOB ?1",
        params![format!("{}*", direct_prefix)],
    );

    let mut nested_collections = Vec::new();
    for child in sibling_candidates {
        // The portion *after* "/foo/bar/"
        let remainder = &child.name[direct_prefix.len()..];
        // If there's no further slash, it's a direct child
        if !remainder.is_empty() && !remainder.contains('/') {
            nested_collections.push(remainder.to_string());
        }
    }

    CollectionExplorerData {
        current_collection,
        reference_block_groups,
        collection_samples,
        sample_block_groups,
        nested_collections,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    /// For these tests we create an in-memory database, run minimal schema
    /// creation, and insert data to test gather_collection_explorer_data.
    #[test]
    fn test_gather_collection_explorer_data() {
        // 1) Set up an in-memory database
        let conn = Connection::open_in_memory().unwrap();

        // Minimal schema for the required tables, adapted from migrations
        conn.execute_batch(
            r#"
            CREATE TABLE collections (
              name TEXT PRIMARY KEY NOT NULL
            ) STRICT;
            
            CREATE TABLE block_groups (
              id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
              collection_name TEXT NOT NULL,
              sample_name TEXT,
              name TEXT NOT NULL
            ) STRICT;
            CREATE TABLE samples (
              name TEXT PRIMARY KEY NOT NULL
            ) STRICT;
        "#,
        )
        .unwrap();

        // 2) Insert data: one collection path + nested sub-collections
        // We'll store e.g. "/foo/bar" as the main path
        conn.execute(r#"INSERT INTO collections(name) VALUES (?1)"#, ["/foo/bar"])
            .unwrap();
        conn.execute(
            r#"INSERT INTO collections(name) VALUES (?1)"#,
            ["/foo/bar/a"],
        )
        .unwrap();
        conn.execute(
            r#"INSERT INTO collections(name) VALUES (?1)"#,
            ["/foo/bar/a/b"],
        )
        .unwrap();
        conn.execute(
            r#"INSERT INTO collections(name) VALUES (?1)"#,
            ["/foo/bar2"],
        )
        .unwrap();
        conn.execute(r#"INSERT INTO collections(name) VALUES (?1)"#, ["/foo/baz"])
            .unwrap();

        // 3) Insert a couple of samples
        conn.execute("INSERT INTO samples(name) VALUES (?1)", ["SampleAlpha"])
            .unwrap();
        conn.execute("INSERT INTO samples(name) VALUES (?1)", ["SampleBeta"])
            .unwrap();

        // 4) Insert block groups: some with sample = null, some with a sample
        // for the collection "/foo/bar"
        conn.execute(
            "INSERT INTO block_groups(collection_name, sample_name, name) VALUES(?1, NULL, ?2)",
            ["/foo/bar", "BG_ReferenceA"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO block_groups(collection_name, sample_name, name) VALUES(?1, NULL, ?2)",
            ["/foo/bar", "BG_ReferenceB"],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO block_groups(collection_name, sample_name, name) VALUES(?1, ?2, ?3)",
            ["/foo/bar", "SampleAlpha", "BG_Alpha1"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO block_groups(collection_name, sample_name, name) VALUES(?1, ?2, ?3)",
            ["/foo/bar", "SampleBeta", "BG_Beta1"],
        )
        .unwrap();

        // 5) Call the function under test—notice we pass the full path
        let explorer_data = gather_collection_explorer_data(&conn, "/foo/bar");

        // 6) Verify results
        // (A) The final path component is "bar"
        assert_eq!(explorer_data.current_collection, "bar");

        // (B) Reference block groups (sample_name IS NULL)
        let base_names: Vec<_> = explorer_data
            .reference_block_groups
            .iter()
            .map(|(_, name)| name.clone())
            .collect();
        assert_eq!(base_names.len(), 2);
        assert!(base_names.contains(&"BG_ReferenceA".to_string()));
        assert!(base_names.contains(&"BG_ReferenceB".to_string()));

        // (C) Collection samples
        // We expect SampleAlpha and SampleBeta
        assert_eq!(explorer_data.collection_samples.len(), 2);
        assert!(explorer_data
            .collection_samples
            .contains(&"SampleAlpha".to_string()));
        assert!(explorer_data
            .collection_samples
            .contains(&"SampleBeta".to_string()));

        // (D) Sample block groups
        // "SampleAlpha"
        let alpha_bg = explorer_data
            .sample_block_groups
            .get("SampleAlpha")
            .unwrap();
        let alpha_bg_names: Vec<_> = alpha_bg.iter().map(|(_, n)| n.clone()).collect();
        assert_eq!(alpha_bg_names, vec!["BG_Alpha1".to_string()]);
        // "SampleBeta"
        let beta_bg = explorer_data.sample_block_groups.get("SampleBeta").unwrap();
        let beta_bg_names: Vec<_> = beta_bg.iter().map(|(_, n)| n.clone()).collect();
        assert_eq!(beta_bg_names, vec!["BG_Beta1".to_string()]);

        // (E) Nested collections: we only want the direct child after "/foo/bar/"
        // e.g. "/foo/bar/a" => child is "a"
        // "/foo/bar/a/b" is not a direct child, it's an extra level
        // "/foo/bar2" doesn't match the prefix "/foo/bar/"
        // ... So only "a" is a direct nested collection
        assert_eq!(explorer_data.nested_collections, vec!["a".to_string()]);
    }

    #[test]
    fn test_trailing_delimiter_behavior() {
        // This verifies how we handle trailing hierarchical delimiters
        assert_eq!(normalize_collection_name("/foo/bar/"), "/foo/bar");
        assert_eq!(normalize_collection_name("////"), "/");
        assert_eq!(normalize_collection_name("/"), "/");

        assert_eq!(collection_basename("/foo/bar/"), "bar");
        assert_eq!(collection_basename("////"), "/");
        assert_eq!(collection_basename("/"), "/");

        assert_eq!(parent_collection("/foo/bar/"), "/foo");
        // parent of /foo => /
        assert_eq!(parent_collection("/foo/"), "/");
        // parent of / => /
        assert_eq!(parent_collection("////"), "/");
        // parent of a single "segment" => "."
        assert_eq!(parent_collection("bar"), ".");
    }
}
