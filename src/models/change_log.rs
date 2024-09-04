use crate::models;
use crate::models::block_group::{BlockGroup, GroupBlock, PathChange};
use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::diff_block::ChangeType;
use crate::models::edge::Edge;
use crate::models::path::{NewBlock, Path};
use crate::models::sequence::Sequence;
use chrono::prelude::*;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use std::collections::HashSet;

#[derive(Debug)]
pub struct ChangeLog {
    pub id: Option<i32>,
    pub path_id: i32,
    pub path_start: i32,
    pub path_end: i32,
    pub seq_hash: String,
    pub seq_start: i32,
    pub seq_end: i32,
    pub strand: String,
}

impl ChangeLog {
    pub fn new(
        path_id: i32,
        path_start: i32,
        path_end: i32,
        seq_hash: String,
        seq_start: i32,
        seq_end: i32,
        seq_strand: String,
    ) -> ChangeLog {
        ChangeLog {
            id: None,
            path_id,
            path_start,
            path_end,
            seq_hash,
            seq_start,
            seq_end,
            strand: seq_strand,
        }
    }

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<ChangeLog> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(ChangeLog {
                    id: Some(row.get(0)?),
                    path_id: row.get(1)?,
                    path_start: row.get(2)?,
                    path_end: row.get(3)?,
                    seq_hash: row.get(4)?,
                    seq_start: row.get(5)?,
                    seq_end: row.get(6)?,
                    strand: row.get(7)?,
                })
            })
            .unwrap();
        rows.map(|row| row.unwrap()).collect()
    }

    pub fn save(&self, conn: &Connection) {
        ChangeLog::create(conn, self);
    }

    pub fn create(conn: &Connection, change_log: &ChangeLog) {
        let mut stmt = conn
            .prepare("INSERT INTO change_log (path_id, path_start, path_end, sequence_hash, sequence_start, sequence_end, sequence_strand) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);")
            .unwrap();
        let placeholders = vec![
            Value::from(change_log.path_id),
            Value::from(change_log.path_start),
            Value::from(change_log.path_end),
            Value::from(change_log.seq_hash.to_string()),
            Value::from(change_log.seq_start),
            Value::from(change_log.seq_end),
            Value::from(change_log.strand.to_string()),
        ];
        stmt.execute(params_from_iter(placeholders)).unwrap();
    }

    pub fn bulk_create(conn: &Connection, change_logs: &[ChangeLog]) {
        let mut rows_to_insert = vec![];
        for change_log in change_logs.iter() {
            let row = format!(
                "({0}, {1}, {2}, \"{3}\", {4}, {5}, \"{6}\")",
                change_log.path_id,
                change_log.path_start,
                change_log.path_end,
                change_log.seq_hash,
                change_log.seq_start,
                change_log.seq_end,
                change_log.strand
            );
            rows_to_insert.push(row);
        }
        let formatted_rows_to_insert = rows_to_insert.join(", ");

        let insert_statement = format!(
            "INSERT INTO change_log (path_id, path_start, path_end, sequence_hash, sequence_start, sequence_end, sequence_strand) VALUES {formatted_rows_to_insert};",
        );
        let _ = conn.execute(&insert_statement, ()).unwrap();
    }
}

#[derive(Debug)]
pub struct ChangeSet {
    id: Option<i32>,
    created: DateTime<Utc>,
    author: String,
    message: String,
}

impl ChangeSet {
    pub fn new(author: &str, message: &str) -> ChangeSet {
        ChangeSet {
            id: None,
            created: Utc::now(),
            author: author.to_string(),
            message: message.to_string(),
        }
    }

    pub fn save(self, conn: &Connection) -> ChangeSet {
        let mut stmt = conn
            .prepare("INSERT INTO change_set (created, author, message) VALUES (?1, ?2, ?3) RETURNING (id);")
            .unwrap();
        let created = Utc::now();
        let placeholders = vec![
            Value::from(created.timestamp()),
            Value::from(self.author.clone()),
            Value::from(self.message.clone()),
        ];
        let mut rows = stmt
            .query_map(params_from_iter(placeholders), |row| row.get(0))
            .unwrap();
        let id = rows.next().unwrap().unwrap();
        ChangeSet {
            id,
            created,
            author: self.author.clone(),
            message: self.message.clone(),
        }
    }

    pub fn add_changes(conn: &Connection, change_set_id: i32, change_log_id: i32) {
        let mut stmt = conn
            .prepare(
                "INSERT INTO change_set_changes (change_set_id, change_log_id) VALUES (?1, ?2);",
            )
            .unwrap();
        stmt.execute((change_set_id, change_log_id)).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Collection, Sample};
    use crate::test_helpers::get_connection;

    fn setup_block_group(conn: &Connection) -> (i32, Path) {
        let a_seq_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAAAA")
            .save(conn);
        let t_seq_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTTTT")
            .save(conn);
        let c_seq_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCCCCCCCC")
            .save(conn);
        let g_seq_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGGGGGGGG")
            .save(conn);
        let _collection = Collection::create(conn, "test");
        let block_group = BlockGroup::create(conn, "test", None, "hg19");
        let edge0 = Edge::create(
            conn,
            Edge::PATH_START_HASH.to_string(),
            0,
            "+".to_string(),
            a_seq_hash.clone(),
            0,
            "+".to_string(),
            0,
            0,
        );
        let edge1 = Edge::create(
            conn,
            a_seq_hash,
            10,
            "+".to_string(),
            t_seq_hash.clone(),
            0,
            "+".to_string(),
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            t_seq_hash,
            10,
            "+".to_string(),
            c_seq_hash.clone(),
            0,
            "+".to_string(),
            0,
            0,
        );
        let edge3 = Edge::create(
            conn,
            c_seq_hash,
            10,
            "+".to_string(),
            g_seq_hash.clone(),
            0,
            "+".to_string(),
            0,
            0,
        );
        let edge4 = Edge::create(
            conn,
            g_seq_hash,
            10,
            "+".to_string(),
            Edge::PATH_END_HASH.to_string(),
            0,
            "+".to_string(),
            0,
            0,
        );
        BlockGroupEdge::bulk_create(
            conn,
            block_group.id,
            vec![edge0.id, edge1.id, edge2.id, edge3.id, edge4.id],
        );
        let path = Path::create(
            conn,
            "chr1",
            block_group.id,
            vec![edge0.id, edge1.id, edge2.id, edge3.id, edge4.id],
        );
        (block_group.id, path)
    }

    #[test]
    fn test_simple_insert_change() {
        let conn = &get_connection(None);
        let (block_group_id, path) = setup_block_group(conn);
        let insert_sequence_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(conn);
        let insert_sequence = Sequence::sequence_from_hash(conn, &insert_sequence_hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 15,
            strand: "+".to_string(),
        };
        Sample::create(conn, "target");
        let new_bg = BlockGroup::get_or_create_sample_block_group(conn, "test", "target", "hg19");
        let new_paths = Path::get_paths(
            conn,
            "select * from path where block_group_id = ?1 AND name = ?2",
            vec![Value::from(new_bg), Value::from("chr1".to_string())],
        );
        let change = PathChange {
            block_group_id: new_bg,
            path: path.clone(),
            start: 7,
            end: 15,
            block: insert.clone(),
            chromosome_index: 1,
            phased: 0,
        };
        let tree = Path::intervaltree_for(conn, &path);
        BlockGroup::insert_change(conn, &change, &tree);
        let changes = ChangeLog::query(conn, "select * from change_log", vec![]);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].path_id, path.id);
        assert_eq!(changes[0].path_start, 7);
        assert_eq!(changes[0].path_end, 15);
        assert_eq!(changes[0].seq_hash, insert.sequence.hash);
        assert_eq!(changes[0].seq_start, 0);
        assert_eq!(changes[0].seq_end, 4);

        let change_set = ChangeSet::new("chris", "test change").save(conn);
        ChangeSet::add_changes(conn, change_set.id.unwrap(), 1);
    }
}
