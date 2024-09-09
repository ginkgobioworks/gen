use crate::models::block_group::{BlockGroup, PathChange};
use crate::models::block_group_edge::BlockGroupEdge;
use crate::models::edge::Edge;
use crate::models::path::{NewBlock, Path};
use crate::models::sequence::Sequence;
use crate::models::strand::Strand;
use chrono::prelude::*;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection, ToSql};
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug)]
pub struct ChangeLog {
    pub id: Option<i32>,
    pub path_id: i32,
    pub path_start: i32,
    pub path_end: i32,
    pub seq_hash: String,
    pub seq_start: i32,
    pub seq_end: i32,
    pub strand: Strand,
}

#[derive(Debug, PartialEq)]
pub struct ChangeLogSummary {
    pub id: i32,
    pub parent_region: (String, i32, i32),
    pub parent_left: String,
    pub parent_impacted: String,
    pub parent_right: String,
    pub new_sequence: String,
}

impl ChangeLog {
    pub fn new(
        path_id: i32,
        path_start: i32,
        path_end: i32,
        seq_hash: String,
        seq_start: i32,
        seq_end: i32,
        seq_strand: Strand,
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
            Value::from(change_log.strand),
        ];
        stmt.execute(params_from_iter(placeholders)).unwrap();
    }

    pub fn bulk_create(conn: &Connection, change_logs: &[ChangeLog]) {
        if !change_logs.is_empty() {
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

    pub fn summary(conn: &Connection, change_log_id: i32) -> ChangeLogSummary {
        let change_log = ChangeLog::query(
            conn,
            "select * from change_log where id = ?1",
            vec![Value::from(change_log_id)],
        );
        let path_id = change_log[0].path_id;
        let path_start = change_log[0].path_start;
        let path_end = change_log[0].path_end;
        let seq_hash = change_log[0].seq_hash.clone();
        let seq_start = change_log[0].seq_start;
        let seq_end = change_log[0].seq_end;
        let path = Path::get(conn, path_id);
        let path_name = path.name.clone();
        let sequence = Path::sequence(conn, path);
        let mut left_bound = path_start - 20;
        if left_bound < 0 {
            left_bound = 0
        };
        let mut right_bound = path_end + 20;
        if right_bound > sequence.len() as i32 {
            right_bound = sequence.len() as i32;
        }
        let mut impacted_sequence = sequence[path_start as usize..path_end as usize].to_string();
        if impacted_sequence.len() > 20 {
            impacted_sequence = format!(
                "{ls}...{rs}",
                ls = &impacted_sequence[..8],
                rs = &impacted_sequence[impacted_sequence.len() - 8..]
            );
        }
        let new_sequence = Sequence::sequence_from_hash(conn, &seq_hash).unwrap();
        let mut updated_sequence = new_sequence.get_sequence(seq_start, seq_end);
        if updated_sequence.len() > 20 {
            updated_sequence = format!(
                "{ls}...{rs}",
                ls = &updated_sequence[..8],
                rs = &updated_sequence[updated_sequence.len() - 8..]
            );
        }
        let left_sequence = sequence[left_bound as usize..path_start as usize].to_string();
        let right_sequence = sequence[path_end as usize..right_bound as usize].to_string();

        ChangeLogSummary {
            id: change_log_id,
            parent_region: (path_name, path_start, path_end),
            parent_left: left_sequence,
            parent_impacted: impacted_sequence,
            parent_right: right_sequence,
            new_sequence: updated_sequence,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChangeSet {
    pub id: Option<i32>,
    pub collection_name: String,
    pub created: DateTime<Utc>,
    pub author: String,
    pub message: String,
}

struct ChangeSummary {
    change_set: ChangeSet,
    changes: Vec<ChangeLog>,
}

impl ChangeSet {
    pub fn new(collection_name: &str, author: &str, message: &str) -> ChangeSet {
        ChangeSet {
            id: None,
            collection_name: collection_name.to_string(),
            created: Utc::now(),
            author: author.to_string(),
            message: message.to_string(),
        }
    }

    pub fn save(self, conn: &Connection) -> ChangeSet {
        let mut stmt = conn
            .prepare("INSERT INTO change_set (collection_name, created, author, message) VALUES (?1, ?2, ?3, ?4) RETURNING (id);")
            .unwrap();
        let created = Utc::now();
        let placeholders = vec![
            Value::from(self.collection_name.clone()),
            Value::from(created.timestamp_nanos_opt()),
            Value::from(self.author.clone()),
            Value::from(self.message.clone()),
        ];
        let mut rows = stmt
            .query_map(params_from_iter(placeholders), |row| row.get(0))
            .unwrap();
        let id = rows.next().unwrap().unwrap();
        ChangeSet {
            id,
            collection_name: self.collection_name.clone(),
            created,
            author: self.author.clone(),
            message: self.message.clone(),
        }
    }

    pub fn query(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<ChangeSet> {
        let mut stmt = conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                Ok(ChangeSet {
                    id: Some(row.get(0)?),
                    collection_name: row.get(1)?,
                    created: DateTime::from_timestamp_nanos(row.get(2)?),
                    author: row.get(3)?,
                    message: row.get(4)?,
                })
            })
            .unwrap();
        rows.map(|row| row.unwrap()).collect()
    }

    pub fn add_changes(conn: &Connection, change_set_id: i32, change_log_id: i32) {
        let mut stmt = conn
            .prepare(
                "INSERT INTO change_set_changes (change_set_id, change_log_id) VALUES (?1, ?2);",
            )
            .unwrap();
        stmt.execute((change_set_id, change_log_id)).unwrap();
    }

    pub fn get_changes(conn: &Connection, change_set_id: i32) -> ChangeSummary {
        let change_set = ChangeSet::query(
            conn,
            "select * from change_set where id = ?1",
            vec![Value::from(change_set_id)],
        )
        .first()
        .unwrap()
        .clone();
        let mut stmt = conn.prepare("select cl.* from change_set_changes csc left join change_log cl on (cl.id = csc.change_log_id and csc.change_set_id = ?1);").unwrap();
        let rows = stmt
            .query_map([change_set_id], |row| {
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
        ChangeSummary {
            change_set,
            changes: rows.map(|row| row.unwrap()).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Collection, Sample};
    use crate::test_helpers::get_connection;

    fn setup_block_group(conn: &Connection) -> (i32, Path) {
        let a_seq = Sequence::new()
            .sequence_type("DNA")
            .sequence("AAAAAAAAAA")
            .save(conn);
        let t_seq = Sequence::new()
            .sequence_type("DNA")
            .sequence("TTTTTTTTTT")
            .save(conn);
        let c_seq = Sequence::new()
            .sequence_type("DNA")
            .sequence("CCCCCCCCCC")
            .save(conn);
        let g_seq = Sequence::new()
            .sequence_type("DNA")
            .sequence("GGGGGGGGGG")
            .save(conn);
        let _collection = Collection::create(conn, "test");
        let block_group = BlockGroup::create(conn, "test", None, "hg19");
        let edge0 = Edge::create(
            conn,
            Edge::PATH_START_HASH.to_string(),
            0,
            Strand::Forward,
            a_seq.hash.clone(),
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge1 = Edge::create(
            conn,
            a_seq.hash,
            10,
            Strand::Forward,
            t_seq.hash.clone(),
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge2 = Edge::create(
            conn,
            t_seq.hash,
            10,
            Strand::Forward,
            c_seq.hash.clone(),
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge3 = Edge::create(
            conn,
            c_seq.hash,
            10,
            Strand::Forward,
            g_seq.hash.clone(),
            0,
            Strand::Forward,
            0,
            0,
        );
        let edge4 = Edge::create(
            conn,
            g_seq.hash,
            10,
            Strand::Forward,
            Edge::PATH_END_HASH.to_string(),
            0,
            Strand::Forward,
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
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNN")
            .save(conn);
        let insert_sequence = Sequence::sequence_from_hash(conn, &insert_sequence.hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.get_sequence(0, 4).to_string(),
            sequence_start: 0,
            sequence_end: 4,
            path_start: 7,
            path_end: 15,
            strand: Strand::Forward,
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

        let change_set = ChangeSet::new("test", "chris", "test change").save(conn);
        let change_set_id = change_set.id.unwrap();
        ChangeSet::add_changes(conn, change_set_id, 1);
        let changes = ChangeSet::get_changes(conn, change_set_id);
        assert_eq!(changes.change_set, change_set);
    }

    #[test]
    fn test_summary() {
        let conn = &get_connection(None);
        let (block_group_id, path) = setup_block_group(conn);
        let insert_sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("NNNNAAATCATAGCATCATCANNNNAAATCATAGCATCATCANNNNAAATCATAGCATCATCANNNNAAATCATAGCATCATCANNNNAAATCATAGCATCATCANNNNAAATCATAGCATCATCANNNNAAATCATAGCATCATCANNNNAAATCATAGCATCATCANNNNAAATCATAGCATCATCA")
            .save(conn);
        let insert_sequence = Sequence::sequence_from_hash(conn, &insert_sequence.hash).unwrap();
        let insert = NewBlock {
            id: 0,
            sequence: insert_sequence.clone(),
            block_sequence: insert_sequence.get_sequence(0, 189).to_string(),
            sequence_start: 0,
            sequence_end: 189,
            path_start: 7,
            path_end: 15,
            strand: Strand::Forward,
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
        let change_id = changes[0].id.unwrap();
        assert_eq!(
            ChangeLog::summary(conn, change_id),
            ChangeLogSummary {
                id: change_id,
                parent_region: (path.name.clone(), 7, 15),
                parent_left: "AAAAAAA".to_string(),
                parent_right: "TTTTTCCCCCCCCCCGGGGG".to_string(),
                parent_impacted: "AAATTTTT".to_string(),
                new_sequence: "NNNNAAAT...CATCATCA".to_string(),
            }
        );
    }
}
