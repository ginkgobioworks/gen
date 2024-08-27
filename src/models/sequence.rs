use noodles::core::Position;
use noodles::fasta;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::{fs, path::PathBuf, str};

#[derive(Clone, Debug)]
pub struct Sequence {
    pub hash: String,
    pub sequence_type: String,
    sequence: String,
    // these 2 fields are only relevant when the sequence is stored externally
    pub name: String,
    pub file_path: String,
    pub length: i32,
    // indicates whether the sequence is stored externally, a quick flag instead of having to
    // check sequence or file_path and do the logic in function calls.
    pub external_sequence: bool,
}

#[derive(Default)]
pub struct NewSequence<'a> {
    sequence_type: Option<&'a str>,
    sequence: Option<&'a str>,
    name: Option<&'a str>,
    file_path: Option<&'a str>,
    length: Option<i32>,
    shallow: bool,
}

impl<'a> NewSequence<'a> {
    pub fn shallow(mut self, setting: bool) -> Self {
        self.shallow = setting;
        self
    }

    pub fn sequence_type(mut self, seq_type: &'a str) -> Self {
        self.sequence_type = Some(seq_type);
        self
    }

    pub fn sequence(mut self, sequence: &'a str) -> Self {
        self.sequence = Some(sequence);
        self.length = Some(sequence.len() as i32);
        self
    }

    pub fn name(mut self, name: &'a str) -> Self {
        self.name = Some(name);
        self
    }

    pub fn file_path(mut self, path: &'a str) -> Self {
        self.file_path = Some(path);
        self.shallow = true;
        self
    }

    pub fn length(mut self, length: i32) -> Self {
        self.length = Some(length);
        self
    }

    pub fn hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.sequence_type.expect("Sequence type must be defined."));
        hasher.update(";");
        if let Some(v) = self.sequence {
            hasher.update(v);
            hasher.update(";");
        }
        if let Some(v) = self.name {
            hasher.update(v);
            hasher.update(";");
        }
        if let Some(v) = self.file_path {
            hasher.update(v);
            hasher.update(";");
        }
        format!("{:x}", hasher.finalize())
    }

    pub fn build(self) -> Sequence {
        let file_path = self.file_path.unwrap_or("").to_string();
        let external_sequence = !file_path.is_empty();
        Sequence {
            hash: self.hash(),
            sequence_type: self.sequence_type.unwrap().to_string(),
            sequence: self.sequence.unwrap_or("").to_string(),
            name: self.name.unwrap_or("").to_string(),
            file_path,
            length: self.length.unwrap(),
            external_sequence,
        }
    }

    pub fn save(mut self, conn: &Connection) -> String {
        let mut length = 0;
        if self.sequence.is_none() && self.file_path.is_none() {
            panic!("Sequence or file_path must be set.");
        }
        if self.file_path.is_some() && self.name.is_none() {
            panic!("A filepath must have an accompanying sequence name");
        }
        if self.length.is_none() {
            if let Some(v) = self.sequence {
                length = v.len() as i32;
            } else {
                // TODO: if name/path specified, grab length automatically
                panic!("Sequence length must be specified.");
            }
        }
        let hash = self.hash();
        let mut obj_hash: String = match conn.query_row(
            "SELECT hash from sequence where hash = ?1;",
            [hash.clone()],
            |row| row.get(0),
        ) {
            Ok(res) => res,
            Err(rusqlite::Error::QueryReturnedNoRows) => "".to_string(),
            Err(_e) => {
                panic!("something bad happened querying the database")
            }
        };
        if obj_hash.is_empty() {
            let mut stmt = conn.prepare("INSERT INTO sequence (hash, sequence_type, sequence, name, file_path, length) VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING (hash);").unwrap();
            let mut rows = stmt
                .query_map(
                    (
                        Value::from(hash.to_string()),
                        Value::from(self.sequence_type.unwrap().to_string()),
                        Value::from(
                            (if self.shallow {
                                ""
                            } else {
                                self.sequence.unwrap()
                            })
                            .to_string(),
                        ),
                        Value::from(self.name.unwrap_or("").to_string()),
                        Value::from(self.file_path.unwrap_or("").to_string()),
                        Value::from(self.length.unwrap_or(length)),
                    ),
                    |row| row.get(0),
                )
                .unwrap();
            obj_hash = rows.next().unwrap().unwrap();
        }
        obj_hash
    }
}

impl Sequence {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> NewSequence<'static> {
        NewSequence {
            shallow: false,
            ..NewSequence::default()
        }
    }
    pub fn get_sequence(
        &self,
        start: impl Into<Option<i32>>,
        end: impl Into<Option<i32>>,
    ) -> String {
        let start: Option<i32> = start.into();
        let end: Option<i32> = end.into();
        let mut start = start.unwrap_or(0);
        let end = end.unwrap_or(self.length);
        if self.external_sequence {
            let mut sequence: Option<String> = None;
            let file_path = self.file_path.clone();
            let name = self.name.clone();
            // noodles is 1 index inclusive and we use that for fetching fastas
            start += 1;
            // todo: handle circles
            let index = format!("{file_path}.fai");
            if fs::metadata(index).is_ok() {
                // noodles reader query is 1 based, inclusive
                let mut reader = fasta::io::indexed_reader::Builder::default()
                    .build_from_path(&file_path)
                    .unwrap();
                sequence = Some(
                    str::from_utf8(
                        reader
                            .query(&format!("{name}:{start}-{end}").parse().unwrap())
                            .unwrap()
                            .sequence()
                            .as_ref(),
                    )
                    .unwrap()
                    .to_string(),
                );
            } else {
                let mut reader = fasta::io::reader::Builder
                    .build_from_path(&file_path)
                    .unwrap();
                for result in reader.records() {
                    let record = result.unwrap();
                    if String::from_utf8(record.name().to_vec()).unwrap() == name {
                        sequence = Some(
                            str::from_utf8(
                                record
                                    .sequence()
                                    .slice(
                                        Position::try_from(start as usize).unwrap()
                                            ..=Position::try_from(end as usize).unwrap(),
                                    )
                                    .unwrap()
                                    .as_ref(),
                            )
                            .unwrap()
                            .to_string(),
                        );
                        break;
                    }
                }
            }
            return sequence
                .unwrap_or_else(|| panic!("{name} not found in fasta file {file_path}"));
        }
        if start == 0 && end == self.length {
            return self.sequence.clone();
        }
        self.sequence[start as usize..end as usize].to_string()
    }

    pub fn sequences(conn: &Connection, query: &str, placeholders: Vec<Value>) -> Vec<Sequence> {
        let mut stmt = conn.prepare_cached(query).unwrap();
        let rows = stmt
            .query_map(params_from_iter(placeholders), |row| {
                let file_path: String = row.get(4).unwrap();
                let mut external_sequence = false;
                if !file_path.is_empty() {
                    external_sequence = true;
                }
                Ok(Sequence {
                    hash: row.get(0).unwrap(),
                    sequence_type: row.get(1).unwrap(),
                    sequence: row.get(2).unwrap(),
                    name: row.get(3).unwrap(),
                    file_path,
                    length: row.get(5).unwrap(),
                    external_sequence,
                })
            })
            .unwrap();
        let mut objs = vec![];
        for row in rows {
            objs.push(row.unwrap());
        }
        objs
    }

    pub fn sequences_by_hash(conn: &Connection, hashes: Vec<String>) -> HashMap<String, Sequence> {
        let joined_hashes = &hashes
            .into_iter()
            .map(|hash| format!("\"{}\"", hash))
            .collect::<Vec<_>>()
            .join(",");
        let sequences = Sequence::sequences(
            conn,
            &format!("select * from sequence where hash in ({0})", joined_hashes),
            vec![],
        );
        sequences
            .into_iter()
            .map(|sequence| (sequence.hash.clone(), sequence))
            .collect::<HashMap<String, Sequence>>()
    }

    pub fn sequence_from_hash(conn: &Connection, hash: &str) -> Option<Sequence> {
        let sequences_by_hash = Sequence::sequences_by_hash(conn, vec![hash.to_string()]);
        sequences_by_hash.get(hash).cloned()
    }
}

mod tests {
    use rusqlite::Connection;
    use std::ops::Deref;
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    use crate::migrations::run_migrations;

    fn get_connection() -> Connection {
        let mut conn = Connection::open_in_memory()
            .unwrap_or_else(|_| panic!("Error opening in memory test db"));
        rusqlite::vtab::array::load_module(&conn).unwrap();
        run_migrations(&mut conn);
        conn
    }

    #[test]
    fn test_builder() {
        let sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCG")
            .build();
        assert_eq!(sequence.length, 4);
        assert_eq!(sequence.sequence, "ATCG");
    }

    #[test]
    fn test_builder_with_from_disk() {
        let sequence = Sequence::new()
            .sequence_type("DNA")
            .name("chr1")
            .file_path("/foo/bar")
            .length(50)
            .build();
        assert_eq!(sequence.length, 50);
        assert_eq!(sequence.sequence, "");
    }

    #[test]
    fn test_create_sequence_in_db() {
        let conn = &mut get_connection();
        let seq_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("AACCTT")
            .save(conn);
        let sequences = Sequence::sequences(
            conn,
            "select * from sequence where hash = ?1",
            vec![Value::from(seq_hash)],
        );
        let sequence = sequences.first().unwrap();
        assert_eq!(&sequence.sequence, "AACCTT");
        assert_eq!(sequence.sequence_type, "DNA");
        assert!(!sequence.external_sequence);
    }

    #[test]
    fn test_create_sequence_on_disk() {
        let conn = &mut get_connection();
        let seq_hash = Sequence::new()
            .sequence_type("DNA")
            .name("chr1")
            .file_path("/some/path.fa")
            .length(10)
            .save(conn);
        let sequences = Sequence::sequences(
            conn,
            "select * from sequence where hash = ?1",
            vec![Value::from(seq_hash)],
        );
        let sequence = sequences.first().unwrap();
        assert_eq!(sequence.sequence_type, "DNA");
        assert_eq!(&sequence.sequence, "");
        assert_eq!(sequence.name, "chr1");
        assert_eq!(sequence.file_path, "/some/path.fa");
        assert_eq!(sequence.length, 10);
        assert!(sequence.external_sequence);
    }

    #[test]
    fn test_get_sequence() {
        let conn = &mut get_connection();
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let seq_hash = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCGATCGATCGATCGGGAACACACAGAGA")
            .save(conn);
        let seq = Sequence::sequence_from_hash(conn, &seq_hash).unwrap();
        assert_eq!(
            seq.get_sequence(None, None),
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA"
        );
        assert_eq!(seq.get_sequence(0, 5), "ATCGA");
        assert_eq!(seq.get_sequence(10, 15), "CGATC");
        assert_eq!(seq.get_sequence(3, None), "GATCGATCGATCGATCGGGAACACACAGAGA");
        assert_eq!(seq.get_sequence(None, 5), "ATCGA");
    }

    #[test]
    fn test_get_sequence_from_disk() {
        let conn = &mut get_connection();
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let seq_hash = Sequence::new()
            .sequence_type("DNA")
            .name("m123")
            .file_path(fasta_path.to_str().unwrap())
            .length(34)
            .save(conn);
        let seq = Sequence::sequence_from_hash(conn, &seq_hash).unwrap();
        assert_eq!(
            seq.get_sequence(None, None),
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA"
        );
        assert_eq!(seq.get_sequence(0, 5), "ATCGA");
        assert_eq!(seq.get_sequence(10, 15), "CGATC");
        assert_eq!(seq.get_sequence(3, None), "GATCGATCGATCGATCGGGAACACACAGAGA");
        assert_eq!(seq.get_sequence(None, 5), "ATCGA");
    }
}
