use cached::proc_macro::cached;
use noodles::bgzf::{self, gzi};
use noodles::core::Region;
use noodles::fasta::{self, fai, indexed_reader::Builder as IndexBuilder};
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::{fs, str, sync};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
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

#[derive(Default, Debug)]
pub struct NewSequence<'a> {
    sequence_type: Option<&'a str>,
    sequence: Option<&'a str>,
    name: Option<&'a str>,
    file_path: Option<&'a str>,
    length: Option<i32>,
    shallow: bool,
}

impl<'a> From<&'a Sequence> for NewSequence<'a> {
    fn from(value: &'a Sequence) -> NewSequence<'a> {
        NewSequence::new()
            .sequence_type(&value.sequence_type)
            .sequence(&value.sequence)
            .name(&value.name)
            .file_path(&value.file_path)
            .length(value.length)
    }
}

impl<'a> NewSequence<'a> {
    pub fn new() -> NewSequence<'static> {
        NewSequence {
            shallow: false,
            ..NewSequence::default()
        }
    }

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
        if !path.is_empty() {
            self.file_path = Some(path);
            self.shallow = true;
        }
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
        } else {
            hasher.update("");
        }
        hasher.update(";");
        if let Some(v) = self.name {
            hasher.update(v);
        } else {
            hasher.update("");
        }
        hasher.update(";");
        if let Some(v) = self.file_path {
            hasher.update(v);
        } else {
            hasher.update("");
        }
        hasher.update(";");

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

    pub fn save(self, conn: &Connection) -> Sequence {
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
        Sequence {
            hash: obj_hash,
            sequence_type: self.sequence_type.unwrap().to_string(),
            sequence: self.sequence.unwrap_or("").to_string(),
            name: self.name.unwrap_or("").to_string(),
            file_path: self.file_path.unwrap_or("").to_string(),
            length: self.length.unwrap_or(length),
            external_sequence: !self.file_path.unwrap_or("").is_empty(),
        }
    }
}

#[cached(key = "String", convert = r#"{ format!("{}", path) }"#)]
fn fasta_index(path: &str) -> Option<fai::Index> {
    let index_path = format!("{path}.fai");
    if fs::metadata(&index_path).is_ok() {
        return Some(fai::read(&index_path).unwrap());
    }
    None
}

#[cached(key = "String", convert = r#"{ format!("{}", path) }"#)]
fn fasta_gzi_index(path: &str) -> Option<gzi::Index> {
    let index_path = format!("{path}.gzi");
    if fs::metadata(&index_path).is_ok() {
        return Some(gzi::read(&index_path).unwrap());
    }
    None
}

fn cached_sequence(file_path: &str, name: &str, start: usize, end: usize) -> Option<String> {
    static SEQUENCE_CACHE: sync::LazyLock<sync::RwLock<HashMap<String, Option<String>>>> =
        sync::LazyLock::new(|| sync::RwLock::new(HashMap::new()));
    let key = format!("{file_path}-{name}");

    {
        let cache = SEQUENCE_CACHE.read().unwrap();
        if let Some(cached_sequence) = cache.get(&key) {
            if let Some(sequence) = cached_sequence {
                return Some(sequence[start..end].to_string());
            }
            return None;
        }
    }

    let mut cache = SEQUENCE_CACHE.write().unwrap();

    let mut sequence: Option<String> = None;
    let region = name.parse::<Region>().unwrap();
    if let Some(index) = fasta_index(file_path) {
        let builder = IndexBuilder::default().set_index(index);
        if let Some(gzi_index) = fasta_gzi_index(file_path) {
            let bgzf_reader = bgzf::indexed_reader::Builder::default()
                .set_index(gzi_index)
                .build_from_path(file_path)
                .unwrap();
            let mut reader = builder.build_from_reader(bgzf_reader).unwrap();
            sequence = Some(
                str::from_utf8(reader.query(&region).unwrap().sequence().as_ref())
                    .unwrap()
                    .to_string(),
            )
        } else {
            let mut reader = builder.build_from_path(file_path).unwrap();
            sequence = Some(
                str::from_utf8(reader.query(&region).unwrap().sequence().as_ref())
                    .unwrap()
                    .to_string(),
            );
        }
    } else {
        let mut reader = fasta::io::reader::Builder
            .build_from_path(file_path)
            .unwrap();
        for result in reader.records() {
            let record = result.unwrap();
            if String::from_utf8(record.name().to_vec()).unwrap() == name {
                sequence = Some(
                    str::from_utf8(record.sequence().as_ref())
                        .unwrap()
                        .to_string(),
                );
                break;
            }
        }
    }
    // this is a LRU cache setup, we just keep the last sequence we fetched so we don't end up loading
    // plant genomes into memory.
    cache.clear();
    cache.insert(key.clone(), sequence);
    // we do this to avoid a clone of potentially large data.
    if let Some(seq) = &cache[&key] {
        return Some(seq[start..end].to_string());
    }
    None
}

impl Sequence {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> NewSequence<'static> {
        NewSequence::new()
    }

    pub fn get_sequence(
        &self,
        start: impl Into<Option<i32>>,
        end: impl Into<Option<i32>>,
    ) -> String {
        // todo: handle circles

        let start: Option<i32> = start.into();
        let end: Option<i32> = end.into();
        let start = start.unwrap_or(0) as usize;
        let end = end.unwrap_or(self.length) as usize;
        if self.external_sequence {
            if let Some(sequence) = cached_sequence(&self.file_path, &self.name, start, end) {
                return sequence;
            } else {
                panic!(
                    "{name} not found in fasta file {file_path}",
                    name = self.name,
                    file_path = self.file_path
                );
            }
        }
        if start == 0 && end as i32 == self.length {
            return self.sequence.clone();
        }
        self.sequence[start..end].to_string()
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
                let hash: String = row.get(0).unwrap();
                let sequence = row.get(2).unwrap();
                Ok(Sequence {
                    hash,
                    sequence_type: row.get(1).unwrap(),
                    sequence,
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

    pub fn sequences_by_hash(conn: &Connection, hashes: Vec<&str>) -> HashMap<String, Sequence> {
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
        let sequences_by_hash = Sequence::sequences_by_hash(conn, vec![hash]);
        sequences_by_hash.get(hash).cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::test_helpers::get_connection;
    use std::time;

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
        let conn = &mut get_connection(None);
        let sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("AACCTT")
            .save(conn);
        assert_eq!(&sequence.sequence, "AACCTT");
        assert_eq!(sequence.sequence_type, "DNA");
        assert!(!sequence.external_sequence);
    }

    #[test]
    fn test_create_sequence_on_disk() {
        let conn = &mut get_connection(None);
        let sequence = Sequence::new()
            .sequence_type("DNA")
            .name("chr1")
            .file_path("/some/path.fa")
            .length(10)
            .save(conn);
        assert_eq!(sequence.sequence_type, "DNA");
        assert_eq!(&sequence.sequence, "");
        assert_eq!(sequence.name, "chr1");
        assert_eq!(sequence.file_path, "/some/path.fa");
        assert_eq!(sequence.length, 10);
        assert!(sequence.external_sequence);
    }

    #[test]
    fn test_get_sequence() {
        let conn = &mut get_connection(None);
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let sequence = Sequence::new()
            .sequence_type("DNA")
            .sequence("ATCGATCGATCGATCGATCGGGAACACACAGAGA")
            .save(conn);
        assert_eq!(
            sequence.get_sequence(None, None),
            "ATCGATCGATCGATCGATCGGGAACACACAGAGA"
        );
        assert_eq!(sequence.get_sequence(0, 5), "ATCGA");
        assert_eq!(sequence.get_sequence(10, 15), "CGATC");
        assert_eq!(
            sequence.get_sequence(3, None),
            "GATCGATCGATCGATCGGGAACACACAGAGA"
        );
        assert_eq!(sequence.get_sequence(None, 5), "ATCGA");
    }

    #[test]
    fn test_get_sequence_from_disk() {
        let conn = &mut get_connection(None);
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/simple.fa");
        let seq = Sequence::new()
            .sequence_type("DNA")
            .name("m123")
            .file_path(fasta_path.to_str().unwrap())
            .length(34)
            .save(conn);
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
    fn test_cached_sequence_performance() {
        let conn = &mut get_connection(None);
        let mut fasta_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        fasta_path.push("fixtures/chr22.fa.gz");
        let sequence = Sequence::new()
            .sequence_type("DNA")
            .file_path(fasta_path.to_str().unwrap())
            .name("chr22")
            .length(51_000_000)
            .save(conn);
        let s = time::Instant::now();
        for _ in 1..1_000_000 {
            sequence.get_sequence(1, 20);
        }
        assert!(s.elapsed().as_secs() < 5);
    }
}
