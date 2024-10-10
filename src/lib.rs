use std::str;

pub mod config;
pub mod exports;
pub mod gfa_reader;
pub mod graph;
pub mod imports;
pub mod migrations;
pub mod models;
pub mod operation_management;
pub mod test_helpers;
pub mod updates;

use crate::migrations::run_migrations;
use noodles::vcf::variant::record::samples::series::value::genotype::Phasing;
use rusqlite::Connection;
use sha2::{Digest, Sha256};

pub fn get_connection(db_path: &str) -> Connection {
    let mut conn =
        Connection::open(db_path).unwrap_or_else(|_| panic!("Error connecting to {}", db_path));
    rusqlite::vtab::array::load_module(&conn).unwrap();
    run_migrations(&mut conn);
    conn
}

pub fn run_query(conn: &Connection, query: &str) {
    let mut stmt = conn.prepare(query).unwrap();
    for entry in stmt.query_map([], |row| Ok(println!("{row:?}"))).unwrap() {
        println!("{entry:?}");
    }
}

pub fn calculate_hash(t: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(t);
    let result = hasher.finalize();

    format!("{:x}", result)
}

pub struct Genotype {
    pub allele: i64,
    pub phasing: Phasing,
}

pub fn parse_genotype(gt: &str) -> Vec<Option<Genotype>> {
    let mut genotypes = vec![];
    let mut phase = match gt.contains('/') {
        true => Phasing::Unphased,
        false => Phasing::Phased,
    };
    for entry in gt.split_inclusive(['|', '/']) {
        let allele;
        let mut phasing = Phasing::Unphased;
        if entry.ends_with(['/', '|']) {
            let (allele_str, phasing_str) = entry.split_at(entry.len() - 1);
            allele = allele_str;
            phasing = match phasing_str == "|" {
                true => Phasing::Phased,
                false => Phasing::Unphased,
            }
        } else {
            allele = entry;
        }
        if allele == "." {
            genotypes.push(None);
        } else {
            genotypes.push(Some(Genotype {
                allele: allele.parse::<i64>().unwrap(),
                phasing: phase,
            }));
        }
        // we're always 1 behind on phase, e.g. 0|1, the | is the phase of the next allele
        phase = phasing;
    }
    genotypes
}

pub fn get_overlap(a: i64, b: i64, x: i64, y: i64) -> (bool, bool, bool) {
    let contains_start = a <= x && x < b;
    let contains_end = a <= y && y < b;
    let overlap = a < y && x < b;
    (contains_start, contains_end, overlap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::get_connection;

    #[test]
    fn it_hashes() {
        assert_eq!(
            calculate_hash("a test"),
            "a82639b6f8c3a6e536d8cc562c3b86ff4b012c84ab230c1e5be649aa9ad26d21"
        );
    }

    #[test]
    fn it_queries() {
        let conn = get_connection(None);
        let sequence_count: i64 = conn
            .query_row(
                "SELECT count(*) from sequence where hash = 'foo'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sequence_count, 0);
    }

    #[test]
    fn parses_genotype() {
        let genotypes = parse_genotype("1");
        let genotype_1 = genotypes[0].as_ref().unwrap();
        assert_eq!(genotype_1.allele, 1);
        assert_eq!(genotype_1.phasing, Phasing::Phased);
        let genotypes = parse_genotype("0|1");
        let genotype_1 = genotypes[0].as_ref().unwrap();
        let genotype_2 = genotypes[1].as_ref().unwrap();
        assert_eq!(genotype_1.allele, 0);
        assert_eq!(genotype_1.phasing, Phasing::Phased);
        assert_eq!(genotype_2.allele, 1);
        assert_eq!(genotype_2.phasing, Phasing::Phased);
        let genotypes = parse_genotype("0/1");
        let genotype_1 = genotypes[0].as_ref().unwrap();
        let genotype_2 = genotypes[1].as_ref().unwrap();
        assert_eq!(genotype_1.allele, 0);
        assert_eq!(genotype_1.phasing, Phasing::Unphased);
        assert_eq!(genotype_2.allele, 1);
        assert_eq!(genotype_2.phasing, Phasing::Unphased);
        let genotypes = parse_genotype("0/1|2");
        let genotype_1 = genotypes[0].as_ref().unwrap();
        let genotype_2 = genotypes[1].as_ref().unwrap();
        let genotype_3 = genotypes[2].as_ref().unwrap();
        assert_eq!(genotype_1.allele, 0);
        assert_eq!(genotype_1.phasing, Phasing::Unphased);
        assert_eq!(genotype_2.allele, 1);
        assert_eq!(genotype_2.phasing, Phasing::Unphased);
        assert_eq!(genotype_3.allele, 2);
        assert_eq!(genotype_3.phasing, Phasing::Phased);
        let genotypes = parse_genotype("2|1|2");
        let genotype_1 = genotypes[0].as_ref().unwrap();
        let genotype_2 = genotypes[1].as_ref().unwrap();
        let genotype_3 = genotypes[2].as_ref().unwrap();
        assert_eq!(genotype_1.allele, 2);
        assert_eq!(genotype_1.phasing, Phasing::Phased);
        assert_eq!(genotype_2.allele, 1);
        assert_eq!(genotype_2.phasing, Phasing::Phased);
        assert_eq!(genotype_3.allele, 2);
        assert_eq!(genotype_3.phasing, Phasing::Phased);
        let genotypes = parse_genotype("2|.|2");
        let genotype_1 = genotypes[0].as_ref().unwrap();
        let genotype_3 = genotypes[2].as_ref().unwrap();
        assert_eq!(genotype_1.allele, 2);
        assert_eq!(genotype_1.phasing, Phasing::Phased);
        assert_eq!(genotype_3.allele, 2);
        assert_eq!(genotype_3.phasing, Phasing::Phased);
        assert!(genotypes[1].is_none());
    }

    #[test]
    fn test_overlaps() {
        assert_eq!(get_overlap(0, 10, 10, 10), (false, false, false));
        assert_eq!(get_overlap(10, 20, 10, 20), (true, false, true));
        assert_eq!(get_overlap(10, 20, 5, 15), (false, true, true));
        assert_eq!(get_overlap(10, 20, 0, 10), (false, true, false));
    }
}
