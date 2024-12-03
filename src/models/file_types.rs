use rusqlite::types::{FromSql, FromSqlResult, ToSqlOutput, Value, ValueRef};
use rusqlite::ToSql;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub enum FileTypes {
    GenBank,
    Fasta,
    GFA,
    GAF,
    VCF,
    Changeset,
    CSV,
}

impl ToSql for FileTypes {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let result = match self {
            FileTypes::GenBank => "gb".into(),
            FileTypes::Fasta => "fasta".into(),
            FileTypes::GFA => "gfa".into(),
            FileTypes::VCF => "vcf".into(),
            FileTypes::Changeset => "changeset".into(),
            FileTypes::CSV => "csv".into(),
            FileTypes::GAF => "gaf".into(),
        };
        Ok(result)
    }
}

impl From<FileTypes> for Value {
    fn from(value: FileTypes) -> Value {
        let result = match value {
            FileTypes::GenBank => "gb",
            FileTypes::Fasta => "fasta",
            FileTypes::GFA => "gfa",
            FileTypes::VCF => "vcf",
            FileTypes::Changeset => "changeset",
            FileTypes::CSV => "csv",
            FileTypes::GAF => "gaf",
        };
        Value::Text(result.to_string())
    }
}

impl FromSql for FileTypes {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        let result = match value.as_str() {
            Ok("gb") => FileTypes::GenBank,
            Ok("fasta") => FileTypes::Fasta,
            Ok("gfa") => FileTypes::GFA,
            Ok("vcf") => FileTypes::VCF,
            Ok("changeset") => FileTypes::Changeset,
            Ok("csv") => FileTypes::CSV,
            Ok("gaf") => FileTypes::GAF,
            _ => panic!("Invalid entry in database"),
        };
        Ok(result)
    }
}
