use rusqlite::types::{FromSql, FromSqlResult, ToSqlOutput, Value, ValueRef};
use rusqlite::ToSql;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum FileTypes {
    Fasta,
    GFA,
    VCF,
    Changeset,
    CSV,
}

impl ToSql for FileTypes {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let result = match self {
            FileTypes::Fasta => "fasta".into(),
            FileTypes::GFA => "gfa".into(),
            FileTypes::VCF => "vcf".into(),
            FileTypes::Changeset => "changeset".into(),
            FileTypes::CSV => "csv".into(),
        };
        Ok(result)
    }
}

impl From<FileTypes> for Value {
    fn from(value: FileTypes) -> Value {
        let result = match value {
            FileTypes::Fasta => "fasta",
            FileTypes::GFA => "gfa",
            FileTypes::VCF => "vcf",
            FileTypes::Changeset => "changeset",
            FileTypes::CSV => "csv",
        };
        Value::Text(result.to_string())
    }
}

impl FromSql for FileTypes {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        let result = match value.as_str() {
            Ok("fasta") => FileTypes::Fasta,
            Ok("gfa") => FileTypes::GFA,
            Ok("vcf") => FileTypes::VCF,
            Ok("changeset") => FileTypes::Changeset,
            Ok("csv") => FileTypes::CSV,
            _ => panic!("Invalid entry in database"),
        };
        Ok(result)
    }
}
