use rusqlite::types::{FromSql, FromSqlResult, ToSqlOutput, Value, ValueRef};
use rusqlite::ToSql;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Strand {
    Forward,
    Reverse,
    Unknown,
    ImportantButUnknown,
}

// example https://docs.rs/rusqlite/latest/rusqlite/types/index.html
impl ToSql for Strand {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let result = match self {
            Strand::Forward => "+".into(),
            Strand::Reverse => "-".into(),
            Strand::Unknown => ".".into(),
            Strand::ImportantButUnknown => "?".into(),
        };
        Ok(result)
    }
}

impl From<Strand> for Value {
    fn from(value: Strand) -> Value {
        let result = match value {
            Strand::Forward => "+",
            Strand::Reverse => "-",
            Strand::Unknown => ".",
            Strand::ImportantButUnknown => "?",
        };
        Value::Text(result.to_string())
    }
}

impl FromSql for Strand {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        let result = match value.as_str() {
            Ok("+") => Strand::Forward,
            Ok("-") => Strand::Reverse,
            Ok(".") => Strand::Unknown,
            Ok("?") => Strand::ImportantButUnknown,
            _ => panic!("Invalid entry in database"),
        };
        Ok(result)
    }
}

impl fmt::Display for Strand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let result = match self {
            Strand::Forward => "+",
            Strand::Reverse => "-",
            Strand::Unknown => ".",
            Strand::ImportantButUnknown => "?",
        };
        formatter.write_str(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_formats() {
        assert_eq!(format!("{v}", v = Strand::Forward), "+");
        assert_eq!(format!("{v}", v = Strand::Reverse), "-");
        assert_eq!(format!("{v}", v = Strand::ImportantButUnknown), "?");
        assert_eq!(format!("{v}", v = Strand::Unknown), ".");
    }

    #[test]
    fn test_column_conversion() {
        assert_eq!(
            Strand::column_result(ValueRef::Text("+".as_bytes())).unwrap(),
            Strand::Forward
        );
        assert_eq!(
            Strand::column_result(ValueRef::Text("-".as_bytes())).unwrap(),
            Strand::Reverse
        );
        assert_eq!(
            Strand::column_result(ValueRef::Text(".".as_bytes())).unwrap(),
            Strand::Unknown
        );
        assert_eq!(
            Strand::column_result(ValueRef::Text("?".as_bytes())).unwrap(),
            Strand::ImportantButUnknown
        );
    }
}
