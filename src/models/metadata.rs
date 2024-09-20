use rusqlite::Connection;

#[derive(Debug)]
pub struct Metadata {
    pub db_uuid: String,
}

pub fn get_db_uuid(conn: &Connection) -> String {
    let mut stmt = conn
        .prepare_cached("select db_uuid from gen_metadata;")
        .unwrap();
    let rows = stmt.query_map([], |row| row.get(0)).unwrap();

    let mut entries: Vec<String> = Vec::new();
    for entry in rows {
        entries.push(entry.unwrap());
    }
    let uuid = entries.first().unwrap().clone();
    uuid
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    use crate::test_helpers::get_connection;

    #[test]
    fn test_sets_uuid() {
        let conn = get_connection(None);
        assert!(!get_db_uuid(&conn).is_empty());
    }
}
