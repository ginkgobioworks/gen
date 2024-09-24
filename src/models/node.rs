use rusqlite::Connection;

pub const BOGUS_SOURCE_NODE_ID: i32 = -1;
pub const BOGUS_TARGET_NODE_ID: i32 = -2;

pub const PATH_START_NODE_ID: i32 = 1;
pub const PATH_END_NODE_ID: i32 = 2;

#[derive(Clone, Debug)]
pub struct Node<'a> {
    pub id: i32,
    pub sequence_hash: &'a str,
}

impl Node<'_> {
    pub fn create<'a>(conn: &'a Connection, sequence_hash: &'a str) -> Node<'a> {
        let insert_statement = format!(
            "INSERT INTO nodes (sequence_hash) VALUES ('{}');",
            sequence_hash
        );
        let _ = conn.execute(&insert_statement, ());
        Node {
            id: conn.last_insert_rowid() as i32,
            sequence_hash,
        }
    }
}
