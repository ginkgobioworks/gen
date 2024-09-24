pub const BOGUS_SOURCE_NODE_ID: i32 = -1;
pub const BOGUS_TARGET_NODE_ID: i32 = -2;

#[derive(Clone, Debug)]
pub struct Node {
    pub id: i32,
    pub sequence_hash: String,
}
