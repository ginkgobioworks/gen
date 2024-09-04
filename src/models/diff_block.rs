use crate::models::block_group::GroupBlock;
use crate::models::edge::Edge;
use std::ops::Deref;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum ChangeType {
    Addition,
    Deletion,
    #[default]
    Unspecified,
}

#[derive(Debug)]
pub struct DiffBlock {
    pub source_edge: Edge,
    pub block: GroupBlock,
    pub dest_edge: Edge,
    pub change_type: ChangeType,
}

#[derive(Debug, Default)]
pub struct NewDiffBlock {
    pub source_edge: Option<Edge>,
    pub block: Option<GroupBlock>,
    pub dest_edge: Option<Edge>,
    pub change_type: ChangeType,
}
