use crate::models::block_group::GroupBlock;
use crate::models::edge::Edge;

#[derive(Debug)]
pub enum ChangeType {
    Addition,
    Deletion,
}

#[derive(Debug)]
pub struct DiffBlock {
    source_edge: Edge,
    block: GroupBlock,
    dest_edge: Edge,
    change_type: ChangeType,
}

#[derive(Debug, Default)]
pub struct NewDiffBlock {
    source_edge: Option<Edge>,
    block: Option<GroupBlock>,
    dest_edge: Option<Edge>,
    change_type: Option<ChangeType>,
}

impl NewDiffBlock {
    pub fn new() -> NewDiffBlock {
        NewDiffBlock {
            ..NewDiffBlock::default()
        }
    }

    pub fn source_edge(&mut self, edge: &Edge) -> &mut Self {
        self.source_edge = Some(edge.clone());
        self
    }

    pub fn dest_edge(&mut self, edge: &Edge) -> &mut Self {
        self.dest_edge = Some(edge.clone());
        self
    }

    pub fn block(&mut self, block: &GroupBlock) -> &mut Self {
        self.block = Some(block.clone());
        self
    }

    pub fn change_type(&mut self, value: ChangeType) -> &mut Self {
        self.change_type = Some(value);
        self
    }

    pub fn build(self) -> DiffBlock {
        DiffBlock {
            source_edge: self.source_edge.unwrap(),
            block: self.block.unwrap(),
            dest_edge: self.dest_edge.unwrap(),
            change_type: self.change_type.unwrap(),
        }
    }
}

impl DiffBlock {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> NewDiffBlock {
        NewDiffBlock::new()
    }
}
