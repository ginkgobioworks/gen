use crate::models::{
    block_group::BlockGroup, file_types::FileTypes, operations::OperationInfo, sample::Sample,
};
use crate::operation_management;
use rusqlite::Connection;
use std::io;

#[allow(clippy::too_many_arguments)]
pub fn derive_subgraph(
    conn: &Connection,
    operation_conn: &Connection,
    collection_name: &str,
    parent_sample_name: Option<&str>,
    new_sample_name: &str,
    region_name: &str,
    start_coordinate: i64,
    end_coordinate: i64,
) -> io::Result<()> {
    let mut session = operation_management::start_operation(conn);
    let _new_sample = Sample::get_or_create(conn, new_sample_name);
    let block_groups = Sample::get_block_groups(conn, collection_name, parent_sample_name);

    let mut parent_block_group_id = 0;
    let mut new_block_group_id = 0;
    for block_group in block_groups {
        if block_group.name == region_name {
            parent_block_group_id = block_group.id;
            let new_block_group = BlockGroup::create(
                conn,
                collection_name,
                Some(new_sample_name),
                &block_group.name,
            );
            new_block_group_id = new_block_group.id;
        }
    }

    if new_block_group_id == 0 {
        panic!("No region found with name: {}", region_name);
    }

    BlockGroup::clone_subgraph(
        conn,
        parent_block_group_id,
        start_coordinate,
        end_coordinate,
        new_block_group_id,
    );

    let summary_str = format!(" {}: 1 new derived block group", new_sample_name);
    operation_management::end_operation(
        conn,
        operation_conn,
        &mut session,
        OperationInfo {
            file_path: "".to_string(),
            file_type: FileTypes::None,
            description: "derive subgraph".to_string(),
        },
        &summary_str,
        None,
    )
    .unwrap();

    println!("Derived subgraph successfully.");

    Ok(())
}
