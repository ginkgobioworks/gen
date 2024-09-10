use std::collections::HashMap;
use std::fmt;

use rusqlite::{types::Value, Connection};

use crate::change_log_command::display;
use gen::models::change_log::{ChangeLog, ChangeLogSummary, ChangeSet};
use gen::models::operations::OperationSummary;
use gen::models::Collection;
use inquire::{
    error::{CustomUserError, InquireResult},
    required, CustomType, MultiSelect, Select, Text,
};

pub fn ui(conn: &Connection) -> InquireResult<()> {
    let options = get_collections(conn);
    let collection_sets = options
        .iter()
        .map(|option| display::CollectionDisplay(option.clone()))
        .collect();
    let collection = Select::new("Select Collection:", collection_sets).prompt()?;
    let collection_name = collection.0.name;

    let options = get_change_sets(conn, collection_name);
    let change_sets = options
        .iter()
        .map(|option| display::ChangeSetDisplay(option.clone()))
        .collect();
    let change_set = Select::new("Select Change Set to View:", change_sets).prompt()?;
    let change_set_id = change_set.0.id.unwrap();

    let changes: Vec<OperationSummary> = get_operation_summary(conn, change_set_id);

    println!(
        "{change_set}\n\
{change_count} operations
    ",
        change_count = changes.len()
    );
    for change in changes.iter() {
        println!("{summary}", summary = change.summary);
    }

    Ok(())
}

fn get_collections(conn: &Connection) -> Vec<Collection> {
    Collection::query(conn, "select * from collection", vec![])
}

fn get_change_sets(conn: &Connection, collection_name: String) -> Vec<ChangeSet> {
    ChangeSet::query(
        conn,
        "select * from change_set where collection_name = ?1",
        vec![Value::from(collection_name)],
    )
}

fn get_operation_summary(conn: &Connection, change_set_id: i32) -> Vec<OperationSummary> {
    OperationSummary::query(conn, "Select os.* from change_set_operations cso left join operation op on (op.id = cso.operation_id AND cso.change_set_id = ?1) left join operation_summary os on (os.operation_id = op.id);", vec![Value::from(change_set_id)])
}
