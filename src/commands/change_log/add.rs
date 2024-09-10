use rusqlite::{types::Value, Connection};

use crate::change_log_command::display;
use gen::models::change_log::{ChangeLog, ChangeSet};
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

    let options = get_change_sets(conn, &collection_name);
    if options.is_empty() {
        println!("No change sets available, create a change set first.")
    }
    let change_sets = options
        .iter()
        .map(|option| display::ChangeSetDisplay(option.clone()))
        .collect();
    let _change_select = Select::new("Change Sets:", change_sets).prompt()?;
    let change_set_id = _change_select.0.id.unwrap();

    let options = get_operations(conn, &collection_name);
    let operations = options
        .iter()
        .map(|option| display::OperationSummaryDisplay(option.clone()))
        .collect();
    let _operation_select = Select::new("Operations:", operations).prompt()?;
    let operation_id = _operation_select.0.operation_id;

    ChangeSet::add_operation(conn, change_set_id, operation_id);

    Ok(())
}

fn get_collections(conn: &Connection) -> Vec<Collection> {
    Collection::query(conn, "select * from collection", vec![])
}

fn get_change_sets(conn: &Connection, collection_name: &str) -> Vec<ChangeSet> {
    ChangeSet::query(
        conn,
        "select * from change_set where collection_name = ?1; ",
        vec![Value::from(collection_name.to_string())],
    )
}

fn get_change_logs(conn: &Connection, collection_name: &str) -> Vec<ChangeLog> {
    ChangeLog::query(conn, "Select cl.* from change_log cl inner join path p on (p.id = cl.path_id) inner join block_group bg on (bg.id = p.block_group_id) where bg.collection_name = ?1 limit 10;", vec![Value::from(collection_name.to_string())])
}

fn get_operations(conn: &Connection, collection_name: &str) -> Vec<OperationSummary> {
    OperationSummary::query(conn, "Select os.* from operation op left join operation_summary os on (os.operation_id = op.id) where op.collection_name = ?1", vec![Value::from(collection_name.to_string())])
}
