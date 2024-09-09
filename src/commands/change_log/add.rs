use rusqlite::{types::Value, Connection};

use gen::models::change_log::{ChangeLog, ChangeSet};
use gen::models::Collection;
use inquire::{
    error::{CustomUserError, InquireResult},
    required, CustomType, MultiSelect, Select, Text,
};

use crate::change_log_command::display;

pub fn ui(conn: &Connection) -> InquireResult<()> {
    let options = get_collections(conn);
    let collection_sets = options
        .iter()
        .map(|option| display::CollectionDisplay(option.clone()))
        .collect();
    let collection = Select::new("Select Collection:", collection_sets).prompt()?;
    let collection_name = collection.0.name;

    let options = get_change_sets(conn, &collection_name);
    let change_sets = options
        .iter()
        .map(|option| display::ChangeSetDisplay(option.clone()))
        .collect();
    let _change_select = Select::new("Change Sets:", change_sets).prompt()?;
    let change_set_id = _change_select.0.id.unwrap();

    let options = get_change_logs(conn, &collection_name);
    let change_logs = options
        .iter()
        .map(|change| display::ChangeSummaryDisplay(ChangeLog::summary(conn, change.id.unwrap())))
        .collect();
    let _change_log_select = Select::new("Latest Changes:", change_logs).prompt()?;
    let change_log_id = _change_log_select.0.id;

    ChangeSet::add_changes(conn, change_set_id, change_log_id);

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
