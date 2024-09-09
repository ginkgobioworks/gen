use std::collections::HashMap;
use std::fmt;

use rusqlite::{types::Value, Connection};

use gen::models::change_log::{ChangeLog, ChangeLogSummary, ChangeSet};
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
    let collection = collection.0.name;

    let options = get_change_sets(conn);
    let change_sets = options
        .iter()
        .map(|option| display::ChangeSetDisplay(option.clone()))
        .collect();
    let change_set = Select::new("Select Change Set to View:", change_sets).prompt()?;
    let change_set_id = change_set.0.id.unwrap();

    let changes: Vec<ChangeLog> = get_change_logs(conn, change_set_id);

    println!(
        "{change_set}\n\
{change_count} changes
    ",
        change_count = changes.len()
    );
    for change in changes.iter() {
        println!()
    }

    Ok(())
}

fn get_collections(conn: &Connection) -> Vec<Collection> {
    Collection::query(conn, "select * from collection", vec![])
}

fn get_change_sets(conn: &Connection) -> Vec<ChangeSet> {
    ChangeSet::query(conn, "select * from change_set", vec![])
}

fn get_change_logs(conn: &Connection, change_set_id: i32) -> Vec<ChangeLog> {
    ChangeLog::query(conn, "Select cl.* from change_set_changes csc left join change_log cl on (csc.change_set_id = ?1 and csc.change_log_id = cl.id);", vec![Value::from(change_set_id)])
}
