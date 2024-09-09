use rusqlite::Connection;

use crate::change_log_command::display;
use gen::models::change_log::ChangeSet;
use gen::models::Collection;
use inquire::{
    error::{CustomUserError, InquireResult},
    required, CustomType, MultiSelect, Select, Text,
};

pub fn ui(
    conn: &Connection,
    collection_name: &Option<String>,
    author: &Option<String>,
    message: &Option<String>,
) -> InquireResult<()> {
    let collection_name = match collection_name {
        Some(v) => v.to_string(),
        None => {
            let options = get_collections(conn);
            let collection_sets = options
                .iter()
                .map(|option| display::CollectionDisplay(option.clone()))
                .collect();
            let collection = Select::new("Select Collection:", collection_sets).prompt()?;
            collection.0.name
        }
    };

    let author_name = match author {
        Some(v) => v.to_string(),
        None => Text::new("Author Name:").prompt()?,
    };

    let message = match message {
        Some(v) => v.to_string(),
        None => Text::new("Commit message:").prompt()?,
    };

    ChangeSet::new(&collection_name, &author_name, &message).save(conn);

    Ok(())
}

fn get_collections(conn: &Connection) -> Vec<Collection> {
    Collection::query(conn, "select * from collection", vec![])
}
