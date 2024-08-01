use rusqlite::{params_from_iter, Connection};

#[derive(Debug)]
pub struct Edge {
    pub id: i32,
    pub source_id: i32,
    pub target_id: Option<i32>,
    pub chromosome_index: i32,
    pub phased: i32,
}

impl Edge {
    pub fn create(
        conn: &Connection,
        source_id: i32,
        target_id: Option<i32>,
        chromosome_index: i32,
        phased: i32,
    ) -> Edge {
        let query;
        let id_query;
        let mut placeholders = vec![];
        if target_id.is_some() {
            query = "INSERT INTO edges (source_id, target_id, chromosome_index, phased) VALUES (?1, ?2, ?3, ?4) RETURNING *";
            id_query = "select id from edges where source_id = ?1 and target_id = ?2";
            placeholders.push(source_id);
            placeholders.push(target_id.unwrap());
            placeholders.push(chromosome_index);
            placeholders.push(phased);
        } else {
            id_query = "select id from edges where source_id = ?1 and target_id is null and chromosome_index = ?2 and phased = ?3";
            query = "INSERT INTO edges (source_id, chromosome_index, phased) VALUES (?1, ?2, ?3) RETURNING *";
            placeholders.push(source_id);
            placeholders.push(chromosome_index);
            placeholders.push(phased);
        }
        let mut stmt = conn.prepare(query).unwrap();
        match stmt.query_row(params_from_iter(&placeholders), |row| {
            Ok(Edge {
                id: row.get(0)?,
                source_id: row.get(1)?,
                target_id: row.get(2)?,
                chromosome_index: row.get(3)?,
                phased: row.get(4)?,
            })
        }) {
            Ok(edge) => edge,
            Err(rusqlite::Error::SqliteFailure(err, details)) => {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    println!("{err:?} {details:?}");
                    Edge {
                        id: conn
                            .query_row(id_query, params_from_iter(&placeholders), |row| row.get(0))
                            .unwrap(),
                        source_id,
                        target_id,
                        chromosome_index,
                        phased,
                    }
                } else {
                    panic!("something bad happened querying the database")
                }
            }
            Err(_) => {
                panic!("something bad happened querying the database")
            }
        }
    }

    pub fn bulk_update(conn: &Connection, edges_to_update: Vec<UpdatedEdge>) {
        for edge_to_update in edges_to_update {
            let update_query;
            let mut placeholders: Vec<i32> = vec![];
            if edge_to_update.new_source_id.is_some() && edge_to_update.new_target_id.is_some() {
                update_query = "update edges set source_id = ?1, target_id = ?2 where id = ?3";
                placeholders.push(edge_to_update.new_source_id.unwrap());
                placeholders.push(edge_to_update.new_target_id.unwrap());
            } else if edge_to_update.new_source_id.is_some() {
                update_query = "update edges set source_id = ?1 where id = ?2";
                placeholders.push(edge_to_update.new_source_id.unwrap());
            } else if edge_to_update.new_target_id.is_some() {
                update_query = "update edges set target_id = ?1 where id = ?2";
                placeholders.push(edge_to_update.new_target_id.unwrap());
            } else {
                continue;
            }

            println!("{update_query} {placeholders:?}");

            let edge = Edge::lookup(
                conn,
                edge_to_update.new_source_id,
                edge_to_update.new_target_id,
            );
            if edge.is_none() {
                placeholders.push(edge_to_update.id);
                println!("updating {update_query} {placeholders:?}");
                let mut stmt = conn.prepare_cached(update_query).unwrap();
                stmt.execute(params_from_iter(&placeholders)).unwrap();
            } else {
                println!("edge exists");
            }
        }
    }

    pub fn lookup(
        conn: &Connection,
        source_id: Option<i32>,
        target_id: Option<i32>,
    ) -> Option<Edge> {
        let query;
        let mut stmt;
        let mut it;
        if source_id.is_some() && target_id.is_some() {
            query = "select id, source_id, target_id, chromosome_index, phased from edges where source_id = ?1 and target_id = ?2;";
            stmt = conn.prepare_cached(query).unwrap();
            it = stmt
                .query([source_id.unwrap(), target_id.unwrap()])
                .unwrap();
        } else if source_id.is_some() {
            query = "select id, source_id, target_id, chromosome_index, phased from edges where source_id = ?1 and target_id is null;";
            stmt = conn.prepare_cached(query).unwrap();
            it = stmt.query([source_id.unwrap()]).unwrap();
        } else if target_id.is_some() {
            query = "select id, source_id, target_id, chromosome_index, phased from edges where target_id = ?1 and source_id is null;";
            stmt = conn.prepare_cached(query).unwrap();
            it = stmt.query([target_id.unwrap()]).unwrap();
        } else {
            return None;
        }

        let row = it.next().unwrap();
        if row.is_some() {
            let edge = row.unwrap();
            let source_id: i32 = edge.get(1).unwrap();
            let target_id: Option<i32> = edge.get(2).unwrap();
            Some(Edge {
                id: edge.get(0).unwrap(),
                source_id,
                target_id,
                chromosome_index: edge.get(3).unwrap(),
                phased: edge.get(4).unwrap(),
            })
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct UpdatedEdge {
    pub id: i32,
    pub new_source_id: Option<i32>,
    pub new_target_id: Option<i32>,
}
