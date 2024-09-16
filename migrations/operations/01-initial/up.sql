CREATE TABLE operation_state (
  db_uuid TEXT PRIMARY KEY NOT NULL,
  operation_id INTEGER,
  FOREIGN KEY(operation_id) REFERENCES operation(id)
) STRICT;

CREATE TABLE operation (
  id INTEGER PRIMARY KEY NOT NULL,
  db_uuid TEXT NOT NULL,
  parent_id INTEGER,
  collection_name TEXT NOT NULL,
  change_type TEXT NOT NULL,
  change_id INTEGER NOT NULL,
  FOREIGN KEY(parent_id) REFERENCES operation(id)
) STRICT;

CREATE TABLE file_addition (
  id INTEGER PRIMARY KEY NOT NULL,
  file_path TEXT NOT NULL,
  file_type TEXT NOT NULL
) STRICT;

CREATE TABLE operation_summary (
  id INTEGER PRIMARY KEY NOT NULL,
  operation_id INTEGER NOT NULL,
  summary TEXT NOT NULL,
  FOREIGN KEY(operation_id) REFERENCES operation(id)
) STRICT;
