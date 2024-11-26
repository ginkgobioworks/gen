CREATE TABLE defaults (
  id INTEGER PRIMARY KEY NOT NULL,
  db_name TEXT,
  collection_name TEXT
) STRICT;

CREATE TABLE operation_state (
  db_uuid TEXT PRIMARY KEY NOT NULL,
  operation_id INTEGER,
  branch_id INTEGER,
  FOREIGN KEY(operation_id) REFERENCES operation(id),
  FOREIGN KEY(branch_id) REFERENCES branch(id)
) STRICT;

CREATE TABLE operation (
  id INTEGER PRIMARY KEY NOT NULL,
  db_uuid TEXT NOT NULL,
  hash TEXT NOT NULL,
  parent_id INTEGER,
  branch_id INTEGER NOT NULL,
  collection_name TEXT,
  change_type TEXT NOT NULL,
  change_id INTEGER NOT NULL,
  FOREIGN KEY(parent_id) REFERENCES operation(id)
  FOREIGN KEY(branch_id) REFERENCES branch(id)
) STRICT;
CREATE UNIQUE INDEX operation_uidx ON operation(db_uuid, branch_id, hash);

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

CREATE TABLE branch (
  id INTEGER PRIMARY KEY NOT NULL,
  db_uuid TEXT NOT NULL,
  name TEXT NOT NULL,
  start_operation_id INTEGER,
  current_operation_id INTEGER,
  FOREIGN KEY(start_operation_id) REFERENCES operation(id),
  FOREIGN KEY(current_operation_id) REFERENCES operation(id)
) STRICT;
CREATE UNIQUE INDEX branch_uidx ON branch(db_uuid, name);

CREATE TABLE branch_masked_operations (
  id INTEGER PRIMARY KEY NOT NULL,
  branch_id INTEGER NOT NULL,
  operation_id INTEGER NOT NULL,
  FOREIGN KEY(branch_id) REFERENCES branch(id),
  FOREIGN KEY(operation_id) REFERENCES operation(id)
) STRICT;
CREATE UNIQUE INDEX branch_mask_op_uidx ON branch_masked_operations(branch_id, operation_id);

INSERT INTO defaults values (1, NULL, NULL);
