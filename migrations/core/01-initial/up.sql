CREATE TABLE gen_metadata (
  db_uuid TEXT PRIMARY KEY NOT NULL
) STRICT;

CREATE TABLE collection (
  name TEXT PRIMARY KEY NOT NULL
) STRICT;

CREATE TABLE sample (
  name TEXT PRIMARY KEY NOT NULL
) STRICT;

CREATE TABLE sequence (
  hash TEXT PRIMARY KEY NOT NULL,
  sequence_type TEXT NOT NULL,
  sequence TEXT NOT NULL,
  name TEXT NOT NULL,
  file_path TEXT NOT NULL,
  length INTEGER NOT NULL
) STRICT;

CREATE TABLE nodes (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  sequence_hash TEXT NOT NULL,
  FOREIGN KEY(sequence_hash) REFERENCES sequence(hash)
) STRICT;

CREATE TABLE block_group (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  collection_name TEXT NOT NULL,
  sample_name TEXT,
  name TEXT NOT NULL,
  FOREIGN KEY(collection_name) REFERENCES collection(name),
  FOREIGN KEY(sample_name) REFERENCES sample(name)
) STRICT;
CREATE UNIQUE INDEX block_group_uidx ON block_group(collection_name, sample_name, name) WHERE sample_name is not null;
CREATE UNIQUE INDEX block_group_null_sample_uidx ON block_group(collection_name, name) WHERE sample_name is null;

CREATE TABLE path (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  block_group_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  FOREIGN KEY(block_group_id) REFERENCES block_group(id)
) STRICT;
CREATE UNIQUE INDEX path_uidx ON path(block_group_id, name);

-- an operation from a vcf can impact multiple paths and samples, so operation is not faceted on that
CREATE TABLE operation (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  parent_id INTEGER,
  collection_name TEXT NOT NULL,
  change_type TEXT NOT NULL,
  change_id INTEGER NOT NULL,
  FOREIGN KEY(parent_id) REFERENCES operation(id)
) STRICT;

CREATE TABLE file_addition (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  file_path TEXT NOT NULL,
  file_type TEXT NOT NULL
) STRICT;

CREATE TABLE operation_summary (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  operation_id INTEGER NOT NULL,
  summary TEXT NOT NULL,
  FOREIGN KEY(operation_id) REFERENCES operation(id)
) STRICT;

CREATE TABLE edges (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  source_hash TEXT NOT NULL,
  source_node_id INTEGER,
  source_coordinate INTEGER NOT NULL,
  source_strand TEXT NOT NULL,
  target_hash TEXT NOT NULL,
  target_node_id INTEGER,
  target_coordinate INTEGER NOT NULL,
  target_strand TEXT NOT NULL,
  chromosome_index INTEGER NOT NULL,
  phased INTEGER NOT NULL,
  FOREIGN KEY(source_hash) REFERENCES sequence(hash),
  FOREIGN KEY(target_hash) REFERENCES sequence(hash),
  constraint chk_phased check (phased in (0, 1))
) STRICT;
CREATE UNIQUE INDEX edge_uidx ON edges(source_hash, source_node_id, source_coordinate, source_strand, target_hash, target_node_id, target_coordinate, target_strand, chromosome_index, phased);
-- CREATE UNIQUE INDEX edge_uidx ON edges(source_node_id, source_coordinate, source_strand, target_node_id, target_coordinate, target_strand, chromosome_index, phased);

CREATE TABLE path_edges (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  path_id INTEGER NOT NULL,
  index_in_path INTEGER NOT NULL,
  edge_id INTEGER NOT NULL,
  FOREIGN KEY(edge_id) REFERENCES edges(id),
  FOREIGN KEY(path_id) REFERENCES path(id)
) STRICT;
CREATE UNIQUE INDEX path_edges_uidx ON path_edges(path_id, edge_id, index_in_path);

CREATE TABLE block_group_edges (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  block_group_id INTEGER NOT NULL,
  edge_id INTEGER NOT NULL,
  FOREIGN KEY(block_group_id) REFERENCES block_group(id),
  FOREIGN KEY(edge_id) REFERENCES edges(id)
) STRICT;
CREATE UNIQUE INDEX block_group_edges_uidx ON block_group_edges(block_group_id, edge_id);

INSERT INTO sequence (hash, sequence_type, sequence, name, file_path, "length") values ("start-node-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy", "OTHER", "start-node-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy", "", "", 64), ("end-node-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", "OTHER", "end-node-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", "", "", 64);
INSERT INTO gen_metadata (db_uuid) values (lower(
    hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-' || '4' ||
    substr(hex( randomblob(2)), 2) || '-' ||
    substr('AB89', 1 + (abs(random()) % 4) , 1)  ||
    substr(hex(randomblob(2)), 2) || '-' ||
    hex(randomblob(6))
  ));
INSERT INTO nodes (id, sequence_hash) values (1, "start-node-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy");
INSERT INTO nodes (id, sequence_hash) values (2, "end-node-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");
UPDATE SQLITE_SEQUENCE SET seq = 2 WHERE name = 'nodes';
