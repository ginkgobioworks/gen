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

CREATE TABLE block_group (
  id INTEGER PRIMARY KEY NOT NULL,
  collection_name TEXT NOT NULL,
  sample_name TEXT,
  name TEXT NOT NULL,
  FOREIGN KEY(collection_name) REFERENCES collection(name),
  FOREIGN KEY(sample_name) REFERENCES sample(name)
) STRICT;
CREATE UNIQUE INDEX block_group_uidx ON block_group(collection_name, sample_name, name) WHERE sample_name is not null;
CREATE UNIQUE INDEX block_group_null_sample_uidx ON block_group(collection_name, name) WHERE sample_name is null;

CREATE TABLE path (
  id INTEGER PRIMARY KEY NOT NULL,
  block_group_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  FOREIGN KEY(block_group_id) REFERENCES block_group(id)
) STRICT;
CREATE UNIQUE INDEX path_uidx ON path(block_group_id, name);

CREATE TABLE change_set (
  id INTEGER PRIMARY KEY NOT NULL,
  collection_name TEXT NOT NULL,
  created INTEGER NOT NULL,
  author TEXT NOT NULL,
  message TEXT NOT NULL,
  FOREIGN KEY(collection_name) REFERENCES collection(name)
) STRICT;

CREATE TABLE change_log (
  id INTEGER PRIMARY KEY NOT NULL,
  path_id INTEGER NOT NULL,
  path_start INTEGER NOT NULL,
  path_end INTEGER NOT NULL,
  sequence_hash TEXT NOT NULL,
  sequence_start INTEGER NOT NULL,
  sequence_end INTEGER NOT NULL,
  sequence_strand TEXT NOT NULL,
  FOREIGN KEY(path_id) REFERENCES path(id),
  FOREIGN KEY(sequence_hash) REFERENCES sequence(hash)
) STRICT;

CREATE TABLE change_set_changes (
  id INTEGER PRIMARY KEY NOT NULL,
  change_set_id INTEGER NOT NULL,
  change_log_id INTEGER NOT NULL,
  FOREIGN KEY(change_set_id) REFERENCES change_set(id),
  FOREIGN KEY(change_log_id) REFERENCES change_log(id)
) STRICT;
CREATE UNIQUE INDEX change_set_changes_uidx ON change_set_changes(change_set_id, change_log_id);

CREATE TABLE edges (
  id INTEGER PRIMARY KEY NOT NULL,
  source_hash TEXT NOT NULL,
  source_coordinate INTEGER NOT NULL,
  source_strand TEXT NOT NULL,
  target_hash TEXT NOT NULL,
  target_coordinate INTEGER NOT NULL,
  target_strand TEXT NOT NULL,
  chromosome_index INTEGER NOT NULL,
  phased INTEGER NOT NULL,
  FOREIGN KEY(source_hash) REFERENCES sequence(hash),
  FOREIGN KEY(target_hash) REFERENCES sequence(hash),
  constraint chk_phased check (phased in (0, 1))
) STRICT;
CREATE UNIQUE INDEX edge_uidx ON edges(source_hash, source_coordinate, source_strand, target_hash, target_coordinate, target_strand, chromosome_index, phased);

CREATE TABLE path_edges (
  id INTEGER PRIMARY KEY NOT NULL,
  path_id INTEGER NOT NULL,
  index_in_path INTEGER NOT NULL,
  edge_id INTEGER NOT NULL,
  FOREIGN KEY(edge_id) REFERENCES edges(id),
  FOREIGN KEY(path_id) REFERENCES path(id)
) STRICT;
CREATE UNIQUE INDEX path_edges_uidx ON path_edges(path_id, edge_id);

CREATE TABLE block_group_edges (
  id INTEGER PRIMARY KEY NOT NULL,
  block_group_id INTEGER NOT NULL,
  edge_id INTEGER NOT NULL,
  FOREIGN KEY(block_group_id) REFERENCES block_group(id),
  FOREIGN KEY(edge_id) REFERENCES edges(id)
) STRICT;
CREATE UNIQUE INDEX block_group_edges_uidx ON block_group_edges(block_group_id, edge_id);

INSERT INTO sequence (hash, sequence_type, sequence, name, file_path, "length") values ("start-node-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy", "OTHER", "start-node-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy", "", "", 64), ("end-node-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", "OTHER", "end-node-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", "", "", 64);
