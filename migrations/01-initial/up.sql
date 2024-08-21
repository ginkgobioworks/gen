CREATE TABLE collection (
  name TEXT PRIMARY KEY NOT NULL
);

CREATE TABLE sample (
  name TEXT PRIMARY KEY NOT NULL
);

CREATE TABLE sequence (
  hash TEXT PRIMARY KEY NOT NULL,
  sequence_type TEXT NOT NULL,
  sequence TEXT,
  file_path TEXT,
  "length" INTEGER NOT NULL
);

CREATE TABLE block_group (
  id INTEGER PRIMARY KEY NOT NULL,
  collection_name TEXT NOT NULL,
  sample_name TEXT,
  name TEXT NOT NULL,
  FOREIGN KEY(collection_name) REFERENCES collection(name),
  FOREIGN KEY(sample_name) REFERENCES sample(name)
);
CREATE UNIQUE INDEX block_group_uidx ON block_group(collection_name, sample_name, name);

CREATE TABLE block (
  id INTEGER PRIMARY KEY NOT NULL,
  sequence_hash TEXT NOT NULL,
  block_group_id INTEGER NOT NULL,
  "start" INTEGER NOT NULL,
  "end" INTEGER NOT NULL,
  strand TEXT NOT NULL DEFAULT "1",
  FOREIGN KEY(sequence_hash) REFERENCES sequence(hash),
  FOREIGN KEY(block_group_id) REFERENCES block_group(id),
  constraint chk_strand check (strand in ('.', '?', '+', '-'))
);
CREATE UNIQUE INDEX block_uidx ON block(sequence_hash, block_group_id, start, end, strand);

CREATE TABLE edges (
  id INTEGER PRIMARY KEY NOT NULL,
  source_id INTEGER,
  target_id INTEGER,
  chromosome_index INTEGER NOT NULL,
  phased INTEGER NOT NULL,
  FOREIGN KEY(source_id) REFERENCES block(id),
  FOREIGN KEY(target_id) REFERENCES block(id),
  constraint chk_phased check (phased in (0, 1))
);
CREATE UNIQUE INDEX edge_uidx ON edges(source_id, target_id, chromosome_index, phased);

CREATE TABLE path (
  id INTEGER PRIMARY KEY NOT NULL,
  block_group_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  FOREIGN KEY(block_group_id) REFERENCES block_group(id)
);
CREATE UNIQUE INDEX path_uidx ON path(block_group_id, name);

CREATE TABLE path_blocks (
  id INTEGER PRIMARY KEY NOT NULL,
  path_id INTEGER NOT NULL,
  source_block_id INTEGER,
  target_block_id INTEGER,
  FOREIGN KEY(source_block_id) REFERENCES block(id),
  FOREIGN KEY(target_block_id) REFERENCES block(id),
  FOREIGN KEY(path_id) REFERENCES path(id)
);
CREATE UNIQUE INDEX path_blocks_uidx ON path_blocks(path_id, source_block_id, target_block_id);

CREATE TABLE change_log (
  hash TEXT PRIMARY KEY NOT NULL,
  path_id INTEGER NOT NULL,
  path_start INTEGER NOT NULL,
  path_end INTEGER NOT NULL,
  sequence_hash TEXT NOT NULL,
  sequence_start INTEGER NOT NULL,
  sequence_end INTEGER NOT NULL,
  sequence_strand TEXT NOT NULL,
  FOREIGN KEY(path_id) REFERENCES path(id),
  FOREIGN KEY(sequence_hash) REFERENCES sequence(hash)
);
CREATE UNIQUE INDEX change_log_uidx ON change_log(hash);