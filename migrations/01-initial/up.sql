CREATE TABLE collection (
  name TEXT PRIMARY KEY NOT NULL
);

CREATE TABLE sample (
  name TEXT PRIMARY KEY NOT NULL
);

CREATE TABLE sequence (
  hash TEXT PRIMARY KEY NOT NULL,
  sequence_type TEXT NOT NULL,
  sequence TEXT NOT NULL,
  "length" INTEGER NOT NULL
);

CREATE TABLE block (
  id INTEGER PRIMARY KEY NOT NULL,
  sequence_hash TEXT NOT NULL,
  path_id INTEGER NOT NULL,
  "start" INTEGER NOT NULL,
  "end" INTEGER NOT NULL,
  strand TEXT NOT NULL DEFAULT "1",
  FOREIGN KEY(sequence_hash) REFERENCES sequence(hash),
  FOREIGN KEY(path_id) REFERENCES path(id),
  constraint chk_strand check (strand in ('-1', '1', '0', '.', '?'))
);

CREATE TABLE edges (
  id INTEGER PRIMARY KEY NOT NULL,
  source_id INTEGER NOT NULL,
  target_id INTEGER,
  FOREIGN KEY(source_id) REFERENCES block(id),
  FOREIGN KEY(target_id) REFERENCES block(id)
);

CREATE UNIQUE INDEX edge_uidx ON edges(source_id, target_id);

CREATE TABLE path (
  id INTEGER PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  path_index INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX path_uidx ON path(name, path_index);

CREATE TABLE path_collection (
  id INTEGER PRIMARY KEY NOT NULL,
  collection_name TEXT NOT NULL,
  path_id INTEGER NOT NULL,
  sample_name TEXT,
  FOREIGN KEY(collection_name) REFERENCES collection(name),
  FOREIGN KEY(path_id) REFERENCES path(id)
  FOREIGN KEY(sample_name) REFERENCES sample(name)
);
CREATE UNIQUE INDEX path_collection_uidx ON path_collection(collection_name, sample_name, path_id);