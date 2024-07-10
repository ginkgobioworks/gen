CREATE TABLE collection (
  id INTEGER PRIMARY KEY NOT NULL,
  name TEXT NOT NULL
);

CREATE TABLE sequence (
  id INTEGER PRIMARY KEY NOT NULL,
  hash TEXT NOT NULL,
  type TEXT NOT NULL,
  name TEXT NOT NULL,
  sequence TEXT NOT NULL,
  "length" INTEGER NOT NULL,
  circular INTEGER NOT NULL DEFAULT FALSE
);

CREATE TABLE sequence_collection (
    id INTEGER PRIMARY KEY NOT NULL,
    collection_id INTEGER NOT NULL,
    sequence_id INTEGER NOT NULL,
    FOREIGN KEY(collection_id) REFERENCES collection(id),
    FOREIGN KEY(sequence_id) REFERENCES sequence(id)
);

CREATE TABLE block (
  id INTEGER PRIMARY KEY NOT NULL,
  sequence_collection_id INTEGER NOT NULL,
  "start" INTEGER NOT NULL,
  "end" INTEGER NOT NULL,
  strand TEXT NOT NULL DEFAULT "1",
  FOREIGN KEY(sequence_collection_id) REFERENCES sequence_collection(id),
  constraint chk_strand check (strand in ('-1', '1', '0', '.', '?'))
);

CREATE TABLE edges (
  id INTEGER PRIMARY KEY NOT NULL,
  source_id INTEGER NOT NULL,
  target_id INTEGER NOT NULL,
  FOREIGN KEY(source_id) REFERENCES block(id),
  FOREIGN KEY(target_id) REFERENCES block(id)
);

CREATE TABLE path (
  id INTEGER PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  collection_id INTEGER NOT NULL,
  start_edge_id INTEGER NOT NULL,
  FOREIGN KEY(collection_id) REFERENCES edges(id),
  FOREIGN KEY(start_edge_id) REFERENCES edges(id)
);