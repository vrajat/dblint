CREATE TABLE locks (
    connectionId varchar(100) PRIMARY KEY,
    spaceId varchar(100),
    pageNo varchar(100),
    numBits varchar(100),
    indexName varchar(100),
    schemaName varchar(100),
    tableName varchar(100),
    lockType varchar(100)
);

CREATE TABLE deadlocks (
  connectionId varchar(100) PRIMARY KEY,
  query TEXT
);

CREATE TABLE holding_locks (
  deadlock_id varchar(100),
  lock_id varchar(100)
);

CREATE TABLE waiting_locks(
  deadlock_id varchar(100),
  lock_id varchar(100)
);