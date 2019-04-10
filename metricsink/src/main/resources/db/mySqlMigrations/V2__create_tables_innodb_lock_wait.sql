CREATE TABLE transactions
(
    connectionId varchar(100) PRIMARY KEY,
    thread varchar(100),
    query TEXT,
    startTime timestamp,
    waitStartTime timestamp,
    lockMode varchar(100),
    lockType varchar(100),
    lockTable varchar(100),
    lockIndex varchar(100),
    lockData varchar(100)
);

CREATE TABLE lock_waits
(
  connectionId int primary key,
  time timestamp,
  waiting varchar(100),
  blocking varchar(100)
);