drop table transactions;
drop table lock_waits;

CREATE TABLE transactions
(
    id integer primary key,
    database_id varchar(100),
    thread varchar(100),
    query TEXT,
    start_time timestamp,
    wait_start_time timestamp,
    lock_mode varchar(100),
    lock_type varchar(100),
    lock_table varchar(100),
    lock_index varchar(100),
    lock_data varchar(100)
);

CREATE TABLE lock_waits
(
  id integer primary key,
  log_time timestamp,
  waiting_id int REFERENCES transactions(id),
  blocking_id int REFERENCES transactions(id),
  waiting_database_id varchar(100),
  blocking_database_id varchar(100)
);

create table long_txns
(
    id INTEGER PRIMARY KEY,
    log_time TIMESTAMP,
    transaction_id int REFERENCES transactions(id),
    database_id varchar(100)
);

