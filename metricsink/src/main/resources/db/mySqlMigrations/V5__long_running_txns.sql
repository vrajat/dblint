drop table transactions;
drop table lock_waits;

CREATE TABLE transactions
(
    id varchar(100) PRIMARY KEY,
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
  id int primary key,
  log_time timestamp,
  waiting_id varchar(100) REFERENCES transactions(id),
  blocking_id varchar(100) REFERENCES transactions(id)
);

create table long_txns
(
    id INT PRIMARY KEY,
    log_time TIMESTAMP,
    transaction_id varchar(100) REFERENCES transactions(id)
);

