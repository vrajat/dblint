CREATE  TABLE user_queries (
  id INTEGER PRIMARY KEY,
  connection_id varchar(100),
  log_time timestamp,
  user_host varchar(100),
  ip_address varchar(100),
  query_time double,
  lock_time double,
  rows_sent double,
  rows_examined double,
  query TEXT
)