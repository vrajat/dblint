create table bad_user_queries(
    query_id int,
    user_id int,
    transaction_id int,
    pid int,
    start_time timestamp,
    end_time timestamp,
    duration double,
    database varchar(100),
    aborted int,
    sql text
);