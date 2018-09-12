CREATE TABLE query_stats (
    db varchar,
    user varchar,
    query_group varchar,
    day timestamp,
    min_duration float,
    avg_duration float,
    median_duration float,
    p75_duration float,
    p90_duration float,
    p95_duration float,
    p99_duration float,
    p999_duration float,
    max_duration float
);