CREATE TABLE query_attributes (
    digest text,
    digest_hash varchar(100) PRIMARY KEY
);

ALTER TABLE user_queries ADD COLUMN digest_hash varchar(100) DEFAULT null REFERENCES query_attributes(digest_hash);
