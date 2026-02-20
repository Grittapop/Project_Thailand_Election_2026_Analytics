CREATE SCHEMA IF NOT EXISTS iceberg.silver;
CREATE SCHEMA IF NOT EXISTS iceberg.gold;

CREATE TABLE IF NOT EXISTS iceberg.silver.ods_constituency (
    constituency_id VARCHAR,
    constituency_no INTEGER,
    prov_id VARCHAR,
    zone VARCHAR,
    total_vote_stations INTEGER,
    registered_vote INTEGER,
    ingestion_date DATE
)   
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ingestion_date']
);


CREATE TABLE IF NOT EXISTS iceberg.silver.ods_mp_candidate (
    mp_candidate_id VARCHAR,
    candidate_no INTEGER,
    party_id INTEGER,
    candidate_name VARCHAR,
    image_url VARCHAR,
    ingestion_date DATE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ingestion_date']
);


CREATE TABLE IF NOT EXISTS iceberg.silver.ods_party_candidate (
    party_no INTEGER,
    list_no INTEGER,
    candidate_name VARCHAR,
    image_url VARCHAR,
    ingestion_date DATE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ingestion_date']
);


CREATE TABLE IF NOT EXISTS iceberg.silver.ods_party (
    party_id INTEGER,
    party_no INTEGER,
    party_name VARCHAR,
    party_abbr VARCHAR,
    party_color VARCHAR,
    logo_url VARCHAR,
    ingestion_date DATE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ingestion_date']
);


 CREATE TABLE IF NOT EXISTS iceberg.silver.ods_province (
    province_id INTEGER,
    prov_id VARCHAR,
    province VARCHAR,
    abbre_thai VARCHAR,
    eng VARCHAR,
    ingestion_date DATE
)
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['ingestion_date']
);


CREATE TABLE IF NOT EXISTS iceberg.silver.ods_stats_cons (
    constituency_id VARCHAR,
    province_id VARCHAR,
    party_id INTEGER,
    vote INTEGER,
    vote_percent DOUBLE,
    rank INTEGER,
    turn_out INTEGER,
    percent_turn_out DOUBLE,
    valid_votes INTEGER,
    invalid_votes INTEGER,
    blank_votes INTEGER,
    ingestion_date DATE
)
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['ingestion_date']
);


CREATE TABLE IF NOT EXISTS iceberg.silver.ods_stats_party (
    party_id INTEGER,
    party_vote INTEGER,
    party_vote_percent DOUBLE,
    mp_app_vote INTEGER,
    mp_app_vote_percent DOUBLE,
    first_mp_app_count INTEGER,
    counted_vote_stations INTEGER,
    percent_count DOUBLE,
    ingestion_date DATE
)
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['ingestion_date']
);



