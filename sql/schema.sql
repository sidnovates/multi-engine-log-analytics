-- PostgreSQL Schema for ETL Project

-- 1. Metadata Table
CREATE TABLE IF NOT EXISTS run_metadata (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(50),
    dataset_name VARCHAR(255),
    avg_batch_size DOUBLE PRECISION,
    total_runtime DOUBLE PRECISION DEFAULT 0.0,
    total_malformed_record_count BIGINT DEFAULT 0,
    total_record_count BIGINT DEFAULT 0,
    execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 1.5 Batch Level Info
CREATE TABLE IF NOT EXISTS batch_metadata (
    run_id INTEGER REFERENCES run_metadata(run_id),
    batch_no INTEGER,
    record_count BIGINT,
    malformed_record_count BIGINT DEFAULT 0,
    batch_runtime DOUBLE PRECISION DEFAULT 0.0,
    PRIMARY KEY (run_id, batch_no)
);

-- 2. Daily Traffic Summary (Query 1)
CREATE TABLE IF NOT EXISTS daily_traffic (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES run_metadata(run_id),
    log_date DATE,
    status_code INTEGER,
    request_count BIGINT,
    total_bytes BIGINT
);

-- 3. Top Requested Resources (Query 2)
CREATE TABLE IF NOT EXISTS top_resources (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES run_metadata(run_id),
    resource_path TEXT,
    request_count BIGINT,
    total_bytes BIGINT,
    distinct_host_count BIGINT,
    hosts_list TEXT
);

-- 4. Hourly Error Analysis (Query 3)
CREATE TABLE IF NOT EXISTS hourly_errors (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES run_metadata(run_id),
    log_date DATE,
    log_hour SMALLINT,
    error_request_count BIGINT,
    total_request_count BIGINT,
    error_rate FLOAT,
    distinct_error_hosts BIGINT,
    hosts_list TEXT
);

-- 5. Query Performance Metrics
CREATE TABLE IF NOT EXISTS query_metadata (
    query_meta_id SERIAL PRIMARY KEY,
    run_id INTEGER,
    batch_no INTEGER,
    FOREIGN KEY (run_id, batch_no) REFERENCES batch_metadata(run_id, batch_no),
    query_number INTEGER,
    query_runtime DOUBLE PRECISION
);
