-- =============================================================================
-- Query 3 : Hourly Error Analysis
-- Pipeline : Apache Pig
-- Output   : log_date, log_hour, error_request_count, total_request_count,
--            error_rate, distinct_error_hosts, hosts_list
--
-- Mirrors HourlyErrorMR.java and q3_hourly_errors.hql semantics:
--   • Groups by (log_date, log_hour)
--   • Counts error requests (status_code >= 400 AND <= 599)
--   • Computes error_rate = error_count / total_count
--   • Collects distinct hosts that made error requests
--
-- Run via Q3Pig.java:
--   pig -param INPUT=<hdfs_input_dir> -param OUTPUT=<hdfs_output_dir> -f q3_hourly_errors.pig
-- =============================================================================

-- ── 1. Load raw log lines ─────────────────────────────────────────────────────
raw = LOAD '$INPUT' AS (line:chararray);

-- ── 2. Parse each line using the same master regex ────────────────────────────
parsed = FOREACH raw GENERATE
    REGEX_EXTRACT(line, '^(\\S+)', 1)                              AS host:chararray,
    REGEX_EXTRACT(line, '\\[(\\d{2}/\\w{3}/\\d{4})', 1)            AS log_date:chararray,
    (int)REGEX_EXTRACT(line,
        '\\[(?:\\d{2}/\\w{3}/\\d{4}):(\\d{2})', 1)                AS log_hour:int,
    (int)REGEX_EXTRACT(line, '"\\s(\\d{3})\\s', 1)                 AS status_code:int;

-- ── 3. Filter malformed lines ─────────────────────────────────────────────────
valid = FILTER parsed BY log_date IS NOT NULL AND log_hour IS NOT NULL
                     AND status_code IS NOT NULL AND host IS NOT NULL;

-- ── 4. Tag each row: is_error = 1 when status >= 400 AND <= 599 ──────────────
--    Same condition as HourlyErrorMR reducer and q3_hourly_errors.hql
tagged = FOREACH valid GENERATE
    log_date,
    log_hour,
    host,
    status_code,
    (status_code >= 400 AND status_code <= 599 ? 1 : 0) AS is_error:int;

-- ── 5. Group by (log_date, log_hour) — same key as HourlyErrorMR mapper ──────
grouped = GROUP tagged BY (log_date, log_hour);

-- ── 6. Compute aggregates per (date, hour) ────────────────────────────────────
q3 = FOREACH grouped {
    -- All records for this (date, hour) bucket
    total_count  = COUNT(tagged);

    -- Only error rows
    error_rows   = FILTER tagged BY is_error == 1;
    error_count  = COUNT(error_rows);

    -- Distinct hosts that produced errors
    distinct_err_hosts = DISTINCT error_rows.host;

    GENERATE
        FLATTEN(group)                              AS (log_date:chararray, log_hour:int),
        (long)error_count                           AS error_request_count:long,
        (long)total_count                           AS total_request_count:long,
        -- error_rate: guard against division by zero (mirrors MR + Hive logic)
        (total_count == 0L ? 0.0
            : (double)error_count / (double)total_count) AS error_rate:double,
        (long)COUNT(distinct_err_hosts)             AS distinct_error_hosts:long,
        BagToString(distinct_err_hosts, ',')        AS hosts_list:chararray;
};

-- ── 7. Store TSV — column order MUST match Q3DAO.saveResult() ────────────────
--    log_date \t log_hour \t error_count \t total_count \t error_rate
--    \t distinct_hosts \t hosts_list
STORE q3 INTO '$OUTPUT' USING PigStorage('\t');
