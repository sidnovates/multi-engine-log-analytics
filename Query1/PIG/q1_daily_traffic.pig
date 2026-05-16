-- =============================================================================
-- Query 1 : Daily Traffic Summary
-- Pipeline : Apache Pig
-- Output   : log_date, status_code, request_count, total_bytes
--
-- Parsing uses the same master regex as MapReduce / MongoDB / Hive pipelines,
-- guaranteeing equivalent semantics across all four execution engines.
--
-- Run via Q1Pig.java:
--   pig -param INPUT=<hdfs_input_dir> -param OUTPUT=<hdfs_output_dir> -f q1_daily_traffic.pig
-- =============================================================================

-- ── 1. Load raw log lines ─────────────────────────────────────────────────────
raw = LOAD '$INPUT' AS (line:chararray);

-- ── 2. Parse each line using the same regex as the master log parser ──────────
--    Fields extracted (in order, matching ParsedLog.java):
--      host, log_date, log_hour, method, resource, protocol, status_code, bytes
--    Lines that fail any regex extract produce NULL → filtered out below.
parsed = FOREACH raw GENERATE
    REGEX_EXTRACT(line, '^(\\S+)', 1)                             AS host:chararray,
    REGEX_EXTRACT(line, '\\[(\\d{2}/\\w{3}/\\d{4})', 1)           AS log_date:chararray,
    (int)REGEX_EXTRACT(line,
        '\\[(?:\\d{2}/\\w{3}/\\d{4}):(\\d{2})', 1)               AS log_hour:int,
    REGEX_EXTRACT(line, '"(\\S+)', 1)                             AS method:chararray,
    REGEX_EXTRACT(line, '"\\S+\\s+(\\S+)', 1)                     AS resource:chararray,
    REGEX_EXTRACT(line, '(HTTP/[\\d.]+)', 1)                      AS protocol:chararray,
    (int)REGEX_EXTRACT(line, '"\\s(\\d{3})\\s', 1)                AS status_code:int,
    (long)REGEX_EXTRACT(line, '\\s(\\d+)$', 1)                    AS bytes:long;

-- ── 3. Filter malformed lines (NULL status_code or log_date = invalid parse) ──
valid = FILTER parsed BY log_date IS NOT NULL AND status_code IS NOT NULL;

-- ── 4. Replace NULL bytes with 0 (mirrors LogParser.java handling of '-') ─────
cleaned = FOREACH valid GENERATE
    log_date,
    status_code,
    (bytes IS NULL ? 0L : bytes) AS bytes:long;

-- ── 5. Group by (log_date, status_code) — same grouping as DailyTrafficMR ─────
grouped = GROUP cleaned BY (log_date, status_code);

-- ── 6. Aggregate: request_count + total_bytes ─────────────────────────────────
q1 = FOREACH grouped GENERATE
    FLATTEN(group)  AS (log_date:chararray, status_code:int),
    COUNT(cleaned)  AS request_count:long,
    SUM(cleaned.bytes) AS total_bytes:long;

-- ── 7. Store TSV output — column order MUST match Q1DAO.saveResult() ──────────
--    log_date \t status_code \t request_count \t total_bytes
STORE q1 INTO '$OUTPUT' USING PigStorage('\t');
