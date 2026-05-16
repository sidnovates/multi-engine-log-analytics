-- =============================================================================
-- Query 1 : Daily Traffic Summary
-- Pipeline : Hive
-- Output   : log_date, status_code, request_count, total_bytes
--
-- Uses LogParserUDF which internally calls LogParser.java — the SAME parser
-- used by MapReduce and MongoDB pipelines. This ensures equivalent semantics
-- across all four execution choices as required by the project specification.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS nasa_etl;
USE nasa_etl;

-- ── 1. Register the UDF JAR and create the function ──────────────────────────
--    The JAR path is injected by Q1Hive.java via --hivevar udf_jar
ADD JAR ${hivevar:udf_jar};
CREATE TEMPORARY FUNCTION parse_log AS 'Project.Hive.UDF.LogParserUDF';

-- ── 2. Raw staging table (one row = one raw log line) ────────────────────────
DROP TABLE IF EXISTS raw_logs_q1;

CREATE EXTERNAL TABLE raw_logs_q1 (
    line STRING
)
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir}';

-- ── 3. Parsed view using LogParserUDF ────────────────────────────────────────
--    parse_log(line) returns: host\tlogDate\tlogHour\tmethod\tresource\tprotocol\tstatus\tbytes
--    Returns NULL for malformed lines — WHERE clause filters them out,
--    exactly like the null-check in the MR Mapper and MongoDB pipeline.
DROP VIEW IF EXISTS parsed_q1;

CREATE VIEW parsed_q1 AS
SELECT
    split(parse_log(line), '\t')[1]               AS log_date,
    CAST(split(parse_log(line), '\t')[6] AS INT)  AS status_code,
    CAST(split(parse_log(line), '\t')[7] AS BIGINT) AS bytes
FROM raw_logs_q1
WHERE parse_log(line) IS NOT NULL;

-- ── 4. Aggregate: same grouping + aggregation as DailyTrafficMR ──────────────
INSERT OVERWRITE DIRECTORY '${hivevar:output_dir}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    log_date,
    status_code,
    COUNT(*)    AS request_count,
    SUM(bytes)  AS total_bytes
FROM parsed_q1
GROUP BY log_date, status_code;
