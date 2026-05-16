-- =============================================================================
-- Query 3 : Hourly Error Analysis
-- Pipeline : Hive
-- Output   : log_date, log_hour, error_request_count, total_request_count,
--            error_rate, distinct_error_hosts, hosts_list
--
-- Uses LogParserUDF → same LogParser.java as MR and MongoDB.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS nasa_etl;
USE nasa_etl;

-- ── 1. Register UDF ───────────────────────────────────────────────────────────
ADD JAR ${hivevar:udf_jar};
CREATE TEMPORARY FUNCTION parse_log AS 'Project.Hive.UDF.LogParserUDF';

-- ── 2. Raw staging table ──────────────────────────────────────────────────────
DROP TABLE IF EXISTS raw_logs_q3;

CREATE EXTERNAL TABLE raw_logs_q3 (
    line STRING
)
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir}';

-- ── 3. Parsed view ────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS parsed_q3;

CREATE VIEW parsed_q3 AS
SELECT
    split(parse_log(line), '\t')[0]               AS host,
    split(parse_log(line), '\t')[1]               AS log_date,
    CAST(split(parse_log(line), '\t')[2] AS INT)  AS log_hour,
    CAST(split(parse_log(line), '\t')[6] AS INT)  AS status_code
FROM raw_logs_q3
WHERE parse_log(line) IS NOT NULL;

-- ── 4. Tag each row as error or not ──────────────────────────────────────────
--    Same condition as HourlyErrorMR: status >= 400 AND status <= 599
DROP TABLE IF EXISTS q3_tagged;

CREATE TABLE q3_tagged AS
SELECT
    log_date,
    log_hour,
    host,
    status_code,
    CASE WHEN status_code >= 400 AND status_code <= 599 THEN 1 ELSE 0 END AS is_error
FROM parsed_q3;

-- ── 5. Totals per (date, hour) ────────────────────────────────────────────────
DROP TABLE IF EXISTS q3_totals;

CREATE TABLE q3_totals AS
SELECT
    log_date,
    log_hour,
    SUM(is_error) AS error_request_count,
    COUNT(*)      AS total_request_count
FROM q3_tagged
GROUP BY log_date, log_hour;

-- ── 6. Distinct error hosts per (date, hour) ──────────────────────────────────
DROP TABLE IF EXISTS q3_error_hosts;

CREATE TABLE q3_error_hosts AS
SELECT
    log_date,
    log_hour,
    COUNT(DISTINCT host)              AS distinct_error_hosts,
    concat_ws(',', collect_set(host)) AS hosts_list
FROM q3_tagged
WHERE is_error = 1
GROUP BY log_date, log_hour;

-- ── 7. Final join + error rate ────────────────────────────────────────────────
INSERT OVERWRITE DIRECTORY '${hivevar:output_dir}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    t.log_date,
    t.log_hour,
    t.error_request_count,
    t.total_request_count,
    CASE
        WHEN t.total_request_count = 0 THEN 0.0
        ELSE CAST(t.error_request_count AS DOUBLE) / t.total_request_count
    END                                       AS error_rate,
    COALESCE(h.distinct_error_hosts, 0)       AS distinct_error_hosts,
    COALESCE(h.hosts_list, '')                AS hosts_list
FROM q3_totals t
LEFT OUTER JOIN q3_error_hosts h
  ON t.log_date = h.log_date AND t.log_hour = h.log_hour;
