-- =============================================================================
-- Query 2 : Top Requested Resources (Top 20)
-- Pipeline : Hive
-- Output   : resource_path, request_count, total_bytes,
--            distinct_host_count, hosts_list
--
-- Uses LogParserUDF → same LogParser.java as MR and MongoDB.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS nasa_etl;
USE nasa_etl;

-- ── 1. Register UDF ───────────────────────────────────────────────────────────
ADD JAR ${hivevar:udf_jar};
CREATE TEMPORARY FUNCTION parse_log AS 'Project.Hive.UDF.LogParserUDF';

-- ── 2. Raw staging table ──────────────────────────────────────────────────────
DROP TABLE IF EXISTS raw_logs_q2;

CREATE EXTERNAL TABLE raw_logs_q2 (
    line STRING
)
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir}';

-- ── 3. Parsed view ────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS parsed_q2;

CREATE VIEW parsed_q2 AS
SELECT
    split(parse_log(line), '\t')[0]                AS host,
    split(parse_log(line), '\t')[4]                AS resource,
    CAST(split(parse_log(line), '\t')[7] AS BIGINT) AS bytes
FROM raw_logs_q2
WHERE parse_log(line) IS NOT NULL;

-- ── 4. Aggregate all resources then take top 20 ───────────────────────────────
--    ORDER BY + LIMIT needs a single reducer for global correctness —
--    same reason TopResourcesMR uses setNumReduceTasks(1) + cleanup().
SET mapreduce.job.reduces=1;

INSERT OVERWRITE DIRECTORY '${hivevar:output_dir}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    resource                              AS resource_path,
    COUNT(*)                              AS request_count,
    SUM(bytes)                            AS total_bytes,
    COUNT(DISTINCT host)                  AS distinct_host_count,
    concat_ws(',', collect_set(host))     AS hosts_list
FROM parsed_q2
GROUP BY resource
ORDER BY request_count DESC
LIMIT 20;
