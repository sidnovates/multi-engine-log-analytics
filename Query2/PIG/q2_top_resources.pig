-- =============================================================================
-- Query 2 : Top 20 Requested Resources
-- Pipeline : Apache Pig
-- Output   : resource, request_count, total_bytes, distinct_host_count, hosts_list
--
-- Mirrors TopResourcesMR.java and q2_top_resources.hql semantics:
--   • Groups by resource path
--   • Orders by request_count DESC, takes top 20
--   • Collects distinct requesting hosts
--
-- Run via Q2Pig.java:
--   pig -param INPUT=<hdfs_input_dir> -param OUTPUT=<hdfs_output_dir> -f q2_top_resources.pig
-- =============================================================================

-- ── 1. Load raw log lines ─────────────────────────────────────────────────────
raw = LOAD '$INPUT' AS (line:chararray);

-- ── 2. Parse each line using the same master regex ────────────────────────────
parsed = FOREACH raw GENERATE
    REGEX_EXTRACT(line, '^(\\S+)', 1)                             AS host:chararray,
    REGEX_EXTRACT(line, '"\\S+\\s+(\\S+)', 1)                     AS resource:chararray,
    (int)REGEX_EXTRACT(line, '"\\s(\\d{3})\\s', 1)                AS status_code:int,
    (long)REGEX_EXTRACT(line, '\\s(\\d+)$', 1)                    AS bytes:long;

-- ── 3. Filter malformed lines ─────────────────────────────────────────────────
valid = FILTER parsed BY resource IS NOT NULL AND host IS NOT NULL;

-- ── 4. Replace NULL bytes with 0 ──────────────────────────────────────────────
cleaned = FOREACH valid GENERATE
    host,
    resource,
    (bytes IS NULL ? 0L : bytes) AS bytes:long;

-- ── 5. Group by resource — same as TopResourcesMR mapper key ─────────────────
grouped = GROUP cleaned BY resource;

-- ── 6. Aggregate per resource ─────────────────────────────────────────────────
aggregated = FOREACH grouped {
    distinct_hosts = DISTINCT cleaned.host;
    GENERATE
        group                       AS resource:chararray,
        COUNT(cleaned)              AS request_count:long,
        SUM(cleaned.bytes)          AS total_bytes:long,
        COUNT(distinct_hosts)       AS distinct_host_count:long,
        BagToString(distinct_hosts, ',') AS hosts_list:chararray;
};

-- ── 7. Sort descending by request_count — same as TopResourcesMR.cleanup() ───
ordered = ORDER aggregated BY request_count DESC, resource DESC;

-- ── 8. Take top 20 — single reducer needed for global correctness ─────────────
top20 = LIMIT ordered 20;

-- ── 9. Store TSV — column order MUST match Q2DAO.saveResult() ────────────────
--    resource \t request_count \t total_bytes \t distinct_host_count \t hosts_list
STORE top20 INTO '$OUTPUT' USING PigStorage('\t');
