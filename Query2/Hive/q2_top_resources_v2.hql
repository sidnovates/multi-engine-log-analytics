-- Parameters: input_dir, output_dir
SET hive.exec.mode.local.auto=false;
SET mapreduce.task.io.sort.mb=32;
DROP TABLE IF EXISTS parsed_logs;
CREATE EXTERNAL TABLE parsed_logs (
    host STRING,
    logDate STRING,
    logHour INT,
    method STRING,
    resource STRING,
    protocol STRING,
    statusCode INT,
    bytes BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir}';

-- Q2 Logic (Native SQL, No UDFs)
INSERT OVERWRITE DIRECTORY '${hivevar:output_dir}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT 
    resource, 
    COUNT(*) as requestCount, 
    SUM(bytes) as totalBytes,
    COUNT(DISTINCT host) as uniqueHosts,
    concat_ws(',', collect_set(host)) as hostsList
FROM parsed_logs
GROUP BY resource
ORDER BY requestCount DESC, resource DESC
LIMIT 20;
