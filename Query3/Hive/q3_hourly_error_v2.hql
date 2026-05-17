-- Parameters: input_dir, output_dir
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

-- Q3 Logic (Native SQL, No UDFs)
INSERT OVERWRITE DIRECTORY '${hivevar:output_dir}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT 
    logDate,
    logHour,
    SUM(IF(statusCode >= 400 AND statusCode <= 599, 1, 0)) AS errorRequests,
    COUNT(*) AS totalRequests,
    (SUM(IF(statusCode >= 400 AND statusCode <= 599, 1, 0)) / COUNT(*)) AS errorRate,
    COUNT(DISTINCT IF(statusCode >= 400 AND statusCode <= 599, host, NULL)) AS errorHosts,
    concat_ws(',', collect_set(IF(statusCode >= 400 AND statusCode <= 599, host, NULL))) AS errorHostsList
FROM parsed_logs
GROUP BY logDate, logHour;
