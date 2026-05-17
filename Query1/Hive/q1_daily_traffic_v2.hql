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

-- Q1 Logic (Native SQL, No UDFs)
INSERT OVERWRITE DIRECTORY '${hivevar:output_dir}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT logDate, statusCode, COUNT(*) as requestCount, SUM(bytes) as totalBytes
FROM parsed_logs
GROUP BY logDate, statusCode;
