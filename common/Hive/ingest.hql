-- Parameters: input_dir, output_dir, udf_jar
SET hive.exec.mode.local.auto=false;
SET mapreduce.task.io.sort.mb=32;
ADD JAR ${hivevar:udf_jar};
CREATE TEMPORARY FUNCTION parse_log AS 'Hive.UDF.LogParserUDF';

-- Raw text table
DROP TABLE IF EXISTS raw_logs;
CREATE EXTERNAL TABLE raw_logs (line STRING)
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir}';

-- Parsed table (Temporary output location as TSV)
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
LOCATION '${hivevar:output_dir}';

-- Ingestion query: apply the UDF exactly once and split the resulting tab-separated string into columns
INSERT OVERWRITE TABLE parsed_logs
SELECT 
    split(parsed, '\t')[0] AS host,
    split(parsed, '\t')[1] AS logDate,
    CAST(split(parsed, '\t')[2] AS INT) AS logHour,
    split(parsed, '\t')[3] AS method,
    split(parsed, '\t')[4] AS resource,
    split(parsed, '\t')[5] AS protocol,
    CAST(split(parsed, '\t')[6] AS INT) AS statusCode,
    CAST(split(parsed, '\t')[7] AS BIGINT) AS bytes
FROM (
    SELECT parse_log(line) as parsed
    FROM raw_logs
    WHERE parse_log(line) IS NOT NULL
) temp;
