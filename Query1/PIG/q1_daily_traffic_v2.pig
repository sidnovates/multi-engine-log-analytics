-- Load the pre-parsed TSV data from Phase 1
parsed_data = LOAD '$INPUT' USING PigStorage('\t') AS (
    host:chararray, 
    logDate:chararray, 
    logHour:int, 
    method:chararray, 
    resource:chararray, 
    protocol:chararray, 
    statusCode:int, 
    bytes:long
);

-- Group by date and status
grouped_data = GROUP parsed_data BY (logDate, statusCode);

-- Calculate requests count and total bytes
daily_traffic = FOREACH grouped_data GENERATE 
    group.logDate AS logDate, 
    group.statusCode AS statusCode, 
    COUNT(parsed_data) AS requestCount, 
    SUM(parsed_data.bytes) AS totalBytes;

-- Store the aggregated results natively
STORE daily_traffic INTO '$OUTPUT' USING PigStorage('\t');
