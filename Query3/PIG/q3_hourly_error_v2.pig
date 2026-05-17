-- Load Phase 1 TSV
parsed_data = LOAD '$INPUT' USING PigStorage('\t') AS (
    host:chararray, logDate:chararray, logHour:int, method:chararray, 
    resource:chararray, protocol:chararray, statusCode:int, bytes:long
);

grouped_data = GROUP parsed_data BY (logDate, logHour);

error_stats = FOREACH grouped_data {
    errors = FILTER parsed_data BY statusCode >= 400 AND statusCode <= 599;
    unique_error_hosts = DISTINCT errors.host;
    GENERATE 
        group.logDate AS logDate, 
        group.logHour AS logHour,
        COUNT(errors) AS errorRequests,
        COUNT(parsed_data) AS totalRequests,
        (COUNT(parsed_data) == 0 ? 0.0 : (double)COUNT(errors) / (double)COUNT(parsed_data)) AS errorRate,
        COUNT(unique_error_hosts) AS errorHosts,
        BagToString(unique_error_hosts, ',') AS errorHostsList;
}

STORE error_stats INTO '$OUTPUT' USING PigStorage('\t');
