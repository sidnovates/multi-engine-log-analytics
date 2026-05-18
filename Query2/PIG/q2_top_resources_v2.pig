-- Load Phase 1 TSV
parsed_data = LOAD '$INPUT' USING PigStorage('\t') AS (
    host:chararray, logDate:chararray, logHour:int, method:chararray, 
    resource:chararray, protocol:chararray, statusCode:int, bytes:long
);

grouped_data = GROUP parsed_data BY resource;

resource_stats = FOREACH grouped_data {
    unique_hosts = DISTINCT parsed_data.host;
    GENERATE 
        group AS resource, 
        COUNT(parsed_data) AS requestCount, 
        SUM(parsed_data.bytes) AS totalBytes,
        COUNT(unique_hosts) AS uniqueHosts,
        BagToString(unique_hosts, ',') AS hostsList;
}

ordered_stats = ORDER resource_stats BY requestCount DESC, resource DESC;
top_resources = LIMIT ordered_stats 20;

STORE top_resources INTO '$OUTPUT' USING PigStorage('\t');
