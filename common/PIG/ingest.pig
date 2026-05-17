REGISTER /home/hitan/DBMS/Project/Pig/UDF/logparser-udf.jar;

DEFINE parse_log Pig.UDF.LogParserPigUDF();

-- Load raw text directly
raw_data = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

-- Apply the Java Regex UDF ONCE
parsed_data = FOREACH raw_data GENERATE parse_log(line) AS parsed_tuple;

-- Filter out the malformed records (the UDF returns null for them)
filtered_data = FILTER parsed_data BY parsed_tuple IS NOT NULL;

-- Flatten the output tuple so it writes smoothly as standard TSV columns
final_data = FOREACH filtered_data GENERATE 
    parsed_tuple.$0 AS host,
    parsed_tuple.$1 AS logDate,
    parsed_tuple.$2 AS logHour,
    parsed_tuple.$3 AS method,
    parsed_tuple.$4 AS resource,
    parsed_tuple.$5 AS protocol,
    parsed_tuple.$6 AS statusCode,
    parsed_tuple.$7 AS bytes;

-- Store the clean structured data to HDFS as a TSV file
STORE final_data INTO '$OUTPUT' USING PigStorage('\t');
