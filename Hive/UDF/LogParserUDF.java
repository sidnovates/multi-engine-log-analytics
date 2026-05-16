package Hive.UDF;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import common.Parsing.LogParser;
import common.Parsing.ParsedLog;

/**
 * LogParserUDF
 * ─────────────
 * Wraps the shared LogParser.java so that Hive uses the EXACT same
 * parsing, cleaning, and filtering logic as MapReduce and MongoDB.
 *
 * This satisfies the project requirement:
 *   "the same parsing rules, the same cleaning rules, the same filtering
 *    conditions must be preserved across all implementations."
 *
 * Output format (tab-separated string):
 *   host \t logDate \t logHour \t method \t resource \t protocol \t statusCode \t bytes
 *
 * Returns NULL for malformed lines (same as LogParser returning null),
 * so HQL can filter with: WHERE parse_log(line) IS NOT NULL
 *
 * Registration in HQL:
 *   ADD JAR /path/to/logparser-udf.jar;
 *   CREATE TEMPORARY FUNCTION parse_log AS 'Hive.UDF.LogParserUDF';
 */
public class LogParserUDF extends UDF {

    // Field indices in the returned tab-separated string
    public static final int IDX_HOST       = 0;
    public static final int IDX_LOG_DATE   = 1;
    public static final int IDX_LOG_HOUR   = 2;
    public static final int IDX_METHOD     = 3;
    public static final int IDX_RESOURCE   = 4;
    public static final int IDX_PROTOCOL   = 5;
    public static final int IDX_STATUS     = 6;
    public static final int IDX_BYTES      = 7;

    /**
     * Called once per row by Hive.
     * Delegates entirely to LogParser.parse() — no parsing logic here.
     *
     * @param line  one raw log line from the input file
     * @return      tab-separated parsed fields, or NULL if malformed
     */
    public Text evaluate(Text line) {

        if (line == null) return null;

        // ✅ Use the SAME LogParser as MR and MongoDB — no duplicate logic
        ParsedLog log = LogParser.parse(line.toString());

        // ❌ Malformed record — return NULL so HQL can count/filter it
        if (log == null) return null;

        // ✅ bytes=-1 is already handled by LogParser (- → 0)
        String result = log.host       + "\t" +
                        log.logDate    + "\t" +
                        log.logHour    + "\t" +
                        log.method     + "\t" +
                        log.resource   + "\t" +
                        log.protocol   + "\t" +
                        log.statusCode + "\t" +
                        log.bytes;

        return new Text(result);
    }
}
