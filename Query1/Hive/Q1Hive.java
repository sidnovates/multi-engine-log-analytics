package Query1.Hive;

import common.sql.MetadataDAO;
import common.sql.Q1DAO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;

/**
 * Q1Hive — Daily Traffic Summary via Apache Hive
 *
 * Uses LogParserUDF so the SAME LogParser.java is called inside Hive,
 * guaranteeing equivalent parsing semantics across all pipelines.
 *
 * Usage:
 *   java Query1.Hive.Q1Hive <batch_file_path> <run_id>
 */
public class Q1Hive {

    private static final String HQL_SCRIPT  = "Query1/Hive/q1_daily_traffic.hql";
    private static final String HDFS_BASE   = "/user/nasa_etl";
    private static final String UDF_JAR = "/home/hitan/DBMS/Project/Hive/UDF/logparser-udf.jar";

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java Query1.Hive.Q1Hive <input_file> <run_id>");
            System.exit(1);
        }
        run(args[0], Integer.parseInt(args[1]));
    }

    public static void run(String filePath, int runId) throws Exception {

        long startTime = System.currentTimeMillis();
        File batchFile = new File(filePath);

        // ── STEP 1: Build the UDF JAR (contains LogParserUDF + LogParser) ────
        System.out.println("Building LogParserUDF JAR...");
        buildUdfJar();

        // ── STEP 2: Upload batch file to HDFS ────────────────────────────────
        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        String hdfsInputDir  = HDFS_BASE + "/input/q1_"  + batchFile.getName();
        String hdfsOutputDir = HDFS_BASE + "/output/q1_" + batchFile.getName();

        Path inPath  = new Path(hdfsInputDir);
        Path outPath = new Path(hdfsOutputDir);

        if (fs.exists(inPath))  fs.delete(inPath,  true);
        if (fs.exists(outPath)) fs.delete(outPath, true);

        fs.mkdirs(inPath);
        fs.copyFromLocalFile(
            new Path(batchFile.getAbsolutePath()),
            new Path(hdfsInputDir + "/" + batchFile.getName())
        );
        System.out.println("Uploaded batch to HDFS: " + hdfsInputDir);

        // ── STEP 3: Count total lines (for malformed calculation) ─────────────
        long totalLines = countLines(batchFile);

        // ── STEP 4: Run Hive script ───────────────────────────────────────────
        System.out.println("Executing Query 1 (Hive)...");
        String absUdfJar = new File(UDF_JAR).getAbsolutePath();
        int exitCode = runHiveScript(HQL_SCRIPT, hdfsInputDir, hdfsOutputDir, absUdfJar);
        if (exitCode != 0) {
            System.err.println("Hive script failed with exit code: " + exitCode);
            System.exit(exitCode);
        }

        // ── STEP 5: Load results from HDFS → PostgreSQL ───────────────────────
        long validCount = 0;
        FileStatus[] parts = fs.listStatus(outPath);
        for (FileStatus status : parts) {
            String name = status.getPath().getName();
            if (!name.startsWith("part-") && !name.startsWith("000")) continue;

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(fs.open(status.getPath())))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;

                    // Format: log_date \t status_code \t request_count \t total_bytes
                    String[] p = line.split("\t", -1);
                    if (p.length < 4) continue;

                    String logDate    = p[0];
                    int    statusCode = Integer.parseInt(p[1].trim());
                    long   reqCount   = Long.parseLong(p[2].trim());
                    long   totalBytes = Long.parseLong(p[3].trim());

                    Q1DAO.saveResult(runId, logDate, statusCode, reqCount, totalBytes);
                    validCount += reqCount;
                }
            }
        }

        // ── STEP 6: Cleanup HDFS ──────────────────────────────────────────────
        if (fs.exists(inPath))  fs.delete(inPath,  true);
        if (fs.exists(outPath)) fs.delete(outPath, true);
        System.out.println("Temporary HDFS directories deleted.");

        // ── STEP 7: Update run_metadata ───────────────────────────────────────
        long   endTime      = System.currentTimeMillis();
        double runtimeSec   = (endTime - startTime) / 1000.0;
        int malformedCount  = (int) Math.max(0, totalLines - validCount);

        MetadataDAO.updateFinalStats(runId, 0.0, malformedCount, 0);

        System.out.println("\nTotal Pipeline Runtime (Hive + SQL): " + (endTime - startTime) + " ms");
        System.out.println("SQL Run ID: " + runId);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Compiles LogParserUDF + LogParser into a single JAR for Hive to load.
     * Only needs to run once per session but is idempotent.
     */
    private static void buildUdfJar() {
        String srcBase  = "/home/hitan/DBMS";
        String hiveExec = "/home/hitan/hive/lib/hive-exec-3.1.3.jar";
        String hadoopCP = runShellOutput("hadoop classpath");

        runShell("mkdir -p /tmp/udf_classes");

        // Compile — use hive-exec JAR directly, NOT hive --service classpath
        runShell("javac -cp \"" + hadoopCP + ":" + hiveExec + "\" " +
                "-d /tmp/udf_classes " +
                srcBase + "/Project/common/Parsing/ParsedLog.java " +
                srcBase + "/Project/common/Parsing/LogParser.java " +
                srcBase + "/Project/Hive/UDF/LogParserUDF.java");

        runShell("mkdir -p " + srcBase + "/Project/Hive/UDF");

        runShell("jar -cf " + UDF_JAR + " -C /tmp/udf_classes .");

        System.out.println("UDF JAR built: " + UDF_JAR);
    }

    // Add this helper — captures shell output as a String
    private static String runShellOutput(String command) {
        try {
            ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
            pb.redirectErrorStream(false);
            Process p = pb.start();
            BufferedReader br = new BufferedReader(
                new InputStreamReader(p.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            p.waitFor();
            return sb.toString().trim();
        } catch (Exception e) {
            System.err.println("Shell output error: " + e.getMessage());
            return "";
        }
    }

    private static int runHiveScript(String hqlPath, String inputDir,
                                     String outputDir, String udfJar) {
        String cmd = String.format(
            "hive --hivevar input_dir=%s --hivevar output_dir=%s --hivevar udf_jar=%s -f %s",
            inputDir, outputDir, udfJar, hqlPath
        );
        return runShell(cmd);
    }

    private static int runShell(String command) {
        try {
            ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
            pb.redirectErrorStream(true);
            pb.redirectInput(ProcessBuilder.Redirect.from(new File("/dev/null")));
            Process p = pb.start();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(p.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            }
            return p.waitFor();
        } catch (Exception e) {
            System.err.println("Shell error: " + e.getMessage());
            return 1;
        }
    }

    private static long countLines(File file) {
        long count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            while (br.readLine() != null) count++;
        } catch (IOException e) {
            System.err.println("Could not count lines: " + e.getMessage());
        }
        return count;
    }
}
