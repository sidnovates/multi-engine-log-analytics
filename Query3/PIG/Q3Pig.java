package Query3.PIG;

import common.sql.MetadataDAO;
import common.sql.Q3DAO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;

/**
 * Q3Pig — Hourly Error Analysis via Apache Pig
 *
 * Orchestrates the full pipeline for Query 3 using Apache Pig:
 *   1. Uploads the batch file to HDFS
 *   2. Runs the Pig Latin script (q3_hourly_errors.pig) via `pig -f`
 *   3. Reads the TSV output from HDFS
 *   4. Persists results to PostgreSQL via Q3DAO
 *
 * The Pig script uses the same master regex as MapReduce, MongoDB, and Hive,
 * guaranteeing equivalent parsing semantics across all four pipelines.
 *
 * Usage:
 *   java Query3.PIG.Q3Pig <batch_file_path> <run_id>
 */
public class Q3Pig {

    private static final String PIG_SCRIPT  = "Query3/PIG/q3_hourly_errors.pig";
    private static final String HDFS_BASE   = "/user/nasa_etl";

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java Query3.PIG.Q3Pig <input_file> <run_id>");
            System.exit(1);
        }
        run(args[0], Integer.parseInt(args[1]));
    }

    public static void run(String filePath, int runId) throws Exception {

        long startTime = System.currentTimeMillis();
        File batchFile = new File(filePath);

        // ── STEP 1: Upload batch file to HDFS ────────────────────────────────
        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        String hdfsInputDir  = HDFS_BASE + "/pig_input/q3_"  + batchFile.getName();
        String hdfsOutputDir = HDFS_BASE + "/pig_output/q3_" + batchFile.getName();

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

        // ── STEP 2: Count total lines (for malformed calculation) ─────────────
        long totalLines = countLines(batchFile);

        // ── STEP 3: Run Pig script ────────────────────────────────────────────
        System.out.println("Executing Query 3 (Pig)...");
        int exitCode = runPigScript(PIG_SCRIPT, hdfsInputDir, hdfsOutputDir);
        if (exitCode != 0) {
            System.err.println("Pig script failed with exit code: " + exitCode);
        }

        // ── STEP 4: Read results from HDFS → PostgreSQL ───────────────────────
        long totalValidRequests = 0;
        if (fs.exists(outPath)) {
            FileStatus[] parts = fs.listStatus(outPath);
            for (FileStatus status : parts) {
                String name = status.getPath().getName();
                if (!name.startsWith("part-") && !name.startsWith(".")) continue;

                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(fs.open(status.getPath())))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.trim().isEmpty()) continue;

                        // Format: log_date \t log_hour \t error_request_count
                        //         \t total_request_count \t error_rate
                        //         \t distinct_error_hosts \t hosts_list
                        String[] p = line.split("\t", -1);
                        if (p.length < 7) continue;

                        String logDate       = p[0].trim();
                        int    logHour       = Integer.parseInt(p[1].trim());
                        long   errorCount    = Long.parseLong(p[2].trim());
                        long   totalCount    = Long.parseLong(p[3].trim());
                        double errorRate     = Double.parseDouble(p[4].trim());
                        long   distinctHosts = Long.parseLong(p[5].trim());
                        String hostsList     = p[6];

                        Q3DAO.saveResult(runId, logDate, logHour,
                                         errorCount, totalCount, errorRate,
                                         distinctHosts, hostsList);
                        totalValidRequests += totalCount;
                    }
                }
            }
        }

        // ── STEP 5: Cleanup HDFS ──────────────────────────────────────────────
        if (fs.exists(inPath))  fs.delete(inPath,  true);
        if (fs.exists(outPath)) fs.delete(outPath, true);
        System.out.println("Temporary HDFS directories deleted.");

        // ── STEP 6: Update run_metadata (malformed count) ─────────────────────
        int malformedCount = (int) Math.max(0, totalLines - totalValidRequests);
        MetadataDAO.updateFinalStats(runId, 0.0, malformedCount, 0);

        long endTime = System.currentTimeMillis();
        System.out.println("\nTotal Pipeline Runtime (Pig + SQL): " + (endTime - startTime) + " ms");
        System.out.println("SQL Run ID: " + runId);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Runs the Pig Latin script via `pig -f`, passing INPUT and OUTPUT as params.
     */
    private static int runPigScript(String scriptPath, String inputDir, String outputDir) {
        String cmd = String.format(
            "pig -param INPUT=%s -param OUTPUT=%s -f %s",
            inputDir, outputDir, scriptPath
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
