package Query1.PIG;

import common.sql.MetadataDAO;
import common.sql.Q1DAO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;

/**
 * Q1Pig — Daily Traffic Summary via Apache Pig
 *
 * Orchestrates the full pipeline for Query 1 using Apache Pig:
 *   1. Uploads the batch file to HDFS
 *   2. Runs the Pig Latin script (q1_daily_traffic.pig) via `pig -f`
 *   3. Reads the TSV output from HDFS
 *   4. Persists results to PostgreSQL via Q1DAO
 *
 * The Pig script uses the same master regex as MapReduce, MongoDB, and Hive,
 * guaranteeing equivalent parsing semantics across all four pipelines.
 *
 * Usage:
 *   java Query1.PIG.Q1Pig <batch_file_path> <run_id>
 */
public class Q1Pig {

    private static final String PIG_SCRIPT  = "Query1/PIG/q1_daily_traffic.pig";
    private static final String HDFS_BASE   = "/user/nasa_etl";

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java Query1.PIG.Q1Pig <input_file> <run_id>");
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

        String hdfsInputDir  = HDFS_BASE + "/pig_input/q1_"  + batchFile.getName();
        String hdfsOutputDir = HDFS_BASE + "/pig_output/q1_" + batchFile.getName();

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
        System.out.println("Executing Query 1 (Pig)...");
        int exitCode = runPigScript(PIG_SCRIPT, hdfsInputDir, hdfsOutputDir);
        if (exitCode != 0) {
            System.err.println("Pig script failed with exit code: " + exitCode);
            // Don't exit — allow pipeline to continue with other queries
        }

        // ── STEP 4: Read results from HDFS → PostgreSQL ───────────────────────
        long validCount = 0;
        if (fs.exists(outPath)) {
            FileStatus[] parts = fs.listStatus(outPath);
            for (FileStatus status : parts) {
                String name = status.getPath().getName();
                // Pig outputs part-* files
                if (!name.startsWith("part-") && !name.startsWith(".")) continue;

                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(fs.open(status.getPath())))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.trim().isEmpty()) continue;

                        // Format: log_date \t status_code \t request_count \t total_bytes
                        String[] p = line.split("\t", -1);
                        if (p.length < 4) continue;

                        String logDate    = p[0].trim();
                        int    statusCode = Integer.parseInt(p[1].trim());
                        long   reqCount   = Long.parseLong(p[2].trim());
                        long   totalBytes = Long.parseLong(p[3].trim());

                        Q1DAO.saveResult(runId, logDate, statusCode, reqCount, totalBytes);
                        validCount += reqCount;
                    }
                }
            }
        }

        // ── STEP 5: Cleanup HDFS ──────────────────────────────────────────────
        if (fs.exists(inPath))  fs.delete(inPath,  true);
        if (fs.exists(outPath)) fs.delete(outPath, true);
        System.out.println("Temporary HDFS directories deleted.");

        // ── STEP 6: Update run_metadata (malformed count) ─────────────────────
        int malformedCount = (int) Math.max(0, totalLines - validCount);
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
