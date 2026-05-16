package Query2.PIG;

import common.sql.MetadataDAO;
import common.sql.Q2DAO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;

/**
 * Q2Pig — Top Requested Resources via Apache Pig
 *
 * Orchestrates the full pipeline for Query 2 using Apache Pig:
 *   1. Uploads the batch file to HDFS
 *   2. Runs the Pig Latin script (q2_top_resources.pig) via `pig -f`
 *   3. Reads the TSV output from HDFS (top 20 resources by request count)
 *   4. Persists results to PostgreSQL via Q2DAO
 *
 * The Pig script uses the same master regex as MapReduce, MongoDB, and Hive,
 * guaranteeing equivalent parsing semantics across all four pipelines.
 *
 * Usage:
 *   java Query2.PIG.Q2Pig <batch_file_path> <run_id>
 */
public class Q2Pig {

    private static final String PIG_SCRIPT  = "Query2/PIG/q2_top_resources.pig";
    private static final String HDFS_BASE   = "/user/nasa_etl";

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java Query2.PIG.Q2Pig <input_file> <run_id>");
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

        String hdfsInputDir  = HDFS_BASE + "/pig_input/q2_"  + batchFile.getName();
        String hdfsOutputDir = HDFS_BASE + "/pig_output/q2_" + batchFile.getName();

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

        // ── STEP 2: Run Pig script ────────────────────────────────────────────
        System.out.println("Executing Query 2 (Pig)...");
        int exitCode = runPigScript(PIG_SCRIPT, hdfsInputDir, hdfsOutputDir);
        if (exitCode != 0) {
            System.err.println("Pig script failed with exit code: " + exitCode);
        }

        // ── STEP 3: Read results from HDFS → PostgreSQL ───────────────────────
        // Q2 outputs only the top-20 rows; no malformed tracking here
        // (same pattern as Q2Hive.java and TopResourcesMR.java)
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

                        // Format: resource \t request_count \t total_bytes
                        //         \t distinct_host_count \t hosts_list
                        String[] p = line.split("\t", -1);
                        if (p.length < 5) continue;

                        String resource      = p[0].trim();
                        long   requestCount  = Long.parseLong(p[1].trim());
                        long   totalBytes    = Long.parseLong(p[2].trim());
                        long   distinctHosts = Long.parseLong(p[3].trim());
                        String hostsList     = p[4];

                        Q2DAO.saveResult(runId, resource, requestCount, totalBytes,
                                         distinctHosts, hostsList);
                    }
                }
            }
        }

        // ── STEP 4: Cleanup HDFS ──────────────────────────────────────────────
        if (fs.exists(inPath))  fs.delete(inPath,  true);
        if (fs.exists(outPath)) fs.delete(outPath, true);
        System.out.println("Temporary HDFS directories deleted.");

        // ── STEP 5: Update run_metadata ───────────────────────────────────────
        // Q2 only outputs top-20; malformed tracking delegated to Q1/Q3
        MetadataDAO.updateFinalStats(runId, 0.0, 0, 0);

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
}
