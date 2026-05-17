package common.PIG;

import common.sql.MetadataDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;

public class DataIngestionPig {

    private static final String PIG_SCRIPT  = "common/PIG/ingest.pig";
    private static final String HDFS_BASE   = "/user/nasa_etl";
    private static final String UDF_JAR     = "Pig/UDF/logparser-udf.jar";

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java common.PIG.DataIngestionPig <input_file> <run_id>");
            System.exit(1);
        }
        run(args[0], Integer.parseInt(args[1]));
    }

    public static void run(String filePath, int runId) throws Exception {
        System.out.println("Starting Phase 1: Pig Ingestion...");
        long startTime = System.currentTimeMillis();
        File batchFile = new File(filePath);

        // ── STEP 1: Upload batch file to HDFS ────────────────────────────────
        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        String hdfsInputDir  = HDFS_BASE + "/pig_input/raw_"  + batchFile.getName();
        // Dynamic output folder for the structured TSV that Q1, Q2, Q3 will read
        String hdfsOutputDir = HDFS_BASE + "/pig_parsed/batch_" + runId;

        Path inPath  = new Path(hdfsInputDir);
        Path outPath = new Path(hdfsOutputDir);

        if (fs.exists(inPath))  fs.delete(inPath,  true);
        if (fs.exists(outPath)) fs.delete(outPath, true);

        fs.mkdirs(inPath);
        fs.copyFromLocalFile(
            new Path(batchFile.getAbsolutePath()),
            new Path(hdfsInputDir + "/" + batchFile.getName())
        );
        System.out.println("Uploaded raw batch to HDFS: " + hdfsInputDir);

        // ── STEP 2: Count total lines locally (for malformed calculation) ─────────────
        long totalLines = countLines(batchFile);

        // ── STEP 3: Run the Pig ingestion script ────────────────────────────────────
        System.out.println("Executing Pig Ingestion Script (Applying UDF Regex)...");
        int exitCode = runPigScript(PIG_SCRIPT, hdfsInputDir, hdfsOutputDir);
        if (exitCode != 0) {
            System.err.println("Pig ingestion script failed with exit code: " + exitCode);
        }

        // ── STEP 4: Count valid parsed rows from the HDFS output ──────────────────
        long validCount = 0;
        if (fs.exists(outPath)) {
            FileStatus[] parts = fs.listStatus(outPath);
            for (FileStatus status : parts) {
                String name = status.getPath().getName();
                if (!name.startsWith("part-") && !name.startsWith(".")) continue;

                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(fs.open(status.getPath())))) {
                    while (br.readLine() != null) {
                        validCount++;
                    }
                }
            }
        }

        // ── STEP 5: Cleanup RAW HDFS INPUT ────────────────────────────────────
        // We delete the raw text folder, but we DO NOT delete the parsed TSV outPath.
        // The V2 queries need it!
        if (fs.exists(inPath))  fs.delete(inPath,  true);
        System.out.println("Temporary raw HDFS input deleted. Parsed TSV is ready for queries.");

        // ── STEP 6: Update SQL run_metadata (malformed count) ─────────────────────
        int malformedCount = (int) Math.max(0, totalLines - validCount);
        MetadataDAO.updateFinalStats(runId, 0.0, malformedCount, 0);

        long endTime = System.currentTimeMillis();
        System.out.println("\nPig Phase 1 Ingestion Runtime: " + (endTime - startTime) + " ms");
        System.out.println("Total Lines: " + totalLines + ", Valid: " + validCount + ", Malformed: " + malformedCount);
    }

    private static int runPigScript(String scriptPath, String inputDir, String outputDir) {
        String absUdfJar = new File(UDF_JAR).getAbsolutePath();
        String cmd = String.format(
            "pig -param INPUT=%s -param OUTPUT=%s -param UDF_JAR=%s -f %s",
            inputDir, outputDir, absUdfJar, scriptPath
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
