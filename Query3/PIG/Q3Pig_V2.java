package Query3.PIG;

import common.sql.Q3DAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;

public class Q3Pig_V2 {

    private static final String PIG_SCRIPT  = "Query3/PIG/q3_hourly_error_v2.pig";
    private static final String HDFS_BASE   = "/user/nasa_etl";

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java Query3.PIG.Q3Pig_V2 <input_dir> <run_id>");
            System.exit(1);
        }
        run(args[0], Integer.parseInt(args[1]));
    }

    public static void run(String hdfsInputDir, int runId) throws Exception {
        long startTime = System.currentTimeMillis();
        
        Configuration conf = new Configuration();
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        if (hadoopConfDir == null) {
            String hadoopHome = System.getenv("HADOOP_HOME");
            if (hadoopHome != null) {
                hadoopConfDir = hadoopHome + "/etc/hadoop";
            } else {
                hadoopConfDir = "../hadoop/etc/hadoop";
            }
        }
        File coreSite = new File(hadoopConfDir, "core-site.xml");
        File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
        if (coreSite.exists()) {
            conf.addResource(new Path(coreSite.getAbsolutePath()));
        }
        if (hdfsSite.exists()) {
            conf.addResource(new Path(hdfsSite.getAbsolutePath()));
        }
        FileSystem    fs   = FileSystem.get(conf);

        String hdfsOutputDir = HDFS_BASE + "/pig_output/q3_batch_" + runId;

        Path outPath = new Path(hdfsOutputDir);
        if (fs.exists(outPath)) fs.delete(outPath, true);

        System.out.println("Executing Query 3 (Pig V2)...");
        int exitCode = runPigScript(PIG_SCRIPT, hdfsInputDir, hdfsOutputDir);
        if (exitCode != 0) {
            System.err.println("Pig script failed with exit code: " + exitCode);
        }

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

                        String[] p = line.split("\t", -1);
                        if (p.length < 7) continue;

                        String date = p[0].trim();
                        int    hour = Integer.parseInt(p[1].trim());
                        long   errorReq = Long.parseLong(p[2].trim());
                        long   totalReq = Long.parseLong(p[3].trim());
                        double errorRate = Double.parseDouble(p[4].trim());
                        long   errorHosts = Long.parseLong(p[5].trim());
                        String hostsList = p[6].trim();

                        Q3DAO.saveResult(runId, date, hour, errorReq, totalReq, errorRate, errorHosts, hostsList);
                    }
                }
            }
        }

        if (fs.exists(outPath)) fs.delete(outPath, true);

        long endTime = System.currentTimeMillis();
        System.out.println("\nQuery 3 Runtime (Pig V2 + SQL): " + (endTime - startTime) + " ms");
    }

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
