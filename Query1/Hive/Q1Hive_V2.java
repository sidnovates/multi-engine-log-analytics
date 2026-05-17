package Query1.Hive;

import common.sql.Q1DAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;

public class Q1Hive_V2 {

    private static final String HQL_SCRIPT  = "Query1/Hive/q1_daily_traffic_v2.hql";
    private static final String HDFS_BASE   = "/user/nasa_etl";

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java Query1.Hive.Q1Hive_V2 <input_dir> <run_id>");
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

        String hdfsOutputDir = HDFS_BASE + "/hive_output/q1_batch_" + runId;

        Path outPath = new Path(hdfsOutputDir);
        if (fs.exists(outPath)) fs.delete(outPath, true);

        System.out.println("Executing Query 1 (Hive V2)...");
        int exitCode = runHiveScript(HQL_SCRIPT, hdfsInputDir, hdfsOutputDir);
        if (exitCode != 0) {
            System.err.println("Hive script failed with exit code: " + exitCode);
        }

        if (fs.exists(outPath)) {
            FileStatus[] parts = fs.listStatus(outPath);
            for (FileStatus status : parts) {
                String name = status.getPath().getName();
                if (!name.startsWith("000") && !name.startsWith("part-")) continue;

                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(fs.open(status.getPath())))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.trim().isEmpty()) continue;

                        String[] p = line.split("\t", -1);
                        if (p.length < 4) continue;

                        String logDate    = p[0].trim();
                        int    statusCode = Integer.parseInt(p[1].trim());
                        long   reqCount   = Long.parseLong(p[2].trim());
                        long   totalBytes = Long.parseLong(p[3].trim());

                        Q1DAO.saveResult(runId, logDate, statusCode, reqCount, totalBytes);
                    }
                }
            }
        }

        if (fs.exists(outPath)) fs.delete(outPath, true);

        long endTime = System.currentTimeMillis();
        System.out.println("\nQuery 1 Runtime (Hive V2 + SQL): " + (endTime - startTime) + " ms");
    }

    private static int runHiveScript(String hqlPath, String inputDir, String outputDir) {
        String cmd = String.format(
            "hive --hivevar input_dir=%s --hivevar output_dir=%s -f %s",
            inputDir, outputDir, hqlPath
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
