package Query3.MR;

import common.Parsing.LogParser;
import common.Parsing.ParsedLog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import common.sql.MetadataDAO;
import common.sql.Q3DAO;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.*;

public class HourlyErrorMR {

    // 🔹 Mapper
    public static class ErrorMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            ParsedLog log = LogParser.parse(value.toString());

            if (log == null) {
                context.getCounter("LOG", "MALFORMED").increment(1);
                return;
            }

            context.getCounter("LOG", "VALID_RECORDS").increment(1);
            String keyStr = log.logDate + "\t" + log.logHour;

            // value: status,host
            context.write(new Text(keyStr),
                    new Text(log.statusCode + "\t" + log.host));
        }
    }

    // 🔹 Reducer
    public static class ErrorReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int totalRequests = 0;
            int errorRequests = 0;

            Set<String> errorHosts = new HashSet<>();

            for (Text val : values) {
                String[] parts = val.toString().split("\t");

                int status = Integer.parseInt(parts[0]);
                String host = parts[1];

                totalRequests++;

                if (status >= 400 && status <= 599) {
                    errorRequests++;
                    errorHosts.add(host);
                }
            }

            double errorRate = (totalRequests == 0) ? 0 : (double) errorRequests / totalRequests;

            String hostsListStr = String.join(",", errorHosts);
            String result = errorRequests + "\t" + totalRequests + "\t" +
                    errorRate + "\t" + errorHosts.size() + "\t" + hostsListStr;

            context.write(key, new Text(result));
        }
    }

    // 🔹 Driver
    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.out.println("Usage: hadoop jar <jar> <class> <input> <output> <run_id>");
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();
        int runId = Integer.parseInt(args[2]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hourly Error Analysis");

        job.setJarByClass(HourlyErrorMR.class);

        job.setMapperClass(ErrorMapper.class);
        job.setReducerClass(ErrorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Delete output directory if it exists
        FileSystem fs = FileSystem.get(outputPath.toUri(), conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        boolean success = job.waitForCompletion(true);
        if (!success)
            System.exit(1);

        // 🔷 STEP 1: Get Job Metrics
        long validRecords = job.getCounters().findCounter("LOG", "VALID_RECORDS").getValue();
        long malformedRecords = job.getCounters().findCounter("LOG", "MALFORMED").getValue();

        // 🔷 STEP 2: Load Results from HDFS to SQL
        FileStatus[] statuses = fs.listStatus(outputPath);
        for (FileStatus status : statuses) {
            String fileName = status.getPath().getName();
            if (fileName.startsWith("part-")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t", -1); // -1 prevents discarding empty trailing strings
                    if (parts.length >= 7) { // date, hour, errorCount, totalCount, errorRate, hosts, hostsList
                        String date = parts[0];
                        int hour = Integer.parseInt(parts[1]);
                        long errorCount = Long.parseLong(parts[2]);
                        long totalCount = Long.parseLong(parts[3]);
                        double errorRate = Double.parseDouble(parts[4]);
                        long hosts = Long.parseLong(parts[5]);
                        String hostsList = parts[6];
                        Q3DAO.saveResult(runId, date, hour, errorCount, totalCount, errorRate, hosts, hostsList);
                    }
                }
                br.close();
            }
        }

        // 🔷 STEP 4: Cleanup - Delete temporary output folder
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Temporary output folder deleted.");
        }
        // 🔷 STEP 3: Final Timing and SQL Update
        long endTime = System.currentTimeMillis();
        double totalRuntimeSec = (endTime - startTime) / 1000.0;
        MetadataDAO.updateFinalStats(runId, 0.0, malformedRecords, 0);

        System.out.println("\nTotal Pipeline Runtime (MR + SQL): " + (endTime - startTime) + " ms");
        System.out.println("SQL Run ID: " + runId);

        System.exit(0);
    }
}
