package Query1.MR;

import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;

import common.Parsing.LogParser;
import common.Parsing.ParsedLog;
import common.sql.MetadataDAO;
import common.sql.Q1DAO;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class DailyTrafficMR {

    // 🔹 Mapper
    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            ParsedLog log = LogParser.parse(line);

            // ✅ Handle malformed logs (IMPORTANT)
            if (log == null) {
                context.getCounter("LOG", "MALFORMED").increment(1);
                return;
            }

            context.getCounter("LOG", "VALID_RECORDS").increment(1);
            // Key: date + status
            String keyStr = log.logDate + "\t" + log.statusCode;
            // Value: count=1, bytes
            String valStr = "1\t" + log.bytes;

            outKey.set(keyStr);
            outValue.set(valStr);

            context.write(outKey, outValue);
        }
    }

    // 🔹 Reducer
    public static class LogReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int totalCount = 0;
            long totalBytes = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\t");

                totalCount += Integer.parseInt(parts[0]);
                totalBytes += Long.parseLong(parts[1]);
            }

            context.write(key, new Text(totalCount + "\t" + totalBytes));
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
        Job job = Job.getInstance(conf, "Daily Traffic (Regex Parsing)");

        job.setJarByClass(DailyTrafficMR.class);

        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
                    String[] parts = line.split("\t");
                    if (parts.length >= 4) { // date, status, count, bytes
                        String date = parts[0];
                        int statusCode = Integer.parseInt(parts[1]);
                        long count = Long.parseLong(parts[2]);
                        long bytes = Long.parseLong(parts[3]);
                        Q1DAO.saveResult(runId, date, statusCode, count, bytes);
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