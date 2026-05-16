package Query2.MR;

import common.Parsing.LogParser;
import common.Parsing.ParsedLog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import common.sql.MetadataDAO;
import common.sql.Q2DAO;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.*;

public class TopResourcesMR {

    // 🔹 Mapper
    public static class ResourceMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            ParsedLog log = LogParser.parse(value.toString());

            if (log == null) {
                context.getCounter("LOG", "MALFORMED").increment(1);
                return;
            }

            context.getCounter("LOG", "VALID_RECORDS").increment(1);
            // key = resource
            // value = host,bytes
            context.write(new Text(log.resource),
                    new Text(log.host + "\t" + log.bytes));
        }
    }

    // 🔹 Reducer
    public static class ResourceReducer extends Reducer<Text, Text, Text, Text> {

        private Map<String, String> resultMap = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            long totalBytes = 0;
            Set<String> uniqueHosts = new HashSet<>();

            for (Text val : values) {
                String[] parts = val.toString().split("\t");

                String host = parts[0];
                int bytes = Integer.parseInt(parts[1]);

                count++;
                totalBytes += bytes;
                uniqueHosts.add(host);
            }

            String hostsListStr = String.join(",", uniqueHosts);
            String result = count + "\t" + totalBytes + "\t" + uniqueHosts.size() + "\t" + hostsListStr;

            resultMap.put(key.toString(), result);
        }

        // 🔥 sort and print top 20
        protected void cleanup(Context context) throws IOException, InterruptedException {

            List<Map.Entry<String, String>> list = new ArrayList<>(resultMap.entrySet());

            // sort by request count (descending)
            list.sort((a, b) -> {
                int countA = Integer.parseInt(a.getValue().split("\t")[0]);
                int countB = Integer.parseInt(b.getValue().split("\t")[0]);
                return countB - countA;
            });

            int topK = Math.min(20, list.size());

            for (int i = 0; i < topK; i++) {
                Map.Entry<String, String> entry = list.get(i);
                context.write(new Text(entry.getKey()), new Text(entry.getValue()));
            }
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
        Job job = Job.getInstance(conf, "Top Resources");

        job.setJarByClass(TopResourcesMR.class);

        job.setMapperClass(ResourceMapper.class);
        job.setReducerClass(ResourceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1); // needed for global top 20

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
                    String[] parts = line.split("\t", -1);
                    if (parts.length >= 5) { // resource, count, bytes, hosts, hostsList
                        String resource = parts[0];
                        long count = Long.parseLong(parts[1]);
                        long bytes = Long.parseLong(parts[2]);
                        long hosts = Long.parseLong(parts[3]);
                        String hostsList = parts[4];
                        Q2DAO.saveResult(runId, resource, count, bytes, hosts, hostsList);
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
