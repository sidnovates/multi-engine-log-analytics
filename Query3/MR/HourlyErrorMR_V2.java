package Query3.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import common.sql.Q3DAO;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.*;

public class HourlyErrorMR_V2 {

    public static class FastErrorMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t", -1);
            if (parts.length < 8) return;

            // TSV Format: host(0), logDate(1), logHour(2), method(3), resource(4), protocol(5), statusCode(6), bytes(7)
            String host = parts[0];
            String logDate = parts[1];
            String logHour = parts[2];
            String statusCode = parts[6];

            context.getCounter("LOG", "VALID_RECORDS").increment(1);
            String keyStr = logDate + "\t" + logHour;
            context.write(new Text(keyStr), new Text(statusCode + "\t" + host));
        }
    }

    public static class ErrorReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: hadoop jar <jar> <class> <input_tsv> <output> <run_id>");
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();
        int runId = Integer.parseInt(args[2]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hourly Error Analysis (Fast TSV parsing)");

        job.setJarByClass(HourlyErrorMR_V2.class);
        job.setMapperClass(FastErrorMapper.class);
        job.setReducerClass(ErrorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(outputPath.toUri(), conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        boolean success = job.waitForCompletion(true);
        if (!success) System.exit(1);

        FileStatus[] statuses = fs.listStatus(outputPath);
        for (FileStatus status : statuses) {
            String fileName = status.getPath().getName();
            if (fileName.startsWith("part-")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t", -1);
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

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("\nQuery 3 Runtime (MR + SQL): " + (endTime - startTime) + " ms");
        System.exit(0);
    }
}
