package Query1.MR;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import common.sql.MetadataDAO;
import common.sql.Q1DAO;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class DailyTrafficMR_V2 {

    public static class FastLogMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t", -1);
            if (parts.length < 8) return; // Safety skip
            
            // TSV Format: host(0), logDate(1), logHour(2), method(3), resource(4), protocol(5), statusCode(6), bytes(7)
            String logDate = parts[1];
            String statusCode = parts[6];
            String bytes = parts[7];

            context.getCounter("LOG", "VALID_RECORDS").increment(1);

            String keyStr = logDate + "\t" + statusCode;
            String valStr = "1\t" + bytes;

            outKey.set(keyStr);
            outValue.set(valStr);

            context.write(outKey, outValue);
        }
    }

    public static class LogReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: hadoop jar <jar> <class> <input_tsv> <output> <run_id>");
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();
        int runId = Integer.parseInt(args[2]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Daily Traffic (Fast TSV parsing)");

        job.setJarByClass(DailyTrafficMR_V2.class);
        job.setMapperClass(FastLogMapper.class);
        job.setReducerClass(LogReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(outputPath.toUri(), conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        boolean success = job.waitForCompletion(true);
        if (!success) System.exit(1);

        // Load Results from HDFS to SQL
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

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("\nQuery 1 Runtime (MR + SQL): " + (endTime - startTime) + " ms");
        System.exit(0);
    }
}
