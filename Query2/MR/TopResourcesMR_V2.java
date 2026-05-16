package Query2.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import common.sql.Q2DAO;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.*;

public class TopResourcesMR_V2 {

    public static class FastResourceMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t", -1);
            if (parts.length < 8) return;

            // TSV Format: host(0), logDate(1), logHour(2), method(3), resource(4), protocol(5), statusCode(6), bytes(7)
            String host = parts[0];
            String resource = parts[4];
            String bytes = parts[7];

            context.getCounter("LOG", "VALID_RECORDS").increment(1);
            context.write(new Text(resource), new Text(host + "\t" + bytes));
        }
    }

    public static class ResourceReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, String> resultMap = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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

        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<String, String>> list = new ArrayList<>(resultMap.entrySet());
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

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: hadoop jar <jar> <class> <input_tsv> <output> <run_id>");
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();
        int runId = Integer.parseInt(args[2]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Resources (Fast TSV parsing)");

        job.setJarByClass(TopResourcesMR_V2.class);
        job.setMapperClass(FastResourceMapper.class);
        job.setReducerClass(ResourceReducer.class);
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

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("\nQuery 2 Runtime (MR + SQL): " + (endTime - startTime) + " ms");
        System.exit(0);
    }
}
