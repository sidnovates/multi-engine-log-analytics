package common.MR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import common.Parsing.LogParser;
import common.Parsing.ParsedLog;
import common.sql.MetadataDAO;

public class DataIngestionMR {

    public static class IngestionMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private Text outValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            ParsedLog log = LogParser.parse(line);

            if (log == null) {
                context.getCounter("LOG", "MALFORMED").increment(1);
                return;
            }

            context.getCounter("LOG", "VALID_RECORDS").increment(1);
            
            // TSV format: host \t logDate \t logHour \t method \t resource \t protocol \t statusCode \t bytes
            String tsvLine = log.host + "\t" + log.logDate + "\t" + log.logHour + "\t" + 
                             log.method + "\t" + log.resource + "\t" + log.protocol + "\t" + 
                             log.statusCode + "\t" + log.bytes;
            
            outValue.set(tsvLine);
            context.write(NullWritable.get(), outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: hadoop jar <jar> <class> <input> <output_dir> <run_id>");
            System.exit(1);
        }

        int runId = Integer.parseInt(args[2]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Ingestion MR (Parse Once)");

        job.setJarByClass(DataIngestionMR.class);
        job.setMapperClass(IngestionMapper.class);
        
        // Map-Only Job (no reducer needed)
        job.setNumReduceTasks(0);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(outputPath.toUri(), conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        boolean success = job.waitForCompletion(true);
        if (!success) {
            System.exit(1);
        }

        // Get Job Metrics
        long malformedRecords = job.getCounters().findCounter("LOG", "MALFORMED").getValue();
        
        // Update SQL with malformed records right after ingestion!
        MetadataDAO.updateFinalStats(runId, 0.0, malformedRecords, 0);
        
        System.out.println("Data Ingestion Complete. TSV Data written to: " + args[1]);
        System.exit(0);
    }
}
