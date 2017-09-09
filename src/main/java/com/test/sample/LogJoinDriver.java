package com.test.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LogJoinDriver {

    public int execute(Path inputPath, Path outputPath) {
        Configuration conf = new Configuration();

        boolean success = false;
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.delete(outputPath, true);
            Job job = Job.getInstance(conf);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, inputPath);
            job.setMapperClass(LogJoinMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

            job.setMapOutputValueClass(NullWritable.class);   /* new */
//			job.setGroupingComparatorClass(LogJoinGroupingComparator.class);
//			job.setPartitionerClass(LogJoinPartitioner.class);
            job.setReducerClass(LogJoinReducer.class);
            job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
            /* Set Tasks */
            job.setNumReduceTasks(2);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setJarByClass(job.getMapperClass());
            success = job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
            success=false;
        }
        if(success) {
            System.out.println("Job finished succesfully");
            return 0;
        } else {
            System.err.println("Job failed");
            return -1;
        }

    }


    public static void main(String args[]) {
        Path inputPath = null;
        Path outputPath = null;
        try {
            String input = args[0];
            String output = args[1];
            inputPath = new Path(input);
            outputPath = new Path(output);
            int exitCode = new LogJoinDriver().execute(inputPath, outputPath);
            System.exit(exitCode);
        } catch(Exception e) {
            System.out.println("Usage: " + LogJoinDriver.class.getSimpleName() + " [input] [output]");
        }
        System.exit(-1);
    }
}
