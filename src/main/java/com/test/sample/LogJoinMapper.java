package com.test.sample;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//public class LogJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
public class LogJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {  /* new */

//    Text outputKey=new Text();
//    Text outputValue=new Text();

    @Override
    protected void setup(Context context) {
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try{
//            String line=value.toString();
//            String tokens[]=line.split(",");
//            String user=tokens[0];
//            String ts=tokens[1];
//            String act=tokens[2];
//            outputKey.set(user+","+ts);
//            outputValue.set(act);
//            context.write(outputKey, outputValue);
            context.write(value, NullWritable.get());  /* new */
    } catch(Exception e) {
            //ignore
        }
    }

    @Override
    protected void cleanup(Context context) {
    }

}
