package com.test.sample;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//public class LogJoinReducer extends Reducer<Text, Text, Text, NullWritable> {
public class LogJoinReducer extends Reducer<Text, NullWritable, Text, NullWritable> {  /* new */

  @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
    }

//    @Override
//    protected void reduce(Text entry, Iterable<Text> value, Context context)
//            throws IOException, InterruptedException {
//        String user=null;
//        long start=-1L;
//        long end=-1L;
//        int total=0;
//        Iterator<Text> it=value.iterator();
//        while(it.hasNext()){
//            String act=it.next().toString();
//            if(act.equals("in")){
//                String[] tokens=entry.toString().split(",");
//                user=tokens[0];
//                start=Long.parseLong(tokens[1]);
//                total=0;
//            }else if(act.equals("out")){
//                String[] tokens=entry.toString().split(",");
//                end=Long.parseLong(tokens[1]);
//                context.write(new Text(user+","+start+","+end+","+total), NullWritable.get());
//            }else{
//                total+=Integer.parseInt(act);
//            }
//        }
//    }

    /* new */
    @Override
    protected void reduce(Text entry, Iterable<NullWritable> value, Context context)
            throws IOException, InterruptedException {
        context.write(entry , NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
    }

}
