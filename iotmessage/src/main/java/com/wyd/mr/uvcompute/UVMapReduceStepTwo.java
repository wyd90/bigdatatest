package com.wyd.mr.uvcompute;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class UVMapReduceStepTwo {

    public static class UVStepTwoMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        private IntWritable v;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            v = new IntWritable(1);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, v);
        }
    }


    public static class UVStepTwoReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> iterator = values.iterator();
            int count = 0;
            while (iterator.hasNext()){
                iterator.next();
                count = count + 1;
            }
            context.write(key, new IntWritable(count));
        }
    }

}
