package com.wyd.hadoop.it18zhang.hdfs.mr.secordarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxTempWithSecordarySort {
    public static class MaxTempWithSsortMapper extends Mapper<IntWritable, IntWritable, MaxTempBean, NullWritable>{

        private MaxTempBean maxTempBean;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            maxTempBean = new MaxTempBean();
        }

        @Override
        protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            maxTempBean.setYear(key.get());
            maxTempBean.setTemp(value.get());
            context.write(maxTempBean, NullWritable.get());
        }
    }

    public static class MaxTempWithSsortReducer extends Reducer<MaxTempBean, NullWritable, IntWritable, IntWritable>{
        @Override
        protected void reduce(MaxTempBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            values.iterator().next();
            context.write(new IntWritable(key.getYear()), new IntWritable(key.getTemp()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME","root");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("secordarySortTest");
        job.setJar("/Users/wangyadi/IdeaProjects/bigdatatest/hadooptest/target/hadooptest-1.0-SNAPSHOT.jar");
        //job.setJarByClass(MaxTempWithSecordarySort.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(MaxTempWithSsortMapper.class);
        job.setReducerClass(MaxTempWithSsortReducer.class);

        job.setPartitionerClass(MaxTempPartitioner.class);
        job.setGroupingComparatorClass(MaxTempGrouping.class);
        job.setSortComparatorClass(MTKeyComparator.class);

        job.setMapOutputKeyClass(MaxTempBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("tempMaxData"));
        FileOutputFormat.setOutputPath(job, new Path("tempMaxDataSecordaryOut"));

        job.waitForCompletion(true);
    }
}
