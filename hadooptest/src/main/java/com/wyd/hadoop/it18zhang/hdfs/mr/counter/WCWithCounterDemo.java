package com.wyd.hadoop.it18zhang.hdfs.mr.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WCWithCounterDemo {

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            context.getCounter("wcm", "wordcountmapper.setup").increment(1);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split(" ");
            for(String word : arr){
                context.write(new Text(word), new IntWritable(1));
                                 //计数器组名        计数器名
                context.getCounter("wcm", "wordcountmapper.readline").increment(1);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            context.getCounter("wcr", "wordcountreducer.setup").increment(1);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for(IntWritable v : values){
                i++;
            }
            context.write(key, new IntWritable(i));
            context.getCounter("wcr", "wordcountreducer.reduce").increment(1);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("HADOOP_USER_NAME","root");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("countertest");
        job.setJar("/Users/wangyadi/IdeaProjects/bigdatatest/hadooptest/target/hadooptest-1.0-SNAPSHOT.jar");
        //job.setJarByClass(WCWithCounterDemo.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/wc"));
        FileOutputFormat.setOutputPath(job, new Path("testCounter"));

        job.waitForCompletion(true);
    }
}
