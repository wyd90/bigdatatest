package com.wyd.hadoop.it18zhang.hdfs.mr.multiInputs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MultiInputWCDemo {

    public static class WordsCountTextMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        private Text keyOut;
        private IntWritable valueOut;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            keyOut = new Text();
            valueOut = new IntWritable(1);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split(" ");
            for(String word : arr){
                keyOut.set(word);
                context.write(keyOut, valueOut);
            }
        }
    }

    public static class WordsCountSeqMapper extends Mapper<IntWritable, Text, Text, IntWritable>{

        private Text keyOut;
        private IntWritable valueOut;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            keyOut = new Text();
            valueOut = new IntWritable(1);
        }

        @Override
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split(" ");
            for(String word : arr){
                keyOut.set(word);
                context.write(keyOut, valueOut);
            }
        }
    }


    public static class WordsCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int i = 0;
            for(IntWritable value : values){
                i++;
            }
            context.write(key, new IntWritable(i));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME","root");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("multipleInputs");
        job.setJar("/Users/wangyadi/IdeaProjects/bigdatatest/hadooptest/target/hadooptest-1.0-SNAPSHOT.jar");
        //job.setJarByClass(MultiInputWCDemo.class);

        MultipleInputs.addInputPath(job, new Path("/wc"), TextInputFormat.class, WordsCountTextMapper.class);
        MultipleInputs.addInputPath(job, new Path("seqTest"), SequenceFileInputFormat.class, WordsCountSeqMapper.class);

        FileOutputFormat.setOutputPath(job, new Path("multiInputsTest"));

        job.setReducerClass(WordsCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }


}
