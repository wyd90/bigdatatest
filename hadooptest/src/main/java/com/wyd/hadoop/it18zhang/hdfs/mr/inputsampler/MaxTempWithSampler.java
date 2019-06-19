package com.wyd.hadoop.it18zhang.hdfs.mr.inputsampler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

public class MaxTempWithSampler {
    public static class MaxTempMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable>{
        @Override
        protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class MaxTempReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            for(IntWritable value : values){
                if(max < value.get()){
                    max = value.get();
                }
            }
            context.write(key, new IntWritable(max));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.setProperty("HADOOP_USER_NAME","root");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("MaxTemp");
        job.setJar("/Users/wangyadi/IdeaProjects/bigdatatest/hadooptest/target/hadooptest-1.0-SNAPSHOT.jar");
        job.setInputFormatClass(SequenceFileInputFormat.class);

        //job.setJarByClass(MaxTempWithSampler.class);
        job.setMapperClass(MaxTempMapper.class);
        job.setReducerClass(MaxTempReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job, new Path("tempMaxData"));
        FileOutputFormat.setOutputPath(job, new Path("tempMaxDataOut"));

        //设置全排序分区类
        job.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("hdfs://node2:9000/user/root/tempMaxDataPar/par.lst"));

        //创建随机采样器对象
        //freq每个key被选中的概率
        //numSamplesc抽取样本的总数
        //maxSplitsSampled划分的分区数
        InputSampler.RandomSampler<IntWritable, IntWritable> sampler = new InputSampler.RandomSampler<IntWritable, IntWritable>(0.1, 1000, 3);

        //写入分区文件
        InputSampler.writePartitionFile(job, sampler);

        job.waitForCompletion(true);

    }
}
