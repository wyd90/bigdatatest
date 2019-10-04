package com.wyd.mr.uvcompute;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class App {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("UVStep1");
        job.setJarByClass(UVMapReduceStepOne.class);
        job.setMapperClass(UVMapReduceStepOne.UVStepOneMapper.class);
        job.setReducerClass(UVMapReduceStepOne.UVStepOneReducer.class);

        job.setMapOutputKeyClass(UVModel.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setPartitionerClass(UVPartitioner.class);
        job.setGroupingComparatorClass(UVGrouping.class);

        FileInputFormat.addInputPath(job, new Path("/Users/wangyadi/yarnData/uv/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/wangyadi/yarnData/uv/output1"));

        boolean state1 = job.waitForCompletion(true);
        System.out.println("job1执行成功");
        if(state1){
            conf = new Configuration();
            Job job2 = Job.getInstance(conf);
            job2.setJobName("UVStep2");
            job2.setJarByClass(UVMapReduceStepTwo.class);

            job2.setMapperClass(UVMapReduceStepTwo.UVStepTwoMapper.class);
            job2.setReducerClass(UVMapReduceStepTwo.UVStepTwoReducer.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job2, new Path("/Users/wangyadi/yarnData/uv/output1"));
            FileOutputFormat.setOutputPath(job2, new Path("/Users/wangyadi/yarnData/uv/output2"));

            job2.waitForCompletion(true);

            System.out.println("job2执行成功");

        }
    }
}
