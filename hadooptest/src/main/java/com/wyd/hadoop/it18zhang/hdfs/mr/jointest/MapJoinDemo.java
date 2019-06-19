package com.wyd.hadoop.it18zhang.hdfs.mr.jointest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapJoinDemo {

    public static class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

        private Map<String, String> customers;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            customers = new HashMap<String, String>();
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(URI.create("hdfs://hdp12"), conf, "root");
            FSDataInputStream in = fs.open(new Path("mapJoinTest/customers/customers.txt"));

            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while((line = br.readLine()) != null){
                String uid = line.substring(0, line.indexOf(","));
                customers.put(uid, line);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String uid = line.substring(line.lastIndexOf(",") + 1);

            String userInfo = customers.get(uid);

            context.write(new Text(line+","+userInfo.substring(line.indexOf(",")+1)),NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("HADOOP_USER_NAME","root");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("MapJoinTest");
        job.setJar("/Users/wangyadi/IdeaProjects/bigdatatest/hadooptest/target/hadooptest-1.0-SNAPSHOT.jar");
        //job.setJarByClass(MapJoinDemo.class);

        job.setMapperClass(MapJoinMapper.class);

        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("mapJoinTest/input"));
        FileOutputFormat.setOutputPath(job, new Path("mapJoinTest/output"));

        job.waitForCompletion(true);
    }
}
