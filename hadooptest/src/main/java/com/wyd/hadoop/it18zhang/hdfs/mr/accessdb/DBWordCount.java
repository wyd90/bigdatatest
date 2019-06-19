package com.wyd.hadoop.it18zhang.hdfs.mr.accessdb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

import java.io.IOException;

public class DBWordCount {
    public static class DBWcMapper extends Mapper<LongWritable, DBInWritable, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, DBInWritable value, Context context) throws IOException, InterruptedException {
            String[] words = value.getTxt().split(" ");
            for(String word : words){
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

     public static class DBWcReducer extends Reducer<Text, IntWritable, DBOutWritable, NullWritable>{
         @Override
         protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
             int count = 0;
             for(IntWritable w : values){
                 count = count + w.get();
             }
             DBOutWritable keyOut = new DBOutWritable();
             keyOut.setWord(key.toString());
             keyOut.setCnts(count);

             context.write(keyOut, NullWritable.get());
         }
     }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME","root");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("dbWordCount");
        job.setJar("/Users/wangyadi/IdeaProjects/bigdatatest/hadooptest/target/hadooptest-1.0-SNAPSHOT.jar");
        //job.setJarByClass(DBWordCount.class);

        DBConfiguration.configureDB(job.getConfiguration(), "com.mysql.jdbc.Driver", "jdbc:mysql://node3/bigdata", "root", "az63091919");
        DBInputFormat.setInput(job, DBInWritable.class, "select id,name,txt from words", "select count(*) from words");

        DBOutputFormat.setOutput(job, "wordcount", "word","cnts");

        job.setMapperClass(DBWcMapper.class);
        job.setReducerClass(DBWcReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(DBOutWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.waitForCompletion(true);

    }
}
