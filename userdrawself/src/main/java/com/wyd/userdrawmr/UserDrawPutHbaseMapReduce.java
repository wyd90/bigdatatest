package com.wyd.userdrawmr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class UserDrawPutHbaseMapReduce {

    public static class UserDrawPutHbaseMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    public static class UserDrawPutHbaseReducer extends TableReducer<Text, NullWritable, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for(NullWritable value : values){
                String[] arr = key.toString().split("[|]");
                String rowKey = arr[1];
                if(!StringUtils.isEmpty(rowKey)){
                    Put put = new Put(Bytes.toBytes(rowKey));

                    //跳过写前日志
                    put.setDurability(Durability.SKIP_WAL);
                    put.addColumn(Bytes.toBytes("draw"), Bytes.toBytes("mdn"), Bytes.toBytes(arr[1]));
                    put.addColumn(Bytes.toBytes("draw"), Bytes.toBytes("male"), Bytes.toBytes(arr[2]));
                    put.addColumn(Bytes.toBytes("draw"), Bytes.toBytes("female"), Bytes.toBytes(arr[3]));
                    put.addColumn(Bytes.toBytes("draw"), Bytes.toBytes("age1"), Bytes.toBytes(arr[4]));
                    put.addColumn(Bytes.toBytes("draw"), Bytes.toBytes("age2"), Bytes.toBytes(arr[5]));
                    put.addColumn(Bytes.toBytes("draw"), Bytes.toBytes("age3"), Bytes.toBytes(arr[6]));
                    put.addColumn(Bytes.toBytes("draw"), Bytes.toBytes("age4"), Bytes.toBytes(arr[7]));
                    put.addColumn(Bytes.toBytes("draw"), Bytes.toBytes("age5"), Bytes.toBytes(arr[8]));

                    context.write(NullWritable.get(), put);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME","root");
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("yarn-site.xml");
        conf.addResource("mapred-site.xml");

        Job job = Job.getInstance(conf);
        job.setJobName("HDFSToHBase");
        //job.setJarByClass(UserDrawPutHbaseMapReduce.class);
        job.setJar("/Users/wangyadi/IdeaProjects/bigdatatest/userdrawself/target/userdrawself-1.0-SNAPSHOT.jar");

        //设置表名
        TableMapReduceUtil.initTableReducerJob("t_draw", UserDrawPutHbaseReducer.class, job);
        job.setMapperClass(UserDrawPutHbaseMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        FileInputFormat.addInputPath(job, new Path("userdraw/out2"));

        job.waitForCompletion(true);
    }
}
