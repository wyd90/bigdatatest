package com.wyd.userdrawmr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReadHbaseToHdfsMapReduce {

    public static class HbaseToHdfsMapper extends TableMapper<Text, Text>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

            StringBuffer sb = new StringBuffer();
            byte[] mdnBytes = value.getValue(Bytes.toBytes("draw"), Bytes.toBytes("mdn"));
            if(mdnBytes != null && mdnBytes.length != 0){
                sb.append(Bytes.toString(mdnBytes)).append("|");
            }
            byte[] maleBytes = value.getValue(Bytes.toBytes("draw"), Bytes.toBytes("male"));
            if(maleBytes != null && maleBytes.length != 0){
                sb.append(Bytes.toString(maleBytes)).append("|");
            }
            byte[] femaleBytes = value.getValue(Bytes.toBytes("draw"), Bytes.toBytes("female"));
            if(femaleBytes != null && femaleBytes.length != 0){
                sb.append(Bytes.toString(femaleBytes)).append("|");
            }
            byte[] age1Bytes = value.getValue(Bytes.toBytes("draw"), Bytes.toBytes("age1"));
            if(age1Bytes != null && age1Bytes.length != 0){
                sb.append(Bytes.toString(age1Bytes)).append("|");
            }
            byte[] age2Bytes = value.getValue(Bytes.toBytes("draw"), Bytes.toBytes("age2"));
            if(age2Bytes != null && age2Bytes.length != 0){
                sb.append(Bytes.toString(age2Bytes)).append("|");
            }
            byte[] age3Bytes = value.getValue(Bytes.toBytes("draw"), Bytes.toBytes("age3"));
            if(age3Bytes != null && age3Bytes.length != 0){
                sb.append(Bytes.toString(age3Bytes)).append("|");
            }
            byte[] age4Bytes = value.getValue(Bytes.toBytes("draw"), Bytes.toBytes("age4"));
            if(age4Bytes != null && age4Bytes.length != 0){
                sb.append(Bytes.toString(age4Bytes)).append("|");
            }
            byte[] age5Bytes = value.getValue(Bytes.toBytes("draw"), Bytes.toBytes("age5"));
            if(age5Bytes != null && age5Bytes.length != 0){
                sb.append(Bytes.toString(age5Bytes));
            }

            context.write(new Text(key.toString()), new Text(sb.toString()));

        }
    }

    public static class HbaseToHdfsReducer extends Reducer<Text, Text, NullWritable, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
                context.write(NullWritable.get(), value);
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
        job.setJar("/Users/wangyadi/IdeaProjects/bigdatatest/userdrawself/target/userdrawself-1.0-SNAPSHOT.jar");
        job.setJobName("HbaseToHdfsMapReduce");
        //job.setJarByClass(ReadHbaseToHdfsMapReduce.class);

        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("t_draw")
                ,scan   //指定查询条件
                ,HbaseToHdfsMapper.class  //mapper class
                ,Text.class     //mapper输出key类型
                ,Text.class     //mapper输出value类型
                ,job
                ,false);

        job.setReducerClass(HbaseToHdfsReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path("userdraw/out3"));

        job.waitForCompletion(true);
    }
}
