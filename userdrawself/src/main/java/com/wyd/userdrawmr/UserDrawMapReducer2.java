package com.wyd.userdrawmr;

import com.wyd.userdraw.UserDraw;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class UserDrawMapReducer2 {
    public static class UserDrawMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split("[|]");
            context.write(new Text(arr[1]), value);
        }
    }

    public static class UserDrawReducer2 extends Reducer<Text, Text, NullWritable, Text> {

        private Map<String, String[]> appMap;
        private Text v;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            appMap = new HashMap<String, String[]>();
            v = new Text();

            Configuration conf = new Configuration();
            //conf.set("fs.defaultFS", "file:///");
            FileSystem fs = FileSystem.get(URI.create("hdfs://hdp12"), conf);
            FSDataInputStream in = fs.open(new Path("userdraw/appdata/appTab.txt"));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] arr = line.split("[|]");
                StringBuffer sb = new StringBuffer();
                sb.append(arr[1]).append(","); //appName
                sb.append(arr[2]).append(",").append(arr[3]).append(","); //性别权重
                //年龄段权重
                sb.append(arr[4]).append(",")
                        .append(arr[5]).append(",")
                        .append(arr[6]).append(",")
                        .append(arr[7]).append(",")
                        .append(arr[8]);

                String[] appArr = sb.toString().split(",");
                appMap.put(arr[0], appArr);
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            UserDraw userDraw = new UserDraw();
            boolean init = true;
            for(Text value : values){
                String[] arr = value.toString().split("[|]");
                String mdn = arr[1];
                String appId = arr[2];
                if(StringUtils.isEmpty(appId)){
                    continue;
                }
                String[] appInfo = appMap.get(appId);
                if(appInfo != null){

                    Double male = Double.valueOf(appInfo[1]);
                    Double female = Double.valueOf(appInfo[2]);
                    Double age1 = Double.valueOf(appInfo[3]);
                    Double age2 = Double.valueOf(appInfo[4]);
                    Double age3 = Double.valueOf(appInfo[5]);
                    Double age4 = Double.valueOf(appInfo[6]);
                    Double age5 = Double.valueOf(appInfo[7]);

                    Long times = Long.valueOf(arr[4]);
                    if(init){
                        userDraw.setStartTimeDay(arr[0]);
                        userDraw.setMDN(mdn);
                        userDraw.initSex(male, female);
                        userDraw.initAge(age1, age2, age3, age4, age5);
                        init = false;
                    } else {
                        userDraw.protraitSex(male, female, times);
                        userDraw.protraitAge(age1, age2, age3, age4, age5, times);
                    }
                }
            }
            if(!init){
                v.set(userDraw.toString());
                context.write(null, v);
            }

        }

        }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME","root");

        Configuration conf = new Configuration();
        Job job2 = Job.getInstance(conf);
        //job2.setJarByClass(UserDrawMapReducer2.class);
        job2.setJar("/Users/wangyadi/IdeaProjects/bigdatatest/userdrawself/target/userdrawself-1.0-SNAPSHOT.jar");
        job2.setJobName("UserDrawMapReduceStage2");

        job2.setMapperClass(UserDrawMapper2.class);
        job2.setReducerClass(UserDrawReducer2.class);


        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path("userdraw/out1"));
        FileOutputFormat.setOutputPath(job2, new Path("userdraw/out2"));

        boolean stage2 = job2.waitForCompletion(true);
    }
}

