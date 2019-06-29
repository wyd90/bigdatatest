package com.wyd.userdrawmr;

import com.wyd.userdraw.UserDraw;
import com.wyd.util.Config;
import com.wyd.util.TextArrayWritable;
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
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class UserDrawMapReduce {

    public static class UserDrawMapper extends Mapper<LongWritable, Text, Text, TextArrayWritable>{

        private SimpleDateFormat sdf;
        private Text k;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            sdf = new SimpleDateFormat("yyyyMMdd");
            k = new Text();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] arr = line.split("[|]");

            String uiqKey = arr[0] + arr[15]; //手机号和appid
            String[] val = new String[5];

            String timeNow = arr[11];
            val[0] = sdf.format(Long.valueOf(timeNow));   //时间
            val[1] = arr[0];    //手机号
            val[2] = arr[15];  //appid
            val[3] = "1"; //计数
            val[4] = arr[12]; //使用时常

            k.set(uiqKey);

            context.write(k, new TextArrayWritable(val));
        }
    }

    public static class UserDrawReducer extends Reducer<Text, TextArrayWritable, NullWritable, Text>{

        private Text k;

        private Text v;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = new Text();
            v = new Text();
        }

        @Override
        protected void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {

            long sum = 0;
            int count = 0;
            String time = "";
            String mdn = "";
            String appId = "";
            for(TextArrayWritable value : values){
                String[] arr = value.toStrings();
                if(!StringUtils.isEmpty(arr[3])){
                    count = count + 1;
                }
                if(StringUtils.isEmpty(arr[4])){
                    sum = sum + Long.valueOf(arr[4]);
                }
                time = arr[0];
                mdn = arr[1];
                appId = arr[2];
            }
            StringBuffer sb = new StringBuffer();
            sb.append(time).append("|"); //时间
            sb.append(mdn).append("|"); //手机号
            sb.append(appId).append("|"); //appid
            sb.append(""+count).append("|"); //计数
            sb.append(""+sum);  //使用时长
            k.set(mdn);
            v.set(sb.toString());
            context.write(NullWritable.get(), v);
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        Job job = Job.getInstance(conf);

        job.setJarByClass(UserDrawMapReduce.class);
        job.setJobName("UserDrawMapReduceStage1");

        job.setMapperClass(UserDrawMapper.class);
        job.setReducerClass(UserDrawReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextArrayWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/Users/wangyadi/yarnData/userDraw/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/wangyadi/yarnData/userDraw/out1"));

        job.waitForCompletion(true);

    }

}
