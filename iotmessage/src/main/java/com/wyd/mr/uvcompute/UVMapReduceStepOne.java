package com.wyd.mr.uvcompute;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class UVMapReduceStepOne {

    public static class UVStepOneMapper extends Mapper<LongWritable, Text, UVModel, IntWritable>{

        private UVModel uvModel;

        private IntWritable v;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            uvModel = new UVModel();
            v = new IntWritable(1);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(!StringUtils.isEmpty(line)){
                String[] arr = line.split(",");
                if(arr.length == 4){
                    String date = arr[2];
                    String[] dateArr = date.split(" ");
                    if(dateArr.length == 2 && (dateArr[0].equals("2017-09-16"))){
                        uvModel.setIp(arr[0]);
                        uvModel.setUrl(arr[3]);

                        context.write(uvModel, v);
                    }
                }
            }

        }
    }

    public static class UVStepOneReducer extends Reducer<UVModel, IntWritable, Text, NullWritable>{

        private Text k;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = new Text();
        }

        @Override
        protected void reduce(UVModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> iterator = values.iterator();
            iterator.next();

            k.set(key.getUrl());
            context.write(k, NullWritable.get());
        }
    }


}
