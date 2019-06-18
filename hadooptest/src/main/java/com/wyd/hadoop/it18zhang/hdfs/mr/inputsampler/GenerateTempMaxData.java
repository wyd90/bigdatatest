package com.wyd.hadoop.it18zhang.hdfs.mr.inputsampler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

public class GenerateTempMaxData {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://hdp12"), conf, "root");

        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path("tempMaxData/temp.seq"), IntWritable.class, IntWritable.class);
        int year = 1970;
        for(int i = 0; i < 100; i++){
            int temp = 0;
            for(int k = 0; k < 10; k++){
                temp = -30 + new Random().nextInt(100);
                writer.append(new IntWritable(year), new IntWritable(temp));
            }
            year ++;
        }

        IOUtils.closeStream(writer);
    }
}
