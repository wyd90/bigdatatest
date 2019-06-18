package com.wyd.hadoop.it18zhang.hdfstest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestMapFile {

    @Test
    public void testSaveMapFile() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.blocksize", "134217728");
        System.setProperty("HADOOP_USER_NAME","root");
        FileSystem fs = FileSystem.get(URI.create("hdfs://hdp12"), conf);

        MapFile.Writer writer = new MapFile.Writer(conf, fs, "/mapTest/a.map", IntWritable.class, Text.class);

        writer.append(new IntWritable(1), new Text("aaa"));
        writer.append(new IntWritable(2), new Text("bbb"));

        IOUtils.closeStream(writer);

    }

    @Test
    public void testReadMapFile() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://hdp12"), conf);

        MapFile.Reader reader = new MapFile.Reader(fs, "/mapTest/a.map", conf);

        IntWritable key = new IntWritable();
        Text value = new Text();

        while (reader.next(key, value)){
            System.out.println(key.toString()+ " "+ value.toString());
        }
        IOUtils.closeStream(reader);

    }


}
