package com.wyd.hadoop.it18zhang.hdfstest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestSeqFile {

    @Test
    public void testSaveSeq() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.blocksize", "134217728");


        FileSystem fs = FileSystem.get(URI.create("hdfs://hdp12"), conf, "root");

        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path("seqTest/a.seq"), IntWritable.class, Text.class);
        //创建压缩的seqence文件，block压缩模式
        //SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path("a.seq.gzip"), IntWritable.class, Text.class, SequenceFile.CompressionType.BLOCK, new GzipCodec());

        writer.append(new IntWritable(1), new Text("hello world tom"));
        writer.append(new IntWritable(2), new Text("tom jack say hello"));

        IOUtils.closeStream(writer);


    }

    @Test
    public void testReadSeq() throws IOException, InterruptedException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create("hdfs://hdp12"), conf, "root");

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("seqTest/a.seq"), conf);

        IntWritable key = new IntWritable();
        Text value = new Text();

        while (reader.next(key, value)){
            System.out.println(reader.getPosition() + " " + key.toString()+" "+value.toString());
        }

    }
}
