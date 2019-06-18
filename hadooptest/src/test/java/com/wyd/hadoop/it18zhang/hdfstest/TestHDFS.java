package com.wyd.hadoop.it18zhang.hdfstest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class TestHDFS {

    @Test
    public void testRead() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hdp12");
        FileSystem fs = FileSystem.get(URI.create("hdfs://hdp12"), conf, "root");

        FSDataInputStream in = fs.open(new Path("a.txt"));

        IOUtils.copyBytes(in, System.out, 4096, true);

    }

    @Test
    public void testWrite() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hdp12");
        conf.set("dfs.blocksize", "134217728");
        FileSystem fs = FileSystem.get(URI.create("hdfs://hdp12"), conf, "root");

        BufferedInputStream in = new BufferedInputStream(new FileInputStream("/Users/wangyadi/Documents/myjson.json"));
        FSDataOutputStream out = fs.create(new Path("/myjson2.json"), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096, true);
    }

    /**
     *向hdfs中写入文件，定制副本数和block块大小
     * 设置1副本，block块为64M
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testWriteWithDIY() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://hdp12"), conf, "root");

        BufferedInputStream in = new BufferedInputStream(new FileInputStream("/Users/wangyadi/Documents/myjson.json"));

        FSDataOutputStream out = fs.create(new Path("myjson3"), true, 4096, new Short("1"), 134217728L);

        IOUtils.copyBytes(in, out, 4096, true);
    }



}
