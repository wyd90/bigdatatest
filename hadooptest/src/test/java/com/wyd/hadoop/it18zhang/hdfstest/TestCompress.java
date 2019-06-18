package com.wyd.hadoop.it18zhang.hdfstest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class TestCompress {

    public void zip(Class codecClass) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hdp12");
        conf.set("dfs.blocksize", "134217728");

        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        FileSystem fs = FileSystem.get(URI.create("hdfs://hdp12"), conf, "root");

        FSDataOutputStream out = fs.create(new Path("compressionTest/a"+codec.getDefaultExtension()));
        BufferedInputStream in = new BufferedInputStream(new FileInputStream("/Users/wangyadi/Documents/a.xml"));

        CompressionOutputStream zipOut = codec.createOutputStream(out);

        IOUtils.copyBytes(in, zipOut, 4096, true);

    }

    public void unzip(Class codecClass) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hdp12");

        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        FileSystem fs = FileSystem.get(URI.create("hdfs://hdp12"), conf, "root");

        FSDataInputStream in = fs.open(new Path("compressionTest/a" + codec.getDefaultExtension()));
        FSDataOutputStream out = fs.create(new Path("compressionTest/a" + codec.getDefaultExtension()+".xml"));

        CompressionInputStream zipIn = codec.createInputStream(in);

        IOUtils.copyBytes(zipIn, out, 4096, true);

    }

    private Class[] zipClasses = {
            DeflateCodec.class,
            GzipCodec.class,
            BZip2Codec.class
    };

    @Test
    public void testCompress() throws IOException, InterruptedException {
        for(Class c : zipClasses){
            zip(c);
        }
    }

    @Test
    public void testUnCompress() throws IOException, InterruptedException {
        for(Class c : zipClasses){
            unzip(c);
        }
    }


}
