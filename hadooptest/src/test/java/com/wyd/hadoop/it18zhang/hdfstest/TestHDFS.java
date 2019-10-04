package com.wyd.hadoop.it18zhang.hdfstest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import java.io.*;
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

    @Test
    public void loadData() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("file:////Users/wangyadi/yarnData/join/input"), conf);
        FSDataInputStream in = fs.open(new Path("/Users/wangyadi/yarnData/join/input/user.txt"));

        BufferedReader bf = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ((line = bf.readLine()) != null){
            System.out.println(line);
        }
        bf.close();
        fs.close();
    }

    @Test
    public void writeToFile() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("file:////Users/wangyadi/yarnData/join/input"), conf);

        FSDataInputStream in = fs.open(new Path("/Users/wangyadi/yarnData/join/input/user.txt"));

        File file = new File("/Users/wangyadi/yarnData/join/fileOutput");
        if(!file.exists()){
            file.createNewFile();
        }
        FileOutputStream os = new FileOutputStream(file);
        IOUtils.copyBytes(in,os,1024,true);
    }

    @Test
    public void writeToFile2() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("file:////Users/wangyadi/yarnData/join/input"), conf);

        FSDataInputStream in = fs.open(new Path("/Users/wangyadi/yarnData/join/input/user.txt"));

        File file = new File("/Users/wangyadi/yarnData/join/fileOutput.txt");
        if(!file.exists()){
            file.createNewFile();
        }
        FileOutputStream os = new FileOutputStream(file);

        byte[] buf = new byte[1024];
        int len = 0;
        while ((len = in.read(buf)) != -1){
            os.write(buf,0,len);
            os.flush();
        }
        in.close();
        os.close();
        fs.close();
    }



}
