package com.wyd.hadoop.it18zhang.hdfs.readhdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * 用数据流的方式读取HDFS上的文件
 */
public class ReadHdfsFromURLDemo {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws IOException {
        InputStream in = new URL("hdfs://node2:9000/user/root/a.txt").openStream();
        IOUtils.copyBytes(in, System.out, 4096, true);
    }
}
