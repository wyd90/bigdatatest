package com.wyd.hadoop.it18zhang.hdfs.mr.secordarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MaxTempPartitioner extends Partitioner<MaxTempBean, NullWritable> {

    public int getPartition(MaxTempBean maxTempBean, NullWritable nullWritable, int i) {
        return (maxTempBean.getYear().hashCode() & Integer.MAX_VALUE) % i;
    }
}
