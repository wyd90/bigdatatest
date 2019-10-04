package com.wyd.mr.uvcompute;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class UVPartitioner extends Partitioner<UVModel, IntWritable> {
    @Override
    public int getPartition(UVModel uvModel, IntWritable intWritable, int i) {
        return (uvModel.getUrl().hashCode() & Integer.MAX_VALUE) % i;
    }
}
