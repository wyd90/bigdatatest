package com.wyd.hadoop.it18zhang.hdfs.mr.secordarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MaxTempGrouping extends WritableComparator {

    public MaxTempGrouping(){
        super(MaxTempBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MaxTempBean o1 = (MaxTempBean) a;
        MaxTempBean o2 = (MaxTempBean) b;
        return o1.getYear().compareTo(o2.getYear());
    }
}
