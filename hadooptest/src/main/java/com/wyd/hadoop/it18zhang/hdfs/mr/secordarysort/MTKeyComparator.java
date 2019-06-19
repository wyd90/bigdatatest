package com.wyd.hadoop.it18zhang.hdfs.mr.secordarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MTKeyComparator extends WritableComparator {

    public MTKeyComparator() {
        super(MaxTempBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MaxTempBean o1 = (MaxTempBean) a;
        MaxTempBean o2 = (MaxTempBean) b;
        if(o1.getYear() - o2.getYear() == 0){
            return o2.getTemp().compareTo(o1.getTemp());
        } else {
            return o1.getYear().compareTo(o2.getYear());
        }
    }
}
