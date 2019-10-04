package com.wyd.mr.uvcompute;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UVGrouping extends WritableComparator {

    public UVGrouping() {
        super(UVModel.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        UVModel auv = (UVModel)a;
        UVModel buv = (UVModel)b;
        return auv.getIp().compareTo(buv.getIp());
    }
}
