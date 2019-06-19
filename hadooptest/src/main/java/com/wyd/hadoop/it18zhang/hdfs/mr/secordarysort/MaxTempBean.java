package com.wyd.hadoop.it18zhang.hdfs.mr.secordarysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MaxTempBean implements WritableComparable<MaxTempBean> {

    private Integer year;
    private Integer temp;

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Integer getTemp() {
        return temp;
    }

    public void setTemp(Integer temp) {
        this.temp = temp;
    }

    public int compareTo(MaxTempBean o) {
        if(o.getYear().compareTo(this.year) == 0) {
            return o.getTemp().compareTo(this.temp);
        } else {
            return this.year.compareTo(o.getYear());
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(temp);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.temp = dataInput.readInt();
    }
}
