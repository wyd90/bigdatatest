package com.wyd.mr.uvcompute;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UVModel implements WritableComparable<UVModel> {

    private String ip;

    private String url;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public int compareTo(UVModel o) {
        if (o.getUrl().compareTo(this.url) == 0) {
            if(o.ip.compareTo(this.ip) == 0) {
                return -1;
            } else {
                return o.ip.compareTo(this.ip);
            }
        } else {
            return o.getUrl().compareTo(this.url);
        }
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.ip);
        dataOutput.writeUTF(this.url);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.ip = dataInput.readUTF();
        this.url = dataInput.readUTF();
    }
}
