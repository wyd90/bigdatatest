package com.wyd.hbase.coprocessor.observer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;

public class CallLogCoprocessor extends BaseRegionObserver {

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        TableName tableName = e.getEnvironment().getRegion().getRegionInfo().getTable();
        TableName tableName1 = TableName.valueOf("ns1:t1");
        if(!tableName1.equals(tableName)){
            return;
        }
        byte[] row = put.getRow();
        String rowStr = Bytes.toString(row);

        String[] split = rowStr.split(",");

        String callee = split[3];
        String callid = split[1];
        String callTime = split[2];
        String duration = split[4];

        int hash = (callee + callTime.substring(0,6)).hashCode();
        DecimalFormat df = new DecimalFormat();
        df.applyPattern("00");
        hash = (hash & Integer.MAX_VALUE) % 100;
        String regNo = df.format(hash);

        String newRowKey = regNo + "," + callee + "," + callTime + "," + callid + "," + duration;

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("ns1:t2"));
        Put put1 = new Put(Bytes.toBytes(newRowKey));

        put1.addColumn(Bytes.toBytes("f"), Bytes.toBytes("0"),row);

        table.put(put1);

        conn.close();
    }
}
