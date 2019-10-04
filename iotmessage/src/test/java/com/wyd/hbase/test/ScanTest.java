package com.wyd.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

public class ScanTest {

    @Test
    public void testScan() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM,"node3:2181,node4:2181,node5:2181");

        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("iotMsg"));

        ResultScanner scanner = table.getScanner(new Scan());

        Iterator<Result> iterator = scanner.iterator();

        while (iterator.hasNext()){
            Result next = iterator.next();
            String row = Bytes.toString(next.getRow());

            Double v = Bytes.toDouble(next.getValue(Bytes.toBytes("f"), Bytes.toBytes("v")));

            System.out.println(row + "," + v);
        }
    }
}
