package com.wyd.hbase.crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import javax.swing.*;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

public class CallLogTest {

    @Test
    public void insertLog() throws IOException {
        String callid = "18518755368";
        String callee = "18731728191";
        String callTime = "20190618181730";
        int dur = 90;

        DecimalFormat df = new DecimalFormat();

        df.applyPattern("00");

        int hash = (callid + callTime.substring(0, 6)).hashCode();
        hash = (hash & Integer.MAX_VALUE) % 100;
        String regNo = df.format(hash);

        DecimalFormat df2 = new DecimalFormat();
        df2.applyPattern("00000");
        String duration = df2.format(dur);

        String rowKey = regNo + "," + callid + "," + callTime + "," + callee + "," + duration;

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("ns1:t1"));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("0"), Bytes.toBytes("beijing"));

        table.put(put);

        conn.close();
    }

    /**
     * 查六月通话详单
     */
    @Test
    public void testSel() throws IOException {
        String callid = "18518755368";

        DecimalFormat df = new DecimalFormat();
        df.applyPattern("00");

        int startHash = (callid + "201906").hashCode();
        startHash = (startHash & Integer.MAX_VALUE) % 100;
        String startRegNo = df.format(startHash);



        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(startRegNo + "," +callid + "," +"20190601000000")));
        RowFilter rowFilter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(startRegNo+ "," + callid + "," +"20190631999999")));

        FilterList ft = new FilterList(FilterList.Operator.MUST_PASS_ALL, rowFilter, rowFilter1);

        Table table = conn.getTable(TableName.valueOf("ns1:t1"));
        Scan scan = new Scan();
        scan.setFilter(ft);

        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iterator = scanner.iterator();

        while (iterator.hasNext()){
            Result next = iterator.next();
            byte[] row = next.getRow();
            byte[] value = next.getValue(Bytes.toBytes("f"), Bytes.toBytes("0"));
            System.out.println(Bytes.toString(row)+":"+Bytes.toString(value));
        }
        conn.close();
    }
}
