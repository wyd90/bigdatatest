package com.wyd.hbase.coprocessor.endpoint;

import com.wyd.hbase.coprocessor.util.DynamicCallUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

public class DynamicTest {

    public static void main(String[] args) throws Throwable {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        String tableName = "ns1:t1";
        TableName tableNameObj = TableName.valueOf(tableName);
        Table table = conn.getTable(tableNameObj);
        DynamicCallUtil.setupToExistTable(conn, table, "hdfs://hdp12/hbase/coprocessor/hbasetest-1.0-SNAPSHOT.jar", CountAndSum.class);
        CountAndSumClient client = new CountAndSumClient(conn);
        CountAndSumClient.CountAndSumResult result = client.call(tableName, "f", "c", null, null);

        System.out.println("count: " + result.count + ", sum: " + result.sum);
        DynamicCallUtil.deleteCoprocessor(conn,table,CountAndSum.class);

        conn.close();


    }
}
