package com.wyd.hbase.coprocessor.observer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class InvertedCoprocessor extends BaseRegionObserver {

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        byte[] row = put.getRow();
        List<Cell> cells = put.get(Bytes.toBytes("f1"), Bytes.toBytes("from"));
        Cell cell = cells.get(0);

        String newRowId = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        Put newPut = new Put(Bytes.toBytes(newRowId));

        newPut.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("from"), row);

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns1:fensi");

        Table table = conn.getTable(tableName);

        table.put(newPut);

        conn.close();
    }
}
