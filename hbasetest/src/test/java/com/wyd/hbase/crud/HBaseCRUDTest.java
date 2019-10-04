package com.wyd.hbase.crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

public class HBaseCRUDTest {

    /**
     * 创建名字空间并创建表
     * @throws IOException
     */
    @Test
    public void createNameSpaceAndTable() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();

        NamespaceDescriptor ns5 = NamespaceDescriptor.create("ns5").build();
        admin.createNamespace(ns5);

        TableName tableName = TableName.valueOf("ns5:t1");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

        HColumnDescriptor f1 = new HColumnDescriptor("f1");
        f1.setMaxVersions(3);
        hTableDescriptor.addFamily(f1);

        byte[][] splitKeys = new byte[][]{Bytes.toBytes("030000"), Bytes.toBytes("060000"), Bytes.toBytes("090000")};

        admin.createTable(hTableDescriptor, splitKeys);

        admin.close();
    }

    /**
     * 取得所有的名字空间
     * @throws IOException
     */
    @Test
    public void getNameSpaces() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();

        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for(NamespaceDescriptor namespaceDescriptor:namespaceDescriptors){
            System.out.println(namespaceDescriptor.getName());
        }
    }

    /**
     * 批量插入数据关闭写前日志
     * @throws IOException
     */
    @Test
    public void batchInsert() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t1");
        HTable table = (HTable) conn.getTable(tableName);

        DecimalFormat format = new DecimalFormat();
        format.applyPattern("000000");
        //不要自动清理缓冲区
        table.setAutoFlush(false,true);
        for(int i = 0; i < 100000; i++){
            Put put = new Put(Bytes.toBytes(format.format(i)));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("id"), Bytes.toBytes(i));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("tom" + i));
            //关闭写前日志
            put.setWriteToWAL(false);
            table.put(put);
            if(i % 2000 == 0){
                table.flushCommits();
            }
        }
        table.flushCommits();
    }

    /**
     * 按rowkey获取单条数据
     * @throws IOException
     */
    @Test
    public void get() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t1");

        Table table = conn.getTable(tableName);

        Get get = new Get(Bytes.toBytes("000001"));
        Result result = table.get(get);

        byte[] res = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
        String name = Bytes.toString(res);
        System.out.printf(name);

        conn.close();
    }

    @Test
    public void put() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t1");

        Table table = conn.getTable(tableName);

        Put put = new Put(Bytes.toBytes("000010"));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("id"), Bytes.toBytes(1111111));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("hank10"));

        table.put(put);

        conn.close();
    }

    /**
     * 删除数据
     * @throws IOException
     */
    @Test
    public void deleteData() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t1");
        Table table = conn.getTable(tableName);

        Delete delete = new Delete(Bytes.toBytes("100001"));
        delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("id"));
        delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"));

        table.delete(delete);
        conn.close();
    }

    @Test
    public void dropTable() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("ns5:t1");
        admin.disableTable(tableName);
        admin.deleteTable(tableName);

        admin.close();
    }

    /**
     * 扫描获取数据 前包后不包[)
     * @throws IOException
     */
    @Test
    public void scan() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t1");
        Table table = conn.getTable(tableName);

        Scan scan = new Scan();

        scan.setStartRow(Bytes.toBytes("000010"));
        scan.setStopRow(Bytes.toBytes("000020"));

        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iterator = scanner.iterator();

        while (iterator.hasNext()){
            Result next = iterator.next();
            byte[] value = next.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(Bytes.toString(value));
        }

        conn.close();

    }

    /**
     * 动态扫描，带版本号
     * @throws IOException
     */
    @Test
    public void dynamicScan() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t1");
        Table table = conn.getTable(tableName);

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("000010"));
        scan.setStopRow(Bytes.toBytes("000011"));
        scan.setMaxVersions();

        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iterator = scanner.iterator();

        while (iterator.hasNext()){
            Result next = iterator.next();
            String row = Bytes.toString(next.getRow());
            System.out.println(row + "=====================================");
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = next.getMap();

            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap : map.entrySet()){
                String family = Bytes.toString(familyMap.getKey());
                System.out.println(family + "--------------------------");
                NavigableMap<byte[], NavigableMap<Long, byte[]>>  valueMap = familyMap.getValue();
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> columnAndValue: valueMap.entrySet()){
                    String columnName = Bytes.toString(columnAndValue.getKey());
                    System.out.println(columnName+"----------------------------");
                    NavigableMap<Long, byte[]> value = columnAndValue.getValue();

                    for(Map.Entry<Long, byte[]> timeAndValue : value.entrySet()){
                        Long time = timeAndValue.getKey();
                        byte[] v = timeAndValue.getValue();
                        if("id".equals(columnName)){
                            System.out.println(time + ":"+Bytes.toInt(v));
                        } else if("name".equals(columnName)){
                            System.out.println(time + ":"+ Bytes.toString(v));
                        }
                    }

                }

            }
        }
    }

    /**
     * 带版本号Get
     * @throws IOException
     */
    @Test
    public void getWithVersions() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t1");
        Table table = conn.getTable(tableName);

        Get get = new Get(Bytes.toBytes("000010"));
        get.setMaxVersions();

        Result result = table.get(get);

        List<Cell> columnCells = result.getColumnCells(Bytes.toBytes("f1"), Bytes.toBytes("name"));
        for(Cell cell : columnCells){
            String f = Bytes.toString(cell.getFamilyArray(), cell.getValueOffset(), cell.getFamilyLength());
            String c = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String v = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

            long ts = cell.getTimestamp();
            System.out.println(f+ "-" + c + "-" + v + "-" + ts);

        }


        conn.close();
    }

    @Test
    public void createTestTableAndData() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("ns5:t2");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

        HColumnDescriptor f1 = new HColumnDescriptor("f1");
        f1.setMaxVersions(3);
        hTableDescriptor.addFamily(f1);

        HColumnDescriptor f2 = new HColumnDescriptor("f2");
        f1.setMaxVersions(3);
        hTableDescriptor.addFamily(f2);

        admin.createTable(hTableDescriptor);

        Table table = conn.getTable(tableName);
        Put put = new Put(Bytes.toBytes("r1"));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("id"), Bytes.toBytes(1));
        put.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("name"), Bytes.toBytes("tom"));
        put.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("age"), Bytes.toBytes(12));
        put.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("addr"), Bytes.toBytes("beijing"));

        Put put2 = new Put(Bytes.toBytes("r2"));
        put2.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("id"), Bytes.toBytes(2));
        put2.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("name"), Bytes.toBytes("jack"));
        put2.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("age"), Bytes.toBytes(18));
        put2.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("addr"), Bytes.toBytes("tianjing"));

        Put put3 = new Put(Bytes.toBytes("r3"));
        put3.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("id"), Bytes.toBytes(3));
        put3.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("name"), Bytes.toBytes("marry"));
        put3.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("age"), Bytes.toBytes(21));
        put3.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("addr"), Bytes.toBytes("shanghai"));

        table.put(put);
        table.put(put2);
        table.put(put3);

        conn.close();


    }

    /**
     * 设置扫描器缓存和批量扫描
     * @throws IOException
     */
    @Test
    public void scanWithCacheAndBatch() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t2");

        Table table = conn.getTable(tableName);

        Scan scan = new Scan();
        scan.setCaching(2);
        scan.setBatch(3);

        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iterator = scanner.iterator();

        while(iterator.hasNext()){
            System.out.println("===============================================");
            Result next = iterator.next();

            byte[] row = next.getRow();
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = next.getMap();

            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap : map.entrySet()){
                byte[] family = familyMap.getKey();
                NavigableMap<byte[], NavigableMap<Long, byte[]>> colMap = familyMap.getValue();
                for(Map.Entry<byte[], NavigableMap<Long, byte[]>> colNameAndValue : colMap.entrySet()){
                    byte[] colName = colNameAndValue.getKey();
                    NavigableMap<Long, byte[]> timeAndValueMap = colNameAndValue.getValue();
                    for(Map.Entry<Long, byte[]> timeAndValue:timeAndValueMap.entrySet()){
                        //Long time = timeAndValue.getKey();
                        byte[] value = timeAndValue.getValue();
                        System.out.println(Bytes.toString(row)+"/"+Bytes.toString(family)+"/"+Bytes.toString(colName)+"/"+Bytes.toString(value));
                    }

                }
            }
        }
        conn.close();
    }

    /**
     * 行健过滤器
     * @throws IOException
     */
    @Test
    public void testRowFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t1");
        Table table = conn.getTable(tableName);

        Scan scan = new Scan();
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("000010")));
        scan.setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result next = iterator.next();
            List<Cell> columnCells = next.getColumnCells(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            for(Cell cell:columnCells){
                String v = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(v);
            }
        }
        conn.close();
    }

    /**
     * 列族过滤器
     * @throws IOException
     */
    @Test
    public void testFamilyFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t2");
        Table table = conn.getTable(tableName);

        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("f2")));

        Scan scan = new Scan();
        scan.setFilter(familyFilter);

        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){
            Result next = iterator.next();
            byte[] row = next.getRow();
            //System.out.println(Bytes.toString(row));
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = next.getMap();
            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap:map.entrySet()){
                byte[] family = familyMap.getKey();

                for(Map.Entry<byte[], NavigableMap<Long, byte[]>> valueMap : familyMap.getValue().entrySet()){
                    byte[] col = valueMap.getKey();
                    System.out.println(Bytes.toString(row)+"/"+Bytes.toString(family)+"/"+Bytes.toString(col));
                }
            }
        }
        conn.close();
    }

    /**
     * 列过滤器
     * @throws IOException
     */
    @Test
    public void testQualifierFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t2");
        Table table = conn.getTable(tableName);

        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("id")));
        Scan scan = new Scan();
        scan.setFilter(qualifierFilter);

        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();

        while (iterator.hasNext()){
            System.out.println("========================================");
            Result next = iterator.next();
            byte[] row = next.getRow();
            //System.out.println(Bytes.toString(row));
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = next.getMap();
            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap:map.entrySet()){
                byte[] family = familyMap.getKey();

                for(Map.Entry<byte[], NavigableMap<Long, byte[]>> valueMap : familyMap.getValue().entrySet()){
                    byte[] col = valueMap.getKey();
                    System.out.println(Bytes.toString(row)+"/"+Bytes.toString(family)+"/"+Bytes.toString(col));
                }
            }

        }

        conn.close();
    }

    /**
     * 值过滤器
     * @throws IOException
     */
    @Test
    public void testValueFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t2");
        Table table = conn.getTable(tableName);

        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("beijing")));

        Scan scan = new Scan();
        scan.setFilter(valueFilter);
        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){
            Result next = iterator.next();
            //除了rowid和addr，其他的值都是空的
            byte[] row = next.getRow();
            byte[] f1Id = next.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2Name = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            byte[] f2Age = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("age"));
            byte[] f2Addr = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("addr"));

            System.out.println("f2Addr="+Bytes.toString(f2Addr));
        }
        conn.close();
    }

    /**
     * 依赖过滤器DependentColumnFilter
     * dropDependentColumn为false，整行返回
     * dropDependentColumn为true，不返回作为条件的列
     * @throws IOException
     */
    @Test
    public void testDependentColumnFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t2");
        Table table = conn.getTable(tableName);

        DependentColumnFilter dependentColumnFilter = new DependentColumnFilter(Bytes.toBytes("f1"),
                Bytes.toBytes("id"),
                false,
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(2)));

        Scan scan = new Scan();
        scan.setFilter(dependentColumnFilter);

        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){
            Result next = iterator.next();
            byte[] row = next.getRow();
            byte[] f1Id = next.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2Name = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            byte[] f2Age = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("age"));
            byte[] f2Addr = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("addr"));

            System.out.println("rowId="+Bytes.toString(row)+",f1Id="+Bytes.toInt(f1Id)+",f2Name="+Bytes.toString(f2Name)+",f2Age="+Bytes.toInt(f2Age)+",f2Addr="+Bytes.toString(f2Addr));
        }
        conn.close();
    }

    /**
     * 单列值过滤器
     * @throws IOException
     */
    @Test
    public void testSingColumnValueFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t2");
        Table table = conn.getTable(tableName);

        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("f2"), Bytes.toBytes("addr"), CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("beijing")));
        Scan scan = new Scan();
        scan.setFilter(singleColumnValueFilter);

        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){
            Result next = iterator.next();
            byte[] row = next.getRow();
            byte[] f1Id = next.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2Name = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            byte[] f2Age = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("age"));
            byte[] f2Addr = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("addr"));

            System.out.println("rowId="+Bytes.toString(row)+",f1Id="+Bytes.toInt(f1Id)+",f2Name="+Bytes.toString(f2Name)+",f2Age="+Bytes.toInt(f2Age)+",f2Addr="+Bytes.toString(f2Addr));
        }
        conn.close();
    }

    /**
     * 排除查询条件的单列查询过滤器，返回值中不包含查询条件
     * @throws IOException
     */
    @Test
    public void testSingleColumnValueExcludeFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t2");
        Table table = conn.getTable(tableName);

        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes("f2"), Bytes.toBytes("addr"), CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("beijing")));
        Scan scan = new Scan();
        scan.setFilter(singleColumnValueExcludeFilter);

        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){
            Result next = iterator.next();
            byte[] row = next.getRow();
            byte[] f1Id = next.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2Name = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            byte[] f2Age = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("age"));
            byte[] f2Addr = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("addr"));

            System.out.println("rowId="+Bytes.toString(row)+",f1Id="+Bytes.toInt(f1Id)+",f2Name="+Bytes.toString(f2Name)+",f2Age="+Bytes.toInt(f2Age)+",f2Addr="+Bytes.toString(f2Addr));
        }
        conn.close();
    }

    /**
     * 前缀过滤器，是rowKey过滤器
     * @throws IOException
     */
    @Test
    public void testPrefixFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t2");
        Table table = conn.getTable(tableName);

        PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes("r1"));

        Scan scan = new Scan();
        scan.setFilter(prefixFilter);

        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){
            Result next = iterator.next();
            byte[] row = next.getRow();
            byte[] f1Id = next.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2Name = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            byte[] f2Age = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("age"));
            byte[] f2Addr = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("addr"));

            System.out.println("rowId="+Bytes.toString(row)+",f1Id="+Bytes.toInt(f1Id)+",f2Name="+Bytes.toString(f2Name)+",f2Age="+Bytes.toInt(f2Age)+",f2Addr="+Bytes.toString(f2Addr));
        }
        conn.close();
    }

    /**
     * 分页过滤器，是rowkey过滤，是在每个region上分页，如果有3个region就会返回30个数据
     * @throws IOException
     */
    @Test
    public void testPageFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t1");
        Table table = conn.getTable(tableName);

        PageFilter pageFilter = new PageFilter(10);


        Scan scan = new Scan();
        scan.setFilter(pageFilter);

        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result next = iterator.next();
            byte[] value = next.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(Bytes.toString(value));
        }
        conn.close();

    }

    /**
     * KeyOnlyFilter只获取列族和列和时间戳的信息而不会获取对应value的信息
     * @throws IOException
     */
    @Test
    public void testKeyOnlyFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t2");
        Table table = conn.getTable(tableName);

        KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();

        Scan scan = new Scan();
        scan.setFilter(keyOnlyFilter);

        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();

        while (iterator.hasNext()){
            Result next = iterator.next();
            List<Cell> columnCells = next.getColumnCells(Bytes.toBytes("f2"), Bytes.toBytes("addr"));
            for(Cell cell : columnCells){
                String f = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String c = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String v = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                long ts = cell.getTimestamp();
                System.out.println(f+"-"+c+"-"+v+"-"+ts);
            }
        }
        conn.close();
    }

    /**
     * 列分页过滤器，假如一行里所有列族下共有5个列，limit 2，offset 2，就代表取第三个和第四个列
     * @throws IOException
     */
    @Test
    public void testColumnPaginationFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t2");
        Table table = conn.getTable(tableName);

        ColumnPaginationFilter columnPaginationFilter = new ColumnPaginationFilter(2, 2);
        Scan scan = new Scan();
        scan.setFilter(columnPaginationFilter);

        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            System.out.println("========================================");
            Result next = iterator.next();
            byte[] row = next.getRow();
            //System.out.println(Bytes.toString(row));
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = next.getMap();
            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap:map.entrySet()){
                byte[] family = familyMap.getKey();

                for(Map.Entry<byte[], NavigableMap<Long, byte[]>> valueMap : familyMap.getValue().entrySet()){
                    byte[] col = valueMap.getKey();
                    System.out.println(Bytes.toString(row)+"/"+Bytes.toString(family)+"/"+Bytes.toString(col));
                }
            }
        }
        conn.close();
    }

    /**
     * 复杂条件查询
     * select * from user where (age >= 13 and name like 'tome%') or addr like 'beijing%'
     * @throws IOException
     */
    @Test
    public void testComboFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("ns5:t2");
        Table table = conn.getTable(tableName);

        SingleColumnValueFilter f1 = new SingleColumnValueFilter(Bytes.toBytes("f2"), Bytes.toBytes("age"), CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(13)));
        SingleColumnValueFilter f2 = new SingleColumnValueFilter(Bytes.toBytes("f2"), Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^tom"));

        FilterList ft1 = new FilterList(FilterList.Operator.MUST_PASS_ALL, f1, f2);

        SingleColumnValueFilter f3 = new SingleColumnValueFilter(Bytes.toBytes("f2"), Bytes.toBytes("addr"), CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^beijing"));

        FilterList ft2 = new FilterList(FilterList.Operator.MUST_PASS_ONE, ft1, f3);

        Scan scan = new Scan();
        scan.setFilter(ft2);

        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){
            Result next = iterator.next();
            byte[] row = next.getRow();
            byte[] f1Id = next.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2Name = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            byte[] f2Age = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("age"));
            byte[] f2Addr = next.getValue(Bytes.toBytes("f2"), Bytes.toBytes("addr"));

            System.out.println("rowId="+Bytes.toString(row)+",f1Id="+Bytes.toInt(f1Id)+",f2Name="+Bytes.toString(f2Name)+",f2Age="+Bytes.toInt(f2Age)+",f2Addr="+Bytes.toString(f2Addr));
        }
        conn.close();
    }

    /**
     * hbase计数器
     * @throws IOException
     */
    @Test
    public void testIncr() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("ns5:t3");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("f1");
        hTableDescriptor.addFamily(columnDescriptor);

        admin.createTable(hTableDescriptor);

        Table table = conn.getTable(tableName);

        Increment r1 = new Increment(Bytes.toBytes("r1"));
        r1.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("click"), 1);
        table.increment(r1);

        conn.close();
    }
}
