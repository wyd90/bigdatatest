package com.wyd.hbase.coprocessor.util;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class DynamicCallUtil {
    /**
     *给表动态加载协处理器
     * @param connection
     * @param table
     * @param jarPath
     * @param cls
     */
    public static void setupToExistTable(Connection connection, Table table, String jarPath, Class<?>... cls) {
        try {
            if(jarPath != null && !jarPath.isEmpty()) {
                Path path = new Path(jarPath);
                HTableDescriptor hTableDescriptor = table.getTableDescriptor();
                for(Class cass : cls) {
                    hTableDescriptor.addCoprocessor(cass.getCanonicalName(), path, Coprocessor.PRIORITY_USER, null);
                }
                connection.getAdmin().modifyTable(table.getName(), hTableDescriptor);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除HBase表上的协处理器
     * @param connection
     * @param table
     * @param cls
     */
    public static void deleteCoprocessor(Connection connection, Table table, Class<?>... cls) {
        System.out.println("begin delete " + table.getName().toString() + " Coprocessor......");
        try {
            HTableDescriptor hTableDescriptor = table.getTableDescriptor();
            for(Class cass : cls) {
                hTableDescriptor.removeCoprocessor(cass.getCanonicalName());
            }
            connection.getAdmin().modifyTable(table.getName(), hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end delete " + table.getName().toString() + " Coprocessor......");
    }
}
