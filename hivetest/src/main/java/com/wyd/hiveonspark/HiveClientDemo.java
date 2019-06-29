package com.wyd.hiveonspark;

import java.sql.*;

public class HiveClientDemo {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection("jdbc:hive2://node1:10000", "root", "");
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("select count(1) cnts from t_web_log group by day");
        while (resultSet.next()){
            System.out.println(resultSet.getLong(1));
        }
        resultSet.close();
        statement.close();
        conn.close();
    }
}
