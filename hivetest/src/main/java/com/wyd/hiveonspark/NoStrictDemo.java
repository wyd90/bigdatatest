package com.wyd.hiveonspark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class NoStrictDemo {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection("jdbc:hive2://node1:10000", "root", "");

        Statement st = conn.createStatement();
        st.execute("create table mydb.t_employee(id int,name string,age int,state string,city string) row format delimited fields terminated by ','");

        st.execute("load data inpath 'student.dat' into table mydb.t_employee");

        st.execute("SET hive.exec.dynamic.partition=true");
        st.execute("SET hive.exec.dynamic.partition.mode=nonstrict");

        st.execute("create table mydb.t_user_depart(id int,name string,age int) partitioned by(sta string, ct string) row format delimited fields terminated by ','");

        st.execute("insert into table mydb.t_user_depart partition(sta,ct) select id,name,age,state,city from mydb.t_employee");

        st.close();
        conn.close();
    }
}
