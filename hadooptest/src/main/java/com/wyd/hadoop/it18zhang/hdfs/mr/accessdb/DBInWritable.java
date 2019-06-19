package com.wyd.hadoop.it18zhang.hdfs.mr.accessdb;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBInWritable implements DBWritable, Writable {

    private Long id;

    private String name;

    private String txt;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTxt() {
        return txt;
    }

    public void setTxt(String txt) {
        this.txt = txt;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(txt);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readLong();
        this.name = dataInput.readUTF();
        this.txt = dataInput.readUTF();
    }

    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setLong(1, id);
        preparedStatement.setString(2, name);
        preparedStatement.setString(3, txt);
    }

    public void readFields(ResultSet resultSet) throws SQLException {
        id = resultSet.getLong(1);
        name = resultSet.getString(2);
        txt = resultSet.getString(3);
    }
}
