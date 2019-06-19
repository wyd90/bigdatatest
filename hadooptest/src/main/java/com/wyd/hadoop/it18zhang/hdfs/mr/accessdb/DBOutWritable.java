package com.wyd.hadoop.it18zhang.hdfs.mr.accessdb;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBOutWritable implements DBWritable,Writable {

    private String word;

    private Integer cnts;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCnts() {
        return cnts;
    }

    public void setCnts(Integer cnts) {
        this.cnts = cnts;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeInt(cnts);
    }

    public void readFields(DataInput dataInput) throws IOException {
        word = dataInput.readUTF();
        cnts = dataInput.readInt();
    }

    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, word);
        preparedStatement.setInt(2, cnts);
    }

    public void readFields(ResultSet resultSet) throws SQLException {
        word = resultSet.getString(1);
        cnts = resultSet.getInt(2);
    }
}
