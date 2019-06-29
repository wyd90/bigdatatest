package com.wyd.spark.javaclient;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SqlDemo {
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession.builder().appName("Java Spark Sql").master("local[*]").getOrCreate();
        Dataset<String> datas = spark.read().textFile("");
        JavaRDD<Person> personJavaRDD = datas.javaRDD().map(line -> {
            String[] parts = line.split(",");
            Person person = new Person();
            person.setId(Long.valueOf(parts[0]));
            person.setName(parts[1]);
            person.setAge(Integer.valueOf(parts[2]));
            return person;
        });

        Dataset<Row> df = spark.createDataFrame(personJavaRDD, Person.class);

        df.createTempView("t_person");

        Dataset<Row> sql = spark.sql("select * from t_person");

        sql.show();

    }
}
