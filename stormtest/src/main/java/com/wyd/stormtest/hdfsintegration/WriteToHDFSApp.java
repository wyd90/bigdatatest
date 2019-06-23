package com.wyd.stormtest.hdfsintegration;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;


public class WriteToHDFSApp {
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");

        SyncPolicy syncPolicy = new CountSyncPolicy(100);

        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/storm-hdfs/");

        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://node2:9000")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("datasourceSpout", new DataSourceSpout(), 2).setNumTasks(2);
        builder.setBolt("hdfsBolt", hdfsBolt, 1).shuffleGrouping("datasourceSpout").setNumTasks(1);

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("hdfstest", conf, builder.createTopology());
    }
}
