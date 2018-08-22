package com.zhishinet.sms;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
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

public class UBUserSMSLogTopology {

    public static StormTopology createTopology(){

        // 每1000个tuple同步到hdfs一次
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);
        // 每个文件的大小为100M
        FileRotationPolicy policy = new FileSizeRotationPolicy(100.0f,FileSizeRotationPolicy.Units.MB);
        // 设置输出目录
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/apps/hive/warehouse/busdata.db/ubusersmslog").withPrefix("ubusersmslog_").withExtension(".txt");
        // 执行HDFS地址
        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://localhost:8020")
                .withFileNameFormat(fileNameFormat).withRotationPolicy(policy).withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("ubusersmslogspout", UBUserSMSLogSpout.getSpout());
        builder.setBolt("dataprocess",new UBUserSMSLogDataProcessBolt()).shuffleGrouping("ubusersmslog_storm");
        builder.setBolt("hdfsbolt", hdfsBolt).shuffleGrouping("dataprocess");
        return builder.createTopology();
    }

    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);
        StormTopology topology = createTopology();
        config.setNumWorkers(1);
        config.setMaxTaskParallelism(2);

        //提交集群模式
//        try {
//            StormSubmitter.submitTopology("mywordcount", config, topology);
//        } catch (AlreadyAliveException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        } catch (InvalidTopologyException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
        //本地运行模式
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("UBUserSMSLogTopology", config, topology);
        Thread.sleep(1000);
    }
}
