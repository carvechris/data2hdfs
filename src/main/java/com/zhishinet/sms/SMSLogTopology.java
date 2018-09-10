package com.zhishinet.sms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhishinet.MyConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.kafka.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.Map;

public class SMSLogTopology {


    public static class SMSLogBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            final String json = tuple.getString(0);
            ObjectMapper objectMapper = new ObjectMapper();
            UBUserSMSLog log = null;
            try {
                log = objectMapper.readValue(json,UBUserSMSLog.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.collector.ack(tuple);
            this.collector.emit(new Values(log.getId(),log.getKey(),log.getMobilePhoneNo(),log.getCode(),log.getState(),log.getReturnMsg(),log.getPostTime(),log.getCreatedOn()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id","key","mobilePhoneNo","code","state","returnMsg","postTime","createdOn"));
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        final String topic = "UBUserSMSLog";
        final String spoutId = "ubusersmslog_storm";
        SpoutConfig spoutConfig = MyConfig.getKafkaSpoutConfig(topic, MyConfig.ZK_HOSTS,MyConfig.ZK_ROOT,spoutId);

        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("\001");
        // sync the filesystem after every 100 tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(100);
        // rotate files when they reach 1MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/tmp/").withExtension(".txt");
        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl(MyConfig.HDFS_URL)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("MyConfig",new KafkaSpout(spoutConfig),10);
        builder.setBolt("SMSLogBolt",new SMSLogBolt()).shuffleGrouping("MyConfig");
        builder.setBolt("HdfsBolt",bolt).shuffleGrouping("SMSLogBolt");

        Config config = MyConfig.getConfigWithKafkaConsumerProps(false,MyConfig.KAFKA_BROKERS);

        if(null != args && args.length > 0) {
            StormSubmitter.submitTopology("SMSLogTopology", config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("SMSLogTopology",config,builder.createTopology());
        }
    }

}



