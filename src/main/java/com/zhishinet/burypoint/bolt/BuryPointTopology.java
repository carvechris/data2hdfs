package com.zhishinet.burypoint.bolt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.zhishinet.MyConfig;
import com.zhishinet.Utils;
import com.zhishinet.burypoint.Field;
import com.zhishinet.burypoint.LoginBuryPoint;
import org.apache.commons.lang.StringUtils;
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
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class BuryPointTopology {

    public static final String TOPIC = "LoginBuryPoint";
    public static final String SPOUTID = "loginburypointstorm";
    public static final String TOPOLOGY_NAME = "BuryPointTopology";
    private final static ObjectMapper mapper = new ObjectMapper();

    public static class SplitDataBolt extends BaseRichBolt {

        private OutputCollector outputCollector;
        private final static Logger logger = LoggerFactory.getLogger(SplitDataBolt.class);

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {

            final String json = tuple.getString(0);
            LoginBuryPoint log = null;
            try {
                log = mapper.readValue(json,LoginBuryPoint.class);
            } catch (IOException e) {
                logger.error("Convert json to object BuryPointTopology errror",e);
                this.outputCollector.fail(tuple);
            }
            if(!Objects.isNull(log)) {
                Values values = new Values();

                values.add((!Objects.isNull(log.getUserid())) ? log.getUserid() : "\\N");
                values.add(StringUtils.isNotBlank(log.getOpenid()) ? log.getOpenid() : "\\N");
                values.add(StringUtils.isNotBlank(log.getUsername()) ? log.getUsername() : "\\N");
                values.add(StringUtils.isNotBlank(log.getFullname()) ? log.getFullname() : "\\N");
                values.add(StringUtils.isNotBlank(log.getUserType()) ? log.getUserType() : "\\N");
                values.add(StringUtils.isNotBlank(log.getOsName()) ? log.getOsName() : "\\N");
                values.add(StringUtils.isNotBlank(log.getOsVersion()) ? log.getOsVersion() : "\\N");
                values.add(StringUtils.isNotBlank(log.getIp()) ? log.getIp() : "\\N");
                values.add(StringUtils.isNotBlank(log.getPlatform()) ? log.getPlatform() : "\\N");
                values.add(StringUtils.isNotBlank(log.getAppName()) ? log.getAppName() : "\\N");
                values.add(StringUtils.isNotBlank(log.getAction()) ? log.getAction() : "\\N");
                values.add(!Objects.isNull(log.getActionTime()) ? Utils.formatDate2String(log.getActionTime()) : "\\N");
                values.add(StringUtils.isNotBlank(log.getNetType()) ? log.getNetType() : "\\N");
                values.add(StringUtils.isNotBlank(log.getLanguage()) ? log.getLanguage() : "\\N");
                values.add((!Objects.isNull(log.getSuccess())) ? log.getSuccess() : "\\N");
                values.add(StringUtils.isNotBlank(log.getErrorCode()) ? log.getErrorCode() : "\\N");
                values.add(!Objects.isNull(log.getCreatedOn()) ? Utils.formatDate2String(log.getCreatedOn()) : "\\N");

                this.outputCollector.ack(tuple);
                this.outputCollector.emit(values);

            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(Field.kafkaMessageFields);
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        SpoutConfig spoutConfig = MyConfig.getKafkaSpoutConfig(TOPIC, MyConfig.ZK_HOSTS,MyConfig.ZK_ROOT, SPOUTID);

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(MyConfig.FIELD_DELIMITER);
        SyncPolicy syncPolicy = new CountSyncPolicy(1);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(MyConfig.FILE_SIZE, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/tmp/storm/BuryPoint/").withExtension(".txt");
        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl(MyConfig.HDFS_URL).withFileNameFormat(fileNameFormat)
                .withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout",new KafkaSpout(spoutConfig),3);
        builder.setBolt("splitDataBolt",new SplitDataBolt(),3).shuffleGrouping("kafkaSpout");
        builder.setBolt("hdfsBolt",hdfsBolt,3).shuffleGrouping("splitDataBolt");

        Config config = MyConfig.getConfigWithKafkaConsumerProps(false,MyConfig.KAFKA_BROKERS);

        if(null != args && args.length > 0) {
//            config.setNumWorkers(3);
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
        }
    }

}



