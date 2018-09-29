package com.zhishinet.burypoint.trient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.zhishinet.MyConfig;
import com.zhishinet.burypoint.Field;
import com.zhishinet.burypoint.LoginBuryPoint;
import com.zhishinet.storm.ZhishinetTridentFileNameFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class BuryPointTopology {

    public static final String TOPIC = "LoginBuryPoint";
    public static final String SPOUTID = "loginburypointstorm";
    public static final String TOPOLOGY_NAME = "BuryPointTopology";
    private final static ObjectMapper mapper = new ObjectMapper();

    public static class SplitData extends BaseFunction {
        private static final Logger logger = LoggerFactory.getLogger(SplitData.class);
        @Override
        public void execute(TridentTuple tuple, TridentCollector outputCollector) {
            final String json = tuple.getString(0);
            LoginBuryPoint log = null;
            try {
                log = mapper.readValue(json,LoginBuryPoint.class);
            } catch (IOException e) {
                logger.error("Convert json to object BuryPointTopology errror",e);
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
                values.add(StringUtils.isNotBlank(log.getActionTime()) ? log.getActionTime() : "\\N");
                values.add(StringUtils.isNotBlank(log.getNetType()) ? log.getNetType() : "\\N");
                values.add(StringUtils.isNotBlank(log.getLanguage()) ? log.getLanguage() : "\\N");
                values.add((!Objects.isNull(log.getSuccess())) ? log.getSuccess() : "\\N");
                values.add(StringUtils.isNotBlank(log.getErrorCode()) ? log.getErrorCode() : "\\N");
                values.add(StringUtils.isNotBlank(log.getCreatedOn()) ? log.getCreatedOn() : "\\N");

                outputCollector.emit(values);
            }
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        RecordFormat recordFormat = new DelimitedRecordFormat().withFieldDelimiter("\001");
        // rotate files when they reach 128MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(MyConfig.FILE_SIZE, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new ZhishinetTridentFileNameFormat().withPath("/user/storm/BuryPoint/").withExtension(".txt");

        HdfsState.Options options = new HdfsState.HdfsFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(rotationPolicy)
                .withFsUrl(MyConfig.HDFS_URL);

        StateFactory factory = new HdfsStateFactory().withOptions(options);

        TridentTopology topology = new TridentTopology();
        topology.newStream("MyConfig",new TransactionalTridentKafkaSpout(MyConfig.getTridentKafkaConfig(TOPIC, MyConfig.ZK_HOSTS,SPOUTID))).parallelismHint(3)
                .each(new Fields("str"),new SplitData(), Field.kafkaMessageFields).parallelismHint(3)
                .partitionPersist(factory, Field.kafkaMessageFields, new HdfsUpdater(), new Fields()).parallelismHint(3);

        Config config = MyConfig.getConfigWithKafkaConsumerProps(false,MyConfig.KAFKA_BROKERS);
        if(null != args && args.length > 0) {
//            config.setNumWorkers(3);
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, topology.build());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME,config,topology.build());
        }
    }
}
