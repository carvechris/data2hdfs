package com.zhishinet.sms.trident;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.zhishinet.MyConfig;
import com.zhishinet.Utils;
import com.zhishinet.sms.Field;
import com.zhishinet.sms.UBUserSMSLog;
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

public class SMSLogTopology {

    public static final String TOPIC = "UBUserSMSLog";
    public static final String SPOUTID = "ubusersmslogstorm";
    public static final String TOPOLOGY_NAME = "SMSLogTopology";
    private final static ObjectMapper mapper = new ObjectMapper();

    public static class SplitData extends BaseFunction {
        private static final Logger logger = LoggerFactory.getLogger(SplitData.class);
        @Override
        public void execute(TridentTuple tuple, TridentCollector outputCollector) {
            final String json = tuple.getString(0);
            UBUserSMSLog log = null;
            try {
                log = mapper.readValue(json,UBUserSMSLog.class);
            } catch (IOException e) {
                logger.error("Convert json to object UBUserSMSLog errror",e);
            }
            if(!Objects.isNull(log)) {
                Values values = new Values();

                if(null == log.getId() || log.getId() <= 0) {
                    logger.error("The message from kafka id is inValidate : {}", json);
                }
                values.add(log.getId());

                if(StringUtils.isBlank(log.getKey())) {
                    logger.error("The message from kafka key is inValidate : {}", json);
                }
                values.add(log.getKey());

                if(StringUtils.isBlank(log.getMobilePhoneNo())) {
                    logger.error("The message from kafka mobilePhoneNo is inValidate : {}", json);
                }
                values.add(log.getMobilePhoneNo());

                if(StringUtils.isBlank(log.getCode())) {
                    logger.error("The message from kafka code is inValidate : {}", json);
                }
                values.add(log.getCode());

                if(null == log.getState() || log.getState() <= 0) {
                    logger.error("The message from kafka state is inValidate : {}", json);
                }
                values.add(log.getState());

                values.add(StringUtils.isNotBlank(log.getReturnMsg()) ? log.getReturnMsg() : "\\N");
                values.add(StringUtils.isNotBlank(log.getPostTime()) ? log.getPostTime() : "\\N");
                values.add(null != log.getCreatedOn() ? Utils.formatDate2String(log.getCreatedOn()) : "\\N");
                values.add((!Objects.isNull(log.getCreatedBy())) ? log.getCreatedBy() : "\\N");
                values.add(null != log.getModifiedOn() ? Utils.formatDate2String(log.getModifiedOn()) : "\\N");
                values.add((!Objects.isNull(log.getModifiedBy())) ? log.getModifiedBy() : "\\N");
                values.add(null != log.getDeletedOn() ? Utils.formatDate2String(log.getDeletedOn()) : "\\N");
                values.add((!Objects.isNull(log.getDeletedBy())) ? log.getDeletedBy() : "\\N");
                values.add(log.isDeleted());
                values.add(StringUtils.isNotBlank(log.getOpenId()) ? log.getOpenId(): "\\N");
                outputCollector.emit(values);
            }
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        RecordFormat recordFormat = new DelimitedRecordFormat().withFieldDelimiter("\001");
        // rotate files when they reach 128MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(MyConfig.FILE_SIZE, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new ZhishinetTridentFileNameFormat().withPath("/user/storm/UserSMSLog/").withExtension(".txt");

        HdfsState.Options options = new HdfsState.HdfsFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(rotationPolicy)
                .withFsUrl(MyConfig.HDFS_URL);

        StateFactory factory = new HdfsStateFactory().withOptions(options);

        TridentTopology topology = new TridentTopology();
        topology.newStream("MyConfig",new TransactionalTridentKafkaSpout(MyConfig.getTridentKafkaConfig(TOPIC, MyConfig.ZK_HOSTS,SPOUTID))).parallelismHint(3)
                .each(new Fields("str"),new SplitData(),Field.kafkaMessageFields).parallelismHint(3)
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
