package com.hand.zhishinet.assessment.bolt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.zhishinet.MyConfig;
import com.hand.zhishinet.assessment.Field;
import com.hand.zhishinet.assessment.vo.UBHomeworkSessionUserTracking;
import com.zhishinet.Utils;
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

public class UBHomeworkSessionUserTrackingTopology {
    public static final String TOPIC = "UBHomeworkSessionUserTracking";
    public static final String SPOUTID = "ubhomeworksessionusertrackingstorm";
    public static final String TOPOLOGY_NAME = "UBHomeworkSessionUserTrackingTopology";

    public static class UBHomeworkSessionUserTrackingBolt extends BaseRichBolt {
        private OutputCollector collector;
        private final static Logger logger = LoggerFactory.getLogger(UBHomeworkSessionUserTrackingBolt.class);

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            final String json = tuple.getString(0);
            ObjectMapper mapper = new ObjectMapper();
            UBHomeworkSessionUserTracking sessionUserTracking = null;
            try {
                sessionUserTracking = mapper.readValue(json,UBHomeworkSessionUserTracking.class);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("The message from kafka, the data is {}", e.getMessage());
                logger.error("The message from kafka transfer to UBHomeworkSessionUserTracking error: {}", e.getMessage());
            }
            if (Objects.isNull(sessionUserTracking)) {
                this.collector.fail(tuple);
            } else {
                Values values = new Values();
                if (sessionUserTracking.getHomeworkSessionUserTrackingId() == null) {
                    logger.error("The message from kafka homeworkSessionUserTrackingId is inValidate : {}", json);
                    this.collector.fail(tuple);
                }
                values.add(sessionUserTracking.getHomeworkSessionUserTrackingId());
                if (sessionUserTracking.getSessionId() == null) {
                    logger.error("The message from kafka sessionId is inValidate : {}", json);
                    this.collector.fail(tuple);
                }
                values.add(sessionUserTracking.getSessionId());
                if (sessionUserTracking.getHomeworkAssessmentId() == null) {
                    logger.error("The message from kafka homeworkAssessmentId is inValidate : {}", json);
                    this.collector.fail(tuple);
                }
                values.add(sessionUserTracking.getHomeworkAssessmentId());
                if (sessionUserTracking.getUserId() == null) {
                    logger.error("The message from kafka userId is inValidate : {}", json);
                    this.collector.fail(tuple);

                }
                values.add(sessionUserTracking.getUserId());
                values.add(!Objects.isNull(sessionUserTracking.getNoOfVisits()) ? sessionUserTracking.getNoOfVisits() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getLastViewedOn()) ? Utils.formatDate2String(sessionUserTracking.getLastViewedOn()) : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getStatusId()) ? sessionUserTracking.getStatusId() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getCompletedOn()) ? Utils.formatDate2String(sessionUserTracking.getCompletedOn()) : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getScore()) ? sessionUserTracking.getScore() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getPercentScore()) ? sessionUserTracking.getPercentScore() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getCompleteAttempts()) ? sessionUserTracking.getCompleteAttempts() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getBeginDate()) ? Utils.formatDate2String(sessionUserTracking.getBeginDate()) : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getEndDate()) ? Utils.formatDate2String(sessionUserTracking.getEndDate()) : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getTimeSpent()) ? sessionUserTracking.getTimeSpent() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getInteractionTimer()) ? sessionUserTracking.getInteractionTimer() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getEmendStatus()) ? sessionUserTracking.getEmendStatus() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getRequiredEmend()) ? sessionUserTracking.getRequiredEmend() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getSubjectId()) ? sessionUserTracking.getSubjectId() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getReadCount()) ? sessionUserTracking.getReadCount() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getShowSubTitle()) ? sessionUserTracking.getShowSubTitle() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getEmendTypeCode()) ? sessionUserTracking.getEmendTypeCode() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getSessionGroupId()) ? sessionUserTracking.getSessionGroupId() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getDisplayOrder()) ? sessionUserTracking.getDisplayOrder() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getCreatedOn()) ? Utils.formatDate2String(sessionUserTracking.getCreatedOn()) : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getCreatedBy()) ? sessionUserTracking.getCreatedBy() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getModifiedOn()) ? Utils.formatDate2String(sessionUserTracking.getModifiedOn()) : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getModifiedBy()) ? sessionUserTracking.getModifiedBy() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getDeletedOn()) ? Utils.formatDate2String(sessionUserTracking.getDeletedOn()) : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getDeletedBy()) ? sessionUserTracking.getDeletedBy() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getDeleted()) ? sessionUserTracking.getDeleted() : "\\N");

                values.add(!Objects.isNull(sessionUserTracking.getHomeworkType()) ? sessionUserTracking.getHomeworkType() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getStandardLevel()) ? sessionUserTracking.getStandardLevel() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getStandardConf()) ? sessionUserTracking.getStandardConf() : "\\N");
                values.add(!Objects.isNull(sessionUserTracking.getOcrErrorMsg()) ? sessionUserTracking.getOcrErrorMsg() : "\\N");

                this.collector.ack(tuple);
                this.collector.emit(values);
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(Field.getHomeworkSessionUserTrackingFields());
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        SpoutConfig spoutConfig = MyConfig.getKafkaSpoutConfig(TOPIC, MyConfig.ZK_HOSTS,MyConfig.ZK_ROOT, SPOUTID);

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(MyConfig.FIELD_DELIMITER);
        SyncPolicy syncPolicy = new CountSyncPolicy(MyConfig.COUNT_SYNC_POLICY);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(MyConfig.FILE_SIZE, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/user/storm/HomeworkSessionUserTracking/").withExtension(".txt");
        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl(MyConfig.HDFS_URL).withFileNameFormat(fileNameFormat)
                .withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout",new KafkaSpout(spoutConfig));
        builder.setBolt("homeworkSessionUserTrackingBolt",new UBHomeworkSessionUserTrackingBolt()).shuffleGrouping("kafkaSpout");
        builder.setBolt("hdfsBolt",hdfsBolt).shuffleGrouping("homeworkSessionUserTrackingBolt");

        Config config = MyConfig.getConfigWithKafkaConsumerProps(false,MyConfig.KAFKA_BROKERS);

        if(null != args && args.length > 0) {
            //config.setNumWorkers(3);
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
        }
    }
}
