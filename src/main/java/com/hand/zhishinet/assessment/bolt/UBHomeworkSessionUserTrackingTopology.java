package com.hand.zhishinet.assessment.bolt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.zhishinet.MyConfig;
import com.hand.zhishinet.assessment.Field;
import com.hand.zhishinet.assessment.vo.UBHomeworkSessionUserTracking;
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
            UBHomeworkSessionUserTracking log = null;
            try {
                log = mapper.readValue(json, UBHomeworkSessionUserTracking.class);
            } catch (IOException e) {
                logger.error("The message from kafka, the data is {}", e.getMessage());
                logger.error("The message from kafka transfer to UBHomeworkSessionUserTracking error: {}", e.getMessage());
            }
            if (Objects.isNull(log)) {
                this.collector.fail(tuple);
            } else {
                Values values = new Values();
                if (log.getHomeworkSessionUserTrackingId() == null) {
                    logger.error("The message from kafka homeworkSessionUserTrackingId is inValidate : {}", json);
                    this.collector.fail(tuple);
                    throw new IllegalArgumentException("The message from kafka homeworkSessionUserTrackingId is inValidate ");
                }
                values.add(log.getHomeworkSessionUserTrackingId());
                if (log.getSessionId() == null) {
                    logger.error("The message from kafka sessionId is inValidate : {}", json);
                    this.collector.fail(tuple);
                    throw new IllegalArgumentException("The message from kafka sessionId is inValidate ");
                }
                values.add(log.getSessionId());
                if (log.getHomeworkAssessmentId() == null) {
                    logger.error("The message from kafka homeworkAssessmentId is inValidate : {}", json);
                    this.collector.fail(tuple);
                    throw new IllegalArgumentException("The message from kafka homeworkAssessmentId is inValidate ");
                }
                values.add(log.getHomeworkAssessmentId());
                if (log.getUserId() == null) {
                    logger.error("The message from kafka userId is inValidate : {}", json);
                    this.collector.fail(tuple);
                    throw new IllegalArgumentException("The message from kafka userId is inValidate ");
                }
                values.add(log.getUserId());
                this.collector.ack(tuple);
                values.add(!Objects.isNull(log.getNoOfVisits()) ? log.getNoOfVisits() : "\\N");
                values.add(!Objects.isNull(log.getLastViewedOn()) ? log.getLastViewedOn() : "\\N");
                values.add(!Objects.isNull(log.getStatusId()) ? log.getStatusId() : "\\N");
                values.add(!Objects.isNull(log.getCompletedOn()) ? log.getCompletedOn() : "\\N");
                values.add(!Objects.isNull(log.getScore()) ? log.getScore() : "\\N");
                values.add(!Objects.isNull(log.getPercentScore()) ? log.getPercentScore() : "\\N");
                values.add(!Objects.isNull(log.getCompleteAttempts()) ? log.getCompleteAttempts() : "\\N");
                values.add(!Objects.isNull(log.getBeginDate()) ? log.getBeginDate() : "\\N");
                values.add(!Objects.isNull(log.getEndDate()) ? log.getEndDate() : "\\N");
                values.add(!Objects.isNull(log.getTimeSpent()) ? log.getTimeSpent() : "\\N");
                values.add(!Objects.isNull(log.getInteractionTimer()) ? log.getInteractionTimer() : "\\N");
                values.add(!Objects.isNull(log.getArticleLocation()) ? log.getArticleLocation() : "\\N");
                values.add(!Objects.isNull(log.getLocation()) ? log.getLocation() : "\\N");
                values.add(!Objects.isNull(log.getChecked()) ? log.getChecked() : "\\N");
                values.add(!Objects.isNull(log.getForLearnerStatus()) ? log.getForLearnerStatus() : "\\N");
                values.add(!Objects.isNull(log.getQuestionIndexs()) ? log.getQuestionIndexs() : "\\N");
                values.add(!Objects.isNull(log.getEmendStatus()) ? log.getEmendStatus() : "\\N");
                values.add(!Objects.isNull(log.getRequiredEmend()) ? log.getRequiredEmend() : "\\N");
                values.add(!Objects.isNull(log.getSubjectId()) ? log.getSubjectId() : "\\N");
                values.add(!Objects.isNull(log.getReadCount()) ? log.getReadCount() : "\\N");
                values.add(!Objects.isNull(log.getShowSubTitle()) ? log.getShowSubTitle() : "\\N");
                values.add(!Objects.isNull(log.getEmendTypeCode()) ? log.getEmendTypeCode() : "\\N");
                values.add(!Objects.isNull(log.getSessionGroupId()) ? log.getSessionGroupId() : "\\N");
                values.add(!Objects.isNull(log.getDisplayOrder()) ? log.getDisplayOrder() : "\\N");
                values.add(!Objects.isNull(log.getCreatedOn()) ? log.getCreatedOn() : "\\N");
                values.add(!Objects.isNull(log.getCreatedBy()) ? log.getCreatedBy() : "\\N");
                values.add(!Objects.isNull(log.getModifiedOn()) ? log.getModifiedOn() : "\\N");
                values.add(!Objects.isNull(log.getModifiedBy()) ? log.getModifiedBy() : "\\N");
                values.add(!Objects.isNull(log.getDeletedOn()) ? log.getDeletedOn() : "\\N");
                values.add(!Objects.isNull(log.getDeletedBy()) ? log.getDeletedBy() : "\\N");
                values.add(!Objects.isNull(log.getDeleted()) ? log.getDeleted() : "\\N");
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

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("\001");
        SyncPolicy syncPolicy = new CountSyncPolicy(100);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(128f, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/user/storm/HomeworkSessionUserTracking/").withExtension(".txt");
        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl(MyConfig.HDFS_URL).withFileNameFormat(fileNameFormat)
                .withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout",new KafkaSpout(spoutConfig),3);
        builder.setBolt("splitDataBolt",new UBHomeworkAssessmentTopology.SplitDataBolt(),3).shuffleGrouping("kafkaSpout");
        builder.setBolt("hdfsBolt",hdfsBolt,3).shuffleGrouping("splitDataBolt");

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
