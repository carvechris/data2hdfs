package com.zhishinet.assessment.interaction.math.bolt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.zhishinet.MyConfig;
import com.zhishinet.Utils;
import com.zhishinet.assessment.interaction.HomeworkAssessmentUserInteraction;
import com.zhishinet.assessment.interaction.math.Field;
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

public class HomeworkAssessmentUserInteractionMathTopology {

    private final static ObjectMapper mapper = new ObjectMapper();
    private static final String TOPIC = "HomeworkAssessmentUserInteractionMath";
    public static final String SPOUTID = "homeworkassessmentuserinteractionmathstorm";
    public static final String TOPOLOGY_NAME = "HomeworkAssessmentUserInteractionMathTopology";

    public static class SplitDataBolt extends BaseRichBolt {

        private OutputCollector outputCollector;
        private static final Logger logger = LoggerFactory.getLogger(SplitDataBolt.class);
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            final String json = tuple.getString(0);
            logger.info("json : {}",json);
            HomeworkAssessmentUserInteraction haui = null;
            try {
                haui = mapper.readValue(json,HomeworkAssessmentUserInteraction.class);
            } catch (IOException e) {
                logger.error("Convert json to object HomeworkAssessmentUserInteraction errror",e);
                this.outputCollector.fail(tuple);
            }
            if (!Objects.isNull(haui)) {
                Values values = new Values();
                // HomeworkAssessmentUserInteractionId
                if (null == haui.getHomeworkAssessmentUserInteractionId() || haui.getHomeworkAssessmentUserInteractionId() <= 0) {
                    logger.error("The message from kafka homeworkAssessmentUserInteractionId is inValidate");
                    return;
                } else {
                    values.add(haui.getHomeworkAssessmentUserInteractionId());
                }

                // HomeworkSessionUserTrackingId
                if (null == haui.getHomeworkSessionUserTrackingId() || haui.getHomeworkSessionUserTrackingId() <= 0) {
                    logger.error("The message from kafka homeworkSessionUserTrackingId is inValidate");
                    return;
                } else {
                    values.add(haui.getHomeworkSessionUserTrackingId());
                }

                // HomeworkAssessmentId
                if (null == haui.getHomeworkAssessmentId() || haui.getHomeworkAssessmentId() <= 0) {
                    logger.error("The message from kafka homeworkAssessmentId is inValidate");
                    return;
                } else {
                    values.add(haui.getHomeworkAssessmentId());
                }

                // QuestionId
                if (null == haui.getQuestionId() || haui.getQuestionId() <= 0) {
                    logger.error("The message from kafka questionId is inValidate");
                    return;
                } else {
                    values.add(haui.getQuestionId());
                }

                values.add((!Objects.isNull(haui.getCorrectResponse())) ? haui.getCorrectResponse() : "\\N");
                values.add(StringUtils.isNotBlank(haui.getUserResponse()) ? haui.getUserResponse() : "\\N");
                values.add(!Objects.isNull(haui.getInteractionDate()) ? Utils.formatDate2String(haui.getInteractionDate()) : "\\N");
                values.add((!Objects.isNull(haui.getAttemptNo())) ? haui.getAttemptNo() : "\\N");
                values.add((!Objects.isNull(haui.getInteractionTimeSpent())) ? haui.getInteractionTimeSpent() : "\\N");
                values.add((!Objects.isNull(haui.getUserScore())) ? haui.getUserScore() : "\\N");
                values.add(StringUtils.isNotBlank(haui.getTextUserResponse()) ? haui.getTextUserResponse() : "\\N");
                values.add(haui.isFeedbackViewed());
                values.add(!Objects.isNull(haui.getCreatedOn()) ? Utils.formatDate2String(haui.getCreatedOn()) : "\\N");
                values.add((!Objects.isNull(haui.getCreatedBy())) ? haui.getCreatedBy() : "\\N");
                values.add(!Objects.isNull(haui.getModifiedOn()) ? Utils.formatDate2String(haui.getModifiedOn()) : "\\N");
                values.add((!Objects.isNull(haui.getModifiedBy())) ? haui.getModifiedBy() : "\\N");
                values.add(!Objects.isNull(haui.getDeletedOn()) ? Utils.formatDate2String(haui.getDeletedOn()) : "\\N");
                values.add((!Objects.isNull(haui.getDeletedBy())) ? haui.getDeletedBy() : "\\N");
                values.add(haui.isDeleted());
                values.add((!Objects.isNull(haui.getSessionId())) ? haui.getSessionId() : "\\N");
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
        SpoutConfig spoutConfig = MyConfig.getKafkaSpoutConfig(TOPIC, MyConfig.ZK_HOSTS,MyConfig.ZK_ROOT,SPOUTID);
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(MyConfig.FIELD_DELIMITER);
        SyncPolicy syncPolicy = new CountSyncPolicy(MyConfig.COUNT_SYNC_POLICY);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(MyConfig.FILE_SIZE, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/tmp/storm/HomeworkAssessmentUserInteractionMath/").withExtension(".txt");
        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl(MyConfig.HDFS_URL).withFileNameFormat(fileNameFormat)
                .withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout",kafkaSpout);
        builder.setBolt("splitDataBolt", new SplitDataBolt()).shuffleGrouping("kafkaSpout");
        builder.setBolt("hdfsBolt", hdfsBolt).shuffleGrouping("splitDataBolt");

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
