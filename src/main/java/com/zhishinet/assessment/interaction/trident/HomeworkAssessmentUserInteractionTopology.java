package com.zhishinet.assessment.interaction.trident;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.zhishinet.MyConfig;
import com.zhishinet.Utils;
import com.zhishinet.assessment.interaction.Field;
import com.zhishinet.assessment.interaction.HomeworkAssessmentUserInteraction;
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
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
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

public class HomeworkAssessmentUserInteractionTopology {

    public static final String TOPIC = "HomeworkAssessmentUserInteraction";
    public static final String SPOUTID = "homeworkassessmentuserinteractionstorm";
    public static final String TOPOLOGY_NAME = "HomeworkAssessmentUserInteractionTopology";

    private final static ObjectMapper mapper = new ObjectMapper();

    public static class SplitData extends BaseFunction {
        private static final Logger logger = LoggerFactory.getLogger(SplitData.class);
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            logger.info("StartParsing data from json to fields");
            final String json = tuple.getString(0);
            HomeworkAssessmentUserInteraction haui = null;
            try {
                haui = mapper.readValue(json,HomeworkAssessmentUserInteraction.class);
            } catch (IOException e) {
                logger.error("Convert json to object HomeworkAssessmentUserInteraction errror",e);
            }
            if (!Objects.isNull(haui)) {
                Values values = new Values();

                // HomeworkAssessmentUserInteractionId
                if (null == haui.getHomeworkAssessmentUserInteractionId() || haui.getHomeworkAssessmentUserInteractionId() <= 0) {
                    logger.error("The message from kafka homeworkAssessmentUserInteractionId is inValidate : {}", json);
                }
                values.add(haui.getHomeworkAssessmentUserInteractionId());

                // HomeworkSessionUserTrackingId
                if (null == haui.getHomeworkSessionUserTrackingId() || haui.getHomeworkSessionUserTrackingId() <= 0) {
                    logger.error("The message from kafka homeworkSessionUserTrackingId is inValidate : {}", json);
                }
                values.add(haui.getHomeworkSessionUserTrackingId());

                // HomeworkAssessmentId
                if (null == haui.getHomeworkAssessmentId() || haui.getHomeworkAssessmentId() <= 0) {
                    logger.error("The message from kafka homeworkAssessmentId is inValidate : {}", json);
                }
                values.add(haui.getHomeworkAssessmentId());

                // QuestionId
                if (null == haui.getQuestionId() || haui.getQuestionId() <= 0) {
                    logger.error("The message from kafka questionId is inValidate : {}", json);
                }
                values.add(haui.getQuestionId());
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
                values.add(StringUtils.isNotBlank(haui.getQuestionAnswer()) ? haui.getQuestionAnswer() : "\\N");
                values.add((!Objects.isNull(haui.getReadCount())) ? haui.getReadCount() : "\\N");
                values.add((!Objects.isNull(haui.getStandardScore())) ? haui.getStandardScore() : "\\N");
                values.add(StringUtils.isNotBlank(haui.getAudioPath()) ? haui.getAudioPath() : "\\N");
                values.add((!Objects.isNull(haui.getOralScore())) ? haui.getOralScore() : "\\N");
                values.add((!Objects.isNull(haui.getGuessWordTimeSpent())) ? haui.getGuessWordTimeSpent() : "\\N");
                values.add((!Objects.isNull(haui.getSessionId())) ? haui.getSessionId() : "\\N");
                collector.emit(values);
            }
        }
    }


    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        RecordFormat recordFormat = new DelimitedRecordFormat().withFieldDelimiter(MyConfig.FIELD_DELIMITER);
        // rotate files when they reach 128MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(MyConfig.FILE_SIZE, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/user/storm/HomeworkAssessmentUserInteraction/").withExtension(".txt");

        HdfsState.Options options = new HdfsState.HdfsFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(rotationPolicy)
                .withFsUrl(MyConfig.HDFS_URL);

        StateFactory factory = new HdfsStateFactory().withOptions(options);

        TridentTopology topology = new TridentTopology();
        topology.newStream("MyConfig",new TransactionalTridentKafkaSpout(MyConfig.getTridentKafkaConfig(TOPIC, MyConfig.ZK_HOSTS, SPOUTID))).parallelismHint(3)
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
