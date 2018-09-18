package com.zhishinet.assessment1.interaction.trident;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.zhishinet.MyConfig;
import com.zhishinet.assessment1.interaction.Field;
import com.zhishinet.assessment1.interaction.HomeworkAssessmentUserInteraction;
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
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class HomeworkAssessmentUserInteractionTopology {

    private static Logger logger = LoggerFactory.getLogger(HomeworkAssessmentUserInteractionTopology.class);
    public static final String TOPIC = "HomeworkAssessmentUserInteraction";
    public static final String SPOUT_ID = "homeworkassessmentuserinteractionstorm";

    private final static Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();

    public static class SplitData extends BaseFunction {
        private static final Logger logger = LoggerFactory.getLogger(SplitData.class);
        @Override
        public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
            logger.info("StartParsing data from json to fields");
            final String json = tuple.getString(0);
            HomeworkAssessmentUserInteraction haui = gson.fromJson(json, HomeworkAssessmentUserInteraction.class);
            if (Objects.isNull(haui)) {
                logger.error("The message from kafka cann't convert 2 HomeworkAssessmentUserInteraction");
                throw new IllegalArgumentException("The message from kafka cann't convert 2 HomeworkAssessmentUserInteraction");
            } else {
                Values values = new Values();

                // HomeworkAssessmentUserInteractionId
                if (null == haui.getHomeworkAssessmentUserInteractionId() || haui.getHomeworkAssessmentUserInteractionId() <= 0) {
                    logger.error("The message from kafka homeworkAssessmentUserInteractionId is inValidate : {}", json);
                    throw new IllegalArgumentException("The message from kafka homeworkAssessmentUserInteractionId is inValidate");
                }
                values.add(haui.getHomeworkAssessmentUserInteractionId());

                // HomeworkSessionUserTrackingId
                if (null == haui.getHomeworkSessionUserTrackingId() || haui.getHomeworkSessionUserTrackingId() <= 0) {
                    logger.error("The message from kafka homeworkSessionUserTrackingId is inValidate : {}", json);
                    throw new IllegalArgumentException("The message from kafka homeworkSessionUserTrackingId is inValidate");
                }
                values.add(haui.getHomeworkSessionUserTrackingId());

                // HomeworkAssessmentId
                if (null == haui.getHomeworkAssessmentId() || haui.getHomeworkAssessmentId() <= 0) {
                    logger.error("The message from kafka homeworkAssessmentId is inValidate : {}", json);
                    throw new IllegalArgumentException("The message from kafka homeworkAssessmentId is inValidate");
                }
                values.add(haui.getHomeworkAssessmentId());

                // QuestionId
                if (null == haui.getQuestionId() || haui.getQuestionId() <= 0) {
                    logger.error("The message from kafka questionId is inValidate : {}", json);
                    throw new IllegalArgumentException("The message from kafka questionId is inValidate");
                }
                values.add(haui.getQuestionId());
                values.add((!Objects.isNull(haui.getCorrectResponse())) ? haui.getCorrectResponse() : "\\N");
                values.add(StringUtils.isNotBlank(haui.getUserResponse()) ? haui.getUserResponse() : "\\N");
                values.add(StringUtils.isNotBlank(haui.getInteractionDate()) ? haui.getInteractionDate() : "\\N");
                values.add((!Objects.isNull(haui.getAttemptNo())) ? haui.getAttemptNo() : "\\N");
                values.add((!Objects.isNull(haui.getInteractionTimeSpent())) ? haui.getInteractionTimeSpent() : "\\N");
                values.add((!Objects.isNull(haui.getUserScore())) ? haui.getUserScore() : "\\N");
                values.add(StringUtils.isNotBlank(haui.getTextUserResponse()) ? haui.getTextUserResponse() : "\\N");
                values.add(haui.isFeedbackViewed());
                values.add(StringUtils.isNotBlank(haui.getCreatedOn()) ? haui.getCreatedOn() : "\\N");
                values.add((!Objects.isNull(haui.getCreatedBy())) ? haui.getCreatedBy() : "\\N");
                values.add(StringUtils.isNotBlank(haui.getModifiedOn()) ? haui.getModifiedOn() : "\\N");
                values.add((!Objects.isNull(haui.getModifiedBy())) ? haui.getModifiedBy() : "\\N");
                values.add(StringUtils.isNotBlank(haui.getDeletedOn()) ? haui.getDeletedOn() : "\\N");
                values.add((!Objects.isNull(haui.getDeletedOn())) ? haui.getDeletedOn() : "\\N");
                values.add(haui.isDeleted());
                values.add(StringUtils.isNotBlank(haui.getQuestionAnswer()) ? haui.getQuestionAnswer() : "\\N");
                values.add((!Objects.isNull(haui.getReadCount())) ? haui.getReadCount() : "\\N");
                values.add((!Objects.isNull(haui.getStandardScore())) ? haui.getStandardScore() : "\\N");
                values.add(StringUtils.isNotBlank(haui.getAudioPath()) ? haui.getAudioPath() : "\\N");
                values.add((!Objects.isNull(haui.getOralScore())) ? haui.getOralScore() : "\\N");
                values.add((!Objects.isNull(haui.getGuessWordTimeSpent())) ? haui.getGuessWordTimeSpent() : "\\N");
                tridentCollector.emit(values);
            }
        }
    }


    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        RecordFormat recordFormat = new DelimitedRecordFormat().withFieldDelimiter("\001");
        // rotate files when they reach 128MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(128.0f, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/user/storm/HomeworkAssessmentUserInteraction/").withExtension(".txt");

        HdfsState.Options options = new HdfsState.HdfsFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(rotationPolicy)
                .withFsUrl(MyConfig.HDFS_URL);

        StateFactory factory = new HdfsStateFactory().withOptions(options);

        TridentTopology topology = new TridentTopology();
        topology.newStream("MyConfig",new TransactionalTridentKafkaSpout((TridentKafkaConfig) MyConfig.getKafkaSpoutConfig(TOPIC, MyConfig.ZK_HOSTS,MyConfig.ZK_ROOT,SPOUT_ID))).parallelismHint(3)
                .each(new Fields("str"),new SplitData(),Field.kafkaMessageFields).parallelismHint(3)
                .partitionPersist(factory, Field.kafkaMessageFields, new HdfsUpdater(), new Fields()).parallelismHint(3);

        Config config = MyConfig.getConfigWithKafkaConsumerProps(false,MyConfig.KAFKA_BROKERS);
        if(null != args && args.length > 0) {
            config.setNumWorkers(3);
            StormSubmitter.submitTopology("HomeworkAssessmentUserInteractionTopology", config, topology.build());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("HomeworkAssessmentUserInteractionTopology",config,topology.build());
        }
    }
}
