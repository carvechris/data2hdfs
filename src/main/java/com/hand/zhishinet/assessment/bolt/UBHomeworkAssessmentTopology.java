package com.hand.zhishinet.assessment.bolt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.zhishinet.MyConfig;
import com.hand.zhishinet.assessment.Field;
import com.hand.zhishinet.assessment.vo.UBHomeworkAssessment;
import com.zhishinet.Utils;
import com.zhishinet.storm.ZhishinetBoltFileNameFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
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

public class UBHomeworkAssessmentTopology {

    public static final String TOPIC = "UBHomeworkAssessment";
    public static final String SPOUTID = "ubhomeworkassessmentstorm";
    public static final String TOPOLOGY_NAME = "UBHomeworkAssessmentTopology";

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
            ObjectMapper mapper = new ObjectMapper();
            UBHomeworkAssessment assessment = null;
            try {
                assessment = mapper.readValue(json, UBHomeworkAssessment.class);
            } catch (IOException e) {
                logger.error("The message from kafka, the data is {}", e.getMessage());
                logger.error("The message from kafka transfer to UBHomeworkAssessment error: {}", e.getMessage());
            }
            if (Objects.isNull(assessment)) {
                this.outputCollector.fail(tuple);
            } else {
                //1.验证必需字段
                Values values = new Values();
                if (null == assessment.getHomeworkAssessmentId() || assessment.getHomeworkAssessmentId() <= 0) {
                    logger.error("The message from kafka homeworkAssessmentId is inValidate : {}", json);
                    this.outputCollector.fail(tuple);
                }
                values.add(assessment.getHomeworkAssessmentId());

                if (StringUtils.isBlank(assessment.getTitle())) {
                    logger.error("The message from kafka title is inValidate : {}", json);
                    this.outputCollector.fail(tuple);
                }
                values.add(assessment.getTitle());

                if (null == assessment.getTenantId() || assessment.getTenantId() <= 0) {
                    logger.error("The message from kafka tenantId is inValidate : {}", json);
                    this.outputCollector.fail(tuple);
                }
                values.add(assessment.getTenantId());

                if (null == assessment.getTimerOn()) {
                    logger.error("The message from kafka isTimerOn is inValidate : {}", json);
                    this.outputCollector.fail(tuple);
                }
                values.add(assessment.getTimerOn());

                if (null == assessment.getTimerMode() || assessment.getTimerMode() <= 0) {
                    logger.error("The message from kafka timerMode is inValidate : {}", json);
                    this.outputCollector.fail(tuple);
                }
                values.add(assessment.getTimerMode());

                if (null == assessment.getAssessmentQuestions() || assessment.getAssessmentQuestions() < 0) {
                    logger.error("The message from kafka assessmentQuestions is inValidate : {}", json);
                    this.outputCollector.fail(tuple);
                }
                values.add(assessment.getAssessmentQuestions());

                /*if(null == assessment.getDeleted()) {
                    logger.error("The message from kafka isDeleted is inValidate : {}", json);
                    this.outputCollector.fail(tuple);
                    throw new IllegalArgumentException("The message from kafka isDeleted is inValidate ");
                }
                values.add(assessment.getDeleted());*/
                values.add(!Objects.isNull(assessment.getDeleted()) ? assessment.getDeleted() : "\\N");

                if (null == assessment.getTemplateType() || assessment.getTemplateType() <= 0) {
                    logger.error("The message from kafka templateType is inValidate : {}", json);
                    this.outputCollector.fail(tuple);
                }
                values.add(assessment.getTemplateType());

                if (null == assessment.getAssessmentBuilderType() || assessment.getAssessmentBuilderType() <= 0) {
                    logger.error("The message from kafka assessmentBuilderType is inValidate : {}", json);
                    this.outputCollector.fail(tuple);
                }
                values.add(assessment.getAssessmentBuilderType());

                if (null == assessment.getOptionRandom()) {
                    logger.error("The message from kafka isOptionRandom is inValidate : {}", json);
                    this.outputCollector.fail(tuple);
                }
                values.add(assessment.getOptionRandom());

                values.add(!Objects.isNull(assessment.getMinimumPassPercentage()) ? assessment.getMinimumPassPercentage() : "\\N");
                values.add(!Objects.isNull(assessment.getBeginDate()) ? Utils.formatDate2String(assessment.getBeginDate()) : "\\N");
                values.add(!Objects.isNull(assessment.getEndDate()) ? Utils.formatDate2String(assessment.getEndDate()) : "\\N");
                values.add(!Objects.isNull(assessment.getAssessmentClassification()) ? assessment.getAssessmentClassification() : "\\N");
                values.add(!Objects.isNull(assessment.getDuration()) ? assessment.getDuration() : "\\N");
                values.add(!Objects.isNull(assessment.getAllowBack()) ? assessment.getAllowBack() : "\\N");
                values.add(!Objects.isNull(assessment.getAllowSkip()) ? assessment.getAllowSkip() : "\\N");
                values.add(!Objects.isNull(assessment.getDisableFeedback()) ? assessment.getDisableFeedback() : "\\N");
                values.add(!Objects.isNull(assessment.getAssessmentBuilderId()) ? assessment.getAssessmentBuilderId() : "\\N");
                values.add(!Objects.isNull(assessment.getSubjectId()) ? assessment.getSubjectId() : "\\N");
                values.add(!Objects.isNull(assessment.getOral()) ? assessment.getOral() : "\\N");
                values.add(!Objects.isNull(assessment.getShowSubTitle()) ? assessment.getShowSubTitle() : "\\N");
                values.add(!Objects.isNull(assessment.getDisplayOrder()) ? assessment.getDisplayOrder() : "\\N");
                values.add(!Objects.isNull(assessment.getTextbookId()) ? assessment.getTextbookId() : "\\N");
                values.add(!Objects.isNull(assessment.getTextbookSeriesId()) ? assessment.getTextbookSeriesId() : "\\N");
                values.add(StringUtils.isNotBlank(assessment.getIntroText()) ? assessment.getIntroText() : "\\N");
                values.add(!Objects.isNull(assessment.getCreatedBy()) ? assessment.getCreatedBy() : "\\N");
                values.add(!Objects.isNull(assessment.getCreatedOn()) ? Utils.formatDate2String(assessment.getCreatedOn()) : "\\N");
                values.add(!Objects.isNull(assessment.getModifiedBy()) ? assessment.getModifiedBy() : "\\N");
                values.add(!Objects.isNull(assessment.getModifiedOn()) ? Utils.formatDate2String(assessment.getModifiedOn()) : "\\N");
                values.add(!Objects.isNull(assessment.getDeletedBy()) ? assessment.getDeletedBy() : "\\N");
                values.add(!Objects.isNull(assessment.getDeletedOn()) ? Utils.formatDate2String(assessment.getDeletedOn()) : "\\N");

                this.outputCollector.ack(tuple);
                this.outputCollector.emit(values);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(Field.getHomeworkAssessmentFields());
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        SpoutConfig spoutConfig = MyConfig.getKafkaSpoutConfig(TOPIC, MyConfig.ZK_HOSTS, MyConfig.ZK_ROOT, SPOUTID);

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("\001");
        SyncPolicy syncPolicy = new CountSyncPolicy(100);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(MyConfig.FILE_SIZE, FileSizeRotationPolicy.Units.MB);
        //  FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/user/storm/HomeworkAssessment/").withExtension(".txt");
        FileNameFormat fileNameFormat = new ZhishinetBoltFileNameFormat().withPath("/user/storm/HomeworkAssessment/").withExtension(".txt");
        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl(MyConfig.HDFS_URL).withFileNameFormat(fileNameFormat)
                .withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig), 3);
        builder.setBolt("splitDataBolt", new SplitDataBolt(), 3).shuffleGrouping("kafkaSpout");
        builder.setBolt("hdfsBolt", hdfsBolt, 3).shuffleGrouping("splitDataBolt");

        Config config = MyConfig.getConfigWithKafkaConsumerProps(false, MyConfig.KAFKA_BROKERS);

        if (null != args && args.length > 0) {
            //config.setNumWorkers(3);
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        }
    }

}



