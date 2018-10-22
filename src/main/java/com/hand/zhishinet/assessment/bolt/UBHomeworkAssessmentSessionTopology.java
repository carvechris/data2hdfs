package com.hand.zhishinet.assessment.bolt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.zhishinet.MyConfig;
import com.hand.zhishinet.assessment.Field;
import com.hand.zhishinet.assessment.vo.UBHomeworkAssessmentSession;
import com.zhishinet.Utils;
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

public class UBHomeworkAssessmentSessionTopology {

    public static final String TOPIC = "UBHomeworkAssessmentSession";
    public static final String SPOUTID = "ubhomeworkassessmentsessionstorm";
    public static final String TOPOLOGY_NAME = "UBHomeworkAssessmentSessionTopology";

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
            UBHomeworkAssessmentSession assessmentSession = null;
            try {
                assessmentSession = mapper.readValue(json, UBHomeworkAssessmentSession.class);
            } catch (IOException e) {
                logger.error("The message from kafka, the data is {}", e.getMessage());
                logger.error("The message from kafka transfer to UBHomeworkAssessmentSession error: {}", e.getMessage());
            }
            if (Objects.isNull(assessmentSession)) {
                this.outputCollector.fail(tuple);
            } else {
                //1.验证必需字段
                Values values = new Values();
                if(null == assessmentSession.getHomeworkAssessmentId() || assessmentSession.getHomeworkAssessmentId() <= 0) {
                    logger.error("The message from kafka homeworkAssessmentId is inValidate : {}", json);
                    this.outputCollector.fail(tuple);
                }
                values.add(assessmentSession.getHomeworkAssessmentId());

                if(null == assessmentSession.getAssessmentSessionId() || assessmentSession.getAssessmentSessionId() <= 0) {
                    logger.error("The message from kafka assessmentSessionId is inValidate : {}", json);
                    this.outputCollector.fail(tuple);
                }
                values.add(assessmentSession.getAssessmentSessionId());

                if(null == assessmentSession.getSessionId() || assessmentSession.getSessionId() <= 0) {
                    logger.error("The message from kafka sessionId is inValidate : {}", json);
                    this.outputCollector.fail(tuple);
                }
                values.add(assessmentSession.getSessionId());

                if(StringUtils.isBlank(assessmentSession.getEmendTypeCode())) {
                    logger.error("The message from kafka emendTypeCode is inValidate : {}", json);
                    this.outputCollector.fail(tuple);
                }
                values.add(assessmentSession.getEmendTypeCode());

                values.add(!Objects.isNull(assessmentSession.getClose()) ? assessmentSession.getClose() : "\\N");
                values.add(!Objects.isNull(assessmentSession.getRequire()) ? assessmentSession.getRequire() : "\\N");
                values.add(!Objects.isNull(assessmentSession.getRequiredEmend()) ? assessmentSession.getRequiredEmend() : "\\N");
                values.add(!Objects.isNull(assessmentSession.getDeleted()) ? assessmentSession.getDeleted() : "\\N");
                values.add(!Objects.isNull(assessmentSession.getSessionGroupId()) ? assessmentSession.getSessionGroupId() : "\\N");

                values.add(!Objects.isNull(assessmentSession.getCreatedBy()) ? assessmentSession.getCreatedBy() : "\\N");
                values.add(!Objects.isNull(assessmentSession.getCreatedOn()) ? Utils.formatDate2String(assessmentSession.getCreatedOn()) : "\\N");
                values.add(!Objects.isNull(assessmentSession.getModifiedBy()) ? assessmentSession.getModifiedBy() : "\\N");
                values.add(!Objects.isNull(assessmentSession.getModifiedOn()) ? Utils.formatDate2String(assessmentSession.getModifiedOn()) : "\\N");
                values.add(!Objects.isNull(assessmentSession.getDeletedBy()) ? assessmentSession.getDeletedBy() : "\\N");
                values.add(!Objects.isNull(assessmentSession.getDeletedOn()) ? Utils.formatDate2String(assessmentSession.getDeletedOn()) : "\\N");

                values.add(!Objects.isNull(assessmentSession.getHomeworkType()) ? assessmentSession.getHomeworkType() : "\\N");

                this.outputCollector.ack(tuple);
                this.outputCollector.emit(values);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(Field.getHomeworkAssessmentSessionFields());
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        SpoutConfig spoutConfig = MyConfig.getKafkaSpoutConfig(TOPIC, MyConfig.ZK_HOSTS,MyConfig.ZK_ROOT, SPOUTID);

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(MyConfig.FIELD_DELIMITER);
        SyncPolicy syncPolicy = new CountSyncPolicy(MyConfig.COUNT_SYNC_POLICY);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(MyConfig.FILE_SIZE, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/tmp/storm/HomeworkAssessmentSession/").withExtension(".txt");
        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl(MyConfig.HDFS_URL).withFileNameFormat(fileNameFormat)
                .withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout",new KafkaSpout(spoutConfig));
        builder.setBolt("splitDataBolt",new SplitDataBolt()).shuffleGrouping("kafkaSpout");
        builder.setBolt("hdfsBolt",hdfsBolt).shuffleGrouping("splitDataBolt");

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



