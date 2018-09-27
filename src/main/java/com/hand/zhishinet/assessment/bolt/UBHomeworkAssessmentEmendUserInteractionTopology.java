package com.hand.zhishinet.assessment.bolt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.zhishinet.MyConfig;
import com.hand.zhishinet.assessment.Field;
import com.hand.zhishinet.assessment.vo.UBHomeworkAssessmentEmendUserInteraction;
import com.zhishinet.Utils;
import com.zhishinet.storm.ZhishinetBoltFileNameFormat;
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

public class UBHomeworkAssessmentEmendUserInteractionTopology {
    public static final String TOPIC = "UBHomeworkAssessmentEmendUserInteraction";
    public static final String SPOUTID = "ubhomeworkassessmentemenduserinteractionstorm";
    public static final String TOPOLOGY_NAME = "UBHomeworkAssessmentEmendUserInteractionTopology";

    public static class UBHomeworkAssessmentEmendUserInteractionBolt extends BaseRichBolt {
        private OutputCollector collector;
        private final static Logger logger = LoggerFactory.getLogger(UBHomeworkAssessmentEmendUserInteractionTopology.UBHomeworkAssessmentEmendUserInteractionBolt.class);

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            final String json = tuple.getString(0);
            ObjectMapper mapper = new ObjectMapper();
            UBHomeworkAssessmentEmendUserInteraction log = null;
            try {
                log = mapper.readValue(json,UBHomeworkAssessmentEmendUserInteraction.class);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("The message from kafka, the data is {}", e.getMessage());
                logger.error("The message from kafka transfer to UBHomeworkAssessmentEmendUserInteraction error: {}", e.getMessage());
            }
            if (Objects.isNull(log)) {
                this.collector.fail(tuple);
            } else {
                Values values = new Values();
                if (log.getHomeworkAssessmentEmendUserInteractionId() == null) {
                    logger.error("The message from kafka homeworkAssessmentEmendUserInteractionId is inValidate : {}", json);
                    this.collector.fail(tuple);
                }
                values.add(log.getHomeworkAssessmentEmendUserInteractionId());
                if (log.getHomeworkSessionUserTrackingId() == null) {
                    logger.error("The message from kafka homeworkSessionUserTrackingId is inValidate : {}", json);
                    this.collector.fail(tuple);
                }
                values.add(log.getHomeworkSessionUserTrackingId());
                if (log.getHomeworkAssessmentId() == null) {
                    logger.error("The message from kafka homeworkAssessmentId is inValidate : {}", json);
                    this.collector.fail(tuple);
                }
                values.add(log.getHomeworkAssessmentId());
                values.add(!Objects.isNull(log.getQuestionId()) ? log.getQuestionId() : "\\N");
                values.add(!Objects.isNull(log.getSourceQuestionId()) ? log.getSourceQuestionId() : "\\N");
                values.add(!Objects.isNull(log.getQuestionAnswer()) ? log.getQuestionAnswer() : "\\N");
                values.add(!Objects.isNull(log.getCorrectResponse()) ? log.getCorrectResponse() : "\\N");
                values.add(!Objects.isNull(log.getUserResponse()) ? log.getUserResponse() : "\\N");
                values.add(!Objects.isNull(log.getInteractionDate()) ? Utils.formatDate2String(log.getInteractionDate()) : "\\N");
                values.add(!Objects.isNull(log.getAttemptNo()) ? log.getAttemptNo() : "\\N");
                values.add(!Objects.isNull(log.getInteractionTimeSpent()) ? log.getInteractionTimeSpent() : "\\N");
                values.add(!Objects.isNull(log.getUserScore()) ? log.getUserScore() : "\\N");
                values.add(!Objects.isNull(log.getTextUserResponse()) ? log.getTextUserResponse() : "\\N");
                values.add(!Objects.isNull(log.getFeedbackViewed()) ? log.getFeedbackViewed() : "\\N");
                values.add(!Objects.isNull(log.getCreatedOn()) ? Utils.formatDate2String(log.getCreatedOn()) : "\\N");
                values.add(!Objects.isNull(log.getCreatedBy()) ? log.getCreatedBy() : "\\N");
                values.add(!Objects.isNull(log.getModifiedOn()) ? Utils.formatDate2String(log.getModifiedOn()) : "\\N");
                values.add(!Objects.isNull(log.getModifiedBy()) ? log.getModifiedBy() : "\\N");
                values.add(!Objects.isNull(log.getDeletedOn()) ? Utils.formatDate2String(log.getDeletedOn()) : "\\N");
                values.add(!Objects.isNull(log.getDeletedBy()) ? log.getDeletedBy() : "\\N");
                values.add(!Objects.isNull(log.getDeleted()) ? log.getDeleted() : "\\N");
                this.collector.ack(tuple);
                this.collector.emit(values);
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(Field.getHomeworkAssessmentEmendUserInteractionFields());
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        SpoutConfig spoutConfig = MyConfig.getKafkaSpoutConfig(TOPIC, MyConfig.ZK_HOSTS,MyConfig.ZK_ROOT, SPOUTID);

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("\001");
        SyncPolicy syncPolicy = new CountSyncPolicy(100);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(MyConfig.FILE_SIZE, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new ZhishinetBoltFileNameFormat().withPath("/user/storm/ubhomeworkassessmentemenduserinteraction/").withExtension(".txt");
        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl(MyConfig.HDFS_URL).withFileNameFormat(fileNameFormat)
                .withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout",new KafkaSpout(spoutConfig),3);
        builder.setBolt("homeworkAssessmentEmendUserInteractionBolt",new UBHomeworkAssessmentEmendUserInteractionTopology.UBHomeworkAssessmentEmendUserInteractionBolt(),3).shuffleGrouping("kafkaSpout");
        builder.setBolt("hdfsBolt",hdfsBolt,3).shuffleGrouping("homeworkAssessmentEmendUserInteractionBolt");

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
