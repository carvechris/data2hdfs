package com.hand.zhishinet.assessment.bolt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.zhishinet.MyConfig;
import com.hand.zhishinet.assessment.Field;
import com.hand.zhishinet.assessment.vo.UBHomeworkSessionUserTrackingAttemptDetail;
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

public class UBHomeworkSessionUserTrackingAttemptDetailTopology {
    public static final String TOPIC = "HomeworkSessionUserTrackingAttemptDetail";
    public static final String SPOUTID = "ubhomeworksessionusertrackingattemptdetailstorm";
    public static final String TOPOLOGY_NAME = "UBHomeworkSessionUserTrackingAttemptDetailTopology";

    public static class UBHomeworkSessionUserTrackingAttemptDetailBolt extends BaseRichBolt {
        private OutputCollector collector;
        private final static Logger logger = LoggerFactory.getLogger(UBHomeworkSessionUserTrackingAttemptDetailBolt.class);

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            final String json = tuple.getString(0);
            ObjectMapper mapper = new ObjectMapper();
            UBHomeworkSessionUserTrackingAttemptDetail trackingAttemptDetail = null;
            try {
                trackingAttemptDetail = mapper.readValue(json, UBHomeworkSessionUserTrackingAttemptDetail.class);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("The message from kafka, the data is {}", e.getMessage());
                logger.error("The message from kafka transfer to UBHomeworkSessionUserTrackingAttemptDetail error: {}", e.getMessage());
            }
            if (Objects.isNull(trackingAttemptDetail)) {
                this.collector.fail(tuple);
            } else {
                Values values = new Values();
                if (trackingAttemptDetail.getHomeworksessionUserTrackingAttemptDetailId() == null) {
                    logger.error("The message from kafka homeworksessionUserTrackingAttemptDetailId is inValidate : {}", json);
                    this.collector.fail(tuple);
                }
                values.add(trackingAttemptDetail.getHomeworksessionUserTrackingAttemptDetailId());
                if (trackingAttemptDetail.getHomeworkSessionUserTrackingId() == null) {
                    logger.error("The message from kafka homeworkSessionUserTrackingId is inValidate : {}", json);
                    this.collector.fail(tuple);
                }
                values.add(trackingAttemptDetail.getHomeworkSessionUserTrackingId());
                if (trackingAttemptDetail.getHomeworkAssessmentId() == null) {
                    logger.error("The message from kafka homeworkAssessmentId is inValidate : {}", json);
                    this.collector.fail(tuple);
                }
                values.add(trackingAttemptDetail.getHomeworkAssessmentId());
                if (trackingAttemptDetail.getAttemptNumber() == null) {
                    logger.error("The message from kafka attemptNumber is inValidate : {}", json);
                    this.collector.fail(tuple);
                }
                values.add(trackingAttemptDetail.getAttemptNumber());
                values.add(!Objects.isNull(trackingAttemptDetail.getNoOfVisits()) ? trackingAttemptDetail.getNoOfVisits() : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getTimeSpent()) ? trackingAttemptDetail.getTimeSpent() : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getStatusId()) ? trackingAttemptDetail.getStatusId() : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getCompletedOn()) ? Utils.formatDate2String(trackingAttemptDetail.getCompletedOn()) : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getScore()) ? trackingAttemptDetail.getScore() : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getPercentScore()) ? trackingAttemptDetail.getPercentScore() : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getAssessmentDifficulty()) ? trackingAttemptDetail.getAssessmentDifficulty() : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getReadCount()) ? trackingAttemptDetail.getReadCount() : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getCreatedOn()) ? Utils.formatDate2String(trackingAttemptDetail.getCreatedOn()) : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getCreatedBy()) ? trackingAttemptDetail.getCreatedBy() : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getModifiedOn()) ? Utils.formatDate2String(trackingAttemptDetail.getModifiedOn()) : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getModifiedBy()) ? trackingAttemptDetail.getModifiedBy() : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getDeletedOn()) ? Utils.formatDate2String(trackingAttemptDetail.getDeletedOn()) : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getDeletedBy()) ? trackingAttemptDetail.getDeletedBy() : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getDeleted()) ? trackingAttemptDetail.getDeleted() : "\\N");
                values.add(!Objects.isNull(trackingAttemptDetail.getSessionId()) ? trackingAttemptDetail.getSessionId() : "\\N");

                this.collector.ack(tuple);
                this.collector.emit(values);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(Field.getUBHomeworkSessionUserTrackingAttemptDetailFields());
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        SpoutConfig spoutConfig = MyConfig.getKafkaSpoutConfig(TOPIC, MyConfig.ZK_HOSTS, MyConfig.ZK_ROOT, SPOUTID);

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(MyConfig.FIELD_DELIMITER);
        SyncPolicy syncPolicy = new CountSyncPolicy(100);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(MyConfig.FILE_SIZE, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/user/storm/HomeworkSessionUserTrackingAttemptDetail/").withExtension(".txt");
        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl(MyConfig.HDFS_URL).withFileNameFormat(fileNameFormat)
                .withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig), 3);
        builder.setBolt("homeworkSessionUserTrackingAttemptDetailBolt", new UBHomeworkSessionUserTrackingAttemptDetailBolt(), 3).shuffleGrouping("kafkaSpout");
        builder.setBolt("hdfsBolt", hdfsBolt, 3).shuffleGrouping("homeworkSessionUserTrackingAttemptDetailBolt");

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
