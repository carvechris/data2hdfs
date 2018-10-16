package com.hand.zhishinet.assessment.bolt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.zhishinet.MyConfig;
import com.hand.zhishinet.assessment.Field;
import com.hand.zhishinet.assessment.vo.UBHomeworkSessionUserTracking;
import com.hand.zhishinet.assessment.vo.UBHomeworkSessionUserTrackingAttemptDetail;
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
            UBHomeworkSessionUserTrackingAttemptDetail log = null;
            try {
                log = mapper.readValue(json,UBHomeworkSessionUserTrackingAttemptDetail.class);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("The message from kafka, the data is {}", e.getMessage());
                logger.error("The message from kafka transfer to UBHomeworkSessionUserTrackingAttemptDetail error: {}", e.getMessage());
            }
            if (Objects.isNull(log)) {
                this.collector.fail(tuple);
            } else {
                Values values = new Values();
                if (log.getHomeworksessionUserTrackingAttemptDetailId() == null) {
                    logger.error("The message from kafka homeworksessionUserTrackingAttemptDetailId is inValidate : {}", json);
                    this.collector.fail(tuple);
                }
                values.add(log.getHomeworksessionUserTrackingAttemptDetailId());
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
                if (log.getAttemptNumber() == null) {
                    logger.error("The message from kafka attemptNumber is inValidate : {}", json);
                    this.collector.fail(tuple);
                }
                values.add(log.getAttemptNumber());
                values.add(!Objects.isNull(log.getNoOfVisits()) ? log.getNoOfVisits() : "\\N");
                values.add(!Objects.isNull(log.getTimeSpent()) ? log.getTimeSpent() : "\\N");
                values.add(!Objects.isNull(log.getStatusId()) ? log.getStatusId() : "\\N");
                values.add(!Objects.isNull(log.getCompletedOn()) ? Utils.formatDate2String(log.getCompletedOn()) : "\\N");
                values.add(!Objects.isNull(log.getScore()) ? log.getScore() : "\\N");
                values.add(!Objects.isNull(log.getPercentScore()) ? log.getPercentScore() : "\\N");
                values.add(!Objects.isNull(log.getAssessmentDifficulty()) ? log.getAssessmentDifficulty() : "\\N");
                values.add(!Objects.isNull(log.getReadCount()) ? log.getReadCount() : "\\N");
                values.add(!Objects.isNull(log.getCreatedOn()) ? Utils.formatDate2String(log.getCreatedOn()) : "\\N");
                values.add(!Objects.isNull(log.getCreatedBy()) ? log.getCreatedBy() : "\\N");
                values.add(!Objects.isNull(log.getModifiedOn()) ? Utils.formatDate2String(log.getModifiedOn()) : "\\N");
                values.add(!Objects.isNull(log.getModifiedBy()) ? log.getModifiedBy() : "\\N");
                values.add(!Objects.isNull(log.getDeletedOn()) ? Utils.formatDate2String(log.getDeletedOn()) : "\\N");
                values.add(!Objects.isNull(log.getDeletedBy()) ? log.getDeletedBy() : "\\N");
                values.add(!Objects.isNull(log.getDeleted()) ? log.getDeleted() : "\\N");
                values.add(!Objects.isNull(log.getSessionId()) ? log.getSessionId() : "\\N");

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

        SpoutConfig spoutConfig = MyConfig.getKafkaSpoutConfig(TOPIC, MyConfig.ZK_HOSTS,MyConfig.ZK_ROOT, SPOUTID);

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("\001");
        SyncPolicy syncPolicy = new CountSyncPolicy(100);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(MyConfig.FILE_SIZE, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new ZhishinetBoltFileNameFormat().withPath("/user/storm/HomeworkSessionUserTrackingAttemptDetail/").withExtension(".txt");
        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl(MyConfig.HDFS_URL).withFileNameFormat(fileNameFormat)
                .withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout",new KafkaSpout(spoutConfig),3);
        builder.setBolt("homeworkSessionUserTrackingAttemptDetailBolt",new UBHomeworkSessionUserTrackingAttemptDetailBolt(),3).shuffleGrouping("kafkaSpout");
        builder.setBolt("hdfsBolt",hdfsBolt,3).shuffleGrouping("homeworkSessionUserTrackingAttemptDetailBolt");

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
