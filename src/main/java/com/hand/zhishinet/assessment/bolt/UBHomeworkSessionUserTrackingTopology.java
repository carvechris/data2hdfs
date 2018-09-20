package com.hand.zhishinet.assessment.bolt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.zhishinet.MyConfig;
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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

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
                logger.error("The message from kafka transfer to UBHomeworkAssessment error: {}", e.getMessage());
            }
            this.collector.ack(tuple);
            this.collector.emit(new Values(log.getHomeworkSessionUserTrackingId(),log.getSessionId(),log.getHomeworkAssessmentId(),
                    log.getUserId(),log.getNoOfVisits(),log.getLastViewedOn(),log.getStatusId(),log.getCompletedOn(),log.getScore(),
                    log.getPercentScore(),log.getCompleteAttempts(),log.getBeginDate(),log.getEndDate(),log.getTimeSpent(),
                    log.getInteractionTimer(),log.getArticleLocation(),log.getLocation(),log.getChecked(),log.getForLearnerStatus(),log.getQuestionIndexs(),
                    log.getEmendStatus(),log.getRequiredEmend(),log.getSubjectId(),log.getReadCount(),log.getShowSubTitle(),log.getEmendTypeCode(),
                    log.getSessionGroupId(),log.getDisplayOrder(),log.getCreatedOn(),log.getCreatedBy(),log.getModifiedOn(),log.getModifiedBy(),log.getDeletedOn(),
                    log.getDeletedBy(),log.getDeleted()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("homeworkSessionUserTrackingId","sessionId","homeworkAssessmentId","userId","noOfVisits",
                    "lastViewedOn","statusId","completedOn","score","percentScore","completeAttempts","beginDate","endDate","timeSpent",
                    "interactionTimer","articleLocation","location","isChecked","forLearnerStatus","questionIndexs","emendStatus","IsRequiredEmend",
                    "subjectId","readCount","showSubTitle","emendTypeCode","sessionGroupId","displayOrder","createdOn","createdBy","modifiedOn",
                    "modifiedBy","deletedOn","deletedBy","deleted"));
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
