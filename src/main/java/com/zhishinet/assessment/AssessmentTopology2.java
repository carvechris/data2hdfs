package com.zhishinet.assessment;


import com.zhishinet.assessment.baseFunction.CustomPersistFunction;
import com.zhishinet.homeworkcenter.processdata.PreProcessLauch2Tracking;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapMapper;
import org.apache.storm.hbase.trident.state.HBaseMapState;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.mongodb.trident.state.MongoState;
import org.apache.storm.mongodb.trident.state.MongoStateFactory;
import org.apache.storm.mongodb.trident.state.MongoStateUpdater;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * tomaer
 */
public class AssessmentTopology2 {

    private static Logger logger = LoggerFactory.getLogger(AssessmentTopology2.class);


    public static void main(String[] args) {

        BrokerHosts boBrokerHosts = new ZkHosts(Conf.ZOOKEEPER_LIST);
        final String spoutId = "HomeworkCenter_storm";
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(boBrokerHosts, Conf.TOPIC_HOMEWORKCENTER, spoutId);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Mongo落盘方式
        String url = "mongodb://dev:dev@10.213.0.42:37017/dev";
        String collection = "test";
        String collection2 = "test2";
        MongoMapper mapper = new SimpleMongoMapper()
                .withFields(Field.ASSESSMENTID, Field.SESSIONID, Field.SUM, Field.COUNT);
        MongoState.Options options = new MongoState.Options()
                .withUrl(url)
                .withCollectionName(collection)
                .withMapper(mapper);
        StateFactory factory = new MongoStateFactory(options);

        MongoState.Options options2 = new MongoState.Options()
                .withUrl(url)
                .withCollectionName(collection2)
                .withMapper(mapper);
        StateFactory factory2 = new MongoStateFactory(options2);


        //HBase存储中间结果
        //HBaseState使用HBaseMapState
        HBaseMapState.Options sumHbaseOptions = new HBaseMapState.Options();
        sumHbaseOptions.tableName = "AssessmentDetails";
        sumHbaseOptions.columnFamily = "Aggregate";
        sumHbaseOptions.mapMapper = new SimpleTridentHBaseMapMapper("sum");

        HBaseMapState.Options countHbaseOptions = new HBaseMapState.Options();
        countHbaseOptions.tableName = "AssessmentDetails";
        countHbaseOptions.columnFamily = "Aggregate";
        countHbaseOptions.mapMapper = new SimpleTridentHBaseMapMapper("count");
//        CustomKeyFactory keyFactory = new CustomKeyFactory();
//        StateFactory redisStateFactory = HBaseMapState.transactional();

        //HBase存储学生明细


        //0. 声明拓扑
        TridentTopology topology = new TridentTopology();

        //1. 流计算
        //1.1 整理出流中需要的信息
        Stream stream = topology.newStream("MyConfig", new TransactionalTridentKafkaSpout(kafkaConfig));


        Stream stream1 = stream
                // 先把Kafka中的数据解析成对象,包括 Field.SESSIONUSERTRACKINGID,Field.SUBJECT_ID, Field.ASSESSMENTID, Field.SESSIONID, Field.SCORE, Field.USERID 字段
                .each(new Fields("str"), new PreProcessLauch2Tracking(), new Fields(Field.SESSIONUSERTRACKINGID, Field.SUBJECT_ID, Field.ASSESSMENTID, Field.SESSIONID, Field.SCORE, Field.USERID));


        //1.2 分流计算Sum和Count , 最后join到一起.  并持久化到mongo中.
        logger.info("===============定义 分流到两个stream =============");
        TridentState tridentState = topology.join(
                //TODO: 中间结果持久化到HBase
                stream1
                        .each(new Fields(Field.SESSIONUSERTRACKINGID, Field.SUBJECT_ID, Field.ASSESSMENTID, Field.SESSIONID, Field.SCORE, Field.USERID),
                        new CustomPersistFunction(),
                        new Fields())
                        .groupBy(new Fields(Field.ASSESSMENTID, Field.SESSIONID))
                        .persistentAggregate(HBaseMapState.transactional(sumHbaseOptions), new Fields(Field.SCORE), new Sum(), new Fields(Field.SUM))
                        .newValuesStream(),
                new Fields(Field.ASSESSMENTID, Field.SESSIONID),
                stream1.groupBy(new Fields(Field.ASSESSMENTID, Field.SESSIONID))
                        .persistentAggregate(HBaseMapState.transactional(countHbaseOptions), new Fields(Field.ASSESSMENTID, Field.SESSIONID), new Count(), new Fields(Field.COUNT))
                        .newValuesStream(),
                new Fields(Field.ASSESSMENTID, Field.SESSIONID),
                new Fields(Field.ASSESSMENTID, Field.SESSIONID, Field.SUM, Field.COUNT)
        ).partitionPersist(
                factory,
                new Fields(Field.ASSESSMENTID, Field.SESSIONID, Field.SUM, Field.COUNT),
                new MongoStateUpdater(),
                new Fields()
        );


        //1.1 存储学生明细


//        //2. 查询计算结果
//        logger.info("===============定义 查询计算结果 =============");
//        LocalDRPC drpc = new LocalDRPC();
//        topology.newDRPCStream("SumAndCount", drpc)
//                .each(new Fields("args"), new CustomSplit(), new Fields(Field.ASSESSMENTID, Field.SESSIONID))
//                .each(new Fields(Field.ASSESSMENTID,Field.SESSIONID), new Debug("true"))
//                .stateQuery(tridentState, new Fields(Field.ASSESSMENTID,Field.SESSIONID), new RedisMapGet(), new Fields("SumAndCount"));


        logger.info("===============提交topology =============");

        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        Config conf = new Config();
        conf.setDebug(false);
        Properties props = new Properties();
        props.put("producer.type", "async");
        props.put("linger.ms", "1500");
        props.put("batch.size", "16384");
        props.put("request.required.acks", "1");
//        props.put("serializer.class", "kafka.serializer.JsonEnscoder");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
        conf.put("kafka.broker.properties", props);
//        config.setNumWorkers(2);
        cluster.submitTopology("TridentTopology", config, topology.build());


//        try {
//            logger.info("===============测试 topology =============");
//            Thread.sleep(2000);
//            for(int i=0;i< 100 ; i++){
//                logger.info("=============== print result per 2 second: " + drpc.execute("SumAndCount", "389259,129678 389245,129678 199927,44776"));
//                Thread.sleep(1000);
//            }
//        }catch (InterruptedException e) {
//            e.printStackTrace();
//        }

    }
}
