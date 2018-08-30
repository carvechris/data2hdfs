package com.zhishinet.homeworkcenter;

import com.zhishinet.homeworkcenter.redis.AssessmentStoreMapper;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.redis.trident.state.RedisState;
import org.apache.storm.redis.trident.state.RedisStateUpdater;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.*;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/8/29 13:10
 */
public class HomeworkCenterTopology {

    //数据预处理.这里可以将数据处理成我们想要的结构
    public static class PreProcessLaunchData extends BaseFunction {
        private static Logger logger = LoggerFactory.getLogger(PreProcessLaunchData.class);
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String json = tuple.getString(0);
            Document launch = null;
            try {
             launch = Document.parse(json);
            }catch (Exception ex) {
                logger.error("Parse Document exception", ex);
            }
            final Integer assessmentId = launch.getInteger(Field.FIELD_ASSESSMENTID);
            final Integer sessionId = launch.getInteger(Field.FIELD_SESSIONID);
            final Double score = launch.getDouble(Field.FIELD_SCORE);
            final Integer userId = launch.getInteger(Field.FIELD_USERID);
            logger.info("Rece Kafka Message AssessmentId : {} ,SessionId : {} ,UserId : {} ,Score : {}",assessmentId,sessionId,userId,score);
            collector.emit(new Values(assessmentId, sessionId, score,userId));
        }
    }

    //从redis查询数据
    public static class FetchSumAndCountFromReids extends BaseQueryFunction<RedisState,String> {
        private static Logger logger = LoggerFactory.getLogger(FetchSumAndCountFromReids.class);
        private static final String REDIS_PREFIX = "strom:trident:";
        @Override
        public List<String> batchRetrieve(RedisState redisState, List<TridentTuple> tridentTuples) {
            List<String> ret = new ArrayList();
            for(int i = 0 ;i <tridentTuples.size(); i++) {
                TridentTuple tuple = tridentTuples.get(i);
                final Integer assessmentId = tuple.getIntegerByField(Field.FIELD_ASSESSMENTID);
                final Integer sessionId = tuple.getIntegerByField(Field.FIELD_SESSIONID);
                final Double score = tuple.getDoubleByField(Field.FIELD_SCORE);
                logger.info("Redis Query Method tridentTuples[{}] Paramter AsessmentId : {} ,SessionId: {} ,Score : {}", i, assessmentId, sessionId, score);
                String valueOfRedis = redisState.getJedis().get(REDIS_PREFIX + assessmentId + ":" + sessionId);
                logger.info("Get value of redis {}{}:{} ,Result : {}",REDIS_PREFIX,assessmentId,sessionId,valueOfRedis);
                ret.add(valueOfRedis);
            }
            return ret;
        }

        @Override
        public void execute(TridentTuple tridentTuple, String s, TridentCollector tridentCollector) {
            logger.info("Redis execute Method Paramter tridentTuple : {} , s : {}", tridentTuple, s);
            if(s == null || "".equals(s)){
                System.out.println("数据缓存没有查询到");
                System.out.println("想外发射数据 AssessmentId : " + tridentTuple.getIntegerByField("AssessmentId") + " ,SessionId : " + tridentTuple.getIntegerByField("SessionId") + ",Score : " + tridentTuple.getDoubleByField("Score"));
                tridentCollector.emit(new Values(tridentTuple.getDoubleByField(Field.FIELD_SCORE), 1));
            } else {
                String[] result = s.split(":");
                tridentCollector.emit(new Values( tridentTuple.getDoubleByField(Field.FIELD_SCORE) + Double.valueOf(result[0]), Integer.valueOf(result[1]) + 1));
            }
        }
    }

//    protected static KafkaSpoutConfig<String, String> newKafkaSpoutConfig(String bootstrapServers) {
//        return KafkaSpoutConfig.builder(bootstrapServers, TOPIC_1, TOPIC_2)
//                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup_" + System.nanoTime())
//                .setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 200)
//                .setRecordTranslator(JUST_VALUE_FUNC, new Fields("str"))
//                .setRetry(newRetryService())
//                .setOffsetCommitPeriodMs(10_000)
//                .setFirstPollOffsetStrategy(EARLIEST)
//                .setMaxUncommittedOffsets(250)
//                .build();
//    }

    public static void main(String[] args) {
        BrokerHosts boBrokerHosts = new ZkHosts(Conf.ZOOKEEPER_LIST);
        final String spoutId = "HomewrokCenter_storm";

        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(boBrokerHosts, Conf.TOPIC_HOMEWROKCENTER, spoutId);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        //HDFS 落盘方式
        Fields persistFields = new Fields(Field.FIELD_ASSESSMENTID, Field.FIELD_SESSIONID,Field.FIELD_SUM,Field.FIELD_COUNT);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPrefix("trident")
                .withExtension(".txt")
                .withPath("/user/tomaer/trident");
        RecordFormat recordFormat = new DelimitedRecordFormat()
                .withFields(persistFields).withFieldDelimiter("\001");
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.MB);
        HdfsState.Options options = new HdfsState.HdfsFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(rotationPolicy)
                .withFsUrl(Conf.HDFS_URL);
        StateFactory hdfsFactory = new HdfsStateFactory().withOptions(options);

        // redis落盘方式
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost(Conf.REDIS_HOST).setPort(Conf.REDIS_PORT)
                .build();
        RedisState.Factory redisFactory = new RedisState.Factory(poolConfig);
        //redis读写和实体映射
        RedisStoreMapper storeMapper = new AssessmentStoreMapper();

        //TODO: HBASE落盘方式





        //构建TridentTopology, 流式API将数据处理为想要的形式
        TridentTopology topology = new TridentTopology();
        TridentState redisState = topology.newStaticState(redisFactory);
        Stream stream = topology.newStream("KafkaSpout",new TransactionalTridentKafkaSpout(kafkaConfig));
        stream.each(new Fields("str"), new PreProcessLaunchData(), new Fields(Field.FIELD_ASSESSMENTID, Field.FIELD_SESSIONID, Field.FIELD_SCORE, Field.FIELD_USERID))
                //.partitionBy(new Fields(Field.FIELD_ASSESSMENTID,Field.FIELD_SESSIONID,Field.FIELD_USERID))
                .filter(new Filter() {
                    @Override
                    public boolean isKeep(TridentTuple tuple) {
                        final Integer assessmentId = tuple.getIntegerByField(Field.FIELD_ASSESSMENTID);
                        final Integer sessionId = tuple.getIntegerByField(Field.FIELD_SESSIONID);
                        final Integer userId = tuple.getIntegerByField(Field.FIELD_USERID);
                        //查询Redis, 有就返回 false。没有返回 true
                        return true;
                    }

                    @Override
                    public void prepare(Map conf, TridentOperationContext context) {

                    }

                    @Override
                    public void cleanup() {

                    }
                })
                .stateQuery(redisState, new Fields(Field.FIELD_ASSESSMENTID, Field.FIELD_SESSIONID, Field.FIELD_SCORE), new FetchSumAndCountFromReids(), new Fields(Field.FIELD_SUM, Field.FIELD_COUNT)).parallelismHint(1)
                //HDFS落盘
//                .partitionPersist(hdfsFactory, persistFields, new HdfsUpdater(), new Fields());
                //Redis落盘
                .partitionPersist(redisFactory, persistFields, new RedisStateUpdater(storeMapper), new Fields());

        //运行
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TridentTopology",new Config(),topology.build());

    }
}
