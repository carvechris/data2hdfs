package com.zhishinet.homeworkcenter;

import com.google.common.collect.Maps;
import com.zhishinet.homeworkcenter.redis.AssessmentStoreMapper;
import org.apache.commons.lang.StringUtils;
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
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
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
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
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
public class HomeworkCenterTopology1 {

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
            Map<String, String> batchMap = Maps.newHashMap();
            for(int i = 0 ;i <tridentTuples.size(); i++) {
                TridentTuple tuple = tridentTuples.get(i);
                final Integer assessmentId = tuple.getIntegerByField(Field.FIELD_ASSESSMENTID);
                final Integer sessionId = tuple.getIntegerByField(Field.FIELD_SESSIONID);
                final Double score = tuple.getDoubleByField(Field.FIELD_SCORE);
                final String valueOfRedis = redisState.getJedis().get(REDIS_PREFIX + assessmentId + ":" + sessionId);
                logger.info("Redis key : {}, value : {}", REDIS_PREFIX + assessmentId + ":" + sessionId, valueOfRedis);
                ret.add(valueOfRedis);
            }
            return ret;
        }

        @Override
        public void execute(TridentTuple tridentTuple, String result, TridentCollector tridentCollector) {
            final Integer assessmentId = tridentTuple.getIntegerByField(Field.FIELD_ASSESSMENTID);
            final Integer sessionId = tridentTuple.getIntegerByField(Field.FIELD_SESSIONID);
            final Double score = tridentTuple.getDoubleByField(Field.FIELD_SCORE);
            logger.info("AssessmentId : {} ,SessionId : {}, Score : {} ,Result : {}", assessmentId, sessionId, score, result);
            final String key = REDIS_PREFIX + assessmentId + ":" + sessionId;
            String value = "";
            if (StringUtils.isBlank(result)) {
                value = score + ":" + 1;
            } else {
                final String [] results = result.split(":");
                value = (Double.valueOf(results[0]) + score) + ":" + (Integer.valueOf(results[1])+ 1);
            }
            tridentCollector.emit(new Values(key, value));
        }
    }

    public static void main(String[] args) {
        BrokerHosts boBrokerHosts = new ZkHosts(Conf.ZOOKEEPER_LIST);
        final String spoutId = "HomewrokCenter_storm";
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(boBrokerHosts, Conf.TOPIC_HOMEWROKCENTER, spoutId);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        Fields persistFields = new Fields("key","value");
        // redis落盘方式
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost(Conf.REDIS_HOST).setPort(Conf.REDIS_PORT).build();
        RedisState.Factory redisFactory = new RedisState.Factory(poolConfig);
        //redis读写和实体映射
        RedisStoreMapper storeMapper = new AssessmentStoreMapper();

        //构建TridentTopology, 流式API将数据处理为想要的形式
        TridentTopology topology = new TridentTopology();
        TridentState redisState = topology.newStaticState(redisFactory);
        Stream stream = topology.newStream("KafkaSpout",new TransactionalTridentKafkaSpout(kafkaConfig));
        stream.each(new Fields("str"), new PreProcessLaunchData(), new Fields(Field.FIELD_ASSESSMENTID, Field.FIELD_SESSIONID, Field.FIELD_SCORE, Field.FIELD_USERID))
        .stateQuery(
                redisState,
                new Fields(Field.FIELD_ASSESSMENTID, Field.FIELD_SESSIONID, Field.FIELD_SCORE),
                new FetchSumAndCountFromReids(),
                new Fields("key", "value")
        ).partitionPersist(redisFactory, persistFields, new RedisStateUpdater(storeMapper), new Fields());
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TridentTopology",new Config(),topology.build());

    }
}
