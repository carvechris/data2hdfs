package com.zhishinet.homeworkcenter;

import com.zhishinet.homeworkcenter.redis.AssessmentLookupMapper;
import com.zhishinet.homeworkcenter.redis.AssessmentStoreMapper1;
import com.zhishinet.homeworkcenter.redis.AssessmentStoreMapper2;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.redis.trident.state.RedisState;
import org.apache.storm.redis.trident.state.RedisStateQuerier;
import org.apache.storm.redis.trident.state.RedisStateUpdater;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/8/29 13:10
 */
public class HomeworkCenterTopology1 {

    private static Logger logger = LoggerFactory.getLogger(HomeworkCenterTopology1.class);

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
            collector.emit(new Values(assessmentId, sessionId, score,userId));
        }
    }




    public static void main(String[] args) throws InterruptedException {

        BrokerHosts boBrokerHosts = new ZkHosts(Conf.ZOOKEEPER_LIST);
        final String spoutId = "HomewrokCenter_storm";
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(boBrokerHosts, Conf.TOPIC_HOMEWROKCENTER, spoutId);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        // redis落盘方式
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost(Conf.REDIS_HOST).setPort(Conf.REDIS_PORT).build();
        RedisState.Factory redisFactory = new RedisState.Factory(poolConfig);

        RedisStoreMapper storeMapper1 = new AssessmentStoreMapper1();
        RedisStoreMapper storeMapper2 = new AssessmentStoreMapper2();
        RedisLookupMapper lookupMapper = new AssessmentLookupMapper();

        TridentTopology topology = new TridentTopology();
        TridentState state = topology.newStaticState(redisFactory);
        Stream stream = topology.newStream("KafkaSpout",new TransactionalTridentKafkaSpout(kafkaConfig))
                .each(new Fields("str"), new PreProcessLaunchData(), new Fields(Field.FIELD_ASSESSMENTID, Field.FIELD_SESSIONID, Field.FIELD_SCORE, Field.FIELD_USERID));

        stream
                .partitionPersist(
                    redisFactory,
                    new Fields(Field.FIELD_ASSESSMENTID, Field.FIELD_SESSIONID, Field.FIELD_SCORE, Field.FIELD_USERID),
                    new RedisStateUpdater(storeMapper1),
                    new Fields()
                );

        stream
                .stateQuery(
                        state,
                        new Fields(Field.FIELD_ASSESSMENTID, Field.FIELD_SESSIONID, Field.FIELD_USERID),
                        new RedisStateQuerier(lookupMapper),
                        new Fields(Field.FIELD_SUM, Field.FIELD_COUNT))
                .partitionPersist(
                        redisFactory,
                        new Fields(Field.FIELD_ASSESSMENTID, Field.FIELD_SESSIONID, Field.FIELD_SUM, Field.FIELD_COUNT),
                        new RedisStateUpdater(storeMapper2),
                        new Fields()
                );
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TridentTopology",new Config(),topology.build());
    }
}
