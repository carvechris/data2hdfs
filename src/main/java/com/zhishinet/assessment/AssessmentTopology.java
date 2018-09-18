package com.zhishinet.assessment;

import com.zhishinet.assessment.redis.filter.AssessmentFilter;
import com.zhishinet.assessment.redis.lookup.AssessmentExistsLookupMapper;
import com.zhishinet.assessment.redis.store.AssessmentSessionTotalScoreTotalCountStoreMapper;
import com.zhishinet.assessment.redis.store.AssessmentSessionUserStoreMapper;
import com.zhishinet.homeworkcenter.processdata.PreProcessLauch2Tracking;
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
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * tomaer
 */
public class AssessmentTopology {

    private static Logger logger = LoggerFactory.getLogger(AssessmentTopology.class);


    public static void main(String[] args) {

        BrokerHosts boBrokerHosts = new ZkHosts(Conf.ZOOKEEPER_LIST);
        final String spoutId = "HomeworkCenter_storm";
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(boBrokerHosts, Conf.TOPIC_HOMEWORKCENTER, spoutId);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        // redis落盘方式
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost(Conf.REDIS_HOST).setPort(Conf.REDIS_PORT).build();
        RedisState.Factory redisFactory = new RedisState.Factory(poolConfig);

        RedisStoreMapper assessmentSessionUserStoreMapper = new AssessmentSessionUserStoreMapper();
        RedisStoreMapper assessmentSessionTotalScoreTotalCountStoreMapper = new AssessmentSessionTotalScoreTotalCountStoreMapper();

        RedisLookupMapper assessmentExistsLookupMapper = new AssessmentExistsLookupMapper();

        TridentTopology topology = new TridentTopology();
        TridentState state = topology.newStaticState(redisFactory);

        Stream stream = topology.newStream("MyConfig",new TransactionalTridentKafkaSpout(kafkaConfig));
        Stream stream1 = stream
                // 先把Kafka中的数据解析成对象,包括 Field.SESSIONUSERTRACKINGID,Field.SUBJECT_ID, Field.ASSESSMENTID, Field.SESSIONID, Field.SCORE, Field.USERID 字段
                .each(new Fields("str"), new PreProcessLauch2Tracking(), new Fields(Field.SESSIONUSERTRACKINGID,Field.SUBJECT_ID, Field.ASSESSMENTID, Field.SESSIONID, Field.SCORE, Field.USERID))
                // 查询这条数据是否已经被处理过
                .stateQuery(
                        state,
                        new Fields(Field.ASSESSMENTID, Field.SESSIONID, Field.USERID, Field.SCORE),
                        new RedisStateQuerier(assessmentExistsLookupMapper),
                        new Fields(Field.ASSESSMENT_EXISTS)
                )
                //处理过的数据直接忽略掉
                .filter(
                        new Fields(Field.ASSESSMENTID, Field.SESSIONID, Field.USERID, Field.SCORE, Field.ASSESSMENT_EXISTS),
                        new AssessmentFilter()
                );
                /**
                 * stream1都是没有被处理过的数据
                 * 将没被处理过的数据 写入redis key :
                 * REDIS_KEY_PREFIX + Field.ASSESSMENTID + "_" + tuple.getIntegerByField(Field.ASSESSMENTID) + ":" + Field.SESSIONID + "_" + tuple.getIntegerByField(Field.SESSIONID) + ":" + Field.USERID + "_" + tuple.getIntegerByField(Field.USERID)
                 */
                stream1
                    .partitionPersist(
                            redisFactory,
                            new Fields(Field.ASSESSMENTID, Field.SESSIONID, Field.SCORE, Field.USERID),
                            new RedisStateUpdater(assessmentSessionUserStoreMapper),
                            new Fields(Field.ASSESSMENTID, Field.SESSIONID, Field.SCORE, Field.USERID)
                    )
                    .newValuesStream();


//        // 查询已经有的作业班级总分 总提交人数
//        Stream stream2 = stream1
//                .stateQuery(
//                        state,
//                        new Fields(Field.ASSESSMENTID, Field.SESSIONID),
//                        new RedisStateQuerier(assessmentExistsLookupMapper),
//                        new Fields(Field.SUM,Field.COUNT)
//                )
//                .groupBy(
//                        new Fields(Field.ASSESSMENTID,Field.SESSIONID,Field.SUM,Field.COUNT)
//                )
//                /**
//                 * 因为根据 Field.ASSESSMENTID,Field.SESSIONID,Field.SUM,Test.Field.COUNT 可能会查询出来多条值
//                 * 对这些值进行计数操作之后就只有有一条值，进行镜像操作，去除最后的count
//                 */
//                .persistentAggregate(
//                        new MemoryMapState.Factory(),
//                        new Fields(Field.ASSESSMENTID,Field.SESSIONID,Field.SUM,Field.COUNT),
//                        new Count(),
//                        new Fields(Field.DISTINCT_RECORDS_FIELD)
//                )
//                .newValuesStream()
//                .project(
//                        new Fields(Field.ASSESSMENTID,Field.SESSIONID,Field.SUM,Field.COUNT)
//                );
        // 消息中的 作业班级总分，总提交人数
        Stream stream3 = topology.join(
                stream1
                    .groupBy(new Fields(Field.ASSESSMENTID,Field.SESSIONID))
                    .persistentAggregate(new MemoryMapState.Factory(),new Fields(Field.SCORE),new Sum(), new Fields(Field.SUM))
                    .newValuesStream(),
                    new Fields(Field.ASSESSMENTID,Field.SESSIONID),
                stream1
                    .groupBy(new Fields(Field.ASSESSMENTID,Field.SESSIONID))
                    .persistentAggregate(new MemoryMapState.Factory(),new Fields(Field.ASSESSMENTID,Field.SESSIONID),new Count(), new Fields(Field.COUNT))
                    .newValuesStream(),
                    new Fields(Field.ASSESSMENTID,Field.SESSIONID),
                new Fields(Field.ASSESSMENTID,Field.SESSIONID,Field.SUM,Field.COUNT)
        );

//        Stream stream4 = topology.merge(stream2,stream3);
//
//        topology.join(
//                stream4
//                        .groupBy(new Fields(Field.ASSESSMENTID,Field.SESSIONID))
//                        .persistentAggregate(new MemoryMapState.Factory(),new Fields(Field.SUM),new Sum(), new Fields(Field.TOTAL_SUM))
//                        .newValuesStream(),
//                        new Fields(Field.ASSESSMENTID,Field.SESSIONID),
//                stream4
//                        .groupBy(new Fields(Field.ASSESSMENTID,Field.SESSIONID))
//                        .persistentAggregate(new MemoryMapState.Factory(),new Fields(Field.COUNT),new Sum(), new Fields(Field.TOTAL_COUNT))
//                        .newValuesStream(),
//                        new Fields(Field.ASSESSMENTID,Field.SESSIONID),
//                new Fields(Field.ASSESSMENTID,Field.SESSIONID,Field.TOTAL_SUM,Field.TOTAL_COUNT)
//        )
//        .partitionPersist(
//            redisFactory,
//            new Fields(Field.ASSESSMENTID,Field.SESSIONID,Field.TOTAL_SUM,Field.TOTAL_COUNT),
//            new RedisStateUpdater(assessmentSessionTotalScoreTotalCountStoreMapper),
//            new Fields()
//        );
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
//        config.setNumWorkers(2);
        cluster.submitTopology("TridentTopology",config,topology.build());
    }
}
