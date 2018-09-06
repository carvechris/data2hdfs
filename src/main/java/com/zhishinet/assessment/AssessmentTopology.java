package com.zhishinet.assessment;

import com.zhishinet.assessment.redis.AssessmentLookupMapper;
import com.zhishinet.assessment.redis.store.AssessmentSessionUserStoreMapper;
import com.zhishinet.assessment.redis.AssessmentStoreMapper2;
import com.zhishinet.assessment.redis.filter.AssessmentFilter;
import com.zhishinet.assessment.redis.lookup.AssessmentExistsLookupMapper;
import com.zhishinet.example.PrintFunction;
import com.zhishinet.example.Test;
import com.zhishinet.homeworkcenter.Conf;
import com.zhishinet.homeworkcenter.Field;
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
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/8/29 13:10
 */
public class AssessmentTopology {

    private static Logger logger = LoggerFactory.getLogger(AssessmentTopology.class);


    public static void main(String[] args) throws InterruptedException {

        BrokerHosts boBrokerHosts = new ZkHosts(Conf.ZOOKEEPER_LIST);
        final String spoutId = "HomeworkCenter_storm";
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(boBrokerHosts, Conf.TOPIC_HOMEWORKCENTER, spoutId);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        // redis落盘方式
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost(Conf.REDIS_HOST).setPort(Conf.REDIS_PORT).build();
        RedisState.Factory redisFactory = new RedisState.Factory(poolConfig);

        RedisStoreMapper assessmentSessionUserStoreMapper = new AssessmentSessionUserStoreMapper();
        RedisStoreMapper storeMapper2 = new AssessmentStoreMapper2();
        RedisLookupMapper lookupMapper = new AssessmentLookupMapper();
        RedisLookupMapper assessmentExistsLookupMapper = new AssessmentExistsLookupMapper();

        TridentTopology topology = new TridentTopology();
        TridentState state = topology.newStaticState(redisFactory);
        Stream stream = topology.newStream("KafkaSpout",new TransactionalTridentKafkaSpout(kafkaConfig));
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
                .filter(
                        new Fields(Field.ASSESSMENTID, Field.SESSIONID, Field.USERID, Field.SCORE, Field.ASSESSMENT_EXISTS),
                        new AssessmentFilter()
                );

        stream1
                .partitionPersist(
                        redisFactory,
                        new Fields(Field.ASSESSMENTID, Field.SESSIONID, Field.SCORE, Field.USERID),
                        new RedisStateUpdater(assessmentSessionUserStoreMapper),
                        new Fields()
                )
                .newValuesStream();

        // 查询已经有的作业班级总分 总提交人数
        Stream stream2 = stream
                .stateQuery(
                        state,
                        new Fields(Field.ASSESSMENTID, Field.SESSIONID),
                        new RedisStateQuerier(assessmentExistsLookupMapper),
                        new Fields(Test.Field.SUM,Test.Field.COUNT)
                );

        // 消息中的 作业班级总分，总提交人数
        Stream stream3 = topology.join(
                stream1
                    .groupBy(new Fields(Test.Field.ASSESSMENTID,Test.Field.SESSIONID))
                    .persistentAggregate(new MemoryMapState.Factory(),new Fields(Test.Field.SCORE),new Sum(), new Fields(Test.Field.SUM))
                    .newValuesStream(),
                    new Fields(Test.Field.ASSESSMENTID,Test.Field.SESSIONID),
                stream1
                    .groupBy(new Fields(Test.Field.ASSESSMENTID,Test.Field.SESSIONID))
                    .persistentAggregate(new MemoryMapState.Factory(),new Fields(Test.Field.ASSESSMENTID,Test.Field.SESSIONID),new Count(), new Fields(Test.Field.COUNT))
                    .newValuesStream(),
                    new Fields(Test.Field.ASSESSMENTID,Test.Field.SESSIONID),
                new Fields(Test.Field.ASSESSMENTID,Test.Field.SESSIONID,Test.Field.SUM,Test.Field.COUNT)
        );
        topology.join(
                stream2,
                new Fields(Test.Field.ASSESSMENTID,Test.Field.SESSIONID),
                stream3,
                new Fields(Test.Field.ASSESSMENTID,Test.Field.SESSIONID),
                new Fields(Test.Field.ASSESSMENTID,Test.Field.SESSIONID,Test.Field.SUM,Test.Field.COUNT)
        )
        .partitionPersist(
                redisFactory,
                new Fields(Field.ASSESSMENTID, Field.SESSIONID, Field.SCORE, Field.USERID),
                new RedisStateUpdater(assessmentSessionUserStoreMapper),
                new Fields()
        );
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TridentTopology",new Config(),topology.build());
    }
}
