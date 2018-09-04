package com.zhishinet.assessment;

import com.zhishinet.assessment.redis.AssessmentLookupMapper;
import com.zhishinet.assessment.redis.AssessmentStoreMapper1;
import com.zhishinet.assessment.redis.AssessmentStoreMapper2;
import com.zhishinet.example.PrintFunction;
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

        RedisStoreMapper storeMapper1 = new AssessmentStoreMapper1();
        RedisStoreMapper storeMapper2 = new AssessmentStoreMapper2();
        RedisLookupMapper lookupMapper = new AssessmentLookupMapper();

        TridentTopology topology = new TridentTopology();
        TridentState state = topology.newStaticState(redisFactory);
        Stream stream = topology.newStream("KafkaSpout",new TransactionalTridentKafkaSpout(kafkaConfig));
        Stream stream1 = stream
                .each(new Fields("str"), new PreProcessLauch2Tracking(), new Fields(Field.SESSIONUSERTRACKINGID,Field.SUBJECT_ID, Field.ASSESSMENTID, Field.SESSIONID, Field.SCORE, Field.USERID));

        stream1.partitionPersist(redisFactory,new Fields(Field.ASSESSMENTID, Field.SESSIONID, Field.SCORE, Field.USERID),new RedisStateUpdater(storeMapper1),new Fields());

        stream1
                .stateQuery(
                    state,
                    new Fields(Field.ASSESSMENTID, Field.SESSIONID, Field.USERID),
                    new RedisStateQuerier(lookupMapper),new Fields(Field.TOTAL_SCORE)
                )
//                .each(
//                        new Fields(Field.ASSESSMENTID, Field.SESSIONID, Field.USERID,Field.TOTAL_SCORE),
//                        new PrintFunction(),
//                        new Fields()
//                )
                .groupBy(
                    new Fields(Field.ASSESSMENTID, Field.SESSIONID)
//                ).aggregate(
//                    new Fields(Field.ASSESSMENTID, Field.SESSIONID),
//                    new Count(),
//                    new Fields(Field.COUNT)
                ).aggregate(
                    new Fields(Field.ASSESSMENTID, Field.SESSIONID),
                    new Sum(),
                    new Fields(Field.SUM)
                ).each(
                    new Fields(Field.ASSESSMENTID, Field.SESSIONID,Field.SUM),
                    new PrintFunction(),
                    new Fields()
                );
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TridentTopology",new Config(),topology.build());
    }
}