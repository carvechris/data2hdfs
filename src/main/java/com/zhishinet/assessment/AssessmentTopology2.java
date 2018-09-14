package com.zhishinet.assessment;


import com.zhishinet.assessment.baseFunction.CustomSplit;
import com.zhishinet.assessment.baseQueryFunction.RedisMapGet;
import com.zhishinet.assessment.redis.store.AssessmentSessionStoreMapper;
import com.zhishinet.homeworkcenter.processdata.PreProcessLauch2Tracking;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
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
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        // redis落盘方式
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost(Conf.REDIS_HOST).setPort(Conf.REDIS_PORT).build();
        RedisState.Factory redisFactory = new RedisState.Factory(poolConfig);
        RedisStoreMapper assessmentSessionStoreMapper = new AssessmentSessionStoreMapper();

        //0. 声明拓扑
        TridentTopology topology = new TridentTopology();

        //1. 流计算
        //1.1 整理出流中需要的信息
        Stream stream = topology.newStream("MyConfig",new TransactionalTridentKafkaSpout(kafkaConfig));
        Stream stream1 = stream
                // 先把Kafka中的数据解析成对象,包括 Field.SESSIONUSERTRACKINGID,Field.SUBJECT_ID, Field.ASSESSMENTID, Field.SESSIONID, Field.SCORE, Field.USERID 字段
                .each(new Fields("str"), new PreProcessLauch2Tracking(), new Fields(Field.SESSIONUSERTRACKINGID,Field.SUBJECT_ID, Field.ASSESSMENTID, Field.SESSIONID, Field.SCORE, Field.USERID));



        //1.2 分流计算Sum 和 Count , 最后join到一起.  并持久化到redis中.
        logger.info("===============定义 分流到两个stream =============");
        TridentState tridentState = topology.join(
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
        ).partitionPersist(
                redisFactory,
                new Fields(Field.ASSESSMENTID,Field.SESSIONID,Field.SUM,Field.COUNT),
                new RedisStateUpdater(assessmentSessionStoreMapper),
                new Fields()
        );

        //2. 查询计算结果
        logger.info("===============定义 查询计算结果 =============");
        LocalDRPC drpc = new LocalDRPC();
        topology.newDRPCStream("SumAndCount", drpc)
                .each(new Fields("args"), new CustomSplit(), new Fields(Field.ASSESSMENTID, Field.SESSIONID))
                .each(new Fields(Field.ASSESSMENTID,Field.SESSIONID), new Debug("true"))
                .stateQuery(tridentState, new Fields(Field.ASSESSMENTID,Field.SESSIONID), new RedisMapGet(), new Fields("SumAndCount"));



        logger.info("===============提交topology =============");

        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
//        config.setNumWorkers(2);
        cluster.submitTopology("TridentTopology",config,topology.build());


        try {
            logger.info("===============测试 topology =============");
            Thread.sleep(2000);
            for(int i=0;i< 100 ; i++){
                logger.info("=============== print result per 2 second: " + drpc.execute("SumAndCount", "389259,129678 389245,129678 199927,44776"));
                Thread.sleep(1000);
            }
        }catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
