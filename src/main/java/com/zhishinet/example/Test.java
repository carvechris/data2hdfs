package com.zhishinet.example;

import com.zhishinet.homeworkcenter.Conf;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.redis.trident.state.RedisState;
import org.apache.storm.redis.trident.state.RedisStateQuerier;
import org.apache.storm.redis.trident.state.RedisStateUpdater;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Test {

    public static StormTopology buildTopology(){
        Fields fields = new Fields("word", "count");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
                new Values("storm", 1),
                new Values("trident", 1),
                new Values("needs", 1),
                new Values("javadoc", 1)
        );
        spout.setCycle(true);

        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost(Conf.REDIS_HOST).setPort(Conf.REDIS_PORT).build();
        RedisState.Factory redisFactory = new RedisState.Factory(poolConfig);

        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        RedisLookupMapper lookupMapper = new WordCountLookupMapper();


        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        stream.partitionPersist(redisFactory,
                fields,
                new RedisStateUpdater(storeMapper).withExpire(86400000),
                new Fields());

        TridentState state = topology.newStaticState(redisFactory);
        stream = stream.stateQuery(state, new Fields("word"),
                new RedisStateQuerier(lookupMapper),
                new Fields("columnName","columnValue")).each(new Fields("word","columnValue"), new PrintFunction(), new Fields());


        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test_wordCounter_for_redis",new Config(),buildTopology());
    }
}
