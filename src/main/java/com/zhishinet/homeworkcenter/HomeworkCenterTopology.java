package com.zhishinet.homeworkcenter;

import com.zhishinet.Conf;
import com.zhishinet.homeworkcenter.redis.AssessmentStoreMapper;
import javafx.collections.transformation.FilteredList;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.redis.trident.state.RedisClusterState;
import org.apache.storm.redis.trident.state.RedisClusterStateUpdater;
import org.apache.storm.redis.trident.state.RedisState;
import org.apache.storm.redis.trident.state.RedisStateUpdater;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String json = tuple.getString(0);
            Document launch = Document.parse(json);
            final Integer assessmentId = launch.getInteger(Field.FIELD_ASSESSMENTID);
            final Integer sessionId = launch.getInteger(Field.FIELD_SESSIONID);
            final Double score = launch.getDouble(Field.FIELD_SCORE);
            System.out.println("======预处理数据==========");
            System.out.println("AssessmentId : " + assessmentId + " , SessionId : "+ sessionId + " ,Score : " + score);
            System.out.println("======预处理数据==========");
            //从HBase取 sum  count
            collector.emit(new Values(assessmentId, sessionId, score));

        }
    }

    //从redis查询数据
    public static class FetchSumAndCountFromReids extends BaseQueryFunction<RedisState,String> {
        private static final String REDIS_PREFIX = "strom:trident:";

        @Override
        public List<String> batchRetrieve(RedisState redisState, List<TridentTuple> list) {
            List<String> ret = new ArrayList();
            System.out.println("----------Redis查询-----------------");
            list.stream().forEach(t -> System.out.println("AsessmentId : " + t.getInteger(0) +" ,SessionId : "+ t.getInteger(1) +" ,Score : "+ t.getDoubleByField("Score")));
            for(TridentTuple input: list) {
                ret.add(redisState.getJedis().get(REDIS_PREFIX + input.getIntegerByField(Field.FIELD_ASSESSMENTID) + ":" +input.getIntegerByField(Field.FIELD_SESSIONID)));
            }
            System.out.println("++++++++查询到的结果++++++++++++");
            ret.stream().forEach(s -> System.out.println(s));
            return ret;
        }

        @Override
        public void execute(TridentTuple tridentTuple, String s, TridentCollector tridentCollector) {
            if(s == null || "".equals(s)){
                System.out.println("数据缓存没有查询到");
                System.out.println("想外发射数据 AssessmentId : " + tridentTuple.getIntegerByField("AssessmentId") + " ,SessionId : " + tridentTuple.getIntegerByField("SessionId") + ",Score : " + tridentTuple.getDoubleByField("Score"));
                tridentCollector.emit(new Values(tridentTuple.getDoubleByField(Field.FIELD_SCORE), 1));
            } else{
                System.out.println("S 的值是 : "+ s);
                String[] result = s.split(":");
                tridentCollector.emit(new Values( tridentTuple.getDoubleByField(Field.FIELD_SCORE) + Double.valueOf(result[0]), Integer.valueOf(result[1]) + 1));
            }
        }
    }


    public static void main(String[] args) {

        BrokerHosts boBrokerHosts = new ZkHosts("macos:2181");
        String topic = "HomewrokCenter";
        String spoutId = "HomewrokCenter_storm";
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(boBrokerHosts, topic, spoutId);
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
                .withFsUrl(Conf.HDFS_URI);
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
        topology.newStream("KafkaSpout",new TransactionalTridentKafkaSpout(kafkaConfig))
                .each(new Fields("str"), new PreProcessLaunchData(), new Fields(Field.FIELD_ASSESSMENTID, Field.FIELD_SESSIONID, Field.FIELD_SCORE))
                .stateQuery(redisState, new Fields(Field.FIELD_ASSESSMENTID, Field.FIELD_SESSIONID, Field.FIELD_SCORE), new FetchSumAndCountFromReids(), new Fields(Field.FIELD_SUM, Field.FIELD_COUNT))
                //HDFS落盘
//                .partitionPersist(hdfsFactory, persistFields, new HdfsUpdater(), new Fields());
                //Redis落盘
                .partitionPersist(redisFactory, persistFields, new RedisStateUpdater(storeMapper), new Fields());

        //运行
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TridentTopology",new Config(),topology.build());

    }
}
