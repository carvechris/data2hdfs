package com.zhishinet;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class MyConfig {

    public static final String ZK_HOSTS = "bigdata-ambari-agent-1:2181,bigdata-ambari-agent-2:2181,bigdata-ambari-agent-3:2181";
//    public static final String ZK_HOSTS = "172.30.74.239:2181,172.30.74.240:2181,172.30.74.241:2181";
//    public static final String ZK_HOSTS = "127.0.0.1:2181";
    public static final int ZK_PORT = 2181;

    public static final String ZK_ROOT = StringUtils.EMPTY;
    public static final String KAFKA_BROKERS = "bigdata-ambari-agent-1:6667,bigdata-ambari-agent-2:6667,bigdata-ambari-agent-3:6667";
//    public static final String KAFKA_BROKERS = "172.30.74.239:6667,172.30.74.240:6667,172.30.74.241:6667";
//    public static final String KAFKA_BROKERS = "127.0.0.1:9092";
    public static final String HDFS_URL = "hdfs://bigdata-ambari-agent-1:8020";
//    public static final String HDFS_URL = "hdfs://172.30.74.239:8020";

    public static SpoutConfig getKafkaSpoutConfig(final String topic, final String zks, final String zkRoot, final String spoutId) {
        ZkHosts zkHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, zkRoot, spoutId);
        List<String> zkServers = new ArrayList<>() ;
        Arrays.stream(zkHosts.brokerZkStr.split(",")).forEach(host-> zkServers.add(host.split(":")[0]));
        spoutConfig.zkServers = zkServers;
        spoutConfig.zkPort = ZK_PORT;
        spoutConfig.socketTimeoutMs = 60 * 1000 ;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()) ;
        spoutConfig.useStartOffsetTimeIfOffsetOutOfRange=true;
        return spoutConfig;
    }

    public static Config getConfigWithKafkaConsumerProps(final boolean debug,final String brokers) {
        Config conf = new Config();
        conf.setDebug(false);
        Properties props = new Properties();
        props.put("metadata.broker.list", brokers);
        props.put("producer.type","async");
        props.put("linger.ms","1500");
        props.put("batch.size","16384");
        props.put("request.required.acks", "1");
        props.put("deserializer.encoding","UTF8");
        props.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,            32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    16384);
        conf.put("kafka.broker.properties", props);
        return conf;
    }
}
