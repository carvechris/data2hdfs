package dengzhilong;

import org.apache.storm.Config;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author zhilong.deng
 * created by 邓志龙
 * zhilong.deng@hand-china.com
 * 2018/4/16
 */
public class CustomConfig{
    private SpoutConfig spoutConfig;

    private Config conf;



    public CustomConfig() {
        this.setConf();
        this.setSpoutConfig();
    }

    public SpoutConfig getSpoutConfig() {
        return spoutConfig;
    }

    private void setSpoutConfig() {
        String topic = "UBUserSMSLog1" ;
        ZkHosts zkHosts = new ZkHosts("localhost:2181");
        //ZkHosts zkHosts = new ZkHosts("erpprdap25:2181,erpprdap27:2181,erpprdap28:2181");
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic,
                "",
                "kafkaStorm3") ;
        List<String> zkServers = new ArrayList<>() ;
        for (String host : zkHosts.brokerZkStr.split(",")) {
            zkServers.add(host.split(":")[0]);
        }
        spoutConfig.zkServers = zkServers;
        spoutConfig.zkPort = 2181;
        spoutConfig.socketTimeoutMs = 60 * 1000 ;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()) ;
        spoutConfig.useStartOffsetTimeIfOffsetOutOfRange=true;
        this.spoutConfig = spoutConfig;
    }

    public Config getConf() {
        return conf;
    }

    private void setConf() {
        Config conf=new Config();
        conf.setDebug(false) ;
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        //props.put("metadata.broker.list", "erpprdap25:6667,erpprdap27:6667,erpprdap28:6667");
        props.put("producer.type","async");
        props.put("linger.ms","1500");
        props.put("batch.size","16384");
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEnscoder");
        /*conf.put(Config.WORKER_HEAP_MEMORY_MB,             2048);
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB,  20480);*/
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,            32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    16384);
        conf.put("kafka.broker.properties", props);
        this.conf = conf;
    }

}
