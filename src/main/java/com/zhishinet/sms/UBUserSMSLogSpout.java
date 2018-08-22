package com.zhishinet.sms;

import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;

/**
 * 接受外部数据源的组件，将外部数据源转化成Storm内部的数据，以Tuple为基本的传输单元下发给Bolt
 */
public class UBUserSMSLogSpout {

    public static KafkaSpout getSpout() {
        BrokerHosts boBrokerHosts = new ZkHosts("127.0.0.1:2182");
        String topic = "UBUserSMSLog";
        String zkRoot = "/";
        String spoutId = "ubusersmslog_storm";
        SpoutConfig spoutConfig = new SpoutConfig(boBrokerHosts, topic, zkRoot, spoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new KafkaSpout(spoutConfig);
    }
}
