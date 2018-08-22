package com.zhishinet.sms;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.Map;

public class UBUserSMSLogDataProcessBolt extends BaseRichBolt {

    private OutputCollector collector;
    private static final String FIELD_SEPARATOR = "\001";

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        final String json = tuple.getString(0);
        System.out.println("Kafka 中的数据为: "+ json);
        ObjectMapper objectMapper = new ObjectMapper();
        UBUserSMSLog log = null;
        try {
            log = objectMapper.readValue(json,UBUserSMSLog.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        StringBuilder result = new StringBuilder(log.getId()).append(FIELD_SEPARATOR);
        result.append(log.getKey()).append(FIELD_SEPARATOR);
        result.append(log.getMobilePhoneNo()).append(FIELD_SEPARATOR);
        result.append(log.getCode()).append(FIELD_SEPARATOR);
        result.append(log.getState()).append(FIELD_SEPARATOR);
        result.append(log.getReturnMsg()).append(FIELD_SEPARATOR);
        result.append(log.getPostTime()).append(FIELD_SEPARATOR);
        result.append(log.getCreatedOn()).append(FIELD_SEPARATOR);
        this.collector.emit(new Values(result.toString()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("result"));
    }
}
