package com.zhishinet.sms;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhishinet.sms.UBUserSMSLog;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class KakfaProceduer {

    public static void main(String[] args) throws InterruptedException, JsonProcessingException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者发送消息
        String topic = "UBUserSMSLog";
        Producer<String, String> procuder = new KafkaProducer<String,String>(props);

        UBUserSMSLog ubUserSMSLog1 = new UBUserSMSLog(
               1, "SMS_Register", "18621811839", "9702", 1, "短信发送成功", "2015-12-14 21:04:40.830", "2015-12-14 21:04:40.830"
        );
        UBUserSMSLog ubUserSMSLog2 = new UBUserSMSLog(
                2, "SMS_ForgetPassword", "18621811839", "1954", 1, "短信发送成功", "2015-12-14 21:06:52.810", "2015-12-14 21:06:52.810"
        );
        UBUserSMSLog ubUserSMSLog3 = new UBUserSMSLog(
                3, "SMS_Register", "15196612442", "3017", 1, "短信发送成功", "2015-12-15 01:25:24.270", "2015-12-15 01:25:24.270"
        );
        Map<Integer,UBUserSMSLog> ubUserSMSLogMap = new HashMap<Integer,UBUserSMSLog>();
        ubUserSMSLogMap.put(1, ubUserSMSLog1);
        ubUserSMSLogMap.put(2, ubUserSMSLog2);
        ubUserSMSLogMap.put(3, ubUserSMSLog3);

        Integer j = 0;
        for (int i = 1; i <= 3; i++) {
            ObjectMapper objectMapper = new ObjectMapper();
            String value = objectMapper.writeValueAsString(ubUserSMSLogMap.get(i));
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, value);
            procuder.send(msg);
            System.out.println(j++);
            if(i == 3) { i = 1;}
        }

    }
}
