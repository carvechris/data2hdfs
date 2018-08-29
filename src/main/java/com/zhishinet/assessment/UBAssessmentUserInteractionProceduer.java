package com.zhishinet.assessment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class UBAssessmentUserInteractionProceduer {

    public static void main(String[] args) throws InterruptedException, JsonProcessingException {
        final List<Assessment> assessments = Arrays.asList(
                new Assessment(1,1,1,78, Arrays.asList(
                        new Question(1,1,1),
                        new Question(1,1,1)
                )),
                new Assessment(1,1,1,78, Arrays.asList(
                        new Question(1,1,1),
                        new Question(1,1,1)
                ))
        );

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者发送消息
        String topic = "Assessment";
        Producer<String, String> procuder = new KafkaProducer<String,String>(props);
        for(Assessment assessment : assessments) {
            ObjectMapper objectMapper = new ObjectMapper();
            String value = objectMapper.writeValueAsString(assessment);
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, value);
            procuder.send(msg);
        }
        procuder.close();

    }
}
