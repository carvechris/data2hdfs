package com.zhishinet.assessment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class UBAssessmentUserInteractionProceduer {

    public static void main(String[] args) throws InterruptedException, JsonProcessingException {
        final List<Assessment> assessments = Arrays.asList(
                new Assessment(1,1,1,78, Arrays.asList(
                        new Question(131077,1,3),
                        new Question(131078,1,1)
                )),
                new Assessment(2,1,1,89, Arrays.asList(
                        new Question(131077,1,2),
                        new Question(131078,1,1)
                )),
                new Assessment(3,1,1,100, Arrays.asList(
                        new Question(131077,1,1),
                        new Question(131078,1,1)
                )),
                new Assessment(4,3,1,78, Arrays.asList(
                        new Question(131177,1,1),
                        new Question(131178,1,1)
                )),
                new Assessment(5,1,1,100, Arrays.asList(
                        new Question(131077,1,1),
                        new Question(131078,1,1)
                )),
                new Assessment(10,2,1,0, Arrays.asList(
                        new Question(231077,2,1),
                        new Question(231078,2,1)
                )),
                new Assessment(33,4,2,20, Arrays.asList(
                        new Question(524011,2,2),
                        new Question(524012,3,1),
                        new Question(524013,1,1),
                        new Question(524014,3,1),
                        new Question(524015,3,1),
                        new Question(524016,3,1),
                        new Question(524017,2,2),
                        new Question(524018,1,1),
                        new Question(524019,1,1),
                        new Question(524020,3,1)
                )),
                new Assessment(34,4,2,30, Arrays.asList(
                        new Question(524011,2,1),
                        new Question(524012,3,1),
                        new Question(524013,1,1),
                        new Question(524014,3,1),
                        new Question(524015,3,1),
                        new Question(524016,3,1),
                        new Question(524017,2,1),
                        new Question(524018,1,1),
                        new Question(524019,1,1),
                        new Question(524020,3,1)
                )),
                new Assessment(22,4,2,80, Arrays.asList(
                        new Question(524011,2,1),
                        new Question(524012,3,1),
                        new Question(524013,1,1),
                        new Question(524014,3,3),
                        new Question(524015,3,3),
                        new Question(524016,3,3),
                        new Question(524017,2,2),
                        new Question(524018,1,1),
                        new Question(524019,1,1),
                        new Question(524020,3,3)
                )),
                new Assessment(93,4,2,70, Arrays.asList(
                        new Question(524011,2,1),
                        new Question(524012,3,3),
                        new Question(524013,1,1),
                        new Question(524014,3,3),
                        new Question(524015,3,3),
                        new Question(524016,3,1),
                        new Question(524017,2,1),
                        new Question(524018,1,1),
                        new Question(524019,1,1),
                        new Question(524020,3,3)
                )),
                new Assessment(23,4,2,100, Arrays.asList(
                        new Question(524011,2,2),
                        new Question(524012,3,3),
                        new Question(524013,1,1),
                        new Question(524014,3,3),
                        new Question(524015,3,3),
                        new Question(524016,3,3),
                        new Question(524017,2,2),
                        new Question(524018,1,1),
                        new Question(524019,1,1),
                        new Question(524020,3,3)
                )),
                new Assessment(64,4,2,80, Arrays.asList(
                        new Question(524011,2,2),
                        new Question(524012,3,3),
                        new Question(524013,1,3),
                        new Question(524014,3,3),
                        new Question(524015,3,3),
                        new Question(524016,3,3),
                        new Question(524017,2,2),
                        new Question(524018,1,1),
                        new Question(524019,1,2),
                        new Question(524020,3,3)
                )),
                new Assessment(39,4,2,30, Arrays.asList(
                        new Question(524011,2,2),
                        new Question(524012,3,2),
                        new Question(524013,1,2),
                        new Question(524014,3,2),
                        new Question(524015,3,2),
                        new Question(524016,3,2),
                        new Question(524017,2,2),
                        new Question(524018,1,2),
                        new Question(524019,1,2),
                        new Question(524020,3,3)
                )),
                new Assessment(67,4,2,40, Arrays.asList(
                        new Question(524011,2,2),
                        new Question(524012,3,2),
                        new Question(524013,1,1),
                        new Question(524014,3,2),
                        new Question(524015,3,4),
                        new Question(524016,3,3),
                        new Question(524017,2,1),
                        new Question(524018,1,1),
                        new Question(524019,1,3),
                        new Question(524020,3,2)
                )),
                new Assessment(38,4,2,20, Arrays.asList(
                        new Question(524011,2,1),
                        new Question(524012,3,1),
                        new Question(524013,1,1),
                        new Question(524014,3,1),
                        new Question(524015,3,1),
                        new Question(524016,3,1),
                        new Question(524017,2,1),
                        new Question(524018,1,1),
                        new Question(524019,1,3),
                        new Question(524020,3,1)
                )),
                new Assessment(86,4,2,20, Arrays.asList(
                        new Question(524011,2,2),
                        new Question(524012,3,2),
                        new Question(524013,1,2),
                        new Question(524014,3,2),
                        new Question(524015,3,2),
                        new Question(524016,3,2),
                        new Question(524017,2,2),
                        new Question(524018,1,2),
                        new Question(524019,1,2),
                        new Question(524020,3,2)
                )),
                new Assessment(75,4,2,40, Arrays.asList(
                        new Question(524011,2,3),
                        new Question(524012,3,3),
                        new Question(524013,1,3),
                        new Question(524014,3,1),
                        new Question(524015,3,3),
                        new Question(524016,3,3),
                        new Question(524017,2,3),
                        new Question(524018,1,3),
                        new Question(524019,1,3),
                        new Question(524020,3,3)
                )),
                new Assessment(91,4,2,0, Arrays.asList(
                        new Question(524011,2,4),
                        new Question(524012,3,4),
                        new Question(524013,1,4),
                        new Question(524014,3,4),
                        new Question(524015,3,4),
                        new Question(524016,3,4),
                        new Question(524017,2,4),
                        new Question(524018,1,4),
                        new Question(524019,1,4),
                        new Question(524020,3,4)
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
