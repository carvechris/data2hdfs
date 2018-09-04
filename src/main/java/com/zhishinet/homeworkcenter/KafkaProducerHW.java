package com.zhishinet.homeworkcenter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.zhishinet.mongo.MongoHelper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;

import java.util.Properties;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/8/29 12:09
 */
public class KafkaProducerHW {
    public static void main(String[] args) throws InterruptedException, JsonProcessingException {
        Properties props = new Properties();
        props.put("bootstrap.servers", Conf.KAFKA_BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> procuder = new KafkaProducer<String, String>(props);

        //从collection中获取数据, 并发事件
        FindIterable<Document> findIterable = MongoHelper.MongoStart().getCollection("stormtest").find();
        MongoCursor<Document> mongoCursor = findIterable.iterator();
        while (mongoCursor.hasNext()) {
            String value = mongoCursor.next().toJson();
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(Conf.TOPIC_HOMEWORKCENTER, value);
            procuder.send(msg);
        }
        procuder.close();
    }
}
