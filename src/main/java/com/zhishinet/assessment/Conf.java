package com.zhishinet.assessment;

public class Conf {

    public final static String HOST = "localhost";

    public final static String ZOOKEEPER_LIST = "localhost:2181";
    public final static String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";


    private final static String HDFS_SCHEMA = "hdfs://";
    private final static String HDFS_HOST = HOST;
    private final static int HDFS_PORT= 8020;
    public final static String HDFS_URL =  HDFS_SCHEMA + HDFS_HOST + ":" + HDFS_PORT;


    public final static String REDIS_HOST = HOST;
    public final static int REDIS_PORT = 6379;
    public final static String TOPIC_HOMEWORKCENTER = "HomeworkCenter";
}
