package com.zhishinet.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/8/29 12:06
 */
public class MongoHelper {
    private static final String DBName = "homeworkcenter";
    private static MongoClient mongoClient ;
    public MongoHelper(){
    }

    //打开数据库连接
    public static MongoDatabase MongoStart(){
        MongoClient client = null;

        ServerAddress serverAddress = new ServerAddress("10.213.0.42",37017);

        List<ServerAddress> seeds = new ArrayList<ServerAddress>();

        seeds.add(serverAddress);

        MongoCredential credentials = MongoCredential.createCredential("dev", "dev", "dev".toCharArray());

        List<MongoCredential> credentialsList = new ArrayList<MongoCredential>();

        credentialsList.add(credentials);

        client = new MongoClient(seeds, credentialsList);

        MongoDatabase db = client.getDatabase("dev");

        return db;
    }
    //关闭连接
    public static void close() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }

}
