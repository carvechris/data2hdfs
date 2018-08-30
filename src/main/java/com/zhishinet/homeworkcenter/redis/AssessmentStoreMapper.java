package com.zhishinet.homeworkcenter.redis;

import org.apache.storm.tuple.ITuple;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/8/30 1:32
 */
public class AssessmentStoreMapper implements RedisStoreMapper {
    private static final String REDIS_PREFIX = "strom:trident:";

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.STRING, "test");
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return REDIS_PREFIX + tuple.getInteger(0)+":"+tuple.getInteger(1);
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getString(2) +":"+ tuple.getString(3);
    }
}