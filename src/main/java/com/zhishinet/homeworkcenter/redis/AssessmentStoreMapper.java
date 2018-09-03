package com.zhishinet.homeworkcenter.redis;

import com.zhishinet.homeworkcenter.Field;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/8/30 1:32
 */
public class AssessmentStoreMapper implements RedisStoreMapper {

    private static Logger logger = LoggerFactory.getLogger(AssessmentStoreMapper.class);

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.STRING, "test");
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getStringByField("key");
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getStringByField("value");
    }
}