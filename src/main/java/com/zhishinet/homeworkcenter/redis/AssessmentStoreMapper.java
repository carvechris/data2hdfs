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
    private static final String REDIS_PREFIX = "strom:trident:";
    private static Logger logger = LoggerFactory.getLogger(AssessmentStoreMapper.class);

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.STRING, "test");
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        final Integer assessmentId = tuple.getIntegerByField(Field.FIELD_ASSESSMENTID);
        final Integer sessionId = tuple.getIntegerByField(Field.FIELD_SESSIONID);
        logger.info("PUT Data to Redis Key {}{}:{}", REDIS_PREFIX, assessmentId, sessionId);
        return REDIS_PREFIX + assessmentId+":"+sessionId;
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        final Double sum = tuple.getDoubleByField(Field.FIELD_SUM);
        final Integer count = tuple.getIntegerByField(Field.FIELD_COUNT);
        logger.info("PUT Data to Redis Vaule {}:{}", sum, count);
        return ""+sum +":"+ count;
    }
}