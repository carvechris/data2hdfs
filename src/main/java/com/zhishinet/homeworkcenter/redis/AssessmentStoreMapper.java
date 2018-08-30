package com.zhishinet.homeworkcenter.redis;

import com.zhishinet.homeworkcenter.Field;
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
        tuple.getValues().stream().forEach(o -> System.out.println(o.toString()));
        final Integer assessmentId = tuple.getIntegerByField(Field.FIELD_ASSESSMENTID);
        final Integer sessionId = tuple.getIntegerByField(Field.FIELD_SESSIONID);
        System.out.println(String.format("getKeyFromTuple AssessmentId : %d, SessionId : %d", assessmentId, sessionId));
        return REDIS_PREFIX + assessmentId+":"+sessionId;
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        final Double sum = tuple.getDoubleByField(Field.FIELD_SUM);
        final Integer count = tuple.getIntegerByField(Field.FIELD_COUNT);
        System.out.println(String.format("getValueFromTuple Sum : %f, Count : %d", sum, count));
        return ""+sum +":"+ count;
    }
}