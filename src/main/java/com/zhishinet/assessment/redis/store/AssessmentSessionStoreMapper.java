package com.zhishinet.assessment.redis.store;

import com.zhishinet.homeworkcenter.Field;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/9/14 11:01
 */
public class AssessmentSessionStoreMapper implements RedisStoreMapper {
    public static final String REDIS_KEY_PREFIX = "com:zhishinet:assessment:";
    private static Logger logger = LoggerFactory.getLogger(AssessmentSessionStoreMapper.class);

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.STRING, "Assessment");
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return REDIS_KEY_PREFIX + Field.ASSESSMENTID + "_" + tuple.getIntegerByField(Field.ASSESSMENTID) + ":" + Field.SESSIONID + "_" + tuple.getIntegerByField(Field.SESSIONID);
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getDoubleByField(Field.SUM) + "_" + tuple.getLongByField(Field.COUNT);
    }
}
