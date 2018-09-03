package com.zhishinet.homeworkcenter.redis;

import com.zhishinet.homeworkcenter.Field;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssessmentStoreMapper1 implements RedisStoreMapper {

    private static Logger logger = LoggerFactory.getLogger(AssessmentStoreMapper1.class);

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, "Assessment");
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return Field.FIELD_ASSESSMENTID + "_" + tuple.getIntegerByField(Field.FIELD_ASSESSMENTID) + "_" + Field.FIELD_SESSIONID + "_" + tuple.getIntegerByField(Field.FIELD_SESSIONID) + "_" + Field.FIELD_USERID + "_" + tuple.getIntegerByField(Field.FIELD_USERID);
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getDoubleByField(Field.FIELD_SCORE) + "";
    }
}