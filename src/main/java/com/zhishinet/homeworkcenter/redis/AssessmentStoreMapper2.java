package com.zhishinet.homeworkcenter.redis;

import com.zhishinet.homeworkcenter.Field;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssessmentStoreMapper2 implements RedisStoreMapper {

    private static Logger logger = LoggerFactory.getLogger(AssessmentStoreMapper2.class);

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.STRING, "Assessment_avg");
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return Field.FIELD_ASSESSMENTID + "_" + tuple.getIntegerByField(Field.FIELD_ASSESSMENTID) + "_" + Field.FIELD_SESSIONID + "_" + tuple.getIntegerByField(Field.FIELD_SESSIONID);
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getDoubleByField(Field.FIELD_SUM) + "_" + tuple.getIntegerByField(Field.FIELD_COUNT) + "_" + tuple.getDoubleByField(Field.FIELD_SUM) / tuple.getIntegerByField(Field.FIELD_COUNT);
    }
}