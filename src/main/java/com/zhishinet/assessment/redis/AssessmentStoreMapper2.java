package com.zhishinet.assessment.redis;

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
        return Field.ASSESSMENTID + "_" + tuple.getIntegerByField(Field.ASSESSMENTID) + "_" + Field.SESSIONID + "_" + tuple.getIntegerByField(Field.SESSIONID);
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getDoubleByField(Field.SUM) + "_" + tuple.getIntegerByField(Field.COUNT) + "_" + tuple.getDoubleByField(Field.SUM) / tuple.getIntegerByField(Field.COUNT);
    }
}