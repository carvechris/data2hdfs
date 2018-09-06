package com.zhishinet.assessment.redis.store;

import com.zhishinet.homeworkcenter.Field;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssessmentSessionUserStoreMapper implements RedisStoreMapper {

    private static Logger logger = LoggerFactory.getLogger(AssessmentSessionUserStoreMapper.class);

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, "Assessment");
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return Field.ASSESSMENTID + "_" + tuple.getIntegerByField(Field.ASSESSMENTID) + "_" + Field.SESSIONID + "_" + tuple.getIntegerByField(Field.SESSIONID) + "_" + Field.USERID + "_" + tuple.getIntegerByField(Field.USERID);
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getDoubleByField(Field.SCORE) + "";
    }
}