package com.zhishinet.assessment.redis.lookup;

import com.google.common.collect.Lists;
import com.zhishinet.homeworkcenter.Field;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * 检查这份作业是否被处理过,为后面的数据过滤做准备
 */
public class AssessmentExistsLookupMapper implements RedisLookupMapper {

    public static final String REDIS_KEY_PREFIX = "com:zhishinet:assessment:";
    private static Logger logger = LoggerFactory.getLogger(AssessmentExistsLookupMapper.class);

    @Override
    public List<Values> toTuple(ITuple tuple, Object o) {
        List<Values> values = Lists.newArrayList();
        values.add(new Values(!Objects.isNull(o)));
        return values;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.ASSESSMENT_EXISTS));
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.STRING, "Assessment");
    }


    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return REDIS_KEY_PREFIX + Field.ASSESSMENTID + "_" + tuple.getIntegerByField(Field.ASSESSMENTID) + ":" + Field.SESSIONID + "_" + tuple.getIntegerByField(Field.SESSIONID) + ":" + Field.USERID + "_" + tuple.getIntegerByField(Field.USERID);
    }

    @Override
    public String getValueFromTuple(ITuple iTuple) {
        return null;
    }
}
