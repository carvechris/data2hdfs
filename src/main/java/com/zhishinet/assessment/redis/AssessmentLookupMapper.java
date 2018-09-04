package com.zhishinet.assessment.redis;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zhishinet.homeworkcenter.Field;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AssessmentLookupMapper implements RedisLookupMapper {

    private static Logger logger = LoggerFactory.getLogger(AssessmentLookupMapper.class);

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, "Assessment");
    }

    @Override
    public List<Values> toTuple(ITuple iTuple, Object o) {
        List<Values> values = Lists.newArrayList();
        values.add(new Values(Double.valueOf(o.toString())));
        return values;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Field.TOTAL_SCORE));
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return Field.ASSESSMENTID + "_" + tuple.getIntegerByField(Field.ASSESSMENTID) + "_" + Field.SESSIONID + "_" + tuple.getIntegerByField(Field.SESSIONID) + "_" + Field.USERID + "_" + tuple.getIntegerByField(Field.USERID);
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return null;
    }
}


