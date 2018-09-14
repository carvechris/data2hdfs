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

public class AssessmentSessionTotalScoreTotalCountLookupMapper implements RedisLookupMapper {

    private static Logger logger = LoggerFactory.getLogger(AssessmentSessionTotalScoreTotalCountLookupMapper.class);
    public static final String REDIS_KEY_PREFIX = "com:zhishinet:assessment:";

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.STRING, "Assessment");
    }

    @Override
    public List<Values> toTuple(ITuple iTuple, Object o) {
        String [] objects = o.toString().split("_");
        List<Values> values = Lists.newArrayList();
        values.add(new Values(Double.valueOf(objects[0]),Integer.valueOf(objects[1])));
        return values;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Field.SUM, Field.COUNT));
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return REDIS_KEY_PREFIX + Field.ASSESSMENTID + "_" + tuple.getIntegerByField(Field.ASSESSMENTID) + ":" + Field.SESSIONID + "_" + tuple.getIntegerByField(Field.SESSIONID);
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return null;
    }
}


