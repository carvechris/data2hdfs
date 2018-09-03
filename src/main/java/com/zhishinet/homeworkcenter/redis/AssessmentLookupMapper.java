package com.zhishinet.homeworkcenter.redis;

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

    public class SumAndCount implements java.io.Serializable {
        private Double sum;
        private Integer count;

        public Double getSum() {
            return sum;
        }

        public void setSum(Double sum) {
            this.sum = sum;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "SumAndCount{" +
                    "sum=" + sum +
                    ", count=" + count +
                    '}';
        }
    }


    private static Map<String,SumAndCount> maps = Maps.newHashMap();

    @Override
    public List<Values> toTuple(ITuple tuple, Object value) {
        List<Values> values = new ArrayList<Values>();
        final Integer assessmentId = tuple.getIntegerByField(Field.ASSESSMENTID);
        final Integer sessionId = tuple.getIntegerByField(Field.SESSIONID);
        SumAndCount sumAndCount = maps.get(assessmentId + sessionId + "");
        if (sumAndCount != null) {
            sumAndCount.setSum(sumAndCount.getSum() + Double.parseDouble(value.toString()));
            sumAndCount.setCount(sumAndCount.getCount() + 1);
            maps.put(assessmentId + sessionId + "", sumAndCount);
        } else {
            sumAndCount = new SumAndCount();
            sumAndCount.setSum(Double.parseDouble(value.toString()));
            sumAndCount.setCount(1);
            maps.put(assessmentId + sessionId + "", sumAndCount);
        }
        values.add(new Values(sumAndCount.getSum(), sumAndCount.getCount()));
        return values;
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, "Assessment");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Field.SUM, Field.COUNT));
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


