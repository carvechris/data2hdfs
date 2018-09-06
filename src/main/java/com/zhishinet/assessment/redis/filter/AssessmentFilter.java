package com.zhishinet.assessment.redis.filter;

import com.zhishinet.homeworkcenter.Field;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssessmentFilter extends BaseFilter {

    public static final Logger logger = LoggerFactory.getLogger(AssessmentFilter.class);
    @Override
    public boolean isKeep(TridentTuple tuple) {
        boolean isKeep = !tuple.getBooleanByField(Field.ASSESSMENT_EXISTS);
        return isKeep;
    }
}
