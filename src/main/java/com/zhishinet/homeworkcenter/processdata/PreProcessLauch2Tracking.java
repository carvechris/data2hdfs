package com.zhishinet.homeworkcenter.processdata;

import com.zhishinet.homeworkcenter.Field;
import com.zhishinet.homeworkcenter.HomeworkCenterTopology1;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreProcessLauch2Tracking extends BaseFunction {

    private static Logger logger = LoggerFactory.getLogger(PreProcessLauch2Tracking.class);
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String json = tuple.getString(0);
        Document launch = null;
        try {
            launch = Document.parse(json);
        }catch (Exception ex) {
            logger.error("Parse Document exception", ex);
        }
        final Integer sessionUserTrackingId = launch.getInteger(Field.SESSIONUSERTRACKINGID);
        final Integer subjectId = launch.getInteger(Field.SUBJECT_ID);
        final Integer assessmentId = launch.getInteger(Field.ASSESSMENTID);
        final Integer sessionId = launch.getInteger(Field.SESSIONID);
        final Double score = launch.getDouble(Field.SCORE);
        final Integer userId = launch.getInteger(Field.USERID);
        collector.emit(new Values(sessionUserTrackingId,subjectId,assessmentId, sessionId, score,userId));
    }
}
