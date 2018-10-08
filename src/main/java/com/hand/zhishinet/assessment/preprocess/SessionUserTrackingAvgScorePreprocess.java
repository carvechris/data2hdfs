package com.hand.zhishinet.assessment.preprocess;

import com.hand.zhishinet.assessment.Field;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/10/8 15:13
 */
public class SessionUserTrackingAvgScorePreprocess extends BaseFunction {
    private static Logger logger = LoggerFactory.getLogger(SessionUserTrackingAvgScorePreprocess.class);
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

        if(Objects.isNull(sessionUserTrackingId)) { logger.error("DataSource From Spout sessionUserTrackingId is null or empty"); return; }
        if(Objects.isNull(subjectId)){ logger.error("DataSource From Spout sessionUserTrackingId is null or empty"); return; }
        if(Objects.isNull(assessmentId)) { logger.error("DataSource From Spout assessmentId is null or empty"); return; }
        if(Objects.isNull(sessionId)) { logger.error("DataSource From Spout sessionId is null or empty"); return; }
        if(Objects.isNull(score)) { logger.error("DataSource From Spout score is null or empty"); return; }
        if(Objects.isNull(userId)) { logger.error("DataSource From Spout userId is null or empty"); return; }

        collector.emit(new Values(sessionUserTrackingId, subjectId, assessmentId, sessionId, score, userId));

    }
}
