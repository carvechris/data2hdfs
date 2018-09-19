package com.zhishinet.assessment.baseFunction;

import com.zhishinet.assessment.Field;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/9/19 16:20
 */
public class CustomPersistFunction extends BaseFunction{
    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        final Integer sessionUserTrackingId = tridentTuple.getIntegerByField(Field.SESSIONUSERTRACKINGID);
        final Integer subjectId = tridentTuple.getIntegerByField(Field.SUBJECT_ID);
        final Integer assessmentId = tridentTuple.getIntegerByField(Field.ASSESSMENTID);
        final Integer sessionId = tridentTuple.getIntegerByField(Field.SESSIONID);
        final Double score = tridentTuple.getDoubleByField(Field.SCORE);
        final Integer userId = tridentTuple.getIntegerByField(Field.USERID);



        tridentCollector.emit(new Values(sessionUserTrackingId, subjectId, assessmentId, sessionId, score, userId));
    }

}
