package com.zhishinet.assessment.baseFunction;


import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/9/14 10:38
 */
public class CustomSplit extends BaseFunction {

    private final static Logger logger = LoggerFactory.getLogger(CustomSplit.class);

    public CustomSplit(){

    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String[] var3 = tridentTuple.getString(0).split(" ");
        int var4 = var3.length;

        String assessmentId= "";
        String sessionId = "";

        for(int var5 = 0; var5 < var4; ++var5) {
            String word = var3[var5];
            assessmentId = word.split(",")[0];
            sessionId = word.split(",")[1];
            logger.info("===================== emit tuples =========== assessmentid {}, sessionId {}", assessmentId, sessionId);
            if(word.length() > 0) {
                tridentCollector.emit(new Values(assessmentId,sessionId));
            }
        }
    }
}
