package com.zhishinet.assessment.baseQueryFunction;

import com.zhishinet.assessment.Field;
import org.apache.storm.redis.trident.state.RedisState;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/9/14 10:55
 */
public class RedisMapGet extends BaseQueryFunction<RedisState, String> {
    public static final String REDIS_KEY_PREFIX = "com:zhishinet:assessment:";

    private static final Logger logger = LoggerFactory.getLogger(RedisMapGet.class);


    @Override
    public List<String> batchRetrieve(RedisState redisState, List<TridentTuple> list) {
        List<String> result = new ArrayList<>();

        String[] keys = new String[list.size()];
        for(int i = 0 ;i <list.size();i++){
            keys[i] = REDIS_KEY_PREFIX + Field.ASSESSMENTID + "_" + list.get(i).getStringByField(Field.ASSESSMENTID) + ":" + Field.SESSIONID + "_" + list.get(i).getStringByField(Field.SESSIONID) ;
            logger.info("===============Current key is {}", keys[i]);
        }

        result = redisState.getJedis().mget(keys);

        if(result == null || result.size() == 0){
            logger.info("===============result is empty");
        }else{
            result.forEach(r ->{
                logger.info("============ every single result is {}",r );
            });
        }

        return  result;
    }

    @Override
    public void execute(TridentTuple tridentTuple, String o, TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(o));
    }
}
