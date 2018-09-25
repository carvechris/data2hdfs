package com.zhishinet.assessment.stateFactory;

import com.zhishinet.assessment.state.CustomMongoState;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;


/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/9/20 14:39
 */
public class CustomMongoStateFactory implements StateFactory {
    private CustomMongoState.Options options;

    public CustomMongoStateFactory(CustomMongoState.Options options) {
        this.options = options;
    }

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        CustomMongoState state = new CustomMongoState(conf, this.options);
        state.prepare();
        return state;
    }
}
