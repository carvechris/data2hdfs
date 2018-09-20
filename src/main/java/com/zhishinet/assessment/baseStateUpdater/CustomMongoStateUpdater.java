package com.zhishinet.assessment.baseStateUpdater;

import com.zhishinet.assessment.state.CustomMongoState;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/9/20 14:37
 */
public class CustomMongoStateUpdater extends BaseStateUpdater<CustomMongoState> {
    public CustomMongoStateUpdater() {
    }

    @Override
    public void updateState(CustomMongoState state, List<TridentTuple> tuples, TridentCollector collector) {
        state.updateState(tuples, collector);
    }
}
