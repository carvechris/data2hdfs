package dengzhilong;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * created by 邓志龙
 * zhilong.deng@hand-china.com
 * 2018/4/11
 */
public class MyKafkaBoltWithRow extends BaseRichBolt{

    private OutputCollector collector;

    private SimpleDateFormat sdf;




    private Logger logger=LoggerFactory.getLogger(MyKafkaBoltWithRow.class);

    public MyKafkaBoltWithRow() {
        sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
    }


    @Override
    public void execute(Tuple tuple) {
        System.out.println("======="+tuple.toString());
        this.collector.ack(tuple);
    }

}
