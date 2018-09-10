package dengzhilong;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 * created by 邓志龙
 * zhilong.deng@hand-china.com
 * 2018/4/11
 */
public class MykafkaSpout{
    /**
     * @param args
     * @throws AuthorizationException
     */
    public static void main(String[] args) throws AuthorizationException {
        // TODO Auto-generated method stub
        CustomConfig customConfig=new CustomConfig();
        SpoutConfig spoutConfig=customConfig.getSpoutConfig();
        //创建  storm自带的kafkaspout
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        TopologyBuilder builder = new TopologyBuilder() ;
        builder.setSpout("spout", kafkaSpout ,1) ;
        //builder.setBolt("bolt", new MyKafkaBolt(), 48).shuffleGrouping("spout") ;
        //builder.setBolt("bolt", new MyKafkaBoltWithNoMetric(), 48).shuffleGrouping("spout") ;
        //builder.setBolt("bolt", new MyKafkaBoltWithHvr("xla_ae_lines","xla"), 48).shuffleGrouping("spout") ;
        //builder.setBolt("bolt", new MyKafkaBoltWithNoMetric2(), 48).shuffleGrouping("spout") ;
        //builder.setBolt("bolt", new MyKafkaBoltWithNoMetricAndQueue(), 48).shuffleGrouping("spout") ;
        //builder.setBolt("bolt", new MyKafkaBoltWithRow(), 48).shuffleGrouping("spout") ;
        builder.setBolt("bolt", new MyKafkaBoltWithRow(), 1).shuffleGrouping("spout") ;
        Config conf = customConfig.getConf();
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 100000);
        //conf.setMessageTimeoutSecs(900);
        conf.setNumAckers(1);
        if (args.length > 0) {
            try {
                conf.setNumWorkers(1);
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (org.apache.storm.generated.AuthorizationException e) {
                e.printStackTrace();
            }
        }else {
            //设置任务最大进程数
            conf.setNumWorkers(6*8);
            conf.setMaxTaskParallelism(16);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("mytopology", conf, builder.createTopology());
        }

    }

}
