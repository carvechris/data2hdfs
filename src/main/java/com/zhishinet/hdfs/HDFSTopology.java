package com.zhishinet.hdfs;

import com.hand.zhishinet.MyConfig;
import com.zhishinet.storm.ZhishinetBoltFileNameFormat;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class HDFSTopology  {

    public static final String TOPOLOGY_NAME = "HDFSTopology";
    public static class MySpout extends BaseRichSpout {

        private SpoutOutputCollector spoutOutputCollector;

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector = spoutOutputCollector;
        }

        int i = 0;
        @Override
        public void nextTuple() {
            this.spoutOutputCollector.emit(new Values(++i,"name"+i,"测试"+i));
            Utils.sleep(200);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id","name","type"));
        }

    }

    public static class MyBolt extends BaseRichBolt {

        private OutputCollector outputCollector;
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            Integer id = tuple.getIntegerByField("id");
            String name = tuple.getStringByField("name");
            String type = tuple.getStringByField("type");
            System.out.println("id is: " +id+" ,name is: "+ name + " , type is: " + type);
            outputCollector.emit(new Values(id,name,type));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id","name","type"));
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("\001");

        // sync the filesystem after every 100 tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(100);

        // rotate files when they reach 1MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.MB);

        FileNameFormat fileNameFormat = new ZhishinetBoltFileNameFormat().withPath("/user/storm/hdfs/");

        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl(MyConfig.HDFS_URL)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("MySpout",new MySpout());
        builder.setBolt("MyBolt",new MyBolt()).shuffleGrouping("MySpout");
        builder.setBolt("HdfsBolt",bolt).shuffleGrouping("MyBolt");
        StormTopology topology = builder.createTopology();

        if(null != args && args.length > 0) {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, new Config(), builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME,new Config(),builder.createTopology());
        }
    }
}
