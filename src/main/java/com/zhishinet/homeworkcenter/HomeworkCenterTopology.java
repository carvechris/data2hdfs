package com.zhishinet.homeworkcenter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.trident.sync.CountSyncPolicy;
import org.apache.storm.hdfs.trident.sync.SyncPolicy;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.topology.TridentTopologyBuilder;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import java.io.IOException;
import java.util.Map;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/8/29 13:10
 */
public class HomeworkCenterTopology {
    public static class HomeworkCenterBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            final String json = tuple.getString(0);
            Document log = null;
            log = Document.parse(json);
            this.collector.emit(new Values(log.get("Score")));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id","key","mobilePhoneNo","code","state","returnMsg","postTime","createdOn"));
        }
    }

    public static class AvgState{
        long count = 0;
        long total = 0;

        double getAverage(){
            return total/count;
        }
    }

    public static class AvgAgg extends BaseAggregator<AvgState>{

        @Override
        public AvgState init(Object batchId, TridentCollector collector) {
            return new AvgState();
        }

        @Override
        public void aggregate(AvgState state, TridentTuple tuple, TridentCollector collector) {
            state.count++;
            state.total++;
        }

        @Override
        public void complete(AvgState state, TridentCollector collector) {
            collector.emit(new Values(state.getAverage()));
        }
    }


    public static void main(String[] args) {

        BrokerHosts boBrokerHosts = new ZkHosts("localhost:2181");
        String topic = "HomewrokCenter";
        String zkRoot = "";
        String spoutId = "HomewrokCenter_storm";
        SpoutConfig spoutConfig = new SpoutConfig(boBrokerHosts, topic, zkRoot, spoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());


        //定义策略, Trident方式准备持久化到HDFS
        Fields hdfsFields = new Fields("AssessmentId", "SessionId", "AverageScore");
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPrefix("trident")
                .withExtension(".txt")
                .withPath("/trident");
        RecordFormat recordFormat = new DelimitedRecordFormat()
                .withFields(hdfsFields).withFieldDelimiter("\001");
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.MB);
        HdfsState.Options options = new HdfsState.HdfsFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(rotationPolicy)
                .withFsUrl("hdfs://localhost:9000");
        StateFactory factory = new HdfsStateFactory().withOptions(options);



        //构建TridentTopology, 流式API将数据处理为想要的形式
        TridentTopology topology = new TridentTopology();
        topology.newStream("KafkaSpout",new KafkaSpout(spoutConfig))
                .aggregate(new Fields("Score"), new AvgAgg(), new Fields("Avg"))
                .partitionPersist(factory, hdfsFields, new HdfsUpdater(), new Fields());





    }
}
