package com.zhishinet.homeworkcenter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.bson.Document;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/8/29 13:10
 */
public class HomeworkCenterTopology {
    //用于算平均分的聚合类
    public static class AvgState{
        float count = 0;
        float total = 0;

        double getAverage(){
            return total/count;
        }
    }

    //数据预处理
    public static class PreProcessData extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String json = tuple.getString(0);
            Document launch = Document.parse(json);
            collector.emit(new Values(launch.get("AssessmentId"), launch.get("SessionId"), launch.get("UserId"), launch.get("Score")));

        }
    }

    public static class Average implements CombinerAggregator<Number> {

        int count = 0;
        double sum = 0;

        @Override
        public Double init(final TridentTuple tuple) {
            this.count++;
            if (!(tuple.getValue(0) instanceof Double)) {

                double d = ((Number) tuple.getValue(0)).doubleValue();

                this.sum += d;

                return d;
            }

            this.sum += (Double) tuple.getValue(0);
            return (Double) tuple.getValue(0);

        }

        @Override
        public Double combine(final Number val1, final Number val2) {
            return this.sum / this.count;

        }

        @Override
        public Double zero() {
            this.sum = 0;
            this.count = 0;
            return 0D;
        }
    }


    public static void main(String[] args) {

        BrokerHosts boBrokerHosts = new ZkHosts("localhost:2181");
        String topic = "HomewrokCenter";
        String spoutId = "HomewrokCenter_storm";
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(boBrokerHosts, topic, spoutId);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());


        //定义策略, Trident方式准备持久化到HDFS
        Fields hdfsFields = new Fields("AssessmentId", "SessionId","Avg");
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
        topology.newStream("KafkaSpout",new TransactionalTridentKafkaSpout(kafkaConfig))
                .each(new Fields("str"), new PreProcessData(), new Fields("AssessmentId", "SessionId", "UserId", "Score"))
                .groupBy(new Fields("AssessmentId", "SessionId"))
                .aggregate(new Fields("Score"), new Average(), new Fields("Avg"))
                .partitionPersist(factory, hdfsFields, new HdfsUpdater(), new Fields());

        //运行
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TridentTopology",new Config(),topology.build());

    }
}
