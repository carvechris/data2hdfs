package com.zhishinet.example;

import org.apache.derby.impl.io.VFMemoryStorageFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {

    public static class Field {
        public static final String ASSESSMENTID = "AssessmentId";
        public static final String SESSIONID = "SessionId";
        public static final String SCORE = "Score";
        public static final String TOTAL_SCORE = "TotalScore";
        public static final String USERID = "UserId";
        public static final String SUM = "Sum";
        public static final String COUNT = "Count";
    }

    public static class PrintFunction extends BaseFunction {
        private static final Logger LOG = LoggerFactory.getLogger(PrintFunction.class);
        @Override
        public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
            LOG.info("{} : {}", tuple.getFields().toString(),tuple.toString());
        }
    }

    public static StormTopology buildTopology(){
        Fields fields = new Fields(Field.ASSESSMENTID,Field.SESSIONID, Field.USERID, Field.SCORE);
        FixedBatchSpout spout = new FixedBatchSpout(fields, 200,
                new Values(389259, 129678, 347113, 85.63636016845703d),
                new Values(389259, 129678, 347110, 91.84091186523438d),
                new Values(389259, 129678, 347107, 92.5d),
                new Values(389259, 129678, 347122, 88.54545593261719d),
                new Values(389259, 129678, 347137, 92.20454406738281d),
                new Values(389259, 129678, 347128, 93.06818389892578d),
                new Values(389259, 129678, 347146, 91.0227279663086d),
                new Values(389259, 129678, 347134, 91.31818389892578d),
                new Values(389259, 129678, 347152, 91.81818389892578d),
                new Values(389259, 129678, 347149, 94.40908813476562d),
                new Values(389259, 129678, 347170, 92.68181610107422d),
                new Values(389259, 129678, 347158, 92.45454406738281d),
                new Values(389259, 129678, 347179, 94.0d),
                new Values(389259, 129678, 347176, 93.79545593261719d),
                new Values(389259, 129678, 347116, 92.68181610107422d),
                new Values(389259, 129678, 347188, 90.86363983154297d),
                new Values(389259, 129678, 347104, 90.81818389892578d),
                new Values(389259, 129678, 347101, 93.29545593261719d),
                new Values(389259, 129678, 347131, 91.86363983154297d),
                new Values(389259, 129678, 347143, 93.11363983154297d),
                new Values(389259, 129678, 347173, 90.7727279663086d),
                new Values(389259, 129678, 347119, 92.40908813476562d),
                new Values(389259, 129678, 347125, 90.70454406738281d),
                new Values(389259, 129678, 347161, 87.4772720336914d),
                new Values(389259, 129678, 347167, 93.15908813476562d),
                new Values(389259, 129678, 347140, 93.31818389892578d),
                new Values(389259, 129678, 347155, 89.95454406738281d),
                new Values(389259, 129678, 347164, 92.61363983154297d),
                new Values(389259, 129678, 347182, 90.95454406738281d),
                new Values(389259, 129678, 347185, 86.68181610107422d),
                new Values(389245, 129678, 347110, 92.0d),
                new Values(389245, 129678, 347119, 88.0d),
                new Values(389245, 129678, 347128, 93.0d),
                new Values(389245, 129678, 347152, 89.0d),
                new Values(389245, 129678, 347140, 91.92308044433594d),
                new Values(389245, 129678, 347155, 91.23076629638672d),
                new Values(389245, 129678, 347161, 87.0d),
                new Values(389245, 129678, 347104, 88.76923370361328d),
                new Values(389245, 129678, 347122, 87.46154022216797d),
                new Values(389245, 129678, 347170, 91.53845977783203d),
                new Values(389245, 129678, 347107, 88.07691955566406d),
                new Values(389245, 129678, 347125, 91.38461303710938d),
                new Values(389245, 129678, 347131, 90.61538696289062d),
                new Values(389245, 129678, 347149, 92.38461303710938d),
                new Values(389245, 129678, 347182, 87.15384674072266d),
                new Values(389245, 129678, 347113, 88.53845977783203d),
                new Values(389245, 129678, 347158, 92.46154022216797d),
                new Values(389245, 129678, 347116, 90.92308044433594d),
                new Values(389245, 129678, 347134, 90.84615325927734d),
                new Values(389245, 129678, 347146, 90.53845977783203d),
                new Values(389245, 129678, 347164, 93.84615325927734d),
                new Values(389245, 129678, 347179, 89.46154022216797d),
                new Values(389245, 129678, 347101, 89.69230651855469d),
                new Values(389245, 129678, 347137, 89.46154022216797d),
                new Values(389245, 129678, 347167, 94.69230651855469d),
                new Values(389245, 129678, 347188, 90.30769348144531d),
                new Values(389245, 129678, 347143, 93.92308044433594d),
                new Values(389245, 129678, 347173, 88.61538696289062d),
                new Values(389245, 129678, 347176, 93.0d),
                new Values(389245, 129678, 347185, 90.07691955566406d),
                new Values(199927, 44776, 53819, 90.69999694824219d),
                new Values(199927, 44776, 53826, 86.0d),
                new Values(199927, 44776, 105198, 85.9000015258789d),
                new Values(199927, 44776, 105201, 85.9000015258789d),
                new Values(199927, 44776, 53827, 91.0d),
                new Values(199927, 44776, 85241, 91.30000305175781d),
                new Values(199927, 44776, 105202, 84.69999694824219d),
                new Values(199927, 44776, 53825, 84.19999694824219d),
                new Values(199927, 44776, 105199, 85.5d),
                new Values(199927, 44776, 53823, 90.30000305175781d),
                new Values(199927, 44776, 67931, 82.0999984741211d),
                new Values(199927, 44776, 105200, 82.9000015258789d),
                new Values(199927, 44776, 53818, 91.30000305175781d),
                new Values(199927, 44776, 53816, 90.69999694824219d),
                new Values(199927, 44776, 53822, 88.0999984741211d),
                new Values(199927, 44776, 53828, 86.69999694824219d),
                new Values(199927, 44776, 59117, 86.5d),
                new Values(199927, 44776, 67932, 84.5999984741211d),
                new Values(199927, 44776, 82173, 89.80000305175781d),
                new Values(199927, 44776, 92190, 88.19999694824219d),
                new Values(199927, 44776, 53820, 88.9000015258789d),
                new Values(199927, 44776, 53817, 86.30000305175781d),
                new Values(199927, 44776, 53821, 90.0d),
                new Values(199927, 44776, 53824, 88.9000015258789d),
                new Values(199927, 44776, 82172, 90.19999694824219d),
                new Values(199927, 44776, 115741, 86.5999984741211d),
                new Values(199923, 44776, 53822, 100.0d),
                new Values(199923, 44776, 115741, 53.333335876464844d),
                new Values(199923, 44776, 53817, 93.33333587646484d),
                new Values(199923, 44776, 53823, 80.0d),
                new Values(199923, 44776, 82172, 93.33333587646484d),
                new Values(199923, 44776, 105200, 26.666667938232422d),
                new Values(199923, 44776, 53821, 66.66667175292969d),
                new Values(199923, 44776, 105198, 46.66666793823242d),
                new Values(199923, 44776, 53818, 66.66667175292969d),
                new Values(199923, 44776, 85241, 66.66667175292969d),
                new Values(199923, 44776, 67931, 66.66667175292969d),
                new Values(199923, 44776, 53820, 66.66667175292969d),
                new Values(199923, 44776, 53816, 73.33333587646484d),
                new Values(199923, 44776, 105201, 60.000003814697266d),
                new Values(199923, 44776, 53827, 66.66667175292969d),
                new Values(199923, 44776, 53824, 73.33333587646484d),
                new Values(199923, 44776, 92190, 60.000003814697266d),
                new Values(199923, 44776, 53825, 40.0d),
                new Values(199923, 44776, 53826, 46.66666793823242d),
                new Values(199923, 44776, 53828, 80.0d),
                new Values(199923, 44776, 67932, 60.000003814697266d),
                new Values(199923, 44776, 82173, 46.66666793823242d),
                new Values(199923, 44776, 105199, 46.66666793823242d),
                new Values(199923, 44776, 53819, 80.0d),
                new Values(199923, 44776, 59117, 80.0d),
                new Values(199923, 44776, 105202, 66.66667175292969d),
                new Values(199917, 44776, 53816, 93.52631378173828d),
                new Values(199917, 44776, 53827, 89.89473724365234d),
                new Values(199917, 44776, 53817, 93.89473724365234d),
                new Values(199917, 44776, 53821, 91.9473648071289d),
                new Values(199917, 44776, 53824, 89.68421173095703d),
                new Values(199917, 44776, 92190, 91.2631607055664d),
                new Values(199917, 44776, 53820, 90.63157653808594d),
                new Values(199917, 44776, 82172, 84.31578826904297d),
                new Values(199917, 44776, 105199, 86.57894897460938d),
                new Values(199917, 44776, 53822, 92.57894897460938d),
                new Values(199917, 44776, 67932, 76.84210205078125d),
                new Values(199917, 44776, 105202, 82.21052551269531d),
                new Values(199917, 44776, 53823, 89.84210205078125d),
                new Values(199917, 44776, 59117, 91.78947448730469d),
                new Values(199917, 44776, 82173, 90.0526351928711d),
                new Values(199917, 44776, 115741, 76.63157653808594d),
                new Values(199917, 44776, 53826, 90.2631607055664d),
                new Values(199917, 44776, 67931, 79.89473724365234d),
                new Values(199917, 44776, 105198, 83.89473724365234d),
                new Values(199917, 44776, 53819, 92.52631378173828d),
                new Values(199917, 44776, 53825, 81.68421173095703d),
                new Values(199917, 44776, 105201, 79.57894897460938d),
                new Values(199917, 44776, 53818, 94.2631607055664d),
                new Values(199917, 44776, 53828, 87.52631378173828d),
                new Values(199917, 44776, 85241, 87.68421173095703d),
                new Values(199917, 44776, 105200, 79.89473724365234d)

        );
        spout.setCycle(false);
        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);
        stream
                .groupBy(new Fields(Field.ASSESSMENTID,Field.SESSIONID))
                .persistentAggregate(new MemoryMapState.Factory(),new Fields(Field.ASSESSMENTID,Field.SESSIONID),new Count(), new Fields(Field.COUNT))
                .newValuesStream()
                .each(new Fields(Field.ASSESSMENTID,Field.SESSIONID,Field.COUNT),new PrintFunction(),new Fields());
        stream
                .groupBy(new Fields(Field.ASSESSMENTID,Field.SESSIONID))
                .persistentAggregate(new MemoryMapState.Factory(),new Fields(Field.SCORE),new Sum(), new Fields(Field.SUM))
                .newValuesStream()
                .each(new Fields(Field.ASSESSMENTID,Field.SESSIONID,Field.SUM),new PrintFunction(),new Fields());
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test_wordCounter_for_redis",new Config(),buildTopology());
    }
}
