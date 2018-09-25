package com.zhishinet.assessment.state;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.zhishinet.assessment.Field;
import org.apache.commons.lang.Validate;
import org.apache.storm.mongodb.common.MongoDBClient;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/9/20 14:17
 */
public class CustomMongoState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(CustomMongoState.class);
    private CustomMongoState.Options options;
    private MongoDBClient mongoClient;
    private Map map;

    public CustomMongoState(Map map, CustomMongoState.Options options) {
        this.options = options;
        this.map = map;
    }

    public void prepare() {
        Validate.notEmpty(this.options.url, "url can not be blank or null");
        Validate.notEmpty(this.options.collectionName, "collectionName can not be blank or null");
        Validate.notNull(this.options.mapper, "MongoMapper can not be null");
        this.mongoClient = new MongoDBClient(this.options.url, this.options.collectionName);
    }

    @Override
    public void beginCommit(Long txid) {
        LOG.debug("beginCommit is noop.");
    }

    @Override
    public void commit(Long txid) {
        LOG.debug("commit is noop.");
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
        List<Document> documents = Lists.newArrayList();
        Iterator var4 = tuples.iterator();

        while(var4.hasNext()) {
            TridentTuple tuple = (TridentTuple)var4.next();
//            Document document = this.options.mapper.toDocument(tuple);
//            documents.add(document);

            final Integer assessmentId = tuple.getIntegerByField(Field.ASSESSMENTID);
            final Integer sessionId = tuple.getIntegerByField(Field.SESSIONID);
            final Long count = tuple.getLongByField(Field.SUM);
            final Double sum = tuple.getDoubleByField(Field.COUNT);

            LOG.info("sum is {}, count is {}", sum, count);

            //filter
            BasicDBObject match = new BasicDBObject();
            match.put("homeworkAssessmentId", assessmentId);
            match.put("sessionId", sessionId);

            this.mongoClient.update(match, new BasicDBObject("$set",new BasicDBObject("homeworkCompletePercent.avgScore",count == 0 ? 0: (sum/count))), true);
        }
    }

    public static class Options implements Serializable {
        private String url;
        private String collectionName;
        private MongoMapper mapper;

        public Options() {
        }

        public CustomMongoState.Options withUrl(String url) {
            this.url = url;
            return this;
        }

        public CustomMongoState.Options withCollectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public CustomMongoState.Options withMapper(MongoMapper mapper) {
            this.mapper = mapper;
            return this;
        }
    }
}
