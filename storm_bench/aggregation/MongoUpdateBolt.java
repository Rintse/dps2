package aggregation;

import org.apache.commons.lang.Validate;
import org.apache.storm.mongodb.common.QueryFilterCreator;
import org.apache.storm.mongodb.common.mapper.MongoUpdateMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.apache.storm.mongodb.bolt.AbstractMongoBolt;
import org.apache.storm.tuple.Fields;
import com.mongodb.client.model.Filters;

import java.io.Serializable;
import java.util.Arrays;
import java.time.Instant;

public class MongoUpdateBolt extends AbstractMongoBolt {

    private QueryFilterCreator queryCreator;
    private MongoUpdateMapper mapper;

    private TimeGetter timeGetter = new SystemTime();

    private boolean upsert;  //the default is false.
    private boolean many;  //the default is false.

    public MongoUpdateBolt(String url, String collectionName, QueryFilterCreator queryCreator, MongoUpdateMapper mapper) {
        super(url, collectionName);

        Validate.notNull(queryCreator, "QueryFilterCreator can not be null");
        Validate.notNull(mapper, "MongoUpdateMapper can not be null");

        this.queryCreator = queryCreator;
        this.mapper = mapper;
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) { return; }

        // Party for this aggregation
	    String party = tuple.getString(0);

        // The results of the aggregation
        AgResult res = (AgResult) tuple.getValue(1);

        // Calculate latency at the moment before output
        Long votes = res.votes;
        Double max_event_time = res.time;
        Double cur_time = timeGetter.get();
        Double latency = cur_time - max_event_time;
        String county = res.county;

        try {
            Bson filter = Filters.eq("county", county);
            Bson update = com.mongodb.client.model.Updates.inc(party, votes);

            mongoClient.update(filter, update, upsert, many);
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    public MongoUpdateBolt withUpsert(boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    public MongoUpdateBolt withMany(boolean many) {
        this.many = many;
        return this;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    }


    private interface TimeGetter {
        public Double get();
    }

    // Gets time from system clock
    private class SystemTime implements TimeGetter, Serializable {
        @Override
        public Double get() {
            Instant time = Instant.now(); // Instant.now() supports nanos since epoch
            return Double.valueOf(time.getEpochSecond()) + Double.valueOf(time.getNano()) / (1000.0*1000*1000);
        }
    }
}
