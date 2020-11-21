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

    public MongoUpdateBolt(String url, String collectionName) {
        super(url, collectionName);
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) { return; }

        // County for this aggregation
	    String county = tuple.getString(0);

        // The results of the aggregation
        AgResult res = (AgResult) tuple.getValue(1);

        // Calculate latency at the moment before output
        Double max_event_time = res.time;
        Double cur_time = sysTime();
        Double latency = cur_time - max_event_time;
        
        String party = res.party + "votes"; // county of the aggregation
        Long votes = res.votes; // aggregation total

        try {
            Bson filter = Filters.eq("county", county);
            Bson update = com.mongodb.client.model.Updates.inc(party, votes);

            mongoClient.update(filter, update, false, false);
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    // Gets time from system clock
    public Double sysTime() {
        Instant time = Instant.now(); // Instant.now() supports nanos since epoch
        return  Double.valueOf(time.getEpochSecond()) + 
                Double.valueOf(time.getNano()) / (1000.0*1000*1000);
    }
}
