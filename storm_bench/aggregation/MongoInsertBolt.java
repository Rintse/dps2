// Max Blankestijn & Rintse van de Vlasakker
// An altered version of the MongoInsertBolt provided with storm

// Removes batched insertion to allow for accurate timestamping of insertion events

// The most notable alterations are the changes in execute() and the addition of
// a timegetter object that gets either the system time, or the time from an NTP server.

// Original MongoInsertBolt:
// https://github.com/apache/storm/blob/master/external/storm-mongodb/src/main/java/org/apache/storm/mongodb/bolt/MongoInsertBolt.java

package aggregation;
import aggregation.AggregationResult;

import org.apache.commons.lang.Validate;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.bson.Document;
import org.apache.storm.mongodb.bolt.AbstractMongoBolt;
import org.apache.storm.cassandra.trident.state.SimpleTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.streams.Pair;
import org.apache.storm.utils.TupleUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Arrays;
import org.apache.storm.tuple.Fields;
import org.apache.commons.net.ntp.TimeStamp;
import java.net.InetAddress;
import java.time.Instant;
import org.apache.commons.net.ntp.NTPUDPClient;
import java.net.SocketException;
import java.io.IOException;
import java.io.Serializable;


public class MongoInsertBolt extends AbstractMongoBolt {

    private static final int DEFAULT_FLUSH_INTERVAL_SECS = 1;
    private MongoMapper mapper;
    private boolean ordered = true;  //default is ordered.
    private TimeGetter timeGetter = new SystemTime();

    public MongoInsertBolt(String url, String collectionName, MongoMapper mapper) {
        super(url, collectionName);
        Validate.notNull(mapper, "MongoMapper can not be null");
        this.mapper = mapper;
    }

    @Override
    public void execute(Tuple tuple) {
        if(TupleUtils.isTick(tuple)) { return; }

        // GemID for this aggregation
	    String gemID = Integer.toString(tuple.getInteger(0));

        // The results of the aggregation
        AggregationResult res = (AggregationResult) tuple.getValue(1);

        // Calculate latency at the moment before output
        Double max_event_time = res.event_time;
        Double cur_time = timeGetter.get();
        Double latency = cur_time - max_event_time;
        String aggregate = Integer.toString(res.price);

        // Build up the final result tuple
        Fields outputFields = new Fields(Arrays.asList("GemID", "aggregate", "latency", "time"));
        List<Object> outputValues = Arrays.asList(gemID, aggregate, latency, cur_time);
        SimpleTuple outputTuple = new SimpleTuple(outputFields, outputValues);

        // And insert into the mongodb
        mongoClient.insert(Arrays.asList(mapper.toDocument(outputTuple)), ordered);
        collector.ack(tuple);
        System.out.println("MONGOINSERT");
    }

    public MongoInsertBolt withOrdered(boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(topoConf, context, collector);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    private interface TimeGetter {
        public Double get();
    }

    // Gets time from NTP server
    private class NTPTime implements TimeGetter, Serializable {
	String NTP_IP;

        public NTPTime(String _NTP_IP) {
            NTP_IP = _NTP_IP;
        }

        @Override
        public Double get() {
            final NTPUDPClient client = new NTPUDPClient();
            try { client.open(); }
            catch (final SocketException e) { System.out.println("Could not establish NTP connection"); }

            Double time = 0.0;
            try {
                TimeStamp recv_time = client // Get an NTP message
                    .getTime(InetAddress.getByName(NTP_IP))
                    .getMessage()
                    .getReceiveTimeStamp();

                // Extract seconds since epoch with nano accuracy
                Double integer_part = Long.valueOf(recv_time.getSeconds()).doubleValue();
                Double fraction = Long.valueOf(recv_time.getFraction()).doubleValue() / 0xFFFFFFFF;
                return integer_part + fraction;
            }
            catch (IOException e) { System.out.println("Could not get time from NTP server"); }
            // NTP request has failed
            return 0.0;
        }
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
