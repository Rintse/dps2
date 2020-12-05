package aggregation;

import org.apache.commons.lang.Validate;
import org.apache.storm.mongodb.common.QueryFilterCreator;
import org.apache.storm.mongodb.common.mapper.MongoUpdateMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.InsertOneModel;
import org.apache.storm.task.TopologyContext;

import java.util.concurrent.LinkedBlockingQueue;
import java.lang.Thread;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;


public class MongoUpdateBolt extends BaseRichBolt {
    // (could) TODO: make sure this contains only one entry per state at all times
    private LinkedBlockingQueue< UpdateOneModel<Document> > dataQueue;
    private LinkedBlockingQueue< InsertOneModel<Document> > latencyQueue;
    
    private final Integer MAX_BATCH_SIZE = 150;
    
    private Thread updateThread;
    private Thread latencyThread;
    private volatile boolean running;

    private String dataUrl;
    private String latencyUrl;
    private String dataCollection;
    private String latencyCollection;

    private AtomicBoolean dataFlush;
    private AtomicBoolean latencyFlush;

    protected OutputCollector collector;
    protected BulkMongoClient dataClient;
    protected BulkMongoClient latencyClient;

    private int flushIntervalSecs = 5;
    private Long totalVotes = new Long(0);

    public MongoUpdateBolt(
        String dataUrl, String dataCollection,
        String latencyUrl, String latencyCollection,
        int flushSecs
    ) {
        Validate.notEmpty(dataUrl, "url cant be blank or null");
        Validate.notEmpty(dataCollection, "collection cant be blank or null");
        Validate.notEmpty(latencyUrl, "url cant be blank or null");
        Validate.notEmpty(latencyCollection, "collection cant be blank or null");
        
        this.dataUrl = dataUrl;
        this.latencyUrl = latencyUrl;
        this.dataCollection = dataCollection;
        this.latencyCollection = latencyCollection;

        assert(flushSecs >= 1);
        flushIntervalSecs = flushSecs;
    }

    @Override
    public void prepare(
        Map<String, Object> topoConf, 
        TopologyContext context,
        OutputCollector collector
    ) {
        this.dataQueue = new LinkedBlockingQueue< UpdateOneModel<Document> >();
        this.latencyQueue = new LinkedBlockingQueue< InsertOneModel<Document> >();

        this.dataFlush = new AtomicBoolean(false);
        this.latencyFlush = new AtomicBoolean(false);

        this.collector = collector;
        this.dataClient = new BulkMongoClient(dataUrl, dataCollection);
        this.latencyClient = new BulkMongoClient(latencyUrl, latencyCollection);
        
        running = true;
        updateThread = new Thread(new BatchUpdater());
        updateThread.start();

        latencyThread = new Thread(new LatencyLogger());
        latencyThread.start();
    }

    @Override
    public void cleanup() {
        running = false;
        updateThread.interrupt();
        latencyThread.interrupt();

        // TODO: Flush instead of clearing
        dataQueue.clear();
        latencyQueue.clear();
        
        this.dataClient.close();
        this.latencyClient.close();

    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) { 
            dataFlush.set(true);
            latencyFlush.set(true);
            return; 
        }

        // County for this aggregation
	    String state = tuple.getString(0);

        // The results of the aggregation
        AgResult res = (AgResult) tuple.getValue(1);

        // Calculate latency at the moment before batching
        Double max_event_time = res.time;
        Double cur_time = sysTimeSeconds();
        Double latency = cur_time - max_event_time;
        try {
            latencyQueue.put(new InsertOneModel<Document>(
                new Document("time", max_event_time)
                .append("latency", latency)
            ));
        } catch(Exception e) { System.out.println("Error logging latency"); }
        
        String party = res.party + "votes"; // state of the aggregation
        Long votes = res.votes; // aggregation total

        Bson filter = Filters.eq("state", state);
        Bson update = com.mongodb.client.model.Updates.inc(party, votes);
    
        try{ // Guarantees tuple is handled
            dataQueue.put(new UpdateOneModel<Document>(filter, update));
            this.collector.ack(tuple);
        } catch(Exception e) {
            System.out.println("ERWT");
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    private class LatencyLogger implements Runnable {
        @Override
        public void run() {
            while (running) {
                if(shouldFlushLatencies()) {
                    LinkedList< InsertOneModel<Document> > inserts = 
                        new LinkedList< InsertOneModel<Document> >();
                    
                    latencyQueue.drainTo(inserts);
                    latencyClient.batchInsert(inserts);
                } 
            }
        }
    }

    private class BatchUpdater implements Runnable {
        @Override
        public void run() {
            while (running) {
                if(shouldFlushData()) {
                    LinkedList< UpdateOneModel<Document> > updates = 
                        new LinkedList< UpdateOneModel<Document> >();
                    
                    dataQueue.drainTo(updates);
                    dataClient.batchUpdate(updates);
                }
            }
        }
    }

    boolean shouldFlushData() {
        boolean forced = dataFlush.getAndSet(false);
        return dataQueue.size() > MAX_BATCH_SIZE || forced;
    }

    boolean shouldFlushLatencies() {
        boolean forced = latencyFlush.getAndSet(false);
        return latencyQueue.size() > MAX_BATCH_SIZE || forced;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(
            super.getComponentConfiguration(), flushIntervalSecs
        );
    }

    // Gets time from system clock
    public Double sysTimeSeconds() {
        Instant time = Instant.now(); // Instant.now() supports nanos since epoch
        return  Double.valueOf(time.getEpochSecond()) + 
                Double.valueOf(time.getNano()) / (1000.0*1000*1000);
    }
}
