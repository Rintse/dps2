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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;


public class MongoUpdateBolt extends BaseRichBolt {
    // (could) TODO: make sure this contains only one entry per state at all times
    private ArrayList< UpdateOneModel<Document> > dataQueue;
    private ArrayList< InsertOneModel<Document> > latencyQueue;
    
    private String dataUrl;
    private String latencyUrl;
    private String dataCollection;
    private String latencyCollection;

    protected OutputCollector collector;
    protected BulkMongoClient dataClient;
    protected BulkMongoClient latencyClient;

    private int flushIntervalSecs = 5;
    private long maxBatchSize = 5000;
    private Long totalVotes = new Long(0);
    

    public MongoUpdateBolt(
        String dataUrl, String dataCollection,
        String latencyUrl, String latencyCollection,
        long batchSize, int flushSecs
    ) {
        Validate.notEmpty(dataUrl, "url cant be blank or null");
        Validate.notEmpty(dataCollection, "collection cant be blank or null");
        Validate.notEmpty(latencyUrl, "url cant be blank or null");
        Validate.notEmpty(latencyCollection, "collection cant be blank or null");
        
        this.dataUrl = dataUrl;
        this.latencyUrl = latencyUrl;
        this.dataCollection = dataCollection;
        this.latencyCollection = latencyCollection;

        maxBatchSize = batchSize;
        flushIntervalSecs = flushSecs;
    }

    @Override
    public void prepare(
        Map<String, Object> topoConf, 
        TopologyContext context,
        OutputCollector collector
    ) {
        this.dataQueue = new ArrayList< UpdateOneModel<Document> >();
        this.latencyQueue = new ArrayList< InsertOneModel<Document> >();

        this.collector = collector;
        this.dataClient = new BulkMongoClient(dataUrl, dataCollection);
        this.latencyClient = new BulkMongoClient(latencyUrl, latencyCollection);
    }

    @Override
    public void cleanup() {
        // TODO: Flush instead of clearing
        dataQueue.clear();
        latencyQueue.clear();
        
        this.dataClient.close();
        this.latencyClient.close();
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) { 
            // Insert regular data
            if(dataQueue.size() > 0) update_batch();
            // Idle (no data in queue): use time to insert latencies
            else insert_latency_batch();
        }
        else {
            // Calculate latency at the moment before batching
            Double max_event_time = tuple.getDoubleByField("time");
            Double cur_time = sysTimeSeconds();
            Double latency = cur_time - max_event_time;
            try {
                latencyQueue.add(new InsertOneModel<Document>(
                    new Document("time", cur_time)
                    .append("latency", latency)
                ));
            } catch(Exception e) { System.out.println("Error logging latency"); }
            
            String state = tuple.getStringByField("state");
            Long rvotes = tuple.getLongByField("Rvotes");
            Long dvotes = tuple.getLongByField("Dvotes");

            Bson filter = Filters.eq("state", state);
            Bson update = com.mongodb.client.model.Updates.combine(
                com.mongodb.client.model.Updates.inc("Rvotes", rvotes),
                com.mongodb.client.model.Updates.inc("Dvotes", dvotes)
            );
        
            try{ // Guarantees tuple is handled
                dataQueue.add(new UpdateOneModel<Document>(filter, update));
                this.collector.ack(tuple);
            } catch(Exception e) {
                this.collector.reportError(e);
                this.collector.fail(tuple);
            }
        }   

        if(dataQueue.size() >= maxBatchSize) update_batch();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    private void update_batch() {
        if(dataQueue.size() == 0) return;

        // Perform a mongo batch update
        dataClient.batchUpdate(dataQueue);
        dataQueue.clear();
    }
    
    private void insert_latency_batch() {
        if(latencyQueue.size() == 0) return;

        latencyClient.batchInsert(latencyQueue);
        latencyQueue.clear();
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
