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
import org.apache.storm.task.TopologyContext;

import java.util.concurrent.LinkedBlockingQueue;
import java.lang.Thread;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.time.Instant;

public class MongoUpdateBolt extends BaseRichBolt {
    // (could) TODO: make sure this contains only one entry per county at all times
    private LinkedBlockingQueue< UpdateOneModel<Document> > queue;
    
    private final Integer NUM_COUNTIES = 3233;
    
    private Thread updateThread;
    private volatile boolean running;

    private String url;
    private String collectionName;

    protected OutputCollector collector;
    protected BulkMongoClient mongoClient;

    public MongoUpdateBolt(String url, String collectionName) {
        Validate.notEmpty(url, "url can not be blank or null");
        Validate.notEmpty(collectionName, "collectionName can not be blank or null");
        
        this.queue = new LinkedBlockingQueue< UpdateOneModel<Document> >();
        this.url = url;
        this.collectionName = collectionName;
    }

    @Override
    public void prepare(
        Map<String, Object> topoConf, 
        TopologyContext context,
        OutputCollector collector
    ) {
        this.collector = collector;
        this.mongoClient = new BulkMongoClient(url, collectionName);
        
        running = true;
        updateThread = new Thread(new BatchUpdater());
        updateThread.start();
    }

    @Override
    public void cleanup() {
        this.mongoClient.close();
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) { return; }

        // County for this aggregation
	    String county = tuple.getString(0);

        // The results of the aggregation
        AgResult res = (AgResult) tuple.getValue(1);

        // Calculate latency at the moment before output
        // Double max_event_time = res.time;
        // Double cur_time = sysTimeSeconds();
        // Double latency = cur_time - max_event_time;
        
        String party = res.party + "votes"; // county of the aggregation
        Long votes = res.votes; // aggregation total

        Bson filter = Filters.eq("county", county);
        Bson update = com.mongodb.client.model.Updates.inc(party, votes);
    
        try{
            queue.put(new UpdateOneModel<Document>(filter, update));
            this.collector.ack(tuple);
        } catch(Exception e) { 
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    private class BatchUpdater implements Runnable {
        @Override
        public void run() {
            while (running) {
                if(queue.size() >= NUM_COUNTIES) {
                    LinkedList< UpdateOneModel<Document> > updates = 
                        new LinkedList< UpdateOneModel<Document> >();
                    queue.drainTo(updates);
                    
                    Integer count = updates.size();
                    Double start = sysTimeSeconds();

                    mongoClient.batchUpdate(updates);
                    
                    System.out.println(
                        Long.toString(count) + 
                        " batch updates took " + 
                        Double.toString(sysTimeSeconds()-start) + 
                        " seconds."
                    );
                }
            }
        }
    }

    // Gets time from system clock
    public Double sysTimeSeconds() {
        Instant time = Instant.now(); // Instant.now() supports nanos since epoch
        return  Double.valueOf(time.getEpochSecond()) + 
                Double.valueOf(time.getNano()) / (1000.0*1000*1000);
    }
}
