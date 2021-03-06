package aggregation;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.utils.TupleUtils;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class AggregatorBolt extends BaseRichBolt {
    private Long maxBatchSize = 2000L;
    private boolean forcedFlush = false;
    
    private int flushIntervalSecs = 5;

    private Long demCount = 0L;
    private Long repCount = 0L;
    private Double max_time = Double.NEGATIVE_INFINITY;

    private String state = "";
    
    List<Tuple> anchors = new ArrayList<Tuple>();
    OutputCollector collector;

    
    public AggregatorBolt(String state, Long maxBatchSize, int flushSecs) {
        this.state = state;
        this.maxBatchSize = maxBatchSize;
        this.flushIntervalSecs = flushSecs;
    }

    @Override
    public void prepare(
        Map<String, Object> topoConf, 
        TopologyContext context, 
        OutputCollector collector
    ) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) { 
            emit_batch();
        }
        else {
            if(tuple.getStringByField("party").equals("D")) {
                demCount++;
            }
            else { 
                repCount++;
            }

            if(tuple.getDoubleByField("event_time") > max_time) {
                max_time = tuple.getDoubleByField("event_time");
            }

            anchors.add(tuple);
        }

        if(anchors.size() > maxBatchSize) emit_batch();
    }

    private void emit_batch() {
        if(demCount + repCount == 0) { return; }

        // Emit the aggregates
        collector.emit(anchors, new Values(state, demCount, repCount, max_time));
        
        for(Tuple tup : anchors) 
            collector.ack(tup);
        
        anchors.clear();

        // Reset the aggregates
        demCount = 0L;
        repCount = 0L;
        max_time = Double.NEGATIVE_INFINITY;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("state", "Dvotes", "Rvotes", "time"));
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(
            super.getComponentConfiguration(), flushIntervalSecs
        );
    }
}
