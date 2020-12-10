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

    
    public AggregatorBolt(Long maxBatchSize, int flushSecs) {
        flushIntervalSecs = flushSecs;
        this.maxBatchSize = maxBatchSize;
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
            forcedFlush = true;
        }
        else {
            System.out.print("Agg: ");
            System.out.println(tuple);

            if(state.equals("")) {
                state = tuple.getStringByField("state");
            }
           
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

        if(shouldFlush()) emit_batch();
    }

    private boolean shouldFlush() {
        boolean forced = forcedFlush;
        if(forced) forcedFlush = false;

        return demCount + repCount >= maxBatchSize || forced;
    }

    private void emit_batch() {
        if(demCount + repCount == 0) { return; }

        // Emit the aggregates
        collector.emit(anchors, new Values(state, demCount, repCount, max_time));

        // Ack all the tuples that participated in the aggregate
        for(Tuple t : anchors) {
            collector.ack(t);
        }

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
